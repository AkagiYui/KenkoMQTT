//! 内嵌 MQTT 3.1.1 broker：基于 rumqttc 的 v4 报文编解码，自建 accept/路由循环。
//! 相比 rumqttd 可干净地随时启停（tokio watch 关停信号），并暴露连接/事件供 UI 展示。
//! 传输：TCP。跨平台（含 Android，纯 tokio 网络）。
//!
//! 已实现（P0/P1）：
//! - QoS 0/1/2 完整（含 PUBREC/PUBREL/PUBCOMP 四次握手与 inflight 状态机）
//! - 投递 QoS 降级 min(发布,订阅)；发布主题/订阅过滤器合法性校验
//! - 持久会话（clean_session=0）：订阅、离线消息队列、inflight 随会话保持，重连补发
//! - 同 client_id 顶替、keepalive 空闲超时、出站有界队列背压
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use rumqttc::mqttbytes::v4::{
    self, ConnAck, ConnectReturnCode, LastWill, Packet, PingResp, PubAck, PubComp, PubRec, PubRel,
    Publish, SubAck, SubscribeReasonCode, UnsubAck,
};
use rumqttc::mqttbytes::{Error as MqttError, QoS};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Notify};

const MAX_PKT: usize = 256 * 1024;
/// 单连接出站队列容量（有界，提供背压 / 丢弃点，避免慢消费者导致内存无界增长）。
const OUT_CAP: usize = 1024;
/// 离线会话消息队列上限（超出丢弃最旧，避免离线会话无界增长）。
const MAX_QUEUE: usize = 4096;

/// 取两个 QoS 的较小值（投递 QoS = min(发布 QoS, 订阅 QoS)）。
fn qos_min(a: QoS, b: QoS) -> QoS {
    if (a as u8) <= (b as u8) {
        a
    } else {
        b
    }
}

/// 校验发布主题名合法性：非空、无通配符、无 NUL。
fn valid_topic(t: &str) -> bool {
    !t.is_empty() && !t.contains('#') && !t.contains('+') && !t.contains('\u{0}')
}

/// 校验订阅过滤器合法性（`+` 独占层级；`#` 只能出现在末尾且独占层级）。
fn valid_filter(f: &str) -> bool {
    if f.is_empty() || f.contains('\u{0}') {
        return false;
    }
    let levels: Vec<&str> = f.split('/').collect();
    let n = levels.len();
    for (i, lv) in levels.iter().enumerate() {
        if lv.contains('#') {
            if *lv != "#" || i != n - 1 {
                return false;
            }
        }
        if lv.contains('+') && *lv != "+" {
            return false;
        }
    }
    true
}

/// 事件下沉抽象：把 broker 与具体 UI 框架（Tauri）解耦，便于独立编译与集成测试。
/// 生产侧由 Tauri 的 `AppHandle` 适配实现；测试侧用收集器 / 空实现。
pub trait EventSink: Send + Sync + 'static {
    fn emit_json(&self, event: &str, payload: serde_json::Value);
}

/// 便捷发射：任意可序列化负载 -> JSON -> sink。
fn sink_emit<S: Serialize>(sink: &Option<Arc<dyn EventSink>>, event: &str, payload: S) {
    if let Some(s) = sink {
        if let Ok(v) = serde_json::to_value(payload) {
            s.emit_json(event, v);
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BrokerConfig {
    pub host: String,
    pub port: u16,
    #[serde(default = "yes")]
    pub allow_anonymous: bool,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub max_clients: usize, // 0 = 不限
}
fn yes() -> bool {
    true
}
impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".into(),
            port: 1883,
            allow_anonymous: true,
            username: String::new(),
            password: String::new(),
            max_clients: 0,
        }
    }
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ClientRow {
    pub client_id: String,
    pub addr: String,
    pub username: String,
    pub subs: usize,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BrokerEvent {
    pub kind: String, // connect | disconnect | subscribe | unsubscribe | publish
    pub client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    pub ts: u64,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct BrokerStats {
    running: bool,
    clients_connected: u64,
    msgs_received: u64,
    msgs_sent: u64,
    retained: u64,
    dropped: u64,
    sessions: u64,
}

/// 发往某个客户端写任务的出站报文。
enum Out {
    ConnAck(ConnAck),
    SubAck(SubAck),
    UnsubAck(UnsubAck),
    PubAck(PubAck),
    PubRec(PubRec),
    PubRel(PubRel),
    PubComp(PubComp),
    PingResp,
    Publish(Publish),
}
impl Out {
    fn write(&self, buf: &mut BytesMut) -> Result<usize, MqttError> {
        match self {
            Out::ConnAck(p) => p.write(buf),
            Out::SubAck(p) => p.write(buf),
            Out::UnsubAck(p) => p.write(buf),
            Out::PubAck(p) => p.write(buf),
            Out::PubRec(p) => p.write(buf),
            Out::PubRel(p) => p.write(buf),
            Out::PubComp(p) => p.write(buf),
            Out::PingResp => PingResp.write(buf),
            Out::Publish(p) => p.write(buf),
        }
    }
}

/// 出站可靠投递（QoS≥1）状态：等待 PUBACK(QoS1) / PUBREC(QoS2 首段) 或 等待 PUBCOMP(QoS2 次段)。
#[derive(Clone)]
enum Phase {
    /// QoS1：等待 PUBACK；QoS2：等待 PUBREC。
    AwaitAck,
    /// QoS2：已收到 PUBREC 并发出 PUBREL，等待 PUBCOMP。
    AwaitComp,
}
#[derive(Clone)]
struct Inflight {
    publish: Publish,
    phase: Phase,
}

/// 在线连接句柄。
struct Conn {
    tx: mpsc::Sender<Out>,
    conn_id: u64,
    kick: Arc<Notify>,
}

/// 会话：clean_session=0 时跨连接保持（订阅 / 离线队列 / inflight / QoS2 入站去重）。
struct Session {
    online: Option<Conn>,
    addr: String,
    username: String,
    clean: bool,
    subs: Vec<(String, QoS)>,
    next_pkid: u16,
    /// 已发出、等待确认的出站消息（重连时重发）。
    inflight: BTreeMap<u16, Inflight>,
    /// 离线期间累积、等待补发的出站消息（尚未分配 pkid）。
    queue: VecDeque<Publish>,
    /// 入站 QoS2：收到 PUBLISH 暂存，等待 PUBREL 再路由（保证 exactly-once）。
    incoming_qos2: HashMap<u16, Publish>,
    will: Option<LastWill>,
}

impl Session {
    fn new(addr: String, username: String, clean: bool) -> Self {
        Session {
            online: None,
            addr,
            username,
            clean,
            subs: vec![],
            next_pkid: 0,
            inflight: BTreeMap::new(),
            queue: VecDeque::new(),
            incoming_qos2: HashMap::new(),
            will: None,
        }
    }

    /// 分配一个未被占用的报文标识符（1..=65535）。
    fn alloc_pkid(&mut self) -> u16 {
        for _ in 0..=u16::MAX {
            self.next_pkid = self.next_pkid.wrapping_add(1);
            if self.next_pkid == 0 {
                self.next_pkid = 1;
            }
            if !self.inflight.contains_key(&self.next_pkid) {
                return self.next_pkid;
            }
        }
        self.next_pkid
    }

    /// 向该会话投递一条已按订阅 QoS 降级的消息。
    /// 在线：QoS0 直接发；QoS≥1 分配 pkid、发送并记入 inflight。
    /// 离线且持久：QoS≥1 入离线队列；QoS0 丢弃。
    fn deliver(&mut self, mut p: Publish, eff: QoS) -> Option<u64> {
        p.qos = eff;
        p.dup = false;
        if eff == QoS::AtMostOnce {
            p.pkid = 0;
            if let Some(c) = &self.online {
                return match c.tx.try_send(Out::Publish(p)) {
                    Ok(()) => Some(1),
                    Err(_) => None,
                };
            }
            return None; // 离线 QoS0 丢弃
        }
        // QoS≥1
        if self.online.is_some() {
            let pkid = self.alloc_pkid();
            p.pkid = pkid;
            let sent = self
                .online
                .as_ref()
                .map(|c| c.tx.try_send(Out::Publish(p.clone())).is_ok())
                .unwrap_or(false);
            self.inflight.insert(
                pkid,
                Inflight {
                    publish: p,
                    phase: Phase::AwaitAck,
                },
            );
            if sent {
                Some(1)
            } else {
                None
            }
        } else if !self.clean {
            if self.queue.len() >= MAX_QUEUE {
                self.queue.pop_front();
            }
            self.queue.push_back(p);
            None
        } else {
            None
        }
    }

    /// 重连后需要重发的报文：inflight（QoS1/2 未确认）重发 + 离线队列补发。
    fn resume_outbound(&mut self) -> Vec<Out> {
        let mut outs = vec![];
        for (pkid, inf) in self.inflight.iter() {
            match inf.phase {
                Phase::AwaitAck => {
                    let mut p = inf.publish.clone();
                    p.dup = true;
                    outs.push(Out::Publish(p));
                }
                Phase::AwaitComp => outs.push(Out::PubRel(PubRel::new(*pkid))),
            }
        }
        let queued: Vec<Publish> = self.queue.drain(..).collect();
        for mut p in queued {
            let pkid = self.alloc_pkid();
            p.pkid = pkid;
            p.dup = false;
            outs.push(Out::Publish(p.clone()));
            self.inflight.insert(
                pkid,
                Inflight {
                    publish: p,
                    phase: Phase::AwaitAck,
                },
            );
        }
        outs
    }
}

#[derive(Default)]
struct Core {
    sessions: HashMap<String, Session>,
    retained: HashMap<String, Publish>,
}

#[derive(Default)]
struct Counters {
    connected: AtomicU64,
    recv: AtomicU64,
    sent: AtomicU64,
    dropped: AtomicU64,
    next_conn: AtomicU64,
}

impl Core {
    fn online_count(&self) -> u64 {
        self.sessions.values().filter(|s| s.online.is_some()).count() as u64
    }
}

struct Running {
    shutdown: watch::Sender<bool>,
    config: BrokerConfig,
    core: Arc<Mutex<Core>>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RetainedRow {
    pub topic: String,
    pub payload: String,
    pub qos: u8,
}

/// 读取当前保留消息（供 UI 的保留消息检查器）。
pub fn retained(state: &BrokerState) -> Vec<RetainedRow> {
    let g = state.running.lock().unwrap();
    let Some(r) = g.as_ref() else { return vec![] };
    let core = r.core.lock().unwrap();
    core.retained
        .values()
        .map(|p| RetainedRow {
            topic: p.topic.clone(),
            payload: String::from_utf8_lossy(&p.payload).to_string(),
            qos: p.qos as u8,
        })
        .collect()
}

#[derive(Default)]
pub struct BrokerState {
    running: Mutex<Option<Running>>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// MQTT 主题过滤器匹配（支持 + 与 #）。
fn topic_matches(filter: &str, topic: &str) -> bool {
    let f: Vec<&str> = filter.split('/').collect();
    let t: Vec<&str> = topic.split('/').collect();
    // 规范：以 `$` 开头的主题（如 $SYS）不被首层通配符 `#`/`+` 匹配。
    if topic.starts_with('$') {
        match f.first() {
            Some(&"#") | Some(&"+") => return false,
            _ => {}
        }
    }
    for (i, seg) in f.iter().enumerate() {
        match *seg {
            "#" => return true,
            "+" => {
                if i >= t.len() {
                    return false;
                }
            }
            s => {
                if i >= t.len() || t[i] != s {
                    return false;
                }
            }
        }
    }
    f.len() == t.len()
}

fn emit<S: Serialize + Clone>(sink: &Option<Arc<dyn EventSink>>, event: &str, payload: S) {
    sink_emit(sink, event, payload);
}

pub fn is_running(state: &BrokerState) -> bool {
    state.running.lock().unwrap().is_some()
}

pub fn current_config(state: &BrokerState) -> Option<BrokerConfig> {
    state.running.lock().unwrap().as_ref().map(|r| r.config.clone())
}

/// 启动 broker。绑定失败或已在运行返回错误。
pub async fn start(
    sink: Arc<dyn EventSink>,
    state: &BrokerState,
    config: BrokerConfig,
) -> Result<(), String> {
    if is_running(state) {
        return Err("broker 已在运行".into());
    }
    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("监听 {addr} 失败: {e}"))?;

    let (sd_tx, sd_rx) = watch::channel(false);
    let core = Arc::new(Mutex::new(Core::default()));
    let counters = Arc::new(Counters::default());
    let cfg = config.clone();

    // accept 循环
    {
        let sink = Some(sink.clone());
        let core = core.clone();
        let counters = counters.clone();
        let mut sd = sd_rx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = sd.changed() => { if *sd.borrow() { break; } }
                    accepted = listener.accept() => {
                        let (stream, peer) = match accepted { Ok(v) => v, Err(_) => continue };
                        let sink = sink.clone();
                        let core = core.clone();
                        let counters = counters.clone();
                        let cfg = cfg.clone();
                        let sd_rx = sd.clone();
                        tokio::spawn(async move {
                            handle_conn(stream, peer.to_string(), sink, core, counters, cfg, sd_rx).await;
                        });
                    }
                }
            }
        });
    }

    // 统计/客户端列表 定时推送
    {
        let sink = sink.clone();
        let core = core.clone();
        let counters = counters.clone();
        let mut sd = sd_rx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = sd.changed() => { if *sd.borrow() { break; } }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        let (rows, retained, sessions) = {
                            let g = core.lock().unwrap();
                            let rows: Vec<ClientRow> = g.sessions.iter()
                                .filter(|(_, s)| s.online.is_some())
                                .map(|(id, s)| ClientRow {
                                    client_id: id.clone(), addr: s.addr.clone(),
                                    username: s.username.clone(), subs: s.subs.len(),
                                }).collect();
                            (rows, g.retained.len() as u64, g.sessions.len() as u64)
                        };
                        sink_emit(&Some(sink.clone()), "broker:stats", BrokerStats {
                            running: true,
                            clients_connected: counters.connected.load(Ordering::Relaxed),
                            msgs_received: counters.recv.load(Ordering::Relaxed),
                            msgs_sent: counters.sent.load(Ordering::Relaxed),
                            retained,
                            dropped: counters.dropped.load(Ordering::Relaxed),
                            sessions,
                        });
                        sink_emit(&Some(sink.clone()), "broker:clients", rows);
                    }
                }
            }
        });
    }

    *state.running.lock().unwrap() = Some(Running { shutdown: sd_tx, config, core });
    sink_emit(&Some(sink), "broker:status", serde_json::json!({"running": true}));
    Ok(())
}

pub fn stop(sink: &Arc<dyn EventSink>, state: &BrokerState) {
    if let Some(r) = state.running.lock().unwrap().take() {
        let _ = r.shutdown.send(true);
    }
    sink.emit_json("broker:status", serde_json::json!({"running": false}));
}

async fn handle_conn(
    stream: tokio::net::TcpStream,
    peer: String,
    app: Option<Arc<dyn EventSink>>,
    core: Arc<Mutex<Core>>,
    counters: Arc<Counters>,
    config: BrokerConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let _ = stream.set_nodelay(true);
    let (mut rd, mut wr) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<Out>(OUT_CAP);

    let writer = tokio::spawn(async move {
        while let Some(out) = rx.recv().await {
            let mut b = BytesMut::new();
            if out.write(&mut b).is_err() {
                continue;
            }
            if wr.write_all(&b).await.is_err() {
                break;
            }
        }
    });

    let conn_id = counters.next_conn.fetch_add(1, Ordering::Relaxed);
    let kick = Arc::new(Notify::new());

    let mut buf = BytesMut::with_capacity(4096);
    let mut client_id = String::new();
    let mut connected = false;
    let mut clean_disconnect = false;
    let mut kicked = false;

    // keepalive 空闲超时：连接前禁用（很长），CONNECT 后置为 1.5×keepalive。
    let mut keepalive = Duration::from_secs(0);
    let idle = tokio::time::sleep(Duration::from_secs(3600));
    tokio::pin!(idle);

    'conn: loop {
        tokio::select! {
            _ = shutdown.changed() => { if *shutdown.borrow() { break; } }
            _ = kick.notified() => { kicked = true; break; }
            _ = &mut idle, if keepalive.as_secs() > 0 => { break; }
            r = rd.read_buf(&mut buf) => {
                match r { Ok(0) => break, Ok(_) => {}, Err(_) => break }
                if keepalive.as_secs() > 0 {
                    idle.as_mut().reset(tokio::time::Instant::now() + keepalive);
                }
                loop {
                    let packet = match v4::read(&mut buf, MAX_PKT) {
                        Ok(p) => p,
                        Err(MqttError::InsufficientBytes(_)) => break,
                        Err(_) => break 'conn,
                    };
                    match packet {
                        Packet::Connect(c) => {
                            if connected { break 'conn; } // 二次 CONNECT 违规
                            let auth_ok = config.allow_anonymous
                                || c.login.as_ref().map_or(false, |l| {
                                    l.username == config.username && l.password == config.password
                                });
                            let cid = if c.client_id.is_empty() {
                                format!("auto-{}", now_ms())
                            } else { c.client_id.clone() };
                            let full = {
                                let g = core.lock().unwrap();
                                config.max_clients > 0
                                    && !g.sessions.contains_key(&cid)
                                    && g.online_count() as usize >= config.max_clients
                            };
                            if !auth_ok || full {
                                let code = if full { ConnectReturnCode::ServiceUnavailable }
                                           else { ConnectReturnCode::BadUserNamePassword };
                                let _ = tx.send(Out::ConnAck(ConnAck::new(code, false))).await;
                                break 'conn;
                            }
                            client_id = cid;
                            let username = c.login.as_ref().map(|l| l.username.clone()).unwrap_or_default();

                            // 会话接管 / 建立，返回 (session_present, 需重发的报文, 被顶替的旧连接)。
                            let (session_present, resend, old_kick) = {
                                let mut g = core.lock().unwrap();
                                // 旧会话是否可续（存在 且 本次非 clean）。
                                let resumable = !c.clean_session
                                    && g.sessions.get(&client_id).is_some();
                                if c.clean_session {
                                    g.sessions.remove(&client_id);
                                }
                                let sess = g.sessions.entry(client_id.clone())
                                    .or_insert_with(|| Session::new(peer.clone(), username.clone(), c.clean_session));
                                let old_kick = sess.online.take().map(|c| c.kick);
                                sess.addr = peer.clone();
                                sess.username = username.clone();
                                sess.clean = c.clean_session;
                                sess.will = c.last_will.clone();
                                sess.online = Some(Conn { tx: tx.clone(), conn_id, kick: kick.clone() });
                                let resend = if resumable { sess.resume_outbound() } else { Vec::new() };
                                counters.connected.store(g.online_count(), Ordering::Relaxed);
                                (resumable, resend, old_kick)
                            };
                            if let Some(k) = old_kick { k.notify_one(); }
                            connected = true;
                            if c.keep_alive > 0 {
                                keepalive = Duration::from_secs((c.keep_alive as u64) * 3 / 2);
                                idle.as_mut().reset(tokio::time::Instant::now() + keepalive);
                            }
                            let _ = tx.send(Out::ConnAck(ConnAck::new(ConnectReturnCode::Success, session_present))).await;
                            for o in resend { let _ = tx.send(o).await; }
                            emit(&app, "broker:event", BrokerEvent {
                                kind: "connect".into(), client_id: client_id.clone(),
                                topic: None, payload: None, ts: now_ms(),
                            });
                        }
                        Packet::Subscribe(s) if connected => {
                            let mut codes = vec![];
                            {
                                let mut g = core.lock().unwrap();
                                if let Some(ci) = g.sessions.get_mut(&client_id) {
                                    for f in &s.filters {
                                        if !valid_filter(&f.path) {
                                            codes.push(SubscribeReasonCode::Failure);
                                            continue;
                                        }
                                        let granted = f.qos; // 支持到 QoS2
                                        if let Some(e) = ci.subs.iter_mut().find(|(p, _)| p == &f.path) {
                                            e.1 = granted;
                                        } else {
                                            ci.subs.push((f.path.clone(), granted));
                                        }
                                        codes.push(SubscribeReasonCode::Success(granted));
                                    }
                                }
                            }
                            let _ = tx.send(Out::SubAck(SubAck::new(s.pkid, codes))).await;
                            // 下发匹配的保留消息（QoS 降级到 min(保留 QoS, 授权 QoS)）。
                            let outs = {
                                let mut g = core.lock().unwrap();
                                let retained: Vec<Publish> = g.retained.values().cloned().collect();
                                let mut outs = vec![];
                                if let Some(sess) = g.sessions.get_mut(&client_id) {
                                    for p in &retained {
                                        let mut best: Option<QoS> = None;
                                        for f in &s.filters {
                                            if valid_filter(&f.path) && topic_matches(&f.path, &p.topic) {
                                                best = Some(match best {
                                                    Some(b) if (b as u8) >= (f.qos as u8) => b,
                                                    _ => f.qos,
                                                });
                                            }
                                        }
                                        if let Some(sq) = best {
                                            let eff = qos_min(p.qos, sq);
                                            let mut d = p.clone();
                                            d.retain = true;
                                            // 复用 deliver 的 pkid/inflight 记账，但保留消息需要 retain=1。
                                            // deliver 会重置 dup/qos/pkid，这里在线时直接借助它。
                                            if sess.online.is_some() {
                                                d.qos = eff;
                                                d.dup = false;
                                                if eff == QoS::AtMostOnce {
                                                    d.pkid = 0;
                                                    outs.push(Out::Publish(d));
                                                } else {
                                                    let pkid = sess.alloc_pkid();
                                                    d.pkid = pkid;
                                                    sess.inflight.insert(pkid, Inflight { publish: d.clone(), phase: Phase::AwaitAck });
                                                    outs.push(Out::Publish(d));
                                                }
                                            }
                                        }
                                    }
                                }
                                outs
                            };
                            for o in outs { let _ = tx.send(o).await; }
                            for f in &s.filters {
                                if valid_filter(&f.path) {
                                    emit(&app, "broker:event", BrokerEvent {
                                        kind: "subscribe".into(), client_id: client_id.clone(),
                                        topic: Some(f.path.clone()), payload: None, ts: now_ms(),
                                    });
                                }
                            }
                        }
                        Packet::Unsubscribe(u) if connected => {
                            {
                                let mut g = core.lock().unwrap();
                                if let Some(ci) = g.sessions.get_mut(&client_id) {
                                    ci.subs.retain(|(f, _)| !u.topics.contains(f));
                                }
                            }
                            let _ = tx.send(Out::UnsubAck(UnsubAck::new(u.pkid))).await;
                            emit(&app, "broker:event", BrokerEvent {
                                kind: "unsubscribe".into(), client_id: client_id.clone(),
                                topic: u.topics.first().cloned(), payload: None, ts: now_ms(),
                            });
                        }
                        Packet::Publish(p) if connected => {
                            if !valid_topic(&p.topic) { break 'conn; }
                            match p.qos {
                                QoS::AtMostOnce => {
                                    counters.recv.fetch_add(1, Ordering::Relaxed);
                                    route_publish(&core, &counters, &p);
                                    emit_publish(&app, &client_id, &p);
                                }
                                QoS::AtLeastOnce => {
                                    counters.recv.fetch_add(1, Ordering::Relaxed);
                                    route_publish(&core, &counters, &p);
                                    emit_publish(&app, &client_id, &p);
                                    let _ = tx.send(Out::PubAck(PubAck::new(p.pkid))).await;
                                }
                                QoS::ExactlyOnce => {
                                    // 入站 QoS2：暂存等待 PUBREL，重复 PUBLISH 只重发 PUBREC。
                                    {
                                        let mut g = core.lock().unwrap();
                                        if let Some(sess) = g.sessions.get_mut(&client_id) {
                                            sess.incoming_qos2.entry(p.pkid).or_insert_with(|| p.clone());
                                        }
                                    }
                                    let _ = tx.send(Out::PubRec(PubRec::new(p.pkid))).await;
                                }
                            }
                        }
                        // 入站 QoS2 第二段：收到 PUBREL 后真正路由并回 PUBCOMP。
                        Packet::PubRel(pr) if connected => {
                            let msg = {
                                let mut g = core.lock().unwrap();
                                g.sessions.get_mut(&client_id).and_then(|s| s.incoming_qos2.remove(&pr.pkid))
                            };
                            if let Some(p) = msg {
                                counters.recv.fetch_add(1, Ordering::Relaxed);
                                route_publish(&core, &counters, &p);
                                emit_publish(&app, &client_id, &p);
                            }
                            let _ = tx.send(Out::PubComp(PubComp::new(pr.pkid))).await;
                        }
                        // 出站 QoS1 确认。
                        Packet::PubAck(pa) if connected => {
                            let mut g = core.lock().unwrap();
                            if let Some(s) = g.sessions.get_mut(&client_id) {
                                s.inflight.remove(&pa.pkid);
                            }
                        }
                        // 出站 QoS2：订阅者已收到 PUBLISH → 回 PUBREL，等待 PUBCOMP。
                        Packet::PubRec(pr) if connected => {
                            let send_rel = {
                                let mut g = core.lock().unwrap();
                                if let Some(s) = g.sessions.get_mut(&client_id) {
                                    if let Some(inf) = s.inflight.get_mut(&pr.pkid) {
                                        inf.phase = Phase::AwaitComp;
                                    }
                                    true
                                } else { false }
                            };
                            if send_rel {
                                let _ = tx.send(Out::PubRel(PubRel::new(pr.pkid))).await;
                            }
                        }
                        // 出站 QoS2 完成。
                        Packet::PubComp(pc) if connected => {
                            let mut g = core.lock().unwrap();
                            if let Some(s) = g.sessions.get_mut(&client_id) {
                                s.inflight.remove(&pc.pkid);
                            }
                        }
                        Packet::PingReq => { let _ = tx.send(Out::PingResp).await; }
                        Packet::Disconnect => { clean_disconnect = true; break 'conn; }
                        _ => {}
                    }
                }
            }
        }
    }

    // 断开处理：clean_session=1 丢弃会话；否则保留会话（订阅/离线队列/inflight），仅置离线。
    if connected {
        let (removed, will) = {
            let mut g = core.lock().unwrap();
            // 仅当在线连接仍是本连接时处理（避免被顶替后误动新连接）。
            let is_current = g
                .sessions
                .get(&client_id)
                .and_then(|s| s.online.as_ref())
                .map(|c| c.conn_id)
                == Some(conn_id);
            if !is_current {
                (false, None)
            } else if clean_disconnect {
                // 客户端主动 DISCONNECT：清理遗嘱，按 clean 决定是否保留会话。
                let sess = g.sessions.get_mut(&client_id).unwrap();
                sess.online = None;
                sess.will = None;
                if sess.clean {
                    g.sessions.remove(&client_id);
                }
                counters.connected.store(g.online_count(), Ordering::Relaxed);
                (true, None)
            } else {
                let sess = g.sessions.get_mut(&client_id).unwrap();
                sess.online = None;
                let will = if kicked { None } else { sess.will.take() };
                if sess.clean {
                    g.sessions.remove(&client_id);
                }
                counters.connected.store(g.online_count(), Ordering::Relaxed);
                (true, will)
            }
        };
        if let Some(w) = will {
            let mut p = Publish::new(w.topic, w.qos, w.message.to_vec());
            p.retain = w.retain;
            route_publish(&core, &counters, &p);
        }
        if removed {
            emit(&app, "broker:event", BrokerEvent {
                kind: "disconnect".into(), client_id: client_id.clone(),
                topic: None, payload: None, ts: now_ms(),
            });
        }
    }
    writer.abort();
}

fn emit_publish(app: &Option<Arc<dyn EventSink>>, client_id: &str, p: &Publish) {
    emit(app, "broker:event", BrokerEvent {
        kind: "publish".into(),
        client_id: client_id.to_string(),
        topic: Some(p.topic.clone()),
        payload: Some(String::from_utf8_lossy(&p.payload).to_string()),
        ts: now_ms(),
    });
}

/// 路由一条发布消息到所有匹配订阅者（含离线持久会话入队）。
fn route_publish(core: &Arc<Mutex<Core>>, counters: &Arc<Counters>, p: &Publish) {
    let mut g = core.lock().unwrap();
    if p.retain {
        if p.payload.is_empty() {
            g.retained.remove(&p.topic);
        } else {
            g.retained.insert(p.topic.clone(), p.clone());
        }
    }
    let mut sent = 0u64;
    let mut dropped = 0u64;
    for sess in g.sessions.values_mut() {
        // 取该订阅者匹配过滤器中的最大授权 QoS。
        let mut best: Option<QoS> = None;
        for (f, q) in &sess.subs {
            if topic_matches(f, &p.topic) {
                best = Some(match best {
                    Some(b) if (b as u8) >= (*q as u8) => b,
                    _ => *q,
                });
            }
        }
        let Some(sub_qos) = best else { continue };
        let eff = qos_min(p.qos, sub_qos);
        // 投递副本 retain=0（新订阅时才带 retain=1）。
        let mut out = p.clone();
        out.retain = false;
        match sess.deliver(out, eff) {
            Some(n) => sent += n,
            None => {
                if sess.online.is_some() && eff == QoS::AtMostOnce {
                    dropped += 1;
                }
            }
        }
    }
    counters.sent.fetch_add(sent, Ordering::Relaxed);
    if dropped > 0 {
        counters.dropped.fetch_add(dropped, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- 纯函数单元测试 ----

    #[test]
    fn test_valid_topic() {
        assert!(valid_topic("a/b/c"));
        assert!(valid_topic("$SYS/broker/x"));
        assert!(!valid_topic(""));
        assert!(!valid_topic("a/+/c"));
        assert!(!valid_topic("a/#"));
        assert!(!valid_topic("a/\u{0}/c"));
    }

    #[test]
    fn test_valid_filter() {
        assert!(valid_filter("a/b/c"));
        assert!(valid_filter("a/+/c"));
        assert!(valid_filter("a/#"));
        assert!(valid_filter("#"));
        assert!(valid_filter("+/+"));
        assert!(!valid_filter("a/#/b"));
        assert!(!valid_filter("a/b#"));
        assert!(!valid_filter("a/c+"));
        assert!(!valid_filter(""));
    }

    #[test]
    fn test_topic_matches_dollar_exclusion() {
        assert!(!topic_matches("#", "$SYS/x"));
        assert!(!topic_matches("+/x", "$SYS/x"));
        assert!(topic_matches("$SYS/#", "$SYS/broker/uptime"));
        assert!(topic_matches("#", "a/b"));
        assert!(topic_matches("a/+", "a/b"));
        assert!(!topic_matches("a/+", "a/b/c"));
    }

    #[test]
    fn test_qos_min() {
        use QoS::*;
        assert_eq!(qos_min(AtMostOnce, ExactlyOnce), AtMostOnce);
        assert_eq!(qos_min(ExactlyOnce, AtLeastOnce), AtLeastOnce);
        assert_eq!(qos_min(AtLeastOnce, AtLeastOnce), AtLeastOnce);
    }

    // ---- 集成测试基座 ----
    fn spawn_broker(cfg: BrokerConfig) -> u16 {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let port = std_listener.local_addr().unwrap().port();
        let (sd_tx, sd_rx) = watch::channel(false);
        Box::leak(Box::new(sd_tx));
        let core = Arc::new(Mutex::new(Core::default()));
        let counters = Arc::new(Counters::default());
        tokio::spawn(async move {
            let listener = TcpListener::from_std(std_listener).unwrap();
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let core = core.clone();
                let counters = counters.clone();
                let cfg = cfg.clone();
                let sd = sd_rx.clone();
                tokio::spawn(async move {
                    handle_conn(stream, peer.to_string(), None, core, counters, cfg, sd).await;
                });
            }
        });
        port
    }

    // P0: 投递 QoS 应降级为 min(发布,订阅)。
    #[tokio::test]
    async fn test_qos_downgrade() {
        use rumqttc::{AsyncClient, Event, MqttOptions, Packet as CPacket, QoS};
        let port = spawn_broker(BrokerConfig::default());
        let mut so = MqttOptions::new("sub", "127.0.0.1", port);
        so.set_keep_alive(Duration::from_secs(5));
        let (sub, mut sel) = AsyncClient::new(so, 10);
        sub.subscribe("k/#", QoS::AtMostOnce).await.unwrap();
        let mut po = MqttOptions::new("pub", "127.0.0.1", port);
        po.set_keep_alive(Duration::from_secs(5));
        let (publisher, mut pel) = AsyncClient::new(po, 10);
        tokio::spawn(async move { loop { if pel.poll().await.is_err() { break; } } });
        for _ in 0..80 {
            match sel.poll().await.unwrap() {
                Event::Incoming(CPacket::SubAck(_)) => {
                    publisher.publish("k/x", QoS::AtLeastOnce, false, b"hi".to_vec()).await.unwrap();
                }
                Event::Incoming(CPacket::Publish(p)) => {
                    assert_eq!(p.qos, QoS::AtMostOnce);
                    assert_eq!(&p.payload[..], b"hi");
                    return;
                }
                _ => {}
            }
        }
        panic!("未收到降级后的消息");
    }

    // P0: 同一 client_id 重连顶替旧连接。
    #[tokio::test]
    async fn test_client_takeover() {
        use rumqttc::{AsyncClient, Event, MqttOptions, Packet as CPacket, QoS};
        let port = spawn_broker(BrokerConfig::default());
        let mut ao = MqttOptions::new("dup", "127.0.0.1", port);
        ao.set_keep_alive(Duration::from_secs(5));
        let (a, mut ael) = AsyncClient::new(ao, 10);
        a.subscribe("t", QoS::AtLeastOnce).await.unwrap();
        for _ in 0..40 {
            if let Event::Incoming(CPacket::SubAck(_)) = ael.poll().await.unwrap() { break; }
        }
        let mut bo = MqttOptions::new("dup", "127.0.0.1", port);
        bo.set_keep_alive(Duration::from_secs(5));
        let (b, mut bel) = AsyncClient::new(bo, 10);
        tokio::spawn(async move { loop { if bel.poll().await.is_err() { break; } } });
        let _ = b.subscribe("t", QoS::AtLeastOnce).await;
        for _ in 0..60 {
            if ael.poll().await.is_err() { return; }
        }
        panic!("旧连接未被顶替踢出");
    }

    // P1: QoS2 端到端（订阅 QoS2、发布 QoS2，完整 PUBREC/PUBREL/PUBCOMP 握手）。
    #[tokio::test]
    async fn test_qos2_roundtrip() {
        use rumqttc::{AsyncClient, Event, MqttOptions, Packet as CPacket, QoS};
        let port = spawn_broker(BrokerConfig::default());
        let mut so = MqttOptions::new("sub2", "127.0.0.1", port);
        so.set_keep_alive(Duration::from_secs(5));
        let (sub, mut sel) = AsyncClient::new(so, 10);
        sub.subscribe("e/#", QoS::ExactlyOnce).await.unwrap();
        let mut po = MqttOptions::new("pub2", "127.0.0.1", port);
        po.set_keep_alive(Duration::from_secs(5));
        let (publisher, mut pel) = AsyncClient::new(po, 10);
        tokio::spawn(async move { loop { if pel.poll().await.is_err() { break; } } });
        for _ in 0..120 {
            match sel.poll().await.unwrap() {
                Event::Incoming(CPacket::SubAck(_)) => {
                    publisher.publish("e/x", QoS::ExactlyOnce, false, b"exactly".to_vec()).await.unwrap();
                }
                Event::Incoming(CPacket::Publish(p)) => {
                    assert_eq!(p.qos, QoS::ExactlyOnce);
                    assert_eq!(&p.payload[..], b"exactly");
                    return;
                }
                _ => {}
            }
        }
        panic!("未完成 QoS2 往返");
    }

    // P1: 持久会话 —— 离线期间的 QoS1 消息在重连后补投，且 session_present=1。
    #[tokio::test]
    async fn test_persistent_session_offline_queue() {
        use rumqttc::{AsyncClient, Event, MqttOptions, Packet as CPacket, QoS};
        let port = spawn_broker(BrokerConfig::default());

        // A: clean_session=false，订阅后主动断开。
        let mut ao = MqttOptions::new("persist", "127.0.0.1", port);
        ao.set_keep_alive(Duration::from_secs(5));
        ao.set_clean_session(false);
        let (a, mut ael) = AsyncClient::new(ao, 10);
        a.subscribe("s", QoS::AtLeastOnce).await.unwrap();
        for _ in 0..40 {
            if let Event::Incoming(CPacket::SubAck(_)) = ael.poll().await.unwrap() { break; }
        }
        a.disconnect().await.unwrap();
        for _ in 0..20 { if ael.poll().await.is_err() { break; } }

        // 发布者在 A 离线期间发送 QoS1。
        let mut po = MqttOptions::new("p3", "127.0.0.1", port);
        po.set_keep_alive(Duration::from_secs(5));
        let (publisher, mut pel) = AsyncClient::new(po, 10);
        tokio::spawn(async move { loop { if pel.poll().await.is_err() { break; } } });
        publisher.publish("s", QoS::AtLeastOnce, false, b"offline".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        // A 以相同 id + clean_session=false 重连，应收到 session_present 与补发消息。
        let mut ro = MqttOptions::new("persist", "127.0.0.1", port);
        ro.set_keep_alive(Duration::from_secs(5));
        ro.set_clean_session(false);
        let (_r, mut rel) = AsyncClient::new(ro, 10);
        let mut present = false;
        for _ in 0..80 {
            match rel.poll().await.unwrap() {
                Event::Incoming(CPacket::ConnAck(ca)) => { present = ca.session_present; }
                Event::Incoming(CPacket::Publish(p)) => {
                    assert_eq!(&p.payload[..], b"offline");
                    assert!(present, "重连应 session_present=1");
                    return;
                }
                _ => {}
            }
        }
        panic!("离线队列消息未补投");
    }

    // 端到端往返（QoS1）。
    #[tokio::test]
    async fn broker_roundtrip() {
        use rumqttc::{AsyncClient, Event, MqttOptions, Packet as CPacket, QoS};
        let port = spawn_broker(BrokerConfig::default());
        let mut mo = MqttOptions::new("t", "127.0.0.1", port);
        mo.set_keep_alive(Duration::from_secs(5));
        let (client, mut eventloop) = AsyncClient::new(mo, 10);
        client.subscribe("k/#", QoS::AtLeastOnce).await.unwrap();
        for _ in 0..50 {
            match eventloop.poll().await.unwrap() {
                Event::Incoming(CPacket::SubAck(_)) => {
                    client.publish("k/x", QoS::AtLeastOnce, false, b"hi".to_vec()).await.unwrap();
                }
                Event::Incoming(CPacket::Publish(p)) => {
                    assert_eq!(&p.payload[..], b"hi");
                    assert_eq!(p.topic, "k/x");
                    return;
                }
                _ => {}
            }
        }
        panic!("未收到往返消息");
    }
}
