//! 内嵌 MQTT 3.1.1 broker：基于 rumqttc 的 v4 报文编解码，自建 accept/路由循环。
//! 相比 rumqttd 可干净地随时启停（tokio watch 关停信号），并暴露连接/事件供 UI 展示。
//! 传输：TCP。跨平台（含 Android，纯 tokio 网络）。
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use rumqttc::mqttbytes::v4::{
    self, ConnAck, ConnectReturnCode, LastWill, Packet, PingResp, PubAck, Publish, SubAck,
    SubscribeReasonCode, UnsubAck,
};
use rumqttc::mqttbytes::{Error as MqttError, QoS};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

const MAX_PKT: usize = 256 * 1024;
/// 单连接出站队列容量（有界，提供背压 / 丢弃点，避免慢消费者导致内存无界增长）。
const OUT_CAP: usize = 1024;

/// 取两个 QoS 的较小值（投递 QoS = min(发布 QoS, 订阅 QoS)）。
fn qos_min(a: QoS, b: QoS) -> QoS {
    if (a as u8) <= (b as u8) {
        a
    } else {
        b
    }
}

/// 校验发布主题名合法性：非空、无通配符、无 NUL、不以 `$`+通配等。
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
}

/// 发往某个客户端写任务的出站报文。
enum Out {
    ConnAck(ConnAck),
    SubAck(SubAck),
    UnsubAck(UnsubAck),
    PubAck(PubAck),
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
            Out::PingResp => PingResp.write(buf),
            Out::Publish(p) => p.write(buf),
        }
    }
}

struct ClientInfo {
    tx: mpsc::Sender<Out>,
    addr: String,
    username: String,
    subs: Vec<(String, QoS)>,
    /// 唯一连接序号：清理时仅当当前表项仍属于本连接才移除，避免顶替后误删新连接。
    conn_id: u64,
    /// 顶替（同 client_id 重连）时用于主动踢掉旧连接。
    kick: Arc<tokio::sync::Notify>,
}

#[derive(Default)]
struct Core {
    clients: HashMap<String, ClientInfo>,
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
                        let (rows, retained) = {
                            let g = core.lock().unwrap();
                            let rows: Vec<ClientRow> = g.clients.iter().map(|(id, c)| ClientRow {
                                client_id: id.clone(), addr: c.addr.clone(),
                                username: c.username.clone(), subs: c.subs.len(),
                            }).collect();
                            (rows, g.retained.len() as u64)
                        };
                        sink_emit(&Some(sink.clone()), "broker:stats", BrokerStats {
                            running: true,
                            clients_connected: counters.connected.load(Ordering::Relaxed),
                            msgs_received: counters.recv.load(Ordering::Relaxed),
                            msgs_sent: counters.sent.load(Ordering::Relaxed),
                            retained,
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
    let kick = Arc::new(tokio::sync::Notify::new());

    let mut buf = BytesMut::with_capacity(4096);
    let mut client_id = String::new();
    let mut connected = false;
    let mut will: Option<LastWill> = None;
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
            _ = &mut idle, if keepalive.as_secs() > 0 => {
                // 超过 1.5×keepalive 未收到任何报文：判定为死连接，断开。
                break;
            }
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
                            // 顶替同名连接不占用新增额度。
                            let full = {
                                let g = core.lock().unwrap();
                                config.max_clients > 0
                                    && !g.clients.contains_key(&cid)
                                    && g.clients.len() >= config.max_clients
                            };
                            if !auth_ok || full {
                                let code = if full { ConnectReturnCode::ServiceUnavailable }
                                           else { ConnectReturnCode::BadUserNamePassword };
                                let _ = tx.send(Out::ConnAck(ConnAck::new(code, false))).await;
                                break 'conn;
                            }
                            client_id = cid;
                            will = c.last_will.clone();
                            let username = c.login.as_ref().map(|l| l.username.clone()).unwrap_or_default();
                            let old = {
                                let mut g = core.lock().unwrap();
                                let old = g.clients.insert(client_id.clone(), ClientInfo {
                                    tx: tx.clone(), addr: peer.clone(), username,
                                    subs: vec![], conn_id, kick: kick.clone(),
                                });
                                counters.connected.store(g.clients.len() as u64, Ordering::Relaxed);
                                old
                            };
                            // 规范：同一 client_id 新连接顶替旧连接，主动踢掉旧连接。
                            if let Some(o) = old { o.kick.notify_one(); }
                            connected = true;
                            if c.keep_alive > 0 {
                                keepalive = Duration::from_secs((c.keep_alive as u64) * 3 / 2);
                                idle.as_mut().reset(tokio::time::Instant::now() + keepalive);
                            }
                            let _ = tx.send(Out::ConnAck(ConnAck::new(ConnectReturnCode::Success, false))).await;
                            emit(&app, "broker:event", BrokerEvent {
                                kind: "connect".into(), client_id: client_id.clone(),
                                topic: None, payload: None, ts: now_ms(),
                            });
                        }
                        Packet::Subscribe(s) if connected => {
                            let mut codes = vec![];
                            {
                                let mut g = core.lock().unwrap();
                                if let Some(ci) = g.clients.get_mut(&client_id) {
                                    for f in &s.filters {
                                        if !valid_filter(&f.path) {
                                            codes.push(SubscribeReasonCode::Failure);
                                            continue;
                                        }
                                        // P0 阶段仅可靠支持到 QoS1，授权 QoS 上限为 1。
                                        let granted = qos_min(f.qos, QoS::AtLeastOnce);
                                        // 重复过滤器：更新 QoS 而非追加，避免重复投递。
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
                            let retained: Vec<Publish> = {
                                let g = core.lock().unwrap();
                                let mut out = vec![];
                                for p in g.retained.values() {
                                    let mut best: Option<QoS> = None;
                                    for f in &s.filters {
                                        if valid_filter(&f.path) && topic_matches(&f.path, &p.topic) {
                                            let gr = qos_min(f.qos, QoS::AtLeastOnce);
                                            best = Some(match best { Some(b) if (b as u8) >= (gr as u8) => b, _ => gr });
                                        }
                                    }
                                    if let Some(sq) = best {
                                        let mut d = p.clone();
                                        d.qos = qos_min(p.qos, sq);
                                        d.dup = false;
                                        d.retain = true;
                                        if d.qos == QoS::AtMostOnce { d.pkid = 0; }
                                        out.push(d);
                                    }
                                }
                                out
                            };
                            for p in retained { let _ = tx.send(Out::Publish(p)).await; }
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
                                if let Some(ci) = g.clients.get_mut(&client_id) {
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
                            counters.recv.fetch_add(1, Ordering::Relaxed);
                            route_publish(&core, &counters, &p);
                            emit(&app, "broker:event", BrokerEvent {
                                kind: "publish".into(), client_id: client_id.clone(),
                                topic: Some(p.topic.clone()),
                                payload: Some(String::from_utf8_lossy(&p.payload).to_string()),
                                ts: now_ms(),
                            });
                            if p.qos == QoS::AtLeastOnce {
                                let _ = tx.send(Out::PubAck(PubAck::new(p.pkid))).await;
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

    // 清理 + 遗嘱：仅当当前表项仍属于本连接时移除（避免被顶替后误删新连接）。
    if connected {
        let removed = {
            let mut g = core.lock().unwrap();
            if g.clients.get(&client_id).map(|c| c.conn_id) == Some(conn_id) {
                g.clients.remove(&client_id);
                counters.connected.store(g.clients.len() as u64, Ordering::Relaxed);
                true
            } else {
                false
            }
        };
        if removed && !clean_disconnect && !kicked {
            if let Some(w) = will.take() {
                let mut p = Publish::new(w.topic, w.qos, w.message.to_vec());
                p.retain = w.retain;
                route_publish(&core, &counters, &p);
            }
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
    for ci in g.clients.values() {
        // 取该订阅者匹配过滤器中的最大授权 QoS，再与发布 QoS 取小。
        let mut best: Option<QoS> = None;
        for (f, q) in &ci.subs {
            if topic_matches(f, &p.topic) {
                best = Some(match best {
                    Some(b) if (b as u8) >= (*q as u8) => b,
                    _ => *q,
                });
            }
        }
        let Some(sub_qos) = best else { continue };
        let eff = qos_min(p.qos, sub_qos);
        let mut out = p.clone();
        out.qos = eff;
        out.dup = false;
        if eff == QoS::AtMostOnce {
            out.pkid = 0;
        }
        // 有界队列：满或已关闭则计入丢弃（QoS0 语义允许丢弃；QoS1/2 的可靠投递在 P1 阶段实现）。
        match ci.tx.try_send(Out::Publish(out)) {
            Ok(()) => sent += 1,
            Err(_) => dropped += 1,
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
        assert!(!valid_filter("a/#/b")); // # 必须在末尾
        assert!(!valid_filter("a/b#")); // + / # 必须独占层级
        assert!(!valid_filter("a/c+"));
        assert!(!valid_filter(""));
    }

    #[test]
    fn test_topic_matches_dollar_exclusion() {
        // 首层通配符不匹配 $ 开头主题
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

    // ---- 集成测试基座：启动一个内嵌 broker，返回监听端口 ----
    fn spawn_broker(cfg: BrokerConfig) -> u16 {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let port = std_listener.local_addr().unwrap().port();
        let (sd_tx, sd_rx) = watch::channel(false);
        // 保持 sender 存活，避免 changed() 立即返回 Err 造成 accept 循环空转。
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

    // P0: 投递 QoS 应降级为 min(发布 QoS, 订阅 QoS)。QoS0 订阅者收到 QoS1 发布应为 QoS0。
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

        let mut subscribed = false;
        for _ in 0..80 {
            match sel.poll().await.unwrap() {
                Event::Incoming(CPacket::SubAck(_)) => {
                    subscribed = true;
                    publisher.publish("k/x", QoS::AtLeastOnce, false, b"hi".to_vec()).await.unwrap();
                }
                Event::Incoming(CPacket::Publish(p)) => {
                    assert_eq!(p.qos, QoS::AtMostOnce, "QoS 应降级到订阅者的 QoS0");
                    assert_eq!(&p.payload[..], b"hi");
                    return;
                }
                _ => {}
            }
        }
        panic!("未收到降级后的消息 (subscribed={subscribed})");
    }

    // P0: 同一 client_id 重连应顶替旧连接（旧连接被断开）。
    #[tokio::test]
    async fn test_client_takeover() {
        use rumqttc::{AsyncClient, Event, MqttOptions, Packet as CPacket, QoS};
        let port = spawn_broker(BrokerConfig::default());

        let mut ao = MqttOptions::new("dup", "127.0.0.1", port);
        ao.set_keep_alive(Duration::from_secs(5));
        let (a, mut ael) = AsyncClient::new(ao, 10);
        a.subscribe("t", QoS::AtLeastOnce).await.unwrap();
        for _ in 0..40 {
            if let Event::Incoming(CPacket::SubAck(_)) = ael.poll().await.unwrap() {
                break;
            }
        }

        // 第二个同名连接顶替。
        let mut bo = MqttOptions::new("dup", "127.0.0.1", port);
        bo.set_keep_alive(Duration::from_secs(5));
        let (b, mut bel) = AsyncClient::new(bo, 10);
        tokio::spawn(async move { loop { if bel.poll().await.is_err() { break; } } });
        let _ = b.subscribe("t", QoS::AtLeastOnce).await;

        // 旧连接 A 应在若干次 poll 内收到断开错误。
        for _ in 0..60 {
            if ael.poll().await.is_err() {
                return; // 旧连接被踢
            }
        }
        panic!("旧连接未被顶替踢出");
    }

    // 端到端：自建 broker + rumqttc 客户端 订阅/发布/收 往返。
    #[tokio::test]
    async fn broker_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (_sd_tx, sd_rx) = watch::channel(false);
        let core = Arc::new(Mutex::new(Core::default()));
        let counters = Arc::new(Counters::default());
        let cfg = BrokerConfig { port, ..Default::default() };
        tokio::spawn(async move {
            loop {
                let (stream, peer) = listener.accept().await.unwrap();
                let core = core.clone();
                let counters = counters.clone();
                let cfg = cfg.clone();
                let sd = sd_rx.clone();
                tokio::spawn(async move {
                    handle_conn(stream, peer.to_string(), None, core, counters, cfg, sd).await;
                });
            }
        });

        use rumqttc::{AsyncClient, Event, MqttOptions, Packet as CPacket, QoS};
        let mut mo = MqttOptions::new("t", "127.0.0.1", port);
        mo.set_keep_alive(Duration::from_secs(5));
        let (client, mut eventloop) = AsyncClient::new(mo, 10);
        client.subscribe("k/#", QoS::AtLeastOnce).await.unwrap();
        // 等 SubAck 后再发
        let mut published = false;
        for _ in 0..50 {
            match eventloop.poll().await.unwrap() {
                Event::Incoming(CPacket::SubAck(_)) => {
                    client.publish("k/x", QoS::AtLeastOnce, false, b"hi".to_vec()).await.unwrap();
                    published = true;
                }
                Event::Incoming(CPacket::Publish(p)) => {
                    assert_eq!(&p.payload[..], b"hi");
                    assert_eq!(p.topic, "k/x");
                    return; // 往返成功
                }
                _ => {}
            }
        }
        panic!("未收到往返消息 (published={published})");
    }
}
