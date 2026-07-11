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
use tauri::{AppHandle, Emitter};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

const MAX_PKT: usize = 256 * 1024;

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
    tx: mpsc::UnboundedSender<Out>,
    addr: String,
    username: String,
    subs: Vec<(String, QoS)>,
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

fn emit<S: Serialize + Clone>(app: &Option<AppHandle>, event: &str, payload: S) {
    if let Some(a) = app {
        let _ = a.emit(event, payload);
    }
}

pub fn is_running(state: &BrokerState) -> bool {
    state.running.lock().unwrap().is_some()
}

pub fn current_config(state: &BrokerState) -> Option<BrokerConfig> {
    state.running.lock().unwrap().as_ref().map(|r| r.config.clone())
}

/// 启动 broker。绑定失败或已在运行返回错误。
pub async fn start(app: AppHandle, state: &BrokerState, config: BrokerConfig) -> Result<(), String> {
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
        let app = app.clone();
        let core = core.clone();
        let counters = counters.clone();
        let mut sd = sd_rx.clone();
        tauri::async_runtime::spawn(async move {
            loop {
                tokio::select! {
                    _ = sd.changed() => { if *sd.borrow() { break; } }
                    accepted = listener.accept() => {
                        let (stream, peer) = match accepted { Ok(v) => v, Err(_) => continue };
                        let app = Some(app.clone());
                        let core = core.clone();
                        let counters = counters.clone();
                        let cfg = cfg.clone();
                        let sd_rx = sd.clone();
                        tauri::async_runtime::spawn(async move {
                            handle_conn(stream, peer.to_string(), app, core, counters, cfg, sd_rx).await;
                        });
                    }
                }
            }
        });
    }

    // 统计/客户端列表 定时推送
    {
        let app = app.clone();
        let core = core.clone();
        let counters = counters.clone();
        let mut sd = sd_rx.clone();
        tauri::async_runtime::spawn(async move {
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
                        let _ = app.emit("broker:stats", BrokerStats {
                            running: true,
                            clients_connected: counters.connected.load(Ordering::Relaxed),
                            msgs_received: counters.recv.load(Ordering::Relaxed),
                            msgs_sent: counters.sent.load(Ordering::Relaxed),
                            retained,
                        });
                        let _ = app.emit("broker:clients", rows);
                    }
                }
            }
        });
    }

    *state.running.lock().unwrap() = Some(Running { shutdown: sd_tx, config, core });
    emit(&Some(app), "broker:status", serde_json::json!({"running": true}));
    Ok(())
}

pub fn stop(app: &AppHandle, state: &BrokerState) {
    if let Some(r) = state.running.lock().unwrap().take() {
        let _ = r.shutdown.send(true);
    }
    let _ = app.emit("broker:status", serde_json::json!({"running": false}));
}

async fn handle_conn(
    stream: tokio::net::TcpStream,
    peer: String,
    app: Option<AppHandle>,
    core: Arc<Mutex<Core>>,
    counters: Arc<Counters>,
    config: BrokerConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let _ = stream.set_nodelay(true);
    let (mut rd, mut wr) = stream.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Out>();

    let writer = tauri::async_runtime::spawn(async move {
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

    let mut buf = BytesMut::with_capacity(4096);
    let mut client_id = String::new();
    let mut connected = false;
    let mut will: Option<LastWill> = None;
    let mut clean_disconnect = false;

    'conn: loop {
        tokio::select! {
            _ = shutdown.changed() => { if *shutdown.borrow() { break; } }
            r = rd.read_buf(&mut buf) => {
                match r { Ok(0) => break, Ok(_) => {}, Err(_) => break }
                loop {
                    let packet = match v4::read(&mut buf, MAX_PKT) {
                        Ok(p) => p,
                        Err(MqttError::InsufficientBytes(_)) => break,
                        Err(_) => break 'conn,
                    };
                    match packet {
                        Packet::Connect(c) => {
                            let auth_ok = config.allow_anonymous
                                || c.login.as_ref().map_or(false, |l| {
                                    l.username == config.username && l.password == config.password
                                });
                            let full = {
                                let g = core.lock().unwrap();
                                config.max_clients > 0 && g.clients.len() >= config.max_clients
                            };
                            if !auth_ok || full {
                                let code = if full { ConnectReturnCode::ServiceUnavailable }
                                           else { ConnectReturnCode::BadUserNamePassword };
                                let _ = tx.send(Out::ConnAck(ConnAck::new(code, false)));
                                break 'conn;
                            }
                            client_id = if c.client_id.is_empty() {
                                format!("auto-{}", now_ms())
                            } else { c.client_id.clone() };
                            will = c.last_will.clone();
                            let username = c.login.as_ref().map(|l| l.username.clone()).unwrap_or_default();
                            {
                                let mut g = core.lock().unwrap();
                                g.clients.insert(client_id.clone(), ClientInfo {
                                    tx: tx.clone(), addr: peer.clone(), username, subs: vec![],
                                });
                                counters.connected.store(g.clients.len() as u64, Ordering::Relaxed);
                            }
                            connected = true;
                            let _ = tx.send(Out::ConnAck(ConnAck::new(ConnectReturnCode::Success, false)));
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
                                        ci.subs.push((f.path.clone(), f.qos));
                                        codes.push(SubscribeReasonCode::Success(f.qos));
                                    }
                                }
                            }
                            let _ = tx.send(Out::SubAck(SubAck::new(s.pkid, codes)));
                            // 下发匹配的保留消息
                            let retained: Vec<Publish> = {
                                let g = core.lock().unwrap();
                                g.retained.values()
                                    .filter(|p| s.filters.iter().any(|f| topic_matches(&f.path, &p.topic)))
                                    .cloned().collect()
                            };
                            for p in retained { let _ = tx.send(Out::Publish(p)); }
                            for f in &s.filters {
                                emit(&app, "broker:event", BrokerEvent {
                                    kind: "subscribe".into(), client_id: client_id.clone(),
                                    topic: Some(f.path.clone()), payload: None, ts: now_ms(),
                                });
                            }
                        }
                        Packet::Unsubscribe(u) if connected => {
                            {
                                let mut g = core.lock().unwrap();
                                if let Some(ci) = g.clients.get_mut(&client_id) {
                                    ci.subs.retain(|(f, _)| !u.topics.contains(f));
                                }
                            }
                            let _ = tx.send(Out::UnsubAck(UnsubAck::new(u.pkid)));
                            emit(&app, "broker:event", BrokerEvent {
                                kind: "unsubscribe".into(), client_id: client_id.clone(),
                                topic: u.topics.first().cloned(), payload: None, ts: now_ms(),
                            });
                        }
                        Packet::Publish(p) if connected => {
                            counters.recv.fetch_add(1, Ordering::Relaxed);
                            route_publish(&core, &counters, &p);
                            emit(&app, "broker:event", BrokerEvent {
                                kind: "publish".into(), client_id: client_id.clone(),
                                topic: Some(p.topic.clone()),
                                payload: Some(String::from_utf8_lossy(&p.payload).to_string()),
                                ts: now_ms(),
                            });
                            if p.qos == QoS::AtLeastOnce {
                                let _ = tx.send(Out::PubAck(PubAck::new(p.pkid)));
                            }
                        }
                        Packet::PingReq => { let _ = tx.send(Out::PingResp); }
                        Packet::Disconnect => { clean_disconnect = true; break 'conn; }
                        _ => {}
                    }
                }
            }
        }
    }

    // 清理 + 遗嘱
    if connected {
        let mut g = core.lock().unwrap();
        g.clients.remove(&client_id);
        counters.connected.store(g.clients.len() as u64, Ordering::Relaxed);
        drop(g);
        if !clean_disconnect {
            if let Some(w) = will.take() {
                let mut p = Publish::new(w.topic, w.qos, w.message.to_vec());
                p.retain = w.retain;
                route_publish(&core, &counters, &p);
            }
        }
        emit(&app, "broker:event", BrokerEvent {
            kind: "disconnect".into(), client_id: client_id.clone(),
            topic: None, payload: None, ts: now_ms(),
        });
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
    for ci in g.clients.values() {
        if ci.subs.iter().any(|(f, _)| topic_matches(f, &p.topic)) {
            if ci.tx.send(Out::Publish(p.clone())).is_ok() {
                sent += 1;
            }
        }
    }
    counters.sent.fetch_add(sent, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

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
