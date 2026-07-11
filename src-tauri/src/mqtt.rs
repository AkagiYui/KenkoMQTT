//! 多连接 MQTT 管理器（MQTT 3.1.1/5.0，TCP/TLS/WS/WSS）。
//! 消息与计算均在后端：原始报文入库，按需做 过滤/格式解码/主题树/图表聚合/占位符/定时发布/导出。
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde_json_path::JsonPath;
use tauri::async_runtime::JoinHandle;
use tauri::{AppHandle, Emitter, State};

use crate::codec::{self, Format};
use crate::model::Profile;

const MAX_MSGS: usize = 5000;

#[derive(Clone)]
enum ClientKind {
    V4(rumqttc::AsyncClient),
    V5(rumqttc::v5::AsyncClient),
}

struct ConnHandle {
    client: ClientKind,
    task: JoinHandle<()>,
}

struct StoredMsg {
    dir: &'static str, // rx | tx
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
    ts: u64,
}

type MsgLog = Arc<Mutex<HashMap<String, VecDeque<StoredMsg>>>>;

#[derive(Default)]
pub struct Manager {
    conns: Mutex<HashMap<String, ConnHandle>>,
    store: MsgLog,
    schedules: Mutex<HashMap<String, JoinHandle<()>>>,
}

// ---- 前端展示用的序列化类型 ----
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MsgRow {
    pub dir: String,
    pub topic: String,
    pub payload: String,
    pub size: usize,
    pub qos: u8,
    pub retain: bool,
    pub ts: u64,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TreeNode {
    pub name: String,
    pub full: String,
    pub count: u64,
    pub children: Vec<TreeNode>,
}
#[derive(Serialize)]
pub struct RatePoint {
    pub t: u64,
    pub v: u64,
}
#[derive(Serialize)]
pub struct ContentPoint {
    pub t: u64,
    pub v: f64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

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

fn v4_qos(n: u8) -> rumqttc::QoS {
    match n {
        1 => rumqttc::QoS::AtLeastOnce,
        2 => rumqttc::QoS::ExactlyOnce,
        _ => rumqttc::QoS::AtMostOnce,
    }
}
fn v5_qos(n: u8) -> rumqttc::v5::mqttbytes::QoS {
    match n {
        1 => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
        2 => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
        _ => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
    }
}

fn emit_status(app: &AppHandle, conn_id: &str, status: &str, detail: Option<String>) {
    let _ = app.emit(
        "mqtt:status",
        serde_json::json!({ "connId": conn_id, "status": status, "detail": detail }),
    );
}

/// 记录一条消息到后端库并向前端发出「有新消息」信号（前端据此按需重新查询）。
fn record(store: &MsgLog, app: &AppHandle, conn_id: &str, dir: &'static str, topic: String, payload: Vec<u8>, qos: u8, retain: bool) {
    {
        let mut g = store.lock().unwrap();
        let dq = g.entry(conn_id.to_string()).or_default();
        dq.push_back(StoredMsg { dir, topic, payload, qos, retain, ts: now_ms() });
        while dq.len() > MAX_MSGS {
            dq.pop_front();
        }
    }
    let _ = app.emit("mqtt:msg", serde_json::json!({ "connId": conn_id }));
}

impl ClientKind {
    async fn subscribe(&self, topic: String, qos: u8) -> Result<(), String> {
        match self {
            ClientKind::V4(c) => c.subscribe(topic, v4_qos(qos)).await.map_err(|e| e.to_string()),
            ClientKind::V5(c) => c.subscribe(topic, v5_qos(qos)).await.map_err(|e| e.to_string()),
        }
    }
    async fn unsubscribe(&self, topic: String) -> Result<(), String> {
        match self {
            ClientKind::V4(c) => c.unsubscribe(topic).await.map_err(|e| e.to_string()),
            ClientKind::V5(c) => c.unsubscribe(topic).await.map_err(|e| e.to_string()),
        }
    }
    async fn publish(&self, topic: String, payload: Vec<u8>, qos: u8, retain: bool) -> Result<(), String> {
        match self {
            ClientKind::V4(c) => c.publish(topic, v4_qos(qos), retain, payload).await.map_err(|e| e.to_string()),
            ClientKind::V5(c) => c.publish(topic, v5_qos(qos), retain, payload).await.map_err(|e| e.to_string()),
        }
    }
    async fn disconnect(&self) {
        match self {
            ClientKind::V4(c) => {
                let _ = c.disconnect().await;
            }
            ClientKind::V5(c) => {
                let _ = c.disconnect().await;
            }
        }
    }
}

impl Manager {
    fn client_of(&self, conn_id: &str) -> Option<ClientKind> {
        self.conns.lock().unwrap().get(conn_id).map(|h| h.client.clone())
    }
    fn insert(&self, conn_id: String, handle: ConnHandle) {
        if let Some(old) = self.conns.lock().unwrap().insert(conn_id, handle) {
            old.task.abort();
        }
    }
    fn remove(&self, conn_id: &str) -> Option<ConnHandle> {
        self.conns.lock().unwrap().remove(conn_id)
    }

    /// 过滤 + 按格式解码后返回最近 limit 条（按时间升序）。
    pub fn query(&self, conn_id: &str, format: Format, filter: Option<String>, limit: usize) -> Vec<MsgRow> {
        let g = self.store.lock().unwrap();
        let Some(dq) = g.get(conn_id) else { return vec![] };
        let f = filter.unwrap_or_default().to_lowercase();
        let mut rows: Vec<MsgRow> = dq
            .iter()
            .rev()
            .filter(|m| {
                if f.is_empty() {
                    return true;
                }
                m.topic.to_lowercase().contains(&f) || codec::decode(&m.payload, format).to_lowercase().contains(&f)
            })
            .take(limit)
            .map(|m| MsgRow {
                dir: m.dir.to_string(),
                topic: m.topic.clone(),
                payload: codec::decode(&m.payload, format),
                size: m.payload.len(),
                qos: m.qos,
                retain: m.retain,
                ts: m.ts,
            })
            .collect();
        rows.reverse();
        rows
    }

    pub fn clear_msgs(&self, conn_id: &str) {
        self.store.lock().unwrap().remove(conn_id);
    }

    pub fn topic_tree(&self, conn_id: &str) -> Vec<TreeNode> {
        #[derive(Default)]
        struct Raw {
            count: u64,
            children: BTreeMap<String, Raw>,
        }
        let g = self.store.lock().unwrap();
        let Some(dq) = g.get(conn_id) else { return vec![] };
        let mut root = Raw::default();
        for m in dq.iter().filter(|m| m.dir == "rx") {
            let mut node = &mut root;
            for seg in m.topic.split('/') {
                node = node.children.entry(seg.to_string()).or_default();
            }
            node.count += 1;
        }
        fn conv(prefix: &str, name: &str, raw: &Raw) -> TreeNode {
            let full = if prefix.is_empty() { name.to_string() } else { format!("{prefix}/{name}") };
            let children = raw.children.iter().map(|(k, v)| conv(&full, k, v)).collect();
            TreeNode { name: name.to_string(), full, count: raw.count, children }
        }
        root.children.iter().map(|(k, v)| conv("", k, v)).collect()
    }

    /// 消息速率：最近 buckets 个 bucket_ms 时间桶内的消息计数。
    pub fn chart_rate(&self, conn_id: &str, bucket_ms: u64, buckets: usize) -> Vec<RatePoint> {
        let g = self.store.lock().unwrap();
        let now = now_ms();
        let bucket_ms = bucket_ms.max(1);
        let start = now.saturating_sub(bucket_ms * buckets as u64);
        let mut counts = vec![0u64; buckets];
        if let Some(dq) = g.get(conn_id) {
            for m in dq.iter() {
                if m.ts >= start && m.ts <= now {
                    let idx = ((m.ts - start) / bucket_ms) as usize;
                    if idx < buckets {
                        counts[idx] += 1;
                    }
                }
            }
        }
        counts
            .into_iter()
            .enumerate()
            .map(|(i, c)| RatePoint { t: start + i as u64 * bucket_ms, v: c })
            .collect()
    }

    /// 内容图表：对匹配 topic 的 JSON 载荷用 JSONPath 提取数值，按时间返回点集（最近 limit 个）。
    pub fn chart_content(&self, conn_id: &str, topic_filter: String, jsonpath: String, limit: usize) -> Result<Vec<ContentPoint>, String> {
        let path = JsonPath::parse(&jsonpath).map_err(|e| e.to_string())?;
        let g = self.store.lock().unwrap();
        let Some(dq) = g.get(conn_id) else { return Ok(vec![]) };
        let mut pts = Vec::new();
        for m in dq.iter() {
            if !topic_filter.is_empty() && !topic_matches(&topic_filter, &m.topic) {
                continue;
            }
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&m.payload) {
                if let Some(node) = path.query(&v).first() {
                    let num = node.as_f64().or_else(|| node.as_str().and_then(|s| s.parse::<f64>().ok()));
                    if let Some(n) = num {
                        pts.push(ContentPoint { t: m.ts, v: n });
                    }
                }
            }
        }
        let len = pts.len();
        if len > limit {
            pts.drain(0..len - limit);
        }
        Ok(pts)
    }

    /// 导出消息为 CSV 或 JSON 文本。
    pub fn export_messages(&self, conn_id: &str, as_csv: bool, fmt: Format) -> String {
        let g = self.store.lock().unwrap();
        let Some(dq) = g.get(conn_id) else { return String::new() };
        if as_csv {
            let mut s = String::from("ts,dir,topic,payload,qos,retain\n");
            for m in dq.iter() {
                let p = codec::decode(&m.payload, fmt).replace('"', "\"\"");
                s.push_str(&format!("{},{},{},\"{}\",{},{}\n", m.ts, m.dir, m.topic, p, m.qos, m.retain));
            }
            s
        } else {
            let rows: Vec<_> = dq
                .iter()
                .map(|m| {
                    serde_json::json!({
                        "ts": m.ts, "dir": m.dir, "topic": m.topic,
                        "payload": codec::decode(&m.payload, fmt), "qos": m.qos, "retain": m.retain
                    })
                })
                .collect();
            serde_json::to_string_pretty(&rows).unwrap_or_default()
        }
    }
}

fn norm_path(path: &str) -> String {
    if path.trim().is_empty() {
        "/mqtt".into()
    } else if path.starts_with('/') {
        path.into()
    } else {
        format!("/{path}")
    }
}

fn transport_for(p: &Profile) -> Result<(String, u16, rumqttc::Transport), String> {
    use rumqttc::{TlsConfiguration, Transport};
    match p.protocol.as_str() {
        "tcp" => Ok((p.host.clone(), p.port, Transport::Tcp)),
        "tls" => {
            let cfg = crate::tls::client_config(p.tls_skip_verify, &p.ca_cert);
            Ok((p.host.clone(), p.port, Transport::Tls(TlsConfiguration::Rustls(cfg))))
        }
        "ws" => Ok((format!("ws://{}:{}{}", p.host, p.port, norm_path(&p.path)), p.port, Transport::Ws)),
        "wss" => {
            let cfg = crate::tls::client_config(p.tls_skip_verify, &p.ca_cert);
            Ok((
                format!("wss://{}:{}{}", p.host, p.port, norm_path(&p.path)),
                p.port,
                Transport::Wss(TlsConfiguration::Rustls(cfg)),
            ))
        }
        other => Err(format!("未知协议: {other}")),
    }
}

pub fn connect(app: AppHandle, mgr: &Manager, profile: Profile) -> Result<(), String> {
    if let Some(old) = mgr.remove(&profile.id) {
        old.task.abort();
        let c = old.client;
        tauri::async_runtime::spawn(async move { c.disconnect().await });
    }
    let conn_id = profile.id.clone();
    let client_id = if profile.client_id.trim().is_empty() {
        format!("kenko-{}", now_ms())
    } else {
        profile.client_id.clone()
    };
    let (addr, port, transport) = transport_for(&profile)?;
    if profile.mqtt_version == 5 {
        connect_v5(app, mgr, &conn_id, &client_id, &profile, addr, port, transport)
    } else {
        connect_v4(app, mgr, &conn_id, &client_id, &profile, addr, port, transport)
    }
}

#[allow(clippy::too_many_arguments)]
fn connect_v4(app: AppHandle, mgr: &Manager, conn_id: &str, client_id: &str, p: &Profile, addr: String, port: u16, transport: rumqttc::Transport) -> Result<(), String> {
    use rumqttc::{AsyncClient, Event, MqttOptions, Packet};
    let mut opts = MqttOptions::new(client_id, addr, port);
    opts.set_transport(transport);
    opts.set_keep_alive(Duration::from_secs(p.keep_alive.max(5)));
    opts.set_clean_session(p.clean_session);
    if !p.username.is_empty() {
        opts.set_credentials(p.username.clone(), p.password.clone());
    }
    if p.will.enabled && !p.will.topic.is_empty() {
        opts.set_last_will(rumqttc::LastWill::new(&p.will.topic, p.will.payload.clone().into_bytes(), v4_qos(p.will.qos), p.will.retain));
    }
    let (client, mut eventloop) = AsyncClient::new(opts, 64);
    let app2 = app.clone();
    let id = conn_id.to_string();
    let store = mgr.store.clone();
    let task = tauri::async_runtime::spawn(async move {
        emit_status(&app2, &id, "connecting", None);
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => emit_status(&app2, &id, "connected", None),
                Ok(Event::Incoming(Packet::Publish(pkt))) => {
                    record(&store, &app2, &id, "rx", pkt.topic.clone(), pkt.payload.to_vec(), pkt.qos as u8, pkt.retain)
                }
                Ok(_) => {}
                Err(e) => {
                    emit_status(&app2, &id, "error", Some(e.to_string()));
                    break;
                }
            }
        }
    });
    mgr.insert(conn_id.to_string(), ConnHandle { client: ClientKind::V4(client), task });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn connect_v5(app: AppHandle, mgr: &Manager, conn_id: &str, client_id: &str, p: &Profile, addr: String, port: u16, transport: rumqttc::Transport) -> Result<(), String> {
    use rumqttc::v5::mqttbytes::v5::Packet;
    use rumqttc::v5::{AsyncClient, Event, MqttOptions};
    let mut opts = MqttOptions::new(client_id, addr, port);
    opts.set_transport(transport);
    opts.set_keep_alive(Duration::from_secs(p.keep_alive.max(5)));
    opts.set_clean_start(p.clean_session);
    if !p.username.is_empty() {
        opts.set_credentials(p.username.clone(), p.password.clone());
    }
    if p.will.enabled && !p.will.topic.is_empty() {
        opts.set_last_will(rumqttc::v5::mqttbytes::v5::LastWill::new(&p.will.topic, p.will.payload.clone().into_bytes(), v5_qos(p.will.qos), p.will.retain, None));
    }
    let (client, mut eventloop) = AsyncClient::new(opts, 64);
    let app2 = app.clone();
    let id = conn_id.to_string();
    let store = mgr.store.clone();
    let task = tauri::async_runtime::spawn(async move {
        emit_status(&app2, &id, "connecting", None);
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => emit_status(&app2, &id, "connected", None),
                Ok(Event::Incoming(Packet::Publish(pkt))) => record(
                    &store,
                    &app2,
                    &id,
                    "rx",
                    String::from_utf8_lossy(&pkt.topic).to_string(),
                    pkt.payload.to_vec(),
                    pkt.qos as u8,
                    pkt.retain,
                ),
                Ok(_) => {}
                Err(e) => {
                    emit_status(&app2, &id, "error", Some(e.to_string()));
                    break;
                }
            }
        }
    });
    mgr.insert(conn_id.to_string(), ConnHandle { client: ClientKind::V5(client), task });
    Ok(())
}

pub async fn disconnect(mgr: State<'_, Manager>, conn_id: String) -> Result<(), String> {
    if let Some(h) = mgr.remove(&conn_id) {
        h.task.abort();
        h.client.disconnect().await;
    }
    Ok(())
}

pub async fn subscribe(mgr: State<'_, Manager>, conn_id: String, topic: String, qos: u8) -> Result<(), String> {
    let c = mgr.client_of(&conn_id).ok_or("未连接")?;
    c.subscribe(topic, qos).await
}

pub async fn unsubscribe(mgr: State<'_, Manager>, conn_id: String, topic: String) -> Result<(), String> {
    let c = mgr.client_of(&conn_id).ok_or("未连接")?;
    c.unsubscribe(topic).await
}

/// 发布（含格式编码与占位符展开），并把发出的报文记入库。
pub async fn publish(
    app: AppHandle,
    mgr: State<'_, Manager>,
    conn_id: String,
    topic: String,
    payload: String,
    qos: u8,
    retain: bool,
    format: Option<Format>,
    expand: Option<bool>,
) -> Result<(), String> {
    let fmt = format.unwrap_or(Format::Plaintext);
    let text = if expand.unwrap_or(false) {
        expand_placeholders(&payload, 0)
    } else {
        payload
    };
    let bytes = codec::encode(&text, fmt)?;
    let c = mgr.client_of(&conn_id).ok_or("未连接")?;
    c.publish(topic.clone(), bytes.clone(), qos, retain).await?;
    record(&mgr.store, &app, &conn_id, "tx", topic, bytes, qos, retain);
    Ok(())
}

pub fn schedule_start(
    app: AppHandle,
    mgr: State<'_, Manager>,
    conn_id: String,
    topic: String,
    payload: String,
    qos: u8,
    retain: bool,
    format: Option<Format>,
    interval_ms: u64,
) -> Result<String, String> {
    let client = mgr.client_of(&conn_id).ok_or("未连接")?;
    let fmt = format.unwrap_or(Format::Plaintext);
    let id = format!("sch-{}-{}", conn_id, now_ms());
    let store = mgr.store.clone();
    let app2 = app.clone();
    let cid = conn_id.clone();
    let handle = tauri::async_runtime::spawn(async move {
        let mut counter = 0u64;
        loop {
            tokio::time::sleep(Duration::from_millis(interval_ms.max(100))).await;
            counter += 1;
            let text = expand_placeholders(&payload, counter);
            let bytes = match codec::encode(&text, fmt) {
                Ok(b) => b,
                Err(_) => continue,
            };
            if client.publish(topic.clone(), bytes.clone(), qos, retain).await.is_err() {
                break;
            }
            record(&store, &app2, &cid, "tx", topic.clone(), bytes, qos, retain);
        }
    });
    mgr.schedules.lock().unwrap().insert(id.clone(), handle);
    Ok(id)
}

pub fn schedule_stop(mgr: State<'_, Manager>, id: String) {
    if let Some(h) = mgr.schedules.lock().unwrap().remove(&id) {
        h.abort();
    }
}

fn uuid_v4() -> String {
    let mut b = [0u8; 16];
    for x in b.iter_mut() {
        *x = fastrand::u8(..);
    }
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
    )
}

fn parse_pair(args: &str, da: f64, db: f64) -> (f64, f64) {
    let mut it = args.split(',').map(|s| s.trim().parse::<f64>());
    let a = it.next().and_then(|r| r.ok()).unwrap_or(da);
    let b = it.next().and_then(|r| r.ok()).unwrap_or(db);
    if a > b {
        (b, a)
    } else {
        (a, b)
    }
}

fn expand_token(token: &str, counter: u64) -> String {
    let (name, args) = match token.split_once('(') {
        Some((n, rest)) => (n.trim(), rest.trim_end_matches(')')),
        None => (token.trim(), ""),
    };
    match name {
        "timestamp" => now_ms().to_string(),
        "uuid" => uuid_v4(),
        "counter" => counter.to_string(),
        "int" => {
            let (a, b) = parse_pair(args, 0.0, 100.0);
            fastrand::i64(a as i64..=b as i64).to_string()
        }
        "float" => {
            let (a, b) = parse_pair(args, 0.0, 1.0);
            format!("{:.4}", a + fastrand::f64() * (b - a))
        }
        "string" => {
            let n = args.trim().parse::<usize>().unwrap_or(4);
            (0..n).map(|_| fastrand::alphanumeric()).collect()
        }
        _ => format!("${{{token}}}"),
    }
}

/// 展开 ${timestamp}/${uuid}/${counter}/${int(a,b)}/${float(a,b)}/${string(n)}。
pub fn expand_placeholders(input: &str, counter: u64) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find("${") {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + 2..];
        if let Some(end) = after.find('}') {
            out.push_str(&expand_token(&after[..end], counter));
            rest = &after[end + 1..];
        } else {
            out.push_str(&rest[pos..]);
            rest = "";
            break;
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders() {
        assert_eq!(expand_placeholders("c=${counter}", 7), "c=7");
        let s = expand_placeholders("id=${uuid}", 0);
        assert!(s.starts_with("id=") && s.len() == 3 + 36);
        let n: i64 = expand_placeholders("${int(5,5)}", 0).parse().unwrap();
        assert_eq!(n, 5);
        assert_eq!(expand_placeholders("${string(0)}x", 0), "x");
        assert_eq!(expand_placeholders("no placeholder", 0), "no placeholder");
    }
}
