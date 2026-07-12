//! 多连接 MQTT 管理器（MQTT 3.1.1/5.0，TCP/TLS/WS/WSS）。
//! 消息与计算均在后端：原始报文入库，按需做 过滤/格式解码/主题树/图表聚合/占位符/定时发布/导出。
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
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

#[derive(Serialize, Deserialize, Clone)]
struct StoredMsg {
    dir: String, // rx | tx
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
    ts: u64,
    /// MQTT 5.0 属性（内容类型 / 用户属性 / 响应主题等），v4 或无属性时为 None。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    props: Option<serde_json::Value>,
}

/// 单连接落盘结构（含 connId，便于安全化文件名后仍能还原原始 id）。
#[derive(Serialize, Deserialize)]
struct PersistedConn {
    conn_id: String,
    msgs: Vec<StoredMsg>,
}

type MsgLog = Arc<Mutex<HashMap<String, VecDeque<StoredMsg>>>>;

#[derive(Default)]
pub struct Manager {
    conns: Mutex<HashMap<String, ConnHandle>>,
    store: MsgLog,
    schedules: Mutex<HashMap<String, JoinHandle<()>>>,
    /// 消息落盘目录（应用数据目录下 messages/）。启动时由 setup 注入并载入历史。
    msg_dir: Mutex<Option<PathBuf>>,
    /// 自上次 flush 以来有新消息的连接（Arc 以便注入到各连接的事件循环任务）。
    dirty: Arc<Mutex<HashSet<String>>>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub props: Option<serde_json::Value>,
}

/// 分页查询选项（后端负责过滤/解码/分页，前端仅展示）。
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryOpts {
    #[serde(default)]
    pub format: Format,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub regex: bool,
    #[serde(default)]
    pub case_sensitive: bool,
    #[serde(default)]
    pub whole_word: bool,
    #[serde(default)]
    pub ignore_qos0: bool,
    #[serde(default)]
    pub dir: Option<String>, // rx | tx
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_limit")]
    pub limit: usize,
}
fn default_limit() -> usize {
    500
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MsgPage {
    pub rows: Vec<MsgRow>,
    /// 过滤后的匹配总数（用于分页）。
    pub total: usize,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TreeNode {
    pub name: String,
    pub full: String,
    pub count: u64,
    /// 该主题最近一条消息的载荷预览（已按格式解码，截断）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest: Option<String>,
    pub ts: u64,
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
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficPoint {
    pub t: u64,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_count: u64,
    pub tx_count: u64,
}

pub fn now_ms() -> u64 {
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

type Matcher = Box<dyn Fn(&str, &str) -> bool>;

/// 依据查询选项构造「主题或载荷是否匹配」的判定闭包。filter 为空返回 None（全匹配）。
fn build_matcher(opts: &QueryOpts) -> Option<Matcher> {
    let raw = opts.filter.as_deref().unwrap_or("").trim();
    if raw.is_empty() {
        return None;
    }
    // 需要正则引擎的情况：显式正则、或全词匹配。
    if opts.regex || opts.whole_word {
        let mut pat = if opts.regex { raw.to_string() } else { regex::escape(raw) };
        if opts.whole_word {
            pat = format!(r"\b{pat}\b");
        }
        let re = regex::RegexBuilder::new(&pat)
            .case_insensitive(!opts.case_sensitive)
            .build();
        if let Ok(re) = re {
            return Some(Box::new(move |topic: &str, payload: &str| re.is_match(topic) || re.is_match(payload)));
        }
        // 正则非法 → 回退子串。
    }
    if opts.case_sensitive {
        let needle = raw.to_string();
        Some(Box::new(move |topic: &str, payload: &str| topic.contains(&needle) || payload.contains(&needle)))
    } else {
        let needle = raw.to_lowercase();
        Some(Box::new(move |topic: &str, payload: &str| {
            topic.to_lowercase().contains(&needle) || payload.to_lowercase().contains(&needle)
        }))
    }
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
    record_props(store, app, conn_id, dir, topic, payload, qos, retain, None);
}

#[allow(clippy::too_many_arguments)]
fn record_props(
    store: &MsgLog,
    app: &AppHandle,
    conn_id: &str,
    dir: &'static str,
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
    props: Option<serde_json::Value>,
) {
    {
        let mut g = store.lock().unwrap();
        let dq = g.entry(conn_id.to_string()).or_default();
        dq.push_back(StoredMsg { dir: dir.to_string(), topic, payload, qos, retain, ts: now_ms(), props });
        while dq.len() > MAX_MSGS {
            dq.pop_front();
        }
    }
    let _ = app.emit("mqtt:msg", serde_json::json!({ "connId": conn_id }));
}

/// 记录消息并把连接标记为脏（供落盘），record 的入库口统一走这里。
fn record_mgr(mgr: &Manager, app: &AppHandle, conn_id: &str, dir: &'static str, topic: String, payload: Vec<u8>, qos: u8, retain: bool) {
    record(&mgr.store, app, conn_id, dir, topic, payload, qos, retain);
    mgr.dirty.lock().unwrap().insert(conn_id.to_string());
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

    /// 过滤（子串 / 正则 / 大小写 / 全词 / 方向 / 忽略QoS0）+ 解码 + 分页。返回按时间升序的一页与匹配总数。
    pub fn query(&self, conn_id: &str, opts: &QueryOpts) -> MsgPage {
        let g = self.store.lock().unwrap();
        let Some(dq) = g.get(conn_id) else {
            return MsgPage { rows: vec![], total: 0 };
        };
        let fmt = opts.format;
        let matcher = build_matcher(opts);
        // 时间升序遍历，收集匹配项索引。
        let matched: Vec<&StoredMsg> = dq
            .iter()
            .filter(|m| {
                if opts.ignore_qos0 && m.qos == 0 {
                    return false;
                }
                if let Some(d) = &opts.dir {
                    if !d.is_empty() && &m.dir != d {
                        return false;
                    }
                }
                match &matcher {
                    Some(f) => f(&m.topic, &codec::decode(&m.payload, fmt)),
                    None => true,
                }
            })
            .collect();
        let total = matched.len();
        let rows: Vec<MsgRow> = matched
            .into_iter()
            .skip(opts.offset)
            .take(opts.limit)
            .map(|m| MsgRow {
                dir: m.dir.clone(),
                topic: m.topic.clone(),
                payload: codec::decode(&m.payload, fmt),
                size: m.payload.len(),
                qos: m.qos,
                retain: m.retain,
                ts: m.ts,
                props: m.props.clone(),
            })
            .collect();
        MsgPage { rows, total }
    }

    pub fn clear_msgs(&self, conn_id: &str) {
        self.store.lock().unwrap().remove(conn_id);
        self.dirty.lock().unwrap().remove(conn_id);
        if let Some(p) = self.msg_file(conn_id) {
            let _ = std::fs::remove_file(p);
        }
    }

    /// 清空某连接下与 topic_filter（支持通配符）匹配的消息。
    pub fn clear_topic(&self, conn_id: &str, topic_filter: &str) {
        {
            let mut g = self.store.lock().unwrap();
            if let Some(dq) = g.get_mut(conn_id) {
                dq.retain(|m| !topic_matches(topic_filter, &m.topic));
            }
        }
        self.dirty.lock().unwrap().insert(conn_id.to_string());
    }

    // ---- 消息落盘：每连接一个 {connId}.json（存最近 MAX_MSGS 条） ----

    fn msg_file(&self, conn_id: &str) -> Option<PathBuf> {
        let g = self.msg_dir.lock().unwrap();
        let dir = g.as_ref()?;
        // 文件名安全化：仅保留字母数字与 -_，其余替换为 _。
        let safe: String = conn_id.chars().map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect();
        Some(dir.join(format!("{safe}.json")))
    }

    /// 注入落盘目录并载入历史消息（启动时调用一次）。
    pub fn init_persistence(&self, dir: PathBuf) {
        let _ = std::fs::create_dir_all(&dir);
        *self.msg_dir.lock().unwrap() = Some(dir.clone());
        // 载入历史：目录下每个 .json 文件名即为 connId 的安全化形式。此处以文件内嵌的 connId 为准。
        let Ok(rd) = std::fs::read_dir(&dir) else { return };
        for entry in rd.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Ok(data) = std::fs::read(&path) else { continue };
            let Ok(persisted) = serde_json::from_slice::<PersistedConn>(&data) else { continue };
            let mut g = self.store.lock().unwrap();
            g.insert(persisted.conn_id, persisted.msgs.into_iter().collect());
        }
    }

    /// 将脏连接的消息写盘。
    pub fn flush(&self) {
        let dirty: Vec<String> = { self.dirty.lock().unwrap().drain().collect() };
        if dirty.is_empty() {
            return;
        }
        for conn_id in dirty {
            let msgs: Vec<StoredMsg> = {
                let g = self.store.lock().unwrap();
                match g.get(&conn_id) {
                    Some(dq) => dq.iter().cloned().collect(),
                    None => continue,
                }
            };
            let Some(path) = self.msg_file(&conn_id) else { continue };
            let payload = PersistedConn { conn_id: conn_id.clone(), msgs };
            if let Ok(bytes) = serde_json::to_vec(&payload) {
                let _ = std::fs::write(&path, bytes);
            }
        }
    }

    pub fn topic_tree(&self, conn_id: &str, format: Format) -> Vec<TreeNode> {
        #[derive(Default)]
        struct Raw {
            count: u64,
            latest: Option<String>,
            ts: u64,
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
            if m.ts >= node.ts {
                node.ts = m.ts;
                let mut s = codec::decode(&m.payload, format);
                if s.chars().count() > 120 {
                    s = s.chars().take(120).collect::<String>() + "…";
                }
                node.latest = Some(s);
            }
        }
        fn conv(prefix: &str, name: &str, raw: &Raw) -> TreeNode {
            let full = if prefix.is_empty() { name.to_string() } else { format!("{prefix}/{name}") };
            let children = raw.children.iter().map(|(k, v)| conv(&full, k, v)).collect();
            TreeNode { name: name.to_string(), full, count: raw.count, latest: raw.latest.clone(), ts: raw.ts, children }
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

    /// 流量图表：最近 buckets 个时间桶内的收/发字节与条数。
    pub fn chart_traffic(&self, conn_id: &str, bucket_ms: u64, buckets: usize) -> Vec<TrafficPoint> {
        let g = self.store.lock().unwrap();
        let now = now_ms();
        let bucket_ms = bucket_ms.max(1);
        let start = now.saturating_sub(bucket_ms * buckets as u64);
        let mut pts: Vec<TrafficPoint> = (0..buckets)
            .map(|i| TrafficPoint { t: start + i as u64 * bucket_ms, rx_bytes: 0, tx_bytes: 0, rx_count: 0, tx_count: 0 })
            .collect();
        if let Some(dq) = g.get(conn_id) {
            for m in dq.iter() {
                if m.ts >= start && m.ts <= now {
                    let idx = ((m.ts - start) / bucket_ms) as usize;
                    if let Some(p) = pts.get_mut(idx.min(buckets - 1)) {
                        if m.dir == "rx" {
                            p.rx_bytes += m.payload.len() as u64;
                            p.rx_count += 1;
                        } else {
                            p.tx_bytes += m.payload.len() as u64;
                            p.tx_count += 1;
                        }
                    }
                }
            }
        }
        pts
    }

    /// 负载图表：对匹配 topic 的消息按时间桶聚合载荷大小（count/avg/sum/max/min）。
    pub fn chart_load(&self, conn_id: &str, topic_filter: &str, method: &str, bucket_ms: u64, buckets: usize) -> Vec<ContentPoint> {
        let g = self.store.lock().unwrap();
        let now = now_ms();
        let bucket_ms = bucket_ms.max(1);
        let start = now.saturating_sub(bucket_ms * buckets as u64);
        let mut sizes: Vec<Vec<f64>> = vec![Vec::new(); buckets];
        if let Some(dq) = g.get(conn_id) {
            for m in dq.iter() {
                if !topic_filter.is_empty() && !topic_matches(topic_filter, &m.topic) {
                    continue;
                }
                if m.ts >= start && m.ts <= now {
                    let idx = (((m.ts - start) / bucket_ms) as usize).min(buckets - 1);
                    sizes[idx].push(m.payload.len() as f64);
                }
            }
        }
        sizes
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                let val = if v.is_empty() {
                    0.0
                } else {
                    match method {
                        "avg" => v.iter().sum::<f64>() / v.len() as f64,
                        "sum" => v.iter().sum::<f64>(),
                        "max" => v.iter().cloned().fold(f64::MIN, f64::max),
                        "min" => v.iter().cloned().fold(f64::MAX, f64::min),
                        _ => v.len() as f64, // count
                    }
                };
                ContentPoint { t: start + i as u64 * bucket_ms, v: val }
            })
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

    /// 导出消息为 CSV / JSON / TXT 文本。
    pub fn export_messages(&self, conn_id: &str, kind: &str, fmt: Format) -> String {
        let g = self.store.lock().unwrap();
        let Some(dq) = g.get(conn_id) else { return String::new() };
        match kind {
            "csv" => {
                let mut s = String::from("ts,dir,topic,payload,qos,retain\n");
                for m in dq.iter() {
                    let p = codec::decode(&m.payload, fmt).replace('"', "\"\"");
                    s.push_str(&format!("{},{},{},\"{}\",{},{}\n", m.ts, m.dir, m.topic, p, m.qos, m.retain));
                }
                s
            }
            "txt" => {
                let mut s = String::new();
                for m in dq.iter() {
                    let arrow = if m.dir == "rx" { "<-" } else { "->" };
                    let payload = codec::decode(&m.payload, fmt);
                    s.push_str(&format!(
                        "[{}] {} {} (QoS{}{})\n{}\n\n",
                        m.ts,
                        arrow,
                        m.topic,
                        m.qos,
                        if m.retain { ",retain" } else { "" },
                        payload
                    ));
                }
                s
            }
            _ => {
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
    let dirty = mgr.dirty.clone();
    let task = tauri::async_runtime::spawn(async move {
        emit_status(&app2, &id, "connecting", None);
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => emit_status(&app2, &id, "connected", None),
                Ok(Event::Incoming(Packet::Publish(pkt))) => {
                    record(&store, &app2, &id, "rx", pkt.topic.clone(), pkt.payload.to_vec(), pkt.qos as u8, pkt.retain);
                    dirty.lock().unwrap().insert(id.clone());
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

/// 把 v5 PUBLISH 属性转为展示用 JSON（仅含存在的字段）。
fn v5_publish_props_json(props: &rumqttc::v5::mqttbytes::v5::PublishProperties) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    if let Some(v) = props.payload_format_indicator {
        m.insert("payloadFormatIndicator".into(), v.into());
    }
    if let Some(v) = props.message_expiry_interval {
        m.insert("messageExpiryInterval".into(), v.into());
    }
    if let Some(v) = props.topic_alias {
        m.insert("topicAlias".into(), v.into());
    }
    if let Some(v) = &props.response_topic {
        m.insert("responseTopic".into(), v.clone().into());
    }
    if let Some(v) = &props.correlation_data {
        m.insert("correlationData".into(), hex::encode(v).into());
    }
    if let Some(v) = &props.content_type {
        m.insert("contentType".into(), v.clone().into());
    }
    if !props.subscription_identifiers.is_empty() {
        m.insert(
            "subscriptionIdentifiers".into(),
            props.subscription_identifiers.iter().map(|&x| x as u64).collect::<Vec<_>>().into(),
        );
    }
    if !props.user_properties.is_empty() {
        let ups: Vec<serde_json::Value> = props
            .user_properties
            .iter()
            .map(|(k, v)| serde_json::json!({ "key": k, "value": v }))
            .collect();
        m.insert("userProperties".into(), ups.into());
    }
    serde_json::Value::Object(m)
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
    let dirty = mgr.dirty.clone();
    let task = tauri::async_runtime::spawn(async move {
        emit_status(&app2, &id, "connecting", None);
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => emit_status(&app2, &id, "connected", None),
                Ok(Event::Incoming(Packet::Publish(pkt))) => {
                    let props = pkt.properties.as_ref().map(v5_publish_props_json);
                    record_props(
                        &store,
                        &app2,
                        &id,
                        "rx",
                        String::from_utf8_lossy(&pkt.topic).to_string(),
                        pkt.payload.to_vec(),
                        pkt.qos as u8,
                        pkt.retain,
                        props,
                    );
                    dirty.lock().unwrap().insert(id.clone());
                }
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
    record_mgr(&mgr, &app, &conn_id, "tx", topic, bytes, qos, retain);
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
    let dirty = mgr.dirty.clone();
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
            dirty.lock().unwrap().insert(cid.clone());
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

    fn push(mgr: &Manager, dir: &str, topic: &str, payload: &[u8], qos: u8) {
        let mut g = mgr.store.lock().unwrap();
        let dq = g.entry("c".into()).or_default();
        dq.push_back(StoredMsg {
            dir: dir.into(),
            topic: topic.into(),
            payload: payload.to_vec(),
            qos,
            retain: false,
            ts: now_ms(),
            props: None,
        });
    }

    #[test]
    fn topic_wildcards() {
        assert!(topic_matches("a/+/c", "a/b/c"));
        assert!(topic_matches("a/#", "a/b/c/d"));
        assert!(!topic_matches("a/+/c", "a/b/d"));
        assert!(!topic_matches("a/b", "a/b/c"));
    }

    #[test]
    fn query_filters_and_pagination() {
        let mgr = Manager::default();
        push(&mgr, "rx", "sensor/temp", b"23.5", 0);
        push(&mgr, "tx", "sensor/hum", b"hello world", 1);
        push(&mgr, "rx", "sensor/temp", b"error!", 2);

        // 子串（大小写不敏感）匹配主题或载荷
        let page = mgr.query("c", &QueryOpts { filter: Some("HELLO".into()), limit: 100, ..Default::default() });
        assert_eq!(page.total, 1);
        assert_eq!(page.rows[0].topic, "sensor/hum");

        // 忽略 QoS0
        let page = mgr.query("c", &QueryOpts { ignore_qos0: true, limit: 100, ..Default::default() });
        assert_eq!(page.total, 2);

        // 方向过滤
        let page = mgr.query("c", &QueryOpts { dir: Some("rx".into()), limit: 100, ..Default::default() });
        assert_eq!(page.total, 2);

        // 正则
        let page = mgr.query("c", &QueryOpts { filter: Some(r"\d+\.\d+".into()), regex: true, limit: 100, ..Default::default() });
        assert_eq!(page.total, 1);
        assert_eq!(page.rows[0].payload, "23.5");

        // 分页：limit 1 但 total 反映全部
        let page = mgr.query("c", &QueryOpts { limit: 1, offset: 0, ..Default::default() });
        assert_eq!(page.total, 3);
        assert_eq!(page.rows.len(), 1);
    }

    #[test]
    fn whole_word_matcher() {
        let m = build_matcher(&QueryOpts { filter: Some("err".into()), whole_word: true, ..Default::default() }).unwrap();
        assert!(!m("t", "errors here")); // err 不是独立单词
        assert!(m("t", "an err occurred"));
    }

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
