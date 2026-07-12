//! 内嵌 MQTT broker：基于 rumqttc 报文编解码，自建 accept/路由循环。
//! 支持 MQTT 3.1.1 与 5.0（按 CONNECT 协议级别分流），可干净随时启停。
//! 传输：TCP。跨平台（含 Android，纯 tokio 网络）。
//!
//! 能力概览：
//! - QoS 0/1/2 完整握手 + inflight 状态机；投递 QoS 降级 min(发布,订阅)
//! - 持久会话（v4 clean_session=0 / v5 session_expiry）：订阅、离线队列、inflight 保持
//! - MQTT 5.0：属性透传、入站 topic alias、No Local / Retain As Published / Retain Handling、
//!   订阅标识符、会话过期、遗嘱延迟（尽力）
//! - 同 client_id 顶替、keepalive 空闲超时、出站有界队列背压、主题/过滤器校验
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use rumqttc::mqttbytes::v4;
use rumqttc::mqttbytes::{Error as MqttError, QoS};
use rumqttc::v5::mqttbytes::v5 as m5;
use rumqttc::v5::mqttbytes::QoS as Q5;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Notify};

const MAX_PKT: usize = 256 * 1024;
const OUT_CAP: usize = 1024;
const MAX_QUEUE: usize = 4096;

const PROTO_V4: u8 = 4;
const PROTO_V5: u8 = 5;

// ---- QoS 工具 ----

fn qos_min(a: QoS, b: QoS) -> QoS {
    if (a as u8) <= (b as u8) { a } else { b }
}
fn qos_from_u8(n: u8) -> QoS {
    match n {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}
fn q5_from_u8(n: u8) -> Q5 {
    match n {
        1 => Q5::AtLeastOnce,
        2 => Q5::ExactlyOnce,
        _ => Q5::AtMostOnce,
    }
}

// ---- 主题校验 / 匹配 ----

fn valid_topic(t: &str) -> bool {
    !t.is_empty() && !t.contains('#') && !t.contains('+') && !t.contains('\u{0}')
}
fn valid_filter(f: &str) -> bool {
    if f.is_empty() || f.contains('\u{0}') {
        return false;
    }
    let levels: Vec<&str> = f.split('/').collect();
    let n = levels.len();
    for (i, lv) in levels.iter().enumerate() {
        if lv.contains('#') && (*lv != "#" || i != n - 1) {
            return false;
        }
        if lv.contains('+') && *lv != "+" {
            return false;
        }
    }
    true
}
fn topic_matches(filter: &str, topic: &str) -> bool {
    let f: Vec<&str> = filter.split('/').collect();
    let t: Vec<&str> = topic.split('/').collect();
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

// ---- 事件下沉抽象 ----

pub trait EventSink: Send + Sync + 'static {
    fn emit_json(&self, event: &str, payload: serde_json::Value);
}
fn sink_emit<S: Serialize>(sink: &Option<Arc<dyn EventSink>>, event: &str, payload: S) {
    if let Some(s) = sink {
        if let Ok(v) = serde_json::to_value(payload) {
            s.emit_json(event, v);
        }
    }
}
fn emit<S: Serialize + Clone>(sink: &Option<Arc<dyn EventSink>>, event: &str, payload: S) {
    sink_emit(sink, event, payload);
}

// ---- 配置 ----

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
    pub max_clients: usize,
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
    pub proto: u8,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BrokerEvent {
    pub kind: String,
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

// ---- 中立消息表示（跨 v4/v5） ----

#[derive(Clone, Default)]
struct MsgProps {
    payload_format: Option<u8>,
    message_expiry: Option<u32>,
    response_topic: Option<String>,
    correlation: Option<Bytes>,
    user_props: Vec<(String, String)>,
    content_type: Option<String>,
    subscription_ids: Vec<usize>,
}
#[derive(Clone)]
struct Msg {
    dup: bool,
    qos: QoS,
    retain: bool,
    topic: String,
    pkid: u16,
    payload: Bytes,
    props: MsgProps,
}
impl Msg {
    fn simple(topic: String, qos: QoS, retain: bool, payload: Bytes) -> Self {
        Msg { dup: false, qos, retain, topic, pkid: 0, payload, props: MsgProps::default() }
    }
}

/// 中立出站报文；由写任务按连接协议编码为 v4 或 v5 字节。
enum Out {
    ConnAck { present: bool, code: ConnCode, assigned_id: Option<String> },
    SubAck { pkid: u16, codes: Vec<u8> }, // 0/1/2=授权QoS, 0x80=失败
    UnsubAck { pkid: u16, n: usize },
    PubAck(u16),
    PubRec(u16),
    PubRel(u16),
    PubComp(u16),
    PingResp,
    Publish(Msg),
}
#[derive(Clone, Copy)]
enum ConnCode {
    Ok,
    BadAuth,
    Unavailable,
}

fn encode(out: &Out, proto: u8, buf: &mut BytesMut) -> Result<(), ()> {
    if proto == PROTO_V5 {
        encode_v5(out, buf)
    } else {
        encode_v4(out, buf)
    }
}

fn encode_v4(out: &Out, buf: &mut BytesMut) -> Result<(), ()> {
    use v4::*;
    let r = match out {
        Out::ConnAck { present, code, .. } => {
            let c = match code {
                ConnCode::Ok => ConnectReturnCode::Success,
                ConnCode::BadAuth => ConnectReturnCode::BadUserNamePassword,
                ConnCode::Unavailable => ConnectReturnCode::ServiceUnavailable,
            };
            ConnAck::new(c, *present).write(buf)
        }
        Out::SubAck { pkid, codes } => {
            let rc: Vec<SubscribeReasonCode> = codes
                .iter()
                .map(|c| match c {
                    0x80 => SubscribeReasonCode::Failure,
                    n => SubscribeReasonCode::Success(qos_from_u8(*n)),
                })
                .collect();
            SubAck::new(*pkid, rc).write(buf)
        }
        Out::UnsubAck { pkid, .. } => UnsubAck::new(*pkid).write(buf),
        Out::PubAck(p) => PubAck::new(*p).write(buf),
        Out::PubRec(p) => PubRec::new(*p).write(buf),
        Out::PubRel(p) => PubRel::new(*p).write(buf),
        Out::PubComp(p) => PubComp::new(*p).write(buf),
        Out::PingResp => PingResp.write(buf),
        Out::Publish(m) => {
            let mut p = Publish::new(m.topic.clone(), m.qos, m.payload.to_vec());
            p.retain = m.retain;
            p.dup = m.dup;
            p.pkid = m.pkid;
            p.write(buf)
        }
    };
    r.map(|_| ()).map_err(|_| ())
}

fn encode_v5(out: &Out, buf: &mut BytesMut) -> Result<(), ()> {
    let r = match out {
        Out::ConnAck { present, code, assigned_id } => {
            let c = match code {
                ConnCode::Ok => m5::ConnectReturnCode::Success,
                ConnCode::BadAuth => m5::ConnectReturnCode::BadUserNamePassword,
                ConnCode::Unavailable => m5::ConnectReturnCode::ServerUnavailable,
            };
            let props = assigned_id.as_ref().map(|id| m5::ConnAckProperties {
                session_expiry_interval: None,
                receive_max: None,
                max_qos: None,
                retain_available: Some(1),
                max_packet_size: None,
                assigned_client_identifier: Some(id.clone()),
                topic_alias_max: None,
                reason_string: None,
                user_properties: vec![],
                wildcard_subscription_available: Some(1),
                subscription_identifiers_available: Some(1),
                shared_subscription_available: Some(1),
                server_keep_alive: None,
                response_information: None,
                server_reference: None,
                authentication_method: None,
                authentication_data: None,
            });
            m5::ConnAck { session_present: *present, code: c, properties: props }.write(buf)
        }
        Out::SubAck { pkid, codes } => {
            let rc: Vec<m5::SubscribeReasonCode> = codes
                .iter()
                .map(|c| match c {
                    0x80 => m5::SubscribeReasonCode::Unspecified,
                    n => m5::SubscribeReasonCode::Success(q5_from_u8(*n)),
                })
                .collect();
            m5::SubAck { pkid: *pkid, return_codes: rc, properties: None }.write(buf)
        }
        Out::UnsubAck { pkid, n } => {
            m5::UnsubAck {
                pkid: *pkid,
                reasons: vec![m5::UnsubAckReason::Success; (*n).max(1)],
                properties: None,
            }
            .write(buf)
        }
        Out::PubAck(p) => m5::PubAck::new(*p, None).write(buf),
        Out::PubRec(p) => m5::PubRec::new(*p, None).write(buf),
        Out::PubRel(p) => m5::PubRel::new(*p, None).write(buf),
        Out::PubComp(p) => m5::PubComp::new(*p, None).write(buf),
        Out::PingResp => m5::PingResp::write(buf),
        Out::Publish(m) => {
            let props = build_v5_props(&m.props);
            let mut p = m5::Publish::new(m.topic.clone(), q5_from_u8(m.qos as u8), m.payload.to_vec(), props);
            p.retain = m.retain;
            p.dup = m.dup;
            p.pkid = m.pkid;
            p.write(buf)
        }
    };
    r.map(|_| ()).map_err(|_| ())
}

fn build_v5_props(mp: &MsgProps) -> Option<m5::PublishProperties> {
    let empty = mp.payload_format.is_none()
        && mp.message_expiry.is_none()
        && mp.response_topic.is_none()
        && mp.correlation.is_none()
        && mp.user_props.is_empty()
        && mp.content_type.is_none()
        && mp.subscription_ids.is_empty();
    if empty {
        return None;
    }
    let mut p = m5::PublishProperties::default();
    p.payload_format_indicator = mp.payload_format;
    p.message_expiry_interval = mp.message_expiry;
    p.response_topic = mp.response_topic.clone();
    p.correlation_data = mp.correlation.clone();
    p.user_properties = mp.user_props.clone();
    p.content_type = mp.content_type.clone();
    p.subscription_identifiers = mp.subscription_ids.clone();
    Some(p)
}

// ---- 中立入站表示 ----

struct SubFilterIn {
    path: String,
    qos: u8,
    nolocal: bool,
    rap: bool,
    retain_handling: u8, // 0=每次订阅发, 1=仅新订阅发, 2=不发
}
struct ConnectIn {
    client_id: String,
    clean: bool,
    keep_alive: u16,
    username: String,
    password: String,
    has_login: bool,
    will: Option<(Msg, u32)>, // (遗嘱消息, 延迟秒)
    session_expiry: Option<u32>,
}
enum In {
    Connect(Box<ConnectIn>),
    Publish(Msg, Option<u16>), // (消息, 入站 topic alias)
    PubAck(u16),
    PubRec(u16),
    PubRel(u16),
    PubComp(u16),
    Subscribe { pkid: u16, filters: Vec<SubFilterIn>, sub_id: Option<usize> },
    Unsubscribe { pkid: u16, topics: Vec<String> },
    PingReq,
    Disconnect,
    Ignore,
}

/// 读取一个中立入站报文；Ok(None)=字节不足，Err=协议错误。
fn read_in(proto: u8, buf: &mut BytesMut) -> Result<Option<In>, ()> {
    if proto == PROTO_V5 {
        read_in_v5(buf)
    } else {
        read_in_v4(buf)
    }
}

fn read_in_v4(buf: &mut BytesMut) -> Result<Option<In>, ()> {
    use v4::Packet;
    match v4::read(buf, MAX_PKT) {
        Err(MqttError::InsufficientBytes(_)) => Ok(None),
        Err(_) => Err(()),
        Ok(pkt) => Ok(Some(match pkt {
            Packet::Connect(c) => {
                let (u, p, has) = c
                    .login
                    .as_ref()
                    .map(|l| (l.username.clone(), l.password.clone(), true))
                    .unwrap_or_default();
                let will = c.last_will.as_ref().map(|w| {
                    (Msg::simple(w.topic.clone(), w.qos, w.retain, Bytes::from(w.message.to_vec())), 0u32)
                });
                In::Connect(Box::new(ConnectIn {
                    client_id: c.client_id.clone(),
                    clean: c.clean_session,
                    keep_alive: c.keep_alive,
                    username: u,
                    password: p,
                    has_login: has,
                    will,
                    session_expiry: None,
                }))
            }
            Packet::Publish(p) => In::Publish(
                Msg {
                    dup: p.dup,
                    qos: p.qos,
                    retain: p.retain,
                    topic: p.topic.clone(),
                    pkid: p.pkid,
                    payload: Bytes::from(p.payload.to_vec()),
                    props: MsgProps::default(),
                },
                None,
            ),
            Packet::PubAck(a) => In::PubAck(a.pkid),
            Packet::PubRec(a) => In::PubRec(a.pkid),
            Packet::PubRel(a) => In::PubRel(a.pkid),
            Packet::PubComp(a) => In::PubComp(a.pkid),
            Packet::Subscribe(s) => In::Subscribe {
                pkid: s.pkid,
                filters: s
                    .filters
                    .iter()
                    .map(|f| SubFilterIn {
                        path: f.path.clone(),
                        qos: f.qos as u8,
                        nolocal: false,
                        rap: false,
                        retain_handling: 0,
                    })
                    .collect(),
                sub_id: None,
            },
            Packet::Unsubscribe(u) => In::Unsubscribe { pkid: u.pkid, topics: u.topics.clone() },
            Packet::PingReq => In::PingReq,
            Packet::Disconnect => In::Disconnect,
            _ => In::Ignore,
        })),
    }
}

fn read_in_v5(buf: &mut BytesMut) -> Result<Option<In>, ()> {
    use m5::Packet;
    match m5::Packet::read(buf, Some(MAX_PKT)) {
        Err(rumqttc::v5::mqttbytes::Error::InsufficientBytes(_)) => Ok(None),
        Err(_) => Err(()),
        Ok(pkt) => Ok(Some(match pkt {
            Packet::Connect(c, will, login) => {
                let (u, p, has) = login
                    .as_ref()
                    .map(|l| (l.username.clone(), l.password.clone(), true))
                    .unwrap_or_default();
                let will = will.as_ref().map(|w| {
                    let delay = w.properties.as_ref().and_then(|p| p.delay_interval).unwrap_or(0);
                    let mut m = Msg::simple(
                        String::from_utf8_lossy(&w.topic).to_string(),
                        qos_from_u8(w.qos as u8),
                        w.retain,
                        Bytes::from(w.message.to_vec()),
                    );
                    if let Some(wp) = &w.properties {
                        m.props.payload_format = wp.payload_format_indicator;
                        m.props.message_expiry = wp.message_expiry_interval;
                        m.props.response_topic = wp.response_topic.clone();
                        m.props.correlation = wp.correlation_data.clone();
                        m.props.user_props = wp.user_properties.clone();
                        m.props.content_type = wp.content_type.clone();
                    }
                    (m, delay)
                });
                let session_expiry = c.properties.as_ref().and_then(|p| p.session_expiry_interval);
                In::Connect(Box::new(ConnectIn {
                    client_id: c.client_id.clone(),
                    clean: c.clean_start,
                    keep_alive: c.keep_alive,
                    username: u,
                    password: p,
                    has_login: has,
                    will,
                    session_expiry,
                }))
            }
            Packet::Publish(p) => {
                let alias = p.properties.as_ref().and_then(|pp| pp.topic_alias);
                let mut props = MsgProps::default();
                if let Some(pp) = &p.properties {
                    props.payload_format = pp.payload_format_indicator;
                    props.message_expiry = pp.message_expiry_interval;
                    props.response_topic = pp.response_topic.clone();
                    props.correlation = pp.correlation_data.clone();
                    props.user_props = pp.user_properties.clone();
                    props.content_type = pp.content_type.clone();
                }
                In::Publish(
                    Msg {
                        dup: p.dup,
                        qos: qos_from_u8(p.qos as u8),
                        retain: p.retain,
                        topic: String::from_utf8_lossy(&p.topic).to_string(),
                        pkid: p.pkid,
                        payload: Bytes::from(p.payload.to_vec()),
                        props,
                    },
                    alias,
                )
            }
            Packet::PubAck(a) => In::PubAck(a.pkid),
            Packet::PubRec(a) => In::PubRec(a.pkid),
            Packet::PubRel(a) => In::PubRel(a.pkid),
            Packet::PubComp(a) => In::PubComp(a.pkid),
            Packet::Subscribe(s) => {
                let sub_id = s.properties.as_ref().and_then(|p| p.id);
                In::Subscribe {
                    pkid: s.pkid,
                    filters: s
                        .filters
                        .iter()
                        .map(|f| SubFilterIn {
                            path: f.path.clone(),
                            qos: f.qos as u8,
                            nolocal: f.nolocal,
                            rap: f.preserve_retain,
                            retain_handling: match f.retain_forward_rule {
                                m5::RetainForwardRule::OnEverySubscribe => 0,
                                m5::RetainForwardRule::OnNewSubscribe => 1,
                                m5::RetainForwardRule::Never => 2,
                            },
                        })
                        .collect(),
                    sub_id,
                }
            }
            Packet::Unsubscribe(u) => In::Unsubscribe { pkid: u.pkid, topics: u.filters.clone() },
            Packet::PingReq(_) => In::PingReq,
            Packet::Disconnect(_) => In::Disconnect,
            _ => In::Ignore,
        })),
    }
}

// ---- 会话 / 核心 ----

#[derive(Clone)]
enum Phase {
    AwaitAck,
    AwaitComp,
}
#[derive(Clone)]
struct Inflight {
    msg: Msg,
    phase: Phase,
}
#[derive(Clone)]
struct Sub {
    filter: String,
    qos: QoS,
    nolocal: bool,
    rap: bool,
    sub_id: Option<usize>,
}
struct Conn {
    tx: mpsc::Sender<Out>,
    conn_id: u64,
    kick: Arc<Notify>,
}
struct Session {
    online: Option<Conn>,
    addr: String,
    username: String,
    proto: u8,
    clean: bool,
    session_expiry: Option<u32>,
    expires_at: Option<u64>,
    subs: Vec<Sub>,
    next_pkid: u16,
    inflight: BTreeMap<u16, Inflight>,
    queue: VecDeque<Msg>,
    incoming_qos2: HashMap<u16, Msg>,
    will: Option<(Msg, u32)>,
}
impl Session {
    fn new(addr: String, username: String, proto: u8, clean: bool) -> Self {
        Session {
            online: None,
            addr,
            username,
            proto,
            clean,
            session_expiry: None,
            expires_at: None,
            subs: vec![],
            next_pkid: 0,
            inflight: BTreeMap::new(),
            queue: VecDeque::new(),
            incoming_qos2: HashMap::new(),
            will: None,
        }
    }
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
    /// 投递一条已按订阅降级/加属性的消息。返回实际在线发送数(用于统计)。
    fn deliver(&mut self, mut m: Msg, eff: QoS) -> u64 {
        m.qos = eff;
        m.dup = false;
        if eff == QoS::AtMostOnce {
            m.pkid = 0;
            if let Some(c) = &self.online {
                return c.tx.try_send(Out::Publish(m)).is_ok() as u64;
            }
            return 0;
        }
        if self.online.is_some() {
            let pkid = self.alloc_pkid();
            m.pkid = pkid;
            let sent = self
                .online
                .as_ref()
                .map(|c| c.tx.try_send(Out::Publish(m.clone())).is_ok())
                .unwrap_or(false);
            self.inflight.insert(pkid, Inflight { msg: m, phase: Phase::AwaitAck });
            sent as u64
        } else if !self.clean {
            if self.queue.len() >= MAX_QUEUE {
                self.queue.pop_front();
            }
            self.queue.push_back(m);
            0
        } else {
            0
        }
    }
    fn resume_outbound(&mut self) -> Vec<Out> {
        let mut outs = vec![];
        for (pkid, inf) in self.inflight.iter() {
            match inf.phase {
                Phase::AwaitAck => {
                    let mut m = inf.msg.clone();
                    m.dup = true;
                    outs.push(Out::Publish(m));
                }
                Phase::AwaitComp => outs.push(Out::PubRel(*pkid)),
            }
        }
        let queued: Vec<Msg> = self.queue.drain(..).collect();
        for mut m in queued {
            let pkid = self.alloc_pkid();
            m.pkid = pkid;
            m.dup = false;
            outs.push(Out::Publish(m.clone()));
            self.inflight.insert(pkid, Inflight { msg: m, phase: Phase::AwaitAck });
        }
        outs
    }
}

#[derive(Default)]
struct Core {
    sessions: HashMap<String, Session>,
    retained: HashMap<String, Msg>,
}
impl Core {
    fn online_count(&self) -> u64 {
        self.sessions.values().filter(|s| s.online.is_some()).count() as u64
    }
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
pub fn retained(state: &BrokerState) -> Vec<RetainedRow> {
    let g = state.running.lock().unwrap();
    let Some(r) = g.as_ref() else { return vec![] };
    let core = r.core.lock().unwrap();
    core.retained
        .values()
        .map(|m| RetainedRow {
            topic: m.topic.clone(),
            payload: String::from_utf8_lossy(&m.payload).to_string(),
            qos: m.qos as u8,
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

pub fn is_running(state: &BrokerState) -> bool {
    state.running.lock().unwrap().is_some()
}
pub fn current_config(state: &BrokerState) -> Option<BrokerConfig> {
    state.running.lock().unwrap().as_ref().map(|r| r.config.clone())
}

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
                            let mut g = core.lock().unwrap();
                            // 会话过期清理。
                            let now = now_ms();
                            let expired: Vec<String> = g.sessions.iter()
                                .filter(|(_, s)| s.online.is_none() && s.expires_at.map_or(false, |e| now >= e))
                                .map(|(k, _)| k.clone()).collect();
                            for k in expired { g.sessions.remove(&k); }
                            let rows: Vec<ClientRow> = g.sessions.iter()
                                .filter(|(_, s)| s.online.is_some())
                                .map(|(id, s)| ClientRow {
                                    client_id: id.clone(), addr: s.addr.clone(),
                                    username: s.username.clone(), subs: s.subs.len(), proto: s.proto,
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

/// 从缓冲区探测 CONNECT 的协议级别（4 或 5）；字节不足返回 None。
fn peek_protocol(buf: &[u8]) -> Option<u8> {
    if buf.len() < 2 {
        return None;
    }
    if buf[0] >> 4 != 1 {
        return Some(PROTO_V4); // 首包非 CONNECT，按 v4 兜底
    }
    // 解析 remaining length（varint），定位可变报头。
    let mut i = 1usize;
    let mut mult = 1u32;
    let mut _len = 0u32;
    loop {
        if i >= buf.len() {
            return None;
        }
        let b = buf[i];
        _len += (b & 0x7f) as u32 * mult;
        i += 1;
        if b & 0x80 == 0 {
            break;
        }
        mult *= 128;
        if i > 5 {
            return Some(PROTO_V4);
        }
    }
    // 可变报头：00 04 'M' 'Q' 'T' 'T' level
    let idx = i + 6;
    if buf.len() > idx {
        Some(buf[idx])
    } else {
        None
    }
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

    let proto = Arc::new(AtomicU8::new(PROTO_V4));
    let writer = {
        let proto = proto.clone();
        tokio::spawn(async move {
            while let Some(out) = rx.recv().await {
                let mut b = BytesMut::new();
                if encode(&out, proto.load(Ordering::Relaxed), &mut b).is_err() {
                    continue;
                }
                if wr.write_all(&b).await.is_err() {
                    break;
                }
            }
        })
    };

    let conn_id = counters.next_conn.fetch_add(1, Ordering::Relaxed);
    let kick = Arc::new(Notify::new());

    let mut buf = BytesMut::with_capacity(4096);
    let mut client_id = String::new();
    let mut connected = false;
    let mut clean_disconnect = false;
    let mut kicked = false;
    let mut my_proto = 0u8; // 0=未定
    let mut inbound_alias: HashMap<u16, String> = HashMap::new();

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
                // 首包前先探测协议级别。
                if my_proto == 0 {
                    match peek_protocol(&buf) {
                        Some(p) => {
                            my_proto = if p == PROTO_V5 { PROTO_V5 } else { PROTO_V4 };
                            proto.store(my_proto, Ordering::Relaxed);
                        }
                        None => continue, // 继续读取直到能判定
                    }
                }
                loop {
                    let item = match read_in(my_proto, &mut buf) {
                        Ok(Some(p)) => p,
                        Ok(None) => break,
                        Err(_) => break 'conn,
                    };
                    match item {
                        In::Connect(c) => {
                            if connected { break 'conn; }
                            let auth_ok = config.allow_anonymous
                                || (c.has_login && c.username == config.username && c.password == config.password);
                            let assigned = c.client_id.is_empty();
                            let cid = if assigned { format!("auto-{}", now_ms()) } else { c.client_id.clone() };
                            let full = {
                                let g = core.lock().unwrap();
                                config.max_clients > 0 && !g.sessions.contains_key(&cid)
                                    && g.online_count() as usize >= config.max_clients
                            };
                            if !auth_ok || full {
                                let code = if full { ConnCode::Unavailable } else { ConnCode::BadAuth };
                                let _ = tx.send(Out::ConnAck { present: false, code, assigned_id: None }).await;
                                break 'conn;
                            }
                            client_id = cid;
                            // 持久性：v4 clean=false，或 v5 session_expiry>0。
                            let persistent = !c.clean || c.session_expiry.map_or(false, |e| e > 0);
                            let (session_present, resend, old_kick) = {
                                let mut g = core.lock().unwrap();
                                // 先摘取旧连接的踢出句柄（无论是否续会话都要踢掉旧连接）。
                                let old_kick = g.sessions.get_mut(&client_id)
                                    .and_then(|s| s.online.take()).map(|c| c.kick);
                                let resumable = persistent && g.sessions.contains_key(&client_id);
                                if !persistent {
                                    g.sessions.remove(&client_id);
                                }
                                let sess = g.sessions.entry(client_id.clone())
                                    .or_insert_with(|| Session::new(peer.clone(), c.username.clone(), my_proto, !persistent));
                                sess.addr = peer.clone();
                                sess.username = c.username.clone();
                                sess.proto = my_proto;
                                sess.clean = !persistent;
                                sess.session_expiry = c.session_expiry;
                                sess.expires_at = None;
                                sess.will = c.will.clone();
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
                            let _ = tx.send(Out::ConnAck {
                                present: session_present,
                                code: ConnCode::Ok,
                                assigned_id: if assigned && my_proto == PROTO_V5 { Some(client_id.clone()) } else { None },
                            }).await;
                            for o in resend { let _ = tx.send(o).await; }
                            emit(&app, "broker:event", BrokerEvent {
                                kind: "connect".into(), client_id: client_id.clone(),
                                topic: None, payload: None, ts: now_ms(),
                            });
                        }
                        In::Subscribe { pkid, filters, sub_id } if connected => {
                            let mut codes = vec![];
                            let mut deliver_retained: Vec<(String, u8, bool)> = vec![]; // (filter, qos, rap)
                            {
                                let mut g = core.lock().unwrap();
                                if let Some(sess) = g.sessions.get_mut(&client_id) {
                                    for f in &filters {
                                        if !valid_filter(&f.path) {
                                            codes.push(0x80);
                                            continue;
                                        }
                                        let granted = f.qos;
                                        let existed = sess.subs.iter().any(|s| s.filter == f.path);
                                        let sub = Sub {
                                            filter: f.path.clone(),
                                            qos: qos_from_u8(granted),
                                            nolocal: f.nolocal,
                                            rap: f.rap,
                                            sub_id,
                                        };
                                        if let Some(e) = sess.subs.iter_mut().find(|s| s.filter == f.path) {
                                            *e = sub;
                                        } else {
                                            sess.subs.push(sub);
                                        }
                                        codes.push(granted);
                                        let send_ret = match f.retain_handling {
                                            2 => false,
                                            1 => !existed,
                                            _ => true,
                                        };
                                        if send_ret {
                                            deliver_retained.push((f.path.clone(), granted, f.rap));
                                        }
                                    }
                                }
                            }
                            let _ = tx.send(Out::SubAck { pkid, codes }).await;
                            // 保留消息下发。
                            let outs = {
                                let mut g = core.lock().unwrap();
                                let retained: Vec<Msg> = g.retained.values().cloned().collect();
                                let mut outs = vec![];
                                if let Some(sess) = g.sessions.get_mut(&client_id) {
                                    for m in &retained {
                                        let mut best: Option<u8> = None;
                                        let mut rap = false;
                                        for (fp, gq, r) in &deliver_retained {
                                            if topic_matches(fp, &m.topic) {
                                                if best.map_or(true, |b| *gq > b) { best = Some(*gq); }
                                                rap |= *r;
                                            }
                                        }
                                        if let Some(gq) = best {
                                            let eff = qos_min(m.qos, qos_from_u8(gq));
                                            let mut d = m.clone();
                                            d.retain = true; // 保留消息下发保持 retain=1
                                            let _ = rap; // 保留消息始终带 retain
                                            if sess.online.is_some() {
                                                if eff == QoS::AtMostOnce {
                                                    d.qos = eff; d.pkid = 0; d.dup = false;
                                                    outs.push(Out::Publish(d));
                                                } else {
                                                    let pkid = sess.alloc_pkid();
                                                    d.qos = eff; d.pkid = pkid; d.dup = false;
                                                    sess.inflight.insert(pkid, Inflight { msg: d.clone(), phase: Phase::AwaitAck });
                                                    outs.push(Out::Publish(d));
                                                }
                                            }
                                        }
                                    }
                                }
                                outs
                            };
                            for o in outs { let _ = tx.send(o).await; }
                            for f in &filters {
                                if valid_filter(&f.path) {
                                    emit(&app, "broker:event", BrokerEvent {
                                        kind: "subscribe".into(), client_id: client_id.clone(),
                                        topic: Some(f.path.clone()), payload: None, ts: now_ms(),
                                    });
                                }
                            }
                        }
                        In::Unsubscribe { pkid, topics } if connected => {
                            let n = topics.len();
                            {
                                let mut g = core.lock().unwrap();
                                if let Some(sess) = g.sessions.get_mut(&client_id) {
                                    sess.subs.retain(|s| !topics.contains(&s.filter));
                                }
                            }
                            let _ = tx.send(Out::UnsubAck { pkid, n }).await;
                            emit(&app, "broker:event", BrokerEvent {
                                kind: "unsubscribe".into(), client_id: client_id.clone(),
                                topic: topics.first().cloned(), payload: None, ts: now_ms(),
                            });
                        }
                        In::Publish(mut m, alias) if connected => {
                            // 入站 topic alias 解析/登记。
                            if let Some(a) = alias {
                                if m.topic.is_empty() {
                                    match inbound_alias.get(&a) {
                                        Some(t) => m.topic = t.clone(),
                                        None => break 'conn,
                                    }
                                } else {
                                    inbound_alias.insert(a, m.topic.clone());
                                }
                            }
                            if !valid_topic(&m.topic) { break 'conn; }
                            match m.qos {
                                QoS::AtMostOnce => {
                                    counters.recv.fetch_add(1, Ordering::Relaxed);
                                    route_publish(&core, &counters, &client_id, &m);
                                    emit_publish(&app, &client_id, &m);
                                }
                                QoS::AtLeastOnce => {
                                    counters.recv.fetch_add(1, Ordering::Relaxed);
                                    route_publish(&core, &counters, &client_id, &m);
                                    emit_publish(&app, &client_id, &m);
                                    let _ = tx.send(Out::PubAck(m.pkid)).await;
                                }
                                QoS::ExactlyOnce => {
                                    {
                                        let mut g = core.lock().unwrap();
                                        if let Some(sess) = g.sessions.get_mut(&client_id) {
                                            sess.incoming_qos2.entry(m.pkid).or_insert_with(|| m.clone());
                                        }
                                    }
                                    let _ = tx.send(Out::PubRec(m.pkid)).await;
                                }
                            }
                        }
                        In::PubRel(pkid) if connected => {
                            let msg = {
                                let mut g = core.lock().unwrap();
                                g.sessions.get_mut(&client_id).and_then(|s| s.incoming_qos2.remove(&pkid))
                            };
                            if let Some(m) = msg {
                                counters.recv.fetch_add(1, Ordering::Relaxed);
                                route_publish(&core, &counters, &client_id, &m);
                                emit_publish(&app, &client_id, &m);
                            }
                            let _ = tx.send(Out::PubComp(pkid)).await;
                        }
                        In::PubAck(pkid) if connected => {
                            let mut g = core.lock().unwrap();
                            if let Some(s) = g.sessions.get_mut(&client_id) { s.inflight.remove(&pkid); }
                        }
                        In::PubRec(pkid) if connected => {
                            {
                                let mut g = core.lock().unwrap();
                                if let Some(s) = g.sessions.get_mut(&client_id) {
                                    if let Some(inf) = s.inflight.get_mut(&pkid) { inf.phase = Phase::AwaitComp; }
                                }
                            }
                            let _ = tx.send(Out::PubRel(pkid)).await;
                        }
                        In::PubComp(pkid) if connected => {
                            let mut g = core.lock().unwrap();
                            if let Some(s) = g.sessions.get_mut(&client_id) { s.inflight.remove(&pkid); }
                        }
                        In::PingReq => { let _ = tx.send(Out::PingResp).await; }
                        In::Disconnect => { clean_disconnect = true; break 'conn; }
                        _ => {}
                    }
                }
            }
        }
    }

    // 断开处理。
    if connected {
        let (removed, will) = {
            let mut g = core.lock().unwrap();
            let is_current = g.sessions.get(&client_id)
                .and_then(|s| s.online.as_ref()).map(|c| c.conn_id) == Some(conn_id);
            if !is_current {
                (false, None)
            } else {
                let sess = g.sessions.get_mut(&client_id).unwrap();
                sess.online = None;
                let will = if clean_disconnect || kicked { sess.will = None; None } else { sess.will.take() };
                if sess.clean {
                    g.sessions.remove(&client_id);
                } else if let Some(exp) = sess.session_expiry {
                    // v5 会话过期倒计时（0xFFFFFFFF 视为永不过期）。
                    if exp != u32::MAX {
                        sess.expires_at = Some(now_ms() + exp as u64 * 1000);
                    }
                }
                counters.connected.store(g.online_count(), Ordering::Relaxed);
                (true, will)
            }
        };
        if let Some((m, delay)) = will {
            if delay == 0 {
                route_publish(&core, &counters, &client_id, &m);
            } else {
                // 遗嘱延迟：延迟后若会话仍离线则发布。
                let core2 = core.clone();
                let counters2 = counters.clone();
                let cid = client_id.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(delay as u64)).await;
                    let still_offline = {
                        let g = core2.lock().unwrap();
                        g.sessions.get(&cid).map_or(true, |s| s.online.is_none())
                    };
                    if still_offline {
                        route_publish(&core2, &counters2, &cid, &m);
                    }
                });
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

fn emit_publish(app: &Option<Arc<dyn EventSink>>, client_id: &str, m: &Msg) {
    emit(app, "broker:event", BrokerEvent {
        kind: "publish".into(),
        client_id: client_id.to_string(),
        topic: Some(m.topic.clone()),
        payload: Some(String::from_utf8_lossy(&m.payload).to_string()),
        ts: now_ms(),
    });
}

/// 路由一条发布消息到所有匹配订阅者（含离线持久会话入队）。
/// `from` 为发布者 client_id，用于 No Local 处理。
fn route_publish(core: &Arc<Mutex<Core>>, counters: &Arc<Counters>, from: &str, m: &Msg) {
    let mut g = core.lock().unwrap();
    if m.retain {
        if m.payload.is_empty() {
            g.retained.remove(&m.topic);
        } else {
            let mut r = m.clone();
            r.pkid = 0;
            g.retained.insert(m.topic.clone(), r);
        }
    }
    let mut sent = 0u64;
    let mut dropped = 0u64;
    for (key, sess) in g.sessions.iter_mut() {
        let mut best: Option<QoS> = None;
        let mut rap = false;
        let mut sub_ids: Vec<usize> = vec![];
        for s in &sess.subs {
            if topic_matches(&s.filter, &m.topic) {
                if s.nolocal && key == from {
                    continue;
                }
                best = Some(match best {
                    Some(b) if (b as u8) >= (s.qos as u8) => b,
                    _ => s.qos,
                });
                rap |= s.rap;
                if let Some(id) = s.sub_id {
                    sub_ids.push(id);
                }
            }
        }
        let Some(sub_qos) = best else { continue };
        let eff = qos_min(m.qos, sub_qos);
        let mut out = m.clone();
        out.retain = if rap { m.retain } else { false };
        out.props.subscription_ids = sub_ids;
        let online = sess.online.is_some();
        let n = sess.deliver(out, eff);
        sent += n;
        if online && eff == QoS::AtMostOnce && n == 0 {
            dropped += 1;
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

    #[test]
    fn test_valid_topic() {
        assert!(valid_topic("a/b/c"));
        assert!(valid_topic("$SYS/broker/x"));
        assert!(!valid_topic(""));
        assert!(!valid_topic("a/+/c"));
        assert!(!valid_topic("a/#"));
    }
    #[test]
    fn test_valid_filter() {
        assert!(valid_filter("a/+/c"));
        assert!(valid_filter("a/#"));
        assert!(valid_filter("#"));
        assert!(!valid_filter("a/#/b"));
        assert!(!valid_filter("a/b#"));
        assert!(!valid_filter(""));
    }
    #[test]
    fn test_topic_matches_dollar_exclusion() {
        assert!(!topic_matches("#", "$SYS/x"));
        assert!(topic_matches("$SYS/#", "$SYS/broker/uptime"));
        assert!(topic_matches("a/+", "a/b"));
        assert!(!topic_matches("a/+", "a/b/c"));
    }
    #[test]
    fn test_peek_protocol() {
        // v4 CONNECT: 10 <len> 00 04 M Q T T 04 ...
        let v4 = [0x10u8, 0x0c, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x00];
        assert_eq!(peek_protocol(&v4), Some(PROTO_V4));
        let v5 = [0x10u8, 0x0c, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x00];
        assert_eq!(peek_protocol(&v5), Some(PROTO_V5));
        assert_eq!(peek_protocol(&[0x10]), None);
    }

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
                let (stream, peer) = match listener.accept().await { Ok(v) => v, Err(_) => continue };
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

    // ---- v4 ----
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
                    return;
                }
                _ => {}
            }
        }
        panic!("未收到降级消息");
    }

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
        panic!("旧连接未被顶替");
    }

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
        panic!("QoS2 未完成");
    }

    #[tokio::test]
    async fn test_persistent_session_offline_queue() {
        use rumqttc::{AsyncClient, Event, MqttOptions, Packet as CPacket, QoS};
        let port = spawn_broker(BrokerConfig::default());
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
        let mut po = MqttOptions::new("p3", "127.0.0.1", port);
        po.set_keep_alive(Duration::from_secs(5));
        let (publisher, mut pel) = AsyncClient::new(po, 10);
        tokio::spawn(async move { loop { if pel.poll().await.is_err() { break; } } });
        publisher.publish("s", QoS::AtLeastOnce, false, b"offline".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
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
                    assert!(present);
                    return;
                }
                _ => {}
            }
        }
        panic!("离线队列未补投");
    }

    // ---- v5 ----
    #[tokio::test]
    async fn test_v5_roundtrip_and_props() {
        use rumqttc::v5::mqttbytes::v5::{Packet as CP, PublishProperties};
        use rumqttc::v5::mqttbytes::QoS;
        use rumqttc::v5::{AsyncClient, Event, MqttOptions};
        let port = spawn_broker(BrokerConfig::default());
        let mut so = MqttOptions::new("v5sub", "127.0.0.1", port);
        so.set_keep_alive(Duration::from_secs(5));
        let (sub, mut sel) = AsyncClient::new(so, 10);
        sub.subscribe("v/#", QoS::AtLeastOnce).await.unwrap();
        let mut po = MqttOptions::new("v5pub", "127.0.0.1", port);
        po.set_keep_alive(Duration::from_secs(5));
        let (publisher, mut pel) = AsyncClient::new(po, 10);
        tokio::spawn(async move { loop { if pel.poll().await.is_err() { break; } } });
        for _ in 0..120 {
            match sel.poll().await.unwrap() {
                Event::Incoming(CP::SubAck(_)) => {
                    let mut props = PublishProperties::default();
                    props.user_properties.push(("k".into(), "v".into()));
                    props.content_type = Some("application/json".into());
                    publisher.publish_with_properties("v/x", QoS::AtLeastOnce, false, b"{}".to_vec(), props).await.unwrap();
                }
                Event::Incoming(CP::Publish(p)) => {
                    assert_eq!(&p.payload[..], b"{}");
                    let pr = p.properties.expect("应透传 v5 属性");
                    assert_eq!(pr.content_type.as_deref(), Some("application/json"));
                    assert!(pr.user_properties.iter().any(|(k, v)| k == "k" && v == "v"));
                    return;
                }
                _ => {}
            }
        }
        panic!("v5 往返/属性透传失败");
    }

    #[tokio::test]
    async fn test_v5_no_local() {
        use rumqttc::v5::mqttbytes::v5::{Filter, Packet as CP};
        use rumqttc::v5::mqttbytes::QoS;
        use rumqttc::v5::{AsyncClient, Event, MqttOptions};
        let port = spawn_broker(BrokerConfig::default());
        // 同一客户端订阅(No Local)并自发布，不应收到自己的消息。
        let mut o = MqttOptions::new("nl", "127.0.0.1", port);
        o.set_keep_alive(Duration::from_secs(5));
        let (c, mut el) = AsyncClient::new(o, 10);
        let mut f = Filter::new("nl/#", QoS::AtLeastOnce);
        f.nolocal = true;
        c.subscribe_many(vec![f]).await.unwrap();
        // 先等到 SubAck，再自发布。
        loop {
            if let Ok(Event::Incoming(CP::SubAck(_))) = el.poll().await {
                c.publish("nl/x", QoS::AtLeastOnce, false, b"self".to_vec()).await.unwrap();
                break;
            }
        }
        // 在有限时间内轮询：若收到自己的 Publish 即失败；超时无消息即通过。
        let deadline = tokio::time::Instant::now() + Duration::from_millis(800);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return; // 未收到自己的发布 -> No Local 生效
            }
            match tokio::time::timeout(remaining, el.poll()).await {
                Ok(Ok(Event::Incoming(CP::Publish(_)))) => panic!("No Local 订阅不应收到自己的发布"),
                Ok(_) => {}
                Err(_) => return, // 超时，通过
            }
        }
    }

    // v4 与 v5 互通：v5 发布，v4 订阅者应收到（QoS 正常）。
    #[tokio::test]
    async fn test_cross_protocol() {
        use rumqttc::{AsyncClient, Event, MqttOptions, Packet as CPacket, QoS};
        let port = spawn_broker(BrokerConfig::default());
        let mut so = MqttOptions::new("v4sub", "127.0.0.1", port);
        so.set_keep_alive(Duration::from_secs(5));
        let (sub, mut sel) = AsyncClient::new(so, 10);
        sub.subscribe("x/#", QoS::AtLeastOnce).await.unwrap();
        // v5 publisher
        use rumqttc::v5::{AsyncClient as A5, MqttOptions as O5};
        use rumqttc::v5::mqttbytes::QoS as Q5t;
        let mut po = O5::new("v5p", "127.0.0.1", port);
        po.set_keep_alive(Duration::from_secs(5));
        let (publisher, mut pel) = A5::new(po, 10);
        tokio::spawn(async move { loop { if pel.poll().await.is_err() { break; } } });
        for _ in 0..120 {
            match sel.poll().await.unwrap() {
                Event::Incoming(CPacket::SubAck(_)) => {
                    publisher.publish("x/y", Q5t::AtLeastOnce, false, b"cross".to_vec()).await.unwrap();
                }
                Event::Incoming(CPacket::Publish(p)) => {
                    assert_eq!(&p.payload[..], b"cross");
                    return;
                }
                _ => {}
            }
        }
        panic!("跨协议投递失败");
    }
}
