use serde::{Deserialize, Serialize};

/// 遗嘱（LWT）配置。
#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct Will {
    pub enabled: bool,
    pub topic: String,
    pub payload: String,
    pub qos: u8,
    pub retain: bool,
    // ---- MQTT 5.0 遗嘱属性 ----
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_interval: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_expiry_interval: Option<u32>,
    #[serde(default)]
    pub content_type: String,
    #[serde(default)]
    pub response_topic: String,
}

/// 键值对（用于 MQTT 5.0 User Properties）。
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct KeyVal {
    pub key: String,
    pub value: String,
}

/// 订阅档案（持久化 + 断线自动重订阅）。含 MQTT 5.0 订阅选项与展示属性。
#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct SubProfile {
    pub topic: String,
    pub qos: u8,
    #[serde(default)]
    pub color: String,
    #[serde(default)]
    pub alias: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 静音：仍订阅但前端隐藏其消息。
    #[serde(default)]
    pub muted: bool,
    /// 该订阅的独立展示格式（空 = 跟随全局）。
    #[serde(default)]
    pub format: String,
    // ---- MQTT 5.0 订阅选项 ----
    #[serde(default)]
    pub nl: bool, // No Local
    #[serde(default)]
    pub rap: bool, // Retain As Published
    #[serde(default)]
    pub rh: u8, // Retain Handling 0/1/2
    #[serde(default)]
    pub favorite: bool,
}

/// 连接档案，持久化为 JSON。id 由前端生成（crypto.randomUUID）。
#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct Profile {
    pub id: String,
    pub name: String,
    pub protocol: String, // tcp | tls | ws | wss
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub path: String, // ws/wss 路径
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_keep_alive")]
    pub keep_alive: u64,
    #[serde(default = "default_true")]
    pub clean_session: bool,
    #[serde(default = "default_mqtt_version")]
    pub mqtt_version: u8, // 4 = 3.1.1, 5 = 5.0
    #[serde(default)]
    pub tls_skip_verify: bool,
    #[serde(default)]
    pub ca_cert: String, // 可选 CA 证书 PEM
    #[serde(default)]
    pub client_cert: String, // 双向 TLS：客户端证书 PEM
    #[serde(default)]
    pub client_key: String, // 双向 TLS：客户端私钥 PEM
    #[serde(default)]
    pub alpn: Vec<String>, // ALPN 协议
    #[serde(default)]
    pub sort_order: i32,
    #[serde(default)]
    pub group: String, // 分组名（空 = 未分组）
    #[serde(default)]
    pub client_id_with_time: bool, // ClientId 追加时间戳
    #[serde(default)]
    pub auto_reconnect: bool,
    #[serde(default = "default_reconnect_period")]
    pub reconnect_period_ms: u64,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: u64, // 秒
    #[serde(default)]
    pub will: Will,
    #[serde(default)]
    pub subscriptions: Vec<SubProfile>,
    // ---- MQTT 5.0 CONNECT 属性 ----
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_expiry_interval: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receive_maximum: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_packet_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_alias_maximum: Option<u16>,
    #[serde(default)]
    pub user_properties: Vec<KeyVal>,
}

fn default_keep_alive() -> u64 {
    60
}
fn default_true() -> bool {
    true
}
fn default_mqtt_version() -> u8 {
    4
}
fn default_reconnect_period() -> u64 {
    3000
}
fn default_connect_timeout() -> u64 {
    30
}
