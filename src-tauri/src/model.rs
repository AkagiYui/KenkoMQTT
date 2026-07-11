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
    pub sort_order: i32,
    #[serde(default)]
    pub will: Will,
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
