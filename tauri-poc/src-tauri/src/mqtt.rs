//! 多连接 MQTT 管理器：支持 MQTT 3.1.1(v4) 与 5.0(v5)，遗嘱，按 connId 管理多条连接。
//! 传输：TCP / TLS / WebSocket / WSS。
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use tauri::async_runtime::JoinHandle;
use tauri::{AppHandle, Emitter, State};

use crate::model::{MessageEvent, Profile, StatusEvent};

#[derive(Clone)]
enum ClientKind {
    V4(rumqttc::AsyncClient),
    V5(rumqttc::v5::AsyncClient),
}

struct ConnHandle {
    client: ClientKind,
    task: JoinHandle<()>,
}

#[derive(Default)]
pub struct Manager {
    conns: Mutex<HashMap<String, ConnHandle>>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
        StatusEvent {
            conn_id: conn_id.to_string(),
            status: status.to_string(),
            detail,
        },
    );
}

fn emit_msg(app: &AppHandle, conn_id: &str, topic: String, payload: String, qos: u8, retain: bool) {
    let _ = app.emit(
        "mqtt:message",
        MessageEvent {
            conn_id: conn_id.to_string(),
            dir: "rx".into(),
            topic,
            payload,
            qos,
            retain,
            ts: now_ms(),
        },
    );
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
        let mut map = self.conns.lock().unwrap();
        if let Some(old) = map.insert(conn_id, handle) {
            old.task.abort();
        }
    }

    fn remove(&self, conn_id: &str) -> Option<ConnHandle> {
        self.conns.lock().unwrap().remove(conn_id)
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

/// 依协议返回 (连接地址, 端口, Transport)。ws/wss 的地址是完整 URL。
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

/// 建立连接。已存在同 id 连接会先被拆除。
pub fn connect(app: AppHandle, mgr: &Manager, profile: Profile) -> Result<(), String> {
    // 拆掉旧连接
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
fn connect_v4(
    app: AppHandle,
    mgr: &Manager,
    conn_id: &str,
    client_id: &str,
    p: &Profile,
    addr: String,
    port: u16,
    transport: rumqttc::Transport,
) -> Result<(), String> {
    use rumqttc::{AsyncClient, Event, MqttOptions, Packet};

    let mut opts = MqttOptions::new(client_id, addr, port);
    opts.set_transport(transport);
    opts.set_keep_alive(Duration::from_secs(p.keep_alive.max(5)));
    opts.set_clean_session(p.clean_session);
    if !p.username.is_empty() {
        opts.set_credentials(p.username.clone(), p.password.clone());
    }
    if p.will.enabled && !p.will.topic.is_empty() {
        opts.set_last_will(rumqttc::LastWill::new(
            &p.will.topic,
            p.will.payload.clone().into_bytes(),
            v4_qos(p.will.qos),
            p.will.retain,
        ));
    }

    let (client, mut eventloop) = AsyncClient::new(opts, 64);
    let app2 = app.clone();
    let id = conn_id.to_string();
    let task = tauri::async_runtime::spawn(async move {
        emit_status(&app2, &id, "connecting", None);
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => emit_status(&app2, &id, "connected", None),
                Ok(Event::Incoming(Packet::Publish(pkt))) => emit_msg(
                    &app2,
                    &id,
                    pkt.topic.clone(),
                    String::from_utf8_lossy(&pkt.payload).to_string(),
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
    mgr.insert(conn_id.to_string(), ConnHandle { client: ClientKind::V4(client), task });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn connect_v5(
    app: AppHandle,
    mgr: &Manager,
    conn_id: &str,
    client_id: &str,
    p: &Profile,
    addr: String,
    port: u16,
    transport: rumqttc::Transport,
) -> Result<(), String> {
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
        opts.set_last_will(rumqttc::v5::mqttbytes::v5::LastWill::new(
            &p.will.topic,
            p.will.payload.clone().into_bytes(),
            v5_qos(p.will.qos),
            p.will.retain,
            None,
        ));
    }

    let (client, mut eventloop) = AsyncClient::new(opts, 64);
    let app2 = app.clone();
    let id = conn_id.to_string();
    let task = tauri::async_runtime::spawn(async move {
        emit_status(&app2, &id, "connecting", None);
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => emit_status(&app2, &id, "connected", None),
                Ok(Event::Incoming(Packet::Publish(pkt))) => emit_msg(
                    &app2,
                    &id,
                    String::from_utf8_lossy(&pkt.topic).to_string(),
                    String::from_utf8_lossy(&pkt.payload).to_string(),
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

pub async fn publish(
    mgr: State<'_, Manager>,
    conn_id: String,
    topic: String,
    payload: String,
    qos: u8,
    retain: bool,
) -> Result<(), String> {
    let c = mgr.client_of(&conn_id).ok_or("未连接")?;
    c.publish(topic, payload.into_bytes(), qos, retain).await
}
