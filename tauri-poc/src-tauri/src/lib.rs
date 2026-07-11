use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::time::Duration;
use tauri::async_runtime::JoinHandle;
use tauri::{AppHandle, Emitter, State};

/// 全局单连接状态。用 std Mutex，命令里先取出/克隆再 await，避免跨 await 持锁。
#[derive(Default)]
struct MqttState {
    client: Mutex<Option<AsyncClient>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConnectOpts {
    host: String,
    port: u16,
    client_id: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
    #[serde(default)]
    keep_alive: Option<u64>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct MessageEvent {
    dir: String,
    topic: String,
    payload: String,
    qos: u8,
    retain: bool,
    ts: u64,
}

#[derive(Serialize, Clone)]
struct StatusEvent {
    status: String,
    detail: Option<String>,
}

fn qos_from(n: u8) -> QoS {
    match n {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn emit_status(app: &AppHandle, status: &str, detail: Option<String>) {
    let _ = app.emit(
        "mqtt://status",
        StatusEvent {
            status: status.into(),
            detail,
        },
    );
}

#[tauri::command]
async fn mqtt_connect(
    app: AppHandle,
    state: State<'_, MqttState>,
    opts: ConnectOpts,
) -> Result<(), String> {
    // 先拆掉已有连接
    let (old_client, old_task) = {
        let oc = state.client.lock().unwrap().take();
        let ot = state.task.lock().unwrap().take();
        (oc, ot)
    };
    if let Some(t) = old_task {
        t.abort();
    }
    if let Some(c) = old_client {
        let _ = c.disconnect().await;
    }

    let client_id = if opts.client_id.is_empty() {
        format!("kenko-{}", now_ms())
    } else {
        opts.client_id.clone()
    };
    let mut mo = MqttOptions::new(client_id, opts.host, opts.port);
    mo.set_keep_alive(Duration::from_secs(opts.keep_alive.unwrap_or(60)));
    if !opts.username.is_empty() {
        mo.set_credentials(opts.username, opts.password);
    }

    let (client, mut eventloop) = AsyncClient::new(mo, 32);
    {
        *state.client.lock().unwrap() = Some(client);
    }

    let app2 = app.clone();
    let handle = tauri::async_runtime::spawn(async move {
        emit_status(&app2, "connecting", None);
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => emit_status(&app2, "connected", None),
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let ev = MessageEvent {
                        dir: "rx".into(),
                        topic: p.topic,
                        payload: String::from_utf8_lossy(&p.payload).to_string(),
                        qos: p.qos as u8,
                        retain: p.retain,
                        ts: now_ms(),
                    };
                    let _ = app2.emit("mqtt://message", ev);
                }
                Ok(_) => {}
                Err(e) => {
                    emit_status(&app2, "error", Some(e.to_string()));
                    break;
                }
            }
        }
    });
    {
        *state.task.lock().unwrap() = Some(handle);
    }
    Ok(())
}

#[tauri::command]
async fn mqtt_disconnect(state: State<'_, MqttState>) -> Result<(), String> {
    let client = { state.client.lock().unwrap().take() };
    let task = { state.task.lock().unwrap().take() };
    if let Some(t) = task {
        t.abort();
    }
    if let Some(c) = client {
        let _ = c.disconnect().await;
    }
    Ok(())
}

#[tauri::command]
async fn mqtt_subscribe(state: State<'_, MqttState>, topic: String, qos: u8) -> Result<(), String> {
    let client = { state.client.lock().unwrap().clone() }.ok_or("未连接")?;
    client
        .subscribe(topic, qos_from(qos))
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn mqtt_publish(
    state: State<'_, MqttState>,
    topic: String,
    payload: String,
    qos: u8,
    retain: bool,
) -> Result<(), String> {
    let client = { state.client.lock().unwrap().clone() }.ok_or("未连接")?;
    client
        .publish(topic, qos_from(qos), retain, payload.into_bytes())
        .await
        .map_err(|e| e.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(MqttState::default())
        .invoke_handler(tauri::generate_handler![
            mqtt_connect,
            mqtt_disconnect,
            mqtt_subscribe,
            mqtt_publish
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
