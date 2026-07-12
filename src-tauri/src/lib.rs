mod android;
mod broker;
mod codec;
mod model;
mod mqtt;
mod profiles_io;
mod store;
pub mod tls;

use android::{check_android_permissions, open_android_settings, platform_info};
use broker::{BrokerConfig, BrokerState};
use model::Profile;
use mqtt::Manager;
use tauri::{AppHandle, Manager as _, State};

// ---- 连接档案（JSON 持久化）----

#[tauri::command]
fn list_profiles(app: AppHandle) -> Vec<Profile> {
    store::load(&app)
}

#[tauri::command]
fn save_profile(app: AppHandle, profile: Profile) -> Result<Profile, String> {
    let mut list = store::load(&app);
    match list.iter_mut().find(|p| p.id == profile.id) {
        Some(existing) => *existing = profile.clone(),
        None => list.push(profile.clone()),
    }
    store::save_all(&app, &list)?;
    Ok(profile)
}

#[tauri::command]
fn delete_profile(app: AppHandle, id: String) -> Result<(), String> {
    let mut list = store::load(&app);
    list.retain(|p| p.id != id);
    store::save_all(&app, &list)
}

// ---- 连接档案 导入/导出（JSON / YAML / XML / CSV / XLSX）----

#[tauri::command]
fn export_profiles(app: AppHandle, format: String) -> Result<profiles_io::ExportOut, String> {
    let list = store::load(&app);
    profiles_io::export(&list, &format)
}

/// data_base64：前端把文件内容以 base64 传入（兼容二进制的 XLSX）。返回导入条数。
#[tauri::command]
fn import_profiles(app: AppHandle, format: String, data_base64: String) -> Result<usize, String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data_base64.trim())
        .map_err(|e| format!("base64 解码失败: {e}"))?;
    let imported = profiles_io::import(&bytes, &format)?;
    let mut list = store::load(&app);
    let mut n = 0;
    for mut p in imported {
        // id 冲突则重新生成，避免覆盖既有连接。
        if list.iter().any(|e| e.id == p.id) {
            p.id = format!("{}-{}", p.id, mqtt::now_ms());
        }
        list.push(p);
        n += 1;
    }
    store::save_all(&app, &list)?;
    Ok(n)
}

// ---- MQTT ----

#[tauri::command]
fn mqtt_connect(app: AppHandle, mgr: State<'_, Manager>, profile: Profile) -> Result<(), String> {
    mqtt::connect(app, &mgr, profile)
}

#[tauri::command]
async fn mqtt_disconnect(mgr: State<'_, Manager>, conn_id: String) -> Result<(), String> {
    mqtt::disconnect(mgr, conn_id).await
}

#[tauri::command]
async fn mqtt_subscribe(mgr: State<'_, Manager>, conn_id: String, topic: String, qos: u8) -> Result<(), String> {
    mqtt::subscribe(mgr, conn_id, topic, qos).await
}

#[tauri::command]
async fn mqtt_unsubscribe(mgr: State<'_, Manager>, conn_id: String, topic: String) -> Result<(), String> {
    mqtt::unsubscribe(mgr, conn_id, topic).await
}

#[allow(clippy::too_many_arguments)]
#[tauri::command]
async fn mqtt_publish(
    app: AppHandle,
    mgr: State<'_, Manager>,
    conn_id: String,
    topic: String,
    payload: String,
    qos: u8,
    retain: bool,
    format: Option<codec::Format>,
    expand: Option<bool>,
) -> Result<(), String> {
    mqtt::publish(app, mgr, conn_id, topic, payload, qos, retain, format, expand).await
}

// ---- 消息库 / 计算（后端） ----

#[tauri::command]
fn messages_query(mgr: State<'_, Manager>, conn_id: String, opts: mqtt::QueryOpts) -> mqtt::MsgPage {
    mgr.query(&conn_id, &opts)
}

#[tauri::command]
fn messages_clear(mgr: State<'_, Manager>, conn_id: String) {
    mgr.clear_msgs(&conn_id)
}

#[tauri::command]
fn messages_clear_topic(mgr: State<'_, Manager>, conn_id: String, topic_filter: String) {
    mgr.clear_topic(&conn_id, &topic_filter)
}

#[tauri::command]
fn topic_tree(mgr: State<'_, Manager>, conn_id: String, format: codec::Format) -> Vec<mqtt::TreeNode> {
    mgr.topic_tree(&conn_id, format)
}

#[tauri::command]
fn chart_rate(mgr: State<'_, Manager>, conn_id: String, bucket_ms: u64, buckets: usize) -> Vec<mqtt::RatePoint> {
    mgr.chart_rate(&conn_id, bucket_ms, buckets)
}

#[tauri::command]
fn chart_traffic(mgr: State<'_, Manager>, conn_id: String, bucket_ms: u64, buckets: usize) -> Vec<mqtt::TrafficPoint> {
    mgr.chart_traffic(&conn_id, bucket_ms, buckets)
}

#[tauri::command]
fn chart_load(mgr: State<'_, Manager>, conn_id: String, topic_filter: String, method: String, bucket_ms: u64, buckets: usize) -> Vec<mqtt::ContentPoint> {
    mgr.chart_load(&conn_id, &topic_filter, &method, bucket_ms, buckets)
}

#[tauri::command]
fn chart_content(mgr: State<'_, Manager>, conn_id: String, topic_filter: String, jsonpath: String, limit: Option<usize>) -> Result<Vec<mqtt::ContentPoint>, String> {
    mgr.chart_content(&conn_id, topic_filter, jsonpath, limit.unwrap_or(200))
}

/// kind: "csv" | "json" | "txt"
#[tauri::command]
fn export_messages(mgr: State<'_, Manager>, conn_id: String, kind: String, format: codec::Format) -> String {
    mgr.export_messages(&conn_id, &kind, format)
}

#[allow(clippy::too_many_arguments)]
#[tauri::command]
fn schedule_start(
    app: AppHandle,
    mgr: State<'_, Manager>,
    conn_id: String,
    topic: String,
    payload: String,
    qos: u8,
    retain: bool,
    format: Option<codec::Format>,
    interval_ms: u64,
) -> Result<String, String> {
    mqtt::schedule_start(app, mgr, conn_id, topic, payload, qos, retain, format, interval_ms)
}

#[tauri::command]
fn schedule_stop(mgr: State<'_, Manager>, id: String) {
    mqtt::schedule_stop(mgr, id)
}

// ---- 内嵌 Broker ----

#[tauri::command]
async fn broker_start(
    app: AppHandle,
    state: State<'_, BrokerState>,
    config: BrokerConfig,
) -> Result<(), String> {
    let _ = store::save_broker(&app, &config);
    broker::start(app.clone(), &state, config).await
}

#[tauri::command]
fn broker_stop(app: AppHandle, state: State<'_, BrokerState>) {
    broker::stop(&app, &state)
}

#[tauri::command]
fn broker_status(state: State<'_, BrokerState>) -> bool {
    broker::is_running(&state)
}

#[tauri::command]
fn broker_get_config(app: AppHandle, state: State<'_, BrokerState>) -> BrokerConfig {
    broker::current_config(&state).unwrap_or_else(|| store::load_broker(&app))
}

#[tauri::command]
fn broker_retained(state: State<'_, BrokerState>) -> Vec<broker::RetainedRow> {
    broker::retained(&state)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(Manager::default())
        .manage(BrokerState::default())
        .setup(|app| {
            // 消息落盘：注入 messages 目录并载入历史，随后每 3 秒把脏连接写盘。
            let handle = app.handle().clone();
            if let Ok(dir) = handle.path().app_data_dir() {
                app.state::<Manager>().init_persistence(dir.join("messages"));
            }
            tauri::async_runtime::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    handle.state::<Manager>().flush();
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            list_profiles,
            save_profile,
            delete_profile,
            export_profiles,
            import_profiles,
            mqtt_connect,
            mqtt_disconnect,
            mqtt_subscribe,
            mqtt_unsubscribe,
            mqtt_publish,
            messages_query,
            messages_clear,
            messages_clear_topic,
            topic_tree,
            chart_rate,
            chart_traffic,
            chart_load,
            chart_content,
            export_messages,
            schedule_start,
            schedule_stop,
            broker_start,
            broker_stop,
            broker_status,
            broker_get_config,
            broker_retained,
            platform_info,
            check_android_permissions,
            open_android_settings
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
