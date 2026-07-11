mod model;
mod mqtt;
mod store;
pub mod tls;

use model::Profile;
use mqtt::Manager;
use tauri::{AppHandle, State};

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

#[tauri::command]
async fn mqtt_publish(
    mgr: State<'_, Manager>,
    conn_id: String,
    topic: String,
    payload: String,
    qos: u8,
    retain: bool,
) -> Result<(), String> {
    mqtt::publish(mgr, conn_id, topic, payload, qos, retain).await
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(Manager::default())
        .invoke_handler(tauri::generate_handler![
            list_profiles,
            save_profile,
            delete_profile,
            mqtt_connect,
            mqtt_disconnect,
            mqtt_subscribe,
            mqtt_unsubscribe,
            mqtt_publish
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
