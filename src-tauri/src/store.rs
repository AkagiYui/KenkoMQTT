//! 连接档案的 JSON 持久化（存于应用数据目录，桌面与 Android 通用）。
use crate::model::Profile;
use std::path::PathBuf;
use tauri::{AppHandle, Manager};

fn profiles_path(app: &AppHandle) -> Result<PathBuf, String> {
    let dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    Ok(dir.join("profiles.json"))
}

pub fn load(app: &AppHandle) -> Vec<Profile> {
    let read = || -> Option<Vec<Profile>> {
        let p = profiles_path(app).ok()?;
        let data = std::fs::read(&p).ok()?;
        serde_json::from_slice(&data).ok()
    };
    let mut list = read().unwrap_or_default();
    list.sort_by(|a, b| a.sort_order.cmp(&b.sort_order).then(a.name.cmp(&b.name)));
    list
}

pub fn save_all(app: &AppHandle, profiles: &[Profile]) -> Result<(), String> {
    let p = profiles_path(app)?;
    let data = serde_json::to_vec_pretty(profiles).map_err(|e| e.to_string())?;
    std::fs::write(&p, data).map_err(|e| e.to_string())
}
