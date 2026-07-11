//! 平台信息 + Android 电池/后台权限检测与跳转设置。
//! 非 Android 平台返回 applicable=false 的占位值。
use serde::Serialize;

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlatformInfo {
    pub os: String,
    pub is_android: bool,
}

#[derive(Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct AndroidPerms {
    /// 是否为 Android（其它平台为 false，前端据此隐藏提示）。
    pub applicable: bool,
    /// 是否成功读取到状态（JNI 失败时为 false，前端可给更弱的提示）。
    pub known: bool,
    /// 是否已被加入电池优化白名单（无限制后台）。
    pub ignoring_battery_optimizations: bool,
}

#[tauri::command]
pub fn platform_info() -> PlatformInfo {
    let os = std::env::consts::OS.to_string();
    PlatformInfo {
        is_android: os == "android",
        os,
    }
}

#[cfg(target_os = "android")]
mod imp {
    use super::AndroidPerms;
    use jni::objects::{JObject, JValue};

    fn with_env<T>(f: impl FnOnce(&mut jni::JNIEnv, &JObject) -> jni::errors::Result<T>) -> Option<T> {
        let ctx = ndk_context::android_context();
        let vm = unsafe { jni::JavaVM::from_raw(ctx.vm().cast()) }.ok()?;
        let mut env = vm.attach_current_thread().ok()?;
        let context = unsafe { JObject::from_raw(ctx.context().cast()) };
        f(&mut env, &context).ok()
    }

    pub fn check() -> AndroidPerms {
        let result = with_env(|env, context| {
            // pm = context.getSystemService("power")
            let svc = env.new_string("power")?;
            let pm = env
                .call_method(
                    context,
                    "getSystemService",
                    "(Ljava/lang/String;)Ljava/lang/Object;",
                    &[JValue::Object(&svc)],
                )?
                .l()?;
            // pkg = context.getPackageName()
            let pkg = env
                .call_method(context, "getPackageName", "()Ljava/lang/String;", &[])?
                .l()?;
            // ignoring = pm.isIgnoringBatteryOptimizations(pkg)
            let ignoring = env
                .call_method(
                    &pm,
                    "isIgnoringBatteryOptimizations",
                    "(Ljava/lang/String;)Z",
                    &[JValue::Object(&pkg)],
                )?
                .z()?;
            Ok::<bool, jni::errors::Error>(ignoring)
        });
        match result {
            Some(ignoring) => AndroidPerms {
                applicable: true,
                known: true,
                ignoring_battery_optimizations: ignoring,
            },
            None => AndroidPerms {
                applicable: true,
                known: false,
                ignoring_battery_optimizations: false,
            },
        }
    }

    /// 打开系统设置页：kind = "battery" 请求忽略电池优化；否则打开应用详情页。
    pub fn open_settings(kind: &str) -> Result<(), String> {
        with_env(|env, context| {
            let pkg = env
                .call_method(context, "getPackageName", "()Ljava/lang/String;", &[])?
                .l()?;
            let pkg_str: String = env.get_string((&pkg).into())?.into();
            let uri_str = env.new_string(format!("package:{pkg_str}"))?;
            // Uri.parse("package:...")
            let uri = env
                .call_static_method(
                    "android/net/Uri",
                    "parse",
                    "(Ljava/lang/String;)Landroid/net/Uri;",
                    &[JValue::Object(&uri_str)],
                )?
                .l()?;
            let action = if kind == "battery" {
                "android.settings.REQUEST_IGNORE_BATTERY_OPTIMIZATIONS"
            } else {
                "android.settings.APPLICATION_DETAILS_SETTINGS"
            };
            let action_str = env.new_string(action)?;
            // intent = new Intent(action, uri)
            let intent = env.new_object(
                "android/content/Intent",
                "(Ljava/lang/String;Landroid/net/Uri;)V",
                &[JValue::Object(&action_str), JValue::Object(&uri)],
            )?;
            // intent.addFlags(FLAG_ACTIVITY_NEW_TASK = 0x10000000)
            env.call_method(
                &intent,
                "addFlags",
                "(I)Landroid/content/Intent;",
                &[JValue::Int(0x1000_0000)],
            )?;
            // context.startActivity(intent)
            env.call_method(
                context,
                "startActivity",
                "(Landroid/content/Intent;)V",
                &[JValue::Object(&intent)],
            )?;
            Ok(())
        })
        .ok_or_else(|| "无法打开系统设置".to_string())
    }
}

#[tauri::command]
pub fn check_android_permissions() -> AndroidPerms {
    #[cfg(target_os = "android")]
    {
        imp::check()
    }
    #[cfg(not(target_os = "android"))]
    {
        AndroidPerms::default()
    }
}

#[tauri::command]
pub fn open_android_settings(_kind: String) -> Result<(), String> {
    #[cfg(target_os = "android")]
    {
        imp::open_settings(&_kind)
    }
    #[cfg(not(target_os = "android"))]
    {
        Err("仅 Android 可用".into())
    }
}
