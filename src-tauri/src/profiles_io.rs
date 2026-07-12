//! 连接档案导入/导出：JSON / YAML / XML 为全量(含遗嘱、订阅等嵌套)，CSV / XLSX 为核心字段扁平表。
//! 文本格式返回原文；二进制(XLSX)以 base64 返回，前端解码后下载。
use crate::model::Profile;
use base64::Engine;
use serde::Serialize;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportOut {
    pub filename: String,
    /// content 是否为 base64（XLSX 为 true）。
    pub base64: bool,
    pub content: String,
}

/// 供 XML 序列化用的根节点（quick-xml 需要具名根）。
#[derive(Serialize, serde::Deserialize)]
struct ProfilesXml {
    #[serde(rename = "profile", default)]
    profile: Vec<Profile>,
}

/// CSV / XLSX 的扁平列。
const FLAT_HEADERS: &[&str] = &[
    "id", "name", "protocol", "host", "port", "path", "clientId", "username", "password", "keepAlive", "cleanSession",
    "mqttVersion", "tlsSkipVerify", "willEnabled", "willTopic", "willPayload", "willQos", "willRetain",
];

fn flat_row(p: &Profile) -> Vec<String> {
    vec![
        p.id.clone(),
        p.name.clone(),
        p.protocol.clone(),
        p.host.clone(),
        p.port.to_string(),
        p.path.clone(),
        p.client_id.clone(),
        p.username.clone(),
        p.password.clone(),
        p.keep_alive.to_string(),
        p.clean_session.to_string(),
        p.mqtt_version.to_string(),
        p.tls_skip_verify.to_string(),
        p.will.enabled.to_string(),
        p.will.topic.clone(),
        p.will.payload.clone(),
        p.will.qos.to_string(),
        p.will.retain.to_string(),
    ]
}

fn get<'a>(map: &'a std::collections::HashMap<String, String>, k: &str) -> &'a str {
    map.get(k).map(|s| s.as_str()).unwrap_or("")
}

/// 从扁平字段构造 Profile。缺失字段用默认值。
fn profile_from_flat(map: &std::collections::HashMap<String, String>) -> Profile {
    let mut p = Profile {
        id: get(map, "id").to_string(),
        name: get(map, "name").to_string(),
        protocol: get(map, "protocol").to_string(),
        host: get(map, "host").to_string(),
        port: get(map, "port").parse().unwrap_or(1883),
        path: get(map, "path").to_string(),
        client_id: get(map, "clientId").to_string(),
        username: get(map, "username").to_string(),
        password: get(map, "password").to_string(),
        keep_alive: get(map, "keepAlive").parse().unwrap_or(60),
        clean_session: get(map, "cleanSession").parse().unwrap_or(true),
        mqtt_version: get(map, "mqttVersion").parse().unwrap_or(4),
        tls_skip_verify: get(map, "tlsSkipVerify").parse().unwrap_or(false),
        ..Default::default()
    };
    if p.protocol.is_empty() {
        p.protocol = "tcp".into();
    }
    if p.id.is_empty() {
        p.id = format!("imported-{}-{}", fastrand::u32(..), p.name);
    }
    p.will.enabled = get(map, "willEnabled").parse().unwrap_or(false);
    p.will.topic = get(map, "willTopic").to_string();
    p.will.payload = get(map, "willPayload").to_string();
    p.will.qos = get(map, "willQos").parse().unwrap_or(0);
    p.will.retain = get(map, "willRetain").parse().unwrap_or(false);
    p
}

fn csv_escape(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// 拆分一行 CSV（支持双引号转义）。
fn csv_split_line(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quote {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_quote = false;
                }
            } else {
                cur.push(c);
            }
        } else if c == '"' {
            in_quote = true;
        } else if c == ',' {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    out.push(cur);
    out
}

fn to_csv(profiles: &[Profile]) -> String {
    let mut s = FLAT_HEADERS.join(",");
    s.push('\n');
    for p in profiles {
        let row: Vec<String> = flat_row(p).iter().map(|c| csv_escape(c)).collect();
        s.push_str(&row.join(","));
        s.push('\n');
    }
    s
}

fn from_csv(text: &str) -> Result<Vec<Profile>, String> {
    let mut lines = text.lines();
    let header = lines.next().ok_or("CSV 为空")?;
    let cols = csv_split_line(header);
    let mut out = Vec::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let vals = csv_split_line(line);
        let map: std::collections::HashMap<String, String> =
            cols.iter().cloned().zip(vals.into_iter()).collect();
        out.push(profile_from_flat(&map));
    }
    Ok(out)
}

fn to_xlsx(profiles: &[Profile]) -> Result<String, String> {
    use rust_xlsxwriter::{Format, Workbook};
    let mut wb = Workbook::new();
    let ws = wb.add_worksheet();
    let bold = Format::new().set_bold();
    for (c, h) in FLAT_HEADERS.iter().enumerate() {
        ws.write_string_with_format(0, c as u16, *h, &bold).map_err(|e| e.to_string())?;
    }
    for (r, p) in profiles.iter().enumerate() {
        for (c, val) in flat_row(p).iter().enumerate() {
            ws.write_string((r + 1) as u32, c as u16, val).map_err(|e| e.to_string())?;
        }
    }
    let buf = wb.save_to_buffer().map_err(|e| e.to_string())?;
    Ok(base64::engine::general_purpose::STANDARD.encode(buf))
}

fn from_xlsx(bytes: &[u8]) -> Result<Vec<Profile>, String> {
    use calamine::{Data, Reader, Xlsx};
    let cursor = std::io::Cursor::new(bytes.to_vec());
    let mut wb: Xlsx<std::io::Cursor<Vec<u8>>> =
        calamine::open_workbook_from_rs(cursor).map_err(|e: calamine::XlsxError| e.to_string())?;
    let name = wb.sheet_names().first().cloned().ok_or("XLSX 无工作表")?;
    let range = wb.worksheet_range(&name).map_err(|e| e.to_string())?;
    let mut rows = range.rows();
    let header: Vec<String> = rows
        .next()
        .ok_or("XLSX 为空")?
        .iter()
        .map(|c| c.to_string())
        .collect();
    let mut out = Vec::new();
    for row in rows {
        let map: std::collections::HashMap<String, String> = header
            .iter()
            .cloned()
            .zip(row.iter().map(|c| match c {
                Data::Empty => String::new(),
                other => other.to_string(),
            }))
            .collect();
        if map.values().all(|v| v.is_empty()) {
            continue;
        }
        out.push(profile_from_flat(&map));
    }
    Ok(out)
}

/// 导出为指定格式：json / yaml / xml / csv / xlsx。
pub fn export(profiles: &[Profile], format: &str) -> Result<ExportOut, String> {
    let ts = crate::mqtt::now_ms();
    let name = |ext: &str| format!("kenkomqtt-connections-{ts}.{ext}");
    match format {
        "json" => Ok(ExportOut {
            filename: name("json"),
            base64: false,
            content: serde_json::to_string_pretty(profiles).map_err(|e| e.to_string())?,
        }),
        "yaml" => Ok(ExportOut {
            filename: name("yaml"),
            base64: false,
            content: serde_yaml_ng::to_string(profiles).map_err(|e| e.to_string())?,
        }),
        "xml" => {
            let root = ProfilesXml { profile: profiles.to_vec() };
            let body = quick_xml::se::to_string_with_root("profiles", &root).map_err(|e| e.to_string())?;
            Ok(ExportOut {
                filename: name("xml"),
                base64: false,
                content: format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{body}"),
            })
        }
        "csv" => Ok(ExportOut { filename: name("csv"), base64: false, content: to_csv(profiles) }),
        "xlsx" => Ok(ExportOut { filename: name("xlsx"), base64: true, content: to_xlsx(profiles)? }),
        other => Err(format!("未知导出格式: {other}")),
    }
}

/// 从字节导入。format 未知时按扩展名/内容猜测由前端决定，这里以显式 format 为准。
pub fn import(bytes: &[u8], format: &str) -> Result<Vec<Profile>, String> {
    match format {
        "json" => serde_json::from_slice(bytes).map_err(|e| e.to_string()),
        "yaml" => {
            let text = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
            serde_yaml_ng::from_str(text).map_err(|e| e.to_string())
        }
        "xml" => {
            let text = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
            let root: ProfilesXml = quick_xml::de::from_str(text).map_err(|e| e.to_string())?;
            Ok(root.profile)
        }
        "csv" => {
            let text = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
            from_csv(text)
        }
        "xlsx" => from_xlsx(bytes),
        other => Err(format!("未知导入格式: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<Profile> {
        let mut a = Profile { id: "a".into(), name: "本地".into(), protocol: "tcp".into(), host: "127.0.0.1".into(), port: 1883, ..Default::default() };
        a.will.enabled = true;
        a.will.topic = "off/line".into();
        a.will.payload = "bye,\"q\"".into();
        let b = Profile { id: "b".into(), name: "云".into(), protocol: "wss".into(), host: "broker".into(), port: 8084, ..Default::default() };
        vec![a, b]
    }

    #[test]
    fn json_roundtrip() {
        let ps = sample();
        let out = export(&ps, "json").unwrap();
        let back = import(out.content.as_bytes(), "json").unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].name, "本地");
    }

    #[test]
    fn yaml_roundtrip() {
        let ps = sample();
        let out = export(&ps, "yaml").unwrap();
        let back = import(out.content.as_bytes(), "yaml").unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[1].protocol, "wss");
    }

    #[test]
    fn xml_roundtrip() {
        let ps = sample();
        let out = export(&ps, "xml").unwrap();
        let back = import(out.content.as_bytes(), "xml").unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].port, 1883);
    }

    #[test]
    fn csv_roundtrip_with_escaping() {
        let ps = sample();
        let out = export(&ps, "csv").unwrap();
        let back = import(out.content.as_bytes(), "csv").unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].will.payload, "bye,\"q\"");
        assert!(back[0].will.enabled);
    }

    #[test]
    fn xlsx_roundtrip() {
        let ps = sample();
        let out = export(&ps, "xlsx").unwrap();
        assert!(out.base64);
        let bytes = base64::engine::general_purpose::STANDARD.decode(out.content).unwrap();
        let back = import(&bytes, "xlsx").unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].host, "127.0.0.1");
    }
}
