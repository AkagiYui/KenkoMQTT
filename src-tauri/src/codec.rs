//! 载荷编解码：在后端完成 明文/JSON/Hex/Base64/MessagePack/CBOR 之间的转换。
//! decode 用于展示（bytes → 可读文本），encode 用于发布（文本 → bytes）。
use base64::Engine;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    #[serde(alias = "plaintext", alias = "text")]
    Plaintext,
    Json,
    Hex,
    Base64,
    #[serde(alias = "msgpack")]
    Msgpack,
    Cbor,
}

fn pretty_json(v: &serde_json::Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

/// bytes → 展示文本。
pub fn decode(bytes: &[u8], fmt: Format) -> String {
    match fmt {
        Format::Plaintext => String::from_utf8_lossy(bytes).to_string(),
        Format::Json => match serde_json::from_slice::<serde_json::Value>(bytes) {
            Ok(v) => pretty_json(&v),
            Err(_) => String::from_utf8_lossy(bytes).to_string(),
        },
        Format::Hex => hex::encode(bytes),
        Format::Base64 => base64::engine::general_purpose::STANDARD.encode(bytes),
        Format::Msgpack => match rmp_serde::from_slice::<serde_json::Value>(bytes) {
            Ok(v) => pretty_json(&v),
            Err(e) => format!("<msgpack 解码失败: {e}>"),
        },
        Format::Cbor => match ciborium::from_reader::<serde_json::Value, _>(bytes) {
            Ok(v) => pretty_json(&v),
            Err(e) => format!("<cbor 解码失败: {e}>"),
        },
    }
}

/// 展示文本 → bytes（发布用）。
pub fn encode(text: &str, fmt: Format) -> Result<Vec<u8>, String> {
    match fmt {
        Format::Plaintext | Format::Json => Ok(text.as_bytes().to_vec()),
        Format::Hex => hex::decode(text.trim().replace([' ', '\n', '\r'], "")).map_err(|e| e.to_string()),
        Format::Base64 => base64::engine::general_purpose::STANDARD
            .decode(text.trim())
            .map_err(|e| e.to_string()),
        Format::Msgpack => {
            let v: serde_json::Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
            rmp_serde::to_vec(&v).map_err(|e| e.to_string())
        }
        Format::Cbor => {
            let v: serde_json::Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
            let mut buf = Vec::new();
            ciborium::into_writer(&v, &mut buf).map_err(|e| e.to_string())?;
            Ok(buf)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips() {
        // hex
        let b = encode("48656c6c6f", Format::Hex).unwrap();
        assert_eq!(b, b"Hello");
        assert_eq!(decode(b"Hello", Format::Hex), "48656c6c6f");
        // base64
        let b = encode("SGk=", Format::Base64).unwrap();
        assert_eq!(b, b"Hi");
        // msgpack round-trip via json
        let mp = encode(r#"{"a":1}"#, Format::Msgpack).unwrap();
        assert!(decode(&mp, Format::Msgpack).contains("\"a\""));
        // cbor round-trip via json
        let cb = encode(r#"[1,2,3]"#, Format::Cbor).unwrap();
        assert!(decode(&cb, Format::Cbor).contains('1'));
        // json pretty
        assert!(decode(br#"{"x":1}"#, Format::Json).contains("\"x\": 1"));
    }
}
