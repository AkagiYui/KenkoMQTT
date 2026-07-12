// 端到端验证 rumqttc 在本机可用：连本地 broker → 订阅 → 发布 → 收到自己发的消息。
// 用法：先在 127.0.0.1:1883 起一个 broker，再 `cargo run --example roundtrip`。
use rumqttc::{Client, Event, MqttOptions, Packet, QoS};
use std::time::Duration;

fn main() {
    let host = std::env::var("MQTT_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("MQTT_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(1883);
    let topic = "kenko/test/roundtrip";

    let mut mo = MqttOptions::new("kenko-roundtrip", host, port);
    mo.set_keep_alive(Duration::from_secs(5));
    let (client, mut conn) = Client::new(mo, 10);

    client.subscribe(topic, QoS::AtLeastOnce).unwrap();
    client
        .publish(topic, QoS::AtLeastOnce, false, b"hello-from-rumqttc".to_vec())
        .unwrap();

    for (i, n) in conn.iter().enumerate() {
        match n {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                println!(
                    "ROUNDTRIP_OK topic={} payload={}",
                    p.topic,
                    String::from_utf8_lossy(&p.payload)
                );
                std::process::exit(0);
            }
            Ok(ev) => println!("ev: {ev:?}"),
            Err(e) => {
                eprintln!("ROUNDTRIP_ERR {e}");
                std::process::exit(2);
            }
        }
        if i > 60 {
            eprintln!("ROUNDTRIP_TIMEOUT");
            std::process::exit(3);
        }
    }
}
