// 验证 TLS(skip-verify) 路径：连自签名 TLS broker(:8883) → 订阅 → 发布 → 收到。
use kenkomqtt_lib::tls::client_config;
use rumqttc::{Client, Event, MqttOptions, Packet, QoS, TlsConfiguration, Transport};
use std::time::Duration;

fn main() {
    let mut mo = MqttOptions::new("kenko-tls", "localhost", 8883);
    mo.set_transport(Transport::Tls(TlsConfiguration::Rustls(client_config(true, ""))));
    mo.set_keep_alive(Duration::from_secs(5));
    let (client, mut conn) = Client::new(mo, 10);
    client.subscribe("kenko/tls", QoS::AtLeastOnce).unwrap();
    client
        .publish("kenko/tls", QoS::AtLeastOnce, false, b"tls-ok".to_vec())
        .unwrap();
    for (i, n) in conn.iter().enumerate() {
        match n {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                println!("TLS_ROUNDTRIP_OK payload={}", String::from_utf8_lossy(&p.payload));
                std::process::exit(0);
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("TLS_ERR {e}");
                std::process::exit(2);
            }
        }
        if i > 60 {
            eprintln!("TLS_TIMEOUT");
            std::process::exit(3);
        }
    }
}
