// 验证 WebSocket 路径：连 ws broker(:8083) → 订阅 → 发布 → 收到。
use rumqttc::{Client, Event, MqttOptions, Packet, QoS, Transport};
use std::time::Duration;

fn main() {
    let mut mo = MqttOptions::new("kenko-ws", "ws://localhost:8083/mqtt", 8083);
    mo.set_transport(Transport::Ws);
    mo.set_keep_alive(Duration::from_secs(5));
    let (client, mut conn) = Client::new(mo, 10);
    client.subscribe("kenko/ws", QoS::AtLeastOnce).unwrap();
    client.publish("kenko/ws", QoS::AtLeastOnce, false, b"ws-ok".to_vec()).unwrap();
    for (i, n) in conn.iter().enumerate() {
        match n {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                println!("WS_ROUNDTRIP_OK payload={}", String::from_utf8_lossy(&p.payload));
                std::process::exit(0);
            }
            Ok(_) => {}
            Err(e) => { eprintln!("WS_ERR {e}"); std::process::exit(2); }
        }
        if i > 60 { eprintln!("WS_TIMEOUT"); std::process::exit(3); }
    }
}
