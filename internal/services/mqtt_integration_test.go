package services_test

import (
	"path/filepath"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"kenkomqtt/internal/database"
	"kenkomqtt/internal/services"
)

// TestBrokerClientRoundTrip 端到端验证：
//  1. 内嵌 broker 能启动并接受 TCP 连接；
//  2. 原生 paho 客户端能连接、订阅、发布并收到消息；
//  3. ClientService 能连接到该 broker，其 Publish 能被外部订阅者收到；
//  4. broker 统计能反映在线客户端数。
func TestBrokerClientRoundTrip(t *testing.T) {
	db, err := database.Initialize(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("初始化数据库失败: %v", err)
	}

	broker := services.NewBrokerService(db)
	cfg := services.BrokerConfig{
		TCPEnabled:     true,
		TCPHost:        "127.0.0.1",
		TCPPort:        31883,
		WSEnabled:      false,
		AllowAnonymous: true,
	}
	if err := broker.Start(cfg); err != nil {
		t.Fatalf("启动 broker 失败: %v", err)
	}
	defer broker.Stop()

	if !broker.IsRunning() {
		t.Fatal("broker 应处于运行状态")
	}

	const addr = "tcp://127.0.0.1:31883"

	// 原生 paho 订阅者
	received := make(chan paho.Message, 4)
	subOpts := paho.NewClientOptions().AddBroker(addr).SetClientID("sub-test")
	sub := paho.NewClient(subOpts)
	if tok := sub.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("订阅者连接失败: %v", tok.Error())
	}
	defer sub.Disconnect(100)

	if tok := sub.Subscribe("kenko/test", 1, func(_ paho.Client, m paho.Message) {
		received <- m
	}); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("订阅失败: %v", tok.Error())
	}

	// 通过 ClientService 连接并发布
	client := services.NewClientService()
	connID := "conn-1"
	if err := client.Connect(connID, services.ClientConnectOptions{
		Protocol: "tcp", Host: "127.0.0.1", Port: 31883, ClientID: "pub-test", KeepAlive: 30, CleanSession: true, MQTTVersion: 4,
	}); err != nil {
		t.Fatalf("ClientService 连接失败: %v", err)
	}
	defer client.DisconnectAll()

	if !client.IsConnected(connID) {
		t.Fatal("ClientService 应报告已连接")
	}

	if err := client.Publish(connID, "kenko/test", `{"v":42}`, 1, false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	select {
	case m := <-received:
		if got := string(m.Payload()); got != `{"v":42}` {
			t.Fatalf("消息内容不符: %q", got)
		}
		if m.Topic() != "kenko/test" {
			t.Fatalf("主题不符: %q", m.Topic())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("超时：未收到发布的消息")
	}

	// 校验统计（broker 内部计数有轻微延迟，做短暂等待）
	waitFor(t, 3*time.Second, func() bool {
		return broker.GetStatus().Stats.ClientsConnected >= 2
	}, "在线客户端数应至少为 2")

	// 校验订阅/取消订阅 API 不报错
	if err := client.Subscribe(connID, "kenko/#", 0); err != nil {
		t.Fatalf("ClientService 订阅失败: %v", err)
	}
	if err := client.Unsubscribe(connID, "kenko/#"); err != nil {
		t.Fatalf("ClientService 取消订阅失败: %v", err)
	}
}

// TestBrokerAuth 验证用户名/密码鉴权：错误凭据应被拒绝，正确凭据应通过。
func TestBrokerAuth(t *testing.T) {
	db, err := database.Initialize(filepath.Join(t.TempDir(), "auth.db"))
	if err != nil {
		t.Fatalf("初始化数据库失败: %v", err)
	}
	broker := services.NewBrokerService(db)
	if err := broker.Start(services.BrokerConfig{
		TCPEnabled: true, TCPHost: "127.0.0.1", TCPPort: 31884,
		AllowAnonymous: false, Username: "admin", Password: "secret",
	}); err != nil {
		t.Fatalf("启动 broker 失败: %v", err)
	}
	defer broker.Stop()

	// 错误密码 → 应连接失败
	bad := paho.NewClient(paho.NewClientOptions().AddBroker("tcp://127.0.0.1:31884").
		SetClientID("bad").SetUsername("admin").SetPassword("wrong").SetConnectRetry(false))
	tok := bad.Connect()
	tok.WaitTimeout(5 * time.Second)
	if tok.Error() == nil {
		bad.Disconnect(50)
		t.Fatal("错误凭据不应连接成功")
	}

	// 正确密码 → 应连接成功
	good := paho.NewClient(paho.NewClientOptions().AddBroker("tcp://127.0.0.1:31884").
		SetClientID("good").SetUsername("admin").SetPassword("secret"))
	tok = good.Connect()
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("正确凭据应连接成功: %v", tok.Error())
	}
	good.Disconnect(50)
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(msg)
}
