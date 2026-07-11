package services

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// Wails 事件名
const (
	ClientMessageName = "client:message" // 收到 / 发出的一条消息
	ClientStatusName  = "client:status"  // 连接状态变化
)

// ClientConnectOptions 描述一次客户端连接所需的参数。
type ClientConnectOptions struct {
	Protocol      string `json:"protocol"` // tcp | tls | ws | wss
	Host          string `json:"host"`
	Port          int    `json:"port"`
	Path          string `json:"path"`
	ClientID      string `json:"clientId"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	KeepAlive     int    `json:"keepAlive"`
	CleanSession  bool   `json:"cleanSession"`
	MQTTVersion   int    `json:"mqttVersion"` // 3 = 3.1, 4 = 3.1.1
	TLSSkipVerify bool   `json:"tlsSkipVerify"`
}

// ClientMessage 是推送给前端的一条 MQTT 消息。
type ClientMessage struct {
	ConnID    string `json:"connId"`
	Direction string `json:"direction"` // received | published
	Topic     string `json:"topic"`
	Payload   string `json:"payload"`
	QoS       byte   `json:"qos"`
	Retain    bool   `json:"retain"`
	Timestamp int64  `json:"timestamp"`
}

// ClientStatusEvent 是连接状态变化事件。
type ClientStatusEvent struct {
	ConnID    string `json:"connId"`
	Status    string `json:"status"` // connecting | connected | disconnected | error | reconnecting
	Detail    string `json:"detail"`
	Timestamp int64  `json:"timestamp"`
}

// ClientService 管理多个 paho MQTT 客户端连接。
type ClientService struct {
	mu    sync.Mutex
	conns map[string]paho.Client
}

// NewClientService 创建客户端服务实例。
func NewClientService() *ClientService {
	return &ClientService{conns: map[string]paho.Client{}}
}

func schemeFor(protocol string) string {
	switch protocol {
	case "tls", "ssl", "mqtts":
		return "ssl"
	case "ws":
		return "ws"
	case "wss":
		return "wss"
	default:
		return "tcp"
	}
}

// Connect 建立一个客户端连接。若同 connID 已存在连接，会先断开。
func (s *ClientService) Connect(connID string, opts ClientConnectOptions) error {
	s.Disconnect(connID)

	scheme := schemeFor(opts.Protocol)
	broker := fmt.Sprintf("%s://%s:%d", scheme, opts.Host, opts.Port)
	if (scheme == "ws" || scheme == "wss") && opts.Path != "" {
		broker += opts.Path
	}

	pahoOpts := paho.NewClientOptions()
	pahoOpts.AddBroker(broker)
	if opts.ClientID != "" {
		pahoOpts.SetClientID(opts.ClientID)
	} else {
		pahoOpts.SetClientID(fmt.Sprintf("kenkomqtt-%d", time.Now().UnixNano()))
	}
	if opts.Username != "" {
		pahoOpts.SetUsername(opts.Username)
	}
	if opts.Password != "" {
		pahoOpts.SetPassword(opts.Password)
	}
	keepAlive := opts.KeepAlive
	if keepAlive <= 0 {
		keepAlive = 60
	}
	pahoOpts.SetKeepAlive(time.Duration(keepAlive) * time.Second)
	pahoOpts.SetCleanSession(opts.CleanSession)
	pahoOpts.SetConnectTimeout(10 * time.Second)
	pahoOpts.SetAutoReconnect(true)
	if opts.MQTTVersion == 3 {
		pahoOpts.SetProtocolVersion(3)
	} else {
		pahoOpts.SetProtocolVersion(4)
	}
	if scheme == "ssl" || scheme == "wss" {
		pahoOpts.SetTLSConfig(&tls.Config{InsecureSkipVerify: opts.TLSSkipVerify})
	}

	pahoOpts.SetDefaultPublishHandler(func(_ paho.Client, msg paho.Message) {
		emitEvent(ClientMessageName, ClientMessage{
			ConnID:    connID,
			Direction: "received",
			Topic:     msg.Topic(),
			Payload:   string(msg.Payload()),
			QoS:       msg.Qos(),
			Retain:    msg.Retained(),
			Timestamp: nowMillis(),
		})
	})
	pahoOpts.SetOnConnectHandler(func(_ paho.Client) {
		s.emitStatus(connID, "connected", "")
	})
	pahoOpts.SetConnectionLostHandler(func(_ paho.Client, err error) {
		detail := ""
		if err != nil {
			detail = err.Error()
		}
		s.emitStatus(connID, "reconnecting", detail)
	})

	s.emitStatus(connID, "connecting", broker)

	client := paho.NewClient(pahoOpts)
	token := client.Connect()
	if !token.WaitTimeout(12 * time.Second) {
		s.emitStatus(connID, "error", "连接超时")
		return fmt.Errorf("连接超时")
	}
	if err := token.Error(); err != nil {
		s.emitStatus(connID, "error", err.Error())
		return fmt.Errorf("连接失败: %w", err)
	}

	s.mu.Lock()
	s.conns[connID] = client
	s.mu.Unlock()
	slog.Info("MQTT 客户端已连接", "connId", connID, "broker", broker)
	return nil
}

// Disconnect 断开并移除指定连接。
func (s *ClientService) Disconnect(connID string) error {
	s.mu.Lock()
	client := s.conns[connID]
	delete(s.conns, connID)
	s.mu.Unlock()
	if client != nil && client.IsConnectionOpen() {
		client.Disconnect(250)
	}
	if client != nil {
		s.emitStatus(connID, "disconnected", "")
	}
	return nil
}

// Subscribe 订阅一个主题。
func (s *ClientService) Subscribe(connID, topic string, qos byte) error {
	client, err := s.get(connID)
	if err != nil {
		return err
	}
	token := client.Subscribe(topic, qos, nil)
	token.WaitTimeout(10 * time.Second)
	return token.Error()
}

// Unsubscribe 取消订阅一个主题。
func (s *ClientService) Unsubscribe(connID, topic string) error {
	client, err := s.get(connID)
	if err != nil {
		return err
	}
	token := client.Unsubscribe(topic)
	token.WaitTimeout(10 * time.Second)
	return token.Error()
}

// Publish 发布一条消息。
func (s *ClientService) Publish(connID, topic, payload string, qos byte, retain bool) error {
	client, err := s.get(connID)
	if err != nil {
		return err
	}
	token := client.Publish(topic, qos, retain, payload)
	token.WaitTimeout(10 * time.Second)
	if token.Error() != nil {
		return token.Error()
	}
	emitEvent(ClientMessageName, ClientMessage{
		ConnID:    connID,
		Direction: "published",
		Topic:     topic,
		Payload:   payload,
		QoS:       qos,
		Retain:    retain,
		Timestamp: nowMillis(),
	})
	return nil
}

// IsConnected 返回指定连接是否处于活动状态。
func (s *ClientService) IsConnected(connID string) bool {
	s.mu.Lock()
	client := s.conns[connID]
	s.mu.Unlock()
	return client != nil && client.IsConnectionOpen()
}

// DisconnectAll 断开所有客户端连接（应用退出时调用）。
func (s *ClientService) DisconnectAll() {
	s.mu.Lock()
	conns := s.conns
	s.conns = map[string]paho.Client{}
	s.mu.Unlock()
	for _, c := range conns {
		if c != nil && c.IsConnectionOpen() {
			c.Disconnect(250)
		}
	}
}

func (s *ClientService) get(connID string) (paho.Client, error) {
	s.mu.Lock()
	client := s.conns[connID]
	s.mu.Unlock()
	if client == nil {
		return nil, fmt.Errorf("连接不存在: %s", connID)
	}
	return client, nil
}

func (s *ClientService) emitStatus(connID, status, detail string) {
	emitEvent(ClientStatusName, ClientStatusEvent{
		ConnID:    connID,
		Status:    status,
		Detail:    detail,
		Timestamp: nowMillis(),
	})
}
