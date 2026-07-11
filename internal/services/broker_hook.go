package services

import (
	"bytes"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// brokerHook 是挂接到 mochi broker 的自定义钩子：
// 负责连接鉴权，并把连接 / 断开 / 订阅 / 发布等事件转发给 BrokerService。
type brokerHook struct {
	mqtt.HookBase
	allowAnonymous bool
	username       []byte
	password       []byte
	emit           func(BrokerEvent)
}

// ID 返回钩子唯一标识。
func (h *brokerHook) ID() string { return "kenko-broker-hook" }

// Provides 声明本钩子实现了哪些回调。
func (h *brokerHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnConnectAuthenticate,
		mqtt.OnACLCheck,
		mqtt.OnSessionEstablished,
		mqtt.OnDisconnect,
		mqtt.OnSubscribed,
		mqtt.OnUnsubscribed,
		mqtt.OnPublished,
	}, []byte{b})
}

// OnConnectAuthenticate 按配置校验用户名 / 密码。
func (h *brokerHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	if h.allowAnonymous {
		return true
	}
	return bytes.Equal(pk.Connect.Username, h.username) &&
		bytes.Equal(pk.Connect.Password, h.password)
}

// OnACLCheck 允许所有已鉴权客户端的读写（调试工具默认放行）。
func (h *brokerHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	return true
}

// OnSessionEstablished 在客户端成功建立会话后触发。
func (h *brokerHook) OnSessionEstablished(cl *mqtt.Client, pk packets.Packet) {
	h.emit(BrokerEvent{
		Type:      "connect",
		ClientID:  cl.ID,
		Remote:    cl.Net.Remote,
		Timestamp: nowMillis(),
	})
}

// OnDisconnect 在客户端断开时触发。
func (h *brokerHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	ev := BrokerEvent{
		Type:      "disconnect",
		ClientID:  cl.ID,
		Remote:    cl.Net.Remote,
		Timestamp: nowMillis(),
	}
	if err != nil {
		ev.Detail = err.Error()
	}
	h.emit(ev)
}

// OnSubscribed 在客户端订阅主题后触发。
func (h *brokerHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, reasonCodes []byte) {
	for _, sub := range pk.Filters {
		h.emit(BrokerEvent{
			Type:      "subscribe",
			ClientID:  cl.ID,
			Topic:     sub.Filter,
			QoS:       sub.Qos,
			Timestamp: nowMillis(),
		})
	}
}

// OnUnsubscribed 在客户端取消订阅后触发。
func (h *brokerHook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	for _, sub := range pk.Filters {
		h.emit(BrokerEvent{
			Type:      "unsubscribe",
			ClientID:  cl.ID,
			Topic:     sub.Filter,
			Timestamp: nowMillis(),
		})
	}
}

// OnPublished 在消息成功发布到 broker 后触发。
func (h *brokerHook) OnPublished(cl *mqtt.Client, pk packets.Packet) {
	h.emit(BrokerEvent{
		Type:      "publish",
		ClientID:  cl.ID,
		Topic:     pk.TopicName,
		Payload:   string(pk.Payload),
		QoS:       pk.FixedHeader.Qos,
		Retain:    pk.FixedHeader.Retain,
		Timestamp: nowMillis(),
	})
}
