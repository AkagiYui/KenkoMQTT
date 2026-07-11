package services

import (
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/wailsapp/wails/v3/pkg/application"
	"gorm.io/gorm"

	"kenkomqtt/internal/models"
)

// Wails 事件名
const (
	BrokerEventName  = "broker:event"  // 单条 broker 活动日志
	BrokerStatsName  = "broker:stats"  // 周期性统计快照
	BrokerStatusName = "broker:status" // 运行状态变化
)

func nowMillis() int64 { return time.Now().UnixMilli() }

// emitEvent 通过 Wails 事件把数据推送给前端；没有运行中的 App 时静默跳过（便于测试）。
func emitEvent(name string, data any) {
	app := application.Get()
	if app == nil || app.Event == nil {
		return
	}
	app.Event.Emit(name, data)
}

// BrokerConfig 是 broker 的可调参数。
type BrokerConfig struct {
	TCPEnabled     bool   `json:"tcpEnabled"`
	TCPHost        string `json:"tcpHost"`
	TCPPort        int    `json:"tcpPort"`
	WSEnabled      bool   `json:"wsEnabled"`
	WSHost         string `json:"wsHost"`
	WSPort         int    `json:"wsPort"`
	AllowAnonymous bool   `json:"allowAnonymous"`
	Username       string `json:"username"`
	Password       string `json:"password"`
	MaxClients     int    `json:"maxClients"`
}

// DefaultBrokerConfig 返回一份合理的默认配置。
func DefaultBrokerConfig() BrokerConfig {
	return BrokerConfig{
		TCPEnabled:     true,
		TCPHost:        "0.0.0.0",
		TCPPort:        1883,
		WSEnabled:      true,
		WSHost:         "0.0.0.0",
		WSPort:         8083,
		AllowAnonymous: true,
		MaxClients:     0,
	}
}

// BrokerEvent 是一条 broker 活动记录（连接、发布、订阅等）。
type BrokerEvent struct {
	Type      string `json:"type"` // connect | disconnect | subscribe | unsubscribe | publish
	ClientID  string `json:"clientId"`
	Remote    string `json:"remote"`
	Topic     string `json:"topic"`
	Payload   string `json:"payload"`
	QoS       byte   `json:"qos"`
	Retain    bool   `json:"retain"`
	Detail    string `json:"detail"`
	Timestamp int64  `json:"timestamp"`
}

// BrokerStats 是发送给前端的统计快照。
type BrokerStats struct {
	Running          bool  `json:"running"`
	StartedAt        int64 `json:"startedAt"`
	Uptime           int64 `json:"uptime"`
	ClientsConnected int64 `json:"clientsConnected"`
	ClientsTotal     int64 `json:"clientsTotal"`
	MessagesReceived int64 `json:"messagesReceived"`
	MessagesSent     int64 `json:"messagesSent"`
	BytesReceived    int64 `json:"bytesReceived"`
	BytesSent        int64 `json:"bytesSent"`
	Subscriptions    int64 `json:"subscriptions"`
	Retained         int64 `json:"retained"`
	Inflight         int64 `json:"inflight"`
}

// BrokerStatus 是 broker 的整体状态。
type BrokerStatus struct {
	Running   bool         `json:"running"`
	StartedAt int64        `json:"startedAt"`
	Config    BrokerConfig `json:"config"`
	Stats     BrokerStats  `json:"stats"`
}

// BrokerClientInfo 描述一个已连接客户端。
type BrokerClientInfo struct {
	ClientID  string `json:"clientId"`
	Remote    string `json:"remote"`
	Listener  string `json:"listener"`
	Username  string `json:"username"`
	Clean     bool   `json:"clean"`
	Protocol  byte   `json:"protocol"`
}

const maxRecentEvents = 500

// BrokerService 管理内嵌的 mochi-mqtt broker 生命周期。
type BrokerService struct {
	settings *SettingsService

	mu        sync.Mutex
	server    *mqtt.Server
	running   bool
	startedAt time.Time
	config    BrokerConfig

	stopStats chan struct{}
	events    []BrokerEvent
	eventsMu  sync.Mutex
}

// NewBrokerService 创建 broker 服务实例。
func NewBrokerService(db *gorm.DB) *BrokerService {
	s := &BrokerService{settings: NewSettingsService(db)}
	s.config = s.loadConfig()
	return s
}

func (s *BrokerService) loadConfig() BrokerConfig {
	raw := s.settings.GetSetting(models.SettingsKeyBrokerConfig)
	cfg := DefaultBrokerConfig()
	if raw != "" {
		_ = models.FromJSON(raw, &cfg)
	}
	return cfg
}

// GetConfig 返回当前（持久化的）broker 配置。
func (s *BrokerService) GetConfig() BrokerConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config
}

// SaveConfig 持久化 broker 配置（不影响正在运行的实例，需重启生效）。
func (s *BrokerService) SaveConfig(cfg BrokerConfig) error {
	s.mu.Lock()
	s.config = cfg
	s.mu.Unlock()
	jsonStr, err := models.ToJSON(cfg)
	if err != nil {
		return err
	}
	return s.settings.SetSetting(models.SettingsKeyBrokerConfig, jsonStr)
}

// IsRunning 返回 broker 是否正在运行。
func (s *BrokerService) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// Start 使用给定配置启动 broker。配置会被持久化。
func (s *BrokerService) Start(cfg BrokerConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("broker 已在运行")
	}
	if !cfg.TCPEnabled && !cfg.WSEnabled {
		return fmt.Errorf("至少需要启用一个监听器 (TCP 或 WebSocket)")
	}

	caps := mqtt.NewDefaultServerCapabilities()
	if cfg.MaxClients > 0 {
		caps.MaximumClients = int64(cfg.MaxClients)
	}

	server := mqtt.New(&mqtt.Options{
		InlineClient: true,
		Capabilities: caps,
		// broker 自身日志降噪，仅保留 error 级别。
		Logger: slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
	})

	hook := &brokerHook{
		allowAnonymous: cfg.AllowAnonymous,
		username:       []byte(cfg.Username),
		password:       []byte(cfg.Password),
		emit:           s.recordEvent,
	}
	if err := server.AddHook(hook, nil); err != nil {
		return fmt.Errorf("添加鉴权钩子失败: %w", err)
	}

	if cfg.TCPEnabled {
		tcp := listeners.NewTCP(listeners.Config{
			ID:      "tcp",
			Address: fmt.Sprintf("%s:%d", cfg.TCPHost, cfg.TCPPort),
		})
		if err := server.AddListener(tcp); err != nil {
			return fmt.Errorf("绑定 TCP 监听器失败 (端口 %d): %w", cfg.TCPPort, err)
		}
	}
	if cfg.WSEnabled {
		ws := listeners.NewWebsocket(listeners.Config{
			ID:      "ws",
			Address: fmt.Sprintf("%s:%d", cfg.WSHost, cfg.WSPort),
		})
		if err := server.AddListener(ws); err != nil {
			return fmt.Errorf("绑定 WebSocket 监听器失败 (端口 %d): %w", cfg.WSPort, err)
		}
	}

	// Serve 是非阻塞的，内部会为每个监听器启动 goroutine。
	if err := server.Serve(); err != nil {
		return fmt.Errorf("启动 broker 失败: %w", err)
	}

	s.server = server
	s.running = true
	s.startedAt = time.Now()
	s.config = cfg
	s.stopStats = make(chan struct{})

	// 持久化配置
	if jsonStr, err := models.ToJSON(cfg); err == nil {
		_ = s.settings.SetSetting(models.SettingsKeyBrokerConfig, jsonStr)
	}

	slog.Info("MQTT broker 已启动", "tcp", cfg.TCPEnabled, "tcpPort", cfg.TCPPort, "ws", cfg.WSEnabled, "wsPort", cfg.WSPort)

	go s.statsLoop(s.stopStats)
	s.recordEvent(BrokerEvent{Type: "started", Timestamp: nowMillis()})
	s.emitStatusLocked()
	return nil
}

// Stop 停止正在运行的 broker。
func (s *BrokerService) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}
	if s.stopStats != nil {
		close(s.stopStats)
		s.stopStats = nil
	}
	if s.server != nil {
		if err := s.server.Close(); err != nil {
			slog.Warn("关闭 broker 出错", "error", err)
		}
	}
	s.server = nil
	s.running = false
	slog.Info("MQTT broker 已停止")
	s.recordEvent(BrokerEvent{Type: "stopped", Timestamp: nowMillis()})
	s.emitStatusLocked()
	return nil
}

// GetStatus 返回 broker 的当前状态与统计。
func (s *BrokerService) GetStatus() BrokerStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return BrokerStatus{
		Running:   s.running,
		StartedAt: s.startedAtMillisLocked(),
		Config:    s.config,
		Stats:     s.statsLocked(),
	}
}

// GetClients 返回当前已连接的客户端列表（不含内嵌客户端）。
func (s *BrokerService) GetClients() []BrokerClientInfo {
	s.mu.Lock()
	server := s.server
	running := s.running
	s.mu.Unlock()

	out := []BrokerClientInfo{}
	if !running || server == nil {
		return out
	}
	for _, cl := range server.Clients.GetAll() {
		if cl.Net.Inline || cl.Closed() {
			continue
		}
		out = append(out, BrokerClientInfo{
			ClientID: cl.ID,
			Remote:   cl.Net.Remote,
			Listener: cl.Net.Listener,
			Username: string(cl.Properties.Username),
			Clean:    cl.Properties.Clean,
			Protocol: cl.Properties.ProtocolVersion,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientID < out[j].ClientID })
	return out
}

// GetRecentEvents 返回最近的 broker 活动日志（最新在前）。
func (s *BrokerService) GetRecentEvents() []BrokerEvent {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()
	out := make([]BrokerEvent, len(s.events))
	// 反转顺序：最新在前
	for i, ev := range s.events {
		out[len(s.events)-1-i] = ev
	}
	return out
}

// ClearEvents 清空活动日志缓存。
func (s *BrokerService) ClearEvents() {
	s.eventsMu.Lock()
	s.events = nil
	s.eventsMu.Unlock()
}

// PublishFromBroker 以内嵌客户端身份直接发布一条消息（用于服务端主动下发调试）。
func (s *BrokerService) PublishFromBroker(topic, payload string, qos byte, retain bool) error {
	s.mu.Lock()
	server := s.server
	running := s.running
	s.mu.Unlock()
	if !running || server == nil {
		return fmt.Errorf("broker 未运行")
	}
	return server.Publish(topic, []byte(payload), retain, qos)
}

// ---- 内部辅助 ----

func (s *BrokerService) recordEvent(ev BrokerEvent) {
	s.eventsMu.Lock()
	s.events = append(s.events, ev)
	if len(s.events) > maxRecentEvents {
		s.events = s.events[len(s.events)-maxRecentEvents:]
	}
	s.eventsMu.Unlock()
	emitEvent(BrokerEventName, ev)
}

func (s *BrokerService) startedAtMillisLocked() int64 {
	if !s.running {
		return 0
	}
	return s.startedAt.UnixMilli()
}

func (s *BrokerService) statsLocked() BrokerStats {
	st := BrokerStats{Running: s.running}
	if s.running && s.server != nil {
		info := s.server.Info
		st.StartedAt = s.startedAt.UnixMilli()
		st.Uptime = int64(time.Since(s.startedAt).Seconds())
		st.ClientsConnected = info.ClientsConnected
		st.ClientsTotal = info.ClientsTotal
		st.MessagesReceived = info.MessagesReceived
		st.MessagesSent = info.MessagesSent
		st.BytesReceived = info.BytesReceived
		st.BytesSent = info.BytesSent
		st.Subscriptions = info.Subscriptions
		st.Retained = info.Retained
		st.Inflight = info.Inflight
	}
	return st
}

func (s *BrokerService) emitStatusLocked() {
	emitEvent(BrokerStatusName, BrokerStatus{
		Running:   s.running,
		StartedAt: s.startedAtMillisLocked(),
		Config:    s.config,
		Stats:     s.statsLocked(),
	})
}

func (s *BrokerService) statsLoop(stop chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			s.mu.Lock()
			stats := s.statsLocked()
			s.mu.Unlock()
			emitEvent(BrokerStatsName, stats)
		}
	}
}
