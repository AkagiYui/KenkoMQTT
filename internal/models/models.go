// Package models 定义持久化数据模型与序列化辅助函数。
package models

import (
	"encoding/json"
	"time"
)

// Connection 客户端连接配置档案，用于连接外部（或本地）MQTT broker。
type Connection struct {
	ID           string    `gorm:"primaryKey" json:"id"`
	Name         string    `json:"name"`
	Protocol     string    `json:"protocol"` // tcp | ws | tls | wss
	Host         string    `json:"host"`
	Port         int       `json:"port"`
	Path         string    `json:"path"`         // WebSocket 路径，如 /mqtt
	ClientID     string    `json:"clientId"`     // 为空时连接时自动生成
	Username     string    `json:"username"`     //
	Password     string    `json:"password"`     //
	KeepAlive    int       `json:"keepAlive"`    // 秒
	CleanSession bool      `json:"cleanSession"` //
	MQTTVersion  int       `json:"mqttVersion"`  // 3=3.1, 4=3.1.1
	TLSSkipVerify bool     `json:"tlsSkipVerify"`
	SortOrder    int       `json:"sortOrder"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

// ConnectionInput 是保存连接档案时前端提交的入参。
// 刻意不含 CreatedAt/UpdatedAt —— 时间戳由后端管理，前端无需（也不应）传递，
// 从而避免把空字符串反序列化为 time.Time 时的解析错误。
type ConnectionInput struct {
	ID            string `json:"id"` // 为空表示新建
	Name          string `json:"name"`
	Protocol      string `json:"protocol"`
	Host          string `json:"host"`
	Port          int    `json:"port"`
	Path          string `json:"path"`
	ClientID      string `json:"clientId"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	KeepAlive     int    `json:"keepAlive"`
	CleanSession  bool   `json:"cleanSession"`
	MQTTVersion   int    `json:"mqttVersion"`
	TLSSkipVerify bool   `json:"tlsSkipVerify"`
	SortOrder     int    `json:"sortOrder"`
}

// Settings 通用键值设置表。
type Settings struct {
	Key   string `gorm:"primaryKey" json:"key"`
	Value string `json:"value"`
}

// 设置键常量
const (
	SettingsKeyWindowState = "window_state"
	SettingsKeyBrokerConfig = "broker_config"
	SettingsKeyTheme        = "theme"
)

// DefaultSettings 提供部分设置项的默认值。
var DefaultSettings = map[string]string{
	SettingsKeyTheme: "system",
}

// WindowState 记录窗口位置、尺寸与最大化状态，用于下次启动恢复。
type WindowState struct {
	X           int  `json:"x"`
	Y           int  `json:"y"`
	Width       int  `json:"width"`
	Height      int  `json:"height"`
	IsMaximised bool `json:"isMaximised"`
}

// ToJSON 将任意值序列化为 JSON 字符串。
func ToJSON(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// FromJSON 将 JSON 字符串反序列化到目标指针。
func FromJSON(s string, v any) error {
	return json.Unmarshal([]byte(s), v)
}

// AllModels 返回所有需要自动迁移的模型。
func AllModels() []any {
	return []any{
		&Connection{},
		&Settings{},
	}
}
