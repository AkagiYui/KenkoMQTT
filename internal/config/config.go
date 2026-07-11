// Package config 提供应用全局配置管理
package config

import (
	"os"
	"path/filepath"
)

var (
	// Version 应用版本号，可通过 ldflags 覆盖
	Version = "0.0.1"
	// BuildHash 构建哈希值，通过 ldflags 注入
	BuildHash = "dev"
	// BuildTime 构建时间，通过 ldflags 注入
	BuildTime = "dev"
	// AppName 应用名称
	AppName = "KenkoMQTT"
	// AppIdentifier 应用唯一标识符
	AppIdentifier = "com.akagiyui.kenkomqtt"
)

// Config 应用配置实例
type Config struct {
	// DataDir 数据存储根目录
	DataDir string
	// LogsDir 日志文件目录
	LogsDir string
	// DBPath SQLite 数据库文件路径
	DBPath string
}

// New 创建新的配置实例，初始化所有必要目录
func New() (*Config, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return nil, err
	}

	dataDir := filepath.Join(configDir, AppIdentifier)
	logsDir := filepath.Join(dataDir, "logs")
	dbPath := filepath.Join(dataDir, "kenkomqtt.db")

	// 确保数据目录存在
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	// 确保日志目录存在
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return nil, err
	}

	return &Config{
		DataDir: dataDir,
		LogsDir: logsDir,
		DBPath:  dbPath,
	}, nil
}
