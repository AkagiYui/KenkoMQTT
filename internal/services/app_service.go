package services

import (
	"time"

	"kenkomqtt/internal/config"
)

// AppService 应用信息服务。
type AppService struct {
	startTime time.Time
}

// NewAppService 创建应用信息服务实例。
func NewAppService() *AppService {
	return &AppService{startTime: time.Now()}
}

// AppInfo 应用信息结构。
type AppInfo struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	BuildHash string `json:"buildHash"`
	BuildTime string `json:"buildTime"`
}

// GetAppInfo 获取应用版本、构建哈希与构建时间。
func (s *AppService) GetAppInfo() AppInfo {
	buildTime := config.BuildTime
	if buildTime == "" || buildTime == "dev" {
		buildTime = s.startTime.UTC().Format(time.RFC3339)
	}
	return AppInfo{
		Name:      config.AppName,
		Version:   config.Version,
		BuildHash: config.BuildHash,
		BuildTime: buildTime,
	}
}
