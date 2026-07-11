package services

import (
	"errors"
	"fmt"
	"log/slog"

	"gorm.io/gorm"

	"kenkomqtt/internal/models"
)

// SettingsService 通用键值设置服务。
type SettingsService struct {
	db *gorm.DB
}

// NewSettingsService 创建设置服务实例。
func NewSettingsService(db *gorm.DB) *SettingsService {
	return &SettingsService{db: db}
}

// GetSetting 读取设置值，不存在时返回默认值（或空字符串）。
func (s *SettingsService) GetSetting(key string) string {
	var setting models.Settings
	if err := s.db.Where("key = ?", key).First(&setting).Error; err != nil {
		if def, ok := models.DefaultSettings[key]; ok {
			return def
		}
		return ""
	}
	return setting.Value
}

// SetSetting 写入设置值（upsert）。
func (s *SettingsService) SetSetting(key, value string) error {
	var setting models.Settings
	result := s.db.Where("key = ?", key).First(&setting)

	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		if err := s.db.Create(&models.Settings{Key: key, Value: value}).Error; err != nil {
			slog.Error("保存设置失败", "key", key, "error", err)
			return fmt.Errorf("保存设置失败: %w", err)
		}
		return nil
	}
	if result.Error != nil {
		return result.Error
	}

	setting.Value = value
	return s.db.Save(&setting).Error
}
