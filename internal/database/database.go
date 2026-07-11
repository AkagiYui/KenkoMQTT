// Package database 提供数据库初始化与连接管理。
package database

import (
	"fmt"
	"log/slog"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"kenkomqtt/internal/models"
)

// Initialize 打开 SQLite 数据库并执行自动迁移。
func Initialize(dbPath string) (*gorm.DB, error) {
	slog.Info("正在初始化数据库", "path", dbPath)

	dsn := dbPath + "?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("无法打开数据库: %w", err)
	}

	if err := db.AutoMigrate(models.AllModels()...); err != nil {
		return nil, fmt.Errorf("数据库迁移失败: %w", err)
	}

	slog.Info("数据库初始化完成")
	return db, nil
}
