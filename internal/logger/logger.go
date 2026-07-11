// Package logger 提供应用日志管理，支持文件输出和自动清理
package logger

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kenkomqtt/internal/config"
)

// Setup 初始化日志系统。
// 返回日志文件句柄，调用方负责在应用退出时关闭。
func Setup(cfg *config.Config) (*os.File, error) {
	logFileName := fmt.Sprintf("kenkomqtt-%s.log", time.Now().Format("2006-01-02"))
	logPath := filepath.Join(cfg.LogsDir, logFileName)

	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("无法打开日志文件: %w", err)
	}

	// 同时输出到标准输出和文件
	multiWriter := io.MultiWriter(os.Stdout, file)
	handler := slog.NewTextHandler(multiWriter, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slog.SetDefault(slog.New(handler))

	slog.Info("日志系统初始化完成", "logPath", logPath)

	cleanOldLogs(cfg.LogsDir)

	return file, nil
}

// cleanOldLogs 清理超过 30 天的日志文件。
func cleanOldLogs(logsDir string) {
	files, err := os.ReadDir(logsDir)
	if err != nil {
		slog.Warn("读取日志目录失败", "error", err)
		return
	}

	cutoff := time.Now().AddDate(0, 0, -30)
	deleted := 0

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if !strings.HasPrefix(f.Name(), "kenkomqtt-") || !strings.HasSuffix(f.Name(), ".log") {
			continue
		}
		info, err := f.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(filepath.Join(logsDir, f.Name())); err != nil {
				slog.Warn("删除过期日志文件失败", "file", f.Name(), "error", err)
			} else {
				deleted++
			}
		}
	}

	if deleted > 0 {
		slog.Info("已清理过期日志文件", "count", deleted)
	}
}
