// Package platform 提供跨平台工具函数
package platform

// IsShiftKeyPressed 检测启动时 Shift 键是否被按住，
// 用于在启动时跳过窗口状态恢复。
func IsShiftKeyPressed() bool {
	return isShiftKeyPressed()
}

// DefaultWindowWidth 默认窗口宽度
const DefaultWindowWidth = 1180

// DefaultWindowHeight 默认窗口高度
const DefaultWindowHeight = 760
