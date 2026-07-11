// Package assets 嵌入应用运行时所需的静态资源（如托盘图标）。
package assets

import _ "embed"

// TrayIcon 是系统托盘图标（PNG，黑色像素，供 macOS 模板图标使用）。
//
//go:embed tray.png
var TrayIcon []byte
