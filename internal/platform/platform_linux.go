//go:build linux

package platform

// isShiftKeyPressed 在 Linux 上无可靠的通用检测方式，始终返回 false。
func isShiftKeyPressed() bool {
	return false
}
