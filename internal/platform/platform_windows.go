//go:build windows

package platform

import "golang.org/x/sys/windows"

const vkShift = 0x10

// isShiftKeyPressed 在 Windows 上检测 Shift 键是否被按住。
func isShiftKeyPressed() bool {
	user32 := windows.NewLazySystemDLL("user32.dll")
	getKeyState := user32.NewProc("GetAsyncKeyState")
	state, _, _ := getKeyState.Call(uintptr(vkShift))
	return state&0x8000 != 0
}
