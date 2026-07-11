package services

import (
	"log/slog"
	"sync"
	"time"

	"github.com/wailsapp/wails/v3/pkg/application"
	"github.com/wailsapp/wails/v3/pkg/events"
	"gorm.io/gorm"

	"kenkomqtt/internal/models"
)

const debounceDelay = 300 * time.Millisecond

// WindowStateService 负责持久化窗口位置与大小，以便下次启动时恢复。
type WindowStateService struct {
	settingsService *SettingsService
	debounceTimer   *time.Timer
	mu              sync.Mutex
	cachedState     models.WindowState
}

// NewWindowStateService 创建窗口状态服务实例。
func NewWindowStateService(db *gorm.DB) *WindowStateService {
	return &WindowStateService{settingsService: NewSettingsService(db)}
}

// LoadWindowState 从数据库加载保存的窗口状态。
func (s *WindowStateService) LoadWindowState() (*models.WindowState, bool) {
	jsonStr := s.settingsService.GetSetting(models.SettingsKeyWindowState)
	if jsonStr == "" {
		return nil, false
	}
	var state models.WindowState
	if err := models.FromJSON(jsonStr, &state); err != nil {
		slog.Warn("解析窗口状态失败，使用默认值", "error", err)
		return nil, false
	}
	return &state, true
}

// SaveWindowState 读取窗口当前状态并写入数据库。
func (s *WindowStateService) SaveWindowState(win application.Window) {
	x, y := win.Position()
	width, height := win.Size()
	state := models.WindowState{
		X: x, Y: y, Width: width, Height: height,
		IsMaximised: win.IsMaximised(),
	}

	s.mu.Lock()
	s.cachedState = state
	s.mu.Unlock()

	s.saveState(state)
}

func (s *WindowStateService) saveState(state models.WindowState) {
	if state.Width <= 0 || state.Height <= 0 {
		return
	}
	jsonStr, err := models.ToJSON(state)
	if err != nil {
		slog.Error("序列化窗口状态失败", "error", err)
		return
	}
	if err := s.settingsService.SetSetting(models.SettingsKeyWindowState, jsonStr); err != nil {
		slog.Error("保存窗口状态失败", "error", err)
	}
}

func (s *WindowStateService) debouncedSave(win application.Window) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.debounceTimer != nil {
		s.debounceTimer.Stop()
	}
	s.debounceTimer = time.AfterFunc(debounceDelay, func() {
		s.SaveWindowState(win)
	})
}

func (s *WindowStateService) flushDebouncedSave() {
	s.mu.Lock()
	hasTimer := s.debounceTimer != nil
	if hasTimer {
		s.debounceTimer.Stop()
		s.debounceTimer = nil
	}
	state := s.cachedState
	s.mu.Unlock()
	if hasTimer {
		s.saveState(state)
	}
}

// SetupWindowStatePersistence 监听窗口的移动、缩放、最大化与关闭事件，自动保存状态。
func (s *WindowStateService) SetupWindowStatePersistence(win application.Window) {
	debouncedSaveFn := func(_ *application.WindowEvent) { s.debouncedSave(win) }

	win.OnWindowEvent(events.Common.WindowDidMove, debouncedSaveFn)
	win.OnWindowEvent(events.Common.WindowDidResize, debouncedSaveFn)
	win.OnWindowEvent(events.Common.WindowMaximise, debouncedSaveFn)
	win.OnWindowEvent(events.Common.WindowUnMaximise, debouncedSaveFn)
	win.OnWindowEvent(events.Common.WindowClosing, func(_ *application.WindowEvent) {
		s.flushDebouncedSave()
	})
}
