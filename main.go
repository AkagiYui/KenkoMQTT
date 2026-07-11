package main

import (
	"embed"
	"log"
	"log/slog"
	"os"
	"runtime"
	"time"

	"github.com/wailsapp/wails/v3/pkg/application"

	"kenkomqtt/internal/assets"
	"kenkomqtt/internal/config"
	"kenkomqtt/internal/database"
	"kenkomqtt/internal/logger"
	"kenkomqtt/internal/models"
	"kenkomqtt/internal/platform"
	"kenkomqtt/internal/services"
)

//go:embed all:frontend/dist
var appAssets embed.FS

func main() {
	cfg, err := config.New()
	if err != nil {
		log.Fatal("初始化配置失败:", err)
	}

	logFile, err := logger.Setup(cfg)
	if err != nil {
		log.Fatal("初始化日志失败:", err)
	}
	defer logFile.Close()

	slog.Info("KenkoMQTT 应用启动", "version", config.Version, "buildHash", config.BuildHash)

	db, err := database.Initialize(cfg.DBPath)
	if err != nil {
		log.Fatal("初始化数据库失败:", err)
	}

	// 服务实例
	appService := services.NewAppService()
	settingsService := services.NewSettingsService(db)
	connectionService := services.NewConnectionService(db)
	brokerService := services.NewBrokerService(db)
	clientService := services.NewClientService()
	windowStateService := services.NewWindowStateService(db)

	// 注册强类型事件（供绑定生成器识别）
	application.RegisterEvent[services.BrokerEvent](services.BrokerEventName)
	application.RegisterEvent[services.BrokerStats](services.BrokerStatsName)
	application.RegisterEvent[services.BrokerStatus](services.BrokerStatusName)
	application.RegisterEvent[services.ClientMessage](services.ClientMessageName)
	application.RegisterEvent[services.ClientStatusEvent](services.ClientStatusName)

	app := application.New(application.Options{
		Name:        config.AppName,
		Description: "本地 MQTT 调试工具（内嵌 broker + 客户端）",
		Services: []application.Service{
			application.NewService(appService),
			application.NewService(settingsService),
			application.NewService(connectionService),
			application.NewService(brokerService),
			application.NewService(clientService),
		},
		Assets: application.AssetOptions{
			Handler: application.AssetFileServerFS(appAssets),
		},
		Mac: application.MacOptions{
			// 关闭最后一个窗口时不退出——应用作为托盘 / 菜单栏程序继续运行 broker。
			ApplicationShouldTerminateAfterLastWindowClosed: false,
		},
	})

	// 应用退出前，优雅关闭 broker 与所有客户端连接。
	app.OnShutdown(func() {
		clientService.DisconnectAll()
		_ = brokerService.Stop()
	})

	frameless := runtime.GOOS != "darwin"

	// 恢复窗口状态（启动时按住 Shift 可跳过）
	skipRestore := platform.IsShiftKeyPressed()
	var savedState *models.WindowState
	if !skipRestore {
		savedState, _ = windowStateService.LoadWindowState()
	}

	const minWindowThreshold = 200
	windowWidth := platform.DefaultWindowWidth
	windowHeight := platform.DefaultWindowHeight
	var windowX, windowY int
	windowStartPos := application.WindowCentered
	windowStartState := application.WindowStateNormal

	if savedState != nil && savedState.Width >= minWindowThreshold && savedState.Height >= minWindowThreshold {
		windowWidth = savedState.Width
		windowHeight = savedState.Height
		windowX = savedState.X
		windowY = savedState.Y
		windowStartPos = application.WindowXY
		if savedState.IsMaximised {
			windowStartState = application.WindowStateMaximised
		}
	}

	windowOptions := application.WebviewWindowOptions{
		Title:     config.AppName,
		Frameless: frameless,
		Mac: application.MacWindow{
			InvisibleTitleBarHeight: 0,
			Backdrop:                application.MacBackdropTranslucent,
			TitleBar:                application.MacTitleBarHiddenInset,
		},
		Windows: application.WindowsWindow{
			DisableFramelessWindowDecorations: false,
		},
		BackgroundColour: application.NewRGB(9, 11, 17),
		URL:              "/",
		DevToolsEnabled:  true,
		KeyBindings: map[string]func(window application.Window){
			"F12": func(window application.Window) {
				window.(*application.WebviewWindow).OpenDevTools()
			},
		},
		Width:           windowWidth,
		Height:          windowHeight,
		X:               windowX,
		Y:               windowY,
		InitialPosition: windowStartPos,
		StartState:      windowStartState,
	}

	setupMenu(app)

	mainWindow := app.Window.NewWithOptions(windowOptions)
	windowStateService.SetupWindowStatePersistence(mainWindow)

	setupSystemTray(app, mainWindow, brokerService)

	if config.BuildHash == "dev" {
		go func() {
			time.Sleep(500 * time.Millisecond)
			mainWindow.OpenDevTools()
		}()
	}

	if err := app.Run(); err != nil {
		slog.Error("应用运行失败", "error", err)
		os.Exit(1)
	}
	slog.Info("KenkoMQTT 应用退出")
}

// setupMenu 配置应用菜单栏（主要影响 macOS）。
func setupMenu(app *application.App) {
	menu := app.NewMenu()

	appSubMenu := menu.AddSubmenu(config.AppName)
	appSubMenu.Add("关于 " + config.AppName).OnClick(func(_ *application.Context) {
		app.Menu.ShowAbout()
	})
	appSubMenu.Add("版本: " + config.Version + " (" + config.BuildHash + ")").SetEnabled(false)
	appSubMenu.AddSeparator()
	appSubMenu.Add("隐藏 " + config.AppName).SetAccelerator("Cmd+H").OnClick(func(_ *application.Context) {
		app.Hide()
	})
	appSubMenu.Add("退出 " + config.AppName).SetAccelerator("Cmd+Q").OnClick(func(_ *application.Context) {
		app.Quit()
	})

	editMenu := menu.AddSubmenu("编辑")
	editMenu.AddRole(application.EditMenu)

	viewMenu := menu.AddSubmenu("视图")
	viewMenu.Add("开发者工具").SetAccelerator("Cmd+Option+I").OnClick(func(_ *application.Context) {
		if w := app.Window.Current(); w != nil {
			w.(*application.WebviewWindow).OpenDevTools()
		}
	})

	app.Menu.Set(menu)
}

// setupSystemTray 创建菜单栏 / 系统托盘图标，支持切换窗口、启停 broker、退出。
func setupSystemTray(app *application.App, mainWindow application.Window, broker *services.BrokerService) {
	tray := app.SystemTray.New()
	if runtime.GOOS == "darwin" {
		tray.SetTemplateIcon(assets.TrayIcon)
	} else {
		tray.SetIcon(assets.TrayIcon)
	}
	tray.SetTooltip(config.AppName)

	trayMenu := app.NewMenu()

	toggleWindow := func() {
		w, ok := mainWindow.(*application.WebviewWindow)
		if !ok {
			return
		}
		if w.IsVisible() && !w.IsMinimised() {
			w.Hide()
		} else {
			w.Show()
			w.Restore()
			w.Focus()
		}
	}

	trayMenu.Add("显示 / 隐藏窗口").OnClick(func(_ *application.Context) { toggleWindow() })
	trayMenu.AddSeparator()

	brokerItem := trayMenu.Add("启动 Broker")
	refreshBrokerItem := func() {
		if broker.IsRunning() {
			brokerItem.SetLabel("停止 Broker")
		} else {
			brokerItem.SetLabel("启动 Broker")
		}
		trayMenu.Update()
	}
	brokerItem.OnClick(func(_ *application.Context) {
		if broker.IsRunning() {
			_ = broker.Stop()
		} else {
			if err := broker.Start(broker.GetConfig()); err != nil {
				slog.Warn("从托盘启动 broker 失败", "error", err)
			}
		}
		refreshBrokerItem()
	})

	trayMenu.AddSeparator()
	trayMenu.Add("退出").OnClick(func(_ *application.Context) { app.Quit() })

	tray.SetMenu(trayMenu)

	// broker 状态变化时刷新托盘菜单标签。
	app.Event.On(services.BrokerStatusName, func(_ *application.CustomEvent) {
		refreshBrokerItem()
	})

	refreshBrokerItem()
}
