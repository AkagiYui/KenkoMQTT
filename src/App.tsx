import { useEffect, useState } from "react"
import { Moon, Sun, MoonStar, Github, Radio, Server, ScrollText, Settings as SettingsIcon, BatteryWarning, X, Languages, AppWindow } from "lucide-react"
import { useTheme } from "@/lib/theme"
import { useI18n } from "@/lib/i18n"
import { pushLog } from "@/lib/log"
import { ClientPage } from "@/pages/ClientPage"
import { BrokerPage } from "@/pages/BrokerPage"
import { LogPage } from "@/pages/LogPage"
import { SettingsPage } from "@/pages/SettingsPage"
import {
  type AndroidPerms,
  platformInfo,
  checkAndroidPermissions,
  openAndroidSettings,
  onBrokerStatus,
} from "@/lib/api"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Toaster } from "@/components/ui/sonner"

type Tab = "client" | "broker" | "log" | "settings"

export default function App() {
  const { theme, toggle, setTheme } = useTheme()
  const { t, lang, setLang } = useI18n()
  const [tab, setTab] = useState<Tab>("client")
  const [isAndroid, setIsAndroid] = useState(false)
  const [isDesktop, setIsDesktop] = useState(true)
  const [perms, setPerms] = useState<AndroidPerms | null>(null)
  const [bannerDismissed, setBannerDismissed] = useState(false)

  const NAV: { key: Tab; label: string; icon: React.ReactNode }[] = [
    { key: "client", label: t("客户端"), icon: <Radio className="size-4" /> },
    { key: "broker", label: t("Broker"), icon: <Server className="size-4" /> },
    { key: "log", label: t("日志"), icon: <ScrollText className="size-4" /> },
    { key: "settings", label: t("设置"), icon: <SettingsIcon className="size-4" /> },
  ]

  useEffect(() => {
    platformInfo()
      .then((p) => {
        setIsAndroid(p.isAndroid)
        setIsDesktop(!p.isAndroid && p.os !== "ios")
        if (p.isAndroid) checkAndroidPermissions().then(setPerms).catch(() => {})
      })
      .catch(() => {})
    const un = onBrokerStatus((running) => {
      pushLog("info", "broker", running ? "broker started" : "broker stopped")
      checkAndroidPermissions().then(setPerms).catch(() => {})
    })
    return () => {
      un.then((f) => f())
    }
  }, [])

  // 快捷键：Cmd/Ctrl+1..4 切换标签，Cmd/Ctrl+Shift+L 打开日志。
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (!(e.metaKey || e.ctrlKey)) return
      const map: Record<string, Tab> = { "1": "client", "2": "broker", "3": "log", "4": "settings" }
      if (map[e.key]) {
        e.preventDefault()
        setTab(map[e.key])
      } else if (e.shiftKey && e.key.toLowerCase() === "l") {
        e.preventDefault()
        setTab("log")
      }
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [])

  async function openNewWindow() {
    try {
      const { WebviewWindow } = await import("@tauri-apps/api/webviewWindow")
      const label = `extra-${Date.now()}`
      new WebviewWindow(label, { url: "index.html", title: "KenkoMQTT", width: 900, height: 720 })
      pushLog("info", "app", `open window ${label}`)
    } catch (e: any) {
      pushLog("error", "app", `open window failed: ${String(e?.message ?? e)}`)
    }
  }

  const showBattery = isAndroid && perms?.applicable && !perms.ignoringBatteryOptimizations && !bannerDismissed

  const themeIcon = theme === "light" ? <Moon className="size-4" /> : theme === "dark" ? <MoonStar className="size-4" /> : <Sun className="size-4" />

  const controls = (
    <>
      <Button variant="ghost" size="icon" className="size-8" onClick={toggle} aria-label={t("切换主题")}>
        {themeIcon}
      </Button>
      <Button
        variant="ghost"
        size="icon"
        className="size-8"
        onClick={() => setLang(lang === "zh" ? "en" : "zh")}
        aria-label={t("切换语言")}
      >
        <Languages className="size-4" />
      </Button>
      {isDesktop && (
        <Button variant="ghost" size="icon" className="size-8" onClick={openNewWindow} aria-label={t("新窗口")}>
          <AppWindow className="size-4" />
        </Button>
      )}
    </>
  )

  return (
    <div className="min-h-screen bg-background text-foreground lg:flex">
      {/* 侧栏导航（宽屏 ≥1024） */}
      <aside className="sticky top-0 hidden h-screen w-56 shrink-0 flex-col border-r border-border p-3 lg:flex">
        <div className="mb-4 px-2 text-base font-bold">KenkoMQTT</div>
        <nav className="flex flex-col gap-1">
          {NAV.map((n) => (
            <button
              key={n.key}
              onClick={() => setTab(n.key)}
              className={cn(
                "flex items-center gap-2 rounded-md px-3 py-2 text-sm transition-colors",
                tab === n.key ? "bg-primary/10 text-primary" : "hover:bg-muted"
              )}
            >
              {n.icon}
              {n.label}
            </button>
          ))}
        </nav>
        <div className="mt-auto flex items-center gap-1">
          {controls}
          <Button
            variant="ghost"
            size="icon"
            className="size-8"
            aria-label="GitHub"
            onClick={() => window.open("https://github.com/AkagiYui/KenkoMQTT", "_blank")}
          >
            <Github className="size-4" />
          </Button>
        </div>
      </aside>

      <div className="min-w-0 flex-1">
        {/* 顶栏 + 标签（窄屏/中屏 <1024） */}
        <header className="sticky top-0 z-10 flex items-center justify-between gap-2 border-b border-border bg-background/80 px-3 py-2 backdrop-blur lg:hidden">
          <h1 className="text-sm font-bold">KenkoMQTT</h1>
          <nav className="flex items-center gap-1">
            {NAV.map((n) => (
              <button
                key={n.key}
                onClick={() => setTab(n.key)}
                className={cn(
                  "flex items-center gap-1.5 rounded-md px-2 py-1.5 text-xs transition-colors",
                  tab === n.key ? "bg-primary/10 text-primary" : "text-muted-foreground hover:bg-muted"
                )}
                aria-label={n.label}
              >
                {n.icon}
                <span className="hidden sm:inline">{n.label}</span>
              </button>
            ))}
          </nav>
          <div className="flex items-center">{controls}</div>
        </header>

        {/* Android 电池优化提示 */}
        {showBattery && (
          <div className="mx-3 mt-3 flex items-start gap-2 rounded-lg border border-warning/40 bg-warning/10 p-3 text-sm">
            <BatteryWarning className="mt-0.5 size-4 shrink-0 text-warning" />
            <div className="flex-1">
              <p className="font-medium">{t("建议关闭电池优化")}</p>
              <p className="text-xs text-muted-foreground">
                {t("Android 后台限制可能在切后台时暂停 Broker/客户端连接。允许「无限制」后台运行可保持稳定。")}
              </p>
              <div className="mt-2 flex gap-2">
                <Button size="sm" className="h-7 text-xs" onClick={() => openAndroidSettings("battery").catch(() => {})}>
                  {t("去设置")}
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-7 text-xs"
                  onClick={() => openAndroidSettings("appdetails").catch(() => {})}
                >
                  {t("应用详情")}
                </Button>
              </div>
            </div>
            <button onClick={() => setBannerDismissed(true)} aria-label={t("关闭")} className="text-muted-foreground">
              <X className="size-4" />
            </button>
          </div>
        )}

        <main>
          {tab === "client" && <ClientPage />}
          {tab === "broker" && <BrokerPage />}
          {tab === "log" && <LogPage />}
          {tab === "settings" && <SettingsPage theme={theme} setTheme={setTheme} />}
        </main>
      </div>

      <Toaster />
    </div>
  )
}
