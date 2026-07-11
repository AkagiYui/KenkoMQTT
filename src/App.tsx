import { useEffect, useState } from "react"
import { Moon, Sun, Github, Radio, Server, BatteryWarning, X } from "lucide-react"
import { useTheme } from "@/lib/theme"
import { ClientPage } from "@/pages/ClientPage"
import { BrokerPage } from "@/pages/BrokerPage"
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

type Tab = "client" | "broker"

const NAV: { key: Tab; label: string; icon: React.ReactNode }[] = [
  { key: "client", label: "客户端", icon: <Radio className="size-4" /> },
  { key: "broker", label: "Broker", icon: <Server className="size-4" /> },
]

export default function App() {
  const { theme, toggle } = useTheme()
  const [tab, setTab] = useState<Tab>("client")
  const [isAndroid, setIsAndroid] = useState(false)
  const [perms, setPerms] = useState<AndroidPerms | null>(null)
  const [bannerDismissed, setBannerDismissed] = useState(false)

  useEffect(() => {
    platformInfo()
      .then((p) => {
        setIsAndroid(p.isAndroid)
        if (p.isAndroid) checkAndroidPermissions().then(setPerms).catch(() => {})
      })
      .catch(() => {})
    // broker 启停后重新检查权限（后台常驻依赖电池白名单）
    const un = onBrokerStatus(() => {
      checkAndroidPermissions().then(setPerms).catch(() => {})
    })
    return () => {
      un.then((f) => f())
    }
  }, [])

  const showBattery = isAndroid && perms?.applicable && !perms.ignoringBatteryOptimizations && !bannerDismissed

  const themeBtn = (
    <Button variant="ghost" size="icon" className="size-8" onClick={toggle} aria-label="切换主题">
      {theme === "dark" ? <Sun className="size-4" /> : <Moon className="size-4" />}
    </Button>
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
          {themeBtn}
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
                  "flex items-center gap-1.5 rounded-md px-2.5 py-1.5 text-xs transition-colors",
                  tab === n.key ? "bg-primary/10 text-primary" : "text-muted-foreground hover:bg-muted"
                )}
              >
                {n.icon}
                {n.label}
              </button>
            ))}
          </nav>
          {themeBtn}
        </header>

        {/* Android 电池优化提示 */}
        {showBattery && (
          <div className="mx-3 mt-3 flex items-start gap-2 rounded-lg border border-warning/40 bg-warning/10 p-3 text-sm">
            <BatteryWarning className="mt-0.5 size-4 shrink-0 text-warning" />
            <div className="flex-1">
              <p className="font-medium">建议关闭电池优化</p>
              <p className="text-xs text-muted-foreground">
                Android 后台限制可能在切后台时暂停 Broker/客户端连接。允许「无限制」后台运行可保持稳定。
              </p>
              <div className="mt-2 flex gap-2">
                <Button size="sm" className="h-7 text-xs" onClick={() => openAndroidSettings("battery").catch(() => {})}>
                  去设置
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-7 text-xs"
                  onClick={() => openAndroidSettings("appdetails").catch(() => {})}
                >
                  应用详情
                </Button>
              </div>
            </div>
            <button onClick={() => setBannerDismissed(true)} aria-label="关闭" className="text-muted-foreground">
              <X className="size-4" />
            </button>
          </div>
        )}

        <main>{tab === "client" ? <ClientPage /> : <BrokerPage />}</main>
      </div>

      <Toaster />
    </div>
  )
}
