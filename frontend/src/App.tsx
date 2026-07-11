import { useEffect, useState } from "react"
import { Server, Radio, Moon, Sun, Github } from "lucide-react"
import { Browser } from "@wailsio/runtime"
import { BrokerService, AppService } from "@bindings/kenkomqtt/internal/services"
import type { BrokerStatus } from "@bindings/kenkomqtt/internal/services"
import { EV, onEvent } from "@/lib/events"
import { useTheme } from "@/lib/theme"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Toaster } from "@/components/ui/sonner"
import { TooltipProvider } from "@/components/ui/tooltip"
import { BrokerPage } from "@/pages/BrokerPage"
import { ClientPage } from "@/pages/ClientPage"

type Tab = "broker" | "client"

function App() {
  const [tab, setTab] = useState<Tab>("broker")
  const [brokerRunning, setBrokerRunning] = useState(false)
  const [version, setVersion] = useState("")
  const { theme, toggle } = useTheme()

  useEffect(() => {
    BrokerService.IsRunning().then(setBrokerRunning).catch(() => {})
    AppService.GetAppInfo()
      .then((info) => setVersion(info.version))
      .catch(() => {})
    const off = onEvent<BrokerStatus>(EV.brokerStatus, (s) => setBrokerRunning(s.running))
    return off
  }, [])

  return (
    <TooltipProvider delayDuration={300}>
      <div className="flex h-full w-full flex-col overflow-hidden bg-background">
        {/* 顶部拖拽标题栏 */}
        <header className="drag flex h-11 shrink-0 items-center justify-between border-b px-4 pl-20">
          <div className="flex items-center gap-2">
            <Radio className="h-4 w-4 text-primary" />
            <span className="text-sm font-semibold tracking-tight">KenkoMQTT</span>
            {version && <span className="text-xs text-muted-foreground">v{version}</span>}
          </div>
          <div className="no-drag flex items-center gap-1">
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7"
              onClick={() => Browser.OpenURL("https://github.com/AkagiYui/KenkoMQTT")}
              title="GitHub"
            >
              <Github className="h-4 w-4" />
            </Button>
            <Button variant="ghost" size="icon" className="h-7 w-7" onClick={toggle} title="切换主题">
              {theme === "dark" ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
            </Button>
          </div>
        </header>

        <div className="flex min-h-0 flex-1">
          {/* 侧边导航 */}
          <nav className="flex w-48 shrink-0 flex-col gap-1 border-r bg-card/40 p-3">
            <NavItem
              active={tab === "broker"}
              icon={<Server className="h-4 w-4" />}
              label="Broker 服务端"
              badge={brokerRunning ? "运行中" : undefined}
              onClick={() => setTab("broker")}
            />
            <NavItem
              active={tab === "client"}
              icon={<Radio className="h-4 w-4" />}
              label="MQTT 客户端"
              onClick={() => setTab("client")}
            />
            <div className="mt-auto px-2 pb-1 text-[11px] leading-relaxed text-muted-foreground">
              本地 MQTT 调试工具
              <br />
              内嵌 broker + 客户端
            </div>
          </nav>

          {/* 主内容区 */}
          <main className="min-w-0 flex-1 overflow-hidden">
            <div className={cn("h-full", tab === "broker" ? "block" : "hidden")}>
              <BrokerPage />
            </div>
            <div className={cn("h-full", tab === "client" ? "block" : "hidden")}>
              <ClientPage />
            </div>
          </main>
        </div>
      </div>
      <Toaster />
    </TooltipProvider>
  )
}

function NavItem({
  active,
  icon,
  label,
  badge,
  onClick,
}: {
  active: boolean
  icon: React.ReactNode
  label: string
  badge?: string
  onClick: () => void
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "flex items-center gap-2.5 rounded-lg px-3 py-2 text-sm font-medium transition-colors",
        active
          ? "bg-primary/15 text-primary"
          : "text-muted-foreground hover:bg-accent hover:text-foreground"
      )}
    >
      {icon}
      <span className="flex-1 text-left">{label}</span>
      {badge && (
        <span className="flex items-center gap-1 text-[10px] text-success">
          <span className="h-1.5 w-1.5 rounded-full bg-success animate-pulse" />
        </span>
      )}
    </button>
  )
}

export default App
