import { useEffect, useRef, useState } from "react"
import { Play, Square, Trash2, Activity, Users, Radio, ArrowDownUp, Save } from "lucide-react"
import { toast } from "sonner"
import { BrokerService } from "@bindings/kenkomqtt/internal/services"
import type {
  BrokerConfig,
  BrokerStats,
  BrokerEvent,
  BrokerClientInfo,
  BrokerStatus,
} from "@bindings/kenkomqtt/internal/services"
import { EV, onEvent } from "@/lib/events"
import { cn, formatBytes, formatTime, formatUptime } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs"
import { ScrollArea } from "@/components/ui/scroll-area"

const emptyStats: BrokerStats = {
  running: false,
  startedAt: 0,
  uptime: 0,
  clientsConnected: 0,
  clientsTotal: 0,
  messagesReceived: 0,
  messagesSent: 0,
  bytesReceived: 0,
  bytesSent: 0,
  subscriptions: 0,
  retained: 0,
  inflight: 0,
}

const eventColors: Record<string, string> = {
  connect: "text-success",
  disconnect: "text-muted-foreground",
  subscribe: "text-primary",
  unsubscribe: "text-warning",
  publish: "text-foreground",
  started: "text-success",
  stopped: "text-destructive",
}

export function BrokerPage() {
  const [config, setConfig] = useState<BrokerConfig | null>(null)
  const [running, setRunning] = useState(false)
  const [stats, setStats] = useState<BrokerStats>(emptyStats)
  const [events, setEvents] = useState<BrokerEvent[]>([])
  const [clients, setClients] = useState<BrokerClientInfo[]>([])
  const [busy, setBusy] = useState(false)
  const runningRef = useRef(false)

  useEffect(() => {
    BrokerService.GetConfig().then(setConfig).catch(() => {})
    BrokerService.GetStatus()
      .then((s: BrokerStatus) => {
        setRunning(s.running)
        runningRef.current = s.running
        setStats(s.stats)
      })
      .catch(() => {})
    BrokerService.GetRecentEvents().then((e) => setEvents(e ?? [])).catch(() => {})
    refreshClients()

    const offStats = onEvent<BrokerStats>(EV.brokerStats, setStats)
    const offEvent = onEvent<BrokerEvent>(EV.brokerEvent, (ev) => {
      setEvents((prev) => [ev, ...prev].slice(0, 500))
    })
    const offStatus = onEvent<BrokerStatus>(EV.brokerStatus, (s) => {
      setRunning(s.running)
      runningRef.current = s.running
      if (!s.running) setClients([])
      refreshClients()
    })

    const timer = setInterval(() => {
      if (runningRef.current) refreshClients()
    }, 2000)

    return () => {
      offStats()
      offEvent()
      offStatus()
      clearInterval(timer)
    }
  }, [])

  function refreshClients() {
    BrokerService.GetClients().then((c) => setClients(c ?? [])).catch(() => {})
  }

  function patch(p: Partial<BrokerConfig>) {
    setConfig((c) => (c ? { ...c, ...p } : c))
  }

  async function handleStart() {
    if (!config) return
    setBusy(true)
    try {
      await BrokerService.Start(config)
      toast.success("Broker 已启动")
    } catch (e: any) {
      toast.error("启动失败", { description: String(e?.message ?? e) })
    } finally {
      setBusy(false)
    }
  }

  async function handleStop() {
    setBusy(true)
    try {
      await BrokerService.Stop()
      toast.success("Broker 已停止")
    } catch (e: any) {
      toast.error("停止失败", { description: String(e?.message ?? e) })
    } finally {
      setBusy(false)
    }
  }

  async function handleSaveConfig() {
    if (!config) return
    try {
      await BrokerService.SaveConfig(config)
      toast.success("配置已保存")
    } catch (e: any) {
      toast.error("保存失败", { description: String(e?.message ?? e) })
    }
  }

  if (!config) return <div className="p-6 text-sm text-muted-foreground">加载中…</div>

  return (
    <div className="flex h-full flex-col">
      {/* 顶部工具条 */}
      <div className="flex shrink-0 items-center justify-between border-b px-5 py-3">
        <div className="flex items-center gap-3">
          <h2 className="text-base font-semibold">Broker 服务端</h2>
          {running ? (
            <Badge variant="success">
              <span className="mr-1 inline-block h-1.5 w-1.5 rounded-full bg-success" />
              运行中 · {formatUptime(stats.uptime)}
            </Badge>
          ) : (
            <Badge variant="secondary">已停止</Badge>
          )}
        </div>
        <div className="flex gap-2">
          {!running ? (
            <Button onClick={handleStart} disabled={busy} variant="success" size="sm">
              <Play /> 启动
            </Button>
          ) : (
            <Button onClick={handleStop} disabled={busy} variant="destructive" size="sm">
              <Square /> 停止
            </Button>
          )}
        </div>
      </div>

      <div className="grid min-h-0 flex-1 grid-cols-1 gap-4 overflow-auto p-5 lg:grid-cols-[minmax(300px,360px)_1fr]">
        {/* 左：配置 */}
        <div className="space-y-4">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center justify-between text-sm">
                监听器 & 鉴权
                <Button variant="ghost" size="sm" className="h-7 gap-1.5 text-xs" onClick={handleSaveConfig}>
                  <Save className="h-3.5 w-3.5" /> 保存
                </Button>
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <ListenerRow
                label="TCP 监听"
                enabled={config.tcpEnabled}
                host={config.tcpHost}
                port={config.tcpPort}
                disabled={running}
                onToggle={(v) => patch({ tcpEnabled: v })}
                onHost={(v) => patch({ tcpHost: v })}
                onPort={(v) => patch({ tcpPort: v })}
              />
              <ListenerRow
                label="WebSocket 监听"
                enabled={config.wsEnabled}
                host={config.wsHost}
                port={config.wsPort}
                disabled={running}
                onToggle={(v) => patch({ wsEnabled: v })}
                onHost={(v) => patch({ wsHost: v })}
                onPort={(v) => patch({ wsPort: v })}
              />

              <div className="space-y-2 border-t pt-3">
                <div className="flex items-center justify-between">
                  <Label className="text-foreground">允许匿名连接</Label>
                  <Switch
                    checked={config.allowAnonymous}
                    disabled={running}
                    onCheckedChange={(v) => patch({ allowAnonymous: v })}
                  />
                </div>
                {!config.allowAnonymous && (
                  <div className="grid grid-cols-2 gap-2 pt-1">
                    <div className="space-y-1">
                      <Label>用户名</Label>
                      <Input
                        value={config.username}
                        disabled={running}
                        onChange={(e) => patch({ username: e.target.value })}
                        placeholder="username"
                      />
                    </div>
                    <div className="space-y-1">
                      <Label>密码</Label>
                      <Input
                        type="password"
                        value={config.password}
                        disabled={running}
                        onChange={(e) => patch({ password: e.target.value })}
                        placeholder="password"
                      />
                    </div>
                  </div>
                )}
              </div>

              <div className="flex items-center justify-between border-t pt-3">
                <Label className="text-foreground">最大客户端数</Label>
                <Input
                  type="number"
                  className="h-8 w-28"
                  value={config.maxClients}
                  disabled={running}
                  onChange={(e) => patch({ maxClients: Number(e.target.value) || 0 })}
                  placeholder="0 = 不限"
                />
              </div>
              <p className="text-[11px] text-muted-foreground">
                运行中无法修改配置，请先停止 Broker。0 表示不限制客户端数量。
              </p>
            </CardContent>
          </Card>
        </div>

        {/* 右：统计 + 日志 */}
        <div className="flex min-h-0 flex-col gap-4">
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <StatTile icon={<Users />} label="在线客户端" value={stats.clientsConnected} />
            <StatTile icon={<Radio />} label="订阅数" value={stats.subscriptions} />
            <StatTile icon={<ArrowDownUp />} label="收/发消息" value={`${stats.messagesReceived}/${stats.messagesSent}`} />
            <StatTile icon={<Activity />} label="收/发流量" value={`${formatBytes(stats.bytesReceived)} / ${formatBytes(stats.bytesSent)}`} small />
          </div>

          <Tabs defaultValue="log" className="flex min-h-0 flex-1 flex-col">
            <div className="flex items-center justify-between">
              <TabsList>
                <TabsTrigger value="log">活动日志</TabsTrigger>
                <TabsTrigger value="clients">
                  客户端 {clients.length > 0 && <Badge variant="secondary" className="ml-1">{clients.length}</Badge>}
                </TabsTrigger>
              </TabsList>
              <Button
                variant="ghost"
                size="sm"
                className="h-7 gap-1.5 text-xs"
                onClick={() => {
                  BrokerService.ClearEvents()
                  setEvents([])
                }}
              >
                <Trash2 className="h-3.5 w-3.5" /> 清空
              </Button>
            </div>

            <TabsContent value="log" className="mt-3 min-h-0 flex-1">
              <Card className="h-full overflow-hidden">
                <ScrollArea className="h-full">
                  {events.length === 0 ? (
                    <Empty text="暂无活动。启动 Broker 后，连接、订阅与发布事件会显示在这里。" />
                  ) : (
                    <div className="divide-y divide-border/60 font-mono text-xs">
                      {events.map((ev, i) => (
                        <div key={i} className="flex items-start gap-3 px-3 py-1.5">
                          <span className="shrink-0 text-muted-foreground">{formatTime(ev.timestamp)}</span>
                          <span className={cn("w-20 shrink-0 font-semibold", eventColors[ev.type] ?? "text-foreground")}>
                            {ev.type}
                          </span>
                          <span className="min-w-0 flex-1 truncate">
                            {ev.clientId && <span className="text-primary">{ev.clientId}</span>}
                            {ev.topic && <span className="text-muted-foreground"> {ev.topic}</span>}
                            {ev.payload && <span className="text-foreground/80"> = {ev.payload}</span>}
                            {ev.detail && <span className="text-muted-foreground"> ({ev.detail})</span>}
                            {ev.qos !== undefined && ev.type === "publish" && (
                              <span className="text-muted-foreground"> [QoS {ev.qos}{ev.retain ? ",retain" : ""}]</span>
                            )}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}
                </ScrollArea>
              </Card>
            </TabsContent>

            <TabsContent value="clients" className="mt-3 min-h-0 flex-1">
              <Card className="h-full overflow-hidden">
                <ScrollArea className="h-full">
                  {clients.length === 0 ? (
                    <Empty text="暂无已连接的客户端。" />
                  ) : (
                    <table className="w-full text-xs">
                      <thead className="sticky top-0 bg-card text-muted-foreground">
                        <tr className="border-b">
                          <th className="px-3 py-2 text-left font-medium">Client ID</th>
                          <th className="px-3 py-2 text-left font-medium">地址</th>
                          <th className="px-3 py-2 text-left font-medium">监听器</th>
                          <th className="px-3 py-2 text-left font-medium">用户名</th>
                        </tr>
                      </thead>
                      <tbody className="font-mono">
                        {clients.map((c) => (
                          <tr key={c.clientId} className="border-b border-border/50">
                            <td className="px-3 py-1.5 text-primary">{c.clientId}</td>
                            <td className="px-3 py-1.5 text-muted-foreground">{c.remote}</td>
                            <td className="px-3 py-1.5">
                              <Badge variant="outline">{c.listener}</Badge>
                            </td>
                            <td className="px-3 py-1.5 text-muted-foreground">{c.username || "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </ScrollArea>
              </Card>
            </TabsContent>
          </Tabs>
        </div>
      </div>
    </div>
  )
}

function ListenerRow(props: {
  label: string
  enabled: boolean
  host: string
  port: number
  disabled: boolean
  onToggle: (v: boolean) => void
  onHost: (v: string) => void
  onPort: (v: number) => void
}) {
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <Label className="text-foreground">{props.label}</Label>
        <Switch checked={props.enabled} disabled={props.disabled} onCheckedChange={props.onToggle} />
      </div>
      <div className="flex gap-2">
        <Input
          className="h-8"
          value={props.host}
          disabled={props.disabled || !props.enabled}
          onChange={(e) => props.onHost(e.target.value)}
          placeholder="0.0.0.0"
        />
        <Input
          type="number"
          className="h-8 w-24"
          value={props.port}
          disabled={props.disabled || !props.enabled}
          onChange={(e) => props.onPort(Number(e.target.value) || 0)}
          placeholder="端口"
        />
      </div>
    </div>
  )
}

function StatTile({
  icon,
  label,
  value,
  small,
}: {
  icon: React.ReactNode
  label: string
  value: string | number
  small?: boolean
}) {
  return (
    <Card className="p-3">
      <div className="flex items-center gap-1.5 text-xs text-muted-foreground [&_svg]:size-3.5">
        {icon}
        {label}
      </div>
      <div className={cn("mt-1 font-semibold tabular-nums", small ? "text-sm" : "text-xl")}>{value}</div>
    </Card>
  )
}

function Empty({ text }: { text: string }) {
  return <div className="flex h-full items-center justify-center p-8 text-center text-xs text-muted-foreground">{text}</div>
}
