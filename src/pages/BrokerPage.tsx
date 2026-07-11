import { useEffect, useRef, useState } from "react"
import { Play, Square, Users, ArrowDownToLine, ArrowUpFromLine, Archive } from "lucide-react"
import { toast } from "sonner"
import {
  type BrokerConfig,
  type BrokerClientRow,
  type BrokerEvt,
  type BrokerStats,
  type RetainedRow,
  brokerStart,
  brokerStop,
  brokerStatus,
  brokerGetConfig,
  brokerRetained,
  onBrokerStats,
  onBrokerClients,
  onBrokerEvent,
  onBrokerStatus,
  newBrokerConfig,
} from "@/lib/api"
import { cn, formatTime } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent } from "@/components/ui/card"
import { Separator } from "@/components/ui/separator"

const eventColor: Record<string, string> = {
  connect: "text-success",
  disconnect: "text-muted-foreground",
  subscribe: "text-primary",
  unsubscribe: "text-warning",
  publish: "text-foreground",
}

export function BrokerPage() {
  const [config, setConfig] = useState<BrokerConfig>(newBrokerConfig())
  const [running, setRunning] = useState(false)
  const [stats, setStats] = useState<BrokerStats | null>(null)
  const [clients, setClients] = useState<BrokerClientRow[]>([])
  const [events, setEvents] = useState<BrokerEvt[]>([])
  const [retained, setRetained] = useState<RetainedRow[]>([])
  const logRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    brokerGetConfig().then(setConfig).catch(() => {})
    brokerStatus().then(setRunning).catch(() => {})
    const us = onBrokerStatus(setRunning)
    const ust = onBrokerStats(setStats)
    const uc = onBrokerClients(setClients)
    const ue = onBrokerEvent((ev) => setEvents((p) => [...p.slice(-299), ev]))
    return () => {
      us.then((f) => f())
      ust.then((f) => f())
      uc.then((f) => f())
      ue.then((f) => f())
    }
  }, [])

  useEffect(() => {
    logRef.current?.scrollTo({ top: logRef.current.scrollHeight })
  }, [events.length])

  // 保留消息轮询（运行期间）
  useEffect(() => {
    if (!running) {
      setRetained([])
      return
    }
    const tick = () => brokerRetained().then(setRetained).catch(() => {})
    tick()
    const h = setInterval(tick, 2000)
    return () => clearInterval(h)
  }, [running])

  function patch(p: Partial<BrokerConfig>) {
    setConfig((c) => ({ ...c, ...p }))
  }

  async function toggle() {
    try {
      if (running) {
        await brokerStop()
        toast.success("Broker 已停止")
      } else {
        await brokerStart(config)
        setRunning(true)
        toast.success(`Broker 已启动 :${config.port}`)
      }
    } catch (e: any) {
      toast.error(running ? "停止失败" : "启动失败", { description: String(e?.message ?? e) })
    }
  }

  return (
    <div className="mx-auto flex max-w-4xl flex-col gap-3 p-3 lg:max-w-6xl">
      {/* 概览统计 */}
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
        <Stat icon={<Users className="size-4" />} label="在线客户端" value={stats?.clientsConnected ?? 0} />
        <Stat icon={<ArrowDownToLine className="size-4" />} label="接收消息" value={stats?.msgsReceived ?? 0} />
        <Stat icon={<ArrowUpFromLine className="size-4" />} label="发送消息" value={stats?.msgsSent ?? 0} />
        <Stat icon={<Archive className="size-4" />} label="保留消息" value={stats?.retained ?? 0} />
      </div>

      <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
        {/* 配置 */}
        <Card>
          <CardContent className="flex flex-col gap-3 p-3 sm:p-4">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium">服务端配置</span>
              <Badge variant={running ? "success" : "secondary"}>{running ? "运行中" : "已停止"}</Badge>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <div className="col-span-2 flex flex-col gap-1 sm:col-span-1">
                <Label className="text-xs text-muted-foreground">监听地址</Label>
                <Input value={config.host} onChange={(e) => patch({ host: e.target.value })} disabled={running} className="h-9" />
              </div>
              <div className="flex flex-col gap-1">
                <Label className="text-xs text-muted-foreground">端口</Label>
                <Input type="number" value={config.port} onChange={(e) => patch({ port: Number(e.target.value) })} disabled={running} className="h-9" />
              </div>
              <div className="flex flex-col gap-1">
                <Label className="text-xs text-muted-foreground">最大连接(0=不限)</Label>
                <Input type="number" value={config.maxClients} onChange={(e) => patch({ maxClients: Number(e.target.value) })} disabled={running} className="h-9" />
              </div>
            </div>
            <div className="flex items-center justify-between rounded-md border border-border/60 p-2">
              <Label className="text-xs text-muted-foreground">允许匿名连接</Label>
              <Switch checked={config.allowAnonymous} onCheckedChange={(v) => patch({ allowAnonymous: v })} disabled={running} />
            </div>
            {!config.allowAnonymous && (
              <div className="grid grid-cols-2 gap-2">
                <Input value={config.username} onChange={(e) => patch({ username: e.target.value })} placeholder="用户名" disabled={running} className="h-9" />
                <Input type="password" value={config.password} onChange={(e) => patch({ password: e.target.value })} placeholder="密码" disabled={running} className="h-9" />
              </div>
            )}
            <Button onClick={toggle} variant={running ? "outline" : "default"} className="gap-1.5">
              {running ? <><Square className="size-4" /> 停止</> : <><Play className="size-4" /> 启动</>}
            </Button>
          </CardContent>
        </Card>

        {/* 客户端列表 */}
        <Card>
          <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
            <span className="text-sm font-medium">在线客户端 ({clients.length})</span>
            <Separator />
            <div className="flex max-h-[30vh] flex-col gap-1 overflow-y-auto lg:max-h-[40vh]">
              {clients.map((c) => (
                <div key={c.clientId} className="flex items-center gap-2 rounded-md bg-muted/40 px-2.5 py-1.5 text-xs">
                  <span className="font-mono font-medium">{c.clientId}</span>
                  <span className="text-muted-foreground">{c.addr}</span>
                  {c.username && <Badge variant="secondary" className="h-4 px-1 text-[10px]">{c.username}</Badge>}
                  <span className="ml-auto text-muted-foreground">{c.subs} 订阅</span>
                </div>
              ))}
              {clients.length === 0 && <div className="py-6 text-center text-sm text-muted-foreground">暂无客户端</div>}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* 活动日志 */}
      <Card>
        <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
          <div className="flex items-center justify-between">
            <span className="text-sm font-medium">活动日志</span>
            <Button variant="outline" size="sm" className="h-7" onClick={() => setEvents([])}>清空</Button>
          </div>
          <Separator />
          <div ref={logRef} className="flex max-h-[40vh] flex-col gap-0.5 overflow-y-auto font-mono text-xs">
            {events.map((ev, i) => (
              <div key={i} className="flex flex-wrap items-baseline gap-2 px-1 py-0.5">
                <span className="text-muted-foreground">{formatTime(ev.ts)}</span>
                <span className={cn("font-semibold", eventColor[ev.kind])}>{ev.kind}</span>
                <span>{ev.clientId}</span>
                {ev.topic && <span className="text-primary">{ev.topic}</span>}
                {ev.payload && <span className="truncate text-muted-foreground">{ev.payload}</span>}
              </div>
            ))}
            {events.length === 0 && <div className="py-6 text-center text-muted-foreground">暂无事件</div>}
          </div>
        </CardContent>
      </Card>

      {/* 保留消息检查器 */}
      <Card>
        <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
          <span className="text-sm font-medium">保留消息 ({retained.length})</span>
          <Separator />
          <div className="flex max-h-[30vh] flex-col gap-1 overflow-y-auto">
            {retained.map((r) => (
              <div key={r.topic} className="flex items-baseline gap-2 rounded-md bg-muted/40 px-2.5 py-1.5 text-xs">
                <span className="font-mono text-primary">{r.topic}</span>
                <Badge variant="secondary" className="h-4 px-1 text-[10px]">Q{r.qos}</Badge>
                <span className="truncate font-mono text-muted-foreground">{r.payload}</span>
              </div>
            ))}
            {retained.length === 0 && <div className="py-6 text-center text-sm text-muted-foreground">暂无保留消息</div>}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function Stat({ icon, label, value }: { icon: React.ReactNode; label: string; value: number }) {
  return (
    <Card>
      <CardContent className="flex flex-col gap-1 p-3">
        <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
          {icon}
          {label}
        </div>
        <div className="text-2xl font-semibold tabular-nums">{value}</div>
      </CardContent>
    </Card>
  )
}
