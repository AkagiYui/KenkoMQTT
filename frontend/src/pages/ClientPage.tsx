import { useEffect, useMemo, useRef, useState } from "react"
import { Plus, Plug, Unplug, Trash2, Send, ArrowDown, ArrowUp, Save } from "lucide-react"
import { toast } from "sonner"
import { ClientService, ConnectionService } from "@bindings/kenkomqtt/internal/services"
import type { ClientMessage, ClientStatusEvent, ClientConnectOptions } from "@bindings/kenkomqtt/internal/services"
import type { Connection } from "@bindings/kenkomqtt/internal/models"
import { EV, onEvent } from "@/lib/events"
import { cn, formatTime, tryPrettyJSON } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
import { Card, CardContent } from "@/components/ui/card"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Separator } from "@/components/ui/separator"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

type Sub = { topic: string; qos: number }
type Status = "disconnected" | "connecting" | "connected" | "reconnecting" | "error"

const statusBadge: Record<Status, { label: string; variant: "success" | "secondary" | "warning" | "destructive" }> = {
  connected: { label: "已连接", variant: "success" },
  connecting: { label: "连接中", variant: "warning" },
  reconnecting: { label: "重连中", variant: "warning" },
  disconnected: { label: "未连接", variant: "secondary" },
  error: { label: "错误", variant: "destructive" },
}

function newDraft(): Connection {
  return {
    id: "",
    name: "新连接",
    protocol: "tcp",
    host: "127.0.0.1",
    port: 1883,
    path: "/mqtt",
    clientId: "",
    username: "",
    password: "",
    keepAlive: 60,
    cleanSession: true,
    mqttVersion: 4,
    tlsSkipVerify: false,
    sortOrder: 0,
    createdAt: "",
    updatedAt: "",
  }
}

export function ClientPage() {
  const [connections, setConnections] = useState<Connection[]>([])
  const [form, setForm] = useState<Connection>(newDraft())
  const [isDraft, setIsDraft] = useState(true)

  const [statusMap, setStatusMap] = useState<Record<string, Status>>({})
  const [subsMap, setSubsMap] = useState<Record<string, Sub[]>>({})
  const [msgMap, setMsgMap] = useState<Record<string, ClientMessage[]>>({})

  const [subTopic, setSubTopic] = useState("#")
  const [subQos, setSubQos] = useState(0)
  const [pubTopic, setPubTopic] = useState("test/topic")
  const [pubPayload, setPubPayload] = useState('{\n  "hello": "world"\n}')
  const [pubQos, setPubQos] = useState(0)
  const [pubRetain, setPubRetain] = useState(false)
  const [filter, setFilter] = useState("")
  const scrollRef = useRef<HTMLDivElement | null>(null)

  const connId = form.id
  const status: Status = (connId && statusMap[connId]) || "disconnected"
  const connected = status === "connected"

  useEffect(() => {
    loadConnections()
    const offMsg = onEvent<ClientMessage>(EV.clientMessage, (m) => {
      setMsgMap((prev) => {
        const list = prev[m.connId] ? [...prev[m.connId], m] : [m]
        return { ...prev, [m.connId]: list.slice(-1000) }
      })
    })
    const offStatus = onEvent<ClientStatusEvent>(EV.clientStatus, (s) => {
      setStatusMap((prev) => ({ ...prev, [s.connId]: s.status as Status }))
      if (s.status === "error" && s.detail) toast.error("连接错误", { description: s.detail })
    })
    return () => {
      offMsg()
      offStatus()
    }
  }, [])

  const messages = useMemo(() => {
    const list = (connId && msgMap[connId]) || []
    if (!filter.trim()) return list
    const f = filter.toLowerCase()
    return list.filter((m) => m.topic.toLowerCase().includes(f) || m.payload.toLowerCase().includes(f))
  }, [msgMap, connId, filter])

  useEffect(() => {
    // 有新消息时滚到底部
    const el = scrollRef.current
    if (el) el.scrollTop = el.scrollHeight
  }, [messages.length])

  function loadConnections(selectId?: string) {
    ConnectionService.ListConnections()
      .then((list) => {
        const conns = list ?? []
        setConnections(conns)
        if (selectId) {
          const found = conns.find((c) => c.id === selectId)
          if (found) {
            setForm(found)
            setIsDraft(false)
          }
        } else if (conns.length > 0 && isDraft && form.name === "新连接") {
          setForm(conns[0])
          setIsDraft(false)
        }
      })
      .catch(() => {})
  }

  function patch(p: Partial<Connection>) {
    setForm((c) => ({ ...c, ...p }))
  }

  function selectConnection(c: Connection) {
    setForm(c)
    setIsDraft(false)
  }

  function newConnection() {
    setForm(newDraft())
    setIsDraft(true)
  }

  async function handleSave(): Promise<Connection | null> {
    try {
      const saved = await ConnectionService.SaveConnection(form)
      if (saved) {
        setForm(saved)
        setIsDraft(false)
        loadConnections(saved.id)
        toast.success("连接已保存")
        return saved
      }
    } catch (e: any) {
      toast.error("保存失败", { description: String(e?.message ?? e) })
    }
    return null
  }

  async function handleDelete(c: Connection) {
    try {
      await ClientService.Disconnect(c.id)
      await ConnectionService.DeleteConnection(c.id)
      toast.success("连接已删除")
      if (form.id === c.id) newConnection()
      loadConnections()
    } catch (e: any) {
      toast.error("删除失败", { description: String(e?.message ?? e) })
    }
  }

  async function handleConnect() {
    let target = form
    if (isDraft || !form.id) {
      const saved = await handleSave()
      if (!saved) return
      target = saved
    }
    setStatusMap((prev) => ({ ...prev, [target.id]: "connecting" }))
    const opts: ClientConnectOptions = {
      protocol: target.protocol,
      host: target.host,
      port: target.port,
      path: target.path,
      clientId: target.clientId,
      username: target.username,
      password: target.password,
      keepAlive: target.keepAlive,
      cleanSession: target.cleanSession,
      mqttVersion: target.mqttVersion,
      tlsSkipVerify: target.tlsSkipVerify,
    }
    try {
      await ClientService.Connect(target.id, opts)
      toast.success("已连接到 Broker")
    } catch (e: any) {
      setStatusMap((prev) => ({ ...prev, [target.id]: "error" }))
      toast.error("连接失败", { description: String(e?.message ?? e) })
    }
  }

  async function handleDisconnect() {
    if (!connId) return
    await ClientService.Disconnect(connId)
    setStatusMap((prev) => ({ ...prev, [connId]: "disconnected" }))
  }

  async function handleSubscribe() {
    if (!connId || !subTopic.trim()) return
    try {
      await ClientService.Subscribe(connId, subTopic, subQos)
      setSubsMap((prev) => {
        const cur = prev[connId] ?? []
        if (cur.some((s) => s.topic === subTopic)) return prev
        return { ...prev, [connId]: [...cur, { topic: subTopic, qos: subQos }] }
      })
    } catch (e: any) {
      toast.error("订阅失败", { description: String(e?.message ?? e) })
    }
  }

  async function handleUnsubscribe(topic: string) {
    if (!connId) return
    try {
      await ClientService.Unsubscribe(connId, topic)
      setSubsMap((prev) => ({ ...prev, [connId]: (prev[connId] ?? []).filter((s) => s.topic !== topic) }))
    } catch (e: any) {
      toast.error("取消订阅失败", { description: String(e?.message ?? e) })
    }
  }

  async function handlePublish() {
    if (!connId || !pubTopic.trim()) return
    try {
      await ClientService.Publish(connId, pubTopic, pubPayload, pubQos, pubRetain)
    } catch (e: any) {
      toast.error("发布失败", { description: String(e?.message ?? e) })
    }
  }

  const subs = (connId && subsMap[connId]) || []
  const isWs = form.protocol === "ws" || form.protocol === "wss"
  const isTls = form.protocol === "tls" || form.protocol === "wss"

  return (
    <div className="flex h-full">
      {/* 左：连接列表 + 表单 */}
      <div className="flex w-[340px] shrink-0 flex-col border-r">
        <div className="flex items-center justify-between border-b px-4 py-3">
          <h2 className="text-base font-semibold">MQTT 客户端</h2>
          <Button variant="outline" size="sm" className="h-7 gap-1.5 text-xs" onClick={newConnection}>
            <Plus className="h-3.5 w-3.5" /> 新建
          </Button>
        </div>

        {connections.length > 0 && (
          <div className="flex gap-1 overflow-x-auto border-b p-2">
            {connections.map((c) => (
              <button
                key={c.id}
                onClick={() => selectConnection(c)}
                className={cn(
                  "group flex items-center gap-1.5 rounded-md px-2.5 py-1 text-xs whitespace-nowrap transition-colors",
                  form.id === c.id ? "bg-primary/15 text-primary" : "text-muted-foreground hover:bg-accent"
                )}
              >
                <span
                  className={cn(
                    "h-1.5 w-1.5 rounded-full",
                    statusMap[c.id] === "connected" ? "bg-success" : "bg-muted-foreground/40"
                  )}
                />
                {c.name}
              </button>
            ))}
          </div>
        )}

        <ScrollArea className="min-h-0 flex-1">
          <div className="space-y-3 p-4">
            <Field label="名称">
              <Input value={form.name} onChange={(e) => patch({ name: e.target.value })} disabled={connected} />
            </Field>
            <div className="grid grid-cols-[110px_1fr] gap-2">
              <Field label="协议">
                <Select value={form.protocol} onValueChange={(v) => patch({ protocol: v })} disabled={connected}>
                  <SelectTrigger className="h-9">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="tcp">mqtt://</SelectItem>
                    <SelectItem value="tls">mqtts://</SelectItem>
                    <SelectItem value="ws">ws://</SelectItem>
                    <SelectItem value="wss">wss://</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label="主机地址">
                <Input value={form.host} onChange={(e) => patch({ host: e.target.value })} disabled={connected} />
              </Field>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <Field label="端口">
                <Input
                  type="number"
                  value={form.port}
                  onChange={(e) => patch({ port: Number(e.target.value) || 0 })}
                  disabled={connected}
                />
              </Field>
              <Field label="MQTT 版本">
                <Select
                  value={String(form.mqttVersion)}
                  onValueChange={(v) => patch({ mqttVersion: Number(v) })}
                  disabled={connected}
                >
                  <SelectTrigger className="h-9">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="4">3.1.1</SelectItem>
                    <SelectItem value="3">3.1</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
            </div>
            {isWs && (
              <Field label="WebSocket 路径">
                <Input value={form.path} onChange={(e) => patch({ path: e.target.value })} disabled={connected} placeholder="/mqtt" />
              </Field>
            )}
            <Field label="Client ID（留空自动生成）">
              <Input value={form.clientId} onChange={(e) => patch({ clientId: e.target.value })} disabled={connected} placeholder="auto" />
            </Field>
            <div className="grid grid-cols-2 gap-2">
              <Field label="用户名">
                <Input value={form.username} onChange={(e) => patch({ username: e.target.value })} disabled={connected} />
              </Field>
              <Field label="密码">
                <Input type="password" value={form.password} onChange={(e) => patch({ password: e.target.value })} disabled={connected} />
              </Field>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <Field label="Keep Alive（秒）">
                <Input
                  type="number"
                  value={form.keepAlive}
                  onChange={(e) => patch({ keepAlive: Number(e.target.value) || 0 })}
                  disabled={connected}
                />
              </Field>
              <div className="flex items-end gap-4 pb-1.5">
                <label className="flex items-center gap-2 text-xs text-muted-foreground">
                  <Switch checked={form.cleanSession} onCheckedChange={(v) => patch({ cleanSession: v })} disabled={connected} />
                  Clean Session
                </label>
              </div>
            </div>
            {isTls && (
              <label className="flex items-center gap-2 text-xs text-muted-foreground">
                <Switch checked={form.tlsSkipVerify} onCheckedChange={(v) => patch({ tlsSkipVerify: v })} disabled={connected} />
                跳过 TLS 证书校验
              </label>
            )}
          </div>
        </ScrollArea>

        <div className="flex items-center gap-2 border-t p-3">
          <Button variant="outline" size="sm" onClick={handleSave} disabled={connected} className="gap-1.5">
            <Save className="h-4 w-4" /> 保存
          </Button>
          {!isDraft && (
            <Button variant="ghost" size="icon" className="h-9 w-9 text-destructive" onClick={() => handleDelete(form)} disabled={connected}>
              <Trash2 className="h-4 w-4" />
            </Button>
          )}
          <div className="flex-1" />
          {!connected ? (
            <Button size="sm" onClick={handleConnect} disabled={status === "connecting"} className="gap-1.5">
              <Plug className="h-4 w-4" /> 连接
            </Button>
          ) : (
            <Button size="sm" variant="destructive" onClick={handleDisconnect} className="gap-1.5">
              <Unplug className="h-4 w-4" /> 断开
            </Button>
          )}
        </div>
      </div>

      {/* 右：订阅 / 发布 / 消息 */}
      <div className="flex min-w-0 flex-1 flex-col">
        <div className="flex items-center gap-3 border-b px-5 py-3">
          <span className="text-sm font-medium">{form.name}</span>
          <Badge variant={statusBadge[status].variant}>{statusBadge[status].label}</Badge>
          <span className="font-mono text-xs text-muted-foreground">
            {form.protocol}://{form.host}:{form.port}
          </span>
        </div>

        <div className="grid grid-cols-1 gap-3 p-4 lg:grid-cols-2">
          {/* 订阅 */}
          <Card>
            <CardContent className="space-y-2 p-3">
              <div className="text-xs font-semibold text-muted-foreground">订阅主题</div>
              <div className="flex gap-2">
                <Input value={subTopic} onChange={(e) => setSubTopic(e.target.value)} placeholder="topic/#" className="h-8" disabled={!connected} />
                <QosSelect value={subQos} onChange={setSubQos} disabled={!connected} />
                <Button size="sm" className="h-8" onClick={handleSubscribe} disabled={!connected}>
                  订阅
                </Button>
              </div>
              <div className="flex flex-wrap gap-1.5 pt-1">
                {subs.length === 0 && <span className="text-xs text-muted-foreground">尚无订阅</span>}
                {subs.map((s) => (
                  <button
                    key={s.topic}
                    onClick={() => handleUnsubscribe(s.topic)}
                    className="group flex items-center gap-1 rounded-md bg-primary/10 px-2 py-0.5 text-xs text-primary hover:bg-destructive/15 hover:text-destructive"
                    title="点击取消订阅"
                  >
                    {s.topic}
                    <span className="text-[10px] opacity-70">Q{s.qos}</span>
                    <Trash2 className="h-3 w-3 opacity-0 group-hover:opacity-100" />
                  </button>
                ))}
              </div>
            </CardContent>
          </Card>

          {/* 发布 */}
          <Card>
            <CardContent className="space-y-2 p-3">
              <div className="text-xs font-semibold text-muted-foreground">发布消息</div>
              <div className="flex gap-2">
                <Input value={pubTopic} onChange={(e) => setPubTopic(e.target.value)} placeholder="topic" className="h-8" disabled={!connected} />
                <QosSelect value={pubQos} onChange={setPubQos} disabled={!connected} />
                <label className="flex items-center gap-1.5 whitespace-nowrap text-xs text-muted-foreground">
                  <Switch checked={pubRetain} onCheckedChange={setPubRetain} disabled={!connected} />
                  保留
                </label>
              </div>
              <Textarea
                value={pubPayload}
                onChange={(e) => setPubPayload(e.target.value)}
                className="min-h-[60px] text-xs"
                disabled={!connected}
              />
              <Button size="sm" className="h-8 w-full gap-1.5" onClick={handlePublish} disabled={!connected}>
                <Send className="h-3.5 w-3.5" /> 发布
              </Button>
            </CardContent>
          </Card>
        </div>

        <Separator />

        {/* 消息流 */}
        <div className="flex items-center justify-between px-5 py-2">
          <div className="text-xs font-semibold text-muted-foreground">消息记录 ({messages.length})</div>
          <div className="flex items-center gap-2">
            <Input
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="过滤主题/内容…"
              className="h-7 w-48 text-xs"
            />
            <Button
              variant="ghost"
              size="sm"
              className="h-7 gap-1.5 text-xs"
              onClick={() => connId && setMsgMap((prev) => ({ ...prev, [connId]: [] }))}
            >
              <Trash2 className="h-3.5 w-3.5" /> 清空
            </Button>
          </div>
        </div>

        <div ref={scrollRef} className="min-h-0 flex-1 overflow-auto px-5 pb-4">
          {messages.length === 0 ? (
            <div className="flex h-full items-center justify-center text-center text-xs text-muted-foreground">
              连接后订阅主题即可接收消息，发布的消息也会记录在此。
            </div>
          ) : (
            <div className="space-y-1.5">
              {messages.map((m, i) => (
                <MessageRow key={i} msg={m} />
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function MessageRow({ msg }: { msg: ClientMessage }) {
  const isRx = msg.direction === "received"
  return (
    <div
      className={cn(
        "rounded-lg border px-3 py-2",
        isRx ? "border-l-2 border-l-success bg-card" : "border-l-2 border-l-primary bg-primary/5"
      )}
    >
      <div className="flex items-center gap-2 text-xs">
        {isRx ? <ArrowDown className="h-3 w-3 text-success" /> : <ArrowUp className="h-3 w-3 text-primary" />}
        <span className="font-mono font-medium">{msg.topic}</span>
        <Badge variant="outline" className="text-[10px]">QoS {msg.qos}</Badge>
        {msg.retain && <Badge variant="warning" className="text-[10px]">retain</Badge>}
        <span className="ml-auto text-muted-foreground">{formatTime(msg.timestamp)}</span>
      </div>
      <pre className="mt-1 overflow-x-auto whitespace-pre-wrap break-all font-mono text-xs text-foreground/90">
        {tryPrettyJSON(msg.payload)}
      </pre>
    </div>
  )
}

function QosSelect({ value, onChange, disabled }: { value: number; onChange: (v: number) => void; disabled?: boolean }) {
  return (
    <Select value={String(value)} onValueChange={(v) => onChange(Number(v))} disabled={disabled}>
      <SelectTrigger className="h-8 w-20 shrink-0">
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="0">QoS 0</SelectItem>
        <SelectItem value="1">QoS 1</SelectItem>
        <SelectItem value="2">QoS 2</SelectItem>
      </SelectContent>
    </Select>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1">
      <Label>{label}</Label>
      {children}
    </div>
  )
}
