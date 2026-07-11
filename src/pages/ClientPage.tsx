import { useEffect, useMemo, useRef, useState } from "react"
import { Plus, Plug, Unplug, Trash2, Send, Save, X, ArrowDown, ArrowUp, Timer, Download } from "lucide-react"
import { toast } from "sonner"
import {
  type Profile,
  type Protocol,
  type Status,
  type MsgRow,
  type Format,
  type TreeNode,
  FORMATS,
  listProfiles,
  saveProfile,
  deleteProfile,
  mqttConnect,
  mqttDisconnect,
  mqttSubscribe,
  mqttUnsubscribe,
  mqttPublish,
  messagesQuery,
  messagesClear,
  topicTree,
  chartRate,
  exportMessages,
  scheduleStart,
  scheduleStop,
  onMsgSignal,
  onStatus,
  newProfile,
  DEFAULT_PORTS,
} from "@/lib/api"
import { cn, formatTime, tryPrettyJSON } from "@/lib/utils"
import { LineChart } from "@/components/LineChart"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
import { Card, CardContent } from "@/components/ui/card"
import { Separator } from "@/components/ui/separator"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

type Sub = { topic: string; qos: number }

const statusMeta: Record<Status, { label: string; variant: "success" | "secondary" | "warning" | "destructive" }> = {
  connected: { label: "已连接", variant: "success" },
  connecting: { label: "连接中", variant: "warning" },
  reconnecting: { label: "重连中", variant: "warning" },
  disconnected: { label: "未连接", variant: "secondary" },
  error: { label: "错误", variant: "destructive" },
}

function download(name: string, text: string) {
  const blob = new Blob([text], { type: "text/plain;charset=utf-8" })
  const url = URL.createObjectURL(blob)
  const a = document.createElement("a")
  a.href = url
  a.download = name
  a.click()
  URL.revokeObjectURL(url)
}

function QosSelect({ value, onChange, className }: { value: number; onChange: (n: number) => void; className?: string }) {
  return (
    <Select value={String(value)} onValueChange={(v) => onChange(Number(v))}>
      <SelectTrigger className={cn("h-9 w-24", className)}><SelectValue /></SelectTrigger>
      <SelectContent>
        <SelectItem value="0">QoS 0</SelectItem>
        <SelectItem value="1">QoS 1</SelectItem>
        <SelectItem value="2">QoS 2</SelectItem>
      </SelectContent>
    </Select>
  )
}

function FormatSelect({ value, onChange, className }: { value: Format; onChange: (f: Format) => void; className?: string }) {
  return (
    <Select value={value} onValueChange={(v) => onChange(v as Format)}>
      <SelectTrigger className={cn("h-9 w-32", className)}><SelectValue /></SelectTrigger>
      <SelectContent>
        {FORMATS.map((f) => (
          <SelectItem key={f} value={f}>{f}</SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}

export function ClientPage() {
  const [profiles, setProfiles] = useState<Profile[]>([])
  const [form, setForm] = useState<Profile>(newProfile())
  const [isDraft, setIsDraft] = useState(true)
  const [statusMap, setStatusMap] = useState<Record<string, Status>>({})
  const [subsMap, setSubsMap] = useState<Record<string, Sub[]>>({})

  // 消息与分析数据均来自后端
  const [msgs, setMsgs] = useState<MsgRow[]>([])
  const [format, setFormat] = useState<Format>("plaintext")
  const [filter, setFilter] = useState("")
  const [tree, setTree] = useState<TreeNode[]>([])
  const [rate, setRate] = useState<[number[], number[]]>([[], []])

  const [subTopic, setSubTopic] = useState("#")
  const [subQos, setSubQos] = useState(0)
  const [pubTopic, setPubTopic] = useState("test/topic")
  const [pubPayload, setPubPayload] = useState('{\n  "value": ${int(0,100)}\n}')
  const [pubQos, setPubQos] = useState(0)
  const [pubRetain, setPubRetain] = useState(false)
  const [pubFormat, setPubFormat] = useState<Format>("plaintext")
  const [pubExpand, setPubExpand] = useState(true)
  const [interval, setIntervalMs] = useState(1000)
  const [scheduleId, setScheduleId] = useState<string | null>(null)

  const listRef = useRef<HTMLDivElement>(null)
  const refreshTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const connIdRef = useRef("")

  const connId = form.id
  const status = statusMap[connId] ?? "disconnected"
  const connected = status === "connected"
  const subs = subsMap[connId] ?? []

  function refreshMsgs() {
    messagesQuery(connIdRef.current, format, filter).then(setMsgs).catch(() => {})
  }

  useEffect(() => {
    connIdRef.current = connId
  }, [connId])

  useEffect(() => {
    loadProfiles()
    const us = onStatus((s) => setStatusMap((prev) => ({ ...prev, [s.connId]: s.status })))
    const um = onMsgSignal((cid) => {
      if (cid !== connIdRef.current) return
      if (refreshTimer.current) return // 节流：最多每 250ms 拉一次
      refreshTimer.current = setTimeout(() => {
        refreshTimer.current = null
        refreshMsgs()
      }, 250)
    })
    return () => {
      us.then((f) => f())
      um.then((f) => f())
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // 切换连接/格式/过滤时重新查询
  useEffect(() => {
    refreshMsgs()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connId, format, filter])

  useEffect(() => {
    listRef.current?.scrollTo({ top: listRef.current.scrollHeight })
  }, [msgs.length])

  // 分析数据轮询（连接期间）
  useEffect(() => {
    if (!connected) return
    let alive = true
    const tick = async () => {
      const pts = await chartRate(connId, 1000, 60).catch(() => [])
      const t = await topicTree(connId).catch(() => [])
      if (!alive) return
      setRate([pts.map((p) => p.t / 1000), pts.map((p) => p.v)])
      setTree(t)
    }
    tick()
    const h = setInterval(tick, 1500)
    return () => {
      alive = false
      clearInterval(h)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connId, connected])

  async function loadProfiles(selectId?: string) {
    const list = await listProfiles()
    setProfiles(list)
    if (selectId) {
      const found = list.find((p) => p.id === selectId)
      if (found) {
        setForm(found)
        setIsDraft(false)
      }
    }
  }

  function newConnection() {
    setForm(newProfile())
    setIsDraft(true)
  }
  function selectConnection(p: Profile) {
    setForm(p)
    setIsDraft(false)
  }
  function patch(p: Partial<Profile>) {
    setForm((f) => ({ ...f, ...p }))
  }
  function setProtocol(protocol: Protocol) {
    patch({ protocol, port: DEFAULT_PORTS[protocol] })
  }

  async function handleSave(): Promise<Profile | null> {
    try {
      const saved = await saveProfile(form)
      setForm(saved)
      setIsDraft(false)
      await loadProfiles()
      toast.success("连接已保存")
      return saved
    } catch (e: any) {
      toast.error("保存失败", { description: String(e?.message ?? e) })
      return null
    }
  }
  async function handleDelete(p: Profile) {
    try {
      await mqttDisconnect(p.id).catch(() => {})
      await deleteProfile(p.id)
      if (form.id === p.id) newConnection()
      loadProfiles()
      toast.success("连接已删除")
    } catch (e: any) {
      toast.error("删除失败", { description: String(e?.message ?? e) })
    }
  }
  async function handleConnect() {
    let target = form
    if (isDraft) {
      const saved = await handleSave()
      if (!saved) return
      target = saved
    }
    setStatusMap((prev) => ({ ...prev, [target.id]: "connecting" }))
    try {
      await mqttConnect(target)
    } catch (e: any) {
      setStatusMap((prev) => ({ ...prev, [target.id]: "error" }))
      toast.error("连接失败", { description: String(e?.message ?? e) })
    }
  }
  async function handleDisconnect() {
    await mqttDisconnect(connId)
    setStatusMap((prev) => ({ ...prev, [connId]: "disconnected" }))
  }
  async function handleSubscribe() {
    if (!connected || !subTopic.trim()) return
    try {
      await mqttSubscribe(connId, subTopic, subQos)
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
    await mqttUnsubscribe(connId, topic).catch(() => {})
    setSubsMap((prev) => ({ ...prev, [connId]: (prev[connId] ?? []).filter((s) => s.topic !== topic) }))
  }
  async function handlePublish() {
    if (!connected || !pubTopic.trim()) return
    try {
      await mqttPublish(connId, pubTopic, pubPayload, pubQos, pubRetain, pubFormat, pubExpand)
    } catch (e: any) {
      toast.error("发布失败", { description: String(e?.message ?? e) })
    }
  }
  async function toggleSchedule() {
    if (scheduleId) {
      await scheduleStop(scheduleId).catch(() => {})
      setScheduleId(null)
    } else {
      if (!connected) return
      try {
        const id = await scheduleStart(connId, pubTopic, pubPayload, pubQos, pubRetain, pubFormat, interval)
        setScheduleId(id)
        toast.success(`定时发布已启动 (${interval}ms)`)
      } catch (e: any) {
        toast.error("定时发布失败", { description: String(e?.message ?? e) })
      }
    }
  }
  async function doExport(csv: boolean) {
    const text = await exportMessages(connId, csv, format).catch(() => "")
    download(`${form.name || "messages"}.${csv ? "csv" : "json"}`, text)
  }

  const isTls = form.protocol === "tls" || form.protocol === "wss"
  const isWs = form.protocol === "ws" || form.protocol === "wss"
  const treeFlat = useMemo(() => flattenTree(tree), [tree])

  return (
    <div className="mx-auto flex max-w-4xl flex-col gap-3 p-3 lg:max-w-6xl">
      {/* 连接选择 */}
      <div className="flex items-center gap-2 overflow-x-auto pb-1">
        {profiles.map((p) => {
          const st = statusMap[p.id] ?? "disconnected"
          return (
            <button
              key={p.id}
              onClick={() => selectConnection(p)}
              className={cn(
                "flex shrink-0 items-center gap-1.5 rounded-full border px-3 py-1.5 text-xs transition-colors",
                form.id === p.id && !isDraft ? "border-primary bg-primary/10" : "border-border hover:bg-muted"
              )}
            >
              <span className={cn("size-1.5 rounded-full", st === "connected" ? "bg-success" : st === "error" ? "bg-destructive" : "bg-muted-foreground")} />
              {p.name}
            </button>
          )
        })}
        <Button variant="outline" size="sm" className="h-7 shrink-0 gap-1 text-xs" onClick={newConnection}>
          <Plus className="size-3.5" /> 新建
        </Button>
      </div>

      <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
        {/* 连接设置 */}
        <Card>
          <CardContent className="flex flex-col gap-3 p-3 sm:p-4">
            <div className="flex items-center justify-between gap-2">
              <Input value={form.name} onChange={(e) => patch({ name: e.target.value })} className="h-8 max-w-[220px] font-medium" disabled={connected} />
              <Badge variant={statusMeta[status].variant}>{statusMeta[status].label}</Badge>
            </div>
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
              <Field label="协议">
                <Select value={form.protocol} onValueChange={(v) => setProtocol(v as Protocol)} disabled={connected}>
                  <SelectTrigger className="h-9"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="tcp">mqtt:// (TCP)</SelectItem>
                    <SelectItem value="tls">mqtts:// (TLS)</SelectItem>
                    <SelectItem value="ws">ws://</SelectItem>
                    <SelectItem value="wss">wss://</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label="MQTT 版本">
                <Select value={String(form.mqttVersion)} onValueChange={(v) => patch({ mqttVersion: Number(v) })} disabled={connected}>
                  <SelectTrigger className="h-9"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="4">3.1.1</SelectItem>
                    <SelectItem value="5">5.0</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label="主机" className="col-span-2 sm:col-span-1">
                <Input value={form.host} onChange={(e) => patch({ host: e.target.value })} disabled={connected} className="h-9" />
              </Field>
              <Field label="端口">
                <Input type="number" value={form.port} onChange={(e) => patch({ port: Number(e.target.value) })} disabled={connected} className="h-9" />
              </Field>
              {isWs && (
                <Field label="路径" className="col-span-2">
                  <Input value={form.path} onChange={(e) => patch({ path: e.target.value })} disabled={connected} className="h-9" />
                </Field>
              )}
              <Field label="Client ID" className="col-span-2">
                <Input value={form.clientId} onChange={(e) => patch({ clientId: e.target.value })} placeholder="留空自动生成" disabled={connected} className="h-9" />
              </Field>
              <Field label="用户名">
                <Input value={form.username} onChange={(e) => patch({ username: e.target.value })} disabled={connected} className="h-9" />
              </Field>
              <Field label="密码">
                <Input type="password" value={form.password} onChange={(e) => patch({ password: e.target.value })} disabled={connected} className="h-9" />
              </Field>
              <Field label="KeepAlive(秒)">
                <Input type="number" value={form.keepAlive} onChange={(e) => patch({ keepAlive: Number(e.target.value) })} disabled={connected} className="h-9" />
              </Field>
              <Field label="Clean Session">
                <div className="flex h-9 items-center">
                  <Switch checked={form.cleanSession} onCheckedChange={(v) => patch({ cleanSession: v })} disabled={connected} />
                </div>
              </Field>
            </div>

            {isTls && (
              <div className="flex flex-col gap-2 rounded-md border border-border/60 p-2">
                <div className="flex items-center justify-between">
                  <Label className="text-xs text-muted-foreground">跳过证书校验（自签名）</Label>
                  <Switch checked={form.tlsSkipVerify} onCheckedChange={(v) => patch({ tlsSkipVerify: v })} disabled={connected} />
                </div>
                <Textarea value={form.caCert} onChange={(e) => patch({ caCert: e.target.value })} placeholder="可选：CA 证书 (PEM)" rows={2} disabled={connected} className="font-mono text-xs" />
              </div>
            )}

            <div className="flex flex-col gap-2 rounded-md border border-border/60 p-2">
              <div className="flex items-center justify-between">
                <Label className="text-xs text-muted-foreground">遗嘱消息 (LWT)</Label>
                <Switch checked={form.will.enabled} onCheckedChange={(v) => patch({ will: { ...form.will, enabled: v } })} disabled={connected} />
              </div>
              {form.will.enabled && (
                <div className="grid grid-cols-2 gap-2">
                  <Input value={form.will.topic} onChange={(e) => patch({ will: { ...form.will, topic: e.target.value } })} placeholder="遗嘱主题" disabled={connected} className="col-span-2 h-9" />
                  <Textarea value={form.will.payload} onChange={(e) => patch({ will: { ...form.will, payload: e.target.value } })} placeholder="遗嘱内容" rows={2} disabled={connected} className="col-span-2" />
                  <QosSelect value={form.will.qos} onChange={(n) => patch({ will: { ...form.will, qos: n } })} />
                  <div className="flex h-9 items-center gap-2">
                    <Switch checked={form.will.retain} onCheckedChange={(v) => patch({ will: { ...form.will, retain: v } })} disabled={connected} />
                    <span className="text-xs text-muted-foreground">retain</span>
                  </div>
                </div>
              )}
            </div>

            <div className="flex flex-wrap gap-2">
              {!connected ? (
                <Button className="gap-1.5" onClick={handleConnect}><Plug className="size-4" /> 连接</Button>
              ) : (
                <Button variant="outline" className="gap-1.5" onClick={handleDisconnect}><Unplug className="size-4" /> 断开</Button>
              )}
              <Button variant="outline" className="gap-1.5" onClick={handleSave} disabled={connected}><Save className="size-4" /> 保存</Button>
              {!isDraft && (
                <Button variant="ghost" className="gap-1.5 text-destructive" onClick={() => handleDelete(form)} disabled={connected}><Trash2 className="size-4" /> 删除</Button>
              )}
            </div>
          </CardContent>
        </Card>

        {/* 订阅 + 发布 */}
        <div className="flex flex-col gap-3">
          <Card>
            <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
              <div className="flex gap-2">
                <Input value={subTopic} onChange={(e) => setSubTopic(e.target.value)} placeholder="订阅主题" className="h-9" />
                <QosSelect value={subQos} onChange={setSubQos} />
                <Button className="h-9" onClick={handleSubscribe} disabled={!connected}>订阅</Button>
              </div>
              {subs.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {subs.map((s) => (
                    <button key={s.topic} onClick={() => handleUnsubscribe(s.topic)} className="flex items-center gap-1 rounded-full bg-muted px-2.5 py-1 text-xs hover:bg-destructive/15">
                      <span className="font-mono">{s.topic}</span>
                      <Badge variant="secondary" className="h-4 px-1 text-[10px]">Q{s.qos}</Badge>
                      <X className="size-3" />
                    </button>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
              <Input value={pubTopic} onChange={(e) => setPubTopic(e.target.value)} placeholder="发布主题" className="h-9" />
              <Textarea value={pubPayload} onChange={(e) => setPubPayload(e.target.value)} rows={3} className="font-mono text-sm" />
              <div className="flex flex-wrap items-center gap-2">
                <QosSelect value={pubQos} onChange={setPubQos} />
                <FormatSelect value={pubFormat} onChange={setPubFormat} />
                <div className="flex items-center gap-1.5">
                  <Switch checked={pubRetain} onCheckedChange={setPubRetain} />
                  <span className="text-xs text-muted-foreground">retain</span>
                </div>
                <div className="flex items-center gap-1.5">
                  <Switch checked={pubExpand} onCheckedChange={setPubExpand} />
                  <span className="text-xs text-muted-foreground">占位符</span>
                </div>
                <Button className="ml-auto h-9 gap-1.5" onClick={handlePublish} disabled={!connected}><Send className="size-4" /> 发布</Button>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-xs text-muted-foreground">定时</span>
                <Input type="number" value={interval} onChange={(e) => setIntervalMs(Number(e.target.value))} className="h-8 w-24" disabled={!!scheduleId} />
                <span className="text-xs text-muted-foreground">ms</span>
                <Button variant={scheduleId ? "outline" : "secondary"} size="sm" className="h-8 gap-1.5" onClick={toggleSchedule} disabled={!connected}>
                  <Timer className="size-3.5" /> {scheduleId ? "停止" : "开始"}
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* 消息 */}
      <Card>
        <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
          <div className="flex flex-wrap items-center gap-2">
            <Input value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="过滤主题/内容（后端）" className="h-8 max-w-xs" />
            <FormatSelect value={format} onChange={setFormat} className="h-8" />
            <div className="ml-auto flex gap-2">
              <Button variant="outline" size="sm" className="h-8 gap-1" onClick={() => doExport(true)}><Download className="size-3.5" />CSV</Button>
              <Button variant="outline" size="sm" className="h-8 gap-1" onClick={() => doExport(false)}><Download className="size-3.5" />JSON</Button>
              <Button variant="outline" size="sm" className="h-8" onClick={() => { messagesClear(connId); refreshMsgs() }}>清空</Button>
            </div>
          </div>
          <Separator />
          <div ref={listRef} className="flex max-h-[45vh] flex-col gap-2 overflow-y-auto">
            {msgs.map((m, i) => (
              <div key={i} className={cn("rounded-md border-l-2 bg-muted/40 px-2.5 py-1.5", m.dir === "rx" ? "border-l-success" : "border-l-primary")}>
                <div className="flex flex-wrap items-baseline gap-2 text-xs">
                  <span className={cn("font-semibold", m.dir === "rx" ? "text-success" : "text-primary")}>
                    {m.dir === "rx" ? <ArrowDown className="inline size-3" /> : <ArrowUp className="inline size-3" />} {m.dir === "rx" ? "收" : "发"}
                  </span>
                  <span className="font-mono">{m.topic}</span>
                  <span className="ml-auto text-muted-foreground">Q{m.qos}{m.retain ? " · retain" : ""} · {m.size}B · {formatTime(m.ts)}</span>
                </div>
                <pre className="mt-1 whitespace-pre-wrap break-all font-mono text-xs text-foreground/80">{tryPrettyJSON(m.payload)}</pre>
              </div>
            ))}
            {msgs.length === 0 && <div className="py-6 text-center text-sm text-muted-foreground">暂无消息</div>}
          </div>
        </CardContent>
      </Card>

      {/* 分析：消息速率 + 主题树 */}
      <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
        <Card>
          <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
            <span className="text-sm font-medium">消息速率 (msg/s)</span>
            <Separator />
            <LineChart data={rate} label="msg/s" />
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
            <span className="text-sm font-medium">主题树 ({treeFlat.length})</span>
            <Separator />
            <div className="flex max-h-[30vh] flex-col overflow-y-auto font-mono text-xs">
              {treeFlat.map((n) => (
                <div key={n.full} className="flex items-center gap-2 py-0.5" style={{ paddingLeft: n.depth * 14 }}>
                  <span className="truncate">{n.name}</span>
                  {n.count > 0 && <Badge variant="secondary" className="h-4 px-1 text-[10px]">{n.count}</Badge>}
                </div>
              ))}
              {treeFlat.length === 0 && <div className="py-6 text-center text-muted-foreground">暂无主题</div>}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function flattenTree(
  nodes: TreeNode[],
  depth = 0,
  out: { name: string; full: string; count: number; depth: number }[] = []
) {
  for (const n of nodes) {
    out.push({ name: n.name, full: n.full, count: n.count, depth })
    if (n.children.length) flattenTree(n.children, depth + 1, out)
  }
  return out
}

function Field({ label, className, children }: { label: string; className?: string; children: React.ReactNode }) {
  return (
    <div className={cn("flex flex-col gap-1", className)}>
      <Label className="text-xs text-muted-foreground">{label}</Label>
      {children}
    </div>
  )
}
