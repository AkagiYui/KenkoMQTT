import { useEffect, useMemo, useState } from "react"
import { Plus, Plug, Unplug, Trash2, Send, Save, Timer, Copy, PlugZap, Settings2, Dices } from "lucide-react"
import { toast } from "sonner"
import {
  type Profile,
  type SubProfile,
  type Protocol,
  type Status,
  type Format,
  FORMATS,
  listProfiles,
  saveProfile,
  deleteProfile,
  mqttConnect,
  mqttDisconnect,
  mqttPublish,
  mqttTestConnection,
  scheduleStart,
  scheduleStop,
  onStatus,
  newProfile,
  DEFAULT_PORTS,
} from "@/lib/api"
import { cn } from "@/lib/utils"
import { pushLog } from "@/lib/log"
import { useI18n } from "@/lib/i18n"
import { MessageViewer } from "@/components/MessageViewer"
import { Analysis } from "@/components/Analysis"
import { SubscriptionPanel } from "@/components/SubscriptionPanel"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
import { Card, CardContent } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

const statusMeta: Record<Status, { label: string; variant: "success" | "secondary" | "warning" | "destructive" }> = {
  connected: { label: "已连接", variant: "success" },
  connecting: { label: "连接中", variant: "warning" },
  reconnecting: { label: "重连中", variant: "warning" },
  disconnected: { label: "未连接", variant: "secondary" },
  error: { label: "错误", variant: "destructive" },
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
  const { t } = useI18n()
  const [profiles, setProfiles] = useState<Profile[]>([])
  const [form, setForm] = useState<Profile>(newProfile())
  const [isDraft, setIsDraft] = useState(true)
  const [statusMap, setStatusMap] = useState<Record<string, Status>>({})
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [testing, setTesting] = useState(false)

  const [pubTopic, setPubTopic] = useState("test/topic")
  const [pubPayload, setPubPayload] = useState('{\n  "value": ${int(0,100)}\n}')
  const [pubQos, setPubQos] = useState(0)
  const [pubRetain, setPubRetain] = useState(false)
  const [pubFormat, setPubFormat] = useState<Format>("plaintext")
  const [pubExpand, setPubExpand] = useState(true)
  const [interval, setIntervalMs] = useState(1000)
  const [scheduleId, setScheduleId] = useState<string | null>(null)

  const connId = form.id
  const status = statusMap[connId] ?? "disconnected"
  const connected = status === "connected"

  // 按分组归类连接（空分组归入「未分组」）
  const grouped = useMemo(() => {
    const m = new Map<string, Profile[]>()
    for (const p of profiles) {
      const g = p.group?.trim() || ""
      if (!m.has(g)) m.set(g, [])
      m.get(g)!.push(p)
    }
    return [...m.entries()]
  }, [profiles])

  useEffect(() => {
    loadProfiles()
    const us = onStatus((s) => {
      setStatusMap((prev) => ({ ...prev, [s.connId]: s.status }))
      pushLog(s.status === "error" ? "error" : "info", "mqtt", `${s.connId.slice(0, 8)} ${s.status}${s.detail ? ": " + s.detail : ""}`)
    })
    const onProfiles = () => loadProfiles()
    window.addEventListener("profiles-changed", onProfiles)
    return () => {
      us.then((f) => f())
      window.removeEventListener("profiles-changed", onProfiles)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

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
      toast.success(t("连接已保存"))
      return saved
    } catch (e: any) {
      toast.error(t("保存失败"), { description: String(e?.message ?? e) })
      return null
    }
  }
  async function handleDelete(p: Profile) {
    try {
      await mqttDisconnect(p.id).catch(() => {})
      await deleteProfile(p.id)
      if (form.id === p.id) newConnection()
      loadProfiles()
      toast.success(t("连接已删除"))
    } catch (e: any) {
      toast.error(t("删除失败"), { description: String(e?.message ?? e) })
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
      toast.error(t("连接失败"), { description: String(e?.message ?? e) })
    }
  }
  async function handleDisconnect() {
    await mqttDisconnect(connId)
    setStatusMap((prev) => ({ ...prev, [connId]: "disconnected" }))
  }
  // 订阅列表变更：更新表单并（非草稿时）持久化到档案。
  function handleSubsChange(subscriptions: SubProfile[]) {
    const next = { ...form, subscriptions }
    setForm(next)
    if (!isDraft) saveProfile(next).then(() => loadProfiles()).catch(() => {})
  }
  async function handleTest() {
    setTesting(true)
    try {
      await mqttTestConnection(form)
      toast.success(t("连接测试成功"))
      pushLog("info", "mqtt", `test ${form.host}:${form.port} ok`)
    } catch (e: any) {
      toast.error(t("连接测试失败"), { description: String(e?.message ?? e) })
      pushLog("error", "mqtt", `test failed: ${String(e?.message ?? e)}`)
    } finally {
      setTesting(false)
    }
  }
  async function handleDuplicate(p: Profile) {
    const copy: Profile = { ...structuredClone(p), id: crypto.randomUUID(), name: `${p.name} (副本)` }
    await saveProfile(copy)
    await loadProfiles(copy.id)
    toast.success(t("已复制连接"))
  }
  function randomClientId() {
    patch({ clientId: `kenko-${Math.random().toString(36).slice(2, 10)}` })
  }
  async function handlePublish() {
    if (!connected || !pubTopic.trim()) return
    try {
      await mqttPublish(connId, pubTopic, pubPayload, pubQos, pubRetain, pubFormat, pubExpand)
    } catch (e: any) {
      toast.error(t("发布失败"), { description: String(e?.message ?? e) })
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
        toast.success(t("定时发布已启动") + ` (${interval}ms)`)
      } catch (e: any) {
        toast.error(t("定时发布失败"), { description: String(e?.message ?? e) })
      }
    }
  }

  const isTls = form.protocol === "tls" || form.protocol === "wss"
  const isWs = form.protocol === "ws" || form.protocol === "wss"

  return (
    <div className="mx-auto flex max-w-4xl flex-col gap-3 p-3 lg:max-w-6xl">
      {/* 连接选择（按分组） */}
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5 pb-1">
        {grouped.map(([g, list]) => (
          <div key={g || "_"} className="flex items-center gap-1.5">
            {g && <span className="text-[11px] font-medium text-muted-foreground">{g}:</span>}
            {list.map((p) => {
              const st = statusMap[p.id] ?? "disconnected"
              return (
                <button
                  key={p.id}
                  onClick={() => selectConnection(p)}
                  onContextMenu={(e) => { e.preventDefault(); handleDuplicate(p) }}
                  title={t("右键复制连接")}
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
          </div>
        ))}
        <Button variant="outline" size="sm" className="h-7 shrink-0 gap-1 text-xs" onClick={newConnection}>
          <Plus className="size-3.5" /> {t("新建")}
        </Button>
      </div>

      <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
        {/* 连接设置 */}
        <Card>
          <CardContent className="flex flex-col gap-3 p-3 sm:p-4">
            <div className="flex items-center justify-between gap-2">
              <Input value={form.name} onChange={(e) => patch({ name: e.target.value })} className="h-8 max-w-[220px] font-medium" disabled={connected} />
              <Badge variant={statusMeta[status].variant}>{t(statusMeta[status].label)}</Badge>
            </div>
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
              <Field label={t("协议")}>
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
              <Field label={t("MQTT 版本")}>
                <Select value={String(form.mqttVersion)} onValueChange={(v) => patch({ mqttVersion: Number(v) })} disabled={connected}>
                  <SelectTrigger className="h-9"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="4">3.1.1</SelectItem>
                    <SelectItem value="5">5.0</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label={t("主机")} className="col-span-2 sm:col-span-1">
                <Input value={form.host} onChange={(e) => patch({ host: e.target.value })} disabled={connected} className="h-9" />
              </Field>
              <Field label={t("端口")}>
                <Input type="number" value={form.port} onChange={(e) => patch({ port: Number(e.target.value) })} disabled={connected} className="h-9" />
              </Field>
              {isWs && (
                <Field label={t("路径")} className="col-span-2">
                  <Input value={form.path} onChange={(e) => patch({ path: e.target.value })} disabled={connected} className="h-9" />
                </Field>
              )}
              <Field label="Client ID" className="col-span-2">
                <Input value={form.clientId} onChange={(e) => patch({ clientId: e.target.value })} placeholder={t("留空自动生成")} disabled={connected} className="h-9" />
              </Field>
              <Field label={t("用户名")}>
                <Input value={form.username} onChange={(e) => patch({ username: e.target.value })} disabled={connected} className="h-9" />
              </Field>
              <Field label={t("密码")}>
                <Input type="password" value={form.password} onChange={(e) => patch({ password: e.target.value })} disabled={connected} className="h-9" />
              </Field>
              <Field label="KeepAlive(s)">
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
                  <Label className="text-xs text-muted-foreground">{t("跳过证书校验（自签名）")}</Label>
                  <Switch checked={form.tlsSkipVerify} onCheckedChange={(v) => patch({ tlsSkipVerify: v })} disabled={connected} />
                </div>
                <Textarea value={form.caCert} onChange={(e) => patch({ caCert: e.target.value })} placeholder={t("可选：CA 证书 (PEM)")} rows={2} disabled={connected} className="font-mono text-xs" />
              </div>
            )}

            <div className="flex flex-col gap-2 rounded-md border border-border/60 p-2">
              <div className="flex items-center justify-between">
                <Label className="text-xs text-muted-foreground">{t("遗嘱消息 (LWT)")}</Label>
                <Switch checked={form.will.enabled} onCheckedChange={(v) => patch({ will: { ...form.will, enabled: v } })} disabled={connected} />
              </div>
              {form.will.enabled && (
                <div className="grid grid-cols-2 gap-2">
                  <Input value={form.will.topic} onChange={(e) => patch({ will: { ...form.will, topic: e.target.value } })} placeholder={t("遗嘱主题")} disabled={connected} className="col-span-2 h-9" />
                  <Textarea value={form.will.payload} onChange={(e) => patch({ will: { ...form.will, payload: e.target.value } })} placeholder={t("遗嘱内容")} rows={2} disabled={connected} className="col-span-2" />
                  <QosSelect value={form.will.qos} onChange={(n) => patch({ will: { ...form.will, qos: n } })} />
                  <div className="flex h-9 items-center gap-2">
                    <Switch checked={form.will.retain} onCheckedChange={(v) => patch({ will: { ...form.will, retain: v } })} disabled={connected} />
                    <span className="text-xs text-muted-foreground">retain</span>
                  </div>
                </div>
              )}
            </div>

            {/* 高级 */}
            <button className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground" onClick={() => setShowAdvanced((v) => !v)}>
              <Settings2 className="size-3.5" /> {t("高级")}
            </button>
            {showAdvanced && (
              <div className="flex flex-col gap-2 rounded-md border border-border/60 p-2">
                <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
                  <Field label={t("分组")}>
                    <Input value={form.group} onChange={(e) => patch({ group: e.target.value })} disabled={connected} className="h-8" />
                  </Field>
                  <Field label={t("连接超时(s)")}>
                    <Input type="number" value={form.connectTimeout} onChange={(e) => patch({ connectTimeout: Number(e.target.value) })} disabled={connected} className="h-8" />
                  </Field>
                  <Field label={t("重连间隔(ms)")}>
                    <Input type="number" value={form.reconnectPeriodMs} onChange={(e) => patch({ reconnectPeriodMs: Number(e.target.value) })} disabled={connected} className="h-8" />
                  </Field>
                  <Field label={t("自动重连")}>
                    <div className="flex h-8 items-center"><Switch checked={form.autoReconnect} onCheckedChange={(v) => patch({ autoReconnect: v })} disabled={connected} /></div>
                  </Field>
                </div>
                <div className="flex flex-wrap items-center gap-3">
                  <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
                    <Switch checked={form.clientIdWithTime} onCheckedChange={(v) => patch({ clientIdWithTime: v })} disabled={connected} />
                    {t("ClientId 追加时间戳")}
                  </label>
                  <Button variant="outline" size="sm" className="h-7 gap-1 text-xs" onClick={randomClientId} disabled={connected}>
                    <Dices className="size-3.5" /> {t("随机 ClientId")}
                  </Button>
                </div>
                {isTls && (
                  <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                    <Textarea value={form.clientCert} onChange={(e) => patch({ clientCert: e.target.value })} placeholder={t("客户端证书 (PEM，双向 TLS)")} rows={2} disabled={connected} className="font-mono text-xs" />
                    <Textarea value={form.clientKey} onChange={(e) => patch({ clientKey: e.target.value })} placeholder={t("客户端私钥 (PEM)")} rows={2} disabled={connected} className="font-mono text-xs" />
                    <Input value={form.alpn.join(",")} onChange={(e) => patch({ alpn: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) })} placeholder="ALPN (mqtt,http/1.1)" disabled={connected} className="col-span-full h-8" />
                  </div>
                )}
                {form.mqttVersion === 5 && (
                  <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
                    <Field label={t("会话过期(s)")}>
                      <Input type="number" value={form.sessionExpiryInterval ?? ""} onChange={(e) => patch({ sessionExpiryInterval: e.target.value ? Number(e.target.value) : null })} disabled={connected} className="h-8" />
                    </Field>
                    <Field label="Receive Max">
                      <Input type="number" value={form.receiveMaximum ?? ""} onChange={(e) => patch({ receiveMaximum: e.target.value ? Number(e.target.value) : null })} disabled={connected} className="h-8" />
                    </Field>
                    <Field label="Max Packet">
                      <Input type="number" value={form.maximumPacketSize ?? ""} onChange={(e) => patch({ maximumPacketSize: e.target.value ? Number(e.target.value) : null })} disabled={connected} className="h-8" />
                    </Field>
                    <Field label="Topic Alias Max">
                      <Input type="number" value={form.topicAliasMaximum ?? ""} onChange={(e) => patch({ topicAliasMaximum: e.target.value ? Number(e.target.value) : null })} disabled={connected} className="h-8" />
                    </Field>
                  </div>
                )}
              </div>
            )}

            <div className="flex flex-wrap gap-2">
              {!connected ? (
                <Button className="gap-1.5" onClick={handleConnect}><Plug className="size-4" /> {t("连接")}</Button>
              ) : (
                <Button variant="outline" className="gap-1.5" onClick={handleDisconnect}><Unplug className="size-4" /> {t("断开")}</Button>
              )}
              <Button variant="outline" className="gap-1.5" onClick={handleTest} disabled={connected || testing}><PlugZap className="size-4" /> {t("测试")}</Button>
              <Button variant="outline" className="gap-1.5" onClick={handleSave} disabled={connected}><Save className="size-4" /> {t("保存")}</Button>
              {!isDraft && (
                <>
                  <Button variant="ghost" className="gap-1.5" onClick={() => handleDuplicate(form)} disabled={connected}><Copy className="size-4" /> {t("复制")}</Button>
                  <Button variant="ghost" className="gap-1.5 text-destructive" onClick={() => handleDelete(form)} disabled={connected}><Trash2 className="size-4" /> {t("删除")}</Button>
                </>
              )}
            </div>
          </CardContent>
        </Card>

        {/* 订阅 + 发布 */}
        <div className="flex flex-col gap-3">
          <SubscriptionPanel
            connId={connId}
            connected={connected}
            mqttVersion={form.mqttVersion}
            subs={form.subscriptions}
            onChange={handleSubsChange}
          />

          <Card>
            <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
              <Input value={pubTopic} onChange={(e) => setPubTopic(e.target.value)} placeholder={t("发布主题")} className="h-9" />
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
                  <span className="text-xs text-muted-foreground">{t("占位符")}</span>
                </div>
                <Button className="ml-auto h-9 gap-1.5" onClick={handlePublish} disabled={!connected}><Send className="size-4" /> {t("发布")}</Button>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-xs text-muted-foreground">{t("定时")}</span>
                <Input type="number" value={interval} onChange={(e) => setIntervalMs(Number(e.target.value))} className="h-8 w-24" disabled={!!scheduleId} />
                <span className="text-xs text-muted-foreground">ms</span>
                <Button variant={scheduleId ? "outline" : "secondary"} size="sm" className="h-8 gap-1.5" onClick={toggleSchedule} disabled={!connected}>
                  <Timer className="size-3.5" /> {scheduleId ? t("停止") : t("开始")}
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* 消息 */}
      <MessageViewer connId={connId} name={form.name} subs={form.subscriptions} />

      {/* 分析：速率 / 流量 / 负载 / 内容 + 主题树 */}
      <Analysis connId={connId} connected={connected} />
    </div>
  )
}

function Field({ label, className, children }: { label: string; className?: string; children: React.ReactNode }) {
  return (
    <div className={cn("flex flex-col gap-1", className)}>
      <Label className="text-xs text-muted-foreground">{label}</Label>
      {children}
    </div>
  )
}
