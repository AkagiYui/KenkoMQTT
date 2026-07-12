import { useState } from "react"
import { Send, Timer, History, Eraser } from "lucide-react"
import { toast } from "sonner"
import { type Format, type PubProps, type KeyVal, FORMATS, mqttPublish, scheduleStart, scheduleStop } from "@/lib/api"
import { useI18n } from "@/lib/i18n"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Switch } from "@/components/ui/switch"
import { Textarea } from "@/components/ui/textarea"
import { Card, CardContent } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

const HISTORY_KEY = "kenko-pub-history"

interface PubHistItem {
  topic: string
  payload: string
  qos: number
  retain: boolean
  format: Format
}

function loadHistory(): PubHistItem[] {
  try {
    return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]")
  } catch {
    return []
  }
}

export function PublishPanel({ connId, connected, mqttVersion }: { connId: string; connected: boolean; mqttVersion: number }) {
  const { t } = useI18n()
  const [topic, setTopic] = useState("test/topic")
  const [payload, setPayload] = useState('{\n  "value": ${int(0,100)}\n}')
  const [qos, setQos] = useState(0)
  const [retain, setRetain] = useState(false)
  const [format, setFormat] = useState<Format>("plaintext")
  const [expand, setExpand] = useState(true)
  const [interval, setInterval] = useState(1000)
  const [scheduleId, setScheduleId] = useState<string | null>(null)
  const [history, setHistory] = useState<PubHistItem[]>(loadHistory)
  const [showHistory, setShowHistory] = useState(false)
  const [showProps, setShowProps] = useState(false)

  // MQTT5 发布属性
  const [contentType, setContentType] = useState("")
  const [responseTopic, setResponseTopic] = useState("")
  const [messageExpiry, setMessageExpiry] = useState("")
  const [topicAlias, setTopicAlias] = useState("")
  const [userProps, setUserProps] = useState<KeyVal[]>([])

  const isV5 = mqttVersion === 5

  function buildProps(): PubProps | undefined {
    if (!isV5) return undefined
    const p: PubProps = {
      contentType: contentType || undefined,
      responseTopic: responseTopic || undefined,
      messageExpiryInterval: messageExpiry ? Number(messageExpiry) : null,
      topicAlias: topicAlias ? Number(topicAlias) : null,
      userProperties: userProps.filter((u) => u.key),
    }
    return p
  }

  function pushHistory(item: PubHistItem) {
    const next = [item, ...history.filter((h) => !(h.topic === item.topic && h.payload === item.payload))].slice(0, 20)
    setHistory(next)
    localStorage.setItem(HISTORY_KEY, JSON.stringify(next))
  }

  async function publish() {
    if (!connected || !topic.trim()) return
    try {
      await mqttPublish(connId, topic, payload, qos, retain, format, expand, buildProps())
      pushHistory({ topic, payload, qos, retain, format })
    } catch (e: any) {
      toast.error(t("发布失败"), { description: String(e?.message ?? e) })
    }
  }

  async function clearRetained() {
    if (!connected || !topic.trim()) return
    try {
      await mqttPublish(connId, topic, "", qos, true, "plaintext", false)
      toast.success(t("已清除保留消息"), { description: topic })
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
        const id = await scheduleStart(connId, topic, payload, qos, retain, format, interval)
        setScheduleId(id)
        toast.success(t("定时发布已启动") + ` (${interval}ms)`)
      } catch (e: any) {
        toast.error(t("定时发布失败"), { description: String(e?.message ?? e) })
      }
    }
  }

  function restore(h: PubHistItem) {
    setTopic(h.topic)
    setPayload(h.payload)
    setQos(h.qos)
    setRetain(h.retain)
    setFormat(h.format)
    setShowHistory(false)
  }

  return (
    <Card>
      <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
        <div className="flex gap-2">
          <Input value={topic} onChange={(e) => setTopic(e.target.value)} placeholder={t("发布主题")} className="h-9" />
          <Button variant="outline" size="icon" className="size-9 shrink-0" title={t("发布历史")} onClick={() => setShowHistory((v) => !v)}>
            <History className="size-4" />
          </Button>
        </div>
        {showHistory && (
          <div className="flex max-h-40 flex-col gap-0.5 overflow-y-auto rounded-md border border-border/60 p-1 text-xs">
            {history.length === 0 && <div className="py-2 text-center text-muted-foreground">{t("暂无历史")}</div>}
            {history.map((h, i) => (
              <button key={i} onClick={() => restore(h)} className="flex items-center gap-2 rounded px-1.5 py-1 text-left hover:bg-muted">
                <span className="font-mono text-primary">{h.topic}</span>
                <span className="truncate text-muted-foreground">{h.payload.replace(/\s+/g, " ").slice(0, 40)}</span>
              </button>
            ))}
          </div>
        )}
        <Textarea value={payload} onChange={(e) => setPayload(e.target.value)} rows={3} className="font-mono text-sm" />
        <div className="flex flex-wrap items-center gap-2">
          <Select value={String(qos)} onValueChange={(v) => setQos(Number(v))}>
            <SelectTrigger className="h-9 w-20"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="0">QoS 0</SelectItem>
              <SelectItem value="1">QoS 1</SelectItem>
              <SelectItem value="2">QoS 2</SelectItem>
            </SelectContent>
          </Select>
          <Select value={format} onValueChange={(v) => setFormat(v as Format)}>
            <SelectTrigger className="h-9 w-28"><SelectValue /></SelectTrigger>
            <SelectContent>
              {FORMATS.map((f) => (
                <SelectItem key={f} value={f}>{f}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <Switch checked={retain} onCheckedChange={setRetain} /> retain
          </label>
          <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <Switch checked={expand} onCheckedChange={setExpand} /> {t("占位符")}
          </label>
          <Button className="ml-auto h-9 gap-1.5" onClick={publish} disabled={!connected}><Send className="size-4" /> {t("发布")}</Button>
        </div>

        {isV5 && (
          <>
            <button className="flex w-fit items-center gap-1 text-xs text-muted-foreground hover:text-foreground" onClick={() => setShowProps((v) => !v)}>
              {t("MQTT5 发布属性")}
            </button>
            {showProps && (
              <div className="grid grid-cols-2 gap-2 rounded-md border border-border/60 p-2">
                <Input value={contentType} onChange={(e) => setContentType(e.target.value)} placeholder="Content Type" className="h-8" />
                <Input value={responseTopic} onChange={(e) => setResponseTopic(e.target.value)} placeholder="Response Topic" className="h-8" />
                <Input value={messageExpiry} onChange={(e) => setMessageExpiry(e.target.value)} placeholder="Message Expiry (s)" type="number" className="h-8" />
                <Input value={topicAlias} onChange={(e) => setTopicAlias(e.target.value)} placeholder="Topic Alias" type="number" className="h-8" />
                {/* User Properties */}
                <div className="col-span-full flex flex-col gap-1">
                  {userProps.map((u, i) => (
                    <div key={i} className="flex gap-1">
                      <Input value={u.key} onChange={(e) => setUserProps(userProps.map((x, j) => (j === i ? { ...x, key: e.target.value } : x)))} placeholder="key" className="h-8" />
                      <Input value={u.value} onChange={(e) => setUserProps(userProps.map((x, j) => (j === i ? { ...x, value: e.target.value } : x)))} placeholder="value" className="h-8" />
                      <Button variant="ghost" size="icon" className="size-8 shrink-0" onClick={() => setUserProps(userProps.filter((_, j) => j !== i))}>×</Button>
                    </div>
                  ))}
                  <Button variant="outline" size="sm" className="h-7 w-fit text-xs" onClick={() => setUserProps([...userProps, { key: "", value: "" }])}>
                    + User Property
                  </Button>
                </div>
              </div>
            )}
          </>
        )}

        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs text-muted-foreground">{t("定时")}</span>
          <Input type="number" value={interval} onChange={(e) => setInterval(Number(e.target.value))} className="h-8 w-24" disabled={!!scheduleId} />
          <span className="text-xs text-muted-foreground">ms</span>
          <Button variant={scheduleId ? "outline" : "secondary"} size="sm" className={cn("h-8 gap-1.5")} onClick={toggleSchedule} disabled={!connected}>
            <Timer className="size-3.5" /> {scheduleId ? t("停止") : t("开始")}
          </Button>
          <Button variant="ghost" size="sm" className="ml-auto h-8 gap-1.5 text-muted-foreground" onClick={clearRetained} disabled={!connected} title={t("向该主题发布空保留消息以清除")}>
            <Eraser className="size-3.5" /> {t("清除保留")}
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
