import { useEffect, useRef, useState } from "react"
import { Plus, X, Star, Eye, EyeOff, Palette, ListChecks } from "lucide-react"
import { toast } from "sonner"
import {
  type SubProfile, FORMATS, subTopics, newSubProfile, mqttSubscribe, mqttUnsubscribe, subCounts,
} from "@/lib/api"
import { useI18n } from "@/lib/i18n"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

const COLORS = ["", "#ef4444", "#f59e0b", "#22c55e", "#3b82f6", "#a855f7", "#ec4899", "#14b8a6"]

/** 订阅一个条目的全部主题（共享其选项与 subId）。 */
async function subscribeEntry(connId: string, s: SubProfile) {
  for (const tp of subTopics(s)) {
    await mqttSubscribe(connId, tp, s.qos, s.nl, s.rap, s.rh, s.subId ?? null).catch(() => {})
  }
}

export function SubscriptionPanel({
  connId,
  connected,
  mqttVersion,
  subs,
  onChange,
}: {
  connId: string
  connected: boolean
  mqttVersion: number
  subs: SubProfile[]
  onChange: (subs: SubProfile[]) => void
}) {
  const { t } = useI18n()
  const [draft, setDraft] = useState<SubProfile>(newSubProfile())
  const [showOpts, setShowOpts] = useState(false)
  const [counts, setCounts] = useState<Record<string, number>>({})
  const isV5 = mqttVersion === 5
  const connRef = useRef(connId)
  useEffect(() => {
    connRef.current = connId
  }, [connId])

  // 订阅计数：轮询后端匹配条数（每条目所有主题求和）。
  useEffect(() => {
    if (!connected) return
    let alive = true
    const tick = async () => {
      const entries = subs.map((s) => ({ id: s.id, topics: subTopics(s) }))
      const flat = entries.flatMap((e) => e.topics)
      if (!flat.length) {
        setCounts({})
        return
      }
      const res = await subCounts(connRef.current, flat).catch(() => [] as number[])
      if (!alive) return
      const map: Record<string, number> = {}
      let i = 0
      for (const e of entries) {
        let sum = 0
        for (let k = 0; k < e.topics.length; k++) sum += res[i++] ?? 0
        map[e.id] = sum
      }
      setCounts(map)
    }
    tick()
    const h = setInterval(tick, 1500)
    return () => {
      alive = false
      clearInterval(h)
    }
  }, [connId, connected, subs])

  function parseTopics(text: string): string[] {
    return text
      .split(/[\n,]/)
      .map((s) => s.trim())
      .filter(Boolean)
  }

  async function add() {
    const topics = parseTopics(draft.topic)
    if (!topics.length) return
    const entry: SubProfile = { ...draft, topic: topics[0], topics }
    onChange([...subs, entry])
    if (connected) {
      try {
        await subscribeEntry(connId, entry)
      } catch (e: any) {
        toast.error(t("订阅失败"), { description: String(e?.message ?? e) })
      }
    }
    setDraft(newSubProfile())
  }

  async function remove(s: SubProfile) {
    onChange(subs.filter((x) => x.id !== s.id))
    if (connected) for (const tp of subTopics(s)) await mqttUnsubscribe(connId, tp).catch(() => {})
  }

  function update(id: string, patch: Partial<SubProfile>) {
    onChange(subs.map((s) => (s.id === id ? { ...s, ...patch } : s)))
  }

  async function toggleEnabled(s: SubProfile) {
    const enabled = !s.enabled
    update(s.id, { enabled })
    if (connected) {
      if (enabled) await subscribeEntry(connId, s)
      else for (const tp of subTopics(s)) await mqttUnsubscribe(connId, tp).catch(() => {})
    }
  }

  async function subscribeAll() {
    if (!connected) return
    for (const s of subs) if (s.enabled) await subscribeEntry(connId, s)
    toast.success(t("已全部订阅"))
  }

  return (
    <Card>
      <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
        <div className="flex flex-wrap items-center gap-2">
          <Input
            value={draft.topic}
            onChange={(e) => setDraft({ ...draft, topic: e.target.value })}
            placeholder={t("订阅主题（多个用逗号/换行）")}
            className="h-9 min-w-[140px] flex-1"
            onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && add()}
          />
          <Select value={String(draft.qos)} onValueChange={(v) => setDraft({ ...draft, qos: Number(v) })}>
            <SelectTrigger className="h-9 w-20"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="0">QoS 0</SelectItem>
              <SelectItem value="1">QoS 1</SelectItem>
              <SelectItem value="2">QoS 2</SelectItem>
            </SelectContent>
          </Select>
          <div className="flex items-center gap-1">
            {COLORS.map((c) => (
              <button
                key={c || "none"}
                onClick={() => setDraft({ ...draft, color: c })}
                className={cn("flex size-5 items-center justify-center rounded-full border", draft.color === c && "ring-2 ring-primary")}
                style={{ background: c || "transparent" }}
                title={c || t("无颜色")}
              >
                {!c && <Palette className="size-3 text-muted-foreground" />}
              </button>
            ))}
          </div>
          <Button className="h-9 gap-1" onClick={add}><Plus className="size-4" />{t("订阅")}</Button>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Input value={draft.alias} onChange={(e) => setDraft({ ...draft, alias: e.target.value })} placeholder={t("别名（可选）")} className="h-8 w-36" />
          {isV5 && (
            <Button variant="ghost" size="sm" className="h-8 text-xs text-muted-foreground" onClick={() => setShowOpts((v) => !v)}>
              {t("MQTT5 选项")}
            </Button>
          )}
          {subs.length > 0 && (
            <Button variant="outline" size="sm" className="ml-auto h-8 gap-1 text-xs" onClick={subscribeAll} disabled={!connected} title={t("全部订阅")}>
              <ListChecks className="size-3.5" /> {t("全订阅")}
            </Button>
          )}
        </div>
        {isV5 && showOpts && (
          <div className="flex flex-wrap items-center gap-3 rounded-md border border-border/60 p-2 text-xs">
            <label className="flex items-center gap-1">
              <input type="checkbox" checked={draft.nl} onChange={(e) => setDraft({ ...draft, nl: e.target.checked })} /> No Local
            </label>
            <label className="flex items-center gap-1">
              <input type="checkbox" checked={draft.rap} onChange={(e) => setDraft({ ...draft, rap: e.target.checked })} /> Retain As Published
            </label>
            <div className="flex items-center gap-1">
              <span className="text-muted-foreground">Retain Handling</span>
              <Select value={String(draft.rh)} onValueChange={(v) => setDraft({ ...draft, rh: Number(v) })}>
                <SelectTrigger className="h-7 w-14"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="0">0</SelectItem>
                  <SelectItem value="1">1</SelectItem>
                  <SelectItem value="2">2</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center gap-1">
              <span className="text-muted-foreground">Sub ID</span>
              <Input
                type="number"
                value={draft.subId ?? ""}
                onChange={(e) => setDraft({ ...draft, subId: e.target.value ? Number(e.target.value) : null })}
                className="h-7 w-20"
              />
            </div>
          </div>
        )}

        {subs.length > 0 && (
          <div className="flex flex-col gap-1">
            {subs.map((s) => (
              <div key={s.id} className={cn("flex items-center gap-2 rounded-md border border-border/50 px-2 py-1 text-xs", !s.enabled && "opacity-50")}>
                <span className="size-2.5 shrink-0 rounded-full border" style={{ background: s.color || "transparent" }} />
                <button
                  className="truncate font-mono hover:text-primary"
                  onClick={() => window.dispatchEvent(new CustomEvent("viewer-search", { detail: subTopics(s)[0] || "" }))}
                  title={subTopics(s).join(", ")}
                >
                  {s.alias || subTopics(s).join(", ")}
                </button>
                <Badge variant="secondary" className="h-4 px-1 text-[10px]">Q{s.qos}</Badge>
                {counts[s.id] > 0 && <Badge className="h-4 px-1 text-[10px]" variant="default">{counts[s.id]}</Badge>}
                {/* 每订阅独立格式 */}
                <Select value={s.format || "_"} onValueChange={(v) => update(s.id, { format: v === "_" ? "" : v })}>
                  <SelectTrigger className="h-6 w-24 text-[10px]"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="_">{t("默认格式")}</SelectItem>
                    {FORMATS.map((f) => (
                      <SelectItem key={f} value={f}>{f}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <div className="ml-auto flex items-center gap-1">
                  <button onClick={() => update(s.id, { favorite: !s.favorite })} title={t("收藏")}>
                    <Star className={cn("size-3.5", s.favorite ? "fill-warning text-warning" : "text-muted-foreground")} />
                  </button>
                  <button onClick={() => update(s.id, { muted: !s.muted })} title={t("静音")}>
                    {s.muted ? <EyeOff className="size-3.5 text-muted-foreground" /> : <Eye className="size-3.5 text-muted-foreground" />}
                  </button>
                  <button onClick={() => toggleEnabled(s)} title={t("启用/停用")}>
                    <span className={cn("inline-block size-2.5 rounded-full", s.enabled ? "bg-success" : "bg-muted-foreground")} />
                  </button>
                  <button onClick={() => remove(s)} title={t("退订")}>
                    <X className="size-3.5 text-destructive" />
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
