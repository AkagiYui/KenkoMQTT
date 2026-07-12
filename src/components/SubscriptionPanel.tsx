import { useState } from "react"
import { Plus, X, Star, Eye, EyeOff, Palette } from "lucide-react"
import { toast } from "sonner"
import { type SubProfile, mqttSubscribe, mqttUnsubscribe } from "@/lib/api"
import { useI18n } from "@/lib/i18n"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

const COLORS = ["", "#ef4444", "#f59e0b", "#22c55e", "#3b82f6", "#a855f7", "#ec4899", "#14b8a6"]

function newSub(): SubProfile {
  return { topic: "", qos: 0, color: "", alias: "", enabled: true, muted: false, format: "", nl: false, rap: false, rh: 0, favorite: false }
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
  const [draft, setDraft] = useState<SubProfile>(newSub())
  const [showOpts, setShowOpts] = useState(false)
  const isV5 = mqttVersion === 5

  async function add() {
    const topic = draft.topic.trim()
    if (!topic) return
    if (subs.some((s) => s.topic === topic)) {
      toast.error(t("该主题已订阅"))
      return
    }
    const sub = { ...draft, topic }
    onChange([...subs, sub])
    if (connected) {
      try {
        await mqttSubscribe(connId, topic, sub.qos, sub.nl, sub.rap, sub.rh)
      } catch (e: any) {
        toast.error(t("订阅失败"), { description: String(e?.message ?? e) })
      }
    }
    setDraft(newSub())
  }

  async function remove(topic: string) {
    onChange(subs.filter((s) => s.topic !== topic))
    if (connected) await mqttUnsubscribe(connId, topic).catch(() => {})
  }

  function update(topic: string, patch: Partial<SubProfile>) {
    onChange(subs.map((s) => (s.topic === topic ? { ...s, ...patch } : s)))
  }

  async function toggleEnabled(s: SubProfile) {
    const enabled = !s.enabled
    update(s.topic, { enabled })
    if (connected) {
      if (enabled) await mqttSubscribe(connId, s.topic, s.qos, s.nl, s.rap, s.rh).catch(() => {})
      else await mqttUnsubscribe(connId, s.topic).catch(() => {})
    }
  }

  return (
    <Card>
      <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
        <div className="flex flex-wrap items-center gap-2">
          <Input value={draft.topic} onChange={(e) => setDraft({ ...draft, topic: e.target.value })} placeholder={t("订阅主题")} className="h-9 min-w-[140px] flex-1" onKeyDown={(e) => e.key === "Enter" && add()} />
          <Select value={String(draft.qos)} onValueChange={(v) => setDraft({ ...draft, qos: Number(v) })}>
            <SelectTrigger className="h-9 w-20"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="0">QoS 0</SelectItem>
              <SelectItem value="1">QoS 1</SelectItem>
              <SelectItem value="2">QoS 2</SelectItem>
            </SelectContent>
          </Select>
          {/* 颜色 */}
          <div className="flex items-center gap-1">
            {COLORS.map((c) => (
              <button
                key={c || "none"}
                onClick={() => setDraft({ ...draft, color: c })}
                className={cn("size-5 rounded-full border", draft.color === c && "ring-2 ring-primary")}
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
          <Input value={draft.alias} onChange={(e) => setDraft({ ...draft, alias: e.target.value })} placeholder={t("别名（可选）")} className="h-8 w-40" />
          {isV5 && (
            <Button variant="ghost" size="sm" className="h-8 text-xs text-muted-foreground" onClick={() => setShowOpts((v) => !v)}>
              {t("MQTT5 选项")}
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
                <SelectTrigger className="h-7 w-16"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="0">0</SelectItem>
                  <SelectItem value="1">1</SelectItem>
                  <SelectItem value="2">2</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        )}

        {subs.length > 0 && (
          <div className="flex flex-col gap-1">
            {subs.map((s) => (
              <div key={s.topic} className={cn("flex items-center gap-2 rounded-md border border-border/50 px-2 py-1 text-xs", !s.enabled && "opacity-50")}>
                <span className="size-2.5 shrink-0 rounded-full border" style={{ background: s.color || "transparent" }} />
                <button className="truncate font-mono hover:text-primary" onClick={() => window.dispatchEvent(new CustomEvent("viewer-search", { detail: s.topic }))} title={t("按此主题过滤")}>
                  {s.alias || s.topic}
                </button>
                <Badge variant="secondary" className="h-4 px-1 text-[10px]">Q{s.qos}</Badge>
                <div className="ml-auto flex items-center gap-1">
                  <button onClick={() => update(s.topic, { favorite: !s.favorite })} title={t("收藏")}>
                    <Star className={cn("size-3.5", s.favorite ? "fill-warning text-warning" : "text-muted-foreground")} />
                  </button>
                  <button onClick={() => update(s.topic, { muted: !s.muted })} title={t("静音")}>
                    {s.muted ? <EyeOff className="size-3.5 text-muted-foreground" /> : <Eye className="size-3.5 text-muted-foreground" />}
                  </button>
                  <button onClick={() => toggleEnabled(s)} title={t("启用/停用")}>
                    <span className={cn("inline-block size-2.5 rounded-full", s.enabled ? "bg-success" : "bg-muted-foreground")} />
                  </button>
                  <button onClick={() => remove(s.topic)} title={t("退订")}>
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
