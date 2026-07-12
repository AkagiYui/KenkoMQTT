import { useEffect, useMemo, useState } from "react"
import { Plus, X, LayoutDashboard } from "lucide-react"
import {
  type Profile, type DashValue, listProfiles, dashboardLatest, chartContent,
} from "@/lib/api"
import { useI18n } from "@/lib/i18n"
import { cn } from "@/lib/utils"
import { LineChart } from "@/components/LineChart"
import { Gauge } from "@/components/Gauge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

type WidgetType = "big" | "gauge" | "line"

interface Widget {
  id: string
  type: WidgetType
  connId: string
  title: string
  topicFilter: string
  jsonpath: string
  unit: string
  min: number
  max: number
  color: string
  size: number // 1..4 列宽
}

const STORAGE_KEY = "kenko-dashboard-widgets"

function loadWidgets(): Widget[] {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]")
  } catch {
    return []
  }
}
function saveWidgets(w: Widget[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(w))
}

export function DashboardPage() {
  const { t } = useI18n()
  const [widgets, setWidgets] = useState<Widget[]>(loadWidgets)
  const [profiles, setProfiles] = useState<Profile[]>([])
  const [adding, setAdding] = useState(false)

  useEffect(() => {
    listProfiles().then(setProfiles).catch(() => {})
  }, [])

  function update(next: Widget[]) {
    setWidgets(next)
    saveWidgets(next)
  }

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-3 p-3">
      <div className="flex items-center gap-2">
        <LayoutDashboard className="size-4 text-muted-foreground" />
        <span className="text-sm font-medium">{t("仪表盘")}</span>
        <Button variant="outline" size="sm" className="ml-auto h-8 gap-1" onClick={() => setAdding((v) => !v)}>
          <Plus className="size-3.5" /> {t("添加组件")}
        </Button>
      </div>

      {adding && (
        <AddWidgetForm
          profiles={profiles}
          onAdd={(w) => {
            update([...widgets, w])
            setAdding(false)
          }}
        />
      )}

      {widgets.length === 0 ? (
        <div className="py-16 text-center text-sm text-muted-foreground">{t("暂无组件，点击「添加组件」")}</div>
      ) : (
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {widgets.map((w) => (
            <div key={w.id} style={{ gridColumn: `span ${Math.min(w.size, 4)}` }} className="min-w-0">
              <WidgetView widget={w} onRemove={() => update(widgets.filter((x) => x.id !== w.id))} />
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function AddWidgetForm({ profiles, onAdd }: { profiles: Profile[]; onAdd: (w: Widget) => void }) {
  const { t } = useI18n()
  const [type, setType] = useState<WidgetType>("big")
  const [connId, setConnId] = useState(profiles[0]?.id ?? "")
  const [title, setTitle] = useState("")
  const [topicFilter, setTopicFilter] = useState("")
  const [jsonpath, setJsonpath] = useState("$.value")
  const [unit, setUnit] = useState("")
  const [min, setMin] = useState(0)
  const [max, setMax] = useState(100)
  const [size, setSize] = useState(1)

  return (
    <Card>
      <CardContent className="grid grid-cols-2 gap-2 p-3 sm:grid-cols-4">
        <Field label={t("类型")}>
          <Select value={type} onValueChange={(v) => setType(v as WidgetType)}>
            <SelectTrigger className="h-8"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="big">Big Number</SelectItem>
              <SelectItem value="gauge">Gauge</SelectItem>
              <SelectItem value="line">Line</SelectItem>
            </SelectContent>
          </Select>
        </Field>
        <Field label={t("连接")}>
          <Select value={connId} onValueChange={setConnId}>
            <SelectTrigger className="h-8"><SelectValue placeholder="—" /></SelectTrigger>
            <SelectContent>
              {profiles.map((p) => (
                <SelectItem key={p.id} value={p.id}>{p.name}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>
        <Field label={t("标题")}>
          <Input value={title} onChange={(e) => setTitle(e.target.value)} className="h-8" />
        </Field>
        <Field label={t("宽度(列)")}>
          <Select value={String(size)} onValueChange={(v) => setSize(Number(v))}>
            <SelectTrigger className="h-8"><SelectValue /></SelectTrigger>
            <SelectContent>
              {[1, 2, 3, 4].map((n) => (
                <SelectItem key={n} value={String(n)}>{n}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>
        <Field label={t("主题过滤（含通配符）")} className="col-span-2">
          <Input value={topicFilter} onChange={(e) => setTopicFilter(e.target.value)} className="h-8 font-mono" placeholder="sensor/+/temp" />
        </Field>
        <Field label="JSONPath" className="col-span-2">
          <Input value={jsonpath} onChange={(e) => setJsonpath(e.target.value)} className="h-8 font-mono" placeholder="$.value" />
        </Field>
        {type !== "line" && (
          <>
            <Field label={t("单位")}>
              <Input value={unit} onChange={(e) => setUnit(e.target.value)} className="h-8" />
            </Field>
            {type === "gauge" && (
              <>
                <Field label="Min">
                  <Input type="number" value={min} onChange={(e) => setMin(Number(e.target.value))} className="h-8" />
                </Field>
                <Field label="Max">
                  <Input type="number" value={max} onChange={(e) => setMax(Number(e.target.value))} className="h-8" />
                </Field>
              </>
            )}
          </>
        )}
        <div className="col-span-full flex justify-end">
          <Button
            size="sm"
            className="h-8"
            disabled={!connId}
            onClick={() =>
              onAdd({
                id: crypto.randomUUID(),
                type,
                connId,
                title: title || topicFilter || "widget",
                topicFilter,
                jsonpath,
                unit,
                min,
                max,
                color: "#3b82f6",
                size,
              })
            }
          >
            {t("添加")}
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}

function WidgetView({ widget: w, onRemove }: { widget: Widget; onRemove: () => void }) {
  const [val, setVal] = useState<DashValue | null>(null)
  const [line, setLine] = useState<[number[], number[]]>([[], []])

  useEffect(() => {
    let alive = true
    const tick = async () => {
      if (w.type === "line") {
        const pts = await chartContent(w.connId, w.topicFilter, w.jsonpath, 120).catch(() => [])
        if (alive) setLine([pts.map((p) => p.t / 1000), pts.map((p) => p.v)])
      } else {
        const v = await dashboardLatest(w.connId, w.topicFilter, w.jsonpath).catch(() => null)
        if (alive) setVal(v)
      }
    }
    tick()
    const h = setInterval(tick, 1500)
    return () => {
      alive = false
      clearInterval(h)
    }
  }, [w.connId, w.topicFilter, w.jsonpath, w.type])

  const num = val?.num
  const display = useMemo(() => (num != null ? num : val?.text ?? "—"), [num, val])

  return (
    <Card className="h-full">
      <CardContent className="flex h-full flex-col gap-1 p-3">
        <div className="flex items-center gap-2">
          <span className="truncate text-xs font-medium text-muted-foreground">{w.title}</span>
          <button className="ml-auto text-muted-foreground hover:text-destructive" onClick={onRemove}>
            <X className="size-3.5" />
          </button>
        </div>
        {w.type === "big" && (
          <div className="flex flex-1 items-center justify-center gap-1">
            <span className={cn("font-semibold", String(display).length > 8 ? "text-2xl" : "text-4xl")}>{display}</span>
            {w.unit && <span className="text-sm text-muted-foreground">{w.unit}</span>}
          </div>
        )}
        {w.type === "gauge" && <Gauge value={num ?? NaN} min={w.min} max={w.max} unit={w.unit} color={w.color} />}
        {w.type === "line" && <LineChart data={line} label={w.jsonpath} height={120} stroke={w.color} />}
      </CardContent>
    </Card>
  )
}

function Field({ label, className, children }: { label: string; className?: string; children: React.ReactNode }) {
  return (
    <div className={cn("flex flex-col gap-1", className)}>
      <label className="text-xs text-muted-foreground">{label}</label>
      {children}
    </div>
  )
}
