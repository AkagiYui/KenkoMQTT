import { useEffect, useMemo, useState } from "react"
import { Plus, X, LayoutDashboard, GripHorizontal, Pencil, Trash2 } from "lucide-react"
import { Responsive, WidthProvider, type Layout } from "react-grid-layout"
import "react-grid-layout/css/styles.css"
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

const ResponsiveGrid = WidthProvider(Responsive)

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
}

interface Dashboard {
  id: string
  name: string
  widgets: Widget[]
  layout: Layout[]
}

interface Store {
  dashboards: Dashboard[]
  activeId: string
}

const STORAGE_KEY = "kenko-dashboards-v2"
const OLD_KEY = "kenko-dashboard-widgets"

function makeDashboard(name: string): Dashboard {
  return { id: crypto.randomUUID(), name, widgets: [], layout: [] }
}

function loadStore(): Store {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (raw) return JSON.parse(raw)
  } catch {
    /* ignore */
  }
  // 从旧的单一仪表盘迁移
  const d = makeDashboard("仪表盘 1")
  try {
    const old = JSON.parse(localStorage.getItem(OLD_KEY) || "[]") as (Widget & { size?: number })[]
    old.forEach((w, i) => {
      d.widgets.push({ ...w })
      d.layout.push({ i: w.id, x: (i * 3) % 12, y: Math.floor(i / 4) * 4, w: (w.size ?? 1) * 3, h: 4 })
    })
  } catch {
    /* ignore */
  }
  return { dashboards: [d], activeId: d.id }
}

export function DashboardPage() {
  const { t } = useI18n()
  const [store, setStore] = useState<Store>(loadStore)
  const [profiles, setProfiles] = useState<Profile[]>([])
  const [adding, setAdding] = useState(false)
  const [renaming, setRenaming] = useState(false)

  useEffect(() => {
    listProfiles().then(setProfiles).catch(() => {})
  }, [])

  function persist(next: Store) {
    setStore(next)
    localStorage.setItem(STORAGE_KEY, JSON.stringify(next))
  }

  const active = store.dashboards.find((d) => d.id === store.activeId) ?? store.dashboards[0]

  function updateActive(patch: Partial<Dashboard>) {
    persist({ ...store, dashboards: store.dashboards.map((d) => (d.id === active.id ? { ...d, ...patch } : d)) })
  }

  function addDashboard() {
    const d = makeDashboard(`${t("仪表盘")} ${store.dashboards.length + 1}`)
    persist({ dashboards: [...store.dashboards, d], activeId: d.id })
  }

  function deleteDashboard() {
    if (store.dashboards.length <= 1) return
    const rest = store.dashboards.filter((d) => d.id !== active.id)
    persist({ dashboards: rest, activeId: rest[0].id })
  }

  function addWidget(w: Widget) {
    const layout: Layout = { i: w.id, x: (active.widgets.length * 3) % 12, y: 9999, w: 3, h: 4, minW: 2, minH: 3 }
    updateActive({ widgets: [...active.widgets, w], layout: [...active.layout, layout] })
    setAdding(false)
  }

  function removeWidget(id: string) {
    updateActive({ widgets: active.widgets.filter((w) => w.id !== id), layout: active.layout.filter((l) => l.i !== id) })
  }

  function onLayoutChange(current: Layout[]) {
    updateActive({ layout: current })
  }

  const layouts = useMemo(
    () => ({ lg: active.layout, md: active.layout, sm: active.layout, xs: active.layout, xxs: active.layout }),
    [active.layout]
  )

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-3 p-3">
      {/* 仪表盘选择条 */}
      <div className="flex flex-wrap items-center gap-2">
        <LayoutDashboard className="size-4 text-muted-foreground" />
        <div className="flex flex-wrap items-center gap-1">
          {store.dashboards.map((d) =>
            renaming && d.id === active.id ? (
              <Input
                key={d.id}
                autoFocus
                defaultValue={d.name}
                className="h-7 w-32 text-xs"
                onBlur={(e) => {
                  updateActive({ name: e.target.value || d.name })
                  setRenaming(false)
                }}
                onKeyDown={(e) => e.key === "Enter" && (e.target as HTMLInputElement).blur()}
              />
            ) : (
              <button
                key={d.id}
                onClick={() => persist({ ...store, activeId: d.id })}
                className={cn(
                  "rounded-full border px-3 py-1 text-xs transition-colors",
                  d.id === active.id ? "border-primary bg-primary/10 text-primary" : "border-border hover:bg-muted"
                )}
              >
                {d.name}
              </button>
            )
          )}
          <Button variant="ghost" size="icon" className="size-7" title={t("新建仪表盘")} onClick={addDashboard}>
            <Plus className="size-3.5" />
          </Button>
        </div>
        <div className="ml-auto flex items-center gap-1">
          <Button variant="ghost" size="icon" className="size-7" title={t("重命名")} onClick={() => setRenaming(true)}>
            <Pencil className="size-3.5" />
          </Button>
          <Button variant="ghost" size="icon" className="size-7 text-destructive" title={t("删除仪表盘")} onClick={deleteDashboard} disabled={store.dashboards.length <= 1}>
            <Trash2 className="size-3.5" />
          </Button>
          <Button variant="outline" size="sm" className="h-7 gap-1" onClick={() => setAdding((v) => !v)}>
            <Plus className="size-3.5" /> {t("添加组件")}
          </Button>
        </div>
      </div>

      {adding && <AddWidgetForm profiles={profiles} onAdd={addWidget} />}

      {active.widgets.length === 0 ? (
        <div className="py-16 text-center text-sm text-muted-foreground">{t("暂无组件，点击「添加组件」")}</div>
      ) : (
        <ResponsiveGrid
          className="layout"
          layouts={layouts}
          breakpoints={{ lg: 1024, md: 768, sm: 640, xs: 480, xxs: 0 }}
          cols={{ lg: 12, md: 10, sm: 6, xs: 4, xxs: 2 }}
          rowHeight={40}
          margin={[12, 12]}
          draggableHandle=".widget-drag"
          onLayoutChange={(cur) => onLayoutChange(cur)}
        >
          {active.widgets.map((w) => (
            <div key={w.id} className="h-full">
              <WidgetView widget={w} onRemove={() => removeWidget(w.id)} />
            </div>
          ))}
        </ResponsiveGrid>
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

  useEffect(() => {
    if (!connId && profiles[0]) setConnId(profiles[0].id)
  }, [profiles, connId])

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
        <Field label={t("主题过滤（含通配符）")} className="col-span-2 sm:col-span-1">
          <Input value={topicFilter} onChange={(e) => setTopicFilter(e.target.value)} className="h-8 font-mono" placeholder="sensor/+/temp" />
        </Field>
        <Field label="JSONPath" className="col-span-2">
          <Input value={jsonpath} onChange={(e) => setJsonpath(e.target.value)} className="h-8 font-mono" placeholder="$.value" />
        </Field>
        {type !== "line" && (
          <Field label={t("单位")}>
            <Input value={unit} onChange={(e) => setUnit(e.target.value)} className="h-8" />
          </Field>
        )}
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
        <div className="col-span-full flex justify-end">
          <Button
            size="sm"
            className="h-8"
            disabled={!connId}
            onClick={() =>
              onAdd({ id: crypto.randomUUID(), type, connId, title: title || topicFilter || "widget", topicFilter, jsonpath, unit, min, max, color: "#3b82f6" })
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
    <Card className="flex h-full flex-col overflow-hidden">
      <div className="widget-drag flex cursor-move items-center gap-1 border-b border-border/50 px-2 py-1">
        <GripHorizontal className="size-3.5 text-muted-foreground" />
        <span className="truncate text-xs font-medium text-muted-foreground">{w.title}</span>
        <button className="ml-auto text-muted-foreground hover:text-destructive" onMouseDown={(e) => e.stopPropagation()} onClick={onRemove}>
          <X className="size-3.5" />
        </button>
      </div>
      <CardContent className="flex flex-1 flex-col justify-center overflow-hidden p-2">
        {w.type === "big" && (
          <div className="flex items-baseline justify-center gap-1">
            <span className={cn("font-semibold", String(display).length > 8 ? "text-2xl" : "text-4xl")}>{display}</span>
            {w.unit && <span className="text-sm text-muted-foreground">{w.unit}</span>}
          </div>
        )}
        {w.type === "gauge" && <Gauge value={num ?? NaN} min={w.min} max={w.max} unit={w.unit} color={w.color} />}
        {w.type === "line" && <LineChart data={line} label={w.jsonpath} height={100} stroke={w.color} />}
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
