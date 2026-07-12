import { useEffect, useState } from "react"
import { Trash2, ChevronRight, ChevronDown } from "lucide-react"
import {
  type TreeNode, type LoadMethod, chartRate, chartTraffic, chartLoad, chartContent, topicTree, messagesClearTopic,
} from "@/lib/api"
import { useI18n } from "@/lib/i18n"
import { cn } from "@/lib/utils"
import { LineChart } from "@/components/LineChart"
import { MultiLineChart } from "@/components/MultiLineChart"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent } from "@/components/ui/card"
import { Separator } from "@/components/ui/separator"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

type ChartTab = "rate" | "traffic" | "load" | "content"
const LOAD_METHODS: LoadMethod[] = ["count", "avg", "sum", "max", "min"]

export function Analysis({ connId, connected }: { connId: string; connected: boolean }) {
  const { t } = useI18n()
  const [tab, setTab] = useState<ChartTab>("rate")
  const [rate, setRate] = useState<[number[], number[]]>([[], []])
  const [traffic, setTraffic] = useState<number[][]>([[], [], []])
  const [load, setLoad] = useState<[number[], number[]]>([[], []])
  const [content, setContent] = useState<[number[], number[]]>([[], []])
  const [tree, setTree] = useState<TreeNode[]>([])

  const [loadTopic, setLoadTopic] = useState("")
  const [loadMethod, setLoadMethod] = useState<LoadMethod>("count")
  const [contentTopic, setContentTopic] = useState("")
  const [contentPath, setContentPath] = useState("$.value")

  useEffect(() => {
    if (!connected) return
    let alive = true
    const tick = async () => {
      const [r, tr, tn] = await Promise.all([
        chartRate(connId, 1000, 60).catch(() => []),
        chartTraffic(connId, 1000, 60).catch(() => []),
        topicTree(connId, "plaintext").catch(() => []),
      ])
      const lo = await chartLoad(connId, loadTopic, loadMethod, 1000, 60).catch(() => [])
      const co = contentPath ? await chartContent(connId, contentTopic, contentPath, 200).catch(() => []) : []
      if (!alive) return
      setRate([r.map((p) => p.t / 1000), r.map((p) => p.v)])
      setTraffic([tr.map((p) => p.t / 1000), tr.map((p) => p.rxBytes), tr.map((p) => p.txBytes)])
      setLoad([lo.map((p) => p.t / 1000), lo.map((p) => p.v)])
      setContent([co.map((p) => p.t / 1000), co.map((p) => p.v)])
      setTree(tn)
    }
    tick()
    const h = setInterval(tick, 1500)
    return () => {
      alive = false
      clearInterval(h)
    }
  }, [connId, connected, loadTopic, loadMethod, contentTopic, contentPath])

  const tabs: { key: ChartTab; label: string }[] = [
    { key: "rate", label: t("速率") },
    { key: "traffic", label: t("流量") },
    { key: "load", label: t("负载") },
    { key: "content", label: t("内容") },
  ]

  return (
    <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
      <Card>
        <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
          <div className="flex flex-wrap items-center gap-1">
            {tabs.map((tb) => (
              <button
                key={tb.key}
                onClick={() => setTab(tb.key)}
                className={cn("rounded-full px-2.5 py-1 text-xs", tab === tb.key ? "bg-primary/15 text-primary" : "text-muted-foreground hover:bg-muted")}
              >
                {tb.label}
              </button>
            ))}
          </div>
          <Separator />
          {tab === "rate" && <LineChart data={rate} label="msg/s" />}
          {tab === "traffic" && (
            <MultiLineChart
              data={traffic}
              series={[
                { label: t("收 (B)"), stroke: "#22c55e" },
                { label: t("发 (B)"), stroke: "#3b82f6" },
              ]}
            />
          )}
          {tab === "load" && (
            <div className="flex flex-col gap-2">
              <div className="flex gap-2">
                <Input value={loadTopic} onChange={(e) => setLoadTopic(e.target.value)} placeholder={t("主题过滤（含通配符）")} className="h-8" />
                <Select value={loadMethod} onValueChange={(v) => setLoadMethod(v as LoadMethod)}>
                  <SelectTrigger className="h-8 w-28"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    {LOAD_METHODS.map((m) => (
                      <SelectItem key={m} value={m}>{m}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <LineChart data={load} label={`size ${loadMethod}`} stroke="#a855f7" />
            </div>
          )}
          {tab === "content" && (
            <div className="flex flex-col gap-2">
              <div className="flex gap-2">
                <Input value={contentTopic} onChange={(e) => setContentTopic(e.target.value)} placeholder={t("主题过滤（含通配符）")} className="h-8" />
                <Input value={contentPath} onChange={(e) => setContentPath(e.target.value)} placeholder="JSONPath 如 $.value" className="h-8 w-40 font-mono" />
              </div>
              <LineChart data={content} label={contentPath} stroke="#f59e0b" />
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
          <span className="text-sm font-medium">{t("主题树")}</span>
          <Separator />
          <div className="flex max-h-[40vh] flex-col overflow-y-auto text-xs">
            {tree.map((n) => (
              <TreeItem key={n.full} node={n} depth={0} connId={connId} />
            ))}
            {tree.length === 0 && <div className="py-6 text-center text-muted-foreground">{t("暂无主题")}</div>}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function TreeItem({ node, depth, connId }: { node: TreeNode; depth: number; connId: string }) {
  const { t } = useI18n()
  const [open, setOpen] = useState(depth < 2)
  const hasChildren = node.children.length > 0
  return (
    <div>
      <div className="group flex items-center gap-1 py-0.5" style={{ paddingLeft: depth * 12 }}>
        {hasChildren ? (
          <button onClick={() => setOpen((o) => !o)} className="text-muted-foreground">
            {open ? <ChevronDown className="size-3" /> : <ChevronRight className="size-3" />}
          </button>
        ) : (
          <span className="w-3" />
        )}
        <button
          className="truncate font-mono hover:text-primary"
          title={t("按此主题过滤")}
          onClick={() => window.dispatchEvent(new CustomEvent("viewer-search", { detail: node.full }))}
        >
          {node.name}
        </button>
        {node.count > 0 && <Badge variant="secondary" className="h-4 px-1 text-[10px]">{node.count}</Badge>}
        {node.latest && <span className="truncate text-muted-foreground">= {node.latest}</span>}
        <button
          className="ml-auto opacity-0 transition-opacity group-hover:opacity-100"
          title={t("清空该节点消息")}
          onClick={() => {
            messagesClearTopic(connId, node.full)
            messagesClearTopic(connId, node.full + "/#")
          }}
        >
          <Trash2 className="size-3 text-destructive" />
        </button>
      </div>
      {open && node.children.map((c) => <TreeItem key={c.full} node={c} depth={depth + 1} connId={connId} />)}
    </div>
  )
}
