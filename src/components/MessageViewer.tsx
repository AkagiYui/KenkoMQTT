import { useEffect, useMemo, useRef, useState } from "react"
import {
  ArrowDown, ArrowUp, Download, Search, Regex, CaseSensitive, WholeWord, Copy, ChevronsLeftRight,
  ChevronFirst, ChevronLast, ChevronLeft, ChevronRight, Braces, GitCompareArrows,
} from "lucide-react"
import {
  type Format, type MsgRow, type QueryOpts, FORMATS, messagesQuery, messagesClear, exportMessages, onMsgSignal,
} from "@/lib/api"
import { lineDiff } from "@/lib/diff"
import { useI18n } from "@/lib/i18n"
import { cn, formatTime, formatBytes, tryPrettyJSON } from "@/lib/utils"
import { JsonView } from "@/components/JsonView"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Card, CardContent } from "@/components/ui/card"
import { Separator } from "@/components/ui/separator"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

const PAGE_SIZE = 200
const HISTORY_KEY = "kenko-search-history"
const COLLAPSE = 800 // 超过该字符数的载荷默认折叠

function loadHistory(): string[] {
  try {
    return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]")
  } catch {
    return []
  }
}

function download(name: string, text: string) {
  const url = URL.createObjectURL(new Blob([text], { type: "text/plain;charset=utf-8" }))
  const a = document.createElement("a")
  a.href = url
  a.download = name
  a.click()
  URL.revokeObjectURL(url)
}

export function MessageViewer({ connId, name }: { connId: string; name: string }) {
  const { t } = useI18n()
  const [format, setFormat] = useState<Format>("plaintext")
  const [search, setSearch] = useState("")
  const [regex, setRegex] = useState(false)
  const [caseSensitive, setCaseSensitive] = useState(false)
  const [wholeWord, setWholeWord] = useState(false)
  const [ignoreQos0, setIgnoreQos0] = useState(false)
  const [dir, setDir] = useState<"all" | "rx" | "tx">("all")
  const [jsonTree, setJsonTree] = useState(false)
  const [diffMode, setDiffMode] = useState(false)

  const [rows, setRows] = useState<MsgRow[]>([])
  const [total, setTotal] = useState(0)
  const [follow, setFollow] = useState(true)
  const [page, setPage] = useState(0) // 仅 follow=false 时使用
  const [history, setHistory] = useState<string[]>(loadHistory)
  const [expanded, setExpanded] = useState<Set<number>>(new Set())
  const [diffSel, setDiffSel] = useState<MsgRow[]>([])
  const [menu, setMenu] = useState<{ x: number; y: number; row: MsgRow } | null>(null)

  const listRef = useRef<HTMLDivElement>(null)
  const throttle = useRef<ReturnType<typeof setTimeout> | null>(null)
  const connRef = useRef(connId)
  useEffect(() => {
    connRef.current = connId
  }, [connId])

  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE))
  const curPage = follow ? pageCount - 1 : Math.min(page, pageCount - 1)

  const opts: QueryOpts = useMemo(
    () => ({
      format,
      filter: search || null,
      regex,
      caseSensitive,
      wholeWord,
      ignoreQos0,
      dir: dir === "all" ? null : dir,
      offset: curPage * PAGE_SIZE,
      limit: PAGE_SIZE,
    }),
    [format, search, regex, caseSensitive, wholeWord, ignoreQos0, dir, curPage]
  )

  function refresh() {
    messagesQuery(connRef.current, {
      ...opts,
      offset: (follow ? Math.max(0, Math.ceil(total / PAGE_SIZE) - 1) : page) * PAGE_SIZE,
    })
      .then((p) => {
        setRows(p.rows)
        setTotal(p.total)
      })
      .catch(() => {})
  }

  // 选项变化时重新查询
  useEffect(() => {
    messagesQuery(connRef.current, opts)
      .then((p) => {
        setRows(p.rows)
        setTotal(p.total)
      })
      .catch(() => {})
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connId, format, search, regex, caseSensitive, wholeWord, ignoreQos0, dir, curPage])

  // 新消息信号：follow 时节流刷新
  useEffect(() => {
    const um = onMsgSignal((cid) => {
      if (cid !== connRef.current || !follow) return
      if (throttle.current) return
      throttle.current = setTimeout(() => {
        throttle.current = null
        refresh()
      }, 250)
    })
    return () => {
      um.then((f) => f())
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [follow, total])

  useEffect(() => {
    if (follow) listRef.current?.scrollTo({ top: listRef.current.scrollHeight })
  }, [rows.length, follow])

  useEffect(() => {
    const close = () => setMenu(null)
    window.addEventListener("click", close)
    return () => window.removeEventListener("click", close)
  }, [])

  // 来自主题树的「点击节点 → 以该主题过滤」
  useEffect(() => {
    const onSearch = (e: Event) => {
      const detail = (e as CustomEvent<string>).detail
      setFollow(true)
      commitSearch(detail)
    }
    window.addEventListener("viewer-search", onSearch as EventListener)
    return () => window.removeEventListener("viewer-search", onSearch as EventListener)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [history])

  function commitSearch(v: string) {
    setSearch(v)
    if (v.trim()) {
      const next = [v, ...history.filter((h) => h !== v)].slice(0, 15)
      setHistory(next)
      localStorage.setItem(HISTORY_KEY, JSON.stringify(next))
    }
  }

  function toggleExpand(ts: number) {
    setExpanded((prev) => {
      const n = new Set(prev)
      n.has(ts) ? n.delete(ts) : n.add(ts)
      return n
    })
  }

  function pickDiff(row: MsgRow) {
    setDiffSel((prev) => {
      if (prev.some((r) => r.ts === row.ts)) return prev.filter((r) => r.ts !== row.ts)
      return [...prev, row].slice(-2)
    })
  }

  async function doExport(kind: "csv" | "json" | "txt") {
    const text = await exportMessages(connId, kind, format).catch(() => "")
    download(`${name || "messages"}.${kind}`, text)
  }

  const diffOps = diffSel.length === 2 ? lineDiff(diffSel[0].payload, diffSel[1].payload) : null

  const toggleBtn = (active: boolean, onClick: () => void, icon: React.ReactNode, title: string) => (
    <Button
      type="button"
      variant={active ? "default" : "outline"}
      size="icon"
      className="size-8"
      onClick={onClick}
      title={title}
      aria-label={title}
    >
      {icon}
    </Button>
  )

  return (
    <Card>
      <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
        {/* 搜索工具条 */}
        <div className="flex flex-wrap items-center gap-2">
          <div className="relative flex-1 min-w-[180px]">
            <Search className="absolute left-2 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              list="search-history"
              value={search}
              onChange={(e) => commitSearch(e.target.value)}
              placeholder={t("搜索主题/内容（后端）")}
              className="h-8 pl-7"
            />
            <datalist id="search-history">
              {history.map((h) => (
                <option key={h} value={h} />
              ))}
            </datalist>
          </div>
          {toggleBtn(regex, () => setRegex((v) => !v), <Regex className="size-4" />, t("正则"))}
          {toggleBtn(caseSensitive, () => setCaseSensitive((v) => !v), <CaseSensitive className="size-4" />, t("区分大小写"))}
          {toggleBtn(wholeWord, () => setWholeWord((v) => !v), <WholeWord className="size-4" />, t("全词匹配"))}
          <Select value={format} onValueChange={(v) => setFormat(v as Format)}>
            <SelectTrigger className="h-8 w-28"><SelectValue /></SelectTrigger>
            <SelectContent>
              {FORMATS.map((f) => (
                <SelectItem key={f} value={f}>{f}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* 方向 / 视图 / 导出 */}
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <div className="flex rounded-md border border-border p-0.5">
            {(["all", "rx", "tx"] as const).map((d) => (
              <button
                key={d}
                onClick={() => setDir(d)}
                className={cn("rounded px-2 py-0.5", dir === d ? "bg-primary/15 text-primary" : "text-muted-foreground")}
              >
                {d === "all" ? t("全部") : d === "rx" ? t("收") : t("发")}
              </button>
            ))}
          </div>
          <label className="flex items-center gap-1 text-muted-foreground">
            <input type="checkbox" checked={ignoreQos0} onChange={(e) => setIgnoreQos0(e.target.checked)} />
            {t("忽略 QoS0")}
          </label>
          {toggleBtn(jsonTree, () => setJsonTree((v) => !v), <Braces className="size-4" />, t("JSON 树"))}
          {toggleBtn(diffMode, () => { setDiffMode((v) => !v); setDiffSel([]) }, <GitCompareArrows className="size-4" />, t("对比"))}
          <label className="flex items-center gap-1 text-muted-foreground">
            <input type="checkbox" checked={follow} onChange={(e) => setFollow(e.target.checked)} />
            {t("跟随最新")}
          </label>
          <div className="ml-auto flex gap-1.5">
            <Button variant="outline" size="sm" className="h-7 gap-1 text-xs" onClick={() => doExport("csv")}><Download className="size-3" />CSV</Button>
            <Button variant="outline" size="sm" className="h-7 gap-1 text-xs" onClick={() => doExport("json")}><Download className="size-3" />JSON</Button>
            <Button variant="outline" size="sm" className="h-7 gap-1 text-xs" onClick={() => doExport("txt")}><Download className="size-3" />TXT</Button>
            <Button variant="outline" size="sm" className="h-7 text-xs" onClick={() => { messagesClear(connId); setRows([]); setTotal(0) }}>{t("清空")}</Button>
          </div>
        </div>

        <Separator />

        {/* diff 面板 */}
        {diffMode && diffOps && (
          <div className="rounded-md border border-border bg-muted/30 p-2 font-mono text-xs">
            <div className="mb-1 text-muted-foreground">{t("对比")}: {diffSel[0].topic} ↔ {diffSel[1].topic}</div>
            {diffOps.map((op, i) => (
              <div
                key={i}
                className={cn(
                  "whitespace-pre-wrap break-all px-1",
                  op.type === "add" && "bg-success/20 text-success",
                  op.type === "del" && "bg-destructive/20 text-destructive"
                )}
              >
                {op.type === "add" ? "+ " : op.type === "del" ? "- " : "  "}
                {op.text}
              </div>
            ))}
          </div>
        )}

        {/* 消息列表 */}
        <div ref={listRef} className="flex max-h-[45vh] flex-col gap-2 overflow-y-auto">
          {rows.map((m, i) => {
            const long = m.payload.length > COLLAPSE
            const isOpen = expanded.has(m.ts)
            const shown = long && !isOpen ? m.payload.slice(0, COLLAPSE) + "…" : m.payload
            const selectedForDiff = diffSel.some((r) => r.ts === m.ts && r.topic === m.topic)
            return (
              <div
                key={`${m.ts}-${i}`}
                onClick={() => diffMode && pickDiff(m)}
                onContextMenu={(e) => {
                  e.preventDefault()
                  setMenu({ x: e.clientX, y: e.clientY, row: m })
                }}
                className={cn(
                  "rounded-md border-l-2 bg-muted/40 px-2.5 py-1.5",
                  m.dir === "rx" ? "border-l-success" : "border-l-primary",
                  diffMode && "cursor-pointer",
                  selectedForDiff && "ring-1 ring-primary"
                )}
              >
                <div className="flex flex-wrap items-baseline gap-2 text-xs">
                  <span className={cn("font-semibold", m.dir === "rx" ? "text-success" : "text-primary")}>
                    {m.dir === "rx" ? <ArrowDown className="inline size-3" /> : <ArrowUp className="inline size-3" />} {m.dir === "rx" ? t("收") : t("发")}
                  </span>
                  <span className="font-mono">{m.topic}</span>
                  <span className="ml-auto text-muted-foreground">
                    Q{m.qos}{m.retain ? " · retain" : ""} · {formatBytes(m.size)} · {formatTime(m.ts)}
                  </span>
                </div>
                {jsonTree && (format === "json" || m.payload.trimStart().startsWith("{") || m.payload.trimStart().startsWith("[")) ? (
                  <JsonView text={m.payload} className="mt-1 text-foreground/80" />
                ) : (
                  <pre className="mt-1 whitespace-pre-wrap break-all font-mono text-xs text-foreground/80">{tryPrettyJSON(shown)}</pre>
                )}
                {long && (
                  <button className="mt-0.5 flex items-center gap-1 text-[11px] text-primary" onClick={(e) => { e.stopPropagation(); toggleExpand(m.ts) }}>
                    <ChevronsLeftRight className="size-3" /> {isOpen ? t("收起") : t("展开完整")}
                  </button>
                )}
                {m.props && Object.keys(m.props).length > 0 && (
                  <div className="mt-1 flex flex-wrap gap-1 text-[10px] text-muted-foreground">
                    {m.props.contentType && <span className="rounded bg-muted px-1">type={m.props.contentType}</span>}
                    {m.props.responseTopic && <span className="rounded bg-muted px-1">resp={m.props.responseTopic}</span>}
                    {m.props.messageExpiryInterval != null && <span className="rounded bg-muted px-1">expiry={m.props.messageExpiryInterval}s</span>}
                    {m.props.topicAlias != null && <span className="rounded bg-muted px-1">alias={m.props.topicAlias}</span>}
                    {m.props.userProperties?.map((u, k) => (
                      <span key={k} className="rounded bg-muted px-1">{u.key}={u.value}</span>
                    ))}
                  </div>
                )}
              </div>
            )
          })}
          {rows.length === 0 && <div className="py-6 text-center text-sm text-muted-foreground">{t("暂无消息")}</div>}
        </div>

        {/* 分页 / 导航 */}
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span>{t("共 {total} 条", { total })}{search && ` · ${t("已过滤")}`}</span>
          <div className="flex items-center gap-1">
            <Button variant="ghost" size="icon" className="size-7" disabled={follow || curPage === 0} onClick={() => setPage(0)}><ChevronFirst className="size-4" /></Button>
            <Button variant="ghost" size="icon" className="size-7" disabled={follow || curPage === 0} onClick={() => setPage((p) => Math.max(0, p - 1))}><ChevronLeft className="size-4" /></Button>
            <span>{curPage + 1}/{pageCount}</span>
            <Button variant="ghost" size="icon" className="size-7" disabled={follow || curPage >= pageCount - 1} onClick={() => setPage((p) => Math.min(pageCount - 1, p + 1))}><ChevronRight className="size-4" /></Button>
            <Button variant="ghost" size="icon" className="size-7" disabled={follow || curPage >= pageCount - 1} onClick={() => setPage(pageCount - 1)}><ChevronLast className="size-4" /></Button>
          </div>
        </div>
      </CardContent>

      {/* 右键菜单 */}
      {menu && (
        <div
          className="fixed z-50 flex flex-col rounded-md border border-border bg-popover py-1 text-sm shadow-md"
          style={{ left: menu.x, top: menu.y }}
          onClick={(e) => e.stopPropagation()}
        >
          <button className="flex items-center gap-2 px-3 py-1 hover:bg-muted" onClick={() => { navigator.clipboard.writeText(menu.row.topic); setMenu(null) }}>
            <Copy className="size-3.5" /> {t("复制主题")}
          </button>
          <button className="flex items-center gap-2 px-3 py-1 hover:bg-muted" onClick={() => { navigator.clipboard.writeText(menu.row.payload); setMenu(null) }}>
            <Copy className="size-3.5" /> {t("复制内容")}
          </button>
        </div>
      )}
    </Card>
  )
}
