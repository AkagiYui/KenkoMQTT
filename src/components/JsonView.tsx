import { useState } from "react"
import { ChevronRight, ChevronDown } from "lucide-react"
import { cn } from "@/lib/utils"

// 轻量 JSON 树 / 语法高亮，无外部依赖。

function Node({ k, value, depth }: { k: string | null; value: unknown; depth: number }) {
  const [open, setOpen] = useState(depth < 2)
  const isObj = value !== null && typeof value === "object"
  const isArr = Array.isArray(value)
  const keyEl = k !== null && <span className="text-primary">{k}</span>

  if (!isObj) {
    let cls = "text-foreground"
    if (typeof value === "string") cls = "text-success"
    else if (typeof value === "number") cls = "text-warning"
    else if (typeof value === "boolean") cls = "text-destructive"
    return (
      <div className="flex gap-1" style={{ paddingLeft: depth * 12 }}>
        {keyEl}
        {k !== null && <span className="text-muted-foreground">:</span>}
        <span className={cls}>{typeof value === "string" ? `"${value}"` : String(value)}</span>
      </div>
    )
  }

  const entries = isArr ? (value as unknown[]).map((v, i) => [String(i), v] as const) : Object.entries(value as object)
  return (
    <div style={{ paddingLeft: depth * 12 }}>
      <button className="flex items-center gap-1" onClick={() => setOpen((o) => !o)}>
        {open ? <ChevronDown className="size-3" /> : <ChevronRight className="size-3" />}
        {keyEl}
        {k !== null && <span className="text-muted-foreground">:</span>}
        <span className="text-muted-foreground">
          {isArr ? `[${entries.length}]` : `{${entries.length}}`}
        </span>
      </button>
      {open && entries.map(([ck, cv]) => <Node key={ck} k={ck} value={cv} depth={depth + 1} />)}
    </div>
  )
}

export function JsonView({ text, className }: { text: string; className?: string }) {
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch {
    return <pre className={cn("whitespace-pre-wrap break-all", className)}>{text}</pre>
  }
  return (
    <div className={cn("font-mono text-xs", className)}>
      <Node k={null} value={parsed} depth={0} />
    </div>
  )
}
