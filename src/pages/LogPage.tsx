import { useEffect, useState } from "react"
import { Trash2 } from "lucide-react"
import { type LogEntry, type LogLevel, subscribeLogs, clearLogs } from "@/lib/log"
import { useI18n } from "@/lib/i18n"
import { cn, formatTime } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"

const levelClass: Record<LogLevel, string> = {
  debug: "text-muted-foreground",
  info: "text-foreground",
  warn: "text-warning",
  error: "text-destructive",
}

export function LogPage() {
  const { t } = useI18n()
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [level, setLevel] = useState<LogLevel | "all">("all")

  useEffect(() => subscribeLogs(setLogs), [])

  const filters: { key: LogLevel | "all"; label: string }[] = [
    { key: "all", label: t("全部") },
    { key: "debug", label: t("调试") },
    { key: "info", label: t("信息") },
    { key: "warn", label: t("警告") },
    { key: "error", label: t("错误") },
  ]
  const shown = level === "all" ? logs : logs.filter((l) => l.level === level)

  return (
    <div className="mx-auto flex max-w-4xl flex-col gap-3 p-3 lg:max-w-6xl">
      <Card>
        <CardContent className="flex flex-col gap-2 p-3 sm:p-4">
          <div className="flex flex-wrap items-center gap-2">
            {filters.map((f) => (
              <button
                key={f.key}
                onClick={() => setLevel(f.key)}
                className={cn(
                  "rounded-full border px-2.5 py-1 text-xs transition-colors",
                  level === f.key ? "border-primary bg-primary/10 text-primary" : "border-border hover:bg-muted"
                )}
              >
                {f.label}
              </button>
            ))}
            <Button variant="outline" size="sm" className="ml-auto h-8 gap-1" onClick={clearLogs}>
              <Trash2 className="size-3.5" /> {t("清空日志")}
            </Button>
          </div>
          <div className="flex max-h-[70vh] flex-col gap-0.5 overflow-y-auto font-mono text-xs">
            {shown
              .slice()
              .reverse()
              .map((l) => (
                <div key={l.id} className="flex gap-2 border-b border-border/40 py-0.5">
                  <span className="shrink-0 text-muted-foreground">{formatTime(l.ts)}</span>
                  <span className={cn("shrink-0 font-semibold uppercase", levelClass[l.level])}>{l.level}</span>
                  <span className="shrink-0 text-muted-foreground">[{l.source}]</span>
                  <span className="break-all">{l.message}</span>
                </div>
              ))}
            {shown.length === 0 && <div className="py-6 text-center text-muted-foreground">{t("暂无日志")}</div>}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
