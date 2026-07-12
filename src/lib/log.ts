// 轻量应用内日志总线：各处调用 pushLog() 记录事件，LogPage 订阅展示。
export type LogLevel = "debug" | "info" | "warn" | "error"

export interface LogEntry {
  id: number
  ts: number
  level: LogLevel
  source: string
  message: string
}

const MAX = 2000
let seq = 0
let entries: LogEntry[] = []
const listeners = new Set<(e: LogEntry[]) => void>()

function emit() {
  const snapshot = entries
  listeners.forEach((l) => l(snapshot))
}

export function pushLog(level: LogLevel, source: string, message: string) {
  entries = [...entries, { id: ++seq, ts: Date.now(), level, source, message }]
  if (entries.length > MAX) entries = entries.slice(entries.length - MAX)
  emit()
}

export function clearLogs() {
  entries = []
  emit()
}

export function getLogs() {
  return entries
}

/** 订阅日志变化，返回取消订阅函数。 */
export function subscribeLogs(cb: (e: LogEntry[]) => void) {
  listeners.add(cb)
  cb(entries)
  return () => {
    listeners.delete(cb)
  }
}
