import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

/** 合并 Tailwind 类名，智能处理冲突。 */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/** 将毫秒时间戳格式化为 HH:MM:SS.mmm。 */
export function formatTime(ms: number): string {
  const d = new Date(ms)
  const pad = (n: number, w = 2) => String(n).padStart(w, "0")
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`
}

/** 将秒数格式化为人类可读的时长（如 1h 02m 03s）。 */
export function formatUptime(seconds: number): string {
  if (seconds <= 0) return "0s"
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = Math.floor(seconds % 60)
  const parts: string[] = []
  if (h > 0) parts.push(`${h}h`)
  if (h > 0 || m > 0) parts.push(`${String(m).padStart(2, "0")}m`)
  parts.push(`${String(s).padStart(2, "0")}s`)
  return parts.join(" ")
}

/** 将字节数格式化为人类可读大小。 */
export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  const units = ["KB", "MB", "GB", "TB"]
  let v = bytes / 1024
  let i = 0
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${v.toFixed(1)} ${units[i]}`
}

/** 若字符串是 JSON，返回美化后的文本，否则原样返回。 */
export function tryPrettyJSON(text: string): string {
  const trimmed = text.trim()
  if (!trimmed || (trimmed[0] !== "{" && trimmed[0] !== "[")) return text
  try {
    return JSON.stringify(JSON.parse(trimmed), null, 2)
  } catch {
    return text
  }
}
