import { createContext, useContext, useEffect, useState, type ReactNode } from "react"

export type Lang = "zh" | "en"

const STORAGE_KEY = "kenkomqtt-lang"

// gettext 风格：以中文原文为 key，en 提供覆盖；缺失时回退中文。
// 这样为既有中文界面加 i18n 只需把字符串包一层 t()，无需另造 key。
const en: Record<string, string> = {
  // 导航 / 外壳
  客户端: "Client",
  Broker: "Broker",
  日志: "Logs",
  设置: "Settings",
  切换主题: "Toggle theme",
  切换语言: "Toggle language",
  新窗口: "New window",
  // 主题
  浅色: "Light",
  深色: "Dark",
  夜间: "Night",
  // Android 电池
  建议关闭电池优化: "Recommend disabling battery optimization",
  "Android 后台限制可能在切后台时暂停 Broker/客户端连接。允许「无限制」后台运行可保持稳定。":
    "Android background limits may pause the broker/client when backgrounded. Allowing unrestricted background keeps it stable.",
  去设置: "Open settings",
  应用详情: "App details",
  关闭: "Close",
  // 设置页
  语言: "Language",
  主题: "Theme",
  连接档案: "Connections",
  导入: "Import",
  导出: "Export",
  "导入/导出连接": "Import / Export connections",
  "从文件导入连接档案，或导出为多种格式。": "Import connection profiles from a file, or export to various formats.",
  格式: "Format",
  已导出: "Exported",
  导入成功: "Import succeeded",
  导入失败: "Import failed",
  导出失败: "Export failed",
  "共 {n} 个连接": "{n} connections",
  "已导入 {n} 个连接": "Imported {n} connections",
  关于: "About",
  本地_MQTT_调试工具: "Local MQTT debugging tool",
  // 日志页
  清空日志: "Clear logs",
  暂无日志: "No logs",
  全部: "All",
  信息: "Info",
  警告: "Warn",
  错误: "Error",
  调试: "Debug",
  // 通用
  连接: "Connect",
  断开: "Disconnect",
  保存: "Save",
  删除: "Delete",
  新建: "New",
  订阅: "Subscribe",
  发布: "Publish",
  复制: "Copy",
  主题_topic: "Topic",
  内容: "Payload",
  已复制: "Copied",
  清空: "Clear",
}

interface I18nCtx {
  lang: Lang
  setLang: (l: Lang) => void
  t: (s: string, params?: Record<string, string | number>) => string
}

const Ctx = createContext<I18nCtx | null>(null)

function interpolate(text: string, params?: Record<string, string | number>): string {
  if (!params) return text
  return text.replace(/\{(\w+)\}/g, (_, k) => (k in params ? String(params[k]) : `{${k}}`))
}

export function I18nProvider({ children }: { children: ReactNode }) {
  const [lang, setLangState] = useState<Lang>(() => {
    const saved = localStorage.getItem(STORAGE_KEY) as Lang | null
    if (saved === "zh" || saved === "en") return saved
    return navigator.language.toLowerCase().startsWith("zh") ? "zh" : "en"
  })

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, lang)
    document.documentElement.lang = lang === "zh" ? "zh-CN" : "en"
  }, [lang])

  const t = (s: string, params?: Record<string, string | number>) => {
    const base = lang === "en" ? en[s] ?? s : s
    return interpolate(base, params)
  }

  return <Ctx.Provider value={{ lang, setLang: setLangState, t }}>{children}</Ctx.Provider>
}

export function useI18n(): I18nCtx {
  const ctx = useContext(Ctx)
  if (!ctx) throw new Error("useI18n 必须在 I18nProvider 内使用")
  return ctx
}
