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
  // 消息查看器
  "搜索主题/内容（后端）": "Search topic/payload (backend)",
  正则: "Regex",
  区分大小写: "Case sensitive",
  全词匹配: "Whole word",
  收: "In",
  发: "Out",
  "忽略 QoS0": "Ignore QoS0",
  "JSON 树": "JSON tree",
  对比: "Diff",
  跟随最新: "Follow latest",
  收起: "Collapse",
  展开完整: "Expand full",
  "共 {total} 条": "{total} total",
  已过滤: "filtered",
  复制主题: "Copy topic",
  复制内容: "Copy payload",
  暂无消息: "No messages",
  // 分析 / 图表
  速率: "Rate",
  流量: "Traffic",
  负载: "Load",
  "收 (B)": "In (B)",
  "发 (B)": "Out (B)",
  "主题过滤（含通配符）": "Topic filter (wildcards)",
  主题树: "Topic tree",
  暂无主题: "No topics",
  按此主题过滤: "Filter by this topic",
  清空该节点消息: "Clear this node's messages",
  // 客户端页
  连接已保存: "Connection saved",
  保存失败: "Save failed",
  连接已删除: "Connection deleted",
  删除失败: "Delete failed",
  连接失败: "Connect failed",
  订阅失败: "Subscribe failed",
  发布失败: "Publish failed",
  定时发布已启动: "Scheduled publish started",
  定时发布失败: "Scheduled publish failed",
  已连接: "Connected",
  连接中: "Connecting",
  重连中: "Reconnecting",
  未连接: "Disconnected",
  协议: "Protocol",
  "MQTT 版本": "MQTT version",
  主机: "Host",
  端口: "Port",
  路径: "Path",
  留空自动生成: "Leave empty to auto-generate",
  用户名: "Username",
  密码: "Password",
  "跳过证书校验（自签名）": "Skip cert verify (self-signed)",
  "可选：CA 证书 (PEM)": "Optional: CA cert (PEM)",
  "遗嘱消息 (LWT)": "Will message (LWT)",
  遗嘱主题: "Will topic",
  遗嘱内容: "Will payload",
  订阅主题: "Subscribe topic",
  发布主题: "Publish topic",
  占位符: "Placeholders",
  定时: "Timer",
  停止: "Stop",
  开始: "Start",
  // 订阅面板 (二)
  该主题已订阅: "Topic already subscribed",
  "别名（可选）": "Alias (optional)",
  "MQTT5 选项": "MQTT5 options",
  无颜色: "No color",
  收藏: "Favorite",
  静音: "Mute",
  "启用/停用": "Enable/disable",
  退订: "Unsubscribe",
  // 连接高级 (一)
  连接测试成功: "Connection test OK",
  连接测试失败: "Connection test failed",
  已复制连接: "Connection duplicated",
  右键复制连接: "Right-click to duplicate",
  高级: "Advanced",
  分组: "Group",
  "连接超时(s)": "Connect timeout (s)",
  "重连间隔(ms)": "Reconnect interval (ms)",
  自动重连: "Auto reconnect",
  "ClientId 追加时间戳": "Append timestamp to ClientId",
  "随机 ClientId": "Random ClientId",
  "客户端证书 (PEM，双向 TLS)": "Client cert (PEM, mutual TLS)",
  "客户端私钥 (PEM)": "Client key (PEM)",
  "会话过期(s)": "Session expiry (s)",
  测试: "Test",
  "遗嘱延迟(s)": "Will delay (s)",
  "消息过期(s)": "Message expiry (s)",
  // 发布面板 (三)
  发布历史: "Publish history",
  暂无历史: "No history",
  "MQTT5 发布属性": "MQTT5 publish props",
  已清除保留消息: "Retained message cleared",
  清除保留: "Clear retained",
  向该主题发布空保留消息以清除: "Publish empty retained to this topic to clear",
  // 订阅增强
  "订阅主题（多个用逗号/换行）": "Topics (comma/newline separated)",
  默认格式: "Default format",
  全订阅: "Sub all",
  全部订阅: "Subscribe all",
  已全部订阅: "All subscribed",
  // 仪表盘
  仪表盘: "Dashboard",
  添加组件: "Add widget",
  "暂无组件，点击「添加组件」": "No widgets — click Add widget",
  类型: "Type",
  标题: "Title",
  "宽度(列)": "Width (cols)",
  单位: "Unit",
  添加: "Add",
  // 连接树
  未分组: "Ungrouped",
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
