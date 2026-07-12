import { useEffect, useState } from "react"

export type Theme = "light" | "dark" | "night"

const STORAGE_KEY = "kenkomqtt-theme"
export const THEMES: Theme[] = ["light", "dark", "night"]

function apply(theme: Theme) {
  const root = document.documentElement
  // night 复用 dark 的所有 dark:* 变体，再叠加 .night 覆盖调色板为纯黑（OLED 友好）。
  root.classList.toggle("dark", theme === "dark" || theme === "night")
  root.classList.toggle("night", theme === "night")
}

/** 主题状态 hook：读取 localStorage，切换时持久化并更新 <html> 的 class。 */
export function useTheme() {
  const [theme, setTheme] = useState<Theme>(() => {
    const saved = localStorage.getItem(STORAGE_KEY) as Theme | null
    if (saved === "light" || saved === "dark" || saved === "night") return saved
    return window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark"
  })

  useEffect(() => {
    apply(theme)
    localStorage.setItem(STORAGE_KEY, theme)
  }, [theme])

  // 循环切换 light → dark → night → light
  const toggle = () => setTheme((t) => THEMES[(THEMES.indexOf(t) + 1) % THEMES.length])
  return { theme, setTheme, toggle }
}
