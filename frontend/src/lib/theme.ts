import { useEffect, useState } from "react"

export type Theme = "light" | "dark"

const STORAGE_KEY = "kenkomqtt-theme"

function apply(theme: Theme) {
  const root = document.documentElement
  root.classList.toggle("dark", theme === "dark")
}

/** 主题状态 hook：读取 localStorage，切换时持久化并更新 <html> 的 class。 */
export function useTheme() {
  const [theme, setTheme] = useState<Theme>(() => {
    const saved = localStorage.getItem(STORAGE_KEY) as Theme | null
    if (saved === "light" || saved === "dark") return saved
    return window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark"
  })

  useEffect(() => {
    apply(theme)
    localStorage.setItem(STORAGE_KEY, theme)
  }, [theme])

  const toggle = () => setTheme((t) => (t === "dark" ? "light" : "dark"))
  return { theme, setTheme, toggle }
}
