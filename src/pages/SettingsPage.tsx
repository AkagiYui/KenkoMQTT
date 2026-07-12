import { useRef, useState } from "react"
import { Upload, Download, Languages, Palette } from "lucide-react"
import { toast } from "sonner"
import { type Theme, THEMES } from "@/lib/theme"
import { useI18n } from "@/lib/i18n"
import { pushLog } from "@/lib/log"
import { type ProfileFormat, PROFILE_FORMATS, exportProfiles, importProfiles } from "@/lib/api"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

function b64ToBlob(b64: string, mime: string): Blob {
  const bin = atob(b64)
  const arr = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i)
  return new Blob([arr], { type: mime })
}

function saveBlob(name: string, blob: Blob) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement("a")
  a.href = url
  a.download = name
  a.click()
  URL.revokeObjectURL(url)
}

export function SettingsPage({ theme, setTheme }: { theme: Theme; setTheme: (t: Theme) => void }) {
  const { t, lang, setLang } = useI18n()
  const [format, setFormat] = useState<ProfileFormat>("json")
  const fileRef = useRef<HTMLInputElement>(null)

  const themeLabel: Record<Theme, string> = { light: t("浅色"), dark: t("深色"), night: t("夜间") }

  async function handleExport() {
    try {
      const out = await exportProfiles(format)
      const mime = format === "xlsx" ? "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" : "text/plain;charset=utf-8"
      const blob = out.base64 ? b64ToBlob(out.content, mime) : new Blob([out.content], { type: mime })
      saveBlob(out.filename, blob)
      toast.success(t("已导出"), { description: out.filename })
      pushLog("info", "settings", `export ${format} -> ${out.filename}`)
    } catch (e: any) {
      toast.error(t("导出失败"), { description: String(e?.message ?? e) })
    }
  }

  async function handleImportFile(file: File) {
    try {
      const dataUrl: string = await new Promise((res, rej) => {
        const r = new FileReader()
        r.onload = () => res(String(r.result))
        r.onerror = () => rej(r.error)
        r.readAsDataURL(file)
      })
      const b64 = dataUrl.split(",")[1] ?? ""
      const n = await importProfiles(format, b64)
      toast.success(t("导入成功"), { description: t("已导入 {n} 个连接", { n }) })
      pushLog("info", "settings", `import ${format}: ${n}`)
      window.dispatchEvent(new CustomEvent("profiles-changed"))
    } catch (e: any) {
      toast.error(t("导入失败"), { description: String(e?.message ?? e) })
      pushLog("error", "settings", `import failed: ${String(e?.message ?? e)}`)
    }
  }

  return (
    <div className="mx-auto flex max-w-2xl flex-col gap-3 p-3">
      {/* 外观 */}
      <Card>
        <CardContent className="flex flex-col gap-3 p-4">
          <div className="flex items-center gap-2 text-sm font-medium">
            <Palette className="size-4" /> {t("主题")}
          </div>
          <div className="flex gap-2">
            {THEMES.map((th) => (
              <button
                key={th}
                onClick={() => setTheme(th)}
                className={cn(
                  "flex-1 rounded-md border px-3 py-2 text-sm transition-colors",
                  theme === th ? "border-primary bg-primary/10 text-primary" : "border-border hover:bg-muted"
                )}
              >
                {themeLabel[th]}
              </button>
            ))}
          </div>
          <div className="mt-2 flex items-center gap-2 text-sm font-medium">
            <Languages className="size-4" /> {t("语言")}
          </div>
          <div className="flex gap-2">
            {(["zh", "en"] as const).map((l) => (
              <button
                key={l}
                onClick={() => setLang(l)}
                className={cn(
                  "flex-1 rounded-md border px-3 py-2 text-sm transition-colors",
                  lang === l ? "border-primary bg-primary/10 text-primary" : "border-border hover:bg-muted"
                )}
              >
                {l === "zh" ? "中文" : "English"}
              </button>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* 导入 / 导出连接 */}
      <Card>
        <CardContent className="flex flex-col gap-3 p-4">
          <div className="text-sm font-medium">{t("导入/导出连接")}</div>
          <p className="text-xs text-muted-foreground">{t("从文件导入连接档案，或导出为多种格式。")}</p>
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs text-muted-foreground">{t("格式")}</span>
            <Select value={format} onValueChange={(v) => setFormat(v as ProfileFormat)}>
              <SelectTrigger className="h-9 w-28"><SelectValue /></SelectTrigger>
              <SelectContent>
                {PROFILE_FORMATS.map((f) => (
                  <SelectItem key={f} value={f}>{f.toUpperCase()}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button className="h-9 gap-1.5" onClick={handleExport}>
              <Download className="size-4" /> {t("导出")}
            </Button>
            <Button variant="outline" className="h-9 gap-1.5" onClick={() => fileRef.current?.click()}>
              <Upload className="size-4" /> {t("导入")}
            </Button>
            <input
              ref={fileRef}
              type="file"
              className="hidden"
              onChange={(e) => {
                const f = e.target.files?.[0]
                if (f) handleImportFile(f)
                e.target.value = ""
              }}
            />
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
