import { Moon, Sun, Github } from "lucide-react"
import { useTheme } from "@/lib/theme"
import { ClientPage } from "@/pages/ClientPage"
import { Button } from "@/components/ui/button"
import { Toaster } from "@/components/ui/sonner"

export default function App() {
  const { theme, toggle } = useTheme()
  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="sticky top-0 z-10 flex items-center justify-between gap-2 border-b border-border bg-background/80 px-4 py-2 backdrop-blur">
        <h1 className="text-sm font-bold">
          KenkoMQTT <span className="font-normal text-muted-foreground">客户端</span>
        </h1>
        <div className="flex items-center gap-1">
          <Button variant="ghost" size="icon" className="size-8" onClick={toggle} aria-label="切换主题">
            {theme === "dark" ? <Sun className="size-4" /> : <Moon className="size-4" />}
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="size-8"
            aria-label="GitHub"
            onClick={() => window.open("https://github.com/AkagiYui/KenkoMQTT", "_blank")}
          >
            <Github className="size-4" />
          </Button>
        </div>
      </header>
      <ClientPage />
      <Toaster />
    </div>
  )
}
