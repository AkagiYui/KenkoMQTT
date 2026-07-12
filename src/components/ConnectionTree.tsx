import { useMemo, useState } from "react"
import { ChevronDown, ChevronRight, GripVertical, Plus, Copy } from "lucide-react"
import { type Profile, type Status, reorderProfiles } from "@/lib/api"
import { useI18n } from "@/lib/i18n"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"

const UNGROUPED = "__ungrouped__"

export function ConnectionTree({
  profiles,
  statusMap,
  selectedId,
  isDraft,
  onSelect,
  onDuplicate,
  onNew,
  onReordered,
}: {
  profiles: Profile[]
  statusMap: Record<string, Status>
  selectedId: string
  isDraft: boolean
  onSelect: (p: Profile) => void
  onDuplicate: (p: Profile) => void
  onNew: () => void
  onReordered: () => void
}) {
  const { t } = useI18n()
  const [dragId, setDragId] = useState<string | null>(null)
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set())

  const groups = useMemo(() => {
    const m = new Map<string, Profile[]>()
    for (const p of profiles) {
      const g = p.group?.trim() || UNGROUPED
      if (!m.has(g)) m.set(g, [])
      m.get(g)!.push(p)
    }
    return [...m.entries()]
  }, [profiles])

  async function persist(list: Profile[]) {
    await reorderProfiles(list.map((p, i) => ({ id: p.id, sortOrder: i, group: p.group?.trim() || "" })))
    onReordered()
  }

  // 将 dragId 插入到 targetId 之前，并归入 targetGroup。
  async function dropOnItem(targetId: string, targetGroup: string) {
    if (!dragId || dragId === targetId) return
    const list = profiles.slice()
    const from = list.findIndex((p) => p.id === dragId)
    if (from < 0) return
    const [moved] = list.splice(from, 1)
    moved.group = targetGroup === UNGROUPED ? "" : targetGroup
    const to = list.findIndex((p) => p.id === targetId)
    list.splice(to < 0 ? list.length : to, 0, moved)
    setDragId(null)
    await persist(list)
  }

  // 拖到分组标题：移动到该分组末尾。
  async function dropOnGroup(group: string) {
    if (!dragId) return
    const list = profiles.slice()
    const from = list.findIndex((p) => p.id === dragId)
    if (from < 0) return
    const [moved] = list.splice(from, 1)
    moved.group = group === UNGROUPED ? "" : group
    list.push(moved)
    setDragId(null)
    await persist(list)
  }

  function toggle(g: string) {
    setCollapsed((prev) => {
      const n = new Set(prev)
      n.has(g) ? n.delete(g) : n.add(g)
      return n
    })
  }

  return (
    <div className="flex flex-col gap-1 rounded-md border border-border/60 p-1.5">
      {groups.map(([g, list]) => (
        <div key={g}>
          {g !== UNGROUPED && (
            <button
              className="flex w-full items-center gap-1 px-1 py-0.5 text-[11px] font-medium text-muted-foreground"
              onClick={() => toggle(g)}
              onDragOver={(e) => e.preventDefault()}
              onDrop={() => dropOnGroup(g)}
            >
              {collapsed.has(g) ? <ChevronRight className="size-3" /> : <ChevronDown className="size-3" />}
              {g}
            </button>
          )}
          {!collapsed.has(g) &&
            list.map((p) => {
              const st = statusMap[p.id] ?? "disconnected"
              const active = selectedId === p.id && !isDraft
              return (
                <div
                  key={p.id}
                  draggable
                  onDragStart={() => setDragId(p.id)}
                  onDragOver={(e) => e.preventDefault()}
                  onDrop={() => dropOnItem(p.id, g)}
                  onClick={() => onSelect(p)}
                  className={cn(
                    "group flex cursor-pointer items-center gap-1.5 rounded px-1.5 py-1 text-xs transition-colors",
                    g !== UNGROUPED && "ml-3",
                    active ? "bg-primary/10 text-primary" : "hover:bg-muted",
                    dragId === p.id && "opacity-40"
                  )}
                >
                  <GripVertical className="size-3 shrink-0 cursor-grab text-muted-foreground opacity-0 group-hover:opacity-100" />
                  <span className={cn("size-1.5 shrink-0 rounded-full", st === "connected" ? "bg-success" : st === "error" ? "bg-destructive" : "bg-muted-foreground")} />
                  <span className="truncate">{p.name}</span>
                  <button
                    className="ml-auto opacity-0 transition-opacity group-hover:opacity-100"
                    title={t("复制")}
                    onClick={(e) => {
                      e.stopPropagation()
                      onDuplicate(p)
                    }}
                  >
                    <Copy className="size-3 text-muted-foreground" />
                  </button>
                </div>
              )
            })}
        </div>
      ))}
      <Button variant="ghost" size="sm" className="h-7 justify-start gap-1 text-xs" onClick={onNew}>
        <Plus className="size-3.5" /> {t("新建")}
      </Button>
    </div>
  )
}
