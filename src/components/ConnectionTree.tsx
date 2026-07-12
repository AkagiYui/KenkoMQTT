import { useMemo } from "react"
import {
  DndContext, closestCenter, MouseSensor, TouchSensor, KeyboardSensor, useSensor, useSensors, type DragEndEvent,
} from "@dnd-kit/core"
import { SortableContext, verticalListSortingStrategy, useSortable, sortableKeyboardCoordinates, arrayMove } from "@dnd-kit/sortable"
import { CSS } from "@dnd-kit/utilities"
import { GripVertical, Plus, Copy } from "lucide-react"
import { type Profile, type Status, reorderProfiles } from "@/lib/api"
import { useI18n } from "@/lib/i18n"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"

const UNGROUPED = "__ungrouped__"

type Row =
  | { kind: "header"; id: string; group: string }
  | { kind: "conn"; id: string; group: string; profile: Profile }

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
  const sensors = useSensors(
    useSensor(MouseSensor, { activationConstraint: { distance: 6 } }),
    useSensor(TouchSensor, { activationConstraint: { delay: 150, tolerance: 8 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates })
  )

  // 组织为「分组头 + 连接」的扁平行；有命名分组时每组带头（未分组显示为「未分组」）。
  const { rows, hasGroups } = useMemo(() => {
    const groups = new Map<string, Profile[]>()
    for (const p of profiles) {
      const g = p.group?.trim() || UNGROUPED
      if (!groups.has(g)) groups.set(g, [])
      groups.get(g)!.push(p)
    }
    const named = [...groups.keys()].some((g) => g !== UNGROUPED)
    // 未分组在前
    const order = [...groups.entries()].sort((a, b) => (a[0] === UNGROUPED ? -1 : b[0] === UNGROUPED ? 1 : 0))
    const out: Row[] = []
    for (const [g, list] of order) {
      if (named) out.push({ kind: "header", id: `hdr:${g}`, group: g })
      for (const p of list) out.push({ kind: "conn", id: p.id, group: g, profile: p })
    }
    return { rows: out, hasGroups: named }
  }, [profiles])

  async function onDragEnd(e: DragEndEvent) {
    const { active, over } = e
    if (!over || active.id === over.id) return
    const from = rows.findIndex((r) => r.id === active.id)
    const to = rows.findIndex((r) => r.id === over.id)
    if (from < 0 || to < 0) return
    const moved = arrayMove(rows, from, to)
    // 依据前置分组头推导每个连接的新分组，并给出顺序。
    let cur = ""
    const orderedConns: { id: string; group: string }[] = []
    for (const r of moved) {
      if (r.kind === "header") cur = r.group === UNGROUPED ? "" : r.group
      else orderedConns.push({ id: r.id, group: hasGroups ? cur : "" })
    }
    await reorderProfiles(orderedConns.map((c, i) => ({ id: c.id, sortOrder: i, group: c.group })))
    onReordered()
  }

  return (
    <div className="flex flex-col gap-0.5 rounded-md border border-border/60 p-1.5">
      <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={onDragEnd}>
        <SortableContext items={rows.map((r) => r.id)} strategy={verticalListSortingStrategy}>
          {rows.map((r) =>
            r.kind === "header" ? (
              <div key={r.id} className="px-1 pt-1 text-[11px] font-medium text-muted-foreground">
                {r.group === UNGROUPED ? t("未分组") : r.group}
              </div>
            ) : (
              <SortableConn
                key={r.id}
                profile={r.profile}
                indent={hasGroups}
                status={statusMap[r.profile.id] ?? "disconnected"}
                active={selectedId === r.profile.id && !isDraft}
                onSelect={() => onSelect(r.profile)}
                onDuplicate={() => onDuplicate(r.profile)}
                dupLabel={t("复制")}
              />
            )
          )}
        </SortableContext>
      </DndContext>
      <Button variant="ghost" size="sm" className="h-7 justify-start gap-1 text-xs" onClick={onNew}>
        <Plus className="size-3.5" /> {t("新建")}
      </Button>
    </div>
  )
}

function SortableConn({
  profile,
  indent,
  status,
  active,
  onSelect,
  onDuplicate,
  dupLabel,
}: {
  profile: Profile
  indent: boolean
  status: Status
  active: boolean
  onSelect: () => void
  onDuplicate: () => void
  dupLabel: string
}) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id: profile.id })
  const style = { transform: CSS.Transform.toString(transform), transition }
  return (
    <div
      ref={setNodeRef}
      style={style}
      onClick={onSelect}
      className={cn(
        "group flex cursor-pointer items-center gap-1.5 rounded px-1.5 py-1 text-xs transition-colors",
        indent && "ml-3",
        active ? "bg-primary/10 text-primary" : "hover:bg-muted",
        isDragging && "opacity-50"
      )}
    >
      <button
        className="shrink-0 cursor-grab touch-none text-muted-foreground opacity-40 group-hover:opacity-100"
        {...attributes}
        {...listeners}
        onClick={(e) => e.stopPropagation()}
        aria-label="拖拽"
      >
        <GripVertical className="size-3" />
      </button>
      <span className={cn("size-1.5 shrink-0 rounded-full", status === "connected" ? "bg-success" : status === "error" ? "bg-destructive" : "bg-muted-foreground")} />
      <span className="truncate">{profile.name}</span>
      <button
        className="ml-auto opacity-0 transition-opacity group-hover:opacity-100"
        title={dupLabel}
        onClick={(e) => {
          e.stopPropagation()
          onDuplicate()
        }}
      >
        <Copy className="size-3 text-muted-foreground" />
      </button>
    </div>
  )
}
