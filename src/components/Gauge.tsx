// 纯 SVG 半圆仪表盘，无外部依赖。
export function Gauge({
  value,
  min = 0,
  max = 100,
  unit = "",
  color = "#3b82f6",
}: {
  value: number
  min?: number
  max?: number
  unit?: string
  color?: string
}) {
  const clamped = Math.max(min, Math.min(max, value))
  const ratio = max > min ? (clamped - min) / (max - min) : 0
  // 半圆：180°(左) → 0°(右)
  const startAngle = Math.PI
  const angle = startAngle - ratio * Math.PI
  const r = 70
  const cx = 100
  const cy = 90
  const arc = (a0: number, a1: number) => {
    const x0 = cx + r * Math.cos(a0)
    const y0 = cy - r * Math.sin(a0)
    const x1 = cx + r * Math.cos(a1)
    const y1 = cy - r * Math.sin(a1)
    const large = a0 - a1 > Math.PI ? 1 : 0
    return `M ${x0} ${y0} A ${r} ${r} 0 ${large} 1 ${x1} ${y1}`
  }
  return (
    <svg viewBox="0 0 200 110" className="w-full">
      <path d={arc(Math.PI, 0)} fill="none" stroke="var(--color-border)" strokeWidth="12" strokeLinecap="round" />
      <path d={arc(Math.PI, angle)} fill="none" stroke={color} strokeWidth="12" strokeLinecap="round" />
      <text x="100" y="80" textAnchor="middle" className="fill-foreground" style={{ fontSize: 22, fontWeight: 600 }}>
        {Number.isFinite(value) ? value.toFixed(value % 1 === 0 ? 0 : 1) : "—"}
      </text>
      {unit && (
        <text x="100" y="100" textAnchor="middle" className="fill-muted-foreground" style={{ fontSize: 11 }}>
          {unit}
        </text>
      )}
      <text x="30" y="105" textAnchor="middle" className="fill-muted-foreground" style={{ fontSize: 9 }}>{min}</text>
      <text x="170" y="105" textAnchor="middle" className="fill-muted-foreground" style={{ fontSize: 9 }}>{max}</text>
    </svg>
  )
}
