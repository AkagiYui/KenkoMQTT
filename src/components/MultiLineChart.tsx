import { useEffect, useRef } from "react"
import uPlot from "uplot"
import "uplot/dist/uPlot.min.css"

export interface Series {
  label: string
  stroke: string
}

// 多序列折线图：data[0] 为 x（秒），其余为各 y 序列。
export function MultiLineChart({
  data,
  series,
  height = 150,
}: {
  data: number[][]
  series: Series[]
  height?: number
}) {
  const el = useRef<HTMLDivElement>(null)
  const plot = useRef<uPlot | null>(null)

  useEffect(() => {
    if (!el.current) return
    const opts: uPlot.Options = {
      width: el.current.clientWidth || 360,
      height,
      cursor: { show: true },
      legend: { show: true },
      scales: { x: { time: true } },
      series: [
        {},
        ...series.map((s) => ({ label: s.label, stroke: s.stroke, width: 2, fill: s.stroke + "22", points: { show: false } })),
      ],
      axes: [
        { stroke: "#888", grid: { stroke: "#8884" }, ticks: { stroke: "#8884" } },
        { stroke: "#888", grid: { stroke: "#8884" }, ticks: { stroke: "#8884" } },
      ],
    }
    plot.current = new uPlot(opts, data as uPlot.AlignedData, el.current)
    const ro = new ResizeObserver(() => {
      if (el.current) plot.current?.setSize({ width: el.current.clientWidth, height })
    })
    ro.observe(el.current)
    return () => {
      ro.disconnect()
      plot.current?.destroy()
      plot.current = null
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    plot.current?.setData(data as uPlot.AlignedData)
  }, [data])

  return <div ref={el} className="w-full" />
}
