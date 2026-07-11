import { useEffect, useRef } from "react"
import uPlot from "uplot"
import "uplot/dist/uPlot.min.css"

// 基于 uPlot 的 canvas 折线图：高频数据下比 DOM/SVG 明显更省。
export function LineChart({
  data,
  height = 150,
  label = "值",
  stroke = "#3b82f6",
}: {
  data: [number[], number[]]
  height?: number
  label?: string
  stroke?: string
}) {
  const el = useRef<HTMLDivElement>(null)
  const plot = useRef<uPlot | null>(null)

  useEffect(() => {
    if (!el.current) return
    const opts: uPlot.Options = {
      width: el.current.clientWidth || 360,
      height,
      cursor: { show: true },
      legend: { show: false },
      scales: { x: { time: true } },
      series: [
        {},
        { label, stroke, width: 2, fill: stroke + "22", points: { show: false } },
      ],
      axes: [
        { stroke: "#888", grid: { stroke: "#8884" }, ticks: { stroke: "#8884" } },
        { stroke: "#888", grid: { stroke: "#8884" }, ticks: { stroke: "#8884" } },
      ],
    }
    plot.current = new uPlot(opts, data, el.current)
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
    plot.current?.setData(data)
  }, [data])

  return <div ref={el} className="w-full" />
}
