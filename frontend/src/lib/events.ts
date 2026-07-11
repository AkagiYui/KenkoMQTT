import { Events } from "@wailsio/runtime"

// Wails 事件名（与后端 services 常量保持一致）
export const EV = {
  brokerEvent: "broker:event",
  brokerStats: "broker:stats",
  brokerStatus: "broker:status",
  clientMessage: "client:message",
  clientStatus: "client:status",
} as const

/**
 * 订阅一个 Wails 事件，回调直接拿到强类型的 payload。
 * 返回取消订阅函数。
 */
export function onEvent<T>(name: string, cb: (data: T) => void): () => void {
  const off = Events.On(name, (e: { data: T }) => cb(e.data as T))
  return off as unknown as () => void
}
