import { invoke } from "@tauri-apps/api/core"
import { listen, type UnlistenFn } from "@tauri-apps/api/event"

export type Protocol = "tcp" | "tls" | "ws" | "wss"
export type Status = "connecting" | "connected" | "reconnecting" | "disconnected" | "error"

export interface Will {
  enabled: boolean
  topic: string
  payload: string
  qos: number
  retain: boolean
}

export interface Profile {
  id: string
  name: string
  protocol: Protocol
  host: string
  port: number
  path: string
  clientId: string
  username: string
  password: string
  keepAlive: number
  cleanSession: boolean
  mqttVersion: number // 4 = 3.1.1, 5 = 5.0
  tlsSkipVerify: boolean
  caCert: string
  sortOrder: number
  will: Will
}

export interface MsgEvent {
  connId: string
  dir: "rx" | "tx"
  topic: string
  payload: string
  qos: number
  retain: boolean
  ts: number
}

export interface StatusEvent {
  connId: string
  status: Status
  detail?: string
}

// ---- 连接档案 ----
export const listProfiles = () => invoke<Profile[]>("list_profiles")
export const saveProfile = (profile: Profile) => invoke<Profile>("save_profile", { profile })
export const deleteProfile = (id: string) => invoke<void>("delete_profile", { id })

// ---- MQTT ----
export const mqttConnect = (profile: Profile) => invoke<void>("mqtt_connect", { profile })
export const mqttDisconnect = (connId: string) => invoke<void>("mqtt_disconnect", { connId })
export const mqttSubscribe = (connId: string, topic: string, qos: number) =>
  invoke<void>("mqtt_subscribe", { connId, topic, qos })
export const mqttUnsubscribe = (connId: string, topic: string) =>
  invoke<void>("mqtt_unsubscribe", { connId, topic })
export const mqttPublish = (connId: string, topic: string, payload: string, qos: number, retain: boolean) =>
  invoke<void>("mqtt_publish", { connId, topic, payload, qos, retain })

// ---- 事件 ----
export const onMessage = (cb: (m: MsgEvent) => void): Promise<UnlistenFn> =>
  listen<MsgEvent>("mqtt:message", (e) => cb(e.payload))
export const onStatus = (cb: (s: StatusEvent) => void): Promise<UnlistenFn> =>
  listen<StatusEvent>("mqtt:status", (e) => cb(e.payload))

export function newProfile(): Profile {
  return {
    id: crypto.randomUUID(),
    name: "新连接",
    protocol: "tcp",
    host: "127.0.0.1",
    port: 1883,
    path: "/mqtt",
    clientId: "",
    username: "",
    password: "",
    keepAlive: 60,
    cleanSession: true,
    mqttVersion: 4,
    tlsSkipVerify: false,
    caCert: "",
    sortOrder: 0,
    will: { enabled: false, topic: "", payload: "", qos: 0, retain: false },
  }
}

export const DEFAULT_PORTS: Record<Protocol, number> = { tcp: 1883, tls: 8883, ws: 8083, wss: 8084 }
