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

// 载荷格式（后端 codec）
export type Format = "plaintext" | "json" | "hex" | "base64" | "msgpack" | "cbor"
export const FORMATS: Format[] = ["plaintext", "json", "hex", "base64", "msgpack", "cbor"]

export interface UserProperty {
  key: string
  value: string
}
// MQTT 5.0 逐条属性（存在才有）
export interface MsgProps {
  payloadFormatIndicator?: number
  messageExpiryInterval?: number
  topicAlias?: number
  responseTopic?: string
  correlationData?: string
  contentType?: string
  subscriptionIdentifiers?: number[]
  userProperties?: UserProperty[]
}

// 后端消息库返回的行（已按格式解码 + 过滤）
export interface MsgRow {
  dir: "rx" | "tx"
  topic: string
  payload: string
  size: number
  qos: number
  retain: boolean
  ts: number
  props?: MsgProps
}

// 消息分页查询选项（过滤/正则/大小写/全词/方向/忽略QoS0/分页均在后端完成）
export interface QueryOpts {
  format: Format
  filter?: string | null
  regex?: boolean
  caseSensitive?: boolean
  wholeWord?: boolean
  ignoreQos0?: boolean
  dir?: "rx" | "tx" | null
  offset?: number
  limit?: number
}
export interface MsgPage {
  rows: MsgRow[]
  total: number
}

export interface StatusEvent {
  connId: string
  status: Status
  detail?: string
}

export interface TreeNode {
  name: string
  full: string
  count: number
  latest?: string
  ts: number
  children: TreeNode[]
}
export interface RatePoint {
  t: number
  v: number
}
export interface ContentPoint {
  t: number
  v: number
}
export interface TrafficPoint {
  t: number
  rxBytes: number
  txBytes: number
  rxCount: number
  txCount: number
}
export type LoadMethod = "count" | "avg" | "sum" | "max" | "min"

// ---- 连接档案 ----
export const listProfiles = () => invoke<Profile[]>("list_profiles")
export const saveProfile = (profile: Profile) => invoke<Profile>("save_profile", { profile })
export const deleteProfile = (id: string) => invoke<void>("delete_profile", { id })

// ---- 连接档案 导入/导出 ----
export type ProfileFormat = "json" | "yaml" | "xml" | "csv" | "xlsx"
export const PROFILE_FORMATS: ProfileFormat[] = ["json", "yaml", "xml", "csv", "xlsx"]
export interface ExportOut {
  filename: string
  base64: boolean
  content: string
}
export const exportProfiles = (format: ProfileFormat) => invoke<ExportOut>("export_profiles", { format })
export const importProfiles = (format: ProfileFormat, dataBase64: string) =>
  invoke<number>("import_profiles", { format, dataBase64 })

// ---- MQTT 连接/订阅/发布 ----
export const mqttConnect = (profile: Profile) => invoke<void>("mqtt_connect", { profile })
export const mqttDisconnect = (connId: string) => invoke<void>("mqtt_disconnect", { connId })
export const mqttSubscribe = (connId: string, topic: string, qos: number) =>
  invoke<void>("mqtt_subscribe", { connId, topic, qos })
export const mqttUnsubscribe = (connId: string, topic: string) =>
  invoke<void>("mqtt_unsubscribe", { connId, topic })
export const mqttPublish = (
  connId: string,
  topic: string,
  payload: string,
  qos: number,
  retain: boolean,
  format: Format = "plaintext",
  expand = false
) => invoke<void>("mqtt_publish", { connId, topic, payload, qos, retain, format, expand })

// ---- 消息库 / 计算（后端负责，前端仅展示） ----
export const messagesQuery = (connId: string, opts: QueryOpts) =>
  invoke<MsgPage>("messages_query", { connId, opts })
export const messagesClear = (connId: string) => invoke<void>("messages_clear", { connId })
export const messagesClearTopic = (connId: string, topicFilter: string) =>
  invoke<void>("messages_clear_topic", { connId, topicFilter })
export const topicTree = (connId: string, format: Format) => invoke<TreeNode[]>("topic_tree", { connId, format })
export const chartRate = (connId: string, bucketMs: number, buckets: number) =>
  invoke<RatePoint[]>("chart_rate", { connId, bucketMs, buckets })
export const chartTraffic = (connId: string, bucketMs: number, buckets: number) =>
  invoke<TrafficPoint[]>("chart_traffic", { connId, bucketMs, buckets })
export const chartLoad = (connId: string, topicFilter: string, method: LoadMethod, bucketMs: number, buckets: number) =>
  invoke<ContentPoint[]>("chart_load", { connId, topicFilter, method, bucketMs, buckets })
export const chartContent = (connId: string, topicFilter: string, jsonpath: string, limit = 200) =>
  invoke<ContentPoint[]>("chart_content", { connId, topicFilter, jsonpath, limit })
export type ExportKind = "csv" | "json" | "txt"
export const exportMessages = (connId: string, kind: ExportKind, format: Format) =>
  invoke<string>("export_messages", { connId, kind, format })
export const scheduleStart = (
  connId: string,
  topic: string,
  payload: string,
  qos: number,
  retain: boolean,
  format: Format,
  intervalMs: number
) => invoke<string>("schedule_start", { connId, topic, payload, qos, retain, format, intervalMs })
export const scheduleStop = (id: string) => invoke<void>("schedule_stop", { id })

// ---- 事件 ----
// 后端收到/发出消息只发「信号」，前端据此按需重新查询（保证后端权威、前端仅展示）
export const onMsgSignal = (cb: (connId: string) => void): Promise<UnlistenFn> =>
  listen<{ connId: string }>("mqtt:msg", (e) => cb(e.payload.connId))
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

// ---- Broker ----
export interface BrokerConfig {
  host: string
  port: number
  allowAnonymous: boolean
  username: string
  password: string
  maxClients: number
}
export interface BrokerClientRow {
  clientId: string
  addr: string
  username: string
  subs: number
}
export interface BrokerEvt {
  kind: "connect" | "disconnect" | "subscribe" | "unsubscribe" | "publish"
  clientId: string
  topic?: string
  payload?: string
  ts: number
}
export interface BrokerStats {
  running: boolean
  clientsConnected: number
  msgsReceived: number
  msgsSent: number
  retained: number
}

export const brokerStart = (config: BrokerConfig) => invoke<void>("broker_start", { config })
export const brokerStop = () => invoke<void>("broker_stop")
export const brokerStatus = () => invoke<boolean>("broker_status")
export const brokerGetConfig = () => invoke<BrokerConfig>("broker_get_config")
export interface RetainedRow {
  topic: string
  payload: string
  qos: number
}
export const brokerRetained = () => invoke<RetainedRow[]>("broker_retained")

export const onBrokerStats = (cb: (s: BrokerStats) => void) =>
  listen<BrokerStats>("broker:stats", (e) => cb(e.payload))
export const onBrokerClients = (cb: (c: BrokerClientRow[]) => void) =>
  listen<BrokerClientRow[]>("broker:clients", (e) => cb(e.payload))
export const onBrokerEvent = (cb: (ev: BrokerEvt) => void) =>
  listen<BrokerEvt>("broker:event", (e) => cb(e.payload))
export const onBrokerStatus = (cb: (running: boolean) => void) =>
  listen<{ running: boolean }>("broker:status", (e) => cb(e.payload.running))

export function newBrokerConfig(): BrokerConfig {
  return { host: "0.0.0.0", port: 1883, allowAnonymous: true, username: "", password: "", maxClients: 0 }
}

// ---- 平台 / Android 权限 ----
export interface PlatformInfo {
  os: string
  isAndroid: boolean
}
export interface AndroidPerms {
  applicable: boolean
  known: boolean
  ignoringBatteryOptimizations: boolean
}
export const platformInfo = () => invoke<PlatformInfo>("platform_info")
export const checkAndroidPermissions = () => invoke<AndroidPerms>("check_android_permissions")
export const openAndroidSettings = (kind: "battery" | "appdetails") =>
  invoke<void>("open_android_settings", { kind })
