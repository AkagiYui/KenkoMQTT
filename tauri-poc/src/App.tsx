import { useEffect, useRef, useState } from "react"
import { invoke } from "@tauri-apps/api/core"
import { listen } from "@tauri-apps/api/event"

type Status = "disconnected" | "connecting" | "connected" | "error"

interface Msg {
  dir: "rx" | "tx"
  topic: string
  payload: string
  qos: number
  retain: boolean
  ts: number
}

interface StatusEvent {
  status: Status
  detail?: string
}

export default function App() {
  const [status, setStatus] = useState<Status>("disconnected")
  const [statusDetail, setStatusDetail] = useState("")
  const [host, setHost] = useState("broker.emqx.io")
  const [port, setPort] = useState(1883)
  const [clientId, setClientId] = useState("kenko-poc-")
  const [username, setUsername] = useState("")
  const [password, setPassword] = useState("")
  const [subTopic, setSubTopic] = useState("kenko/poc/#")
  const [pubTopic, setPubTopic] = useState("kenko/poc/hello")
  const [pubPayload, setPubPayload] = useState('{"msg":"hi from tauri"}')
  const [msgs, setMsgs] = useState<Msg[]>([])
  const [err, setErr] = useState("")
  const listRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const un1 = listen<Msg>("mqtt://message", (e) => {
      setMsgs((m) => [...m.slice(-499), e.payload])
    })
    const un2 = listen<StatusEvent>("mqtt://status", (e) => {
      setStatus(e.payload.status)
      setStatusDetail(e.payload.detail ?? "")
    })
    return () => {
      un1.then((f) => f())
      un2.then((f) => f())
    }
  }, [])

  useEffect(() => {
    listRef.current?.scrollTo({ top: listRef.current.scrollHeight })
  }, [msgs])

  async function call(cmd: string, args?: Record<string, unknown>) {
    setErr("")
    try {
      return await invoke(cmd, args)
    } catch (e) {
      setErr(String(e))
      throw e
    }
  }

  const connected = status === "connected"

  return (
    <div className="app">
      <header>
        <h1>KenkoMQTT · Tauri PoC</h1>
        <span className={`badge ${status}`}>
          {status}
          {statusDetail ? ` · ${statusDetail}` : ""}
        </span>
      </header>

      <section className="card">
        <div className="grid">
          <label>Host<input value={host} onChange={(e) => setHost(e.target.value)} disabled={connected} /></label>
          <label>Port<input type="number" value={port} onChange={(e) => setPort(+e.target.value)} disabled={connected} /></label>
          <label>Client ID<input value={clientId} onChange={(e) => setClientId(e.target.value)} disabled={connected} /></label>
          <label>Username<input value={username} onChange={(e) => setUsername(e.target.value)} disabled={connected} /></label>
          <label>Password<input type="password" value={password} onChange={(e) => setPassword(e.target.value)} disabled={connected} /></label>
        </div>
        <div className="row">
          {!connected ? (
            <button className="primary" onClick={() => call("mqtt_connect", { opts: { host, port, clientId, username, password } })}>连接</button>
          ) : (
            <button onClick={() => call("mqtt_disconnect")}>断开</button>
          )}
        </div>
      </section>

      <section className="card">
        <div className="row">
          <input value={subTopic} onChange={(e) => setSubTopic(e.target.value)} placeholder="订阅主题" />
          <button disabled={!connected} onClick={() => call("mqtt_subscribe", { topic: subTopic, qos: 0 })}>订阅</button>
        </div>
        <div className="row">
          <input value={pubTopic} onChange={(e) => setPubTopic(e.target.value)} placeholder="发布主题" />
        </div>
        <div className="row">
          <textarea value={pubPayload} onChange={(e) => setPubPayload(e.target.value)} rows={2} />
        </div>
        <div className="row">
          <button className="primary" disabled={!connected} onClick={() =>
            call("mqtt_publish", { topic: pubTopic, payload: pubPayload, qos: 0, retain: false }).then(() =>
              setMsgs((m) => [...m.slice(-499), { dir: "tx", topic: pubTopic, payload: pubPayload, qos: 0, retain: false, ts: Date.now() }]))
          }>发布</button>
          <button onClick={() => setMsgs([])}>清空</button>
        </div>
      </section>

      {err && <div className="err">{err}</div>}

      <section className="card messages">
        <div className="msglist" ref={listRef}>
          {msgs.map((m, i) => (
            <div key={i} className={`msg ${m.dir}`}>
              <div className="mtop">
                <span className="dir">{m.dir === "rx" ? "▼ 收" : "▲ 发"}</span>
                <span className="topic">{m.topic}</span>
                <span className="meta">QoS{m.qos}{m.retain ? " · retain" : ""}</span>
              </div>
              <pre>{m.payload}</pre>
            </div>
          ))}
          {msgs.length === 0 && <div className="empty">暂无消息</div>}
        </div>
      </section>
    </div>
  )
}
