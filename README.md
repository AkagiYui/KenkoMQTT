# KenkoMQTT

本地 **MQTT 调试工具**，集客户端（Client）与内嵌服务端（Broker）于一体，
**跨平台**：Windows / macOS / Android（iOS 亦可构建）。

基于 [Tauri 2](https://tauri.app) + Rust + React + Tailwind v4 / shadcn/ui。
MQTT 客户端使用 [rumqttc](https://github.com/bytebeam/rumqtt)，内嵌 broker 基于 rumqttc 的 MQTT 报文编解码自建（支持随时启停）。

## 功能特性

### 客户端
- 多连接并行，连接档案本地持久化（JSON，位于应用数据目录）
- 协议：`mqtt://`(TCP) / `mqtts://`(TLS) / `ws://` / `wss://`
- 版本：**MQTT 3.1.1 与 5.0**
- TLS：跳过校验（自签名）/ 指定 CA 证书 / 系统根证书
- 遗嘱消息（LWT）、KeepAlive、Clean Session、自定义 Client ID、账号密码
- 多订阅（含 QoS、退订）、发布（QoS / retain）
- 消息列表：收发方向、QoS/retain、时间戳、JSON 美化、主题/内容过滤

### Broker 服务端（全平台，含 Android）
- 一键启动 / 停止内嵌 MQTT broker，随时可开关
- 参数：监听地址、端口、匿名连接开关、用户名/密码鉴权、最大连接数
- 实时统计：在线客户端、收发消息数、保留消息数
- 在线客户端列表、活动日志（连接/断开/订阅/退订/发布）
- 通配符订阅、保留消息、QoS 0/1、遗嘱转发

### 平台适配
- 响应式布局三档断点：窄屏（单列 + 顶部标签）/ 中屏 / 宽屏（侧栏导航 + 多列）
- 明 / 暗主题
- **Android**：检测「电池优化 / 无限制后台」状态，未放行时在前端提示并可一键跳转系统设置
  （保证后台常驻 broker / 客户端连接更稳定）

## 开发

```bash
pnpm install
pnpm tauri dev            # 桌面开发
pnpm tauri android dev    # Android 开发（需 Android SDK/NDK + JDK）
```

## 构建

```bash
pnpm tauri build                       # 当前桌面平台生产包
pnpm tauri android build --apk         # Android APK
```

CI（`.github/workflows/build.yaml`）在 push 到 `main` 时构建 **Windows / macOS / Android** 生产产物并上传为 artifact。

- 应用标识（bundle id，全平台）：`com.akagiyui.kenkomqtt`

## 目录结构

```
src/            React 前端（pages/ 客户端与 Broker 页，lib/api.ts Tauri 桥接）
src-tauri/      Rust 后端
  src/mqtt.rs     客户端连接管理（v4/v5 双栈 + 传输选择）
  src/broker.rs   内嵌 MQTT broker（自建 accept/路由，可启停）
  src/tls.rs      rustls 客户端配置（skip-verify / CA / 系统根）
  src/android.rs  平台信息 + Android 电池/后台权限检测与跳转
  src/store.rs    连接档案 / broker 配置的 JSON 持久化
```
