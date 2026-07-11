# KenkoMQTT

本地 **MQTT 调试工具**，集成服务端（Broker）与客户端（Client）于一体的桌面应用。
基于 [Wails 3](https://v3.wails.io) + React + shadcn/ui 构建，内嵌 [mochi-mqtt](https://github.com/mochi-mqtt/server) broker 与 [paho](https://github.com/eclipse/paho.mqtt.golang) 客户端。

## 功能特性

### Broker 服务端
- 一键启动 / 停止内嵌 MQTT broker，无需额外安装
- 可调参数：TCP 监听地址与端口、WebSocket 监听地址与端口、匿名连接开关、用户名/密码鉴权、最大客户端数
- 实时统计：在线客户端数、订阅数、收发消息数、收发流量、运行时长
- 实时活动日志：连接 / 断开 / 订阅 / 取消订阅 / 发布
- 已连接客户端列表（Client ID、地址、监听器、用户名）
- 配置持久化到本地 SQLite

### MQTT 客户端
- 多连接档案管理（保存 / 编辑 / 删除），持久化到本地数据库
- 支持 `mqtt://`、`mqtts://`（TLS）、`ws://`、`wss://` 四种协议
- 可配置 Client ID、用户名/密码、Keep Alive、Clean Session、MQTT 版本（3.1 / 3.1.1）、跳过 TLS 校验
- 订阅 / 取消订阅任意主题（含 QoS 选择）
- 发布消息（主题、QoS、Retain、payload）
- 实时消息流，区分收 / 发方向，JSON 自动美化，支持主题/内容过滤

### 桌面体验
- 系统托盘 / 菜单栏图标：切换窗口显隐、启动 / 停止 Broker、退出
- 关闭窗口后应用仍作为托盘程序在后台运行 broker
- 明 / 暗主题切换，窗口位置与大小自动记忆
- 跨平台：macOS / Windows / Linux

## 技术栈

| 层 | 技术 |
| --- | --- |
| 应用框架 | Wails 3 (Go + WebView) |
| 前端 | React 18 + TypeScript + Vite + Tailwind CSS v4 + shadcn/ui |
| MQTT Broker | github.com/mochi-mqtt/server/v2 |
| MQTT Client | github.com/eclipse/paho.mqtt.golang |
| 存储 | SQLite (gorm + glebarez 纯 Go 驱动) |

## 开发

前置依赖：Go ≥ 1.25、Node ≥ 22、[pnpm](https://pnpm.io)、[Wails3 CLI](https://v3.wails.io)、[Task](https://taskfile.dev)。

```bash
# 安装 Wails3 CLI
go install github.com/wailsapp/wails/v3/cmd/wails3@latest

# 开发模式（热重载）
wails3 dev

# 生产构建（生成 bin/ 下的可执行文件）
wails3 build

# 打包为平台安装包 / 应用（macOS .app、Windows 安装程序等）
wails3 package
```

## 测试

```bash
go test ./internal/...
```

包含内嵌 broker 与客户端的端到端集成测试（连接、订阅、发布、鉴权）。

## 目录结构

```
main.go                     # Wails 应用入口：服务注册、菜单、系统托盘、窗口状态
internal/
  config/                   # 应用配置与版本信息
  logger/                   # 日志（文件 + 标准输出，自动清理）
  platform/                 # 跨平台工具（Shift 键检测等）
  database/                 # SQLite 初始化与迁移
  models/                   # 数据模型（连接档案、设置、窗口状态）
  services/                 # BrokerService / ClientService / ConnectionService 等
  assets/                   # 托盘图标
frontend/                   # React + shadcn/ui 前端
  src/pages/                # BrokerPage（服务端）/ ClientPage（客户端）
  src/components/ui/        # shadcn/ui 组件
```

## License

MIT
