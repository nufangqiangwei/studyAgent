# Web Server 入口

`main/server` 是面向 Web 客户端的进程入口，负责提供嵌入式前端主页，以及用户与 Runtime
之间的协议适配。入口不拥有 Task、Approval 等业务状态。

Web 专用 Runtime application 组合根使用 `DataDir/runtime.db` 和 `DataDir/artifacts/`，
显式注册 LLM、Approval、Capability、Agent、Interaction、Task 与 Web Gateway。Task 只注册
Virtual Definition；当前 Capability Catalog 为空且授权规则显式 Deny，Agent 也不会声明
Workspace 能力。

`POST /v1/tasks` 与 `GET /v1/tasks/{id}` 已通过 Web Gateway、Task Service、Runtime Adapter
和 SQLite 持久化真实接通。关闭进程后使用相同 DataDir 与 Runtime ID 重启，仍可查询之前
创建的 Task；响应不是来自旧进程的 Adapter 缓存。当前创建只会持久化到 `created`：
自动 `Ready`、`Assign` 和 `Start` 尚未实现，因此不会调用模型，也不会自动执行 Agent。
Capability Catalog 仍为空，Agent 当前不能读取或修改工作区。

生产 `Run` 会先构建 application、恢复并 Start Runtime，再启动 Runtime Serve。只有 Runtime
已经处于 Live 且 Serve 没有立即失败时才会创建 HTTP Listener 并输出监听地址。启动恢复失败
不会对外提供 HTTP；Runtime 或 HTTP 发生 fatal 时，另一侧也会停止并完成资源清理。

## 启动

```powershell
$env:AGENT_DATA_DIR = ".agent/runtime"
$env:AGENT_RUNTIME_ID = "agent-server"
$env:AGENT_PROVIDER = "deepseek"
$env:AGENT_MODEL = "deepseek-chat"
$env:AGENT_BASE_URL = "https://api.deepseek.com"
$env:AGENT_API_KEY = "<set-your-api-key-locally>"
go run ./main/server
```

默认监听 `127.0.0.1:8080`，可通过 `-address` 或 `AGENT_SERVER_ADDRESS` 修改。当前未提供
TLS 和真实身份认证；所有接口暂时要求 `X-User-ID` 请求头，它只用于固定协议中的用户身份，
不能视为可信认证结果。

收到进程取消信号后，Server 会先停止接受新 HTTP 请求并等待在途请求，再 Drain Runtime、
停止 Runtime Serve，最后关闭 application。HTTP 优雅关闭超过 `shutdown-timeout` 时会强制
关闭连接，但仍继续清理 Runtime、Artifact 与 SQLite 资源。启动或关闭错误只报告操作阶段，
不会输出 API Key 或 Runtime 内部 Payload。

Web Server 的本地 Runtime 与模型配置同时支持环境变量和命令行参数，命令行参数优先。默认
使用 `.agent/runtime` 作为 SQLite 与 Artifact 数据目录、`agent-server` 作为 Runtime ID、
`deepseek` 作为 Provider，并使用 `https://api.deepseek.com` 作为 OpenAI-compatible Base URL。
模型名必须通过 `AGENT_MODEL` 或 `-model` 提供。

常用配置包括：

| 环境变量 | 命令行参数 | 说明 |
| --- | --- | --- |
| `AGENT_DATA_DIR` | `-data-dir` | SQLite 与 Artifact 数据目录 |
| `AGENT_RUNTIME_ID` | `-runtime-id` | 稳定的 Runtime ID |
| `AGENT_PROVIDER` | `-provider` | 模型 Provider |
| `AGENT_MODEL` | `-model` | 必填模型名 |
| `AGENT_BASE_URL` | `-base-url` | 模型 API Base URL |
| `AGENT_MODEL_TIMEOUT` | `-model-timeout` | 单次模型调用超时 |
| `AGENT_SYSTEM_PROMPT` | `-system-prompt` | Agent 系统提示词 |
| `AGENT_MAX_TURNS` | `-max-turns` | 单请求最大 Agent 轮数 |
| `AGENT_MAX_TOKENS` | `-max-tokens` | 最大模型输出 Token；`0` 使用 Provider 默认值 |

API Key 只能通过 `AGENT_API_KEY` 提供，不支持明文命令行参数。HTTP 的请求头读取、Keep-Alive
空闲和优雅关闭超时分别可通过 `AGENT_SERVER_READ_HEADER_TIMEOUT`、
`AGENT_SERVER_IDLE_TIMEOUT`、`AGENT_SERVER_SHUTDOWN_TIMEOUT` 设置，也可由对应命令行参数覆盖。

启动后访问 `http://127.0.0.1:8080/` 会直接返回 Agent 工作台主页。主页及其静态资源已嵌入
服务器二进制，不依赖进程的当前工作目录；`/v1/*` 继续保留给 REST/WebSocket 接口。

## REST 接口

### 创建任务

```text
POST /v1/tasks
Content-Type: application/json
X-User-ID: user-1
```

```json
{
  "task_id": "task-42",
  "goal_id": "goal-1",
  "title": "示例任务",
  "input": "完成指定工作"
}
```

`task_id` 可省略，后续由 Runtime Gateway 生成。接通后成功响应为 `201`：

```json
{
  "task": {
    "task_id": "task-42",
    "goal_id": "goal-1",
    "user_id": "user-1",
    "title": "示例任务",
    "input": "完成指定工作",
    "phase": "created",
    "created_at": "2026-07-22T00:00:00Z",
    "updated_at": "2026-07-22T00:00:00Z"
  }
}
```

创建只表示 Task 已进入 `created`，不会自动分配 Agent 或启动执行。

### 查看任务

```text
GET /v1/tasks/{task_id}
X-User-ID: user-1
```

成功响应为 `200`，响应体使用与创建接口相同的 `task` 包装结构。

## 审批 WebSocket

```text
GET /v1/approvals/ws
X-User-ID: user-1
Upgrade: websocket
```

连接是只下行的，并且目前只向已经在线的 WebSocket 连接发送新通知；断线期间的通知不会在
重连后补发，也没有游标或历史重放。接通 Runtime 后，服务端发送：

```json
{
  "type": "approval.requested",
  "version": 1,
  "approval": {
    "approval_id": "approval-1",
    "call_id": "call-1",
    "user_id": "user-1",
    "capability_ref": "workspace.write",
    "capability_version": "v1",
    "risk_summary": "将修改工作区文件",
    "arguments_digest": "sha256:...",
    "requested_at": "2026-07-22T00:00:00Z"
  }
}
```

本入口不会接收 `approve`/`deny`。审批决议回传将在 Runtime Interaction/Gateway 协议实现后
另行对接。

## Runtime 对接边界

当前组合根把 Interaction 和 Web Gateway 的 Presenter 绑定到同一个 Runtime Adapter。
Adapter 只获得 Runtime 的 durable ingress、Runtime/Plan 标识和 IDGenerator，不获得 Store、
Host 或业务 Service 对象：

- `CreateTask`：声明 Virtual Task 实例，发送 `task.create`，按持久化相关 ID 接收
  `task.status`。
- `GetTask`：发送 `task.get`，不能直接读取 TaskService 对象、Snapshot 或数据库表。
- `SubscribeApprovalRequests`：只把 Runtime 已持久化并提交成功的 `approval.requested`
  投影到当前用户的连接；断线恢复、游标和重放语义由后续 Gateway 协议定义。
- 审批决议：未来增加独立的上行协议，转换成 `approval.resolve`，并保留用户身份和审计事实。

Gateway 的业务关联、等待状态和恢复事实属于 Runtime 上的 Service；HTTP handler 和
WebSocket 连接只是进程资源，不得写入 Journal 或成为恢复依据。
