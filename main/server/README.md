# Web Server 入口

`main/server` 是面向 Web 客户端的进程入口，负责提供嵌入式前端主页，以及用户与 Runtime
之间的协议适配。入口不拥有 Task、Approval 等业务状态。

当前阶段先固定 HTTP/WebSocket 边界。Runtime 侧的 Interaction/Gateway Service 尚未接入，
入口默认使用不可用端口：REST 返回 `503 runtime_unavailable`，审批 WebSocket 在升级前返回
同样的 `503`。这样不会用入口内存伪造可恢复的 Runtime 业务状态。

## 启动

```powershell
go run ./main/server
```

默认监听 `127.0.0.1:8080`，可通过 `-address` 或 `AGENT_SERVER_ADDRESS` 修改。当前未提供
TLS 和真实身份认证；所有接口暂时要求 `X-User-ID` 请求头，它只用于固定协议中的用户身份，
不能视为可信认证结果。

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
    "title": "示例任务",
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

连接是只下行的。接通 Runtime 后，服务端发送：

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

## 后续 Runtime 对接

后续实现应在 `services/` 下提供 Interaction/Gateway Service，并为本入口的 `RuntimePort`
提供适配器：

- `CreateTask`：声明 Virtual Task 实例，发送 `task.create`，按持久化相关 ID 接收
  `task.status`。
- `GetTask`：发送 `task.get`，不能直接读取 TaskService 对象、Snapshot 或数据库表。
- `SubscribeApprovalRequests`：只把 Runtime 已持久化并提交成功的 `approval.requested`
  投影到当前用户的连接；断线恢复、游标和重放语义由后续 Gateway 协议定义。
- 审批决议：未来增加独立的上行协议，转换成 `approval.resolve`，并保留用户身份和审计事实。

Gateway 的业务关联、等待状态和恢复事实属于 Runtime 上的 Service；HTTP handler 和
WebSocket 连接只是进程资源，不得写入 Journal 或成为恢复依据。
