# Web Task Gateway Service

`webgateway` 是挂载在 `serviceruntime` EventBus 上的持久化 Web Task 入口。它拥有
外部请求状态和 Task 所有权，但不读取 Task State、Journal、Instance Store 或 SQLite，
也不依赖同步的 `serviceruntime/request.Client`。

## 协议与实例

Gateway 使用 `webgateway.service@v1`、`building.ScopeRuntimeSingleton`，默认地址为
`web.gateway`。模块必须显式注册并挂载：

```go
gateway, err := webgateway.NewModule(webgateway.ModuleOptions{
    Presenter: presenter,
    Clock: clock,
    DefaultAgent: agentAddress,
    LegacyDefaultAgent: historicalAgentAddress,
})
if err != nil {
    return err
}
if err := gateway.Register(builder); err != nil {
    return err
}
manifest.Services = append(manifest.Services, gateway.Mount(webgateway.DefaultAddress))
```

`Mount` 将版本号和 `DefaultAgent` 编码到不可变 `ServiceMount.Config`；Factory 只从激活
请求携带的该 Revision 配置重建行为。升级前已经持久化且 Config 为空的 Plan 使用模块
显式提供的 legacy fallback；任何非空但损坏、未知版本的配置仍会拒绝激活。

公开命令为 `web.task.create` 和 `web.task.get` v1。两者都要求调用方提供稳定
`RequestID` 和 `UserID`。相同 RequestID、操作、用户和规范化 Payload 幂等；不同身份
返回稳定 `web_task_conflict` Presentation。

## Create 状态机

```text
web.task.create
  -> 持久化 declaring_task（包括稳定 TaskAddress、InstanceID、CallID）
  -> durable system.call / service.instance.declare
  -> system.result
  -> 持久化 waiting_task + durable task.create
  -> task.status(created)
  -> 持久化 marking_ready + durable task.mark_ready
  -> task.status(ready)
  -> 持久化 assigning + durable task.assign(DefaultAgent)
  -> task.status(assigned)
  -> 持久化 starting + durable task.start
  -> task.status(running | terminal)
  -> 持久化 succeeded + TaskID->TaskAddress 映射 + Presentation Effect
```

TaskAddress 和 InstanceID 从 TaskID/RequestID 确定性派生。`task.create` 的 `From`
由 Runtime 设置为 Gateway 地址，`ReplyTo` 也显式使用 Gateway 地址，因此 Gateway
成为 Task Owner。

若 Task 终态事件先于 `task.start` 的 Running Reply 到达，Gateway 只持久化最小终态事实，
进入 `resolving_terminal` 并原子发送 `task.get`。迟到的非终态 Reply 不会覆盖该事实；
只有与终态事实一致的权威 Task State 才完成最终 Presentation。该状态仅用于 create Saga
恢复，不是 Task 列表或时间线 Projection。

## Get、权限与恢复

Get 只查询 Gateway 已持久化的 TaskID 到 TaskAddress 映射。用户不匹配和映射不存在都
返回相同的 `web_task_not_found`，不会扫描 Runtime 目录或读取 Task 内部状态。

终态事件和 Presentation Effect 在同一个消息提交中原子持久化。PresentationID、
Effect Key 和 IdempotencyKey 均稳定；`Presenter` 必须按 PresentationID 去重，
Reconciler 使用同一个 PresentationID 安全重放。Replay 只执行 `Apply`，不会调用
Presenter、发送 Outgoing 或重新计划 Effect。

Materialized State 最多保留 `RetainedTerminalRequests` 个终态请求；Task 所有权映射
独立保留，Journal 仍保存完整请求事实。超过投影窗口的历史 Presentation 应由上层持久化
投影或 Journal 派生工具查询，而不是扩大 Gateway Snapshot。

## 验证

```text
go test ./services/webgateway ./services/task ./serviceruntime/...
go vet ./services/webgateway
```
