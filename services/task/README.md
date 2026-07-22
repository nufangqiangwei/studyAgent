# Task Service

`task` 是构建在 `serviceruntime` 上的可选业务模块。它是 TaskAggregate
的唯一写入者，维护任务阶段、Owner、执行者、当前 Run、等待原因、尝试次数、
失败次数和最终结果，但不执行模型、Capability、Agent Spawn 或 Goal 聚合。

## 实例模型

Task Definition 使用 `building.ScopeVirtual`：

```text
一个 Task
  -> 一个 TaskService 实例
  -> 一个 Durable Mailbox
  -> 一个独立 State Stream
```

模块只注册 Definition，不在 Manifest 中静态 mount。应用层 Gateway 可以直接
声明实例；业务 Service 应通过持久化的 `system.call / service.instance.declare`
声明实例：

```go
tasks := task.NewModule(clock)
if err := tasks.Register(builder); err != nil {
    return err
}

record, err := runtime.DeclareInstance(ctx, serviceruntime.InstanceDeclaration{
    InstanceID: "task-42",
    Address:    "task.42",
    Component:  task.Component,
    Metadata:   map[string]string{"task_id": "42"},
})
```

声明只创建 Runtime 的实例记录；随后向新地址发送 `task.create`，才能创建业务
TaskState。重复声明相同地址和 Definition 是幂等的，冲突声明由 Runtime 拒绝。

## 当前状态机

```text
Created -> Ready -> Running <-> Waiting -> Completed
                    |              |
                    +------------> Failed -> Ready (retry)
                    |
                    +------------> CancelRequested -> Cancelled

Ready -> Suspended -> Ready
```

当前 Agent 协议不能暂停一个正在执行的 Run，因此 `task.suspend` 只接受 Ready
任务。运行中取消会先持久化 cancellation request，再发送 `agent.cancel`；Task
只有收到 Agent 的终态回复后才进入 Cancelled。

主要入站协议：

```text
Command: task.create, task.mark_ready, task.assign, task.start,
         task.suspend, task.resume, task.retry, task.cancel
Query:   task.get
Event:   task.execution.waiting, task.execution.resumed
Reply:   agent.completed
```

主要出站协议：

```text
Command: agent.execute, agent.cancel
Reply:   task.status
Event:   task.status.changed, task.completed, task.failed, task.cancelled
```

`task.start` 从 TaskID 和 Attempt 派生稳定 RunID，并把自身地址设置为 Agent
回复目标。TaskService 只接受 `AssignedTo` 地址发出的执行报告，并校验 TaskID、
ActiveRunID 和 CorrelationID；旧 Attempt 的迟到回复会被安全忽略。

大输入和最终结果使用 `contract.ArtifactRef`。TaskState 不复制 Agent 消息历史、
模型回合、Capability 参数、Approval 详情或 Goal 依赖图。

## Service 发现与赋值

当前 Virtual Service 没有 Manifest mount，因此 Factory 不会收到静态依赖地址。
Task Owner（通常是 Orchestrator）通过 `task.assign` 持久化选择后的 Agent 地址。

后续 System Service 提供当前 Service 信息查询时，推荐保持如下边界：

```text
Orchestrator
  -> 查询 System Service 的 Service 信息
  -> 选择符合策略的 Agent
  -> task.assign

TaskService
  -> 只保存 AssignedTo
  -> 不复制全量 Service 目录
  -> 不承担 Agent 选择策略
```

如果 System Service 后续提供按地址查询或类型证明协议，可以在赋值流程中增加
验证；不应把全量 Service 列表保存进 TaskState。

## 当前限制

- 现有 `services/agent` 只公开终态 `agent.completed`，尚未产生等待/恢复进度。
  因此接入当前 Agent 时，Task 在整个 Agent Run 期间保持 Running。
- `task.execution.waiting/resumed` 已定义并可供未来 Agent 或 Supervisor 使用，
  但 TaskService 不读取 Agent 内部 RunState 来推断等待状态。
- TaskService 没有 Effect Executor；执行通过可靠 Outbox 消息交给 Agent。
- 跨进程恢复需要应用显式注入 SQLite Storage；内存 Storage 只适合测试。

## 验证

```text
go test ./services/task
go test ./serviceruntime/...
```
