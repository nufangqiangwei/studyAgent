# Service Runtime

`serviceruntime` 是新的事件溯源服务运行框架。它位于项目根目录下，与旧的 `internal/runtime` 完全隔离，不在旧实现上增量修改。

当前实现是可运行的单进程 MVP，既提供内存测试存储，也提供可跨进程重开的 SQLite 持久化存储，已经包含：

- 可序列化的 Message、StoredEvent 和 Snapshot 协议。
- Service、Factory、Decision 和纯 Replay `Apply` 协议。
- ServiceDefinition、RuntimeManifest、Register 和不可变 RuntimePlan。
- 通用 Compile 校验和可注册 PlanValidator。
- Journal、Snapshot、Inbox、Outbox、Effect 和 ServiceInstance 存储端口。
- 原子的 `ACK + Events + Snapshot + Outbox + Effects` 提交协议。
- At-least-once 投递、稳定 ID、Inbox 去重、Lease 和 Dead Letter 状态。
- ServiceInstance 生命周期、InstanceDirectory、ActivationEpoch 和 Fencing。
- Snapshot + Journal Replay、按需 Activation 和 Passivation。
- EventBus、ServiceHost、Effect Worker 和启动 Recovery。
- 静态挂载服务与通用 Virtual Service 动态实例。

## 包结构

```text
serviceruntime/
  contract/            基础可序列化协议
  service/             Service、Factory、Decision
  building/            Register、Manifest、Plan、RoutingTable
  persistence/         持久化端口
  persistence/memory/  内存事务实现
  persistence/sqlite/  SQLite 事务与跨进程持久化实现
  instance/            实例记录、生命周期、Directory、Lease
  activation/          Factory 创建、Snapshot + Replay、Activation
  request/             同步外观的跨 Service 请求客户端与 Reply Broker
  transport/           EventBus 和 Outbox 投递
  host/                单条 Inbox 消息处理闭环
  effect/              Executor、Reconciler、Effect Worker
  recovery/            启动恢复协调
  builder.go           对象图装配
  runtime.go           顶层生命周期和操作入口
```

## 核心运行顺序

```text
Builder.Build
  -> Register.Compile
  -> 创建/恢复静态 ServiceInstanceRecord
  -> 装配 Storage、Directory、Activation、Bus、Host、Effect、Recovery

Runtime.Start
  -> 暂停 EventBus
  -> 恢复 InstanceDirectory
  -> Snapshot + Journal Replay（只调用 Apply）
  -> Reconcile 未完成 Effect
  -> 激活静态服务和有 Pending Inbox 的动态服务
  -> 开启 Live Delivery

Runtime.HandleNext
  -> Claim Inbox
  -> 激活或恢复 Service
  -> Service.Handle
  -> 校验 Decision 和消息协议
  -> 派生稳定 ID
  -> Service.Apply 新事件
  -> 原子 Commit
  -> Commit 成功后更新内存 Activation

Runtime.Serve
  -> 持续扫描有 Pending Inbox 的 ServiceInstance
  -> 每个就绪实例由独立 goroutine 调用 ServiceHost
  -> 独立推进 Outbox 和 Effect
  -> context 取消或 Runtime.Close 时停止
```

## Service 内主动请求其他 Service

Runtime 在创建 Service 时，会通过 `service.CreateRequest.Requests` 注入一个绑定当前 Service 地址的 `request.Client`。Service 不持有目标 Service 对象；`Command` 和 `Query` 仍然把 Message 投递到目标 Durable Inbox，只是调用处会等待对应 Reply，因此业务代码看起来是同步调用。

```go
func newCaller(_ context.Context, create service.CreateRequest) (service.Service, error) {
    return &callerService{}, nil
}

func (s *callerService) Handle(
    ctx context.Context,
    state service.State,
    message contract.Message,
) (service.Decision, error) {
    var response ValueResponse
    if err := request.Query(
        ctx,
        "value.main",
        "value.get",
        ValueRequest{Key: "answer"},
        &response,
    ); err != nil {
        return service.Decision{}, err
    }

    payload, _ := json.Marshal(response)
    return service.Decision{Reply: &service.Reply{
        Key: "value-result", Type: "caller.value-result", Version: 1,
        Payload: payload,
    }}, nil
}
```

对应的进程入口只需启动 Runtime 推进器，不再手工调用 `HandleNext`：

```go
if _, err := runtime.Start(ctx); err != nil {
    return err
}
go func() {
    _ = runtime.Serve(ctx)
}()
```

Host 会把同一个 Client 放入本次 `Handle` 的 context，因此 Service 可以直接使用包级函数；需要显式保存依赖时，也可以使用 `create.Requests.Query(...)`。四个便捷方法与 `contract.Message.Kind` 一一对应：

- `Command` ↔ `MessageCommand`：发送 Command 并等待 Reply。
- `Query` ↔ `MessageQuery`：发送 Query 并等待 Reply。
- `Event` ↔ `MessageEvent`：发布 Event，不等待 Reply。
- `Reply` ↔ `MessageReply`：针对原始 Message 发送关联 Reply；Service 正常处理时仍优先返回 `Decision.Reply`，保证 Outbox 原子提交。
- `Call` / `Dispatch`：可指定 Kind、Version、Payload 和 Metadata 的底层接口。

Reply 使用原请求 Message ID 作为 `CausationID` 关联。嵌套请求自动继承当前消息的 `CorrelationID`、UserID、GoalID 和 RunID。调用 context 的 Deadline 会写进 Message，目标 `Handle` 也会收到相同截止时间；没有 Deadline 时使用 Builder 的 `RequestTimeout`，默认 30 秒。

### 同步请求的约束

- `Command/Query` 只保证当前进程内的同步调用外观，等待中的 Go 调用栈不会跨进程重启恢复。
- 调用是在 Caller 的 Decision 提交前即时投递的，不与 Caller 的 `ACK + Events + Outbox` 原子提交。因此 Command 接收方仍应幂等；需要严格可恢复语义时，应使用 `Decision.Outgoing` 和后续消息继续状态机。
- 同一个 ServiceInstance 同一时间仍只处理一条消息。不要构造 `A.Query(B) -> B.Query(A)` 这样的同步调用环；环形或双向协作应使用 `Event` 或底层 `Dispatch`。
- Runtime 按实例并发，而不是使用可能被全部阻塞的固定小工作池。A 等待 B 时，B 可以继续执行，但 A 自己的下一条 Inbox 消息必须等当前 Handle 返回。

## 动态服务

动态服务的定义必须使用 `building.ScopeVirtual`。通过 `Runtime.DeclareInstance` 创建逻辑实例后，调用方使用明确的 `Message.To` 定向发送：

```go
record, err := runtime.DeclareInstance(ctx, serviceruntime.InstanceDeclaration{
    Address:   "worker.42",
    Component: contract.ComponentRef{Type: "worker", Version: "v1"},
})

_, err = runtime.Publish(ctx, contract.Message{
    Kind:    contract.MessageCommand,
    Type:    "worker.execute",
    Version: 1,
    To:      record.Address,
    Payload: payload,
})
```

动态实例不会修改 RuntimePlan。Plan 固定允许使用的实现版本和静态路由，InstanceDirectory 负责把动态地址解析到 Durable Mailbox。

## 验证

```text
go test ./serviceruntime/...
```

端到端测试覆盖命令投递、ServiceHandle/Apply、事件与 Snapshot、Outbox、Effect、下游订阅、动态实例、Fencing、Sequence 冲突和重启恢复。

## SQLite 持久化

生产或需要进程重启恢复的本地 Runtime 应显式创建并注入 SQLite Store：

```go
store, err := sqlite.Open(ctx, "runtime-data/runtime.db", sqlite.Options{})
if err != nil {
    return err
}

builder, err := serviceruntime.NewBuilder(serviceruntime.BuilderOptions{
    Storage: store,
})
```

SQLite 实现使用 WAL、`BEGIN IMMEDIATE` 和 busy timeout。`CommitMessage` 在同一事务中提交 Inbox ACK、Journal Events、Snapshot、Outbox 和 Effects。测试会关闭数据库、创建新的 Store，再验证 Journal Replay、Pending Outbox 和 Pending Effect 恢复。

## 当前边界

- Builder 未显式注入 Storage 时仍使用内存实现；需要跨进程恢复时必须注入 `persistence/sqlite` Store。
- 当前没有远程 Transport；Message、地址、配置和持久化结构已经保持可序列化。
- 本目录不包含 Task、Agent、Policy、Capability、Model、Orchestrator、Memory 或 Knowledge 业务服务。
- 业务服务只能通过 Message 通信，不能持有其他服务对象。可靠的状态派生消息优先放入 `Decision.Outgoing`；确需同步外观时使用注入的 `request.Client`。
