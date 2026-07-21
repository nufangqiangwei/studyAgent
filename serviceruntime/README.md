# Service Runtime

`serviceruntime` 是新的事件溯源服务运行框架。它位于项目根目录下，与旧的 `internal/runtime` 完全隔离，不在旧实现上增量修改。

当前实现是可运行的单进程 MVP，既提供内存测试存储，也提供可跨进程重开的 SQLite 持久化存储，已经包含：

- 可序列化的 Message、StoredEvent 和 Snapshot 协议。
- 独立 Artifact 数据平面、内存/本地文件后端、流式写入和不可变引用。
- 持久化 Plan 级 inline Payload 上限，默认 64 KiB。
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
- 结构化 RuntimeError、可注入 RetryPolicy，以及 Conflict/Stale/Corrupt 的差异化处理。
- Inbox、Outbox、Effect 和 Activation 执行期 Lease 心跳与安全接管。
- Snapshot checksum、Schema 校验、坏 Snapshot Journal 回退、Snapshot Migrator 和 Event Upcaster。
- 持久化 RuntimePlan hash、冻结 Definition Catalog，以及跨 PlanRevision 的旧消息恢复。
- 按目标 Mailbox 持久化的 `StreamID + Sequence` 顺序、Retry 阻塞和 Dead Letter 解锁。

## 包结构

```text
serviceruntime/
  contract/            基础可序列化协议
  service/             Service、Factory、Decision
  artifact/            Artifact Reader/Writer、内存和本地不可变文件后端
  building/            Register、Manifest、Plan、RoutingTable
  persistence/         持久化端口
  persistence/memory/  内存事务实现
  persistence/sqlite/  SQLite 事务与跨进程持久化实现
  instance/            实例记录、生命周期、Directory、Lease
  activation/          Factory 创建、Snapshot + Replay、Activation
  request/             待删除：旧的同步外观请求客户端与 Reply Broker
  connection/          可选连接模块、driver、Effect Executor 与 Bus 事件协议
  system/              内置 system.call 控制面 Service 与可靠执行器
  transport/           EventBus 和 Outbox 投递
  host/                单条 Inbox 消息处理闭环
  effect/              Executor、Reconciler、Effect Worker
  recovery/            启动恢复协调
  builder.go           对象图装配
  instance_controller.go 通用动态实例控制端口实现
  runtime.go           顶层生命周期和操作入口
```

## 核心运行顺序

```text
Builder.Build
  -> Register.Compile
  -> 创建/恢复静态 ServiceInstanceRecord
  -> 装配 Storage、ArtifactStore、Directory、Activation、Bus、Host、Effect、Recovery

Runtime.Start
  -> 暂停 EventBus
  -> 加载并校验全部持久化 PlanRevision
  -> 恢复跨 Revision InstanceDirectory
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
  -> Sequence Conflict 时丢弃旧 Activation，再从 Journal 恢复

Runtime.Serve
  -> 持续扫描有 Pending Inbox 的 ServiceInstance
  -> 每个就绪实例由独立 goroutine 调用 ServiceHost
  -> 独立推进 Outbox 和 Effect
  -> context 取消或 Runtime.Close 时停止
  -> CorruptState 等致命错误会停止投递并把 Runtime 标记为 Failed
```

## Service 间消息通信

Service 不持有其他 Service 对象，也不注入同步请求客户端。`Handle` 通过 `service.Decision.Outgoing` 描述后续 Command、Query 或 Event；Host 会把这些消息与 Inbox ACK、状态事件和 Effect 一起原子提交到 Outbox。需要响应当前消息时返回 `service.Decision.Reply`。

这种通信方式不依赖等待中的 Go 调用栈，进程退出、Worker 迁移或延迟投递后仍可从持久化状态继续。需要多阶段协作时，由业务状态机消费后续 Reply/Event 并产生下一步 Decision。

进程入口可以启动 Runtime 推进器自动处理 Inbox、Outbox 和 Effect：

```go
if _, err := runtime.Start(ctx); err != nil {
    return err
}
go func() {
    _ = runtime.Serve(ctx)
}()
```

## 可选 Connection 模块

`connection` 是显式安装的业务模块，不是 Runtime 核心组件。通用 `Builder`、`Runtime` 和 `RuntimeStorage` 都不导入或自动挂载它。应用装配层负责注册模块、driver 和 Manifest mount：

```go
connections := connection.NewModule(connection.ModuleOptions{})
if err := connections.RegisterDriver("websocket", websocketDriver); err != nil {
    return err
}
if err := connections.Register(builder); err != nil {
    return err
}

manifest.Services = append(
    manifest.Services,
    connections.Mount(connection.DefaultAddress),
)
runtime, err := builder.Build(ctx, manifest)
```

Service 通过 `Decision.Outgoing` 向 Connection Service 发送 open/send/close/get/list 消息：

```go
payload, err := json.Marshal(connection.OpenRequest{
    Key:    "primary-feed",
    Driver: "websocket",
    Config: config,
})
if err != nil {
    return service.Decision{}, err
}
return service.Decision{Outgoing: []service.OutgoingMessage{{
    Key:     "open-primary-feed",
    Kind:    contract.MessageCommand,
    Type:    connection.OpenMessageType,
    Version: 1,
    To:      connection.DefaultAddress,
    Payload: payload,
}}}, nil
```

Connection Service 的 `Handle` 只生成 Domain Event、Reply 和 `PlannedEffect`。实际 open/send/close 由注册的 Effect Executor 在 `CommitMessage` 成功后执行；`desired_open` 等业务状态保存在 Connection Service 的 Journal/Snapshot 中，不再使用独立的核心 `ConnectionStore`。

Driver 入站数据采用两级持久化事件链：

```text
Driver callback
  -> connection.driver.* Event
  -> Connection Service Inbox
  -> Handle returns targeted Outgoing Event
  -> atomic Outbox commit
  -> connection.message_received / opened / closed / error
  -> owner Service Inbox
```

连接所有者的 `ServiceDefinition.Consumes` 必须声明需要处理的公开事件，例如：

```go
Consumes: []building.MessageContract{
    {Kind: contract.MessageEvent, Type: connection.OpenedEventType, Version: 1},
    {Kind: contract.MessageEvent, Type: connection.MessageReceivedEventType, Version: 1},
    {Kind: contract.MessageEvent, Type: connection.ClosedEventType, Version: 1},
    {Kind: contract.MessageEvent, Type: connection.ErrorEventType, Version: 1},
}
```

公开事件带有明确的 `To=OwnerAddress`，默认不会广播给其他 Service。Driver 应尽量为远端帧提供稳定 `Event.ID`；Runtime 使用 `ConnectionID + Generation + FrameID` 派生消息 ID，以便 Inbox 对重投帧去重。发送 Effect 同样把稳定 EffectID 放入 `Frame.ID`，由需要 exactly-once 语义的外部协议进行幂等处理。

连接 Session、goroutine 和 cancel 只存在于 Service Activation。Activation 恢复后通过通用 `service.ActivationResource` 重建 `desired_open=true` 的连接，Passivate 时释放资源，但不会修改持久化的期望状态。
## 内置 Runtime System Service

Builder 会为每个新编译的 Plan 自动注册并挂载保留组件 `runtime.system@v1`：

```text
Address:  system.runtime
Command:  system.call v1
Reply:    system.result v1
```

EventBus 不拦截或解释 `system.call`，它仍按普通 Command 投递到系统 Service 的 Durable Inbox。系统 Service 的 `Handle` 只校验调用并声明持久化 Effect；提交成功后，Effect Executor 通过窄 `assembly.InstanceControl` 端口执行 Runtime 控制操作，再用稳定 MessageID 把 `system.result` 送回调用者 Inbox。执行后、Effect 完成落库前崩溃时，Reconciler 会安全重放；实例声明和结果消息均保持幂等。

当前公开的首个操作是 `service.instance.declare`。调用方必须在 Definition 中显式获得授权：

```go
building.ServiceDefinition{
    Component: supervisorComponent,
    SystemOperations: []string{
        assembly.SystemOperationDeclareInstance,
    },
}
```

授权 Service 通过普通 `Decision.Outgoing` 异步调用，不能持有 Runtime、Instance Store 或 Directory：

```go
callPayload, err := json.Marshal(system.Call{
    CallID:           spawnID,
    Operation:        system.DeclareInstanceOperation,
    OperationVersion: 1,
    Payload: declarationPayload,
})
if err != nil {
    return service.Decision{}, err
}

return service.Decision{Outgoing: []service.OutgoingMessage{{
    Key:     "declare-child",
    Kind:    contract.MessageCommand,
    Type:    system.CallMessageType,
    Version: system.CallVersion,
    To:      system.Address,
    ReplyTo: supervisorAddress,
    Payload: callPayload,
}}}, nil
```

调用方应把 `CallID` 和等待状态写入自己的事件流，并在后续 `system.result` Reply 中继续 Saga，不能同步等待 Go 调用栈。相同 Address、InstanceID、Component 和 ParentID 的重复声明返回已有实例；相同地址对应不同声明时返回冲突。系统 Service 属于 Runtime 通用控制面，不包含 `agent.spawn`、预算或 ForkPolicy 等业务语义。

系统组件和 `system.call` 路由属于持久化 RuntimePlan。为已经持久化的部署首次启用该内置组件时，应使用新的 `PlanRevision`，避免以相同 Revision 改变 Plan hash。

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

## Artifact 数据平面

大文本、文件、模型请求/响应和向量不进入 Message、Event、Reply、Effect Result 或 Snapshot。Runtime 只传播 `contract.ArtifactRef`，实际字节通过独立 `artifact.Store` 流式访问。Artifact Store 不属于 `persistence.RuntimeStorage`，因为本地文件或对象存储不能与 SQLite 假装共享同一事务。

本地可恢复部署应显式注入本地 Store：

```go
artifacts, err := artifactlocal.Open(
    "runtime-data/artifacts",
    artifactlocal.Options{Name: "local"},
)
if err != nil {
    return err
}
defer artifacts.Close()

builder, err := serviceruntime.NewBuilder(serviceruntime.BuilderOptions{
    Storage:   sqliteStore,
    Artifacts: artifacts,
})
```

Runtime 入口可使用 `BeginArtifact` 流式写入，或使用 `WriteArtifact` 从 `io.Reader` 写入。写入先落到 staging，`Commit` 校验 size/checksum 后以不覆盖既有对象的方式发布；同一 Key + 相同内容的重试幂等，同一 Key + 不同内容返回冲突。Effect 应从稳定 `EffectID` 派生 Key，先完成 Artifact Commit，再把小型 `ArtifactRef` 写入 Effect Result。提交 Artifact 后、Effect 成功落库前崩溃时，Reconciler 按稳定 Key 和 checksum 查询并恢复，不能重新调用 LLM。

Factory 通过 `service.CreateRequest.Artifacts` 只能把只读 `artifact.Reader` 交给 Service；写权限由 Runtime 入口、显式模块或 Effect Executor 使用。`Handle` 可以读取不可变 Artifact，但 `Apply` 和 Replay 禁止读取。影响业务状态的解析结果必须成为 Event 中的确定事实，不能在 Replay 时重新解析 Artifact。

每个 RuntimePlan 都持久化 `InlinePayloadPolicy`：

```go
Payloads: building.InlinePayloadPolicy{
    MaxMessageBytes: 64 * 1024,
    MaxEventBytes:   64 * 1024,
    MaxReplyBytes:   64 * 1024,
    MaxEffectBytes:  64 * 1024,
},
```

零值使用 64 KiB 默认值。EventBus 校验外部 Message/Reply，Host 校验 Event、Outgoing、Reply 和 Effect Payload，Effect Worker 校验执行与 Reconcile 结果。超限数据必须先写 Artifact，再传递引用。

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

SQLite 还持久化不可变 Runtime Manifest/hash、每个目标 Mailbox 的消息 Sequence、Inbox Stream Head，以及 Effect Deadline/Metadata。Schema 使用递增 migration，已有数据库会从旧版本向前升级。

## 当前边界

- Builder 未显式注入 Storage 时仍使用内存实现；需要跨进程恢复时必须注入 `persistence/sqlite` Store。
- Artifact Store 必须通过 `BuilderOptions.Artifacts` 显式注入；未配置时 Artifact API 返回 `artifact.ErrUnavailable`。注入对象由应用关闭，Runtime 不取得其所有权。
- 当前 Artifact 后端提供内存和单机本地文件实现；ACL、保留期、staging/orphan GC 和远程对象存储 Router 尚未实现。
- 当前没有远程 Transport；Message、地址、配置和持久化结构已经保持可序列化。
- PlanRevision 可以跨重启并存，但恢复旧 Revision 时，当前进程仍需注册该 Revision 引用的 Service Factory 和 Effect Executor。
- 有序消息的 Sequence 以目标 Mailbox 为作用域；同一 Stream 的 Retry 会阻止该 Mailbox 的后续 Sequence，Dead Letter 视为终止并推进 Stream Head。
- 本目录不包含 Task、Agent、Policy、Capability、Model、Orchestrator、Memory 或 Knowledge 业务服务。
- 业务服务只能通过 Message 通信，不能持有其他服务对象。可靠的状态派生消息优先放入 `Decision.Outgoing`；不要新增对待删除 `request.Client` 的依赖。
