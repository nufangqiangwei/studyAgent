# Service 开发规范

> 适用范围：`serviceruntime` 上的业务 Service、可选模块及其 Effect
>
> 依据：当前 `serviceruntime` 代码、`runtime-framework-interface-design.md` 和 `event-sourced-service-runtime-architecture.md`

## 1. 目标与优先级

本规范说明如何为新的事件溯源 Runtime 开发一个 Service。目标是让每个 Service 都可以独立注册、可靠投递、确定性 Replay、崩溃恢复和版本演进，同时不破坏 Runtime 的通用边界。

判断实现行为时，优先级如下：

1. `serviceruntime` 当前代码和测试。
2. 本规范。
3. Runtime 接口设计与目标架构文档。
4. 旧 `internal/`、NativeLoop 和旧 runner 文档仅作历史参考。

## 2. 先判断是不是 Service

适合作为业务 Service 的能力通常满足以下一项或多项：

- 拥有独立业务状态和状态所有权。
- 需要通过 Command、Query、Event 或 Reply 与其他模块协作。
- 需要 Durable Mailbox、重试、恢复、延时继续或多阶段 Saga。
- 需要独立扩缩、替换实现或未来迁移到远程进程。

以下能力通常不是业务 Service：

- Journal、Inbox、Outbox、Lease、Activation、Fencing 等通用 Runtime 基础设施。
- 纯序列化 DTO、无状态算法或可直接作为库调用的确定性函数。
- 只为某个 Service 执行已持久化副作用的 `effect.Executor`。
- 进程内资源的生命周期适配器；它应由 Factory 或 `ActivationResource` 管理。

不要为了复用而把普通 helper 包装成 Service，也不要把具有独立状态所有权的业务能力塞进 Runtime 根对象。

## 3. 依赖边界

Service 的业务实现优先只依赖：

- `serviceruntime/contract`
- `serviceruntime/service`
- `serviceruntime/artifact`，仅在 Service 确实需要读取不可变 Artifact 时依赖 `artifact.Reader`
- 自己的领域类型和 Go 标准库

Definition、Module 和装配代码可以额外依赖：

- `serviceruntime/building`
- `serviceruntime/effect`
- `serviceruntime/assembly`，仅在确实需要把外部回调重新送入 Durable Inbox 时使用

业务 Service 禁止依赖：

- `serviceruntime/host`
- `serviceruntime/transport`
- `serviceruntime/activation`
- `serviceruntime/recovery`
- `serviceruntime/persistence/memory` 或 `persistence/sqlite`
- Runtime 根对象的具体实现
- 已归档的 `internal/...`

Service 不得取得其他 Service 的 Go 对象、持久化 Store、Artifact Writer、Activation 或内存状态。`service.CreateRequest.Artifacts` 是只读不可变数据端口，不授予业务 Service 写 Artifact 或访问 Runtime Storage 的能力。跨服务依赖表示逻辑地址和消息契约，不表示对象注入。

`ServiceDefinition.Dependencies` 当前用于编译期绑定、类型检查和环检测。若 Service 运行时需要解析后的依赖地址，应先扩展通用、可序列化的创建协议；不得通过读取全局 Plan、导入 Runtime 根包或注入目标 Service 对象绕过边界。

## 4. 推荐包结构

当前可选模块可参考 `serviceruntime/connection`。推荐按职责拆分：

```text
<service>/
  contract.go      ComponentRef、消息/事件/Effect 类型和版本
  types.go         输入、输出和公开 DTO
  state.go         持久化业务状态、编码与解码
  service.go       Descriptor、InitialState、Handle、Apply
  factory.go       Factory 和 ServiceDefinition
  module.go        注册 Service、Effect、Validator、Binder
  effects.go       Executor、Reconciler 和外部适配
  migration.go     Snapshot Migrator / Event Upcaster（需要时）
  *_test.go        协议、决策、Replay、恢复和集成测试
```

文件名可以按领域调整，但以下边界不能合并：

- 业务决策与外部副作用执行分开。
- 持久化业务状态与进程资源分开。
- Service 实现与 Runtime 装配分开。
- 公开消息契约与内部 helper 分开。

## 5. 先设计协议，再写 Handle

实现前先明确：

1. Service 唯一拥有哪部分业务状态。
2. 消费哪些 Command、Query、Event、Reply。
3. 产生哪些领域事件、Outgoing、Reply 和 Effect。
4. 每条消息如何关联 CorrelationID、CausationID 和业务 Stream。
5. 哪些步骤可能暂停，收到什么消息后恢复。
6. 提交前、提交后和外部结果未知时分别如何恢复。
7. State、Event、Message 和 Effect 如何做版本演进。

### 5.1 命名与版本

- `ComponentRef.Type`、MessageType、EventType 和 EffectType 使用稳定、带领域命名空间的名称，例如 `connection.manager`、`connection.open`。
- `ComponentRef.Version` 必须显式，例如 `v1`。
- Message、Event、Reply 和 Effect 的 `Version` 必须为正数。
- 已持久化或对外发布的类型不能无版本修改语义；不兼容变更增加版本并提供迁移或 Upcaster。
- 区分内部状态事件和公开集成事件，避免其他 Service 依赖本 Service 的内部 Event Stream 结构。

### 5.2 Payload

- 使用明确的 struct 和 JSON tag，不把 `any` 当作默认协议。
- Payload 必须可序列化、可校验、尺寸受控。
- map、slice、`json.RawMessage`、`[]byte` 和时间指针在跨边界保存前需要复制。
- 大文本、文件、模型请求、向量等使用 `ArtifactRef`，不长期内嵌在 Message、Event 或 Snapshot。
- RuntimePlan 默认限制 Message、Event、Reply 和 Effect inline Payload 为 64 KiB；需要调整时显式设置 `RuntimeManifest.Payloads`，不能用提高上限代替 Artifact。
- Artifact Key 必须从 EffectID、MessageID 或稳定业务 ID 派生；不要使用随机重试 ID。
- Secret、Token、连接句柄和不必要的敏感参数不得进入 Journal、Snapshot、Metadata 或错误消息。

## 6. ServiceDefinition 规范

每个 Service 必须提供完整的 `building.ServiceDefinition`：

- `Component`：与 `Descriptor().Component` 完全一致。
- `Factory`：能够为每个逻辑实例创建新的 Service 对象。
- `Consumes`：精确声明允许处理的 Kind、Type、Version。
- `Produces`：精确声明所有 Outgoing 和 Reply 的 Kind、Type、Version。
- `EffectExecutors`：声明可能出现在 Decision 中的 ExecutorRef。
- `SystemOperations`：声明允许该 Definition 通过 `system.call` 使用的 Runtime 控制操作；默认没有权限。
- `Dependencies`：声明构建期依赖名称、是否必需和可接受的 ServiceType。
- `StateSchema` / `ConfigSchema`：有状态或可配置 Service 应明确声明。
- `Scope`：按真实实例模型选择。

Scope 使用规则：

- `ScopeRuntimeSingleton`：同一 Component 在一个 Plan 中只能静态挂载一次。
- `ScopeMounted`：由 Manifest 静态挂载，可存在多个不同地址。
- `ScopeVirtual`：由 `Runtime.DeclareInstance` 动态创建逻辑实例，不为每个实例修改 RuntimePlan。

Host 会验证实际输入是否在 `Consumes` 中、Decision 的 Outgoing/Reply 是否在 `Produces` 中、Effect 是否在 `EffectExecutors` 中。不要依赖“代码能走到”来替代契约声明。

## 7. State 与状态所有权

- 一个业务事实只能有一个 Service 作为写入者。
- `service.State.Data` 保存显式 JSON 状态，`SchemaVersion` 必须与状态编码匹配。
- ServiceInstanceRecord 只保存 Address、Mailbox、Lifecycle、Epoch 等控制状态，不能复制整个业务状态。
- 不把其他 Service 的状态、消息历史或 Saga 细节复制到本 Service；只保存继续本状态机所需的引用和已确认事实。
- map 和 slice 解码后应初始化，写入 State 前应避免共享可变引用。
- Snapshot 是优化，不是事实来源；任何状态都必须能从 InitialState + Journal Events 重建。

业务状态不得保存在 Service struct 字段、Module 全局变量或 goroutine 闭包中。Service struct 只保存不可变配置、通用依赖和进程资源句柄；权威业务状态始终来自传入的 `service.State`。

## 8. Handle 规范

`Handle` 是 Live 决策函数。它必须：

1. 校验消息 Kind、Type、Version、调用者、Payload 和领域前置条件。
2. 从传入 State 解码当前业务状态。
3. 产生声明式 Decision，不直接提交状态。
4. 对同一个 MessageID 和同一历史状态产生可幂等重试的输出。

`Handle` 禁止：

- 直接调用 EventBus 或 `request.Client`。
- 直接调用其他 Service。
- 直接调用 Runtime、Instance Store 或 `Runtime.DeclareInstance`；需要 Runtime 控制能力时发送异步 `system.call`。
- 直接写 Journal、Inbox、Outbox、Snapshot、Effect Store 或业务数据库。
- 直接执行 LLM、MCP、HTTP 写请求、文件写入或其他外部副作用。
- 启动决定业务正确性的后台 goroutine。
- 修改 Activation 中的权威 State；内存 State 只由 Host 在 Commit 成功后替换。

Live Handle 可以使用注入的 Clock 形成事件中的确定时间，但 `Apply` 必须只读取事件内时间。需要持久化身份时，优先从 MessageID 和稳定业务键派生；不要在每次重试中调用随机 `New` 生成新的 Event、Outgoing 或 Effect 身份。

### 8.1 Query

Query 必须只读：

- 不产生 `Events`。
- 不产生 `Effects`。
- 可以返回 Reply；若无需响应，也可以返回空 Decision。
- 不得把“先改状态再查询”隐藏在 helper 或进程资源中。

### 8.2 业务错误与 Go error

- 可预期的领域拒绝应使用稳定错误码；输入有 `ReplyTo` 时优先返回 `Reply.Error`。
- 无 `ReplyTo` 的非法命令可以返回 Go error，由 Runtime Retry/Dead Letter 策略处理。
- 状态损坏、未知持久化事件、违反不变量和无法安全继续的情况必须返回 error，不能伪装成成功 Reply。
- 不在错误中泄露 Secret、完整敏感 Payload 或外部凭证。

## 9. Decision 规范

### 9.1 Key 与稳定 ID

- Events、Outgoing、Effects 和 Reply 的 `Key` 均非空。
- 一个 Decision 内所有输出共享同一个 Key 命名空间，必须全局唯一。
- Key 必须由稳定的业务含义构成；同一输入重试时保持不变。
- Host 使用 `InputMessage.ID + Decision.Key` 派生稳定 EventID、MessageID、ReplyID 和 EffectID。
- Key 用于幂等身份，不要把 slice 下标作为长期语义，也不要包含随机数或当前进程地址。

### 9.2 Event

- Event 表达已经决定发生的业务事实，使用过去式语义。
- Event Payload 包含 Apply 所需的全部确定信息，包括时间、生成后的业务 ID 和已确认外部结果。
- 不持久化可以从其他字段稳定推导的大对象。

### 9.3 Outgoing 与 Reply

- Command、Query 和 Reply 必须有明确目标；广播 Event 可由 RoutingTable 解析。
- 只希望通知某个 Owner 的 Event 应设置明确 `To`，避免意外广播。
- 默认让 Host 继承 CorrelationID、设置 CausationID；只有建立新的业务关联或 Stream 时才显式覆盖。
- Outgoing 和 Reply 必须在 `Produces` 中声明。
- Deadline 必须晚于当前处理时间；已过期工作不应产生新的副作用。

### 9.4 Effect

- 外部操作只通过 `PlannedEffect` 描述。
- `ExecutorRef` 必须同时注册到 Effect Registry，并声明在 ServiceDefinition 中。
- `IdempotencyKey` 必须稳定并适合传递给外部系统。
- 同一业务操作的重投不得产生新的外部身份。
- Effect Payload 只保存执行和 Reconciliation 必需的数据或 ArtifactRef。

### 9.5 Runtime System Call

`system.runtime` 是 Runtime 自动挂载的通用控制面 Service。业务 Service 只能通过显式目标的 `system.call` Command 异步请求 Runtime 能力，不能注入 Runtime 根对象或控制 Store。

- Definition 必须在 `SystemOperations` 中声明所需操作；默认拒绝。
- `CallID` 必须使用稳定业务 ID，并持久化 Pending 状态。
- 调用必须设置 `ReplyTo`，后续通过 `system.result` Reply 继续状态机。
- 系统调用由已持久化 Effect 执行；业务 Service 不得假设当前 Handle 返回前操作已经完成。
- 当前 `service.instance.declare` 只接受 `ScopeVirtual` Component；Runtime 从持久化 Parent Record 计算 RootID 和 Depth。
- 重复调用必须保持 Address、InstanceID、Component 和 ParentID 一致，否则作为冲突拒绝。
- Agent 深度、预算、授权范围等业务规则仍由 AgentSupervisor/Policy 检查，不能下沉到通用 System Service。

## 10. Apply 与 Replay 规范

`Apply` 是唯一的 Replay 路径：

- 只根据 `service.State` 和 `StoredEvent` 返回新 State。
- 不读取当前时间、随机数、环境变量、网络、Artifact 或进程缓存。
- 不发送消息，不执行 Effect，不启动 goroutine。
- 按 EventType 和 EventVersion 显式分派。
- 未知、损坏或不兼容事件必须返回 error；不能静默忽略后继续恢复。
- 不修改输入 State 共享的 map、slice 或 RawMessage。

推荐测试同一事件序列多次 Replay 都得到字节等价或语义等价状态，并验证 Replay 期间外部执行器调用次数为零。

## 11. Factory 与 ActivationResource

Factory 必须：

- 仅根据 `service.CreateRequest` 和模块级构建依赖创建 Service。
- 为不同 Instance 创建隔离的 Service 对象。
- 不读取其他 Service 的业务状态。
- 不从 Snapshot 恢复业务状态；恢复由 ActivationManager 和 Restorer 完成。
- 返回的 `Descriptor.Component` 与 DefinitionRef 一致，StateSchema 与 Definition 一致。

HTTP Client、SDK、连接、缓存、worker 等进程资源不进入 State 或 Snapshot。需要按 Activation 恢复和释放时，实现 `service.ActivationResource`：

- `RestoreResources` 在 Snapshot + Journal Replay 完成后、Activation 可见前执行。
- `ReleaseResources` 在 Passivation 和 Lease 释放前执行。
- 两者不得直接修改 Durable State。
- Restore 失败后必须能安全 Release，避免泄漏半创建资源。
- Restore 和 Release 应尽量幂等，并正确响应 context 取消。

## 12. Effect Executor 与 Reconciliation

Executor 只处理已经持久化的 EffectRecord，不能反向修改 Service 内存状态。

- 执行前尊重 Deadline、context 和外部幂等键。
- 外部 API 支持幂等键时必须传递 `IdempotencyKey`。
- 外部写操作结果未知时，不能简单重做。
- 能出现“已执行但结果未落库”的 Effect 应提供 Reconciler。
- Reconciler 先查询外部事实，再选择 Complete、Retry、Compensate、AskUser 或 Fail。
- Effect 结果若需要推进业务状态，应通过 Durable Message/Event 回到目标 Inbox，而不是调用 Service 对象。

只读、天然幂等的操作可以使用安全重试；写文件、发消息、创建远程资源和修改外部数据必须明确写出未知结果恢复策略。

## 13. Module 与注册

一个可选业务模块应集中拥有自己的 Factory、Effect、Driver、资源目录和 Validator，并通过窄 Registrar 接口安装：

```go
type Registrar interface {
    RegisterService(building.ServiceDefinition) error
    RegisterEffect(effect.Spec) error
    RegisterPlanValidator(building.PlanValidator) error // 需要时
    RegisterRuntimeBinder(assembly.RuntimeBinder) error // 需要时
}
```

模块必须在 `Builder.Build` 前完成注册。Runtime 核心不得导入模块包，也不得按模块名自动挂载。

只有外部 callback 无法通过普通 Decision 表达时才使用 RuntimeBinder。Binder 只能取得通用 `RuntimePorts`，并把 callback 转成稳定、可序列化 Message 送入 Durable Inbox；除显式的 Artifact 数据端口外，不能取得 Host、持久化 Store 或 Service 对象。模块和 Effect Executor 可以通过 `RuntimePorts.Artifacts` 流式写入，但 Durable Message 和 Effect Result 只能保存最终 `ArtifactRef`。

`serviceruntime/connection` 是当前参考实现，但不是核心特例。新模块应复用它的注册方式，不复制它的领域模型。

## 14. 版本与迁移

以下变更需要显式版本策略：

- State JSON 结构变化：提高 SchemaVersion，提供 Snapshot Migrator。
- Event Payload 或语义变化：增加 EventVersion，提供 Event Upcaster 或兼容 Apply 分支。
- Message 协议不兼容变化：增加 Message Version，并在 Consumes/Produces 中同时声明迁移期版本。
- Component 实现不兼容变化：发布新的 ComponentRef Version。

旧 PlanRevision 可能在重启后继续恢复，因此不能因为发布新版本就删除旧 Factory、Effect Executor、Migrator 或 Upcaster。清理旧版本前必须证明没有实例、消息、事件和 Effect 仍引用它。

## 15. 并发、安全与可观测性

- 同一 Mailbox 的顺序不代表整个进程只有一个 Service 在运行；不同实例会并行推进。
- 不依赖进程全局串行、goroutine 顺序或内存锁保存业务正确性。
- 外部 callback、ActivationResource 和共享 Driver Registry 必须自行保证线程安全。
- 授权规则属于 Policy 或本 Service 明确拥有的领域规则；不要信任调用者自报 Depth、Owner、Grant 或权限。
- Runtime 基础设施事件由 RuntimeEventRecorder 记录；Service 的业务事实使用领域 Event，不要混用两套事件。
- Metadata 只保存小型索引和追踪信息，不保存大 Payload 或 Secret。

## 16. 测试要求

每个 Service 至少覆盖：

### 单元测试

- InitialState 编码、解码和 SchemaVersion。
- 每种 Consumes 消息的 Handle 正常、拒绝和边界路径。
- Query 不产生 Event 或 Effect。
- Decision Key 全局唯一且重试稳定。
- Produces、EffectExecutors 和 Descriptor 与 Definition 一致。
- Apply 对每种 EventType/EventVersion 的状态转换。
- 未知事件、坏 Payload 和损坏状态返回 error。
- Replay 确定性且无外部调用。
- map、slice、RawMessage 和 Metadata 没有别名修改。

### 集成与恢复测试

- Register + Manifest Compile 成功，非法路由/依赖/契约失败。
- Publish -> Inbox -> Handle -> Apply -> Commit -> Outbox/Effect 完整闭环。
- 重复 MessageID 不产生重复业务事实。
- 提交前失败会重试，提交后恢复不会重复 Handle。
- Snapshot + Tail Events 与全 Journal Replay 结果一致。
- Sequence Conflict、Stale Epoch、Lease Lost 和 Dead Letter 行为。
- Pending Outbox、Pending Effect 和未知 Effect 的重启恢复。
- ActivationResource 的 Restore、Release、失败清理和 Passivation。
- Virtual Service 需要覆盖 Declare、定向投递、Passivate、恢复和 Terminate。

基础命令：

```text
go test ./serviceruntime/<service>/...
go test ./serviceruntime/...
```

测试使用 fake Clock、稳定 ID、内存 Store 和 fake Executor；不依赖真实 LLM、网络或外部数据库。涉及 SQLite 协议时再增加关闭数据库、重新 Open 后的恢复测试。

## 17. 开发顺序

新增 Service 按以下顺序实现：

1. 定义状态所有权、消息流程和恢复点。
2. 定义 Component、Message、Event、Reply、Effect 类型与版本。
3. 定义 State Schema、Config Schema 和序列化 DTO。
4. 编写 ServiceDefinition 和 Compile 失败测试。
5. 实现 InitialState、Handle 和 Decision 单元测试。
6. 实现 Apply 和纯 Replay 测试。
7. 实现 Factory；需要进程资源时再实现 ActivationResource。
8. 实现并注册 Effect Executor / Reconciler。
9. 实现 Module 注册和 Manifest mount 示例。
10. 增加 Runtime 闭环、幂等、崩溃恢复和版本迁移测试。
11. 更新模块 README 或相关架构文档，不把模块细节写进 Runtime 核心说明。

## 18. 提交前检查表

- [ ] Service 状态所有权单一且明确。
- [ ] Service 实现没有导入 Host、Transport、具体 Storage 或旧 `internal/`。
- [ ] Component、消息、事件、Effect 和 Schema 都有显式版本。
- [ ] Consumes、Produces、Dependencies 和 EffectExecutors 声明完整。
- [ ] Handle 只产生 Decision，没有直接消息发送、Store 写入或外部副作用。
- [ ] Query 没有 Event 和 Effect。
- [ ] Decision Key 全局唯一、稳定、可幂等重试。
- [ ] Apply 确定、无副作用，并拒绝未知持久化事件。
- [ ] 业务状态没有藏在 Service struct、Module 全局变量或 goroutine 中。
- [ ] 进程资源通过 Factory / ActivationResource 重建和释放。
- [ ] 外部写 Effect 有稳定 IdempotencyKey 和未知结果处理策略。
- [ ] 旧 PlanRevision 所需 Factory、Executor 和迁移器仍可用。
- [ ] 单元、集成、重复投递和重启恢复测试齐全。
