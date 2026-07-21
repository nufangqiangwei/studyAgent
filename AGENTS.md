# Agents Project Map

本文件是 agent 学习开发项目的导航地图。具体设计放在 `docs/`，当前实现说明放在 `serviceruntime/README.md`；根目录保留入口、约定和索引。

## 项目定位

本项目用于学习和实现一种命令行 agent 产品形态，目标体验参考 Codex CLI、Gemini CLI、Claude Code 这类开发者工具：

- 通过 CLI 与 agent 交互。
- 能读取项目上下文、规划任务、调用工具、修改代码并运行验证。
- 支持多模型、多供应商和可插拔业务能力。
- 以 Go 语言实现，强调模块解耦、接口边界清晰、可测试、可恢复和可扩展。

## 当前实现与历史代码

### 现行实现

`serviceruntime/` 是当前唯一的 Runtime 实现和后续开发基线。设计、修复、测试和扩展应以该目录的实际包边界、`serviceruntime/README.md` 和相关测试为准。

### `internal/` 已归档

原 `internal/` 代码属于旧架构归档，已经不再使用：

- 不把 `internal/` 当作当前实现或设计依据。
- 不在 `internal/` 中继续新增功能、修复现行 Runtime 或复用旧对象图。
- 不为新代码增加对 `internal/...` 的依赖。
- 除非用户明确要求做归档分析、迁移或删除，否则不要读取、修改或整理 `internal/`。
- `main.go` 目前仍引用旧的 `internal/entrance/app`，应视为遗留入口，不代表当前 Runtime 的装配方式。后续入口迁移应直接装配 `serviceruntime`，不能把新 Runtime 重新接回旧对象图。

若旧文档仍把 `internal/llm`、`internal/state`、`NativeLoop` 或旧事件 runner 描述为“当前实现”，这些内容只作为历史材料。与 `serviceruntime` 和两份新设计文档冲突时，以 `serviceruntime` 实际代码为准。

## 新 Runtime 架构定位

新的 Runtime 是进程内、事件溯源、可恢复的通用服务运行平台。第一阶段使用单 Go 进程和本地 Transport，但消息、地址、配置、状态与持久化协议必须保持可序列化，为未来远程 Transport、多进程部署和 worker 迁移保留边界。

Runtime 负责：

- 注册服务定义，校验 Manifest，编译不可变 `RuntimePlan`。
- 托管通用服务实例、Mailbox、生命周期、Activation、Lease 和 Fencing。
- 通过 EventBus 路由并可靠投递 Command、Query、Event 和 Reply。
- 通过 Inbox、Journal、Snapshot、Outbox 和 Effect Store 保存运行事实。
- 在单条消息边界内原子提交 ACK、事件、快照、出站消息和副作用计划。
- 从持久化 Plan、实例记录、Snapshot、Journal、Inbox、Outbox 和 Effect 恢复运行。
- 提供结构化错误、重试、Dead Letter、Reconciliation 和可观测事件。

Runtime 不负责：

- 理解 Agent、Task、Capability、Policy、Model、Orchestrator、Memory 或 Knowledge 的业务语义。
- 决定 Goal 是否完成、选择长期策略或聚合多个业务响应。
- 在核心包中为某种 ServiceType、MessageType、Tool 或模型写特殊分支。
- 直接持有具体业务 Service 对象，或允许 Service 之间互相调用 Go 对象。

## 核心对象与边界

必须严格区分以下四类对象：

```text
ServiceDefinition
  构建期静态定义：Component、Factory、消息契约、Schema、依赖和 Scope

RuntimePlan
  编译后的不可变部署计划：Runtime Revision、静态挂载、路由和恢复策略

ServiceInstanceRecord
  运行期持久化事实：逻辑地址、Mailbox、State Stream、生命周期和 ActivationEpoch

Activation
  当前进程中的临时对象：Service、恢复后的 State、Sequence、Lease 和进程资源
```

约束：

- Register 只保存构建期定义和校验器，不保存运行状态。
- `RuntimePlan` 必须不可变；getter 返回 map、slice、Metadata、Config 和 RawMessage 的副本。
- 配置变化生成新的 `PlanRevision`，不能原地修改正在运行的 Plan。
- Message、Event、Snapshot、Effect 和 ServiceInstance 都要关联其 `PlanRevision`。
- 恢复旧 Revision 时使用旧 Revision 的服务定义和路由，不能偷偷切换到最新 Plan。
- Runtime 顶层只持有基础设施接口和已装配组件，不提供 `Agent()`、`Policy()`、`ToolRegistry()` 等业务对象访问口。
- 核心校验只处理通用约束；业务约束通过 `PlanValidator` 等扩展注册。

## Service 协议：Live 与 Replay 分离

业务服务实现通用 `Service` / `ServiceFactory` 协议：

- `InitialState` 创建可序列化的初始业务状态。
- `Handle` 只处理新的 Inbox Message，返回声明式 `Decision`。
- `Apply` 只根据已经持久化的 `StoredEvent` 更新状态。
- Factory 只重建 HTTP Client、连接、缓存等进程资源，不读取其他服务的业务状态。

`Decision` 可声明：

- `Events`：当前服务的领域事件。
- `Outgoing`：后续 Command、Query 或 Event。
- `Reply`：对当前请求的定向响应。
- `Effects`：提交成功后才允许执行的外部副作用。

必须保持：

- `Handle` 不直接调用 EventBus、其他 Service 或外部 Effect。
- `Apply` 是确定性、无副作用的纯状态转换；不得调用 LLM、网络、文件、随机数或当前时间。
- Replay 只执行 `Apply`，不能重新调用 `Handle`，也不能重新产生 Outgoing、Reply 或 Effect。
- 当前时间、生成 ID 和外部调用结果必须先成为持久化事实，再参与恢复。
- Activation 的内存状态只能在原子 Commit 成功后更新；提交失败时丢弃本次计算结果。

## 消息、路由与 EventBus

跨服务通信只能使用可序列化 `Message`。Message 分为：

- Command：单播，请求目标服务改变状态或执行动作。
- Query：单播，只读；Runtime 级约束是不产生领域事件或外部 Effect。
- Event：按 RoutingTable 发布给零到多个订阅者，也可带明确 `To` 定向投递。
- Reply：按 `ReplyTo` / `To` 返回，不参与发布订阅。

消息必须使用稳定 `ID`，并携带必要的 `CorrelationID`、`CausationID`、`RuntimeID`、`PlanRevision`、`StreamID` 和 `Sequence`。大对象只保存 `ArtifactRef`，不能塞入 Journal 或消息 Payload。

EventBus 是可靠网络，不是业务大脑。它只负责：

- 路由和 `AddressResolver -> DeliveryTarget` 地址解析。
- Inbox 持久化、去重、Claim、Lease、顺序、ACK、Retry 和 Dead Letter。
- Outbox Claim、投递、完成和重试。
- 按 MessageID 提供 At-least-once 下的幂等基础。

EventBus 不负责业务完成判断、多响应聚合、Agent 选择、审批决策、替代策略或 Saga 补偿。这些状态属于 Orchestrator、Gateway 或具体业务 Service。

## 持久化与一致性

可恢复状态分为四层，不能序列化整个 Go Runtime 或 Service 对象：

| 状态层 | 事实来源 | 示例 |
| --- | --- | --- |
| Service Instance State | `ServiceInstanceStore` | Address、DefinitionRef、Mailbox、Lifecycle、Epoch |
| Service Business State | 独立 Event Stream + Snapshot | AgentState、TaskState、CapabilityCallState |
| Message Transport State | Inbox / Outbox | Claim、ACK、Attempt、Delivery Status |
| Effect State | Effect Store | Planned、Started、Succeeded、Failed、ReconciliationRequired |

关键原则：

- Journal 是业务状态事实来源，Snapshot 只是恢复优化。
- 每个有状态 Aggregate 使用独立 Stream 和单调 Sequence；不要把全局事件广播给所有 Aggregate 做恢复。
- Snapshot 必须记录 Schema、PlanRevision、LastSequence 和 checksum；坏 Snapshot 应在可行时回退到 Journal 重建。
- goroutine、mutex、channel、`context.Context`、Client、连接、文件句柄、Socket、Timer 和内存缓存不得持久化。
- 持久化重新创建资源所需的稳定引用、期望状态、请求 ID、结果和 ResumeAt。

单条 Inbox 消息只通过一个原子提交端口完成：

```text
Inbox ACK
+ Journal Append(expectedSequence)
+ Snapshot
+ Outbox Records
+ Effect Records
+ ActivationEpoch fencing check
```

`MessageCommitStore.CommitMessage` 必须检查 Inbox LeaseToken、幂等提交、`ActivationEpoch`、Journal `expectedSequence` 和稳定 ID 冲突。系统承诺 At-least-once，不宣称 Exactly-once；通过稳定 EventID、MessageID、ReplyID、EffectID、Inbox 去重和外部 `IdempotencyKey` 获得可靠结果。

## 实例、Activation 与 Fencing

静态服务由 Builder 根据 Plan 创建 `ServiceInstanceRecord`；动态服务通过受控 API 创建记录，但不能修改 RuntimePlan。所有实例统一使用 Instance Store、Directory、Mailbox、Stream、生命周期和 Activation 协议。

生命周期属于 ServiceHost / Supervisor，不属于业务 Service。Activation 是可丢弃、可重建的进程内缓存：

```text
读取 ServiceInstanceRecord
  -> 获取新的 ActivationLease，ActivationEpoch + 1
  -> 按 DefinitionRef 查找 Factory
  -> 创建 Service 进程对象
  -> 加载 Snapshot
  -> Replay Tail Events，只调用 Apply
  -> 暴露已完整恢复的 Activation
```

- 任一步失败都不能暴露半恢复对象。
- 每次激活使用新 Epoch；迟到的旧 Activation 写入必须被 Fencing 拒绝。
- Epoch 阻止旧进程写入，`expectedSequence` 阻止并发状态覆盖。
- Passivation 只释放内存资源，不删除实例记录、Mailbox、Journal 或 Snapshot。
- Directory 是 Store 的可重建投影，只返回 DeliveryTarget，不返回 Service 对象。

## Effect 与恢复

Effect 必须先作为 `PlannedEffect` 与业务提交一起持久化，再由 Runtime Effect Worker 调用注册的 Executor。具体外部语义属于模块注册的 Executor / Reconciler，不属于 Runtime 核心。

若进程在外部操作完成后、结果落库前崩溃，`Started` Effect 的结果未知。恢复时必须先 Reconcile，再决定完成、重试、补偿、请求用户或失败，不能盲目重做外部写操作。

启动恢复顺序保持为：

```text
加载并校验指定 RuntimePlan Revision
  -> EventBus 保持 Paused
  -> 恢复 ServiceInstanceRecord
  -> 重建 InstanceDirectory
  -> Snapshot + Tail Events，仅 Apply
  -> 扫描 Pending Inbox / Outbox
  -> Reconcile 未完成 Effect
  -> 获取新 Activation Lease / Epoch
  -> 按需激活静态服务和有待处理消息的实例
  -> EventBus 切换 Live
```

崩溃前的 Active 实例在重启后先进入 Recovering，不能直接沿用旧 Activation。Pending Outbox 继续投递，不能重新执行产生它的 `Handle`。

## 业务服务目标边界

以下能力是未来挂载到 EventBus 的业务服务，不属于 `serviceruntime` 核心：

- `TaskService`：TaskState 的唯一写入者；其他服务只能通过 Message 报告进度。
- `Agent Service`：单任务模型循环和可恢复 AgentState。
- `AgentSupervisor`：统一创建 Root/Child Agent，强制深度、宽度、并发、预算和 Attached/Detached 规则。
- `Capability Gateway`：Capability 调用 Saga、Policy 协调、Provider 路由和结果关联。
- `Capability Service`：真正持有 Tool、MCP、HTTP Client 或本地执行器。
- `Policy Service`：Allow / Ask / Deny 和用户审批状态。
- `Model Service`：模型供应商适配和请求响应归一化。
- `Orchestrator Service`：Goal 分解、任务协调、响应聚合、替代策略和补偿。
- `Memory Service`：结构化用户档案、偏好、习惯和事件记忆。
- Knowledge Gateway、Retrieval、Ingestion、Embedding、Vector Store、Document Store 和 Rerank Service。

业务状态所有权必须独立：Task 状态、Goal 状态、Agent 执行状态、Capability Saga、Policy Approval、ServiceInstance 生命周期和消息投递状态不能合并到一个共享 RunState。

Agent、Capability 和知识系统还应保持：

- 主 Agent 与子 Agent 使用同一 Spawn、Mailbox、Activation 和恢复协议；主 Agent 只是 Depth=0 的根实例。
- Agent 只看到 CapabilityDescriptor 并产生声明式 CapabilityCall，不持有 Capability 实例。
- Attached 子 Agent 遵守结构化并发；Detached 子 Agent 把所有权和 ReplyTo 转移给 Orchestrator。
- Retrieval 与 Ingestion 分开；知识写入是可恢复 Saga，不是一次 Vector Upsert。
- Document Store 和结构化 Memory 是事实来源，Vector Index 只是可重建投影。
- 文档、Revision、Chunk、Embedding Request、Vector 和 Effect 使用稳定 ID。
- 知识读写携带 Namespace、ACL 和敏感性，并在写入与查询阶段分别经过 Policy。

## 当前代码结构

```text
serviceruntime/
  README.md             当前实现、运行流程、示例和边界
  builder.go            编译 Plan 并装配 Runtime 对象图
  runtime.go            顶层生命周期、消息入口和动态实例入口
  serve.go              持续推进 Inbox、Outbox 和 Effect
  util.go               Clock、稳定 ID 和基础辅助实现

  contract/             强类型 ID、Message、StoredEvent、Snapshot、可观测事件
  service/              Service、Factory、State、Decision、ActivationResource
  building/             Register、Manifest、Plan、PlanCatalog、RoutingTable
  assembly/             模块装配期可用的通用 Runtime ports / binder
  persistence/          Journal、Snapshot、Inbox、Outbox、Effect、Plan、Commit 等端口
  persistence/memory/   内存事务实现，主要用于测试和临时运行
  persistence/sqlite/   SQLite/WAL 事务实现和 schema migration
  instance/             ServiceInstanceRecord、生命周期、Directory 和 Lease 端口
  activation/           Factory 创建、Snapshot + Replay、Activation 和 Passivation
  transport/            EventBus、路由、Inbox 入箱和 Outbox 投递
  host/                 Claim -> Handle -> Apply -> Commit 单消息闭环
  effect/               Effect registry、Executor、Reconciler 和 Worker
  recovery/             暂停投递下的启动恢复协调
  fault/                结构化 RuntimeError 和 RetryPolicy
  lease/                Inbox、Outbox、Effect、Activation 的 Lease 心跳辅助
  connection/           可选业务模块，不属于 Runtime 核心
  request/              待删除的旧同步请求外观；禁止新增依赖

internal/               旧架构归档，不再使用
main.go                 遗留 CLI 入口，仍指向已归档 internal
docs/                   项目与架构文档
```

`connection` 必须由应用显式注册 ServiceDefinition、Effect 和 Runtime binder；通用 Builder、Runtime 和 Storage 不得硬编码或自动挂载它。其他新业务模块也遵循相同模式。

## 当前已实现能力与边界

`serviceruntime` 当前是可运行的单进程 MVP，已经包括：

- Service、Decision、Message、Journal、Snapshot、Inbox、Outbox、Effect 和 Instance 协议。
- Register、可扩展 PlanValidator、不可变 Plan 和持久化 Plan Catalog。
- 内存与 SQLite 存储，以及原子的 `ACK + Events + Snapshot + Outbox + Effects`。
- 稳定 ID、Inbox 去重、At-least-once、Retry、Dead Letter 和 Mailbox 内顺序。
- Activation、Passivation、Lifecycle、Lease、Epoch、Fencing 和 Sequence Conflict 恢复。
- Snapshot checksum、坏快照回退、Snapshot Migrator 和 Event Upcaster。
- EventBus、ServiceHost、Effect Worker、Recovery 和持续运行的 `Runtime.Serve`。
- 静态挂载与通用 Virtual Service 动态实例。
- 跨重启持久化 PlanRevision，并在恢复旧消息时使用对应旧 Plan。

当前边界：

- 未显式注入 Storage 时 Builder 使用内存实现；跨进程恢复必须注入 `persistence/sqlite`。
- 当前没有远程 Transport。
- 恢复旧 Revision 时仍需在当前进程注册对应 Factory、Effect Executor 和迁移器。
- `serviceruntime` 不包含 Task、Agent、Policy、Capability、Model、Orchestrator、Memory 或 Knowledge 业务服务。
- `request/` 是待移除兼容层；可靠协作使用 `Decision.Outgoing`、Reply/Event 和业务状态机，不依赖等待中的 Go 调用栈。

## 核心运行路径

```text
Builder.Build
  -> Register.Compile
  -> 持久化并加载 Plan Catalog
  -> 创建/恢复静态 ServiceInstanceRecord
  -> 装配 Storage、Directory、Activation、Bus、Host、Effect、Recovery

Runtime.Start
  -> 暂停 EventBus
  -> 恢复 Plan、Instance、State、Inbox、Outbox 和 Effect
  -> 按需激活
  -> 开启 Live Delivery

ServiceHost.HandleNext
  -> Claim Inbox
  -> Activate / Restore
  -> Service.Handle
  -> 通用校验并派生稳定 ID
  -> Service.Apply 新事件
  -> CommitMessage
  -> 提交成功后更新 Activation

Runtime.Serve
  -> 并行推进就绪 Mailbox
  -> 独立推进 Outbox
  -> 独立推进 Effect
  -> fatal / CorruptState 时暂停投递并标记 Failed
```

## 开发约定

开发或评审具体 Service 时，必须先阅读 [Service 开发规范](docs/service-development-guide.md)。详细规范只在该文档维护，`AGENTS.md` 不复制 Service 实现细则。

实现新功能时按以下顺序判断归属：

1. 先确定它属于 Runtime 基础设施、可选模块还是 EventBus 上的业务 Service。
2. 定义或复用最小接口与可序列化结构。
3. 通过 Registry、Factory、PlanValidator、Effect Registry 或 Runtime binder 注册。
4. 通过 Manifest、ComponentRef、ServiceAddress 和 PlanRevision 连接对象图。
5. 为模块本身增加单元测试，为运行路径增加集成与恢复测试。
6. 记录必要的 Runtime Event、稳定 ID、错误分类和恢复事实。

长期禁止：

- 在 Runtime 核心中按 Agent、Tool、Model、Capability 或业务 MessageType 分支。
- Service 之间持有或直接调用彼此的 Go 对象。
- 在 `Handle` 中绕过 Outbox 发送消息或执行外部副作用。
- 在 Replay 中执行外部操作。
- 用全局可变状态、隐藏调用栈或内存 goroutine 作为恢复依据。
- 为新功能扩张 `request.Client` 或设计同步跨服务等待。
- 为现行功能修改已归档的 `internal/`。

基础验证命令：

```text
go test ./serviceruntime/...
```

涉及持久化、并发、租约或恢复的变更，还应覆盖提交前/后崩溃、重复投递、Lease 过期、旧 Epoch、Sequence Conflict、坏 Snapshot、Pending Outbox 和未知 Effect 等场景。

## 设计评审检查表

提交设计或代码前检查：

- Runtime 核心是否仍然通用，没有业务服务名称和消息类型特例？
- 静态 Definition、不可变 Plan、持久化 Instance 和临时 Activation 是否分离？
- Service 是否只通过 Message、Decision、Event 和 Effect 协作？
- Live `Handle` 与 Replay `Apply` 是否严格分开？
- 状态所有权、Stream、Sequence、Snapshot 和 Schema 是否明确？
- 单消息提交是否覆盖 ACK、Journal、Snapshot、Outbox、Effect 和 Fencing？
- 稳定 ID、幂等、At-least-once、Retry、Dead Letter 和 Reconciliation 是否完整？
- 是否能在不依赖原进程调用栈和内存对象的情况下恢复？
- 新能力是否通过接口、注册表、Factory、配置或模块组合扩展？
- 测试是否不依赖真实 LLM，并覆盖关键恢复路径？

## 文档导航

- [Service Runtime 实现说明](serviceruntime/README.md)：当前已实现能力、包结构、运行流程、示例和限制；判断代码现状时优先阅读。
- [Runtime 框架接口设计](docs/runtime-framework-interface-design.md)：Runtime 核心接口、对象关系、原子提交、Activation、Effect 和恢复设计。
- [事件溯源服务 Runtime 架构](docs/event-sourced-service-runtime-architecture.md)：完整目标架构，以及 Task、Agent、Capability、Policy、Orchestrator、Memory 和 Knowledge 等业务服务边界。
- [Service 开发规范](docs/service-development-guide.md)：业务 Service 的依赖边界、协议、状态、Decision、Replay、Effect、注册、版本和测试要求。
- [文档索引](docs/README.md)：`docs/` 目录内文档清单和维护原则。
- [项目概览](docs/project-overview.md)：项目目标、用户场景和非目标。
- [产品能力](docs/product-goals.md)：CLI agent 核心能力和学习拆解。
- [开发约定](docs/development-guide.md)：Go 代码风格、接口、测试和解耦原则。
- 其他仍基于旧 `internal/`、NativeLoop 或旧 runner 的架构、模块、现状和重构文档属于历史参考，不能覆盖上述新设计与当前代码。

## 推荐阅读顺序

1. 先读 [Service Runtime 实现说明](serviceruntime/README.md)，确认当前代码已经实现到哪一步。
2. 再读 [Runtime 框架接口设计](docs/runtime-framework-interface-design.md)，理解核心接口和必须保持的不变量。
3. 开发具体 Service 前读 [Service 开发规范](docs/service-development-guide.md)。
4. 再读 [事件溯源服务 Runtime 架构](docs/event-sourced-service-runtime-architecture.md)，理解未来业务服务如何挂载到 Runtime。
5. 按任务阅读 `serviceruntime` 对应包和测试；代码与设计稿不一致时，以代码和测试描述的已实现行为为准，并明确记录差异。
6. 最后参考项目概览、产品能力和开发约定；旧 `internal/` 文档只用于历史追溯。

## 本地 Go 开发环境

当前 IDE 使用的 Go SDK 路径：

```text
C:\Users\qiangwei\go\pkg\mod\golang.org\toolchain@v0.0.1-go1.26.0.windows-amd64
```

## 本地辅助文档目录约定

- `localDocs/` 和 `plan/` 是用户用于理解项目的本地辅助文档目录。
- 在没有用户明确允许的情况下，agent 禁止读取、列举、搜索、总结、写入、修改或删除这两个目录下的任何内容。
- 如果任务确实需要使用这两个目录中的内容，必须先向用户请求许可；只有当用户明确授权后，才能在授权范围内访问。

## 根目录职责

根目录保留高层入口和新 Runtime：

- `AGENTS.md`：项目导航、现行架构约束和开发规则。
- `go.mod`：Go 模块定义。
- `serviceruntime/`：当前事件溯源服务 Runtime 实现。
- `docs/`：项目文档和设计稿。
- `main.go`：尚未迁移的遗留 CLI 入口，不作为当前 Runtime 架构依据。
- `internal/`：只读归档，不再用于现行开发。

新增通用 Runtime 能力应进入职责明确的 `serviceruntime` 子包；新增 Agent 产品能力优先作为可注册模块或独立业务 Service 实现，避免逻辑堆积在根包、`main.go` 或一个中央循环中。
