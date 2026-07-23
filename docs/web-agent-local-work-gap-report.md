# Web Agent 本地文件编辑与报告交付功能补齐报告

> 初始盘点日期：2026-07-22
> 最近更新：2026-07-23（实现基线：`newFramework@467bbf6`，已合并 PR #13-#16）
> 盘点范围：`serviceruntime/`、`services/`、`main/server/`、`main/cli/`、`frontend/` 及现行设计文档  
> 目标：以 Web Server 为用户入口，展示真实历史任务和每个任务的历史执行记录，并让 Agent 能可靠完成本地文件编辑和报告写入  
> 限制：本报告持续记录当前实现与目标之间的差距，不替代各模块开发规范和具体 Issue 验收说明

## 1. 状态标记

| 标记 | 含义 |
| --- | --- |
| **[已完成]** | 当前代码已有可运行实现，并有对应测试或明确入口。 |
| **[部分完成]** | 基础对象或单独模块已经存在，但尚未形成目标要求的端到端能力。 |
| **[待实现-P0]** | 达成“Web 发起任务，Agent 修改本地文件并交付报告”的必需项。 |
| **[待实现-P1]** | MVP 可运行后应尽快补齐，影响可用性、安全性或长期运行。 |
| **[待实现-P2]** | 不阻塞首个闭环，可在稳定性和体验阶段迭代。 |

## 2. 总体结论

当前项目已经完成事件溯源 Runtime、Task/Agent/Capability/Approval 等核心业务模块的主要骨架，并已将 Web Server 真实装配到 SQLite Runtime、本地 Artifact Store、持久化 Web Gateway 和 Runtime Adapter。`POST /v1/tasks` 与 `GET /v1/tasks/{id}` 已可创建、查询并跨重启恢复处于 `created` 阶段的真实 Task。

但是，**Web 提交尚未自动推进到 Ready/Assigned/Started，历史任务列表与执行记录仍主要是前端演示数据；“Agent 可编辑本地文件并写报告”在现行 `serviceruntime` 业务链路中仍未实现。**

当前真实状态可以概括为：

```text
Web 页面与静态历史展示                    [已完成]
Web Server 静态资源与 API 边界            [已完成]
Web Server -> Runtime 真实装配与适配        [已完成]
真实 Task 创建与单项查询、SQLite 重启恢复  [已完成]
Web 提交 -> Ready/Assign/Start             [已完成]
真实任务列表与历史执行记录                 [待实现-P0]
Task -> Agent -> Model 的独立模块能力       [部分完成]
本地文件读取、搜索、补丁、写入 Capability  [待实现-P0]
写文件审批的完整上行决议链路               [待实现-P0]
结果 Artifact/报告文件在 Web 中展示        [待实现-P0]
完整执行与文件写入的跨重启恢复验证          [待实现-P0]
```

因此，现在还不能把产品状态描述为“Web Agent 已能查看真实历史并完成本地编辑”。更准确的描述是：**Web Server 与 Runtime 的真实持久化入口已经接通，当前可以可靠创建和查询 Created Task；任务自动执行、历史 Projection、Workspace 能力、审批和结果展示仍待补齐。**

## 3. 当前已完成能力

### 3.1 Runtime 基础设施

- **[已完成]** 可序列化 Message、StoredEvent、Snapshot、ArtifactRef 协议。
- **[已完成]** Journal、Snapshot、Inbox、Outbox、Effect、ServiceInstance 的内存与 SQLite 存储。
- **[已完成]** 单消息边界内的 `ACK + Events + Snapshot + Outbox + Effects` 原子提交。
- **[已完成]** At-least-once、稳定 ID、Inbox 去重、Lease、Dead Letter、ActivationEpoch 和 Fencing。
- **[已完成]** Snapshot + Journal Replay、坏快照回退、PlanRevision 恢复和 Pending Outbox/Effect 恢复。
- **[已完成]** 本地不可变 Artifact Store，可流式保存模型输入输出、报告文本和其他大对象。
- **[已完成]** Runtime 的单进程持续推进器，可并行推进 Inbox、Outbox 和 Effect。

这些能力可以作为 Web Agent 的可靠底座，但它们只解决通用运行与恢复，不自动提供任务列表、执行历史或工作区文件工具。

### 3.2 Task Service

- **[已完成]** 一个 Task 对应一个 Virtual Service 实例、独立 Mailbox 和独立事件流。
- **[已完成]** `Created / Ready / Running / Waiting / Suspended / Completed / Failed / Cancelled` 状态机。
- **[已完成]** 创建、分配、开始、暂停、恢复、重试、取消和单项查询协议。
- **[已完成]** TaskID、Attempt、RunID、AssignedTo、最终 ResultRef、错误和时间等当前状态。
- **[已完成]** Task 向 Agent 发送 `agent.execute`，并接收 `agent.completed` 终态回复。
- **[已完成]** 对迟到 Run、错误 Agent 地址和错误 CorrelationID 的防护。
- **[部分完成]** 已定义 `task.execution.waiting/resumed`，但当前 Agent 尚未实际发送这些进度消息。
- **[部分完成]** Journal 中存在完整状态迁移事实，但没有面向 Web 的任务列表和执行历史 Projection。

### 3.3 Agent、模型、Capability 与审批

- **[已完成]** Agent 的可恢复模型回合：能力发现、Prompt 准备、模型调用、Capability 调用、最终输出准备。
- **[已完成]** Agent Run 保存 Turn、模型请求/响应 Artifact、Capability 调用结果和终态输出。
- **[已完成]** LLM Client 通过持久化 Effect 调用模型，并将结果保存到 Artifact Store。
- **[已完成]** Capability Catalog、Provider、AuthorizationEvaluator、Effect/Service Command 两类执行计划。
- **[已完成]** Capability 调用的 Allow/Ask/Deny、等待审批、执行、终态结果和幂等状态机。
- **[已完成]** Approval Service 的请求、批准、拒绝、取消、过期和审计状态机。
- **[已完成]** Interaction Service 可接收一次用户请求、发送 `agent.execute`、接收终态并调用 Presenter。
- **[部分完成]** CLI 已装配 SQLite、Artifact、LLM、Agent、Capability、Approval 和 Interaction，但 Capability Catalog 被明确配置为空。
- **[待实现-P0]** 当前新架构没有任何真实 `workspace.*` Provider、Executor 或 Reconciler。

### 3.4 Web Server 与前端

- **[已完成]** Go Web Server 可嵌入并提供前端静态资源。
- **[已完成]** 已定义 `POST /v1/tasks`、`GET /v1/tasks/{task_id}` 和审批下行 WebSocket 的 HTTP 边界。
- **[已完成]** 请求体大小、JSON 字段、TaskID、用户头、方法、错误响应和静态资源缓存等基础校验。
- **[已完成]** Web Server 默认构建并启动真实 SQLite Runtime 和本地 Artifact Store，不再在生产路径使用 `unavailableRuntimePort`。
- **[已完成]** Web Gateway 通过 `system.call` 声明 Virtual Task，并以 Durable Message 完成 `task.create/task.get`。
- **[已完成]** Runtime Adapter 只持有窄 Ingress/ID 端口，可等待提交后的 Presentation，并安全处理取消、迟到结果、去重和 Runtime 不可用。
- **[已完成]** `POST /v1/tasks` 可创建真实 `created` Task，`GET /v1/tasks/{id}` 可按用户读取当前状态；SQLite 重启后仍可查询同一 Task。
- **[已完成]** 生产 Run 按 Build → Start → Serve → Listen 顺序启动，并监督 Runtime/HTTP fatal 与正常关闭。
- **[已完成]** 前端已有历史任务侧栏、任务选择、对话区、执行进度卡、输入区和响应式布局。
- **[部分完成]** 历史任务、消息和执行进度全部由 `frontend/app/page.tsx` 中的常量或本地 State 生成。
- **[部分完成]** 输入框发送后只追加本地固定回复，没有调用 `/v1/tasks` 或 Runtime。
- **[部分完成]** 审批 WebSocket 已接收提交后的在线通知，但没有 pending list、Cursor、断线重放或 approve/deny 上行。
- **[待实现-P0]** 当前 API 没有任务列表、Run 列表、时间线、结果内容、取消、重试和审批决议接口。

### 3.5 当前验证基线

- **[已完成]** `go test ./serviceruntime/... ./services/... ./main/server ./main/cli` 当前通过。
- **[已完成]** Web Runtime 集成测试覆盖真实 SQLite/Artifact Store、HTTP Handler、Gateway、Task、Presentation、关闭重建和重启后查询。
- **[已完成]** 当前测试覆盖相同 TaskID 幂等创建、内容冲突、跨用户安全 NotFound、Runtime 未 Live/fatal/Adapter Close 的有界返回，以及关键 Adapter 并发路径的 race 检测。
- **[部分完成]** 尚未覆盖 Web 提交后 Ready/Assign/Start、真实模型执行、审批等待、Workspace 写入和报告交付的完整端到端恢复流程。
- **[部分完成]** 前端测试验证静态页面包含历史任务和输入框，尚未验证真实 API、断线重连或持久化数据。

## 4. P0：必须补齐的端到端能力

### 4.1 Web Server 真实装配与 Runtime 适配

状态：**[已完成]**

`main/server` 已新增真实 Web application 组合根，显式装配：

- SQLite Runtime Store。
- 本地 Artifact Store。
- LLM Client Module。
- Agent Module。
- Capability Module。
- Approval Module。
- Task Module。
- Interaction Module。
- 持久化 Web Gateway Module。

当前已实现：

1. Server 先打开 SQLite 和 Artifact Store，Build/Start/Serve Runtime，确认 Live 后才创建 HTTP Listener。
2. Module 注册、Build、Start、Runtime Serve 或 HTTP Serve 失败都会返回明确错误并清理已创建资源。
3. 正常退出先停止 HTTP、Drain Runtime、取消并等待 Runtime Serve，再关闭 Runtime、Artifact Store 和 SQLite。
4. RuntimeID、PlanRevision、数据目录、非 Secret 模型配置和当前空 Capability/Deny 规则均由显式配置固定。
5. PlanRevision 覆盖组合版本、Agent 行为配置、Provider/Model/BaseURL、Capability/授权规则版本和 Web Gateway 协议版本；APIKey 和机器局部路径不进入 Manifest/hash。
6. Web Handler 只调用 `RuntimePort`；Adapter 只获得 durable ingress、Runtime/Plan 标识和 IDGenerator，不读取 Task State、Journal、Snapshot 或 SQLite。
7. Adapter 发出的 Gateway Command 带稳定 MessageID、CorrelationID、UserID、From/To 和版本化 Payload；结果通过提交后的 Presentation Effect 返回，不依赖同步 Service Reply。
8. HTTP Context 取消只移除进程内 waiter，不撤销已经入箱的 Durable Gateway 请求；迟到 Presentation 会安全完成去重。
9. SQLite 关闭重建后，可通过新的 Runtime/Adapter 查询旧 Task，验证结果不依赖旧进程内缓存。

该项原验收标准已经满足：生产 Web Server 不再默认返回 `runtime_unavailable`，创建的 Task 可跨进程重建查询。

本地 Workspace Capability、工作区根目录和写入权限规则没有伪装进本组合根，继续由 4.5 和 4.6 单独跟踪。

### 4.2 用户提交到 Task 启动的可靠 Saga

状态：**[已完成]**

Web Gateway 已可靠完成”记录请求 → 声明 Virtual Task → `task.create` → `task.mark_ready` → `task.assign` → `task.start` → 持久化 Running 结果 → Presentation”的完整 Saga。实现关键点：

```text
接收 Web CreateTask
  -> 持久化用户 RequestID / TaskID              [已完成]
  -> system.call 声明 Virtual Task 实例          [已完成]
  -> task.create                                 [已完成]
  -> task.mark_ready
  -> task.assign（先使用配置的默认 Agent）
  -> task.start
  -> 保存 ActiveRunID 并返回/投影状态
```

细化逻辑：

1. 客户端必须提供稳定 `request_id` 或 `Idempotency-Key`；重复提交不得创建第二个 Task。
2. 未提供 TaskID 时，由 Gateway 从稳定 RequestID 派生，不能在每次重试中随机生成。
3. `service.instance.declare`、`task.create`、`mark_ready`、`assign`、`start` 每一步都要保存等待状态和稳定 CallID/MessageID。
4. 相同步骤的重复 Reply 要幂等忽略；内容冲突要返回稳定冲突错误。
5. 声明实例成功、`task.create` 尚未投递时崩溃，重启后应从 Gateway State 继续，而不是遗留空 Task 实例。
6. MVP 没有 Orchestrator 时可绑定一个配置中的默认 Agent 地址，但选择结果必须持久化在 TaskState。
7. 创建 API 要明确语义：若返回 `201 created`，前端必须继续触发启动；若产品期望“一次提交自动执行”，则由 Gateway Saga 自动推进，不能只停留在 Created。
8. Agent 不可用、Plan 不包含目标 Agent、任务输入无效等错误要进入明确失败状态，不能无限 Pending。
9. Task 创建者/Owner 和 UserID 必须由可信入口写入，查询和控制操作只能作用于该用户可见的 Task。

验收标准：一次 Web 提交最终能稳定进入 Running，并能在进程重启或重复 HTTP 请求后继续同一个 Task/Run。

### 4.3 真实任务列表与历史执行记录 Projection

状态：**[待实现-P0]**

现有 Task Query 只能返回一个 Task 的当前状态；Agent Query 只能按 RunID 返回一个 Run；HTTP 层没有列表或时间线接口。需要增加独立的用户可读 Projection，不能把全部历史复制进 TaskState，也不能让 HTTP 直接扫 Journal 表。

建议的三个只读模型：

### TaskSummary

- TaskID、GoalID、Title、UserID。
- 当前 Phase、WaitKind、AssignedTo、ActiveRunID。
- Attempt、FailureCount。
- CreatedAt、UpdatedAt、CompletedAt。
- 最后一次可公开错误摘要。
- 最后一次输出/报告引用。

### RunSummary

- RunID、TaskID、Attempt、Agent 地址。
- Run Phase、开始时间、结束时间、耗时。
- 模型 Turn 数、Capability 调用数、失败数。
- 输出 ArtifactRef、错误码和安全错误摘要。

### TimelineItem

- 稳定 EventID、TaskID、RunID、Sequence/全局 Offset。
- `task / agent / model / capability / approval / effect` 分类。
- 可公开的状态、摘要、OccurredAt。
- CallID、ApprovalID、EffectID 等关联引用。
- 可选 DetailsRef；不得内嵌模型大响应、完整文件内容或 Secret。

细化逻辑：

1. Projection 只消费已提交成功的公开业务事件，不能把 Runtime 内部 Journal Event 原样暴露给 UI。
2. TaskService 需为 Created、Ready、Assigned、Started、Waiting、Resumed、Suspended、Retry、CancelRequested 和终态提供完整公开状态事件。目前 `task.status.changed` 只覆盖部分等待/恢复路径。
3. Agent 需发布用户可读的 Run/Turn 进度事件；当前内部 `agent.run.*` StoredEvent 不能直接当跨服务公开协议。
4. Timeline 事件必须带持久化时间；当前部分 Agent Turn 记录只有顺序，没有每一步发生时间，需要补充确定性时间事实。
5. 列表按 `UpdatedAt desc + TaskID` 稳定排序，使用游标分页，不能使用会随新增数据漂移的纯 offset 分页。
6. 查询默认按可信 UserID 隔离，TaskID 存在但不属于用户时应返回统一的 NotFound，避免信息泄露。
7. Projection 的重复事件按 EventID 去重；乱序时按 Stream Sequence/Offset 等待或拒绝推进。
8. Projection 自身需要可恢复；可使用自己的事件流/Checkpoint，重启后从最后 Offset 继续。
9. 历史投影损坏时应支持从公开事件重建，而不是修改 Task/Agent 权威状态。
10. 明确定义“历史执行记录”粒度：至少包含 Task 状态迁移、每次 Run、模型回合、Capability 调用、审批等待和文件操作结果。

验收标准：前端刷新或 Server 重启后，历史任务列表、Run 列表和时间线与运行前一致，且不存在静态演示数据。

### 4.4 Agent 进度事件与实时推送

状态：**[待实现-P0]**

当前 Agent 只向 Task 返回终态 `agent.completed`；Task 在 Agent 执行期间持续显示 Running，无法展示模型等待、Capability 等待或审批等待。

需要补充的公开进度至少包括：

- Run started。
- Capability catalog resolved。
- Prompt prepared。
- Model requested / completed / rejected。
- Capability requested。
- Capability waiting approval / executing / completed / failed。
- Task waiting / resumed。
- Final output preparing。
- Run completed / failed / cancelled。

细化逻辑：

1. Agent 进入 `PhaseWaitingModel`、`PhaseWaitingCapability` 等阶段时，向 Task 或 Projection 发送定向、持久化的公开事件。
2. Capability 进入 WaitingApproval 时，要能投影为“等待用户确认”，而不是泛化为 Running。
3. Task 只保存任务级 WaitKind 和引用，不复制 Agent Turn、Capability 参数或 ApprovalState。
4. Web 实时流推荐采用“先读当前快照，再从 Cursor 订阅增量”的模式。
5. SSE/WebSocket 消息带稳定 EventID；浏览器重连时提交 Last-Event-ID/Cursor，Server 先补发缺失事件。
6. 连接断开不能影响 Runtime 推进；实时连接只是投影传输资源，不是任务恢复依据。
7. 慢客户端应有缓冲上限；超过上限后断开并要求按 Cursor 重连，不能阻塞 Runtime Outbox。
8. UI 收到重复事件必须按 EventID 去重，收到旧版本状态不能覆盖新状态。

验收标准：Agent 等待模型、工具或审批时，页面显示真实阶段；浏览器刷新后能恢复到当前状态并补齐漏掉的时间线。

### 4.5 本地 Workspace Capability

状态：**[待实现-P0]**

Capability/Approval 框架已存在，但还没有本地文件 Provider 和 Executor。要完成“本地文件编辑、写报告”，至少需要以下版本化能力：

| 能力 | MVP 状态 | 用途 |
| --- | --- | --- |
| `workspace.list_files@v1` | **[待实现-P0]** | 枚举允许范围内的文件。 |
| `workspace.search_text@v1` | **[待实现-P0]** | 搜索文件内容并返回结构化命中。 |
| `workspace.read_file@v1` | **[待实现-P0]** | 分段读取文本并返回 checksum。 |
| `workspace.apply_patch@v1` | **[待实现-P0]** | 以精确补丁修改一个或多个文件。 |
| `workspace.write_file@v1` | **[待实现-P0]** | 新建或整体写入报告文件。 |
| `workspace.diff@v1` | **[待实现-P1]** | 返回本次任务相关变更摘要或 Artifact。 |
| `workspace.run_tests@v1` | **[待实现-P1]** | 在受限配置下执行验证命令。 |

#### 4.5.1 Provider 与 Agent 可见能力

1. 每个能力提供明确 InputSchema、OutputSchema、RiskTags、DescriptorRevision 和 ExecutorRef。
2. AgentSpec 中的 CapabilityPrompt 必须与 Catalog 中真实 Descriptor 对齐；不能只在 Prompt 中写一个不存在的工具名。
3. Capability list 是每个 Run 的冻结视图；运行中升级 Descriptor 不得悄悄改变旧 Run 行为。
4. Provider 的 `Plan` 只形成 Effect，不读取或修改文件。
5. 文件系统访问只发生在已持久化 Effect 的 Executor 中。
6. 读操作可以默认 Allow；写入、覆盖、删除、移动和命令执行分别配置 Ask/Deny 策略。

#### 4.5.2 路径安全小逻辑

1. 工作区根目录在进程启动时解析为固定绝对路径，并进入 Plan/模块版本配置。
2. 外部参数只接受相对路径；拒绝绝对路径、盘符、UNC、`\\?\` 前缀、NUL、空路径和路径穿越。
3. 使用 `filepath.Rel` 等路径语义判断是否在根目录内，不能用字符串前缀判断。
4. Windows 下需要处理大小写、保留设备名、NTFS Alternate Data Stream 和 Junction/Reparse Point。
5. 已存在目标必须解析符号链接/重解析点后的真实路径；新文件必须先校验最近已存在父目录的真实路径。
6. 默认拒绝访问 `.git`、Runtime 数据目录、凭据目录和配置的敏感路径；例外必须显式配置。
7. 目录遍历、搜索和读取要限制文件数、单文件大小、总字节数、深度和执行时间。
8. 二进制文件默认不作为文本读取；返回稳定错误或只返回元数据。

#### 4.5.3 读取与搜索小逻辑

1. `read_file` 返回规范化路径、内容/ArtifactRef、size、checksum、编码和行范围。
2. 大文件按行或字节分段读取，结果过大时写入 Artifact，不塞入消息 Payload。
3. `search_text` 返回文件、行号、列、受限上下文和是否截断。
4. 搜索忽略规则要明确，可复用 `.gitignore` 语义，但不能因此放宽敏感目录限制。
5. 读取失败需区分 NotFound、PermissionDenied、Binary、TooLarge、ChangedDuringRead 和 Timeout。
6. 同一个只读 Effect 使用稳定 EffectID 可以安全重试。

#### 4.5.4 补丁与写报告小逻辑

1. 修改前必须携带上一次读取得到的 `expected_checksum`；未读取或文件已变化时拒绝覆盖。
2. 新建文件必须声明 `create_only`；覆盖已有文件必须声明 `replace_existing` 并经过对应授权。
3. Patch 应使用精确上下文；默认不做静默模糊匹配，匹配零次或多次都返回冲突。
4. 重复应用同一个 Patch 时，如果目标已经等于预期 after checksum，应幂等返回成功。
5. 写入使用同目录 staging 临时文件、flush/close 后原子替换，避免留下半文件。
6. 尽量保留原文件编码、BOM、换行风格和权限；无法保留时在结果中明确说明。
7. 写入结果至少包含 changed、path、before checksum、after checksum、bytes 和 diff/report ArtifactRef。
8. 多文件 Patch 不能声称具备不存在的跨文件原子事务。MVP 可按文件拆成独立 Effect，并在 Agent/Task 历史中明确部分成功；后续再引入 PatchSet Saga。
9. 报告内容较大时不能长期内嵌在 Capability 参数中。需要先把确定内容物化为 Artifact，再让 `workspace.write_file` 使用 `content_ref` 写入。
10. 当前 Agent 只支持 inline Capability arguments；需补充“大参数物化为 ArtifactRef”的 Agent 协议，否则长报告仍会受 inline 上限阻塞。
11. 报告默认使用 UTF-8，并由用户任务或安全配置决定允许的输出目录和文件名。
12. 成功写入后，Agent 最终回答必须引用实际 Effect 结果，不能仅根据模型意图声称文件已创建。

#### 4.5.5 写文件幂等与崩溃恢复

1. IdempotencyKey 至少绑定 CallID、规范化路径、before checksum 和 after checksum。
2. Executor 开始前记录 before checksum，并使用确定性 staging 名称或可重建映射。
3. 若崩溃后目标 checksum 等于 after checksum，Reconciler 判定成功并发送同一个结果 MessageID。
4. 若目标仍等于 before checksum/仍不存在，Reconciler 可使用同一个 Effect 身份安全重试。
5. 若目标既不是 before 也不是 after，进入 `ReconciliationRequired`，停止自动覆盖并请求用户处理。
6. 不可盲目重复删除、移动或多文件写操作；这些能力未设计恢复协议前应保持 Deny。
7. Effect 成功但结果消息未确认时，Reconciler 必须重发相同 `capability.execution.completed`，不能重新写文件。

验收标准：Agent 能先读取文件、基于 checksum 写入 Markdown 报告或应用补丁；重复投递和进程崩溃不会重复破坏文件，也不会覆盖并发修改。

### 4.6 Web 审批闭环

状态：**[待实现-P0]**

当前 Approval Service 已有领域状态机，Web Server 只有审批请求下行 WebSocket，没有 approve/deny 上行协议。

需要补齐：

1. `GET /v1/approvals?status=pending`：浏览器首次进入或重连后读取当前用户待处理审批。
2. `POST /v1/approvals/{approval_id}/resolve`：提交 approve/deny、reason_code 和稳定 RequestID。
3. HTTP 入口将可信 Actor 转换为 `approval.resolve` Command；`DecidedBy` 不能由请求体自报。
4. 重复相同决议返回相同终态；冲突决议返回 `409 approval_conflict`。
5. 已过期、已取消或不属于当前用户的审批不能被批准。
6. 审批卡至少展示 Capability、目标路径、操作类型、覆盖/新建、参数摘要和风险说明，不展示 Secret 或完整敏感内容。
7. 浏览器断线期间产生的审批通过 pending list + cursor 补齐，不能依赖一次性 WebSocket 推送。
8. Approval resolved 只表示授权，UI 必须继续等待 Capability 执行结果，不能立即显示“文件已写入”。
9. MVP 权限建议：工作区只读 Allow；创建/修改文件 Ask；删除/移动/任意 Shell Deny，后续再配置放宽。

验收标准：写文件操作暂停在 WaitingUser；用户批准后继续执行，拒绝后 Task/Run 以清晰的可恢复业务结果结束。

### 4.7 结果 Artifact、文件变更与报告展示

状态：**[待实现-P0]**

Task 和 Agent 已保存最终 ArtifactRef，但 Web 没有读取和展示通道。

细化逻辑：

1. 增加面向 Task/Run 的结果接口，不建议提供可任意读取 Artifact Key 的通用裸接口。
2. Server 根据当前用户是否可访问 Task，再通过 Artifact Reader 流式返回结果。
3. 校验 Artifact Store、Key、Size 和 Checksum，损坏时返回稳定错误并记录 Runtime 事件。
4. 正确设置 Content-Type、Content-Length、`nosniff` 和安全下载文件名。
5. 文本报告可在页面预览；Markdown 渲染必须过滤 HTML，避免 XSS。
6. 大结果使用流式下载，不把全部内容读入 Server 内存。
7. 时间线中的文件操作显示路径、before/after checksum、变更状态和 diff Artifact，而不是展示完整文件内容。
8. Task Completed 的定义应要求真实 Output Artifact；若任务要求写报告，还应能关联成功的 workspace 写入结果。
9. 最终 UI 同时展示：Agent 总结、已修改/创建文件、验证结果、警告和可下载报告。

验收标准：任务完成后，用户可在 Web 中查看最终回答，确认实际写入的文件，并下载或预览报告。

### 4.8 前端从静态演示切换到真实数据

状态：**[待实现-P0]**

细化逻辑：

1. 删除生产路径中的硬编码 `taskGroups`、固定对话和固定进度卡；演示数据只能存在于 Story/Test Fixture。
2. 页面加载时请求任务列表，处理 Loading、Empty、Error、Retry 和分页加载状态。
3. 选择 Task 后并行读取 Task 当前状态、Run 列表和首屏 Timeline；切换任务时取消旧请求，防止迟到响应覆盖新页面。
4. 新建任务使用稳定 RequestID 调用真实 API；成功后把返回 Task 插入列表并订阅其事件。
5. 输入框在提交中防重复点击；网络超时后使用同一 Idempotency-Key 重试。
6. 状态标签必须由后端 Phase 驱动，不能对所有历史任务固定显示“进行中”。
7. Timeline 按服务端 Sequence 排序；本地 optimistic item 与服务端 EventID 对齐后去重。
8. 实时连接中断时显示“正在重连”，按 Cursor 补齐后再恢复 Live 标记。
9. 对 Cancel、Retry、Approve、Deny 等按钮执行阶段前置校验，并处理 409 冲突后的状态刷新。
10. 输出和错误均使用后端安全摘要；不直接渲染未经处理的模型 HTML。
11. 长列表和长 Timeline 要分页或虚拟化，不能一次加载整个 Journal。
12. 无障碍状态使用 aria-live；进度不能只依赖颜色表达。

验收标准：浏览器刷新后看到的是 SQLite/Projection 中的真实任务；新任务和进度来自 Runtime，不出现本地固定 Agent 回复。

### 4.9 端到端恢复与一致性测试

状态：**[部分完成]**

当前已完成基础子集：真实 HTTP Handler → Web Gateway → Virtual Task → Presentation 的 SQLite 创建/查询闭环、关闭重建后查询、相同 TaskID 幂等、内容冲突、跨用户安全 NotFound，以及 Runtime 未 Live/fatal/Adapter Close 的有界失败。

完整产品闭环仍需增加以下黑盒/集成场景：

1. Web 创建任务 -> Task Ready/Assigned/Started -> Agent -> Model -> 完成 -> Web 查看结果。
2. Agent 读取文件 -> 用户批准写入 -> 写报告 -> Task Completed。
3. 重复相同 HTTP Idempotency-Key，只产生一个 Task、一个 Run 和一次文件写 Effect。
4. Server 在 Task 创建 Saga 每个步骤后重启，恢复后继续同一个 Task。
5. Server 在模型 Effect、Capability Effect、Approval Pending、Outbox Pending 时重启。
6. 写文件完成但 Effect 未落库时重启，Reconciler 根据 checksum 判定完成，不重复破坏文件。
7. 写文件期间外部修改目标，Reconciler 停止并进入冲突状态。
8. 浏览器断线后任务继续运行；重连按 Cursor 补齐历史。
9. Pending Approval 重启后仍可查询和决议。
10. Task 失败后 Retry 产生新 Attempt/Run，旧 Run 保留在历史中。
11. Cancel、迟到 Agent Reply 和重复 Capability 结果不改变已确定终态。
12. 用户 A 无法列表、读取、订阅或审批用户 B 的 Task。
13. 路径穿越、符号链接/Junction 逃逸、绝对路径和敏感目录访问全部被拒绝。
14. 长报告走 ArtifactRef，不触发 Message/Event/Effect inline Payload 上限。

## 5. P1：闭环可运行后需要补齐

### 5.1 Task 与“对话”语义统一

状态：**[待实现-P1]**

当前前端把一个 Task 展示为可继续发送消息的对话，而 TaskService 当前表示一个固定输入、可多次 Attempt 的任务。需要明确选择：

- MVP 方案：一个提交对应一个 Task；历史页面只查看，不在已完成 Task 中追加消息。
- 对话方案：增加 Conversation/Session 所有者，每条用户输入创建新的 Task/Run，并显式关联上下文。
- 不建议直接修改已完成 Task 的原始 Input，也不建议把全部聊天历史塞入 TaskState。

在协议确定前，前端对历史 Task 的输入框应禁用或明确表示“基于该任务创建后续任务”。

### 5.2 Cancel、Retry、Suspend 和 Resume 的 Web 控制面

状态：**[待实现-P1]**

- 增加取消、重试、暂停、恢复端点及稳定请求 ID。
- 按当前 Task Phase 控制按钮可用性。
- Running Cancel 只表示取消请求，直到 Agent 返回终态前不能显示 Cancelled。
- Retry 必须创建新 Attempt/Run，保留旧 Run 历史。
- 当前 Agent 没有通用暂停协议，因此 Running 状态不应提供伪暂停按钮。

### 5.3 验证工具与完成条件

状态：**[待实现-P1]**

- 增加受限 `workspace.diff`、`workspace.status`、`workspace.run_tests`。
- 命令必须来自配置允许列表或结构化验证能力，不能默认开放任意 Shell。
- 限制 cwd、环境变量、超时、输出字节和子进程树。
- 命令输出过大时写 Artifact；退出码、耗时和截断状态进入结构化结果。
- Agent 的最终回答应基于真实工具结果列出“已验证/未验证”，不能把未执行测试写成通过。

### 5.4 身份、授权与本地部署边界

状态：**[待实现-P1]**

- 当前 `X-User-ID` 只是协议占位，不是可信认证。
- 仅监听 localhost 的单用户模式可以暂时固定 UserID，但需要拒绝非本机绑定或明确告警。
- 多用户/远程访问前必须接入真实认证、会话、CSRF/Origin 检查和 Task/Artifact ACL。
- Artifact Store 当前没有 ACL、保留期和孤儿 GC；单用户 MVP 可延后，但必须限制任意 Key 读取。

### 5.5 保留期、压缩与清理

状态：**[待实现-P1]**

- Interaction 当前物化状态只保留最近 5 个终态请求，但 Journal 完整保留；Web 历史不能直接依赖这个 5 条投影。
- Web Gateway 当前只保留最近 128 个终态请求；淘汰完整 RequestState 后会失去旧 RequestID 的 fingerprint/conflict 判断，需要在清理前定义持久化 Idempotency Window 或紧凑 tombstone。
- 重复终态 Presentation 已有稳定 PresentationID/IdempotencyKey，但不同输入 MessageID 仍可能派生不同 EffectID；长期幂等验收前应统一效果事实身份。
- Agent State 当前保留全部 Run/Turn，长期运行会持续增长，需要终态压缩或独立历史 Projection。
- Task Instance、Timeline Projection、Artifact、staging 文件和 tombstone 都要有明确保留策略。
- 清理前必须保留 Idempotency Window，不能导致旧 RequestID 被重新执行。
- 清理操作应是可审计、可恢复的后台工作，不应由 HTTP 请求直接删除权威事实。

### 5.6 错误、Dead Letter 与运维可见性

状态：**[待实现-P1]**

- Web 需要区分领域失败、审批拒绝、文件冲突、模型失败、Runtime 暂停和系统故障。
- Runtime Serve 正常取消和 fatal 已有监督；仍需为“Serve 超时且 Runtime.Close 继续等待”的异常路径提供真正有界、context-aware 的停止协议。
- 配置解析应允许合法命令行值覆盖格式错误的环境变量，并在创建 SQLite/Artifact 目录前完成 BaseURL 等纯配置校验。
- 暴露只读健康状态：Runtime Lifecycle、Paused/Failed、Pending/Dead Letter 数量和最后致命错误摘要。
- Timeline 中标记 Retry、Dead Letter、ReconciliationRequired，但不泄露敏感 Payload。
- 为卡住的 Task 提供“为什么未推进”的可读诊断，而不是只显示 Running。

## 6. P2：体验与扩展项

- **[待实现-P2]** 模型流式输出和增量回答展示。
- **[待实现-P2]** 多 Agent、AgentSupervisor、子任务和 Orchestrator 策略。
- **[待实现-P2]** 附件上传、输入 Artifact 和报告模板。
- **[待实现-P2]** Task 搜索、标签、归档、收藏和分享。
- **[待实现-P2]** Diff 可视化、逐文件审批和补丁局部接受。
- **[待实现-P2]** 远程 Transport、远程 Artifact Store 和多进程 Worker。
- **[待实现-P2]** 工作区多根目录、项目配置发现和插件化 Capability 包。

## 7. 建议 API 面

以下是满足当前目标所需的最小 API 形状；最终字段需随 Runtime 消息契约一起版本化。

| API | 当前状态 | 目标用途 |
| --- | --- | --- |
| `POST /v1/tasks` | **[部分完成]** | 已可靠创建真实 `created` Task；自动启动和客户端 Idempotency-Key 仍待补齐。 |
| `GET /v1/tasks` | **[待实现-P0]** | 按用户游标分页读取真实历史任务。 |
| `GET /v1/tasks/{id}` | **[已完成]** | 按当前用户读取真实 Task 当前状态；不存在或跨用户统一返回安全 NotFound。 |
| `GET /v1/tasks/{id}/runs` | **[待实现-P0]** | 读取每次 Attempt/Run。 |
| `GET /v1/tasks/{id}/timeline` | **[待实现-P0]** | 按 Cursor 读取执行历史。 |
| `GET /v1/tasks/{id}/events` | **[待实现-P0]** | SSE/WebSocket 实时增量与断线续传。 |
| `GET /v1/tasks/{id}/result` | **[待实现-P0]** | 授权后流式读取最终 Artifact。 |
| `POST /v1/tasks/{id}/cancel` | **[待实现-P1]** | 请求取消当前 Run。 |
| `POST /v1/tasks/{id}/retry` | **[待实现-P1]** | 从 Failed 创建新 Attempt。 |
| `GET /v1/approvals?status=pending` | **[待实现-P0]** | 重连后恢复待审批列表。 |
| `GET /v1/approvals/ws` | **[部分完成]** | 推送已持久化审批请求，需要 Cursor/重放。 |
| `POST /v1/approvals/{id}/resolve` | **[待实现-P0]** | 可信用户 approve/deny。 |

## 8. 推荐实现顺序

1. **[已完成]** 建立 Web Server Runtime 组合根、持久化 Gateway、Runtime Adapter 和受监督生命周期。
2. **[已完成]** Web Gateway 已完成 `mark_ready/assign/start` 启动 Saga。一次 Web 提交可稳定进入 Running 阶段。
3. **[待实现-P0]** 定义公开 Task/Run/Timeline 协议并实现可恢复 Projection。
4. **[待实现-P0]** 将前端历史任务、详情和输入改为真实 API。
5. **[待实现-P0]** 实现只读 Workspace 能力：list/search/read。
6. **[待实现-P0]** 实现 write_file/apply_patch、权限判断、审批和 Reconciler。
7. **[待实现-P0]** 增加结果 Artifact/报告预览、文件变更摘要和实时进度流。
8. **[部分完成]** 已覆盖 Web 创建/查询、重复 TaskID、用户隔离和 SQLite 重启；继续补审批等待与写文件崩溃点测试。
9. **[待实现-P1]** 再补 Cancel/Retry、受限验证命令、保留期、安全和运维视图。
10. **[待实现-P2]** 最后扩展多轮会话、流式输出和多 Agent。

## 9. MVP 完成判定

只有以下条件全部满足，才建议把“Web Agent 可查看历史任务、完成本地文件编辑并写报告”标记为 **[已完成]**：

- **[待实现-P0]** Web 页面不再使用生产硬编码任务和固定 Agent 回复。
- **[已完成]** Web Server 真实连接 SQLite Runtime 和 Artifact Store。
- **[待实现-P0]** 一次任务提交可可靠创建、分配并启动 Task/Agent Run。
- **[待实现-P0]** 页面可查询真实 Task 列表、每次 Run 和可恢复 Timeline。
- **[待实现-P0]** Agent 至少具备 list/search/read/apply_patch/write_file。
- **[待实现-P0]** 写操作有路径沙箱、expected checksum、稳定幂等键和 Reconciler。
- **[待实现-P0]** 写操作需要审批时，Web 可完整 approve/deny 并继续状态机。
- **[待实现-P0]** Task 完成后可查看最终回答、变更文件和报告 Artifact。
- **[部分完成]** Server 重启和重复 TaskID 已验证不会重复创建 Task；浏览器断线、审批和文件 Effect 重投仍待覆盖。
- **[待实现-P0]** 对上述闭环有自动化端到端测试，且现有 Runtime/Service 测试继续通过。

## 10. 最终判断

目前 Runtime 的可靠提交/恢复能力、Task/Agent/Capability/Approval 的状态所有权、Web Runtime 真实组合根、持久化 Gateway，以及现有 Web 页面布局都值得继续保留。下一阶段不需要重写这些基础，而应集中补齐三条产品链路：

```text
Task Ready/Assign/Start 与可恢复执行历史 Projection
  + 安全可恢复的本地 Workspace Capability
  + 审批、结果与前端真实数据/实时展示闭环
```

完成这三条链路后，项目才从“已接通真实 Runtime、但只创建 Created Task 的 Web 界面”进入“可以真实完成本地文件编辑和报告交付的 Web Agent MVP”。
