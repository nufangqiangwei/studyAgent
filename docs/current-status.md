# 项目最新现状

更新时间：2026-07-01

本文是给后续 ChatGPT / agent 读取的最新项目状态快照。它基于 `tmp/` 目录中的四份历史材料，并以当前代码实现为准：

- `tmp/native-loop-agent.md`
- `tmp/前期指导.md`
- `tmp/当前进度汇报.md`
- `tmp/项目架构.md`

这些 `tmp/` 文件记录了项目从同步 NativeLoop、workspace 工具、policy、session/context 调整阶段演进过来的背景。当前代码已经继续向事件驱动、可恢复运行时推进，因此旧文件中的 `internal/app`、`internal/tools`、`internal/workspace`、`internal/agent/loop.go` 等路径不再代表最新实现。

## 一句话结论

项目已经从早期的 `LLM + tool calling CLI`，推进到一个具备事件、状态机、effect outbox、event inbox、文件持久化和可恢复 runner 的 Go CLI agent runtime 骨架。

当前主线不再是“给每个 agent 写同步循环”，而是：

```text
CLI / command
  -> Agent
  -> prompt builder
  -> agent/runner
  -> event inbox
  -> state machine + reducers
  -> effect store
  -> effect worker
  -> LLM runtime / tool registry
  -> runtime events
  -> persisted state
```

## 当前代码结构

当前入口仍然很薄：

```text
main.go
  -> internal/entrance/app.Run
```

当前主要包边界：

| 包 | 当前职责 |
| --- | --- |
| `internal/entrance/app` | 应用装配入口，串联 startup、config、logging、runtime persistence、LLM provider、agent selector、CLI/startupcmd。 |
| `internal/entrance/cli` | 交互式 CLI 输入循环和 slash command 适配。 |
| `internal/entrance/startupcmd` | 非交互命令入口，把启动命令交给 command registry。 |
| `internal/capability/command` | 命令接口、注册表和命令分发。 |
| `internal/capability/builtin/command` | 内置命令：`run`、`help`、`status`、`model`、`agent`、`set-agent`。 |
| `internal/agent` | agent 接口、agent 工厂注册表、具体 agent，以及把 agent spec 装配到 runner 的适配层。 |
| `internal/agent/runner` | 当前 ReAct 执行核心：事件驱动 runner、effect worker、ReAct reducer、同步 `Run` 便利封装、恢复入口。 |
| `internal/state` | 可恢复运行状态、状态机、reducer registry、effect store、event inbox、内存和文件存储。 |
| `internal/event` | 运行事件定义、事件 registry、dispatcher、hook 和事件 envelope。 |
| `internal/llm` | LLM 调用运行时、请求构建、tool call 归一化、上下文窗口查询和压缩接口。 |
| `internal/foundation/llmClient` | provider 无关请求/响应类型，以及 OpenAI、DeepSeek、Gemini、Anthropic、mock 适配。 |
| `internal/capability/tool` | LLM tool 接口、默认工具注册表、policy 接入、异步审批错误。 |
| `internal/capability/builtin/workspace` | workspace 只读工具和写入工具：`list_files`、`read_file`、`search_text`、`get_workspace_summary`、`apply_patch`、`write_file`。 |
| `internal/foundation/workspace` | workspace 文件系统能力：安全路径解析、读取、搜索、列表、snapshot 和忽略规则。 |
| `internal/foundation/policy` | read / validate / modify 三档工具权限策略。 |
| `internal/runtime/persistence` | runtime task state、agent snapshot、runtime snapshot 和 event store 的本地持久化。 |
| `internal/content` | 单次执行的 Env、IO、配置快照、小接口和 context 绑定。 |
| `internal/prompt` | prompt builder 和内置 prompt。 |

## 从 tmp 材料到当前实现的演进

`tmp/前期指导.md` 建议的 `workspace -> policy -> verify tools -> patch tools -> goal loop` 路线中，当前已经完成并迁移到新包边界的是：

- workspace 文件访问能力：现在位于 `internal/foundation/workspace`。
- 只读 workspace tools：现在位于 `internal/capability/builtin/workspace`。
- policy 三档权限：现在位于 `internal/foundation/policy`。
- 写入工具 `apply_patch` / `write_file`：已经实现，默认 dry-run，并接入 policy。
- provider 多模型接入：现在位于 `internal/foundation/llmClient`。
- session 文件存储：已移除；恢复事实来源统一到 `internal/runtime/persistence`。

旧文档里描述的同步 `NativeLoop` 已不再是当前代码的实际文件结构。现在的替代实现是：

```text
internal/agent/runtime_agent.go
  -> internal/agent/runner.AgentRunner
  -> internal/state.Machine
  -> internal/agent/runner.ReActReducer
  -> internal/agent/runner.RuntimeEffectWorker
```

因此后续文档和实现讨论中，除非指代架构概念，否则应优先使用 “runner / state machine / effect worker” 来描述当前执行核心。

## 当前运行链路

一次 `go run . run "..."` 的关键路径是：

```text
main.go
  -> entrance/app.Run
  -> startup.Parse
  -> config.LoadOptional
  -> logging.New
  -> runtime persistence root
  -> llmClient/provider.New
  -> llm.ResolveAndCacheContextWindowTokens
  -> agent selector
  -> content.Env
  -> startupcmd.Run
  -> command.Registry.Execute("run")
  -> active Agent.Run
  -> prompt.NativeBuilder.Build
  -> runner.NewAgentRunner
  -> runner.Run
```

`runner.Run` 是同步便利封装，但它内部通过事件和 effect 推进：

```text
Start
  -> enqueue RunStarted
  -> Advance consumes event
  -> state reducer creates effect
  -> DispatchNextEffect claims effect
  -> effect worker emits next event
  -> next Advance consumes event
  -> terminal state or suspension
```

这个设计已经满足“不要依赖同步调用栈保存执行状态”的方向。`RunState`、runtime events、pending effects、pending inbox events 都可通过 store 保存。

## 已经具备的能力

### 1. 事件驱动 runner

`internal/agent/runner` 已支持：

- `Start`：创建 run state 并把 `RunStarted` 放入 event inbox，不立即执行模型或工具。
- `HandleEvent`：把外部事件写入 inbox。
- `Advance` / `ProcessNextEvent`：消费 inbox 事件并交给 dispatcher/state machine。
- `DispatchNextEffect`：claim effect 并交给 effect worker 执行。
- `Run`：同步封装，持续推进直到完成、失败或无法继续。
- `Recover`：发现非终态 run，写入幂等 `RunResumed` 事件。
- `Result`：返回结构化 `RunResult`，包含状态、最终回答、事件列表和错误状态。

### 2. 状态机和 reducer

`internal/state` 已提供：

- `RunState`：`RunID`、`Phase`、`Step`、`MaxSteps`、`LastEventID`、`Waiting`、`Error`、`Extensions`。
- 运行状态：`idle`、`running`、`waiting`、`completed`、`failed`、`cancelled`。
- `CoreRunReducer`：处理 run start/resume/complete/fail/cancel、wait、step limit。
- `ReducerRegistry`：多个 reducer 可组合处理事件。
- `Machine`：append event、reduce state、append effects、save state。

`internal/agent/runner.ReActReducer` 负责 ReAct 语义：

- 模型请求创建后进入等待模型结果。
- 模型响应无 tool call 时生成完成 effect。
- 模型响应有 tool call 时写入 pending tool，并生成 tool dispatch effect。
- tool result / user input / user approval 回来后追加 tool message，并触发下一次模型调用。

### 3. effect / event 持久化

`internal/state` 已有内存和文件实现：

- `StateStore`
- `EventStore`
- `EffectStore`
- `EventInboxStore`

文件存储使用 JSONL，并带有：

- 路径锁和 stale lock 清理。
- effect / inbox claim lease。
- lease renew。
- expired lease 后可由其他 worker 重新 claim。
- deterministic effect ID，避免事件重放时重复产生 effect。
- 截断尾部 JSONL 的容错读取。

### 4. 工具、权限和用户等待点

默认工具集当前包含：

- `ask_user`
- `list_files`
- `read_file`
- `search_text`
- `get_workspace_summary`
- `apply_patch`
- `write_file`

工具执行统一走 `internal/capability/tool.Manage`，先生成 policy request，再交给 `internal/foundation/policy` 判断。当前三档模式是：

- `read`
- `validate`
- `modify`

agent 创建工具注册表时使用 `WithAsyncPolicyApproval()`。当 policy 返回 `ask` 时，工具注册表返回 `ApprovalRequiredError`，runner 将其转换成 `UserApprovalRequired` 事件和 `waiting user_approval` 状态，而不是在工具内部同步阻塞。

`ask_user` 也被 runner 特殊处理成 `UserInputRequested`，可暂停等待 `UserInputReceived`。

### 5. LLM provider 和模型调用运行时

`internal/foundation/llmClient/provider` 根据 model 名称推断 provider：

- `mock*` -> mock
- `gpt-*` / `o*` / `chatgpt-*` -> OpenAI
- `deepseek*` -> DeepSeek
- `deepseek-v4*` -> DeepSeek Anthropic-compatible
- `gemini*` -> Gemini
- `claude*` -> Anthropic

`internal/llm.Runtime` 负责：

- 构造统一 `llmClient.Request`。
- 复制 messages/tools/metadata，避免共享切片被修改。
- 调用底层 provider client。
- 标准化 tool call ID 和空参数。
- 从 response 生成 assistant message。
- 生成 tool result message。

启动时 app 会调用 `ResolveAndCacheContextWindowTokens` 预取模型上下文窗口；Gemini 和 OpenRouter 支持 metadata 查询，其他 provider 或查询失败时走 fallback。

### 6. workspace 和写入工具

`internal/foundation/workspace` 已实现：

- workspace root 规范化。
- 禁止绝对路径、盘符路径、NUL、`..` 逃逸。
- symlink existing prefix 检查。
- `.gitignore` 和默认忽略规则。
- list / read / search / snapshot。
- 文件大小、搜索数量、上下文行数限制。

`apply_patch` / `write_file` 已实现保守约束：

- 默认 `dry_run=true`。
- 非 dry-run 需要通过 policy。
- 禁止二进制内容。
- 限制 patch 和目标文件大小。
- 禁止修改 `.git/`。
- 高风险路径需要 `confirm_high_risk=true`。
- 删除 patch 需要 `allow_delete=true`。

## 当前未完成或需要澄清的边界

### 1. GoalLoop / Verifier / Planner 尚未实现

当前项目已经有单任务 ReAct runner，但还没有高层目标循环：

- 没有 `GoalLoop`。
- 没有 `Verifier`。
- 没有 `Planner`。
- 没有任务级 stop condition。
- 没有 maker/checker 分离。

所以项目还不能稳定表达“修复直到测试通过”这类 goal-oriented coding loop。

### 2. 验证工具尚未落地

policy 中已经预留了 `run_command`、`run_tests`、`git_status`、`git_diff` 的规则，但当前默认工具集中还没有这些工具实现。

`get_workspace_summary` 内部会读取 git status 摘要，但它不等价于可供 agent 主动调用的 `git_status` / `git_diff` 工具。

下一步如果要做可验证开发闭环，应优先补：

- `internal/execx`
- `run_tests`
- `git_status`
- `git_diff`
- 受限 `run_command`

### 3. context compression 还没有接入当前 runner 主链路

`internal/llm/compression.go` 已有：

- `SessionCompressor`
- `LLMSessionCompressor`
- `ContextTokenCounter`
- context window token lookup 和 fallback
- compression prompt

但当前 `agent/runner` 主链路没有调用 compressor，也没有在 runner 中写入 `ContextCompressed` / `ContextPersisted` 事件。启动阶段只做 context window metadata 预取。

因此旧进度汇报中“context compression 已接入 loop”的结论不适用于当前最新 runner 实现。

### 4. session 与 runtime state 的关系需要继续收口

当前有两套持久化：

- `internal/runtime/persistence`：当前 runner 的恢复事实来源，保存 task states、agent snapshots、runtime snapshots 和 events。
- `internal/entrance/app` 的 async queue：保存待处理 events/effects 队列。

当前主 runner 的模型事件、工具事件进入 `internal/runtime/persistence` 的 event store；旧的 session event helper 路径已经移除。后续需要明确：

- runtime store 是否成为唯一恢复事实来源。
- 是否需要在 runtime event store 之外再生成用户可读审计视图。

建议保持 runtime store 作为恢复事实来源；如果需要用户可读审计视图，应从 runtime events 投影生成。

### 5. prompt/context builder 已被简化为当前任务 prompt

当前 `prompt.NativeBuilder` 每次根据 task 和 workdir 生成 system + user messages。runner 的消息历史存在 run data extension 中，但还没有完整的：

- project instructions 注入。
- pinned context。
- recent context 裁剪。
- working summary。
- tool evidence summary。
- 从历史 session/context snapshot 恢复上下文。

这意味着当前可以执行单次 ReAct run，但还不是长期上下文管理完善的 agent runtime。

### 6. Project skills / memory 尚未开始

当前尚未实现：

- `.agent/instructions.md`
- `.agent/commands.md`
- `.agent/permissions.md`
- `.agent/memory.md`
- skill registry
- project memory 注入 prompt

`AGENTS.md` 仍是外部 agent 读取项目约定的入口，不是本项目 runtime 内部自动读取的能力。

## 当前验证状态

本次整理时执行：

```powershell
go test ./... -count=1
```

结果：全部通过。

测试覆盖到的关键路径包括：

- agent runner 同步 `Run`。
- event inbox 先入队、后处理。
- model request / model response 事件链。
- tool dispatch 和 tool execution 分离。
- `ask_user` 暂停和 `UserInputReceived` 恢复。
- policy approval 暂停和批准后继续执行。
- recover 非终态 run。
- effect / event inbox lease 和过期重 claim。
- 文件 store 跨 reopen 持久化。
- workspace / policy / provider / CLI / app 等现有包。

当前工作区在整理前已有未提交代码改动，主要集中在 runner、state、event、tool registry、llm loop 等运行时相关文件。本文档只描述现状，不代表这些代码已经提交。

## 推荐下一步

建议后续按这个顺序推进：

1. 把旧文档中仍指向 `internal/app`、`internal/tools`、`internal/workspace`、同步 `NativeLoop` 的内容逐步更新为当前包结构。
2. 明确 `agent/runner` 是否就是当前 NativeLoop 概念的正式实现，并在文档中统一命名。
3. 补齐验证工具：`internal/execx`、`run_tests`、`git_status`、`git_diff`、受限 `run_command`。
4. 明确是否需要从 runtime event store 投影用户可读审计记录。
5. 将 context budget / compression 接入 event-driven runner，或者先把旧文档中“已接入”的说法降级为“接口已存在”。
6. 在验证工具稳定后，再实现 `GoalLoop`、`Verifier`、`Planner` 和 stop condition。
7. 最后再做 project skills / memory、automation、sub-agent、MCP 等更高层能力。

## 后续读取原则

当旧文档和本文冲突时，以本文和当前代码为准。旧文档保留为设计演进记录，不应直接作为当前实现状态引用。
