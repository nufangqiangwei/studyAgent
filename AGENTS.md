# Agents Project Map

本文件是 agent 学习开发项目的导航地图。具体内容放在 `docs/` 目录，根目录只保留入口、约定和索引。

## 项目定位

本项目用于学习和实现一种命令行 agent 产品形态，目标体验参考 Codex CLI、Gemini CLI、Claude Code 这类开发者工具：

- 通过 CLI 与 agent 交互。
- 能读取项目上下文、规划任务、调用工具、修改代码并运行验证。
- 支持多模型或多供应商接入。
- 以 Go 语言实现，强调模块解耦、接口边界清晰、可测试和可扩展。

## Architecture Philosophy: Programmable Runtime

This project should be designed as a programmable agent runtime, not as a collection of hard-coded agent scripts.

The architecture should follow these principles:

### 1. Core Runtime Should Stay Generic

The core loop should not know the concrete business behavior of any specific agent, tool, model provider, or workflow.

The core runtime is responsible for:

* executing one task lifecycle
* managing session context
* calling the LLM
* dispatching tool calls
* enforcing policy
* recording observable execution events
* returning structured results

The core runtime should not contain special-case logic such as:

* `if agentName == "analyze"`
* `if toolName == "shell"`
* `if task contains "build"`
* hard-coded fallback strategies for specific tasks
* direct dependency on concrete agent implementations

Concrete behavior should be provided through registered modules, configuration, task definitions, policies, and routing decisions.

### 2. Prefer Interface + Registry + Factory

Every replaceable capability should be represented by an interface and registered through a registry/factory mechanism.

Examples:

* Agent implementations
* Tool implementations
* LLM providers
* Context compressors
* Policy checkers
* Task planners
* Result evaluators
* State transition handlers

A new capability should usually be added by:

1. defining or reusing an interface
2. implementing the concrete module
3. registering the module in a registry
4. referencing it by name, tag, or config
5. adding focused tests

Avoid modifying the core runtime whenever a new concrete capability is added.

### 3. Use Tags / Names as Runtime References

Modules should be referenced by stable names or tags instead of direct concrete dependencies.

For example:

```text
agent: "analyze"
tool: "shell"
policy: "default-safe"
provider: "deepseek"
compressor: "summary-v1"
```

Runtime components should use these names to look up concrete implementations from registries.

This keeps the system configurable and composable.

### 4. Separate Execution, Routing, and Policy

Do not mix execution logic, routing decisions, and permission checks in the same component.

Use this separation:

```text
NativeLoop
  executes one task lifecycle

Router / Planner
  decides what should happen next

Policy
  decides whether an action is allowed

Registry
  provides available implementations

Manager
  owns lifecycle of registered modules

Session / Context Store
  stores conversation and execution state
```

A component should have one primary responsibility.

### 5. Design the System as a Runtime Object Graph

Configuration and task definitions should describe how modules are connected.

The runtime should construct an object graph from these definitions.

Example conceptual structure:

```text
Workflow
  -> Agent
      -> NativeLoop
          -> LLM Provider
          -> Tool Registry
          -> Policy
          -> Session Store
          -> Event Recorder
```

Avoid designs where object relationships are hidden inside ad-hoc code paths.

### 6. Add New Behavior by Composition, Not Mutation

When adding a new feature, first ask:

* Can this be a new module?
* Can this be a new policy?
* Can this be a new router rule?
* Can this be a new tool?
* Can this be a new agent?
* Can this be a new lifecycle hook?
* Can this be a new state transition?

Prefer adding a small composable unit instead of expanding a large central function.

### 7. NativeLoop Is the Single-Task Execution Engine

NativeLoop should be treated as a single-task ReAct execution engine.

It should handle:

* preparing the prompt
* calling the LLM
* parsing model output
* executing tool calls
* appending tool results
* checking stop conditions
* enforcing max step limits
* reporting execution status
* returning the final task result

NativeLoop should not be responsible for:

* deciding high-level user intent
* orchestrating multiple agents
* choosing long-term strategy
* managing product-level workflows
* embedding business-specific logic

Higher-level orchestration should be handled outside NativeLoop.

### 8. Workflow / GoalLoop Owns Multi-Agent Coordination

When a task requires multiple agents or multiple stages, use a higher-level workflow component.

The workflow layer may:

* decompose a user goal into tasks
* select which agent should run
* inspect task results
* decide whether to retry, continue, stop, or switch strategy
* create follow-up tasks
* coordinate multiple NativeLoop executions

The workflow layer should not directly execute tool calls. Tool execution belongs to NativeLoop.

### 9. State Machine Should Represent Observable Runtime State

State machines should model real runtime states that can be observed, tested, and logged.

Good state examples:

```text
Created
Preparing
CallingModel
WaitingTool
ExecutingTool
ObservingResult
EvaluatingStop
Completed
Failed
Cancelled
StepLimitReached
NeedsAlternativeStrategy
```

Avoid fake states that are only comments or documentation and cannot be observed from code.

A state transition should usually happen because of a concrete event:

```text
ModelReturnedToolCall
ToolSucceeded
ToolFailed
MaxStepsReached
FinalAnswerProduced
PolicyRejectedAction
AlternativeStrategyRequested
```

### 10. Failure Handling Should Be Strategy-Oriented

Repeated failure should not only consume more ReAct steps.

When repeated attempts fail, the system should expose enough state for the planner or workflow layer to choose another strategy.

Example:

```text
Task: build service
Failure: dependency problem
Repeated fix attempts: 10
NativeLoop state: NeedsAlternativeStrategy
Workflow decision: try Docker build environment
```

The low-level loop should report the failure pattern.
The higher-level workflow should decide the alternative path.

### 11. Prefer Structured Results

Important components should return structured results instead of only plain text.

Examples:

```go
type RunResult struct {
    Status       RunStatus
    FinalAnswer  string
    StepsUsed    int
    Error        error
    Events       []RunEvent
    Summary      string
    NextHint     string
}
```

Structured results make orchestration, retry, testing, logging, and context compression easier.

### 12. Observability Is Part of the Architecture

Every major runtime action should be observable.

Record events for:

* model request started
* model response received
* tool call requested
* tool execution started
* tool execution completed
* policy rejected action
* state changed
* context compressed
* task completed
* task failed

The event system should not be added as an afterthought.
It is required for debugging agent behavior.

### 13. Anti-Patterns to Avoid

Avoid these patterns:

* putting all logic into one large loop function
* hard-coding concrete agents inside NativeLoop
* hard-coding concrete tools inside NativeLoop
* mixing task planning with tool execution
* hiding retry strategy inside prompt text only
* using global mutable state for runtime behavior
* making tools depend on specific LLM providers
* making agents directly manage provider-specific API details
* adding special cases instead of defining interfaces
* returning only strings where structured results are needed
* changing core runtime code for every new feature

### 14. Preferred Development Flow

When implementing a new feature, follow this order:

1. Identify which layer owns the feature.
2. Define the interface or structured type first.
3. Implement the smallest concrete module.
4. Register the module.
5. Connect it through config, tag, or constructor options.
6. Add unit tests for the module.
7. Add integration tests for the runtime path.
8. Add logging or events for observability.
9. Avoid modifying unrelated layers.

### 15. Design Review Checklist

Before accepting a code change, check:

* Is the core runtime still generic?
* Did this introduce hard-coded agent/tool/provider logic?
* Can this feature be replaced or extended through an interface?
* Is the module registered instead of directly wired everywhere?
* Are routing, execution, and policy still separated?
* Does the result expose enough structured state?
* Are important transitions observable?
* Can this be tested without calling a real LLM?
* Does the change keep NativeLoop focused on single-task execution?

The goal is to build a configurable, observable, testable agent runtime that can grow by composition.

### Event-Driven and Resumable Loop

NativeLoop must be designed as an event-driven and resumable execution runtime.

The ReAct loop must not assume that tool execution is always synchronous or that the current process will stay alive while a tool is running.

A tool call may be:

* completed immediately in the same process
* executed asynchronously by another worker
* suspended until user approval
* resumed after an external callback
* resumed after the process restarts
* resumed after persisted context is restored

Therefore, NativeLoop should not be implemented as a blocking loop that waits inside the same call stack for every tool result.

Instead, each important runtime step should be represented by explicit events and persisted state.

Example events:

```text
RunStarted
ModelRequestCreated
ModelResponseReceived
ToolCallRequested
ToolCallDispatched
ToolCallCompleted
ToolCallFailed
UserApprovalRequired
UserApprovalReceived
ContextPersisted
RunResumed
RunCompleted
RunFailed
```

A ReAct execution should be able to stop after dispatching a tool call and continue later when a tool result event is received.

Conceptual flow:

```text
1. NativeLoop receives RunStarted or RunResumed
2. NativeLoop restores RunState and context
3. NativeLoop advances execution until the next suspension point
4. If the model requests a tool call, NativeLoop records ToolCallRequested
5. The tool call is dispatched
6. NativeLoop persists state
7. The process may exit
8. Later, ToolCallCompleted is received
9. NativeLoop restores context
10. NativeLoop appends the tool result
11. NativeLoop continues the ReAct execution
```

NativeLoop should treat tool execution as an external event source, not merely as a blocking function call.

This requires the runtime to separate:

```text
Decision
  what should happen next

Dispatch
  send the tool call to an executor

Persistence
  save the run state and context

Resume
  continue execution after receiving an event
```

The core runtime should support suspension points.

Common suspension points include:

```text
WaitingForToolResult
WaitingForUserApproval
WaitingForExternalCallback
WaitingForScheduledResume
StepLimitReached
NeedsAlternativeStrategy
```

A run state should contain enough information to resume safely:

```go
type RunState struct {
    RunID         string
    TaskID        string
    Status        RunStatus
    CurrentStep   int
    Messages      []Message
    PendingTools  []PendingToolCall
    LastEventID   string
    Summary       string
    CreatedAt     time.Time
    UpdatedAt     time.Time
}
```

Tool calls should have stable IDs.

```go
type PendingToolCall struct {
    ToolCallID string
    ToolName   string
    Arguments  []byte
    Status     ToolCallStatus
    CreatedAt  time.Time
}
```

Tool results should be correlated back to the original tool call by ID.

```go
type ToolResultEvent struct {
    RunID      string
    ToolCallID string
    ToolName   string
    Result     []byte
    Error      string
}
```

The loop should advance by consuming events:

```go
type LoopEvent interface {
    EventID() string
    RunID() string
    EventType() string
}
```

NativeLoop should expose an API closer to:

```go
func (l *NativeLoop) HandleEvent(ctx context.Context, event LoopEvent) (*LoopAdvanceResult, error)
```

instead of only:

```go
func (l *NativeLoop) Run(ctx context.Context, task Task) (*RunResult, error)
```

A synchronous `Run` method may still exist as a convenience wrapper, but it should be implemented on top of the event-driven runtime.

The synchronous wrapper may repeatedly call `HandleEvent` until the run reaches a terminal state or a suspension point.

NativeLoop terminal states include:

```text
Completed
Failed
Cancelled
```

NativeLoop non-terminal suspended states include:

```text
WaitingForToolResult
WaitingForUserApproval
WaitingForExternalCallback
NeedsAlternativeStrategy
```

Important rule:

NativeLoop must not rely on in-memory call stack state to continue execution.

Any state required to continue the task must be represented in RunState, events, session messages, pending tool calls, or persisted context.

This allows ReAct execution to survive process exit, worker migration, delayed tool execution, and external approval flows.


## 文档导航

- [项目概览](docs/project-overview.md)：项目目标、用户场景和非目标。
- [产品能力](docs/product-goals.md)：CLI agent 应具备的核心能力和学习拆解。
- [架构设计](docs/architecture.md)：推荐分层、数据流和依赖方向。
- [模块规划](docs/modules.md)：Go 包和模块职责边界。
- [代码模块实现](docs/code-modules.md)：当前代码骨架、扩展入口和模块协作方式。
- [开发路线](docs/development-roadmap.md)：从最小可用版本到进阶能力的迭代路径。
- [开发约定](docs/development-guide.md)：代码风格、接口设计、测试和解耦原则。

## 推荐阅读顺序

1. 先读 [项目概览](docs/project-overview.md)，明确这个项目要做什么。
2. 再读 [产品能力](docs/product-goals.md)，把目标产品拆成可学习的能力点。
3. 然后读 [架构设计](docs/architecture.md)、[模块规划](docs/modules.md) 和 [代码模块实现](docs/code-modules.md)，确定 Go 代码组织方式。
4. 最后按 [开发路线](docs/development-roadmap.md) 分阶段实现，并遵守 [开发约定](docs/development-guide.md)。

## 本地辅助文档目录约定

- `localDocs/` 和 `plan/` 是用户用于理解项目的本地辅助文档目录。
- 在没有用户明确允许的情况下，agent 禁止读取、列举、搜索、总结、写入、修改或删除这两个目录下的任何内容。
- 如果任务确实需要使用这两个目录中的内容，必须先向用户请求许可；只有当用户明确授权后，才能在授权范围内访问。

## 根目录职责

根目录建议只保留项目入口文件和高层说明：

- `AGENTS.md`：项目导航地图。
- `go.mod`：Go 模块定义。
- `main.go`：当前程序启动入口，负责调用 `internal/app`。
- `docs/`：项目文档。
- `internal/`：当前 Go 模块骨架。

后续代码增长后，建议将业务代码逐步迁移到 `cmd/`、`internal/`、`pkg/` 等目录，避免所有逻辑堆在 `main.go`。
