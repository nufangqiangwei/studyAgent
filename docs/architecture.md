# 架构设计

项目采用分层架构。外层负责输入输出和具体基础设施，内层负责 agent 核心流程。依赖方向应尽量从外到内，核心模块不直接依赖 CLI、文件系统实现或某个模型 SDK。

## 推荐分层

```text
main.go
  当前程序入口，将进程输入输出交给 app

internal/app
  应用编排，连接 CLI、agent core、配置、日志和运行时上下文

internal/content
  运行时上下文类型和跨模块小接口，承载 IO、agent runner、命令注册表、logger 和配置快照

internal/startup
  启动参数解析，输出命令名和全局配置

internal/startupcmd
  启动命令调用适配，把启动解析结果交给 command registry

internal/cli
  交互式输入输出适配，把用户输入转换为 command registry 调用

internal/command
  命令接口、命令实现、命令注册和命令执行分发，对外暴露 Command 抽象

internal/agent
  agent 接口、agent 工厂注册表和核心循环，决定何时调用模型、工具和验证

internal/prompt
  prompt engineering，在每次 LLM 请求前构造输入消息

internal/llm
  模型供应商抽象和实现

internal/logging
  日志级别和日志输出

internal/tools
  工具接口、注册表和具体工具，对外暴露 Tool 抽象

internal/session
  会话、消息、上下文窗口和历史记录

internal/workspace
  项目文件读取、搜索、路径处理和工作区元信息

internal/config
  配置文件加载、环境变量读取和默认值合并
```

## 依赖方向

推荐依赖关系：

```text
main.go -> internal/app
internal/app -> internal/content
internal/app -> internal/startup
internal/app -> internal/startupcmd
internal/app -> internal/cli
internal/app -> internal/command
internal/app -> internal/agent
internal/app -> internal/prompt
internal/app -> internal/llm
internal/app -> internal/tools
internal/app -> internal/config
internal/command -> internal/content
internal/agent -> internal/session
internal/agent -> internal/tools interfaces
internal/agent -> internal/llm interfaces
internal/tools -> internal/content
internal/tools -> internal/workspace
internal/startupcmd -> internal/startup
internal/startupcmd -> internal/command
internal/startupcmd -> internal/content
internal/cli -> internal/command
internal/cli -> internal/content
```

核心原则：

- `internal/agent` 不直接读取终端输入。
- `internal/agent` 不直接读取环境变量。
- `internal/agent` 不绑定具体模型 SDK。
- `internal/agent` 不直接执行 shell 命令，而是通过工具接口。
- 运行时配置使用 `content.Env` 绑定到 `context.Context`，避免多个 agent、命令或测试并发运行时互相覆盖状态。
- 包级注册表只保存稳定的默认能力；单次执行需要使用 app 装配出的 registry 和 env，不把请求态数据放进全局变量。
- `main.go` 只做启动和依赖装配，不放业务逻辑。

## 核心数据流

```text
User Input
  -> main.go
  -> App
  -> Runtime Env (content.Env + context.Context)
  -> Startup Parser
  -> Startup Command Adapter or CLI Adapter
  -> Command Registry
  -> Agent Core
  -> Prompt Builder
  -> LLM Client
  -> Tool Registry
  -> Workspace / Shell / File Tools
  -> Agent Core
  -> CLI Output
```

## 运行时上下文

`internal/content` 定义当前执行所需的轻量接口和运行时数据：

- `Env`：包含 IO、`AgentRunner`、`CommandRegistry`、`Logger`、配置快照和运行模式。
- `WithEnv`：把一次执行的 `Env` 绑定到 `context.Context`。
- `EnvFromContext`：工具或底层动作在需要 IO、配置或 logger 时从 context 读取当前执行环境。

这种方式让父子 context 可以持有不同 `Env`。例如并发运行多个 agent 或测试多个命令时，每个执行流都使用自己的输入输出、模型配置和 active agent，不依赖进程级共享变量。

## 关键接口方向

模型接口示例：

```go
type Client interface {
    ModelName() string
    Complete(ctx context.Context, req Request) (Response, error)
}
```

工具接口示例：

```go
type Tool interface {
    Name() string
    Description() string
    InputSchema() json.RawMessage
    Execute(ctx context.Context, input json.RawMessage) (Result, error)
}
```

当前核心包的对外抽象方向：

- `internal/agent` 对外暴露 `Agent` 和 `NewAgent`，包内维护 `Catalog` 注册默认 agent。
- `internal/command` 对外暴露 `Command`，包内维护 `Registry`，默认命令注册到 `Manage`。
- `internal/tools` 对外暴露 `Tool`，包内维护工具 `Registry` 和默认工具集合。
- `internal/content` 提供 `AgentRunner`、`AgentSelector`、`CommandRegistry` 等小接口，避免命令层绑定 agent 具体实现。

接口应由使用方定义，具体实现由 app 启动阶段装配或由包内 registry 注册。

## 错误处理

- 工具错误应返回给 agent，而不是直接退出程序。
- 配置错误可以在启动阶段失败。
- 用户输入错误应由 CLI 层展示清楚。
- agent 循环中的错误要包含当前步骤信息，便于调试。

## 可测试性

- agent core 使用 fake LLM 和 fake tools 测试。
- workspace 工具使用临时目录测试。
- CLI 层使用输入输出流测试。
- 不依赖真实模型完成单元测试。
