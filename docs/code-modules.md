# 代码模块实现

当前代码已经建立第一版可运行骨架。默认命令可以通过下面方式启动：

```powershell
go run . run "summarize this project"
```

当前开发机推荐使用 Go 1.26 toolchain：

```powershell
& "C:\Users\qiangwei\go\pkg\mod\golang.org\toolchain@v0.0.1-go1.26.0.windows-amd64\bin\go.exe" run . run "summarize this project"
```

如果希望直接使用 `go run`，需要确保这个 toolchain 的 `bin` 目录排在 `C:\Program Files\Go\bin` 前面：

```powershell
$env:Path = "C:\Users\qiangwei\go\pkg\mod\golang.org\toolchain@v0.0.1-go1.26.0.windows-amd64\bin;$env:Path"
go version
go run . run "summarize this project"
```

## 当前包结构

```text
.
├── main.go
└── internal/
    ├── app/
    ├── startup/
    ├── startupcmd/
    ├── cli/
    ├── content/
    ├── config/
    ├── command/
    ├── agent/
    ├── prompt/
    ├── llm/
    │   ├── anthropic/
    │   ├── deepseek/
    │   ├── gemini/
    │   ├── mock/
    │   ├── openai/
    │   └── provider/
    ├── logging/
    └── tools/
```

## 运行时上下文模块

位置：`internal/content`

职责：

- 定义单次执行使用的 `Env`。
- 定义 IO、配置快照和跨模块小接口。
- 通过 `WithEnv` 把 `Env` 绑定到 `context.Context`。
- 通过 `EnvFromContext` 从 context 取回当前执行环境。

当前 `Env` 包含：

```go
type Env struct {
    IO       IO
    Agent    AgentRunner
    Registry CommandRegistry
    Logger   Logger
    Config   Config
    RunModel string
}
```

这个包是当前解耦命令、工具和 agent 的共享边界。命令通过 `content.Env` 使用 agent runner、命令注册表、IO 和配置；工具在需要终端输入输出时从 context 读取 `Env`。不同执行流可以派生不同 context 并绑定不同 `Env`，因此并发运行 agent、命令或测试时不会依赖同一个全局运行态。

## 启动参数解析模块

位置：`internal/startup`

职责：

- 解析全局启动参数。
- 决定启动命令，例如 `run`、`help`、`version`。
- 提供 model、workdir、log-level 等启动配置；provider 由 model 名称推断。

当前支持：

```text
agent [flags] <command> [args]

--provider string
--config string
--model string
--workdir string
--log-level string
--help, -h
--version, -v
```

`--provider` 当前仅保留兼容旧命令行输入，模型请求模块不再依赖它。

该模块不依赖 command registry，也不直接执行命令。它只负责把启动输入解析成结构化配置。

## 命令解析执行模块

位置：`internal/command`

职责：

- 定义 `Command` 接口。
- 提供 `Registry` 注册表。
- 提供真实命令实现和统一执行分发。
- 提供默认命令：`run`、`agent`、`set-agent`、`help`、`status`、`version`、`model`。
- 不负责读取启动参数，也不负责交互式输入循环。

新增命令步骤：

1. 实现 `Command` 接口。
2. 在 `RegisterDefaults` 中注册，或后续由 app/plugin 注册。
3. 命令只通过 `Env` 使用输入输出、logger、agent runner 和配置。

命令模块不依赖 agent 具体实现。它只依赖 `AgentRunner` 小接口：

```go
type AgentRunner interface {
    Run(ctx context.Context, task string) error
}
```

当前注册机制：

- `command.NewRegistry` 创建空注册表。
- `RegisterDefaults` 注册内置命令。
- 包级 `command.Manage` 在 `init` 中创建并注册默认命令，供 app 默认装配。
- `Registry.Execute` 会把当前 registry 写入 `env.Registry`，再通过 `content.WithEnv` 绑定到 context，保证命令内部和后续工具能拿到同一份执行环境。

## 命令调用包装模块

位置：`internal/startupcmd`、`internal/cli`

职责：

- `internal/startupcmd` 把启动解析结果转换为一次 `command.Registry.Execute` 调用。
- `internal/cli` 负责 banner、prompt、读取输入和 slash 命令解析。
- CLI 中的 `/run`、`/status`、`/help` 等命令最终都调用 `internal/command` 中的同一套命令实现。
- CLI 中未匹配到已注册命令的输入会作为普通用户消息发给模型。
- `/exit` 和 `/quit` 只控制交互会话退出，不注册为业务命令。

## Agent Native Loop

位置：`internal/agent`

职责：

- 定义对外 `Agent` 接口。
- 定义 agent 工厂 `NewAgent` 和创建参数 `CreatAgentOptions`。
- 维护 agent 工厂注册表。
- 接收 task。
- 调用 prompt builder 构建发送给 LLM 的消息。
- 调用 LLM client。
- 收集步骤结果。
- 后续扩展工具调用、测试反馈和多步循环。

当前 `NativeLoop` 是最小单步 loop，`MaxSteps` 默认为 1。它依赖两个接口：

- `LLMClient`
- `PromptBuilder`

这让测试时可以替换 fake LLM 或 fake prompt builder，不需要真实模型。

当前 agent 注册机制：

- `runtime/agents.FactoryRegistry` 是通用 agent 工厂注册表，负责规范化名称、列举和按名称创建实例。
- `runtime/agents/builtinagents.NewFactoryRegistry` 注册 `default`、`analyze` 和 `tool-tester`，具体构造逻辑留在 builtin agent 模块内。
- `runtime/runservice.Service` 只负责 run 生命周期和 active agent 选择；实际实例由 runtime setup builder 通过 factory registry 创建。
- `content.AgentRunner` 和 `content.AgentSwitcher` 是命令层使用的外部接口，命令不依赖 `DefaultAgent`、`AnalyzeAgent` 或 `ToolsTesterAgent` 具体类型。
- 每个 agent 创建自己的工具注册表并注入 `NativeLoop`，agent loop 只依赖 `ToolRegistry` 接口。

## Prompt Engineering 模块

位置：`internal/prompt`

职责：

- 在每次请求 LLM 前生成最终输入文本。
- 维护 system prompt。
- 组合 task、workspace、loop step 等上下文。
- 返回 `llm.Message` 列表。

当前实现是 `NativeBuilder`。后续可以增加不同 prompt 策略，例如：

- code review prompt。
- tool-use prompt。
- patch generation prompt。
- test-fix prompt。

prompt 模块只负责构建消息，不负责调用模型。

## LLM 请求模块

位置：`internal/llm`

职责：

- 定义统一 `Client`、`Request`、`Response`、`Message` 类型。
- 提供默认 `mock` client，便于本地开发测试。
- 提供 provider factory。
- 为 OpenAI、DeepSeek、Gemini、Claude/Anthropic 准备独立请求构建模块。

当前 provider 行为：

- provider 由 `model_name` 推断，不依赖 `provider` 配置。
- `mock-*`：返回本地 mock 响应。
- `gpt-*`、`o*`、`chatgpt-*`：发送 OpenAI Chat Completions 请求。
- `deepseek-*`：发送 DeepSeek OpenAI 兼容请求。
- `deepseek-v4*`：发送 DeepSeek Anthropic 兼容 Messages 请求。
- `gemini-*`：发送 Gemini `generateContent` 请求。
- `claude-*`：发送 Anthropic Messages 请求。

真实 HTTP 调用在 `llm.Client.Complete` 中发生；`provider.New` 只完成 endpoint、请求构建器和响应解析器装配。非 mock provider 缺少 `api_key` 时会在调用模型时返回错误，不影响 `status` 这类不调用模型的命令。`model_url` 可填写完整 endpoint，也可填写常见 base URL，例如 OpenAI 的 `/v1` base、DeepSeek 的 `/anthropic` base、Gemini 的 `/v1beta` base。

## 日志模块

位置：`internal/logging`

职责：

- 提供简单分级日志。
- 支持 `debug`、`info`、`warn`、`error`、`silent`。
- 输出到 app 注入的 writer。

其他模块只依赖小型 logger 接口，不直接绑定具体日志实现。

## 工具模块

位置：`internal/tools`

职责：

- 定义 `Tool` 接口。
- 提供工具注册表。
- 提供统一工具执行结果。

当前注册机制：

- `tools.NewManage` 创建空工具管理对象。
- tool 包在 `init` 中把默认工具注册到内部 `currentRegistry`。
- `NewDefaultManage` 按默认工具名从 `currentRegistry` 派生 agent 持有的工具管理对象。
- `AddTool(name, manage)` 只能按名称从 `currentRegistry` 复制已注册工具到目标管理对象；直接注册方法保持包内私有。
- `RegisteredTools` 读取当前默认工具列表；内部使用锁保护。
- agent 创建时把工具管理对象注入 `NativeLoop`，loop 只依赖 `ToolRegistry` 接口。

当前已实现 `ask_user` 工具，用于让 agent 在关键信息缺失时向终端用户提问。该工具通过 `content.EnvFromContext` 获取当前执行流的输入输出，因此不会直接依赖全局 stdin/stdout。agent loop 已能把注册工具声明传给 LLM，并在模型返回 tool call 后执行工具、把结果带入下一步模型请求。后续加入文件读取、文本搜索、shell 执行、补丁应用等能力时，可以继续复用同一套工具注册和执行接口。

## 当前执行流

```text
main.go
  -> internal/app.Run
  -> startup.Parse
  -> logging.New
  -> config.LoadOptional
  -> provider.New
  -> runtime/agents.FactoryRegistry
  -> entrance/app.runtimeSetupBuilder
  -> runtime/runservice.Service
  -> content.WithEnv
  -> command.Manage
  -> startupcmd.Run or runtimecli.Run
  -> command.Registry.Execute
  -> run command
  -> agent.NativeLoop.Run
  -> prompt.NativeBuilder.Build
  -> llm.Client.Complete
```

## 扩展原则

- 新命令加到 `internal/command` 或独立 command 包，再注册到 registry。
- 新 agent 实现 `runtime/agents.Agent`，提供 factory spec，并注册到 `runtime/agents.FactoryRegistry`；无需修改 app 或 run service 的构造分支。
- 新 prompt 策略实现 `agent.PromptBuilder` 需要的方法。
- 新 LLM provider 实现 `llm.Client`。
- 新工具实现 `tools.Tool`，由 registry 管理。
- app 层负责装配依赖，核心模块不直接读环境变量、不直接读命令行参数。
- 需要单次运行状态时，优先通过 `content.Env` 和 context 传递，不新增请求态全局变量。
