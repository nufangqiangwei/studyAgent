# 模块规划

本项目使用 Go 开发，建议以 `internal/` 为主要业务代码目录。每个模块只暴露自己稳定的职责边界，避免跨模块读取内部状态。

## 目录建议

```text
.
├── AGENTS.md
├── docs/
├── go.mod
├── main.go
├── internal/
│   ├── app/
│   ├── agent/
│   ├── cli/
│   ├── command/
│   ├── config/
│   ├── content/
│   ├── llm/
│   ├── logging/
│   ├── prompt/
│   ├── session/
│   ├── startup/
│   ├── startupcmd/
│   ├── tools/
│   └── workspace/
└── pkg/
```

`pkg/` 只有在某些包确实需要被外部项目复用时再使用。学习阶段可以先不创建。

## 模块职责

### main.go

- 程序入口。
- 将 `os.Args`、标准输入输出和 `context.Context` 交给 `internal/app`。
- 不放业务逻辑。

后续如果需要发布多个二进制，可以迁移为 `cmd/agent/main.go`。

### internal/startup

- 启动参数解析。
- 解析全局 flags。
- 输出启动配置和命令名。
- 不依赖命令注册表，不执行命令。

### internal/startupcmd

- 启动命令调用包装。
- 接收 `startup.Config`、`command.Registry` 和 `content.Env`。
- 只负责把启动命令转交给统一 command registry。
- 启动命令错误直接向上返回。

### internal/command

- 定义对外命令接口 `Command`。
- 维护命令注册表 `Registry`。
- 命令执行分发。
- 默认命令注册到包级 `Manage`，当前包括 `run`、`agent`、`set-agent`、`help`、`status`、`version`、`model`。
- 后续新增命令只需要实现接口并注册。
- 命令只依赖 `content.Env` 中的 `AgentRunner`、`CommandRegistry`、IO、logger 和配置快照，不依赖 agent 具体实现。

### internal/app

- 应用级编排。
- 读取启动参数和配置文件。
- 创建 LLM client、agent selector 和运行时 `content.Env`。
- 选择命令行模式或交互式 CLI 模式。
- 连接 CLI 输入输出。
- 管理启动和退出流程。

### internal/content

- 定义运行时 `Env`、`IO` 和配置快照。
- 定义跨模块小接口，例如 `AgentRunner`、`AgentSelector`、`Logger`、`CommandInfo`、`CommandRegistry`。
- 通过 `WithEnv` 把当前执行环境绑定到 `context.Context`。
- 通过 `EnvFromContext` 让工具等底层动作读取当前执行流的 IO 和配置。
- 子 agent 或并发任务需要独立配置时，派生子 context 并绑定自己的 `Env`，避免互相覆盖运行态。

### internal/agent

- 定义对外 `Agent` 接口和 `NewAgent` 工厂类型。
- 维护 agent 工厂注册表 `Catalog`。
- 当前默认注册 `default`、`analyze` 和 `tools-tester` agent。
- agent 任务循环。
- 决定下一步是调用模型、调用工具还是结束。
- 管理步骤状态。
- 处理模型响应和工具结果。

### internal/prompt

- prompt engineering。
- 在每次 LLM 请求前构造 system/user/tool 消息。
- 管理不同 prompt 策略。
- 不负责调用模型。

### internal/llm

- 定义模型调用接口。
- 实现不同模型供应商适配器。
- 提供 provider 请求构建模块。
- 处理请求、响应、流式输出和错误映射。
- 不包含 agent 决策逻辑。

### internal/logging

- 日志级别解析。
- 日志输出。
- 为其他模块提供可替换的 logger 实现。

### internal/tools

- 定义对外工具接口 `Tool`。
- 维护工具注册表 `Registry`。
- 维护默认工具集合和当前工具注册表。
- 当前已实现 `ask_user` 工具。
- 后续补充文件读取工具、文本搜索工具、shell 执行工具。
- 后续可扩展为插件系统。

### internal/config

- 配置文件读取。
- 环境变量读取。
- 默认模型、超时、工作区路径等配置。
- 配置校验。
- 只负责持久化配置来源，不承载单次执行的运行态；运行态统一放在 `internal/content.Env`。

### internal/cli

- 复杂交互式输入。
- 输出格式化。
- 流式文本展示。
- 用户确认提示。
- 把普通文本和 slash 命令转换为统一 command registry 调用。
- 未匹配到已注册命令的输入默认作为用户消息发给模型。
- `/exit` 和 `/quit` 只作为交互会话控制，不作为 command 包命令。

### internal/session

- 用户消息、模型消息和工具消息。
- 会话历史。
- 上下文裁剪。
- token 或长度预算。

### internal/workspace

- 工作区路径解析。
- 文件枚举。
- 文件读取。
- 文本搜索。
- 项目类型识别。

## 已实现模块

当前已创建：

```text
main.go
internal/app
internal/startup
internal/startupcmd
internal/cli
internal/command
internal/agent
internal/content
internal/prompt
internal/llm
internal/logging
internal/config
internal/tools
```

后续优先补充：

```text
internal/workspace
internal/session
```

## 解耦原则

- 模块之间通过接口、结构体和 `context.Context` 传递数据，不共享请求态全局变量。
- 外层模块依赖内层抽象，具体实现由启动阶段注入。
- `agent`、`command`、`tools` 可以维护包级默认注册表，但注册表只表达可用能力，不保存单次运行环境。
- 单次运行环境必须通过 `content.Env` 传递，并绑定到 context，便于并发运行和测试隔离。
- 不允许形成循环依赖。
- 每个包都应该能用一句话说明职责。
- 如果一个包开始同时处理启动参数、命令、模型、prompt 和工具，说明它需要拆分。

## 下一步模块

建议下一阶段优先补齐：

- `internal/workspace`：文件枚举、文件读取、文本搜索。
- `internal/session`：消息历史、上下文窗口、步骤记录。
- `internal/config`：环境变量、用户级配置和更完整的配置优先级合并。
