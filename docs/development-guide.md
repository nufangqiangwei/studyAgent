# 开发约定

本项目以学习为主，但代码仍应按真实工程要求组织。核心要求是模块解耦、边界清晰、可测试。

## Go 代码风格

- 使用 `gofmt` 格式化所有 Go 文件。
- 包名短小明确，避免 `common`、`utils` 这类含义过宽的包。
- 函数优先返回错误，不在底层包直接 `panic` 或 `os.Exit`。
- 公共类型和接口必须有清晰职责。
- 不提前抽象，等重复和边界稳定后再抽象。

## 接口设计

- 接口放在使用方附近，而不是所有实现方共享一个大接口包。
- 接口尽量小，优先一到三个方法。
- 业务逻辑依赖接口，启动阶段注入具体实现。
- 不让核心模块直接依赖具体模型 SDK、终端库或文件系统细节。

## 模块边界

- CLI 负责展示和输入，不负责 agent 决策。
- agent core 负责任务循环，不负责具体文件读写实现。
- tools 负责执行动作，不负责决定任务目标。
- llm 负责模型通信，不负责业务流程。
- workspace 负责工作区访问，不负责模型 prompt 设计。

## 错误处理

- 错误信息应包含操作上下文，例如文件路径、工具名、步骤名。
- 底层错误向上返回，由调用方决定展示、重试或终止。
- 工具执行失败是 agent 可处理状态，不一定是程序崩溃。

## 测试策略

- 对 agent core 使用 fake LLM 和 fake tools。
- 对 workspace 使用临时目录。
- 对 CLI 使用可注入的输入输出流。
- 对模型供应商适配器做少量集成测试，默认不依赖真实密钥。
- 每个修复或新能力至少覆盖核心成功路径和一个失败路径。

## 配置约定

配置来源建议按优先级合并：

1. 命令行参数。
2. 环境变量。
3. 项目配置文件。
4. 用户级配置文件。
5. 默认值。

配置读取集中在 `internal/config`，不要散落在业务模块里。

单次运行环境集中在 `internal/content.Env`，并通过 `content.WithEnv` 绑定到 `context.Context`。命令、agent 和工具需要 IO、logger、active agent、命令注册表或配置快照时，应优先从参数或 context 获取，不要新增请求态全局变量。子 agent、并发任务或测试需要不同运行配置时，派生子 context 并绑定自己的 `Env`。

`agent`、`command`、`tools` 包可以维护默认注册表，用于声明可用能力；注册表不应保存当前用户输入、当前模型响应、当前 IO 或其他单次执行状态。

## 日志约定

- 用户可见输出由 CLI 层控制。
- 调试日志和工具调用记录应可开关。
- 日志不要泄漏 API key、token 或敏感文件内容。

## 提交前检查

每次代码变更后建议运行：

```powershell
gofmt -w .
go test ./...
```

如果新增命令行行为，还应手动运行对应 `go run` 命令确认输出。

## 当前 Go Toolchain

当前开发机推荐使用 Go 1.26 toolchain：

```powershell
& "C:\Users\qiangwei\go\pkg\mod\golang.org\toolchain@v0.0.1-go1.26.0.windows-amd64\bin\go.exe" version
& "C:\Users\qiangwei\go\pkg\mod\golang.org\toolchain@v0.0.1-go1.26.0.windows-amd64\bin\go.exe" test ./...
```

如果希望直接输入 `go run`，需要让该 toolchain 的 `bin` 目录排在系统 Go 前面：

```powershell
$env:Path = "C:\Users\qiangwei\go\pkg\mod\golang.org\toolchain@v0.0.1-go1.26.0.windows-amd64\bin;$env:Path"
go version
go test ./...
```

如果出现标准库版本和 go tool 版本不一致，应优先检查：

```powershell
where.exe go
go version
go env GOOS GOARCH GOHOSTOS GOHOSTARCH GOROOT GOVERSION
```

不要只把 `GOROOT` 指向某个 toolchain 目录却继续使用另一个版本的 `go.exe`，这会导致 go tool 和标准库版本不匹配。

如果 `go run . run "summarize this project"` 在 Windows 上提示找不到临时构建出的可执行文件，例如路径末尾是 `agent` 而不是 `agent.exe`，通常是用户级 Go 配置把 `GOOS` 写成了 `linux`。可以检查：

```powershell
go env GOOS GOARCH GOHOSTOS GOHOSTARCH GOROOT GOVERSION
go env GOENV
```

如果确认需要恢复 Windows 本机运行默认值，可以移除用户级覆盖：

```powershell
go env -u GOOS
```
