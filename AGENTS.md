# Agents Project Map

本文件是 agent 学习开发项目的导航地图。具体内容放在 `docs/` 目录，根目录只保留入口、约定和索引。

## 项目定位

本项目用于学习和实现一种命令行 agent 产品形态，目标体验参考 Codex CLI、Gemini CLI、Claude Code 这类开发者工具：

- 通过 CLI 与 agent 交互。
- 能读取项目上下文、规划任务、调用工具、修改代码并运行验证。
- 支持多模型或多供应商接入。
- 以 Go 语言实现，强调模块解耦、接口边界清晰、可测试和可扩展。

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

## 根目录职责

根目录建议只保留项目入口文件和高层说明：

- `AGENTS.md`：项目导航地图。
- `go.mod`：Go 模块定义。
- `main.go`：当前程序启动入口，负责调用 `internal/app`。
- `docs/`：项目文档。
- `internal/`：当前 Go 模块骨架。
- `localDocs/``plan/`：该目录是开发人员辅助理解项目的文档。只有当用户明确要求的时候，才允许读取这两个目录下的文件，其他时间禁止读写。

后续代码增长后，建议将业务代码逐步迁移到 `cmd/`、`internal/`、`pkg/` 等目录，避免所有逻辑堆在 `main.go`。
