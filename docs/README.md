# Docs

这里存放 agent 学习开发项目的具体文档。根目录 `AGENTS.md` 只作为导航地图，详细内容都从这里展开。

## 文档列表

- [Service Runtime 实现说明](../serviceruntime/README.md)：当前实现、包结构、运行流程和边界。
- [Runtime 框架接口设计](runtime-framework-interface-design.md)：Runtime 核心接口和架构不变量。
- [事件溯源服务 Runtime 架构](event-sourced-service-runtime-architecture.md)：目标架构和业务服务边界。
- [Service 开发规范](service-development-guide.md)：Service 的协议、状态、Decision、Replay、Effect、注册、版本和测试规范。
- [CapabilityService 与 ApprovalService 开发边界](capability-approval-service-development-guide.md)：能力调用、内置权限判断、人工审批、Provider 和 Effect Executor 的职责与恢复边界。
- [项目最新现状](current-status.md)：截至 2026-07-01 的当前实现快照，说明项目如何从 `tmp/` 历史材料演进到现在的事件驱动 runner。
- [项目概览](project-overview.md)
- [产品能力](product-goals.md)
- [架构设计](architecture.md)
- [模块规划](modules.md)
- [代码模块实现](code-modules.md)
- [开发路线](development-roadmap.md)
- [开发约定](development-guide.md)

## 文档维护原则

- 需要了解当前代码真实状态时，优先读 `../serviceruntime/README.md`、实际代码和测试。
- `current-status.md` 以及仍以旧 `internal/`、NativeLoop 或旧 runner 为当前实现的文档只作历史参考。
- 文档先描述稳定目标，再描述当前实现状态。
- 架构文档描述依赖方向，不绑定某个临时实现细节。
- 模块文档只写职责边界，具体 API 以代码和测试为准。
- 路线文档按阶段维护，每完成一个阶段后再细化下一阶段。
