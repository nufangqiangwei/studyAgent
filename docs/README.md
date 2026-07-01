# Docs

这里存放 agent 学习开发项目的具体文档。根目录 `AGENTS.md` 只作为导航地图，详细内容都从这里展开。

## 文档列表

- [项目最新现状](current-status.md)：截至 2026-07-01 的当前实现快照，说明项目如何从 `tmp/` 历史材料演进到现在的事件驱动 runner。
- [项目概览](project-overview.md)
- [产品能力](product-goals.md)
- [架构设计](architecture.md)
- [模块规划](modules.md)
- [代码模块实现](code-modules.md)
- [开发路线](development-roadmap.md)
- [开发约定](development-guide.md)

## 文档维护原则

- 需要了解当前代码真实状态时，优先读 `current-status.md`；其他文档可能保留阶段性设计和旧路径描述。
- 文档先描述稳定目标，再描述当前实现状态。
- 架构文档描述依赖方向，不绑定某个临时实现细节。
- 模块文档只写职责边界，具体 API 以代码和测试为准。
- 路线文档按阶段维护，每完成一个阶段后再细化下一阶段。
