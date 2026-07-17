# Event-Sourced Service Runtime 组件清单

> 依据：`localDocs/event-sourced-service-runtime-architecture.md` 目标架构草案  
> 用途：作为后续逐个确认组件职责、输入输出、状态所有权和依赖关系的工作清单。  
> 说明：本清单描述目标重构方案，不代表当前代码实现情况。

## 一、清单顺序

组件暂时按照文档建议的建设与运行顺序排列：

```text
定义与编译
  → 持久化与消息基础设施
  → 服务宿主与生命周期
  → Task
  → Agent 与 Model
  → Capability 与 Policy
  → Orchestrator
  → Knowledge / RAG
  → Memory
  → 持久化实现与分布式扩展
```

本文件暂时只登记组件，不详细定义职责。后续确认一个组件后，再补充它的职责、边界、输入、输出、状态和依赖。

## 二、构建期控制平面

1. `RuntimeManifest`
2. `ServiceDefinition`
3. `ServiceDescriptor`
4. `ServiceFactory`
5. `Register`
6. `RuntimeBuilder`
7. `RuntimePlan`
8. `RoutingTable`
9. `RuntimeBootstrap`

## 三、消息与持久化基础设施

10. `Message Envelope`
11. `Transport`
12. `In-Process Transport`
13. `EventBus`
14. `MessageRouter`
15. `Durable Mailbox`
16. `Inbox`
17. `Outbox`
18. `Outbox Dispatcher`
19. `Dead Letter`
20. `EventStore / Journal`
21. `Snapshot Store`
22. `Effect Store`
23. `Artifact Store`
24. `Projection`
25. `Stream Sequence / Global Offset`

## 四、服务宿主、寻址与生命周期

26. `ServiceHost`
27. `ServiceInstanceStore`
28. `InstanceDirectory`
29. `AddressResolver`
30. `ActivationManager`
31. `Activation Lease / ActivationEpoch / Fencing`
32. `Service Supervisor`
33. `Scheduler`
34. `Effect Worker / Effect Executor`
35. `Reconciliation Processor`

## 五、通用服务处理协议

36. `Service`
37. `Service.Handle`
38. `Service.Apply`
39. `Decision`
40. `Aggregate.Decide`
41. `Aggregate.Apply`

以上是服务实现需要遵守的通用协议。它们不是独立部署的服务，但属于 Runtime 必须定义的核心扩展边界。

## 六、Task 组件

42. `TaskService`
43. `TaskAggregate`
44. `TaskStateMachine`
45. `Task Projection`

## 七、Agent 与 Model 组件

46. `Agent Service`
47. `Model Service`
48. `AgentSupervisor`
49. `AgentInstanceStore`
50. `Agent Instance Directory Projection`
51. `Agent Activation`
52. `AgentSpec Registry`

## 八、Capability 与 Policy 组件

53. `Capability Gateway`
54. `Policy Service`
55. `User Interaction / Approval Service`
56. `Capability Service`
57. `Local Tool Capability Service`
58. `MCP Capability Service`
59. `HTTP Capability Service`
60. `Internal Capability Service`

## 九、Goal 与工作流协调组件

61. `Orchestrator Service`
62. `Aggregator Service`
63. `Goal Aggregate / Goal State Machine`

## 十、Knowledge、RAG 与向量组件

64. `Document Store Service`
65. `Embedding Service`
66. `Vector Store Service`
67. `Knowledge Ingestion Service`
68. `RAG / Retrieval Service`
69. `Knowledge Gateway`
70. `Rerank Service`

## 十一、Memory 组件

71. `Memory Service`
72. `Memory Policy`
73. `Memory Extraction Workflow`

## 十二、存储与部署扩展组件

74. `Transactional Persistence Adapter`
75. `SQLite Persistence Adapter`
76. `Remote Transport`
77. `Distributed Service Discovery`
78. `Remote Service Host / Multi-Process Deployment`

## 十三、重要数据模型与协议对象

以下对象不是独立运行组件，但会决定组件之间的边界，需要在后续逐项设计时同步确认：

1. `ServiceAddress`
2. `Message`
3. `Command`
4. `Query`
5. `Event`
6. `Reply`
7. `Effect`
8. `OutgoingMessage`
9. `RuntimeManifest`
10. `RuntimePlan`
11. `ServiceInstanceRecord`
12. `AgentInstanceRecord`
13. `AgentInstanceMetadata`
14. `ActivationLease`
15. `DeliveryTarget`
16. `StoredEvent`
17. `Snapshot`
18. `TaskState`
19. `TaskWaitState`
20. `AgentSpec`
21. `AgentInvocation`
22. `AgentExecutionState`
23. `RunSnapshot`
24. `CapabilityDescriptor`
25. `CapabilitySelector`
26. `CapabilityGrant`
27. `CapabilityCall`
28. `CapabilityCallState`
29. `PolicyDecision`
30. `ApprovalState`
31. `RequestGroup`
32. `RetrievalState`
33. `IngestionState`
34. `ChunkState`
35. `KnowledgeScope`
36. `Document Revision`
37. `EmbeddingRequest / EmbeddingResult`
38. `VectorRecord`

## 十四、后续逐项确认模板

后续每个组件统一补充以下内容：

```text
组件名称：
所属层级：
是否独立服务：

负责：
不负责：

接收：
产生：

拥有的状态：
不允许拥有的状态：

依赖接口：
被谁依赖：

持久化内容：
恢复方式：
幂等要求：
关键事件：
```
