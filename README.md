# Environmental Sample Chain Orchestrator

一个从 0-1 构建的环境样本链路编排项目基线，用来管理采样、容器封签、跨机构交接、实验室接收、分装、实验批次、仪器结果修订、质量复核和证据包导出。

## 当前实现

- `src/domain/types.ts`：领域对象、状态和同步模型
- `src/store/store.ts`：内存 Store、全局 ID、审计、Outbox 和事务快照回滚
- `src/services/sampling-service.ts`：采样记录与封存容器
- `src/services/custody-service.ts`：交出、接收、封签和温度轨迹
- `src/services/receiving-service.ts`：实验室接收、接受/隔离/拒收
- `src/services/aliquot-service.ts`：批量分装、单位换算、容量扣减和幂等操作
- `src/services/batch-service.ts`：实验批次成员、协议冻结和运行生命周期
- `src/services/result-service.ts`：仪器运行、外部运行号幂等和结果 revision
- `src/services/quality-service.ts`：质量结论、质控门槛和批次批准
- `src/services/evidence-service.ts`：一致性证据包、自包含清单、内容哈希、原子提交和冻结
- `src/services/evidence-file-adapter.ts`：证据包对象存储适配器（幂等上传、超时注入）
- `src/services/sync-service.ts`：离线 Envelope、客户端序列、payload 冲突和重放
- `src/demo/smoke.ts`：一条可运行的端到端样例

## 证据包一致性保证

证据包按三个串行阶段生成，保证"一致快照、失败可安全重试、同请求同逻辑版本、冻结后不变"：

1. **prepare（事务内）**：一次性读取项目范围内的采样、容器、交接、接收、分装、批次、运行、结果修订和质量结论，做租户/项目作用域校验与批准过滤；工件（manifest）自包含全部源对象快照，`snapshotVersion` 只由导出数据内容派生。
2. **upload（事务外）**：通过 `EvidenceFileAdapter` 写入对象存储；适配器对相同 `(key, contentHash)` 幂等，超时后重试不会产生重复或损坏对象。
3. **commit（事务内）**：文件记录与包索引在同一事务中发布；索引只会与 `COMMITTED` 文件一起出现。任何阶段失败只留下 `FAILED` 标记（不产生"看似完成但缺文件"的索引），客户端按原 `requestKey` 重试复用同一包记录。

同一 `(tenantId, projectId, requestKey)` 的并发调用共享同一个进行中的 Promise；已 `READY`/`FROZEN` 的请求永远返回同一逻辑版本（同 `manifestHash`、同 `snapshotVersion`）。冻结前校验文件索引与哈希；冻结后即使源对象被删除/修订，工件内容不变。`verify()` 可检测存储对象丢失。

默认导出只包含 `APPROVED` 批次、`CURRENT` 结果修订以及带 `APPROVED` 质量结论的结果；`includeRejected: true` 导出完整复核轨迹。

## 运行

```bash
npm install
npm run build
npm run smoke
npm test
```

当前实现配套 23 个测试（`test/`，使用 node:test），覆盖并发生成、文件失败与模糊超时、重复请求、结果修订、对象删除、跨项目引用和未批准结果过滤。
