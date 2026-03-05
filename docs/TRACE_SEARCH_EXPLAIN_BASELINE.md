# Trace 检索分页索引与 EXPLAIN 基线

## 目标查询

1. Replay job trace 检索（id-desc 游标）：

```sql
SELECT id,status,topic_filter,trace_id,command_id,total_items,processed_items,success_items,failed_items,last_error,created_at,updated_at
FROM replay_jobs
WHERE trace_id=$1 AND command_id=$2 AND id < $3
ORDER BY id DESC
LIMIT $4;
```

2. Outbox trace 检索（id-desc 游标）：

```sql
SELECT id,topic,status,retry_count,max_retries,trace_id,command_id,created_at,updated_at
FROM outbox_events
WHERE trace_id=$1 AND command_id=$2 AND id < $3
ORDER BY id DESC
LIMIT $4;
```

## 索引建议

- `idx_replay_jobs_trace_command_id_desc(trace_id, command_id, id DESC)`
- `idx_replay_jobs_trace_id_desc(trace_id, id DESC)`
- `idx_replay_jobs_command_id_desc(command_id, id DESC)`
- `idx_outbox_trace_command(trace_id, command_id, id DESC)`（已存在）
- `idx_outbox_trace_id_desc(trace_id, id DESC)`
- `idx_outbox_command_id_desc(command_id, id DESC)`

## EXPLAIN 基线

基线通过仓储集成测试输出并断言：
- 测试：`TestStoreTraceCorrelationExplainUsesCompositeIndexesSQL`
- 文件：`backend/internal/repository/store_sql_regression_integration_test.go`

预期计划关键字：
- replay 查询包含以下之一：`idx_replay_jobs_trace_command_id_desc` / `idx_replay_jobs_command_id_desc` / `idx_replay_jobs_trace_id_desc`
- outbox 查询包含以下之一：`idx_outbox_trace_command` / `idx_outbox_command_id_desc` / `idx_outbox_trace_id_desc`
