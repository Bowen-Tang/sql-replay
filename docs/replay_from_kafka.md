# SQL Replay — Realtime Kafka Playback

> This README documents the new **Kafka realtime replay** capability added to `sql-replay`, including **usage**, **SASL/TLS**, **batch fetch & batch commit**, and **best practices** for high throughput.  
> 文档同时提供 **中文** 版本（见下半部分）。

---

## English

### Overview

`sql-replay` can now **replay SQL in realtime from Kafka**.

**Design guarantees**
- **Per-ID single connection**: one `id` → one dedicated `*sql.Conn` (never shared with other ids). All SQL for that id is executed serially on the same connection. `USE db` happens naturally inside SQL.
- **Order within an ID** is preserved; **IDs run in parallel** (per partition).
- **Offsets**: with a consumer group, the program **resumes from the last committed offset**; if none exists, it starts from **earliest** (when `-kafka-start=auto`).
- **Throughput control**: global FIFO queue, **batched fetch** (e.g., 1000 msgs every 100ms), and **batched commit** (e.g., commit every 1000 messages per partition).
- **Idle GC**: if an id has no activity for 1 hour (configurable), its dedicated connection and goroutine are closed.

### Kafka message format

Each Kafka message must be a JSON object:

```json
{
  "id": 127212454577296093,
  "query": "set autocommit=0",
  "query_type": "com_query",
  "ts": "2025-09-06 09:32:48.837816",
  "db": "test1"
}
```

- `id` (number or numeric string): **routing key**; all SQL with the same `id` share exactly one DB connection.
- `query`: the **full SQL** to execute (no protocol split).
- `db`: **initial** database name used only when creating the first connection for this `id` (if empty, falls back to `-kafka-default-db`). Later database changes should be done via normal `USE xxx` SQL.
- `ts`: used only if `-kafka-replay-with-gap=true` to reproduce per-id timing gaps; otherwise parsing can be skipped.

### Build

```bash
go build -o sql-replay ./...
```

**Dependencies**
- Go 1.20+
- MySQL driver: `github.com/go-sql-driver/mysql`
- Kafka client: `github.com/segmentio/kafka-go`

### Modes

Existing modes: `parsemysqlslow`, `parsetidbslow`, `replay`, `load`, `report`  
New mode: **`replay-kafka`**

### Quick start (Kafka)

```bash
./sql-replay -mode replay-kafka \
  -kafka-brokers '10.0.0.1:9092,10.0.0.2:9092' \
  -kafka-topic tx_sql -kafka-group replayer-g1 \
  -kafka-start auto \
  -kafka-dsn 'user:pass@tcp(127.0.0.1:4000)/?parseTime=true&multiStatements=true' \
  -kafka-default-db test1 \
  -kafka-id-queue-cap 2048 -kafka-idle-ttl 1h -kafka-stats-interval 10s \
  -kafka-ts-layout '2006-01-02 15:04:05.999999' -kafka-ts-loc 'Asia/Tokyo' \
  -kafka-replay-with-gap=false \
  -kafka-error-log-dir /var/log/sql-replay -kafka-stop-on-error=false \
  -kafka-sasl-mech plain -kafka-username alice -kafka-password 'secret' -kafka-tls=false \
  -kafka-fetch-queue-cap 10000 -kafka-fetch-batch 1000 -kafka-fetch-interval 100ms \
  -kafka-commit-every 1000 -kafka-commit-interval 200ms
```

### Flags (Kafka)

**Connectivity**
- `-kafka-brokers`: comma-separated `host:port`.
- `-kafka-topic`: topic name.
- `-kafka-group`: consumer group (enables offset resume).
- `-kafka-start`: `auto | committed | earliest | latest`.  
  - `auto`: use committed if exists; otherwise start from earliest (recommended).

**Security (SASL/TLS)**
- `-kafka-sasl-mech`: `plain | scram-sha256 | scram-sha512` (empty = no SASL).
- `-kafka-username`, `-kafka-password`: SASL credentials.
- `-kafka-tls`: enable TLS; `-kafka-insecure-skip-verify` to skip cert verification (not recommended).

**DB replay semantics**
- `-kafka-dsn`: DSN **without** database name, e.g. `...?parseTime=true&multiStatements=true`.
- `-kafka-default-db`: default db when event `db` is empty for the **first** connection of an id.
- **Guarantee**: one `id` → one dedicated `*sql.Conn`; serial execution preserves `SET autocommit`, transactions, session variables, and `USE` semantics.

**Timing**
- `-kafka-replay-with-gap`: if `true`, per-id `sleep = (ts(cur) - ts(prev) - prev_exec_time)`; if `false`, do not sleep (higher throughput).
- `-kafka-ts-layout`, `-kafka-ts-loc`: timestamp parsing for `ts` field; when gap is disabled, parsing is skipped for better CPU efficiency.

**Throughput & backpressure**
- `-kafka-fetch-queue-cap`: global FIFO capacity (default 10000). Acts as entrance buffer.
- `-kafka-fetch-batch`: number of messages to fetch per tick (default 1000).
- `-kafka-fetch-interval`: fetch tick interval (default 100ms). If the FIFO is full, fetching waits.
- `-kafka-id-queue-cap`: **per-id** queue capacity (default 1024). Limits how much a **hot id** can buffer ahead of execution.

**Offset commit (batched)**
- `-kafka-commit-every`: commit per partition after N advanced offsets (default 1000).
- `-kafka-commit-interval`: also commit at least every T (default 200ms).

**Other**
- `-kafka-idle-ttl`: idle timeout to recycle id workers (default 1h).
- `-kafka-stats-interval`: periodic stats print interval (default 10s).
- `-kafka-error-log-dir`: append failed SQL lines with reason (partition/offset/id/error/SQL).
- `-kafka-stop-on-error`: stop the process on first SQL error (default false).

### Stats output

```
[stats] up=10s parts=3 active_ids=60 total_conns=60 active_conns=60
```
- `parts`: number of partitions currently instantiated.
- `active_ids`: number of id workers (equals number of dedicated DB connections).
- `total_conns`: same as active_ids (1 id = 1 connection).
- `active_conns`: connections executing or with pending queue > 0.

### Best practices

**Throughput vs latency**
- For **high throughput**, set:
  - `-kafka-replay-with-gap=false`
  - `-kafka-fetch-batch=1000`, `-kafka-fetch-interval=100ms`
  - `-kafka-commit-every=1000`, `-kafka-commit-interval=200ms`
  - `-kafka-id-queue-cap=2048~4096` to smooth hot-id bursts
- For **lower latency**, reduce `-kafka-fetch-batch` to 256~512 and keep commit thresholds smaller.

**Avoid hot-id backpressure**
- If many messages belong to one id, its per-id queue may fill up and stall global dispatch (by design).  
  Raise `-kafka-id-queue-cap` moderately (e.g., 2048 or 4096) or reduce `-kafka-fetch-batch`.

**Database tuning (for replay/benchmark environments)**
- Consider relaxed durability for offline replay: `innodb_flush_log_at_trx_commit=2`, `sync_binlog=0`, larger `innodb_log_file_size`, adequate `max_connections`.
- Ensure `multiStatements=true` in DSN if you plan to enable multi-statement batching in the future (feature ready to be added; current version executes one statement per message).

**SASL/TLS**
- Verify mechanism matches broker config; wrong SASL config leads to auth failures.
- Prefer TLS in production (`-kafka-tls=true`) with proper CA/certificates.

### Troubleshooting

- `Unknown Topic Or Partition` during commit  
  Ensure we **include Topic in commit** (fixed in current code). Also verify topic/group/ACLs.
- High CPU in decoder while gap disabled  
  Current code skips `ts` parsing when `-kafka-replay-with-gap=false`.
- Low QPS vs Kafka ingest  
  Concurrency upper bound ≈ **number of active ids**. To reach higher QPS, increase active ids, optimize DB latency, and ensure batching/commit settings are sane.

### Example: SASL + TLS

```bash
./sql-replay -mode replay-kafka \
  -kafka-brokers 'k1:9093,k2:9093' \
  -kafka-topic tx_sql -kafka-group replayer-g1 -kafka-start auto \
  -kafka-dsn 'user:pass@tcp(10.0.0.10:4000)/?parseTime=true&multiStatements=true' \
  -kafka-default-db test1 \
  -kafka-sasl-mech scram-sha512 -kafka-username alice -kafka-password 'secret' \
  -kafka-tls=true -kafka-insecure-skip-verify=false \
  -kafka-fetch-queue-cap 10000 -kafka-fetch-batch 1000 -kafka-fetch-interval 100ms \
  -kafka-commit-every 1000 -kafka-commit-interval 200ms
```

---

## 中文

### 概述

`sql-replay` 现在支持**从 Kafka 实时回放 SQL**。

**核心语义保证**
- **同一 id 绑定同一物理连接**：1 个 `id` → 1 条专属 `*sql.Conn`，绝不与其它 id 共用；同一 id 内严格串行。`USE db` 在这条连接上自然生效。
- **同一 id 内顺序**严格保持；分区内按 id 并行。
- **位点**：使用消费组时，默认**自动读取上一次位点**；若不存在，则从**最早**开始（`-kafka-start=auto` 时）。
- **吞吐控制**：全局 FIFO、**批量抓取**（如每 100ms 抓 1000 条）与**批量提交**（如每分区累计 1000 条提交一次）。
- **空闲回收**：某个 id 超过 1 小时无活动则回收其连接与 goroutine（可配置）。

### Kafka 消息格式

```json
{
  "id": 127212454577296093,
  "query": "set autocommit=0",
  "query_type": "com_query",
  "ts": "2025-09-06 09:32:48.837816",
  "db": "test1"
}
```

- `id`：**路由键**；同一 id 的 SQL 必须在**同一条连接**上串行执行。
- `query`：**完整 SQL**（无需区分协议）。
- `db`：仅在该 id 第一次创建连接时使用；为空则落到 `-kafka-default-db`。后续切库靠 `USE` 语句本身。
- `ts`：只有当 `-kafka-replay-with-gap=true` 时用于复现间隔；否则会跳过解析以节省 CPU。

### 构建

```bash
go build -o sql-replay ./...
```

**依赖**
- Go 1.20+
- `github.com/go-sql-driver/mysql`
- `github.com/segmentio/kafka-go`

### 模式

已有模式：`parsemysqlslow`, `parsetidbslow`, `replay`, `load`, `report`  
新增模式：**`replay-kafka`**

### 快速开始（Kafka）

```bash
./sql-replay -mode replay-kafka \
  -kafka-brokers '10.0.0.1:9092,10.0.0.2:9092' \
  -kafka-topic tx_sql -kafka-group replayer-g1 \
  -kafka-start auto \
  -kafka-dsn 'user:pass@tcp(127.0.0.1:4000)/?parseTime=true&multiStatements=true' \
  -kafka-default-db test1 \
  -kafka-id-queue-cap 2048 -kafka-idle-ttl 1h -kafka-stats-interval 10s \
  -kafka-ts-layout '2006-01-02 15:04:05.999999' -kafka-ts-loc 'Asia/Tokyo' \
  -kafka-replay-with-gap=false \
  -kafka-error-log-dir /var/log/sql-replay -kafka-stop-on-error=false \
  -kafka-sasl-mech plain -kafka-username alice -kafka-password 'secret' -kafka-tls=false \
  -kafka-fetch-queue-cap 10000 -kafka-fetch-batch 1000 -kafka-fetch-interval 100ms \
  -kafka-commit-every 1000 -kafka-commit-interval 200ms
```

### 主要参数（Kafka）

**连接**
- `-kafka-brokers`：`host:port` 列表。
- `-kafka-topic`：Topic 名。
- `-kafka-group`：消费者组（用于位点持久化与断点续跑）。
- `-kafka-start`：`auto | committed | earliest | latest`  
  - `auto`：有位点用位点，没位点从最早开始（推荐）。

**安全（SASL/TLS）**
- `-kafka-sasl-mech`：`plain | scram-sha256 | scram-sha512`（空字符串表示不开启 SASL）。
- `-kafka-username`, `-kafka-password`：SASL 账号密码。
- `-kafka-tls`：启用 TLS；`-kafka-insecure-skip-verify`：跳过证书校验（不推荐）。

**数据库回放语义**
- `-kafka-dsn`：**不带库名**的 DSN，例如 `...?parseTime=true&multiStatements=true`。
- `-kafka-default-db`：当消息 `db` 为空时，首次建连所用的默认库。
- **保证**：1 个 `id` = 1 条专属 `*sql.Conn`；同一 id 内串行执行，保持 `SET autocommit`、事务、会话变量与 `USE` 语义。

**时间与间隔**
- `-kafka-replay-with-gap`：若为 `true`，同一 id 内会按 `sleep = ts差值 - 上一条执行耗时` 进行休眠；为 `false` 则不休眠（吞吐更高）。
- `-kafka-ts-layout`, `-kafka-ts-loc`：`ts` 字段解析配置；当不开启 gap 时，程序会跳过解析提升性能。

**吞吐与背压**
- `-kafka-fetch-queue-cap`：全局 FIFO 容量（默认 10000）。
- `-kafka-fetch-batch`：每次批量抓取条数（默认 1000）。
- `-kafka-fetch-interval`：抓取周期（默认 100ms）。若 FIFO 满则等待。
- `-kafka-id-queue-cap`：**每个 id 的私有队列容量**（默认 1024）；用于限制“热点 id”的积压量，避免拖慢全局分发。

**位点提交（批量）**
- `-kafka-commit-every`：每分区累计 N 条就提交一次（默认 1000）。
- `-kafka-commit-interval`：至少每 T 时间也提交一次（默认 200ms）。

**其它**
- `-kafka-idle-ttl`：空闲回收时长（默认 1h）。
- `-kafka-stats-interval`：统计输出间隔（默认 10s）。
- `-kafka-error-log-dir`：错误 SQL 日志目录（逐行追加，包含分区/offset/id/错误/SQL）。
- `-kafka-stop-on-error`：遇到第一条错误是否直接退出（默认 false）。

### 统计输出说明

```
[stats] up=10s parts=3 active_ids=60 total_conns=60 active_conns=60
```
- `parts`：当前活跃分区数。
- `active_ids`：活跃 id 数（= 专属连接数）。
- `total_conns`：与 active_ids 相同（严格 1 id = 1 连接）。
- `active_conns`：正在执行或队列非空的连接数。

### 最佳实践

**高吞吐优先**
- 关闭时间间隔复现：`-kafka-replay-with-gap=false`
- 拉取与提交对齐：`-kafka-fetch-batch=1000`、`-kafka-commit-every=1000`
- 合理设置全局 FIFO 与 id 私有队列：`-kafka-fetch-queue-cap=10000`，`-kafka-id-queue-cap=2048~4096`

**应对热点 id**
- 如果大量消息集中在单个 id，可能导致其私有队列打满并阻塞全局分发（设计上的背压）。  
  建议提高 `-kafka-id-queue-cap` 或适度降低 `-kafka-fetch-batch`。

**数据库参数（仅限回放/压测环境）**
- 可考虑放宽持久性以提升吞吐：`innodb_flush_log_at_trx_commit=2`, `sync_binlog=0`，增大 `innodb_log_file_size`，提高 `max_connections` 等。生产环境请谨慎。

**SASL/TLS**
- 机制与集群配置需一致；错误的 SASL 设置会导致认证失败。
- 生产环境建议开启 TLS，并正确配置 CA/证书。

### 常见问题

- 提交 offset 报 `Unknown Topic Or Partition`  
  已在当前代码中修复（提交时带上 Topic）。仍报错请检查 topic/group/ACL。
- 关闭 gap 后 CPU 仍高  
  当前版本在 `-kafka-replay-with-gap=false` 时会跳过 `ts` 解析；如仍高，检查 JSON 大小、抓取批量与全局 FIFO 是否过大导致 GC 压力。
- 回放 QPS 低于 Kafka 生产速率  
  并发上限≈**活跃 id 数**。要提升 QPS：增加活跃 id、优化数据库延迟、并合理设置抓取/提交批量。

### 示例：SASL + TLS

```bash
./sql-replay -mode replay-kafka \
  -kafka-brokers 'k1:9093,k2:9093' \
  -kafka-topic tx_sql -kafka-group replayer-g1 -kafka-start auto \
  -kafka-dsn 'user:pass@tcp(10.0.0.10:4000)/?parseTime=true&multiStatements=true' \
  -kafka-default-db test1 \
  -kafka-sasl-mech scram-sha512 -kafka-username alice -kafka-password 'secret' \
  -kafka-tls=true -kafka-insecure-skip-verify=false \
  -kafka-fetch-queue-cap 10000 -kafka-fetch-batch 1000 -kafka-fetch-interval 100ms \
  -kafka-commit-every 1000 -kafka-commit-interval 200ms
```