**[中文](https://github.com/Bowen-Tang/sql-replay/blob/main/README.md) | [English](https://github.com/Bowen-Tang/sql-replay/blob/main/README_EN.md)**
# 功能介绍
![image](docs/diagrams/traffic_replay_flow.png)



## 适用场景
1. 版本升级兼容性及性能评估
2. 系统迁移兼容性及性能评估

## 支持的源端数据库
1. MySQL 5.6, 5.7, 8.0
2. Auroa MySQL 5.7/8.0
3. 云上 MySQL RDS

支持的日志格式示例：
```
# Time: 2024-01-19T16:29:48.141142Z
# User@Host: t1[t1] @  [10.2.103.21]  Id:   797
# Query_time: 0.000038  Lock_time: 0.000000 Rows_sent: 1  Rows_examined: 1
SET timestamp=1705681788;
SELECT c FROM sbtest1 WHERE id=250438;
```

```
# Time: 240119 16:29:48
# User@Host: t1[t1] @  [10.2.103.21]  Id:   797
# Query_time: 0.000038  Lock_time: 0.000000 Rows_sent: 1  Rows_examined: 1
SET timestamp=1705681788;
SELECT c FROM sbtest1 WHERE id=250438;
```

```
# Time: 231106  0:06:36
# User@Host: coplo2o[coplo2o] @  [10.0.2.34]  Id: 45827727
# Query_time: 1.066695  Lock_time: 0.000042 Rows_sent: 1  Rows_examined: 7039 Thread_id: 45827727 Schema: db Errno: 0 Killed: 0 Bytes_received: 0 Bytes_sent: 165 Read_first
: 0 Read_last: 0 Read_key: 1 Read_next: 7039 Read_prev: 0 Read_rnd: 0 Read_rnd_next: 0 Sort_merge_passes: 0 Sort_range_count: 0 Sort_rows: 0 Sort_scan_count: 0 Created_tmp_
disk_tables: 0 Created_tmp_tables: 0 Start: 2023-11-06T00:06:35.589701 End: 2023-11-06T00:06:36.656396 Launch_time: 0.000000
# QC_Hit: No  Full_scan: No  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No  Filesort: No  Filesort_on_disk: No
use db;
SET timestamp=1699200395;
SELECT c FROM sbtest1 WHERE id=250438;
```

## parse 部分
读取 MySQL 慢查询日志，去掉 MySQL 中自动生成的 set timestamp=xx/# Administor/-- 等无效 SQL，生成一个可以格式化的 json 文件，用于回放
## replay 部分
1. 读取 parse 阶段（或抓包工具 tshark 经 parse-tshark -mode parse2file）生成的格式化 json 文件，可过滤上游数据库用户、上游 SQL 类型（all、select）、上游数据库名（仅支持抓包工具采集的日志）来进行回放
2. 根据 connection id 并行，相同 connection id 的 SQL 串行
3. 将回放结果输出成 json 文件（按照 connection id 区分）
## load 部分
1. 解析 replay 生成的 json 文件，使用 TiDB Parse 模块对 SQL 进行格式化，并生成指纹（sql digest）
2. 将解析出来的信息写入数据库的 replay_info 表中
## report 部分
对回放结果进行分析，生成回放报告（含响应时间对比、错误信息）

# 操作示例 
## 下载并解压 
```
mkdir replay && cd replay && wget https://github.com/Bowen-Tang/sql-replay/releases/download/v0.3.3/v0.3.3.zip
unzip v0.3.3.zip
```
 
## 1. 解析慢查询日志
```
./sql-replay -mode parse -slow-in /opt/slow.log -slow-out /opt/slow.format
```
说明：/opt/slow.log 为慢查询日志路径，slow.format 则为输出的格式化文件

## 2. 连接目标库回放

```
mkdir out # 用于存储回放结果
# 回放所有用户、所有 SQL
./sql-replay -mode replay -db 'user:password@tcp(ip:port)/db' -speed 1.0 -slow-out /opt/slow.format -replay-out ./out/sb1_all -username all -sqltype all -dbname all -lang en
# 回放所有用户、select 语句
./sql-replay -mode replay -db 'user:password@tcp(ip:port)/db' -speed 1.0 -slow-out /opt/slow.format -replay-out ./out/sb1_select -username all -sqltype select -dbname db1 -lang zh
```
说明：

1. out 为回放结果存储目录**（可更换为其他目录，需手动创建）**，sb1_all/sb1_select 为回放任务名称;speed 为回放速度，当慢查询周期很长但语句很少时建议增大回放速度，当需要模拟更大压力时，建议增大回放速度
2. 'user:password@tcp(ip:port)/db' 中的 db 指的是用于回放的目标库

## 3. 导入回放结果到数据库
**连接目标库，创建表结构**
```
CREATE TABLE `test`.`replay_info` (
`sql_text` longtext DEFAULT NULL,
`sql_type` varchar(16) DEFAULT NULL,
`sql_digest` varchar(64) DEFAULT NULL,
`query_time` bigint(20) DEFAULT NULL,
`rows_sent` bigint(20) DEFAULT NULL,
`execution_time` bigint(20) DEFAULT NULL,
`rows_returned` bigint(20) DEFAULT NULL,
`error_info` text DEFAULT NULL,
`file_name` varchar(64) DEFAULT NULL
);
```
**导入数据**
```
# 导入回放任务 sb1_all 的回放数据 
./sql-replay -mode load -db 'user:password@tcp(ip:port)/db' -out-dir ./out -replay-name sb1_all -table replay_info 
# 导入回放任务 sb1_select 的回放数据 
./sql-replay -mode load -db 'user:password@tcp(ip:port)/db' -out-dir ./out -replay-name sb1_select -table replay_info 
```
说明：-out-dir 为回放结果存储目录，-replay-name 回放任务名称，table 为写入结果表

## 4. 生成报告

```
./sql-replay -mode report -db 'user:password@tcp(ip:port)/db' -replay-name slow1 -port ':8081'
```
说明：执行完可访问 IP:PORT 访问报告内容

# 报告示例 

![image](https://github.com/Bowen-Tang/sql-replay/assets/52245161/c72dcbea-ad39-4ade-ad09-24dd163b913a)
Replay Summary 中，记录了 SQL 总耗时对比、快的 SQL 条数、慢的 SQL 条数、错误的 SQL 条数

![image](https://github.com/Bowen-Tang/sql-replay/assets/52245161/6f027083-88ff-49a3-a6fc-f7bf952f9f6f)
Sql Error Info 中，根据 sql_digest 以及 error_info（前 10 位）排序




说明：
1. 为方便展示，将文本内容使用 ... 进行了省略，但依旧可以通过双击单元格选择内容后复制完整内容；另外 sample_sql_text 支持预览



# 倍速回放效果（1 倍（默认），10 倍，50 倍）
![image](https://github.com/Bowen-Tang/sql-replay/assets/52245161/8ebbee92-586d-4090-97d6-9e2c87c640bc)

# 回放基本按照原始顺序回放
回放文件内容

![image](https://github.com/Bowen-Tang/sql-replay/assets/52245161/e87e84fd-7318-41d0-8356-ddce5c744e2d)

数据库记录的 SQL 执行顺序

![image](https://github.com/Bowen-Tang/sql-replay/assets/52245161/7d9d7f84-80a1-44d6-b3d5-8f27b933ffcb)



# 编译安装方法

1. 安装 golang 1.20 及以上
2. 下载项目

```
git clone https://github.com/Bowen-Tang/sql-replay
```

4. 编译 sql-replay

```
    cd sql-replay
    go mod tidy
    go build
```
# 回放建议
1. 当数据库中就一个 database，一个 user 时，使用 -username all -dbname all 来回放
2. 当数据库中有多个 database、多个 user 时，建议启动多个 sql-replay 进程并行回放（否则将出现大量 SQL 报错），每个进程对应不同的 -username 和 -dbname（注意 -db 中的用户名、数据库名也需保持一致）

# 已知问题
1. 通过慢查询回放时，由于日志中没有记录 database 信息，所以在 replay 时，只能指定 -db all，或者不指定，否则不会进行回放（如果想要在慢查询回放时过滤库，可以通过指定 -username 以及 -db 中的用户名和数据库名的形式来完成对应库的回放）
2. insert into ... (),(),(),() 数十万行的 SQL 回放时，有可能会导致程序崩溃
3. 抓包回放的 SQL 中，如果是预编译 ? 占位符类型时，回放时这部分 SQL 会执行报错
4. SQL 回放顺序并不完全与真实执行顺序相等
5. MySQL 慢查询日志中记录的执行时间可能比真实时间慢（如 select sleep(10)，并不会记录为 10 秒，如 MySQL 5.7 中并不包含等锁时间等）
6. 云上 RDS 的慢查询日志格式不尽相同，可能不支持；暂不支持 MariaDB，当前无法获取 connection_id，后续加上
7. 当 connection_id 值过多（>4096）时，进行回放时会遇到 too many open files 错误，临时解决办法：回放前 ulimit -n 1000000
