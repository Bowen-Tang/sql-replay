# 原理介绍

1. parse_tool 读取慢查询日志，并输出成格式化的 json 内容
2. replay_tool 读取格式化后的 json 文件，指定目标库执行 SQL 并行回放（不同连接号并行，相同连接号串行），并将回放结果按连接号输出到不同文件中
3. load_tool 导入回放结果到数据库的表中
4. gen_report.py 用于分析回放结果，并生成报告

# 操作示例 
## 下载并解压 
```
mkdir replay && cd replay && wget https://github.com/Bowen-Tang/sql-replay/releases/download/master/tools.zip
unzip tools.zip && chmod +x *tool
```
 
## 1. 解析慢查询日志

```
./parse_tool -slow_in /opt/slow.log -slow_out slow.format
```
说明：/opt/slow.log 为慢查询日志路径，slow.format 则为输出的格式化文件

## 2. 连接目标库回放

```
mkdir out
./replay_tool -db "user:password@tcp(ip:port)/db_name" -slow_out ./slow.format -replay_out ./out/sb1  -username all -sqltype all
```
说明：out 为回放结果存储目录（可更换为其他目录，需手动创建），sb1 仅为标识本次回放的名称（无明确含义）;可指定源端用户回放；sqltype 支持设置为 select 以及 all


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
./load_tool -db "username:password@tcp(ip:port)/test" -out_dir out -replay_name sb1 -table test.replay_info

```
说明：-out_dir 读取回放结果存储目录 out，-replay_name 为步骤 2 中的 sb1，table 为 结果表

## 4. 生成报告

```
python3 ./gen_report.py --user xx --password xx --host xxxx --database test --port xx --outfile_prefix sb1 --tablename replay_info
```
说明：执行完成会输出 sb1.html，下载到本地查看

# 报告示例 
![image](https://github.com/Bowen-Tang/sql-replay/assets/52245161/cd480ac6-cee3-4b3d-996a-9f66a71a3a87)
![image](https://github.com/Bowen-Tang/sql-replay/assets/52245161/725e8ceb-df10-4004-bc05-54ec8a86abe8)


report/sb1.html



# 编译安装方法

1. 安装 golang 1.20 及以上
2. Python 3 环境： mysql-connector-python (8.0.29)、pandas (1.1.5)
3. 下载项目

```
git clone https://github.com/Bowen-Tang/sql-replay
```

4. 编译 parse_tool

```
    cd sql-replay/parse
    go mod init parse_tool
    go mod tidy
    go build
```

6. 编译 replay_tool

```
   cd ../replay
   go mod init replay_tool
   go mod tidy
   go build
```

7. 编译 load_tool

```
   cd ../load
   go mod init load_tool
   go mod tidy
   go build
```

# 改进计划
1. parse 部分增加 user 过滤以及 sql type 过滤功能
2. replay 时可设置最大执行时间
3. 报告生成使用 go
