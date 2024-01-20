原理介绍：
1. parse_tool 读取慢查询日志，并输出成格式化的 json 内容
2. replay_tool 读取格式化后的 json 文件，指定目标库执行 SQL 并行回放（不同连接号并行，相同连接号串行），并将回放结果按连接号输出到不同文件中
3. load_tool 导入回放结果到数据库的表中
4. gen_report.py 用于分析回放结果，并生成报告

操作示例：










编译方法：
1. 安装 golang 1.20 及以上
2. Python 3 环境：
   mysql-connector-python (8.0.29)
   pandas (1.1.5)

3. 下载项目
git clone https://github.com/Bowen-Tang/sql-replay

4. 编译 parse_tool
   cd sql_replay/parse
   go mod init parse_tool
   go mod tidy
   go build

5. 编译 replay_tool
   cd ../replay
   go mod init replay_tool
   go mod tidy
   go build

6. 编译 load_tool
   cd ../load
   go mod init load_tool
   go mod tidy
   go build
