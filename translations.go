package main

var translations = map[string]map[string]string{
    "en": {
        "usage": "Usage: ./sql-replay -mode replay -db <mysql_connection_string> -speed 1.0 -slow-out <slow_output_file> -replay-out <replay_output_file> -username <all|username> -sqltype <all|select> -dbname <all|dbname> -lang <language_code>",
        "invalid_speed": "Invalid replay speed. The speed must be a positive number.",
        "replay_info": "Filter Rule: Source user - %s, Source database - %s, Source SQL type - %s\nReplay speed: %f",
        "parsing_start": "Parameters read successfully, starting data parsing:",
        "file_open_error": "Error opening file:",
        "parsing_complete": "Data parsing completed:",
        "parsing_time": "Data parsing time:",
        "replay_start": "Starting SQL replay",
        "db_open_error": "Error opening database for %s:",
        "sql_exec_error": "Error executing SQL for %s:",
        "replay_complete": "SQL replay completed:",
        "replay_time": "SQL replay time:",
    },
    "zh": {
        "usage": "用法: ./sql-replay -mode replay -db <mysql连接字符串> -speed 1.0 -slow-out <慢查询输出文件> -replay-out <回放输出文件> -username <all|用户名> -sqltype <all|select> -dbname <all|数据库名> -lang <语言代码>",
        "invalid_speed": "无效的回放速度。速度必须是正数。",
        "replay_info": "过滤规则：源端用户 - %s，源端数据库 - %s，源端 SQL 类型 - %s\n回放速度: %f",
        "parsing_start": "参数读取成功，开始解析数据:",
        "file_open_error": "打开文件错误:",
        "parsing_complete": "完成数据解析:",
        "parsing_time": "数据解析耗时:",
        "replay_start": "开始 SQL 回放",
        "db_open_error": "为 %s 打开数据库时出错:",
        "sql_exec_error": "执行 %s 的 SQL 时出错:",
        "replay_complete": "SQL 回放完成:",
        "replay_time": "SQL 回放时间:",
    },
}
