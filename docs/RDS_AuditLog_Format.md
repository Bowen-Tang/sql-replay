**HW Cloud RDS 审计日志内容示例**
```
"21596830293", "93726418", "0", "Query", "2024-09-04T00:57:27 UTC", "select", "SELECT pushcode FROM els WHERE els.pn = 'staff500' AND els.ut = '20' ORDER BY els.ct DESC limit 0,1", "root[root] @  [10.3.2.22]", "", "", "10.3.2.22", "portal"
"21596830294", "93594738", "0", "Query", "2024-09-04T00:57:27 UTC", "set_option", "SET autocommit=0", "root[root] @  [10.3.2.34]", "", "", "10.3.2.34", "portal"
"21596830295", "93789840", "0", "Query", "2024-09-04T00:57:27 UTC", "select", "select @@session.tx_read_only", "root[root] @  [10.3.2.21]", "", "", "10.3.2.21", "portal"
```

**HW Cloud RDS 审计日志存储方式**

按 100MB 一个文件切割，按序号存储，如 1~9

**脚本1：format.sh**

```
#!/bin/bash

# 检查参数数量
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 input_file output_file"
    exit 1
fi

input_file="$1"  # 输入文件名
output_file="$2" # 输出文件名

# 初始化变量
rows=''

# 逐行读取输入文件
while IFS= read -r line
do
    # 处理新行
    row="$line"

    # 判断当前行是否以 '"2159' 开头
    if [[ "${row:0:12}" =~ ^\"[0-9]{11} ]]; then
        # 将之前的内容写入到输出文件中
        if [ -n "$rows" ]; then
            echo "${rows}" >> "$output_file"
        fi
        # 重置 rows 变量
        rows="$row"
    else
        # 追加到 rows 变量中
        rows="${rows} ${row}"
    fi
done < "$input_file"

# 处理最后一段数据
if [ -n "$rows" ]; then
    echo -e "${rows}" >> "$output_file"
fi
```

**运行格式化**

```for i in `seq 1 9`; do sh ../format.sh ./$i ./for/$i; done```

**python 脚本（用于格式化成 sql-replay 可回放的文件）**

```
# -*- coding: utf-8 -*-

import json
import time

def process_file(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as csv_file, open(output_file, 'a', encoding='utf-8') as json_file:
        for line in csv_file:
            # 使用 '", "' 作为分隔符手动分割行数据
            row = line.strip().split('", "')

            # 处理提取的字段
            if len(row) < 7:
                continue  # 跳过不完整的行

            # 去除字段开始和结束的引号
            connection_id = row[1].strip('"')
            ts = row[4].strip('"')
            sql_type = row[5].strip('"')
            sql = row[6].strip('"')
            jd = ".000001"
            dbname = "portal"

            # 去掉时区部分
            ts = ts.replace(" UTC", "")

            # 处理时间戳为浮点数，6 位精度
            try:
                ts_struct = time.strptime(ts, "%Y-%m-%dT%H:%M:%S")
                timestamp = time.mktime(ts_struct)
                timestamp = round(timestamp, 6)
            except ValueError:
                timestamp = 0.0

            # 创建 JSON 对象
            log_entry = {
                "connection_id": connection_id,
                "query_time": 100,
                "sql": sql,
                "rows_sent": 0,
                "username": "t1",
                "sql_type": sql_type,
                "dbname": dbname,
                "ts": timestamp  # 不带引号的浮点数
            }

            # 写入 JSON 文件
            json.dump(log_entry, json_file)
            json_file.write('\n')

# 处理多个文件
for i in range(1, 10):
    input_file = f"{i}"
    output_file = "db1.log"
    process_file(input_file, output_file)
```

