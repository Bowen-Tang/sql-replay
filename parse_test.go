package main

import (
    "bufio"
    "encoding/json"
    "os"
    "strings"
    "testing"
)

func TestParseLogs(t *testing.T) {
    slowLogPath := "test_slow_log.txt"
    slowOutputPath := "test_output.json"

    // 创建测试输入文件
    input := `/usr/sbin/mysqld, Version: 5.7.44 (MySQL Community Server (GPL)). started with:
Tcp port: 3306  Unix socket: /var/lib/mysql/mysql.sock
Time                 Id Command    Argument
# Time: 2024-08-30T06:09:28.060156Z
# User@Host: t1[t1] @ localhost [127.0.0.1]  Id:     9
# Query_time: 0.000065  Lock_time: 0.000022 Rows_sent: 0  Rows_examined: 1
SET timestamp=1724998168;
UPDATE stock SET s_quantity = 86, s_ytd = s_ytd + 4, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 0 WHERE s_i_id = 52521 AND s_w_id = 3;
# Time: 2024-08-30T06:09:28.060206Z
# User@Host: t1[t1] @ localhost [127.0.0.1]  Id:     6
# Query_time: 0.000109  Lock_time: 0.000030 Rows_sent: 0  Rows_examined: 1
SET timestamp=1724998168;
UPDATE district SET d_next_o_id = 4172 + 1 WHERE d_id = 1 AND d_w_id = 6;
# Time: 2024-01-19T16:29:48.141142Z
# User@Host: t1[t1] @  [10.2.103.21]  Id:   797
# Query_time: 0.000038  Lock_time: 0.000000 Rows_sent: 1  Rows_examined: 1
SET timestamp=1705681788;
SELECT c FROM sbtest1 WHERE id=250438;
# Time: 240119 16:29:48
# User@Host: t1[t1] @  [10.2.103.21]  Id:   797
# Query_time: 0.000038  Lock_time: 0.000000 Rows_sent: 1  Rows_examined: 1
SET timestamp=1705681788;
SELECT c FROM sbtest1 WHERE id=250438;
# Time: 231106  0:06:36
# User@Host: coplo2o[coplo2o] @  [10.0.2.34]  Id: 45827727
# Query_time: 1.066695  Lock_time: 0.000042 Rows_sent: 1  Rows_examined: 7039 Thread_id: 45827727 Schema: db Errno: 0 Killed: 0 Bytes_received: 0 Bytes_sent: 165 Read_first: 0 Read_last: 0 Read_key: 1 Read_next: 7039 Read_prev: 0 Read_rnd: 0 Read_rnd_next: 0 Sort_merge_passes: 0 Sort_range_count: 0 Sort_rows: 0 Sort_scan_count: 0 Created_tmp_disk_tables: 0 Created_tmp_tables: 0 Start: 2023-11-06T00:06:35.589701 End: 2023-11-06T00:06:36.656396 Launch_time: 0.000000
# QC_Hit: No  Full_scan: No  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No  Filesort: No  Filesort_on_disk: No
use db;
SET timestamp=1699200395;
SELECT c FROM sbtest1 WHERE id=250438;`

    expectedOutput := []LogEntry{
        {
            ConnectionID: "9",
            QueryTime:    65,
            SQL:          "UPDATE stock SET s_quantity = 86, s_ytd = s_ytd + 4, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 0 WHERE s_i_id = 52521 AND s_w_id = 3;",
            RowsSent:     0,
            Username:     "t1",
            SQLType:      "UPDATE",
            DBName:       "",
            Timestamp:    1724998168.060156,
        },
        {
            ConnectionID: "6",
            QueryTime:    109,
            SQL:          "UPDATE district SET d_next_o_id = 4172 + 1 WHERE d_id = 1 AND d_w_id = 6;",
            RowsSent:     0,
            Username:     "t1",
            SQLType:      "UPDATE",
            DBName:       "",
            Timestamp:    1724998168.060206,
        },
        {
            ConnectionID: "797",
            QueryTime:    38,
            SQL:          "SELECT c FROM sbtest1 WHERE id=250438;",
            RowsSent:     1,
            Username:     "t1",
            SQLType:      "SELECT",
            DBName:       "",
            Timestamp:    1705681788.141142,
        },
        {
            ConnectionID: "797",
            QueryTime:    38,
            SQL:          "SELECT c FROM sbtest1 WHERE id=250438;",
            RowsSent:     1,
            Username:     "t1",
            SQLType:      "SELECT",
            DBName:       "",
            Timestamp:    1705681788,
        },
        {
            ConnectionID: "45827727",
            QueryTime:    1066695,
            SQL:          "SELECT c FROM sbtest1 WHERE id=250438;",
            RowsSent:     1,
            Username:     "coplo2o",
            SQLType:      "SELECT",
            DBName:       "",
            Timestamp:    1699229196,
        },
    }

    // 写入测试输入文件
    err := os.WriteFile(slowLogPath, []byte(input), 0644)
    if err != nil {
        t.Fatalf("Failed to write test input file: %v", err)
    }

    // 调用 ParseLogs 函数
    ParseLogs(slowLogPath, slowOutputPath)

    // 读取并解析输出文件
    outputFile, err := os.Open(slowOutputPath)
    if err != nil {
        t.Fatalf("Failed to open output file: %v", err)
    }
    defer outputFile.Close()

    scanner := bufio.NewScanner(outputFile)
    var actualOutput []LogEntry
    for scanner.Scan() {
        var entry LogEntry
        err := json.Unmarshal(scanner.Bytes(), &entry)
        if err != nil {
            t.Fatalf("Failed to unmarshal JSON: %v", err)
        }
        actualOutput = append(actualOutput, entry)
    }

    if err := scanner.Err(); err != nil {
        t.Fatalf("Error reading output file: %v", err)
    }

    // 比较实际输出和预期输出
    if len(actualOutput) != len(expectedOutput) {
        t.Fatalf("Output length does not match expected length.\nActual: %v\nExpected: %v", len(actualOutput), len(expectedOutput))
    }

    for i := range actualOutput {
        if actualOutput[i].ConnectionID != expectedOutput[i].ConnectionID ||
            actualOutput[i].QueryTime != expectedOutput[i].QueryTime ||
            actualOutput[i].SQL != expectedOutput[i].SQL ||
            actualOutput[i].RowsSent != expectedOutput[i].RowsSent ||
            actualOutput[i].Username != expectedOutput[i].Username ||
            !strings.EqualFold(actualOutput[i].SQLType, expectedOutput[i].SQLType) ||
            actualOutput[i].DBName != expectedOutput[i].DBName ||
            !floatEquals(actualOutput[i].Timestamp, expectedOutput[i].Timestamp) {
            t.Errorf("Output does not match expected output at index %d.\nActual: %v\nExpected: %v", i, actualOutput[i], expectedOutput[i])
        }
    }

    // 清理测试文件
    os.Remove(slowLogPath)
    os.Remove(slowOutputPath)
}

func floatEquals(a, b float64) bool {
    const epsilon = 1e-6
    return (a-b) < epsilon && (b-a) < epsilon
}
