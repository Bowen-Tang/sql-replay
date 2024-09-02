package main

import (
    "bufio"
    "encoding/json"
    "fmt"
    "os"
    "sync"
    "testing"
    "time"

    "bou.ke/monkey"
)

var executionOrder []string
var mu sync.Mutex

func generateReplayFiles(filePath string) error {
    entries := []LogEntry{
        {ConnectionID: "1", QueryTime: 100000, SQL: "SELECT * FROM table1;", RowsSent: 1, Username: "user1", SQLType: "SELECT", Timestamp: 1.0},
        {ConnectionID: "1", QueryTime: 100000, SQL: "UPDATE table1 SET col1 = 'value1';", RowsSent: 0, Username: "user1", SQLType: "UPDATE", Timestamp: 10.0},
        {ConnectionID: "2", QueryTime: 100000, SQL: "INSERT INTO table2 (col1) VALUES ('value2');", RowsSent: 0, Username: "user2", SQLType: "INSERT", Timestamp: 2.1},
        {ConnectionID: "2", QueryTime: 100000, SQL: "DELETE FROM table2 WHERE col1 = 'value2';", RowsSent: 0, Username: "user2", SQLType: "DELETE", Timestamp: 13.0},
        {ConnectionID: "3", QueryTime: 100000, SQL: "SELECT * FROM table3;", RowsSent: 1, Username: "user3", SQLType: "SELECT", Timestamp: 9.0},
        {ConnectionID: "3", QueryTime: 100000, SQL: "UPDATE table3 SET col1 = 'value3';", RowsSent: 0, Username: "user3", SQLType: "UPDATE", Timestamp: 14.0},
        {ConnectionID: "4", QueryTime: 100000, SQL: "INSERT INTO table4 (col1) VALUES ('value4');", RowsSent: 0, Username: "user4", SQLType: "INSERT", Timestamp: 2.0},
        {ConnectionID: "4", QueryTime: 100000, SQL: "DELETE FROM table4 WHERE col1 = 'value4';", RowsSent: 0, Username: "user4", SQLType: "DELETE", Timestamp: 20.0},
    }

    file, err := os.Create(filePath)
    if err != nil {
        return err
    }
    defer file.Close()

    for _, entry := range entries {
        jsonEntry, err := json.Marshal(entry)
        if err != nil {
            return err
        }
        file.Write(jsonEntry)
        file.WriteString("\n")
    }

    return nil
}

func mockExecuteSQLAndRecord(task SQLTask, baseReplayOutputFilePath string) error {
    // 模拟 SQL 执行时间
    time.Sleep(time.Duration(task.Entry.QueryTime) * time.Microsecond)

    // 打印当前时间戳，连接号，SQL
//    fmt.Printf("%s, 连接号: %s, SQL: %s\n", time.Now().Format("2006-01-02 15:04:05.000"), task.Entry.ConnectionID, task.Entry.SQL)

    // 记录执行顺序
    mu.Lock()
    executionOrder = append(executionOrder, task.Entry.ConnectionID)
    mu.Unlock()

    record := SQLExecutionRecord{
        SQL:           task.Entry.SQL,
        QueryTime:     task.Entry.QueryTime,
        RowsSent:      task.Entry.RowsSent,
        ExecutionTime: task.Entry.QueryTime,
        RowsReturned:  int64(task.Entry.RowsSent),
        ErrorInfo:     "",
    }

    jsonData, err := json.Marshal(record)
    if err != nil {
        return err
    }

    replayOutputFilePath := fmt.Sprintf("%s.%s", baseReplayOutputFilePath, task.Entry.ConnectionID)
    file, err := os.OpenFile(replayOutputFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
    if err != nil {
        return err
    }
    defer file.Close()

    _, err = file.Write(jsonData)
    if err != nil {
        return err
    }
    _, err = file.WriteString("\n")
    return err
}

func TestSQLReplay(t *testing.T) {
    replayFilePath := "test_replay.json"
    replayOutputFilePath := "test_replay_output"
    speed1 := 1.0
    speed100 := 100.0

    // 生成回放文件
    err := generateReplayFiles(replayFilePath)
    if err != nil {
        t.Fatalf("Failed to generate replay files: %v", err)
    }

    // 解析回放文件
    tasksMap, _, err := ParseLogEntries(replayFilePath, "all", "all", "all")
    if err != nil {
        t.Fatalf("Failed to parse replay files: %v", err)
    }

    // 清理之前的回放结果文件
    for connID := range tasksMap {
        os.Remove(fmt.Sprintf("%s.%s", replayOutputFilePath, connID))
    }

    // 使用 monkey 补丁替换 ExecuteSQLAndRecord 函数
    var patch = monkey.Patch(ExecuteSQLAndRecord, mockExecuteSQLAndRecord)
    defer patch.Unpatch()

    // 第一次回放，使用 1.0 倍速
    start1 := time.Now()
    StartSQLReplay("root1@tcp(127.0.0.1:4000)/test", speed1, replayFilePath, replayOutputFilePath, "all", "all", "all", "en")
    duration1 := time.Since(start1)

    // 验证 1.0 倍速回放时间
    if duration1 <= 19*time.Second {
        t.Errorf("1x speed replay did not complete in expected time: expected more than 19s, got %v", duration1)
    }

    // 验证 1.0 倍速回放的连接号启动顺序
    expectedOrder := []string{"1", "4", "2", "3", "1", "2", "3", "4"}
    if len(executionOrder) != len(expectedOrder) {
        t.Errorf("Execution order length does not match expected length.\nActual: %v\nExpected: %v", len(executionOrder), len(expectedOrder))
    } else {
        for i, connID := range expectedOrder {
            if executionOrder[i] != connID {
                t.Errorf("Execution order does not match at index %d.\nActual: %v\nExpected: %v", i, executionOrder[i], connID)
            }
        }
    }

    // 清理之前的回放结果文件
    for connID := range tasksMap {
        os.Remove(fmt.Sprintf("%s.%s", replayOutputFilePath, connID))
    }

    // 第二次回放，使用 100.0 倍速
    start2 := time.Now()
    StartSQLReplay("root1@tcp(127.0.0.1:4000)/test", speed100, replayFilePath, replayOutputFilePath, "all", "all", "all", "en")
    duration2 := time.Since(start2)

    // 验证 100.0 倍速回放时间
    if duration2 >= 1*time.Second {
        t.Errorf("100x speed replay did not complete in expected time: expected less than 1s, got %v", duration2)
    }

    // 验证回放结果
    expectedOutputs := map[string][]SQLExecutionRecord{
        "1": {
            {SQL: "SELECT * FROM table1;", QueryTime: 100000, RowsSent: 1, ExecutionTime: 100000, RowsReturned: 1, ErrorInfo: "", FileName: ""},
            {SQL: "UPDATE table1 SET col1 = 'value1';", QueryTime: 100000, RowsSent: 0, ExecutionTime: 100000, RowsReturned: 0, ErrorInfo: "", FileName: ""},
        },
        "2": {
            {SQL: "INSERT INTO table2 (col1) VALUES ('value2');", QueryTime: 100000, RowsSent: 0, ExecutionTime: 100000, RowsReturned: 0, ErrorInfo: "", FileName: ""},
            {SQL: "DELETE FROM table2 WHERE col1 = 'value2';", QueryTime: 100000, RowsSent: 0, ExecutionTime: 100000, RowsReturned: 0, ErrorInfo: "", FileName: ""},
        },
        "3": {
            {SQL: "SELECT * FROM table3;", QueryTime: 100000, RowsSent: 1, ExecutionTime: 100000, RowsReturned: 1, ErrorInfo: "", FileName: ""},
            {SQL: "UPDATE table3 SET col1 = 'value3';", QueryTime: 100000, RowsSent: 0, ExecutionTime: 100000, RowsReturned: 0, ErrorInfo: "", FileName: ""},
        },
        "4": {
            {SQL: "INSERT INTO table4 (col1) VALUES ('value4');", QueryTime: 100000, RowsSent: 0, ExecutionTime: 100000, RowsReturned: 0, ErrorInfo: "", FileName: ""},
            {SQL: "DELETE FROM table4 WHERE col1 = 'value4';", QueryTime: 100000, RowsSent: 0, ExecutionTime: 100000, RowsReturned: 0, ErrorInfo: "", FileName: ""},
        },
    }

    for connID, expectedOutput := range expectedOutputs {
        outputFilePath := fmt.Sprintf("%s.%s", replayOutputFilePath, connID)
        outputFile, err := os.Open(outputFilePath)
        if err != nil {
            t.Fatalf("Failed to open replay output file: %v", err)
        }
        defer outputFile.Close()

        scanner := bufio.NewScanner(outputFile)
        var actualOutput []SQLExecutionRecord
        for scanner.Scan() {
            var record SQLExecutionRecord
            err := json.Unmarshal(scanner.Bytes(), &record)
            if err != nil {
                t.Fatalf("Failed to unmarshal JSON: %v", err)
            }
            actualOutput = append(actualOutput, record)
        }

        if err := scanner.Err(); err != nil {
            t.Fatalf("Error reading replay output file: %v", err)
        }

        if len(actualOutput) != len(expectedOutput) {
            t.Fatalf("Output length does not match expected length for connection %s.\nActual: %v\nExpected: %v", connID, len(actualOutput), len(expectedOutput))
        }

        for i := range actualOutput {
            if actualOutput[i].SQL != expectedOutput[i].SQL ||
                actualOutput[i].QueryTime != expectedOutput[i].QueryTime ||
                actualOutput[i].RowsSent != expectedOutput[i].RowsSent ||
                actualOutput[i].ExecutionTime != expectedOutput[i].ExecutionTime ||
                actualOutput[i].RowsReturned != expectedOutput[i].RowsReturned ||
                actualOutput[i].ErrorInfo != expectedOutput[i].ErrorInfo {
                t.Errorf("Output does not match expected output at index %d for connection %s.\nActual: %v\nExpected: %v", i, connID, actualOutput[i], expectedOutput[i])
            }
        }
    }

    // 清理测试文件
    os.Remove(replayFilePath)
    for connID := range tasksMap {
        os.Remove(fmt.Sprintf("%s.%s", replayOutputFilePath, connID))
    }
}
