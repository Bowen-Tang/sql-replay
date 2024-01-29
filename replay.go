package main

import (
    "bufio"
    "database/sql"
    "encoding/json"
    "fmt"
    "os"
    "sync"
    "time"
//    "flag"
    _ "github.com/go-sql-driver/mysql"
)

type SQLExecutionRecord2 struct {
    SQL           string `json:"sql"`
    QueryTime     int64  `json:"query_time"`
    RowsSent      int    `json:"rows_sent"`
    ExecutionTime int64  `json:"execution_time"`
    RowsReturned  int64  `json:"rows_returned"`
    ErrorInfo     string `json:"error_info,omitempty"`
}

type LogEntry2 struct {
    ConnectionID string `json:"connection_id"`
    QueryTime    int64  `json:"query_time"`
    SQL          string `json:"sql"`
    RowsSent     int    `json:"rows_sent"`
    Username     string `json:"username"`  // 新增字段 username
    SQLType      string `json:"sql_type"`  // 新增字段 sql_type
    DBName       string `json:"dbname"`    // 新增字段 dbname
    Ts           float64 `json:"ts"` // 新增字段，以秒为单位，包含6位精度
}

// SQLTask 代表 SQL 执行任务
type SQLTask struct {
    Entry LogEntry2
    DB    *sql.DB
}

func ExecuteSQLAndRecord(task SQLTask, basereplayOutputFilePath string) error {
    if task.DB == nil {
        return fmt.Errorf("database connection is nil")
    }
    startTime := time.Now()

    rows, err := task.DB.Query(task.Entry.SQL)
    var rowsReturned int64
    var errorInfo string

    if err != nil {
        errorInfo = err.Error()
    } else {
        for rows.Next() {
            rowsReturned++
        }
        rows.Close()
    }

    executionTime := time.Since(startTime).Microseconds()

    record := SQLExecutionRecord2{
        SQL:           task.Entry.SQL,
        QueryTime:     task.Entry.QueryTime,
        RowsSent:      task.Entry.RowsSent,
        ExecutionTime: executionTime,
        RowsReturned:  rowsReturned,
        ErrorInfo:     errorInfo,
    }

    jsonData, err := json.Marshal(record)
    if err != nil {
        return err
    }

    replayOutputFilePath := fmt.Sprintf("%s.%s", basereplayOutputFilePath, task.Entry.ConnectionID)
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

func ReplaySQL(dbConnStr string, Speed float64,slowOutputPath, replayOutputFilePath, filterUsername, filterSQLType, filterDBName string) {
    if dbConnStr == "" || slowOutputPath == "" || replayOutputFilePath == "" {
        fmt.Println("Usage: ./sql-replay -mode replay -db <mysql_connection_string> -speed 1.0 -slow-out <slow_output_file> -replay-out <replay_output_file> -username <all|username> -sqltype <all|select> -dbname <all|dbname>")
        return
    }
    // 检查 Speed 参数是否为正数
    if Speed <= 0 {
        fmt.Println("Invalid replay speed. The speed must be a positive number.")
        return
    }

    fmt.Println("回放目标用户：", filterUsername," 回放目标数据库：",filterDBName," 回放 SQL 范围: ",filterSQLType," 回放速度: ",Speed)

    ts0 := time.Now() // 程序开始时记录时间
    fmt.Println("参数读取成功，开始解析数据:", ts0)

    inputFile, err := os.Open(slowOutputPath)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer inputFile.Close()

    var wg sync.WaitGroup
    scanner := bufio.NewScanner(inputFile)
    buf := make([]byte, 0, 512*1024*1024) // 512MB的缓冲区
    scanner.Buffer(buf, bufio.MaxScanTokenSize)

    tasksMap := make(map[string][]LogEntry2)

    for scanner.Scan() {
        var entry LogEntry2
        if err := json.Unmarshal([]byte(scanner.Text()), &entry); err != nil {
            fmt.Println("Error parsing log entry:", err)
            continue
        }

        if filterUsername != "all" && entry.Username != filterUsername {
            continue
        }

        if filterSQLType != "all" && entry.SQLType != filterSQLType {
            continue
        }
// 新增 dbname
        if filterDBName != "all" && entry.DBName != filterDBName {
            continue
        }

        tasksMap[entry.ConnectionID] = append(tasksMap[entry.ConnectionID], entry)
    }

    ts1 := time.Now() // map 构建完成时间
    fmt.Println("完成数据解析: ", ts1)
    fmt.Printf("数据解析耗时: %v\n", ts1.Sub(ts0)) // 打印 ts1-ts0 时间
    fmt.Println("开始 SQL 回放")

    // 为每个 ConnectionID 创建 goroutine
    for connID, entries := range tasksMap {
        wg.Add(1)
        go func(connID string, entries []LogEntry2) {
            defer wg.Done()

            db, err := sql.Open("mysql", dbConnStr)
            if err != nil {
                fmt.Println("Error opening database for", connID, ":", err)
                return
            }
            defer db.Close()

        var prevTs float64 = 0 // 初始化前一个时间戳为 0

            if err := db.Ping(); err != nil {
                fmt.Println("Database connection failed for", connID, ":", err)
                return
            }

            for _, entry := range entries {
                // 如果不是第一条记录，计算时间间隔并等待
                if prevTs != 0 {
                    interval := entry.Ts - prevTs // 计算时间间隔
                    sleepDuration := time.Duration(interval * float64(time.Second) / Speed)
                    time.Sleep(sleepDuration)
                }
                prevTs = entry.Ts // 更新前一个时间戳
                task := SQLTask{Entry: entry, DB: db}
                if err := ExecuteSQLAndRecord(task, replayOutputFilePath); err != nil {
                    fmt.Println("Error executing SQL for", connID, ":", err)
                }
            }
        }(connID, entries)
    }

    wg.Wait()
    ts2 := time.Now() // 回放完成时间
    fmt.Println("SQL 回放完成:", ts2)
    fmt.Printf("SQL 回放时间: %v\n", ts2.Sub(ts1)) // 打印 ts2-ts1 时间
}
