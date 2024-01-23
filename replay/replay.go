package main

import (
    "bufio"
    "database/sql"
    "encoding/json"
    "fmt"
    "os"
    "sync"
    "time"
    "flag"
    _ "github.com/go-sql-driver/mysql"
)

type SQLExecutionRecord struct {
    SQL           string `json:"sql"`
    QueryTime     int64  `json:"query_time"`
    RowsSent      int    `json:"rows_sent"`
    ExecutionTime int64  `json:"execution_time"`
    RowsReturned  int64  `json:"rows_returned"`
    ErrorInfo     string `json:"error_info,omitempty"`
}

type LogEntry struct {
    ConnectionID string `json:"connection_id"`
    QueryTime    int64  `json:"query_time"`
    SQL          string `json:"sql"`
    RowsSent     int    `json:"rows_sent"`
    Username     string `json:"username"`  // 新增字段 username
    SQLType      string `json:"sql_type"`  // 新增字段 sql_type
}

// SQLTask 代表 SQL 执行任务
type SQLTask struct {
    Entry LogEntry
    DB    *sql.DB
}

func ExecuteSQLAndRecord(task SQLTask, baseOutputFilePath string) error {
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

    record := SQLExecutionRecord{
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

    outputFilePath := fmt.Sprintf("%s.%s", baseOutputFilePath, task.Entry.ConnectionID)
    file, err := os.OpenFile(outputFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
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

func main() {
    mysqlConnStr := flag.String("db", "", "MySQL connection string")
    inputFilePath := flag.String("input", "", "Path to input file")
    outputFilePath := flag.String("output", "", "Path to output file")
    filterUsername := flag.String("username", "all", "Username to filter (default 'all')")
    filterSQLType := flag.String("sqltype", "all", "SQL type to filter (default 'all')")

    flag.Parse()

    if *mysqlConnStr == "" || *inputFilePath == "" || *outputFilePath == "" {
        fmt.Println("Usage: ./replay_tool -db <mysql_connection_string> -input <input_file> -output <output_file> -username <username> -sqltype <sql_type>")
        return
    }

    inputFile, err := os.Open(*inputFilePath)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer inputFile.Close()

    var wg sync.WaitGroup
    scanner := bufio.NewScanner(inputFile)
    tasksMap := make(map[string][]LogEntry)

    for scanner.Scan() {
        var entry LogEntry
        if err := json.Unmarshal([]byte(scanner.Text()), &entry); err != nil {
            fmt.Println("Error parsing log entry:", err)
            continue
        }

        if *filterUsername != "all" && entry.Username != *filterUsername {
            continue
        }

        if *filterSQLType != "all" && entry.SQLType != *filterSQLType {
            continue
        }

        tasksMap[entry.ConnectionID] = append(tasksMap[entry.ConnectionID], entry)
    }

    // 为每个 ConnectionID 创建 goroutine
    for connID, entries := range tasksMap {
        wg.Add(1)
        go func(connID string, entries []LogEntry) {
            defer wg.Done()

            db, err := sql.Open("mysql", *mysqlConnStr)
            if err != nil {
                fmt.Println("Error opening database for", connID, ":", err)
                return
            }
            defer db.Close()

            if err := db.Ping(); err != nil {
                fmt.Println("Database connection failed for", connID, ":", err)
                return
            }

            for _, entry := range entries {
                task := SQLTask{Entry: entry, DB: db}
                if err := ExecuteSQLAndRecord(task, *outputFilePath); err != nil {
                    fmt.Println("Error executing SQL for", connID, ":", err)
                }
            }
        }(connID, entries)
    }

    wg.Wait()
}