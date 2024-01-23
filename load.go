package main

import (
    "database/sql"
    "encoding/json"
//    "flag"
    "fmt"
    "io/ioutil"
    _ "github.com/go-sql-driver/mysql"
        "github.com/pingcap/tidb/pkg/parser" // 确保已经正确导入 TiDB 包
    "path/filepath"
    "strings"
)

type SQLExecutionRecord3 struct {
    SQL           string `json:"sql"`
    QueryTime     int64  `json:"query_time"`
    RowsSent      int    `json:"rows_sent"`
    ExecutionTime int64  `json:"execution_time"`
    RowsReturned  int64  `json:"rows_returned"`
    ErrorInfo     string `json:"error_info,omitempty"`
    FileName      string // 文件名
}


func processFiles(out_dir, replay_name, tableName string, db *sql.DB) error {
    filePaths, err := filepath.Glob(filepath.Join(out_dir, replay_name+"*"))
    if err != nil {
        return err
    }

    for _, filePath := range filePaths {
        fileName := filepath.Base(filePath)
        err := processFile(filePath, fileName, tableName, db)
        if err != nil {
            return err
        }
    }

    return nil
}

func processFile(filePath, fileName, tableName string, db *sql.DB) error {
    fileContent, err := ioutil.ReadFile(filePath)
    if err != nil {
        return err
    }

    lines := strings.Split(string(fileContent), "\n")
    batchSize := 1000 // 每 1000 行数据进行一次批量插入

    for i := 0; i < len(lines); i += batchSize {
        end := i + batchSize
        if end > len(lines) {
            end = len(lines)
        }

        err := insertBatch(lines[i:end], fileName, tableName, db)
        if err != nil {
            return err
        }
    }

    return nil
}

func insertBatch(lines []string, fileName, tableName string, db *sql.DB) error {
    valueStrings := make([]string, 0, len(lines))
    valueArgs := make([]interface{}, 0, len(lines)*9) // 9 是字段数量

    for _, line := range lines {
        if line == "" {
            continue
        }
        var record SQLExecutionRecord3
        err := json.Unmarshal([]byte(line), &record)
        if err != nil {
            return err
        }

        normalizedSQL := parser.Normalize(record.SQL)
        digest := parser.DigestNormalized(normalizedSQL).String()
        words := strings.Fields(normalizedSQL)
        sqlType := "other"
        if len(words) > 0 {
            sqlType = words[0]
        }

        valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
        valueArgs = append(valueArgs, record.SQL, sqlType, digest, record.QueryTime, record.RowsSent, record.ExecutionTime, record.RowsReturned, record.ErrorInfo, fileName)
    }

    if len(valueStrings) == 0 {
        return nil // 没有数据要插入
    }

    stmt := fmt.Sprintf("INSERT INTO %s (sql_text, sql_type, sql_digest, query_time, rows_sent, execution_time, rows_returned, error_info, file_name) VALUES %s",
        tableName, strings.Join(valueStrings, ","))
    _, err := db.Exec(stmt, valueArgs...)
    return err
}

func LoadData(dbConnStr, outDir, replayOut, tableName string)  {
    if dbConnStr == "" || outDir == "" || replayOut == "" || tableName == "" {
        fmt.Println("Usage: ./sql-replay -mode load -db <DB_CONN_STRING> -out-dir <DIRECTORY> -replay-name <REPORT_OUT_FILE_NAME> -table <replay_info>")
        return
    }

    db, err := sql.Open("mysql", dbConnStr)
    if err != nil {
        fmt.Println("Error connecting to database:", err)
        return
    }
    defer db.Close()

    err = processFiles(outDir, replayOut, tableName, db)
    if err != nil {
        fmt.Println("Error processing files:", err)
    }
}
