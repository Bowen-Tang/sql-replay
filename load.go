package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "path/filepath"
    "strings"

    _ "github.com/go-sql-driver/mysql"
    "github.com/pingcap/tidb/pkg/parser"
)

type SQLExecutionRecord3 struct {
    SQL           string `json:"sql"`
    QueryTime     int64  `json:"query_time"`
    RowsSent      int    `json:"rows_sent"`
    ExecutionTime int64  `json:"execution_time"`
    RowsReturned  int64  `json:"rows_returned"`
    ErrorInfo     string `json:"error_info,omitempty"`
    FileName      string // File name
}

const batchSize = 1000

func LoadData(dbConnStr, outDir, replayOut, tableName string) {
    if !validateInputs(dbConnStr, outDir, replayOut, tableName) {
        return
    }

    db, err := sql.Open("mysql", dbConnStr)
    if err != nil {
        fmt.Println("Error connecting to database:", err)
        return
    }
    defer db.Close()

    if err := processFiles(outDir, replayOut, tableName, db); err != nil {
        fmt.Println("Error processing files:", err)
    }
}

func validateInputs(dbConnStr, outDir, replayOut, tableName string) bool {
    if dbConnStr == "" || outDir == "" || replayOut == "" || tableName == "" {
        fmt.Println("Usage: ./sql-replay -mode load -db <DB_CONN_STRING> -out-dir <DIRECTORY> -replay-name <REPORT_OUT_FILE_NAME> -table <replay_info>")
        return false
    }
    return true
}

func processFiles(outDir, replayName, tableName string, db *sql.DB) error {
    filePaths, err := filepath.Glob(filepath.Join(outDir, replayName+"*"))
    if err != nil {
        return fmt.Errorf("error finding files: %w", err)
    }

    for _, filePath := range filePaths {
        fileName := filepath.Base(filePath)
        if err := processFile(filePath, fileName, tableName, db); err != nil {
            return fmt.Errorf("error processing file %s: %w", fileName, err)
        }
    }

    return nil
}

func processFile(filePath, fileName, tableName string, db *sql.DB) error {
    fileContent, err := ioutil.ReadFile(filePath)
    if err != nil {
        return fmt.Errorf("error reading file: %w", err)
    }

    lines := strings.Split(string(fileContent), "\n")
    for i := 0; i < len(lines); i += batchSize {
        end := min(i+batchSize, len(lines))
        if err := insertBatch(lines[i:end], fileName, tableName, db); err != nil {
            return fmt.Errorf("error inserting batch: %w", err)
        }
    }

    return nil
}

func insertBatch(lines []string, fileName, tableName string, db *sql.DB) error {
    records := parseRecords(lines)
    if len(records) == 0 {
        return nil // No data to insert
    }

    query, args := buildInsertQuery(records, fileName, tableName)
    _, err := db.Exec(query, args...)
    return err
}

func parseRecords(lines []string) []SQLExecutionRecord3 {
    var records []SQLExecutionRecord3
    for _, line := range lines {
        if line == "" {
            continue
        }
        var record SQLExecutionRecord3
        if err := json.Unmarshal([]byte(line), &record); err != nil {
            fmt.Printf("Error parsing JSON: %v\n", err)
            continue
        }
        records = append(records, record)
    }
    return records
}

func buildInsertQuery(records []SQLExecutionRecord3, fileName, tableName string) (string, []interface{}) {
    valueStrings := make([]string, 0, len(records))
    valueArgs := make([]interface{}, 0, len(records)*9)

    for _, record := range records {
        normalizedSQL := parser.Normalize(record.SQL)
        digest := parser.DigestNormalized(normalizedSQL).String()
        sqlType := getSQLType(normalizedSQL)

        valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
        valueArgs = append(valueArgs, record.SQL, sqlType, digest, record.QueryTime, record.RowsSent, record.ExecutionTime, record.RowsReturned, record.ErrorInfo, fileName)
    }

    query := fmt.Sprintf("INSERT INTO %s (sql_text, sql_type, sql_digest, query_time, rows_sent, execution_time, rows_returned, error_info, file_name) VALUES %s",
        tableName, strings.Join(valueStrings, ","))
    return query, valueArgs
}

func getSQLType(normalizedSQL string) string {
    words := strings.Fields(normalizedSQL)
    if len(words) > 0 {
        return strings.ToLower(words[0])
    }
    return "other"
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
