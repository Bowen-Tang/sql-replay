package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/parser"
)

const (
	batchSize = 1000
	workers   = 4
)

func LoadData(dbConnStr, outDir, replayOut, tableName string) {
	if !validateInputs(dbConnStr, outDir, replayOut, tableName) {
		return
	}

        fmt.Printf("load batchsize: %d, load workers: %d\n",batchSize,workers)

	db, err := sql.Open("mysql", dbConnStr)
	if err != nil {
		fmt.Println("connect to db failed:", err)
		return
	}
	defer db.Close()

        ts_create_table := time.Now()
        fmt.Printf("[%s] Begin create table - REPLAY_INFO\n", ts_create_table.Format("2006-01-02 15:04:05.000"))
	if err := createTableIfNotExists(db, tableName); err != nil {
		fmt.Println("create table failed:", err)
		return
	}

	if err := processFilesParallel(outDir, replayOut, tableName, db); err != nil {
		fmt.Println("process files failed:", err)
	}
}

func createTableIfNotExists(db *sql.DB, tableName string) error {
	createTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		sql_text longtext DEFAULT NULL,
		sql_type varchar(16) DEFAULT NULL,
		sql_digest varchar(64) DEFAULT NULL,
		query_time bigint(20) DEFAULT NULL,
		rows_sent bigint(20) DEFAULT NULL,
		execution_time bigint(20) DEFAULT NULL,
		rows_returned bigint(20) DEFAULT NULL,
		error_info text DEFAULT NULL,
		file_name varchar(64) NOT NULL,
		db_name varchar(64) DEFAULT NULL
	)`, tableName)

	_, err := db.Exec(createTableSQL)
	return err
}

func processFilesParallel(outDir, replayName, tableName string, db *sql.DB) error {
	filePaths, err := filepath.Glob(filepath.Join(outDir, replayName+"*"))
	if err != nil {
		return fmt.Errorf("find files failed: %w", err)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(filePaths))
	semaphore := make(chan struct{}, workers)

	for _, filePath := range filePaths {
		wg.Add(1)
		go func(fp string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			fileName := filepath.Base(fp)
			if err := processFile(fp, fileName, tableName, db); err != nil {
				errChan <- fmt.Errorf("process file %s failed: %w", fileName, err)
			} else {
				logCompletion(fileName)
			}
		}(filePath)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
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
		} else {
			logCompletion(fileName)
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

func parseRecords(lines []string) []SQLExecutionRecord {
	var records []SQLExecutionRecord
	for _, line := range lines {
		if line == "" {
			continue
		}
		var record SQLExecutionRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			fmt.Printf("Error parsing JSON: %v\n", err)
			continue
		}
		records = append(records, record)
	}
	return records
}

func buildInsertQuery(records []SQLExecutionRecord, fileName, tableName string) (string, []interface{}) {
	valueStrings := make([]string, 0, len(records))
	valueArgs := make([]interface{}, 0, len(records)*9)

	for _, record := range records {
		normalizedSQL := parser.Normalize(record.SQL)
		digest := parser.DigestNormalized(normalizedSQL).String()
		sqlType := getSQLType(normalizedSQL)

		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs, record.SQL, sqlType, digest, record.QueryTime, record.RowsSent, record.ExecutionTime, record.RowsReturned, record.ErrorInfo, fileName, record.DBName)
	}

	query := fmt.Sprintf("INSERT INTO %s (sql_text, sql_type, sql_digest, query_time, rows_sent, execution_time, rows_returned, error_info, file_name, db_name) VALUES %s",
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

func logCompletion(fileName string) {
	currentTime := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Printf("[%s] Completed processing file: %s\n", currentTime, fileName)
}
