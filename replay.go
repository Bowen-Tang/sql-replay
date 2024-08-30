package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type SQLExecutionRecord struct {
    SQL           string `json:"sql"`
    QueryTime     int64  `json:"query_time"`
    RowsSent      int    `json:"rows_sent"`
    ExecutionTime int64  `json:"execution_time"`
    RowsReturned  int64  `json:"rows_returned"`
    ErrorInfo     string `json:"error_info,omitempty"`
    FileName      string // File name
}

type LogEntry struct {
	ConnectionID string  `json:"connection_id"`
	QueryTime    int64   `json:"query_time"`
	SQL          string  `json:"sql"`
	RowsSent     int     `json:"rows_sent"`
	Username     string  `json:"username"`
	SQLType      string  `json:"sql_type"`
	DBName       string  `json:"dbname"`
	Timestamp    float64 `json:"ts"`
}

type SQLTask struct {
	Entry LogEntry
	DB    *sql.DB
}

var i18n *I18n

func init() {
	var err error
	i18n, err = NewI18n("en")
	if err != nil {
		panic(err)
	}
}

func ExecuteSQLAndRecord(task SQLTask, baseReplayOutputFilePath string) error {
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

func ParseLogEntries(slowOutputPath, filterUsername, filterSQLType, filterDBName string) (map[string][]LogEntry, float64, error) {
	inputFile, err := os.Open(slowOutputPath)
	if err != nil {
		return nil, 0, fmt.Errorf("file open error: %w", err)
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	buf := make([]byte, 0, 512*1024*1024) // 512MB buffer
	scanner.Buffer(buf, bufio.MaxScanTokenSize)

	tasksMap := make(map[string][]LogEntry)
	var minTimestamp float64 = 9999999999.999999

	for scanner.Scan() {
		var entry LogEntry
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

		if filterDBName != "all" && entry.DBName != filterDBName {
			continue
		}
		tasksMap[entry.ConnectionID] = append(tasksMap[entry.ConnectionID], entry)

		if entry.Timestamp < minTimestamp {
			minTimestamp = entry.Timestamp
		}
	}

	return tasksMap, minTimestamp, nil
}

func ReplaySQLForConnection(connID string, entries []LogEntry, dbConnStr string, replayOutputFilePath string, minTimestamp float64, speed float64, lang string) {
	db, err := sql.Open("mysql", dbConnStr)
	if err != nil {
		fmt.Printf(i18n.T(lang, "db_open_error")+"\n", connID, err)
		return
	}
	defer db.Close()

	var prevTimestamp float64 = entries[0].Timestamp - (entries[0].Timestamp - minTimestamp)
	var lastQueryTime int64 = 0

	for _, entry := range entries {
		interval := (entry.Timestamp - prevTimestamp - float64(lastQueryTime)/1e6) / speed
		if interval > 0 {
			sleepDuration := time.Duration(interval * float64(time.Second))
			time.Sleep(sleepDuration)
		}
		prevTimestamp = entry.Timestamp

		task := SQLTask{Entry: entry, DB: db}
		if err := ExecuteSQLAndRecord(task, replayOutputFilePath); err != nil {
			fmt.Printf(i18n.T(lang, "sql_exec_error")+"\n", connID, err)
		}
		lastQueryTime = entry.QueryTime
	}
}

func StartSQLReplay(dbConnStr string, speed float64, slowOutputPath, replayOutputFilePath, filterUsername, filterSQLType, filterDBName, lang string) {
	if dbConnStr == "" || slowOutputPath == "" || replayOutputFilePath == "" {
		fmt.Println(i18n.T(lang, "usage"))
		return
	}

	if speed <= 0 {
		fmt.Println(i18n.T(lang, "invalid_speed"))
		return
	}

	fmt.Printf(i18n.T(lang, "replay_info")+"\n", filterUsername, filterDBName, filterSQLType, speed)

	ts0 := time.Now()
	fmt.Println(i18n.T(lang, "parsing_start"), ts0)

	tasksMap, minTimestamp, err := ParseLogEntries(slowOutputPath, filterUsername, filterSQLType, filterDBName)
	if err != nil {
		fmt.Println(i18n.T(lang, "file_open_error"), err)
		return
	}

	ts1 := time.Now()
	fmt.Println(i18n.T(lang, "parsing_complete"), ts1)
	fmt.Printf("%s %v\n", i18n.T(lang, "parsing_time"), ts1.Sub(ts0))
	fmt.Println(i18n.T(lang, "replay_start"))

	var wg sync.WaitGroup

	for connID, entries := range tasksMap {
		wg.Add(1)
		go func(connID string, entries []LogEntry) {
			defer wg.Done()
			ReplaySQLForConnection(connID, entries, dbConnStr, replayOutputFilePath, minTimestamp, speed, lang)
		}(connID, entries)
	}

	wg.Wait()
	ts2 := time.Now()
	fmt.Println(i18n.T(lang, "replay_complete"), ts2)
	fmt.Printf("%s %v\n", i18n.T(lang, "replay_time"), ts2.Sub(ts1))
}
