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

type SQLExecutionRecord2 struct {
	SQL           string `json:"sql"`
	QueryTime     int64  `json:"query_time"`
	RowsSent      int    `json:"rows_sent"`
	ExecutionTime int64  `json:"execution_time"`
	RowsReturned  int64  `json:"rows_returned"`
	ErrorInfo     string `json:"error_info,omitempty"`
}

type LogEntry2 struct {
	ConnectionID string  `json:"connection_id"`
	QueryTime    int64   `json:"query_time"`
	SQL          string  `json:"sql"`
	RowsSent     int     `json:"rows_sent"`
	Username     string  `json:"username"` // 新增字段 username
	SQLType      string  `json:"sql_type"` // 新增字段 sql_type
	DBName       string  `json:"dbname"`   // 新增字段 dbname
	Ts           float64 `json:"ts"`       // 新增字段，以秒为单位，包含6位精度
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

var i18n *I18n

func init() {
	var err error
	i18n, err = NewI18n("translations.json", "en")
	if err != nil {
		panic(err)
	}
}

func ReplaySQL(dbConnStr string, Speed float64, slowOutputPath, replayOutputFilePath, filterUsername, filterSQLType, filterDBName, lang string) {
	if dbConnStr == "" || slowOutputPath == "" || replayOutputFilePath == "" {
		fmt.Println(i18n.T(lang, "usage"))
		return
	}

	if Speed <= 0 {
		fmt.Println(i18n.T(lang, "invalid_speed"))
		return
	}

	fmt.Printf(i18n.T(lang, "replay_info"), filterUsername, filterDBName, filterSQLType, Speed)

	ts0 := time.Now()
	fmt.Println(i18n.T(lang, "parsing_start"), ts0)

	inputFile, err := os.Open(slowOutputPath)
	if err != nil {
		fmt.Println(i18n.T(lang, "file_open_error"), err)
		return
	}
	defer inputFile.Close()

	var wg sync.WaitGroup
	scanner := bufio.NewScanner(inputFile)
	buf := make([]byte, 0, 512*1024*1024) // 512MB的缓冲区
	scanner.Buffer(buf, bufio.MaxScanTokenSize)

	tasksMap := make(map[string][]LogEntry2)
	var minTs float64 = 9999999999.999999 // 增加最小时间戳

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

		// 获取最小时间戳
		if entry.Ts < minTs {
			minTs = entry.Ts
		}
	}
	//    fmt.Printf("最小ts: %f\n", minTs)
	ts1 := time.Now() // map 构建完成时间
	fmt.Println(i18n.T(lang, "parsing_complete"), ts1)
	fmt.Printf("%s %v\n", i18n.T(lang, "parsing_time"), ts1.Sub(ts0))
	fmt.Println(i18n.T(lang, "replay_start"))

	// 为每个 ConnectionID 创建 goroutine
	for connID, entries := range tasksMap {
		wg.Add(1)
		go func(connID string, entries []LogEntry2) {
			defer wg.Done()

			db, err := sql.Open("mysql", dbConnStr)
			if err != nil {
				fmt.Printf(i18n.T(lang, "db_open_error")+"\n", connID, err)
				return
			}
			defer db.Close()

			// 初始化 prevTs 和 Last_QueryTime 0627
			var prevTs float64 = entries[0].Ts - (entries[0].Ts - minTs)
			var lastQueryTime int64 = 0
			//            fmt.Printf("第一个 ts: %f\n", prevTs)

			for _, entry := range entries {
				// 计算时间间隔并等待
				// interval := (entry.Ts - prevTs) / Speed
				// 计算时间间隔并等待，减去上一个 QueryTime 0627
				interval := (entry.Ts - prevTs - float64(lastQueryTime)/1e6) / Speed
				//                fmt.Printf("间隔: %f\n", interval)
				if interval > 0 {
					sleepDuration := time.Duration(interval * float64(time.Second))
					time.Sleep(sleepDuration)
				}
				prevTs = entry.Ts // 更新前一个时间戳

				// 执行 SQL
				task := SQLTask{Entry: entry, DB: db}
				if err := ExecuteSQLAndRecord(task, replayOutputFilePath); err != nil {
					fmt.Printf(i18n.T(lang, "sql_exec_error")+"\n", connID, err)
				}
				// 更新 Last_QueryTime 0627
				lastQueryTime = entry.QueryTime
			}
		}(connID, entries)
	}

	wg.Wait()
	ts2 := time.Now() // 回放完成时间
	fmt.Println(i18n.T(lang, "replay_complete"), ts2)
	fmt.Printf("%s %v\n", i18n.T(lang, "replay_time"), ts2.Sub(ts1))
}
