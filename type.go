package main

import "database/sql"

// LogEntry 表示日志条目
type LogEntry struct {
	ConnectionID string  `json:"connection_id"`
	QueryTime    int64   `json:"query_time"`
	SQL          string  `json:"sql"`
	RowsSent     int     `json:"rows_sent"`
	Username     string  `json:"username"`
	SQLType      string  `json:"sql_type"`
	DBName       string  `json:"dbname"`
	Timestamp    float64 `json:"ts"`
	Digest       string  `json:"digest"`
}

// SQLExecutionRecord 表示SQL执行记录
type SQLExecutionRecord struct {
	SQL           string `json:"sql"`
	QueryTime     int64  `json:"query_time"`
	RowsSent      int    `json:"rows_sent"`
	ExecutionTime int64  `json:"execution_time"`
	RowsReturned  int64  `json:"rows_returned"`
	ErrorInfo     string `json:"error_info,omitempty"`
	FileName      string // File name
	DBName        string `json:"dbname"`
}

// SQLTask 表示SQL任务
type SQLTask struct {
	Entry LogEntry
	DB    *sql.DB
}
