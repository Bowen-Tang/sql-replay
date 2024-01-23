package main

import (
    "bufio"
    "encoding/json"
    "fmt"
    "github.com/pingcap/tidb/pkg/parser"
    "os"
    "flag"
    "regexp"
    "strconv"
    "strings"
)

// LogEntry 代表慢查询日志中的一条记录
type LogEntry struct {
    ConnectionID string `json:"connection_id"`
    QueryTime    int64  `json:"query_time"`
    SQL          string `json:"sql"`
    RowsSent     int    `json:"rows_sent"`
    Username     string `json:"username"`  // 新增字段 username
    SQLType      string `json:"sql_type"`  // 新增字段 sql_type
}

func main() {
    var (
        slowLogPath string
        slow_outputPath string
    )

    flag.StringVar(&slowLogPath, "slow_in", "", "Path to slow query log file")
    flag.StringVar(&slow_outputPath, "slow_out", "", "Path to slow output JSON file")
    flag.Parse()

    if slowLogPath == "" || slow_outputPath == "" {
        fmt.Println("Usage: ./parse_tool -slow_in <path_to_slow_query_log> -slow_out <path_to_slow_output_file>")
        return
    }

    file, err := os.Open(slowLogPath)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer file.Close()

    outputFile, err := os.Create(slow_outputPath)
    if err != nil {
        fmt.Println("Error creating output file:", err)
        return
    }
    defer outputFile.Close()

    scanner := bufio.NewScanner(file)
    var currentEntry LogEntry
    var sqlBuffer strings.Builder

    reUser := regexp.MustCompile(`User@Host: (\w+)\[`) // 正则表达式用于提取 username
    reConnectionID := regexp.MustCompile(`Id:\s*(\d+)`) // 用于提取 ConnectionID

    for scanner.Scan() {
        line := scanner.Text()

        if strings.HasPrefix(line, "# User@Host:") {
            if sqlBuffer.Len() > 0 {
                currentEntry.SQL = strings.TrimSpace(sqlBuffer.String())
                normalizedSQL := parser.Normalize(currentEntry.SQL)
                words := strings.Fields(normalizedSQL)
                currentEntry.SQLType = "other"
                if len(words) > 0 {
                    currentEntry.SQLType = words[0]
                }
                jsonEntry, _ := json.Marshal(currentEntry)
                fmt.Fprintln(outputFile, string(jsonEntry))
                sqlBuffer.Reset()
            }

            currentEntry = LogEntry{}
            match := reUser.FindStringSubmatch(line)
            if len(match) > 1 {
                currentEntry.Username = match[1] // 设置 username
            }
            matchID := reConnectionID.FindStringSubmatch(line)
            if len(matchID) > 1 {
                currentEntry.ConnectionID = matchID[1] // 设置 ConnectionID
            }
        } else if strings.HasPrefix(line, "# Query_time:") {
            reTime := regexp.MustCompile(`Query_time: (\d+\.\d+)`)
            matchTime := reTime.FindStringSubmatch(line)
            if len(matchTime) > 1 {
                queryTime, _ := strconv.ParseFloat(matchTime[1], 64)
                currentEntry.QueryTime = int64(queryTime * 1000000)
            }

            reRows := regexp.MustCompile(`Rows_sent: (\d+)`)
            matchRows := reRows.FindStringSubmatch(line)
            if len(matchRows) > 1 {
                currentEntry.RowsSent, _ = strconv.Atoi(matchRows[1])
            }
        } else if !strings.HasPrefix(line, "#") {
            if !(strings.HasPrefix(line, "SET timestamp=") || strings.HasPrefix(line, "-- ")) {
                sqlBuffer.WriteString(line + " ")
            }
        }
    }

    // 处理最后一个条目（如果有的话）
    if sqlBuffer.Len() > 0 {
        currentEntry.SQL = strings.TrimSpace(sqlBuffer.String())
        normalizedSQL := parser.Normalize(currentEntry.SQL)
        words := strings.Fields(normalizedSQL)
        currentEntry.SQLType = "other"
        if len(words) > 0 {
            currentEntry.SQLType = words[0]
        }
        jsonEntry, _ := json.Marshal(currentEntry)
        fmt.Fprintln(outputFile, string(jsonEntry))
    }

    if err := scanner.Err(); err != nil {
        fmt.Println("Error reading file:", err)
    }
}