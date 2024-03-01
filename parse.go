package main

import (
    "bufio"
    "encoding/json"
    "fmt"
    "os"
    "regexp"
    "strconv"
    "strings"
    "time"

    "github.com/pingcap/tidb/pkg/parser"
)

type LogEntry1 struct {
    ConnectionID string  `json:"connection_id"`
    QueryTime    int64   `json:"query_time"`
    SQL          string  `json:"sql"`
    RowsSent     int     `json:"rows_sent"`
    Username     string  `json:"username"`
    SQLType      string  `json:"sql_type"`
    Ts           float64 `json:"ts"`
}

func ParseLogs(slowLogPath, slowOutputPath string) {
    if slowLogPath == "" || slowOutputPath == "" {
        fmt.Println("Usage: ./sql-replay -mode parse -slow-in <path_to_slow_query_log> -slow-out <path_to_slow_output_file>")
        return
    }

    file, err := os.Open(slowLogPath)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer file.Close()

    outputFile, err := os.Create(slowOutputPath)
    if err != nil {
        fmt.Println("Error creating output file:", err)
        return
    }
    defer outputFile.Close()

    scanner := bufio.NewScanner(file)
    buf := make([]byte, 0, 512*1024*1024) // 512MB buffer
    scanner.Buffer(buf, bufio.MaxScanTokenSize)

    var currentEntry LogEntry1
    var sqlBuffer strings.Builder
    var entryStarted bool = false

    // 增加对 MySQL 5.6 时间格式的匹配
    reTime56 := regexp.MustCompile(`Time: (\d{6})  ?(\d{1,2}:\d{2}:\d{2})`)

    reTime := regexp.MustCompile(`Time: ([\d-T:.Z]+)`)
    reUser := regexp.MustCompile(`User@Host: (\w+)\[`)
    reConnectionID := regexp.MustCompile(`Id:\s*(\d+)`)

    for scanner.Scan() {
        line := scanner.Text()

        if strings.HasPrefix(line, "# Time:") {
            if entryStarted {
                // Finish the current entry before starting a new one
                finalizeEntry(&currentEntry, &sqlBuffer, outputFile)
            }
            entryStarted = true // Mark the beginning of a new entry

            // MySQL 5.6 Time Format
            if match := reTime56.FindStringSubmatch(line); len(match) > 1 {
                // 这里直接使用match[1]和match[2]，因为已经通过替换处理了小时小于10的情况
                timeStr := fmt.Sprintf("%s %s", match[1], match[2])
                parsedTime, err := time.Parse("060102 15:04:05", timeStr)
                if err != nil {
                    fmt.Println("Error parsing time:", err)
                    continue
                }
                currentEntry.Ts = float64(parsedTime.UnixNano()) / 1e9
                continue
            }

            // MySQL 5.7/8.0 Time Format
            if match := reTime.FindStringSubmatch(line); len(match) > 1 {
                parsedTime, _ := time.Parse(time.RFC3339Nano, match[1])
                currentEntry.Ts = float64(parsedTime.UnixNano()) / 1e9
                continue
            }
            continue
        }

        if entryStarted {
            if strings.HasPrefix(line, "# User@Host:") {
                match := reUser.FindStringSubmatch(line)
                if len(match) > 1 {
                    currentEntry.Username = match[1]
                }
                matchID := reConnectionID.FindStringSubmatch(line)
                if len(matchID) > 1 {
                    currentEntry.ConnectionID = matchID[1]
                }
            } else if strings.HasPrefix(line, "# Query_time:") {
                processQueryTimeAndRowsSent(line, &currentEntry)
            } else if !strings.HasPrefix(line, "#") {
                if !(strings.HasPrefix(line, "SET timestamp=") || strings.HasPrefix(line, "-- ") || strings.HasPrefix(line, "use ")) {
                    sqlBuffer.WriteString(line + " ")
                }
            }
        }
    }

    // Process the last entry if there is one
    if entryStarted {
        finalizeEntry(&currentEntry, &sqlBuffer, outputFile)
    }

    if err := scanner.Err(); err != nil {
        fmt.Println("Error reading file:", err)
    }
}

func processQueryTimeAndRowsSent(line string, entry *LogEntry1) {
    reTime := regexp.MustCompile(`Query_time: (\d+\.\d+)`)
    matchTime := reTime.FindStringSubmatch(line)
    if len(matchTime) > 1 {
        queryTime, _ := strconv.ParseFloat(matchTime[1], 64)
        entry.QueryTime = int64(queryTime * 1000000) // Convert seconds to microseconds
    }

    reRows := regexp.MustCompile(`Rows_sent: (\d+)`)
    matchRows := reRows.FindStringSubmatch(line)
    if len(matchRows) > 1 {
        entry.RowsSent, _ = strconv.Atoi(matchRows[1])
    }
}

func finalizeEntry(entry *LogEntry1, sqlBuffer *strings.Builder, outputFile *os.File) {
    entry.SQL = strings.TrimSpace(sqlBuffer.String())
    // 检查 SQL 是否为空，如果为空，则不处理这条记录
    if entry.SQL == "" {
        return
    }
    normalizedSQL := parser.Normalize(entry.SQL)
    words := strings.Fields(normalizedSQL)
    entry.SQLType = "other"
    if len(words) > 0 {
        entry.SQLType = words[0]
    }
    jsonEntry, _ := json.Marshal(entry)
    fmt.Fprintln(outputFile, string(jsonEntry))
    // Reset for next entry
    *entry = LogEntry1{}
    sqlBuffer.Reset()
}