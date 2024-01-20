package main

import (
    "bufio"
    "encoding/json"
    "fmt"
    "os"
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
}

func main() {
    if len(os.Args) < 3 {
        fmt.Println("Usage: go run parse_log.go <path_to_slow_query_log> <path_to_output_file>")
        return
    }

    slowLogPath := os.Args[1]
    outputPath := os.Args[2]

    file, err := os.Open(slowLogPath)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer file.Close()

    outputFile, err := os.Create(outputPath)
    if err != nil {
        fmt.Println("Error creating output file:", err)
        return
    }
    defer outputFile.Close()

    scanner := bufio.NewScanner(file)
    var currentEntry LogEntry
    var sqlBuffer strings.Builder

    for scanner.Scan() {
        line := scanner.Text()

        if strings.HasPrefix(line, "# User@Host:") {
            // Process previous entry
            if sqlBuffer.Len() > 0 {
                currentEntry.SQL = strings.TrimSpace(sqlBuffer.String())
                jsonEntry, _ := json.Marshal(currentEntry)
                fmt.Fprintln(outputFile, string(jsonEntry))
                sqlBuffer.Reset()
            }

            // Reset entry
            currentEntry = LogEntry{}
            re := regexp.MustCompile(`Id:\s*(\d+)`)
            match := re.FindStringSubmatch(line)
            if len(match) > 1 {
                currentEntry.ConnectionID = match[1]
            }
        }

        if strings.HasPrefix(line, "# Query_time:") {
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
        }

        if !strings.HasPrefix(line, "#") {
            if !(strings.HasPrefix(line, "SET timestamp=") || strings.HasPrefix(line, "-- ")) {
                sqlBuffer.WriteString(line + " ")
            }
        }
    }

    // Process last entry if any
    if sqlBuffer.Len() > 0 {
        currentEntry.SQL = strings.TrimSpace(sqlBuffer.String())
        jsonEntry, _ := json.Marshal(currentEntry)
        fmt.Fprintln(outputFile, string(jsonEntry))
    }

    if err := scanner.Err(); err != nil {
        fmt.Println("Error reading file:", err)
    }
}
