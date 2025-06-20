package main

import (
    "bufio"
    "fmt"
    "os"
    "regexp"
    "strings"
    "time"
)

// ParseMariaDBLogs 解析 MariaDB 慢日志，支持两条相同时间戳的条目第二条无 # Time
func ParseMariaDBLogs(slowLogPath, slowOutputPath string) {
    if slowLogPath == "" || slowOutputPath == "" {
        fmt.Println("Usage: ./sql-replay -mode parsemariadbslow -slow-in <slow.log> -slow-out <out.json>")
        return
    }

    inFile, err := os.Open(slowLogPath)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer inFile.Close()

    outFile, err := os.Create(slowOutputPath)
    if err != nil {
        fmt.Println("Error creating output file:", err)
        return
    }
    defer outFile.Close()

    // 扩大 Scanner 缓冲到 512MB
    scanner := bufio.NewScanner(inFile)
    buf := make([]byte, 0, 512*1024*1024)
    scanner.Buffer(buf, len(buf))

    // 正则
    reTime56 := regexp.MustCompile(`^# Time: (\d{6})\s+([\d:.]+)`)
    reTime   := regexp.MustCompile(`^# Time: ([\d\-T:\.Z]+)`)
    reUser   := regexp.MustCompile(`^# User@Host: ([^\[]+)\[`)
    reThread := regexp.MustCompile(`^# Thread_id: (\d+)\s+Schema:\s*(\S+)`)

    var (
        entryStarted bool
        lastTs       float64        // 最近一次 # Time 解析出的时间戳
        current      LogEntry
        sqlBuf       strings.Builder
    )

    // flush：调用主仓库的 finalizeEntry，然后重置 state
    flush := func() {
        // 只有在有 SQL 的时候才写出
        if sqlBuf.Len() > 0 {
            finalizeEntry(&current, &sqlBuf, outFile)
        }
        // 重置为下一个条目初始态
        current = LogEntry{Timestamp: lastTs}
        sqlBuf.Reset()
    }

    for scanner.Scan() {
        line := scanner.Text()

        // 检测 "# Time:" —— 普通新条目
        if strings.HasPrefix(line, "# Time:") {
            if entryStarted {
                flush()
            } else {
                entryStarted = true
            }

            // 5.6 短格式
            if m := reTime56.FindStringSubmatch(line); len(m) == 3 {
                if ts, err := time.ParseInLocation("060102 15:04:05", m[1]+" "+m[2], time.Local); err == nil {
                    lastTs = float64(ts.UnixNano()) / 1e9
                    current.Timestamp = lastTs
                }
                continue
            }
            // 5.7+ ISO 格式
            if m := reTime.FindStringSubmatch(line); len(m) == 2 {
                if ts, err := time.Parse(time.RFC3339Nano, m[1]); err == nil {
                    lastTs = float64(ts.UnixNano()) / 1e9
                    current.Timestamp = lastTs
                }
            }
            continue
        }

        if !entryStarted {
            continue
        }

        // 如果碰到新的 "# User@Host:"，且已开始条目，且已有部分内容，则认为是同一时间的下一条
        if strings.HasPrefix(line, "# User@Host:") && (sqlBuf.Len() > 0 || current.Username != "") {
            flush()
        }

        switch {
        case strings.HasPrefix(line, "# User@Host:"):
            if m := reUser.FindStringSubmatch(line); len(m) == 2 {
                current.Username = m[1]
            }

        case strings.HasPrefix(line, "# Thread_id:"):
            if m := reThread.FindStringSubmatch(line); len(m) == 3 {
                current.ConnectionID = m[1]
                current.DBName       = m[2]
            }

        case strings.HasPrefix(line, "# Query_time:"):
            processQueryTimeAndRowsSent(line, &current)

        default:
            // 跳过注释/SET/use 行
            if strings.HasPrefix(line, "#") ||
                strings.HasPrefix(line, "SET timestamp=") ||
                strings.HasPrefix(line, "-- ") ||
                strings.HasPrefix(line, "use ") {
                continue
            }
            // 普通 SQL 行
            sqlBuf.WriteString(line)
            sqlBuf.WriteString(" ")
        }
    }

    // 收尾最后一条
    if entryStarted {
        flush()
    }
    if err := scanner.Err(); err != nil {
        fmt.Println("Error scanning file:", err)
    }
}