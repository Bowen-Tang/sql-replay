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

func ParseTiDBLogs(slowLogPath, slowOutputPath string) {
    if slowLogPath == "" || slowOutputPath == "" {
        fmt.Println("Usage: ./sql-replay -mode parsetidbslow -slow-in <path_to_slow_query_log> -slow-out <path_to_slow_output_file>")
        return
    }

    file, err := os.Open(slowLogPath)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer file.Close()

    var entries []LogEntry
    bufferSize := 1024 * 1024 * 10 // 1MB
    scanner := bufio.NewScanner(file)
    buf := make([]byte, bufferSize)
    scanner.Buffer(buf, bufferSize)

    var entry LogEntry
    var isInternal bool
    var sqlStatement string
    var isPrepared string // 声明 isPrepared 变量
    timeRegex := regexp.MustCompile(`# Time:\s+(\d+-\d+-\d+T\d+:\d+:\d+\.\d+\+\d+:\d+)`)
    userHostRegex := regexp.MustCompile(`# User@Host:\s+(\w+)`)
    connIDRegex := regexp.MustCompile(`# Conn_ID:\s+(\d+)`)
    queryTimeRegex := regexp.MustCompile(`# Query_time:\s+(\d+\.\d+)`)
    dbRegex := regexp.MustCompile(`# DB:\s+(\w+)`)
    isInternalRegex := regexp.MustCompile(`# Is_internal:\s+(true|false)`)
    preparedRegex := regexp.MustCompile(`# Prepared:\s+(true|false)`)

    for scanner.Scan() {
        line := scanner.Text()

        // 检查是否为 Time 字段
        if timeRegex.MatchString(line) {
            if !isInternal { // 如果 Is_internal 为 true，跳过这个日志段落
                if isPrepared == "true" {
                    entry.SQL = formatSQL(sqlStatement)
                } else {
                    entry.SQL = sqlStatement
                }
                if entry.ConnectionID != "" && entry.SQL != "" { // 确保 entry 被正确填充
                    entries = append(entries, entry)
                }
            }

            // 初始化新的日志段落
            sqlStatement = ""
            entry = LogEntry{}
            isInternal = false
            isPrepared = "false"

            // 提取 Time 字段并转换为带时区的时间戳
            match := timeRegex.FindStringSubmatch(line)
            if match != nil {
                parsedTime, err := time.Parse(time.RFC3339Nano, match[1])
                if err == nil {
                    entry.Timestamp = float64(parsedTime.UnixNano()) / 1e9
                } else {
                    fmt.Println("Error parsing time:", err)
                }
            }
        } else if userHostRegex.MatchString(line) {
            // 提取 Username
            match := userHostRegex.FindStringSubmatch(line)
            if match != nil {
                entry.Username = match[1]
            }
        } else if connIDRegex.MatchString(line) {
            // 提取 ConnectionID
            match := connIDRegex.FindStringSubmatch(line)
            if match != nil {
                entry.ConnectionID = match[1]
            }
        } else if queryTimeRegex.MatchString(line) {
            // 提取 QueryTime
            match := queryTimeRegex.FindStringSubmatch(line)
            if match != nil {
                queryTime, _ := strconv.ParseFloat(match[1], 64)
                entry.QueryTime = int64(queryTime * 1e6) // 转换为微秒
            }
        } else if dbRegex.MatchString(line) {
            // 提取 DBName
            match := dbRegex.FindStringSubmatch(line)
            if match != nil {
                entry.DBName = match[1]
            }
        } else if isInternalRegex.MatchString(line) {
            // 检查是否为内部 SQL
            match := isInternalRegex.FindStringSubmatch(line)
            if match != nil && match[1] == "true" {
                isInternal = true
            }
        } else if preparedRegex.MatchString(line) {
            // 检查是否为预编译 SQL
            match := preparedRegex.FindStringSubmatch(line)
            if match != nil {
                isPrepared = match[1]
            }
        } else if !strings.HasPrefix(line, "#") {
            // 检查是否以 "use "（忽略大小写）开头
            if !strings.HasPrefix(strings.ToLower(line), "use ") {
                // 处理 SQL 语句
                sqlStatement += strings.TrimSpace(line)
                normalizedSQL := parser.Normalize(sqlStatement)
                entry.Digest = parser.DigestNormalized(normalizedSQL).String()
                words := strings.Fields(normalizedSQL)
                entry.SQLType = "other"
                if len(words) > 0 {
                    entry.SQLType = words[0]
                }
            }
        }
    }

    // 在处理最后一个日志段落的部分
    if !isInternal {
        if isPrepared == "true" {
            entry.SQL = formatSQL(sqlStatement)
        } else {
            entry.SQL = sqlStatement
        }
        if entry.ConnectionID != "" && entry.SQL != "" { // 确保 entry 被正确填充
            entries = append(entries, entry)
        }
    }

    outputFile, err := os.Create(slowOutputPath)
    if err != nil {
        fmt.Println("Error creating output file:", err)
        return
    }
    defer outputFile.Close()

    // 逐个输出 JSON 对象
    for _, entry := range entries {
        jsonEntry, err := json.Marshal(entry)
        if err != nil {
            fmt.Println("Error marshaling JSON:", err)
            return
        }
        outputFile.Write(jsonEntry)
        outputFile.WriteString("\n")
    }

    fmt.Println("Logs processed and written to output json")
}

// formatSQL 函数用于格式化 SQL 语句，替换 ? 占位符为对应的参数值。
func formatSQL(input string) string {
    // 使用正则表达式匹配 arguments 部分
    argumentsRegex := regexp.MustCompile(`\[arguments:\s*(\((.*?)\)|([^()]+))\]`)
    match := argumentsRegex.FindStringSubmatch(input)

    var arguments []string
    if len(match) > 1 {
        // 提取 arguments 部分并去掉多余的空格
        var argumentsStr string
        if match[2] != "" {
            // 如果存在括号，提取括号内的内容
            argumentsStr = match[2]
        } else {
            // 否则直接使用匹配的内容
            argumentsStr = match[3]
        }

        // 拆分参数并去掉多余的空格
        arguments = strings.Split(argumentsStr, ",")
        for i := range arguments {
            arguments[i] = strings.TrimSpace(arguments[i]) // 去除空格
        }

        // 去掉原始 input 中的 arguments 部分
        input = strings.Replace(input, match[0], "", 1)
    }

    // 替换 ? 占位符，注意考虑引号情况
    var result strings.Builder
    argIndex := 0 // 当前参数索引
    inQuotes := 0 // 引号计数：0 表示不在引号内，1 表示在单引号内，2 表示在双引号内

    for i, char := range input {
        if char == '"' {
            inQuotes = (inQuotes + 2) % 4 // 切换双引号状态
        } else if char == '\'' {
            inQuotes = (inQuotes + 1) % 2 // 切换单引号状态
        }

        // 判断是否为 ? 占位符
        if char == '?' && inQuotes == 0 {
            // 判断是否有参数可用
            if argIndex < len(arguments) {
                arg := arguments[argIndex]
                argIndex++ // 递增参数索引

                // 判断参数类型，如果是字符串则加上引号
                if strings.HasPrefix(arg, "'") && strings.HasSuffix(arg, "'") {
                    arg = arg[1 : len(arg)-1] // 去掉引号
                    result.WriteString("'" + arg + "'")
                } else if strings.HasPrefix(arg, "\"") && strings.HasSuffix(arg, "\"") {
                    arg = arg[1 : len(arg)-1] // 去掉引号
                    result.WriteString("'" + arg + "'")
                } else {
                    result.WriteString(arg)
                }
                continue
            }
        }

        // 处理转义字符和保留原字符
        if char == '\\' && i < len(input)-1 && (input[i+1] == '"' || input[i+1] == '\'') {
            result.WriteRune(char)
            continue
        }
        result.WriteRune(char) // 其他字符直接写入
    }

    return result.String()
}
