package main

import (
    "flag"
    "fmt"
    "os"
)

func main() {
    var mode string
    flag.StringVar(&mode, "mode", "", "Mode of operation: parse, replay, or load")

    // 共用的标志
    var slowLogPath, slowOutputPath, dbConnStr, replayOutputFilePath, filterUsername, filterSQLType, outDir, replayOut, tableName string
    flag.StringVar(&slowLogPath, "slow-in", "", "Path to slow query log file")
    flag.StringVar(&slowOutputPath, "slow-out", "", "Path to slow output JSON file")
    flag.StringVar(&dbConnStr, "db", "username:password@tcp(localhost:3306)/test", "Database connection string")
    flag.StringVar(&outDir, "out-dir", "", "Directory containing the JSON files")
    flag.StringVar(&replayOut, "replay-name", "", "replayout filename of the JSON files")
    flag.StringVar(&tableName, "table", "", "Name of the table to insert data into")
    flag.StringVar(&replayOutputFilePath, "replay-out", "", "Path to output json file")
    flag.StringVar(&filterUsername, "username", "all|username", "Username to filter (default 'all')")
    flag.StringVar(&filterSQLType, "sqltype", "all|select", "SQL type to filter (default 'all')")
    flag.Parse()

    if mode == "" {
        fmt.Println("Usage: ./sql-replay -mode [parse|replay|load]")
        os.Exit(1)
    }

    switch mode {
    case "parse":
        ParseLogs(slowLogPath, slowOutputPath)
    case "replay":
        ReplaySQL(dbConnStr, slowOutputPath, replayOutputFilePath, filterUsername, filterSQLType)
    case "load":
        LoadData(dbConnStr, outDir, replayOut, tableName)
    default:
        fmt.Println("Invalid mode. Available modes: parse, replay, load")
        os.Exit(1)
    }
}
