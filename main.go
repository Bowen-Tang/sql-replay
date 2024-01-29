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
    var slowLogPath, slowOutputPath, dbConnStr, replayOutputFilePath, filterUsername, filterSQLType,filterDBName, outDir, replayOut, tableName string
    var Speed float64
    flag.StringVar(&slowLogPath, "slow-in", "", "Path to slow query log file")
    flag.StringVar(&slowOutputPath, "slow-out", "", "Path to slow output JSON file")
    flag.StringVar(&dbConnStr, "db", "username:password@tcp(localhost:3306)/test", "Database connection string")
    flag.StringVar(&outDir, "out-dir", "", "Directory containing the JSON files")
    flag.StringVar(&replayOut, "replay-name", "", "replayout filename of the JSON files")
    flag.StringVar(&tableName, "table", "", "Name of the table to insert data into")
    flag.StringVar(&replayOutputFilePath, "replay-out", "", "Path to output json file")
    flag.StringVar(&filterUsername, "username", "all", "Username to filter (default 'all',or username)")
    flag.StringVar(&filterSQLType, "sqltype", "all", "SQL type to filter (default 'all',or select)")
    flag.StringVar(&filterDBName, "dbname", "all", "SQL type to filter (default 'all',or dbname)")
    flag.Float64Var(&Speed, "speed", 1.0, "Replay speed, default 1.0")
    flag.Parse()

    if mode == "" {
        fmt.Println("Usage: ./sql-replay -mode [parse|replay|load]")
        os.Exit(1)
    }

    switch mode {
    case "parse":
        ParseLogs(slowLogPath, slowOutputPath)
    case "replay":
        ReplaySQL(dbConnStr, Speed, slowOutputPath, replayOutputFilePath, filterUsername, filterSQLType, filterDBName)
    case "load":
        LoadData(dbConnStr, outDir, replayOut, tableName)
    default:
        fmt.Println("Invalid mode. Available modes: parse, replay, load")
        os.Exit(1)
    }
}
