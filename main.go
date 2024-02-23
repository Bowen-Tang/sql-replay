package main

import (
    "flag"
    "fmt"
    "os"
)

func main() {
    var mode string
    flag.StringVar(&mode, "mode", "", "Mode of operation: parse, replay, load, report")

    // 共用的标志
    var slowLogPath, slowOutputPath, dbConnStr, replayOutputFilePath, filterUsername, filterSQLType,filterDBName, outDir, replayOut, tableName, Port string
    var Speed float64
    flag.StringVar(&slowLogPath, "slow-in", "", "Path to slow query log file")
    flag.StringVar(&slowOutputPath, "slow-out", "", "Path to slow output JSON file")
    flag.StringVar(&dbConnStr, "db", "username:password@tcp(localhost:3306)/test", "Database connection string")
    flag.StringVar(&outDir, "out-dir", "", "Directory containing the JSON files")
    flag.StringVar(&replayOut, "replay-name", "", "replayout filename of the JSON files")
    flag.StringVar(&tableName, "table", "replay_info", "Name of the table to insert data into")
    flag.StringVar(&replayOutputFilePath, "replay-out", "", "Path to output json file")
    flag.StringVar(&filterUsername, "username", "all", "Username to filter (default 'all',or username)")
    flag.StringVar(&filterSQLType, "sqltype", "all", "SQL type to filter (default 'all',or select)")
    flag.StringVar(&filterDBName, "dbname", "all", "SQL type to filter (default 'all',or dbname)")
    flag.Float64Var(&Speed, "speed", 1.0, "Replay speed, default 1.0")
    flag.StringVar(&Port, "port", ":8081", "Report Web port")

    flag.Parse()

    if mode == "" {
        fmt.Println("Usage: ./sql-replay -mode [parse|replay|load|report]")
        fmt.Println("    1. parse mode: ./sql-replay -mode parse -slow-in <path_to_slow_query_log> -slow-out <path_to_slow_output_file>")
        fmt.Println("    2. replay mode: ./sql-replay -mode replay -db <mysql_connection_string> -speed 1.0 -slow-out <slow_output_file> -replay-out <replay_output_file> -username <all|username> -sqltype <all|select> -dbname <all|dbname>")
        fmt.Println("    3. load mode: ./sql-replay -mode load -db <DB_CONN_STRING> -out-dir <DIRECTORY> -replay-name <REPORT_OUT_FILE_NAME> -table <replay_info>")
        fmt.Println("    4. report mode: ./sql-replay -mode report -db <mysql_connection_string> -replay-name <replay name> -port ':8081'")
        os.Exit(1)
    }

    switch mode {
    case "parse":
        ParseLogs(slowLogPath, slowOutputPath)
    case "replay":
        ReplaySQL(dbConnStr, Speed, slowOutputPath, replayOutputFilePath, filterUsername, filterSQLType, filterDBName)
    case "load":
        LoadData(dbConnStr, outDir, replayOut, tableName)
    case "report":
        Report(dbConnStr, replayOut, Port)
    default:
        fmt.Println("Invalid mode. Available modes: parse, replay, load, report")
        os.Exit(1)
    }
}
