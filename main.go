package main

import (
	"flag"
	"fmt"
	"os"
)

// Version information for the SQL Replay Tool
var version = "0.3.4, build date 20241014"

var showVersion bool

func main() {
	var mode string
	flag.StringVar(&mode, "mode", "", "Mode of operation: parsemysqlslow ,parsetidbslow , replay, load, report")

	// Define flags for various operation parameters
	var slowLogPath, slowOutputPath, dbConnStr, replayOutputFilePath, filterUsername, filterSQLType, filterDBName, ignoreDigests, outDir, replayOut, tableName, Port string
	var Speed float64
	var lang string
	var maxConnections int

	flag.BoolVar(&showVersion, "version", false, "Show version info")
	flag.StringVar(&slowLogPath, "slow-in", "", "Path to slow query log file")
	flag.StringVar(&slowOutputPath, "slow-out", "", "Path to slow output JSON file")
	flag.StringVar(&dbConnStr, "db", "username:password@tcp(localhost:3306)/test", "Database connection string")
	flag.StringVar(&outDir, "out-dir", "", "Directory containing the JSON files")
	flag.StringVar(&replayOut, "replay-name", "", "Replay output filename")
	flag.StringVar(&tableName, "table", "replay_info", "Name of the table to insert data into")
	flag.StringVar(&replayOutputFilePath, "replay-out", "", "Path to output json file")
	flag.StringVar(&filterUsername, "username", "all", "Username to filter (default 'all', or specific username)")
	flag.StringVar(&filterSQLType, "sqltype", "all", "SQL type to filter (default 'all', or 'select')")
	flag.StringVar(&filterDBName, "dbname", "all", "Database name to filter (default 'all', or specific dbname)")
	flag.StringVar(&ignoreDigests, "ignoredigests", "", "Ignore the Specific digests")
	flag.Float64Var(&Speed, "speed", 1.0, "Replay speed multiplier")
	flag.StringVar(&Port, "port", ":8081", "Report web server port")
	flag.StringVar(&lang, "lang", "en", "Language for output (e.g., 'en' for English, 'zh' for Chinese)")
	flag.IntVar(&maxConnections, "maxconnections", 100, "Maximum number of mysql connections to use, default is 100")
	flag.Parse()

	if showVersion {
		fmt.Println("SQL Replay Tool Version:", version)
		os.Exit(0)
	}

	if mode == "" {
		printUsage()
		os.Exit(1)
	}

	// Execute the appropriate function based on the selected mode
	switch mode {
	case "parsemysqlslow":
		ParseLogs(slowLogPath, slowOutputPath)
	case "parsetidbslow":
		ParseTiDBLogs(slowLogPath, slowOutputPath)
	case "replay":
		StartSQLReplay(dbConnStr, Speed, slowOutputPath, replayOutputFilePath, filterUsername, filterSQLType, filterDBName, ignoreDigests, lang, maxConnections)
	case "load":
		LoadData(dbConnStr, outDir, replayOut, tableName)
	case "report":
		Report(dbConnStr, replayOut, Port)
	default:
		fmt.Println("Invalid mode. Available modes: parse, replay, load, report")
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: ./sql-replay -mode [parse|replay|load|report]")
	fmt.Println("    1. parse mysql slow log: ./sql-replay -mode parsemysqlslow -slow-in <path_to_slow_query_log> -slow-out <path_to_slow_output_file>")
	fmt.Println("    2. parse tidb slow log: ./sql-replay -mode parsetidbslow -slow-in <path_to_slow_query_log> -slow-out <path_to_slow_output_file>")
	fmt.Println("    3. replay mode: ./sql-replay -mode replay -db <mysql_connection_string> -speed 1.0 -maxconnections 100 -slow-out <slow_output_file> -replay-out <replay_output_file> -username <all|username> -sqltype <all|select> -dbname <all|dbname> -ignoredigests <digest1,digest2...> -lang <en|zh>")
	fmt.Println("    4. load mode: ./sql-replay -mode load -db <DB_CONN_STRING> -out-dir <DIRECTORY> -replay-name <REPORT_OUT_FILE_NAME> -table <replay_info>")
	fmt.Println("    5. report mode: ./sql-replay -mode report -db <mysql_connection_string> -replay-name <replay name> -port ':8081'")
}
