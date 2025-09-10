package main

import (
        "context"
        "flag"
        "fmt"
        "os"
        "strings"
        "time"
)

// Version information for the SQL Replay Tool
var version = "0.3.4-kafka, build date 20250910"

var showVersion bool

func main() {
        var mode string
        flag.StringVar(&mode, "mode", "", "Mode of operation: parsemysqlslow, parsetidbslow, replay, load, report, replay-kafka")

        // ----- Common / existing flags -----
        var slowLogPath, slowOutputPath, dbConnStr, replayOutputFilePath, filterUsername, filterSQLType, filterDBName, ignoreDigests, outDir, replayOut, tableName, Port string
        var Speed float64
        var lang string

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

        // ----- New: Kafka realtime replay flags -----
        var (
                kBrokers       string // comma-separated
                kTopic         string
                kGroup         string
                kStart         string // auto|committed|earliest|latest
                kDSN           string // DSN WITHOUT db name
                kDefaultDB     string
                kIDQueueCap    int
                kIdleTTL       string
                kStatsInterval string
                kTSLayout      string
                kTSLoc         string
                kReplayWithGap bool
                kErrLogDir     string
                kStopOnError   bool

                // SASL/TLS
                kSASLMech             string // "", plain, scram-sha256, scram-sha512
                kSASLUser             string
                kSASLPass             string
                kEnableTLS            bool
                kTLSInsecureSkipVerify bool

                        // New: fetch/commit batching
                        kFetchQueueCap  int    // e.g. 10000
                        kFetchBatch     int    // e.g. 1000
                        kFetchInterval  string // e.g. "100ms"
                        kCommitEvery    int    // e.g. 1000
                        kCommitInterval string // e.g. "200ms"
            )

        flag.StringVar(&kBrokers, "kafka-brokers", "", "Kafka broker list, e.g. '10.0.0.1:9092,10.0.0.2:9092'")
        flag.StringVar(&kTopic, "kafka-topic", "", "Kafka topic to consume")
        flag.StringVar(&kGroup, "kafka-group", "", "Kafka consumer group (required for offset resume)")
        flag.StringVar(&kStart, "kafka-start", "auto", "Start position: auto|committed|earliest|latest (auto = resume if exists else earliest)")
        flag.StringVar(&kDSN, "kafka-dsn", "username:password@tcp(localhost:3306)/?parseTime=true&multiStatements=true", "Target DB DSN WITHOUT database name")
        flag.StringVar(&kDefaultDB, "kafka-default-db", "", "Default DB name when event has empty db field")
        flag.IntVar(&kIDQueueCap, "kafka-id-queue-cap", 1024, "Per-id bounded queue size")
        flag.StringVar(&kIdleTTL, "kafka-idle-ttl", "1h", "Idle TTL to auto-close an id worker (e.g. 1h)")
        flag.StringVar(&kStatsInterval, "kafka-stats-interval", "10s", "Stats print interval (e.g. 10s)")
        flag.StringVar(&kTSLayout, "kafka-ts-layout", "2006-01-02 15:04:05.999999", "Timestamp layout for 'ts' field")
        flag.StringVar(&kTSLoc, "kafka-ts-loc", "Asia/Tokyo", "Time location for ts parsing (e.g. Asia/Tokyo or Local)")
        flag.BoolVar(&kReplayWithGap, "kafka-replay-with-gap", true, "Replay with per-id ts gap (sleep = Δts - prev_exec_us)")
        flag.StringVar(&kErrLogDir, "kafka-error-log-dir", ".", "Directory to write error replay logs")
        flag.BoolVar(&kStopOnError, "kafka-stop-on-error", false, "Stop immediately on first SQL execution error")

        // SASL/TLS flags
        flag.StringVar(&kSASLMech, "kafka-sasl-mech", "", "SASL mechanism: '', 'plain', 'scram-sha256', or 'scram-sha512'")
        flag.StringVar(&kSASLUser, "kafka-username", "", "SASL username")
        flag.StringVar(&kSASLPass, "kafka-password", "", "SASL password")
        flag.BoolVar(&kEnableTLS, "kafka-tls", false, "Enable TLS for Kafka connections")
        flag.BoolVar(&kTLSInsecureSkipVerify, "kafka-insecure-skip-verify", false, "Skip TLS cert verification (NOT recommended)")

            // Fetch/Commit batching flags
            flag.IntVar(&kFetchQueueCap, "kafka-fetch-queue-cap", 10000, "Size of global FIFO ingress queue")
            flag.IntVar(&kFetchBatch, "kafka-fetch-batch", 1000, "Number of messages to fetch per batch")
            flag.StringVar(&kFetchInterval, "kafka-fetch-interval", "100ms", "Interval between fetch batches (e.g. 100ms)")
            flag.IntVar(&kCommitEvery, "kafka-commit-every", 1000, "Commit offsets every N messages per partition")
            flag.StringVar(&kCommitInterval, "kafka-commit-interval", "200ms", "Commit offsets at least every given duration")

        flag.Parse()

        if showVersion {
                fmt.Println("SQL Replay Tool Version:", version)
                os.Exit(0)
        }

        if mode == "" {
                printUsage()
                os.Exit(1)
        }

        switch mode {
        case "parsemysqlslow":
                ParseLogs(slowLogPath, slowOutputPath)

        case "parsetidbslow":
                ParseTiDBLogs(slowLogPath, slowOutputPath)

        case "replay":
                StartSQLReplay(dbConnStr, Speed, slowOutputPath, replayOutputFilePath, filterUsername, filterSQLType, filterDBName, ignoreDigests, lang)

        case "load":
                LoadData(dbConnStr, outDir, replayOut, tableName)

        case "report":
                Report(dbConnStr, replayOut, Port)

        case "replay-kafka":
                idleTTL, err := time.ParseDuration(kIdleTTL)
                if err != nil {
                        fmt.Println("invalid -kafka-idle-ttl:", err)
                        os.Exit(1)
                }
                statsEvery, err := time.ParseDuration(kStatsInterval)
                if err != nil {
                        fmt.Println("invalid -kafka-stats-interval:", err)
                        os.Exit(1)
                }

                        fetchEvery, err := time.ParseDuration(kFetchInterval)
                        if err != nil {
                                fmt.Println("invalid -kafka-fetch-interval:", err)
                                os.Exit(1)
                        }
                        commitEveryDur, err := time.ParseDuration(kCommitInterval)
                        if err != nil {
                                fmt.Println("invalid -kafka-commit-interval:", err)
                                os.Exit(1)
                        }

                if kBrokers == "" || kTopic == "" || kGroup == "" {
                        fmt.Println("replay-kafka requires -kafka-brokers, -kafka-topic, -kafka-group")
                        os.Exit(1)
                }
                if kSASLMech != "" && (kSASLUser == "" || kSASLPass == "") {
                        fmt.Println("SASL requires -kafka-username and -kafka-password")
                        os.Exit(1)
                }

                ctx, cancel := context.WithCancel(context.Background())
                defer cancel()

                opt := KafkaReplayOptions{
                        Brokers:       splitComma(kBrokers),
                        Topic:         kTopic,
                        Group:         kGroup,
                        Start:         kStart,
                        DSN:           kDSN,
                        DefaultDB:     kDefaultDB,
                        IDQueueCap:    kIDQueueCap,
                        IdleTTL:       idleTTL,
                        StatsInterval: statsEvery,
                        TSLayout:      kTSLayout,
                        TSLocation:    kTSLoc,
                        ReplayWithGap: kReplayWithGap,
                        ErrorLogDir:   kErrLogDir,
                        StopOnError:   kStopOnError,

                        SASLMechanism:          strings.ToLower(kSASLMech),
                        SASLUsername:           kSASLUser,
                        SASLPassword:           kSASLPass,
                        TLSEnable:              kEnableTLS,
                        TLSInsecureSkipVerify:  kTLSInsecureSkipVerify,

                                // fetch/commit batching
                                FetchQueueCap:  kFetchQueueCap,
                                FetchBatch:     kFetchBatch,
                                FetchInterval:  fetchEvery,
                                CommitEvery:    kCommitEvery,
                                CommitInterval: commitEveryDur,
                }

                if err := StartKafkaReplay(ctx, opt); err != nil {
                        fmt.Println("replay-kafka error:", err)
                        os.Exit(1)
                }

        default:
                fmt.Println("Invalid mode. Available modes: parsemysqlslow, parsetidbslow, replay, load, report, replay-kafka")
                os.Exit(1)
        }
}

func splitComma(s string) []string {
        if s == "" {
                return nil
        }
        parts := strings.Split(s, ",")
        out := make([]string, 0, len(parts))
        for _, p := range parts {
                p = strings.TrimSpace(p)
                if p != "" {
                        out = append(out, p)
                }
        }
        return out
}

func printUsage() {
        fmt.Println("Usage: ./sql-replay -mode [parsemysqlslow|parsetidbslow|replay|load|report|replay-kafka]")
        fmt.Println("    1. parse mysql slow log:")
        fmt.Println("       ./sql-replay -mode parsemysqlslow -slow-in <path_to_slow_query_log> -slow-out <slow_output_file>")
        fmt.Println("    2. parse tidb slow log:")
        fmt.Println("       ./sql-replay -mode parsetidbslow -slow-in <path_to_slow_query_log> -slow-out <slow_output_file>")
        fmt.Println("    3. replay (from parsed json):")
        fmt.Println("       ./sql-replay -mode replay -db <mysql_connection_string> -speed 1.0 -slow-out <slow_output_file> -replay-out <replay_output_file> -username <all|username> -sqltype <all|select> -dbname <all|dbname> -ignoredigests <d1,d2> -lang <en|zh>")
        fmt.Println("    4. load:")
        fmt.Println("       ./sql-replay -mode load -db <DB_CONN_STRING> -out-dir <DIRECTORY> -replay-name <REPORT_OUT_FILE_NAME> -table <replay_info>")
        fmt.Println("    5. report:")
        fmt.Println("       ./sql-replay -mode report -db <mysql_connection_string> -replay-name <replay name> -port ':8081'")
        fmt.Println("    6. replay-kafka (realtime from Kafka):")
        fmt.Println("       ./sql-replay -mode replay-kafka \\")
        fmt.Println("         -kafka-brokers '10.0.0.1:9092,10.0.0.2:9092' -kafka-topic tx_sql -kafka-group replayer-g1 \\")
        fmt.Println("         -kafka-start auto \\")
        fmt.Println("         -kafka-dsn 'user:pass@tcp(172.16.0.10:4000)/?parseTime=true&multiStatements=true' \\")
        fmt.Println("         -kafka-default-db test1 -kafka-id-queue-cap 1024 -kafka-idle-ttl 1h -kafka-stats-interval 10s \\")
        fmt.Println("         -kafka-ts-layout '2006-01-02 15:04:05.999999' -kafka-ts-loc 'Asia/Tokyo' \\")
        fmt.Println("         -kafka-replay-with-gap=true -kafka-error-log-dir /var/log/sql-replay -kafka-stop-on-error=false \\")
        fmt.Println("         -kafka-sasl-mech plain -kafka-username yourUser -kafka-password 'yourPass' -kafka-tls=true \\")
        fmt.Println("         -kafka-fetch-queue-cap 10000 -kafka-fetch-batch 1000 -kafka-fetch-interval 100ms \\")
        fmt.Println("         -kafka-commit-every 1000 -kafka-commit-interval 200ms")
}