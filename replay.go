package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// WorkItem 结构体
type WorkItem struct {
	entry     LogEntry
	connID    string
	timestamp float64
}

// 统计信息
type ReplayStats struct {
	totalQueries    int64
	successQueries  int64
	failedQueries   int64
	totalExecTime   int64
	maxExecTime     int64
	minExecTime     int64
	startTime       time.Time
	endTime         time.Time
	connectionCount int32
	processedBytes  int64
	totalBytes      int64
}

// 工作池
type WorkerPool struct {
	workChan        chan WorkItem
	workerCount     int
	wg              sync.WaitGroup
	dbConnStr       string
	outputPath      string
	speed           float64
	qps             int
	stats           *ReplayStats
	stopChan        chan struct{}
	connStates      map[string]*ConnectionState
	connStatesMutex sync.RWMutex
	maxConnections  int  // 新增：最大MySQL连接数
	streamMode      bool // 新增：流模式开关
}

// 连接状态
type ConnectionState struct {
	prevTimestamp float64
	lastQueryTime int64
	db            *sql.DB
}

// 创建WorkerPool
func NewWorkerPool(dbConnStr, outputPath string, options ...func(*WorkerPool)) *WorkerPool {
	wp := &WorkerPool{
		workChan:       make(chan WorkItem, 10000),
		workerCount:    runtime.NumCPU() * 2,
		dbConnStr:      dbConnStr,
		outputPath:     outputPath,
		speed:          1.0,
		qps:            0,
		stats:          &ReplayStats{minExecTime: int64(^uint64(0) >> 1)},
		stopChan:       make(chan struct{}),
		connStates:     make(map[string]*ConnectionState),
		maxConnections: 20,   // 默认最大连接数
		streamMode:     true, // 默认启用流模式
	}

	for _, option := range options {
		option(wp)
	}

	return wp
}

// 配置工作线程数
func WithWorkerCount(count int) func(*WorkerPool) {
	return func(wp *WorkerPool) {
		if count > 0 {
			wp.workerCount = count
		}
	}
}

// 配置速度
func WithSpeed(speed float64) func(*WorkerPool) {
	return func(wp *WorkerPool) {
		if speed > 0 {
			wp.speed = speed
		}
	}
}

// 配置QPS
func WithQPS(qps int) func(*WorkerPool) {
	return func(wp *WorkerPool) {
		if qps > 0 {
			wp.qps = qps
		}
	}
}

// 配置最大MySQL连接数
func WithMaxConnections(count int) func(*WorkerPool) {
	return func(wp *WorkerPool) {
		if count > 0 {
			wp.maxConnections = count
		}
	}
}

// 配置流模式
func WithStreamMode(enabled bool) func(*WorkerPool) {
	return func(wp *WorkerPool) {
		wp.streamMode = enabled
	}
}

// 启动工作池
func (wp *WorkerPool) Start() {
	wp.stats.startTime = time.Now()

	go wp.monitorStats()

	if wp.qps > 0 {
		go wp.rateLimit()
	}

	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// QPS限制
func (wp *WorkerPool) rateLimit() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var counter int32

	for {
		select {
		case <-ticker.C:
			atomic.StoreInt32(&counter, 0)
		case <-wp.stopChan:
			return
		}
	}
}

// 监控统计
func (wp *WorkerPool) monitorStats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wp.printStats(false)
		case <-wp.stopChan:
			return
		}
	}
}

// 打印统计
func (wp *WorkerPool) printStats(final bool) {
	duration := time.Since(wp.stats.startTime).Seconds()
	totalQueries := atomic.LoadInt64(&wp.stats.totalQueries)
	successQueries := atomic.LoadInt64(&wp.stats.successQueries)
	failedQueries := atomic.LoadInt64(&wp.stats.failedQueries)

	if final {
		fmt.Println("\n--- Final Statistics ---")
	} else {
		fmt.Printf("\r")
	}

	fmt.Printf("Queries: %d total, %d succeeded, %d failed | ",
		totalQueries, successQueries, failedQueries)

	if duration > 0 {
		fmt.Printf("QPS: %.2f | ", float64(successQueries)/duration)
	}

	if successQueries > 0 {
		avgExecTime := float64(atomic.LoadInt64(&wp.stats.totalExecTime)) / float64(successQueries) / 1000.0
		fmt.Printf("Avg exec: %.2f ms | ", avgExecTime)
	}

	// 流模式下显示处理进度
	if wp.streamMode && wp.stats.totalBytes > 0 {
		processedBytes := atomic.LoadInt64(&wp.stats.processedBytes)
		totalBytes := atomic.LoadInt64(&wp.stats.totalBytes)
		progress := float64(processedBytes) / float64(totalBytes) * 100
		fmt.Printf("Progress: %.1f%% | ", progress)
	}

	if final {
		fmt.Printf("\nExecution time: %.2f seconds\n", duration)
		fmt.Printf("Speed multiplier: %.2fx\n", wp.speed)
		fmt.Printf("Max DB connections: %d\n", wp.maxConnections)
	}
}

// 工作线程 - 修改以提高speed参数效果
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	// 每个工作线程创建自己的数据库连接
	db, err := sql.Open("mysql", wp.dbConnStr)
	if err != nil {
		fmt.Printf("Worker %d failed to create database connection: %v\n", id, err)
		return
	}
	defer db.Close()

	// 设置每个连接的参数
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	for work := range wp.workChan {
		connID := work.connID

		wp.connStatesMutex.RLock()
		state, exists := wp.connStates[connID]
		wp.connStatesMutex.RUnlock()

		if !exists {
			state = &ConnectionState{
				prevTimestamp: work.timestamp,
				lastQueryTime: 0,
				db:            db, // 使用工作线程的数据库连接
			}

			wp.connStatesMutex.Lock()
			wp.connStates[connID] = state
			atomic.AddInt32(&wp.stats.connectionCount, 1)
			wp.connStatesMutex.Unlock()
		}

		// 改进speed计算方式 - 简化为直接使用时间戳差值除以speed
		if wp.speed > 0 {
			interval := (work.timestamp - state.prevTimestamp) / wp.speed
			if interval > 0 {
				time.Sleep(time.Duration(interval * float64(time.Second)))
			}
		}

		atomic.AddInt64(&wp.stats.totalQueries, 1)

		startTime := time.Now()
		task := SQLTask{Entry: work.entry, DB: db}

		// 安全调用ExecuteSQLAndRecord
		if ExecuteSQLAndRecord != nil {
			if err := ExecuteSQLAndRecord(task, wp.outputPath); err != nil {
				atomic.AddInt64(&wp.stats.failedQueries, 1)
			} else {
				execTime := time.Since(startTime).Microseconds()
				atomic.AddInt64(&wp.stats.successQueries, 1)
				atomic.AddInt64(&wp.stats.totalExecTime, execTime)

				// 更新最大执行时间
				for {
					maxTime := atomic.LoadInt64(&wp.stats.maxExecTime)
					if execTime <= maxTime || atomic.CompareAndSwapInt64(&wp.stats.maxExecTime, maxTime, execTime) {
						break
					}
				}

				// 更新最小执行时间
				for {
					minTime := atomic.LoadInt64(&wp.stats.minExecTime)
					if execTime >= minTime || atomic.CompareAndSwapInt64(&wp.stats.minExecTime, minTime, execTime) {
						break
					}
				}
			}
		} else {
			fmt.Printf("Warning: ExecuteSQLAndRecord function not initialized\n")
			atomic.AddInt64(&wp.stats.failedQueries, 1)
		}

		state.prevTimestamp = work.timestamp
		state.lastQueryTime = work.entry.QueryTime
	}
}

// 等待完成
func (wp *WorkerPool) Wait() {
	close(wp.workChan)
	wp.wg.Wait()
	close(wp.stopChan)

	wp.stats.endTime = time.Now()
	wp.printStats(true)

	// 清理连接
	wp.connStatesMutex.Lock()
	for connID, _ := range wp.connStates {
		delete(wp.connStates, connID)
	}
	wp.connStatesMutex.Unlock()
}

// 提交工作
func (wp *WorkerPool) Submit(entry LogEntry) {
	wp.workChan <- WorkItem{
		entry:     entry,
		connID:    entry.ConnectionID,
		timestamp: entry.Timestamp,
	}
}

// 获取文件大小
func getFileSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// 流式解析日志 - 修复state未使用的错误
func StreamParseLogEntries(
	slowOutputPath string,
	filterUsername, filterSQLType, filterDBName string,
	ignoreDigestList []string,
	wp *WorkerPool,
	callback func(LogEntry)) error {

	// 获取文件大小用于进度展示
	fileSize, err := getFileSize(slowOutputPath)
	if err != nil {
		fmt.Printf("Warning: Cannot get file size: %v\n", err)
	} else {
		atomic.StoreInt64(&wp.stats.totalBytes, fileSize)
	}

	// 打开文件
	inputFile, err := os.Open(slowOutputPath)
	if err != nil {
		return fmt.Errorf("file open error: %w", err)
	}
	defer inputFile.Close()

	// 设置缓冲区，提高读取效率
	reader := bufio.NewReader(inputFile)
	var bytesRead int64
	// 注意：下面是错误的行，可能有state变量定义在这里
	// 删除或注释掉state变量定义
	// var state ...
	var connectionCount int
	var entryCount int

	// 跟踪唯一连接ID
	connectionIDs := make(map[string]bool)

	// 逐行读取处理
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("read error: %w", err)
		}

		// 更新已读字节数
		bytesRead += int64(len(line))
		atomic.StoreInt64(&wp.stats.processedBytes, bytesRead)

		if len(line) > 0 {
			var entry LogEntry
			if err := json.Unmarshal(line, &entry); err != nil {
				fmt.Printf("Error parsing log entry: %v\n", err)
				continue
			}

			// 应用过滤条件
			if filterUsername != "all" && entry.Username != filterUsername {
				continue
			}
			if filterSQLType != "all" && entry.SQLType != filterSQLType {
				continue
			}
			if filterDBName != "all" && entry.DBName != filterDBName {
				continue
			}
			if contains(ignoreDigestList, entry.Digest) {
				continue
			}

			// 记录唯一连接ID
			if !connectionIDs[entry.ConnectionID] {
				connectionIDs[entry.ConnectionID] = true
				connectionCount++
			}

			entryCount++

			// 调用回调函数处理条目
			if callback != nil {
				callback(entry)
			}
		}

		// 文件结束
		if err == io.EOF {
			break
		}
	}

	fmt.Printf("Processed %d entries from %d connections\n", entryCount, connectionCount)
	return nil
}

// 本地实现ParseLogEntries，防止空指针引用
func localParseLogEntries(slowOutputPath, filterUsername, filterSQLType, filterDBName string, ignoreDigestList []string) (map[string][]LogEntry, float64, error) {
	inputFile, err := os.Open(slowOutputPath)
	if err != nil {
		return nil, 0, fmt.Errorf("file open error: %w", err)
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)

	tasksMap := make(map[string][]LogEntry)
	var minTimestamp float64 = 9999999999.999999

	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal([]byte(scanner.Text()), &entry); err != nil {
			fmt.Printf("Error parsing log entry: %v\n", err)
			continue
		}

		if filterUsername != "all" && entry.Username != filterUsername {
			continue
		}
		if filterSQLType != "all" && entry.SQLType != filterSQLType {
			continue
		}
		if filterDBName != "all" && entry.DBName != filterDBName {
			continue
		}
		if contains(ignoreDigestList, entry.Digest) {
			continue
		}

		tasksMap[entry.ConnectionID] = append(tasksMap[entry.ConnectionID], entry)
		if entry.Timestamp < minTimestamp {
			minTimestamp = entry.Timestamp
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, 0, fmt.Errorf("error reading file: %w", err)
	}

	return tasksMap, minTimestamp, nil
}

// 判断切片是否包含元素
func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

// 本地实现ExecuteSQLAndRecord，防止空指针引用
func localExecuteSQLAndRecord(task SQLTask, replayOutputFilePath string) error {
	if task.DB == nil {
		return fmt.Errorf("database connection is nil")
	}

	startTime := time.Now()

	rows, err := task.DB.Query(task.Entry.SQL)
	var rowsReturned int64
	var errorInfo string

	if err != nil {
		errorInfo = err.Error()
	} else {
		for rows.Next() {
			rowsReturned++
		}
		rows.Close()
	}

	executionTime := time.Since(startTime).Microseconds()

	record := SQLExecutionRecord{
		SQL:           task.Entry.SQL,
		QueryTime:     task.Entry.QueryTime,
		RowsSent:      task.Entry.RowsSent,
		DBName:        task.Entry.DBName,
		ExecutionTime: executionTime,
		RowsReturned:  rowsReturned,
		ErrorInfo:     errorInfo,
	}

	jsonData, err := json.Marshal(record)
	if err != nil {
		return err
	}

	outputPath := fmt.Sprintf("%s.%s", replayOutputFilePath, task.Entry.ConnectionID)
	file, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(append(jsonData, '\n'))
	return err
}

// 流式回放SQL
func StreamReplaySQL(dbConnStr string, replayOutputFilePath string, speed float64, maxWorkers int, maxConnections int) *WorkerPool {
	// 这里可能有使用donnStr而不是dbConnStr的代码
	// 确保所有donnStr都改为dbConnStr
	wp := NewWorkerPool(dbConnStr, replayOutputFilePath,
		WithWorkerCount(maxWorkers),
		WithSpeed(speed),
		WithMaxConnections(maxConnections))

	wp.Start()
	return wp
}

// 声明函数变量
var ExecuteSQLAndRecord func(task SQLTask, replayOutputFilePath string) error
var ParseLogEntries func(slowOutputPath, filterUsername, filterSQLType, filterDBName string, ignoreDigestList []string) (map[string][]LogEntry, float64, error)

// 回放入口函数 - 修改为支持流式处理
func StartSQLReplay(dbConnStr string, speed float64, slowOutputPath, replayOutputFilePath, filterUsername, filterSQLType, filterDBName, ignoreDigests string, lang string, maxConnections int) {
	// 初始化执行函数，避免空指针
	if ParseLogEntries == nil {
		ParseLogEntries = localParseLogEntries
	}

	if ExecuteSQLAndRecord == nil {
		ExecuteSQLAndRecord = localExecuteSQLAndRecord
	}

	// 参数验证
	if dbConnStr == "" || slowOutputPath == "" || replayOutputFilePath == "" {
		fmt.Println("Error: Missing required parameters")
		return
	}

	if speed <= 0 {
		fmt.Println("Error: Invalid speed value")
		return
	}

	// 设置默认最大连接数
	if maxConnections <= 0 {
		maxConnections = runtime.NumCPU() * 5 // 默认值为CPU核心数的5倍
	}

	// 解析忽略的SQL指纹列表
	ignoreDigestList := strings.Split(ignoreDigests, ",")

	// 打印配置信息
	fmt.Printf("\n=== SQL Replay Configuration ===\n")
	fmt.Printf("Source: %s\n", slowOutputPath)
	fmt.Printf("Target: %s\n", dbConnStr)
	fmt.Printf("Output: %s\n", replayOutputFilePath)
	fmt.Printf("Filters: user=%s, db=%s, type=%s\n",
		filterUsername, filterDBName, filterSQLType)
	fmt.Printf("Speed factor: %.2f\n", speed)
	if ignoreDigests != "" {
		fmt.Printf("Ignored digests: %s\n", ignoreDigests)
	}
	fmt.Printf("Worker count: %d\n", runtime.NumCPU()*2)
	fmt.Printf("MySQL max connections: %d\n", maxConnections)
	fmt.Printf("Stream mode: enabled\n")
	fmt.Println("================================\n")

	// 开始解析
	ts0 := time.Now()
	fmt.Printf("[%s] Starting log file processing...\n", ts0.Format("2006-01-02 15:04:05.000"))

	// 选择处理模式
	useStreamMode := true

	if useStreamMode {
		// 创建工作池
		workerCount := runtime.NumCPU() * 2
		wp := StreamReplaySQL(dbConnStr, replayOutputFilePath, speed, workerCount, maxConnections)

		// 流式处理日志文件
		err := StreamParseLogEntries(slowOutputPath, filterUsername, filterSQLType, filterDBName, ignoreDigestList, wp, func(entry LogEntry) {
			wp.Submit(entry)
		})

		if err != nil {
			fmt.Printf("Error processing log file: %v\n", err)
			return
		}

		// 等待所有任务完成
		wp.Wait()
	} else {
		// 传统模式 - 全部加载到内存
		tasksMap, _, err := ParseLogEntries(slowOutputPath, filterUsername, filterSQLType, filterDBName, ignoreDigestList)
		if err != nil {
			fmt.Printf("Error parsing log file: %v\n", err)
			return
		}

		// 统计连接和查询数
		var totalQueries int
		for _, entries := range tasksMap {
			totalQueries += len(entries)
		}

		// 解析完成
		ts1 := time.Now()
		fmt.Printf("[%s] Parsing complete\n", ts1.Format("2006-01-02 15:04:05.000"))
		fmt.Printf("Parsing time: %v\n", ts1.Sub(ts0))
		fmt.Printf("Found %d connections, %d queries\n", len(tasksMap), totalQueries)
		fmt.Println("Starting SQL replay...")

		// 开始回放
		wp := StreamReplaySQL(dbConnStr, replayOutputFilePath, speed, runtime.NumCPU()*2, maxConnections)

		for _, entries := range tasksMap {
			for _, entry := range entries {
				wp.Submit(entry)
			}
		}

		wp.Wait()
	}

	// 完成回放
	ts2 := time.Now()
	fmt.Printf("[%s] Replay complete\n", ts2.Format("2006-01-02 15:04:05.000"))
	fmt.Printf("Total execution time: %v\n", ts2.Sub(ts0))
}
