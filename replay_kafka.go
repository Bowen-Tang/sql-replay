package main

import (
        "bufio"
        "bytes"
        "context"
        "database/sql"
        "encoding/json"
        "errors"
        "fmt"
        "net/url"
        "os"
        "path/filepath"
        "strconv"
        "strings"
        "sync"
        "sync/atomic"
        "time"
        "crypto/tls"
        "github.com/segmentio/kafka-go"
        "github.com/segmentio/kafka-go/sasl/plain"
        "github.com/segmentio/kafka-go/sasl/scram"
        _ "github.com/go-sql-driver/mysql"
)

/************
 * CLI 参数 *
 ************/

type KafkaReplayOptions struct {
        Brokers       []string
        Topic         string
        Group         string
        Start         string // auto|committed|earliest|latest ; auto=有位点走位点，否则earliest
        DSN           string // 不带库名：user:pass@tcp(host:port)/?parseTime=true&multiStatements=true
        DefaultDB     string // 事件缺失 db 时的初始库
        IDQueueCap    int
        IdleTTL       time.Duration
        StatsInterval time.Duration
        TSLayout      string // "2006-01-02 15:04:05.999999"
        TSLocation    string // "Asia/Tokyo"
        ReplayWithGap bool   // 是否按 ts 还原间隔
        ErrorLogDir   string // 错误 SQL 输出目录
        StopOnError   bool

        // —— 新增：读取/提交的批量与节流参数 ——
        FetchQueueCap  int           // 全局 FIFO 队列容量（默认 10000）
        FetchBatch     int           // 每次批量拉取条数（默认 1000）
        FetchInterval  time.Duration // 两次批量拉取的周期（默认 100ms）
        CommitEvery    int           // 每分区提交门限：累计 N 条提交（默认 1000）
        CommitInterval time.Duration // 提交的时间门限（默认 200ms）

        // —— 新增：SASL/TLS 连接选项 ——
        SASLMechanism         string // "", "plain", "scram-sha256", "scram-sha512"
        SASLUsername          string
        SASLPassword          string
        TLSEnable             bool
        TLSInsecureSkipVerify bool
}

// 入口：在 main.go 中按 -mode=replay-kafka 调用
func StartKafkaReplay(ctx context.Context, opt KafkaReplayOptions) error {
        if len(opt.Brokers) == 0 || opt.Topic == "" || opt.DSN == "" {
                return errors.New("brokers/topic/dsn are required")
        }
        if opt.IDQueueCap <= 0 {
                opt.IDQueueCap = 1024
        }
        if opt.IdleTTL <= 0 {
                opt.IdleTTL = time.Hour
        }
        if opt.StatsInterval <= 0 {
                opt.StatsInterval = 10 * time.Second
        }
        if opt.TSLayout == "" {
                opt.TSLayout = "2006-01-02 15:04:05.999999"
        }
        if opt.TSLocation == "" {
                opt.TSLocation = "Local"
        }
        if opt.ErrorLogDir == "" {
                opt.ErrorLogDir = "."
        }
        if opt.Start == "" {
                opt.Start = "auto"
        }

        // 新增：默认读取/提交批量参数
        if opt.FetchQueueCap <= 0 {
                opt.FetchQueueCap = 20000
        }
        if opt.FetchBatch <= 0 {
                opt.FetchBatch = 1000
        }
        if opt.FetchInterval <= 0 {
                opt.FetchInterval = 10 * time.Millisecond
        }
        if opt.CommitEvery <= 0 {
                opt.CommitEvery = 1000
        }
        if opt.CommitInterval <= 0 {
                opt.CommitInterval = 10 * time.Millisecond
        }

        loc, err := time.LoadLocation(opt.TSLocation)
        if err != nil {
                return fmt.Errorf("load time location: %w", err)
        }

        // 关键点：Group 模式 + StartOffset=earliest，即可实现“有位点用位点；无位点从头”
        startOffset := kafka.FirstOffset
        if strings.EqualFold(opt.Start, "latest") {
                startOffset = kafka.LastOffset
        }
        if strings.EqualFold(opt.Start, "committed") {
                // 若组无位点，Reader仍会用 StartOffset；我们选择 earliest（更安全）
                startOffset = kafka.FirstOffset
        }

        // —— 新增：根据 SASL/TLS 选项构建 Dialer ——
        dialer, err := buildDialer(opt)
        if err != nil {
                return err
        }


        reader := kafka.NewReader(kafka.ReaderConfig{
                Brokers:     opt.Brokers,
                Topic:       opt.Topic,
                GroupID:     opt.Group,   // 使用消费组，才能自动恢复“上一次位点”
                StartOffset: startOffset, // 组内无位点时才生效；auto=earliest 即满足“无则从头”
                MaxBytes:    10e6,
                Dialer:      dialer,      // << 启用 SASL/TLS
        })
        defer reader.Close()

        fmt.Printf("[kafka-replay] brokers=%v topic=%s group=%s start=%s sasl=%s tls=%v\n",
                opt.Brokers, opt.Topic, opt.Group, opt.Start, saslSummary(opt), opt.TLSEnable)

        commitCh := make(chan kafka.Message, 4096)
        // 批量位点提交（每分区 N 条或每 T 时间）
        go commitLoop(ctx, reader, opt.Topic, opt, commitCh)

        parts := newPartitionSet()
        dbf := &DBFactory{baseDSN: opt.DSN, m: make(map[string]*sql.DB)}
        errlog := &ErrorLogger{dir: opt.ErrorLogDir}

        startTime := time.Now()
        go statsLoop(ctx, parts, startTime, opt.StatsInterval)

        // —— 新增：全局 FIFO 入口队列 + 两段式流水线（抓取 → 分发） ——
        ingressQ := make(chan kafka.Message, opt.FetchQueueCap)
        go fetchLoop(ctx, reader, opt, ingressQ)                                 // 每 100ms 批量抓 1000，队列满则阻塞
        go dispatchLoop(ctx, ingressQ, parts, dbf, errlog, opt, loc, commitCh)   // FIFO 逐条解析并分发到各分区 runner

        <-ctx.Done()
        parts.CloseAll()
        return ctx.Err()
}

/****************
 * 结构与实现层 *
 ****************/

// —— 新增：根据选项创建支持 SASL/TLS 的 Dialer ——
func buildDialer(opt KafkaReplayOptions) (*kafka.Dialer, error) {
        d := &kafka.Dialer{
                Timeout:   10 * time.Second,
                DualStack: true,
        }
        if opt.TLSEnable {
                d.TLS = &tls.Config{
                        InsecureSkipVerify: opt.TLSInsecureSkipVerify,
                        MinVersion:         tls.VersionTLS12,
                }
        }
        switch strings.ToLower(strings.TrimSpace(opt.SASLMechanism)) {
        case "":
                // no SASL
        case "plain":
                d.SASLMechanism = plain.Mechanism{
                        Username: opt.SASLUsername,
                        Password: opt.SASLPassword,
                }
        case "scram-sha256":
                mech, err := scram.Mechanism(scram.SHA256, opt.SASLUsername, opt.SASLPassword)
                if err != nil {
                        return nil, fmt.Errorf("scram-sha256: %w", err)
                }
                d.SASLMechanism = mech
        case "scram-sha512":
                mech, err := scram.Mechanism(scram.SHA512, opt.SASLUsername, opt.SASLPassword)
                if err != nil {
                        return nil, fmt.Errorf("scram-sha512: %w", err)
                }
                d.SASLMechanism = mech
        default:
                return nil, fmt.Errorf("unsupported SASL mechanism: %s", opt.SASLMechanism)
        }
        return d, nil
}

func saslSummary(opt KafkaReplayOptions) string {
        if opt.SASLMechanism == "" {
                return "none"
        }
        if opt.SASLUsername != "" {
                return opt.SASLMechanism + "(user=" + opt.SASLUsername + ")"
        }
        return opt.SASLMechanism
}


// Kafka 事件（解析后的）
type Event struct {
        ID    uint64
        Query string
        TS    time.Time
        DB    string // 初次建连用；后续切库由 SQL 自身的 USE 完成
}

// —— 精确解码：id 可能很大，必须用 UseNumber；parseTS=false 时跳过 ts 解析（省 CPU）
func decodeEvent(m kafka.Message, layout string, loc *time.Location, parseTS bool) (Event, error) {
        type wire struct {
                ID        json.RawMessage `json:"id"`
                Query     string          `json:"query"`
                QueryType string          `json:"query_type"`
                TS        string          `json:"ts"`
                DB        string          `json:"db"`
        }
        var w wire
        dec := json.NewDecoder(bytes.NewReader(m.Value))
        dec.UseNumber()
        if err := dec.Decode(&w); err != nil {
                return Event{}, err
        }

        // parse id
        var id uint64
        {
                // 先按数字解析
                var num json.Number
                if err := json.Unmarshal(w.ID, &num); err == nil {
                        u, e := parseUint64(num)
                        if e != nil {
                                return Event{}, fmt.Errorf("parse id: %w", e)
                        }
                        id = u
                } else {
                        // 再尝试字符串数字
                        var s string
                        if e2 := json.Unmarshal(w.ID, &s); e2 != nil {
                                return Event{}, fmt.Errorf("id neither number nor string: %v/%v", err, e2)
                        }
                        u, e3 := parseUint64(json.Number(s))
                        if e3 != nil {
                                return Event{}, fmt.Errorf("parse id string: %w", e3)
                        }
                        id = u
                }
        }

        // parse ts（可选）
        var t time.Time
        if parseTS {
                tp, err := time.ParseInLocation(layout, strings.TrimSpace(w.TS), loc)
                if err != nil {
                        return Event{}, fmt.Errorf("parse ts: %w, raw=%q", err, w.TS)
                }
                t = tp
        }
        return Event{ID: id, Query: w.Query, TS: t, DB: w.DB}, nil
}

func parseUint64(n json.Number) (uint64, error) {
        s := n.String()
        if u, err := strconv.ParseUint(s, 10, 64); err == nil {
                return u, nil
        }
        if i, err := strconv.ParseInt(s, 10, 64); err == nil && i >= 0 {
                return uint64(i), nil
        }
        return 0, fmt.Errorf("not uint64: %s", s)
}

/************
 * 抓取 & 分发 *
 ************/

// 批量抓取：每 FetchInterval 拉取至多 FetchBatch 条，入口 FIFO 满时阻塞等待空位
func fetchLoop(ctx context.Context, reader *kafka.Reader, opt KafkaReplayOptions, out chan<- kafka.Message) {
        ticker := time.NewTicker(opt.FetchInterval)
        defer ticker.Stop()
        for {
                select {
                case <-ctx.Done():
                        return
                case <-ticker.C:
                        for i := 0; i < opt.FetchBatch; i++ {
                                msg, err := reader.FetchMessage(ctx)
                                if err != nil {
                                        if errors.Is(err, context.Canceled) {
                                                return
                                        }
                                        fmt.Printf("[kafka-replay] fetch error: %v\n", err)
                                        time.Sleep(100 * time.Millisecond)
                                        break
                                }
                                // 队列满会在这里阻塞，直到有空位，满足你的“没空位就等待”
                                out <- msg
                        }
                }
        }
}

// 全局 FIFO 分发：严格 FIFO 出队，解析后按分区分发到 PartitionRunner
func dispatchLoop(
        ctx context.Context,
        in <-chan kafka.Message,
        parts *partitionSet,
        dbf *DBFactory,
        errlog *ErrorLogger,
        opt KafkaReplayOptions,
        loc *time.Location,
        commitCh chan<- kafka.Message,
) {
        for {
                select {
                case <-ctx.Done():
                        return
                case msg := <-in:
                        evt, derr := decodeEvent(msg, opt.TSLayout, loc, opt.ReplayWithGap)
                        if derr != nil {
                                _ = errlog.Append(msg.Partition, msg.Offset, 0, "", fmt.Errorf("decode: %w", derr), string(msg.Value))
                                commitCh <- kafka.Message{Topic: opt.Topic, Partition: msg.Partition, Offset: msg.Offset}
                                continue
                        }
                        pr := parts.GetOrCreate(msg.Partition, func() *PartitionRunner {
                                return NewPartitionRunner(msg.Partition, commitCh, dbf, errlog, opt)
                        })
                        pr.In() <- partitionEvent{Msg: msg, Evt: evt}
                }
        }
}

/*** 错误日志 ***/

type ErrorLogger struct {
        dir string
        mu  sync.Mutex
}

func (l *ErrorLogger) Append(partition int, offset int64, id uint64, db string, err error, sqlText string) error {
        l.mu.Lock()
        defer l.mu.Unlock()
        day := time.Now().Format("20060102")
        path := filepath.Join(l.dir, fmt.Sprintf("replay_errors_%s.log", day))
        f, ferr := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
        if ferr != nil {
                return ferr
        }
        defer f.Close()
        bw := bufio.NewWriterSize(f, 64*1024)
        line := fmt.Sprintf("%s\tpart=%d\toffset=%d\tid=%d\tdb=%s\terr=%v\tSQL: %s\n",
                time.Now().Format(time.RFC3339Nano), partition, offset, id, db, err, strings.TrimSpace(sqlText))
        _, werr := bw.WriteString(line)
        if werr == nil {
                werr = bw.Flush()
        }
        return werr
}

/*** DB 工厂：按库名拿 *sql.DB；idWorker 再从该 DB 获取 **专属** *sql.Conn ***/

type DBFactory struct {
        baseDSN string
        mu      sync.Mutex
        m       map[string]*sql.DB // dbName -> *sql.DB（连接池对象，可复用）
}

func (f *DBFactory) Get(dbName string) (*sql.DB, error) {
        if dbName == "" {
                return nil, errors.New("empty dbName")
        }
        f.mu.Lock()
        defer f.mu.Unlock()
        if db := f.m[dbName]; db != nil {
                return db, nil
        }
        dsn := attachDBName(f.baseDSN, dbName)
        db, err := sql.Open("mysql", dsn)
        if err != nil {
                return nil, err
        }
        // 这里可按需设置池参数（不影响“每 id 一条 *sql.Conn”的强约束）
        f.m[dbName] = db
        return db, nil
}

func attachDBName(baseDSN, db string) string {
        if strings.Contains(baseDSN, "/?") {
                return strings.Replace(baseDSN, "/?", "/"+url.QueryEscape(db)+"?", 1)
        }
        if strings.HasSuffix(baseDSN, "/") {
                return baseDSN + url.QueryEscape(db)
        }
        return baseDSN + "/" + url.QueryEscape(db)
}

/*** idWorker：同一 id 串行、同一物理连接（绝不与其他 id 共用） ***/

type idWorker struct {
        id         uint64
        initialDB  string
        q          chan idJob
        conn       *sql.Conn       // 专属连接：仅此 id 使用
        lastTS     time.Time
        prevExec   time.Duration
        lastActive atomic.Int64 // unix seconds
        busy       atomic.Int64 // >0 表示执行中
        parent     *PartitionRunner
        dbf        *DBFactory
}

type idJob struct {
        Msg kafka.Message
        Evt Event
}

func (w *idWorker) run(ctx context.Context) {
        defer func() {
                if w.conn != nil {
                        _ = w.conn.Close()
                        totalConns.Add(-1)
                }
        }()

        for job := range w.q {
                now := time.Now().Unix()
                w.lastActive.Store(now)

                // —— 首次固定“专属连接”：从对应库的 *sql.DB 获取 **独占** *sql.Conn
                if w.conn == nil {
                        dbName := w.initialDB
                        if dbName == "" {
                                dbName = w.parent.opt.DefaultDB
                        }
                        db, err := w.dbf.Get(dbName)
                        if err != nil {
                                _ = w.parent.errlog.Append(job.Msg.Partition, job.Msg.Offset, w.id, dbName, err, job.Evt.Query)
                                w.parent.ackDone(job.Msg.Partition, job.Msg.Offset)
                                continue
                        }
                        conn, err := db.Conn(ctx) // 每个 id 一条物理连接；之后不再变更对象引用
                        if err != nil {
                                _ = w.parent.errlog.Append(job.Msg.Partition, job.Msg.Offset, w.id, dbName, err, job.Evt.Query)
                                w.parent.ackDone(job.Msg.Partition, job.Msg.Offset)
                                continue
                        }
                        w.conn = conn
                        totalConns.Add(1)
                }

                // —— 时间间隔复现（同 id）
                if !w.lastTS.IsZero() && w.parent.opt.ReplayWithGap {
                        gap := job.Evt.TS.Sub(w.lastTS) - w.prevExec
                        if gap > 0 {
                                time.Sleep(gap)
                        }
                }

                // —— 直接执行 SQL（包含 USE；在同一连接上自然切库）
                w.busy.Add(1)
                t0 := time.Now()
                _, err := w.conn.ExecContext(ctx, job.Evt.Query)
                execDur := time.Since(t0)
                w.busy.Add(-1)

                w.prevExec = execDur
                w.lastTS = job.Evt.TS

                if err != nil {
                        _ = w.parent.errlog.Append(job.Msg.Partition, job.Msg.Offset, w.id, "", err, job.Evt.Query)
                        if w.parent.opt.StopOnError {
                                os.Exit(1)
                        }
                }

                // —— 位点完成
                w.parent.ackDone(job.Msg.Partition, job.Msg.Offset)
        }
}

/*** PartitionRunner：每分区 1 个，负责分发与连续位点提交 ***/

type PartitionRunner struct {
        partID int
        in     chan partitionEvent

        workers map[uint64]*idWorker
        mu      sync.Mutex

        done       map[int64]bool
        nextCommit int64
        commitOut  chan<- kafka.Message
        errlog     *ErrorLogger
        dbf        *DBFactory
        opt        KafkaReplayOptions
        gcTicker   *time.Ticker
        closeOnce  sync.Once
        closed     chan struct{}
}

type partitionEvent struct {
        Msg kafka.Message
        Evt Event
}

func NewPartitionRunner(partID int, commitOut chan<- kafka.Message, dbf *DBFactory, errlog *ErrorLogger, opt KafkaReplayOptions) *PartitionRunner {
        pr := &PartitionRunner{
                partID:    partID,
                in:        make(chan partitionEvent, 4096),
                workers:   make(map[uint64]*idWorker),
                done:      make(map[int64]bool),
                nextCommit: -1,
                commitOut: commitOut,
                errlog:    errlog,
                dbf:       dbf,
                opt:       opt,
                gcTicker:  time.NewTicker(time.Minute),
                closed:    make(chan struct{}),
        }
        go pr.loop()
        return pr
}

func (pr *PartitionRunner) In() chan<- partitionEvent { return pr.in }

func (pr *PartitionRunner) loop() {
        for {
                select {
                case ev, ok := <-pr.in:
                        if !ok {
                                pr.cleanup()
                                return
                        }
                        if pr.nextCommit == -1 {
                                pr.nextCommit = ev.Msg.Offset
                        }
                        w := pr.getOrCreateWorker(ev.Evt.ID, ev.Evt.DB)
                        // 有界队列，必要时阻塞形成背压，避免内存无限增长
                        w.q <- idJob{Msg: ev.Msg, Evt: ev.Evt}

                case <-pr.gcTicker.C:
                        pr.gcIdle()

                case <-pr.closed:
                        pr.cleanup()
                        return
                }
        }
}

func (pr *PartitionRunner) getOrCreateWorker(id uint64, initDB string) *idWorker {
        pr.mu.Lock()
        defer pr.mu.Unlock()
        if w := pr.workers[id]; w != nil {
                return w
        }
        w := &idWorker{
                id:        id,
                initialDB: initDB,
                q:         make(chan idJob, pr.opt.IDQueueCap),
                parent:    pr,
                dbf:       pr.dbf,
        }
        pr.workers[id] = w
        go w.run(context.Background())
        return w
}

func (pr *PartitionRunner) ackDone(partition int, offset int64) {
        if partition != pr.partID {
                return
        }
        pr.mu.Lock()
        pr.done[offset] = true
        for pr.done[pr.nextCommit] {
                delete(pr.done, pr.nextCommit)
                pr.commitOut <- kafka.Message{Topic: pr.opt.Topic, Partition: pr.partID, Offset: pr.nextCommit}
                pr.nextCommit++
        }
        pr.mu.Unlock()
}

func (pr *PartitionRunner) gcIdle() {
        now := time.Now().Unix()
        pr.mu.Lock()
        for id, w := range pr.workers {
                if now-w.lastActive.Load() > int64(pr.opt.IdleTTL/time.Second) && w.busy.Load() == 0 {
                        close(w.q) // 退出时会关闭并减少连接数
                        delete(pr.workers, id)
                }
        }
        pr.mu.Unlock()
}

func (pr *PartitionRunner) cleanup() {
        pr.gcTicker.Stop()
        pr.mu.Lock()
        for id, w := range pr.workers {
                close(w.q)
                delete(pr.workers, id)
        }
        pr.mu.Unlock()
}

func (pr *PartitionRunner) Close() {
        pr.closeOnce.Do(func() { close(pr.closed) })
}

/*** 分区集合 & 提交 & 统计 ***/

type partitionSet struct {
        mu    sync.Mutex
        parts map[int]*PartitionRunner
}

func newPartitionSet() *partitionSet { return &partitionSet{parts: make(map[int]*PartitionRunner)} }

func (ps *partitionSet) GetOrCreate(partID int, ctor func() *PartitionRunner) *PartitionRunner {
        ps.mu.Lock()
        defer ps.mu.Unlock()
        if p := ps.parts[partID]; p != nil {
                return p
        }
        p := ctor()
        ps.parts[partID] = p
        return p
}

func (ps *partitionSet) Snapshot() []*PartitionRunner {
        ps.mu.Lock()
        defer ps.mu.Unlock()
        arr := make([]*PartitionRunner, 0, len(ps.parts))
        for _, p := range ps.parts {
                arr = append(arr, p)
        }
        return arr
}

func (ps *partitionSet) CloseAll() {
        ps.mu.Lock()
        defer ps.mu.Unlock()
        for _, p := range ps.parts {
                p.Close()
        }
}

// 批量位点提交：对同一分区合并最新 offset；满足 CommitEvery 或 CommitInterval 时提交
func commitLoop(ctx context.Context, reader *kafka.Reader, topic string, opt KafkaReplayOptions, ch <-chan kafka.Message) {
        ticker := time.NewTicker(opt.CommitInterval)
        defer ticker.Stop()
        latest := make(map[int]int64)    // partition -> 最新 offset
        committed := make(map[int]int64) // partition -> 已提交 offset
        for {
                select {
                case <-ctx.Done():
                        return
                case m := <-ch:
                        if cur, ok := latest[m.Partition]; !ok || m.Offset > cur {
                                latest[m.Partition] = m.Offset
                        }
                        if opt.CommitEvery > 0 && latest[m.Partition]-committed[m.Partition] >= int64(opt.CommitEvery) {
                                if err := reader.CommitMessages(ctx, kafka.Message{Topic: topic, Partition: m.Partition, Offset: latest[m.Partition]}); err != nil {
                                        fmt.Printf("[kafka-replay] commit error: part=%d off=%d err=%v\n", m.Partition, latest[m.Partition], err)
                                } else {
                                        committed[m.Partition] = latest[m.Partition]
                                }
                        }
                case <-ticker.C:
                        for p, off := range latest {
                                if off > committed[p] {
                                        if err := reader.CommitMessages(ctx, kafka.Message{Topic: topic, Partition: p, Offset: off}); err != nil {
                                                fmt.Printf("[kafka-replay] commit error: part=%d off=%d err=%v\n", p, off, err)
                                                continue
                                        }
                                        committed[p] = off
                                }
                        }
                }
        }
}

var totalConns atomic.Int64 // 打开的 *sql.Conn 数（= 活跃 id 数）

func statsLoop(ctx context.Context, parts *partitionSet, start time.Time, every time.Duration) {
        t := time.NewTicker(every)
        defer t.Stop()
        for {
                select {
                case <-ctx.Done():
                        return
                case <-t.C:
                        prs := parts.Snapshot()
                        var activeIDs, activeConns int64
                        for _, pr := range prs {
                                pr.mu.Lock()
                                activeIDs += int64(len(pr.workers))
                                for _, w := range pr.workers {
                                        if w.busy.Load() > 0 || len(w.q) > 0 {
                                                activeConns++
                                        }
                                }
                                pr.mu.Unlock()
                        }
                        fmt.Printf("[stats] up=%s parts=%d active_ids=%d total_conns=%d active_conns=%d\n",
                                time.Since(start).Truncate(time.Second),
                                len(prs),
                                activeIDs,
                                totalConns.Load(), // == 打开的专属连接数
                                activeConns,       // 正在执行或队列非空的连接数
                        )
                }
        }
}