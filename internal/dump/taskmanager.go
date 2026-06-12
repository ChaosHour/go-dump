package dump

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ChaosHour/go-dump/internal/log"
	"github.com/go-sql-driver/mysql"
)

// txRunner abstracts over *sql.Tx (FTWRL path) and *sql.Conn (no-lock path) so
// workers can prepare statements and commit without knowing which lock strategy was used.
type txRunner interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type txTxRunner struct{ tx *sql.Tx }

func (r *txTxRunner) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return r.tx.PrepareContext(ctx, query)
}
func (r *txTxRunner) Commit(_ context.Context) error   { return r.tx.Commit() }
func (r *txTxRunner) Rollback(_ context.Context) error { return r.tx.Rollback() }

type txConnRunner struct{ conn *sql.Conn }

func (r *txConnRunner) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return r.conn.PrepareContext(ctx, query)
}
func (r *txConnRunner) Commit(ctx context.Context) error {
	_, err := r.conn.ExecContext(ctx, "COMMIT")
	return err
}
func (r *txConnRunner) Rollback(ctx context.Context) error {
	_, err := r.conn.ExecContext(ctx, "ROLLBACK")
	return err
}

type TaskManager struct {
	CreateChunksWaitGroup  *sync.WaitGroup
	ProcessChunksWaitGroup *sync.WaitGroup
	ChunksChannel          chan DataChunk
	DB                     *sql.DB
	ThreadsCount           int
	tasksPool              []*Task
	workers                []txRunner // one per worker thread; type depends on lock strategy
	workersDB              []*sql.DB
	databaseEngines        map[string]*Table
	TotalChunks            int64
	Queue                  int64
	CompletedChunks        int64 // incremented atomically by workers as each chunk finishes
	StartTime              time.Time
	DestinationDir         string
	TablesWithoutPKOption  string
	SkipUseDatabase        bool
	GetMasterStatus        bool
	GetSlaveStatus         bool
	Compress               bool
	CompressLevel          int
	IsolationLevel         sql.IsolationLevel
	mySQLHost              *MySQLHost
	mySQLCredentials       *MySQLCredentials
	DumpOptions            *DumpOptions
	ctx                    context.Context
	mysqlVersion           [3]int // [major, minor, patch], populated by detectMySQLVersion
	metadata               *DumpMetadata // set via SetMetadata before workers start; nil in dry-run

	// Binlog info captured by getMasterData, written to metadata.
	BinlogFile     string
	BinlogPosition int
	GTIDSet        string
}

func NewTaskManager(
	wgC *sync.WaitGroup,
	wgP *sync.WaitGroup,
	cDC chan DataChunk,
	db *sql.DB,
	dumpOptions *DumpOptions) TaskManager {

	return TaskManager{
		CreateChunksWaitGroup:  wgC,
		ProcessChunksWaitGroup: wgP,
		ChunksChannel:          cDC,
		DB:                     db,
		databaseEngines:        make(map[string]*Table),
		ThreadsCount:           dumpOptions.Threads,
		DestinationDir:         dumpOptions.DestinationDir,
		TablesWithoutPKOption:  dumpOptions.TablesWithoutUKOption,
		SkipUseDatabase:        dumpOptions.SkipUseDatabase,
		GetMasterStatus:        dumpOptions.GetMasterStatus,
		GetSlaveStatus:         dumpOptions.GetSlaveStatus,
		Compress:               dumpOptions.Compress,
		CompressLevel:          dumpOptions.CompressLevel,
		IsolationLevel:         dumpOptions.IsolationLevel,
		mySQLHost:              dumpOptions.MySQLHost,
		mySQLCredentials:       dumpOptions.MySQLCredentials,
		DumpOptions:            dumpOptions,
		ctx:                    context.Background(),
		// Set here (and refreshed in Run before PrintStatus starts) so the
		// progress goroutine never reads a zero or concurrently-written value.
		StartTime: time.Now(),
	}
}

// SetContext sets the context used for transactions and DB operations.
func (tm *TaskManager) SetContext(ctx context.Context) {
	tm.ctx = ctx
}

// SetMetadata attaches the dump metadata so workers can mark tables done as
// they finish. Must be called before StartWorkers/CreateChunks to avoid races.
func (tm *TaskManager) SetMetadata(meta *DumpMetadata) {
	tm.metadata = meta
}

// markTableDone records a finished table in metadata.json immediately, giving
// --resume per-table granularity if the dump dies later on.
func (tm *TaskManager) markTableDone(t *Task) {
	if tm.metadata == nil {
		return
	}
	tm.metadata.MarkTableDone(t.Table.GetUnescapedSchema(), t.Table.GetUnescapedName(), t.TotalChunks)
	log.Debugf("Table %s complete (%d chunks) — recorded in metadata.", t.Table.GetUnescapedFullName(), t.TotalChunks)
}

// detectMySQLVersion populates tm.mysqlVersion from SELECT VERSION().
func (tm *TaskManager) detectMySQLVersion() {
	var versionStr string
	if err := tm.DB.QueryRowContext(tm.ctx, "SELECT VERSION()").Scan(&versionStr); err != nil {
		log.Warningf("Could not detect MySQL version: %v", err)
		return
	}
	versionStr = strings.Split(versionStr, "-")[0]
	parts := strings.Split(versionStr, ".")
	for i := 0; i < 3 && i < len(parts); i++ {
		tm.mysqlVersion[i], _ = strconv.Atoi(parts[i])
	}
	log.Debugf("MySQL version detected: %d.%d.%d", tm.mysqlVersion[0], tm.mysqlVersion[1], tm.mysqlVersion[2])
}

// MySQLVersion returns the detected server version as a "X.Y.Z" string.
func (tm *TaskManager) MySQLVersion() string {
	return mysqlVersionString(tm.mysqlVersion)
}

// mysqlAtLeast returns true if the connected server is >= major.minor.patch.
func (tm *TaskManager) mysqlAtLeast(major, minor, patch int) bool {
	if tm.mysqlVersion[0] != major {
		return tm.mysqlVersion[0] > major
	}
	if tm.mysqlVersion[1] != minor {
		return tm.mysqlVersion[1] > minor
	}
	return tm.mysqlVersion[2] >= patch
}

func (tm *TaskManager) addDatabaseEngine(t *Table) {
	if len(tm.databaseEngines) == 0 {
		tm.databaseEngines = make(map[string]*Table)
	}
	if _, ok := tm.databaseEngines[t.Engine]; !ok {
		tm.databaseEngines[t.Engine] = t
	}
}

func (tm *TaskManager) AddTask(t *Task) {
	if len(tm.tasksPool) == 0 {
		t.Id = 0
	} else {
		t.Id = tm.tasksPool[len(tm.tasksPool)-1].Id + 1
	}
	tm.tasksPool = append(tm.tasksPool, t)
	tm.addDatabaseEngine(t.Table)
}

func (tm *TaskManager) GetTasksPool() []*Task {
	return tm.tasksPool
}

func (tm *TaskManager) AddWorkersDB() {
	for i := 0; i < tm.ThreadsCount; i++ {
		conn, err := GetMySQLConnection(tm.mySQLHost, tm.mySQLCredentials)
		if err != nil {
			log.Fatalf("Error with the database connection: %s", err.Error())
		}
		tm.AddWorkerDB(conn)
	}
}

func (tm *TaskManager) AddWorkerDB(db *sql.DB) {
	tm.workersDB = append(tm.workersDB, db)
	tm.workers = append(tm.workers, nil)
}

// isInnoDBOnly returns true when every table in the task pool uses InnoDB.
// An empty engine string (unknown) is treated conservatively as non-InnoDB.
func (tm *TaskManager) isInnoDBOnly() bool {
	if len(tm.databaseEngines) == 0 {
		return false
	}
	for engine := range tm.databaseEngines {
		if !strings.EqualFold(engine, "InnoDB") {
			return false
		}
	}
	return true
}

// createWorkersBeginTx opens plain transactions at the configured isolation level.
// Only used when the user explicitly opted out of a consistent backup
// (--lock-tables=false): the snapshots are NOT synchronised to a single instant.
func (tm *TaskManager) createWorkersBeginTx() error {
	for i, dbW := range tm.workersDB {
		txW, err := dbW.BeginTx(tm.ctx, &sql.TxOptions{
			Isolation: tm.IsolationLevel,
			ReadOnly:  true,
		})
		if err != nil {
			return fmt.Errorf("worker %d: begin transaction: %w", i, err)
		}
		tm.workers[i] = &txTxRunner{tx: txW}
	}
	return nil
}

// createWorkersConsistentSnapshot opens one dedicated connection per worker and
// issues START TRANSACTION WITH CONSISTENT SNAPSHOT on each. The InnoDB read view
// is established immediately (unlike BeginTx, where it only starts at the first
// read), so when this runs inside the lock window every worker shares the same
// point in time. REPEATABLE READ is forced because CONSISTENT SNAPSHOT is only
// honoured at that isolation level.
func (tm *TaskManager) createWorkersConsistentSnapshot() error {
	for i, dbW := range tm.workersDB {
		conn, err := dbW.Conn(tm.ctx)
		if err != nil {
			return fmt.Errorf("worker %d: get connection: %w", i, err)
		}
		if _, err := conn.ExecContext(tm.ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			_ = conn.Close()
			return fmt.Errorf("worker %d: set isolation level: %w", i, err)
		}
		if _, err := conn.ExecContext(tm.ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
			_ = conn.Close()
			return fmt.Errorf("worker %d: start transaction: %w", i, err)
		}
		tm.workers[i] = &txConnRunner{conn: conn}
	}
	return nil
}

// isMultiMaster detects MariaDB multi-master replication.
func (tm *TaskManager) isMultiMaster() (bool, error) {
	rows, err := tm.DB.Query("SELECT @@default_master_connection")
	if err != nil {
		return false, err
	}
	rows.Close()
	return true, nil
}

func (tm *TaskManager) getSlaveData() {
	log.Info("Getting Replica Status")
	isMultiMaster, _ := tm.isMultiMaster()
	useNewSyntax := tm.mysqlAtLeast(8, 0, 22)

	var query string
	switch {
	case isMultiMaster && useNewSyntax:
		query = "SHOW ALL REPLICAS STATUS"
	case isMultiMaster:
		query = "SHOW ALL SLAVES STATUS"
	case useNewSyntax:
		query = "SHOW REPLICA STATUS"
	default:
		query = "SHOW SLAVE STATUS"
	}

	slaveData, err := tm.DB.Query(query)
	if err != nil {
		log.Fatalf("Error getting slave information: %s", err.Error())
	}
	defer slaveData.Close()

	var connectionName, relayMasterLogFile, masterHost, executedGtidSet, gtidSlavePos string
	var execMasterLogPos, masterPort uint64
	var haveGtidSlavePos, haveExecutedGtidSet = false, false
	cols, _ := slaveData.Columns()
	var out []interface{}
	iterations := 0

	for i := 0; i < len(cols); i++ {
		switch strings.ToUpper(cols[i]) {
		case "CONNECTION_NAME":
			out = append(out, &connectionName)
		case "RELAY_MASTER_LOG_FILE":
			out = append(out, &relayMasterLogFile)
		case "MASTER_HOST":
			out = append(out, &masterHost)
		case "MASTER_PORT":
			out = append(out, &masterPort)
		case "EXECUTED_GTID_SET":
			haveExecutedGtidSet = true
			out = append(out, &executedGtidSet)
		case "GTID_SLAVE_POS":
			haveGtidSlavePos = true
			out = append(out, &gtidSlavePos)
		case "EXEC_MASTER_LOG_POS":
			out = append(out, &execMasterLogPos)
		default:
			out = append(out, new(interface{}))
		}
	}

	buffer, _ := NewSlaveDataBuffer(tm)
	for slaveData.Next() {
		iterations++
		if err := slaveData.Scan(out...); err != nil {
			log.Fatalf(err.Error())
		}
		fmt.Fprintln(buffer, "Connection Name: ", connectionName)
		fmt.Fprintln(buffer, "  Relay Master Log File: ", relayMasterLogFile)
		fmt.Fprintln(buffer, "  Master Host: ", masterHost)
		fmt.Fprintln(buffer, "  Master Port: ", masterPort)
		fmt.Fprintln(buffer, "  Exec Master Log Pos: ", execMasterLogPos)
		if haveExecutedGtidSet {
			fmt.Fprintln(buffer, "  Executed GTID Set: ", executedGtidSet)
		}
		if haveGtidSlavePos {
			fmt.Fprintln(buffer, "  GTID Slave Pos: ", gtidSlavePos)
		}
	}
	if err := buffer.Close(); err != nil {
		log.Fatalf("Error finalising slave-data file: %v", err)
	}

	if iterations == 0 {
		log.Fatalf("There is no slave information. Make sure that the server is acting as a slave server.")
	}
}

func (tm *TaskManager) getMasterData() {
	log.Info("Getting Binary Log Status")

	// SHOW BINARY LOG STATUS replaced SHOW MASTER STATUS in MySQL 8.4.0.
	// SHOW REPLICA STATUS replaced SHOW SLAVE STATUS in MySQL 8.0.22 (handled in getSlaveData).
	query := "SHOW MASTER STATUS"
	if tm.mysqlAtLeast(8, 4, 0) {
		query = "SHOW BINARY LOG STATUS"
	}

	var masterFile, binlogDoDb, binlogIgnoreDB, executedGTIDSet string
	var masterPosition int

	masterRows, err := tm.DB.Query(query)
	if err != nil {
		log.Fatalf("%s", err.Error())
	}
	defer masterRows.Close()

	cols, _ := masterRows.Columns()
	if len(cols) < 1 {
		log.Fatal("Error getting binary log information. Make sure binary logging is enabled. Use --get-master-status=false to skip.")
	}
	var out []interface{}
	supportGTID := false

	for i := 0; i < len(cols); i++ {
		switch strings.ToUpper(cols[i]) {
		case "FILE":
			out = append(out, &masterFile)
		case "POSITION":
			out = append(out, &masterPosition)
		case "BINLOG_DO_DB":
			out = append(out, &binlogDoDb)
		case "BINLOG_IGNORE_DB":
			out = append(out, &binlogIgnoreDB)
		case "EXECUTED_GTID_SET":
			supportGTID = true
			out = append(out, &executedGTIDSet)
		default:
			log.Warningf("Unknown column \"%s\" in binary log status output — skipping.", cols[i])
			out = append(out, new(interface{}))
		}
	}

	if !masterRows.Next() {
		if err := masterRows.Err(); err != nil {
			log.Fatalf("Error reading binary log status: %v", err)
		}
		log.Fatal("Binary log status returned no rows. Make sure binary logging is enabled, or omit --get-master-status.")
	}
	if err = masterRows.Scan(out...); err != nil {
		log.Fatalf("Error reading Master data information: %s", err.Error())
	}

	// Store for metadata.json.
	tm.BinlogFile = masterFile
	tm.BinlogPosition = masterPosition
	if supportGTID {
		tm.GTIDSet = executedGTIDSet
	}

	buffer, _ := NewMasterDataBuffer(tm)
	fmt.Fprintln(buffer, "Master File:", masterFile)
	fmt.Fprintln(buffer, "Master Position: ", masterPosition)
	fmt.Fprintln(buffer, "Binlog Do DB: ", binlogDoDb)
	fmt.Fprintln(buffer, "Binlog Ignore DB: ", binlogIgnoreDB)
	if supportGTID {
		fmt.Fprintln(buffer, "Executed Gtid Set: ", executedGTIDSet)
	}
	if err := buffer.Close(); err != nil {
		log.Fatalf("Error finalising master-data file: %v", err)
	}
}

func (tm *TaskManager) WriteTablesSQL(addDropTable bool) {
	for _, task := range tm.tasksPool {
		buffer, err := NewTableDefinitionBuffer(task)
		if err != nil {
			log.Fatalf("Error creating definition buffer for %s: %s", task.Table.GetUnescapedFullName(), err.Error())
		}
		if !tm.SkipUseDatabase {
			fmt.Fprintf(buffer, GetUseDatabaseSQL(task.Table.GetSchema())+";\n")
		}
		fmt.Fprintf(buffer, "/*!40101 SET NAMES binary*/;\n")
		fmt.Fprintf(buffer, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n")
		if addDropTable {
			fmt.Fprintf(buffer, GetDropTableIfExistSQL(task.Table.GetName())+";\n")
		}
		fmt.Fprintf(buffer, task.Table.CreateTableSQL+";\n")
		if err := buffer.Close(); err != nil {
			log.Fatalf("Error finalising definition file for %s: %v", task.Table.GetUnescapedFullName(), err)
		}
	}
}

// GetTransactions establishes the worker snapshots that define the backup's
// point in time.
//
// Consistent path (lockTables=true): one dedicated connection acquires
// FLUSH TABLES WITH READ LOCK (--all-databases, or when non-InnoDB tables are
// present) or LOCK TABLES ... READ (InnoDB-only table list). While the lock is
// held, every worker issues START TRANSACTION WITH CONSISTENT SNAPSHOT and the
// binlog coordinates are captured — so all snapshots and the recorded position
// refer to the same instant. The lock is released immediately afterwards; the
// window is typically milliseconds.
//
// Limitation: non-InnoDB tables are write-protected only during the lock
// window. Their data is read later without MVCC, so writes occurring after the
// unlock can appear in the dump. Holding the lock for the whole dump (mydumper's
// --no-locks=false behaviour for non-transactional tables) is not implemented.
// WriteSchemaCreateSQL writes one <schema>-schema-create.sql file per dumped
// schema so the dump restores onto a server where the database does not exist
// yet. SHOW CREATE DATABASE preserves charset/collation; IF NOT EXISTS is added
// mysqldump-style so restoring into an existing database is a no-op.
func (tm *TaskManager) WriteSchemaCreateSQL() {
	schemas := make(map[string]bool)
	for _, task := range tm.tasksPool {
		schemas[task.Table.GetUnescapedSchema()] = true
	}
	for schema := range schemas {
		var name, createSQL string
		// SHOW CREATE DATABASE cannot be parameterised; the name comes from
		// information_schema and is backtick-quoted.
		query := fmt.Sprintf("SHOW CREATE DATABASE `%s`", schema)
		if err := tm.DB.QueryRowContext(tm.ctx, query).Scan(&name, &createSQL); err != nil {
			log.Fatalf("Error reading database definition for %s: %v", schema, err)
		}
		createSQL = strings.Replace(createSQL, "CREATE DATABASE ", "CREATE DATABASE /*!32312 IF NOT EXISTS*/ ", 1)

		bufferOptions := tm.GetBufferOptions()
		bufferOptions.Path = filepath.Join(tm.DestinationDir, schema+"-schema-create.sql")
		buffer, err := NewBuffer(bufferOptions)
		if err != nil {
			log.Fatalf("Error creating schema-create file for %s: %v", schema, err)
		}
		fmt.Fprintf(buffer, "%s;\n", createSQL)
		if err := buffer.Close(); err != nil {
			log.Fatalf("Error finalising schema-create file for %s: %v", schema, err)
		}
	}
}

func (tm *TaskManager) GetTransactions(lockTables bool, allDatabases bool) {
	tm.detectMySQLVersion()

	if !lockTables {
		log.Warning("Running without --lock-tables: worker snapshots are not synchronised and the dump is NOT point-in-time consistent.")
		if err := tm.createWorkersBeginTx(); err != nil {
			log.Fatalf("Error creating workers: %v", err)
		}
		if tm.GetMasterStatus {
			tm.getMasterData()
		}
		if tm.GetSlaveStatus {
			tm.getSlaveData()
		}
		return
	}

	// With a table list, LOCK TABLES needs a non-empty pool; --all-databases
	// always uses FTWRL. An empty pool with nothing to lock still gets
	// consistent-snapshot workers (there is just nothing to synchronise against).
	useLock := allDatabases || len(tm.tasksPool) > 0
	var lockConn *sql.Conn
	var startLocking time.Time

	if useLock {
		if len(tm.tasksPool) > 0 && !tm.isInnoDBOnly() {
			log.Warning("Non-InnoDB tables detected: they are write-protected only during the brief lock window; concurrent changes can appear in their dump files.")
		}

		// LOCK TABLES / FTWRL are session-scoped: every lock-related statement
		// must run on this single pinned connection, never on the pool —
		// otherwise UNLOCK TABLES can land on a different session and the lock
		// stays held until the connection is recycled.
		var err error
		lockConn, err = tm.DB.Conn(tm.ctx)
		if err != nil {
			log.Fatalf("Error acquiring the lock connection: %v", err)
		}
		defer lockConn.Close()

		lockSQL := GetLockTablesSQL(tm.tasksPool, "READ")
		if allDatabases || !tm.isInnoDBOnly() {
			// FTWRL freezes the binlog position globally and also covers
			// non-InnoDB engines during the window.
			lockSQL = GetLockAllTablesSQL()
		}
		log.Info("Locking tables to synchronise worker snapshots and binlog position.")
		startLocking = time.Now()
		if _, err := lockConn.ExecContext(tm.ctx, lockSQL); err != nil {
			log.Fatalf("Error locking the tables: %v", err)
		}
	}

	log.Debug("Starting worker snapshots")
	snapErr := tm.createWorkersConsistentSnapshot()

	if snapErr == nil {
		// Captured inside the lock window so the coordinates match the worker
		// snapshots — required for seeding replicas from this dump.
		if tm.GetMasterStatus {
			tm.getMasterData()
		}
		if tm.GetSlaveStatus {
			tm.getSlaveData()
		}
	}

	if useLock {
		if _, err := lockConn.ExecContext(tm.ctx, "UNLOCK TABLES"); err != nil {
			log.Criticalf("Error unlocking the tables: %v", err)
		} else {
			log.Infof("Unlocking the tables. Tables were locked for %s", time.Since(startLocking))
		}
	}

	if snapErr != nil {
		log.Fatalf("Error creating worker snapshots: %v", snapErr)
	}

	log.Debugf("Added %d transactions", len(tm.workersDB))
}

func (tm *TaskManager) StartWorkers() error {
	log.Infof("Starting %d workers", len(tm.workers))
	for i := range tm.workers {
		tm.ProcessChunksWaitGroup.Add(1)
		go tm.StartWorker(i)
	}
	log.Debugf("All workers are running")
	return nil
}

func (tm *TaskManager) DisplaySummary() error {
	for _, task := range tm.tasksPool {
		fmt.Printf("   %d -> %s\n", task.TotalChunks, task.Table.GetFullName())
	}
	return nil
}

// PrintStatus logs progress every 5 seconds until the queue drains or ctx is cancelled.
// Output: "Progress: 450/1500 (30.0%) | Rate: 12.3 chunks/s | ETA: ~1m42s"
func (tm *TaskManager) PrintStatus() {
	// Wait for work to begin before printing anything.
	select {
	case <-time.After(3 * time.Second):
	case <-tm.ctx.Done():
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tm.ctx.Done():
			return
		case <-ticker.C:
			completed := atomic.LoadInt64(&tm.CompletedChunks)
			total := atomic.LoadInt64(&tm.TotalChunks)
			queue := atomic.LoadInt64(&tm.Queue)

			if queue <= 0 && completed > 0 {
				return
			}

			elapsed := time.Since(tm.StartTime).Seconds()
			var pct float64
			var etaStr, rateStr string

			if total > 0 {
				pct = float64(completed) / float64(total) * 100
			}
			if elapsed > 0 && completed > 0 {
				rate := float64(completed) / elapsed
				rateStr = fmt.Sprintf("%.1f chunks/s", rate)
				if total > 0 && rate > 0 {
					remaining := float64(total-completed) / rate
					eta := time.Duration(remaining) * time.Second
					etaStr = fmt.Sprintf("~%s", eta.Round(time.Second))
				}
			}

			if total > 0 && etaStr != "" {
				log.Infof("Progress: %d/%d (%.1f%%) | Rate: %s | ETA: %s",
					completed, total, pct, rateStr, etaStr)
			} else if total > 0 {
				log.Infof("Progress: %d/%d (%.1f%%)", completed, total, pct)
			} else {
				log.Infof("Progress: %d chunks completed", completed)
			}
		}
	}
}

func (tm *TaskManager) CleanChunkChannel() {
	for {
		_, ok := <-tm.ChunksChannel
		if !ok {
			log.Debugf("Channel closed.")
			break
		}
	}
}

// maxChunkAttempts bounds retries of a single chunk query on transient errors
// that leave the worker's snapshot transaction intact.
const maxChunkAttempts = 3

// isRetryableChunkError reports whether a chunk query may be retried on the
// same worker transaction without losing the consistent snapshot. Connection
// loss (ErrBadConn, 2006, 2013) is deliberately NOT retryable: the snapshot
// dies with the connection, and re-reading on a new one would silently break
// point-in-time consistency.
func isRetryableChunkError(err error) bool {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1205, 1213: // lock wait timeout, deadlock
			return true
		}
	}
	return false
}

func (tm *TaskManager) StartWorker(workerId int) {
	bufferChunk := make(map[string]*Buffer)

	var query string
	var stmt *sql.Stmt
	var err error

	// Each chunk is staged in memory and only copied to the dump file after the
	// chunk query succeeds, so a retried query never duplicates rows that were
	// already flushed to disk. The buffer is reused across chunks.
	var chunkStage bytes.Buffer

	for {
		chunk, ok := <-tm.ChunksChannel
		if !ok {
			log.Debugf("Channel %d is closed.", workerId)
			break
		}
		atomic.AddInt64(&tm.Queue, -1)
		log.Debugf("Queue -1: %d ", atomic.LoadInt64(&tm.Queue))

		// Only re-prepare when the query changes (e.g. different table or WHERE clause).
		// Closing stmt after every chunk and re-using it on the next caused errors.
		if newQuery := chunk.GetPrepareSQL(); newQuery != query {
			if stmt != nil {
				stmt.Close()
			}
			query = newQuery
			stmt, err = tm.workers[workerId].PrepareContext(tm.ctx, query)
			if err != nil {
				log.Fatalf("Error preparing statement. Query: %s, Error: %s.", query, err.Error())
			}
		}

		tablename := chunk.Task.Table.GetUnescapedFullName()
		if _, ok := bufferChunk[tablename]; !ok {
			b, err := NewChunkBuffer(&chunk, workerId)
			if err != nil {
				log.Fatalf("Error creating dump file for %s: %v", tablename, err)
			}
			bufferChunk[tablename] = b
		}

		buffer := bufferChunk[tablename]
		if !chunk.Task.TaskManager.SkipUseDatabase {
			fmt.Fprintf(buffer, "USE %s;\n", chunk.Task.Table.GetSchema())
		}
		if err := buffer.Flush(); err != nil {
			// A failed write (e.g. disk full) means a truncated backup — abort.
			log.Fatalf("Error writing dump file for %s: %v", tablename, err)
		}

		var parseErr error
		for attempt := 1; ; attempt++ {
			chunkStage.Reset()
			parseErr = chunk.Parse(stmt, &chunkStage)
			if parseErr == nil || attempt >= maxChunkAttempts || !isRetryableChunkError(parseErr) {
				break
			}
			log.Warningf("Transient error on chunk %d of %s (attempt %d/%d), retrying: %v",
				chunk.Sequence, tablename, attempt, maxChunkAttempts, parseErr)
			select {
			case <-tm.ctx.Done():
				log.Fatalf("Dump cancelled while retrying chunk for %s: %v", tablename, tm.ctx.Err())
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}
		if parseErr != nil {
			log.Fatalf("Error parsing chunk for %s: %s", chunk.Task.Table.GetFullName(), parseErr.Error())
		}
		if _, err := buffer.Write(chunkStage.Bytes()); err != nil {
			log.Fatalf("Error writing dump file for %s: %v", tablename, err)
		}
		// Flush before counting the chunk as completed: a table is only marked
		// done in metadata once every one of its chunks has been handed to the OS.
		if err := buffer.Flush(); err != nil {
			log.Fatalf("Error flushing dump file for %s: %v", tablename, err)
		}
		atomic.AddInt64(&tm.CompletedChunks, 1)
		chunk.Task.NoteChunkCompleted()
	}

	// Close stmt once when the worker is done, not after every chunk.
	if stmt != nil {
		stmt.Close()
	}
	for _, buffer := range bufferChunk {
		if err := buffer.Close(); err != nil {
			// Close performs the final flush; an error here means the file on
			// disk is incomplete. Never let it look like a successful backup.
			log.Fatalf("Error finalising dump file: %v", err)
		}
	}
	if err := tm.workers[workerId].Commit(tm.ctx); err != nil {
		log.Warningf("Worker %d commit error: %v", workerId, err)
	}
	tm.ProcessChunksWaitGroup.Done()
}

func (tm *TaskManager) AddChunk(chunk DataChunk) {
	tm.ChunksChannel <- chunk
}

func (tm *TaskManager) CreateChunks(db *sql.DB) {
	log.Debugf("tasksPool  %v", tm.tasksPool)
	for _, t := range tm.tasksPool {
		tm.CreateChunksWaitGroup.Add(1)
		log.Debugf("CreateChunksWaitGroup TaskManager Add %v", tm.CreateChunksWaitGroup)
		go t.CreateChunks(db)
	}
	tm.CreateChunksWaitGroup.Done()
	log.Debugf("CreateChunksWaitGroup TaskManager Done %v", tm.CreateChunksWaitGroup)
}

func (tm *TaskManager) GetBufferOptions() *BufferOptions {
	bufferOptions := new(BufferOptions)
	if tm.Compress {
		bufferOptions.Compress = true
		bufferOptions.CompressLevel = tm.CompressLevel
	}
	bufferOptions.Type = BufferTypeFile
	return bufferOptions
}
