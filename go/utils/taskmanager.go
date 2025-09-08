package utils

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/outbrain/golib/log"
)

func NewTaskManager(
	wgC *sync.WaitGroup,
	wgP *sync.WaitGroup,
	cDC chan DataChunk,
	db *sql.DB,
	dumpOptions *DumpOptions) TaskManager {

	tm := TaskManager{
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
		DumpOptions:            dumpOptions}
	return tm
}

type TaskManager struct {
	CreateChunksWaitGroup  *sync.WaitGroup //Create Chunks WaitGroup
	ProcessChunksWaitGroup *sync.WaitGroup //Create Chunks WaitGroup
	ChunksChannel          chan DataChunk
	DB                     *sql.DB
	ThreadsCount           int
	tasksPool              []*Task
	workersTx              []*sql.Tx
	workersDB              []*sql.DB
	databaseEngines        map[string]*Table
	TotalChunks            int64
	Queue                  int64
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
			log.Critical("Error whith the database connection. %s", err.Error())
		}
		conn.Ping()
		tm.AddWorkerDB(conn)
	}

}

func (tm *TaskManager) AddWorkerDB(db *sql.DB) {
	tm.workersDB = append(tm.workersDB, db)
	tm.workersTx = append(tm.workersTx, nil)
}

func (tm *TaskManager) lockTables() {
	query := GetLockTablesSQL(tm.tasksPool, "READ")

	if _, err := tm.DB.Exec(query); err != nil {
		log.Criticalf("Error unlocking the tables: %s", err.Error())
	}
}

func (tm *TaskManager) unlockTables() {
	log.Debugf("Unlocking tables")
	if _, err := tm.DB.Exec("UNLOCK TABLES"); err != nil {
		log.Criticalf("Error unlocking the tables: %s", err.Error())
	}
}

func (tm *TaskManager) lockAllTables() {
	query := GetLockAllTablesSQL()
	if _, err := tm.DB.Exec(query); err != nil {
		log.Fatalf("Error locking table: %s", err.Error())
	}
}

func (tm *TaskManager) createWorkers() error {
	for i, dbW := range tm.workersDB {
		txW, err := dbW.BeginTx(context.Background(), &sql.TxOptions{
			Isolation: tm.IsolationLevel,
			ReadOnly:  true})
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %v", err)
		}
		tm.workersTx[i] = txW
	}
	return nil
}

func (tm *TaskManager) isMultiMaster() (bool, error) {
	_, err := tm.DB.Query("SELECT @@default_master_connection")
	if err != nil {
		return false, err
	}
	return true, nil
}

// getSlaveData collects the slave data from the node that you are taking the backup.
// It detect if the slave has multi master replication and collect and store the information for all the channels.
func (tm *TaskManager) getSlaveData() {
	log.Info("Getting Slave Status")
	isMultiMaster, _ := tm.isMultiMaster()
	var query string

	if isMultiMaster {
		query = "SHOW ALL SLAVES STATUS"
	} else {
		query = "SHOW SLAVE STATUS"
	}
	slaveData, err := tm.DB.Query(query)

	if err != nil {
		log.Fatalf("Error getting slave information: %s", err.Error())
	}

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
	buffer.Flush()
	buffer.Close()

	if iterations == 0 {
		log.Fatalf("There is no slave information. Make sure that the server is acting as a slave server.")
	}
}

func (tm *TaskManager) getMasterData() {

	log.Info("Getting Master Status")

	var masterFile, binlogDoDb, binlogIgnoreDB, executedGTIDSet string
	var masterPosition int

	masterRows, err := tm.DB.Query(GetMasterStatusSQL())
	if err != nil {
		log.Fatalf("%s", err.Error())
	}
	cols, _ := masterRows.Columns()

	if len(cols) < 1 {
		log.Fatal("Error getting the master data information. Make sure that the logs are enabled. If you want to skip the collection of the master information please use the option --master-data=false. Use --help for more information.")
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
			log.Warningf("Unknown option \"%s\" on the Mastet Inforamtion. Please report this bug. MASTER DATA WILL NOT BE AVAILABLE!", cols[i])
		}
	}

	masterRows.Next()
	err = masterRows.Scan(out...)
	if err != nil {
		log.Fatalf("Error reading Master data information: %s", err.Error())
	}
	masterRows.Close()
	buffer, _ := NewMasterDataBuffer(tm)

	fmt.Fprintln(buffer, "Master File:", masterFile)
	fmt.Fprintln(buffer, "Master Position: ", masterPosition)
	fmt.Fprintln(buffer, "Binlog Do DB: ", binlogDoDb)
	fmt.Fprintln(buffer, "Binlog Ignore DB: ", binlogIgnoreDB)
	if supportGTID {
		fmt.Fprintln(buffer, "Executed Gtid Set: ", executedGTIDSet)
	}
	buffer.Flush()
	buffer.Close()
}

func (tm *TaskManager) WriteTablesSQL(addDropTable bool) {
	for _, task := range tm.tasksPool {
		buffer, _ := NewTableDefinitionBuffer(task)

		if !tm.SkipUseDatabase {
			fmt.Fprintf(buffer, GetUseDatabaseSQL(task.Table.GetSchema())+";\n")
		}

		fmt.Fprintf(buffer, "/*!40101 SET NAMES binary*/;\n")
		fmt.Fprintf(buffer, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n")

		if addDropTable {
			fmt.Fprintf(buffer, GetDropTableIfExistSQL(task.Table.GetName())+";\n")
		}

		fmt.Fprintf(buffer, task.Table.CreateTableSQL+";\n")
		buffer.Flush()
	}
}

func (tm *TaskManager) GetTransactions(lockTables bool, allDatabases bool) {
	var startLocking time.Time

	if lockTables {
		log.Infof("Locking tables to get a consistent backup.")
		startLocking = time.Now()
		if allDatabases {
			tm.lockAllTables()
		} else {
			tm.lockTables()
		}
	}

	log.Debug("Starting workers")
	if err := tm.createWorkers(); err != nil {
		tm.unlockTables() // Cleanup if needed
		log.Fatalf("Error creating workers: %v", err)
		return
	}

	// GET MASTER DATA
	if tm.GetMasterStatus {
		tm.getMasterData()
	}
	if tm.GetSlaveStatus {
		tm.getSlaveData()
	}

	log.Debugf("Added %d transactions", len(tm.workersDB))

	if lockTables {
		tm.unlockTables()
		lockedTime := time.Since(startLocking)
		log.Infof("Unlocking the tables. Tables were locked for %s", lockedTime)
	}
}

func (tm *TaskManager) StartWorkers() error {
	log.Infof("Starting %d workers", len(tm.workersTx))
	// Simplify: remove unused range variable
	for i := range tm.workersTx {
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

func (tm *TaskManager) PrintStatus() {
	time.Sleep(2 * time.Second)
	log.Infof("Status. Queue: %d of %d", tm.Queue, tm.TotalChunks)
	for tm.Queue > 0 {
		log.Infof("Queue: %d of %d", tm.Queue, tm.TotalChunks)
		time.Sleep(1 * time.Second)
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

func (tm *TaskManager) StartWorker(workerId int) {
	bufferChunk := make(map[string]*Buffer)

	var query string
	var stmt *sql.Stmt
	var err error
	for {
		chunk, ok := <-tm.ChunksChannel
		tm.Queue = tm.Queue - 1
		log.Debugf("Queue -1: %d ", tm.Queue)

		if !ok {
			log.Debugf("Channel %d is closed.", workerId)
			break
		}

		if query != chunk.GetPrepareSQL() {
			query = chunk.GetPrepareSQL()
			if stmt != nil {
				stmt.Close()
			}
			stmt, err = tm.workersTx[workerId].Prepare(query)
		} else {
			stmt, err = tm.workersTx[workerId].Prepare(query)
		}

		if err != nil {
			log.Fatalf("Error starting the worker. Query: %s, Error: %s.", query, err.Error())
		}

		tablename := chunk.Task.Table.GetUnescapedFullName()

		if _, ok := bufferChunk[tablename]; !ok {
			bufferChunk[tablename], _ = NewChunkBuffer(&chunk, workerId)
		}

		buffer := bufferChunk[tablename]

		if !chunk.Task.TaskManager.SkipUseDatabase {
			fmt.Fprintf(buffer, "USE %s\n", chunk.Task.Table.GetSchema())
		}

		buffer.Flush()

		chunk.Parse(stmt, buffer)

		stmt.Close()
	}
	for _, buffer := range bufferChunk {
		buffer.Close()
	}
	tm.workersTx[workerId].Commit()
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
