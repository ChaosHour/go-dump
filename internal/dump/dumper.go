package dump

import (
	"context"
	"database/sql"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/ChaosHour/go-dump/internal/log"
)

// Run executes the dump. Callers are responsible for validating opts before calling.
func Run(ctx context.Context, opts *DumpOptions) {
	startExecution := time.Now()

	cores := runtime.NumCPU()
	if opts.Threads > cores {
		log.Warningf("Requested %d threads but only %d CPU cores are available.", opts.Threads, cores)
	}

	cDataChunk := make(chan DataChunk, opts.ChannelBufferSize)
	var wgCreateChunks sync.WaitGroup
	var wgProcessChunks sync.WaitGroup

	tmdb, err := GetMySQLConnection(opts.MySQLHost, opts.MySQLCredentials)
	if err != nil {
		log.Criticalf("Error with the database connection: %s", err.Error())
		return
	}
	setPoolLimits(tmdb, opts.Threads)

	taskManager := NewTaskManager(
		&wgCreateChunks,
		&wgProcessChunks,
		cDataChunk,
		tmdb,
		opts)
	taskManager.SetContext(ctx)

	dbchunks, err := GetMySQLConnection(opts.MySQLHost, opts.MySQLCredentials)
	if err != nil {
		log.Criticalf("Error with the database connection: %s", err.Error())
		return
	}
	setPoolLimits(dbchunks, opts.Threads)

	tablesToParse := resolveTables(opts, dbchunks)

	// priorMeta carries the previous run's metadata when resuming, so completed
	// tables stay recorded in the new metadata.json and checksums.txt.
	var priorMeta *DumpMetadata
	if opts.Resume && opts.TemporalOptions.Execute {
		if err := os.MkdirAll(opts.DestinationDir, 0755); err != nil {
			log.Fatalf("Error creating destination directory %s: %s", opts.DestinationDir, err.Error())
		}
		tablesToParse, priorMeta = applyResumeFilter(opts.DestinationDir, tablesToParse)
	}

	for table := range tablesToParse {
		t := strings.Split(table, ".")
		task := NewTask(
			t[0], t[1],
			opts.ChunkSize,
			opts.OutputChunkSize,
			&taskManager)
		task.PrintInfo()
		taskManager.AddTask(&task)
		log.Debugf("Table: %+v", task.Table)
	}

	log.Debugf("Added %d worker connections to the task manager.", opts.Threads)

	taskManager.AddWorkersDB()

	// Written before the goroutine starts — PrintStatus reads it concurrently.
	taskManager.StartTime = time.Now()
	go taskManager.PrintStatus()

	if opts.TemporalOptions.DryRun {
		taskManager.CreateChunksWaitGroup.Add(1)
		go taskManager.CreateChunks(dbchunks)
		go taskManager.CleanChunkChannel()
		taskManager.CreateChunksWaitGroup.Wait()
		close(taskManager.ChunksChannel)
		taskManager.DisplaySummary()
	}

	if opts.TemporalOptions.Execute {
		if err := os.MkdirAll(opts.DestinationDir, 0755); err != nil {
			log.Fatalf("Error creating destination directory %s: %s", opts.DestinationDir, err.Error())
		}

		// Snapshots are taken before chunk boundaries are computed, so for
		// InnoDB tables the boundaries match the data the workers will read.
		taskManager.GetTransactions(opts.LockTables, opts.TemporalOptions.AllDatabases)

		// Initialise metadata after GetTransactions so mysqlVersion and binlog are known.
		// Register an exit hook so that any log.Fatalf call anywhere in the dump path
		// writes status="failed" to metadata.json rather than leaving it as "in_progress".
		var activeMeta *DumpMetadata
		log.RegisterExitHook(func() {
			if activeMeta != nil {
				activeMeta.Fail()
			}
		})

		meta := NewDumpMetadata(
			opts.DestinationDir,
			opts.AppVersion,
			opts.MySQLHost.HostName,
			taskManager.MySQLVersion(),
		)
		activeMeta = meta
		meta.SetBinlog(taskManager.BinlogFile, taskManager.BinlogPosition, taskManager.GTIDSet)
		// On resume, carry tables completed by the prior run so metadata.json
		// keeps describing the whole dump set on disk.
		meta.MergeDoneTables(priorMeta)
		for _, task := range taskManager.GetTasksPool() {
			meta.AddTable(task.Table.GetUnescapedSchema(), task.Table.GetUnescapedName(), task.Table.estNumberOfRows)
		}
		_ = meta.Write()

		// Metadata must be attached before chunking/workers start: workers mark
		// each table done in metadata.json as its last chunk is flushed.
		taskManager.SetMetadata(meta)

		// Definition and schema-create files are written up front so a partial
		// dump (or a resumed one) is restorable as far as it got.
		taskManager.WriteSchemaCreateSQL()
		taskManager.WriteTablesSQL(opts.AddDropTable)
		if counts := taskManager.WriteObjectsSQL(); counts != nil {
			meta.SetObjects(counts)
		}

		taskManager.CreateChunksWaitGroup.Add(1)
		go taskManager.CreateChunks(dbchunks)

		taskManager.StartWorkers()
		log.Debugf("ProcessChunksWaitGroup: %+v", taskManager.ProcessChunksWaitGroup)
		taskManager.CreateChunksWaitGroup.Wait()
		close(taskManager.ChunksChannel)
		taskManager.ProcessChunksWaitGroup.Wait()

		// Catch-all sweep: zero-chunk tables and compressed dumps (whose files
		// are only complete after the buffers above are closed) are marked done
		// here; uncompressed tables were already marked by the workers.
		for _, task := range taskManager.GetTasksPool() {
			meta.MarkTableDone(task.Table.GetUnescapedSchema(), task.Table.GetUnescapedName(), uint64(task.TotalChunks))
		}

		if opts.Checksum {
			if err := RunChecksums(taskManager.GetTasksPool(), tmdb, opts.DestinationDir, meta); err != nil {
				log.Warningf("Checksum error: %v", err)
			}
			// Flush checksum values into metadata now that SetChecksum has been called.
			_ = meta.Write()
		}

		meta.Complete()
		log.Info("Dump complete.")
	}

	log.Infof("Execution time: %s", time.Since(startExecution))
}

func resolveTables(opts *DumpOptions, db *sql.DB) map[string]bool {
	if opts.TemporalOptions.AllDatabases {
		return TablesFromAllDatabases(db, opts.TemporalOptions.IncludeSystemDatabases)
	}

	var tablesFromDatabases, tablesFromString, tablesToParse map[string]bool

	if len(opts.TemporalOptions.Databases) > 0 {
		tablesFromDatabases = TablesFromDatabase(opts.TemporalOptions.Databases, db)
		log.Debugf("tablesFromDatabases: %v", tablesFromDatabases)
	}
	if len(opts.TemporalOptions.Tables) > 0 {
		tablesFromString = TablesFromString(opts.TemporalOptions.Tables)
	}
	if len(tablesFromDatabases) > 0 {
		tablesToParse = tablesFromDatabases
	}
	if len(tablesFromString) > 0 {
		if len(tablesToParse) > 0 {
			for table := range tablesFromString {
				if _, ok := tablesToParse[table]; !ok {
					tablesToParse[table] = true
				}
			}
		} else {
			tablesToParse = tablesFromString
		}
	}
	return tablesToParse
}
