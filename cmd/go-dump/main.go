package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/ChaosHour/go-dump/internal/dump"
	"github.com/ChaosHour/go-dump/internal/log"
)

// AppVersion is overridden at build time via -ldflags "-X main.AppVersion=<ver>".
var AppVersion string = "dev"

func printOption(w io.Writer, f *flag.Flag) {
	fmt.Fprint(w, "   --", f.Name, "\t", f.Usage)
	if f.DefValue != "" {
		fmt.Fprint(w, " Default [", f.DefValue, "]")
	}
	fmt.Fprint(w, "\n")
}

func PrintUsage(flags map[string]*flag.Flag) {
	w := tabwriter.NewWriter(os.Stdout, 30, 0, 1, ' ', tabwriter.TabIndent)
	fmt.Fprintln(w, "Usage: go-dump  --destination path [--databases str] [--tables str] [--all-databases] [--dry-run | --execute] [--help] [--debug] [--quiet] [--version] [--lock-tables] [--consistent] [--isolation-level str] [--channel-buffer-size num] [--chunk-size num] [--tables-without-uniquekey str] [--threads num] [--mysql-user str] [--mysql-password str] [--mysql-host str] [--mysql-port num] [--mysql-socket path] [--add-drop-table] [--insert-mode str] [--triggers] [--routines] [--events] [--skip-definer] [--get-master-status] [--get-slave-status] [--output-chunk-size num] [--skip-use-database] [--compress] [--compress-format str] [--compress-level num] [--where str] [--ini-file str]")
	fmt.Fprintln(w, "go-dump dumps a database or a table from a MySQL server and creates SQL statements to recreate each table. One file per table per thread is written to the destination directory.")
	fmt.Fprint(w, "Example: go-dump --destination /tmp/dbdump --databases mydb --mysql-user myuser --mysql-password password\n\n")
	fmt.Fprint(w, "Options description\n\n")

	fmt.Fprintln(w, "# General:")
	for _, opt := range []string{"help", "dry-run", "execute", "debug", "quiet", "version",
		"lock-tables", "lock-wait-timeout", "channel-buffer-size", "chunk-size", "tables-without-uniquekey",
		"threads", "compress", "compress-format", "compress-level", "consistent", "isolation-level", "where", "ini-file"} {
		printOption(w, flags[opt])
	}
	fmt.Fprintln(w, "\n# MySQL options:")
	for _, opt := range []string{"mysql-user", "mysql-password", "mysql-host", "mysql-port", "mysql-socket"} {
		printOption(w, flags[opt])
	}
	fmt.Fprintln(w, "\n# Databases or tables to dump:")
	for _, opt := range []string{"all-databases", "databases", "tables"} {
		printOption(w, flags[opt])
	}
	fmt.Fprintln(w, "\n# Output options:")
	for _, opt := range []string{"destination", "add-drop-table", "insert-mode", "triggers", "routines", "events", "skip-definer", "get-master-status", "get-slave-status", "output-chunk-size", "statement-size", "skip-use-database"} {
		printOption(w, flags[opt])
	}
	w.Flush()
}

var dumpOptions = dump.GetDumpOptions()
var flagSet = make(map[string]bool)

func main() {
	var (
		flagHelp, flagVersion bool
		flagIniFile           string
	)

	var consistent = true
	flag.StringVar(&dumpOptions.TemporalOptions.Tables, "tables", "", "List of comma-separated tables to dump. Include the database name: \"mydb.mytable,mydb2.mytable2\".")
	flag.StringVar(&dumpOptions.TemporalOptions.Databases, "databases", "", "List of comma-separated databases to dump.")
	flag.BoolVar(&dumpOptions.TemporalOptions.AllDatabases, "all-databases", false, "Dump all databases.")
	flag.BoolVar(&dumpOptions.TemporalOptions.IncludeSystemDatabases, "include-system-databases", false, "Include mysql schema with --all-databases (for on-prem account migration). Excluded by default to protect Cloud SQL IAM permissions.")
	flag.StringVar(&dumpOptions.MySQLHost.HostName, "mysql-host", "localhost", "MySQL hostname.")
	flag.StringVar(&dumpOptions.MySQLHost.SocketFile, "mysql-socket", "", "MySQL socket file.")
	flag.IntVar(&dumpOptions.MySQLHost.Port, "mysql-port", 3306, "MySQL port number.")
	flag.StringVar(&dumpOptions.MySQLCredentials.User, "mysql-user", "root", "MySQL user name.")
	flag.StringVar(&dumpOptions.MySQLCredentials.Password, "mysql-password", "", "MySQL password.")
	flag.IntVar(&dumpOptions.Threads, "threads", 1, "Number of threads to use.")
	flag.Uint64Var(&dumpOptions.ChunkSize, "chunk-size", 1000, "Number of rows per read chunk.")
	flag.Uint64Var(&dumpOptions.OutputChunkSize, "output-chunk-size", 0, "Number of rows per INSERT statement (0 = same as --chunk-size).")
	flag.Uint64Var(&dumpOptions.StatementSize, "statement-size", 16*1024*1024, "Max bytes per INSERT statement. Rows are grouped until this cap so wide TEXT/BLOB rows never exceed the target's max_allowed_packet. 0 disables the cap.")
	flag.IntVar(&dumpOptions.ChannelBufferSize, "channel-buffer-size", 1000, "Task channel buffer size.")
	flag.BoolVar(&dumpOptions.LockTables, "lock-tables", true, "Lock tables to get a consistent backup.")
	flag.IntVar(&dumpOptions.LockWaitTimeout, "lock-wait-timeout", 60, "Seconds to wait for FLUSH TABLES WITH READ LOCK / LOCK TABLES before aborting. Prevents a blocked lock from stalling the whole server. 0 = server default.")
	flag.StringVar(&dumpOptions.TablesWithoutUKOption, "tables-without-uniquekey", "error", "Action for tables without a primary or unique key. Valid: 'error', 'single-chunk', 'skip'.")
	flag.BoolVar(&dumpOptions.TemporalOptions.Debug, "debug", false, "Display debug information.")
	flag.StringVar(&dumpOptions.DestinationDir, "destination", "", "Directory to store the dumps.")
	flag.BoolVar(&flagHelp, "help", false, "Display this message.")
	flag.BoolVar(&flagVersion, "version", false, "Display version and exit.")
	flag.BoolVar(&dumpOptions.TemporalOptions.DryRun, "dry-run", false, "Calculate and display chunk counts per table without dumping.")
	flag.BoolVar(&dumpOptions.TemporalOptions.Execute, "execute", false, "Execute the dump.")
	flag.BoolVar(&dumpOptions.SkipUseDatabase, "skip-use-database", false, "Omit USE \"database\" statements from output files.")
	flag.BoolVar(&dumpOptions.GetMasterStatus, "get-master-status", false, "Record the binary log position at dump time.")
	flag.BoolVar(&dumpOptions.GetSlaveStatus, "get-slave-status", false, "Record the replica status at dump time.")
	flag.BoolVar(&dumpOptions.AddDropTable, "add-drop-table", false, "Prepend DROP TABLE IF EXISTS before each CREATE TABLE (and DROP ... IF EXISTS before triggers/routines/events).")
	flag.StringVar(&dumpOptions.InsertMode, "insert-mode", dump.InsertModeInsert, "Statement verb for data rows: 'insert', 'replace', or 'insert-ignore'. replace/insert-ignore make a load idempotent against rows already present in the target.")
	flag.BoolVar(&dumpOptions.DumpTriggers, "triggers", false, "Dump triggers for the dumped tables (<schema>.<table>-triggers.sql).")
	flag.BoolVar(&dumpOptions.DumpRoutines, "routines", false, "Dump stored procedures and functions for the dumped schemas (<schema>-routines.sql).")
	flag.BoolVar(&dumpOptions.DumpEvents, "events", false, "Dump events for the dumped schemas (<schema>-events.sql).")
	flag.BoolVar(&dumpOptions.SkipDefiner, "skip-definer", false, "Strip DEFINER=... from trigger/routine/event definitions so they load where the definer account does not exist.")
	flag.BoolVar(&dumpOptions.Checksum, "checksum", false, "Run CHECKSUM TABLE after dump and write checksums.txt.")
	flag.BoolVar(&dumpOptions.Resume, "resume", false, "Resume a previous dump: skip completed tables and clean partial files.")
	flag.BoolVar(&dumpOptions.Compress, "compress", false, "Compress output files (see --compress-format).")
	flag.StringVar(&dumpOptions.CompressFormat, "compress-format", "gzip", "Compression format: 'gzip' (.gz) or 'zstd' (.zst). zstd is faster and smaller; requires go-load from this repo or the zstd CLI to decompress.")
	flag.IntVar(&dumpOptions.CompressLevel, "compress-level", 1, "Compression level: gzip 1 (fastest) to 9 (smallest); zstd 1 (fastest) to 19 (smallest).")
	flag.BoolVar(&dumpOptions.TemporalOptions.Quiet, "quiet", false, "Suppress INFO messages.")
	flag.StringVar(&dumpOptions.TemporalOptions.IsolationLevel, "isolation-level", "REPEATABLE READ", "Transaction isolation level. Use 'REPEATABLE READ' for a consistent backup.")
	flag.BoolVar(&dumpOptions.Consistent, "consistent", true, "Require a consistent (point-in-time) backup.")
	var dummyWhere string
	flag.StringVar(&dummyWhere, "where", "", "WHERE filter: global \"expr\" or per-table \"db.tbl:expr,db.tbl2:expr\".")
	flag.StringVar(&flagIniFile, "ini-file", "", "INI file for configuration options.")

	flag.Parse()

	if dummyWhere != "" {
		dump.ParseWhereCondition(dummyWhere, dumpOptions)
	}

	flag.Visit(func(f *flag.Flag) { flagSet[f.Name] = true })

	if flagIniFile != "" {
		dump.ParseIniFile(flagIniFile, dumpOptions, flagSet)
	}

	// Last-resort credential fallback — avoids passwords in shell history.
	if dumpOptions.MySQLCredentials.Password == "" && !flagSet["mysql-password"] {
		if envPwd := os.Getenv("GODUMP_PASSWORD"); envPwd != "" {
			dumpOptions.MySQLCredentials.Password = envPwd
		}
	}

	flags := make(map[string]*flag.Flag)
	flag.CommandLine.VisitAll(func(f *flag.Flag) { flags[f.Name] = f })

	if flagHelp {
		PrintUsage(flags)
		return
	}
	if flagVersion {
		fmt.Println("go-dump version:", AppVersion)
		return
	}

	if dumpOptions.TemporalOptions.Debug {
		log.SetLevel(log.DEBUG)
	} else if dumpOptions.TemporalOptions.Quiet {
		log.SetLevel(log.WARNING)
	} else {
		log.SetLevel(log.INFO)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Warning("Interrupt received — cancelling in-flight operations...")
		cancel()
	}()

	switch dumpOptions.TablesWithoutUKOption {
	case "error", "single-chunk", "skip":
		log.Debugf("Tables without primary or unique key will be handled with: \"%s\".", dumpOptions.TablesWithoutUKOption)
	default:
		log.Fatalf("Invalid --tables-without-uniquekey value \"%s\". Valid values: error, single-chunk, skip.",
			dumpOptions.TablesWithoutUKOption)
	}

	switch dumpOptions.InsertMode {
	case dump.InsertModeInsert, dump.InsertModeReplace, dump.InsertModeInsertIgnore:
	default:
		log.Fatalf("Invalid --insert-mode value \"%s\". Valid values: insert, replace, insert-ignore.",
			dumpOptions.InsertMode)
	}

	if !dumpOptions.LockTables && dumpOptions.Consistent {
		log.Fatal("--lock-tables is required for a consistent backup. Use --help for more information.")
	}

	if dumpOptions.DestinationDir == "" {
		log.Fatal("--destination is required. Use --help for more information.")
	}

	switch strings.ToUpper(dumpOptions.TemporalOptions.IsolationLevel) {
	case "SERIALIZABLE":
		dumpOptions.IsolationLevel = sql.LevelSerializable
	case "REPEATABLE READ":
		dumpOptions.IsolationLevel = sql.LevelRepeatableRead
	case "READ COMMITTED":
		dumpOptions.IsolationLevel = sql.LevelReadCommitted
		consistent = false
	case "READ UNCOMMITTED":
		dumpOptions.IsolationLevel = sql.LevelReadUncommitted
		consistent = false
	default:
		log.Fatalf("Unknown isolation level \"%s\". Use --help for more information.", dumpOptions.TemporalOptions.IsolationLevel)
	}

	if !consistent && dumpOptions.Consistent {
		log.Fatalf("Isolation level \"%s\" is not compatible with --consistent. Use --help for more information.",
			dumpOptions.TemporalOptions.IsolationLevel)
	}

	if dumpOptions.OutputChunkSize == 0 {
		dumpOptions.OutputChunkSize = dumpOptions.ChunkSize
	}

	switch dumpOptions.CompressFormat {
	case dump.CompressFormatGzip:
		if dumpOptions.CompressLevel < 1 || dumpOptions.CompressLevel > 9 {
			log.Fatal("--compress-level must be between 1 and 9 for gzip.")
		}
	case dump.CompressFormatZstd:
		if dumpOptions.CompressLevel < 1 || dumpOptions.CompressLevel > 19 {
			log.Fatal("--compress-level must be between 1 and 19 for zstd.")
		}
	default:
		log.Fatalf("--compress-format must be 'gzip' or 'zstd', got %q.", dumpOptions.CompressFormat)
	}

	if !dumpOptions.TemporalOptions.AllDatabases &&
		dumpOptions.TemporalOptions.Databases == "" &&
		dumpOptions.TemporalOptions.Tables == "" {
		log.Fatal("Specify at least one of --databases, --tables, or --all-databases. Use --help for more information.")
	}

	if !dumpOptions.TemporalOptions.DryRun && !dumpOptions.TemporalOptions.Execute {
		log.Fatal("Specify --dry-run or --execute. Use --help for more information.")
	}

	if dumpOptions.TemporalOptions.DryRun && dumpOptions.TemporalOptions.Execute {
		log.Fatal("--dry-run and --execute are mutually exclusive.")
	}

	dumpOptions.AppVersion = AppVersion
	dump.Run(ctx, dumpOptions)
}
