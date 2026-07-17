package dump

import "database/sql"

type DumpOptions struct {
	AppVersion            string
	MySQLHost             *MySQLHost
	MySQLCredentials      *MySQLCredentials
	Threads               int
	ChunkSize             uint64
	OutputChunkSize       uint64
	StatementSize         uint64 // max bytes per INSERT statement; a row group is flushed once it exceeds this
	ChannelBufferSize     int
	LockTables            bool
	LockWaitTimeout       int // seconds to wait for FTWRL / LOCK TABLES before aborting
	TablesWithoutUKOption string
	DestinationDir        string
	AddDropTable          bool
	GetMasterStatus       bool
	GetSlaveStatus        bool
	SkipUseDatabase       bool
	Compress              bool
	CompressFormat        string // "gzip" or "zstd"
	CompressLevel         int
	IsolationLevel        sql.IsolationLevel
	Consistent            bool
	Checksum              bool
	Resume                bool
	DumpTriggers          bool
	DumpRoutines          bool // stored procedures and functions
	DumpEvents            bool
	SkipDefiner           bool              // strip DEFINER=... from trigger/routine/event definitions
	WhereConditions       map[string]string // table -> where condition
	GlobalWhereCondition  string            // fallback for all tables
	TemporalOptions       TemporalOptions
}

type TemporalOptions struct {
	Tables, Databases, IsolationLevel           string
	AllDatabases, Debug, DryRun, Execute, Quiet bool
	IncludeSystemDatabases                      bool
}

type MySQLHost struct {
	HostName   string
	SocketFile string
	Port       int
}

type MySQLCredentials struct {
	User     string
	Password string
}

// GetDumpOptions returns a new DumpOptions with production-safe defaults.
func GetDumpOptions() *DumpOptions {
	return &DumpOptions{
		MySQLHost:             &MySQLHost{HostName: "localhost", Port: 3306},
		MySQLCredentials:      &MySQLCredentials{},
		Threads:               1,
		ChunkSize:             1000,
		OutputChunkSize:       0,
		StatementSize:         16 * 1024 * 1024,
		ChannelBufferSize:     1000,
		LockTables:            true,
		LockWaitTimeout:       60,
		TablesWithoutUKOption: "error",
		AddDropTable:          false,
		GetMasterStatus:       true,
		GetSlaveStatus:        false,
		SkipUseDatabase:       false,
		Compress:              false,
		CompressFormat:        CompressFormatGzip,
		CompressLevel:         1,
		IsolationLevel:        sql.LevelRepeatableRead,
		Consistent:            true,
		WhereConditions:       make(map[string]string),
		TemporalOptions: TemporalOptions{
			IsolationLevel: "REPEATABLE READ",
		},
	}
}
