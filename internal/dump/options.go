package dump

import "database/sql"

type DumpOptions struct {
	AppVersion            string
	MySQLHost             *MySQLHost
	MySQLCredentials      *MySQLCredentials
	Threads               int
	ChunkSize             uint64
	OutputChunkSize       uint64
	ChannelBufferSize     int
	LockTables            bool
	TablesWithoutUKOption string
	DestinationDir        string
	AddDropTable          bool
	GetMasterStatus       bool
	GetSlaveStatus        bool
	SkipUseDatabase       bool
	Compress              bool
	CompressLevel         int
	IsolationLevel        sql.IsolationLevel
	Consistent            bool
	Checksum              bool
	Resume                bool
	WhereConditions       map[string]string // table -> where condition
	GlobalWhereCondition  string            // fallback for all tables
	TemporalOptions       TemporalOptions
}

type TemporalOptions struct {
	Tables, Databases, IsolationLevel                    string
	AllDatabases, Debug, DryRun, Execute, Quiet          bool
	IncludeSystemDatabases                               bool
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
		ChannelBufferSize:     1000,
		LockTables:            true,
		TablesWithoutUKOption: "error",
		AddDropTable:          false,
		GetMasterStatus:       true,
		GetSlaveStatus:        false,
		SkipUseDatabase:       false,
		Compress:              false,
		CompressLevel:         1,
		IsolationLevel:        sql.LevelRepeatableRead,
		Consistent:            true,
		WhereConditions:       make(map[string]string),
		TemporalOptions: TemporalOptions{
			IsolationLevel: "REPEATABLE READ",
		},
	}
}
