package utils

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/outbrain/golib/log"
	ini "gopkg.in/ini.v1"
)

// normalizeTableName converts a table name like "schema.table" to "`schema`.`table`"
func normalizeTableName(tableName string) string {
	if strings.Contains(tableName, ".") {
		parts := strings.Split(tableName, ".")
		if len(parts) == 2 {
			return fmt.Sprintf("`%s`.`%s`", parts[0], parts[1])
		}
	}
	return tableName
}

type DumpOptions struct {
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
	WhereConditions       map[string]string // table -> where condition
	GlobalWhereCondition  string            // fallback for all tables
	TemporalOptions       TemporalOptions
}

type TemporalOptions struct {
	Tables, Databases, IsolationLevel           string
	AllDatabases, Debug, DryRun, Execute, Quiet bool
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

// GetDumpOptions creates and returns a new DumpOptions instance with default values
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

func ParseString(s interface{}) []byte {

	escape := false
	var rets []byte
	for _, b := range s.([]byte) {
		switch b {
		case byte('\''):
			escape = true
		case byte('\\'):
			escape = true
		case byte('"'):
			escape = true
		case byte('\n'):
			b = byte('n')
			escape = true
		case byte('\r'):
			b = byte('r')
			escape = true
		}

		if escape {
			rets = append(rets, byte('\\'), b)
			escape = false
		} else {
			rets = append(rets, b)
		}
	}
	return rets
}

func TablesFromString(tablesParam string) map[string]bool {
	ret := make(map[string]bool)

	tables := strings.Split(tablesParam, ",")

	for _, table := range tables {
		if _, ok := ret[table]; !ok {
			ret[table] = true
		}
	}
	return ret
}

func getTablesFromQuery(query string, db *sql.DB) map[string]bool {
	tables := make(map[string]bool)

	if db == nil {
		log.Fatal("Database connection is nil")
		return tables
	}

	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error executing query: %v", err)
		return tables
	}
	defer rows.Close()

	// ... rest of the function ...
	return tables
}

func TablesFromAllDatabases(db *sql.DB) map[string]bool {

	query := `SELECT TABLE_SCHEMA, TABLE_NAME
		FROM information_schema.TABLES WHERE TABLE_TYPE ='BASE TABLE'  AND
		TABLE_SCHEMA NOT IN ('performance_schema') AND
		NOT (TABLE_SCHEMA = 'mysql' AND (TABLE_NAME = 'slow_log' OR TABLE_NAME = 'general_log'))`

	log.Debug("Query: ", query)
	return getTablesFromQuery(query, db)
}

func TablesFromDatabase(databases string, db *sql.DB) map[string]bool {
	tables := make(map[string]bool)

	for _, database := range strings.Split(databases, ",") {
		// Sanitize database name
		database = strings.TrimSpace(database)

		// Use proper SQL query with placeholders
		query := "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?"
		rows, err := db.Query(query, database)
		if err != nil {
			log.Fatalf("Error querying tables from database %s: %v", database, err)
		}
		defer rows.Close()

		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				log.Fatalf("Error scanning table name: %v", err)
			}
			tables[database+"."+tableName] = true
		}

		if err = rows.Err(); err != nil {
			log.Fatalf("Error iterating table rows: %v", err)
		}
	}

	return tables
}

func GetLockAllTablesSQL() string {
	return "FLUSH TABLES WITH READ LOCK"
}

func GetLockTablesSQL(tasksPool []*Task, mode string) string {
	var tables []string
	for _, task := range tasksPool {
		tables = append(tables, fmt.Sprintf(" %s %s", task.Table.GetFullName(), mode))
	}
	return fmt.Sprintf("LOCK TABLES %s", strings.Join(tables, ","))
}

func GetUseDatabaseSQL(schema string) string {
	return fmt.Sprintf("USE %s", schema)
}

func GetMasterStatusSQL() string {
	return "SHOW MASTER STATUS"
}

func GetDropTableIfExistSQL(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
}

func GetShowCreateTableSQL(table string) string {
	return fmt.Sprintf("SHOW CREATE TABLE %s", table)
}

// GetMySQLConnection return the string to connect to the mysql server
func GetMySQLConnection(host *MySQLHost, credentials *MySQLCredentials) (*sql.DB, error) {
	var hoststring, userpass string
	userpass = fmt.Sprintf("%s:%s", credentials.User, credentials.Password)

	if len(host.SocketFile) > 0 {
		hoststring = fmt.Sprintf("unix(%s)", host.SocketFile)
	} else {
		hoststring = fmt.Sprintf("tcp(%s:%d)", host.HostName, host.Port)
	}
	log.Debugf(fmt.Sprintf("%s@%s/", userpass, hoststring))
	db, err := sql.Open("mysql", fmt.Sprintf("%s@%s/", userpass, hoststring))
	if err != nil {
		log.Fatalf("MySQL connection error: %s", err.Error())
	}
	err = db.Ping()
	if err != nil {
		log.Fatalf("MySQL connection error: %s", err.Error())
	}

	return db, nil
}

func ParseIniFile(iniFile string, do *DumpOptions, flagSet map[string]bool) {
	cfg, err := ini.Load(iniFile)
	if err != nil {
		log.Errorf("Failed to read the ini file %s: %s", iniFile, err.Error())
	}

	// Check the different sections in the ini file
	for section := range cfg.Sections() {
		cfg.Sections()[section].Name()
		switch cfg.Sections()[section].Name() {
		case "client", "mysqldump":
			parseMySQLIniOptions(cfg.Sections()[section], do, flagSet)
		case "go-dump":
			parseIniOptions(cfg.Sections()[section], do, flagSet)
		}
	}
}

func parseMySQLIniOptions(section *ini.Section, do *DumpOptions, flagSet map[string]bool) {
	var err error
	for key := range section.Keys() {
		if flagSet["mysql-"+section.Keys()[key].Name()] {
			continue
		}

		switch section.Keys()[key].Name() {
		case "user":
			do.MySQLCredentials.User = section.Keys()[key].Value()
		case "password":
			do.MySQLCredentials.Password = section.Keys()[key].Value()
		case "host":
			do.MySQLHost.HostName = section.Keys()[key].Value()
		case "port":
			if section.Keys()[key].Value() != "" {
				do.MySQLHost.Port, err = strconv.Atoi(section.Keys()[key].Value())
				if err != nil {
					log.Fatalf("Port number %s can not be converted to integer. Error: %s", section.Keys()[key].Value(), err.Error())
				}
			}
		case "socket":
			do.MySQLHost.SocketFile = section.Keys()[key].Value()
		}
	}
}

func parseIniOptions(section *ini.Section, do *DumpOptions, flagSet map[string]bool) {
	var errInt, errBool error
	for key := range section.Keys() {
		if flagSet[section.Keys()[key].Name()] {
			continue
		}

		switch section.Keys()[key].Name() {
		case "mysql-user":
			do.MySQLCredentials.User = section.Keys()[key].Value()
		case "mysql-password":
			do.MySQLCredentials.Password = section.Keys()[key].Value()
		case "mysql-host":
			do.MySQLHost.HostName = section.Keys()[key].Value()
		case "mysql-port":
			if section.Keys()[key].Value() != "" {
				do.MySQLHost.Port, errInt = strconv.Atoi(section.Keys()[key].Value())
			}
		case "mysql-socket":
			do.MySQLHost.SocketFile = section.Keys()[key].Value()
		case "threads":
			if section.Keys()[key].Value() != "" {
				do.Threads, errInt = strconv.Atoi(section.Keys()[key].Value())
			}
		case "chunk-size":
			do.ChunkSize, errInt = strconv.ParseUint(section.Keys()[key].Value(), 10, 64)
		case "output-chunk-size":
			do.OutputChunkSize, errInt = strconv.ParseUint(section.Keys()[key].Value(), 10, 64)
		case "lock-tables":
			do.LockTables, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "tables-without-uniquekey":
			do.TablesWithoutUKOption = section.Keys()[key].Value()
		case "destination":
			do.DestinationDir = section.Keys()[key].Value()
		case "skip-use-database":
			do.SkipUseDatabase, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "get-master-status":
			do.GetMasterStatus, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "get-slave-status":
			do.LockTables, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "add-drop-table":
			do.AddDropTable, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "compress":
			do.Compress, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "compress-level":
			if section.Keys()[key].Value() != "" {
				do.CompressLevel, errInt = strconv.Atoi(section.Keys()[key].Value())
			}
		case "consistent":
			do.Consistent, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "where":
			// Parse table-specific WHERE conditions
			whereValue := section.Keys()[key].Value()
			if strings.Contains(whereValue, ":") {
				// Table-specific: "table:condition,table2:condition2"
				parts := strings.Split(whereValue, ",")
				if do.WhereConditions == nil {
					do.WhereConditions = make(map[string]string)
				}
				for _, part := range parts {
					if tableCond := strings.SplitN(strings.TrimSpace(part), ":", 2); len(tableCond) == 2 {
						normalizedTableName := normalizeTableName(tableCond[0])
						do.WhereConditions[normalizedTableName] = tableCond[1]
					}
				}
			} else {
				// Global WHERE condition
				do.GlobalWhereCondition = whereValue
			}
		case "tables":
			do.TemporalOptions.Tables = section.Keys()[key].Value()
		case "databases":
			do.TemporalOptions.Databases = section.Keys()[key].Value()
		case "isolation-level":
			do.TemporalOptions.IsolationLevel = section.Keys()[key].Value()
		case "all-databases":
			do.TemporalOptions.AllDatabases, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "debug":
			do.TemporalOptions.Debug, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "dry-run":
			do.TemporalOptions.DryRun, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "execute":
			do.TemporalOptions.Execute, errBool = strconv.ParseBool(section.Keys()[key].Value())
		case "quiet":
			do.TemporalOptions.Quiet, errBool = strconv.ParseBool(section.Keys()[key].Value())
		default:
			log.Warningf("Unknown option %s", section.Keys()[key].Name())
		}

		if errInt != nil {
			log.Fatalf("Variable %s with the value %s can not be converted to integer. Error: %s",
				section.Keys()[key].Name(), section.Keys()[key].Value(), errInt.Error())
		}
		if errBool != nil {
			log.Fatalf("Variable %s with the value %s can not be converted to boolean. Error: %s",
				section.Keys()[key].Name(), section.Keys()[key].Value(), errBool.Error())
		}
	}
}
