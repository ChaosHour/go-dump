package dump

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/ChaosHour/go-dump/internal/log"
	"github.com/go-sql-driver/mysql"
)

// escapeIdentifier backtick-quotes a MySQL identifier, doubling any embedded
// backticks.
func escapeIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// NormalizeTableName converts "schema.table" to "`schema`.`table`".
func NormalizeTableName(tableName string) string {
	if strings.Contains(tableName, ".") {
		parts := strings.Split(tableName, ".")
		if len(parts) == 2 {
			return fmt.Sprintf("`%s`.`%s`", parts[0], parts[1])
		}
	}
	return tableName
}

// ParseString escapes a byte slice for safe use inside single-quoted SQL literals.
// Uses '' for single-quote (compatible with NO_BACKSLASH_ESCAPES).
func ParseString(s interface{}) []byte {
	var rets []byte
	for _, b := range s.([]byte) {
		switch b {
		case byte('\''):
			rets = append(rets, byte('\''), byte('\''))
		case byte('\\'):
			rets = append(rets, byte('\\'), byte('\\'))
		case byte('\n'):
			rets = append(rets, byte('\\'), byte('n'))
		case byte('\r'):
			rets = append(rets, byte('\\'), byte('r'))
		case byte(0):
			rets = append(rets, byte('\\'), byte('0'))
		case byte(26): // Ctrl-Z / SUB
			rets = append(rets, byte('\\'), byte('Z'))
		default:
			rets = append(rets, b)
		}
	}
	return rets
}

// ParseWhereCondition parses a --where value and populates opts.
// Format: "condition" (global) or "db.tbl:condition,db.tbl2:condition2" (per-table).
func ParseWhereCondition(whereValue string, do *DumpOptions) {
	if !strings.Contains(whereValue, ":") {
		do.GlobalWhereCondition = whereValue
		return
	}
	if do.WhereConditions == nil {
		do.WhereConditions = make(map[string]string)
	}
	for _, part := range strings.Split(whereValue, ",") {
		if kv := strings.SplitN(strings.TrimSpace(part), ":", 2); len(kv) == 2 {
			do.WhereConditions[NormalizeTableName(kv[0])] = kv[1]
		}
	}
}

// TablesFromString parses a comma-separated "schema.table" list.
func TablesFromString(tablesParam string) map[string]bool {
	ret := make(map[string]bool)
	for _, table := range strings.Split(tablesParam, ",") {
		ret[table] = true
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
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			log.Fatalf("Error scanning table row: %v", err)
			return tables
		}
		tables[schema+"."+table] = true
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Error iterating table rows: %v", err)
	}
	return tables
}

// TablesFromAllDatabases returns all base tables across non-system databases.
// By default it excludes mysql, sys, information_schema, and performance_schema —
// safe for both on-prem and Cloud SQL (which manages mysql.* via IAM).
// Pass includeSystem=true to include mysql (minus slow_log/general_log) for
// on-prem full-cluster migrations that need to transfer accounts.
func TablesFromAllDatabases(db *sql.DB, includeSystem bool) map[string]bool {
	var query string
	if includeSystem {
		query = `SELECT TABLE_SCHEMA, TABLE_NAME
			FROM information_schema.TABLES
			WHERE TABLE_TYPE = 'BASE TABLE'
			  AND TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'sys')
			  AND NOT (TABLE_SCHEMA = 'mysql' AND TABLE_NAME IN ('slow_log', 'general_log'))`
	} else {
		query = `SELECT TABLE_SCHEMA, TABLE_NAME
			FROM information_schema.TABLES
			WHERE TABLE_TYPE = 'BASE TABLE'
			  AND TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')`
	}
	log.Debugf("TablesFromAllDatabases query (includeSystem=%v): %s", includeSystem, query)
	return getTablesFromQuery(query, db)
}

// TablesFromDatabase returns all base tables in the given comma-separated list of databases.
// Fixed: was using defer inside a loop which held all connections open simultaneously.
func TablesFromDatabase(databases string, db *sql.DB) map[string]bool {
	tables := make(map[string]bool)
	for _, database := range strings.Split(databases, ",") {
		database = strings.TrimSpace(database)
		if database == "" {
			continue
		}
		if err := tablesFromSingleDatabase(database, db, tables); err != nil {
			log.Fatalf("Error querying tables from database %s: %v", database, err)
		}
	}
	return tables
}

func tablesFromSingleDatabase(database string, db *sql.DB, out map[string]bool) error {
	const q = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'"
	rows, err := db.Query(q, database)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		out[database+"."+tableName] = true
	}
	return rows.Err()
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

func GetDropTableIfExistSQL(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
}

func GetShowCreateTableSQL(table string) string {
	return fmt.Sprintf("SHOW CREATE TABLE %s", table)
}

// setPoolLimits configures sql.DB pool sizes relative to thread count.
// Prevents connection exhaustion while keeping idle connections ready.
func setPoolLimits(db *sql.DB, threads int) {
	max := threads + 4
	db.SetMaxOpenConns(max)
	db.SetMaxIdleConns(max)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)
}

// GetMySQLConnection opens and verifies a MySQL connection.
// Uses mysql.Config.FormatDSN so passwords with special characters are handled correctly.
func GetMySQLConnection(host *MySQLHost, credentials *MySQLCredentials) (*sql.DB, error) {
	cfg := mysql.Config{
		User:                 credentials.User,
		Passwd:               credentials.Password,
		AllowNativePasswords: true,
	}
	if len(host.SocketFile) > 0 {
		cfg.Net = "unix"
		cfg.Addr = host.SocketFile
	} else {
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("%s:%d", host.HostName, host.Port)
	}

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("mysql open %s: %w", cfg.Addr, err)
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("mysql ping %s: %w", cfg.Addr, err)
	}
	return db, nil
}
