package dump

import (
	"database/sql"
	"fmt"

	"github.com/ChaosHour/go-dump/internal/log"
)

type ColumnsMap map[string]int

// Table contains the name and metadata of a MySQL table.
type Table struct {
	name            string
	schema          string
	primaryKey      []string
	uniqueKey       []string
	keyForChunks    string
	estNumberOfRows uint64
	estDataSize     uint64
	estIndexSize    uint64

	CreateTableSQL string
	IsLocked       bool
	Engine         string
	Collation      string
}

func (t *Table) GetFullName() string {
	return fmt.Sprintf("`%s`.`%s`", t.schema, t.name)
}

func (t *Table) GetSchema() string {
	return fmt.Sprintf("`%s`", t.schema)
}

func (t *Table) GetName() string {
	return fmt.Sprintf("`%s`", t.name)
}

func (t *Table) GetUnescapedSchema() string {
	return t.schema
}

func (t *Table) GetUnescapedName() string {
	return t.name
}

func (t *Table) GetUnescapedFullName() string {
	return fmt.Sprintf("%s.%s", t.schema, t.name)
}

// GetPrimaryOrUniqueKey returns the single-column key used for chunking.
// Empty string means no usable key exists.
func (t *Table) GetPrimaryOrUniqueKey() string {
	if len(t.keyForChunks) > 0 {
		return t.keyForChunks
	}
	if len(t.primaryKey) == 1 {
		t.keyForChunks = t.primaryKey[0]
		return t.keyForChunks
	}
	if len(t.uniqueKey) > 0 {
		t.keyForChunks = t.uniqueKey[0]
		return t.keyForChunks
	}
	return ""
}

func (t *Table) getTableInformation(db *sql.DB) error {
	var tableName string
	// SHOW CREATE TABLE cannot be parameterised; name is backtick-escaped via GetFullName.
	err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", t.GetFullName())).Scan(&tableName, &t.CreateTableSQL)
	if err != nil {
		return fmt.Errorf("SHOW CREATE TABLE %s: %w", t.GetFullName(), err)
	}

	// ENGINE and TABLE_COLLATION are NULLable in MySQL 8.0+ for generated columns/expressions.
	var engine, collation sql.NullString
	const q = `SELECT ENGINE, TABLE_COLLATION, DATA_LENGTH, INDEX_LENGTH, TABLE_ROWS
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	err = db.QueryRow(q, t.GetUnescapedSchema(), t.GetUnescapedName()).
		Scan(&engine, &collation, &t.estDataSize, &t.estIndexSize, &t.estNumberOfRows)
	if err != nil {
		return fmt.Errorf("information_schema for %s: %w", t.GetFullName(), err)
	}
	if engine.Valid {
		t.Engine = engine.String
	}
	if collation.Valid {
		t.Collation = collation.String
	}
	return nil
}

func (t *Table) getData(db *sql.DB) error {
	if err := t.getTableInformation(db); err != nil {
		return err
	}

	// MUL (non-unique index) is excluded — it is never usable as a chunk key.
	// Integer types only: chunk bounds use int64 arithmetic (min..max, +1).
	// NOT NULL is required because BETWEEN / >= never match NULL — rows with a
	// NULL key would silently be excluded from every chunk.
	const colQuery = `SELECT COLUMN_NAME, COLUMN_KEY
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		  AND COLUMN_KEY IN ('PRI', 'UNI')
		  AND IS_NULLABLE = 'NO'
		  AND DATA_TYPE IN ('tinyint', 'smallint', 'int', 'mediumint', 'bigint')`

	rows, err := db.Query(colQuery, t.GetUnescapedSchema(), t.GetUnescapedName())
	if err != nil {
		return fmt.Errorf("column information for %s: %w", t.GetFullName(), err)
	}
	defer rows.Close()

	var cName, cKey string
	for rows.Next() {
		if err := rows.Scan(&cName, &cKey); err != nil {
			return fmt.Errorf("scanning column row for %s: %w", t.GetFullName(), err)
		}
		switch cKey {
		case "PRI":
			t.primaryKey = append(t.primaryKey, cName)
		case "UNI":
			t.uniqueKey = append(t.uniqueKey, cName)
		}
	}
	return rows.Err()
}

// NewTable creates a Table and loads its metadata from MySQL.
func NewTable(schema string, name string, db *sql.DB) *Table {
	table := &Table{name: name, schema: schema, IsLocked: false}
	if err := table.getData(db); err != nil {
		log.Fatalf("Error loading table metadata for %s.%s: %v", schema, name, err)
	}
	return table
}
