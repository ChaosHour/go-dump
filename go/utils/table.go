package utils

import (
	"database/sql"
	"fmt"

	"github.com/outbrain/golib/log"
)

type ColumnsMap map[string]int

// Table contains the name and type of a table.
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

// getColumnsInformationSQL return the SQL statment to get the columns
// information of a table
func (t *Table) getColumnsInformationSQL() string {
	return fmt.Sprintf(`SELECT COLUMN_NAME,COLUMN_KEY
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'
		  AND COLUMN_KEY IN ('PRI','UNI','MUL')
			AND DATA_TYPE IN ('tinyint','smallint','int','mediumint','bigint','timestamp')
				`, t.GetUnescapedSchema(), t.GetUnescapedName())
}

/*
TABLE_CATALOG: def
	TABLE_SCHEMA: panel_socialtools_dev
		TABLE_NAME: twitter_collector_twitterstatusentitiesmedia
		TABLE_TYPE: BASE TABLE
				ENGINE: InnoDB
			 VERSION: 10
		ROW_FORMAT: Dynamic
		TABLE_ROWS: 57891
AVG_ROW_LENGTH: 299
	 DATA_LENGTH: 17350656
MAX_DATA_LENGTH: 0
	INDEX_LENGTH: 4227072
		 DATA_FREE: 0
AUTO_INCREMENT: NULL
	 CREATE_TIME: 2018-02-15 11:58:17
	 UPDATE_TIME: NULL
		CHECK_TIME: NULL
TABLE_COLLATION: latin1_swedish_ci
			CHECKSUM: NULL
CREATE_OPTIONS:
 TABLE_COMMENT:
*/

// GetFullName return a string with database and table name escaped.
func (t *Table) GetFullName() string {
	return fmt.Sprintf("`%s`.`%s`", t.schema, t.name)
}

// GetSchema return a string with the database name escaped.
func (t *Table) GetSchema() string {
	return fmt.Sprintf("`%s`", t.schema)
}

// GetName return a string with the table name escaped.
func (t *Table) GetName() string {
	return fmt.Sprintf("`%s`", t.name)
}

// GetUnescapedSchema return a string with the database name.
func (t *Table) GetUnescapedSchema() string {
	return t.schema
}

// GetUnescapedName return a string with the table name.
func (t *Table) GetUnescapedName() string {
	return t.name
}

// GetUnescapedFullName return a string with database and table name.
func (t *Table) GetUnescapedFullName() string {
	return fmt.Sprintf("%s.%s", t.schema, t.name)
}

// GetPrimaryOrUniqueKey return a string with the name of the unique or primary
// key filed that we will use to split the table.
// Empty string means that the table doens't have any primary or unique key to use.
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

// getTableInformation collect and store the table information
func (t *Table) getTableInformation(db *sql.DB) error {

	var tableName string
	err := db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", t.GetFullName())).Scan(&tableName, &t.CreateTableSQL)
	if err != nil {
		log.Fatalf("Error getting show create table: %s", err.Error())
	}

	query := fmt.Sprintf(`SELECT ENGINE, TABLE_COLLATION, DATA_LENGTH, INDEX_LENGTH,
		TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='%s' AND TABLE_NAME='%s'`,
		t.GetUnescapedSchema(), t.GetUnescapedName())
	err = db.QueryRow(query).Scan(&t.Engine, &t.Collation,
		&t.estDataSize, &t.estIndexSize, &t.estNumberOfRows)
	return err
}

// getData collect the table information
func (t *Table) getData(db *sql.DB) error {

	t.getTableInformation(db)

	rows, err := db.Query(t.getColumnsInformationSQL())

	if err != nil {
		log.Fatal("Error getting column details for table ", t.GetFullName(), " : ", err.Error())
	}

	var cName, cKey string

	for rows.Next() {
		rows.Scan(&cName, &cKey)
		switch cKey {
		case "PRI":
			t.primaryKey = append(t.primaryKey, cName)
		case "UNI":
			t.uniqueKey = append(t.uniqueKey, cName)
		default:

		}
	}
	return nil
}

// NewTable create a new Table object.
func NewTable(schema string, name string, db *sql.DB) *Table {
	table := &Table{
		name:     name,
		schema:   schema,
		IsLocked: false,
	}

	table.getData(db)
	return table
}
