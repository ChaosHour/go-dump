package dump

import (
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/ChaosHour/go-dump/internal/log"
)

// DataChunk holds the information for one read chunk of a table.
type DataChunk struct {
	Min           int64
	Max           int64
	Sequence      uint64
	Task          *Task
	IsSingleChunk bool
	IsLastChunk   bool
}

// GetWhereSQL returns the WHERE clause for this chunk, merging chunking bounds
// with any per-table or global WHERE condition from DumpOptions.
func (dc *DataChunk) GetWhereSQL() string {
	baseWhere := ""
	if dc.IsSingleChunk {
		baseWhere = ""
	} else if dc.IsLastChunk {
		baseWhere = fmt.Sprintf(" WHERE %s >= ?", escapeIdentifier(dc.Task.Table.GetPrimaryOrUniqueKey()))
	} else {
		baseWhere = fmt.Sprintf(" WHERE %s BETWEEN ? AND ?", escapeIdentifier(dc.Task.Table.GetPrimaryOrUniqueKey()))
	}

	dumpOptions := dc.Task.TaskManager.DumpOptions
	whereCondition := ""
	tableName := dc.Task.Table.GetFullName()
	if condition, exists := dumpOptions.WhereConditions[tableName]; exists {
		whereCondition = condition
	} else if dumpOptions.GlobalWhereCondition != "" {
		whereCondition = dumpOptions.GlobalWhereCondition
	}

	if whereCondition != "" {
		if baseWhere != "" {
			baseWhere += " AND (" + whereCondition + ")"
		} else {
			baseWhere = " WHERE (" + whereCondition + ")"
		}
	}
	return baseWhere
}

func (dc *DataChunk) GetOrderBYSQL() string {
	if dc.IsSingleChunk {
		return ""
	}
	return fmt.Sprintf(" ORDER BY %s", escapeIdentifier(dc.Task.Table.GetPrimaryOrUniqueKey()))
}

func (dc *DataChunk) GetPrepareSQL() string {
	return fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ * FROM %s%s%s",
		dc.Task.Table.GetFullName(), dc.GetWhereSQL(), dc.GetOrderBYSQL())
}

func (dc *DataChunk) GetSampleSQL() string {
	return fmt.Sprintf("SELECT * FROM %s LIMIT 1", dc.Task.Table.GetFullName())
}

// countingWriter tracks bytes written so Parse can cap INSERT statement size.
type countingWriter struct {
	w io.Writer
	n uint64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += uint64(n)
	return n, err
}

// Parse executes the chunk query and writes the resulting INSERT statements to w.
// Callers stage the output in memory so a failed chunk can be retried without
// duplicating rows already written to the dump file.
//
// Rows are grouped into multi-row INSERTs, split on whichever limit is hit
// first: OutputChunkSize rows, or StatementSize bytes. The byte cap exists
// because wide rows (large TEXT/BLOB columns) can push a row-count-only group
// past the target server's max_allowed_packet, producing a dump that cannot
// be restored anywhere. The cap is checked at row boundaries, so a statement
// can exceed it by at most one row.
func (dc *DataChunk) Parse(stmt *sql.Stmt, w io.Writer) error {
	var rows *sql.Rows
	var err error
	if dc.IsSingleChunk {
		log.Debugf("Is single chunk %s.", dc.Task.Table.GetFullName())
		rows, err = stmt.Query()
	} else if dc.IsLastChunk {
		log.Debugf("Last chunk %s.", dc.Task.Table.GetFullName())
		rows, err = stmt.Query(dc.Min)
	} else {
		rows, err = stmt.Query(dc.Min, dc.Max)
	}

	if err != nil {
		return fmt.Errorf("query chunk for %s: %w", dc.Task.Table.GetFullName(), err)
	}
	defer rows.Close()

	buffer := &countingWriter{w: w}
	maxStmtBytes := dc.Task.TaskManager.DumpOptions.StatementSize

	tablename := dc.Task.Table.GetFullName()
	if dc.IsSingleChunk {
		fmt.Fprintf(buffer, "-- Single chunk on %s\n", tablename)
	} else {
		fmt.Fprintf(buffer, "-- Chunk %d - from %d to %d\n", dc.Sequence, dc.Min, dc.Max)
	}

	columns, _ := rows.ColumnTypes()
	buff := make([]interface{}, len(columns))
	data := make([]interface{}, len(columns))
	for i := range buff {
		buff[i] = &data[i]
	}

	// Build INSERT prefix once with explicit column names.
	colNames := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = "`" + col.Name() + "`"
	}
	insertPrefix := fmt.Sprintf("INSERT INTO %s (%s) VALUES \n(",
		dc.Task.Table.GetName(), strings.Join(colNames, ","))

	firstRow := true
	var rowsNumber uint64
	var stmtStart uint64 // buffer.n at the start of the current INSERT statement

	for rows.Next() {
		err = rows.Scan(buff...)
		if err != nil {
			return fmt.Errorf("scan row for %s: %w", dc.Task.Table.GetFullName(), err)
		}

		if firstRow {
			stmtStart = buffer.n
			fmt.Fprintf(buffer, "%s", insertPrefix)
			firstRow = false
		} else {
			rowsNumber++
			splitOnRows := dc.Task.OutputChunkSize > 0 && rowsNumber%dc.Task.OutputChunkSize == 0
			splitOnBytes := maxStmtBytes > 0 && buffer.n-stmtStart >= maxStmtBytes
			if splitOnRows || splitOnBytes {
				fmt.Fprintf(buffer, ");\n")
				stmtStart = buffer.n
				fmt.Fprintf(buffer, "%s", insertPrefix)
			} else {
				fmt.Fprintf(buffer, "),\n(")
			}
		}

		max := len(data)
		for i, d := range data {
			switch d.(type) {
			case []byte:
				buffer.Write([]byte("'"))
				buffer.Write(ParseString(d))
				buffer.Write([]byte("'"))
			case int64:
				fmt.Fprintf(buffer, "%d", d)
			case uint64:
				fmt.Fprintf(buffer, "%d", d)
			case nil:
				buffer.Write([]byte("NULL"))
			case time.Time:
				fmt.Fprintf(buffer, "'%s'", d.(time.Time).Format("2006-01-02 15:04:05"))
			case float64:
				fmt.Fprintf(buffer, "%g", d)
			default:
				// Safe fallback for any other driver type (DECIMAL, BIT, ENUM, JSON, etc.).
				str := fmt.Sprintf("%v", d)
				buffer.Write([]byte("'"))
				buffer.Write(ParseString([]byte(str)))
				buffer.Write([]byte("'"))
			}
			if i != max-1 {
				fmt.Fprintf(buffer, ",")
			}
		}
	}

	// Check for errors that occurred during row iteration (e.g. network failure mid-result-set).
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error for %s: %w", dc.Task.Table.GetFullName(), err)
	}

	if !firstRow {
		fmt.Fprintf(buffer, ");\n")
	}
	return nil
}

func NewSingleDataChunk(task *Task) DataChunk {
	return DataChunk{Sequence: 1, Task: task, IsSingleChunk: true}
}

func NewDataChunk(task *Task) DataChunk {
	return DataChunk{
		Min:           task.chunkMin,
		Max:           task.chunkMax,
		Sequence:      task.TotalChunks,
		Task:          task,
		IsSingleChunk: false,
		IsLastChunk:   false}
}

func NewDataLastChunk(task *Task) DataChunk {
	return DataChunk{
		Min:           task.chunkMin,
		Sequence:      task.TotalChunks,
		Task:          task,
		IsSingleChunk: false,
		IsLastChunk:   true}
}
