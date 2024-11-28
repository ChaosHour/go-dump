package utils

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/outbrain/golib/log"
)

// DataChunk is the structure to handle the information of each chunk
type DataChunk struct {
	Min           int64
	Max           int64
	Sequence      uint64
	Task          *Task
	IsSingleChunk bool
	IsLastChunk   bool
}

// GetWhereSQL return the where condition for a chunk
func (dc *DataChunk) GetWhereSQL() string {
	if dc.IsSingleChunk {
		return ""
	}

	if dc.IsLastChunk {
		return fmt.Sprintf(" WHERE %s >= ?", dc.Task.Table.GetPrimaryOrUniqueKey())
	} else {
		return fmt.Sprintf(" WHERE %s BETWEEN ? AND ?", dc.Task.Table.GetPrimaryOrUniqueKey())
	}
}

func (dc *DataChunk) GetOrderBYSQL() string {
	if dc.IsSingleChunk {
		return ""
	}

	return fmt.Sprintf(" ORDER BY %s", dc.Task.Table.GetPrimaryOrUniqueKey())
}

func (dc *DataChunk) GetPrepareSQL() string {

	return fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ * FROM %s%s%s",
		dc.Task.Table.GetFullName(), dc.GetWhereSQL(), dc.GetOrderBYSQL())

}

func (dc *DataChunk) GetSampleSQL() string {
	return fmt.Sprintf("SELECT * FROM %s LIMIT 1", dc.Task.Table.GetFullName())
}

func (dc *DataChunk) Parse(stmt *sql.Stmt, buffer *Buffer) error {

	var rows *sql.Rows
	var err error
	if dc.IsSingleChunk {
		log.Debugf("Is single chunk %s.", dc.Task.Table.GetFullName())
		rows, err = stmt.Query()
	} else {
		if dc.IsLastChunk {
			rows, err = stmt.Query(dc.Min)
			log.Debugf("Last chunk %s.", dc.Task.Table.GetFullName())
		} else {
			rows, err = stmt.Query(dc.Min, dc.Max)
		}
	}

	if err != nil {
		log.Fatalf("%s", err.Error())
	}

	tablename := dc.Task.Table.GetFullName()

	if dc.IsSingleChunk {
		fmt.Fprintf(buffer, "-- Single chunk on %s\n", tablename)
	} else {
		fmt.Fprintf(buffer, "-- Chunk %d - from %d to %d\n",
			dc.Sequence, dc.Min, dc.Max)
	}

	columns, _ := rows.ColumnTypes()
	buff := make([]interface{}, len(columns))
	data := make([]interface{}, len(columns))
	for i := range buff {
		buff[i] = &data[i]
	}
	firstRow := true

	//var rowsNumber = uint64(0)
	for rows.Next() {

		/*
			if rowsNumber > 0 && rowsNumber%dc.Task.OutputChunkSize == 0 {
				fmt.Fprintf(buffer, ");\n\n")
				firstRow = true
			}
		*/

		if firstRow {
			fmt.Fprintf(buffer, "INSERT INTO %s VALUES \n(", dc.Task.Table.GetName())
		}
		err = rows.Scan(buff...)

		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		if !firstRow {
			fmt.Fprintf(buffer, "),\n(")
		} else {
			firstRow = false
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
			case nil:
				buffer.Write([]byte("NULL"))
			case time.Time:
				fmt.Fprintf(buffer, "%s", d)
			case float64:
				fmt.Fprintf(buffer, "%g", d)
			default:
				buffer.Write(d.([]byte))
			}
			if i != max-1 {
				fmt.Fprintf(buffer, ",")
			}
		}
	}
	rows.Close()
	fmt.Fprintf(buffer, ");\n")

	return nil
}

// Create a single chunk for a table, this is only when the table doesn't have
// primary key and the flag --table-without-pk-option is "single-chunk"
func NewSingleDataChunk(task *Task) DataChunk {
	return DataChunk{
		Sequence:      1,
		Task:          task,
		IsSingleChunk: true}

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
