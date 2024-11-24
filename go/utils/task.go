package utils

import (
	"database/sql"
	"fmt"

	"github.com/outbrain/golib/log"
)

type Task struct {
	Table           *Table
	ChunkSize       uint64
	OutputChunkSize uint64
	TaskManager     *TaskManager
	Tx              *sql.Tx
	Id              int64
	TotalChunks     uint64
	chunkMin        int64
	chunkMax        int64
}

func (t *Task) AddChunk(chunk DataChunk) {
	t.TaskManager.AddChunk(chunk)
	t.TotalChunks = t.TotalChunks + 1
	t.TaskManager.TotalChunks = t.TaskManager.TotalChunks + 1
	t.TaskManager.Queue = t.TaskManager.Queue + 1
	t.chunkMin = t.chunkMax + 1
	log.Debugf("Queue +1: %d ", t.TaskManager.Queue)
}

func (t *Task) GetSingleChunkTestQuery() string {
	return fmt.Sprintf("SELECT 1 FROM %s LIMIT 1 ", t.Table.GetFullName())
}

func (t *Task) GetChunkSqlQuery() string {
	keyForChunks := t.Table.GetPrimaryOrUniqueKey()

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= %d LIMIT 1 OFFSET %d", keyForChunks, t.Table.GetFullName(), keyForChunks, t.chunkMax, t.ChunkSize)

	return query
}

func (t *Task) GetLastChunkSqlQuery() string {

	keyForChunks := t.Table.GetPrimaryOrUniqueKey()
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s >= %d LIMIT 1",
		keyForChunks, t.Table.GetFullName(), keyForChunks, t.chunkMin)
}

func (t *Task) CreateChunks(db *sql.DB) {
	t.TotalChunks = 0
	t.chunkMax = 0
	t.chunkMin = 0

	var (
		tx       = db
		chunkMax = int64(0)
		chunkMin = int64(0)
		stopLoop = false
	)

	defer func() {
		t.TaskManager.CreateChunksWaitGroup.Done()
	}()

	if len(t.Table.GetPrimaryOrUniqueKey()) == 0 {
		switch t.TaskManager.TablesWithoutPKOption {
		case "single-chunk":
			log.Debugf(`Table %s doesn't have any primary or unique key, we will make it in a single chunk.`, t.Table.GetFullName())
			err := tx.QueryRow(t.GetSingleChunkTestQuery()).Scan(&chunkMax)
			switch err {
			case nil:
				t.AddChunk(NewSingleDataChunk(t))
			case sql.ErrNoRows:
				return
			default:
				log.Errorf("Error getting rows for table '%s'", t.Table.GetUnescapedFullName())
			}
			return
		case "error":
			log.Fatalf(`The table %s doesn't have any primary or unique key and the --tables-without-uniquekey is "error"`, t.Table.GetFullName())
		}
	}

	for !stopLoop {

		err := tx.QueryRow(t.GetChunkSqlQuery()).Scan(&chunkMax)
		if err != nil && err == sql.ErrNoRows {

			err := tx.QueryRow(t.GetLastChunkSqlQuery()).Scan(&chunkMin)
			if err == nil {
				t.AddChunk(NewDataLastChunk(t))
			}
			stopLoop = true
		} else {
			t.chunkMax = chunkMax
			t.AddChunk(NewDataChunk(t))
		}
	}

	log.Debugf("Table processed %s - %d chunks created",
		t.Table.GetFullName(), t.TotalChunks)

}

func (t *Task) PrintInfo() {
	var estimatedChunks = int(0)
	chunks := float64(t.Table.estNumberOfRows) / float64(t.ChunkSize)
	if chunks > 0 {
		estimatedChunks = int(chunks + 1)
	}

	log.Infof("Table: %s Engine: %s Estimated Chunks: %v", t.Table.GetUnescapedFullName(), t.Table.Engine, estimatedChunks)
}

func NewTask(schema string,
	table string,
	chunkSize uint64,
	outputChunkSize uint64,
	tm *TaskManager) Task {

	return Task{
		Table:           NewTable(schema, table, tm.DB),
		ChunkSize:       chunkSize,
		OutputChunkSize: outputChunkSize,
		TaskManager:     tm}
}
