package dump

import (
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ChaosHour/go-dump/internal/log"
)

type Task struct {
	Table           *Table
	ChunkSize       uint64
	OutputChunkSize uint64
	TaskManager     *TaskManager
	Id              int64
	TotalChunks     uint64
	chunkMin        int64
	chunkMax        int64

	// Per-table completion tracking so a table can be marked done in
	// metadata.json as soon as its last chunk is written — not only at the end
	// of the whole dump. This is what gives --resume per-table granularity.
	ChunksCompleted int64       // incremented atomically by workers
	chunkingDone    atomic.Bool // set when CreateChunks has produced all chunks
	markedDone      atomic.Bool // ensures the table is marked done exactly once
}

func (t *Task) AddChunk(chunk DataChunk) {
	t.TaskManager.AddChunk(chunk)
	t.TotalChunks = t.TotalChunks + 1
	atomic.AddInt64(&t.TaskManager.TotalChunks, 1)
	atomic.AddInt64(&t.TaskManager.Queue, 1)
	t.chunkMin = t.chunkMax + 1
	log.Debugf("Queue +1: %d ", atomic.LoadInt64(&t.TaskManager.Queue))
}

// NoteChunkCompleted records one finished chunk and marks the table done in
// the dump metadata once chunk creation has finished and every chunk is written.
func (t *Task) NoteChunkCompleted() {
	atomic.AddInt64(&t.ChunksCompleted, 1)
	t.maybeMarkDone()
}

// maybeMarkDone marks the table done in metadata when all chunks are complete.
// Called from both the chunk creator (covers zero-chunk tables and the case
// where workers finished before chunking was flagged done) and the workers
// (covers the normal case where the last chunk finishes after chunking ended).
func (t *Task) maybeMarkDone() {
	// With --compress, chunk data sits inside each worker's gzip stream until
	// the file is closed at the end of the run; marking the table done earlier
	// would claim completeness for files that still lack their gzip trailer.
	// Compressed dumps are marked done by the final sweep in Run instead.
	if t.TaskManager.Compress {
		return
	}
	if !t.chunkingDone.Load() {
		return
	}
	// TotalChunks is written only by the CreateChunks goroutine; reading it
	// after observing chunkingDone=true is ordered by the atomic store/load.
	if atomic.LoadInt64(&t.ChunksCompleted) != int64(t.TotalChunks) {
		return
	}
	if t.markedDone.CompareAndSwap(false, true) {
		t.TaskManager.markTableDone(t)
	}
}

func (t *Task) GetSingleChunkTestQuery() string {
	return fmt.Sprintf("SELECT 1 FROM %s LIMIT 1 ", t.Table.GetFullName())
}

func (t *Task) GetChunkSqlQuery() string {
	keyForChunks := escapeIdentifier(t.Table.GetPrimaryOrUniqueKey())
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s >= %d LIMIT 1 OFFSET %d",
		keyForChunks, t.Table.GetFullName(), keyForChunks, t.chunkMax, t.ChunkSize)
}

func (t *Task) GetLastChunkSqlQuery() string {
	keyForChunks := escapeIdentifier(t.Table.GetPrimaryOrUniqueKey())
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s >= %d LIMIT 1",
		keyForChunks, t.Table.GetFullName(), keyForChunks, t.chunkMin)
}

// GetMinKeyQuery returns the query for the smallest key value, used to seed
// the chunk bounds.
func (t *Task) GetMinKeyQuery() string {
	return fmt.Sprintf("SELECT MIN(%s) FROM %s",
		escapeIdentifier(t.Table.GetPrimaryOrUniqueKey()), t.Table.GetFullName())
}

func (t *Task) CreateChunks(db *sql.DB) {
	t.TotalChunks = 0
	t.chunkMax = 0
	t.chunkMin = 0

	var (
		ctx      = t.TaskManager.ctx
		tx       = db
		chunkMax = int64(0)
		chunkMin = int64(0)
	)

	defer func() {
		t.chunkingDone.Store(true)
		t.maybeMarkDone()
		t.TaskManager.CreateChunksWaitGroup.Done()
	}()

	if len(t.Table.GetPrimaryOrUniqueKey()) == 0 {
		switch t.TaskManager.TablesWithoutPKOption {
		case "single-chunk":
			log.Debugf("Table %s has no primary/unique key — dumping as a single chunk.", t.Table.GetFullName())
			err := tx.QueryRowContext(ctx, t.GetSingleChunkTestQuery()).Scan(&chunkMax)
			switch {
			case err == nil:
				t.AddChunk(NewSingleDataChunk(t))
			case errors.Is(err, sql.ErrNoRows):
				// Empty table — nothing to dump.
			default:
				// A skipped table must never end up in a dump marked complete.
				log.Fatalf("Error testing rows for table %s: %v", t.Table.GetFullName(), err)
			}
			return
		case "skip":
			log.Warningf("Table %s has no primary/unique key — skipping (--tables-without-uniquekey=skip).", t.Table.GetFullName())
			return
		case "error":
			log.Fatalf("Table %s has no primary/unique key and --tables-without-uniquekey=error.", t.Table.GetFullName())
		}
	}

	// Seed the chunk bounds from the smallest key value. Starting at zero
	// (the old behaviour) silently excluded rows with negative keys.
	// Note: keys above math.MaxInt64 (unsigned BIGINT upper half) fail here
	// with a scan error — loud, not silent.
	var minKey sql.NullInt64
	if err := tx.QueryRowContext(ctx, t.GetMinKeyQuery()).Scan(&minKey); err != nil {
		log.Fatalf("Error getting minimum key value for %s: %v", t.Table.GetFullName(), err)
	}
	if !minKey.Valid {
		// Empty table — nothing to dump.
		return
	}
	t.chunkMin = minKey.Int64
	t.chunkMax = minKey.Int64

chunkLoop:
	for {
		// Propagate context so Ctrl+C aborts in-flight chunk queries.
		err := tx.QueryRowContext(ctx, t.GetChunkSqlQuery()).Scan(&chunkMax)
		switch {
		case err == nil:
			t.chunkMax = chunkMax
			t.AddChunk(NewDataChunk(t))
		case errors.Is(err, sql.ErrNoRows):
			// Fewer than ChunkSize rows remain: emit the unbounded tail chunk if
			// any rows are left, then stop.
			err := tx.QueryRowContext(ctx, t.GetLastChunkSqlQuery()).Scan(&chunkMin)
			switch {
			case err == nil:
				t.AddChunk(NewDataLastChunk(t))
			case errors.Is(err, sql.ErrNoRows):
				// No rows past chunkMin — table fully chunked.
			default:
				log.Fatalf("Error creating last chunk for %s: %v", t.Table.GetFullName(), err)
			}
			break chunkLoop
		default:
			// Connection failures and context cancellation land here. An
			// incomplete chunk set must abort the dump, not loop or continue.
			log.Fatalf("Error creating chunks for %s: %v", t.Table.GetFullName(), err)
		}
	}

	log.Debugf("Table processed %s - %d chunks created", t.Table.GetFullName(), t.TotalChunks)
}

func (t *Task) PrintInfo() {
	var estimatedChunks = int(0)
	chunks := float64(t.Table.estNumberOfRows) / float64(t.ChunkSize)
	if chunks > 0 {
		estimatedChunks = int(chunks + 1)
	}
	log.Infof("Table: %s Engine: %s Estimated Chunks: %v", t.Table.GetUnescapedFullName(), t.Table.Engine, estimatedChunks)
}

func NewTask(schema string, table string, chunkSize uint64, outputChunkSize uint64, tm *TaskManager) Task {
	return Task{
		Table:           NewTable(schema, table, tm.DB),
		ChunkSize:       chunkSize,
		OutputChunkSize: outputChunkSize,
		TaskManager:     tm}
}
