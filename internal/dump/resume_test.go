package dump

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCleanPartialFiles(t *testing.T) {
	dir := t.TempDir()

	// Create files that should be removed (belong to interrupted table).
	keepFiles := []string{
		"mydb.orders-definition.sql",
		"mydb.users-thread0.sql",
		"mydb.users-definition.sql",
	}
	removeFiles := []string{
		"mydb.events-thread0.sql",
		"mydb.events-thread1.sql",
		"mydb.events-definition.sql",
		"mydb.events-thread0.sql.gz",
		"mydb.events-thread1.sql.zst",
	}

	for _, name := range append(keepFiles, removeFiles...) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("sql"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	inProgress := map[string]bool{"mydb.events": true}
	cleanPartialFiles(dir, inProgress)

	for _, name := range keepFiles {
		if _, err := os.Stat(filepath.Join(dir, name)); os.IsNotExist(err) {
			t.Errorf("kept file was removed: %s", name)
		}
	}
	for _, name := range removeFiles {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("partial file was not removed: %s", name)
		}
	}
}

func TestCleanPartialFiles_EmptyInProgress(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mydb.orders-thread0.sql"), []byte("sql"), 0644); err != nil {
		t.Fatal(err)
	}
	// Empty inProgress map — nothing should be deleted.
	cleanPartialFiles(dir, map[string]bool{})
	if _, err := os.Stat(filepath.Join(dir, "mydb.orders-thread0.sql")); os.IsNotExist(err) {
		t.Error("file should not have been removed with empty inProgress set")
	}
}

func TestApplyResumeFilter_NoMetadata(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]bool{
		"db.tableA": true,
		"db.tableB": true,
	}
	result, prior := applyResumeFilter(dir, tables)
	if len(result) != 2 {
		t.Errorf("expected 2 tables when no metadata, got %d", len(result))
	}
	if prior != nil {
		t.Error("expected nil prior metadata when none exists")
	}
}

func TestApplyResumeFilter_SkipsDone(t *testing.T) {
	dir := t.TempDir()

	// Write a prior metadata.json with one done table and one pending.
	meta := NewDumpMetadata(dir, "1.0.0", "host", 3306, "8.0.43")
	meta.AddTable("db", "tableA", 1000)
	meta.AddTable("db", "tableB", 2000)
	meta.MarkTableDone("db", "tableA", 10)
	_ = meta.Write()

	tables := map[string]bool{
		"db.tableA": true,
		"db.tableB": true,
	}
	result, prior := applyResumeFilter(dir, tables)

	if prior == nil {
		t.Fatal("expected prior metadata to be returned")
	}
	if result["db.tableA"] {
		t.Error("db.tableA is done and should have been removed from the result")
	}
	if !result["db.tableB"] {
		t.Error("db.tableB is pending and should remain in the result")
	}
	if len(result) != 1 {
		t.Errorf("expected 1 table in result, got %d", len(result))
	}
}

func TestApplyResumeFilter_CleanPartialFiles(t *testing.T) {
	dir := t.TempDir()

	meta := NewDumpMetadata(dir, "1.0.0", "host", 3306, "8.0.43")
	meta.AddTable("db", "orders", 1000) // pending — partial files should be cleaned
	_ = meta.Write()

	// Create a partial chunk file for the pending table.
	partialFile := filepath.Join(dir, "db.orders-thread0.sql")
	if err := os.WriteFile(partialFile, []byte("partial"), 0644); err != nil {
		t.Fatal(err)
	}

	tables := map[string]bool{"db.orders": true}
	applyResumeFilter(dir, tables)

	if _, err := os.Stat(partialFile); !os.IsNotExist(err) {
		t.Error("partial chunk file should have been removed by applyResumeFilter")
	}
}

func TestApplyResumeFilter_AllDone(t *testing.T) {
	dir := t.TempDir()

	meta := NewDumpMetadata(dir, "1.0.0", "host", 3306, "8.0.43")
	meta.AddTable("db", "tableA", 1000)
	meta.MarkTableDone("db", "tableA", 10)
	meta.Complete()

	tables := map[string]bool{"db.tableA": true}
	result, _ := applyResumeFilter(dir, tables)

	// Complete dump: warning is logged, result is still filtered.
	if result["db.tableA"] {
		t.Error("db.tableA should be filtered out (done), even from a complete dump")
	}
}

func TestMergeDoneTables(t *testing.T) {
	dir := t.TempDir()

	prior := NewDumpMetadata(dir, "1.0.0", "host", 3306, "8.0.43")
	prior.AddTable("db", "done_table", 1000)
	prior.AddTable("db", "pending_table", 2000)
	prior.MarkTableDone("db", "done_table", 7)
	prior.SetChecksum("db", "done_table", 12345)

	fresh := NewDumpMetadata(dir, "1.0.0", "host", 3306, "8.0.43")
	fresh.AddTable("db", "pending_table", 2000) // re-dumped this run
	fresh.MergeDoneTables(prior)

	if len(fresh.Tables) != 2 {
		t.Fatalf("expected 2 tables after merge, got %d", len(fresh.Tables))
	}
	var carried *TableMetadata
	for _, tbl := range fresh.Tables {
		if tbl.Name == "done_table" {
			carried = tbl
		}
		if tbl.Name == "pending_table" && tbl.Status != TableStatusPending {
			t.Errorf("pending_table should stay pending, got %s", tbl.Status)
		}
	}
	if carried == nil {
		t.Fatal("done_table from prior run was not merged")
	}
	if carried.Status != TableStatusDone || carried.Chunks != 7 || carried.Checksum != 12345 {
		t.Errorf("carried table lost state: %+v", carried)
	}

	// Merging nil and re-merging must be safe and not duplicate.
	fresh.MergeDoneTables(nil)
	fresh.MergeDoneTables(prior)
	if len(fresh.Tables) != 2 {
		t.Errorf("re-merge duplicated tables: got %d", len(fresh.Tables))
	}
}

func TestMaybeMarkDone(t *testing.T) {
	dir := t.TempDir()
	meta := NewDumpMetadata(dir, "1.0.0", "host", 3306, "8.0.43")
	meta.AddTable("db", "t1", 100)

	tm := &TaskManager{metadata: meta}
	task := &Task{
		Table:       &Table{schema: "db", name: "t1"},
		TaskManager: tm,
	}

	// Chunking not finished — must not mark.
	task.TotalChunks = 2
	task.NoteChunkCompleted()
	task.NoteChunkCompleted()
	if meta.Tables[0].Status == TableStatusDone {
		t.Fatal("table marked done before chunking finished")
	}

	// Chunking finished and all chunks completed — must mark exactly once.
	task.chunkingDone.Store(true)
	task.maybeMarkDone()
	if meta.Tables[0].Status != TableStatusDone {
		t.Fatal("table should be marked done")
	}
	if meta.Tables[0].Chunks != 2 {
		t.Errorf("chunks = %d, want 2", meta.Tables[0].Chunks)
	}

	// Compressed dumps defer marking to the final sweep.
	meta2 := NewDumpMetadata(dir, "1.0.0", "host", 3306, "8.0.43")
	meta2.AddTable("db", "t2", 100)
	tmC := &TaskManager{metadata: meta2, Compress: true}
	taskC := &Task{Table: &Table{schema: "db", name: "t2"}, TaskManager: tmC}
	taskC.TotalChunks = 1
	taskC.chunkingDone.Store(true)
	taskC.NoteChunkCompleted()
	if meta2.Tables[0].Status == TableStatusDone {
		t.Error("compressed dump tables must not be marked done early")
	}
}
