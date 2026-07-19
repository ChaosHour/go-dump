package dump

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDumpMetadataLifecycle(t *testing.T) {
	dir := t.TempDir()

	meta := NewDumpMetadata(dir, "1.0.0", "db01.example.com", 3306, "8.0.43")
	if meta.Status != "in_progress" {
		t.Fatalf("expected status in_progress, got %s", meta.Status)
	}
	if meta.MySQLHost != "db01.example.com" {
		t.Errorf("MySQLHost = %q, want %q", meta.MySQLHost, "db01.example.com")
	}
	if meta.MySQLVersion != "8.0.43" {
		t.Errorf("MySQLVersion = %q, want %q", meta.MySQLVersion, "8.0.43")
	}

	// metadata.json should have been written by NewDumpMetadata.
	assertFileExists(t, filepath.Join(dir, "metadata.json"))

	meta.SetBinlog("binlog.000042", 12345, "abc-123:1-42")
	meta.AddTable("mydb", "orders", 50000)
	meta.AddTable("mydb", "users", 1000)

	if err := meta.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Load back and verify.
	loaded, err := LoadDumpMetadata(dir)
	if err != nil {
		t.Fatalf("LoadDumpMetadata: %v", err)
	}
	if loaded.BinlogFile != "binlog.000042" {
		t.Errorf("BinlogFile = %q, want %q", loaded.BinlogFile, "binlog.000042")
	}
	if loaded.BinlogPosition != 12345 {
		t.Errorf("BinlogPosition = %d, want 12345", loaded.BinlogPosition)
	}
	if loaded.GTIDSet != "abc-123:1-42" {
		t.Errorf("GTIDSet = %q, want %q", loaded.GTIDSet, "abc-123:1-42")
	}
	if len(loaded.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(loaded.Tables))
	}
	if loaded.Tables[0].Schema != "mydb" || loaded.Tables[0].Name != "orders" {
		t.Errorf("Tables[0] = %+v", loaded.Tables[0])
	}
	if loaded.Tables[0].Status != TableStatusPending {
		t.Errorf("expected status pending, got %s", loaded.Tables[0].Status)
	}

	// Mark one table done.
	meta.MarkTableDone("mydb", "orders", 50)
	loaded2, _ := LoadDumpMetadata(dir)
	for _, tbl := range loaded2.Tables {
		if tbl.Name == "orders" && tbl.Status != TableStatusDone {
			t.Errorf("expected orders status done, got %s", tbl.Status)
		}
		if tbl.Name == "users" && tbl.Status != TableStatusPending {
			t.Errorf("expected users status pending, got %s", tbl.Status)
		}
	}

	// Complete the dump.
	meta.Complete()
	loaded3, _ := LoadDumpMetadata(dir)
	if loaded3.Status != "complete" {
		t.Errorf("expected status complete, got %s", loaded3.Status)
	}
	if loaded3.EndTime == nil {
		t.Error("EndTime should be set after Complete()")
	}
}

func TestDumpMetadataFail(t *testing.T) {
	dir := t.TempDir()
	meta := NewDumpMetadata(dir, "1.0.0", "host", 3306, "8.0.43")
	meta.Fail()

	loaded, err := LoadDumpMetadata(dir)
	if err != nil {
		t.Fatalf("LoadDumpMetadata: %v", err)
	}
	if loaded.Status != "failed" {
		t.Errorf("expected status failed, got %s", loaded.Status)
	}
	if loaded.EndTime == nil {
		t.Error("EndTime should be set after Fail()")
	}
}

func TestDumpMetadataSetChecksum(t *testing.T) {
	dir := t.TempDir()
	meta := NewDumpMetadata(dir, "1.0.0", "host", 3306, "8.0.43")
	meta.AddTable("db", "tbl", 1000)
	meta.SetChecksum("db", "tbl", 987654321)
	_ = meta.Write()

	loaded, _ := LoadDumpMetadata(dir)
	if loaded.Tables[0].Checksum != 987654321 {
		t.Errorf("Checksum = %d, want 987654321", loaded.Tables[0].Checksum)
	}
}

func TestResumeState(t *testing.T) {
	meta := &DumpMetadata{
		Tables: []*TableMetadata{
			{Schema: "db", Name: "a", Status: TableStatusDone},
			{Schema: "db", Name: "b", Status: TableStatusPending},
			{Schema: "db", Name: "c", Status: TableStatusFailed},
		},
	}
	done, inProgress := meta.ResumeState()

	if !done["db.a"] {
		t.Error("db.a should be done")
	}
	if done["db.b"] || done["db.c"] {
		t.Error("db.b and db.c should not be done")
	}
	if !inProgress["db.b"] || !inProgress["db.c"] {
		t.Error("db.b and db.c should be in-progress")
	}
	if inProgress["db.a"] {
		t.Error("db.a should not be in-progress")
	}
}

func TestLoadDumpMetadata_NotExist(t *testing.T) {
	_, err := LoadDumpMetadata(t.TempDir())
	if !os.IsNotExist(err) {
		t.Errorf("expected not-exist error, got %v", err)
	}
}

func TestLoadDumpMetadata_Corrupt(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), []byte("not json {{{"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadDumpMetadata(dir)
	if err == nil {
		t.Error("expected parse error, got nil")
	}
}

func TestDumpMetadataAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	meta := NewDumpMetadata(dir, "1.0.0", "host", 3306, "8.0.43")

	// Write several times rapidly — no tmp file should be left behind.
	for i := 0; i < 20; i++ {
		meta.AddTable("db", fmt.Sprintf("tbl%d", i), uint64(i*100))
		_ = meta.Write()
	}

	// Verify no .tmp file remains.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("leftover tmp file: %s", e.Name())
		}
	}

	// Verify the written JSON is valid.
	data, _ := os.ReadFile(filepath.Join(dir, "metadata.json"))
	var m DumpMetadata
	if err := json.Unmarshal(data, &m); err != nil {
		t.Errorf("metadata.json is not valid JSON: %v", err)
	}
	if len(m.Tables) != 20 {
		t.Errorf("expected 20 tables, got %d", len(m.Tables))
	}
}

func TestDumpMetadataStartTime(t *testing.T) {
	before := time.Now().UTC().Add(-time.Second)
	dir := t.TempDir()
	meta := NewDumpMetadata(dir, "1.0.0", "host", 3306, "8.0.43")
	after := time.Now().UTC().Add(time.Second)

	if meta.StartTime.Before(before) || meta.StartTime.After(after) {
		t.Errorf("StartTime %v is outside expected range [%v, %v]", meta.StartTime, before, after)
	}
}

func assertFileExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("expected file to exist: %s", path)
	}
}
