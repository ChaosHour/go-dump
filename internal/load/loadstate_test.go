package load

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewLoadState_Fresh(t *testing.T) {
	dir := t.TempDir()
	ls, err := NewLoadState(dir)
	if err != nil {
		t.Fatalf("NewLoadState: %v", err)
	}
	if ls.Len() != 0 {
		t.Errorf("expected 0 completed files, got %d", ls.Len())
	}
	if ls.StartTime.IsZero() {
		t.Error("StartTime should be set")
	}
}

func TestMark_And_HasFile(t *testing.T) {
	dir := t.TempDir()
	ls, _ := NewLoadState(dir)

	if ls.HasFile("a.sql") {
		t.Error("a.sql should not be present before Mark")
	}
	if err := ls.Mark("a.sql"); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	if !ls.HasFile("a.sql") {
		t.Error("a.sql should be present after Mark")
	}
	if ls.HasFile("b.sql") {
		t.Error("b.sql should not be present")
	}
}

func TestMark_Idempotent(t *testing.T) {
	dir := t.TempDir()
	ls, _ := NewLoadState(dir)

	for i := 0; i < 5; i++ {
		if err := ls.Mark("a.sql"); err != nil {
			t.Fatalf("Mark[%d]: %v", i, err)
		}
	}
	if ls.Len() != 1 {
		t.Errorf("expected 1 unique file after 5 idempotent marks, got %d", ls.Len())
	}
}

func TestLoadState_Persistence(t *testing.T) {
	dir := t.TempDir()
	ls, _ := NewLoadState(dir)
	_ = ls.Mark("file1.sql")
	_ = ls.Mark("file2.sql")

	// Load fresh state from disk — should see both files.
	ls2, err := NewLoadState(dir)
	if err != nil {
		t.Fatalf("NewLoadState (resume): %v", err)
	}
	if !ls2.HasFile("file1.sql") {
		t.Error("file1.sql missing after reload")
	}
	if !ls2.HasFile("file2.sql") {
		t.Error("file2.sql missing after reload")
	}
	if ls2.Len() != 2 {
		t.Errorf("expected 2 files after reload, got %d", ls2.Len())
	}
}

func TestLoadState_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	ls, _ := NewLoadState(dir)
	_ = ls.Mark("a.sql")

	// Verify no .tmp file is left on disk.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("leftover tmp file: %s", e.Name())
		}
	}
	// Verify state file exists.
	if _, err := os.Stat(filepath.Join(dir, "load-state.json")); os.IsNotExist(err) {
		t.Error("load-state.json should exist after Mark")
	}
}

func TestLoadState_Len(t *testing.T) {
	dir := t.TempDir()
	ls, _ := NewLoadState(dir)
	for _, name := range []string{"a.sql", "b.sql", "c.sql"} {
		_ = ls.Mark(name)
	}
	if ls.Len() != 3 {
		t.Errorf("Len = %d, want 3", ls.Len())
	}
}

func TestProgress_SetGetPersist(t *testing.T) {
	dir := t.TempDir()
	ls, _ := NewLoadState(dir)

	if got := ls.Progress("a.sql"); got != 0 {
		t.Errorf("Progress before Set = %d, want 0", got)
	}
	if err := ls.SetProgress("a.sql", 42); err != nil {
		t.Fatalf("SetProgress: %v", err)
	}
	if got := ls.Progress("a.sql"); got != 42 {
		t.Errorf("Progress = %d, want 42", got)
	}

	// A new state loaded from disk must see the partial progress.
	ls2, err := NewLoadState(dir)
	if err != nil {
		t.Fatalf("NewLoadState (resume): %v", err)
	}
	if got := ls2.Progress("a.sql"); got != 42 {
		t.Errorf("Progress after reload = %d, want 42", got)
	}
}

func TestMark_ClearsProgress(t *testing.T) {
	dir := t.TempDir()
	ls, _ := NewLoadState(dir)

	_ = ls.SetProgress("a.sql", 10)
	if err := ls.Mark("a.sql"); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	if got := ls.Progress("a.sql"); got != 0 {
		t.Errorf("Progress after Mark = %d, want 0", got)
	}

	ls2, err := NewLoadState(dir)
	if err != nil {
		t.Fatalf("NewLoadState (resume): %v", err)
	}
	if !ls2.HasFile("a.sql") {
		t.Error("a.sql should be complete after reload")
	}
	if got := ls2.Progress("a.sql"); got != 0 {
		t.Errorf("Progress after reload = %d, want 0", got)
	}
}

func TestNewLoadState_OldFormatWithoutProgress(t *testing.T) {
	// load-state.json written by a previous go-load version has no
	// file_progress key — it must still load, with zero progress everywhere.
	dir := t.TempDir()
	old := `{"start_time":"2026-01-01T00:00:00Z","completed_files":["a.sql"]}`
	if err := os.WriteFile(filepath.Join(dir, "load-state.json"), []byte(old), 0644); err != nil {
		t.Fatal(err)
	}
	ls, err := NewLoadState(dir)
	if err != nil {
		t.Fatalf("NewLoadState: %v", err)
	}
	if !ls.HasFile("a.sql") {
		t.Error("a.sql should be complete")
	}
	if got := ls.Progress("b.sql"); got != 0 {
		t.Errorf("Progress = %d, want 0", got)
	}
	if err := ls.SetProgress("b.sql", 3); err != nil {
		t.Fatalf("SetProgress on old-format state: %v", err)
	}
}

func TestNewLoadState_CorruptFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "load-state.json"), []byte("not-json"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := NewLoadState(dir)
	if err == nil {
		t.Error("expected error loading corrupt load-state.json, got nil")
	}
}
