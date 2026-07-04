package load

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── DELIMITER directive handling in parseStatements ─────────────────────────

func TestSplitStatements_DelimiterTriggerBody(t *testing.T) {
	input := []byte(`SET @x=1;
DELIMITER ;;
CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW
BEGIN
  SET @a = 1;
  SET @b = 2;
END;;
DELIMITER ;
SET @y=2;
`)
	got := splitStatements(input)
	if len(got) != 3 {
		t.Fatalf("expected 3 statements, got %d: %v", len(got), got)
	}
	if got[0] != "SET @x=1" {
		t.Errorf("[0] = %q", got[0])
	}
	if !strings.HasPrefix(got[1], "CREATE TRIGGER") || !strings.HasSuffix(got[1], "END") {
		t.Errorf("[1] trigger body wrong: %q", got[1])
	}
	if !strings.Contains(got[1], "SET @a = 1;") {
		t.Errorf("[1] lost internal semicolons: %q", got[1])
	}
	if got[2] != "SET @y=2" {
		t.Errorf("[2] = %q", got[2])
	}
}

func TestSplitStatements_DelimiterCaseInsensitive(t *testing.T) {
	input := []byte("delimiter $$\nSELECT 1; SELECT 2$$\ndelimiter ;\nSELECT 3;")
	got := splitStatements(input)
	if len(got) != 2 {
		t.Fatalf("expected 2 statements, got %d: %v", len(got), got)
	}
	if got[0] != "SELECT 1; SELECT 2" {
		t.Errorf("[0] = %q", got[0])
	}
	if got[1] != "SELECT 3" {
		t.Errorf("[1] = %q", got[1])
	}
}

func TestSplitStatements_PartialDelimiterMatchRestored(t *testing.T) {
	// Under delimiter ";;", a lone ";" is ordinary statement text and must be
	// restored into the statement when the second ";" never arrives.
	input := []byte("DELIMITER ;;\nSET @a='x'; SET @b='y';;\nDELIMITER ;\n")
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement, got %d: %v", len(got), got)
	}
	if got[0] != "SET @a='x'; SET @b='y'" {
		t.Errorf("[0] = %q", got[0])
	}
}

func TestSplitStatements_DeleteIsNotADirective(t *testing.T) {
	// "DELETE"/"DELIMITERX" share a prefix with "DELIMITER" — they must be
	// treated as SQL, not directives.
	input := []byte("DELETE FROM t WHERE id = 1;\nSELECT 1;")
	got := splitStatements(input)
	if len(got) != 2 {
		t.Fatalf("expected 2 statements, got %d: %v", len(got), got)
	}
	if got[0] != "DELETE FROM t WHERE id = 1" {
		t.Errorf("[0] = %q", got[0])
	}
}

func TestSplitStatements_DelimiterTokenInString(t *testing.T) {
	// A ";;" inside a quoted string must not terminate the statement while
	// the delimiter is ";;".
	input := []byte("DELIMITER ;;\nSET @v = 'a;;b';;\nDELIMITER ;\n")
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement, got %d: %v", len(got), got)
	}
	if got[0] != "SET @v = 'a;;b'" {
		t.Errorf("[0] = %q", got[0])
	}
}

func TestSplitStatements_DelimiterAtEOFWithoutNewline(t *testing.T) {
	input := []byte("DELIMITER ;;\nSELECT 1;;\nDELIMITER ;")
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement, got %d: %v", len(got), got)
	}
}

func TestSplitStatements_GuardCommentsAroundDelimiterBlock(t *testing.T) {
	// The exact shape go-dump writes for object files.
	input := []byte(`USE ` + "`db`" + `;
/*!50003 SET @saved_sql_mode = @@sql_mode */;
/*!50003 SET sql_mode = 'STRICT_TRANS_TABLES' */;
DELIMITER ;;
CREATE PROCEDURE p()
BEGIN
  SELECT 1;
END;;
DELIMITER ;
/*!50003 SET sql_mode = @saved_sql_mode */;
`)
	got := splitStatements(input)
	if len(got) != 5 {
		t.Fatalf("expected 5 statements, got %d: %v", len(got), got)
	}
	if !strings.HasPrefix(got[3], "CREATE PROCEDURE") || !strings.Contains(got[3], "SELECT 1;") {
		t.Errorf("[3] = %q", got[3])
	}
}

// ── findFiles ordering with object files ────────────────────────────────────

func TestFindFiles_ObjectFileOrdering(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"db.t1-triggers.sql",
		"db.t1.sql",
		"db-routines.sql",
		"db-events.sql",
		"db.t1-definition.sql",
		"db-schema-create.sql",
	}
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("SELECT 1;"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	files, err := findFiles(dir, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range files {
		got = append(got, filepath.Base(f.Path))
	}
	want := []string{
		"db-schema-create.sql",
		"db.t1-definition.sql",
		"db.t1.sql",
		"db-routines.sql",
		"db-events.sql",
		"db.t1-triggers.sql",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d files, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("position %d: got %s, want %s (full order: %v)", i, got[i], want[i], got)
		}
	}

	// Object files must be flagged post (and not schema, not data).
	for _, f := range files {
		base := filepath.Base(f.Path)
		isObj := strings.Contains(base, "-triggers.") || strings.Contains(base, "-routines.") || strings.Contains(base, "-events.")
		if isObj != f.IsPost {
			t.Errorf("%s: IsPost = %v, want %v", base, f.IsPost, isObj)
		}
		if isObj && f.IsSchema {
			t.Errorf("%s: object file wrongly flagged as schema", base)
		}
	}
}

func TestFindFiles_CompressedObjectFiles(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"db.t1.sql.gz", "db.t1-triggers.sql.gz"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte{}, 0644); err != nil {
			t.Fatal(err)
		}
	}
	files, err := findFiles(dir, "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d: %+v", len(files), files)
	}
	if !strings.Contains(files[1].Path, "-triggers.sql.gz") || !files[1].IsPost {
		t.Errorf("compressed trigger file not ordered last / flagged post: %+v", files)
	}
}
