package load

import (
	"os"
	"path/filepath"
	"testing"
)

// ── splitStatements ──────────────────────────────────────────────────────────

func TestSplitStatements_Basic(t *testing.T) {
	input := []byte("SELECT 1; SELECT 2; SELECT 3;")
	got := splitStatements(input)
	if len(got) != 3 {
		t.Fatalf("expected 3 statements, got %d: %v", len(got), got)
	}
	if got[0] != "SELECT 1" {
		t.Errorf("[0] = %q, want %q", got[0], "SELECT 1")
	}
}

func TestSplitStatements_CreateAndInsert(t *testing.T) {
	input := []byte(`
CREATE TABLE ` + "`t`" + ` (` + "`id`" + ` int);
INSERT INTO ` + "`t`" + ` (` + "`id`" + `) VALUES (1),(2),(3);
`)
	got := splitStatements(input)
	if len(got) != 2 {
		t.Fatalf("expected 2 statements, got %d: %v", len(got), got)
	}
}

func TestSplitStatements_SemicolonInString(t *testing.T) {
	// Semicolons inside single-quoted strings must not split the statement.
	input := []byte("INSERT INTO t (v) VALUES ('hello; world');")
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement, got %d: %v", len(got), got)
	}
}

func TestSplitStatements_DoubleQuoteEscape(t *testing.T) {
	// go-dump's ParseString escapes single quotes as ''. The splitter must
	// handle the '' toggle correctly (in→out→in) without false splits.
	input := []byte("INSERT INTO t (v) VALUES ('O''Brien; trouble');")
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement (''  escape), got %d: %v", len(got), got)
	}
}

func TestSplitStatements_BackslashEscape(t *testing.T) {
	// Backslash before a quote (\'): the ' should NOT toggle string state.
	input := []byte(`INSERT INTO t (v) VALUES ('it\'s fine; here');`)
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement (backslash escape), got %d: %v", len(got), got)
	}
}

func TestSplitStatements_BackslashPairInString(t *testing.T) {
	// \\ inside a string is a literal backslash — must not affect the string state.
	input := []byte(`INSERT INTO t (v) VALUES ('C:\\path; ok');`)
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement (double backslash), got %d: %v", len(got), got)
	}
}

func TestSplitStatements_Empty(t *testing.T) {
	got := splitStatements([]byte{})
	if len(got) != 0 {
		t.Errorf("expected 0 statements from empty input, got %d", len(got))
	}
}

func TestSplitStatements_WhitespaceOnly(t *testing.T) {
	got := splitStatements([]byte("  \n\t  "))
	if len(got) != 0 {
		t.Errorf("expected 0 statements from whitespace-only input, got %d", len(got))
	}
}

func TestSplitStatements_NoTrailingSemicolon(t *testing.T) {
	// Trailing content without a semicolon should still be emitted.
	got := splitStatements([]byte("SELECT 1; SELECT 2"))
	if len(got) != 2 {
		t.Fatalf("expected 2 statements, got %d: %v", len(got), got)
	}
	if got[1] != "SELECT 2" {
		t.Errorf("[1] = %q, want %q", got[1], "SELECT 2")
	}
}

func TestSplitStatements_Comments(t *testing.T) {
	// go-dump emits /* ... */ comments in the header; they should be
	// treated as part of the statement they precede.
	input := []byte("/*!40101 SET NAMES binary*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n")
	got := splitStatements(input)
	if len(got) != 2 {
		t.Fatalf("expected 2 statements, got %d: %v", len(got), got)
	}
}

func TestSplitStatements_MultipleValues(t *testing.T) {
	// Realistic go-dump INSERT with multiple value tuples and escaped chars.
	input := []byte("INSERT INTO `mydb`.`t` (`id`,`name`) VALUES (1,'Alice''s'),(2,'Bob; CEO');")
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement (multi-value INSERT), got %d: %v", len(got), got)
	}
}

func TestSplitStatements_LineCommentWithQuoteAndSemicolon(t *testing.T) {
	// An apostrophe or semicolon inside a "--" comment must not desync the parser.
	input := []byte("-- it's a chunk; really\nINSERT INTO t (v) VALUES (1);")
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement, got %d: %v", len(got), got)
	}
}

func TestSplitStatements_TrailingCommentOnlySkipped(t *testing.T) {
	// go-dump writes a "-- Chunk N" header even for chunks that turn out empty;
	// a trailing comment-only segment must not be sent to the server.
	input := []byte("INSERT INTO t (v) VALUES (1);\n-- Chunk 2 - from 5 to 9\n")
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement (trailing comment skipped), got %d: %v", len(got), got)
	}
}

func TestSplitStatements_BacktickIdentifier(t *testing.T) {
	// Semicolons and quotes inside backtick-quoted identifiers are literal.
	input := []byte("INSERT INTO `odd;name``s` (`a'b`) VALUES (1);")
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement (backtick identifier), got %d: %v", len(got), got)
	}
}

func TestSplitStatements_PlainBlockCommentSkipped(t *testing.T) {
	input := []byte("SELECT 1;\n/* trailing note; with ' quote */\n")
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement (plain block comment skipped), got %d: %v", len(got), got)
	}
}

func TestSplitStatements_VersionedCommentExecuted(t *testing.T) {
	// /*! ... */ is executable SQL and must be emitted even when standing alone.
	input := []byte("/*!40101 SET NAMES binary*/;")
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement (versioned comment), got %d: %v", len(got), got)
	}
}

func TestSplitStatements_DoubleQuotedString(t *testing.T) {
	input := []byte(`INSERT INTO t (v) VALUES ("a;b");`)
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement (double-quoted string), got %d: %v", len(got), got)
	}
}

func TestSplitStatements_HashComment(t *testing.T) {
	input := []byte("SELECT 1; # trailing; note\n")
	got := splitStatements(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 statement (hash comment skipped), got %d: %v", len(got), got)
	}
}

func TestSplitStatements_DoubleDashInExpression(t *testing.T) {
	// "1--2" without whitespace after the dashes is NOT a comment in MySQL.
	input := []byte("SELECT 1--2; SELECT 3;")
	got := splitStatements(input)
	if len(got) != 2 {
		t.Fatalf("expected 2 statements (a--b is not a comment), got %d: %v", len(got), got)
	}
	if got[0] != "SELECT 1--2" {
		t.Errorf("[0] = %q, want %q", got[0], "SELECT 1--2")
	}
}

// ── findFiles ────────────────────────────────────────────────────────────────

func TestFindFiles_BasicSQL(t *testing.T) {
	dir := t.TempDir()
	touch(t, dir, "mydb.users-definition.sql")
	touch(t, dir, "mydb.users-thread0.sql")
	touch(t, dir, "mydb.users-thread1.sql")
	touch(t, dir, "mydb.orders-definition.sql")
	touch(t, dir, "mydb.orders-thread0.sql")

	files, err := findFiles(dir, "*.sql")
	if err != nil {
		t.Fatalf("findFiles: %v", err)
	}

	var schemas, data int
	for _, f := range files {
		if f.IsSchema {
			schemas++
		} else {
			data++
		}
	}
	if schemas != 2 {
		t.Errorf("expected 2 schema files, got %d", schemas)
	}
	if data != 3 {
		t.Errorf("expected 3 data files, got %d", data)
	}
	// Schema files must sort before data files.
	for i, f := range files {
		if !f.IsSchema {
			for j := i + 1; j < len(files); j++ {
				if files[j].IsSchema {
					t.Error("schema files must appear before data files")
				}
			}
			break
		}
	}
}

func TestFindFiles_CompressedAutoDetect(t *testing.T) {
	dir := t.TempDir()
	touch(t, dir, "mydb.users-definition.sql")     // schema: plain
	touch(t, dir, "mydb.users-thread0.sql.gz")     // data: compressed
	touch(t, dir, "mydb.orders-definition.sql.gz") // schema: compressed

	// Default pattern "*.sql" should still find *.sql.gz data files.
	files, err := findFiles(dir, "*.sql")
	if err != nil {
		t.Fatalf("findFiles: %v", err)
	}

	var schemas, data int
	for _, f := range files {
		if f.IsSchema {
			schemas++
		} else {
			data++
		}
	}
	if schemas != 2 {
		t.Errorf("expected 2 schema files (plain + compressed), got %d", schemas)
	}
	if data != 1 {
		t.Errorf("expected 1 compressed data file, got %d", data)
	}
}

func TestFindFiles_ZstdAutoDetect(t *testing.T) {
	dir := t.TempDir()
	touch(t, dir, "mydb.users-definition.sql.zst") // schema: zstd
	touch(t, dir, "mydb.users-thread0.sql.zst")    // data: zstd
	touch(t, dir, "mydb.orders-thread0.sql.gz")    // data: gzip (mixed dump)
	touch(t, dir, "mydb.orders-definition.sql")    // schema: plain

	// Default pattern "*.sql" should still find *.sql.zst files.
	files, err := findFiles(dir, "*.sql")
	if err != nil {
		t.Fatalf("findFiles: %v", err)
	}

	var schemas, data int
	for _, f := range files {
		if f.IsSchema {
			schemas++
		} else {
			data++
		}
	}
	if schemas != 2 {
		t.Errorf("expected 2 schema files (zstd + plain), got %d", schemas)
	}
	if data != 2 {
		t.Errorf("expected 2 data files (zstd + gzip), got %d", data)
	}
}

func TestFindFiles_EmptyDir(t *testing.T) {
	files, err := findFiles(t.TempDir(), "*.sql")
	if err != nil {
		t.Fatalf("findFiles: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected 0 files in empty dir, got %d", len(files))
	}
}

func TestFindFiles_DefinitionNotDuplicated(t *testing.T) {
	dir := t.TempDir()
	touch(t, dir, "mydb.users-definition.sql")

	files, err := findFiles(dir, "*.sql")
	if err != nil {
		t.Fatalf("findFiles: %v", err)
	}
	if len(files) != 1 {
		t.Errorf("expected 1 file (definition not duplicated), got %d: %v", len(files), files)
	}
	if !files[0].IsSchema {
		t.Error("definition file should be marked as schema")
	}
}

func TestFindFiles_SchemaCreateFirst(t *testing.T) {
	dir := t.TempDir()
	touch(t, dir, "mydb.users-thread0.sql")
	touch(t, dir, "mydb.users-definition.sql")
	touch(t, dir, "mydb-schema-create.sql")

	files, err := findFiles(dir, "*.sql")
	if err != nil {
		t.Fatalf("findFiles: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("expected 3 files, got %d: %v", len(files), files)
	}
	// Load order: database creation → table definition → data.
	if filepath.Base(files[0].Path) != "mydb-schema-create.sql" {
		t.Errorf("files[0] = %s, want mydb-schema-create.sql first", files[0].Path)
	}
	if !files[0].IsSchema {
		t.Error("schema-create file must be marked as schema (serial load)")
	}
	if filepath.Base(files[1].Path) != "mydb.users-definition.sql" {
		t.Errorf("files[1] = %s, want definition second", files[1].Path)
	}
	if filepath.Base(files[2].Path) != "mydb.users-thread0.sql" {
		t.Errorf("files[2] = %s, want data file last", files[2].Path)
	}
	if files[2].IsSchema {
		t.Error("data file must not be marked as schema")
	}
}

func TestFindFiles_SkipsReplicationStatusFiles(t *testing.T) {
	dir := t.TempDir()
	touch(t, dir, "mydb.users-thread0.sql")
	touch(t, dir, "master-data.sql")
	touch(t, dir, "slave-data.sql")

	files, err := findFiles(dir, "*.sql")
	if err != nil {
		t.Fatalf("findFiles: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected only the data file, got %d: %v", len(files), files)
	}
	if filepath.Base(files[0].Path) != "mydb.users-thread0.sql" {
		t.Errorf("unexpected file: %s", files[0].Path)
	}
}

func TestFindFiles_GZPatternExplicit(t *testing.T) {
	dir := t.TempDir()
	touch(t, dir, "mydb.users-definition.sql.gz")
	touch(t, dir, "mydb.users-thread0.sql.gz")

	files, err := findFiles(dir, "*.sql.gz")
	if err != nil {
		t.Fatalf("findFiles: %v", err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2 files with explicit .gz pattern, got %d", len(files))
	}
}

func touch(t *testing.T, dir, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte{}, 0644); err != nil {
		t.Fatalf("touch %s: %v", name, err)
	}
}
