package load

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	mysql "github.com/go-sql-driver/mysql"
)

// ── fake driver ──────────────────────────────────────────────────────────────
// A minimal database/sql driver that records every executed statement and can
// be told to fail statements matching a prefix. Lets us test doLoadFile's
// session setup (SET SQL_LOG_BIN=0 and friends) without a MySQL server.

type stmtRecorder struct {
	mu     sync.Mutex
	stmts  []string
	failOn string // statement prefix to fail
	err    error  // error returned for failOn matches

	// queryFn, when set, answers SELECT/SHOW queries: it returns column names
	// and row values for a given query. Lets tests script server state
	// (gtid_mode, gtid_executed, replica status) without a MySQL server.
	queryFn func(q string) (cols []string, rows [][]driver.Value, err error)
}

func (r *stmtRecorder) record(q string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stmts = append(r.stmts, q)
	if r.failOn != "" && strings.HasPrefix(q, r.failOn) {
		return r.err
	}
	return nil
}

func (r *stmtRecorder) executed() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.stmts))
	copy(out, r.stmts)
	return out
}

type fakeDriver struct{ rec *stmtRecorder }

func (d *fakeDriver) Open(_ string) (driver.Conn, error) { return &fakeConn{rec: d.rec}, nil }

type fakeConn struct{ rec *stmtRecorder }

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{conn: c, query: query}, nil
}
func (c *fakeConn) Close() error              { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) { return nil, driver.ErrSkip }

// ExecContext is what conn.ExecContext uses directly.
func (c *fakeConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if err := c.rec.record(query); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}

// QueryContext answers queries via the recorder's queryFn.
func (c *fakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := c.rec.record(query); err != nil {
		return nil, err
	}
	if c.rec.queryFn == nil {
		return nil, fmt.Errorf("no queryFn configured for query %q", query)
	}
	cols, rows, err := c.rec.queryFn(query)
	if err != nil {
		return nil, err
	}
	return &fakeRows{cols: cols, rows: rows}, nil
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

type fakeStmt struct {
	conn  *fakeConn
	query string
}

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }
func (s *fakeStmt) Exec(_ []driver.Value) (driver.Result, error) {
	if err := s.conn.rec.record(s.query); err != nil {
		return nil, err
	}
	return driver.ResultNoRows, nil
}
func (s *fakeStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("not implemented")
}

var fakeDriverSeq int64

// newFakeDB registers a fresh fake driver instance (sql.Register panics on
// duplicate names, so each call gets a unique name) and returns the DB plus
// its statement recorder.
func newFakeDB(t *testing.T) (*sql.DB, *stmtRecorder) {
	t.Helper()
	rec := &stmtRecorder{}
	name := fmt.Sprintf("fake-load-%d", atomic.AddInt64(&fakeDriverSeq, 1))
	sql.Register(name, &fakeDriver{rec: rec})
	db, err := sql.Open(name, "dsn-ignored")
	if err != nil {
		t.Fatalf("open fake db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, rec
}

// writeTempSQL creates a small dump-style data file and returns its path.
func writeTempSQL(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.sql")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write temp sql: %v", err)
	}
	return path
}

// ── tests ────────────────────────────────────────────────────────────────────

func TestDoLoadFile_DefaultDoesNotTouchBinlog(t *testing.T) {
	db, rec := newFakeDB(t)
	imp := New(db, 1, false)

	path := writeTempSQL(t, "INSERT INTO t (id) VALUES (1);\n")
	if err := imp.doLoadFile(context.Background(), path); err != nil {
		t.Fatalf("doLoadFile: %v", err)
	}

	for _, q := range rec.executed() {
		if strings.Contains(q, "SQL_LOG_BIN") {
			t.Errorf("binlog statement executed without --skip-binlog: %q", q)
		}
	}
}

func TestDoLoadFile_SkipBinlogSetBeforeData(t *testing.T) {
	db, rec := newFakeDB(t)
	imp := New(db, 1, false)
	imp.SetSkipBinlog(true)

	path := writeTempSQL(t, "INSERT INTO t (id) VALUES (1);\nINSERT INTO t (id) VALUES (2);\n")
	if err := imp.doLoadFile(context.Background(), path); err != nil {
		t.Fatalf("doLoadFile: %v", err)
	}

	stmts := rec.executed()
	binlogIdx, firstDataIdx := -1, -1
	for i, q := range stmts {
		if q == "SET SQL_LOG_BIN=0" && binlogIdx == -1 {
			binlogIdx = i
		}
		if strings.HasPrefix(q, "INSERT") && firstDataIdx == -1 {
			firstDataIdx = i
		}
	}
	if binlogIdx == -1 {
		t.Fatalf("SET SQL_LOG_BIN=0 never executed; statements: %v", stmts)
	}
	if firstDataIdx == -1 {
		t.Fatalf("no data statement executed; statements: %v", stmts)
	}
	if binlogIdx > firstDataIdx {
		t.Errorf("SET SQL_LOG_BIN=0 executed at %d, AFTER first data statement at %d — data would be binlogged", binlogIdx, firstDataIdx)
	}
}

func TestDoLoadFile_SkipBinlogPrivilegeErrorGetsHint(t *testing.T) {
	db, rec := newFakeDB(t)
	rec.failOn = "SET SQL_LOG_BIN=0"
	rec.err = &mysql.MySQLError{Number: 1227, Message: "Access denied; you need (at least one of) the SUPER, SYSTEM_VARIABLES_ADMIN privilege(s)"}

	imp := New(db, 1, false)
	imp.SetSkipBinlog(true)

	path := writeTempSQL(t, "INSERT INTO t (id) VALUES (1);\n")
	err := imp.doLoadFile(context.Background(), path)
	if err == nil {
		t.Fatal("expected error when SET SQL_LOG_BIN=0 is denied, got nil")
	}
	if !strings.Contains(err.Error(), "SUPER or SYSTEM_VARIABLES_ADMIN") {
		t.Errorf("error 1227 should carry the privilege hint, got: %v", err)
	}
	if !strings.Contains(err.Error(), "--skip-binlog") {
		t.Errorf("error 1227 should mention the flag to drop, got: %v", err)
	}
	// The load must abort: no data statement may run after the failed SET.
	for _, q := range rec.executed() {
		if strings.HasPrefix(q, "INSERT") {
			t.Errorf("data statement executed after SET SQL_LOG_BIN=0 failed — partially-logged load: %q", q)
		}
	}
}

func TestDoLoadFile_SkipBinlogGenericErrorAborts(t *testing.T) {
	db, rec := newFakeDB(t)
	rec.failOn = "SET SQL_LOG_BIN=0"
	rec.err = &mysql.MySQLError{Number: 1193, Message: "Unknown system variable"}

	imp := New(db, 1, false)
	imp.SetSkipBinlog(true)

	path := writeTempSQL(t, "INSERT INTO t (id) VALUES (1);\n")
	err := imp.doLoadFile(context.Background(), path)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "SET SQL_LOG_BIN=0") {
		t.Errorf("error should identify the failing statement, got: %v", err)
	}
	if strings.Contains(err.Error(), "SUPER or SYSTEM_VARIABLES_ADMIN") {
		t.Errorf("non-1227 error must not get the privilege hint, got: %v", err)
	}
}

func TestDoLoadFile_SessionSettingsAlwaysApplied(t *testing.T) {
	db, rec := newFakeDB(t)
	imp := New(db, 1, false)
	imp.SetSkipBinlog(true)

	path := writeTempSQL(t, "INSERT INTO t (id) VALUES (1);\n")
	if err := imp.doLoadFile(context.Background(), path); err != nil {
		t.Fatalf("doLoadFile: %v", err)
	}

	want := []string{"SET FOREIGN_KEY_CHECKS=0", "SET UNIQUE_CHECKS=0"}
	got := rec.executed()
	for _, w := range want {
		if !slices.Contains(got, w) {
			t.Errorf("session setting %q not executed; statements: %v", w, got)
		}
	}
}
