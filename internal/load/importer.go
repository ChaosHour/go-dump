package load

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ChaosHour/go-dump/internal/log"
	mysql "github.com/go-sql-driver/mysql"
)

// FileLoad represents a single SQL file to load.
type FileLoad struct {
	Path     string
	IsSchema bool
}

// Importer loads SQL files produced by go-dump into a MySQL server.
type Importer struct {
	db         *sql.DB
	workers    int
	skipSchema bool
}

// New creates an Importer. Call Close() when done.
func New(db *sql.DB, workers int, skipSchema bool) *Importer {
	return &Importer{db: db, workers: workers, skipSchema: skipSchema}
}

// Close releases the underlying database connection pool.
func (imp *Importer) Close() error {
	if imp.db != nil {
		return imp.db.Close()
	}
	return nil
}

// ImportDirectory loads all SQL files in dir matching pattern.
// Schema (definition) files are loaded serially first; data files run in parallel.
// Pass a non-nil LoadState to enable resume (skip already-loaded files).
func (imp *Importer) ImportDirectory(ctx context.Context, dir, pattern string, ls *LoadState) error {
	files, err := findFiles(dir, pattern)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no SQL files found in %s matching %q (or *.sql.gz variant)", dir, pattern)
	}

	// Schema files: serial, must complete before data workers start.
	if !imp.skipSchema {
		for _, f := range files {
			if !f.IsSchema {
				continue
			}
			name := filepath.Base(f.Path)
			if ls != nil && ls.HasFile(name) {
				log.Infof("Resume: skipping schema %s", name)
				continue
			}
			if err := imp.loadFile(ctx, f.Path); err != nil {
				return fmt.Errorf("schema %s: %w", name, err)
			}
			if ls != nil {
				_ = ls.Mark(name)
			}
			log.Infof("Loaded schema: %s", name)
		}
	}

	// Count data files for progress reporting.
	var total int64
	for _, f := range files {
		if !f.IsSchema {
			atomic.AddInt64(&total, 1)
		}
	}

	var done int64

	// Progress ticker — same 5-second cadence as go-dump.
	progressCtx, stopProgress := context.WithCancel(ctx)
	defer stopProgress()
	if total > 0 {
		go func() {
			t := time.NewTicker(5 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-progressCtx.Done():
					return
				case <-t.C:
					log.Infof("Progress: %d/%d files loaded", atomic.LoadInt64(&done), atomic.LoadInt64(&total))
				}
			}
		}()
	}

	// Data files: parallel workers via semaphore.
	sem := make(chan struct{}, imp.workers)
	errsCh := make(chan error, int(total)+1)
	var wg sync.WaitGroup

	for _, f := range files {
		if f.IsSchema {
			continue
		}
		name := filepath.Base(f.Path)
		if ls != nil && ls.HasFile(name) {
			log.Infof("Resume: skipping %s", name)
			atomic.AddInt64(&done, 1)
			continue
		}

		wg.Add(1)
		go func(fl FileLoad, fname string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if err := imp.loadFile(ctx, fl.Path); err != nil {
				errsCh <- fmt.Errorf("%s: %w", fname, err)
				return
			}
			atomic.AddInt64(&done, 1)
			if ls != nil {
				_ = ls.Mark(fname)
			}
			log.Debugf("Loaded: %s", fname)
		}(f, name)
	}

	wg.Wait()
	stopProgress()
	close(errsCh)

	var errs []string
	for e := range errsCh {
		errs = append(errs, e.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("%d file(s) failed:\n  %s", len(errs), strings.Join(errs, "\n  "))
	}

	log.Infof("Progress: %d/%d files loaded", atomic.LoadInt64(&done), total)
	return nil
}

// ImportFile loads a single SQL file.
func (imp *Importer) ImportFile(ctx context.Context, path string) error {
	return imp.loadFile(ctx, path)
}

const maxRetries = 3

// loadFile opens path (plain or .gz), acquires a dedicated connection,
// sets session variables, and streams SQL statements one at a time.
// Retries up to maxRetries times on transient MySQL connection errors.
func (imp *Importer) loadFile(ctx context.Context, path string) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt*attempt) * time.Second
			log.Warningf("Retry %d/%d for %s (waiting %s): %v",
				attempt, maxRetries-1, filepath.Base(path), delay, lastErr)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		lastErr = imp.doLoadFile(ctx, path)
		if lastErr == nil || !isRetryable(lastErr) {
			return lastErr
		}
	}
	return fmt.Errorf("after %d attempts: %w", maxRetries, lastErr)
}

// doLoadFile performs a single attempt at loading path.
func (imp *Importer) doLoadFile(ctx context.Context, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var underlying io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("gzip: %w", err)
		}
		defer gz.Close()
		underlying = gz
	}

	conn, err := imp.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Close()

	// Per-connection settings for faster, safe restore.
	for _, set := range []string{
		"SET FOREIGN_KEY_CHECKS=0",
		"SET UNIQUE_CHECKS=0",
		"SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO'",
	} {
		if _, err := conn.ExecContext(ctx, set); err != nil {
			return fmt.Errorf("%s: %w", set, err)
		}
	}

	return execStream(ctx, conn, bufio.NewReaderSize(underlying, 4*1024*1024))
}

// execStream reads from r and executes each SQL statement as it is found.
// Memory usage is O(max_statement_size), not O(file_size).
func execStream(ctx context.Context, conn *sql.Conn, r io.Reader) error {
	return parseStatements(r, func(stmt string) error {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", truncate(stmt, 120), err)
		}
		return nil
	})
}

// parseStatements reads SQL from r and calls cb for each complete statement.
// The scanner understands enough MySQL syntax that semicolons inside other
// constructs never split a statement:
//   - single- and double-quoted strings, with both \' and '' escaping
//   - backtick-quoted identifiers ('' doubling, no backslash escapes)
//   - line comments ("-- " per MySQL — the dashes must be followed by
//     whitespace — and "#") and /* block comments */
//
// Comment bytes are kept in the statement text (MySQL accepts leading
// comments, and /*!...*/ versioned comments are executable), but a segment
// containing only plain comments or whitespace is skipped rather than sent to
// the server, where it would raise ER_EMPTY_QUERY.
func parseStatements(r io.Reader, cb func(string) error) error {
	var stmt strings.Builder
	var inQuote byte // 0 when outside; otherwise the opening ', " or `
	var prev byte
	escaped := false
	inLineComment := false
	inBlockComment := false
	blockJustOpened := false
	hasSQL := false // statement contains something beyond comments/whitespace
	dashRun := 0    // consecutive '-' bytes seen outside quotes/comments

	emit := func() error {
		s := strings.TrimSpace(stmt.String())
		stmt.Reset()
		wasSQL := hasSQL
		hasSQL = false
		if s == "" || !wasSQL {
			return nil
		}
		return cb(s)
	}

	buf := make([]byte, 64*1024)
	for {
		n, readErr := r.Read(buf)
		for i := 0; i < n; i++ {
			b := buf[i]

			switch {
			case escaped:
				stmt.WriteByte(b)
				escaped = false

			case inLineComment:
				stmt.WriteByte(b)
				if b == '\n' {
					inLineComment = false
				}

			case inBlockComment:
				stmt.WriteByte(b)
				if blockJustOpened {
					blockJustOpened = false
					// "/*!" versioned comments are executable SQL.
					if b == '!' {
						hasSQL = true
					}
				} else if prev == '*' && b == '/' {
					inBlockComment = false
				}

			case inQuote != 0:
				stmt.WriteByte(b)
				if b == '\\' && inQuote != '`' {
					escaped = true
				} else if b == inQuote {
					inQuote = 0
				}

			case b == ';':
				// Terminating semicolons are not included in the statement body.
				if err := emit(); err != nil {
					return err
				}

			case b == '\'' || b == '"' || b == '`':
				inQuote = b
				hasSQL = true
				stmt.WriteByte(b)

			case b == '#':
				inLineComment = true
				stmt.WriteByte(b)

			case b == '*' && prev == '/':
				inBlockComment = true
				blockJustOpened = true
				stmt.WriteByte(b)

			case b == '-':
				// Possibly the start of a "-- " comment; decided on the next byte.
				dashRun++
				stmt.WriteByte(b)

			case dashRun >= 2 && (b == ' ' || b == '\t' || b == '\r' || b == '\n'):
				inLineComment = b != '\n'
				stmt.WriteByte(b)

			default:
				if b != ' ' && b != '\t' && b != '\r' && b != '\n' && b != '/' {
					hasSQL = true
				}
				stmt.WriteByte(b)
			}

			if b != '-' {
				dashRun = 0
			}
			prev = b
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read: %w", readErr)
		}
	}
	// Handle trailing content without a terminating semicolon.
	return emit()
}

// splitStatements is a convenience wrapper around parseStatements that
// collects all statements into a slice. Used in tests.
func splitStatements(data []byte) []string {
	var stmts []string
	_ = parseStatements(strings.NewReader(string(data)), func(s string) error {
		stmts = append(stmts, s)
		return nil
	})
	return stmts
}

// isRetryable reports whether err is a transient MySQL connection error
// worth retrying (server gone away, lost connection).
func isRetryable(err error) bool {
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number == 2006 || me.Number == 2013
	}
	return false
}

// findFiles returns all SQL files in dir: schema files first (database
// schema-create files, then table definition files), then data files matching
// pattern. When pattern ends with ".sql", compressed variants (*.sql.gz) are
// automatically included so compressed dumps work without requiring the user
// to change the pattern flag.
func findFiles(dir, pattern string) ([]FileLoad, error) {
	var files []FileLoad
	seen := make(map[string]bool)

	// Always collect schema-create and definition files as schema (plain and
	// compressed). Ordering between them is handled by the sort below.
	for _, defPat := range []string{
		"*-schema-create.sql", "*-schema-create.sql.gz",
		"*-definition.sql", "*-definition.sql.gz",
	} {
		matches, err := filepath.Glob(filepath.Join(dir, defPat))
		if err != nil {
			return nil, err
		}
		for _, p := range matches {
			if !seen[p] {
				seen[p] = true
				files = append(files, FileLoad{Path: p, IsSchema: true})
			}
		}
	}

	// Data files matching the user's pattern.
	dataPatterns := []string{pattern}
	// When pattern ends with ".sql" (the default), also check "*.sql.gz" so
	// compressed dumps work without a flag change.
	if strings.HasSuffix(pattern, ".sql") {
		dataPatterns = append(dataPatterns, pattern+".gz")
	}

	for _, pat := range dataPatterns {
		matches, err := filepath.Glob(filepath.Join(dir, pat))
		if err != nil {
			return nil, err
		}
		for _, p := range matches {
			base := filepath.Base(p)
			if seen[p] || strings.Contains(base, "-definition") || strings.Contains(base, "-schema-create") {
				continue
			}
			// master-data.sql / slave-data.sql hold replication coordinates as
			// plain text for the operator — they are not loadable SQL.
			if strings.HasPrefix(base, "master-data.") || strings.HasPrefix(base, "slave-data.") {
				continue
			}
			seen[p] = true
			files = append(files, FileLoad{Path: p, IsSchema: false})
		}
	}

	// Load order: database creation, then table definitions, then data.
	rank := func(f FileLoad) int {
		switch {
		case strings.Contains(filepath.Base(f.Path), "-schema-create"):
			return 0
		case f.IsSchema:
			return 1
		default:
			return 2
		}
	}
	sort.Slice(files, func(i, j int) bool {
		ri, rj := rank(files[i]), rank(files[j])
		if ri != rj {
			return ri < rj
		}
		return files[i].Path < files[j].Path
	})
	return files, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
