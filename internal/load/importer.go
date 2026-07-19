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

	"github.com/klauspost/compress/zstd"

	"github.com/ChaosHour/go-dump/internal/log"
	mysql "github.com/go-sql-driver/mysql"
)

// FileLoad represents a single SQL file to load.
type FileLoad struct {
	Path     string
	IsSchema bool
	IsPost   bool // trigger/routine/event file — loaded serially after all data
}

// Importer loads SQL files produced by go-dump into a MySQL server.
type Importer struct {
	db         *sql.DB
	workers    int
	skipSchema bool
	skipBinlog bool
}

// New creates an Importer. Call Close() when done.
func New(db *sql.DB, workers int, skipSchema bool) *Importer {
	return &Importer{db: db, workers: workers, skipSchema: skipSchema}
}

// SetSkipBinlog makes every load connection run SET SQL_LOG_BIN=0, so the
// restore is written to the target only — not to its binlog, its replicas, or
// its GTID history. Requires SUPER or SYSTEM_VARIABLES_ADMIN on the target.
func (imp *Importer) SetSkipBinlog(v bool) {
	imp.skipBinlog = v
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
		return fmt.Errorf("no SQL files found in %s matching %q (or *.sql.gz / *.sql.zst variants)", dir, pattern)
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
		if !f.IsSchema && !f.IsPost {
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
		if f.IsSchema || f.IsPost {
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

			if err := imp.loadDataFile(ctx, fl.Path, ls); err != nil {
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

	// Post-data files (routines, events, triggers): serial, and only after
	// every data file has loaded — a trigger created before its table's data
	// would fire for every restored row. Skipped with --data-only, like the
	// schema files.
	if !imp.skipSchema {
		for _, f := range files {
			if !f.IsPost {
				continue
			}
			name := filepath.Base(f.Path)
			if ls != nil && ls.HasFile(name) {
				log.Infof("Resume: skipping %s", name)
				continue
			}
			if err := imp.loadFile(ctx, f.Path); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			if ls != nil {
				_ = ls.Mark(name)
			}
			log.Infof("Loaded: %s", name)
		}
	}

	log.Infof("Progress: %d/%d files loaded", atomic.LoadInt64(&done), total)
	return nil
}

// ImportFile loads a single SQL file.
func (imp *Importer) ImportFile(ctx context.Context, path string) error {
	return imp.loadFile(ctx, path)
}

const maxRetries = 3

// txBatchBytes is the transaction size for data files: statements are applied
// inside explicit transactions committed once this many statement bytes have
// accumulated. Commits are the durability points for mid-file resume — only
// committed statements are recorded in load-state.json, so a crash or retry
// never re-applies rows the target already has.
const txBatchBytes = 64 * 1024 * 1024

// loadFile opens path (plain, .gz, or .zst), acquires a dedicated connection,
// sets session variables, and streams SQL statements one at a time.
// Retries up to maxRetries times on transient MySQL connection errors.
// Used for schema and post-data (routine/event/trigger) files, whose DDL
// statements commit implicitly and cannot be batched.
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

// loadDataFile loads a data file inside batched transactions with
// statement-level progress, so an interrupted or retried load never
// re-applies committed rows. Progress is kept in memory for transparent
// retries and, when ls is non-nil, persisted to load-state.json so a later
// --resume run continues mid-file (like mysqlsh's load progress file).
func (imp *Importer) loadDataFile(ctx context.Context, path string, ls *LoadState) error {
	name := filepath.Base(path)
	applied := int64(0)
	if ls != nil {
		applied = ls.Progress(name)
		if applied > 0 {
			log.Infof("Resume: %s — continuing after %d committed statement(s)", name, applied)
		}
	}
	progress := func(total int64) error {
		applied = total
		if ls != nil {
			if err := ls.SetProgress(name, total); err != nil {
				return fmt.Errorf("record progress for %s: %w", name, err)
			}
		}
		return nil
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt*attempt) * time.Second
			log.Warningf("Retry %d/%d for %s (waiting %s, resuming after %d statements): %v",
				attempt, maxRetries-1, name, delay, applied, lastErr)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		lastErr = imp.withFileConn(ctx, path, func(conn *sql.Conn, r io.Reader) error {
			return execBatched(ctx, conn, r, applied, progress)
		})
		if lastErr == nil || !isRetryable(lastErr) {
			return lastErr
		}
	}
	return fmt.Errorf("after %d attempts: %w", maxRetries, lastErr)
}

// doLoadFile performs a single attempt at loading path.
func (imp *Importer) doLoadFile(ctx context.Context, path string) error {
	return imp.withFileConn(ctx, path, func(conn *sql.Conn, r io.Reader) error {
		return execStream(ctx, conn, r)
	})
}

// withFileConn opens path (plain, .gz, or .zst), acquires a dedicated
// connection with the load session variables set, and hands the decompressed
// statement stream to fn.
func (imp *Importer) withFileConn(ctx context.Context, path string, fn func(*sql.Conn, io.Reader) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var underlying io.Reader = f
	switch {
	case strings.HasSuffix(path, ".gz"):
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("gzip: %w", err)
		}
		defer gz.Close()
		underlying = gz
	case strings.HasSuffix(path, ".zst"):
		zr, err := zstd.NewReader(f)
		if err != nil {
			return fmt.Errorf("zstd: %w", err)
		}
		defer zr.Close()
		underlying = zr
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

	// SQL_LOG_BIN is session-scoped, so it must be set on every connection the
	// load uses — a connection that misses it would silently replicate its
	// files downstream. Fail hard rather than degrade to a partially-logged load.
	if imp.skipBinlog {
		if _, err := conn.ExecContext(ctx, "SET SQL_LOG_BIN=0"); err != nil {
			var me *mysql.MySQLError
			if errors.As(err, &me) && me.Number == 1227 {
				return fmt.Errorf("SET SQL_LOG_BIN=0 requires SUPER or SYSTEM_VARIABLES_ADMIN "+
					"(not granted on Cloud SQL/RDS — drop --skip-binlog there): %w", err)
			}
			return fmt.Errorf("SET SQL_LOG_BIN=0: %w", err)
		}
	}

	return fn(conn, bufio.NewReaderSize(underlying, 4*1024*1024))
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

// execBatched streams statements from r into conn inside explicit
// transactions, skipping the first skip statements (already committed by a
// previous attempt) and reporting each commit through progress.
func execBatched(ctx context.Context, conn *sql.Conn, r io.Reader, skip int64, progress func(int64) error) error {
	return batchStream(r, skip, txBatchBytes, func(stmt string) error {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", truncate(stmt, 120), err)
		}
		return nil
	}, progress)
}

// batchStream drives a resumable, transactional statement stream through
// exec. Statements are wrapped in BEGIN/COMMIT batches of roughly batchBytes
// statement bytes; progress is called with the cumulative statement count
// after every successful COMMIT — and never before it, so the recorded count
// only ever covers durable rows.
//
// The first skip statements are not re-executed, with one exception: USE
// statements are session state, not data — go-dump writes unqualified
// INSERTs that rely on the USE line before them, so a resumed stream must
// replay USE to land the remaining statements in the right schema.
func batchStream(r io.Reader, skip int64, batchBytes uint64, exec func(string) error, progress func(int64) error) error {
	var seen int64
	var inTx bool
	var txBytes uint64

	err := parseStatements(r, func(stmt string) error {
		seen++
		if seen <= skip {
			if isUseStatement(stmt) {
				return exec(stmt)
			}
			return nil
		}
		if !inTx {
			if err := exec("BEGIN"); err != nil {
				return err
			}
			inTx = true
			txBytes = 0
		}
		if err := exec(stmt); err != nil {
			return err
		}
		txBytes += uint64(len(stmt))
		if txBytes >= batchBytes {
			if err := exec("COMMIT"); err != nil {
				return err
			}
			inTx = false
			return progress(seen)
		}
		return nil
	})
	if err != nil {
		// The connection's open transaction (if any) rolls back when the
		// caller closes it — nothing uncommitted survives to be double-loaded.
		return err
	}
	if inTx {
		if err := exec("COMMIT"); err != nil {
			return err
		}
		return progress(seen)
	}
	return nil
}

// isUseStatement reports whether stmt is a USE statement ("USE `db`" or
// "USE db", any case). Statements arrive trimmed from parseStatements.
func isUseStatement(stmt string) bool {
	if len(stmt) < 4 {
		return false
	}
	if upperByte(stmt[0]) != 'U' || upperByte(stmt[1]) != 'S' || upperByte(stmt[2]) != 'E' {
		return false
	}
	return stmt[3] == ' ' || stmt[3] == '\t' || stmt[3] == '`'
}

// parseStatements reads SQL from r and calls cb for each complete statement.
// The scanner understands enough MySQL syntax that semicolons inside other
// constructs never split a statement:
//   - single- and double-quoted strings, with both \' and ” escaping
//   - backtick-quoted identifiers (” doubling, no backslash escapes)
//   - line comments ("-- " per MySQL — the dashes must be followed by
//     whitespace — and "#") and /* block comments */
//   - DELIMITER directives (client-side, never sent to the server), so
//     trigger/routine/event files whose bodies contain semicolons load
//     correctly. A directive is only recognised on a line with no SQL
//     before it, matching how go-dump and mysqldump emit them.
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

	// Statement terminator state. delim is usually ";" but changes via
	// DELIMITER directives; delimPos counts how many delimiter bytes have
	// matched so far (multi-byte delimiters match incrementally).
	delim := []byte{';'}
	delimPos := 0

	// DELIMITER directive detection. candidate is true while the current line
	// could still be a directive (no SQL preceded it in the statement); wordPos
	// tracks case-insensitive matching against "DELIMITER"; once the word plus
	// whitespace is seen, inDirective captures the rest of the line as the new
	// delimiter token.
	const directiveWord = "DELIMITER"
	candidate := true
	wordPos := 0
	inDirective := false
	var dirBuf []byte

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

	applyDirective := func() {
		if token := strings.TrimSpace(string(dirBuf)); token != "" {
			delim = []byte(token)
		}
		// The directive line (and any leading whitespace/comments) is consumed;
		// it is a client instruction, not SQL.
		stmt.Reset()
		hasSQL = false
		inDirective = false
		candidate = true
		wordPos = 0
		dirBuf = dirBuf[:0]
		delimPos = 0
	}

	buf := make([]byte, 64*1024)
	for {
		n, readErr := r.Read(buf)
		for i := 0; i < n; i++ {
			b := buf[i]

			if inDirective {
				if b == '\n' {
					applyDirective()
				} else {
					dirBuf = append(dirBuf, b)
				}
				prev = b
				continue
			}

			// Track the DELIMITER keyword at the start of a possible directive
			// line. The matched letters still flow through normal processing
			// below, so the statement is intact if the line turns out not to be
			// a directive (e.g. "DELETE ...").
			if candidate && inQuote == 0 && !inLineComment && !inBlockComment && !escaped && delimPos == 0 {
				switch {
				case wordPos < len(directiveWord) && upperByte(b) == directiveWord[wordPos]:
					wordPos++
				case wordPos == len(directiveWord) && (b == ' ' || b == '\t'):
					inDirective = true
					dirBuf = dirBuf[:0]
					prev = b
					continue // the space is part of the directive, not SQL
				case wordPos == 0 && (b == ' ' || b == '\t' || b == '\r'):
					// leading whitespace — line may still be a directive
				default:
					candidate = false
					wordPos = 0
				}
			}

		reprocess:
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

			case delimPos > 0 || b == delim[0]:
				// Delimiter bytes are not included in the statement body.
				if b == delim[delimPos] {
					delimPos++
					if delimPos == len(delim) {
						delimPos = 0
						if err := emit(); err != nil {
							return err
						}
					}
				} else {
					// Partial delimiter match failed: the matched bytes were
					// ordinary SQL after all. Restore them and reread b.
					stmt.Write(delim[:delimPos])
					delimPos = 0
					goto reprocess
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
			// A new line with no SQL accumulated yet may open a directive.
			if b == '\n' && inQuote == 0 && !inBlockComment {
				candidate = !hasSQL
				wordPos = 0
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
	// Handle a directive or trailing content without a final newline/delimiter.
	if inDirective {
		applyDirective()
	}
	if delimPos > 0 {
		stmt.Write(delim[:delimPos])
	}
	return emit()
}

// upperByte upper-cases a single ASCII letter.
func upperByte(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return b - ('a' - 'A')
	}
	return b
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
// pattern, then post-data object files (routines, events, triggers). When
// pattern ends with ".sql", compressed variants (*.sql.gz, *.sql.zst) are
// automatically included so compressed dumps work without requiring the user
// to change the pattern flag.
func findFiles(dir, pattern string) ([]FileLoad, error) {
	var files []FileLoad
	seen := make(map[string]bool)

	// Always collect schema-create and definition files as schema (plain and
	// compressed). Ordering between them is handled by the sort below.
	for _, defPat := range []string{
		"*-schema-create.sql", "*-schema-create.sql.gz", "*-schema-create.sql.zst",
		"*-definition.sql", "*-definition.sql.gz", "*-definition.sql.zst",
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

	// Object files load after all data. Collected before the data glob so the
	// default "*.sql" pattern never picks them up as data.
	for _, postPat := range []string{
		"*-routines.sql", "*-routines.sql.gz", "*-routines.sql.zst",
		"*-events.sql", "*-events.sql.gz", "*-events.sql.zst",
		"*-triggers.sql", "*-triggers.sql.gz", "*-triggers.sql.zst",
	} {
		matches, err := filepath.Glob(filepath.Join(dir, postPat))
		if err != nil {
			return nil, err
		}
		for _, p := range matches {
			if !seen[p] {
				seen[p] = true
				files = append(files, FileLoad{Path: p, IsPost: true})
			}
		}
	}

	// Data files matching the user's pattern.
	dataPatterns := []string{pattern}
	// When pattern ends with ".sql" (the default), also check "*.sql.gz" and
	// "*.sql.zst" so compressed dumps work without a flag change.
	if strings.HasSuffix(pattern, ".sql") {
		dataPatterns = append(dataPatterns, pattern+".gz", pattern+".zst")
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
			// plain text for the operator, and change-replication-source.sql is
			// an operator-edited template — none are loadable during restore.
			if strings.HasPrefix(base, "master-data.") || strings.HasPrefix(base, "slave-data.") ||
				strings.HasPrefix(base, "change-replication-source.") {
				continue
			}
			seen[p] = true
			files = append(files, FileLoad{Path: p, IsSchema: false})
		}
	}

	// Load order: database creation, table definitions, data, then routines,
	// events, and finally triggers (which must never precede their table's data).
	rank := func(f FileLoad) int {
		base := filepath.Base(f.Path)
		switch {
		case strings.Contains(base, "-schema-create"):
			return 0
		case f.IsSchema:
			return 1
		case !f.IsPost:
			return 2
		case strings.Contains(base, "-routines."):
			return 3
		case strings.Contains(base, "-events."):
			return 4
		default: // triggers
			return 5
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
