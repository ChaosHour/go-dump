# go-dump Improvement Plan

Goal: production-ready, reliable MySQL logical backup tool with mydumper-parity features
(checksums, resume, metadata, restore). Work through issues in order — each section
can be reviewed and approved before moving on.

---

## Production Readiness Review (2026-06-11)

**Verdict: close, but not production-ready yet.** The refactor is in good shape —
builds clean, `go vet` clean, 48 tests pass (including `-race`), SQL escaping is
NO_BACKSLASH_ESCAPES-safe, metadata/checksum/state files are written atomically, and
go-load has retries and streaming parsing. However, a fresh end-to-end read found
**6 blockers**, two of which can silently produce an incomplete or inconsistent backup
with exit code 0. For a backup tool, silent data loss is the disqualifying failure
mode. Estimate: the blockers below are 1–2 focused sessions of work; after that, plus
one verified dump→restore→checksum cycle against a real workload, this is shippable.

> **Update (2026-06-12): everything below is resolved.** All 6 blockers,
> 4 major issues, and 7 moderate issues are fixed, and all three exit criteria
> are validated — see the fix sections and "Verdict (2026-06-12):
> production-ready" at the end of this review.

### Blockers (all fixed ✅ — see "Blocker fixes (2026-06-11)" below)

**B1. Silent data loss: single-chunk tables with `--threads > 1`**
(`internal/dump/taskmanager.go:587`)
`StartWorker` pulls from the shared chunk channel, then does
`if chunk.IsSingleChunk && workerId != 0 { continue }` — the chunk is consumed,
`Queue` is decremented, and the chunk is **dropped, never re-queued**. A table dumped
via `--tables-without-uniquekey=single-chunk` has an (N−1)/N chance its only chunk
lands on a non-zero worker and its data is silently missing from the dump. No error,
exit code 0.
*Fix:* remove the workerId restriction (any worker can process a single chunk — each
worker has its own snapshot), or route single chunks to a dedicated channel.

**B2. `CreateChunks` treats every non-ErrNoRows error as success → infinite loop**
(`internal/dump/task.go:90`)
`if err != nil && err == sql.ErrNoRows` is just `err == sql.ErrNoRows`. Any other
error — network failure, scan failure, **context cancellation from Ctrl+C** — falls
into the else branch, which adds a chunk with a stale `chunkMax` and loops forever,
flooding the channel with garbage ranges.
*Fix:* three-way branch: `ErrNoRows` → last chunk; other error → log + abort the task
(and propagate failure); nil → add chunk. Also check `ctx.Err()` in the loop.

**B3. The LOCK TABLES / FTWRL path does not actually produce a consistent snapshot**
(`internal/dump/taskmanager.go:232-244`, `444-487`)
`createWorkersBeginTx` uses `BeginTx`, which issues plain `START TRANSACTION`. InnoDB
only establishes the REPEATABLE READ read view at the **first read**, which happens
after `UNLOCK TABLES` when writes have already resumed. The lock window protects
nothing; each worker's snapshot is taken at a different, later instant. (The
InnoDB-only path using `START TRANSACTION WITH CONSISTENT SNAPSHOT` is correct —
this bug affects the fallback path for non-InnoDB / mixed-engine pools.)
*Fix:* issue `START TRANSACTION WITH CONSISTENT SNAPSHOT` on every worker connection
*inside* the lock window (it's valid regardless of engine; non-InnoDB tables are
protected by the lock itself only while held — document that limitation, as mydumper does).

**B4. Session-scoped locks issued on a connection pool**
(`internal/dump/taskmanager.go:197-218`)
`lockTables`/`lockAllTables`/`unlockTables` run `tm.DB.Exec(...)` on a *pool*.
`LOCK TABLES`/FTWRL are per-session: `UNLOCK TABLES` can execute on a **different**
pooled connection (a no-op), leaving the read lock held for up to `ConnMaxLifetime`
(5 min) — a production outage on a write-heavy primary.
*Fix:* take a dedicated `*sql.Conn` for the lock lifecycle; lock, capture master
data, create worker snapshots, and unlock all on that conn.

**B5. Binlog coordinates not synchronized with the snapshot in the no-lock path**
(`internal/dump/taskmanager.go:444-487`)
In the InnoDB-only path, worker snapshots are started sequentially with no barrier,
then `getMasterData()` reads `SHOW MASTER/BINARY LOG STATUS` with no lock. Writes
landing between snapshot creation and the position read make `metadata.json`'s
binlog/GTID coordinates unusable for seeding a replica, and the N snapshots are N
slightly different points in time.
*Fix:* take a brief FTWRL (or `LOCK INSTANCE FOR BACKUP` + `FTWRL`) just long enough
to start all worker snapshots and read the binlog position, then release — this is
the mydumper sequence; the lock window is milliseconds.

**B6. Write errors (disk full) are warnings, not failures**
(`internal/dump/taskmanager.go:613`, `625-628`)
`buffer.Flush()` return is ignored and `buffer.Close()` errors are logged at WARNING.
A disk-full during the final flush produces a truncated `.sql` file, status
`"complete"` in metadata.json, and exit code 0.
*Fix:* treat any Flush/Close error in the worker path as fatal for the dump; mark
metadata `"failed"`.

### Major (all fixed ✅ — see "Major-issue fixes (2026-06-12)" below)

- **Resume is all-or-nothing in practice** (`internal/dump/dumper.go:123-126`):
  `MarkTableDone` is only called for *all* tables after *all* workers finish, so a
  crash mid-dump leaves every table pending and `--resume` re-dumps everything.
  Mark each table done as its last chunk completes (needs a per-task completed-chunk
  counter compared against `TotalChunks` after the create phase ends). ✅
- **Resume overwrites metadata.json, erasing prior completed tables**
  (`internal/dump/dumper.go:104-115`): a resumed run builds fresh metadata containing
  only the filtered (re-dumped) tables; previously-done tables disappear from
  metadata.json and checksums.txt. Merge with the prior run's metadata on resume. ✅
- **No `CREATE DATABASE IF NOT EXISTS`** in any output file: definition files begin
  with ``USE `db`;`` — restoring to a fresh server fails. Emit a per-schema
  `*-schema-create.sql` (and have go-load run them first). ✅
- **Worker chunk errors call `log.Fatalf`** (`taskmanager.go:600,616`): one transient
  error kills the whole dump with no retry (go-load has retries; go-dump doesn't). ✅

### Moderate / polish (all fixed ✅ — see "Moderate-issue fixes (2026-06-12)" below)

- `tm.StartTime` is written in `GetTransactions` while the `PrintStatus` goroutine
  (started earlier in `Run`) reads it — an unsynchronized race; also progress rates
  are computed from zero-time until then.
- `timestamp` is in the allowed chunk-key DATA_TYPE list (`table.go:107`) but
  `CreateChunks` scans the key into `int64` — fails, and via B2 becomes an infinite
  loop. Either drop timestamp support or handle it. Unsigned BIGINT PKs > MaxInt64
  also overflow `int64` chunk bounds.
- Chunk-key column name is interpolated unquoted (`task.go:38,44`) — backtick it.
- `getMasterData` doesn't check `masterRows.Next()` — binlog disabled yields a
  confusing scan error instead of the intended hint.
- `CHECKSUM TABLE` runs after UNLOCK on a live server, so checksums reflect
  post-dump data; mismatches are expected under write load. Document that `--checksum`
  / `--verify` are only meaningful on a quiesced source.
- go-load's `parseStatements` doesn't understand `--`/`/* */` comments or backticked
  identifiers; an apostrophe inside either desyncs the splitter. Low risk with
  go-dump-generated files; worth hardening if loading foreign dumps.
- Repo hygiene before merge: remove the stray `*.sql` dump
  artifacts from the repo root (add `*.sql` at root to `.gitignore`), and the unused
  `Task.Tx` field / `GetMasterStatusSQL()` dead code.

### Blocker fixes (2026-06-11)

All six blockers fixed in this session. What changed:

- **B1** (`taskmanager.go` StartWorker): removed the
  `chunk.IsSingleChunk && workerId != 0 { continue }` skip — any worker now
  processes single chunks (each worker has its own synchronised snapshot, and a
  single-chunk table produces exactly one chunk/file, so there is no contention).
- **B2** (`task.go` CreateChunks): the chunk loop now branches three ways —
  `nil` → add chunk; `ErrNoRows` → emit tail chunk and stop; any other error
  (network, scan, context cancellation) → `log.Fatalf`, which runs the exit hook
  and marks metadata `"failed"`. The single-chunk probe error path was also
  upgraded from `Errorf` (silent table skip) to `Fatalf`.
- **B3/B4/B5** (`taskmanager.go` GetTransactions, rewritten): one dedicated
  `*sql.Conn` (never the pool) acquires the lock — `LOCK TABLES ... READ` for an
  InnoDB-only table list, FTWRL for `--all-databases` or mixed engines. While
  the lock is held, every worker runs
  `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ` +
  `START TRANSACTION WITH CONSISTENT SNAPSHOT` (read view established
  immediately, unlike `BeginTx`), and binlog/GTID coordinates are captured.
  Then `UNLOCK TABLES` runs on the same pinned connection. `BeginTx` workers
  remain only for the explicit `--lock-tables=false` path, which now logs a
  warning that the dump is not point-in-time consistent. Non-InnoDB limitation
  (write-protected only during the window) is documented in the function comment
  and logged as a warning when such tables are present.
- **B6** (`taskmanager.go`): all dump-file `Flush`/`Close` errors are now fatal
  instead of warnings — chunk buffers, definition files, and master/slave data
  files. A disk-full can no longer produce a truncated file with
  `status: "complete"` and exit 0. `NewChunkBuffer` errors are also checked.

Verification performed (live MySQL 8.x at 127.0.0.1):

- B1: no-PK InnoDB table dumped with `--threads 4 --tables-without-uniquekey
  single-chunk`, 5 consecutive runs — all rows present every run (pre-fix odds
  of silent loss: ~75% per run).
- B2: SIGINT sent 3s into a dump of an 8.5M-row test table: process exited within ~0.5s with
  "Error creating chunks ... context canceled" and metadata `status: "failed"`
  (pre-fix: infinite chunk-creation loop).
- B3–B5: dump with `--get-master-status --checksum`: log shows
  lock → snapshots → binlog capture → unlock on one connection, lock window
  16–18ms; binlog file/position/GTID recorded in both `master-data.sql` and
  `metadata.json`; `status: "complete"`.
- Full-scale regression: 8.5M-row table, 4 threads, 1m11s, dumped row
  count exactly matches `SELECT COUNT(*)` on the source.
- `go build ./...`, `go vet ./...`, `go test -race ./...` all pass (48 tests,
  including the live-MySQL integration test).

Remaining before calling it production-ready: the **Major** items above
(per-table resume granularity, resume metadata merge, `CREATE DATABASE`
emission, worker retry) and exit criteria 2–3 below.

### Major-issue fixes (2026-06-12)

All four major issues fixed. What changed:

- **Per-table resume granularity** (`task.go`, `taskmanager.go`, `dumper.go`):
  each `Task` tracks `ChunksCompleted` (atomic) plus `chunkingDone`/`markedDone`
  flags. Workers flush the dump file after every chunk and call
  `NoteChunkCompleted`; when chunk creation has ended and the last chunk is
  flushed, the table is marked `done` in metadata.json immediately. Both the
  chunk creator and the workers run the check (`maybeMarkDone`), covering
  zero-chunk tables and either finishing order. **Compressed dumps are excluded**
  from early marking — gzip streams only become valid on file close, so they are
  marked by the end-of-run sweep as before (documented in code). `Buffer.Flush`
  now also flushes the gzip writer so flushed bytes actually reach the file.
- **Resume metadata merge** (`metadata.go`, `resume.go`, `dumper.go`,
  `checksum.go`): `applyResumeFilter` returns the prior run's metadata;
  `MergeDoneTables` carries its `done` tables (with chunk counts and checksums)
  into the new metadata.json. `RunChecksums` covers carried tables too —
  reusing their recorded checksum, or computing one if the prior run died
  before its checksum phase. A warning is logged that a resumed dump is not
  consistent to a single point in time.
- **CREATE DATABASE emission** (`taskmanager.go` `WriteSchemaCreateSQL`,
  `load/importer.go`): go-dump writes one `<schema>-schema-create.sql` per
  schema from `SHOW CREATE DATABASE` (charset/collation preserved,
  `IF NOT EXISTS` added mysqldump-style). go-load orders restores
  schema-create → definitions → data. Definition and schema-create files are
  now written *before* data dumping starts, so a partial dump is restorable as
  far as it got. Found and fixed while verifying: go-load's `*.sql` glob was
  also picking up `master-data.sql`/`slave-data.sql` (plain-text status files)
  and failing the restore — now excluded.
- **Worker chunk retry** (`taskmanager.go`, `datachunk.go`): each chunk is
  staged in a reusable in-memory buffer and only copied to the dump file after
  the query succeeds, so a retry can never duplicate rows already on disk.
  Transient errors that leave the snapshot transaction intact (lock wait
  timeout 1205, deadlock 1213) are retried up to 3 times with backoff.
  Connection loss (ErrBadConn/2006/2013) is deliberately *not* retried: the
  snapshot dies with the connection, and re-reading on a new one would silently
  break point-in-time consistency — the dump fails and `--resume` (now
  per-table) makes the restart cheap.

Verification (live MySQL 8.0.43):

- Restore-from-scratch: dumped 2 tables, `DROP DATABASE`, `go-load --verify`
  recreated the database from `majortest-schema-create.sql`, loaded schema +
  data in order, and all checksums verified OK.
- Per-table granularity: `kill -9` six seconds into a 3-table dump (2 small +
  8.5M-row table): metadata.json showed the small tables `done` (5 chunks each)
  and the big table `pending`.
- Resume merge: `--resume --checksum` on that directory skipped the 2 done
  tables, cleaned and re-dumped only the big table; final metadata.json lists
  all 3 tables done with chunk counts and checksums, and checksums.txt covers
  all 3 (small tables checksummed via the carried-table path since the first
  run died before its checksum phase).
- New unit tests: `TestMergeDoneTables`, `TestMaybeMarkDone` (incl. compressed
  exclusion), `TestApplyResumeFilter_*` updated for the new return value,
  `TestFindFiles_SchemaCreateFirst`, `TestFindFiles_SkipsReplicationStatusFiles`.
  `go build`, `go vet`, `go test -race ./...` all pass (53 tests).

### Moderate-issue fixes (2026-06-12)

All moderate/polish items fixed. What changed:

- **`StartTime` race** (`taskmanager.go`, `dumper.go`): `StartTime` is now set in
  `NewTaskManager` and refreshed in `Run` immediately before the `PrintStatus`
  goroutine starts — both writes happen-before the goroutine; the write inside
  `GetTransactions` (which raced with the running goroutine) is gone.
- **Chunk-key edge cases** (`table.go`, `task.go`): the chunk-key column query
  now requires integer types only (dropped `timestamp` — chunk bounds use
  int64 arithmetic, so a timestamp PK failed at runtime) and `IS_NULLABLE='NO'`
  (BETWEEN/`>=` never match NULL, so a nullable unique key silently excluded
  NULL-key rows from every chunk). Both cases now fall through to the
  `--tables-without-uniquekey` handling. **Bonus data-loss fix found here:**
  chunk bounds previously started at 0, silently excluding rows with negative
  keys; bounds are now seeded from `SELECT MIN(key)`. Keys above MaxInt64
  (upper half of unsigned BIGINT) fail loudly at the MIN scan with a clear
  error naming the table.
- **Chunk-key quoting** (`mysql.go` `escapeIdentifier`, `task.go`,
  `datachunk.go`): the key column is backtick-quoted (with embedded-backtick
  doubling) in chunk boundary queries, WHERE clauses, ORDER BY, and the MIN
  query — reserved-word column names (e.g. `` `order` ``) now work.
- **`getMasterData` empty result** (`taskmanager.go`): `masterRows.Next()` is
  checked; binlog-disabled servers get "Binary log status returned no rows.
  Make sure binary logging is enabled, or omit --get-master-status." instead of
  a confusing scan error.
- **Checksum caveat**: `RunChecksums` logs that `CHECKSUM TABLE` reads current
  state and only matches the dump on quiesced sources; same caveat added to the
  README checksums section.
- **go-load statement parser hardened** (`load/importer.go`): full state
  machine covering single/double-quoted strings (`\'` and `''`), backtick
  identifiers, `-- ` (whitespace-required, per MySQL), `#`, and `/* */`
  comments. Semicolons/apostrophes inside any of those no longer desync the
  splitter. `/*!...*/` versioned comments still execute; segments containing
  only plain comments are skipped instead of being sent to the server (fixes
  `ER_EMPTY_QUERY` on trailing `-- Chunk N` headers from empty chunks).
  8 new splitter tests.
- **Repo hygiene**: stray `*.sql` dump artifacts removed from the repo
  root; `.gitignore` extended (root-level dump artifacts, `go-load` binary) —
  and the pre-existing unanchored `go-dump` pattern was caught ignoring the new
  `cmd/go-dump/` source directory; binary patterns are now root-anchored
  (`/go-dump`, `/go-load`). Dead code removed: unused `Task.Tx` field and
  `GetMasterStatusSQL()`.

Verification (live MySQL 8.0.43): a database with four edge-case tables —
PK spanning −1000..999, a `TIMESTAMP` PK, a nullable unique key with NULL rows,
and a reserved-word PK column (`` `order` ``) — dumped completely (all 1000
negative-key rows present; pre-fix: zero), restored via go-load into a dropped
database, and passed checksum verification. `go build`, `go vet`,
`go test -race ./...` all pass (61 tests).

### What "production ready" should require (exit criteria)

1. B1–B6 fixed, with regression tests for B1 (multi-thread single-chunk) and B2
   (error injection in chunking). ✅ (2026-06-11)
2. One full dump → restore → `--verify` cycle on a multi-table, multi-engine dataset
   (e.g. sakila + a MyISAM table) with `--threads 4`, plus a kill -9 mid-dump followed
   by `--resume` and a row-count comparison. ✅ (2026-06-12)
3. Replica-seed validation: restore a dump, point a replica at the recorded
   binlog/GTID coordinates from metadata.json, confirm it catches up cleanly.
   ✅ (2026-06-12)

### Exit-criteria validation results (2026-06-12)

**Criterion 2 — multi-engine dump/restore/resume cycles.** Dataset: `exitcrit`
database with `orders` (InnoDB, 2,097,152 rows, DECIMAL + escaped-quote text),
`customers` (InnoDB, 8,192), `legacy_log` (MyISAM, 16,384), and `no_pk_notes`
(InnoDB, no PK → single-chunk), dumped with `--threads 4 --checksum`.

- *Full cycle:* mixed engines correctly triggered the FTWRL path (non-InnoDB
  warning logged, lock held 41ms). `DROP DATABASE` → `go-load --verify` →
  checksums OK, **per-table row counts identical to source**, MyISAM engine
  preserved through restore.
- *Kill −9 + resume cycle:* SIGKILL 4s into a fresh dump left `customers`,
  `legacy_log`, `no_pk_notes` marked `done` and `orders` `pending` (per-table
  granularity working). `--resume` skipped the 3 done tables, cleaned and
  re-dumped only `orders`. Restore of the resumed dump: checksums OK,
  **row counts identical to source**.

**Criterion 3 — replica seed from recorded coordinates.** A throwaway MySQL 8.0
container (`server-id=99`, GTID ON) was attached to the primary's Docker
network. Sequence: fresh dump with `--get-master-status` → restore via go-load →
`RESET MASTER` + `SET GLOBAL gtid_purged = '<gtid_set from metadata.json>'` →
`CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1` (with a
`REPLICATE_WILD_DO_TABLE=('exitcrit.%')` guard filter) → `START REPLICA`.
Result: both replication threads running, `Seconds_Behind_Source: 0`, no
errors; two post-dump writes on the primary appeared on the replica within
seconds and final counts were source+1 per written table. The dump +
metadata.json GTID coordinates are sufficient to seed a replica cleanly.
(Test container, temp `godump_repl` user, and test database all removed;
the pre-existing primary/replica containers were not touched.)

---

## Verdict (2026-06-12): production-ready ✅

All review findings (6 blockers, 4 major, 7 moderate) are fixed, and all three
exit criteria validated against live MySQL 8.0.43. Documented operating
limitations (by design, called out in README/logs):

- Non-InnoDB tables are write-protected only during the brief lock window;
  their data is read afterwards without MVCC.
- A resumed dump is not consistent to a single point in time across the
  resume boundary (warned at runtime).
- `--checksum` / `--verify` are only meaningful on quiesced sources.
- Chunk keys must be NOT NULL integer columns ≤ MaxInt64; other tables use
  `--tables-without-uniquekey` handling.

---

## Current Status (2026-06-10)

### Test Results

All 12 tests pass:

```
TestNewSingleDataChunk               PASS (unit)
TestNewDataChunk                     PASS (unit)
TestNewLastDataChunk                 PASS (unit)
TestTable                            PASS (unit)
TestAddTask                          PASS (unit)
TestTaskGetChunkSqlQuery             PASS (unit)
TestTaskGetLastChunkSqlQuery         PASS (unit)
TestGetLockTablesSQL                 PASS (unit)
TestCreateTaskManager                PASS (integration — requires MySQL at 127.0.0.1:3306)
TestLoadIniFile                      PASS (integration — reads test/test.ini)
TestParseWhereCondition_Global       PASS (unit)
TestParseWhereCondition_TableSpecific PASS (unit)
```

### Features Added (2026-06-10 session 3)

- **README rewrite**: full documentation covering how it works, MySQL version support, all flags, INI format, common examples, output files, restore instructions, privilege requirements, Cloud SQL notes.
- **FTWRL avoidance** (`taskmanager.go`): `isInnoDBOnly()` check on the task pool. When all tables are InnoDB, workers open dedicated connections and issue `START TRANSACTION WITH CONSISTENT SNAPSHOT` — eliminating the write-lock window entirely. Non-InnoDB tables fall back to FTWRL/LOCK TABLES automatically. `txRunner` interface (`txTxRunner` / `txConnRunner`) keeps `StartWorker` engine-agnostic.
- **Resume** (`resume.go`, `metadata.go`, `dumper.go`): `--resume` flag. Loads prior `metadata.json`, skips tables marked `done`, removes partial chunk files for `in_progress` tables, and logs how many tables are being skipped vs re-dumped. `DumpMetadata.ResumeState()` returns the done/in-progress split.

### Features Added (2026-06-10 session 2)

- **`GODUMP_PASSWORD` env var** (`main.go`): password fallback after flag/INI resolution, avoids credentials in shell history.
- **Connection pool limits** (`mysql.go`): `SetMaxOpenConns`, `SetMaxIdleConns`, `SetConnMaxLifetime`, `SetConnMaxIdleTime` — prevents exhaustion on large servers.
- **`--all-databases` Cloud SQL exclusions** (`mysql.go`, `options.go`): excludes `mysql`, `sys`, `information_schema`, `performance_schema` by default. New `--include-system-databases` flag for on-prem account migrations.
- **Progress display** (`taskmanager.go`): `CompletedChunks` counter (atomic), `StartTime`. `PrintStatus` now shows `Progress: N/M (X.X%) | Rate: Y chunks/s | ETA: ~Zs` every 5 seconds.
- **`metadata.json`** (`internal/dump/metadata.go`): written atomically at dump start (`in_progress`) and finish (`complete`). Captures MySQL version, binlog file/position, GTID set, per-table row estimates and chunk counts, and checksum values.
- **Per-table checksums** (`internal/dump/checksum.go`): `--checksum` flag runs `CHECKSUM TABLE` after workers finish (outside lock window), writes `checksums.txt` atomically, and updates `metadata.json`. `VerifyChecksums()` available for restore-time validation.

### Bugs Fixed (2026-06-10 session 1)

- **`SHOW BINARY LOG STATUS` wrong version threshold**: `getMasterData()` used
  `mysqlAtLeast(8, 0, 22)` but `SHOW BINARY LOG STATUS` was introduced in MySQL 8.4.0.
  Fixed to `mysqlAtLeast(8, 4, 0)`. MySQL 8.0.x continues to use `SHOW MASTER STATUS`.
- **`lockTables()` with empty pool**: Generated `LOCK TABLES ` (no table list) causing
  a SQL syntax error. Added early return when `tasksPool` is empty.
- **`TestCreateTaskManager` no skip guard**: Added `t.Skip` when `tmdb == nil` so the
  test is correctly skipped in environments without MySQL at 127.0.0.1:3306.

### Verified Working

- Live dump of an 8.5M-row test table: 170 chunks,
  4 threads, ~49 seconds, ~1.9 GB output.
- All 18 production issues from the original code review are fixed.
- INI file parsing with `mysql-password = value` (spaces around `=`) works correctly.

---

## Section 1 — Critical Bugs (all fixed ✅)

1.1 `getTablesFromQuery` stub — `--all-databases` was broken ✅
1.2 `NewFileBuffer` nil `FileDescriptor` on plain-file path ✅
1.3 Empty INSERT written when chunk has no rows ✅
1.4 INI `get-slave-status` set `LockTables` instead of `GetSlaveStatus` ✅
1.5 Worker decremented Queue before checking channel-closed ✅
1.6 `ParseString` used `\'` instead of `''` for single quotes ✅
1.7 Race conditions on `Queue` and `TotalChunks` — fixed with `sync/atomic` ✅

---

## Section 2 — Correctness / Safety Issues (all fixed ✅)

2.1 `TablesFromDatabase` included views ✅
2.2 `getColumnsInformationSQL` raw string interpolation ✅
2.3 `getData` ignored error from `getTableInformation` ✅
2.4 `outputChunkSize` logic was commented out ✅
2.5 `SHOW MASTER STATUS` / `SHOW SLAVE STATUS` deprecated syntax ✅ (8.0.22 for REPLICA, 8.4.0 for BINARY LOG STATUS)
2.6 `runtime.GOMAXPROCS` set to `--threads` ✅
2.7 Context not propagated through DB calls ✅
2.8 Signal handler called `log.Fatalf` without cleanup ✅
2.9 INSERT used positional values, not column names ✅

---

## Section 3 — mydumper-Parity Features

### 3.1 Per-table checksums

After each table is fully dumped, compute a checksum and write a verification file.

**Design:**

- Add `--checksum` flag (default `false`; opt-in).
- After all chunks complete for a table, run:
  ```sql
  CHECKSUM TABLE `schema`.`table`;
  ```
  MySQL's native CRC32-based checksum (same algorithm as mydumper).
- Write one line per table to `<destination>/checksums.txt`:
  ```
  schema.table  <checksum_value>  <row_count>  <dump_timestamp>
  ```
- Add a `--verify` flag (or separate subcommand) that reads `checksums.txt`, runs
  `CHECKSUM TABLE` on the target, and compares.

**Implementation steps:**

1. Add `Checksum bool` to `DumpOptions` and wire the `--checksum` flag in `main.go`.
2. Add `RunChecksums(tasks []*Task, db *sql.DB, dest string) error` in `internal/dump`.
3. Call it in `dumper.go`'s `Run()` after `wgProcessChunks.Wait()` when `Checksum=true`.
4. Write `checksums.txt` atomically (write to `.tmp`, rename).
5. Add `VerifyChecksums(dest string, db *sql.DB) error` for restore-time verification.

**Risk:** `CHECKSUM TABLE` is a full table scan. Run after UNLOCK TABLES so it doesn't
extend the lock window.

---

### 3.2 Resume / incremental dump

Skip tables already completed when `--resume` is passed and a prior dump exists.

**Design:**

- At dump start, write `<destination>/metadata.json` with `status: in_progress`.
- Each completed table is recorded in metadata as `done` (write-then-rename).
- On `--resume`, read `metadata.json`; skip `done` tables; delete partial files for
  `in_progress` tables and re-dump them.

**Implementation steps:**

1. Define `DumpMetadata` / `TableMetadata` structs (see 3.3 for fields).
2. Add `Resume bool` to `DumpOptions` and wire `--resume` flag.
3. In `Run()`:
   - If `--resume` and `metadata.json` exists: load it, build `skipTables` set.
   - If `--resume` and `metadata.json` absent: log warning, run full dump.
4. Filter out `skipTables` in `resolveTables()`.
5. After each task completes, update `metadata.json` via a mutex-protected writer.
6. On clean finish, set `status: complete` and write end timestamp.

**Risk:** Partial chunk files from a crashed worker need cleanup. Chunk files are already
written atomically (`.tmp` rename in `Buffer.Close()`). Resume only needs to delete
files without a matching `done` metadata entry.

---

### 3.3 Dump metadata file

Write `<destination>/metadata.json` at dump start, updated throughout the run.

**Fields:**

```json
{
  "go_dump_version": "1.2.0",
  "start_time": "2026-06-10T14:00:00Z",
  "end_time": "2026-06-10T14:05:30Z",
  "status": "complete",
  "mysql_host": "db01.example.com",
  "mysql_version": "8.0.43",
  "binlog_file": "binlog.000042",
  "binlog_position": 1421,
  "gtid_set": "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-123",
  "character_set": "utf8mb4",
  "tables": [
    {
      "schema": "mydb",
      "name": "orders",
      "row_estimate": 1500000,
      "chunks": 1500,
      "status": "done",
      "checksum": 3829201847
    }
  ]
}
```

**Implementation steps:**

1. Add `internal/dump/metadata.go` with `DumpMetadata`, `TableMetadata` structs
   and `WriteMetadata(path string, m *DumpMetadata) error` (atomic write).
2. Call `WriteMetadata` at dump start (status=`in_progress`) and on clean finish.
3. Refactor `getMasterData()` to return a struct instead of writing directly to a
   file — populate the binlog/GTID fields in metadata from that struct.

---

### 3.4 Progress display

Real-time progress with percentage and ETA (like mydumper's status output).

**Design:**

```
2026-06-10 14:02:15 INFO Progress: 847/1500 chunks (56.5%) | ETA: ~1m12s | Rate: 14.2 chunks/s
```

**Implementation steps:**

1. Add `CompletedChunks int64` to `TaskManager` (atomic); increment in `StartWorker`
   after `chunk.Parse` returns successfully.
2. Add `StartTime time.Time` to `TaskManager`, set at the start of `GetTransactions`.
3. Rewrite `PrintStatus` to compute:
   - Percentage: `CompletedChunks * 100 / TotalChunks`
   - Rate: chunks per second (rolling 10s window or simple elapsed average)
   - ETA: `(TotalChunks - CompletedChunks) / rate`
4. Print every 5 seconds instead of every second (reduce log noise).
5. Respect `--quiet` flag (already plumbed; just check before printing).

---

### 3.5 go-load updates (https://github.com/ChaosHour/go-load)

**Bugs / gaps in current go-load:**

- `--pattern` default misses single-chunk tables (no `-thread` suffix). Change to `*.sql`.
- `chunkSize` / `channelBufferSize` flags exist but are unused. Remove or implement.
- `multiStatements=true` loads entire file into RAM. Replace with per-statement execution.
- Progress bar total is always `1`. Track statement count or byte count.
- Error collection from parallel workers is racy. Collect all errors.
- No compressed-file support. Add gzip decompression for `.gz` files.
- INI file handles only 3 keys. Add `mysql-port` and `mysql-socket`.
- `tls=skip-verify` is hardcoded. Make it a flag.
- `go.mod` is on Go 1.19. Bump to Go 1.23.

**Features to add (coordinate with go-dump changes):**

- Read `metadata.json` (Section 3.3) for charset, server version, restore order.
- Checksum verification (Section 3.1): verify each table before loading.
- Resume support (`--resume`): write `load-state.json`; skip already-loaded tables.
- `SET FOREIGN_KEY_CHECKS=0` per worker connection, not just from dump file content.

---

### 3.6 Consistent snapshot without `FLUSH TABLES WITH READ LOCK`

For InnoDB-only databases, FTWRL blocks all writes during the lock window. When
`--consistent` is true and all engines are InnoDB:

- Open all worker connections and run
  `START TRANSACTION WITH CONSISTENT SNAPSHOT` on each simultaneously.
- Only fall back to FTWRL when non-InnoDB tables are present.
- Document the tradeoff.

---

## Section 4 — Cloud SQL / Multi-Environment Compatibility

**Connection approach:** Direct TCP without TLS/SSL (private IP within VPC). All
three use cases work today with no connection-layer code changes needed.

| Source → Target | Method | Works today? |
|---|---|---|
| On-prem → On-prem | Direct TCP / socket | ✅ |
| On-prem → Cloud SQL | Direct TCP (private IP, no TLS) | ✅ |
| Cloud SQL → Cloud SQL | Direct TCP (private IP, no TLS) | ✅ |

### 4.1 Safer `--all-databases` exclusions for Cloud SQL

Cloud SQL manages its own `mysql.*` permission tables (user, db, tables_priv, etc.)
via IAM. Dumping and restoring those tables to another Cloud SQL instance will corrupt
or override IAM-managed permissions. The current exclusion list only drops
`mysql.slow_log` and `mysql.general_log`.

**Fix:** Exclude the entire `mysql` schema and `sys` schema by default in
`TablesFromAllDatabases`. Add `--include-system-databases` flag to opt back in for
on-prem-to-on-prem full-cluster migrations that need to transfer accounts.

```go
// Default exclusions — safe for both on-prem and Cloud SQL.
const allDatabasesQuery = `
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM information_schema.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
      AND TABLE_SCHEMA NOT IN (
          'information_schema', 'performance_schema', 'sys', 'mysql'
      )`

// With --include-system-databases: add mysql back, still skip slow_log/general_log.
```

**Implementation steps:**

1. Add `IncludeSystemDatabases bool` to `TemporalOptions` and wire the flag.
2. Change `TablesFromAllDatabases` to accept an `includeSystem bool` parameter.
3. Use the stricter query by default; use the broader one only when `includeSystem=true`.

---

### 4.2 Cloud SQL privilege requirements (no SUPER)

Cloud SQL root lacks SUPER. Confirmed that the current code does not require SUPER:

- `FLUSH TABLES WITH READ LOCK` → requires `RELOAD` (Cloud SQL root has it) ✅
- `LOCK TABLES ... READ` → no privilege beyond table access ✅
- `SHOW MASTER STATUS` / `SHOW REPLICA STATUS` → requires `REPLICATION CLIENT` (Cloud SQL root has it) ✅
- `SET GLOBAL` → removed from dump output (`max_allowed_packet` fix) ✅

No code changes needed. Document this in the README as a note on Cloud SQL usage.

---

## Section 5 — Code Quality / Maintainability

### 5.1 P3: Password via environment variable

Support `GODUMP_PASSWORD` env var as a fallback when `--mysql-password` and INI
are not set. Avoids credentials in shell history.

```go
if dumpOptions.MySQLCredentials.Password == "" {
    dumpOptions.MySQLCredentials.Password = os.Getenv("GODUMP_PASSWORD")
}
```

### 5.2 Connection pool configuration

Set explicit limits to avoid connection exhaustion on large servers:

```go
db.SetMaxOpenConns(dumpOptions.Threads + 2)
db.SetMaxIdleConns(dumpOptions.Threads + 2)
db.SetConnMaxLifetime(5 * time.Minute)
```

Apply in `GetMySQLConnection` or in `dumper.go`'s `Run()`.

---

## Section 6 — Test Coverage

### 6.1 Unit test `TablesFromDatabase` and `TablesFromAllDatabases` with sqlmock

Use `github.com/DATA-DOG/go-sqlmock` to test without a live MySQL instance.

### 6.2 Unit test `ParseString` / SQL escaping

Add cases: single quote, backslash, newline, null byte, multi-byte UTF-8, empty slice.

### 6.3 Integration test harness

Add `docker-compose.yml` or `make test-integration` that spins up MySQL 8.0 + sakila,
runs a full dump, verifies checksums, and runs go-load to restore.

---

## Execution Order

| Priority | Section | Effort | Status |
|----------|---------|--------|--------|
| 1 | 1.1–1.7 critical bugs | Small–Medium | ✅ Done |
| 2 | 2.1–2.9 correctness | Medium | ✅ Done |
| 3 | 5.1 password env var | Small | ✅ Done |
| 4 | 5.2 connection pool limits | Small | ✅ Done |
| 5 | 4.1 --all-databases Cloud SQL exclusions | Small | ✅ Done |
| 6 | 3.4 progress display (rate, %, ETA) | Small | ✅ Done |
| 7 | 3.3 metadata file | Small | ✅ Done |
| 8 | 3.1 checksums | Medium | ✅ Done |
| 9 | 3.2 resume | Medium | ✅ Done |
| 10 | 3.6 FTWRL avoidance | Medium | ✅ Done |
| 11 | 3.5 go-load updates | Medium | ✅ Done |
| 12 | 6.x tests | Medium | ✅ Done |

---

### Features Added (2026-06-10 session 4)

- **go-load integrated** (`cmd/go-load/`, `internal/load/`): ported from https://github.com/ChaosHour/go-load into this repo as a sibling binary. All PLAN.md 3.5 bugs fixed and features added:
  - Pattern default changed from `*-thread*.sql` to `*.sql` (catches single-chunk tables)
  - `chunkSize`/`channelBufferSize` unused flags removed
  - `multiStatements=true` replaced with per-statement execution via `splitStatements()` (SQL-aware: tracks quoted strings and backslash escapes)
  - Progress: 5-second ticker showing `N/M files loaded` (same cadence as go-dump)
  - Error collection: all parallel worker errors gathered before returning
  - gzip support: automatic `.gz` decompression when filename ends with `.gz`
  - INI: `mysql-port` and `mysql-socket` added to both `[client]` and `[go-load]` sections
  - TLS: removed hardcoded `tls=skip-verify`; DSN built with `mysql.NewConfig()` (no TLS by default)
  - Go module: uses this repo's `go 1.23.2`
  - `SET FOREIGN_KEY_CHECKS=0`, `SET UNIQUE_CHECKS=0`, `SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO'` per connection
  - Reads `metadata.json` from dump directory and logs source version/binlog position
  - `--verify` flag: calls `dump.VerifyChecksums()` after load
  - `--resume` flag: `load-state.json` tracks loaded files, skips them on next run
  - `GOLOAD_PASSWORD` env var fallback (same pattern as `GODUMP_PASSWORD`)
  - Makefile: `build-go-load`, `build-go-load-{linux,linux-arm64,darwin,darwin-arm64}`, `build-all-binaries`, `build-all` updated

### Production fixes + tests (2026-06-10 session 5)

- **`log.RegisterExitHook`** (`internal/log/log.go`): any `log.Fatalf` call now runs registered cleanup functions before `os.Exit`. Thread-safe (mutex-protected slice).
- **Graceful shutdown on crash** (`internal/dump/dumper.go`): registers a hook in `Run()` that calls `meta.Fail()` — metadata.json gets status `"failed"` instead of staying `"in_progress"` forever when a worker or buffer error kills the process.
- **go-load streaming parser** (`internal/load/importer.go`): replaced `io.ReadAll` + `splitStatements` with `parseStatements` + `execStream`. Memory usage is now O(largest single statement) not O(file size). The `parseStatements` function is shared by both the streaming production path and the `splitStatements` helper used in tests.
- **go-load retry logic**: `loadFile` wraps `doLoadFile` with up to 3 attempts and quadratic backoff (1s, 4s). Retries on `driver.ErrBadConn` and MySQL errors 2006/2013 (server gone away, lost connection).
- **go-load compressed file auto-detection**: `findFiles` now automatically globs for `*.sql.gz` data files when the pattern ends with `.sql`. Prevents silent data loss when loading a `--compress` dump without changing `--pattern`.
- **Tests — 48 total, all pass**:
  - `TestParseString` (10 subtests) — all escape sequences, empty input
  - `TestDumpMetadata*` (7 tests) — lifecycle, fail, checksum, start time, atomic write, round-trip, corrupt/missing
  - `TestResumeState` — done/inProgress split
  - `TestLoadDumpMetadata_NotExist`, `_Corrupt` — error paths
  - `TestCleanPartialFiles`, `_EmptyInProgress` — file cleanup
  - `TestApplyResumeFilter_*` (4 tests) — no metadata, skip done, clean partial, complete dump
  - `TestSplitStatements_*` (11 tests) — basic, CREATE+INSERT, semicolons in strings, `''` and `\'` escapes, `\\`, empty, whitespace, no trailing semicolon, comments, multi-value INSERT
  - `TestFindFiles_*` (5 tests) — basic, compressed auto-detect, empty dir, no definition duplication, explicit `.gz` pattern
  - `TestNewLoadState_*`, `TestMark_*`, `TestLoadState_*` (7 tests) — fresh state, persistence, idempotent mark, atomic write, corrupt file

*Last updated: 2026-06-10 (session 5)*
