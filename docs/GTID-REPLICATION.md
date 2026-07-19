# GTID & Replication: current state, gaps, and planned features

Status of GTID handling in go-dump/go-load as of 2026-07-19. Replica seeding
is now a one-command operation (`go-load --skip-binlog --set-gtid-purged
--start-replication`); this document tracks how each piece works and what
remains open.

## What works today

### Dump side — complete

- `--get-master-status` captures `SHOW MASTER STATUS` / `SHOW BINARY LOG STATUS`
  (8.4+) **inside the lock window**, after the worker consistent snapshots are
  opened and before `UNLOCK TABLES` (`internal/dump/taskmanager.go`,
  `GetTransactions`). The recorded binlog file/position and `Executed_Gtid_Set`
  therefore match the dumped data exactly — correct for replica seeding.
- Coordinates are persisted twice:
  - `metadata.json` → `mysql_host`, `mysql_port` (0.3.0+), `binlog_file`,
    `binlog_position`, `gtid_set` (`internal/dump/metadata.go`, `SetBinlog`)
  - `master-data.sql` → human-readable text report
- `--get-slave-status` records `SHOW REPLICA STATUS` (incl. `Executed_Gtid_Set`
  / MariaDB `Gtid_Slave_Pos`, multi-source aware) to `slave-data.sql` — useful
  when dumping from a replica to seed a sibling.
- GTID capture is **opt-in** (both flags default to `false`), so default dumps
  carry no replication state at all.
- Data/definition files contain only `USE`, `SET NAMES binary`,
  `FOREIGN_KEY_CHECKS=0`, `DROP TABLE IF EXISTS` (opt-in) and `INSERT`s. No
  `SET @@GLOBAL.GTID_PURGED`, no `CHANGE MASTER`. Restores are replication-safe
  by construction — the mysqldump `gtid_purged` footgun does not exist here.

### Load side — nearly complete

- `go-load` reads `metadata.json` and **logs** the source binlog/GTID
  coordinates (`cmd/go-load/main.go`).
- `master-data.*` / `slave-data.*` / `change-replication-source.*` are
  correctly excluded from loadable files (`internal/load/importer.go`,
  `findFiles`).
- `go-load --skip-binlog` runs `SET SQL_LOG_BIN=0` on every load connection
  (`internal/load/importer.go`, `doLoadFile`) — loads without replicating
  downstream or touching `gtid_executed`. Aborts (rather than degrading) if the
  privilege is missing, with a Cloud SQL/RDS hint on error 1227.
- `go-load --set-gtid-purged` applies the dump's GTID set to the target after
  the load, with errant-transaction and running-replica guards
  (`internal/load/gtid.go`) — see item 1 below.
- `CHANGE REPLICATION SOURCE TO` + `START REPLICA` can run three ways:
  automatically (`go-load --start-replication`, item 3 below), from printed
  SQL reviewed by the operator (`go-load --show-replication`), or from the
  dump's filled-in template (`change-replication-source.sql`).

## Gaps / planned features

### 1. `go-load --set-gtid-purged` — ✅ DONE (2026-07-03)

Implemented in `internal/load/gtid.go` (`ApplyGTIDPurged`), wired to run after
a successful directory load (and after `--verify`). Behaviour:

- Reads `gtid_set` from `metadata.json`; fails before loading anything if the
  dump has no GTID set. The set is whitespace-stripped and format-validated
  before being embedded in SQL.
- Requires `gtid_mode=ON`; points file/position users at
  `change-replication-source.sql` otherwise.
- Refuses on a **running** replication channel always; on a configured-stopped
  channel unless `--force` (version-gated `SHOW REPLICA STATUS` /
  `SHOW SLAVE STATUS`).
- Errant gate: if `GTID_SUBTRACT(gtid_executed, dump_set)` is non-empty,
  refuses **even with `--force`** and points at go-gtids. Remediation stays
  out of go-load by design.
- Empty `gtid_executed` (the `--skip-binlog` path): sets `gtid_purged`
  directly, no reset, no `--force`. Non-empty subset: requires `--force`, then
  version-gated `RESET MASTER` / `RESET BINARY LOGS AND GTIDS` (8.4+).
- Verifies afterwards that `gtid_executed` equals the dump set exactly.
- Unit-tested against a scripted fake driver (11 tests: refusal paths, version
  gating, injection attempts); matrix-tested on 5.7.44/8.0.45/8.4.10/9.7.1
  including the errant-refusal and reset→seed success paths.

Not implemented (still open): `--set-gtid-purged=append` (leading-`+` subset
append, 8.0+) for partial-seed scenarios.

### 2. `go-load --skip-binlog` — ✅ DONE (2026-07-03)

Implemented: `SET SQL_LOG_BIN=0` is applied in `doLoadFile` alongside the other
per-connection session settings, which covers every connection the load uses
(each file gets a dedicated `db.Conn`). Fails hard on error — a
partially-logged load is worse than an aborted one — and error 1227 gets a
Cloud SQL/RDS-specific message.

Validated 2026-07-03 against a live MySQL 8.0.43 primary→replica pair
(`gtid_mode=ON`): load with the flag left the replica untouched and
`gtid_executed` byte-identical; the same load without the flag replicated all
rows downstream. Post-test errant-GTID check via go-gtids: clean.

Remaining interaction to honour in item 1: with `--skip-binlog` the load adds
nothing to `gtid_executed`, so `RESET MASTER` may be skippable when the target
started empty. `--set-gtid-purged` should check `@@gtid_executed` at runtime
instead of assuming.

### 3. `go-load --start-replication` / `--show-replication` — ✅ DONE (2026-07-19)

- **Dump-time template — ✅ DONE (2026-07-03):** with `--get-master-status`,
  go-dump writes `change-replication-source.sql`
  (`taskmanager.go writeChangeSourceTemplate`): captured coordinates filled in,
  GTID auto-position as the active statements, 5.7 and file/position variants
  commented. Always plain (never gzipped); go-load's `findFiles` skips it
  during restore.
- **`go-load --show-replication` — ✅ DONE (2026-07-19):** read-only; parses
  `metadata.json` (never scrapes SQL comments) and prints ready-to-run setup
  SQL to a clean stdout: GTID auto-position runnable, file/position and 5.7
  `CHANGE MASTER` as commented variants. `--source-host/--source-port/
  --repl-user/--repl-password` (or `GOLOAD_REPL_PASSWORD`) fill in what
  metadata cannot know; placeholders otherwise. GTID set is validated by the
  same sanitizer as `--set-gtid-purged` before being embedded in SQL
  (`internal/load/replication.go`, `BuildReplicationSQL`).
- **`go-load --start-replication` — ✅ DONE (2026-07-19):** executes the setup
  after the load (and after `--verify` / `--set-gtid-purged`):
  - `--replication-mode auto|gtid|file-pos`: auto prefers GTID auto-position
    when the dump has a `gtid_set` and the target has `gtid_mode=ON`, else
    binlog file/position; explicit modes fail loudly on missing prerequisites
    (`chooseReplMode`).
  - Version-gated syntax: `CHANGE REPLICATION SOURCE TO` + `SOURCE_*` +
    `START REPLICA` on 8.0.23+; `CHANGE MASTER TO` + `MASTER_*` +
    `START SLAVE` on 5.7/pre-8.0.23; `GET_SOURCE_PUBLIC_KEY` vs
    `GET_MASTER_PUBLIC_KEY`, omitted entirely before 8.0.4
    (`buildChangeSourceSQL`).
  - Safety gates mirror `--set-gtid-purged`: running channel always aborts;
    configured-but-stopped channel requires `--force`; GTID mode requires
    `gtid_executed` to **exactly equal** the dump's set (checked with
    `GTID_SUBTRACT` in both directions — missing GTIDs would make
    AUTO_POSITION re-fetch rows the load already inserted; extra GTIDs are
    errant).
  - `--source-ssl` (`SOURCE_SSL=1`) and `--get-source-public-key` cover
    `caching_sha2_password` sources without TLS.
  - After `START REPLICA`, polls replica status (15s bound): both threads
    `Yes` → success with lag reported; `Last_IO_Error`/`Last_SQL_Error` →
    immediate hard error (bad credentials surface in seconds, not as a green
    exit); still `Connecting` at timeout → error with thread states.
  - Passwords are redacted from every logged statement.
  - Validated 2026-07-19 against 8.0.46 source → 8.4 replica: one-command
    seeding (GTID auto-position, 0s behind, new writes applied), wrong
    password surfaced `Access denied` as a hard error, `file-pos` mode came
    up with `Auto_Position: 0`.

### 4. Rename `master-data.sql` / `slave-data.sql` → `.txt`

The files are plain-text reports (`Master File: binlog.000042`), not executable
SQL. The `.sql` extension invites piping them into `mysql` (syntax error) and
forces the skip-list in `importer.go findFiles`. Renaming to `master-data.txt` /
`slave-data.txt`:

- Keep reading both names in `findFiles` skip logic for old dumps.
- Alternatively make them valid SQL comments (`-- Master File: ...`) like
  mydumper's metadata file — backwards compatible with the current extension.

### 5. MariaDB GTID support on restore

`getSlaveData` already reads `Gtid_Slave_Pos`, but seeding a MariaDB replica
needs `SET GLOBAL gtid_slave_pos = '...'` +
`CHANGE MASTER TO ... , MASTER_USE_GTID = slave_pos` — different from MySQL.
`metadata.json` currently only has the MySQL-style `gtid_set` field from
`SHOW MASTER STATUS`; MariaDB's equivalent is `gtid_binlog_pos` /
`gtid_current_pos`. Needs:

- Capture `@@gtid_binlog_pos` on MariaDB in `getMasterData`.
- A `flavor` field in `metadata.json` (`mysql` | `mariadb`) so go-load can pick
  the right restore statements.

### 6. Test strategy

- Unit: metadata round-trip with/without `gtid_set`; version gating of
  `RESET MASTER` vs `RESET BINARY LOGS AND GTIDS` statement selection.
- Integration (requires MySQL, extend `make test-integration`):
  1. Start two containers (primary with `gtid_mode=ON`, empty replica).
  2. Write rows, dump primary with `--get-master-status`.
  3. Write more rows on the primary *after* the dump.
  4. `go-load` into the replica, apply `--set-gtid-purged` + `--change-source`.
  5. Assert replica catches up and `CHECKSUM TABLE` matches on both; run
     [go-gtids](https://github.com/ChaosHour/go-gtids) source→replica and
     assert "No Errant Transactions".
  - Repeat with `gtid_mode=OFF` using file/position.
- Integration for `--skip-binlog`: load on a primary with a downstream replica,
  assert the replica did **not** receive the rows and `gtid_executed` on the
  primary is unchanged.

## Suggested implementation order

1. ~~`go-load --skip-binlog` (item 2)~~ — ✅ done 2026-07-03.
2. ~~Dump-time `change-replication-source.sql` template (item 3)~~ — ✅ done
   2026-07-03.
3. ~~`go-load --set-gtid-purged` (item 1)~~ — ✅ done 2026-07-03.
4. ~~`go-load --start-replication` + `--show-replication` (item 3, load-time
   half)~~ — ✅ done 2026-07-19.
5. File rename (item 4), MariaDB flavor support (item 5), and
   `--set-gtid-purged=append` as follow-ups.
