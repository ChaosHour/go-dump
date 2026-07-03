# GTID & Replication: current state, gaps, and planned features

Status of GTID handling in go-dump/go-load as of 2026-07-03, and the work needed
to make replica seeding a one-command operation.

## What works today

### Dump side — complete

- `--get-master-status` captures `SHOW MASTER STATUS` / `SHOW BINARY LOG STATUS`
  (8.4+) **inside the lock window**, after the worker consistent snapshots are
  opened and before `UNLOCK TABLES` (`internal/dump/taskmanager.go`,
  `GetTransactions`). The recorded binlog file/position and `Executed_Gtid_Set`
  therefore match the dumped data exactly — correct for replica seeding.
- Coordinates are persisted twice:
  - `metadata.json` → `binlog_file`, `binlog_position`, `gtid_set`
    (`internal/dump/metadata.go`, `SetBinlog`)
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

### Load side — partial

- `go-load` reads `metadata.json` and **logs** the source binlog/GTID
  coordinates (`cmd/go-load/main.go`).
- `master-data.*` / `slave-data.*` are correctly excluded from loadable files
  (`internal/load/importer.go`, `findFiles`).
- `go-load --skip-binlog` runs `SET SQL_LOG_BIN=0` on every load connection
  (`internal/load/importer.go`, `doLoadFile`) — loads without replicating
  downstream or touching `gtid_executed`. Aborts (rather than degrading) if the
  privilege is missing, with a Cloud SQL/RDS hint on error 1227.
- Everything after the data load (`gtid_purged`,
  `CHANGE REPLICATION SOURCE TO`, `START REPLICA`) is **manual** — documented in
  README "Replication and GTIDs".

## Gaps / planned features

### 1. `go-load --set-gtid-purged` (highest value)

Automate the GTID handoff after a seed restore.

- Read `gtid_set` from `metadata.json` (fail with a clear error if empty or the
  dump was taken without `--get-master-status`).
- Execute, in order, on a single connection:
  1. `RESET MASTER` (MySQL ≤ 8.0 / 5.7) or `RESET BINARY LOGS AND GTIDS`
     (8.4+) — version-detect the same way `taskmanager.go` does
     (`mysqlAtLeast`).
  2. `SET GLOBAL gtid_purged = '<set>'`.
- Safety requirements:
  - Refuse to run if `SHOW REPLICA STATUS` returns rows with running threads
    (target is already a replica) unless `--force`.
  - Errant-transaction gate: before touching GTID state, verify the target's
    `gtid_executed` is a subset of the dump's `gtid_set`
    (`GTID_SUBTRACT(target, dump) = ''`). This is the same check
    [go-gtids](https://github.com/ChaosHour/go-gtids) performs between two
    live servers — reuse its logic (see `pkg/gtids` in that repo). Keep the
    fix/remediation out of go-load: on failure, refuse and point the operator
    at go-gtids.
  - Refuse if the target has other databases with data beyond what was just
    loaded? At minimum warn: `RESET MASTER` destroys the target's binlog
    history.
  - Requires `SUPER`/`SYSTEM_VARIABLES_ADMIN`; detect the privilege error and
    print a Cloud SQL/RDS-specific hint (use provider external replication API).
  - MySQL 8.0+ allows *appending* to `gtid_purged` with a leading `+`; consider
    `--set-gtid-purged=append` for partial-seed scenarios.

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

### 3. `go-load --change-source` / emit a helper script at dump time

Two options (pick one, or both):

- **Load-time:** `go-load --change-source --source-host ... --source-user ...
  --source-password-env ...` runs `CHANGE REPLICATION SOURCE TO ...
  SOURCE_AUTO_POSITION=1` + `START REPLICA` after `--set-gtid-purged`.
  Version-gate the syntax (`CHANGE MASTER TO` + `MASTER_AUTO_POSITION` on 5.7,
  `START SLAVE` on 5.7).
- **Dump-time:** with `--get-master-status`, also write
  `change-replication-source.sql` containing a ready-to-edit template with the
  captured coordinates filled in (both GTID auto-position and file/position
  variants, one commented out). Zero risk, no new privileges, helps the manual
  workflow immediately. **Recommended first step.**

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
2. Dump-time `change-replication-source.sql` template (item 3b) — small, safe,
   immediately useful.
3. `go-load --set-gtid-purged` (item 1).
4. `go-load --change-source` (item 3a).
5. File rename (item 4) and MariaDB flavor support (item 5) as follow-ups.
