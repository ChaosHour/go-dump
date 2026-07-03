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
- Only `CHANGE REPLICATION SOURCE TO` + `START REPLICA` remain manual, and the
  dump ships a filled-in template (`change-replication-source.sql`) for them.

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

### 3. `go-load --change-source` / emit a helper script at dump time

- **Dump-time template — ✅ DONE (2026-07-03):** with `--get-master-status`,
  go-dump writes `change-replication-source.sql`
  (`taskmanager.go writeChangeSourceTemplate`): captured coordinates filled in,
  GTID auto-position as the active statements, 5.7 and file/position variants
  commented. Always plain (never gzipped); go-load's `findFiles` skips it
  during restore.
- **Load-time `--change-source` — still open:** `go-load --change-source
  --source-host ... --source-user ... --source-password-env ...` runs
  `CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1` + `START REPLICA`
  after `--set-gtid-purged`, then polls `SHOW REPLICA STATUS` until the
  threads run or a timeout. Version-gate the syntax (`CHANGE MASTER TO` +
  `MASTER_AUTO_POSITION`, `START SLAVE` on 5.7).

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
4. `go-load --change-source` (item 3, load-time half) — next up.
5. File rename (item 4), MariaDB flavor support (item 5), and
   `--set-gtid-purged=append` as follow-ups.
