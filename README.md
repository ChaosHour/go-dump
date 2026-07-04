# go-dump

A parallel MySQL logical backup tool written in Go. Produces one SQL file per table
per thread with a consistent, point-in-time snapshot using InnoDB MVCC — similar to
[mydumper](https://github.com/mydumper/mydumper) but without the C dependency.

## Contents

- [How it works](#how-it-works)
- [MySQL version support](#mysql-version-support)
- [Use cases](#use-cases)
- [Building](#building)
- [Quick start](#quick-start)
- [All flags](#all-flags)
- [INI file configuration](#ini-file-configuration)
- [Common examples](#common-examples)
- [Output files](#output-files)
- [Restoring](#restoring)
- [Replication and GTIDs](#replication-and-gtids)
- [Limitations](#limitations)
- [Required privileges](#required-privileges)
- [Cloud SQL notes](#cloud-sql-notes)

---

## How it works

1. **Table discovery** — resolves the full table list from `--databases`, `--tables`,
   or `--all-databases`.
2. **Chunking** — for each table, queries the primary (or unique) key to split rows into
   fixed-size ranges. Tables without a usable key are handled as a single chunk or
   skipped, depending on `--tables-without-uniquekey`.
3. **Consistent snapshot** — locks tables (`LOCK TABLES … READ` or
   `FLUSH TABLES WITH READ LOCK` for `--all-databases`), opens one `REPEATABLE READ`
   transaction per worker thread, then immediately releases the lock. All workers read
   from the same MVCC snapshot for the lifetime of the dump.
4. **Parallel workers** — N goroutines consume chunks from a channel and write
   compressed or plain SQL files to the destination directory.
5. **Metadata** — writes `metadata.json` at start and updates it on completion.
   Records MySQL version, binlog position, GTID set, and per-table chunk counts.
6. **Checksums** *(optional)* — runs `CHECKSUM TABLE` for every table after the lock
   is released and writes `checksums.txt`. These can be used to verify a restore.

---

## MySQL version support

| Version | Supported | Notes |
|---------|-----------|-------|
| MySQL 5.7 | ✅ | Full support (matrix-tested on 5.7.44) |
| MySQL 8.0.x | ✅ | Uses `SHOW MASTER STATUS` / `SHOW REPLICA STATUS` (matrix-tested on 8.0.45) |
| MySQL 8.4 | ✅ | Uses `SHOW BINARY LOG STATUS` / `SHOW REPLICA STATUS` (matrix-tested on 8.4.10) |
| MySQL 9.x | ✅ | Same as 8.4 (matrix-tested on 9.7.1) |
| Google Cloud SQL MySQL 5.7 | ✅ | Direct TCP (private IP) |
| Google Cloud SQL MySQL 8.0 | ✅ | Direct TCP (private IP) |

Every version above is exercised by `make test-versions` (see [Building](#building)):
GTID capture, full restore round trip with checksum verification, and
`--skip-binlog` GTID invariance.

Authentication: `mysql_native_password` (5.7) and `caching_sha2_password` (8.0+)
are both handled automatically by the driver.

---

## Use cases

All three topologies work over a direct TCP connection (no proxy required):

- **On-prem → On-prem**: standard usage, any supported version pair.
- **On-prem → Cloud SQL**: connect to the Cloud SQL private IP directly.
- **Cloud SQL → Cloud SQL**: run go-dump on a GCE VM with access to both instances.

---

## Building

Requires Go 1.23+.

```bash
# Clone
git clone https://github.com/ChaosHour/go-dump.git
cd go-dump

# Build native binary → bin/go-dump
make build

# Cross-compile
make build-linux          # linux/amd64
make build-linux-arm64    # linux/arm64
make build-darwin         # darwin/amd64
make build-darwin-arm64   # darwin/arm64 (Apple Silicon)
make build-all            # all of the above

# Run tests (no MySQL required)
make test

# Run integration tests (requires MySQL at 127.0.0.1:3306)
make test-integration

# Multi-version matrix: dump/restore/skip-binlog against MySQL 5.7, 8.0, 8.4, 9
# Containers use ports 33057/33080/33084/33090 — safe alongside a local 3306.
make test-versions-up      # start the four containers (first run pulls images)
make test-versions         # build binaries + run test/test-versions.sh
make test-versions-down    # tear down (removes volumes)
```

The matrix (`test/docker-compose.versions.yml` + `test/test-versions.sh`)
verifies, per version: GTID capture in `metadata.json`, a full
drop-database→restore→checksum-verify round trip, `go-load --skip-binlog`
leaving `gtid_executed` byte-identical, the `change-replication-source.sql`
template, and `--set-gtid-purged` (both the errant-transaction refusal and the
reset→seed success path). MySQL 5.7 runs under amd64 emulation on Apple
Silicon (Rosetta).

The `VERSION` file controls the embedded version string. Override at build time:

```bash
make build VERSION=1.2.0
```

---

## Quick start

```bash
# Dry run — shows estimated chunk counts without touching the filesystem
go-dump \
  --mysql-host db01.example.com \
  --mysql-user backup \
  --mysql-password secret \
  --databases myapp \
  --destination /backups/myapp \
  --dry-run

# Execute the dump
go-dump \
  --mysql-host db01.example.com \
  --mysql-user backup \
  --mysql-password secret \
  --databases myapp \
  --destination /backups/myapp \
  --threads 4 \
  --chunk-size 50000 \
  --execute

# Same thing with an INI file (recommended for production)
go-dump --ini-file /etc/go-dump/primary.ini --databases myapp --destination /backups/myapp --execute
```

---

## All flags

### Mode

| Flag | Default | Description |
|------|---------|-------------|
| `--dry-run` | false | Calculate chunk counts per table and print a summary. No files written. |
| `--execute` | false | Run the dump. Mutually exclusive with `--dry-run`. |
| `--version` | false | Print version and exit. |
| `--help` | false | Print usage and exit. |

### MySQL connection

| Flag | Default | Description |
|------|---------|-------------|
| `--mysql-host` | localhost | MySQL server hostname or IP. |
| `--mysql-port` | 3306 | MySQL server port. |
| `--mysql-socket` | — | Unix socket path. Takes precedence over host/port when set. |
| `--mysql-user` | root | MySQL user. |
| `--mysql-password` | — | MySQL password. Also readable from `GODUMP_PASSWORD` env var. |
| `--ini-file` | — | Path to an INI file (see [INI file configuration](#ini-file-configuration)). |

### What to dump

| Flag | Default | Description |
|------|---------|-------------|
| `--databases` | — | Comma-separated list of databases: `mydb,reporting`. |
| `--tables` | — | Comma-separated list of `schema.table` pairs: `mydb.orders,mydb.users`. |
| `--all-databases` | false | Dump every user database. Excludes `mysql`, `sys`, `information_schema`, `performance_schema` by default. |
| `--include-system-databases` | false | With `--all-databases`, include the `mysql` schema (minus `slow_log`/`general_log`). Use for on-prem full-cluster migrations. **Do not use with Cloud SQL.** |
| `--where` | — | Filter rows. Global: `"status = 'active'"`. Per-table: `"db.tbl:expr,db.tbl2:expr"`. |

### Parallelism and chunking

| Flag | Default | Description |
|------|---------|-------------|
| `--threads` | 1 | Number of parallel worker goroutines. Match to available CPU cores and disk I/O capacity. |
| `--chunk-size` | 1000 | Rows per read chunk (key range query). Larger = fewer queries, more memory per worker. |
| `--output-chunk-size` | 0 | Rows per `INSERT` statement. 0 = same as `--chunk-size`. |
| `--statement-size` | 16MiB | Max **bytes** per `INSERT` statement, checked at row boundaries. Keeps wide TEXT/BLOB rows from producing statements larger than the target's `max_allowed_packet`. 0 disables. |
| `--channel-buffer-size` | 1000 | Depth of the chunk work queue. Rarely needs tuning. |
| `--tables-without-uniquekey` | error | What to do with tables that have no PK or unique key: `error` (abort), `single-chunk` (dump entire table in one query), `skip`. |

### Consistency

| Flag | Default | Description |
|------|---------|-------------|
| `--consistent` | true | Require a consistent (point-in-time) backup via MVCC. |
| `--lock-tables` | true | Lock tables to establish the consistent snapshot. Required when `--consistent=true`. |
| `--isolation-level` | REPEATABLE READ | Transaction isolation level. Options: `REPEATABLE READ`, `READ COMMITTED`, `READ UNCOMMITTED`, `SERIALIZABLE`. |

### Output

| Flag | Default | Description |
|------|---------|-------------|
| `--destination` | — | **Required.** Directory to write dump files. Created if it does not exist. |
| `--add-drop-table` | false | Prepend `DROP TABLE IF EXISTS` before each `CREATE TABLE`. |
| `--skip-use-database` | false | Omit `USE \`schema\`` statements from chunk files. |
| `--compress` | false | Gzip-compress output files (`.sql.gz`). |
| `--compress-level` | 1 | Compression level 1 (fastest) to 9 (smallest). |
| `--checksum` | false | Run `CHECKSUM TABLE` after the dump and write `checksums.txt`. |
| `--get-master-status` | false | Record binlog file/position and GTID set in `master-data.sql` and `metadata.json`, and write the `change-replication-source.sql` setup template. |
| `--get-slave-status` | false | Record replica status in `slave-data.sql`. |
| `--output-chunk-size` | 0 | Rows per INSERT statement (0 = same as chunk-size). |

### Logging

| Flag | Default | Description |
|------|---------|-------------|
| `--debug` | false | Enable debug-level logging. |
| `--quiet` | false | Suppress INFO messages (warnings and errors still print). |

---

## INI file configuration

The `--ini-file` flag accepts a MySQL-style INI file. This is the recommended way to
manage credentials and defaults in production — keeps passwords out of shell history
and process listings.

Supported sections: `[client]`, `[mysqldump]` (MySQL standard keys), and `[go-dump]`
(go-dump-specific keys).

```ini
[client]
user     = backup_user
password = s3cr3t#with#hashes   # '#' in passwords is handled correctly
host     = db01.example.com
port     = 3306

[go-dump]
threads             = 8
chunk-size          = 50000
output-chunk-size   = 5000
destination         = /backups/mysql
compress            = true
compress-level      = 1
add-drop-table      = true
get-master-status   = true
checksum            = true
tables-without-uniquekey = single-chunk
consistent          = true
isolation-level     = REPEATABLE READ
```

Command-line flags override INI values. The `GODUMP_PASSWORD` environment variable is
used as a last resort if `--mysql-password` is not set and the INI file has no password.

### MySQL defaults-group-suffix

Use multiple INI sections to target different servers:

```ini
# ~/.my.cnf
[client_primary1]
user     = backup
password = primary_password
host     = primary1.db.internal
port     = 3306

[client_replica1]
user     = backup
password = replica_password
host     = replica1.db.internal
port     = 3306
```

```bash
go-dump --ini-file ~/.my.cnf --databases myapp --destination /backups/myapp --execute
# (go-dump reads [go-dump] section; [client] / [mysqldump] sections for credentials)
```

---

## Common examples

### Dump a single database

```bash
go-dump \
  --mysql-host db01.example.com \
  --mysql-user backup \
  --mysql-password secret \
  --databases myapp \
  --destination /backups/myapp \
  --threads 4 \
  --chunk-size 50000 \
  --get-master-status \
  --execute
```

### Dump multiple databases

```bash
go-dump \
  --ini-file /etc/go-dump/prod.ini \
  --databases "myapp,reporting,analytics" \
  --destination /backups/prod \
  --threads 8 \
  --chunk-size 100000 \
  --compress \
  --add-drop-table \
  --execute
```

### Dump specific tables

```bash
go-dump \
  --ini-file /etc/go-dump/prod.ini \
  --tables "myapp.orders,myapp.order_items,myapp.customers" \
  --destination /backups/orders \
  --threads 4 \
  --execute
```

### Dry run to estimate chunk counts

```bash
go-dump \
  --ini-file /etc/go-dump/prod.ini \
  --databases myapp \
  --destination /tmp/unused \
  --threads 4 \
  --chunk-size 50000 \
  --dry-run
```

Output:
```
2026-06-10 14:00:01 INFO Table: myapp.orders Engine: InnoDB Estimated Chunks: 1200
2026-06-10 14:00:01 INFO Table: myapp.customers Engine: InnoDB Estimated Chunks: 80
2026-06-10 14:00:01 INFO Table: myapp.products Engine: InnoDB Estimated Chunks: 12
   1200 -> `myapp`.`orders`
     80 -> `myapp`.`customers`
     12 -> `myapp`.`products`
```

### Dump all user databases (on-prem)

```bash
go-dump \
  --ini-file /etc/go-dump/prod.ini \
  --all-databases \
  --destination /backups/full \
  --threads 8 \
  --chunk-size 100000 \
  --get-master-status \
  --add-drop-table \
  --checksum \
  --execute
```

### Dump all databases including mysql schema (account migration)

```bash
go-dump \
  --ini-file /etc/go-dump/source.ini \
  --all-databases \
  --include-system-databases \
  --destination /backups/full-cluster \
  --threads 4 \
  --execute
```

> **Note:** Do not use `--include-system-databases` with Cloud SQL. Cloud SQL manages
> its own `mysql.*` permission tables via IAM.

### Dump with checksums for restore verification

```bash
go-dump \
  --ini-file /etc/go-dump/prod.ini \
  --databases myapp \
  --destination /backups/myapp \
  --threads 8 \
  --chunk-size 50000 \
  --get-master-status \
  --checksum \
  --execute
```

After restore, verify:
```bash
# (checksums.txt is in the dump directory — go-load or a custom script
#  can run CHECKSUM TABLE on the target and compare)
cat /backups/myapp/checksums.txt
```

### Dump with row filters (WHERE conditions)

```bash
# Global filter — applies to every table
go-dump \
  --ini-file /etc/go-dump/prod.ini \
  --databases myapp \
  --destination /backups/active-only \
  --where "status = 'active' AND created_at >= '2025-01-01'" \
  --execute

# Per-table filters — different condition per table
go-dump \
  --ini-file /etc/go-dump/prod.ini \
  --databases myapp \
  --destination /backups/filtered \
  --where "myapp.orders:total > 100.00,myapp.customers:country = 'US'" \
  --execute
```

### Compressed dump

```bash
go-dump \
  --ini-file /etc/go-dump/prod.ini \
  --databases myapp \
  --destination /backups/myapp \
  --threads 8 \
  --compress \
  --compress-level 1 \
  --execute
# Output files: myapp.orders-thread0.sql.gz, myapp.orders-definition.sql.gz, etc.
```

### Large table — tune chunk size

For an 8M-row table dumped with 4 threads:

```bash
go-dump \
  --ini-file /etc/go-dump/prod.ini \
  --tables "analytics.events" \
  --destination /backups/events \
  --threads 4 \
  --chunk-size 100000 \
  --output-chunk-size 10000 \
  --tables-without-uniquekey single-chunk \
  --execute
```

Progress output (every 5 seconds):
```
2026-06-10 14:02:15 INFO Progress: 450/1700 (26.5%) | Rate: 14.2 chunks/s | ETA: ~1m28s
2026-06-10 14:02:20 INFO Progress: 521/1700 (30.6%) | Rate: 14.1 chunks/s | ETA: ~1m22s
```

### Cloud SQL (on-prem → Cloud SQL via private IP)

```bash
go-dump \
  --mysql-host 10.80.0.3 \
  --mysql-user backup \
  --mysql-password secret \
  --databases myapp \
  --destination /backups/cloudsql \
  --threads 4 \
  --chunk-size 50000 \
  --get-master-status \
  --execute
```

---

## Output files

A dump of `myapp.orders` with 4 threads produces:

```
/backups/myapp/
├── metadata.json                          # dump metadata (MySQL version, binlog, table status)
├── master-data.sql                        # binlog position / GTID set (--get-master-status)
├── change-replication-source.sql          # ready-to-edit replica setup script (--get-master-status)
├── slave-data.sql                         # replica status (--get-slave-status)
├── checksums.txt                          # CHECKSUM TABLE results (--checksum)
├── myapp-schema-create.sql                # CREATE DATABASE IF NOT EXISTS (one per schema)
├── myapp.orders-definition.sql            # CREATE TABLE statement
├── myapp.orders-thread0.sql               # rows assigned to worker 0
├── myapp.orders-thread1.sql               # rows assigned to worker 1
├── myapp.orders-thread2.sql               # rows assigned to worker 2
├── myapp.orders-thread3.sql               # rows assigned to worker 3
├── myapp.customers-definition.sql
└── myapp.customers.sql                    # tables without a PK produce a single file
```

### metadata.json

Written at dump start (`status: in_progress`) and updated on clean finish
(`status: complete`). Used by resume logic and restore verification.

```json
{
  "go_dump_version": "1.0.0",
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
      "schema": "myapp",
      "name": "orders",
      "row_estimate": 1500000,
      "chunks": 170,
      "status": "done",
      "checksum": 3829201847
    }
  ]
}
```

### checksums.txt

```
# go-dump checksum file — generated 2026-06-10T14:05:30Z
# Format: schema.table  checksum  timestamp
myapp.orders      3829201847    2026-06-10T14:05:31Z
myapp.customers   918273645     2026-06-10T14:05:32Z
```

> **Caveat:** `CHECKSUM TABLE` runs *after* the locks are released and reads the
> current table state. The recorded values only match the dumped data if no
> writes occurred between the snapshot and the checksum. Use `--checksum` /
> `go-load --verify` on quiesced sources (maintenance windows, stopped
> replicas) — on a live primary, a mismatch does not necessarily mean the dump
> is bad.

---

## Restoring

go-load (in this repo) loads files in the right order automatically:
database creation (`*-schema-create.sql`), then table definitions, then data
files in parallel — so a dump restores onto a server where the database does
not exist yet.

```bash
go-load --host target-host --user root --password ... --directory /backups/myapp --workers 4 --verify
```

With standard MySQL tooling instead:

```bash
# Create the database and schema first
mysql -h target-host -u root -p < /backups/myapp/myapp-schema-create.sql
mysql -h target-host -u root -p myapp < /backups/myapp/myapp.orders-definition.sql

# Restore data files (parallel with xargs or a shell loop)
ls /backups/myapp/myapp.orders-thread*.sql | xargs -P4 -I{} mysql -h target-host -u root -p myapp < {}

# Or restore compressed files
ls /backups/myapp/*.sql.gz | xargs -P4 -I{} sh -c 'zcat {} | mysql -h target-host -u root -p myapp'
```

For point-in-time recovery, apply binary logs from the position recorded in
`master-data.sql` (or `metadata.json` `binlog_file`/`binlog_position`).

---

## Replication and GTIDs

go-dump captures the executed GTID set (and binlog file/position) **inside the lock
window**, at the exact instant the worker snapshots are opened — so the recorded
coordinates match the dumped data. This is what makes a dump usable for seeding a
replica.

Two distinct workflows:

| Goal | How |
|------|-----|
| Seed a new replica and start replication | Dump **with** `--get-master-status`, restore with `--skip-binlog --set-gtid-purged`, run the generated `change-replication-source.sql` |
| Repopulate tables/databases without touching replication | Dump **without** `--get-master-status` (the default) and just restore |

A complete real-world transcript of re-seeding a live replica — including the
failures (OOM, oversized packets) and the schema-objects companion steps — is
in [docs/REPLICA-SEEDING-WALKTHROUGH.md](docs/REPLICA-SEEDING-WALKTHROUGH.md).

### Dumping with GTIDs (replica seeding)

```bash
# On (or against) the primary:
go-dump \
  --ini-file /etc/go-dump/primary.ini \
  --databases myapp \
  --destination /backups/seed \
  --threads 8 \
  --get-master-status \
  --checksum \
  --execute
```

This records in `/backups/seed/metadata.json`:

```json
"binlog_file": "binlog.000042",
"binlog_position": 1421,
"gtid_set": "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-123"
```

and the same values in human-readable form in `master-data.sql` (a text report,
not executable SQL — go-load skips it automatically).

The dump also writes **`change-replication-source.sql`** — a ready-to-edit
script with the captured coordinates already filled in (GTID auto-position,
5.7 syntax, and file/position variants). go-load never executes it; it is for
you.

Restore onto the new replica, then configure replication:

```bash
# 1. Load the data and hand over the GTID state in one step:
#    --skip-binlog     keeps the load out of the replica's own GTID history
#    --set-gtid-purged sets gtid_purged to the dump's snapshot GTID set
#                      (from metadata.json), with safety checks — it refuses
#                      on running replicas and on errant transactions.
go-load --host replica1 --user root --password ... \
  --directory /backups/seed --workers 8 --verify \
  --skip-binlog --set-gtid-purged
```

```sql
-- 2. Point the replica at the primary: edit SOURCE_USER / SOURCE_PASSWORD in
--    the dump's change-replication-source.sql and run it, or by hand:
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST = 'primary1.db.internal',
  SOURCE_PORT = 3306,
  SOURCE_USER = 'repl',
  SOURCE_PASSWORD = '...',
  SOURCE_AUTO_POSITION = 1;         -- MySQL 8.0.23+; use CHANGE MASTER TO / MASTER_AUTO_POSITION=1 on 5.7
START REPLICA;                      -- START SLAVE on 5.7

-- 3. Verify
SHOW REPLICA STATUS\G               -- Replica_IO_Running / Replica_SQL_Running: Yes
```

Then prove the seed is clean with go-gtids — see the step-through guide below.

**What `--set-gtid-purged` does, and its guard rails:**

- Reads `gtid_set` from the dump's `metadata.json` (fails up front if the dump
  was taken without `--get-master-status`).
- Requires `gtid_mode=ON` on the target.
- **Refuses** if the target has a *running* replication channel (always), or a
  configured-but-stopped one (unless `--force`).
- If the target's `gtid_executed` is empty (the `--skip-binlog` path, or a
  fresh instance): just sets `gtid_purged`. No reset, no `--force` needed.
- If `gtid_executed` is non-empty but a subset of the dump's set (e.g. the
  load itself was binlogged): requires `--force`, then runs `RESET MASTER`
  (8.4+/9: `RESET BINARY LOGS AND GTIDS`) before setting `gtid_purged` —
  destructive to the target's own binlog history, hence the flag.
- If the target has transactions **beyond** the dump's set: refuses, even with
  `--force` — that's errant-transaction territory; inspect with go-gtids.
- Verifies afterwards that `gtid_executed` equals the dump's set exactly.

### Verifying a replica with go-gtids (errant transaction check)

[go-gtids](https://github.com/ChaosHour/go-gtids) is a companion tool that
compares `gtid_executed` between two servers and reports **errant
transactions** — GTIDs the replica has that its primary does not.

**Why it matters:** an errant transaction means someone (or something) wrote
directly to the replica. The replica's data has silently diverged from the
primary, and the damage surfaces later at the worst time: if that replica is
promoted during a failover, other replicas request its errant GTIDs from their
new source, and either fail with error 1236 (`master has purged binary logs`)
or — worse — apply changes the rest of the topology never had. Catching errant
GTIDs while the topology is healthy is cheap; catching them mid-failover is an
outage.

**When to run it:**

- **After seeding a replica** with go-dump/go-load (step 4 of the recipe
  above) — proves the seed + `gtid_purged` handoff left nothing extra behind.
- **Before any planned failover or promotion** — a clean check means
  auto-positioning will work on every replica.
- **Periodically / after incidents** — e.g. after someone had a root shell on
  a replica, or after a load was run against the "wrong" host.
- **After `go-load` into a live topology** — confirm the load landed where you
  intended and nothing leaked onto a replica.

**Step-through:**

```bash
# 1. Install (or download a release binary from the repo)
go install github.com/ChaosHour/go-gtids/cmd/go-gtids@latest

# 2. Credentials: go-gtids reads user/password from ~/.my.cnf
cat ~/.my.cnf
# [client]
# user=root
# password=s3cr3t

# 3. Run it: -s is the primary (source), -t the replica (target)
go-gtids -s primary1.db.internal -source-port 3306 -t replica1 -target-port 3306
```

Healthy output — each server's identity and GTID history, then the verdict:

```
[+] Source -> primary1 gtid_executed: 2ac8ec13-...:1-40593, ...
[+] server_uuid: 2ac8ec13-...
[+] Target -> replica1 gtid_executed: 2ac8ec13-...:1-40593, ...
[+] server_uuid: 2af7e535-...
[+] No Errant Transactions:
```

```bash
# 4. If errant GTIDs ARE reported, remediate with the fix flags:
#    -fix          inject empty transactions for the errant GTIDs on the SOURCE,
#                  so the replica's history becomes a subset again (most common;
#                  keeps auto-position failover working; the divergent DATA on
#                  the replica still needs to be reconciled by you)
#    -fix-replica  apply to the replica instead
go-gtids -s primary1.db.internal -t replica1 -fix
```

> **Reading the output:** compare each server's `server_uuid` against the UUIDs
> in the GTID sets. A range under the *replica's own* `server_uuid` that the
> primary lacks = direct writes on the replica. The fix flags repair the **GTID
> bookkeeping** (empty transactions make the sets consistent) — they do not and
> cannot un-write the divergent rows. For data reconciliation, re-seed the
> replica with go-dump/go-load, or use pt-table-checksum/pt-table-sync.

Equivalent manual check, if you only have a mysql client:

```sql
-- On the replica: anything the replica executed that the primary didn't?
SELECT GTID_SUBTRACT(@@global.gtid_executed, '<primary gtid_executed>');
-- '' (empty) = clean; anything else = errant GTIDs
```

To prevent errant transactions in the first place, set
`super_read_only = ON` on replicas (blocks even SUPER users; the replication
applier is exempt), and use `go-load --skip-binlog` when a replica must be
written to deliberately (it keeps the write out of the GTID history — though
the data divergence is then yours to manage).

For non-GTID (file/position) replication, use `binlog_file` / `binlog_position`
from `metadata.json` instead:

```sql
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST = 'primary1.db.internal',
  SOURCE_LOG_FILE = 'binlog.000042',
  SOURCE_LOG_POS = 1421, ...;
```

> **Operational notes:**
> - `RESET MASTER` is destructive on the replica (wipes its binlogs and GTID
>   history). Only run it on the freshly-seeded replica, never on the primary.
> - `SET GLOBAL gtid_purged` requires `SUPER` / `SYSTEM_VARIABLES_ADMIN`, which
>   managed services (Cloud SQL, RDS) do not grant — use the provider's external
>   replication API there instead.
> - These post-restore steps are manual today. Automating them in go-load is
>   planned — see [docs/GTID-REPLICATION.md](docs/GTID-REPLICATION.md).

### Dumping without GTIDs (repopulating tables, replication-safe)

This is the **default behaviour** — GTID/binlog capture is opt-in:

```bash
# No --get-master-status: no GTID or binlog state is recorded anywhere.
go-dump \
  --ini-file /etc/go-dump/prod.ini \
  --tables "myapp.orders,myapp.order_items" \
  --destination /backups/orders \
  --threads 4 \
  --add-drop-table \
  --execute

# Restore into an existing server without touching its replication state:
go-load --host target-host --user app_admin --password ... \
  --directory /backups/orders --workers 4
```

This is safe for replication because, unlike `mysqldump` (which embeds
`SET @@GLOBAL.GTID_PURGED` by default when `gtid_mode=ON`), go-dump data files
contain **only** `USE`, `SET NAMES`, `FOREIGN_KEY_CHECKS=0`, and `INSERT`
statements. Restoring a go-dump can never overwrite the target's GTID state,
stop its replication threads, or change its replication configuration —
regardless of whether the dump was taken with `--get-master-status`.

Two things to be aware of when loading into a live topology:

- The load's writes go through the target's binlog like any other client
  traffic. If the target is a **primary**, the restored rows replicate to its
  replicas normally (usually what you want when repopulating a table). To load
  **without** replicating downstream, use `go-load --skip-binlog` (below).
- If the target is a **replica**, writing to it directly will make it diverge
  from its primary (errant GTIDs). That is true of any client write, not
  specific to go-dump — `--skip-binlog` avoids the errant GTIDs, but the data
  divergence remains yours to manage.

### Loading without writing the binlog (`--skip-binlog`)

`go-load --skip-binlog` runs `SET SQL_LOG_BIN=0` on **every** connection the
load uses (session-scoped, so it is applied per-connection, schema and data
alike). The restored rows are written to the target only: nothing goes to its
binlog, nothing replicates to its replicas, and nothing is added to its
`gtid_executed`.

```bash
# Repopulate a table on a primary WITHOUT pushing the load to its replicas:
go-load --host primary1 --user admin --password ... \
  --directory /backups/orders --workers 4 --skip-binlog
```

Notes:

- Requires `SUPER` or `SYSTEM_VARIABLES_ADMIN` on the target. Cloud SQL and RDS
  do not grant these — go-load fails with a clear error there; drop the flag.
- If the connection cannot disable binary logging, the load **aborts** rather
  than continuing with a partially-logged restore.
- When seeding a replica, loading with `--skip-binlog` keeps `gtid_executed`
  empty on the fresh instance, which means `SET GLOBAL gtid_purged` works
  without the destructive `RESET MASTER` step.

---

## Limitations

Know these before relying on a dump for replica seeding or disaster recovery:

- **Base tables only.** go-dump discovers `TABLE_TYPE = 'BASE TABLE'` — it does
  **not** dump views, triggers, stored procedures/functions, or events. If your
  schema uses them, capture them separately and load them after the data:

  ```bash
  mysqldump --no-data --no-create-info --routines --events --triggers \
    -h source-host -u backup -p myapp > myapp-objects.sql
  ```

  A replica seeded without its views/routines will error on reads that use
  them, even though replication itself runs fine.
- **Users and grants are not dumped** unless `--all-databases
  --include-system-databases` is used (raw `mysql.*` tables, on-prem only —
  never against Cloud SQL/RDS). For portable account DDL, use
  `pt-show-grants` or `mysqlpump --users` separately.
- **Character sets:** dump files carry raw column bytes and are loaded on
  default-charset (utf8mb4) connections. This round-trips utf8mb4/ascii/binary
  columns correctly; schemas storing non-UTF-8 bytes in text columns (e.g.
  latin1 with high-bit characters) should be test-restored and checksum-verified
  before you rely on the dump.
- **Non-InnoDB tables** are only write-protected during the brief lock window;
  concurrent writes after the unlock can appear in their dump files (warned at
  runtime).
- **`CHECKSUM TABLE` runs after the locks are released** — on a live primary a
  checksum mismatch does not necessarily mean a bad dump (see the caveat in
  [Output files](#output-files)).

---

## Required privileges

```sql
-- Minimum privileges for the backup user
GRANT SELECT, RELOAD, LOCK TABLES, REPLICATION CLIENT ON *.* TO 'backup'@'%';

-- If using --get-slave-status
GRANT SELECT, RELOAD, LOCK TABLES, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'backup'@'%';
```

| Privilege | Required for |
|-----------|-------------|
| `SELECT` | Reading table data |
| `RELOAD` | `FLUSH TABLES WITH READ LOCK` (used with `--all-databases`) |
| `LOCK TABLES` | `LOCK TABLES … READ` (used with `--databases` / `--tables`) |
| `REPLICATION CLIENT` | `SHOW MASTER STATUS` / `SHOW BINARY LOG STATUS` / `SHOW REPLICA STATUS` |
| `REPLICATION SLAVE` | `SHOW REPLICA STATUS` when `--get-slave-status` |

`SUPER` is **not** required. go-dump is compatible with Cloud SQL, RDS, and other
managed MySQL services that restrict super-user access.

---

## Cloud SQL notes

- Connect directly to the Cloud SQL private IP — no Auth Proxy required.
- The Cloud SQL root user has `RELOAD` and `REPLICATION CLIENT` — all required
  privileges are available.
- **Do not use `--include-system-databases`** with Cloud SQL. The `mysql.*` tables
  are managed by Cloud SQL's IAM layer; importing them into another instance will
  corrupt permissions.
- `--all-databases` safely excludes `mysql`, `sys`, `information_schema`, and
  `performance_schema` by default.

---

## License

[Apache 2.0](LICENSE)
