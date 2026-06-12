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
| MySQL 5.7 | ✅ | Full support |
| MySQL 8.0.x | ✅ | Uses `SHOW MASTER STATUS` / `SHOW REPLICA STATUS` |
| MySQL 8.4+ | ✅ | Uses `SHOW BINARY LOG STATUS` / `SHOW REPLICA STATUS` |
| Google Cloud SQL MySQL 5.7 | ✅ | Direct TCP (private IP) |
| Google Cloud SQL MySQL 8.0 | ✅ | Direct TCP (private IP) |

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
```

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
| `--get-master-status` | false | Record binlog file/position and GTID set in `master-data.sql` and `metadata.json`. |
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
