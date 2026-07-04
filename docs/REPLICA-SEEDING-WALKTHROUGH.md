# Walkthrough: re-seeding a live replica with go-dump / go-load (GTID auto-position)

A real, start-to-finish transcript of wiping a replica and re-seeding it from
its primary using GTIDs — run 2026-07-03 against a MySQL 8.0.43 Docker pair
(`primary1` on 3306 → `replica1` on 3307, GTID replication, channel
`source_3`, `Auto_Position=1`). Every output below is from the actual run,
including the two failures and what they taught us.

The same flow seeds a **brand-new** replica; the differences are called out in
[Fresh replica vs re-seed](#fresh-replica-vs-re-seed).

## The flags that make this work

| Flag | Tool | What it does |
|------|------|--------------|
| `--get-master-status` | go-dump | Captures binlog file/position + `Executed_Gtid_Set` **inside the lock window**, writes them to `metadata.json` and `master-data.sql`, and generates `change-replication-source.sql` |
| `--checksum` | go-dump | `CHECKSUM TABLE` per table → `checksums.txt`, verified later by `go-load --verify` |
| `--statement-size` | go-dump | Caps INSERT statement bytes (default 16MiB) so wide TEXT/BLOB rows can't exceed the target's `max_allowed_packet` |
| `--skip-binlog` | go-load | `SET SQL_LOG_BIN=0` on every load connection — the restore adds nothing to the replica's own GTID history |
| `--set-gtid-purged` | go-load | After the load, sets `gtid_purged` to the dump's snapshot GTID set, with safety gates (see step 5) |
| `--force` | go-load | Required here because the target has a configured (stopped) channel and a non-empty `gtid_executed` |

## Step 0 — Recon: know what you're about to destroy

```bash
mysql --defaults-group-suffix=_replica1 --commands=on -e "SHOW REPLICA STATUS\G" \
  | grep -E "Source_Host|Source_User|Auto_Position|Channel_Name|Running:"
```
```
                  Source_Host: 172.20.0.3
                  Source_User: repl
           Replica_IO_Running: Yes
          Replica_SQL_Running: Yes
                Auto_Position: 1
                 Channel_Name: source_3
```

Three things this told us:

- **Auto_Position=1 and a named channel** — the replica's connection config
  (including the stored `repl` password) lives in `mysql.slave_master_info`
  and **survives** `STOP REPLICA` and `RESET MASTER`. We will not need the
  replication password at all. (`RESET REPLICA ALL` would wipe it — don't.)
- **Databases to reseed**: `ca_analysis chaos go_bills roll_back sakila`.
- **Non-table objects**: 9 views, 7 routines, 6 triggers, 1 event
  (`information_schema.views/routines/triggers/events`). Routines, triggers,
  and events ride along with `--triggers --routines --events`; views need the
  extra pass in step 5.

## Step 1 — Dump the primary with GTID capture

```bash
go-dump --mysql-host 127.0.0.1 --mysql-port 3306 --mysql-user root --mysql-password ... \
  --all-databases --destination ./test-seed \
  --tables-without-uniquekey single-chunk \
  --threads 4 --chunk-size 50000 --output-chunk-size 5000 \
  --get-master-status --checksum --add-drop-table \
  --execute
```
```
INFO Locking tables to synchronise worker snapshots and binlog position.
INFO Replication setup template written → test-seed/change-replication-source.sql
INFO Unlocking the tables. Tables were locked for 20.459492ms
INFO Checksums written for 25 tables → ./test-seed/checksums.txt
INFO Dump complete.
```

The primary was **locked for ~20ms** — that's the entire production impact of
capturing coordinates that exactly match the data. `metadata.json` now holds:

```json
"binlog_file": "binlog.000056",
"gtid_set": "1d1fff5a-...:123,\n2ac8ec13-...:1-40595,\n2af7e535-...:1-11"
```

### What went wrong first (kept here on purpose)

Our first two attempts failed, and both failures are things you'll hit in real
production:

1. **Replica OOM-killed (exit 137).** The first dump used the default
   `--output-chunk-size` (= `--chunk-size` = 50,000 rows per INSERT). Four
   load workers × ~50k-row statements against a memory-constrained Docker VM
   killed mysqld mid-load. Fix: `--output-chunk-size 5000` and fewer load
   workers. Match statement size and parallelism to the *target's* memory,
   not the source's.
2. **`packet for query is too large`.** One table (`chaos.notes`, huge TEXT
   rows) produced a **single 148MB INSERT statement** — row-count grouping
   says nothing about bytes. This drove two fixes now in the code:
   go-dump's `--statement-size` (default 16MiB byte cap per INSERT — the same
   148MB of data now dumps as 7 statements) and go-load negotiating
   `max_allowed_packet` with the server instead of refusing at the driver
   default.

## Step 2 — Stop replication on the replica

```sql
STOP REPLICA;   -- config and credentials remain stored
```

`--set-gtid-purged` refuses to run against a **running** channel no matter
what, so this is not optional.

## Step 3 — Drop the user databases on the replica

```sql
SET SESSION sql_log_bin=0;
DROP DATABASE ca_analysis; DROP DATABASE chaos; DROP DATABASE go_bills;
DROP DATABASE roll_back;  DROP DATABASE sakila;
```

`sql_log_bin=0` matters: without it every DROP generates a GTID under the
*replica's* server_uuid — errant transactions you'd have to explain away
later. System schemas (`mysql`, `sys`) are untouched, so users/grants survive.

## Step 4 — Load + GTID handoff in one command

```bash
go-load --host 127.0.0.1 --port 3307 --user root --password ... \
  --directory ./test-seed --workers 2 --verify \
  --skip-binlog --set-gtid-purged --force
```

What each part did in this run:

- `--skip-binlog` — the restore wrote nothing to the replica's binlog or GTID
  history.
- `--verify` — re-ran `CHECKSUM TABLE` on the replica and compared with
  `checksums.txt` from the dump.
- `--set-gtid-purged` walked its gates: GTID set validated → `gtid_mode=ON` →
  channel exists but stopped (allowed by `--force`) → `gtid_executed`
  non-empty but a **subset** of the dump set → `--force` authorizes
  `RESET MASTER` → `SET GLOBAL gtid_purged = '<dump set>'` → read-back
  verified equal.

Why `--force` was genuinely needed twice over: the replica had a configured
channel, and its `gtid_executed` still held the old history (`RESET MASTER`
is destructive, so it is never implicit). On a **fresh** instance neither
condition exists and `--force` is unnecessary.

Had the replica contained transactions **beyond** the dump set (true errant
transactions), `--set-gtid-purged` would have refused **even with `--force`**
and pointed at [go-gtids](https://github.com/ChaosHour/go-gtids).

Actual output of step 4 in this run:

```
INFO Progress: 25/25 files loaded
INFO All checksums verified OK (./test-seed/checksums.txt)
INFO Applying the dump's GTID set to the target (--set-gtid-purged)...
WARNING Executing RESET MASTER on the target (destroys its binlog history).
INFO gtid_purged set to the dump's snapshot GTID set. Next: CHANGE REPLICATION
     SOURCE TO ... SOURCE_AUTO_POSITION=1 ..., then START REPLICA.
```

## Step 5 — Views, routines, triggers, events

This schema had 9 views, 7 routines, 6 triggers, 1 event — without them the
replica errors on any read through a view, even though replication itself
would run fine.

go-dump now carries routines, triggers, and events natively: add
`--triggers --routines --events` (and usually `--skip-definer`) to the
step-1 dump, and go-load applies them in the right order — triggers after
the data, so they don't fire during the restore — on the same
`--skip-binlog` connections, keeping the replica's GTID history clean:

```bash
go-dump ... --triggers --routines --events --skip-definer --execute
go-load ... --skip-binlog --set-gtid-purged --directory /backups/seed
```

That leaves only **views**, which still need a mysqldump pass, **loading
with binlog disabled** for the same errant-GTID reason as step 3.

Two mysqldump traps we hit doing this for real:

- **`--set-gtid-purged=OFF` is mandatory.** mysqldump's default (`AUTO`)
  embeds `SET @@GLOBAL.GTID_PURGED` in its output when `gtid_mode=ON` — which
  fails (or worse) on a replica whose GTID state you just set. This is the
  exact footgun go-dump's own files avoid by design.
- **`--no-create-info` suppresses view definitions too**, not just
  `CREATE TABLE`. Routines/triggers/events came through; views silently
  didn't. Views need their own pass, named explicitly per database.

```bash
# Routines, triggers, events (no table DDL, no data):
mysqldump -h primary -uroot -p --no-data --no-create-info \
  --routines --events --triggers --set-gtid-purged=OFF \
  --databases ca_analysis chaos go_bills roll_back sakila \
  | mysql -h replica -uroot -p --init-command="SET SESSION sql_log_bin=0"

# Views: name them explicitly (per database), write to a file, then load.
# (In zsh, use ${=views} — zsh does not word-split unquoted variables.)
for db in chaos sakila; do
  views=$(mysql -h primary -N -e \
    "SELECT table_name FROM information_schema.views WHERE table_schema='$db';" | tr '\n' ' ')
  echo "USE \`$db\`;" >> views.sql
  mysqldump -h primary -uroot -p --no-data --skip-triggers --set-gtid-purged=OFF \
    $db $views >> views.sql
done
mysql -h replica -uroot -p --init-command="SET SESSION sql_log_bin=0" < views.sql
```

Result in this run: `views: 9, routines: 7, triggers: 6, events: 1` on the
replica, and `gtid_executed` unchanged throughout (the `--init-command`
binlog-off trick verified after every load).

## Step 6 — Start replication

The channel config survived, so for a re-seed it's just:

```sql
START REPLICA;
```
```
Replica_IO_Running: Yes
Replica_SQL_Running: Yes
Seconds_Behind_Source: 0
Auto_Position: 1
```

With `Auto_Position=1`, the replica tells the primary "I have exactly the
dump's GTID set" and the primary streams everything after the snapshot.

## Step 7 — Prove it

```bash
# 1. Marker row written on the PRIMARY:
#    CREATE TABLE chaos.seed_check (...); INSERT ... 'replicated after seed';
#    → appeared on the replica within seconds.

# 2. Errant-transaction check:
go-gtids -s 127.0.0.1 -source-port 3306 -t 127.0.0.1 -target-port 3307
# [+] No Errant Transactions:

# 3. Row-count spot checks, primary vs replica:
#    sakila.rental:                     16044 = 16044  OK
#    sakila.payment:                    16049 = 16049  OK
#    ca_analysis.DailyBudgetDetail_test: 8496868 = 8496868  OK
#    chaos.notes (the 148MB-of-TEXT table): 903 = 903  OK
```

## Fresh replica vs re-seed

| | Re-seed (this run) | Fresh replica |
|---|---|---|
| `STOP REPLICA` first | Required | n/a |
| `--force` | Required (stopped channel + non-empty `gtid_executed`) | Not needed |
| `RESET MASTER` | Run by `--set-gtid-purged` under `--force` | Not needed (`gtid_executed` empty with `--skip-binlog`) |
| `CHANGE REPLICATION SOURCE TO` | Skipped — stored config survives | Edit `<repl_user>`/`<repl_password>` in the dump's `change-replication-source.sql` and run it |

## Operator checklist

- [ ] `SHOW REPLICA STATUS` — note channel name, Auto_Position, source host
- [ ] Count views/routines/triggers/events — plan the mysqldump companion step
- [ ] Dump with `--get-master-status --checksum`; sane `--output-chunk-size`
- [ ] `STOP REPLICA`
- [ ] Drop user databases with `sql_log_bin=0`
- [ ] `go-load --verify --skip-binlog --set-gtid-purged` (`--force` for re-seeds)
- [ ] Load schema objects with `sql_log_bin=0`
- [ ] `START REPLICA` (or run the edited `change-replication-source.sql`)
- [ ] Marker-row test + go-gtids clean
