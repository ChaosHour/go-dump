# Scale test plan — what must pass before calling go-dump production-ready at size

Everything validated so far ran against small Docker databases (largest table:
8.5M rows / ~1GB; largest single value: ~MB-scale TEXT). This plan lists the
tests that close the gap to "trusted on real production sizes", roughly in
order of risk.

## Known scale-sensitive design points (verify, don't assume)

| Area | Design today | Risk at size |
|------|--------------|--------------|
| Chunk staging | Each keyed chunk staged in RAM before write (retry safety) | `threads × chunk bytes` of RAM; wide rows × big `--chunk-size` can OOM the dump host |
| Single-chunk tables | Streamed (not staged) since the scale-hardening branch; one unchunked query, no parallelism | A 500GB keyless table = one thread, one multi-hour query, one huge file; long-running snapshot |
| Chunk boundary discovery | One `LIMIT 1 OFFSET chunk-size` index probe per chunk, serial per table | ~20k probes for a 1B-row table at 50k chunk — measure, it may dominate |
| Snapshot lifetime | Workers hold REPEATABLE READ snapshots for the whole dump | Hours-long snapshots on a busy primary → InnoDB history-list growth, undo bloat, purge lag |
| Lock acquisition | `--lock-wait-timeout` (60s default) bounds FTWRL wait | Verify behaviour when FTWRL queues behind a long query: dump aborts cleanly, server unblocks |
| ETA / progress | Row estimates from `information_schema.TABLE_ROWS` | Estimates can be off ±50% on big InnoDB tables; cosmetic |
| Checksums | `CHECKSUM TABLE` after unlock, full table scan | Hours on TB tables; drift on a live primary makes them advisory |
| Resume | Per-table granularity; mixed-snapshot warning | Resumed dumps are NOT one snapshot — never use a resumed dump + its GTID set for seeding |

## Tier 1 — must pass (correctness and survival)

1. **100GB+ single database, mixed schema** (int PKs, UUID PK, composite PK,
   one keyless table ≥ 5GB, one table with TEXT/BLOB rows ≥ 10MB each):
   - dump completes; RSS of go-dump stays ≈ `threads × chunk bytes` (monitor
     with `ps`/`time -v`); no statement in output exceeds `--statement-size`
     by more than one row.
   - full restore onto a default-config target (64MB `max_allowed_packet`,
     no server tuning) succeeds; `go-load --verify` passes.
2. **Sparse/skewed keys**: table with AUTO_INCREMENT gaps (e.g. delete 90% of
   rows) — chunk count and runtime stay sane (OFFSET probing is by row count,
   not key value, so gaps should not matter — verify).
3. **Negative and near-BIGINT-max keys**: signed key starting below zero and
   unsigned key above 2^63 — the first must dump completely; the second must
   fail loudly (documented), not silently truncate.
4. **Kill/resume at size**: kill -9 the dump at ~50%; `--resume` finishes;
   restore + `CHECKSUM TABLE` against source matches (quiesced source).
5. **Replica seeding at size**: full walkthrough (docs/REPLICA-SEEDING-
   WALKTHROUGH.md) against ≥100GB with concurrent writes on the primary
   during the dump; after `START REPLICA`, replica catches up and
   `pt-table-checksum` (or quiesced `CHECKSUM TABLE`) matches; go-gtids clean.

## Tier 2 — should pass (production behaviour)

6. **Busy-primary lock test**: run a deliberate 5-minute `SELECT SLEEP()`-ish
   query, start the dump — FTWRL must abort at `--lock-wait-timeout` with the
   processlist hint, and the server must be unblocked immediately after.
7. **Long-snapshot impact**: dump a 4+ hour dataset on a primary taking
   steady writes; graph history list length — document the impact and the
   "dump from a replica" recommendation.
8. **Network blips**: drop the dump connection mid-run (kill it server-side)
   — dump must abort (snapshot lost, not retryable) with a clear error, and
   `--resume` must recover.
9. **Disk-full on destination** mid-dump: hard abort, no `complete` status in
   metadata.json.
10. **go-load into a 5.7→8.0 upgrade target** at size (charset/collation
    edge: `utf8mb3` sources).

## Tier 3 — nice to have

11. Compressed dump at size (`--compress`) — CPU vs size tradeoff numbers for
    the README.
12. Throughput tuning table: threads × chunk-size matrix on the test box,
    published in the README.
13. `--dry-run` cost on a 1B-row table (it runs the full chunk probing).

## Suggested rig

- Generate data with `sysbench oltp_common` (int PKs) plus custom tables for
  UUID/keyless/wide-TEXT shapes, or restore a production-shaped snapshot into
  a lab instance.
- Two VMs (or two large containers with fixed `--memory` limits to make OOM
  behaviour deterministic and observable), MySQL 8.0, `gtid_mode=ON`.
- Record for every run: wall time, go-dump RSS peak, mysqld RSS on both ends,
  history list length peak, dump size on disk, restore wall time.
