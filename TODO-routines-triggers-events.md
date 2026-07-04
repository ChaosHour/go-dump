# TODO: Dump events, stored procedures/functions, and triggers

Goal: `go-dump` can optionally dump triggers, stored procedures, functions, and
events alongside table schemas and data, and `go-load` applies them in the
correct order. Follows the mydumper file-naming convention so output stays
recognizable.

Status: **implemented and verified end-to-end** (dump → drop → go-load restore
against MySQL 8.0.45, plain and compressed, with and without `--skip-definer`).
Remaining items are at the bottom.

## Design decisions (settled)

- [x] Flag shape: separate `--triggers`, `--routines`, `--events` bools
      (mydumper style), all off by default — no behavior change for existing
      users. Also available as INI keys.
- [x] Definer policy: definitions keep `DEFINER=...` by default (faithful);
      `--skip-definer` strips it for restores where the definer account does
      not exist on the target.
- [x] File naming (mydumper-compatible):
      - `<schema>.<table>-triggers.sql` (triggers grouped per table)
      - `<schema>-routines.sql` (procedures + functions, one file per schema)
      - `<schema>-events.sql` (one file per schema)
- [x] Scope: triggers follow the dumped table list (only triggers of selected
      tables); routines/events are schema-level and dump for every schema
      that has at least one table in the dump set.

## Dump side (`internal/dump`) — done

- [x] `options.go`: `DumpTriggers`, `DumpRoutines`, `DumpEvents`,
      `SkipDefiner` on `DumpOptions`, default `false`.
- [x] `cmd/go-dump/main.go`: flags wired, usage text updated.
- [x] `ini.go`: `triggers`, `routines`, `events`, `skip-definer` keys.
- [x] `objects.go` (new): discovery via `information_schema`, capture via
      `SHOW CREATE TRIGGER/PROCEDURE/FUNCTION/EVENT` with column-name-based
      scanning (version-safe), trigger `ACTION_ORDER` preserved.
- [x] mysqldump-style `sql_mode` / `character_set_client` /
      `collation_connection` save-set-restore guards around each definition;
      per-event `time_zone` guard.
- [x] Bodies wrapped in `DELIMITER ;;` … `DELIMITER ;` blocks.
- [x] `--skip-definer` regex handles backtick/single/double-quoted and
      unquoted user@host plus `CURRENT_USER[()]` (unit-tested).
- [x] Output goes through the shared buffer pipeline — `--compress` works.
- [x] `--add-drop-table` also emits versioned-comment `DROP ... IF EXISTS`
      before each object.
- [x] Privileges: discovery/`SHOW CREATE` failures warn and skip the object;
      file-write failures abort the dump (same policy as schema files).
- [x] `metadata.json`: `"objects": {"triggers": N, "routines": N, "events": N}`.
- [x] Resume: object files are rewritten only for tables still in the task
      pool; files from the prior run remain on disk (same behavior as
      `-definition.sql` files).

## Load side (`internal/load`) — done

- [x] `importer.go` phases: schema-create → definitions → data (parallel) →
      routines → events → triggers (serial, only after all data succeeded).
- [x] `parseStatements` understands client-side `DELIMITER` directives
      (case-insensitive, multi-char delimiters, partial-match restore,
      EOF without trailing newline) — object files also restore with the
      plain `mysql` client.
- [x] `--data-only` skips object files along with schema files.
- [x] `--skip-binlog` covers object files (same `loadFile` path sets
      `SQL_LOG_BIN=0` per connection).
- [x] Resume (`load-state.json`) tracks object files like any other file.

## Tests — done

- [x] Unit: definer-stripping table test (quoted/unquoted/CURRENT_USER/
      embedded backticks/inside-string non-match).
- [x] Unit: DELIMITER parsing (trigger body, `$$`, case-insensitivity,
      partial-match restore, `;;` inside strings, EOF handling, exact
      go-dump file shape).
- [x] Unit: `findFiles` ordering and post-flagging, plain and compressed.
- [x] Manual end-to-end vs MySQL 8.0.45: table + trigger + procedure +
      function + DISABLED event; dump; DROP DATABASE; go-load restore.
      Verified: trigger did **not** fire during load (audit-row canary),
      event still DISABLED, routines callable, trigger fires post-restore.

## Docs — done

- [x] README: flags table, object-dump example, output-files tree, restore
      ordering, Limitations rewritten (views still not dumped; object reads
      not MVCC-consistent).
- [x] `docs/REPLICA-SEEDING-WALKTHROUGH.md`: step 5 now uses the native
      flags; mysqldump remains only for views.

## Remaining / follow-ups

- [ ] Add trigger/routine/event coverage to `test/test-versions.sh` so the
      5.7/8.0/8.4/9 matrix exercises the object path automatically.
- [ ] Views (separate feature — needs two-pass create-or-replace ordering for
      view-on-view dependencies, like mydumper's temp-table trick).
- [ ] Grants/users (`SHOW GRANTS` dumping — separate feature).
- [ ] Optional: `--skip-definer` equivalent on the **load** side (strip while
      loading third-party dumps).
