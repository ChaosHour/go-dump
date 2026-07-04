# TODO: Dump events, stored procedures/functions, and triggers

Goal: `go-dump` can optionally dump triggers, stored procedures, functions, and
events alongside table schemas and data, and `go-load` applies them in the
correct order. Follows the mydumper file-naming convention so output stays
recognizable.

Overall difficulty: **low-to-moderate**. Each object type is a
`SHOW CREATE ...` (or `information_schema` query) captured on the same
connection that already holds the consistent snapshot, written through the
existing buffer/compress pipeline. The subtle parts are definer handling,
apply ordering on load, and privilege errors.

## Design decisions (settle first)

- [ ] Flag shape: separate `--triggers`, `--routines`, `--events` bools
      (mydumper style) vs. one `--objects=triggers,routines,events` list.
      Leaning separate bools + keep all off by default (no behavior change).
- [ ] Definer policy: add `--skip-definer` to strip `DEFINER=...` clauses so
      dumps load on servers where the definer user doesn't exist (very common
      restore failure). Decide default: keep definers (faithful) vs. strip.
- [ ] File naming (proposed, mydumper-compatible):
      - `<schema>.<table>-triggers.sql` (triggers grouped per table)
      - `<schema>-routines.sql` (procedures + functions, one file per schema)
      - `<schema>-events.sql` (one file per schema)
- [ ] Whether `--all-databases` implicitly includes these objects when the
      flags are set, and how `--tables` filtering interacts with triggers
      (trigger belongs to a table → dump only triggers of selected tables;
      routines/events are schema-level → dump when the schema is in scope).

## Dump side (`internal/dump`)

- [ ] `options.go`: add `DumpTriggers`, `DumpRoutines`, `DumpEvents`,
      `SkipDefiner` to `DumpOptions`; defaults `false` in `GetDumpOptions()`.
- [ ] `cmd/go-dump/main.go`: wire the new flags, help text, and validation.
- [ ] `mysql.go`: discovery queries:
      - Triggers: `SELECT TRIGGER_NAME FROM information_schema.TRIGGERS
        WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?` then
        `SHOW CREATE TRIGGER` per trigger.
      - Routines: `SELECT ROUTINE_NAME, ROUTINE_TYPE FROM
        information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ?` then
        `SHOW CREATE PROCEDURE` / `SHOW CREATE FUNCTION`.
      - Events: `SELECT EVENT_NAME FROM information_schema.EVENTS
        WHERE EVENT_SCHEMA = ?` then `SHOW CREATE EVENT`.
- [ ] Run all object reads on the consistency connection (same place table
      schemas are captured, before the chunk workers start), so definitions
      match the data snapshot under `--consistent`.
- [ ] Capture and emit `sql_mode` / charset context: `SHOW CREATE TRIGGER`
      returns the trigger's `sql_mode` and charset columns — write
      `SET @saved_sql_mode…` style guards around each definition (mysqldump
      does this; skipping it causes subtle restore breakage).
- [ ] Wrap trigger/routine/event bodies in `DELIMITER ;;` … `DELIMITER ;`
      blocks in the output files (bodies contain `;`).
- [ ] Implement `--skip-definer` stripping (regex on the CREATE statement is
      what mydumper does; handle both `DEFINER=`user`@`host`` and quoted
      variants).
- [ ] Write through the existing buffer pipeline so `--compress` and
      `--dry-run` behave the same as for schema files
      (see `WriteSchemaCreateSQL` in `taskmanager.go` as the template).
- [ ] Privilege handling: `SHOW CREATE ...` on routines needs `SELECT` on
      `mysql.proc` or `SHOW ROUTINE`/definer rights depending on version;
      events need `EVENT` privilege. Fail with a clear per-object error and a
      hint, not a mid-dump panic. Decide: hard fail vs. warn-and-skip
      (mydumper warns; propose warn + non-zero note in metadata).
- [ ] `metadata.go`: record which object types were dumped (and any skipped
      due to privileges) so a resume/load knows what to expect.
- [ ] `resume.go`: object files are small — simplest correct behavior is to
      re-dump them on resume; make sure resume doesn't choke on the new files.

## Load side (`internal/load`)

- [ ] `importer.go`: recognize the new file patterns and order phases:
      1. `*-schema-create.sql` (exists today)
      2. table definitions (exists today)
      3. data (exists today)
      4. `*-routines.sql`, `*-events.sql` (schema-level, after schema create;
         safe after data)
      5. `*.<table>-triggers.sql` **after that table's data is loaded** —
         loading triggers before data would fire them during import.
- [ ] `DELIMITER` is a client-side construct — the loader must parse the
      `;;`-delimited statements itself (or write files without DELIMITER and
      split on a marker comment). Decide format with the dump side.
- [ ] Events: decide whether to load with `SET GLOBAL event_scheduler` note
      or leave scheduler state alone (leave alone; document it).
- [ ] Loading into a replica-seeding target: routines/triggers/events execute
      under the session `sql_log_bin=0` path already used for data — confirm
      the skip-binlog path covers these files too (`skipbinlog_test.go`).

## Tests

- [ ] Unit: definer-stripping regex table test (quoted/unquoted, weird hosts).
- [ ] Unit: file-pattern ordering in the importer (trigger file sorts after
      its table's data).
- [ ] Integration (existing docker test harness in `test/`): create a proc,
      function, trigger, and event in the seed schema; dump; load into a
      clean instance; assert all four exist and the trigger did not fire
      during load (row counts match source).
- [ ] Privilege test: dump as a user without `EVENT` privilege → warn path.

## Docs

- [ ] README: new flags with examples, file-naming table, definer caveat,
      load-order explanation.
- [ ] `docs/` replica-seeding walkthrough: note that triggers are applied
      after data and binlog-skipped during seed.

## Explicitly out of scope (this pass)

- Views (worth doing later — they have their own ordering problem:
  view-of-view dependencies; mydumper does two-pass "temp table then view").
- `--no-data` schema-only mode changes beyond what falls out naturally.
- Grants/users (`SHOW GRANTS` dumping is a separate feature).
