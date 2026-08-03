# TODO: `--insert-mode=upsert` (ON DUPLICATE KEY UPDATE)

Follow-up to `--insert-mode` (`insert` / `replace` / `insert-ignore`, added
2026-08-02, `internal/dump/datachunk.go` `insertVerb()`). Not started.

## Why

`replace` is a delete+insert under the hood: it re-triggers `AUTO_INCREMENT`
churn and fires delete+insert triggers instead of update triggers. For
syncing rows into a table that already has data (e.g. reseeding/refreshing a
replica table without dropping it), `INSERT ... ON DUPLICATE KEY UPDATE` is
usually the better upsert primitive — real update semantics, single
statement, no phantom delete.

## Scope

Unlike `replace`/`insert-ignore` (verb-only swap), this needs the trailing
clause built per statement, not just the prefix:

```sql
INSERT INTO `t` (`a`,`b`,`c`) VALUES (...), (...)
ON DUPLICATE KEY UPDATE `a`=VALUES(`a`), `b`=VALUES(`b`), `c`=VALUES(`c`);
```

- `internal/dump/datachunk.go` `Parse()`: needs the non-key column list (all
  columns, or all-minus-key?) to generate the `ON DUPLICATE KEY UPDATE`
  clause once per chunk, appended after the closing `;` position instead of
  a fixed suffix.
- Decide: update all columns including the key (harmless, matches mydumper
  idempotency) vs. exclude the primary/unique key column(s) (smaller
  statement). Lean toward excluding the key — it's redundant since it must
  match for the branch to fire.
- `--insert-mode=upsert` value, wired same as `replace`/`insert-ignore`
  through `options.go`, `main.go` flag validation, `ini.go`.
- go-load: still no changes expected — it executes whatever statement verb
  is in the file, same as the existing modes.
- Tests: extend `TestInsertVerb`-style coverage, plus a case verifying the
  generated `ON DUPLICATE KEY UPDATE` column list matches `colNames` minus
  the key.
- README: document alongside `--insert-mode`, with the same trigger-caveat
  treatment `replace` got (no delete+insert triggers fire; real `UPDATE`
  triggers fire instead — call this out explicitly since it's the opposite
  trigger behavior from `replace`).

## Status

- [ ] Not implemented — logged as a future request, deferred from the
      `--insert-mode` review/implementation session (2026-08-02).
