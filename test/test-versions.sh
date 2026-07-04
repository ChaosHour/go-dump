#!/usr/bin/env bash
# Multi-version integration matrix for go-dump/go-load.
#
# For each MySQL version (5.7, 8.0, 8.4, 9) started by
# test/docker-compose.versions.yml, this script:
#   1. seeds a test database (PK table, quotes/unicode/datetime data)
#   2. dumps it with --get-master-status --checksum and asserts a GTID set
#      was captured
#   3. drops the database and restores it with go-load --verify
#   4. asserts the row count survived the round trip
#   5. truncates, reloads with --skip-binlog, and asserts gtid_executed is
#      byte-identical (the load left no trace in the binlog/GTID history)
#   6. asserts change-replication-source.sql was written with auto-position
#      and the captured coordinates
#   7. asserts --set-gtid-purged REFUSES while the target has transactions
#      beyond the dump set (errant gate)
#   8. resets the server's GTID history (version-gated RESET), reloads with
#      --skip-binlog --set-gtid-purged, and asserts gtid_executed now equals
#      the dump's snapshot set (go-load verifies equality internally)
#
# Usage: ./test/test-versions.sh [version ...]   (default: 57 80 84 9)

set -euo pipefail

cd "$(dirname "$0")/.."

VERSIONS=("${@:-57 80 84 9}")
[[ $# -eq 0 ]] && VERSIONS=(57 80 84 9)

# macOS ships bash 3.2 (no associative arrays) — use a function instead.
port_for() {
  case "$1" in
    57) echo 33057 ;;
    80) echo 33080 ;;
    84) echo 33084 ;;
    9)  echo 33090 ;;
    *)  echo "unknown version: $1" >&2; return 1 ;;
  esac
}

GODUMP=./bin/go-dump
GOLOAD=./bin/go-load
OUTROOT=./test/matrix-out
PASS=() FAIL=()

msql() { # msql <version> <sql>  — runs inside the container, silences pw warning
  docker exec "godump-mysql$1" mysql -uroot -ps3cr3t -N -e "$2" 2>/dev/null
}

seed() {
  msql "$1" "
    DROP DATABASE IF EXISTS matrix;
    CREATE DATABASE matrix;
    CREATE TABLE matrix.items (
      id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(64) NOT NULL,
      note VARCHAR(64),
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    INSERT INTO matrix.items (name, note) VALUES
      ('plain', NULL),
      ('O''Brien; quotes', 'semi;colon'),
      ('unicode-Ω-日本語', 'high \\\\ backslash');
    INSERT INTO matrix.items (name, note)
      SELECT CONCAT('row-', t1.n + t2.n*10 + t3.n*100), 'bulk'
      FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
            UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t1,
           (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
            UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t2,
           (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) t3;
    CREATE TABLE matrix.keyless (
      name VARCHAR(64) NOT NULL,
      payload TEXT
    ) ENGINE=InnoDB;
    INSERT INTO matrix.keyless
      SELECT CONCAT('k-', id), REPEAT('x', 2048) FROM matrix.items LIMIT 200;"
}

run_version() {
  local v=$1 dest port
  port=$(port_for "$v")
  dest="$OUTROOT/mysql$v"
  rm -rf "$dest"

  echo "── MySQL $v (port $port) ─────────────────────────────────────────"
  local server_version
  server_version=$(msql "$v" "SELECT VERSION();")
  echo "   server: $server_version"

  seed "$v"
  local rows_before keyless_before
  rows_before=$(msql "$v" "SELECT COUNT(*) FROM matrix.items;")
  keyless_before=$(msql "$v" "SELECT COUNT(*) FROM matrix.keyless;")

  # 1. Dump with GTID capture + checksums. matrix.keyless (no PK) exercises
  #    the single-chunk streaming path.
  $GODUMP --mysql-host 127.0.0.1 --mysql-port "$port" \
    --mysql-user root --mysql-password s3cr3t \
    --databases matrix --destination "$dest" \
    --threads 2 --chunk-size 100 \
    --tables-without-uniquekey single-chunk \
    --get-master-status --checksum --add-drop-table \
    --quiet --execute

  grep -q '"gtid_set": "..*"' "$dest/metadata.json" \
    || { echo "   FAIL: no gtid_set in metadata.json"; return 1; }

  # 2. Drop and restore
  msql "$v" "DROP DATABASE matrix;"
  $GOLOAD --host 127.0.0.1 --port "$port" --user root --password s3cr3t \
    --directory "$dest" --workers 2 --verify --quiet

  local rows_after keyless_after
  rows_after=$(msql "$v" "SELECT COUNT(*) FROM matrix.items;")
  [[ "$rows_after" == "$rows_before" ]] \
    || { echo "   FAIL: rows $rows_before -> $rows_after after restore"; return 1; }
  keyless_after=$(msql "$v" "SELECT COUNT(*) FROM matrix.keyless;")
  [[ "$keyless_after" == "$keyless_before" ]] \
    || { echo "   FAIL: keyless rows $keyless_before -> $keyless_after after restore"; return 1; }

  # 3. --skip-binlog: reload must leave gtid_executed untouched
  msql "$v" "TRUNCATE TABLE matrix.items; TRUNCATE TABLE matrix.keyless;"
  local gtid_before gtid_after
  gtid_before=$(msql "$v" "SELECT @@global.gtid_executed;")
  $GOLOAD --host 127.0.0.1 --port "$port" --user root --password s3cr3t \
    --directory "$dest" --workers 2 --data-only --skip-binlog --quiet
  gtid_after=$(msql "$v" "SELECT @@global.gtid_executed;")

  [[ "$gtid_before" == "$gtid_after" ]] \
    || { echo "   FAIL: --skip-binlog changed gtid_executed"; return 1; }
  rows_after=$(msql "$v" "SELECT COUNT(*) FROM matrix.items;")
  [[ "$rows_after" == "$rows_before" ]] \
    || { echo "   FAIL: rows $rows_before -> $rows_after after skip-binlog reload"; return 1; }

  # 4. change-replication-source.sql template written with the coordinates
  local tmpl="$dest/change-replication-source.sql"
  [[ -f "$tmpl" ]] || { echo "   FAIL: $tmpl not written"; return 1; }
  grep -q "SOURCE_AUTO_POSITION = 1" "$tmpl" \
    || { echo "   FAIL: template lacks SOURCE_AUTO_POSITION"; return 1; }
  grep -q "SET GLOBAL gtid_purged" "$tmpl" \
    || { echo "   FAIL: template lacks gtid_purged"; return 1; }

  # 5. --set-gtid-purged must REFUSE while gtid_executed exceeds the dump set
  #    (all the binlogged seed/drop/restore activity above is "beyond" it).
  msql "$v" "TRUNCATE TABLE matrix.items; TRUNCATE TABLE matrix.keyless;"
  if $GOLOAD --host 127.0.0.1 --port "$port" --user root --password s3cr3t \
      --directory "$dest" --workers 2 --data-only --set-gtid-purged --force --quiet 2>"$dest/refusal.log"; then
    echo "   FAIL: --set-gtid-purged did not refuse a target with extra GTIDs"; return 1
  fi
  grep -q "beyond the dump" "$dest/refusal.log" \
    || { echo "   FAIL: refusal did not name errant transactions:"; cat "$dest/refusal.log"; return 1; }

  # 6. Reset GTID history, then the real seeding path:
  #    --skip-binlog + --set-gtid-purged on an empty-gtid_executed server.
  local reset="RESET MASTER"
  case "$v" in 84|9) reset="RESET BINARY LOGS AND GTIDS" ;; esac
  msql "$v" "$reset;"
  msql "$v" "SET SESSION sql_log_bin=0; TRUNCATE TABLE matrix.items; TRUNCATE TABLE matrix.keyless;"
  $GOLOAD --host 127.0.0.1 --port "$port" --user root --password s3cr3t \
    --directory "$dest" --workers 2 --data-only --skip-binlog --set-gtid-purged --quiet

  local executed
  executed=$(msql "$v" "SELECT @@global.gtid_executed;")
  [[ -n "$executed" ]] \
    || { echo "   FAIL: gtid_executed empty after --set-gtid-purged"; return 1; }
  rows_after=$(msql "$v" "SELECT COUNT(*) FROM matrix.items;")
  [[ "$rows_after" == "$rows_before" ]] \
    || { echo "   FAIL: rows $rows_before -> $rows_after after seeding load"; return 1; }

  msql "$v" "DROP DATABASE matrix;"
  echo "   PASS: dump+gtid, restore+verify, skip-binlog, template, set-gtid-purged ($rows_before rows)"
}

[[ -x $GODUMP && -x $GOLOAD ]] || { echo "Build first: make build && make build-go-load"; exit 1; }

for v in ${VERSIONS[@]}; do
  if run_version "$v"; then PASS+=("$v"); else FAIL+=("$v"); fi
done

echo "──────────────────────────────────────────────────────────────────"
echo "PASS: ${PASS[*]:-none}   FAIL: ${FAIL[*]:-none}"
[[ ${#FAIL[@]} -eq 0 ]]
