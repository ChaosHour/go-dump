package load

import (
	"fmt"
	"strings"
)

// ReplicationInfo carries everything needed to point a replica seeded from a
// dump at the dump's source server. Coordinates come from the dump's
// metadata.json; host, port, user, and password may be overridden by flags
// when the replica reaches the source by a different address or account.
type ReplicationInfo struct {
	SourceHost   string
	SourcePort   int
	ReplUser     string // "<repl_user>" placeholder when empty
	ReplPassword string // "<repl_password>" placeholder when empty
	BinlogFile   string
	BinlogPos    int
	GTIDSet      string // raw from metadata.json; sanitised and validated here
}

// escapeSQLString escapes a value for embedding in a single-quoted MySQL
// string literal: backslashes and single quotes are backslash-escaped.
func escapeSQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `'`, `\'`)
}

// BuildReplicationSQL renders ready-to-run replication setup statements from
// the dump's recorded coordinates. Nothing is executed — the output is for
// the operator to review and run manually (or pipe into the mysql client).
//
// When the dump carries a GTID set, the GTID AUTO_POSITION variant is emitted
// as the runnable block and file/position as a commented alternative;
// without a GTID set, file/position is the runnable block. The MySQL 5.7
// CHANGE MASTER syntax is always included as a commented block.
func BuildReplicationSQL(info ReplicationInfo) (string, error) {
	if info.SourceHost == "" {
		return "", fmt.Errorf("no source host: metadata.json has no mysql_host and --source-host was not given")
	}

	gtidSet := ""
	if info.GTIDSet != "" {
		s, err := sanitizeGTIDSet(info.GTIDSet)
		if err != nil {
			return "", fmt.Errorf("gtid_set in metadata.json: %w", err)
		}
		gtidSet = s
	}
	if gtidSet == "" && info.BinlogFile == "" {
		return "", fmt.Errorf("the dump has no replication coordinates (no gtid_set, no binlog_file) — " +
			"was it taken with --get-master-status?")
	}

	port := info.SourcePort
	if port == 0 {
		port = 3306
	}
	user := "<repl_user>"
	if info.ReplUser != "" {
		user = escapeSQLString(info.ReplUser)
	}
	password := "<repl_password>"
	if info.ReplPassword != "" {
		password = escapeSQLString(info.ReplPassword)
	}
	host := escapeSQLString(info.SourceHost)

	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	w("-- Replication setup for a replica seeded from this dump.")
	w("-- Coordinates parsed from metadata.json (captured inside the dump's lock window).")
	w("--   Source:               %s:%d", info.SourceHost, port)
	if info.BinlogFile != "" {
		w("--   Binlog file/position: %s:%d", info.BinlogFile, info.BinlogPos)
	}
	if gtidSet != "" {
		w("--   Executed GTID set:    %s", gtidSet)
	}
	w("--")
	w("-- Review before running. If the source uses caching_sha2_password without")
	w("-- TLS, add GET_SOURCE_PUBLIC_KEY = 1 to the CHANGE statement.")

	changeHeader := func(commented bool) {
		p := ""
		if commented {
			p = "-- "
		}
		w("%sCHANGE REPLICATION SOURCE TO", p)
		w("%s  SOURCE_HOST = '%s',", p, host)
		w("%s  SOURCE_PORT = %d,", p, port)
		w("%s  SOURCE_USER = '%s',", p, user)
		w("%s  SOURCE_PASSWORD = '%s',", p, password)
	}

	if gtidSet != "" {
		w("")
		w("-- ── GTID auto-position (MySQL 8.0.23+ syntax) ──")
		w("-- Prerequisite: the target's gtid_executed must equal the dump's GTID set.")
		w("-- go-load --set-gtid-purged does this with safety checks; to do it by hand")
		w("-- on a target with EMPTY gtid_executed, uncomment:")
		w("-- SET GLOBAL gtid_purged = '%s';", gtidSet)
		changeHeader(false)
		w("  SOURCE_AUTO_POSITION = 1;")
		w("START REPLICA;")
		w("")
		w("-- ── File/position alternative (gtid_mode=OFF topologies) ──")
		changeHeader(true)
		w("--   SOURCE_LOG_FILE = '%s',", info.BinlogFile)
		w("--   SOURCE_LOG_POS = %d;", info.BinlogPos)
		w("-- START REPLICA;")
	} else {
		w("")
		w("-- ── File/position (dump has no GTID set) ──")
		changeHeader(false)
		w("  SOURCE_LOG_FILE = '%s',", info.BinlogFile)
		w("  SOURCE_LOG_POS = %d;", info.BinlogPos)
		w("START REPLICA;")
	}

	w("")
	w("-- ── MySQL 5.7 / pre-8.0.23 syntax ──")
	w("-- CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d,", host, port)
	w("--   MASTER_USER='%s', MASTER_PASSWORD='%s',", user, password)
	if gtidSet != "" {
		w("--   MASTER_AUTO_POSITION=1;")
	} else {
		w("--   MASTER_LOG_FILE='%s', MASTER_LOG_POS=%d;", info.BinlogFile, info.BinlogPos)
	}
	w("-- START SLAVE;")

	return b.String(), nil
}
