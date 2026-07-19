package load

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/ChaosHour/go-dump/internal/log"
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

// StartOptions controls StartReplication.
type StartOptions struct {
	Mode               string // "auto", "gtid", or "file-pos"
	SourceSSL          bool   // add SOURCE_SSL = 1 (encrypt the replication connection)
	GetSourcePublicKey bool   // add GET_SOURCE_PUBLIC_KEY = 1 (caching_sha2_password without TLS)
	Force              bool   // allow acting on a configured-but-stopped channel
}

// chooseReplMode resolves the replication mode. "auto" picks GTID
// auto-position when the dump carries a GTID set and the target has
// gtid_mode=ON, falling back to file/position otherwise; explicit modes are
// validated against what the dump and target actually support.
func chooseReplMode(requested, gtidSet string, gtidModeOn bool, binlogFile string) (string, error) {
	switch requested {
	case "auto":
		if gtidSet != "" && gtidModeOn {
			return "gtid", nil
		}
		if binlogFile == "" {
			return "", fmt.Errorf("no usable coordinates: the dump has no binlog_file and GTID auto-position is unavailable " +
				"(dump has no gtid_set or target gtid_mode is not ON)")
		}
		return "file-pos", nil
	case "gtid":
		if gtidSet == "" {
			return "", fmt.Errorf("--replication-mode gtid: the dump has no gtid_set (was it taken with --get-master-status on a GTID-enabled source?)")
		}
		if !gtidModeOn {
			return "", fmt.Errorf("--replication-mode gtid: target gtid_mode is not ON")
		}
		return "gtid", nil
	case "file-pos":
		if binlogFile == "" {
			return "", fmt.Errorf("--replication-mode file-pos: the dump has no binlog_file (was it taken with --get-master-status?)")
		}
		return "file-pos", nil
	default:
		return "", fmt.Errorf("--replication-mode must be auto, gtid, or file-pos (got %q)", requested)
	}
}

// buildChangeSourceSQL renders the CHANGE statement for the target server
// version, plus a copy with the password redacted for logging.
func buildChangeSourceSQL(v [3]int, info ReplicationInfo, gtidSet, mode string, opts StartOptions) (stmt, redacted string) {
	modern := versionAtLeast(v, 8, 0, 23)

	kw := func(modernName, legacyName string) string {
		if modern {
			return modernName
		}
		return legacyName
	}

	_ = gtidSet // coordinates travel via AUTO_POSITION; gtid_purged is seeded separately

	build := func(password string) string {
		var clauses []string
		add := func(c string) { clauses = append(clauses, c) }

		add(fmt.Sprintf("%s = '%s'", kw("SOURCE_HOST", "MASTER_HOST"), escapeSQLString(info.SourceHost)))
		port := info.SourcePort
		if port == 0 {
			port = 3306
		}
		add(fmt.Sprintf("%s = %d", kw("SOURCE_PORT", "MASTER_PORT"), port))
		add(fmt.Sprintf("%s = '%s'", kw("SOURCE_USER", "MASTER_USER"), escapeSQLString(info.ReplUser)))
		add(fmt.Sprintf("%s = '%s'", kw("SOURCE_PASSWORD", "MASTER_PASSWORD"), password))

		if opts.SourceSSL {
			add(fmt.Sprintf("%s = 1", kw("SOURCE_SSL", "MASTER_SSL")))
		}
		// GET_MASTER_PUBLIC_KEY exists from 8.0.4 (caching_sha2_password did
		// not exist before 8.0); silently skip on older targets.
		if opts.GetSourcePublicKey && versionAtLeast(v, 8, 0, 4) {
			add(fmt.Sprintf("%s = 1", kw("GET_SOURCE_PUBLIC_KEY", "GET_MASTER_PUBLIC_KEY")))
		}

		if mode == "gtid" {
			add(fmt.Sprintf("%s = 1", kw("SOURCE_AUTO_POSITION", "MASTER_AUTO_POSITION")))
		} else {
			add(fmt.Sprintf("%s = '%s'", kw("SOURCE_LOG_FILE", "MASTER_LOG_FILE"), escapeSQLString(info.BinlogFile)))
			add(fmt.Sprintf("%s = %d", kw("SOURCE_LOG_POS", "MASTER_LOG_POS"), info.BinlogPos))
		}

		return fmt.Sprintf("%s %s",
			kw("CHANGE REPLICATION SOURCE TO", "CHANGE MASTER TO"),
			strings.Join(clauses, ", "))
	}

	return build(escapeSQLString(info.ReplPassword)), build("<redacted>")
}

// replicaStatusRow returns the first row of SHOW REPLICA STATUS (SHOW SLAVE
// STATUS pre-8.0.22) as a column-name → value map, or nil when no channel is
// configured.
func replicaStatusRow(ctx context.Context, conn *sql.Conn, v [3]int) (map[string]string, error) {
	stmt := "SHOW SLAVE STATUS"
	if versionAtLeast(v, 8, 0, 22) {
		stmt = "SHOW REPLICA STATUS"
	}
	rows, err := conn.QueryContext(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", stmt, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, rows.Err()
	}
	vals := make([]any, len(cols))
	for i := range vals {
		vals[i] = new(sql.RawBytes)
	}
	if err := rows.Scan(vals...); err != nil {
		return nil, err
	}
	m := make(map[string]string, len(cols))
	for i, c := range cols {
		if rb, ok := vals[i].(*sql.RawBytes); ok {
			m[strings.ToUpper(c)] = string(*rb)
		}
	}
	return m, nil
}

// statusField reads a replica-status field that differs only by the
// Replica_/Slave_ prefix across versions.
func statusField(m map[string]string, suffix string) string {
	for k, v := range m {
		if strings.HasSuffix(k, suffix) {
			return v
		}
	}
	return ""
}

// assessReplicaStatus interprets one status row: healthy (both threads
// running), a hard error (either Last_*_Error set), or still starting.
func assessReplicaStatus(m map[string]string) (healthy bool, hardErr string, state string) {
	io := statusField(m, "_IO_RUNNING")
	sqlThread := statusField(m, "_SQL_RUNNING")
	if e := m["LAST_IO_ERROR"]; e != "" {
		return false, "IO thread: " + e, ""
	}
	if e := m["LAST_SQL_ERROR"]; e != "" {
		return false, "SQL thread: " + e, ""
	}
	if strings.EqualFold(io, "Yes") && strings.EqualFold(sqlThread, "Yes") {
		return true, "", ""
	}
	return false, "", fmt.Sprintf("IO thread: %s, SQL thread: %s", io, sqlThread)
}

// startReplicaHealthTimeout bounds how long StartReplication waits for both
// threads to report running before giving up. The IO thread legitimately
// reports "Connecting" for a few seconds on first start.
const startReplicaHealthTimeout = 15 * time.Second

// StartReplication configures the target as a replica of the dump source and
// starts it, then waits for both threads to come up healthy.
//
// Safety gates, matching ApplyGTIDPurged:
//   - a RUNNING replication channel always aborts;
//   - a configured-but-stopped channel requires force;
//   - in GTID mode, the target's gtid_executed must equal the dump's GTID set
//     (seed it with --set-gtid-purged) — anything less re-fetches rows the
//     load already inserted, anything more is errant.
func StartReplication(ctx context.Context, db *sql.DB, info ReplicationInfo, opts StartOptions) error {
	if info.SourceHost == "" {
		return fmt.Errorf("--start-replication: no source host: metadata.json has no mysql_host and --source-host was not given")
	}
	if info.ReplUser == "" {
		return fmt.Errorf("--start-replication: --repl-user is required")
	}

	gtidSet := ""
	if info.GTIDSet != "" {
		s, err := sanitizeGTIDSet(info.GTIDSet)
		if err != nil {
			return fmt.Errorf("--start-replication: gtid_set in metadata.json: %w", err)
		}
		gtidSet = s
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("--start-replication: acquire connection: %w", err)
	}
	defer conn.Close()

	v, err := serverVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("--start-replication: %w", err)
	}

	configured, running, err := replicaConfigured(ctx, conn, v)
	if err != nil {
		return fmt.Errorf("--start-replication: %w", err)
	}
	if running {
		return fmt.Errorf("--start-replication: target has a RUNNING replication channel. " +
			"Refusing to reconfigure an active replica — STOP REPLICA first if you really mean to re-point it")
	}
	if configured && !opts.Force {
		return fmt.Errorf("--start-replication: target has a configured (stopped) replication channel. " +
			"Re-run with --force if you are deliberately re-pointing this replica")
	}

	var gtidMode string
	if err := conn.QueryRowContext(ctx, "SELECT @@global.gtid_mode").Scan(&gtidMode); err != nil {
		// 5.6+ always has gtid_mode; treat a read failure as fatal rather than guessing.
		return fmt.Errorf("--start-replication: read gtid_mode: %w", err)
	}

	mode, err := chooseReplMode(opts.Mode, gtidSet, strings.EqualFold(gtidMode, "ON"), info.BinlogFile)
	if err != nil {
		return fmt.Errorf("--start-replication: %w", err)
	}
	log.Infof("Replication mode: %s", mode)

	if mode == "gtid" {
		// AUTO_POSITION replicates everything the target's GTID state does not
		// cover — that state must exactly equal the dump's snapshot set.
		var missing, extra string
		if err := conn.QueryRowContext(ctx,
			fmt.Sprintf("SELECT GTID_SUBTRACT('%s', @@global.gtid_executed), GTID_SUBTRACT(@@global.gtid_executed, '%s')", gtidSet, gtidSet),
		).Scan(&missing, &extra); err != nil {
			return fmt.Errorf("--start-replication: GTID state check: %w", err)
		}
		missing = strings.Join(strings.Fields(missing), "")
		extra = strings.Join(strings.Fields(extra), "")
		if extra != "" {
			return fmt.Errorf("--start-replication: target has transactions beyond the dump's GTID set: %s. "+
				"Errant transactions or a non-empty target — inspect with go-gtids before replicating", extra)
		}
		if missing != "" {
			return fmt.Errorf("--start-replication: target gtid_executed does not cover the dump's GTID set (missing %s). "+
				"AUTO_POSITION would re-fetch transactions whose rows the load already inserted — "+
				"seed the GTID state first with --set-gtid-purged", missing)
		}
	}

	changeStmt, redacted := buildChangeSourceSQL(v, info, gtidSet, mode, opts)
	log.Infof("Executing: %s", redacted)
	if _, err := conn.ExecContext(ctx, changeStmt); err != nil {
		return fmt.Errorf("--start-replication: %s: %w", redacted, err)
	}

	startStmt := "START SLAVE"
	if versionAtLeast(v, 8, 0, 22) {
		startStmt = "START REPLICA"
	}
	log.Infof("Executing: %s", startStmt)
	if _, err := conn.ExecContext(ctx, startStmt); err != nil {
		return fmt.Errorf("--start-replication: %s: %w", startStmt, err)
	}

	// Wait for both threads: "Connecting" is normal for a few seconds, a
	// Last_*_Error (bad credentials, missing binlogs, apply failure) is not.
	deadline := time.Now().Add(startReplicaHealthTimeout)
	var lastState string
	for {
		m, err := replicaStatusRow(ctx, conn, v)
		if err != nil {
			return fmt.Errorf("--start-replication: %w", err)
		}
		if m != nil {
			healthy, hardErr, state := assessReplicaStatus(m)
			if healthy {
				behind := statusField(m, "SECONDS_BEHIND_SOURCE")
				if behind == "" {
					behind = statusField(m, "SECONDS_BEHIND_MASTER")
				}
				log.Infof("Replication running: IO=Yes SQL=Yes, seconds behind source: %s", behind)
				return nil
			}
			if hardErr != "" {
				return fmt.Errorf("--start-replication: replication failed to start — %s", hardErr)
			}
			lastState = state
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("--start-replication: replication threads not both running after %s (%s). "+
				"Check SHOW REPLICA STATUS on the target", startReplicaHealthTimeout, lastState)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
