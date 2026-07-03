package load

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/ChaosHour/go-dump/internal/log"
)

// gtidSetPattern matches a sanitised (whitespace-stripped) GTID set:
// comma-separated uuid:interval[:interval...] entries. Validated before the
// set is embedded in SQL — a GTID set never legitimately contains quotes.
var gtidSetPattern = regexp.MustCompile(`^[0-9a-fA-F-]+(:[0-9]+(-[0-9]+)?)+(,[0-9a-fA-F-]+(:[0-9]+(-[0-9]+)?)+)*$`)

// sanitizeGTIDSet strips all whitespace (metadata.json stores the set with
// embedded newlines, as MySQL returns it) and validates the result.
func sanitizeGTIDSet(set string) (string, error) {
	cleaned := strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\n', '\r', '\t':
			return -1
		}
		return r
	}, set)
	if cleaned == "" {
		return "", fmt.Errorf("GTID set is empty")
	}
	if !gtidSetPattern.MatchString(cleaned) {
		return "", fmt.Errorf("GTID set %q does not look like a valid GTID set", cleaned)
	}
	return cleaned, nil
}

// serverVersion parses SELECT VERSION() into [major, minor, patch].
func serverVersion(ctx context.Context, conn *sql.Conn) ([3]int, error) {
	var raw string
	if err := conn.QueryRowContext(ctx, "SELECT VERSION()").Scan(&raw); err != nil {
		return [3]int{}, fmt.Errorf("detect server version: %w", err)
	}
	var v [3]int
	parts := strings.Split(strings.Split(raw, "-")[0], ".")
	for i := 0; i < 3 && i < len(parts); i++ {
		v[i], _ = strconv.Atoi(parts[i])
	}
	return v, nil
}

func versionAtLeast(v [3]int, major, minor, patch int) bool {
	if v[0] != major {
		return v[0] > major
	}
	if v[1] != minor {
		return v[1] > minor
	}
	return v[2] >= patch
}

// replicaConfigured reports whether the server has any replication channel,
// and whether any channel's IO or SQL thread is running.
func replicaConfigured(ctx context.Context, conn *sql.Conn, v [3]int) (configured, running bool, err error) {
	stmt := "SHOW SLAVE STATUS"
	if versionAtLeast(v, 8, 0, 22) {
		stmt = "SHOW REPLICA STATUS"
	}
	rows, err := conn.QueryContext(ctx, stmt)
	if err != nil {
		return false, false, fmt.Errorf("%s: %w", stmt, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return false, false, err
	}
	// Track the *_IO_Running / *_SQL_Running columns by position.
	runningIdx := make([]int, 0, 2)
	for i, c := range cols {
		u := strings.ToUpper(c)
		if strings.HasSuffix(u, "_IO_RUNNING") || strings.HasSuffix(u, "_SQL_RUNNING") {
			runningIdx = append(runningIdx, i)
		}
	}

	vals := make([]any, len(cols))
	for i := range vals {
		vals[i] = new(sql.RawBytes)
	}
	for rows.Next() {
		configured = true
		if err := rows.Scan(vals...); err != nil {
			return configured, false, err
		}
		for _, i := range runningIdx {
			if rb, ok := vals[i].(*sql.RawBytes); ok && strings.EqualFold(string(*rb), "Yes") {
				running = true
			}
		}
	}
	return configured, running, rows.Err()
}

// ApplyGTIDPurged seeds the target server's GTID history from the dump's
// captured gtid_set so the target can replicate from the dump source with
// SOURCE_AUTO_POSITION=1.
//
// Safety gates, in order:
//   - the dump GTID set must be well-formed (it is embedded in SQL);
//   - gtid_mode must be ON on the target;
//   - a running replication channel always aborts; a configured-but-stopped
//     channel requires force;
//   - if the target's gtid_executed is non-empty, it must be a subset of the
//     dump's set (anything extra means transactions the dump source never
//     had — errant; use go-gtids to inspect). A subset still requires force,
//     because clearing it takes RESET MASTER / RESET BINARY LOGS AND GTIDS,
//     which destroys the target's binlog history.
//
// The cheap path — empty gtid_executed, e.g. a fresh instance loaded with
// --skip-binlog — needs no reset and no force.
func ApplyGTIDPurged(ctx context.Context, db *sql.DB, dumpGTIDSet string, force bool) error {
	set, err := sanitizeGTIDSet(dumpGTIDSet)
	if err != nil {
		return fmt.Errorf("--set-gtid-purged: %w (was the dump taken with --get-master-status?)", err)
	}

	// One connection for the whole sequence: the checks and the SET must see
	// and act on the same session/server state.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("--set-gtid-purged: acquire connection: %w", err)
	}
	defer conn.Close()

	var gtidMode string
	if err := conn.QueryRowContext(ctx, "SELECT @@global.gtid_mode").Scan(&gtidMode); err != nil {
		return fmt.Errorf("--set-gtid-purged: read gtid_mode: %w", err)
	}
	if !strings.EqualFold(gtidMode, "ON") {
		return fmt.Errorf("--set-gtid-purged: target gtid_mode is %s, need ON. "+
			"For gtid_mode=OFF topologies use file/position replication instead "+
			"(see change-replication-source.sql in the dump directory)", gtidMode)
	}

	v, err := serverVersion(ctx, conn)
	if err != nil {
		return fmt.Errorf("--set-gtid-purged: %w", err)
	}

	configured, running, err := replicaConfigured(ctx, conn, v)
	if err != nil {
		return fmt.Errorf("--set-gtid-purged: %w", err)
	}
	if running {
		return fmt.Errorf("--set-gtid-purged: target has a RUNNING replication channel. " +
			"Refusing to touch GTID state on an active replica — STOP REPLICA first if you really mean to re-seed it")
	}
	if configured && !force {
		return fmt.Errorf("--set-gtid-purged: target has a configured (stopped) replication channel. " +
			"Re-run with --force if you are deliberately re-seeding this replica")
	}

	var executed string
	if err := conn.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&executed); err != nil {
		return fmt.Errorf("--set-gtid-purged: read gtid_executed: %w", err)
	}
	executed = strings.Join(strings.Fields(executed), "")

	if executed != "" {
		// Errant gate: anything the target executed that the dump set does not
		// cover means writes the dump source never had. Never reset over those.
		var extra string
		q := fmt.Sprintf("SELECT GTID_SUBTRACT(@@global.gtid_executed, '%s')", set)
		if err := conn.QueryRowContext(ctx, q).Scan(&extra); err != nil {
			return fmt.Errorf("--set-gtid-purged: errant-transaction check: %w", err)
		}
		if strings.TrimSpace(extra) != "" {
			return fmt.Errorf("--set-gtid-purged: target has transactions beyond the dump's GTID set: %s. "+
				"This looks like errant transactions or a target that was never empty — inspect with "+
				"go-gtids (https://github.com/ChaosHour/go-gtids) before touching GTID state",
				strings.Join(strings.Fields(extra), ""))
		}

		reset := "RESET MASTER"
		if versionAtLeast(v, 8, 4, 0) {
			reset = "RESET BINARY LOGS AND GTIDS"
		}
		if !force {
			return fmt.Errorf("--set-gtid-purged: target gtid_executed is not empty (it is a subset of the dump set, "+
				"likely from the load itself). Clearing it requires %s, which DESTROYS the target's binlog history. "+
				"Re-run with --force to proceed, or avoid this entirely by loading with --skip-binlog", reset)
		}
		log.Warningf("Executing %s on the target (destroys its binlog history).", reset)
		if _, err := conn.ExecContext(ctx, reset); err != nil {
			return fmt.Errorf("--set-gtid-purged: %s: %w", reset, err)
		}
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", set)); err != nil {
		return fmt.Errorf("--set-gtid-purged: SET GLOBAL gtid_purged: %w", err)
	}

	// Read back and confirm: gtid_executed must now equal the dump set.
	var after string
	if err := conn.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&after); err != nil {
		return fmt.Errorf("--set-gtid-purged: verify: %w", err)
	}
	if strings.Join(strings.Fields(after), "") != set {
		return fmt.Errorf("--set-gtid-purged: verification failed: gtid_executed is %q, expected %q", after, set)
	}

	log.Infof("gtid_purged set to the dump's snapshot GTID set. Next: CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1 "+
		"(ready-to-edit template: change-replication-source.sql in the dump directory), then START REPLICA. GTID set: %s", set)
	return nil
}
