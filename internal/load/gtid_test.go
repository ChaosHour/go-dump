package load

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
)

// ── sanitizeGTIDSet ──────────────────────────────────────────────────────────

func TestSanitizeGTIDSet(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"simple", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-123", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-123", false},
		{"multi with newlines (metadata.json form)",
			"1d1fff5a-c9bc-11ed-9c19-02a36d996b94:123,\n2ac8ec13-9255-11f0-8705-6238a95b9967:1-40586",
			"1d1fff5a-c9bc-11ed-9c19-02a36d996b94:123,2ac8ec13-9255-11f0-8705-6238a95b9967:1-40586", false},
		{"multiple intervals", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:11-18", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:11-18", false},
		{"empty", "", "", true},
		{"whitespace only", " \n\t", "", true},
		{"sql injection attempt", "x'; DROP TABLE t; --", "", true},
		{"quote smuggling", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5'", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sanitizeGTIDSet(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestVersionAtLeast(t *testing.T) {
	tests := []struct {
		v       [3]int
		m, n, p int
		want    bool
	}{
		{[3]int{8, 0, 45}, 8, 4, 0, false},
		{[3]int{8, 4, 10}, 8, 4, 0, true},
		{[3]int{9, 7, 1}, 8, 4, 0, true},
		{[3]int{5, 7, 44}, 8, 0, 22, false},
		{[3]int{8, 0, 22}, 8, 0, 22, true},
	}
	for _, tt := range tests {
		if got := versionAtLeast(tt.v, tt.m, tt.n, tt.p); got != tt.want {
			t.Errorf("versionAtLeast(%v, %d.%d.%d) = %v, want %v", tt.v, tt.m, tt.n, tt.p, got, tt.want)
		}
	}
}

// ── ApplyGTIDPurged ──────────────────────────────────────────────────────────

const dumpSet = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-123"

// gtidServer scripts a fake server for ApplyGTIDPurged. The zero value is a
// fresh, empty, gtid_mode=ON MySQL 8.0 instance. Behaviours flip when the
// corresponding SET/RESET statements execute, mimicking a real server.
type gtidServer struct {
	rec        *stmtRecorder
	version    string
	gtidMode   string
	executed   string // current gtid_executed
	subtract   string // GTID_SUBTRACT(executed, dumpSet) result
	replicaRow [][]driver.Value
	replicaCol []string
}

func newGTIDServer(rec *stmtRecorder) *gtidServer {
	s := &gtidServer{
		rec:        rec,
		version:    "8.0.45",
		gtidMode:   "ON",
		replicaCol: []string{"Replica_IO_Running", "Replica_SQL_Running"},
	}
	rec.queryFn = func(q string) ([]string, [][]driver.Value, error) {
		switch {
		case strings.Contains(q, "gtid_mode"):
			return []string{"@@global.gtid_mode"}, [][]driver.Value{{s.gtidMode}}, nil
		case strings.Contains(q, "VERSION()"):
			return []string{"VERSION()"}, [][]driver.Value{{s.version}}, nil
		case strings.Contains(q, "REPLICA STATUS"), strings.Contains(q, "SLAVE STATUS"):
			return s.replicaCol, s.replicaRow, nil
		case strings.Contains(q, "GTID_SUBTRACT"):
			return []string{"sub"}, [][]driver.Value{{s.subtract}}, nil
		case strings.Contains(q, "gtid_executed"):
			// Reflect state changes made by RESET / SET gtid_purged.
			cur := s.executed
			for _, st := range s.rec.executed() {
				if strings.HasPrefix(st, "RESET") {
					cur = ""
				}
				if strings.HasPrefix(st, "SET GLOBAL gtid_purged") {
					cur = dumpSet
				}
			}
			return []string{"@@global.gtid_executed"}, [][]driver.Value{{cur}}, nil
		}
		return nil, nil, nil
	}
	return s
}

func executedStatements(rec *stmtRecorder, prefix string) []string {
	var out []string
	for _, s := range rec.executed() {
		if strings.HasPrefix(s, prefix) {
			out = append(out, s)
		}
	}
	return out
}

func TestApplyGTIDPurged_EmptyExecutedNoResetNeeded(t *testing.T) {
	db, rec := newFakeDB(t)
	newGTIDServer(rec)

	if err := ApplyGTIDPurged(context.Background(), db, dumpSet, false); err != nil {
		t.Fatalf("expected success on empty gtid_executed, got: %v", err)
	}
	if got := executedStatements(rec, "RESET"); len(got) != 0 {
		t.Errorf("RESET must not run when gtid_executed is empty, got: %v", got)
	}
	sets := executedStatements(rec, "SET GLOBAL gtid_purged")
	if len(sets) != 1 || !strings.Contains(sets[0], dumpSet) {
		t.Errorf("expected one SET GLOBAL gtid_purged with the dump set, got: %v", sets)
	}
}

func TestApplyGTIDPurged_GtidModeOff(t *testing.T) {
	db, rec := newFakeDB(t)
	s := newGTIDServer(rec)
	s.gtidMode = "OFF"

	err := ApplyGTIDPurged(context.Background(), db, dumpSet, false)
	if err == nil || !strings.Contains(err.Error(), "gtid_mode") {
		t.Fatalf("expected gtid_mode error, got: %v", err)
	}
}

func TestApplyGTIDPurged_RunningReplicaRefusedEvenWithForce(t *testing.T) {
	db, rec := newFakeDB(t)
	s := newGTIDServer(rec)
	s.replicaRow = [][]driver.Value{{"Yes", "Yes"}}

	err := ApplyGTIDPurged(context.Background(), db, dumpSet, true)
	if err == nil || !strings.Contains(err.Error(), "RUNNING") {
		t.Fatalf("expected running-replica refusal, got: %v", err)
	}
	if got := executedStatements(rec, "SET GLOBAL gtid_purged"); len(got) != 0 {
		t.Errorf("gtid_purged must not be touched on a running replica, got: %v", got)
	}
}

func TestApplyGTIDPurged_StoppedChannelNeedsForce(t *testing.T) {
	db, rec := newFakeDB(t)
	s := newGTIDServer(rec)
	s.replicaRow = [][]driver.Value{{"No", "No"}}

	err := ApplyGTIDPurged(context.Background(), db, dumpSet, false)
	if err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("expected force-required error for stopped channel, got: %v", err)
	}

	// Same server with --force: proceeds (gtid_executed empty → no RESET).
	db2, rec2 := newFakeDB(t)
	s2 := newGTIDServer(rec2)
	s2.replicaRow = [][]driver.Value{{"No", "No"}}
	if err := ApplyGTIDPurged(context.Background(), db2, dumpSet, true); err != nil {
		t.Fatalf("expected success with --force on stopped channel, got: %v", err)
	}
}

func TestApplyGTIDPurged_NonEmptySubsetNeedsForce(t *testing.T) {
	db, rec := newFakeDB(t)
	s := newGTIDServer(rec)
	s.executed = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100" // subset of dump set
	s.subtract = ""                                           // nothing beyond the dump

	err := ApplyGTIDPurged(context.Background(), db, dumpSet, false)
	if err == nil || !strings.Contains(err.Error(), "RESET MASTER") {
		t.Fatalf("expected error naming RESET MASTER, got: %v", err)
	}
	if !strings.Contains(err.Error(), "--skip-binlog") {
		t.Errorf("error should suggest --skip-binlog as the non-destructive path, got: %v", err)
	}
	if got := executedStatements(rec, "RESET"); len(got) != 0 {
		t.Errorf("RESET must not run without --force, got: %v", got)
	}
}

func TestApplyGTIDPurged_NonEmptySubsetWithForceResets(t *testing.T) {
	db, rec := newFakeDB(t)
	s := newGTIDServer(rec)
	s.executed = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	s.subtract = ""

	if err := ApplyGTIDPurged(context.Background(), db, dumpSet, true); err != nil {
		t.Fatalf("expected success with --force, got: %v", err)
	}
	resets := executedStatements(rec, "RESET")
	if len(resets) != 1 || resets[0] != "RESET MASTER" {
		t.Errorf("expected exactly [RESET MASTER] on 8.0, got: %v", resets)
	}
}

func TestApplyGTIDPurged_84UsesResetBinaryLogs(t *testing.T) {
	db, rec := newFakeDB(t)
	s := newGTIDServer(rec)
	s.version = "8.4.10"
	s.executed = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	s.subtract = ""

	if err := ApplyGTIDPurged(context.Background(), db, dumpSet, true); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	resets := executedStatements(rec, "RESET")
	if len(resets) != 1 || resets[0] != "RESET BINARY LOGS AND GTIDS" {
		t.Errorf("expected [RESET BINARY LOGS AND GTIDS] on 8.4, got: %v", resets)
	}
}

func TestApplyGTIDPurged_ErrantTransactionsAlwaysRefused(t *testing.T) {
	db, rec := newFakeDB(t)
	s := newGTIDServer(rec)
	s.executed = dumpSet + ",aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-5"
	s.subtract = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-5" // beyond the dump

	err := ApplyGTIDPurged(context.Background(), db, dumpSet, true) // force must NOT override
	if err == nil || !strings.Contains(err.Error(), "go-gtids") {
		t.Fatalf("expected errant refusal pointing at go-gtids, got: %v", err)
	}
	if got := executedStatements(rec, "RESET"); len(got) != 0 {
		t.Errorf("RESET must never run over errant transactions, got: %v", got)
	}
}

func TestApplyGTIDPurged_InvalidSetRefused(t *testing.T) {
	db, _ := newFakeDB(t)
	err := ApplyGTIDPurged(context.Background(), db, "'; DROP TABLE users; --", false)
	if err == nil || !strings.Contains(err.Error(), "valid GTID set") {
		t.Fatalf("expected validation error, got: %v", err)
	}
}

func TestApplyGTIDPurged_57UsesShowSlaveStatus(t *testing.T) {
	db, rec := newFakeDB(t)
	s := newGTIDServer(rec)
	s.version = "5.7.44"
	s.replicaCol = []string{"Slave_IO_Running", "Slave_SQL_Running"}

	if err := ApplyGTIDPurged(context.Background(), db, dumpSet, false); err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if got := executedStatements(rec, "SHOW SLAVE STATUS"); len(got) != 1 {
		t.Errorf("5.7 must use SHOW SLAVE STATUS, statements: %v", rec.executed())
	}
	if got := executedStatements(rec, "SHOW REPLICA STATUS"); len(got) != 0 {
		t.Errorf("5.7 must not use SHOW REPLICA STATUS, statements: %v", rec.executed())
	}
}
