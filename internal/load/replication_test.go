package load

import (
	"strings"
	"testing"
)

func TestBuildReplicationSQL_GTID(t *testing.T) {
	out, err := BuildReplicationSQL(ReplicationInfo{
		SourceHost: "db01.example.com",
		SourcePort: 3306,
		ReplUser:   "repl",
		BinlogFile: "binlog.000042",
		BinlogPos:  1421,
		GTIDSet:    "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-123",
	})
	if err != nil {
		t.Fatalf("BuildReplicationSQL: %v", err)
	}

	for _, want := range []string{
		"SOURCE_HOST = 'db01.example.com',",
		"SOURCE_PORT = 3306,",
		"SOURCE_USER = 'repl',",
		"SOURCE_PASSWORD = '<repl_password>',",
		"SOURCE_AUTO_POSITION = 1;",
		"START REPLICA;",
		"-- SET GLOBAL gtid_purged = '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-123';",
		"--   SOURCE_LOG_FILE = 'binlog.000042',",
		"--   SOURCE_LOG_POS = 1421;",
		"--   MASTER_AUTO_POSITION=1;",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
	// The GTID variant is the runnable block: file/position must only appear commented.
	if strings.Contains(out, "\n  SOURCE_LOG_FILE") {
		t.Error("file/position variant should be commented when a GTID set is present")
	}
}

func TestBuildReplicationSQL_FilePosOnly(t *testing.T) {
	out, err := BuildReplicationSQL(ReplicationInfo{
		SourceHost: "10.0.0.5",
		BinlogFile: "mysql-bin.000007",
		BinlogPos:  98765,
	})
	if err != nil {
		t.Fatalf("BuildReplicationSQL: %v", err)
	}
	for _, want := range []string{
		"SOURCE_PORT = 3306,", // default when unset
		"  SOURCE_LOG_FILE = 'mysql-bin.000007',",
		"  SOURCE_LOG_POS = 98765;",
		"SOURCE_USER = '<repl_user>',",
		"--   MASTER_LOG_FILE='mysql-bin.000007', MASTER_LOG_POS=98765;",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\n%s", want, out)
		}
	}
	if strings.Contains(out, "AUTO_POSITION") && !strings.Contains(out, "-- ") {
		t.Error("no AUTO_POSITION variant expected without a GTID set")
	}
}

func TestBuildReplicationSQL_MultilineGTIDSetSanitised(t *testing.T) {
	// metadata.json stores the set as MySQL returns it — with embedded newlines.
	out, err := BuildReplicationSQL(ReplicationInfo{
		SourceHost: "h",
		GTIDSet:    "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,\n4f22fb58-82db-22f2-af44-d91bb540c673:1-9",
	})
	if err != nil {
		t.Fatalf("BuildReplicationSQL: %v", err)
	}
	if !strings.Contains(out, "'3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,4f22fb58-82db-22f2-af44-d91bb540c673:1-9'") {
		t.Errorf("GTID set not sanitised:\n%s", out)
	}
}

func TestBuildReplicationSQL_PasswordEscaped(t *testing.T) {
	out, err := BuildReplicationSQL(ReplicationInfo{
		SourceHost:   "h",
		ReplUser:     "repl",
		ReplPassword: `p'ss\word`,
		BinlogFile:   "b.1",
		BinlogPos:    4,
	})
	if err != nil {
		t.Fatalf("BuildReplicationSQL: %v", err)
	}
	if !strings.Contains(out, `SOURCE_PASSWORD = 'p\'ss\\word',`) {
		t.Errorf("password not escaped:\n%s", out)
	}
}

func TestBuildReplicationSQL_NoCoordinates(t *testing.T) {
	_, err := BuildReplicationSQL(ReplicationInfo{SourceHost: "h"})
	if err == nil || !strings.Contains(err.Error(), "--get-master-status") {
		t.Errorf("expected no-coordinates error, got %v", err)
	}
}

func TestBuildReplicationSQL_NoHost(t *testing.T) {
	_, err := BuildReplicationSQL(ReplicationInfo{BinlogFile: "b.1"})
	if err == nil || !strings.Contains(err.Error(), "source host") {
		t.Errorf("expected no-host error, got %v", err)
	}
}

func TestBuildReplicationSQL_InvalidGTIDSet(t *testing.T) {
	_, err := BuildReplicationSQL(ReplicationInfo{
		SourceHost: "h",
		GTIDSet:    "'; DROP TABLE users; --",
	})
	if err == nil {
		t.Error("expected error for malformed GTID set")
	}
}

func TestEscapeSQLString(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", "plain"},
		{"o'brien", `o\'brien`},
		{`back\slash`, `back\\slash`},
		{`both'\`, `both\'\\`},
	}
	for _, c := range cases {
		if got := escapeSQLString(c.in); got != c.want {
			t.Errorf("escapeSQLString(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// --- StartReplication pure helpers ---

func TestChooseReplMode(t *testing.T) {
	cases := []struct {
		name      string
		requested string
		gtidSet   string
		gtidOn    bool
		binlog    string
		want      string
		wantErr   bool
	}{
		{"auto prefers gtid", "auto", "uuid:1-5", true, "b.1", "gtid", false},
		{"auto falls back when gtid_mode off", "auto", "uuid:1-5", false, "b.1", "file-pos", false},
		{"auto falls back when no gtid set", "auto", "", true, "b.1", "file-pos", false},
		{"auto with nothing usable", "auto", "", false, "", "", true},
		{"explicit gtid ok", "gtid", "uuid:1-5", true, "", "gtid", false},
		{"explicit gtid without set", "gtid", "", true, "b.1", "", true},
		{"explicit gtid mode off", "gtid", "uuid:1-5", false, "b.1", "", true},
		{"explicit file-pos ok", "file-pos", "uuid:1-5", true, "b.1", "file-pos", false},
		{"explicit file-pos without binlog", "file-pos", "uuid:1-5", true, "", "", true},
		{"bogus mode", "chaos", "uuid:1-5", true, "b.1", "", true},
	}
	for _, c := range cases {
		got, err := chooseReplMode(c.requested, c.gtidSet, c.gtidOn, c.binlog)
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: expected error, got mode %q", c.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.name, err)
		} else if got != c.want {
			t.Errorf("%s: mode = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestBuildChangeSourceSQL_ModernGTID(t *testing.T) {
	info := ReplicationInfo{
		SourceHost: "primary1", SourcePort: 3306,
		ReplUser: "repl", ReplPassword: "s'cret",
		BinlogFile: "b.1", BinlogPos: 4,
	}
	stmt, redacted := buildChangeSourceSQL([3]int{8, 0, 43}, info, "u:1-5", "gtid",
		StartOptions{SourceSSL: true, GetSourcePublicKey: true})

	for _, want := range []string{
		"CHANGE REPLICATION SOURCE TO",
		"SOURCE_HOST = 'primary1'",
		"SOURCE_PORT = 3306",
		"SOURCE_USER = 'repl'",
		`SOURCE_PASSWORD = 's\'cret'`,
		"SOURCE_SSL = 1",
		"GET_SOURCE_PUBLIC_KEY = 1",
		"SOURCE_AUTO_POSITION = 1",
	} {
		if !strings.Contains(stmt, want) {
			t.Errorf("stmt missing %q:\n%s", want, stmt)
		}
	}
	if strings.Contains(stmt, "SOURCE_LOG_FILE") {
		t.Error("gtid mode must not set SOURCE_LOG_FILE")
	}
	if strings.Contains(redacted, "cret") || !strings.Contains(redacted, "<redacted>") {
		t.Errorf("redacted statement leaks the password:\n%s", redacted)
	}
}

func TestBuildChangeSourceSQL_LegacyFilePos(t *testing.T) {
	info := ReplicationInfo{
		SourceHost: "primary1",
		ReplUser:   "repl", ReplPassword: "x",
		BinlogFile: "mysql-bin.000007", BinlogPos: 98765,
	}
	stmt, _ := buildChangeSourceSQL([3]int{5, 7, 44}, info, "", "file-pos",
		StartOptions{GetSourcePublicKey: true})

	for _, want := range []string{
		"CHANGE MASTER TO",
		"MASTER_HOST = 'primary1'",
		"MASTER_PORT = 3306", // default port
		"MASTER_LOG_FILE = 'mysql-bin.000007'",
		"MASTER_LOG_POS = 98765",
	} {
		if !strings.Contains(stmt, want) {
			t.Errorf("stmt missing %q:\n%s", want, stmt)
		}
	}
	// 5.7 has no caching_sha2_password: the public-key clause must be omitted.
	if strings.Contains(stmt, "PUBLIC_KEY") {
		t.Errorf("5.7 statement must not contain a public-key clause:\n%s", stmt)
	}
	if strings.Contains(stmt, "SOURCE_") {
		t.Errorf("5.7 statement must use MASTER_* keywords only:\n%s", stmt)
	}
}

func TestBuildChangeSourceSQL_LegacyKeywordsUpTo8022(t *testing.T) {
	info := ReplicationInfo{SourceHost: "h", ReplUser: "r", BinlogFile: "b.1", BinlogPos: 4}
	stmt, _ := buildChangeSourceSQL([3]int{8, 0, 22}, info, "u:1", "gtid",
		StartOptions{GetSourcePublicKey: true})
	if !strings.Contains(stmt, "CHANGE MASTER TO") || !strings.Contains(stmt, "MASTER_AUTO_POSITION = 1") {
		t.Errorf("8.0.22 must use legacy CHANGE MASTER syntax:\n%s", stmt)
	}
	if !strings.Contains(stmt, "GET_MASTER_PUBLIC_KEY = 1") {
		t.Errorf("8.0.22 supports GET_MASTER_PUBLIC_KEY:\n%s", stmt)
	}
}

func TestAssessReplicaStatus(t *testing.T) {
	healthy, hardErr, _ := assessReplicaStatus(map[string]string{
		"REPLICA_IO_RUNNING": "Yes", "REPLICA_SQL_RUNNING": "Yes",
		"LAST_IO_ERROR": "", "LAST_SQL_ERROR": "",
	})
	if !healthy || hardErr != "" {
		t.Errorf("both Yes should be healthy, got healthy=%v err=%q", healthy, hardErr)
	}

	healthy, hardErr, state := assessReplicaStatus(map[string]string{
		"REPLICA_IO_RUNNING": "Connecting", "REPLICA_SQL_RUNNING": "Yes",
	})
	if healthy || hardErr != "" || !strings.Contains(state, "Connecting") {
		t.Errorf("Connecting should be still-starting, got healthy=%v err=%q state=%q", healthy, hardErr, state)
	}

	_, hardErr, _ = assessReplicaStatus(map[string]string{
		"REPLICA_IO_RUNNING": "No", "REPLICA_SQL_RUNNING": "No",
		"LAST_IO_ERROR": "Access denied for user 'repl'",
	})
	if !strings.Contains(hardErr, "Access denied") {
		t.Errorf("Last_IO_Error should surface as hard error, got %q", hardErr)
	}

	// Legacy column names (5.7 / pre-8.0.22).
	healthy, _, _ = assessReplicaStatus(map[string]string{
		"SLAVE_IO_RUNNING": "Yes", "SLAVE_SQL_RUNNING": "Yes",
	})
	if !healthy {
		t.Error("legacy Slave_* columns should be recognised")
	}
}
