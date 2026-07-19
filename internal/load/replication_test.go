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
