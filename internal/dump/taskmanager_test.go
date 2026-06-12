package dump

import (
	"database/sql"
	"os"
	"sync"
	"testing"
)

func getMySQLHost() *MySQLHost {
	return &MySQLHost{
		HostName: "127.0.0.1",
		Port:     3306,
	}
}

func getMySQLCredentials() *MySQLCredentials {
	return &MySQLCredentials{
		User:     "root",
		Password: "s3cr3t",
	}
}

func getDumpOptions() *DumpOptions {
	return &DumpOptions{
		MySQLHost:             getMySQLHost(),
		MySQLCredentials:      getMySQLCredentials(),
		Threads:               1,
		ChunkSize:             1000,
		OutputChunkSize:       1000,
		ChannelBufferSize:     1000,
		LockTables:            true,
		TablesWithoutUKOption: "single-chunk",
		DestinationDir:        "/tmp/testbackup",
		AddDropTable:          true,
		GetMasterStatus:       true,
		GetSlaveStatus:        false,
		SkipUseDatabase:       false,
		Compress:              false,
		CompressLevel:         0,
		IsolationLevel:        sql.LevelRepeatableRead,
		Consistent:            true,
		WhereConditions:       make(map[string]string),
		GlobalWhereCondition:  "",
	}
}

var dumpOptions = getDumpOptions()

var tmdb, _ = GetMySQLConnection(dumpOptions.MySQLHost, dumpOptions.MySQLCredentials)

var cDataChunk = make(chan DataChunk, dumpOptions.ChannelBufferSize)

var wgCreateChunks sync.WaitGroup
var wgProcessChunks sync.WaitGroup

var taskManager = NewTaskManager(
	&wgCreateChunks,
	&wgProcessChunks,
	cDataChunk,
	tmdb,
	dumpOptions)

// TestCreateTaskManager is an integration test requiring MySQL at 127.0.0.1:3306
// with root/s3cr3t. Run with: go test ./internal/dump/ -run TestCreateTaskManager
func TestCreateTaskManager(t *testing.T) {
	if tmdb == nil {
		t.Skip("MySQL not available at 127.0.0.1:3306 — skipping integration test")
	}
	if _, err := os.Stat(taskManager.DestinationDir); os.IsNotExist(err) {
		os.MkdirAll(taskManager.DestinationDir, 0755)
	}
	taskManager.AddWorkersDB()
	taskManager.GetTransactions(true, false)
}

func TestLoadIniFile(t *testing.T) {
	testOptions := getDumpOptions()

	skipUser := map[string]bool{"mysql-user": true}

	ParseIniFile("../../test/test.ini", testOptions, skipUser)

	if testOptions.Threads != 3 {
		t.Errorf("Threads should be 3")
	}

	if testOptions.MySQLCredentials.User != dumpOptions.MySQLCredentials.User {
		t.Errorf("MySQL user shouldn't change.")
	}
}
