package dump

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type TableStatus string

const (
	TableStatusPending TableStatus = "pending"
	TableStatusDone    TableStatus = "done"
	TableStatusFailed  TableStatus = "failed"
)

// TableMetadata tracks per-table dump state. Used for resume and checksum verification.
type TableMetadata struct {
	Schema      string      `json:"schema"`
	Name        string      `json:"name"`
	RowEstimate uint64      `json:"row_estimate"`
	Chunks      uint64      `json:"chunks"`
	Status      TableStatus `json:"status"`
	Checksum    int64       `json:"checksum,omitempty"`
}

// DumpMetadata is written to <destination>/metadata.json throughout the dump run.
// It is the source of truth for resume logic and restore verification.
type DumpMetadata struct {
	GoDumpVersion  string           `json:"go_dump_version"`
	StartTime      time.Time        `json:"start_time"`
	EndTime        *time.Time       `json:"end_time,omitempty"`
	Status         string           `json:"status"` // "in_progress" | "complete" | "failed"
	MySQLHost      string           `json:"mysql_host"`
	MySQLVersion   string           `json:"mysql_version"`
	BinlogFile     string           `json:"binlog_file,omitempty"`
	BinlogPosition int              `json:"binlog_position,omitempty"`
	GTIDSet        string           `json:"gtid_set,omitempty"`
	CharacterSet   string           `json:"character_set"`
	Objects        map[string]int   `json:"objects,omitempty"` // "triggers"/"routines"/"events" -> count dumped
	Tables         []*TableMetadata `json:"tables"`

	mu   sync.Mutex
	path string
}

// NewDumpMetadata initialises a metadata object and writes the initial in_progress file.
func NewDumpMetadata(destDir, appVersion, mysqlHost, mysqlVersion string) *DumpMetadata {
	m := &DumpMetadata{
		GoDumpVersion: appVersion,
		StartTime:     time.Now().UTC(),
		Status:        "in_progress",
		MySQLHost:     mysqlHost,
		MySQLVersion:  mysqlVersion,
		CharacterSet:  "utf8mb4",
		path:          destDir + "/metadata.json",
	}
	// Best-effort initial write — don't fatal here, the dump is more important.
	_ = m.write()
	return m
}

// AddTable registers a table in the metadata with pending status.
func (m *DumpMetadata) AddTable(schema, name string, rowEstimate uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Tables = append(m.Tables, &TableMetadata{
		Schema:      schema,
		Name:        name,
		RowEstimate: rowEstimate,
		Status:      TableStatusPending,
	})
}

// MarkTableDone updates a table's status and chunk count, then flushes the file.
func (m *DumpMetadata) MarkTableDone(schema, name string, chunks uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.Tables {
		if t.Schema == schema && t.Name == name {
			t.Status = TableStatusDone
			t.Chunks = chunks
			break
		}
	}
	_ = m.write()
}

// MergeDoneTables copies tables recorded as done in a prior run's metadata into
// m, so a resumed run's metadata (and checksums) still describe the entire dump
// set on disk — not just the tables re-dumped in this run.
func (m *DumpMetadata) MergeDoneTables(prior *DumpMetadata) {
	if prior == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	existing := make(map[string]bool, len(m.Tables))
	for _, t := range m.Tables {
		existing[t.Schema+"."+t.Name] = true
	}
	for _, t := range prior.Tables {
		if t.Status == TableStatusDone && !existing[t.Schema+"."+t.Name] {
			carried := *t
			m.Tables = append(m.Tables, &carried)
		}
	}
}

// TablesSnapshot returns a copy of the current table entries for safe
// iteration without holding the metadata lock.
func (m *DumpMetadata) TablesSnapshot() []TableMetadata {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]TableMetadata, 0, len(m.Tables))
	for _, t := range m.Tables {
		out = append(out, *t)
	}
	return out
}

// SetBinlog records the binlog position captured at the consistent snapshot point.
func (m *DumpMetadata) SetBinlog(file string, position int, gtidSet string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.BinlogFile = file
	m.BinlogPosition = position
	m.GTIDSet = gtidSet
}

// SetChecksum records the CHECKSUM TABLE result for a table.
func (m *DumpMetadata) SetChecksum(schema, name string, checksum int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.Tables {
		if t.Schema == schema && t.Name == name {
			t.Checksum = checksum
			break
		}
	}
}

// SetObjects records how many triggers/routines/events were dumped.
func (m *DumpMetadata) SetObjects(counts map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Objects = counts
	_ = m.write()
}

// Complete marks the dump finished and writes the final metadata file.
func (m *DumpMetadata) Complete() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	m.EndTime = &now
	m.Status = "complete"
	_ = m.write()
}

// Fail marks the dump as failed and writes the metadata file.
func (m *DumpMetadata) Fail() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	m.EndTime = &now
	m.Status = "failed"
	_ = m.write()
}

// ResumeState returns two sets derived from a prior run's metadata:
//   - done: "schema.table" keys that completed successfully (safe to skip)
//   - inProgress: "schema.table" keys that were pending/in_progress (partial files exist)
func (m *DumpMetadata) ResumeState() (done map[string]bool, inProgress map[string]bool) {
	done = make(map[string]bool)
	inProgress = make(map[string]bool)
	for _, t := range m.Tables {
		key := t.Schema + "." + t.Name
		if t.Status == TableStatusDone {
			done[key] = true
		} else {
			inProgress[key] = true
		}
	}
	return
}

// LoadDumpMetadata reads an existing metadata.json for resume logic.
func LoadDumpMetadata(destDir string) (*DumpMetadata, error) {
	path := destDir + "/metadata.json"
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m DumpMetadata
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("metadata.json parse error: %w", err)
	}
	m.path = path
	return &m, nil
}

// Write serialises the metadata to a temp file and renames it atomically.
func (m *DumpMetadata) Write() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.write()
}

// write is the internal locked version. Caller must hold m.mu.
func (m *DumpMetadata) write() error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}

// mysqlVersionString formats the [3]int version array as "X.Y.Z".
func mysqlVersionString(v [3]int) string {
	return fmt.Sprintf("%d.%d.%d", v[0], v[1], v[2])
}
