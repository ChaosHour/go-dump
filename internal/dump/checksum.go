package dump

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/ChaosHour/go-dump/internal/log"
)

// ChecksumResult holds the result of a single CHECKSUM TABLE call.
type ChecksumResult struct {
	Schema    string
	Name      string
	Checksum  int64
	Timestamp time.Time
}

// RunChecksums runs CHECKSUM TABLE for every task and writes <dest>/checksums.txt.
// It also updates the metadata file with each table's checksum value.
// This runs after UNLOCK TABLES so it does not extend the lock window.
func RunChecksums(tasks []*Task, db *sql.DB, dest string, meta *DumpMetadata) error {
	log.Info("Running table checksums...")
	log.Info("Note: CHECKSUM TABLE reads the current table state — values only match the dumped data if no writes occurred since the snapshot.")

	var results []ChecksumResult
	for _, task := range tasks {
		fullName := task.Table.GetFullName()
		// CHECKSUM TABLE cannot be parameterised; name is backtick-escaped via GetFullName.
		row := db.QueryRow(fmt.Sprintf("CHECKSUM TABLE %s", fullName))

		var tableName string
		var checksum sql.NullInt64
		if err := row.Scan(&tableName, &checksum); err != nil {
			log.Warningf("CHECKSUM TABLE %s failed: %v", fullName, err)
			continue
		}
		if !checksum.Valid {
			log.Warningf("CHECKSUM TABLE %s returned NULL (table may be empty or use non-InnoDB engine)", fullName)
			continue
		}

		result := ChecksumResult{
			Schema:    task.Table.GetUnescapedSchema(),
			Name:      task.Table.GetUnescapedName(),
			Checksum:  checksum.Int64,
			Timestamp: time.Now().UTC(),
		}
		results = append(results, result)

		if meta != nil {
			meta.SetChecksum(result.Schema, result.Name, result.Checksum)
		}

		log.Debugf("Checksum %s.%s = %d", result.Schema, result.Name, result.Checksum)
	}

	// Cover tables completed by a prior run when resuming, so checksums.txt
	// keeps describing the whole dump set: carry an existing checksum over, or
	// compute one now if the prior run died before its checksum phase.
	// (A checksum of 0 — empty table — is indistinguishable from "absent" and
	// is recomputed; that costs one scan of an empty table.)
	if meta != nil {
		current := make(map[string]bool, len(results))
		for _, r := range results {
			current[r.Schema+"."+r.Name] = true
		}
		for _, t := range meta.TablesSnapshot() {
			if t.Status != TableStatusDone || current[t.Schema+"."+t.Name] {
				continue
			}
			checksumVal := t.Checksum
			if checksumVal == 0 {
				fullName := fmt.Sprintf("`%s`.`%s`", t.Schema, t.Name)
				var tableName string
				var fresh sql.NullInt64
				if err := db.QueryRow(fmt.Sprintf("CHECKSUM TABLE %s", fullName)).Scan(&tableName, &fresh); err != nil || !fresh.Valid {
					log.Warningf("CHECKSUM TABLE %s (carried from prior run) failed: %v", fullName, err)
					continue
				}
				checksumVal = fresh.Int64
				meta.SetChecksum(t.Schema, t.Name, checksumVal)
			}
			results = append(results, ChecksumResult{
				Schema:    t.Schema,
				Name:      t.Name,
				Checksum:  checksumVal,
				Timestamp: time.Now().UTC(),
			})
		}
	}

	if err := writeChecksumFile(dest+"/checksums.txt", results); err != nil {
		return fmt.Errorf("writing checksums.txt: %w", err)
	}

	log.Infof("Checksums written for %d tables → %s/checksums.txt", len(results), dest)
	return nil
}

// writeChecksumFile writes results atomically to path.
// Format per line: schema.table <TAB> checksum <TAB> timestamp
func writeChecksumFile(path string, results []ChecksumResult) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	fmt.Fprintf(f, "# go-dump checksum file — generated %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(f, "# Format: schema.table\\tchecksum\\ttimestamp\n")
	for _, r := range results {
		fmt.Fprintf(f, "%s.%s\t%d\t%s\n", r.Schema, r.Name, r.Checksum, r.Timestamp.Format(time.RFC3339))
	}

	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// VerifyChecksums reads <dest>/checksums.txt and compares each entry against
// CHECKSUM TABLE on the provided database connection. Returns any mismatches as errors.
func VerifyChecksums(dest string, db *sql.DB) error {
	path := dest + "/checksums.txt"
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}

	var mismatches []string
	for _, line := range splitLines(string(data)) {
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		var schemaTable, ts string
		var expected int64
		if _, err := fmt.Sscanf(line, "%s\t%d\t%s", &schemaTable, &expected, &ts); err != nil {
			log.Warningf("Skipping malformed checksum line: %q", line)
			continue
		}

		// Re-quote the table name safely.
		var schema, name string
		if n, _ := fmt.Sscanf(schemaTable, "%s", &schemaTable); n != 1 {
			continue
		}
		// schemaTable is "schema.name" — split on first dot.
		for i, c := range schemaTable {
			if c == '.' {
				schema = schemaTable[:i]
				name = schemaTable[i+1:]
				break
			}
		}
		if schema == "" || name == "" {
			log.Warningf("Skipping unparseable table name: %q", schemaTable)
			continue
		}

		fullName := fmt.Sprintf("`%s`.`%s`", schema, name)
		var tbl string
		var actual sql.NullInt64
		if err := db.QueryRow(fmt.Sprintf("CHECKSUM TABLE %s", fullName)).Scan(&tbl, &actual); err != nil {
			mismatches = append(mismatches, fmt.Sprintf("%s: query error: %v", schemaTable, err))
			continue
		}
		if !actual.Valid || actual.Int64 != expected {
			mismatches = append(mismatches, fmt.Sprintf("%s: expected %d, got %v", schemaTable, expected, actual))
		} else {
			log.Debugf("Checksum OK: %s = %d", schemaTable, expected)
		}
	}

	if len(mismatches) > 0 {
		return fmt.Errorf("checksum verification failed:\n  %s",
			joinLines(mismatches, "\n  "))
	}
	log.Infof("All checksums verified OK (%s)", path)
	return nil
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i, c := range s {
		if c == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func joinLines(ss []string, sep string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += sep
		}
		out += s
	}
	return out
}
