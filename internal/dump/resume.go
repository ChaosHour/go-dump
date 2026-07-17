package dump

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/ChaosHour/go-dump/internal/log"
)

// applyResumeFilter loads a prior run's metadata.json from destDir, removes already-done
// tables from tablesToParse, and cleans up partial chunk files from interrupted tables.
// Returns the filtered table set and the prior metadata (nil when none was usable) so
// the caller can merge already-done tables into the new run's metadata.
func applyResumeFilter(destDir string, tablesToParse map[string]bool) (map[string]bool, *DumpMetadata) {
	prior, err := LoadDumpMetadata(destDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Warningf("--resume: no metadata.json found in %s — running full dump.", destDir)
		} else {
			log.Warningf("--resume: could not load metadata.json: %v — running full dump.", err)
		}
		return tablesToParse, nil
	}

	if prior.Status == "complete" {
		log.Warningf("--resume: prior dump in %s is already marked complete. Use a new destination or remove metadata.json to force a fresh dump.", destDir)
	}

	done, inProgress := prior.ResumeState()

	// Remove partial chunk files left by the interrupted run.
	if len(inProgress) > 0 {
		cleanPartialFiles(destDir, inProgress)
	}

	// Build the filtered table set.
	filtered := make(map[string]bool, len(tablesToParse))
	skipped := 0
	for t := range tablesToParse {
		if done[t] {
			log.Infof("Resume: skipping completed table %s", t)
			skipped++
		} else {
			filtered[t] = true
		}
	}

	log.Infof("Resume: %d table(s) already complete, %d to dump this run.", skipped, len(filtered))
	if skipped > 0 && len(filtered) > 0 {
		log.Warning("Resume: tables dumped in this run use a NEW snapshot — the combined dump is not consistent to a single point in time.")
	}
	return filtered, prior
}

// cleanPartialFiles removes chunk files in destDir for the given "schema.table" keys.
// It matches any file whose base name (without .sql, .sql.gz, or .sql.zst) starts with a
// table key, covering: schema.table-threadN.sql, schema.table-definition.sql, schema.table.sql.
func cleanPartialFiles(destDir string, tables map[string]bool) {
	entries, err := os.ReadDir(destDir)
	if err != nil {
		log.Warningf("Resume: could not read destination directory %s: %v", destDir, err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Strip extensions to get the table-name prefix.
		base := strings.TrimSuffix(name, ".gz")
		base = strings.TrimSuffix(base, ".zst")
		base = strings.TrimSuffix(base, ".sql")

		for tableKey := range tables {
			// base is "schema.table-thread0", "schema.table-definition", or "schema.table"
			if base == tableKey || strings.HasPrefix(base, tableKey+"-") {
				path := filepath.Join(destDir, name)
				log.Infof("Resume: removing partial file %s", name)
				if err := os.Remove(path); err != nil {
					log.Warningf("Resume: could not remove %s: %v", path, err)
				}
				break
			}
		}
	}
}
