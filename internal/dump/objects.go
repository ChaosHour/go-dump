package dump

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/ChaosHour/go-dump/internal/log"
)

// Object dumping: triggers, stored procedures/functions, and events.
//
// Each object is captured with SHOW CREATE ... and written mysqldump-style:
// sql_mode / charset guards around every definition, and DELIMITER ;; blocks
// because the bodies contain semicolons. File naming follows mydumper:
//
//	<schema>.<table>-triggers.sql   triggers, grouped per table
//	<schema>-routines.sql           procedures and functions, per schema
//	<schema>-events.sql             events, per schema
//
// go-load applies these after all data files, so triggers never fire during
// the restore.
//
// Object definitions are not MVCC-protected: unlike table data, a definition
// changed between the lock window and this read is captured in its newer
// form. The window is the few milliseconds between snapshot and these reads.

// definerRe matches the DEFINER clause of CREATE TRIGGER/PROCEDURE/FUNCTION/
// EVENT statements: DEFINER=CURRENT_USER, DEFINER=user@host, with the user and
// host parts optionally backtick-, single- or double-quoted.
var definerRe = regexp.MustCompile(`(?i)\s+DEFINER\s*=\s*(?:CURRENT_USER(?:\(\))?|` +
	"(?:`(?:[^`]|``)*`" + `|'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"|[0-9A-Za-z$_]+)` +
	`\s*@\s*` +
	"(?:`(?:[^`]|``)*`" + `|'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"|[0-9A-Za-z$_.%-]+))`)

// StripDefiner removes the DEFINER=... clause from a CREATE statement so the
// object loads on servers where the definer account does not exist. The object
// is then created with the invoker as definer.
func StripDefiner(stmt string) string {
	return definerRe.ReplaceAllString(stmt, "")
}

// showCreateObject runs a SHOW CREATE ... query and returns the single result
// row keyed by lower-cased column name. Columns that are NULL (e.g. the body
// when the dump user lacks privileges to see it) are absent from the map.
func showCreateObject(db *sql.DB, query string) (map[string]string, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("no rows returned")
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(cols))
	for i, c := range cols {
		if vals[i].Valid {
			out[strings.ToLower(c)] = vals[i].String
		}
	}
	return out, nil
}

// writeObjectGuardsHeader saves the session state that each definition below
// overrides, mirroring mysqldump so definitions execute under the sql_mode and
// charset they were created with, not the restore session's.
func writeObjectGuardsHeader(b *Buffer) {
	fmt.Fprintf(b, "/*!50003 SET @saved_cs_client = @@character_set_client */;\n")
	fmt.Fprintf(b, "/*!50003 SET @saved_col_connection = @@collation_connection */;\n")
	fmt.Fprintf(b, "/*!50003 SET @saved_sql_mode = @@sql_mode */;\n")
}

func writeObjectGuardsFooter(b *Buffer) {
	fmt.Fprintf(b, "/*!50003 SET sql_mode = @saved_sql_mode */;\n")
	fmt.Fprintf(b, "/*!50003 SET character_set_client = @saved_cs_client */;\n")
	fmt.Fprintf(b, "/*!50003 SET collation_connection = @saved_col_connection */;\n")
}

// writeObjectDefinition writes one object's creation-context SETs, an optional
// DROP, and the CREATE statement wrapped in DELIMITER ;; (bodies contain
// semicolons; go-load and the mysql client both understand the directive).
func writeObjectDefinition(b *Buffer, def map[string]string, createKey, dropSQL string, skipDefiner bool) {
	if cs := def["character_set_client"]; cs != "" {
		fmt.Fprintf(b, "/*!50003 SET character_set_client = %s */;\n", cs)
	}
	if col := def["collation_connection"]; col != "" {
		fmt.Fprintf(b, "/*!50003 SET collation_connection = %s */;\n", col)
	}
	// sql_mode can legitimately be the empty string; write it either way so
	// the definition never inherits the restore session's mode.
	sqlMode := def["sql_mode"]
	fmt.Fprintf(b, "/*!50003 SET sql_mode = '%s' */;\n", strings.ReplaceAll(sqlMode, "'", "''"))

	if dropSQL != "" {
		fmt.Fprintf(b, "%s;\n", dropSQL)
	}
	createSQL := def[createKey]
	if skipDefiner {
		createSQL = StripDefiner(createSQL)
	}
	fmt.Fprintf(b, "DELIMITER ;;\n%s;;\nDELIMITER ;\n", createSQL)
}

// WriteObjectsSQL dumps triggers, routines, and events per the Dump* options.
// It returns per-kind counts of objects written, or nil when no object dumping
// was requested. Discovery or SHOW CREATE failures (typically missing
// privileges) skip the object with a warning; file-write failures abort the
// dump, exactly like the schema files.
func (tm *TaskManager) WriteObjectsSQL() map[string]int {
	opts := tm.DumpOptions
	if !opts.DumpTriggers && !opts.DumpRoutines && !opts.DumpEvents {
		return nil
	}
	counts := make(map[string]int)
	if opts.DumpTriggers {
		counts["triggers"] = tm.writeTriggers()
	}
	if opts.DumpRoutines {
		counts["routines"] = tm.writeRoutines()
	}
	if opts.DumpEvents {
		counts["events"] = tm.writeEvents()
	}
	return counts
}

// schemasInPool returns the sorted distinct schemas of the tables being dumped.
func (tm *TaskManager) schemasInPool() []string {
	set := make(map[string]bool)
	for _, task := range tm.tasksPool {
		set[task.Table.GetUnescapedSchema()] = true
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// newObjectBuffer opens a dump file for object definitions and writes the
// shared USE / guard header.
func (tm *TaskManager) newObjectBuffer(fileName, schema string) (*Buffer, error) {
	bufferOptions := tm.GetBufferOptions()
	bufferOptions.Path = filepath.Join(tm.DestinationDir, fileName)
	buffer, err := NewBuffer(bufferOptions)
	if err != nil {
		return nil, err
	}
	if !tm.SkipUseDatabase {
		fmt.Fprintf(buffer, "%s;\n", GetUseDatabaseSQL(escapeIdentifier(schema)))
	}
	writeObjectGuardsHeader(buffer)
	return buffer, nil
}

// writeTriggers dumps the triggers of every table in the task pool, one
// <schema>.<table>-triggers.sql file per table that has readable triggers.
// ACTION_ORDER is preserved so FOLLOWS/PRECEDES chains recreate correctly.
func (tm *TaskManager) writeTriggers() int {
	total := 0
	for _, task := range tm.tasksPool {
		schema := task.Table.GetUnescapedSchema()
		table := task.Table.GetUnescapedName()

		names, err := queryStrings(tm.DB, `SELECT TRIGGER_NAME
			FROM information_schema.TRIGGERS
			WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?
			ORDER BY EVENT_MANIPULATION, ACTION_TIMING, ACTION_ORDER`, schema, table)
		if err != nil {
			log.Warningf("Skipping triggers for %s.%s — cannot list them: %v", schema, table, err)
			continue
		}
		if len(names) == 0 {
			continue
		}

		var defs []map[string]string
		for _, name := range names {
			def, err := showCreateObject(tm.DB, fmt.Sprintf("SHOW CREATE TRIGGER %s.%s",
				escapeIdentifier(schema), escapeIdentifier(name)))
			if err != nil || def["sql original statement"] == "" {
				log.Warningf("Skipping trigger %s.%s — definition not readable (missing TRIGGER privilege?): %v",
					schema, name, err)
				continue
			}
			defs = append(defs, def)
		}
		if len(defs) == 0 {
			continue
		}

		buffer, err := tm.newObjectBuffer(fmt.Sprintf("%s.%s-triggers.sql", schema, table), schema)
		if err != nil {
			log.Fatalf("Error creating triggers file for %s.%s: %v", schema, table, err)
		}
		for _, def := range defs {
			dropSQL := ""
			if tm.DumpOptions.AddDropTable {
				dropSQL = fmt.Sprintf("/*!50032 DROP TRIGGER IF EXISTS %s */", escapeIdentifier(def["trigger"]))
			}
			writeObjectDefinition(buffer, def, "sql original statement", dropSQL, tm.DumpOptions.SkipDefiner)
			total++
		}
		writeObjectGuardsFooter(buffer)
		if err := buffer.Close(); err != nil {
			log.Fatalf("Error finalising triggers file for %s.%s: %v", schema, table, err)
		}
		log.Infof("Dumped %d trigger(s) for %s.%s", len(defs), schema, table)
	}
	return total
}

// writeRoutines dumps stored procedures and functions, one
// <schema>-routines.sql file per schema that has readable routines. The body
// column of SHOW CREATE is NULL when the dump user is not the definer and
// lacks SELECT on mysql.proc (5.7) / SHOW_ROUTINE (8.0) — those are skipped
// with a warning.
func (tm *TaskManager) writeRoutines() int {
	total := 0
	for _, schema := range tm.schemasInPool() {
		rows, err := tm.DB.QueryContext(tm.ctx, `SELECT ROUTINE_NAME, ROUTINE_TYPE
			FROM information_schema.ROUTINES
			WHERE ROUTINE_SCHEMA = ?
			ORDER BY ROUTINE_TYPE, ROUTINE_NAME`, schema)
		if err != nil {
			log.Warningf("Skipping routines for %s — cannot list them: %v", schema, err)
			continue
		}
		type routine struct{ name, kind string }
		var routines []routine
		for rows.Next() {
			var r routine
			if err := rows.Scan(&r.name, &r.kind); err != nil {
				rows.Close()
				log.Fatalf("Error scanning routine row for %s: %v", schema, err)
			}
			routines = append(routines, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			log.Warningf("Skipping routines for %s — error listing them: %v", schema, err)
			continue
		}
		if len(routines) == 0 {
			continue
		}

		type routineDef struct {
			def        map[string]string
			kind, name string
			createKey  string
		}
		var defs []routineDef
		for _, r := range routines {
			// ROUTINE_TYPE is PROCEDURE or FUNCTION; it doubles as the SHOW
			// CREATE keyword and the result column name ("Create Procedure").
			def, err := showCreateObject(tm.DB, fmt.Sprintf("SHOW CREATE %s %s.%s",
				r.kind, escapeIdentifier(schema), escapeIdentifier(r.name)))
			createKey := "create " + strings.ToLower(r.kind)
			if err != nil || def[createKey] == "" {
				log.Warningf("Skipping %s %s.%s — body not readable (missing SHOW_ROUTINE/SELECT on mysql.proc?): %v",
					strings.ToLower(r.kind), schema, r.name, err)
				continue
			}
			defs = append(defs, routineDef{def: def, kind: r.kind, name: r.name, createKey: createKey})
		}
		if len(defs) == 0 {
			continue
		}

		buffer, err := tm.newObjectBuffer(schema+"-routines.sql", schema)
		if err != nil {
			log.Fatalf("Error creating routines file for %s: %v", schema, err)
		}
		for _, rd := range defs {
			dropSQL := ""
			if tm.DumpOptions.AddDropTable {
				dropSQL = fmt.Sprintf("/*!50003 DROP %s IF EXISTS %s */", rd.kind, escapeIdentifier(rd.name))
			}
			writeObjectDefinition(buffer, rd.def, rd.createKey, dropSQL, tm.DumpOptions.SkipDefiner)
			total++
		}
		writeObjectGuardsFooter(buffer)
		if err := buffer.Close(); err != nil {
			log.Fatalf("Error finalising routines file for %s: %v", schema, err)
		}
		log.Infof("Dumped %d routine(s) for schema %s", len(defs), schema)
	}
	return total
}

// writeEvents dumps events, one <schema>-events.sql file per schema that has
// readable events. Each event's time_zone is restored around its definition —
// schedules are evaluated in the creation time zone. Event scheduler state on
// the target is deliberately left alone.
func (tm *TaskManager) writeEvents() int {
	total := 0
	for _, schema := range tm.schemasInPool() {
		names, err := queryStrings(tm.DB, `SELECT EVENT_NAME
			FROM information_schema.EVENTS
			WHERE EVENT_SCHEMA = ?
			ORDER BY EVENT_NAME`, schema)
		if err != nil {
			log.Warningf("Skipping events for %s — cannot list them (missing EVENT privilege?): %v", schema, err)
			continue
		}
		if len(names) == 0 {
			continue
		}

		var defs []map[string]string
		for _, name := range names {
			def, err := showCreateObject(tm.DB, fmt.Sprintf("SHOW CREATE EVENT %s.%s",
				escapeIdentifier(schema), escapeIdentifier(name)))
			if err != nil || def["create event"] == "" {
				log.Warningf("Skipping event %s.%s — definition not readable (missing EVENT privilege?): %v",
					schema, name, err)
				continue
			}
			defs = append(defs, def)
		}
		if len(defs) == 0 {
			continue
		}

		buffer, err := tm.newObjectBuffer(schema+"-events.sql", schema)
		if err != nil {
			log.Fatalf("Error creating events file for %s: %v", schema, err)
		}
		fmt.Fprintf(buffer, "/*!50106 SET @saved_time_zone = @@time_zone */;\n")
		for _, def := range defs {
			if tz := def["time_zone"]; tz != "" {
				fmt.Fprintf(buffer, "/*!50106 SET time_zone = '%s' */;\n", strings.ReplaceAll(tz, "'", "''"))
			}
			dropSQL := ""
			if tm.DumpOptions.AddDropTable {
				dropSQL = fmt.Sprintf("/*!50106 DROP EVENT IF EXISTS %s */", escapeIdentifier(def["event"]))
			}
			writeObjectDefinition(buffer, def, "create event", dropSQL, tm.DumpOptions.SkipDefiner)
			total++
		}
		fmt.Fprintf(buffer, "/*!50106 SET time_zone = @saved_time_zone */;\n")
		writeObjectGuardsFooter(buffer)
		if err := buffer.Close(); err != nil {
			log.Fatalf("Error finalising events file for %s: %v", schema, err)
		}
		log.Infof("Dumped %d event(s) for schema %s", len(defs), schema)
	}
	return total
}

// queryStrings runs a single-column query and returns the values.
func queryStrings(db *sql.DB, query string, args ...interface{}) ([]string, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
