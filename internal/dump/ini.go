package dump

import (
	"os"
	"strconv"
	"strings"

	"github.com/ChaosHour/go-dump/internal/log"
	ini "gopkg.in/ini.v1"
)

// getPasswordFromFile reads the mysql-password value directly from the raw INI file,
// working around gopkg.in/ini.v1's special handling of '#' and ';' in values.
// Handles both "mysql-password=value" and "mysql-password = value" (spaces around =).
func getPasswordFromFile(iniFile string) string {
	content, err := os.ReadFile(iniFile)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(content), "\n") {
		line = strings.TrimSpace(line)
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == "mysql-password" {
			value := strings.TrimSpace(parts[1])
			if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
				return value[1 : len(value)-1]
			}
			return value
		}
	}
	return ""
}

// ParseIniFile loads opts from an INI file, skipping keys already set via flags.
func ParseIniFile(iniFile string, do *DumpOptions, flagSet map[string]bool) {
	cfg, err := ini.Load(iniFile)
	if err != nil {
		log.Errorf("Failed to read the ini file %s: %s", iniFile, err.Error())
	}

	for _, section := range cfg.Sections() {
		switch section.Name() {
		case "client", "mysqldump":
			parseMySQLIniOptions(section, do, flagSet)
		case "go-dump":
			parseIniOptions(section, do, flagSet, iniFile)
		}
	}
}

func parseMySQLIniOptions(section *ini.Section, do *DumpOptions, flagSet map[string]bool) {
	var err error
	for _, key := range section.Keys() {
		if flagSet["mysql-"+key.Name()] {
			continue
		}
		switch key.Name() {
		case "user":
			do.MySQLCredentials.User = key.Value()
		case "password":
			do.MySQLCredentials.Password = strings.Trim(key.Value(), "\"")
		case "host":
			do.MySQLHost.HostName = key.Value()
		case "port":
			if key.Value() != "" {
				do.MySQLHost.Port, err = strconv.Atoi(key.Value())
				if err != nil {
					log.Fatalf("Port number %s can not be converted to integer. Error: %s", key.Value(), err.Error())
				}
			}
		case "socket":
			do.MySQLHost.SocketFile = key.Value()
		}
	}
}

func parseIniOptions(section *ini.Section, do *DumpOptions, flagSet map[string]bool, iniFile string) {
	var errInt, errBool error
	for _, key := range section.Keys() {
		if flagSet[key.Name()] {
			continue
		}
		switch key.Name() {
		case "mysql-user":
			do.MySQLCredentials.User = key.Value()
		case "mysql-password":
			// Raw file read preserves passwords containing '#' or ';' (ini comment chars).
			// Fall back to the parsed value for passwords without special characters.
			if pwd := getPasswordFromFile(iniFile); pwd != "" {
				do.MySQLCredentials.Password = pwd
			} else {
				do.MySQLCredentials.Password = key.Value()
			}
		case "mysql-host":
			do.MySQLHost.HostName = key.Value()
		case "mysql-port":
			if key.Value() != "" {
				do.MySQLHost.Port, errInt = strconv.Atoi(key.Value())
			}
		case "mysql-socket":
			do.MySQLHost.SocketFile = key.Value()
		case "threads":
			if key.Value() != "" {
				do.Threads, errInt = strconv.Atoi(key.Value())
			}
		case "chunk-size":
			do.ChunkSize, errInt = strconv.ParseUint(key.Value(), 10, 64)
		case "output-chunk-size":
			do.OutputChunkSize, errInt = strconv.ParseUint(key.Value(), 10, 64)
		case "lock-tables":
			do.LockTables, errBool = strconv.ParseBool(key.Value())
		case "tables-without-uniquekey":
			do.TablesWithoutUKOption = key.Value()
		case "destination":
			do.DestinationDir = key.Value()
		case "skip-use-database":
			do.SkipUseDatabase, errBool = strconv.ParseBool(key.Value())
		case "get-master-status":
			do.GetMasterStatus, errBool = strconv.ParseBool(key.Value())
		case "get-slave-status":
			do.GetSlaveStatus, errBool = strconv.ParseBool(key.Value())
		case "add-drop-table":
			do.AddDropTable, errBool = strconv.ParseBool(key.Value())
		case "insert-mode":
			if key.Value() != "" {
				do.InsertMode = key.Value()
			}
		case "triggers":
			do.DumpTriggers, errBool = strconv.ParseBool(key.Value())
		case "routines":
			do.DumpRoutines, errBool = strconv.ParseBool(key.Value())
		case "events":
			do.DumpEvents, errBool = strconv.ParseBool(key.Value())
		case "skip-definer":
			do.SkipDefiner, errBool = strconv.ParseBool(key.Value())
		case "checksum":
			do.Checksum, errBool = strconv.ParseBool(key.Value())
		case "resume":
			do.Resume, errBool = strconv.ParseBool(key.Value())
		case "compress":
			do.Compress, errBool = strconv.ParseBool(key.Value())
		case "compress-format":
			if key.Value() != "" {
				do.CompressFormat = key.Value()
			}
		case "compress-level":
			if key.Value() != "" {
				do.CompressLevel, errInt = strconv.Atoi(key.Value())
			}
		case "consistent":
			do.Consistent, errBool = strconv.ParseBool(key.Value())
		case "where":
			whereValue := key.Value()
			if strings.Contains(whereValue, ":") {
				if do.WhereConditions == nil {
					do.WhereConditions = make(map[string]string)
				}
				for _, part := range strings.Split(whereValue, ",") {
					if tableCond := strings.SplitN(strings.TrimSpace(part), ":", 2); len(tableCond) == 2 {
						do.WhereConditions[NormalizeTableName(tableCond[0])] = tableCond[1]
					}
				}
			} else {
				do.GlobalWhereCondition = whereValue
			}
		case "tables":
			do.TemporalOptions.Tables = key.Value()
		case "databases":
			do.TemporalOptions.Databases = key.Value()
		case "isolation-level":
			do.TemporalOptions.IsolationLevel = key.Value()
		case "all-databases":
			do.TemporalOptions.AllDatabases, errBool = strconv.ParseBool(key.Value())
		case "include-system-databases":
			do.TemporalOptions.IncludeSystemDatabases, errBool = strconv.ParseBool(key.Value())
		case "debug":
			do.TemporalOptions.Debug, errBool = strconv.ParseBool(key.Value())
		case "dry-run":
			do.TemporalOptions.DryRun, errBool = strconv.ParseBool(key.Value())
		case "execute":
			do.TemporalOptions.Execute, errBool = strconv.ParseBool(key.Value())
		case "quiet":
			do.TemporalOptions.Quiet, errBool = strconv.ParseBool(key.Value())
		default:
			log.Warningf("Unknown option %s", key.Name())
		}

		if errInt != nil {
			log.Fatalf("Variable %s with value %s can not be converted to integer: %s",
				key.Name(), key.Value(), errInt.Error())
		}
		if errBool != nil {
			log.Fatalf("Variable %s with value %s can not be converted to boolean: %s",
				key.Name(), key.Value(), errBool.Error())
		}
	}
}
