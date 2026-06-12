package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/ChaosHour/go-dump/internal/dump"
	"github.com/ChaosHour/go-dump/internal/load"
	"github.com/ChaosHour/go-dump/internal/log"
	mysql "github.com/go-sql-driver/mysql"
	ini "gopkg.in/ini.v1"
)

// AppVersion is set at build time via -ldflags "-X main.AppVersion=<ver>".
var AppVersion = "dev"

func main() {
	var (
		host     string
		port     int
		user     string
		password string
		socket   string
		database string

		directory  string
		file       string
		pattern    string
		workers    int
		dataOnly   bool
		verify     bool
		resume     bool
		quiet      bool
		debug      bool
		iniFile    string
		flagVersion bool
	)

	flag.StringVar(&host, "host", "localhost", "MySQL host")
	flag.IntVar(&port, "port", 3306, "MySQL port")
	flag.StringVar(&user, "user", "root", "MySQL user")
	flag.StringVar(&password, "password", "", "MySQL password (or set GOLOAD_PASSWORD env var)")
	flag.StringVar(&socket, "socket", "", "MySQL Unix socket path (overrides --host/--port)")
	flag.StringVar(&database, "database", "", "Target database name. If set, execute USE <db> on each connection instead of relying on USE statements in SQL files.")
	flag.StringVar(&directory, "directory", "", "Directory containing SQL files to load")
	flag.StringVar(&file, "file", "", "Single SQL file to load")
	flag.StringVar(&pattern, "pattern", "*.sql", "File glob pattern for data files in --directory. Use '*.sql.gz' for compressed dumps.")
	flag.IntVar(&workers, "workers", 4, "Number of parallel workers for data files")
	flag.BoolVar(&dataOnly, "data-only", false, "Skip schema (definition) files; load data files only")
	flag.BoolVar(&verify, "verify", false, "Verify checksums after loading (requires checksums.txt in --directory)")
	flag.BoolVar(&resume, "resume", false, "Resume a previous load: skip files recorded in load-state.json")
	flag.BoolVar(&quiet, "quiet", false, "Suppress INFO messages")
	flag.BoolVar(&debug, "debug", false, "Print debug information")
	flag.StringVar(&iniFile, "ini-file", "", "INI configuration file (supports [client] and [go-load] sections)")
	flag.BoolVar(&flagVersion, "version", false, "Print version and exit")
	flag.Parse()

	if flagVersion {
		fmt.Println("go-load version:", AppVersion)
		return
	}

	// Track explicitly-set flags so INI file doesn't clobber them.
	flagSet := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { flagSet[f.Name] = true })

	if iniFile != "" {
		host, port, user, password, socket = parseIni(iniFile, host, port, user, password, socket, flagSet)
	}

	// Env var fallback — avoids passwords in shell history.
	if password == "" && !flagSet["password"] {
		if env := os.Getenv("GOLOAD_PASSWORD"); env != "" {
			password = env
		}
	}

	if debug {
		log.SetLevel(log.DEBUG)
	} else if quiet {
		log.SetLevel(log.WARNING)
	} else {
		log.SetLevel(log.INFO)
	}

	if file == "" && directory == "" {
		log.Fatal("Specify --file or --directory. Use --help for usage.")
	}

	db, err := connect(host, port, user, password, socket, database)
	if err != nil {
		log.Fatalf("Cannot connect to MySQL: %v", err)
	}
	db.SetMaxOpenConns(workers + 2)
	db.SetMaxIdleConns(workers + 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Warning("Interrupt received — cancelling in-flight operations...")
		cancel()
	}()

	imp := load.New(db, workers, dataOnly)
	defer imp.Close()

	if directory != "" {
		// Log useful context from the source dump's metadata if present.
		if meta, err := dump.LoadDumpMetadata(directory); err == nil {
			log.Infof("Source: MySQL %s on %s, dump status: %s",
				meta.MySQLVersion, meta.MySQLHost, meta.Status)
			if meta.BinlogFile != "" {
				log.Infof("Binlog position: %s:%d  GTID: %s",
					meta.BinlogFile, meta.BinlogPosition, meta.GTIDSet)
			}
		}

		var ls *load.LoadState
		if resume {
			ls, err = load.NewLoadState(directory)
			if err != nil {
				log.Fatalf("Cannot initialise resume state: %v", err)
			}
			log.Infof("Resume: %d file(s) already loaded", ls.Len())
		}

		if err := imp.ImportDirectory(ctx, directory, pattern, ls); err != nil {
			log.Fatalf("Load failed: %v", err)
		}

		if verify {
			log.Info("Verifying checksums against target database...")
			if err := dump.VerifyChecksums(directory, db); err != nil {
				log.Fatalf("Checksum verification failed:\n  %v", err)
			}
			log.Info("Checksum verification passed.")
		}
	} else {
		if err := imp.ImportFile(ctx, file); err != nil {
			log.Fatalf("Load failed: %v", err)
		}
	}

	log.Info("Load complete.")
}

// connect opens and pings a MySQL connection using a properly-built DSN.
func connect(host string, port int, user, password, socket, database string) (*sql.DB, error) {
	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.DBName = database
	cfg.AllowNativePasswords = true
	cfg.ParseTime = true

	if socket != "" {
		cfg.Net = "unix"
		cfg.Addr = socket
	} else {
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("%s:%d", host, port)
	}

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// parseIni reads connection settings from an INI file.
// Supports [client] and [go-load] sections with mysql-host/host style keys.
// Flag values always take precedence.
func parseIni(path, host string, port int, user, password, socket string, flagSet map[string]bool) (string, int, string, string, string) {
	cfg, err := ini.Load(path)
	if err != nil {
		log.Warningf("Cannot load INI file %s: %v", path, err)
		return host, port, user, password, socket
	}

	applySection := func(section *ini.Section) {
		for _, key := range section.Keys() {
			switch key.Name() {
			case "mysql-host", "host":
				if !flagSet["host"] {
					host = key.Value()
				}
			case "mysql-port", "port":
				if !flagSet["port"] {
					if v, err := strconv.Atoi(key.Value()); err == nil {
						port = v
					}
				}
			case "mysql-user", "user":
				if !flagSet["user"] {
					user = key.Value()
				}
			case "mysql-password", "password":
				if !flagSet["password"] {
					password = key.Value()
				}
			case "mysql-socket", "socket":
				if !flagSet["socket"] {
					socket = key.Value()
				}
			}
		}
	}

	for _, section := range cfg.Sections() {
		switch section.Name() {
		case "client", "go-load":
			applySection(section)
		}
	}
	return host, port, user, password, socket
}
