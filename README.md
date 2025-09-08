# go-dump

[![Build Status](https://travis-ci.org/martinarrieta/go-dump.svg?branch=master)](https://travis-ci.org/martinarrieta/go-dump)

**IMPORTANT!** The project is under development and there is a lot of work to do.

This is a parallel MySQL dump in Go.

You can either dump a database or multiple databases or a table or multiple tables.

## How it works?

go-dump uses the [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) capabilities from the storage engines (currently only [InnoDB](https://dev.mysql.com/doc/refman/5.7/en/innodb-multi-versioning.html)) to get a consistent state of a backup. The concept is very similar to [mydumper](https://github.com/maxbube/mydumper) but with some extra features that I found useful.

## Usage

The parameters and options are listed here:

```bash
Usage: go-dump  --destination path [--databases str] [--tables str] [--all-databases]
[--dry-run | --execute ] [--help] [--debug] [--quiet] [--version] [--lock-tables]
[--consistent] [--isolation-level str] [--channel-buffer-size num] [--chunk-size num]
[--tables-without-uniquekey str] [--threads num] [--mysql-user str] [--mysql-password str]
[--mysql-host str] [--mysql-port num] [--mysql-socket path] [--add-drop-table]
[--get-master-status] [--get-slave-status] [--output-chunk-size num] [--skip-use-database]
[--compress] [--compress-level] [--where str] [--ini-files str]

go-dump dumps a database or a table from a MySQL server and creates the SQL statements
to recreate a table. This tool create one file per table per thread in the destination directory
Example: go-dump --destination /tmp/dbdump --databases mydb --mysql-user myuser --mysql-password password
```

## Building

Use the provided Makefile for building:

```bash
# Build the binary
make build

# Build for Linux
make build-linux

# Run tests
make test

# Clean build artifacts
make clean

# Show all available targets
make help
```

The binary will be created in the `bin/` directory.

## Selective Dumping with WHERE Conditions

You can now use the `--where` flag to dump only specific rows that match a WHERE condition. The tool supports both global and table-specific WHERE conditions:

### Global WHERE Condition (applies to all tables)

```bash
# Using command line flag
./bin/go-dump --destination /tmp/dump --databases mydb --where "status = 'active'" --execute

# Using INI file
# Add to your config file:
# where = status = 'active' AND created_at > '2023-01-01'
./bin/go-dump --ini-file config.ini --destination /tmp/dump --execute
```

### Table-Specific WHERE Conditions

```bash
# Using command line flag
./bin/go-dump --destination /tmp/dump --databases mydb --where "sakila.customer:customer_id < 100,sakila.payment:amount > 10.00" --execute

# Using INI file
# Add to your config file:
# where = sakila.customer:customer_id < 100,sakila.payment:amount > 10.00
./bin/go-dump --ini-file config.ini --destination /tmp/dump --execute
```

### Examples of WHERE conditions

- **Global conditions:**
  - `--where "customer_id < 1000"`
  - `--where "status = 'active' AND created_at > '2023-01-01'"`

- **Table-specific conditions:**
  - `--where "sakila.customer:customer_id < 100"`
  - `--where "sakila.customer:active = 1,sakila.payment:amount > 10.00"`
  - `--where "mydb.users:status = 'active',mydb.orders:total > 100"`

## Options description

### General

- `--help` - Display this message. Default [false]
- `--dry-run` - Just calculate the number of chunks per table and display it. Default [false]
- `--execute` - Execute the dump. Default [false]
- `--debug` - Display debug information. Default [false]
- `--quiet` - Do not display INFO messages during the process. Default [false]
- `--version` - Display version and exit. Default [false]
- `--lock-tables` - Lock tables to get consistent backup. Default [true]
- `--channel-buffer-size` - Task channel buffer size. Default [1000]
- `--chunk-size` - Chunk size to get the rows. Default [1000]
- `--tables-without-uniquekey` - Action to have with tables without any primary or unique key. Valid actions are: 'error', 'single-chunk'. Default [error]
- `--threads` - Number of threads to use. Default [1]
- `--compress` - Enable compression to the output files. Default [false]
- `--compress-level` - Compression level from 1 (best speed) to 9 (best compression). Default [1]
- `--consistent` - Get a consistent backup. Default [true]
- `--isolation-level` - Isolation level to use. If you need a consistent backup, leave the default 'REPEATABLE READ', other options READ COMMITTED, READ UNCOMMITTED and SERIALIZABLE. Default [REPEATABLE READ]
- `--where` - Custom WHERE condition for selective dumping (e.g., "status = 'active'").
- `--ini-file` - INI file to read the configuration options.

### MySQL options

- `--mysql-user` - MySQL user name. Default [root]
- `--mysql-password` - MySQL password.
- `--mysql-host` - MySQL hostname. Default [localhost]
- `--mysql-port` - MySQL port number. Default [3306]
- `--mysql-socket` - MySQL socket file.

### Databases or tables to dump

- `--all-databases` - Dump all the databases. Default [false]
- `--databases` - List of comma separated databases to dump.
- `--tables` - List of comma separated tables to dump. Each table should have the database name included, for example "mydb.mytable,mydb2.mytable2".

### Output options

- `--destination` - Directory to store the dumps.
- `--add-drop-table` - Add drop table before create table. Default [false]
- `--get-master-status` - Get the master data. Default [true]
- `--get-slave-status` - Get the slave data. Default [false]
- `--output-chunk-size` - Chunk size to output the rows. Default [0]
- `--skip-use-database` - Skip USE "database" in the dump. Default [false]

## Download

Each release includes pre-built binaries. You can check the [latest release on GitHub](https://github.com/martinarrieta/go-dump/releases) and download them.

## Examples

```bash
go-dump --threads 8 --chunk-size 50000 --output-chunk-size 1000  --channel-buffer-size  2000 --tables-without-uniquekey "single-chunk" --add-drop-table  --databases "test" --mysql-user root --destination /tmp/testbackup   --execute   --skip-use-database

2018-04-08 01:40:44 INFO Locking tables to get a consistent backup.
2018-04-08 01:40:44 INFO Getting Master Status
2018-04-08 01:40:44 INFO Cols [File Position Binlog_Do_DB Binlog_Ignore_DB Executed_Gtid_Set]
2018-04-08 01:40:44 INFO Master File: binlog.000008
Master Position: 154
2018-04-08 01:40:44 INFO Unlocking the tables. Tables were locked for 5.197763ms
2018-04-08 01:40:44 INFO Starting 8 workers
2018-04-08 01:40:46 INFO Status. Queue: 0 of 80
2018-04-08 01:40:49 INFO Status. Queue: 50 of 120
2018-04-08 01:40:54 INFO Status. Queue: 10 of 120
2018-04-08 01:41:00 INFO Waiting for the creation of all the chunks.
2018-04-08 01:41:00 INFO Execution time: 15.498141583s
```

### Explanation of this command

This command will execute 8 threads `--threads 8`, it will read in chunks of 50000 rows `--chunk-size 50000` and it will write in chunks of 1000 rows `--output-chunk-size 1000`, the buffer for the chunks it will be 2000 `--channel-buffer-size  2000` and the tables without a primary or unique key will be done in a single chunk `--tables-without-uniquekey "single-chunk"`. It will add the drop table command `--add-drop-table` and the database that it will backup it is "test" `--databases "test"`. The user to connect to the mysql database is "root" `--mysql-user root` and the destination directory is "/tmp/testbackup" `--destination /tmp/testbackup`. We want to execute `--execute` the backup and we don't want to add the "USE DATABASE" command on each file `--skip-use-database`.

## Focus of this project

The main focus of this project is to be able to make and restore consistent logical backups from MySQL.
