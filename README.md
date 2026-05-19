# localdb

[![Test](https://github.com/josephvusich/localdb/actions/workflows/test.yml/badge.svg)](https://github.com/josephvusich/localdb/actions/workflows/test.yml)

A simple SQLite wrapper for Go with built-in schema versioning.

## Install

```
go get github.com/josephvusich/localdb/v2
```

## SQLite driver

As of v2, localdb no longer imports a SQLite driver itself. Callers must blank-import a driver that registers itself with `database/sql`:

```go
import _ "github.com/mattn/go-sqlite3" // cgo-based, registered as "sqlite3"
// or
import _ "modernc.org/sqlite"          // pure Go, registered as "sqlite"
```

Then pass the registered driver name via `OpenOptions.DriverName`. The field is required — `Open` returns an error if it is empty.

## Usage

### Define a schema and open a database

```go
import (
    "log"

    "github.com/josephvusich/localdb/v2"
    _ "github.com/mattn/go-sqlite3"
)

schema := localdb.NewSqlSchema(`CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL
)`)

db, err := localdb.Open(localdb.OpenOptions{
    File:       "app.db",
    Schema:     schema,
    DriverName: "sqlite3",
})
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

On first open, the root schema is executed automatically. On subsequent opens, the existing database is validated against the schema and upgraded if needed.

### Schema upgrades

Register incremental upgrades starting from version 2. Version 0 is the default on an uninitialized database (the state before the root schema is applied), and version 1 is the root schema passed to `NewSqlSchema`, so the first upgrade is version 2:

```go
schema := localdb.NewSqlSchema(`CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
)`)

schema.DefineUpgrade(2, `ALTER TABLE users ADD COLUMN email TEXT;`)
schema.DefineUpgrade(3, `ALTER TABLE users ADD COLUMN active INTEGER DEFAULT 1;`)
```

Upgrades run in a transaction when the database is opened. If any upgrade step fails, the transaction is rolled back.

### Upgrade hooks

Use `DefinePreUpgrade` and `DefinePostUpgrade` to run Go code before or after a version's SQL, within the same transaction. This is useful for tasks like backfilling new columns:

```go
schema.DefineUpgrade(2, `ALTER TABLE users ADD COLUMN email TEXT;`)

schema.DefinePostUpgrade(2, func(tx sqlx.Ext) error {
    _, err := tx.Exec(`UPDATE users SET email = name || '@example.com'`)
    return err
})
```

If a hook returns an error, the entire upgrade transaction is rolled back.

### Backup before upgrade

To back up the database before running schema upgrades, set `BackupDir`.

```go
db, err := localdb.Open(localdb.OpenOptions{
    File:      "app.db",
    Schema:    schema,
    BackupDir: "backups",
})

// Backups follow the pattern "backups/app.before_v2_upgrade.db"
```

### Version tracking

By default, localdb uses the SQLite `application_id` PRAGMA to store the schema ID and `user_version` to store the schema version. This behavior can be altered by providing a custom `VersionStorer` implementation via `OpenOptions.VersionStorer`.

### Queries

Use `Handle()` to access the underlying `sqlx.DB`:

```go
var name string
err := db.Handle().QueryRowx(`SELECT name FROM users WHERE id = ?`, 1).Scan(&name)
```

### Transactions

`WrapTx` commits on success and rolls back on error or panic:

```go
err := db.WrapTx(func(tx sqlx.Ext) error {
    _, err := tx.Exec(`INSERT INTO users (name, email) VALUES (?, ?)`, "alice", "alice@example.com")
    return err
})
```

## Upgrading from v1

v2 is a breaking release that removes the bundled `github.com/mattn/go-sqlite3` import and requires callers to choose a driver explicitly. To upgrade:

1. Change the import path from `github.com/josephvusich/localdb` to `github.com/josephvusich/localdb/v2`.
2. Add a blank import of your chosen SQLite driver to your `main` package (or any package that runs at startup):

   ```go
   import _ "github.com/mattn/go-sqlite3"
   ```

   Without this, `Open` will fail at runtime with `sql: unknown driver "sqlite3" (forgotten import?)`.

3. Set `OpenOptions.DriverName` on every `Open` call. The field is required; `Open` returns an error if it is empty. For the v1 behavior:

   ```go
   db, err := localdb.Open(localdb.OpenOptions{
       File:       "app.db",
       Schema:     schema,
       DriverName: "sqlite3",
   })
   ```

4. To use a different driver, register it under your chosen name and pass that name. For example, `modernc.org/sqlite` (pure Go, no cgo) registers itself as `"sqlite"`:

   ```go
   import _ "modernc.org/sqlite"

   db, err := localdb.Open(localdb.OpenOptions{
       File:       "app.db",
       Schema:     schema,
       DriverName: "sqlite",
   })
   ```

   Note that DSN option syntax differs between drivers (e.g. mattn accepts `_busy_timeout=250`, modernc uses `_pragma=busy_timeout(250)`).
