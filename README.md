# localdb

A simple SQLite3 wrapper for Go with built-in schema versioning.

## Install

```
go get github.com/josephvusich/localdb
```

## Usage

### Define a schema and open a database

```go
schema := localdb.NewSqlSchema(`CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL
)`)

db, err := localdb.Open(localdb.OpenOptions{
    File:   "app.db",
    Schema: schema,
})
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

On first open, the root schema is executed automatically. On subsequent opens, the existing database is validated against the schema and upgraded if needed.

### Schema upgrades

Register incremental upgrades starting from version 2:

```go
schema := localdb.NewSqlSchema(`CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
)`)

schema.DefineUpgrade(2, `ALTER TABLE users ADD COLUMN email TEXT;`)
schema.DefineUpgrade(3, `ALTER TABLE users ADD COLUMN active INTEGER DEFAULT 1;`)
```

Upgrades run in a transaction when the database is opened. If any upgrade step fails, the transaction is rolled back.

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
