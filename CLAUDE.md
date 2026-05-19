# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

localdb is a Go library providing a lightweight SQLite wrapper with built-in schema versioning. It is a single-package library (`package localdb`) with no sub-packages.

As of v2, the module path is `github.com/josephvusich/localdb/v2`. The library no longer bundles a SQLite driver — callers blank-import their own (`mattn/go-sqlite3`, `modernc.org/sqlite`, etc.) and must specify the registered driver name via `OpenOptions.DriverName` (required; `Open` errors if empty).

## Commands

- **Run tests:** `make test` (runs `go vet ./...` then `go test ./...`)
- **Run a single test:** `go test -run TestDBTestSuite/TestOpen ./...`
- **Install:** `make install` (runs tests first)

## Dependencies

- `github.com/jmoiron/sqlx` — extended SQL library (sqlx.DB, sqlx.Tx, sqlx.Stmt)
- `github.com/mattn/go-sqlite3` — test-only driver (registered in `driver_test.go`). The library itself does not import any SQLite driver.
- `github.com/stretchr/testify` — test assertions and suite runner

## Architecture

### Core types (db.go)

- **DB** — main struct wrapping `*sqlx.DB` with a `Schema` for versioning. Created via `Open(OpenOptions)`.
- **Handle** — interface combining `sqlx.Ext` + `Preparer`, satisfied by both `*sqlx.DB` and `*sqlx.Tx`. Used throughout to abstract over transactional/non-transactional contexts.
- **StmtCache** — thread-safe (`sync.Map`) prepared statement cache. Wraps a preparer function and returns `Stmt` handles that remove themselves from the cache on `Close()`.
- **WrapTx()** — transaction helper that auto-commits on success, rolls back on error/panic. Accepts both typed (`func(tx) (T, error)`) and untyped (`func(tx) error`) callbacks.

### Schema versioning (schema.go, version.go)

- **Schema** — interface: `ApplicationID()`, `LatestVersion()`, `Copy()`, `Upgrade(tx, currentVersion) (newVersion, error)`.
- **SqlSchema** — concrete implementation using ordered SQL strings. ApplicationID is derived from CRC32C of the root schema. Upgrades are registered via `DefineUpgrade(version, sql)`.
- **VersionStorer/VersionReader** — interfaces for reading/writing schema version. Default implementation `SqliteVersion` uses SQLite PRAGMAs (`application_id`, `user_version`).
- **FallbackVersion** — decorator that tries a primary `VersionStorer`, falls back to a secondary `VersionReader`. Used for migrating legacy databases.

### Database lifecycle

`Open()` → creates/opens SQLite file → reads current version via `VersionStorer` → if version < latest, optionally backs up the file → runs `Schema.Upgrade()` in a transaction → stores new version.

## Testing

Tests use `testify/suite` (`DBTestSuite`). Each test gets an isolated temp directory via `suite.T().TempDir()`. Run the full suite with `make test` or target individual tests with `-run TestDBTestSuite/TestName`.
