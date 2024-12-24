package localdb

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var errDetectPanic = errors.New("this should never happen")

type DB struct {
	opened time.Time
	root   *sqlx.DB
	schema Schema
}

type OpenOptions struct {
	// File provides the path to the database itself.
	File string

	// Schema to use for database upgrades.
	Schema Schema

	// Optional, defaults to SqliteVersion.
	VersionStorer VersionStorer

	// If BackupDir is non-empty, a backup database
	// will be created in the specified directory prior
	// to attempting a schema upgrade. Backup files take
	// the form of "before_v%d_upgrade.%s", where %d is
	// Schema.LatestVersion(), and %s is the result of
	// calling filepath.Base on File.
	BackupDir string

	// Connection options, see https://github.com/mattn/go-sqlite3
	// These are added to any baked-in options in File.
	DSNOptions map[string]string

	// MaxOpenConns sets the maximum number of open connections to the database.
	// If MaxOpenConns <= -1, there is no limit. If MaxOpenConns == 0, the limit will be
	// set to 1 (the default.)
	MaxOpenConns int
}

func assembleDSN(inputDSN string, dsnOpts map[string]string) (dsn string, err error) {
	parts := strings.SplitN(inputDSN, "?", 2)

	if len(parts) == 1 && (dsnOpts == nil || len(dsnOpts) == 0) {
		return inputDSN, nil
	}

	var flags url.Values
	if len(parts) == 2 {
		flags, err = url.ParseQuery(parts[1])
		if err != nil {
			return "", fmt.Errorf("unable to parse input DSN: %w", err)
		}
	} else {
		flags = url.Values{}
	}

	if dsnOpts != nil {
		for k, v := range dsnOpts {
			flags.Add(k, v)
		}
	}

	dsn = parts[0]
	if len(flags) != 0 {
		dsn += "?" + flags.Encode()
	}
	return dsn, nil
}

// Open creates or opens a database file using the provided SqlSchema.
// The database schema is upgraded to match SqlSchema, if older.
// Upgrading the schema always happens under transaction.
// If the database contains a higher version number than SqlSchema,
// or a non-zero, non-matching application_id, the upgrade
// transaction is discarded, the database is closed, and the error
// is returned.
//
// Note that PRAGMA application_id and user_version are reserved
// for use by this library, and are set to the current SqlSchema's
// schemaId and version, respectively.
func Open(options OpenOptions) (*DB, error) {
	now := time.Now()

	dsn, err := assembleDSN(options.File, options.DSNOptions)
	if err != nil {
		return nil, fmt.Errorf("error assembling DSN: %w", err)
	}

	sq, err := sqlx.Open("sqlite3", fmt.Sprintf("file:%s", dsn))
	if err != nil {
		return nil, err
	}
	var once sync.Once
	defer once.Do(func() {
		if err := sq.Close(); err != nil {
			panic(err)
		}
	})

	if options.MaxOpenConns == 0 {
		sq.SetMaxOpenConns(1)
	} else {
		sq.SetMaxOpenConns(options.MaxOpenConns)
	}

	db := &DB{
		opened: now,
		root:   sq,
		schema: options.Schema.Copy(),
	}

	vs := options.VersionStorer
	if vs == nil {
		vs = &SqliteVersion{}
	}

	if err = initDB(db, options, vs); err != nil {
		return nil, err
	}

	once.Do(func() {
		// nothing
	})
	return db, nil
}

// WrapTx begins a new transaction and invokes f.
// If f() panics or returns an error, the transaction is
// discarded and the error is returned.
// Any error while discarding the transaction will trigger
// a panic.
// f() should not attempt to discard or commit the underlying
// transaction.
// If f() returns nil, WrapTx returns any error encountered
// while committing the transaction, or nil on success.
func (d *DB) WrapTx(f func(tx sqlx.Ext) error) error {
	tx, err := d.root.Beginx()
	if err != nil {
		return err
	}

	var once sync.Once
	defer once.Do(func() {
		if err := tx.Rollback(); err != nil {
			panic(err)
		}
	})

	if err = f(tx); err != nil {
		return err
	}

	err = errDetectPanic
	once.Do(func() {
		err = tx.Commit()
	})
	if errors.Is(err, errDetectPanic) {
		panic("logic error")
	}
	return err
}

func (d *DB) Handle() sqlx.Ext {
	return d.root
}

func (d *DB) Close() error {
	return d.root.Close()
}
