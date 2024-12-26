package localdb

import (
	"database/sql"
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

type Handle interface {
	sqlx.Ext

	Prepare(string) (*sql.Stmt, error)
	Preparex(string) (*sqlx.Stmt, error)
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
	// the form of "${BASENAME}.before_v%d_upgrade.${EXT}",
	// where %d is, and ${BASENAME} and ${EXT} are derived
	// from File. If File is "test.db", the resulting backup
	// name for v1 => v2 would be "test.before_v2_upgrade.db".
	// If File has no extension, the backup file will also be
	// extensionless.
	//
	// Note that in versions prior to 1.4.0, the format for
	// backup filenames was different, and the example above
	// would have resulted in "before_v2_upgrade.test.db".
	// The behavior was changed to enable backups to lexically
	// sort alongside the main database file, and to retain a
	// dot-prefix if the main database file was also dot-prefixed.
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

// WrapTx begins a new transaction and invokes fn().
// If fn() panics or returns an error, the transaction is
// discarded and the error is returned.
// Any error while discarding the transaction will trigger
// a panic.
// fn() should not attempt to discard or commit the underlying
// transaction.
// If fn() returns nil, WrapTx returns any error encountered
// while committing the transaction, or nil on success.
//
// fn must have one of the following signatures:
//
//	func(sqlx.Ext) error
//	func(Handle) error
//
// If fn does not have one of the above signatures, WrapTx
// will panic without attempting to begin a transaction.
func (d *DB) WrapTx(fn any) error {
	var f func(Handle) error

	switch fn := fn.(type) {
	case func(Handle) error:
		f = fn
		break
	case func(sqlx.Ext) error:
		f = func(h Handle) error {
			return fn(h)
		}
		break
	default:
		panic("invalid function signature passed to WrapTx")
	}

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

func (d *DB) Handle() Handle {
	return d.root
}

func (d *DB) Close() error {
	return d.root.Close()
}
