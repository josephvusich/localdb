package localdb

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var errDetectPanic = errors.New("this should never happen")

type DB struct {
	opened time.Time
	root   *sqlx.DB
	schema *RawSchema
}

type OpenOptions struct {
	AssumeVersion int32
}

// Open creates or opens a database file using the provided RawSchema.
// The database schema is upgraded to match RawSchema, if older.
// Upgrading the schema always happens under transaction.
// If the database contains a higher version number than RawSchema,
// or a non-zero, non-matching application_id, the upgrade
// transaction is discarded, the database is closed, and the error
// is returned.
//
// Note that PRAGMA application_id and user_version are reserved
// for use by this library, and are set to the current RawSchema's
// schemaId and version, respectively.
func Open(file string, schema *RawSchema, vs VersionStorer) (*DB, error) {
	now := time.Now()

	sq, err := sqlx.Open("sqlite3", fmt.Sprintf("file:%s", file))
	if err != nil {
		return nil, err
	}
	var once sync.Once
	defer once.Do(func() {
		if err := sq.Close(); err != nil {
			panic(err)
		}
	})
	sq.SetMaxOpenConns(1)

	db := &DB{
		opened: now,
		root:   sq,
		schema: schema.copy(),
	}

	if err = initDB(db, schema, vs); err != nil {
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
