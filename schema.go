package localdb

import (
	"fmt"
	"hash/crc32"

	"github.com/jmoiron/sqlx"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type SchemaLegacyHelper func(q sqlx.Queryer) (applicationId int32, userVersion int32, err error)

type Schema interface {
	// ApplicationID should be unique to
	// this Schema. SqlSchema uses the CRC32
	// of the root schema script.
	ApplicationID() int32

	// LatestVersion returns the highest
	// version of a database supported
	// by the Schema.
	LatestVersion() int32

	// Copy must perform a deep copy,
	// ensuring that a given database
	// connection Schema will not be
	// modified by other writers.
	Copy() Schema

	// Upgrade the database, if necessary.
	// Returns the new version, which may
	// be the same as the current version.
	Upgrade(tx sqlx.Ext, currentVersion int32) (updatedVersion int32, err error)
}

type SqlSchema struct {
	// Default value is the CRC32C of rootSchema.
	// Must not be altered once this SqlSchema is
	// referenced by an open DB.
	ID int32

	// VersionStorer is set to SqliteVersion by default.
	// May be overridden prior to use to assist with
	// migrating existing databases.
	VersionStorer VersionStorer

	versions []string
	legacy   SchemaLegacyHelper
}

func (s *SqlSchema) ApplicationID() int32 {
	return s.ID
}

// NewSqlSchema initializes a new SQLite schema.
func NewSqlSchema(rootSchema string) *SqlSchema {
	return &SqlSchema{
		ID:            int32(crc32.Checksum([]byte(rootSchema), crc32cTable)),
		VersionStorer: &SqliteVersion{},
		versions:      []string{rootSchema},
	}
}

// DefineUpgrade registers a new version of the schema. DefineUpgrade only
// affects subsequent Open calls, already-opened databases are
// not affected. The root SqlSchema version is always 1, so the
// first call to DefineUpgrade must have a newVersion equal to 2.
// newSchema should contain all instructions necessary to
// alter or migrate data to the new schema version.
func (s *SqlSchema) DefineUpgrade(newVersion int, newSchema string) {
	if len(s.versions)+1 != newVersion {
		panic("non-incremental DefineUpgrade version")
	}
	s.versions = append(s.versions, newSchema)
}

func initDB(db *DB, schema Schema, vs VersionStorer) error {
	return db.WrapTx(func(tx sqlx.Ext) error {
		applicationId, err := vs.GetApplicationId(tx)
		if err != nil {
			return err
		}

		if applicationId != 0 && applicationId != schema.ApplicationID() {
			return fmt.Errorf("application_id (%d) does not match schema ID (%d)", applicationId, schema.ApplicationID())
		}

		if err = vs.SetApplicationId(tx, schema.ApplicationID()); err != nil {
			return err
		}

		userVersion, err := vs.GetUserVersion(tx)
		if err != nil {
			return err
		}

		if userVersion > schema.LatestVersion() {
			return fmt.Errorf("user_version (%d) is higher than the schema version (%d)", userVersion, schema.LatestVersion())
		}

		newVersion, err := schema.Upgrade(tx, userVersion)
		if err != nil {
			return err
		}

		return vs.SetUserVersion(tx, newVersion)
	})
}

func (s *SqlSchema) LatestVersion() int32 {
	return int32(len(s.versions))
}

func (s *SqlSchema) Upgrade(tx sqlx.Ext, currentVersion int32) (newVersion int32, err error) {
	newVersion = s.LatestVersion()

	for i := currentVersion; i < newVersion; i++ {
		if _, err := tx.Exec(s.versions[i]); err != nil {
			return -1, err
		}
	}

	return newVersion, nil
}

func (s *SqlSchema) Copy() Schema {
	dupe := make([]string, len(s.versions))
	copy(dupe, s.versions)
	return &SqlSchema{
		ID:       s.ID,
		versions: dupe,
		legacy:   s.legacy,
	}
}
