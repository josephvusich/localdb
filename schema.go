package localdb

import (
	"fmt"
	"hash/crc32"

	"github.com/jmoiron/sqlx"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type SchemaLegacyHelper func(q sqlx.Queryer) (applicationId int32, userVersion int32, err error)

type Schema interface {
	ApplicationID() int32
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

		newVersion, err := schema.Upgrade(tx, userVersion)
		if err != nil {
			return err
		}

		return vs.SetUserVersion(tx, newVersion)
	})
}

func (s *SqlSchema) Upgrade(tx sqlx.Ext, currentVersion int32) (newVersion int32, err error) {
	newVersion = int32(len(s.versions))

	for i := currentVersion; i < newVersion; i++ {
		if _, err := tx.Exec(s.versions[i]); err != nil {
			return -1, err
		}
	}

	return newVersion, nil
}

// Creates a deep copy.
func (s *SqlSchema) copy() *SqlSchema {
	dupe := make([]string, len(s.versions))
	copy(dupe, s.versions)
	return &SqlSchema{
		ID:       s.ID,
		versions: dupe,
		legacy:   s.legacy,
	}
}
