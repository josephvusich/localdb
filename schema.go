package localdb

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"

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

// UpgradeHook is a callback invoked before or after a schema upgrade step.
// It receives the transaction handle and can return an error to abort the upgrade.
type UpgradeHook func(tx sqlx.Ext) error

type versionHooks struct {
	pre  UpgradeHook
	post UpgradeHook
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
	hooks    map[int]versionHooks
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
		hooks:         make(map[int]versionHooks),
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

// DefinePreUpgrade registers a callback to run before the SQL for the given
// version. The callback runs within the same transaction as the upgrade.
// Panics if the version is out of range or a pre-upgrade hook is already defined.
func (s *SqlSchema) DefinePreUpgrade(version int, fn UpgradeHook) {
	if version < 1 || version > len(s.versions) {
		panic("DefinePreUpgrade version out of range")
	}
	h := s.hooks[version]
	if h.pre != nil {
		panic("pre-upgrade hook already defined")
	}
	h.pre = fn
	s.hooks[version] = h
}

// DefinePostUpgrade registers a callback to run after the SQL for the given
// version. The callback runs within the same transaction as the upgrade.
// Panics if the version is out of range or a post-upgrade hook is already defined.
func (s *SqlSchema) DefinePostUpgrade(version int, fn UpgradeHook) {
	if version < 1 || version > len(s.versions) {
		panic("DefinePostUpgrade version out of range")
	}
	h := s.hooks[version]
	if h.post != nil {
		panic("post-upgrade hook already defined")
	}
	h.post = fn
	s.hooks[version] = h
}

func backupFilename(options OpenOptions, schema Schema) string {
	base := filepath.Base(options.File)
	ext := filepath.Ext(base)
	base = strings.TrimSuffix(base, ext)
	return filepath.Join(options.BackupDir, fmt.Sprintf("%s.before_v%d_upgrade%s", base, schema.LatestVersion(), ext))
}

func initDB(db *DB, options OpenOptions, vs VersionStorer) error {
	schema := options.Schema

	applicationId, err := vs.GetApplicationId(db.Handle())
	if err != nil {
		return err
	}

	if applicationId != 0 && applicationId != schema.ApplicationID() {
		return fmt.Errorf("application_id (%d) does not match schema ID (%d)", applicationId, schema.ApplicationID())
	}

	userVersion, err := vs.GetUserVersion(db.Handle())
	if err != nil {
		return err
	}

	if userVersion > schema.LatestVersion() {
		return fmt.Errorf("user_version (%d) is higher than the schema version (%d)", userVersion, schema.LatestVersion())
	}

	if applicationId == schema.ApplicationID() && userVersion == schema.LatestVersion() {
		return nil
	}

	if options.BackupDir != "" && applicationId != 0 && userVersion != 0 {
		os.MkdirAll(options.BackupDir, 0755)
		backupFile := backupFilename(options, schema)
		if _, err = db.Handle().Exec(`VACUUM INTO ?`, backupFile); err != nil {
			return fmt.Errorf("unable to create backup %s: %w", backupFile, err)
		}
	}

	return db.WrapTx(func(tx sqlx.Ext) error {
		if err = vs.SetApplicationId(tx, schema.ApplicationID()); err != nil {
			return err
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
		version := int(i) + 1
		if h, ok := s.hooks[version]; ok && h.pre != nil {
			if err := h.pre(tx); err != nil {
				return -1, fmt.Errorf("error during v%d pre-upgrade hook: %w", version, err)
			}
		}
		if _, err := tx.Exec(s.versions[i]); err != nil {
			return -1, fmt.Errorf("error during v%d schema upgrade: %w", version, err)
		}
		if h, ok := s.hooks[version]; ok && h.post != nil {
			if err := h.post(tx); err != nil {
				return -1, fmt.Errorf("error during v%d post-upgrade hook: %w", version, err)
			}
		}
	}

	return newVersion, nil
}

func (s *SqlSchema) Copy() Schema {
	dupe := make([]string, len(s.versions))
	copy(dupe, s.versions)
	dupeHooks := make(map[int]versionHooks, len(s.hooks))
	for k, v := range s.hooks {
		dupeHooks[k] = v
	}
	return &SqlSchema{
		ID:       s.ID,
		versions: dupe,
		hooks:    dupeHooks,
		legacy:   s.legacy,
	}
}
