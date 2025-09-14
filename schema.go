package localdb

import (
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type SchemaLegacyHelper func(q sqlx.Queryer) (applicationId int32, userVersion int32, err error)

var (
	NoErrContinueUpgrade     = errors.New("incremental upgrade completed, more versions remaining")
	NoErrContinueUpgradeNoTx = fmt.Errorf("%w, next version must not have a transaction", NoErrContinueUpgrade)
)

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
	// If either of NoErrContinueUpgrade or
	// NoErrContinueUpgradeNoTx is returned,
	// Upgrade will be called again, with or
	// without a transaction, respectively.
	// Upgrade must always behave atomically,
	// and never leave the database in a non-
	// -upgradable state.
	// The tx parameter is always of underlying
	// type *UpgradeTx.
	Upgrade(tx sqlx.Ext, currentVersion int32) (updatedVersion int32, err error)
}

// UpgradeTx is the underlying implementation for Upgrade's tx parameter.
type UpgradeTx struct {
	sqlx.Ext

	// Tx returns true, unless the previous call to upgrade returned
	// NoErrContinueUpgradeNoTx.
	Tx bool
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
	noTx     map[int]bool
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
		noTx:          make(map[int]bool),
	}
}

type SqlSchemaUpgrade struct {
	Version int
	Schema  string

	// If NoTx is true, this particular upgrade will be executed without a transaction.
	NoTx bool
}

// DefineUpgradeEx is an extended form of DefineUpgrade, with additional options.
// See SqlSchemaUpgrade and DefineUpgrade for details.
func (s *SqlSchema) DefineUpgradeEx(upgrade SqlSchemaUpgrade) {
	s.DefineUpgrade(upgrade.Version, upgrade.Schema)
	s.noTx[upgrade.Version] = upgrade.NoTx
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

	if options.BackupDir != "" {
		os.MkdirAll(options.BackupDir, 0755)
		backupFile := backupFilename(options, schema)
		if _, err = db.Handle().Exec(`VACUUM INTO ?`, backupFile); err != nil {
			return fmt.Errorf("unable to create backup %s: %w", backupFile, err)
		}
	}

	// Always start under transaction unless Upgrade returns NoErrContinueUpgradeNoTx.
	withTx := true
	for continueUpgrade := true; continueUpgrade; {
		if !withTx {
			userVersion, continueUpgrade, withTx, err = upgradeStep(&UpgradeTx{db.Handle(), false}, schema, vs, userVersion)
		} else {
			err = db.WrapTx(func(tx sqlx.Ext) error {
				if err := vs.SetApplicationId(tx, schema.ApplicationID()); err != nil {
					return err
				}

				userVersion, continueUpgrade, withTx, err = upgradeStep(&UpgradeTx{tx, true}, schema, vs, userVersion)
				return err
			})
		}

		if err != nil {
			return err
		}
	}
	return nil
}

func upgradeStep(handle sqlx.Ext, schema Schema, vs VersionStorer, userVersion int32) (newVersion int32, continueUpgrade, withTx bool, err error) {
	newVersion, err = schema.Upgrade(handle, userVersion)
	if err != nil {
		if !errors.Is(err, NoErrContinueUpgrade) {
			return -1, false, false, err
		}
		continueUpgrade = true
		withTx = !errors.Is(err, NoErrContinueUpgradeNoTx)
		log.Println("Updated withTx:", withTx)
	}

	if err := vs.SetUserVersion(handle, newVersion); err != nil {
		panic(err)
	}

	return newVersion, continueUpgrade, withTx, nil
}

func (s *SqlSchema) LatestVersion() int32 {
	return int32(len(s.versions))
}

func (s *SqlSchema) Upgrade(tx sqlx.Ext, currentVersion int32) (newVersion int32, err error) {
	newVersion = s.LatestVersion()

	log.Println("Upgrading from", currentVersion, "to", newVersion)
	for i := currentVersion; i < newVersion; i++ {
		skipTx := s.noTx[int(i)+1]
		if skipTx == tx.(*UpgradeTx).Tx {
			if skipTx {
				log.Println("skipTx=true, bailing because we have a tx")
				return i, NoErrContinueUpgradeNoTx
			}
			log.Println("skipTx=false, bailing because we do not have a tx")
			return i, NoErrContinueUpgrade
		}

		if _, err := tx.Exec(s.versions[i]); err != nil {
			return -1, fmt.Errorf("error applying schema version %d: %w", i+1, err)
		}

		if skipTx {
			// Never run more than one step when operating without a transaction
			if s.noTx[int(i)+2] {
				return i + 1, NoErrContinueUpgradeNoTx
			}
			return i + 1, NoErrContinueUpgrade
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
