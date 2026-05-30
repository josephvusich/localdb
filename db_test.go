package localdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
)

type DBTestSuite struct {
	suite.Suite

	DBFile string
}

func TestDBTestSuite(t *testing.T) {
	suite.Run(t, new(DBTestSuite))
}

func (suite *DBTestSuite) SetupTest() {
	suite.DBFile = filepath.Join(suite.T().TempDir(), "test.db")
}

func (suite *DBTestSuite) TestBackupFilename() {
	cases := map[string]string{
		"foo/test.db":     "bar/test.before_v1_upgrade.db",
		"foo/test.foo.db": "bar/test.foo.before_v1_upgrade.db",
		"foo/test":        "bar/test.before_v1_upgrade",
	}

	schema := NewSqlSchema("")

	for in, out := range cases {
		suite.Require().Equal(out, backupFilename(OpenOptions{
			BackupDir: "bar",
			File:      in,
		}, schema))
	}
}

func (suite *DBTestSuite) TestAssembleDSN() {
	result, err := assembleDSN(suite.DBFile, nil)
	suite.Require().NoError(err)
	suite.Require().Equal(suite.DBFile, result)

	result, err = assembleDSN("test.db?foo=bar", nil)
	suite.Require().NoError(err)
	suite.Require().Equal("test.db?foo=bar", result)

	result, err = assembleDSN("test.db", url.Values{"fizz": {"buzz"}})
	suite.Require().NoError(err)
	suite.Require().Equal("test.db?fizz=buzz", result)

	result, err = assembleDSN("test.db?foo=bar", url.Values{"fizz": {"buzz"}})
	suite.Require().NoError(err)
	suite.Require().Equal("test.db?fizz=buzz&foo=bar", result)

	// Multiple values under one key (modernc-style _pragma) round-trip
	// and merge with baked-in values from the path.
	result, err = assembleDSN("test.db?_pragma=foreign_keys(on)", url.Values{
		"_pragma": {"busy_timeout(250)", "journal_mode(wal)"},
	})
	suite.Require().NoError(err)
	suite.Require().Equal(
		"test.db?_pragma=foreign_keys%28on%29&_pragma=busy_timeout%28250%29&_pragma=journal_mode%28wal%29",
		result,
	)
}

func (suite *DBTestSuite) TestStmtCache() {
	schema := NewSqlSchema(`CREATE TABLE t ( foo TEXT UNIQUE, bar NUMERIC )`)
	insert := `INSERT INTO t (foo, bar) VALUES (?, ?) ON CONFLICT (foo) DO UPDATE SET bar = excluded.bar`

	db, err := Open(OpenOptions{File: suite.DBFile, Schema: schema, DriverName: "sqlite"})
	suite.Require().NoError(err)

	cache := NewStmtCache(db.Handle().Preparex)
	a, err := cache.Prepare(insert)
	suite.Require().NoError(err)

	b, err := cache.Prepare(insert)
	suite.Require().NoError(err)

	suite.Require().Equal(a, b, "repeated calls to Prepare should return the same statement")

	_, err = a.Exec("a", 1)
	suite.Require().NoError(err)
	row := db.Handle().QueryRowx(`SELECT bar FROM t WHERE foo=?`, "a")
	var bar int
	suite.Require().NoError(row.Scan(&bar))
	suite.Require().Equal(1, bar)

	suite.Require().NoError(b.Close())
	b, err = cache.Prepare(insert)
	suite.Require().NoError(err)
	suite.Require().NotEqual(a, b, "Prepare should return a new statement after closing the previous statement")

	a, err = cache.Prepare(insert)
	suite.Require().NoError(err)
	suite.Require().Equal(b, a, "repeated calls to Prepare should return the same statement")

	suite.Require().NoError(cache.Close())

	b, err = cache.Prepare(insert)
	suite.Require().NoError(err)
	suite.Require().NotEqual(a, b, "Prepare should return a new statement after calling Close on the StmtCache")

	_, err = b.Exec("a", 2)
	suite.Require().NoError(err)
	query, err := cache.Prepare(`SELECT bar FROM t WHERE foo=?`)
	suite.Require().NoError(err)
	row = query.QueryRowx("a")
	suite.Require().NoError(row.Scan(&bar))
	suite.Require().Equal(2, bar)

	a, err = cache.Prepare(insert)
	suite.Require().NoError(err)
	suite.Require().Equal(a, b, "repeated calls to Prepare should return the same statement")

	suite.Require().NoError(cache.Close())
}

func (suite *DBTestSuite) TestOpen() {
	schema := NewSqlSchema(`CREATE TABLE t ( foo TEXT, bar NUMERIC )`)

	db, err := Open(OpenOptions{File: suite.DBFile, Schema: schema, DriverName: "sqlite"})
	suite.Require().NoError(err)

	vs := &SqliteVersion{}
	appId, err := vs.GetApplicationId(db.Handle())
	suite.Require().NoError(err)
	suite.Require().Equal(schema.ID, appId)
	userVersion, err := vs.GetUserVersion(db.Handle())
	suite.Require().NoError(err)
	suite.Require().Equal(schema.ID, appId)
	suite.Require().Equal(int32(1), userVersion)

	suite.Require().NoError(db.Close())
}

func (suite *DBTestSuite) TestUpgrade() {
	schema := NewSqlSchema(`CREATE TABLE t ( foo TEXT, bar NUMERIC )`)
	schema.DefineUpgrade(2, `
ALTER TABLE t RENAME TO p;
ALTER TABLE p ADD COLUMN extra TEXT;
`)

	vs := &SqliteVersion{}
	db, err := Open(OpenOptions{File: suite.DBFile, Schema: schema, VersionStorer: vs, DriverName: "sqlite"})
	suite.Require().NoError(err)

	_, err = db.Handle().Exec(`INSERT INTO p (foo, bar, extra) VALUES (?, ?, ?)`, "f", 2, "foobar")
	suite.Require().NoError(err)

	appId, err := vs.GetApplicationId(db.Handle())
	suite.Require().NoError(err)
	suite.Require().Equal(schema.ID, appId)
	userVersion, err := vs.GetUserVersion(db.Handle())
	suite.Require().NoError(err)
	suite.Require().Equal(schema.ID, appId)
	suite.Require().Equal(int32(2), userVersion)

	suite.Require().NoError(db.Close())

	suite.Require().NoFileExists(filepath.Join(filepath.Dir(suite.DBFile), "test.before_v2_upgrade.db"))
}

func (suite *DBTestSuite) TestUpgradeHooks() {
	schema := NewSqlSchema(`CREATE TABLE t ( foo TEXT, bar NUMERIC )`)
	schema.DefineUpgrade(2, `ALTER TABLE t ADD COLUMN extra TEXT;`)

	var order []string
	schema.DefinePreUpgrade(2, func(tx sqlx.Ext) error {
		order = append(order, "pre")
		// Insert a row before the ALTER adds the extra column
		_, err := tx.Exec(`INSERT INTO t (foo, bar) VALUES (?, ?)`, "pre", 1)
		return err
	})
	schema.DefinePostUpgrade(2, func(tx sqlx.Ext) error {
		order = append(order, "post")
		// Backfill the new column for existing rows
		_, err := tx.Exec(`UPDATE t SET extra = 'backfilled' WHERE foo = ?`, "pre")
		return err
	})

	db, err := Open(OpenOptions{File: suite.DBFile, Schema: schema, DriverName: "sqlite"})
	suite.Require().NoError(err)

	suite.Require().Equal([]string{"pre", "post"}, order)

	row := db.Handle().QueryRowx(`SELECT extra FROM t WHERE foo = ?`, "pre")
	var extra string
	suite.Require().NoError(row.Scan(&extra))
	suite.Require().Equal("backfilled", extra)

	suite.Require().NoError(db.Close())
}

func (suite *DBTestSuite) TestUpgradeHookFailure() {
	schema := NewSqlSchema(`CREATE TABLE t ( foo TEXT )`)
	schema.DefineUpgrade(2, `ALTER TABLE t ADD COLUMN bar TEXT;`)
	schema.DefinePostUpgrade(2, func(tx sqlx.Ext) error {
		return fmt.Errorf("hook failed")
	})

	_, err := Open(OpenOptions{File: suite.DBFile, Schema: schema, DriverName: "sqlite"})
	suite.Require().Error(err)
	suite.Require().EqualError(err, "error during v2 post-upgrade hook: hook failed")
}

func (suite *DBTestSuite) TestBackupBehavior() {
	schema := NewSqlSchema(`CREATE TABLE t ( foo TEXT, bar NUMERIC )`)
	schema.DefineUpgrade(2, `
ALTER TABLE t RENAME TO p;
ALTER TABLE p ADD COLUMN extra TEXT;
`)

	vs := &SqliteVersion{}
	backupDir := filepath.Dir(suite.DBFile)
	db, err := Open(OpenOptions{
		File:          suite.DBFile,
		DriverName:    "sqlite",
		BackupDir:     backupDir,
		Schema:        schema,
		VersionStorer: vs,
	})
	suite.Require().NoError(err)

	_, err = db.Handle().Exec(`INSERT INTO p (foo, bar, extra) VALUES (?, ?, ?)`, "f", 2, "foobar")
	suite.Require().NoError(err)

	appId, err := vs.GetApplicationId(db.Handle())
	suite.Require().NoError(err)
	suite.Require().Equal(schema.ID, appId)
	userVersion, err := vs.GetUserVersion(db.Handle())
	suite.Require().NoError(err)
	suite.Require().Equal(schema.ID, appId)
	suite.Require().Equal(int32(2), userVersion)

	suite.Require().NoError(db.Close())

	suite.Require().NoFileExists(filepath.Join(backupDir, "test.before_v2_upgrade.db"), "upgrading newly created DB should not create a backup")

	schema.DefineUpgrade(3, `ALTER TABLE p ADD COLUMN extra2 TEXT;`)
	db, err = Open(OpenOptions{
		File:          suite.DBFile,
		DriverName:    "sqlite",
		BackupDir:     "",
		Schema:        schema,
		VersionStorer: vs,
	})
	suite.Require().NoError(err)
	_, err = db.Handle().Exec(`INSERT INTO p (foo, bar, extra, extra2) VALUES (?, ?, ?, ?)`, "f", 2, "foobar", "more")
	suite.Require().NoError(err)
	suite.Require().NoError(db.Close())
	suite.Require().NoFileExists(filepath.Join(backupDir, "test.before_v3_upgrade.db"), "upgrading without BackupDir should not create a backup")

	schema.DefineUpgrade(4, `ALTER TABLE p ADD COLUMN extra3 TEXT;`)
	db, err = Open(OpenOptions{
		File:          suite.DBFile,
		DriverName:    "sqlite",
		BackupDir:     backupDir,
		Schema:        schema,
		VersionStorer: vs,
	})
	suite.Require().NoError(err)
	_, err = db.Handle().Exec(`INSERT INTO p (foo, bar, extra, extra2, extra3) VALUES (?, ?, ?, ?, ?)`, "f", 2, "foobar", "more", "even_more")
	suite.Require().NoError(err)
	suite.Require().NoError(db.Close())
	suite.Require().FileExists(filepath.Join(backupDir, "test.before_v4_upgrade.db"), "upgrading existing DB with a valid BackupDir should create backup")
}

func (suite *DBTestSuite) TestLegacyUpgrade() {
	legacyDB, err := sqlx.Open("sqlite", fmt.Sprintf("file:%s", suite.DBFile))
	suite.Require().NoError(err)

	_, err = legacyDB.Exec(`CREATE TABLE p (foo TEXT, bar NUMERIC, extra TEXT )`)
	suite.Require().NoError(err)

	suite.Require().NoError(legacyDB.Close())

	schema := NewSqlSchema(`CREATE TABLE t ( foo TEXT, bar NUMERIC )`)
	schema.DefineUpgrade(2, `
ALTER TABLE t RENAME TO p;
ALTER TABLE p ADD COLUMN extra TEXT;
`)
	schema.DefineUpgrade(3, `ALTER TABLE p ADD COLUMN more TEXT`)

	legacy := &FallbackVersion{
		VersionStorer: &SqliteVersion{},
		FallbackReader: &mockReader{
			ID: schema.ApplicationID(),
		},
	}

	db, err := Open(OpenOptions{File: suite.DBFile, Schema: schema, VersionStorer: legacy, DriverName: "sqlite"})
	suite.Require().NoError(err)

	suite.Require().Equal(1, legacy.FallbackReader.(*mockReader).callAppId)
	suite.Require().Equal(1, legacy.FallbackReader.(*mockReader).callVersion)

	_, err = db.Handle().Exec(`INSERT INTO p (foo, bar, extra, more) VALUES (?, ?, ?, ?)`, "f", 2, "foo", "bar")
	suite.Require().NoError(err)

	appId, err := legacy.GetApplicationId(db.Handle())
	suite.Require().NoError(err)
	suite.Require().Equal(schema.ID, appId)

	userVersion, err := legacy.GetUserVersion(db.Handle())
	suite.Require().NoError(err)
	suite.Require().Equal(int32(3), userVersion)

	suite.Require().NoError(db.Close())

	// Re-open to make sure LegacyMetadata is not called twice
	_, err = Open(OpenOptions{File: suite.DBFile, Schema: schema, VersionStorer: legacy, DriverName: "sqlite"})
	suite.Require().NoError(err)

	suite.Require().Equal(1, legacy.FallbackReader.(*mockReader).callAppId)
	suite.Require().Equal(1, legacy.FallbackReader.(*mockReader).callVersion)
}

type mockReader struct {
	ID                     int32
	callAppId, callVersion int
}

func (m *mockReader) GetApplicationId(tx sqlx.Queryer) (int32, error) {
	m.callAppId++
	return m.ID, nil
}

func (m *mockReader) GetUserVersion(tx sqlx.Queryer) (int32, error) {
	m.callVersion++
	return 2, nil
}

// onDiskPageSize returns the SQLite file's actual on-disk page size by reading
// the file header (bytes 16-17, big-endian uint16, with the stored value 1
// representing 65536). This is the authoritative source of truth — unlike
// PRAGMA page_size, which can report a queued value that was never written.
func onDiskPageSize(t *testing.T, file string) int {
	t.Helper()
	f, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var hdr [18]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		t.Fatal(err)
	}
	if string(hdr[:16]) != "SQLite format 3\x00" {
		t.Fatalf("not a SQLite file: %q", hdr[:16])
	}
	ps := int(binary.BigEndian.Uint16(hdr[16:18]))
	if ps == 1 {
		ps = 65536
	}
	return ps
}

// TestOnOpenSequencesPageSizeBeforeWAL is the gating evidence that the OnOpen
// hook actually causes page_size to take effect on a fresh database. It opens
// with page_size(65536) in DSN, switches to WAL via OnOpen, then reads the
// file header directly to confirm the on-disk page size is 64 KiB.
func TestOnOpenSequencesPageSizeBeforeWAL(t *testing.T) {
	file := filepath.Join(t.TempDir(), "test.db")

	schema := NewSqlSchema(
		`CREATE TABLE t (id INTEGER PRIMARY KEY, payload BLOB) STRICT;`)

	db, err := Open(OpenOptions{
		File:       file,
		Schema:     schema,
		DriverName: "sqlite",
		DSNOptions: url.Values{
			"_pragma": []string{"page_size(65536)"},
		},
		OnOpen: func(h Handle) error {
			var mode string
			if err := sqlx.Get(h, &mode, "PRAGMA journal_mode = WAL"); err != nil {
				return err
			}
			if mode != "wal" {
				return fmt.Errorf("journal_mode = %q; want wal", mode)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.Handle().Exec(`INSERT INTO t (payload) VALUES (randomblob(8192))`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if ps := onDiskPageSize(t, file); ps != 65536 {
		t.Errorf("on-disk page_size = %d; want 65536", ps)
	}
}

// TestDSNJournalModeWALDefeatsPageSize documents the upstream modernc.org/sqlite
// behavior that motivates the OnOpen hook: when both page_size and
// journal_mode=wal are in DSN _pragma, the driver alphabetizes them and
// applies journal_mode first, which creates the file at the default page size
// before page_size takes effect.
//
// This test t.Skips if upstream ever fixes the ordering — that's the signal
// that the OnOpen workaround can be retired.
func TestDSNJournalModeWALDefeatsPageSize(t *testing.T) {
	file := filepath.Join(t.TempDir(), "test.db")
	schema := NewSqlSchema(
		`CREATE TABLE t (id INTEGER PRIMARY KEY, payload BLOB) STRICT;`)

	db, err := Open(OpenOptions{
		File:       file,
		Schema:     schema,
		DriverName: "sqlite",
		DSNOptions: url.Values{
			"_pragma": []string{
				"page_size(65536)",
				"journal_mode(wal)",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Handle().Exec(`INSERT INTO t (payload) VALUES (randomblob(8192))`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	ps := onDiskPageSize(t, file)
	if ps == 65536 {
		t.Skip("upstream modernc.org/sqlite no longer reorders pragmas; " +
			"the OnOpen workaround in consumers can be simplified")
	}
	if ps != 4096 {
		t.Errorf("on-disk page_size = %d; want 4096 (default — upstream ordering forces it)", ps)
	}
}
