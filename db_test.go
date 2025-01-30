package localdb

import (
	"fmt"
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

	result, err = assembleDSN("test.db", map[string]string{"fizz": "buzz"})
	suite.Require().NoError(err)
	suite.Require().Equal("test.db?fizz=buzz", result)

	result, err = assembleDSN("test.db?foo=bar", map[string]string{"fizz": "buzz"})
	suite.Require().NoError(err)
	suite.Require().Equal("test.db?fizz=buzz&foo=bar", result)
}

func (suite *DBTestSuite) TestStmtCache() {
	schema := NewSqlSchema(`CREATE TABLE t ( foo TEXT UNIQUE, bar NUMERIC )`)
	insert := `INSERT INTO t (foo, bar) VALUES (?, ?) ON CONFLICT (foo) DO UPDATE SET bar = excluded.bar`

	db, err := Open(OpenOptions{File: suite.DBFile, Schema: schema})
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

	db, err := Open(OpenOptions{File: suite.DBFile, Schema: schema})
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
	db, err := Open(OpenOptions{File: suite.DBFile, Schema: schema, VersionStorer: vs})
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

func (suite *DBTestSuite) TestUpgradeWithBackup() {
	schema := NewSqlSchema(`CREATE TABLE t ( foo TEXT, bar NUMERIC )`)
	schema.DefineUpgrade(2, `
ALTER TABLE t RENAME TO p;
ALTER TABLE p ADD COLUMN extra TEXT;
`)

	vs := &SqliteVersion{}
	backupDir := filepath.Dir(suite.DBFile)
	db, err := Open(OpenOptions{
		File:          suite.DBFile,
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

	suite.Require().FileExists(filepath.Join(backupDir, "test.before_v2_upgrade.db"))
}

func (suite *DBTestSuite) TestLegacyUpgrade() {
	legacyDB, err := sqlx.Open("sqlite3", fmt.Sprintf("file:%s", suite.DBFile))
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

	db, err := Open(OpenOptions{File: suite.DBFile, Schema: schema, VersionStorer: legacy})
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
	_, err = Open(OpenOptions{File: suite.DBFile, Schema: schema, VersionStorer: legacy})
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
