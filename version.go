package localdb

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

type VersionStorer interface {
	SetApplicationId(tx sqlx.Execer, appId int32) error
	SetUserVersion(tx sqlx.Execer, version int32) error

	VersionReader
}

type VersionReader interface {
	GetApplicationId(tx sqlx.Queryer) (appId int32, err error)
	GetUserVersion(tx sqlx.Queryer) (version int32, err error)
}

type FallbackVersion struct {
	// Primary
	VersionStorer

	// Secondary
	FallbackReader VersionReader
}

func (f *FallbackVersion) GetApplicationId(tx sqlx.Queryer) (appId int32, err error) {
	appId, err = f.VersionStorer.GetApplicationId(tx)
	if err != nil || appId != 0 {
		return
	}

	return f.FallbackReader.GetApplicationId(tx)
}

func (f *FallbackVersion) GetUserVersion(tx sqlx.Queryer) (userVersion int32, err error) {
	userVersion, err = f.VersionStorer.GetUserVersion(tx)
	if err != nil || userVersion != 0 {
		return
	}

	return f.FallbackReader.GetUserVersion(tx)
}

type SqliteVersion struct{}

func (sv *SqliteVersion) queryPragma(tx sqlx.Queryer, pragma string) (appId int32, err error) {
	row := tx.QueryRowx(fmt.Sprintf(`PRAGMA %s`, pragma))
	err = row.Scan(&appId)
	return appId, err
}

func (sv *SqliteVersion) setPragma(tx sqlx.Execer, pragma string, value int32) error {
	_, err := tx.Exec(fmt.Sprintf(`PRAGMA %s = %d`, pragma, value))
	return err
}

func (sv *SqliteVersion) GetApplicationId(tx sqlx.Queryer) (appId int32, err error) {
	return sv.queryPragma(tx, "application_id")
}

func (sv *SqliteVersion) GetUserVersion(tx sqlx.Queryer) (version int32, err error) {
	return sv.queryPragma(tx, "user_version")
}

func (sv *SqliteVersion) SetApplicationId(tx sqlx.Execer, appId int32) error {
	return sv.setPragma(tx, "application_id", appId)
}

func (sv *SqliteVersion) SetUserVersion(tx sqlx.Execer, version int32) error {
	return sv.setPragma(tx, "user_version", version)
}
