package localdb

// Tests need a driver registered with database/sql. The core
// library no longer imports a driver, so consumers must do this
// in their own code. We use modernc.org/sqlite (registered as
// "sqlite") so that running the test suite does not require CGo.
import _ "modernc.org/sqlite"
