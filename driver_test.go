package localdb

// Tests need a driver registered under DefaultDriverName.
// The core library no longer imports a driver, so consumers
// must do this in their own code.
import _ "github.com/mattn/go-sqlite3"
