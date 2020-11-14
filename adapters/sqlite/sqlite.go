package sqlite

import (
	"strings"

	"github.com/samonzeweb/godb/dberror"
)

var Driver = "sqlite3"

type SQLite struct{}

var Adapter = SQLite{}

func (SQLite) DriverName() string {
	return Driver
}

func (SQLite) Quote(identifier string) string {
	return "\"" + identifier + "\""
}

func (SQLite) ParseError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "UNIQUE constraint failed") {
		return dberror.UniqueConstraint{Message: msg, Field: strings.Split(strings.Split(msg, "failed: ")[1], ".")[1], Err: err}
	}
	if strings.Contains(msg, "CHECK constraint failed") {
		return dberror.CheckConstraint{Message: msg, Field: strings.Split(strings.Split(msg, "failed: ")[1], ".")[1], Err: err}
	}
	return err
}
