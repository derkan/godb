package postgresql

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/samonzeweb/godb/adapters"
	"github.com/samonzeweb/godb/dberror"
)

var Driver = "postgres"

type PostgreSQL struct{}

var Adapter = PostgreSQL{}

func (PostgreSQL) DriverName() string {
	return Driver
}

func (PostgreSQL) Quote(identifier string) string {
	return "\"" + identifier + "\""
}

func (PostgreSQL) ReplacePlaceholders(originalPlaceholder string, sql string) string {
	sqlBuffer := bytes.NewBuffer(make([]byte, 0, len(sql)))
	count := 1
	for {
		pp := strings.Index(sql, originalPlaceholder)
		if pp == -1 {
			break
		}
		sqlBuffer.WriteString(sql[:pp])
		sqlBuffer.WriteString("$")
		sqlBuffer.WriteString(strconv.Itoa(count))
		count++
		sql = sql[pp+1:]
	}
	sqlBuffer.WriteString(sql)
	return sqlBuffer.String()
}

func (p PostgreSQL) ReturningBuild(columns []string) string {
	suffixBuffer := bytes.NewBuffer(make([]byte, 0, 16*len(columns)+1))
	suffixBuffer.WriteString("RETURNING ")
	for i, column := range columns {
		if i > 0 {
			suffixBuffer.WriteString(", ")
		}
		suffixBuffer.WriteString(column)
	}
	return suffixBuffer.String()
}

func (p PostgreSQL) FormatForNewValues(columns []string) []string {
	formatedColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		formatedColumns = append(formatedColumns, p.Quote(column))
	}
	return formatedColumns
}

func (p PostgreSQL) GetReturningPosition() adapters.ReturningPosition {
	return adapters.ReturningPostgreSQL
}

func (p PostgreSQL) ParseError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "duplicate key value violates unique constraint") {
		return dberror.UniqueConstraint{Message: msg, Field: dberror.ExtractStr(msg, "constraint \"", "\""), Err: err}
	}
	if strings.Contains(msg, "violates foreign key constraint") {
		return dberror.ForeignKeyConstraint{Message: msg, Field: dberror.ExtractStr(msg, "constraint \"", "\""), Err: err}
	}
	if strings.Contains(msg, "violates check constraint") {
		return dberror.CheckConstraint{Message: msg, Field: dberror.ExtractStr(msg, "constraint \"", "\""), Err: err}
	}

	return err
}
