package postgresql

import (
	"bytes"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

type PostgreSQL struct{}

var Adapter = PostgreSQL{}

func (PostgreSQL) DriverName() string {
	return "postgres"
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

func (p PostgreSQL) ReturningSuffix(autoColumns []string) string {
	suffixBuffer := bytes.NewBuffer(make([]byte, 0, 16*len(autoColumns)+1))
	suffixBuffer.WriteString("RETURNING ")
	for i, columns := range autoColumns {
		if i > 0 {
			suffixBuffer.WriteString(",")
		}
		suffixBuffer.WriteString(p.Quote(columns))
	}
	return suffixBuffer.String()
}
