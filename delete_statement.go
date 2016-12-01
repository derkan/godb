package godb

import "time"

// deleteStatement is a DELETE sql statement builder.
// Initialise it with the Delete function.
type deleteStatement struct {
	db *DB

	fromTable string
	where     []*Condition
	suffixes  []string
}

// DeleteFrom initialise a DELETE statement builder.
func (db *DB) DeleteFrom(tableName string) *deleteStatement {
	ds := &deleteStatement{db: db}
	ds.fromTable = tableName
	return ds
}

// Where add a condition using string and arguments.
func (ds *deleteStatement) Where(sql string, args ...interface{}) *deleteStatement {
	return ds.WhereQ(Q(sql, args...))
}

// WhereQ add a simple or complex predicate generated with Q and
// confunctions.
func (ds *deleteStatement) WhereQ(condition *Condition) *deleteStatement {
	ds.where = append(ds.where, condition)
	return ds
}

// Suffix add an expression to suffix the statement.
func (ds *deleteStatement) Suffix(suffix string) *deleteStatement {
	ds.suffixes = append(ds.suffixes, suffix)
	return ds
}

// ToSQL returns a string with the SQL statement (containing placeholders),
// the arguments slices, and an error.
func (ds *deleteStatement) ToSQL() (string, []interface{}, error) {
	sqlWhereLength, argsWhereLength, err := sumOfConditionsLengths(ds.where)
	if err != nil {
		return "", nil, err
	}

	sqlBuffer := newSQLBuffer(
		ds.db.adapter,
		sqlWhereLength+64,
		argsWhereLength,
	)

	sqlBuffer.write("DELETE")

	if err := sqlBuffer.writeFrom(ds.fromTable); err != nil {
		return "", nil, err
	}

	if err := sqlBuffer.writeWhere(ds.where); err != nil {
		return "", nil, err
	}

	if err := sqlBuffer.writeStrings(ds.suffixes); err != nil {
		return "", nil, err
	}

	return sqlBuffer.sqlString(), sqlBuffer.sqlArguments(), nil
}

// Do executes the builded query, and return RowsAffected()
func (ds *deleteStatement) Do() (int64, error) {
	sql, args, err := ds.ToSQL()
	if err != nil {
		return 0, err
	}
	sql = ds.db.replacePlaceholders(sql)
	ds.db.logPrintln("DELETE : ", sql, args)

	// Execute the DELETE statement
	startTime := time.Now()
	queryable, err := ds.db.getQueryable(sql)
	if err != nil {
		return 0, err
	}
	result, err := queryable.Exec(args...)
	condumedTime := timeElapsedSince(startTime)
	ds.db.addConsumedTime(condumedTime)
	ds.db.logDuration(condumedTime)
	if err != nil {
		ds.db.logPrintln("ERROR : ", err)
		return 0, err
	}

	// TODO : check if RowsAffected() is implemented by the driver
	rowsAffected, err := result.RowsAffected()
	return rowsAffected, err
}
