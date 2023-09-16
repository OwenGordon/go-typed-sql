package typed

import (
	"database/sql"
	"fmt"
	"strings"
)

type DB struct {
	*sql.DB
}

func NewDB(dataSourceName string) (*DB, error) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func NewTable[T DatabaseSerializable[T]](db *DB, obj T) error {
	var columns []string
	for _, col := range obj.Columns() {
		dataType := ""
		switch col.Second.(type) {
		case int:
			dataType = "INT"
		case string:
			dataType = "VARCHAR(255)"
		// Add more cases as needed for other data types
		default:
			dataType = "VARCHAR(255)"
		}
		columns = append(columns, fmt.Sprintf("%s %s", col.First, dataType))
	}

	primaryKey := obj.PrimaryKeyColumn()
	columns = append(columns, fmt.Sprintf("PRIMARY KEY (%s)", primaryKey))

	query := fmt.Sprintf("CREATE TABLE %s (%s)", obj.TableName(), strings.Join(columns, ", "))
	_, err := db.Exec(query)
	return err
}

func Create[T DatabaseSerializable[T]](db *DB, obj T) error {
	columns := obj.Columns()

	columnNames := make([]string, 0, len(columns))
	columnValues := make([]interface{}, 0, len(columns))
	for _, pair := range columns {
		columnNames = append(columnNames, pair.First)
		columnValues = append(columnValues, pair.Second)
	}

	placeholders := make([]string, len(columnNames))
	for i := range columnNames {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		obj.TableName(),
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "),
	)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(query, columnValues...)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func Read[T DatabaseSerializable[T]](db *DB, obj T) ([]T, error) {
	columns := obj.Columns()

	columnNames := make([]string, 0, len(columns))
	columnValues := make([]interface{}, 0, len(columns))
	for _, pair := range columns {
		columnNames = append(columnNames, pair.First)
		columnValues = append(columnValues, pair.Second)
	}

	query := fmt.Sprintf(
		"SELECT %s FROM %s",
		strings.Join(columnNames, ", "),
		obj.TableName(),
	)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var objs []T
	for rows.Next() {
		newObj := obj.New()
		if err := newObj.ScanRow(rows); err != nil {
			return nil, err
		}
		objs = append(objs, newObj)
	}
	return objs, nil
}

func Update[T DatabaseSerializable[T]](db *DB, obj T) error {
	columns := obj.Columns()

	columnNames := make([]string, 0, len(columns))
	columnValues := make([]interface{}, 0, len(columns))
	for _, pair := range columns {
		columnNames = append(columnNames, pair.First)
		columnValues = append(columnValues, pair.Second)
	}

	setClauses := make([]string, len(columnNames))
	for i, colName := range columnNames {
		setClauses[i] = fmt.Sprintf("%s = ?", colName)
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = ?",
		obj.TableName(),
		strings.Join(setClauses, ", "),
		obj.PrimaryKeyColumn(),
	)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(query, append(columnValues, obj.PrimaryKeyValue())...)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func Delete[T DatabaseSerializable[T]](db *DB, obj T) error {
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s = ?",
		obj.TableName(),
		obj.PrimaryKeyColumn(),
	)

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(query, obj.PrimaryKeyValue())
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
