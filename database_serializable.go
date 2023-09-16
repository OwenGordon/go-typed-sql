package typed

import "database/sql"

type Pair[A any, B any] struct {
	First  A
	Second B
}

type DatabaseSerializable[T any] interface {
	TableName() string
	Columns() []Pair[string, interface{}]
	PrimaryKeyColumn() string
	PrimaryKeyValue() interface{}
	ScanRow(*sql.Rows) error
	New() T
}
