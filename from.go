package csvsql

type FromComponent struct {
	Table string
}

func (f *FromComponent) Type() string {
	return "FROM"
}

func (f *FromComponent) Validate() error {
	if f.Table == "" {
		return &ErrInvalidQuery{"FROM must specify a table"}
	}
	return nil
}

func (qb *QueryBuilder) From(table string) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	qb.query.From = &FromComponent{
		Table: table,
	}
	return qb
}
