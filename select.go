package csvsql

type SelectComponent struct {
	Columns []string
}

func (s *SelectComponent) Type() string {
	return "SELECT"
}

func (s *SelectComponent) Validate() error {
	if len(s.Columns) == 0 {
		return &ErrInvalidQuery{"SELECT must specify at least one column"}
	}
	return nil
}

func (qb *QueryBuilder) Select(columns ...string) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	qb.query.Select = &SelectComponent{
		Columns: columns,
	}
	return qb
}
