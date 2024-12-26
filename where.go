package csvsql

type WhereComponent struct {
	Condition Condition
}

func (w *WhereComponent) Type() string {
	return "WHERE"
}

func (w *WhereComponent) Validate() error {
	if w.Condition == nil {
		return &ErrInvalidQuery{"WHERE must have a condition"}
	}
	return nil
}

func (qb *QueryBuilder) Where(column, operator, value string) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	condition, err := NewSimpleCondition(column, operator, value)
	if err != nil {
		qb.err = err
		return qb
	}
	qb.query.Where = &WhereComponent{
		Condition: condition,
	}
	return qb
}

func (qb *QueryBuilder) WhereFunc(fn func(row map[string][]string, tables map[string]*Table) (bool, error)) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	if fn == nil {
		qb.err = &ErrInvalidQuery{"custom condition function cannot be nil"}
		return qb
	}
	customCondition := CustomCondition(fn)
	qb.query.Where = &WhereComponent{
		Condition: &customCondition,
	}
	return qb
}
