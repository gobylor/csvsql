package csvsql

type QueryComponent interface {
	Type() string
	Validate() error
}

type Query struct {
	Select *SelectComponent
	From   *FromComponent
	Where  *WhereComponent
	Joins  []*JoinComponent
	Union  *UnionComponent
}

type QueryBuilder struct {
	query *Query
	err   error
}

func NewQuery() *QueryBuilder {
	return &QueryBuilder{
		query: &Query{},
	}
}

func (qb *QueryBuilder) And(other *QueryBuilder) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	if other == nil || other.query.Where == nil {
		qb.err = &ErrInvalidQuery{"cannot AND with nil condition"}
		return qb
	}

	if qb.query.Where == nil {
		qb.err = &ErrInvalidQuery{"cannot AND with nil condition"}
		return qb
	}

	composite, err := NewCompositeCondition(
		qb.query.Where.Condition,
		other.query.Where.Condition,
		And.String(),
	)
	if err != nil {
		qb.err = err
		return qb
	}

	qb.query.Where.Condition = composite
	return qb
}

func (qb *QueryBuilder) Or(other *QueryBuilder) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	if other == nil || other.query.Where == nil {
		qb.err = &ErrInvalidQuery{"cannot OR with nil condition"}
		return qb
	}

	if qb.query.Where == nil {
		qb.err = &ErrInvalidQuery{"cannot OR with nil condition"}
		return qb
	}

	composite, err := NewCompositeCondition(
		qb.query.Where.Condition,
		other.query.Where.Condition,
		Or.String(),
	)
	if err != nil {
		qb.err = err
		return qb
	}

	qb.query.Where.Condition = composite
	return qb
}

func (qb *QueryBuilder) Build() (*Query, error) {
	if qb.err != nil {
		return nil, qb.err
	}

	if qb.query.Select != nil {
		if err := qb.query.Select.Validate(); err != nil {
			return nil, err
		}
	}

	if qb.query.From != nil {
		if err := qb.query.From.Validate(); err != nil {
			return nil, err
		}
	}

	if qb.query.Where != nil {
		if err := qb.query.Where.Validate(); err != nil {
			return nil, err
		}
	}

	for _, join := range qb.query.Joins {
		if err := join.Validate(); err != nil {
			return nil, err
		}
	}

	if qb.query.Union != nil {
		if err := qb.query.Union.Validate(); err != nil {
			return nil, err
		}
	}

	return qb.query, nil
}
