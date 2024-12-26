package csvsql

type UnionType string

const (
	Union    UnionType = "UNION"
	UnionAll UnionType = "UNION ALL"
)

type UnionComponent struct {
	UnionKind UnionType
	Queries   []*Query
}

func (u *UnionComponent) Type() string {
	return "UNION"
}

func (u *UnionComponent) Validate() error {
	if len(u.Queries) == 0 {
		return &ErrInvalidQuery{"UNION must have at least one query"}
	}
	return nil
}

func (qb *QueryBuilder) Union(other *QueryBuilder) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	if other == nil {
		qb.err = &ErrInvalidQuery{"cannot UNION with no queries"}
		return qb
	}
	if qb.query.Union == nil {
		qb.query.Union = &UnionComponent{
			UnionKind: Union,
			Queries:   []*Query{other.query},
		}
	} else {
		qb.query.Union.Queries = append(qb.query.Union.Queries, other.query)
	}
	return qb
}

func (qb *QueryBuilder) UnionAll(other *QueryBuilder) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	if other == nil {
		qb.err = &ErrInvalidQuery{"cannot UNION ALL with nil query"}
		return qb
	}
	if qb.query.Union == nil {
		qb.query.Union = &UnionComponent{
			UnionKind: UnionAll,
			Queries:   []*Query{other.query},
		}
	} else {
		qb.query.Union.Queries = append(qb.query.Union.Queries, other.query)
	}
	return qb
}
