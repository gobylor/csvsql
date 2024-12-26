package csvsql

import (
	"fmt"
)

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
)

type JoinComponent struct {
	Table     string
	Condition JoinConditionEvaluator
	JoinType  JoinType
}

func (j *JoinComponent) Type() string {
	return "JOIN"
}

func (j *JoinComponent) Validate() error {
	if j.Table == "" {
		return &ErrInvalidQuery{"JOIN must specify a table"}
	}
	if j.Condition == nil {
		return &ErrInvalidQuery{"JOIN must have a condition"}
	}
	return nil
}

type JoinConditionEvaluator interface {
	EvaluateJoin(row map[string][]string, tables map[string]*Table) (bool, error)
}

type JoinCondition struct {
	LeftTable  string
	LeftCol    string
	Op         Operator
	RightTable string
	RightCol   string
}

func (jc *JoinCondition) EvaluateJoin(row map[string][]string, tables map[string]*Table) (bool, error) {
	leftTable, ok := tables[jc.LeftTable]
	if !ok {
		return false, fmt.Errorf("left table %s not found", jc.LeftTable)
	}

	rightTable, ok := tables[jc.RightTable]
	if !ok {
		return false, fmt.Errorf("right table %s not found", jc.RightTable)
	}

	leftIdx, err := leftTable.GetColumnIndex(jc.LeftCol)
	if err != nil {
		return false, err
	}

	rightIdx, err := rightTable.GetColumnIndex(jc.RightCol)
	if err != nil {
		return false, err
	}

	leftRow, ok := row[jc.LeftTable]
	if !ok {
		return false, fmt.Errorf("left table %s not found in row data", jc.LeftTable)
	}

	rightRow, ok := row[jc.RightTable]
	if !ok {
		return false, fmt.Errorf("right table %s not found in row data", jc.RightTable)
	}

	return jc.Op.Evaluate(leftRow[leftIdx], rightRow[rightIdx])
}

type CompositeJoinCondition struct {
	Left     JoinConditionEvaluator
	Right    JoinConditionEvaluator
	Operator LogicalOperator
}

func (c *CompositeJoinCondition) EvaluateJoin(row map[string][]string, tables map[string]*Table) (bool, error) {
	leftResult, err := c.Left.EvaluateJoin(row, tables)
	if err != nil {
		return false, err
	}

	rightResult, err := c.Right.EvaluateJoin(row, tables)
	if err != nil {
		return false, err
	}

	result, err := c.Operator.Evaluate(
		fmt.Sprintf("%v", leftResult),
		fmt.Sprintf("%v", rightResult),
	)
	if err != nil {
		return false, fmt.Errorf("operator evaluation error: %w", err)
	}
	return result, nil
}

func (qb *QueryBuilder) InnerJoin(table string) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	join := &JoinComponent{
		Table:    table,
		JoinType: InnerJoin,
	}
	qb.query.Joins = append(qb.query.Joins, join)
	return qb
}

func (qb *QueryBuilder) LeftJoin(table string) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	join := &JoinComponent{
		Table:    table,
		JoinType: LeftJoin,
	}
	qb.query.Joins = append(qb.query.Joins, join)
	return qb
}

func (qb *QueryBuilder) RightJoin(table string) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	join := &JoinComponent{
		Table:    table,
		JoinType: RightJoin,
	}
	qb.query.Joins = append(qb.query.Joins, join)
	return qb
}

func (qb *QueryBuilder) On(leftTable, leftCol, operator, rightTable, rightCol string) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	if len(qb.query.Joins) == 0 {
		qb.err = &ErrInvalidQuery{"No JOIN clause to add condition to"}
		return qb
	}

	op, err := GetOperator(operator)
	if err != nil {
		qb.err = err
		return qb
	}

	lastJoin := qb.query.Joins[len(qb.query.Joins)-1]
	lastJoin.Condition = &JoinCondition{
		LeftTable:  leftTable,
		LeftCol:    leftCol,
		Op:         op,
		RightTable: rightTable,
		RightCol:   rightCol,
	}
	return qb
}
