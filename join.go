package csvsql

import (
	"fmt"
	"strconv"
	"strings"
	"time"
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

type CustomJoinCondition func(row map[string][]string, tables map[string]*Table) (bool, error)

func (fn CustomJoinCondition) EvaluateJoin(row map[string][]string, tables map[string]*Table) (bool, error) {
	return fn(row, tables)
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

func (qb *QueryBuilder) OnFunc(fn func(row map[string][]string, tables map[string]*Table) (bool, error)) *QueryBuilder {
	if qb.err != nil {
		return qb
	}
	if len(qb.query.Joins) == 0 {
		qb.err = &ErrInvalidQuery{"No JOIN clause to add condition to"}
		return qb
	}
	if fn == nil {
		qb.err = &ErrInvalidQuery{"join condition function cannot be nil"}
		return qb
	}

	lastJoin := qb.query.Joins[len(qb.query.Joins)-1]
	lastJoin.Condition = CustomJoinCondition(fn)
	return qb
}

// Common date formats
const (
	DateFormat     = "2006-01-02"
	DateTimeFormat = "2006-01-02 15:04:05"
)

type Result struct {
	value string
	err   error
}

func (r Result) Must() string {
	if r.err != nil {
		panic(r.err)
	}
	return r.value
}

func (r Result) String() (string, error) {
	return r.value, r.err
}

func (r Result) Int() (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	return strconv.Atoi(strings.TrimSpace(r.value))
}

func (r Result) MustInt() int {
	i, err := r.Int()
	if err != nil {
		panic(err)
	}
	return i
}

func (r Result) Float() (float64, error) {
	if r.err != nil {
		return 0, r.err
	}
	return strconv.ParseFloat(strings.TrimSpace(r.value), 64)
}

func (r Result) MustFloat() float64 {
	f, err := r.Float()
	if err != nil {
		panic(err)
	}
	return f
}

func (r Result) Time(layout string) (time.Time, error) {
	if r.err != nil {
		return time.Time{}, r.err
	}
	return time.Parse(layout, strings.TrimSpace(r.value))
}

func (r Result) MustTime(layout string) time.Time {
	t, err := r.Time(layout)
	if err != nil {
		panic(err)
	}
	return t
}

func (r Result) Date() (time.Time, error) {
	return r.Time(DateFormat)
}

func (r Result) MustDate() time.Time {
	return r.MustTime(DateFormat)
}

func (r Result) DateTime() (time.Time, error) {
	return r.Time(DateTimeFormat)
}

func (r Result) MustDateTime() time.Time {
	return r.MustTime(DateTimeFormat)
}

func (r Result) Bool() (bool, error) {
	if r.err != nil {
		return false, r.err
	}
	return strconv.ParseBool(strings.TrimSpace(r.value))
}

func (r Result) MustBool() bool {
	b, err := r.Bool()
	if err != nil {
		panic(err)
	}
	return b
}

type TableRow struct {
	table *Table
	data  []string
	err   error
}

func (r *TableRow) Get(column string) Result {
	if r.err != nil {
		return Result{err: r.err}
	}
	idx, err := r.table.GetColumnIndex(column)
	if err != nil {
		return Result{err: err}
	}
	return Result{value: r.data[idx]}
}

func (r *TableRow) MustGet(column string) string {
	return r.Get(column).Must()
}

func GetRow(row map[string][]string, tables map[string]*Table, tableName string) *TableRow {
	table, ok := tables[tableName]
	if !ok {
		return &TableRow{err: fmt.Errorf("table %s not found", tableName)}
	}

	data, ok := row[tableName]
	if !ok {
		return &TableRow{err: fmt.Errorf("row data for table %s not found", tableName)}
	}

	return &TableRow{
		table: table,
		data:  data,
	}
}
