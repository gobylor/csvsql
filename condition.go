package csvsql

import (
	"fmt"
	"strings"
)

type Condition interface {
	Evaluate(row map[string][]string, tables map[string]*Table) (bool, error)
	Type() string
}

type SimpleCondition struct {
	Column string
	Op     Operator
	Value  string
}

func (c *SimpleCondition) Type() string {
	return "Simple"
}

func (c *SimpleCondition) Evaluate(row map[string][]string, tables map[string]*Table) (bool, error) {
	parts := strings.Split(c.Column, ".")
	var tableName, colName string
	var table *Table
	var colIdx int

	if len(parts) == 2 {
		tableName, colName = parts[0], parts[1]
		var ok bool
		table, ok = tables[tableName]
		if !ok {
			return false, fmt.Errorf("table %s not found", tableName)
		}
		var err error
		colIdx, err = table.GetColumnIndex(colName)
		if err != nil {
			return false, fmt.Errorf("column error: %w", err)
		}
	} else if len(parts) == 1 {
		colName = parts[0]
		foundInTable := ""
		var foundIdx int

		for tName, t := range tables {
			if idx, err := t.GetColumnIndex(colName); err == nil {
				if foundInTable != "" {
					return false, fmt.Errorf("ambiguous column name: %s exists in multiple tables", colName)
				}
				foundInTable = tName
				foundIdx = idx
				table = t
			}
		}

		if foundInTable == "" {
			return false, fmt.Errorf("column not found in any table: %s", colName)
		}
		tableName = foundInTable
		colIdx = foundIdx
	} else {
		return false, fmt.Errorf("invalid column name format: %s", c.Column)
	}

	tableRow, ok := row[tableName]
	if !ok {
		return false, fmt.Errorf("table %s not found in row data", tableName)
	}

	if colIdx >= len(tableRow) {
		return false, fmt.Errorf("column index %d out of range for table %s", colIdx, tableName)
	}

	return c.Op.Evaluate(tableRow[colIdx], c.Value)
}

type CustomCondition func(row map[string][]string, tables map[string]*Table) (bool, error)

func (fn *CustomCondition) Type() string {
	return "Custom"
}

func (fn *CustomCondition) Evaluate(row map[string][]string, tables map[string]*Table) (bool, error) {
	if fn == nil {
		return false, &ErrInvalidQuery{"custom condition function is nil"}
	}
	return (*fn)(row, tables)
}

type CompositeCondition struct {
	Left     Condition
	Right    Condition
	Operator LogicalOperator
}

func (c *CompositeCondition) Type() string {
	return "Composite"
}

func (c *CompositeCondition) Evaluate(row map[string][]string, tables map[string]*Table) (bool, error) {
	if c.Left == nil || c.Right == nil {
		return false, &ErrInvalidQuery{"composite condition requires both left and right conditions"}
	}

	leftResult, err := c.Left.Evaluate(row, tables)
	if err != nil {
		return false, fmt.Errorf("left condition error: %w", err)
	}

	if c.Operator == And && !leftResult {
		return false, nil
	}

	if c.Operator == Or && leftResult {
		return true, nil
	}

	rightResult, err := c.Right.Evaluate(row, tables)
	if err != nil {
		return false, fmt.Errorf("right condition error: %w", err)
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

func NewSimpleCondition(column, operator, value string) (*SimpleCondition, error) {
	if column == "" {
		return nil, &ErrInvalidQuery{"column name cannot be empty"}
	}

	op, err := GetOperator(operator)
	if err != nil {
		return nil, &ErrInvalidQuery{fmt.Sprintf("invalid operator: %s", operator)}
	}

	return &SimpleCondition{
		Column: column,
		Op:     op,
		Value:  value,
	}, nil
}

func NewCompositeCondition(left, right Condition, operator string) (*CompositeCondition, error) {
	if left == nil || right == nil {
		return nil, &ErrInvalidQuery{"both conditions must be non-nil"}
	}

	op, err := GetOperator(operator)
	if err != nil {
		return nil, &ErrInvalidQuery{fmt.Sprintf("invalid composite operator: %s", operator)}
	}

	logicalOp, ok := op.(LogicalOperator)
	if !ok {
		return nil, &ErrInvalidQuery{fmt.Sprintf("operator %s is not a logical operator", operator)}
	}

	return &CompositeCondition{
		Left:     left,
		Right:    right,
		Operator: logicalOp,
	}, nil
}
