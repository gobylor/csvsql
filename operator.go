package csvsql

import (
	"fmt"
	"regexp"
	"strings"
)

type Operator interface {
	Evaluate(left, right string) (bool, error)
	String() string
}

type ComparisonOperator string

const (
	Equal            ComparisonOperator = "="
	NotEqual         ComparisonOperator = "!="
	GreaterThan      ComparisonOperator = ">"
	GreaterThanEqual ComparisonOperator = ">="
	LessThan         ComparisonOperator = "<"
	LessThanEqual    ComparisonOperator = "<="
)

func (op ComparisonOperator) Evaluate(left, right string) (bool, error) {
	switch op {
	case Equal:
		return left == right, nil
	case NotEqual:
		return left != right, nil
	case GreaterThan:
		return left > right, nil
	case GreaterThanEqual:
		return left >= right, nil
	case LessThan:
		return left < right, nil
	case LessThanEqual:
		return left <= right, nil
	default:
		return false, fmt.Errorf("unsupported operator: %s", op)
	}
}

func (op ComparisonOperator) String() string {
	return string(op)
}

// LogicalOperator represents logical operators (AND, OR)
type LogicalOperator string

const (
	And LogicalOperator = "AND"
	Or  LogicalOperator = "OR"
)

func (op LogicalOperator) Evaluate(left, right string) (bool, error) {
	leftBool := left == "true"
	rightBool := right == "true"

	switch op {
	case And:
		return leftBool && rightBool, nil
	case Or:
		return leftBool || rightBool, nil
	default:
		return false, fmt.Errorf("unsupported logical operator: %s", op)
	}
}

func (op LogicalOperator) String() string {
	return string(op)
}

type LikeOperator struct{}

func (op LikeOperator) Evaluate(value, pattern string) (bool, error) {
	pattern = strings.ReplaceAll(pattern, "%", ".*")
	pattern = strings.ReplaceAll(pattern, "_", ".")

	match, err := regexp.MatchString("^"+pattern+"$", value)
	if err != nil {
		return false, fmt.Errorf("invalid LIKE pattern: %w", err)
	}
	return match, nil
}

func (op LikeOperator) String() string {
	return "LIKE"
}

func GetOperator(op string) (Operator, error) {
	switch ComparisonOperator(op) {
	case Equal, NotEqual, GreaterThan, GreaterThanEqual, LessThan, LessThanEqual:
		return ComparisonOperator(op), nil
	}

	switch LogicalOperator(op) {
	case And, Or:
		return LogicalOperator(op), nil
	}

	if op == "LIKE" {
		return &LikeOperator{}, nil
	}

	return nil, fmt.Errorf("unsupported operator: %s", op)
}
