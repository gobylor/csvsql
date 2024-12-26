package csvsql

import (
	"fmt"
	"strings"
)

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

func (s *SelectComponent) expandWildcards(tables map[string]*Table, mainTable string, joinedTables []string) ([]string, error) {
	if len(s.Columns) == 0 {
		return nil, &ErrInvalidQuery{"SELECT must specify at least one column"}
	}

	var expandedColumns []string
	seen := make(map[string]bool) // Track seen column names to avoid duplicates

	for _, col := range s.Columns {
		if col == "*" {
			// Add columns from main table first
			mainTableCols := prefixColumns(tables[mainTable].Headers, mainTable)
			for _, col := range mainTableCols {
				if !seen[col] {
					expandedColumns = append(expandedColumns, col)
					seen[col] = true
				}
			}

			// Add columns from joined tables in the order they were joined
			for _, tableName := range joinedTables {
				if table, ok := tables[tableName]; ok {
					tableCols := prefixColumns(table.Headers, tableName)
					for _, col := range tableCols {
						if !seen[col] {
							expandedColumns = append(expandedColumns, col)
							seen[col] = true
						}
					}
				}
			}
		} else if strings.HasSuffix(col, ".*") {
			tableName := strings.TrimSuffix(col, ".*")
			table, ok := tables[tableName]
			if !ok {
				return nil, fmt.Errorf("table %s not found", tableName)
			}
			tableCols := prefixColumns(table.Headers, tableName)
			for _, col := range tableCols {
				if !seen[col] {
					expandedColumns = append(expandedColumns, col)
					seen[col] = true
				}
			}
		} else {
			if !seen[col] {
				expandedColumns = append(expandedColumns, col)
				seen[col] = true
			}
		}
	}

	return expandedColumns, nil
}

func prefixColumns(columns []string, tableName string) []string {
	prefixed := make([]string, len(columns))
	for i, col := range columns {
		prefixed[i] = tableName + "." + col
	}
	return prefixed
}
