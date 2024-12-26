package csvsql

import (
	"fmt"
	"strings"
)

type Engine struct {
	tables map[string]*Table
}

func NewEngine() *Engine {
	return &Engine{
		tables: make(map[string]*Table),
	}
}

func (e *Engine) CreateTable(alias, filepath string) error {
	table, err := NewTableFromCSV(alias, filepath)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	e.tables[alias] = table
	return nil
}

func (e *Engine) ExecuteQuery(q *Query) ([][]string, error) {
	results, err := e.executeQueryInternal(q)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	if q.Union != nil {
		return e.handleUnionOperation(q, results)
	}

	return results, nil
}

func (e *Engine) executeQueryInternal(q *Query) ([][]string, error) {
	if err := e.validateQuery(q); err != nil {
		return nil, err
	}

	mainTable := e.tables[q.From.Table]
	joinedRows := e.initializeJoinedRows(mainTable)

	if err := e.processJoins(q, &joinedRows); err != nil {
		return nil, err
	}

	if err := e.applyWhereCondition(q, &joinedRows); err != nil {
		return nil, err
	}

	return e.projectColumns(q, joinedRows)
}

func (e *Engine) validateQuery(q *Query) error {
	if q.From == nil {
		return fmt.Errorf("FROM clause is required")
	}

	mainTable, ok := e.tables[q.From.Table]
	if !ok {
		return fmt.Errorf("table %s not found", q.From.Table)
	}

	if q.Select == nil {
		q.Select = &SelectComponent{Columns: mainTable.Headers}
	}

	return nil
}

type JoinedRow struct {
	mainRow    []string
	mainTable  string
	joinedRows map[string][]string
	isFiltered bool
}

func (e *Engine) initializeJoinedRows(mainTable *Table) []JoinedRow {
	joinedRows := make([]JoinedRow, 0, len(mainTable.Rows))
	for _, mainRow := range mainTable.Rows {
		joinedRows = append(joinedRows, JoinedRow{
			mainRow:    mainRow,
			mainTable:  mainTable.Name,
			joinedRows: make(map[string][]string),
			isFiltered: false,
		})
	}
	return joinedRows
}

func (e *Engine) processJoins(q *Query, joinedRows *[]JoinedRow) error {
	for _, join := range q.Joins {
		joinedTable, ok := e.tables[join.Table]
		if !ok {
			return fmt.Errorf("join table %s not found", join.Table)
		}

		if err := e.performJoin(join, joinedTable, joinedRows); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) performJoin(join *JoinComponent, joinedTable *Table, joinedRows *[]JoinedRow) error {
	var newJoinedRows []JoinedRow

	for _, jr := range *joinedRows {
		if jr.isFiltered {
			continue
		}

		matched := false
		for _, joinRow := range joinedTable.Rows {
			if match, err := e.evaluateJoinCondition(join, jr, joinRow, joinedTable); err != nil {
				return err
			} else if match {
				newJr := e.createNewJoinedRow(jr, join.Table, joinRow)
				newJoinedRows = append(newJoinedRows, newJr)
				matched = true
			}
		}

		if !matched && join.JoinType == InnerJoin {
			jr.isFiltered = true
		}
	}

	if len(newJoinedRows) > 0 {
		*joinedRows = newJoinedRows
	}
	return nil
}

func (e *Engine) evaluateJoinCondition(join *JoinComponent, jr JoinedRow, joinRow []string, joinedTable *Table) (bool, error) {
	if join.Condition == nil {
		return true, nil
	}

	rowMap := make(map[string][]string)
	tableMap := make(map[string]*Table)

	rowMap[jr.mainTable] = jr.mainRow
	tableMap[jr.mainTable] = e.tables[jr.mainTable]

	for tableName, row := range jr.joinedRows {
		rowMap[tableName] = row
		tableMap[tableName] = e.tables[tableName]
	}

	rowMap[join.Table] = joinRow
	tableMap[join.Table] = joinedTable

	return join.Condition.EvaluateJoin(rowMap, tableMap)
}

func (e *Engine) createNewJoinedRow(jr JoinedRow, tableName string, joinRow []string) JoinedRow {
	newJr := JoinedRow{
		mainRow:    jr.mainRow,
		mainTable:  jr.mainTable,
		joinedRows: make(map[string][]string),
		isFiltered: false,
	}
	for k, v := range jr.joinedRows {
		newJr.joinedRows[k] = v
	}
	newJr.joinedRows[tableName] = joinRow
	return newJr
}

func (e *Engine) createCombinedRow(jr JoinedRow) map[string][]string {
	combinedRow := make(map[string][]string)

	combinedRow[jr.mainTable] = jr.mainRow

	for tableName, row := range jr.joinedRows {
		combinedRow[tableName] = row
	}
	return combinedRow
}

func (e *Engine) findMainTable(row []string) *Table {
	for _, table := range e.tables {
		if len(table.Headers) == len(row) {
			return table
		}
	}
	return nil
}

func (e *Engine) applyWhereCondition(q *Query, joinedRows *[]JoinedRow) error {
	if q.Where == nil {
		return nil
	}

	for i := range *joinedRows {
		if (*joinedRows)[i].isFiltered {
			continue
		}

		tableData := e.createTableDataMap()
		combinedRow := e.createCombinedRow((*joinedRows)[i])

		match, err := q.Where.Condition.Evaluate(combinedRow, tableData)
		if err != nil {
			return fmt.Errorf("where condition evaluation failed: %w", err)
		}
		if !match {
			(*joinedRows)[i].isFiltered = true
		}
	}
	return nil
}

func (e *Engine) createTableDataMap() map[string]*Table {
	tableData := make(map[string]*Table, len(e.tables))
	for name, table := range e.tables {
		tableData[name] = table
	}
	return tableData
}

func (e *Engine) projectColumns(q *Query, joinedRows []JoinedRow) ([][]string, error) {
	var joinedTables []string
	for _, join := range q.Joins {
		joinedTables = append(joinedTables, join.Table)
	}

	// Get regular columns
	expandedColumns, err := q.Select.expandWildcards(e.tables, q.From.Table, joinedTables)
	if err != nil {
		return nil, fmt.Errorf("failed to expand wildcards: %w", err)
	}

	headers := expandedColumns
	for _, customCol := range q.Select.CustomColumns {
		headers = append(headers, customCol.Name)
	}

	results := [][]string{headers}

	for _, jr := range joinedRows {
		if jr.isFiltered {
			continue
		}

		resultRow, err := e.createResultRow(expandedColumns, jr, q)
		if err != nil {
			return nil, err
		}
		results = append(results, resultRow)
	}

	return results, nil
}

func (e *Engine) createResultRow(columns []string, jr JoinedRow, q *Query) ([]string, error) {
	var resultRow []string

	for _, col := range columns {
		val, err := e.getColumnValue(col, jr, e.tables[jr.mainTable])
		if err != nil {
			return nil, fmt.Errorf("failed to get column value: %w", err)
		}
		resultRow = append(resultRow, val)
	}

	if len(q.Select.CustomColumns) > 0 {
		combinedRow := e.createCombinedRow(jr)
		tableData := e.createTableDataMap()

		for _, customCol := range q.Select.CustomColumns {
			val, err := customCol.Func(combinedRow, tableData)
			if err != nil {
				return nil, fmt.Errorf("failed to compute custom column %s: %w", customCol.Name, err)
			}
			resultRow = append(resultRow, val)
		}
	}

	return resultRow, nil
}

func (e *Engine) getColumnValue(col string, jr JoinedRow, mainTable *Table) (string, error) {
	parts := strings.Split(col, ".")
	var tableName, colName string

	if len(parts) == 2 {
		tableName, colName = parts[0], parts[1]
	} else if len(parts) == 1 {
		colName = parts[0]
		tableName = e.findTableForColumn(colName, jr.mainTable)
		if tableName == "" {
			return "", fmt.Errorf("column not found in any table: %s", colName)
		}
	} else {
		return "", fmt.Errorf("invalid column name format: %s", col)
	}

	return e.extractColumnValue(tableName, colName, jr)
}

func (e *Engine) findTableForColumn(colName string, mainTableName string) string {
	mainTable := e.tables[mainTableName]
	if _, err := mainTable.GetColumnIndex(colName); err == nil {
		return mainTableName
	}

	foundInTable := ""
	for tableName, table := range e.tables {
		if _, err := table.GetColumnIndex(colName); err == nil {
			if foundInTable != "" {
				return "" // Ambiguous column
			}
			foundInTable = tableName
		}
	}

	return foundInTable
}

func (e *Engine) extractColumnValue(tableName, colName string, jr JoinedRow) (string, error) {
	table := e.tables[tableName]
	idx, err := table.GetColumnIndex(colName)
	if err != nil {
		return "", err
	}

	if tableName == jr.mainTable {
		return jr.mainRow[idx], nil
	}

	joinedRow, ok := jr.joinedRows[tableName]
	if !ok {
		return "", fmt.Errorf("table %s not found in joined result", tableName)
	}

	return joinedRow[idx], nil
}

func (e *Engine) handleUnionOperation(q *Query, results [][]string) ([][]string, error) {
	if len(results) == 0 {
		return results, nil
	}

	baseColumns := len(results[0])
	for _, unionQuery := range q.Union.Queries {
		unionResults, err := e.executeQueryInternal(unionQuery)
		if err != nil {
			return nil, fmt.Errorf("union query execution failed: %w", err)
		}
		if len(unionResults) > 0 && len(unionResults[0]) != baseColumns {
			return nil, fmt.Errorf("UNION queries must have the same number of columns")
		}
	}

	return e.mergeUnionResults(q, results)
}

func (e *Engine) mergeUnionResults(q *Query, baseResults [][]string) ([][]string, error) {
	finalResults := [][]string{baseResults[0]}
	seen := make(map[string]bool)

	processRow := func(row []string) {
		if q.Union.UnionKind == UnionAll {
			finalResults = append(finalResults, row)
			return
		}

		key := createRowKey(row)
		if !seen[key] {
			seen[key] = true
			finalResults = append(finalResults, row)
		}
	}

	for _, row := range baseResults[1:] {
		processRow(row)
	}

	for _, unionQuery := range q.Union.Queries {
		unionResults, err := e.executeQueryInternal(unionQuery)
		if err != nil {
			return nil, err
		}

		for _, row := range unionResults[1:] {
			processRow(row)
		}
	}

	return finalResults, nil
}

func createRowKey(row []string) string {
	var key strings.Builder
	for i, val := range row {
		if i > 0 {
			key.WriteString("|")
		}
		key.WriteString(strings.TrimSpace(val))
	}
	return key.String()
}
