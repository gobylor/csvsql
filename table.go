package csvsql

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/xuri/excelize/v2"
)

type Table struct {
	Name      string
	Headers   []string
	Rows      [][]string
	HeaderMap map[string]int
}

func NewTableFromCSV(name, filepath string) (*Table, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("open file error: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read headers error: %w", err)
	}

	headerMap := make(map[string]int)
	for i, header := range headers {
		headerMap[strings.ToLower(header)] = i
	}

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read rows error: %w", err)
	}

	return &Table{
		Name:      name,
		Headers:   headers,
		Rows:      rows,
		HeaderMap: headerMap,
	}, nil
}

func NewTableFromXlsx(name, filepath string, sheetName ...string) (*Table, error) {
	f, err := excelize.OpenFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("open xlsx file error: %w", err)
	}
	defer f.Close()

	targetSheet := f.GetSheetList()[0]

	if len(sheetName) > 0 {
		targetSheet = sheetName[0]
	}
	if utf8.RuneCountInString(targetSheet) > 31 {
		runes := []rune(targetSheet)
		targetSheet = string(runes[:31])
	}

	rows, err := f.GetRows(targetSheet)
	if err != nil {
		return nil, fmt.Errorf("read xlsx rows error: %w", err)
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("xlsx file is empty")
	}

	headers := rows[0]
	headerMap := make(map[string]int)
	for i, header := range headers {
		headerMap[strings.ToLower(header)] = i
	}

	dataRows := make([][]string, 0, len(rows)-1)
	for _, row := range rows[1:] {
		normalizedRow := make([]string, len(headers))
		for i := range normalizedRow {
			if i < len(row) {
				normalizedRow[i] = row[i]
			}
		}
		dataRows = append(dataRows, normalizedRow)
	}

	return &Table{
		Name:      name,
		Headers:   headers,
		Rows:      dataRows,
		HeaderMap: headerMap,
	}, nil
}

func (t *Table) GetColumnIndex(column string) (int, error) {
	if idx, ok := t.HeaderMap[strings.ToLower(column)]; ok {
		return idx, nil
	}
	return -1, fmt.Errorf("column %s not found", column)
}

func (t *Table) GetColumnValue(rowIdx int, column string) (string, error) {
	idx, err := t.GetColumnIndex(column)
	if err != nil {
		return "", err
	}
	return t.Rows[rowIdx][idx], nil
}
