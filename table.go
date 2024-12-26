package csvsql

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
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
