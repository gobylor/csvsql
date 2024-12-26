package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gobylor/csvsql"
)

func main() {
	// Initialize the engine with error handling
	eng := csvsql.NewEngine()
	if err := initializeTables(eng); err != nil {
		log.Fatalf("Failed to initialize tables: %v", err)
	}

	// Run all example queries
	runExamples(eng)
}

func initializeTables(eng *csvsql.Engine) error {
	tables := map[string]string{
		"users":  "data/users.csv",
		"orders": "data/orders.csv",
	}

	for name, path := range tables {
		if err := eng.CreateTable(name, path); err != nil {
			return fmt.Errorf("failed to register table %s: %v", name, err)
		}
	}
	return nil
}

func runExamples(eng *csvsql.Engine) {
	examples := []struct {
		name         string
		queryBuilder func() (*csvsql.Query, error)
	}{
		{"Basic SELECT with WHERE condition", example1},
		{"Custom function filtering", example2},
		{"JOIN with condition", example3},
		{"Multiple conditions (AND)", example4},
		{"UNION operation", example5},
		{"Wildcard SELECT", example6},
		{"Custom SELECT fields", example7},
	}

	for _, ex := range examples {
		fmt.Printf("\n=== %s ===\n", ex.name)
		query, err := ex.queryBuilder()
		if err != nil {
			log.Printf("Failed to build query for %s: %v", ex.name, err)
			continue
		}

		results, err := eng.ExecuteQuery(query)
		if err != nil {
			log.Printf("Failed to execute query for %s: %v", ex.name, err)
			continue
		}

		printResults(results)
	}
}

// Example 1: Basic SELECT with WHERE condition
func example1() (*csvsql.Query, error) {
	return csvsql.NewQuery().
		Select("name", "age", "email", "city").
		From("users").
		Where("age", ">", "25").
		Build()
}

// Example 2: Custom function filtering
func example2() (*csvsql.Query, error) {
	return csvsql.NewQuery().
		Select("name", "email", "registration_date").
		From("users").
		WhereFunc(func(row map[string][]string, tables map[string]*csvsql.Table) (bool, error) {
			table := tables["users"]
			emailIdx, err := table.GetColumnIndex("email")
			if err != nil {
				return false, err
			}
			dateIdx, err := table.GetColumnIndex("registration_date")
			if err != nil {
				return false, err
			}

			// Check if user has Gmail and registered in first quarter
			userRow := row["users"]
			isGmail := strings.Contains(userRow[emailIdx], "@gmail.com")
			regDate, _ := time.Parse("2006-01-02", userRow[dateIdx])
			isQ1 := regDate.Before(time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC))

			return isGmail && isQ1, nil
		}).
		Build()
}

// Example 3: JOIN with condition
func example3() (*csvsql.Query, error) {
	return csvsql.NewQuery().
		Select("users.name", "users.email", "orders.product", "orders.amount").
		From("users").
		InnerJoin("orders").
		On("users", "id", "=", "orders", "user_id").
		Build()
}

// Example 4: Multiple conditions (AND)
func example4() (*csvsql.Query, error) {
	baseQuery := csvsql.NewQuery().
		Select("name", "age", "email").
		From("users").
		Where("age", ">", "30")

	return baseQuery.
		And(csvsql.NewQuery().Where("age", string(csvsql.LessThan), "50")).
		Build()
}

// Example 5: UNION operation
func example5() (*csvsql.Query, error) {
	highValueOrders := csvsql.NewQuery().
		Select("users.name", "orders.product", "orders.amount").
		From("users").
		InnerJoin("orders").
		On("users", "id", "=", "orders", "user_id").
		Where("orders.amount", ">", "500")

	lowValueOrders := csvsql.NewQuery().
		Select("users.name", "orders.product", "orders.amount").
		From("users").
		InnerJoin("orders").
		On("users", "id", "", "orders", "user_id").
		Where("orders.amount", "<", "100")

	return highValueOrders.Union(lowValueOrders).Build()
}

// Example 6: Wildcard SELECT
func example6() (*csvsql.Query, error) {
	return csvsql.NewQuery().
		Select("*").
		From("users").
		InnerJoin("orders").
		On("users", "id", "=", "orders", "user_id").
		Build()
}

// Example 7: Custom SELECT fields
func example7() (*csvsql.Query, error) {
	return csvsql.NewQuery().
		Select("name", "age").
		SelectCustom("age_category", func(row map[string][]string, tables map[string]*csvsql.Table) (string, error) {
			userRow := row["users"]
			ageIdx, err := tables["users"].GetColumnIndex("age")
			if err != nil {
				return "", err
			}

			age := userRow[ageIdx]
			ageInt, err := strconv.Atoi(age)
			if err != nil {
				return "", err
			}

			switch {
			case ageInt < 25:
				return "Young", nil
			case ageInt < 50:
				return "Middle-aged", nil
			default:
				return "Senior", nil
			}
		}).
		From("users").
		Build()
}

func printResults(results [][]string) {
	if len(results) == 0 {
		fmt.Println("No results found")
		return
	}

	// Calculate column widths
	colWidths := make([]int, len(results[0]))
	for _, row := range results {
		for i, cell := range row {
			if len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	// Print headers
	printRow(results[0], colWidths)
	printSeparator(colWidths)

	// Print data rows
	for _, row := range results[1:] {
		printRow(row, colWidths)
	}
	fmt.Println()
}

func printRow(row []string, colWidths []int) {
	for i, cell := range row {
		fmt.Printf("%-*s", colWidths[i]+2, cell)
	}
	fmt.Println()
}

func printSeparator(colWidths []int) {
	for _, width := range colWidths {
		fmt.Print(strings.Repeat("-", width+2))
	}
	fmt.Println()
}
