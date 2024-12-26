# CSVSQL

[![Go Reference](https://pkg.go.dev/badge/github.com/gobylor/csvsql.svg)](https://pkg.go.dev/github.com/gobylor/csvsql)
[![Go Report Card](https://goreportcard.com/badge/github.com/gobylor/csvsql)](https://goreportcard.com/report/github.com/gobylor/csvsql)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

CSVSQL is a powerful Go library that enables SQL-like querying capabilities over CSV files. It provides a fluent query builder interface and supports various SQL operations including SELECT, WHERE, JOIN, and UNION.

## 🌟 Features

- 🔍 **SQL-like Query Interface**: Familiar SQL syntax for querying CSV files
- 📁 **Multiple File Formats**: 
  - CSV files
  - Excel (XLSX) files
- 🔄 **Rich Query Operations**: 
  - JOIN operations (INNER, LEFT, RIGHT)
    - Standard column equality joins
    - Custom join conditions with `OnFunc`
  - WHERE clauses with multiple conditions
    - Standard comparison operators
    - Custom filtering with `WhereFunc`
  - SELECT operations
    - Standard column selection
    - Custom computed columns with `SelectCustom`
  - UNION and UNION ALL
  - Column and table aliasing
  - Wildcard selects (`SELECT *` and `table.*`)
  - Export query results to CSV
- 🎯 **Advanced Filtering**: 
  - Support for custom filtering functions
  - Multiple comparison operators
  - LIKE pattern matching
  - Complex conditions with AND/OR
- 🔒 **Type Safety**: Type-safe query building with compile-time checks
- 🚀 **Performance**: Efficient memory usage and optimized operations
- 🛡️ **Error Handling**: Comprehensive error checking and descriptive messages

## 📦 Installation

```bash
go get github.com/gobylor/csvsql
```

## 🚀 Quick Start

### Basic Setup
```go
package main

import (
    "fmt"
    "github.com/gobylor/csvsql"
)

func main() {
    eng := csvsql.NewEngine()
    eng.CreateTable("users", "data/users.csv")
    eng.CreateTable("orders", "data/orders.xlsx")
}
```

### Basic Query
```go
query, _ := csvsql.NewQuery().
    Select("name", "age", "email").
    From("users").
    Where("age", ">", "25").
    Build()

results, _ := eng.ExecuteQuery(query)
for i, row := range results {
    fmt.Println(row)
}

// Export results to CSV
err := eng.ExportToCSV(query, "output.csv")
if err != nil {
    log.Fatal(err)
}
```

### Using Wildcards
```go
// Select all columns from all involved tables
query, _ := csvsql.NewQuery().
    Select("*").
    From("users").
    InnerJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    Build()

// Select all columns from a specific table
query, _ = csvsql.NewQuery().
    Select("users.*", "orders.amount").
    From("users").
    InnerJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    Build()
```

### Join Operations
```go
// Inner Join
query, _ := csvsql.NewQuery().
    Select("users.name", "orders.product").
    From("users").
    InnerJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    Build()

// Left Join
query, _ = csvsql.NewQuery().
    Select("users.name", "orders.product").
    From("users").
    LeftJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    Build()

// Right Join
query, _ = csvsql.NewQuery().
    Select("users.name", "orders.product").
    From("users").
    RightJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    Build()
```

### Custom Column Computation
```go
// Basic custom column computation
query, _ := csvsql.NewQuery().
    Select("name", "age").
    SelectCustom("age_category", func(row map[string][]string, tables map[string]*csvsql.Table) (string, error) {
        age := csvsql.GetRow(row, tables, "users").Get("age").MustInt()
        
        switch {
        case age < 25:
            return "Young", nil
        case age < 50:
            return "Middle-aged", nil
        default:
            return "Senior", nil
        }
    }).
    From("users").
    Build()

// Complex computed columns
query, _ = csvsql.NewQuery().
    Select("users.name").
    SelectCustom("total_spending", func(row map[string][]string, tables map[string]*csvsql.Table) (string, error) {
        orders := csvsql.GetRow(row, tables, "orders")
        amount := orders.Get("amount").MustFloat()
        tax := orders.Get("tax").MustFloat()
        
        return fmt.Sprintf("%.2f", amount + tax), nil
    }).
    From("users").
    InnerJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    Build()
```

### Advanced Filtering with Custom Functions
```go
// Basic custom filtering
query, _ := csvsql.NewQuery().
    Select("name", "email", "registration_date").
    From("users").
    WhereFunc(func(row map[string][]string, tables map[string]*csvsql.Table) (bool, error) {
        users := csvsql.GetRow(row, tables, "users")
        email := users.Get("email").Must()
        return strings.Contains(email, "@gmail.com"), nil
    }).
    Build()

// Complex filtering with multiple conditions
query, _ = csvsql.NewQuery().
    Select("users.name", "orders.product", "orders.amount").
    From("users").
    InnerJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    WhereFunc(func(row map[string][]string, tables map[string]*csvsql.Table) (bool, error) {
        users := csvsql.GetRow(row, tables, "users")
        orders := csvsql.GetRow(row, tables, "orders")
        
        // Complex filtering logic
        userType := users.Get("user_type").Must()
        orderAmount := orders.Get("amount").MustFloat()
        orderDate := orders.Get("order_date").MustDate()
        
        isVIP := userType == "VIP"
        isHighValue := orderAmount > 1000
        isRecent := orderDate.After(time.Now().AddDate(0, -3, 0))
        
        return isVIP && isHighValue && isRecent, nil
    }).
    Build()
```

The custom function features allow you to:
- Create computed columns with complex logic
- Implement advanced filtering conditions
- Access and manipulate data from multiple tables
- Perform type-safe operations with built-in conversion methods
- Combine multiple conditions in sophisticated ways

### Advanced Filtering
```go
// Multiple conditions with AND
query, _ := csvsql.NewQuery().
    Select("name", "age", "email").
    From("users").
    Where("age", ">", "30").
    And(csvsql.NewQuery().Where("age", "<", "50")).
    Build()

// Pattern matching with LIKE
query, _ = csvsql.NewQuery().
    Select("name", "email").
    From("users").
    Where("email", "LIKE", "%@gmail.com").
    Build()

// Custom filtering function
query, _ = csvsql.NewQuery().
    Select("name", "email", "registration_date").
    From("users").
    WhereFunc(func(row map[string][]string, tables map[string]*csvsql.Table) (bool, error) {
        users := csvsql.GetRow(row, tables, "users")
        
        email := users.Get("email").Must()
        regDate := users.Get("registration_date").MustDate()
        
        isGmail := strings.Contains(email, "@gmail.com")
        isBeforeQ2_2023 := regDate.Before(time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC))
        
        return isGmail && isBeforeQ2_2023, nil
    }).
    Build()
```

### UNION Operations
```go
// UNION (removes duplicates)
highValue := csvsql.NewQuery().
    Select("users.name", "orders.amount").
    From("users").
    InnerJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    Where("orders.amount", ">", "500")

lowValue := csvsql.NewQuery().
    Select("users.name", "orders.amount").
    From("users").
    InnerJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    Where("orders.amount", "<", "100")

query, _ := highValue.Union(lowValue).Build()

// UNION ALL (keeps duplicates)
query, _ = highValue.UnionAll(lowValue).Build()
```

### Custom Join Conditions
```go
// Join with custom condition function
query, _ := csvsql.NewQuery().
    Select("users.name", "orders.product", "orders.amount").
    From("users").
    InnerJoin("orders").
    OnFunc(func(row map[string][]string, tables map[string]*csvsql.Table) (bool, error) {
        users := csvsql.GetRow(row, tables, "users")
        orders := csvsql.GetRow(row, tables, "orders")
        
        // Get values with type conversion using the fluent API
        userId := users.Get("id").Must()
        orderUserId := orders.Get("user_id").Must()
        orderAmount := orders.Get("amount").MustFloat()
        
        // Custom join condition: match user_id and amount > 100
        return userId == orderUserId && orderAmount > 100, nil
    }).
    Build()

// Complex join conditions with multiple criteria
query, _ = csvsql.NewQuery().
    Select("users.name", "orders.product", "inventory.stock").
    From("users").
    InnerJoin("orders").
    OnFunc(func(row map[string][]string, tables map[string]*csvsql.Table) (bool, error) {
        users := csvsql.GetRow(row, tables, "users")
        orders := csvsql.GetRow(row, tables, "orders")
        
        // Complex business logic for joining
        userId := users.Get("id").Must()
        orderUserId := orders.Get("user_id").Must()
        orderDate := orders.Get("order_date").MustDate()
        userType := users.Get("user_type").Must()
        
        // Join only VIP users' orders from the last month
        isVIP := userType == "VIP"
        isRecentOrder := orderDate.After(time.Now().AddDate(0, -1, 0))
        
        return userId == orderUserId && isVIP && isRecentOrder, nil
    }).
    Build()
```

The `OnFunc` feature allows you to:
- Define complex join conditions with custom logic
- Access and compare multiple columns from both tables
- Implement business-specific joining rules
- Perform type conversions and data validation during joins
- Combine multiple conditions in a single join criteria

## 🛠️ Supported Operations

### Column Selection
- Regular columns: `Select("name", "age")`
- All columns: `Select("*")`
- Table-specific columns: `Select("users.*")`
- Mixed selection: `Select("users.*", "orders.amount")`
- Custom computed columns: `SelectCustom("age_category", computeFunc)`

### Data Access
- Safe access: `row.Get("column")`
- String values: `row.Get("column").Must()` or `row.Get("column").String()`
- Integer values: `row.Get("column").MustInt()` or `row.Get("column").Int()`
- Float values: `row.Get("column").MustFloat()` or `row.Get("column").Float()`
- Date values: `row.Get("column").MustDate()` or `row.Get("column").Date()`
- DateTime values: `row.Get("column").MustDateTime()` or `row.Get("column").DateTime()`
- Custom time format: `row.Get("column").MustTime(layout)` or `row.Get("column").Time(layout)`
- Boolean values: `row.Get("column").MustBool()` or `row.Get("column").Bool()`

### Comparison Operators
- `=` Equal
- `!=` Not Equal
- `>` Greater Than
- `>=` Greater Than or Equal
- `<` Less Than
- `<=` Less Than or Equal
- `LIKE` Pattern Matching (supports `%` and `_` wildcards)

### Logical Operators
- `AND`
- `OR`

### Join Types
- `INNER JOIN`
- `LEFT JOIN`
- `RIGHT JOIN`

### Set Operations
- `UNION` (removes duplicates)
- `UNION ALL` (keeps duplicates)

### Pattern Matching
- `%` matches any sequence of characters
- `_` matches any single character

## 📊 Data Types

Supported data types for comparisons:
- String
- Integer
- Float
- Date (YYYY-MM-DD format)

## 🤝 Contributing

We welcome contributions! Here's how you can help:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

For major changes, please open an issue first to discuss what you would like to change.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
