# CSVSQL

[![Go Reference](https://pkg.go.dev/badge/github.com/gobylor/csvsql.svg)](https://pkg.go.dev/github.com/gobylor/csvsql)
[![Go Report Card](https://goreportcard.com/badge/github.com/gobylor/csvsql)](https://goreportcard.com/report/github.com/gobylor/csvsql)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

CSVSQL is a powerful Go library that enables SQL-like querying capabilities over CSV files. It provides a fluent query builder interface and supports various SQL operations including SELECT, WHERE, JOIN, and UNION.

## 🌟 Features

- 🔍 **SQL-like Query Interface**: Familiar SQL syntax for querying CSV files
- 🔄 **Rich Query Operations**: 
  - JOIN operations (INNER, LEFT, RIGHT)
  - WHERE clauses with multiple conditions
  - UNION and UNION ALL
  - Column and table aliasing
- 🎯 **Advanced Filtering**: Support for custom filtering functions
- 🔒 **Type Safety**: Type-safe query building with compile-time checks
- 🚀 **Performance**: Efficient memory usage and optimized operations
- 🛡️ **Error Handling**: Comprehensive error checking and descriptive messages

## 📦 Installation

```bash
go get github.com/gobylor/csvsql
```

## 🚀 Quick Start

```go
package main

import (
    "fmt"
    "github.com/gobylor/csvsql"
)

func main() {
    // Initialize the engine
    eng := csvsql.NewEngine()

    // Register CSV files
    if err := eng.CreateTable("users", "data/users.csv"); err != nil {
        panic(err)
    }

    // Build a query
    query, err := csvsql.NewQuery().
        Select("name", "age", "email").
        From("users").
        Where("age", ">", "25").
        And(csvsql.NewQuery().Where("city", "=", "Shanghai")).
        Build()
    if err != nil {
        panic(err)
    }

    // Execute and handle results
    results, err := eng.ExecuteQuery(query)
    if err != nil {
        panic(err)
    }

    // Print results
    for _, row := range results {
        fmt.Println(row)
    }
}
```

## 📚 Advanced Usage

### 🔄 JOIN Operations

```go
// Inner Join Example
query, _ := csvsql.NewQuery().
    Select("users.name", "orders.product", "orders.amount").
    From("users").
    InnerJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    Where("orders.amount", ">", "100").
    Build()

// Left Join Example
query, _ := csvsql.NewQuery().
    Select("users.name", "orders.product").
    From("users").
    LeftJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    Build()
```

### 🎯 Custom Filtering

```go
query, _ := csvsql.NewQuery().
    Select("name", "email", "registration_date").
    From("users").
    WhereFunc(func(row map[string][]string, tables map[string]*csvsql.Table) (bool, error) {
        userRow := row["users"]
        emailIdx, _ := tables["users"].GetColumnIndex("email")
        dateIdx, _ := tables["users"].GetColumnIndex("registration_date")
        
        // Filter Gmail users registered in Q1
        isGmail := strings.Contains(userRow[emailIdx], "@gmail.com")
        regDate, _ := time.Parse("2006-01-02", userRow[dateIdx])
        isQ1 := regDate.Before(time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC))
        
        return isGmail && isQ1, nil
    }).
    Build()
```

### 🔗 UNION Operations

```go
// Combine high-value and low-value orders
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

query := highValue.Union(lowValue).Build()
```

## 🛠️ Supported Operations

### Comparison Operators
- `=` Equal
- `!=` Not Equal
- `>` Greater Than
- `>=` Greater Than or Equal
- `<` Less Than
- `<=` Less Than or Equal
- `LIKE` Pattern Matching

### Logical Operators
- `AND`
- `OR`

### Join Types
- `INNER JOIN`
- `LEFT JOIN`
- `RIGHT JOIN`

### Set Operations
- `UNION`
- `UNION ALL`

## 📊 Data Types

Supported data types for comparisons:
- String
- Integer
- Float
- Date (YYYY-MM-DD format)

## ⚡ Performance Tips

- Tables are loaded into memory for efficient querying
- Use appropriate indexes for frequently queried columns
- Consider memory usage when working with large CSV files
- Optimize JOIN conditions for better performance
- Use custom filtering functions for complex conditions

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
