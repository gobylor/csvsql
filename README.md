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
  - Wildcard selects (`SELECT *` and `table.*`)
  - Custom column computation
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

### Basic Query
```go
query, _ := csvsql.NewQuery().
    Select("name", "age", "email").
    From("users").
    Where("age", ">", "25").
    Build()
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
query, _ := csvsql.NewQuery().
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
query, _ := csvsql.NewQuery().
    Select("users.name", "orders.product").
    From("users").
    LeftJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    Build()

// Right Join
query, _ := csvsql.NewQuery().
    Select("users.name", "orders.product").
    From("users").
    RightJoin("orders").
    On("users", "id", "=", "orders", "user_id").
    Build()
```

### Custom Column Computation
```go
query, _ := csvsql.NewQuery().
    Select("name", "age").
    SelectCustom("age_category", func(row map[string][]string, tables map[string]*Table) (string, error) {
        ageIdx, _ := tables["users"].GetColumnIndex("age")
        age, _ := strconv.Atoi(row["users"][ageIdx])
        
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
```

### Advanced Filtering
```go
// Custom filtering function
query, _ := csvsql.NewQuery().
    Select("name", "email", "registration_date").
    From("users").
    WhereFunc(func(row map[string][]string, tables map[string]*Table) (bool, error) {
        userRow := row["users"]
        emailIdx, _ := tables["users"].GetColumnIndex("email")
        dateIdx, _ := tables["users"].GetColumnIndex("registration_date")
        
        // Filter Gmail users registered before 2023-04-01
        isGmail := strings.Contains(userRow[emailIdx], "@gmail.com")
        regDate, _ := time.Parse("2006-01-02", userRow[dateIdx])
        isBeforeQ2_2023 := regDate.Before(time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC))
        
        return isGmail && isBeforeQ2_2023, nil
    }).
    Build()

// Multiple conditions with AND
query, _ := csvsql.NewQuery().
    Select("name", "age", "email").
    From("users").
    Where("age", ">", "30").
    And(csvsql.NewQuery().Where("age", "<", "50")).
    Build()

// Pattern matching with LIKE
query, _ := csvsql.NewQuery().
    Select("name", "email").
    From("users").
    Where("email", "LIKE", "%@gmail.com").
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

query := highValue.Union(lowValue).Build()

// UNION ALL (keeps duplicates)
query := highValue.UnionAll(lowValue).Build()
```

## 🛠️ Supported Operations

### Column Selection
- Regular columns: `Select("name", "age")`
- All columns: `Select("*")`
- Table-specific columns: `Select("users.*")`
- Mixed selection: `Select("users.*", "orders.amount")`
- Custom computed columns: `SelectCustom("age_category", computeFunc)`

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
