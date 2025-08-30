# JSON-LD Query Syntax Reference

## Overview

Fluree supports JSON-LD based analytical queries. This document provides
comprehensive syntax reference based on the Malli schema definitions and
examples from the codebase.

## Key Format Support

Fluree queries support multiple naming conventions that are automatically
normalized:

- **camelCase** (preferred): `selectOne`, `orderBy`, `groupBy`
- **kebab-case**: `select-one`, `order-by`, `group-by`  
- **Clojure Keywords**: `:select`, `:selectOne`, `:select-one`

All examples in this document use **camelCase** format as the preferred style.

## Basic Query Structure

```json
{
  "@context": <context-definition>,
  "select": <select-clause>,
  "where": <where-clause>,
  "from": <from-clause>,
  "fromNamed": <from-named-clause>,
  "orderBy": <order-by-clause>,
  "groupBy": <group-by-clause>,
  "having": <having-clause>,
  "values": <values-clause>,
  "limit": <limit-clause>,
  "offset": <offset-clause>,
  "t": <time-clause>,
  "opts": <options-map>
}
```

**Required:** One select clause (`select`, `selectOne`, `selectDistinct`, or
`construct`)
**Optional:** All other clauses

## Context Clause

Defines namespace prefixes for IRIs in the query.

### Syntax
```json
"@context": {
  "prefix": "iri",
  ...
}
```

### Examples
```json
{
  "@context": {
    "schema": "http://schema.org/",
    "ex": "http://example.org/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  }
}
```

```json
{
  "@context": [
    {"schema": "http://schema.org/"},
    {"ex": "http://example.org/"}
  ]
}
```

## Select Clause

Specifies what data to return from the query.

### Basic Select
```json
"select": ["?var1", "?var2"]
```

### Select All
```json
"select": "*"
```

### Select with Aggregation
```json
"select": [
  "?name",
  {"?avgAge": ["avg", "?age"]},
  {"?count": ["count", "?person"]}
]
```

### Select One (returns single result)
```json
"selectOne": ["?name", "?age"]
```

### Select Distinct
```json
"selectDistinct": ["?type"]
```

### Graph Crawl Selection
```json
"select": {
  "?person": [
    "schema:name",
    "schema:age",
    {
      "schema:knows": [
        "schema:name"
      ]
    }
  ]
}
```

## Where Clause

Defines patterns to match in the data.

### Basic Node Pattern
```json
"where": {
  "@id": "?person",
  "@type": "schema:Person",
  "schema:name": "?name",
  "schema:age": "?age"
}
```

### Multiple Patterns
```json
"where": [
  {"@id": "?person", "@type": "schema:Person"},
  {"@id": "?person", "schema:name": "?name"},
  {"@id": "?person", "schema:age": "?age"}
]
```

### Property Paths
```json
"where": {
  "@id": "?person",
  "schema:knows/schema:name": "?friendName"
}
```

### Nested Patterns
```json
"where": {
  "@id": "?person",
  "schema:address": {
    "@id": "?address",
    "schema:city": "?city",
    "schema:postalCode": "?zip"
  }
}
```

### Pattern Operations

#### Optional
```json
"where": [
  {"@id": "?person", "schema:name": "?name"},
  {"optional": [
    {"@id": "?person", "schema:age": "?age"}
  ]}
]
```

#### Union
```json
"where": [
  {"@id": "?person", "schema:name": "?name"},
  {"union": [
    [{"@id": "?person", "schema:email": "?contact"}],
    [{"@id": "?person", "schema:phone": "?contact"}]
  ]}
]
```

#### Filter
```json
"where": [
  {"@id": "?person", "schema:age": "?age"},
  {"filter": [">", "?age", 18]}
]
```

#### Bind
```json
"where": [
  {"@id": "?person", "schema:age": "?age"},
  {"bind": [
    ["?isAdult", [">=", "?age", 18]]
  ]}
]
```

#### Exists
```json
"where": [
  {"@id": "?person", "schema:name": "?name"},
  {"exists": [
    {"@id": "?person", "schema:age": "?age"}
  ]}
]
```

#### Not Exists
```json
"where": [
  {"@id": "?person", "schema:name": "?name"},
  {"not-exists": [
    {"@id": "?person", "schema:age": "?age"}
  ]}
]
```

#### Minus
```json
"where": [
  {"@id": "?person", "schema:name": "?name"},
  {"minus": [
    {"@id": "?person", "schema:age": "?age"}
  ]}
]
```

#### Values
```json
"where": [
  {"@id": "?person", "schema:name": "?name"},
  {"values": [
    ["?name", ["Alice", "Bob", "Charlie"]]
  ]}
]
```

## Functions and Expressions

### Comparison Operators
- `"="` - Equal
- `"!="` - Not equal
- `"<"` - Less than
- `">"` - Greater than
- `"<="` - Less than or equal
- `">="` - Greater than or equal

### Logical Operators
- `"&&"` - Logical AND
- `"||"` - Logical OR
- `"!"` - Logical NOT

### Arithmetic Operators
- `"+"` - Addition
- `"-"` - Subtraction
- `"*"` - Multiplication
- `"/"` - Division

### String Functions
- `"str"` - Convert to string
- `"strlen"` - String length
- `"substr"` - Substring
- `"strStarts"` - String starts with
- `"strEnds"` - String ends with
- `"contains"` - String contains
- `"regex"` - Regular expression match

### Test Functions
- `"bound"` - Check if variable is bound
- `"isIRI"` - Check if value is IRI
- `"isBlank"` - Check if value is blank node
- `"isLiteral"` - Check if value is literal

### Aggregate Functions
- `"count"` - Count values
- `"count-distinct"` - Count distinct values
- `"sum"` - Sum numeric values
- `"avg"` - Average of numeric values
- `"min"` - Minimum value
- `"max"` - Maximum value
- `"sample1"` - Sample one value
- `"groupconcat"` - Concatenate values

### Function Call Syntax
```json
["function-name", "arg1", "arg2", ...]
```

### Examples
```json
{"filter": [">", "?age", 18]}
{"filter": ["&&", [">", "?age", 18], ["<", "?age", 65]]}
{"filter": ["regex", "?name", "^John"]}
{"bind": [["?adult", [">=", "?age", 18]]]}
```

## Order By Clause

Sorts query results.

### Single Variable
```json
"orderBy": "?name"
```

### Multiple Variables
```json
"orderBy": ["?name", "?age"]
```

### With Direction
```json
"orderBy": [["desc", "?age"]]
```

### Mixed Directions
```json
"orderBy": [["desc", "?age"], ["asc", "?name"]]
```

## Group By Clause

Groups results by specified variables.

### Single Variable
```json
"groupBy": "?department"
```

### Multiple Variables
```json
"groupBy": ["?department", "?location"]
```

### With Aggregation
```json
{
  "select": ["?department", {"?avgAge": ["avg", "?age"]}],
  "where": {
    "@id": "?person",
    "schema:department": "?department",
    "schema:age": "?age"
  },
  "groupBy": "?department"
}
```

## Having Clause

Filters grouped results based on aggregate conditions.

### Syntax
```json
"having": ["function", "arg1", "arg2"]
```

### Examples
```json
{
  "select": ["?name", "?favNums"],
  "where": {
    "schema:name": "?name",
    "ex:favNums": "?favNums"
  },
  "groupBy": "?name",
  "having": [">=", ["count", "?favNums"], 2]
}
```

```json
{
  "select": ["?department", {"?count": ["count", "?person"]}],
  "where": {
    "@id": "?person",
    "schema:department": "?department"
  },
  "groupBy": "?department",
  "having": [">", ["count", "?person"], 5]
}
```

## Values Clause

Provides explicit values for variables.

### Syntax
```json
"values": [
  ["?var1", [value1, value2, ...]],
  ["?var2", [value1, value2, ...]]
]
```

### Example
```json
{
  "select": ["?name", "?age"],
  "where": {
    "@id": "?person",
    "schema:name": "?name",
    "schema:age": "?age"
  },
  "values": [
    ["?name", ["Alice", "Bob", "Charlie"]]
  ]
}
```

## Limit and Offset

Controls result set size and pagination.

### Syntax
```json
"limit": 10,
"offset": 20
```

### Example
```json
{
  "select": ["?name", "?age"],
  "where": {
    "@id": "?person",
    "schema:name": "?name",
    "schema:age": "?age"
  },
  "orderBy": "?name",
  "limit": 10,
  "offset": 0
}
```

## Time Clause

Queries data at specific points in time.

### Syntax
```json
"t": <time-specification>
```

### Examples
```json
"t": "2023-01-01T00:00:00.000Z"
```

```json
"t": 1000
```

## From Clause

Specifies named graphs to query.

### Syntax
```json
"from": "graph-name"
```

### Example
```json
{
  "select": ["?s", "?p", "?o"],
  "from": "my-graph",
  "where": {
    "@id": "?s",
    "?p": "?o"
  }
}
```

## Options

Query execution options.

### Syntax
```json
"opts": {
  "max-fuel": 1000000,
  "identity": "did:example:123",
  "format": "fql",
  "meta": true
}
```

### Available Options
- `max-fuel` - Maximum computational resources
- `identity` - Identity for policy evaluation
- `format` - Output format (`:fql` or `:sparql`)
- `meta` - Include metadata in results
- `policy` - Policy restrictions
- `policy-class` - Policy class restrictions
- `objectVarParsing` - Controls whether bare object strings that look like variables (e.g., `"?x"`) are parsed as variables in the WHERE clause.
  - Default: `true`
  - When `false`, scalar object values like `"?not-a-var"` are treated as string literals. Use the explicit JSON-LD form to bind a variable: `{"@variable": "?v"}`.
  - This flag does not affect variable parsing for `@id` or predicate keys; those are always treated as variables when they begin with `?`.
  - Explicit `{"@variable": "?..."}` is always honored regardless of this flag.

### Example: Literal match vs explicit variable
```json
{
  "@context": {"ex": "http://example.org/"},
  "opts": {"objectVarParsing": false},
  "select": ["?s"],
  "where": [
    {"@id": "?s", "ex:prop": "?not-a-var"}
  ]
}
```

To bind a variable when the flag is false:
```json
{
  "@context": {"ex": "http://example.org/"},
  "opts": {"objectVarParsing": false},
  "select": ["?v"],
  "where": [
    {"@id": "ex:s", "ex:prop": {"@variable": "?v"}}
  ]
}
```

## Complete Examples

### Basic Query
```json
{
  "@context": {
    "schema": "http://schema.org/",
    "ex": "http://example.org/"
  },
  "select": ["?name", "?age"],
  "where": {
    "@id": "?person",
    "@type": "schema:Person",
    "schema:name": "?name",
    "schema:age": "?age"
  },
  "orderBy": "?name"
}
```

### Aggregation Query
```json
{
  "@context": {
    "schema": "http://schema.org/"
  },
  "select": [
    "?department",
    {"?avgAge": ["avg", "?age"]},
    {"?count": ["count", "?person"]}
  ],
  "where": {
    "@id": "?person",
    "schema:department": "?department",
    "schema:age": "?age"
  },
  "groupBy": "?department",
  "having": [">", ["count", "?person"], 5],
  "orderBy": [["desc", "?avgAge"]]
}
```

### Complex Query with Filters
```json
{
  "@context": {
    "schema": "http://schema.org/",
    "ex": "http://example.org/"
  },
  "select": ["?person", "?name", "?friendName"],
  "where": [
    {
      "@id": "?person",
      "@type": "schema:Person",
      "schema:name": "?name",
      "schema:age": "?age"
    },
    {
      "@id": "?person",
      "schema:knows": "?friend"
    },
    {
      "@id": "?friend",
      "schema:name": "?friendName"
    },
    {
      "filter": ["&&", [">", "?age", 18], ["<", "?age", 65]]
    }
  ],
  "orderBy": "?name",
  "limit": 50
}
```

## Error Handling

Common query errors and their causes:

### Schema Validation Errors
- **Missing select clause**: Query must have exactly one of `select`, `select-
    one`, `select-distinct`, or `construct`
- **Invalid variable**: Variables must start with `?` (e.g., `?name`)
- **Invalid function**: Function calls must follow proper syntax: `["function-
    name", "arg1", "arg2"]`

### Type Errors
- **Invalid literal**: Check that strings, numbers, and booleans are properly
    formatted
- **Invalid IRI**: IRIs must be valid URIs or use defined prefixes

### Logic Errors
- **Unbound variables**: Variables in SELECT must be bound in WHERE clause
- **Invalid aggregation**: Aggregate functions can only be used with GROUP BY

## Performance Tips

1. **Use indexes**: Structure queries to leverage existing property and type
      indexes
2. **Limit results**: Always use LIMIT for large datasets
3. **Specific patterns**: Use specific type constraints early in WHERE clause
4. **Avoid Cartesian products**: Ensure proper joins between graph patterns
5. **Filter early**: Apply filters as early as possible in the query execution