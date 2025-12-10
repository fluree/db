# Iceberg Virtual Graph Guide

This guide covers the Iceberg virtual graph integration for Fluree, enabling SPARQL queries over Apache Iceberg tables with predicate pushdown optimization.

## Overview

Iceberg virtual graphs allow you to query Apache Iceberg tables using SPARQL, with automatic translation of RDF patterns to Iceberg table scans. The integration supports:

- **R2RML mappings** for RDF-to-relational translation
- **Predicate pushdown** for efficient filtering at the storage layer
- **Time-travel queries** via Iceberg snapshots
- **Multi-table support** with automatic routing

## Configuration

### Basic Configuration

```clojure
{:type :iceberg
 :name "my-iceberg-vg"
 :config {:warehouse-path "/path/to/iceberg/warehouse"
          :mapping "path/to/mapping.ttl"
          :table "namespace/tablename"}}
```

### Configuration Options

| Option | Description |
|--------|-------------|
| `:warehouse-path` | Path to Iceberg warehouse directory |
| `:mapping` | Path to R2RML mapping file (TTL format) |
| `:table` | Iceberg table name (namespace/table format) |
| `:metadata-location` | Direct path to table metadata (optional, for S3/remote) |

## R2RML Mappings

R2RML mappings define how Iceberg table columns map to RDF predicates.

### Example Mapping

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/airlines/> .

<#AirlineMapping>
    a rr:TriplesMap ;

    rr:logicalTable [
        rr:tableName "openflights/airlines"
    ] ;

    rr:subjectMap [
        rr:template "http://example.org/airlines/{id}" ;
        rr:class ex:Airline
    ] ;

    rr:predicateObjectMap [
        rr:predicate ex:name ;
        rr:objectMap [ rr:column "name" ]
    ] ;

    rr:predicateObjectMap [
        rr:predicate ex:country ;
        rr:objectMap [ rr:column "country" ]
    ] .
```

### Mapping Elements

- **`rr:logicalTable`**: Specifies the Iceberg table name
- **`rr:subjectMap`**: Defines how row IDs become RDF subject IRIs
- **`rr:predicateObjectMap`**: Maps columns to RDF predicates

## SPARQL Queries

### Basic Query

```sparql
PREFIX ex: <http://example.org/airlines/>

SELECT ?name ?country
FROM <iceberg/airlines>
WHERE {
  ?airline a ex:Airline .
  ?airline ex:name ?name .
  ?airline ex:country ?country .
}
LIMIT 100
```

### Filtering with Literals

Literal values in triple patterns are pushed down to Iceberg:

```sparql
PREFIX ex: <http://example.org/airlines/>

SELECT ?name
FROM <iceberg/airlines>
WHERE {
  ?airline ex:name ?name .
  ?airline ex:country "United States" .  # Pushed to Iceberg as equality filter
}
```

### Filtering with VALUES Clause (Recommended for IN-style queries)

The VALUES clause is the recommended way to filter on multiple discrete values. Each value is pushed down to Iceberg as an equality predicate:

```sparql
PREFIX ex: <http://example.org/airlines/>

SELECT ?name ?country
FROM <iceberg/airlines>
WHERE {
  ?airline ex:name ?name .
  ?airline ex:country ?country .
  VALUES ?country { "United States" "Canada" "Mexico" }
}
```

This query executes separate filtered scans for each country value, with predicate pushdown applied to each scan.

### Filtering with FILTER Comparisons

Comparison operators in FILTER clauses can be pushed down:

```sparql
PREFIX ex: <http://example.org/airlines/>

SELECT ?name ?id
FROM <iceberg/airlines>
WHERE {
  ?airline ex:name ?name .
  ?airline ex:id ?id .
  FILTER(?id > 1000)
}
```

Supported pushdown operators:
- `=`, `!=` (equality)
- `>`, `>=`, `<`, `<=` (range comparisons)
- `bound()`, `!bound()` (null checks)

### Aggregations

```sparql
PREFIX ex: <http://example.org/airlines/>

SELECT (COUNT(?airline) AS ?count)
FROM <iceberg/airlines>
WHERE {
  ?airline ex:country "United States" .
}
```

## Time-Travel Queries

Iceberg's snapshot-based time-travel is supported via the virtual graph alias:

### Query at Specific Time

```sparql
SELECT ?name
FROM <iceberg/airlines@iso:2024-01-15T00:00:00Z>
WHERE {
  ?airline ex:name ?name .
}
```

### Query at Specific Snapshot

```sparql
SELECT ?name
FROM <iceberg/airlines@t:12345678901234>
WHERE {
  ?airline ex:name ?name .
}
```

### Alias Format

```
<name>:<branch>@iso:<ISO-8601-timestamp>
<name>:<branch>@t:<snapshot-id>
```

## Multi-Table Support

A single Iceberg virtual graph can span multiple tables with different R2RML mappings:

```turtle
# Airlines mapping
<#AirlineMapping>
    rr:logicalTable [ rr:tableName "openflights/airlines" ] ;
    rr:subjectMap [ rr:template "http://example.org/airline/{id}" ; rr:class ex:Airline ] ;
    # ... predicates

# Airports mapping
<#AirportMapping>
    rr:logicalTable [ rr:tableName "openflights/airports" ] ;
    rr:subjectMap [ rr:template "http://example.org/airport/{id}" ; rr:class ex:Airport ] ;
    # ... predicates

# Routes mapping
<#RouteMapping>
    rr:logicalTable [ rr:tableName "openflights/routes" ] ;
    rr:subjectMap [ rr:template "http://example.org/route/{id}" ; rr:class ex:Route ] ;
    # ... predicates
```

Queries automatically route to the correct table based on predicates used:

```sparql
PREFIX ex: <http://example.org/>

SELECT ?airlineName ?airportName
FROM <iceberg/openflights>
WHERE {
  ?airline a ex:Airline .
  ?airline ex:name ?airlineName .
  ?airport a ex:Airport .
  ?airport ex:name ?airportName .
}
```

## Predicate Pushdown

The Iceberg integration automatically pushes predicates to the storage layer for efficient filtering.

### What Gets Pushed Down

| Pattern Type | Pushed Down | Example |
|--------------|-------------|---------|
| Literal in triple | Yes | `?s ex:country "US"` |
| VALUES clause | Yes | `VALUES ?x { "A" "B" }` |
| FILTER equality | Yes* | `FILTER(?x = "value")` |
| FILTER comparison | Yes* | `FILTER(?x > 100)` |
| FILTER IN | No** | `FILTER(?x IN ("A", "B"))` |

\* FILTER pushdown for non-literal comparisons requires the variable to be bound in the same table.

\*\* See Limitations section.

### Verifying Pushdown

Enable debug logging to see what predicates are pushed:

```
DEBUG f.d.v.iceberg - Iceberg query: {:table "airlines", :coalesced-predicates [{:op :eq, :column "country", :value "US"}], ...}
```

The Iceberg scan report shows the applied filter:

```
INFO o.a.iceberg.SnapshotScan - Scanning table ... with filter country = "US"
```

## Performance Tips

1. **Use VALUES for multi-value filters**: VALUES clauses push predicates to Iceberg, while FILTER IN currently does not.

2. **Filter on partition columns**: If your Iceberg table is partitioned, filtering on partition columns enables partition pruning.

3. **Project only needed columns**: Only columns referenced in the query are read from Iceberg.

4. **Use LIMIT**: Limits are passed to Iceberg for early termination.

## Limitations and Future Work

### Current Limitations

#### FILTER IN Pushdown

The `FILTER(?x IN (...))` syntax is parsed and identified as pushable, but the predicate metadata does not survive through the WHERE executor pipeline. The filter is applied client-side after reading all rows.

**Workaround**: Use VALUES clauses instead:

```sparql
# Instead of (not pushed down):
FILTER(?country IN ("United States", "Canada"))

# Use (pushed down):
VALUES ?country { "United States" "Canada" }
```

Both produce correct results, but VALUES is significantly more efficient for large tables.

#### Multi-Variable VALUES

VALUES clauses with multiple variables are not currently pushed down:

```sparql
# Not pushed down:
VALUES (?country ?status) { ("US" "active") ("CA" "active") }
```

#### IRI Values in VALUES

VALUES clauses containing IRI values (not literals) are not pushed down:

```sparql
# Not pushed down:
VALUES ?type { ex:Airline ex:Charter }
```

#### Join Pushdown

Joins between tables within the same Iceberg virtual graph are executed as nested loop joins, not pushed to Iceberg as SQL joins.

### Future Work

- [ ] Fix FILTER IN pushdown by preserving pattern metadata through WHERE executor
- [ ] Support multi-variable VALUES pushdown
- [ ] Implement join pushdown for multi-table queries
- [ ] Add support for OPTIONAL pattern pushdown
- [ ] Type coercion for numeric/timestamp columns in predicates
- [ ] Statistics-based query planning
- [ ] Parallel execution for multi-table queries
