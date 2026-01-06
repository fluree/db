# Iceberg Virtual Graph Integration

Fluree supports querying Apache Iceberg tables directly via SPARQL using virtual graphs. This integration provides high-performance access to columnar data lakes with predicate pushdown, column projection, and time-travel capabilities.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Performance](#performance)
- [SPARQL Query Examples](#sparql-query-examples)
- [Configuration](#configuration)
- [R2RML Mappings](#r2rml-mappings)
- [API Reference](#api-reference)

## Overview

The Iceberg virtual graph integration allows you to:

- Query Iceberg tables using standard SPARQL syntax
- Join Iceberg data with Fluree's native graph data
- Push predicates down to the Iceberg layer for efficient filtering
- Project only needed columns to minimize I/O
- Perform time-travel queries using Iceberg snapshots

## Architecture

The integration consists of several layers:

```
SPARQL Query
     │
     ▼
┌─────────────────────────────────────┐
│  Virtual Graph Query Executor       │
│  - Pattern routing                  │
│  - Predicate extraction             │
│  - Solution transformation          │
└─────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────┐
│  Plan Compiler (Phase 3)            │
│  - ScanOp (columnar or row-based)   │
│  - HashJoinOp (multi-table joins)   │
│  - FilterOp, ProjectOp              │
└─────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────┐
│  ITabularSource Protocol            │
│  - scan-batches (row maps)          │
│  - scan-arrow-batches (columnar)    │
│  - Predicate pushdown               │
│  - Column projection                │
└─────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────┐
│  Apache Iceberg                     │
│  - Parquet file reading             │
│  - Row group pruning                │
│  - Arrow vectorized reads           │
└─────────────────────────────────────┘
```

### Execution Modes

The integration supports two execution modes:

1. **Row Maps Mode** (default): Converts Arrow batches to Clojure maps for compatibility
2. **Columnar Mode**: Returns raw Arrow `VectorSchemaRoot` batches for maximum performance

## Performance

### Benchmark Results

Benchmarks run on the OpenFlights dataset (airlines: 6,162 rows, routes: 67,663 rows):

#### Scan Method Comparison

| Method | Time | Speedup |
|--------|------|---------|
| `scan-batches` (row maps) | 31.6 ms | baseline |
| `scan-arrow-batches` (Arrow) | 10.5 ms | **3.02x** |

#### ScanOp Execution Modes

| Mode | Time | Speedup |
|------|------|---------|
| Row-maps mode | 17.6 ms | baseline |
| Columnar mode | 9.5 ms | **1.85x** |

#### Column Projection

| Columns | Time | Speedup |
|---------|------|---------|
| All 8 columns | 7.3 ms | baseline |
| 2 columns (id, name) | 4.4 ms | **1.64x** |

### Optimization Summary

| Optimization | Speedup | Applied When |
|--------------|---------|--------------|
| Raw Arrow batches | **3x** | Columnar execution enabled |
| Column projection | **1.6x** | `SELECT ?specific ?columns` (not `SELECT *`) |
| Predicate pushdown | **varies** | `FILTER` clauses, literal values in patterns |
| Combined | **3-5x** | Queries using all optimizations |

## SPARQL Query Examples

### Example 1: Simple Query with Column Projection

```sparql
PREFIX ex: <http://example.org/>

SELECT ?name ?country
WHERE {
  ?airline a ex:Airline ;
           ex:name ?name ;
           ex:country ?country .
}
```

**Optimizations applied:**
- Column projection: Only reads `name` and `country` columns (2 of 8)
- Expected speedup: ~1.6x from projection alone

### Example 2: Filtered Query with Predicate Pushdown

```sparql
PREFIX ex: <http://example.org/>

SELECT ?name ?iata
WHERE {
  ?airline a ex:Airline ;
           ex:name ?name ;
           ex:iata ?iata ;
           ex:country "United States" .
}
```

**Optimizations applied:**
- Predicate pushdown: `country = "United States"` pushed to Iceberg
- Column projection: Only reads `name`, `iata` columns
- Row group pruning at Parquet level
- Expected speedup: ~3x with columnar execution

### Example 3: IN List Filter (from VALUES)

```sparql
PREFIX ex: <http://example.org/>

SELECT ?name ?country
WHERE {
  ?airline a ex:Airline ;
           ex:name ?name ;
           ex:country ?country .

  VALUES ?country { "United States" "Canada" "Mexico" }
}
```

**Optimizations applied:**
- VALUES clause converted to `IN` predicate
- Pushed to Iceberg for row group pruning
- Columnar batch processing for remaining rows

### Example 4: Multi-Table Join Query

```sparql
PREFIX ex: <http://example.org/>

SELECT ?airlineName ?srcAirport ?dstAirport
WHERE {
  ?route a ex:Route ;
         ex:operatedBy ?airline ;
         ex:sourceAirport ?src ;
         ex:destAirport ?dst .

  ?airline ex:name ?airlineName .
  ?src ex:name ?srcAirport .
  ?dst ex:name ?dstAirport .
}
LIMIT 1000
```

**Optimizations applied:**
- Greedy join ordering based on cardinality estimation
- Hash joins between routes → airlines → airports
- Column projection on all three tables
- Arrow batch processing throughout pipeline
- Expected speedup: ~3-5x combined

### Example 5: Range Filter

```sparql
PREFIX ex: <http://example.org/>

SELECT ?name ?altitude
WHERE {
  ?airport a ex:Airport ;
           ex:name ?name ;
           ex:altitude ?altitude .

  FILTER (?altitude > 5000)
}
```

**Optimizations applied:**
- Range predicate pushed to Iceberg
- Row group pruning based on column statistics
- Only matching row groups are read

### Example 6: Aggregate Query

```sparql
PREFIX ex: <http://example.org/>

SELECT ?country (COUNT(?airline) as ?count)
WHERE {
  ?airline a ex:Airline ;
           ex:country ?country ;
           ex:active "Y" .
}
GROUP BY ?country
ORDER BY DESC(?count)
```

**Optimizations applied:**
- Equality predicate `active = "Y"` pushed down
- Column projection: Only `country` and `active` columns
- Aggregation performed on filtered results

## Configuration

### Creating an Iceberg Source

Three factory functions are available:

```clojure
(require '[fluree.db.tabular.iceberg :as iceberg])

;; 1. Hadoop-based (local filesystem, simple)
(def source
  (iceberg/create-iceberg-source
    {:warehouse-path "/path/to/warehouse"}))

;; 2. REST catalog (cloud-agnostic, recommended for production)
(def source
  (iceberg/create-rest-iceberg-source
    {:uri "http://localhost:8181"
     :store my-s3-store
     :auth-token "optional-bearer-token"}))

;; 3. Fluree storage (uses existing Fluree store)
(def source
  (iceberg/create-fluree-iceberg-source
    {:store my-fluree-store
     :warehouse-path "s3://bucket/warehouse"}))
```

### Enabling Columnar Execution

```clojure
(require '[fluree.db.virtual-graph.iceberg :as iceberg-vg])

;; Enable columnar execution globally
(binding [iceberg-vg/*columnar-execution* true]
  ;; Queries in this scope use Arrow batches
  (query db sparql-query))

;; Or per-plan compilation
(require '[fluree.db.virtual-graph.iceberg.plan :as plan])

(plan/compile-plan sources pattern-groups join-graph stats time-travel
                   {:use-arrow-batches? true})
```

## R2RML Mappings

Virtual graphs use R2RML-style mappings to translate between RDF and tabular data:

```clojure
{:table "openflights/airlines"
 :class "http://example.org/Airline"
 :subject-template "http://example.org/airline/{id}"
 :predicates
 {"http://example.org/name"
  {:type :column :value "name" :datatype :string}

  "http://example.org/country"
  {:type :column :value "country" :datatype :string}

  "http://example.org/iata"
  {:type :column :value "iata" :datatype :string}}}
```

### Join Mappings (for multi-table queries)

```clojure
{:table "openflights/routes"
 :class "http://example.org/Route"
 :subject-template "http://example.org/route/{airline_id}/{src_id}/{dst_id}"
 :predicates
 {"http://example.org/operatedBy"
  {:type :ref
   :parent-triples-map "<#AirlineMapping>"
   :join-conditions [{:child "airline_id" :parent "id"}]}

  "http://example.org/sourceAirport"
  {:type :ref
   :parent-triples-map "<#AirportMapping>"
   :join-conditions [{:child "src_id" :parent "id"}]}}}
```

## API Reference

### ITabularSource Protocol

```clojure
(defprotocol ITabularSource
  (scan-batches [this table-name opts]
    "Scan returning lazy seq of row maps.")

  (scan-arrow-batches [this table-name opts]
    "Scan returning lazy seq of Arrow VectorSchemaRoot batches.")

  (scan-rows [this table-name opts]
    "Convenience method, delegates to scan-batches.")

  (get-schema [this table-name opts]
    "Returns table schema with column types.")

  (get-statistics [this table-name opts]
    "Returns statistics for query planning.")

  (supported-predicates [this]
    "Returns set of supported predicate operations."))
```

### Scan Options

```clojure
{:columns ["col1" "col2"]      ; Column projection (nil = all)
 :predicates [{:column "x"     ; Predicate pushdown
               :op :eq
               :value 42}]
 :snapshot-id 12345678         ; Time travel by snapshot
 :as-of-time #inst "2024-01-01" ; Time travel by timestamp
 :batch-size 4096              ; Rows per batch
 :limit 1000}                  ; Max rows to return
```

### Supported Predicates

| Operation | Example | Description |
|-----------|---------|-------------|
| `:eq` | `{:op :eq :value 42}` | Equality |
| `:ne` | `{:op :ne :value 42}` | Not equal |
| `:gt` | `{:op :gt :value 0}` | Greater than |
| `:gte` | `{:op :gte :value 0}` | Greater than or equal |
| `:lt` | `{:op :lt :value 100}` | Less than |
| `:lte` | `{:op :lte :value 100}` | Less than or equal |
| `:in` | `{:op :in :value [1 2 3]}` | In list |
| `:between` | `{:op :between :value [0 100]}` | Range (inclusive) |
| `:is-null` | `{:op :is-null}` | Is null |
| `:not-null` | `{:op :not-null}` | Is not null |
| `:and` | `{:op :and :predicates [...]}` | Logical AND |
| `:or` | `{:op :or :predicates [...]}` | Logical OR |

## Running Benchmarks

```bash
# Build OpenFlights test data
make iceberg-openflights

# Run benchmarks
clojure -M:dev:iceberg -e \
  "(require 'fluree.db.iceberg-columnar-benchmark) \
   (fluree.db.iceberg-columnar-benchmark/run-benchmark)"
```

## Troubleshooting

### Verbose Hadoop Logging

Suppress Hadoop/Parquet debug logs:

```bash
FLUREE_LOG_LEVEL=error clojure -M:dev:iceberg ...
```

### Resource Leaks

Always fully consume lazy sequences from `scan-batches` and `scan-arrow-batches`, or resources may leak. The sequences auto-close when exhausted.

### GraalVM Native Image

For native image builds, ensure Iceberg and Arrow classes are included in reflection config. See `graalvm/reflect-config.json`.
