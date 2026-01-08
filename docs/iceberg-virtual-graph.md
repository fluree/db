# Iceberg Virtual Graph Integration

Fluree supports querying Apache Iceberg tables via SPARQL using **Iceberg virtual graphs** (VGs) plus a **subset of R2RML** for mapping Iceberg columns to RDF terms.

For implementation details and roadmap, see `docs/ICEBERG_SPARQL_STRATEGY.md` and `docs/ICEBERG_R2RML_SUPPORT_GAPS.md`.

## Table of Contents

- [Overview](#overview)
- [Current Status](#current-status)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [R2RML Mappings](#r2rml-mappings)
- [SPARQL Query Examples](#sparql-query-examples)
- [OPTIONAL Patterns](#optional-patterns)
- [Transitive Property Paths](#transitive-property-path-queries)
- [UNION Queries](#union-queries)
- [Predicate Pushdown](#predicate-pushdown)
- [Time-Travel Queries](#time-travel-queries)
- [Multi-Table Joins](#multi-table-joins)
- [Performance](#performance)
- [API Reference](#api-reference)
- [Troubleshooting](#troubleshooting)

## Overview

The Iceberg virtual graph integration allows you to:

- Query Iceberg tables using standard SPARQL syntax
- Push predicates down to the Iceberg layer for efficient filtering
- Project only needed columns to minimize I/O
- Perform time-travel queries using Iceberg snapshots
- Join multiple Iceberg tables (when your R2RML mapping defines RefObjectMap join edges and your SPARQL query traverses them)
- Execute OPTIONAL patterns with left outer join semantics (see limitations below)

### Requirements / Scope

- **JVM only**: Iceberg VGs require JVM Iceberg/Arrow deps.
- **R2RML is a mapping layer, not a SQL engine**: Iceberg VGs support `rr:tableName` logical tables; `rr:sqlQuery` is not supported.
- **RDF term modeling is limited**: the Iceberg R2RML subset currently focuses on subject IRI templates + column-to-literal mappings + RefObjectMap joins.

## Current Status

| Feature | Status | Notes |
|---------|--------|-------|
| Single-table queries | ✅ Complete | Full predicate pushdown |
| Multi-table joins | ✅ Complete | Hash joins when the query traverses RefObjectMap edges |
| Predicate pushdown | ✅ Complete | File/row-group pruning + row-level filtering |
| Column projection | ✅ Complete | Only requested columns read |
| Time travel | ✅ Complete | Snapshot ID or timestamp |
| VALUES clause pushdown | ✅ Complete | Converted to IN predicates |
| FILTER comparison pushdown | ✅ Complete | `=`, `!=`, `>`, `>=`, `<`, `<=` |
| Residual FILTER evaluation | ✅ Complete | Full SPARQL function support post-scan via Fluree eval |
| BIND evaluation | ✅ Complete | Full SPARQL function support post-scan via Fluree eval |
| OPTIONAL patterns | ✅ Complete | Left outer join semantics |
| Transitive property paths | ✅ Complete | `pred+` (one-or-more), `pred*` (zero-or-more) |
| Anti-joins | ✅ Complete | FILTER EXISTS, FILTER NOT EXISTS, MINUS |
| Vectorized execution | ⚠️ Experimental | Columnar plan exists, but disabled by default |
| Aggregations (GROUP BY) | ✅ Complete | COUNT, SUM, AVG, MIN, MAX, COUNT DISTINCT (computed in VG; not pushed to Iceberg) |
| HAVING | ✅ Complete | Evaluated after aggregation, before DISTINCT |
| DISTINCT | ✅ Complete | Applied in VG (correct SPARQL modifier order) |
| ORDER BY / LIMIT / OFFSET | ✅ Complete | Applied in VG (correct SPARQL modifier order) |
| UNION patterns | ✅ Complete | UNION-only queries and UNION with other patterns supported |
| Subqueries | ✅ Complete | Delegated to standard Fluree execution via `:query` patterns |

### Architecture

```
SPARQL Query
     │
     ▼
┌─────────────────────────────────────┐
│  Virtual Graph Query Executor       │
│  - Pattern routing by predicate     │
│  - Predicate extraction             │
│  - Solution transformation          │
└─────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────┐
│  Plan Compiler                      │
│  - ScanOp (columnar or row-based)   │
│  - HashJoinOp (multi-table joins)   │
│  - Left outer join for OPTIONAL     │
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

## Quick Start

### 1. Create an Iceberg virtual graph

Use `fluree.db.api/create-virtual-graph` to publish an Iceberg VG into the nameservice (loaded lazily on first query).

#### Option A: Local development (HadoopTables) — simplest

```clojure
(require '[fluree.db.api :as fluree])

(def conn @(fluree/connect-file {:storage-path "./data"}))

@(fluree/create-virtual-graph conn
   {:name "openflights-vg"
    :type :iceberg
    :config {:warehouse-path "./dev-resources/iceberg/openflights"
             :mapping "dev-resources/openflights/r2rml.ttl"}})
```

#### Option B: REST catalog (recommended for production catalogs)

REST catalog mode currently requires a Fluree `store` for file reads (e.g. an `S3Store`):

```clojure
(require '[fluree.db.api :as fluree]
         '[fluree.db.storage.s3 :as s3])

(def conn @(fluree/connect-file {:storage-path "./data"}))
(def store (s3/open "my-bucket" "my/prefix")) ;; uses AWS env vars for credentials

@(fluree/create-virtual-graph conn
   {:name "analytics-vg"
    :type :iceberg
    :config {:store store
             :catalog {:type :rest
                       :uri "http://localhost:8181"
                       :auth-token "optional-bearer-token"}
             :mapping "path/to/mapping.ttl"}})
```

### (Advanced) Create an Iceberg Source directly

```clojure
(require '[fluree.db.tabular.iceberg :as iceberg])

;; Local development with Hadoop catalog
(def source
  (iceberg/create-iceberg-source
    {:warehouse-path "/path/to/iceberg/warehouse"}))

;; Production with REST catalog
(def source
  (iceberg/create-rest-iceberg-source
    {:uri "http://localhost:8181"
     :store my-s3-store
     :auth-token "optional-bearer-token"}))
```

### 2. Define R2RML Mapping

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .

<#AirlineMapping>
    a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "openflights/airlines" ] ;
    rr:subjectMap [
        rr:template "http://example.org/airline/{id}" ;
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

### 3. Query with SPARQL

```sparql
PREFIX ex: <http://example.org/>

SELECT ?name ?country
FROM <openflights-vg>
WHERE {
  ?airline a ex:Airline ;
           ex:name ?name ;
           ex:country ?country .
  FILTER(?country = "United States")
}
LIMIT 100
```

## Configuration

### Factory Functions

Three factory functions are available depending on your deployment:

```clojure
(require '[fluree.db.tabular.iceberg :as iceberg])

;; 1. Hadoop-based (local filesystem, simple development)
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

### Virtual Graph Configuration

```clojure
{:type :iceberg
 :name "my-iceberg-vg"
 :config {:warehouse-path "/path/to/iceberg/warehouse"
          :mapping "path/to/mapping.ttl"}}
```

| Option | Description |
|--------|-------------|
| `:warehouse-path` | Path to Iceberg warehouse directory |
| `:mapping` | Path to R2RML mapping file (TTL format) |
| `:mappingInline` | Inline R2RML mapping (Turtle string or JSON-LD) |
| `:store` | Fluree store for file reads (e.g., `S3Store`, `FileStore`) |
| `:catalog` | REST catalog config, e.g. `{:type :rest :uri \"...\" :auth-token \"...\"}` |

## R2RML Mappings

R2RML mappings define how Iceberg table columns map to RDF predicates.

### Basic Mapping

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .

<#AirlineMapping>
    a rr:TriplesMap ;

    rr:logicalTable [
        rr:tableName "openflights/airlines"
    ] ;

    rr:subjectMap [
        rr:template "http://example.org/airline/{id}" ;
        rr:class ex:Airline
    ] ;

    rr:predicateObjectMap [
        rr:predicate ex:name ;
        rr:objectMap [ rr:column "name" ]
    ] ;

    rr:predicateObjectMap [
        rr:predicate ex:country ;
        rr:objectMap [ rr:column "country" ]
    ] ;

    rr:predicateObjectMap [
        rr:predicate ex:iata ;
        rr:objectMap [ rr:column "iata" ]
    ] .
```

### Join Mappings (RefObjectMap)

For multi-table queries, use `rr:parentTriplesMap` to define relationships:

```turtle
<#RouteMapping>
    a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "openflights/routes" ] ;
    rr:subjectMap [
        rr:template "http://example.org/route/{id}" ;
        rr:class ex:Route
    ] ;

    # Reference to airlines table via foreign key
    rr:predicateObjectMap [
        rr:predicate ex:operatedBy ;
        rr:objectMap [
            rr:parentTriplesMap <#AirlineMapping> ;
            rr:joinCondition [
                rr:child "airline_id" ;   # Column in routes table
                rr:parent "id"            # Column in airlines table
            ]
        ]
    ] ;

    # Reference to airports table
    rr:predicateObjectMap [
        rr:predicate ex:sourceAirport ;
        rr:objectMap [
            rr:parentTriplesMap <#AirportMapping> ;
            rr:joinCondition [
                rr:child "src_id" ;
                rr:parent "id"
            ]
        ]
    ] .
```

### Mapping Elements Reference

| Element | Description |
|---------|-------------|
| `rr:logicalTable` | Specifies the Iceberg table name |
| `rr:subjectMap` | Defines how row IDs become RDF subject IRIs |
| `rr:template` | URI template with `{column}` placeholders |
| `rr:class` | RDF class for subjects |
| `rr:predicateObjectMap` | Maps columns to RDF predicates |
| `rr:parentTriplesMap` | References another mapping for joins |
| `rr:joinCondition` | Defines join keys between tables |

## SPARQL Query Examples

### Basic Query with Column Projection

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
- Column projection: Only reads `name` and `country` columns

### Equality Filter with Predicate Pushdown

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
- Row group pruning at Parquet level

### VALUES Clause (Recommended for IN-style queries)

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

### Range Filters

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

### Multi-Table Join

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
- Hash joins across tables when the query traverses RefObjectMap edges (FK predicate)
- Column projection on all three tables

### Aggregate Query

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

**Supported aggregation functions:** COUNT, COUNT(DISTINCT), SUM, AVG, MIN, MAX

### OPTIONAL Patterns

OPTIONAL provides left outer join semantics - results are returned even when the optional pattern doesn't match.

```sparql
PREFIX ex: <http://example.org/>

SELECT ?name ?country
WHERE {
  ?airline a ex:Airline ;
           ex:name ?name .
  OPTIONAL {
    ?airline ex:country ?country .
  }
}
```

Airlines without a country value are still returned with `?country` unbound.

#### Multi-table OPTIONAL

OPTIONAL works with multi-table joins:

```sparql
PREFIX ex: <http://example.org/>

SELECT ?routeId ?airlineName
WHERE {
  ?route a ex:Route ;
         ex:routeId ?routeId .
  OPTIONAL {
    ?route ex:operatedBy ?airline .
    ?airline ex:name ?airlineName .
  }
}
```

**Note:** Complex multi-table OPTIONAL blocks (patterns spanning multiple joins within the OPTIONAL) may require careful handling. See limitations.

### Transitive Property Path Queries

Transitive property paths allow traversing relationships recursively. This is useful for hierarchical data like organizational structures, category taxonomies, or social networks.

#### One-or-More (`+`) - Forward Traversal

Find all people that Alice knows (transitively):

```sparql
PREFIX ex: <http://example.org/>

SELECT ?person
WHERE {
  ex:alice <ex:knows+> ?person .
}
```

In FQL/JSON-LD syntax:
```json
{"@context": {"ex": "http://example.org/"},
 "where": [{"@id": "ex:alice", "<ex:knows+>": "?person"}],
 "select": "?person"}
```

#### One-or-More (`+`) - Backward Traversal

Find all people who can reach Bob through the knows relationship:

```sparql
PREFIX ex: <http://example.org/>

SELECT ?person
WHERE {
  ?person <ex:knows+> ex:bob .
}
```

#### Zero-or-More (`*`) - Includes Self

Zero-or-more includes the starting node (reflexive):

```sparql
PREFIX ex: <http://example.org/>

SELECT ?person
WHERE {
  ex:alice <ex:knows*> ?person .
}
```

Returns `ex:alice` plus all transitively reachable nodes.

#### Both Variables Unbound

Find all (subject, object) pairs connected by the transitive predicate:

```sparql
PREFIX ex: <http://example.org/>

SELECT ?x ?y
WHERE {
  ?x <ex:knows+> ?y .
}
LIMIT 1000
```

**Note:** This can be expensive for large graphs. Use LIMIT.

#### Cycle Detection

The implementation uses BFS with cycle detection, so cycles in the data don't cause infinite loops:

```
ex:a knows ex:b
ex:b knows ex:c
ex:c knows ex:a  ← cycle back to ex:a
```

Query: `ex:a <ex:knows+> ?who` returns `[ex:b, ex:c]` (terminates correctly; does not re-emit the start node).

#### Depth Limit

A configurable depth limit (default: 100) prevents runaway queries on very deep hierarchies. If exceeded, a warning is logged and results up to that depth are returned.

### UNION Queries

UNION combines results from multiple query branches. Each branch can query different predicates or even different tables.

```sparql
PREFIX ex: <http://example.org/>

SELECT ?name
WHERE {
  { ?airline a ex:Airline ; ex:name ?name ; ex:country "US" }
  UNION
  { ?airline a ex:Airline ; ex:name ?name ; ex:country "DE" }
}
```

UNION can also be combined with other patterns:

```sparql
PREFIX ex: <http://example.org/>

SELECT ?airlineName ?routeSource
WHERE {
  ?route ex:operatedBy ?airline .
  ?airline ex:name ?airlineName .
  {
    ?route ex:sourceAirport "JFK"
  }
  UNION
  {
    ?route ex:sourceAirport "LAX"
  }
}
```

## Predicate Pushdown

The Iceberg integration automatically pushes predicates to the storage layer.

### What Gets Pushed Down

| Pattern Type | Pushed Down | Example |
|--------------|-------------|---------|
| Literal in triple | Yes | `?s ex:country "US"` |
| VALUES clause | Yes | `VALUES ?x { "A" "B" }` |
| FILTER equality | Yes | `FILTER(?x = "value")` |
| FILTER comparison | Yes | `FILTER(?x > 100)` |
| FILTER bound() | Yes | `FILTER(bound(?x))` |

### Supported Predicate Operators

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

### Verifying Pushdown

Enable debug logging to see what predicates are pushed:

```bash
FLUREE_LOG_LEVEL=debug clojure -M:dev:iceberg ...
```

```
DEBUG f.d.v.iceberg - Iceberg query: {:table "airlines", :predicates [{:op :eq, :column "country", :value "US"}], ...}
```

## Time-Travel Queries

Iceberg's snapshot-based time-travel is supported via the virtual graph alias.

### Query at Specific Time

```sparql
SELECT ?name
FROM <openflights-vg@iso:2024-01-15T00:00:00Z>
WHERE {
  ?airline ex:name ?name .
}
```

### Query at Specific Snapshot

```sparql
SELECT ?name
FROM <openflights-vg@t:12345678901234>
WHERE {
  ?airline ex:name ?name .
}
```

### Alias Format

```
<name>@iso:<ISO-8601-timestamp>
<name>@t:<snapshot-id>
```

## Multi-Table Joins

A single Iceberg virtual graph can span multiple tables with different R2RML mappings.

### Join Graph Construction

At virtual graph creation, join relationships are extracted from R2RML RefObjectMaps:

```clojure
;; Automatically extracted from R2RML
{:edges [{:parent-table "airlines"
          :child-table "routes"
          :parent-columns ["id"]
          :child-columns ["airline_id"]
          :predicate-iri "http://example.org/operatedBy"}]
 :tables #{"airlines" "routes" "airports"}}
```

### Join Planning

Queries automatically route to the correct tables and apply hash joins when the query traverses RefObjectMap edges:

1. **Table Identification**: Patterns are grouped by which table they reference
2. **Join Edge Traversal**: Joins are only applied when patterns use the FK predicate from the RefObjectMap
3. **Hash Join Execution**: Hash joins with proper null handling for OPTIONAL

### Example Multi-Table Query

```sparql
PREFIX ex: <http://example.org/>

SELECT ?airlineName ?airportName
FROM <openflights-vg>
WHERE {
  ?airline a ex:Airline .
  ?airline ex:name ?airlineName .
  ?airport a ex:Airport .
  ?airport ex:name ?airportName .
}
```

## Performance

### Benchmark Results

Benchmarks run on the OpenFlights dataset (airlines: 6,162 rows, routes: 67,663 rows):

#### Scan Method Comparison

| Method | Time | Speedup |
|--------|------|---------|
| `scan-batches` (row maps) | 31.6 ms | baseline |
| `scan-arrow-batches` (Arrow) | 10.5 ms | **3.02x** |

#### Column Projection Impact

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
| VALUES clause | **significant** | Multi-value equality filters |
| Combined | **3-5x** | Queries using all optimizations |

### Performance Tips

1. **Use VALUES for large multi-value filters**: VALUES clauses are reliably converted to `IN` pushdown. Simple `FILTER (in ?x [...])` can also push down when it matches the supported single-variable comparison form, but VALUES is typically clearer and avoids edge cases.

2. **Filter on partition columns**: If your Iceberg table is partitioned, filtering on partition columns enables partition pruning.

3. **Project only needed columns**: Only columns referenced in the query are read from Iceberg.

4. **Use LIMIT**: LIMIT is applied by the SPARQL engine. Iceberg scan functions support an optional per-scan `:limit`, but the Iceberg VG does not currently push SPARQL LIMIT down to scans (and for joins, per-scan limits can be incorrect).

5. **Prefer equality filters**: Equality predicates enable more aggressive row group pruning.

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
{:columns [\"col1\" \"col2\"]      ; Column projection (nil = all)
 :predicates [{:column \"x\"       ; Predicate pushdown
               :op :eq
               :value 42}]
 :snapshot-id 12345678            ; Time travel by snapshot
 :as-of-time #inst \"2024-01-01\"  ; Time travel by timestamp
 :batch-size 4096                 ; Rows per batch
 :limit 1000                      ; Max rows to return
 :copy-batches true}              ; Copy Arrow batches for safe holding
```

### ITabularPlan Protocol (Plan Execution)

```clojure
(defprotocol ITabularPlan
  (open! [this] "Initialize the plan operator.")
  (next-batch! [this] "Produce the next batch of results.")
  (close! [this] "Release all resources.")
  (estimated-rows [this] "Return estimated output row count."))
```

### Plan Operators

| Operator | Purpose |
|----------|---------|
| `ScanOp` | Reads from ITabularSource with pushdown |
| `HashJoinOp` | Columnar hash join (inner or left outer) |
| `FilterOp` | Applies residual predicates |
| `ProjectOp` | Column selection/renaming |

## Troubleshooting

### Verbose Hadoop Logging

Suppress Hadoop/Parquet debug logs:

```bash
FLUREE_LOG_LEVEL=error clojure -M:dev:iceberg ...
```

### Resource Leaks

Always fully consume lazy sequences from `scan-batches` and `scan-arrow-batches`, or resources may leak. The sequences auto-close when exhausted.

### GraalVM Native Image

For native image builds, ensure Iceberg and Arrow classes are included in reflection config. See `resources/META-INF/native-image/com.fluree/db/reflect-config.json`.

### Common Issues

| Issue | Solution |
|-------|----------|
| "Cannot resolve metadata for table" | Check warehouse-path and table name format |
| Slow queries without pushdown | Verify predicates are using supported patterns |
| Memory issues with large joins | Reduce batch-size, enable columnar execution |
| Missing results with OPTIONAL | Check join orientation (probe=required side) |
| "Unsupported transitive path" error | Reachability check (both S and O bound) not supported; use forward/backward traversal |
| Transitive query returns empty | Ensure predicate IRI matches R2RML mapping; check FK column exists |
| Transitive depth limit warning | Deep hierarchy hit default 100-level limit; results truncated |

## Limitations and Future Work

### Current Limitations

1. **FILTER IN Pushdown**: Use VALUES clauses instead for better pushdown.

2. **Multi-Variable VALUES**: VALUES clauses with multiple variables are not pushed down:
   ```sparql
   # Not pushed down:
   VALUES (?country ?status) { ("US" "active") ("CA" "active") }
   ```

3. **Complex OPTIONAL Blocks**: Multi-table OPTIONAL blocks require careful handling:
   ```sparql
   # Simple case (works):
   ?airline ex:name ?name .
   OPTIONAL { ?airline ex:country ?country }

   # Complex case (may need attention):
   ?route ex:source ?src .
   OPTIONAL {
     ?route ex:airline ?airline .
     ?airline ex:name ?airlineName .
   }
   ```

4. **Aggregation Pushdown**: GROUP BY aggregations are computed in the Iceberg VG (not pushed down to Iceberg itself).
   - Note: This still requires materializing grouped rows in memory. Use selective predicates (pushdown) and LIMIT where possible.

5. **Transitive Property Path Limitations**:
   - **Reachability check not supported**: Both subject and object bound (e.g., `ex:a <ex:knows+> ex:z`) throws an error. Use forward/backward traversal with filtering instead.
   - **Single-table only**: Transitive paths work within a single table's self-referential FK. Cross-table transitive paths are not yet supported.
   - **Simple predicates only**: No support for inverse paths (`^ex:pred`), sequence paths (`ex:a/ex:b`), alternative paths (`ex:a|ex:b`), or depth modifiers (`ex:pred+3`).

### Future Work

- [x] GROUP BY aggregations (COUNT, SUM, AVG, MIN, MAX, COUNT DISTINCT)
- [ ] GROUP BY aggregation pushdown to Iceberg
- [x] Transitive property paths (`pred+`, `pred*`)
- [ ] Transitive reachability check (`[:v :v :v]` pattern)
- [ ] Cross-table transitive paths
- [x] UNION pattern support (basic support complete)
- [ ] UNION schema alignment (consistent output columns across branches)
- [ ] Statistics-based query planning improvements
- [ ] Parallel execution for multi-table queries
- [ ] Spill-to-disk for large joins

## Running Benchmarks

```bash
# Build OpenFlights test data
make iceberg-openflights

# Run benchmarks
clojure -M:dev:iceberg -e \
  "(require 'fluree.db.iceberg-columnar-benchmark) \
   (fluree.db.iceberg-columnar-benchmark/run-benchmark)"
```
