# R2RML Query Integration

This module provides R2RML (RDB to RDF Mapping Language) support for querying
Iceberg graph sources as RDF.

## Module Structure

- `mod.rs` - Module exports and R2rmlPattern definition
- `operator.rs` - R2rmlScanOperator for query execution
- `provider.rs` - R2rmlProvider trait for mapping and table access
- `rewrite.rs` - Query rewriting for R2RML graph sources

## RefObjectMap Join Implementation

RefObjectMap enables joins across TriplesMap definitions. The current
implementation:

1. Pre-scans parent tables and builds lookup maps (keyed by join columns)
2. For each child row, looks up the parent subject using the child's join
   column values
3. Caches lookups per `(parent_tm_iri, sorted_parent_join_cols)` to avoid
   redundant scans

### Lookup Cache Key Design

The lookup cache is keyed by a composite key:
```rust
type LookupCacheKey = (String, Vec<String>);  // (parent_tm_iri, sorted_parent_join_cols)
```

This ensures that different RefObjectMaps referencing the same parent TriplesMap
but with different join columns maintain separate lookups.

## Future Optimization Notes

### Per-POM Child Column Precomputation

**Current state:** In the row iteration loop, child column names are extracted
from the RefObjectMap for each row:

```rust
let child_cols: Vec<&str> = rom.child_columns();
```

**Optimization:** Precompute `child_columns` and `lookup_key` for each
RefObjectMap POM outside the row loop, storing them alongside the filtered POMs.
This would eliminate repeated Vec allocations and sorting per row.

**Impact:** Micro-optimization. Only meaningful for tables with millions of rows
and many RefObjectMap predicates. The current implementation is correct and
performs well for typical workloads.

### Parallel Parent Table Scanning

**Current state:** Parent tables are scanned sequentially when building lookups.

**Optimization:** For mappings with multiple RefObjectMaps referencing different
parent tables, scan them in parallel using `tokio::join!` or similar.

**Impact:** Would reduce latency for complex multi-table joins, especially over
high-latency storage (S3).

### Streaming Parent Lookups

**Current state:** All parent table batches are collected into memory before
building the lookup HashMap.

**Optimization:** For very large parent tables, consider a streaming approach
that builds the lookup incrementally, or use external sorting + merge join
semantics.

**Impact:** Would reduce memory footprint for parent tables with millions of
rows. Current approach is fine for typical dimension tables (thousands to
hundreds of thousands of rows).

### Predicate Pushdown to Iceberg

**Current state:** Predicate filters are evaluated after materializing RDF terms.

**Optimization:** Push constant value filters down to the Iceberg scan layer
where possible (e.g., `?airline ex:country "United States"` could push
`country = "United States"` to Parquet predicate pushdown).

**Impact:** Significant for selective filters on large tables. Requires
analyzing object map types to determine which predicates can be pushed.

## Testing

See `fluree-db-api/tests/it_graph_source_r2rml.rs` for integration tests:

- `engine_e2e_ref_object_map_join_execution` - E2E join with orphan FK handling
- `test_ref_object_map_composite_key_parsing` - Composite join key parsing
- Various mapping and term materialization tests
