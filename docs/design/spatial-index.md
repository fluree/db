# Spatial Index Design

This document describes the design of Fluree's geospatial indexing system, covering both the inline GeoPoint encoding for POINT geometries and the S2 cell-based spatial index for complex geometries.

## Overview

Fluree provides two complementary approaches to geospatial indexing:

1. **Inline GeoPoint**: POINT geometries are encoded directly in the binary index as packed 60-bit lat/lng values, enabling efficient latitude-band scans for proximity queries.

2. **S2 Spatial Index**: Complex geometries (polygons, linestrings, etc.) are indexed using Google's S2 cell system, which maps spherical geometries to hierarchical cell IDs.

This dual approach optimizes for the most common use cases while supporting the full range of OGC GeoSPARQL operations.

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │             Query Engine                │
                        └─────────────────────────────────────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
            ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
            │ GeoSearchOp   │   │ S2SearchOp    │   │ RangeProvider │
            │ (POINT nearby)│   │ (within/etc)  │   │ (standard)    │
            └───────────────┘   └───────────────┘   └───────────────┘
                    │                   │                   │
                    ▼                   ▼                   ▼
            ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
            │ POST Index    │   │ S2 Sidecar    │   │ Binary Index  │
            │ (lat-band)    │   │ (cell index)  │   │ (SPOT/PSOT)   │
            └───────────────┘   └───────────────┘   └───────────────┘
```

## Part 1: Inline GeoPoint

### Encoding

POINT geometries are stored using a 60-bit packed encoding:

```
┌──────────────────────────────────────────────────────────────┐
│  Upper 30 bits: Latitude    │  Lower 30 bits: Longitude     │
│  [-90, 90] → [0, 2³⁰-1]     │  [-180, 180] → [0, 2³⁰-1]     │
└──────────────────────────────────────────────────────────────┘
```

This provides:
- **~0.3mm precision** at the equator (30 bits = 1 billion divisions per range)
- **8 bytes total** vs ~25+ bytes for WKT string storage
- **Ordered by latitude** for efficient latitude-band range scans

### Latitude-Band Queries

For proximity queries, the GeoSearchOperator:

1. Converts the query radius to a latitude delta: `δ_lat = radius / EARTH_RADIUS`
2. Computes latitude bounds: `[center_lat - δ, center_lat + δ]`
3. Scans the POST index for the latitude band
4. Post-filters with exact haversine distance

**False Positive Rate**: 22-70% depending on latitude and query radius (eliminated by post-filter).

### Antimeridian Handling

Queries crossing the antimeridian (±180°) are split into two scans:
- `[min_lng, 180]` in the eastern hemisphere
- `[-180, max_lng]` in the western hemisphere

Results are merged and deduplicated.

## Part 2: S2 Spatial Index

### Why S2?

The S2 cell system provides several properties ideal for spatial indexing:

1. **Hierarchical**: Cells at level N contain 4 children at level N+1
2. **Hilbert-curve ordering**: Nearby cells have nearby IDs (good for range scans)
3. **Equal-area**: Cells at the same level have roughly equal area
4. **No gaps/overlaps**: Cells tile the sphere exactly

### Cell Levels

| Level | Approx. Area | Use Case |
|-------|--------------|----------|
| 0 | 85M km² | Hemisphere |
| 8 | 300k km² | Countries |
| 12 | 20k km² | Cities |
| 16 | 1.3 km² | Neighborhoods |
| 20 | 80 m² | Buildings |
| 30 | 0.7 cm² | Sub-centimeter |

Default configuration: `min_level=4`, `max_level=16`, `max_cells=8`

### Index Structure

The spatial index stores entries sorted by `(cell_id, subject_id, t DESC, op ASC)`:

```rust
struct CellEntry {
    cell_id: u64,       // S2 cell ID (Hilbert order)
    subject_id: u64,    // Subject that owns this geometry
    geo_handle: u32,    // Handle into geometry arena
    t: i64,             // Transaction time
    op: u8,             // 1 = assert, 0 = retract
}
```

This sort order enables:
- Efficient range scans for S2 coverings
- Time-travel via replay (max-t wins at each `(cell_id, subject_id)`)
- Global deduplication across cells (same subject may appear in multiple cells)

### Geometry Arena

WKT strings are stored in a deduplicated geometry arena:

```rust
struct ArenaEntry {
    wkt: Vec<u8>,           // Original WKT bytes
    metadata: GeometryMetadata,
}

struct GeometryMetadata {
    geom_type: GeometryType,
    bbox: Option<BBox>,     // For bbox prefiltering
    centroid: Option<(f64, f64)>,
}
```

The arena:
- Deduplicates identical WKT strings
- Stores precomputed bounding boxes for fast rejection
- Uses WKT as source of truth (no normalization)

### Query Pipeline

```
Query Geometry
     │
     ▼
┌─────────────────┐
│ Generate S2     │  ~10 µs per geometry
│ Covering        │
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Merge Cell      │  Combine adjacent ranges
│ Ranges          │
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Scan Index      │  Binary search + sequential scan
│ + Novelty       │  Merge with uncommitted changes
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Replay at t     │  Time-travel filtering
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Global Dedup    │  Same subject from multiple cells
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ BBox Prefilter  │  Reject by bounding box
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Exact Predicate │  within/contains/intersects
└─────────────────┘
     │
     ▼
Results
```

### Time-Travel Semantics

The index supports Fluree's time-travel model:

1. **Replay**: For each `(cell_id, subject_id)` group, take the entry with maximum `t ≤ to_t`. If that entry is a retraction (`op=0`), the geometry is not visible at `to_t`.

2. **Same-t disambiguation**: When multiple operations share the same `(cell_id, subject_id, t)`, `op` is used as a deterministic tie-break in the sort key (`op ASC`).

3. **Novelty overlay**: Uncommitted changes are merged with the snapshot index using a merge-sorted iterator, with novelty entries winning on exact ties by merge contract.

## Benchmarks

This document focuses on the design. Benchmark results depend on dataset shape, S2 covering configuration, hardware, and query selectivity.

For v1, it is recommended to maintain a benchmark suite that measures:
- Index build throughput and artifact sizes (leaflets, arena, manifest)
- Query latency for `within` / `contains` / `intersects` / `nearby` across a range of query sizes
- Overlay-on vs overlay-off query performance (epoch caching effectiveness)

The benchmark suite lives in `fluree-db-spatial/benches/spatial_bench.rs` and is runnable with:

```bash
cargo bench -p fluree-db-spatial
```

## Design Decisions

### 1. WKT as Source of Truth

**Decision**: Store WKT bytes directly without normalization.

**Rationale**:
- Simple deduplication via string hashing
- No risk of altering geometry semantics
- Normalization (winding order, dateline wrapping) can be added later if needed

### 2. Separate Index for Complex Geometries

**Decision**: Use a sidecar S2 index rather than extending the main binary index.

**Rationale**:
- Complex geometries generate multiple cell entries (8+ per geometry)
- Spatial queries have different access patterns than standard SPOT/POST
- Isolation allows independent optimization and evolution

### 3. Global Dedup Across Cells

**Decision**: Deduplicate after scanning all covering ranges, not per-cell.

**Rationale**:
- Polygon coverings often return the same subject from multiple cells
- Per-cell dedup would miss cross-cell duplicates
- HashMap-based dedup is fast enough (O(n) amortized)

### 4. BBox Prefiltering

**Decision**: Store precomputed bounding boxes and filter before exact tests.

**Rationale**:
- BBox intersection is O(1) vs O(n) for polygon intersection
- Eliminates 40-60% of candidates in typical queries
- Minimal storage overhead (4 floats per geometry)

### 5. Time-Travel via Replay

**Decision**: Store all versions and replay at query time.

**Rationale**:
- Consistent with Fluree's immutable ledger model
- No special handling for historical queries
- Novelty overlay uses same replay logic

### 6. Graph-Scoped Indexes

**Decision**: Spatial indexes are keyed by `(graph_id, predicate_iri)`.

**Rationale**:
- Named graphs may contain different geometry schemas
- Allows graph-specific index configuration
- Fallback to default graph (g_id=0) for convenience

**Implementation**: Each named graph has a numeric `g_id` (0 for default, 1+ for named graphs). The `GraphRef` struct carries this `g_id`, and `GraphOperator` updates `ctx.binary_g_id` when entering a GRAPH pattern. The S2 search operator uses `ctx.binary_g_id` together with the predicate IRI to construct the provider map key (format: `"g{g_id}:{predicate_iri}"`).

**Cross-Ledger Datasets**: The `g_id` is only meaningful relative to the active `binary_store`. Each ledger has its own binary index with its own g_id namespace. In cross-ledger datasets, switching to a different ledger's graph also requires switching the `binary_store` context.

## Limitations

### Known Limitations

1. **Time-travel floor**: Queries for `t < index.base_t` are rejected. The index must be rebuilt to cover earlier times.

2. **Antimeridian polygons**: Polygons crossing the antimeridian (±180°) produce large coverings due to bbox spanning nearly 360° of longitude. A test case with a small antimeridian-crossing polygon generated 226 cells (vs typical 8). Query performance degrades proportionally.

3. **Polar regions**: Polar regions are actually handled efficiently by S2 (4 cells for 85°N arctic polygon), but the cell geometry differs significantly from mid-latitudes.

### Future Work

1. **R-tree for spatial joins**: Ephemeral R-tree construction for joining two geometry columns.

2. **Streaming query results**: Currently all candidates are collected before filtering; streaming would reduce memory for large result sets.

3. **Remote spatial service**: The `fluree-db-spatial` crate supports a provider trait for future remote deployment.

## Related Documentation

- [Geospatial](../indexing-and-search/geospatial.md) - User-facing geospatial documentation
- [Index Format](index-format.md) - Binary index format details
- [Background Indexing](../indexing-and-search/background-indexing.md) - Index lifecycle
