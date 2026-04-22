# Geospatial Data

Fluree provides native support for geographic point data using the OGC GeoSPARQL standard. POINT geometries from `geo:wktLiteral` values are stored in an optimized binary format enabling efficient storage and index-accelerated proximity queries.

## Status

Geospatial support is implemented with:

- **Inline GeoPoint encoding**: POINT geometries stored as packed 60-bit lat/lng values
- **Automatic detection**: `geo:wktLiteral` POINT values automatically converted to native format
- **Full round-trip**: GeoPoints preserved through commit, index, and query paths
- **~0.3mm precision**: 30-bit encoding per coordinate provides sub-millimeter accuracy
- **Index-accelerated proximity queries**: POST latitude-band scans with haversine post-filtering
- **Time travel support**: Point-in-time geo queries via `from: "<ledger>@t:<t>"` (see examples below)

Non-POINT geometries (polygons, linestrings, multipolygons, etc.) are indexed using a separate S2 cell-based spatial index that enables efficient containment and intersection queries.

## Storing Geographic Data

### WKT Literal Format

Geographic data uses the Well-Known Text (WKT) format with the `geo:wktLiteral` datatype:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "@graph": [
    {
      "@id": "ex:eiffel-tower",
      "@type": "ex:Landmark",
      "ex:name": "Eiffel Tower",
      "ex:location": {
        "@value": "POINT(2.2945 48.8584)",
        "@type": "geo:wktLiteral"
      }
    }
  ]
}
```

**Important**: WKT uses `POINT(longitude latitude)` order (X, Y), which is the opposite of common lat/lng conventions.

### Coordinate Order

| Format | Order | Example |
|--------|-------|---------|
| WKT | longitude, latitude | `POINT(2.2945 48.8584)` |
| Common conventions | latitude, longitude | `48.8584, 2.2945` |

Fluree handles the conversion internally, storing coordinates in latitude-primary order for efficient latitude-band index scans.

### Valid POINT Syntax

Fluree recognizes these POINT formats:

```
POINT(2.2945 48.8584)           # Standard 2D point
POINT( 2.2945  48.8584 )        # Whitespace is flexible
POINT(-122.4194 37.7749)        # Negative coordinates (San Francisco)
```

The following are **not** supported for native GeoPoint storage (stored as strings instead):

```
POINT EMPTY                      # Empty point
POINT Z(2.2945 48.8584 100)     # 3D point with altitude
POINT M(2.2945 48.8584 1.0)     # Point with measure
POINT ZM(2.2945 48.8584 100 1)  # 3D point with measure
<http://...>POINT(...)          # SRID prefix
point(2.2945 48.8584)           # Lowercase (case-sensitive)
```

### Coordinate Validation

Coordinates must be within valid ranges:

- **Latitude**: -90.0 to 90.0 (degrees)
- **Longitude**: -180.0 to 180.0 (degrees)
- **Finite values only**: NaN and infinity are rejected

Invalid coordinates cause the value to be stored as a plain string rather than a native GeoPoint.

## Querying Geographic Data

### Basic Retrieval

GeoPoints are returned in WKT format in query results:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "from": "places:main",
  "where": [
    { "@id": "?place", "@type": "ex:Landmark" },
    { "@id": "?place", "ex:location": "?loc" }
  ],
  "select": ["?place", "?loc"]
}
```

Result:

```json
[
  ["ex:eiffel-tower", "POINT(2.2945 48.8584)"]
]
```

### SPARQL Queries

```sparql
PREFIX ex: <http://example.org/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>

SELECT ?place ?location
WHERE {
  ?place a ex:Landmark ;
         ex:location ?location .
}
```

### Output Formats

GeoPoints appear differently based on output format:

**JSON-LD (default):**
```json
{
  "@id": "ex:eiffel-tower",
  "ex:location": {
    "@value": "POINT(2.2945 48.8584)",
    "@type": "geo:wktLiteral"
  }
}
```

**SPARQL JSON:**
```json
{
  "type": "literal",
  "value": "POINT(2.2945 48.8584)",
  "datatype": "http://www.opengis.net/ont/geosparql#wktLiteral"
}
```

**Typed JSON:**
```json
{
  "@value": "POINT(2.2945 48.8584)",
  "@type": "geo:wktLiteral"
}
```

## Storage Encoding

### Binary Format

GeoPoints are stored using a compact 60-bit encoding:

- **Upper 30 bits**: Latitude scaled from [-90, 90] to [0, 2^30-1]
- **Lower 30 bits**: Longitude scaled from [-180, 180] to [0, 2^30-1]

This provides:

- **8 bytes total storage** per point (vs ~25+ bytes for WKT string)
- **~0.3mm precision** at the equator
- **Ordered encoding** enabling efficient range scans by latitude band

### Index Structure

GeoPoints use `ObjKind::GEO_POINT` (0x14) in the binary index:

| Component | Encoding |
|-----------|----------|
| Object kind | 1 byte (0x14) |
| Object key | 8 bytes (packed lat/lng) |

The latitude-primary encoding enables POST index scans that efficiently retrieve all points within a latitude band.

## Distance Queries

Fluree supports the `geof:distance` function (OGC GeoSPARQL) for calculating haversine distances between geographic points.

### geof:distance Function

Calculate the distance between two points in meters:

**JSON-LD Query (bind + filter):**
```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "from": "places:main",
  "where": [
    { "@id": "?place", "ex:location": "?loc" },
    { "@id": "ex:paris", "ex:location": "?parisLoc" },
    ["bind", "?distance", "(geof:distance ?loc ?parisLoc)"],
    ["filter", "(< ?distance 500000)"]
  ],
  "select": ["?place", "?distance"]
}
```

**SPARQL:**
```sparql
PREFIX ex: <http://example.org/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>

SELECT ?place ?distance
WHERE {
  ?place ex:location ?loc .
  ex:paris ex:location ?parisLoc .
  BIND(geof:distance(?loc, ?parisLoc) AS ?distance)
  FILTER(?distance < 500000)
}
ORDER BY ?distance
```

**Function aliases:** `geof:distance`, `geo_distance`, `geodistance`

**Arguments:**
- Two GeoPoint values (stored as `geo:wktLiteral` POINT)
- Or two WKT POINT strings

**Returns:** Distance in meters (Double)

**Calculation:** Uses the haversine formula with Earth's mean radius (6,371 km), accurate to within 0.3% for typical distances.

## Proximity Search

Fluree supports index-accelerated proximity queries that find points within a given distance of a center point.

### Index-Accelerated Point Proximity

Use a `geof:distance` bind + filter pattern to run an accelerated proximity search over inline GeoPoints. This pattern works identically in both JSON-LD and SPARQL queries — the query optimizer detects the Triple + Bind(geof:distance) + Filter combination and rewrites it into an index-accelerated scan.

**JSON-LD Query (find restaurants within 5km, include distance, limit to 10):**
```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "from": "places:main",
  "where": [
    { "@id": "?place", "@type": "ex:Restaurant" },
    { "@id": "?place", "ex:location": "?loc" },
    ["bind", "?distance", "(geof:distance ?loc \"POINT(2.35 48.85)\")"],
    ["filter", "(<= ?distance 5000)"]
  ],
  "select": ["?place", "?distance"],
  "orderBy": ["?distance"],
  "limit": 10
}
```

**SPARQL (same pattern, same acceleration):**
```sparql
PREFIX ex: <http://example.org/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>

SELECT ?station ?distance
WHERE {
  ?station a ex:GasStation ;
           ex:location ?loc .
  BIND(geof:distance(?loc, "POINT(2.35 48.85)"^^geo:wktLiteral) AS ?distance)
  FILTER(?distance < 10000)
}
ORDER BY ?distance
LIMIT 10
```

### How Index Acceleration Works

1. **Latitude-band scan**: The query planner converts the radius to latitude bounds and scans only points in `[lat - δ, lat + δ]`
2. **Haversine post-filter**: Results are filtered by exact haversine distance to eliminate false positives
3. **Distance sorting**: Results can be sorted by distance for k-nearest-neighbor queries

**Performance characteristics:**
- Uses POST index with latitude-primary encoding
- Scans only relevant latitude band (not full table scan)
- False positive rate: 22-70% depending on latitude and radius (eliminated by post-filter)
- Handles antimeridian crossing with multiple range scans

### Time Travel Support

Point proximity queries support time travel via the `from` ledger selector.

**JSON-LD with time travel:**
```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "from": "places:main@t:100",
  "where": [
    { "@id": "?place", "ex:location": "?loc" },
    ["bind", "?dist", "(geof:distance ?loc \"POINT(2.35 48.85)\")"],
    ["filter", "(<= ?dist 5000)"]
  ],
  "select": ["?place"]
}
```

**SPARQL with time travel:**
```sparql
PREFIX ex: <http://example.org/>
PREFIX fluree: <https://ns.flur.ee/ledger#>

SELECT ?place ?loc
FROM <ledger:places:main?t=100>
WHERE {
  ?place ex:location ?loc .
}
```

Time travel correctly handles:
- Points that existed at time `t` but were later retracted
- Points added after time `t` (excluded from results)
- Overlay novelty merging for recent uncommitted data

### Graph Scoping

Point proximity queries respect graph context. When used inside a GRAPH pattern, the query scans only the specified named graph:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "from": "world:main",
  "where": [
    ["graph", "http://example.org/france", [
      { "@id": "?city", "ex:location": "?loc" },
      ["bind", "?dist", "(geof:distance ?loc \"POINT(2.35 48.85)\")"],
      ["filter", "(<= ?dist 50000)"]
    ]]
  ],
  "select": ["?city"]
}
```

This returns only cities from the France graph within 50km of Paris, not cities from other named graphs.

## S2 Spatial Index (Complex Geometries)

Fluree provides an S2 cell-based spatial index for complex geometries (polygons, linestrings, multipolygons). This index enables efficient spatial predicate queries like "find all places within this region" or "find all regions that contain this point."

### Supported Operations

| Operation | Description | Use Case |
|-----------|-------------|----------|
| `within` | Find geometries that are completely inside a query geometry | "Find all buildings within this city boundary" |
| `contains` | Find geometries that completely contain a query geometry | "Find the district that contains this point" |
| `intersects` | Find geometries that overlap with a query geometry | "Find all parcels that touch this proposed road" |
| `nearby` | Find geometries within a radius (with distances) | "Find polygons within 10km of this point" |

### Query Syntax

**JSON-LD Query (find places within a polygon):**
```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#",
    "idx": "https://ns.flur.ee/index#"
  },
  "from": "places:main",
  "where": [
    {
      "idx:spatial": "within",
      "idx:property": "ex:boundary",
      "idx:geometry": "POLYGON((2.0 48.0, 3.0 48.0, 3.0 49.0, 2.0 49.0, 2.0 48.0))",
      "idx:result": "?place"
    }
  ],
  "select": ["?place"]
}
```

**Find regions containing a point:**
```json
{
  "where": [
    {
      "idx:spatial": "contains",
      "idx:property": "ex:boundary",
      "idx:geometry": "POINT(2.35 48.85)",
      "idx:result": "?district"
    }
  ],
  "select": ["?district"]
}
```

**Find intersecting parcels:**
```json
{
  "where": [
    {
      "idx:spatial": "intersects",
      "idx:property": "ex:parcel",
      "idx:geometry": "LINESTRING(2.0 48.0, 3.0 49.0)",
      "idx:result": "?parcel"
    }
  ],
  "select": ["?parcel"]
}
```

**Find polygons near a point (with distances):**
```json
{
  "@context": {
    "ex": "http://example.org/",
    "idx": "https://ns.flur.ee/index#"
  },
  "from": "places:main",
  "where": [
    {
      "idx:spatial": "nearby",
      "idx:property": "ex:boundary",
      "idx:geometry": "POINT(2.35 48.85)",
      "idx:radius": 10000,
      "idx:result": {
        "idx:id": "?region",
        "idx:distance": "?dist"
      }
    }
  ],
  "select": ["?region", "?dist"],
  "orderBy": ["?dist"]
}
```

### How It Works

The S2 spatial index uses Google's S2 geometry library to map geometries to hierarchical cells on a sphere:

1. **Ingestion**: When a `geo:wktLiteral` polygon/linestring is committed, the indexer generates an S2 cell covering and stores cell entries in the spatial index.

2. **Query**: When you query with a spatial predicate, the system:
   - Generates an S2 covering for your query geometry
   - Scans the index for matching cell ranges
   - Applies bounding-box prefiltering
   - Performs exact geometry tests on candidates

3. **Time-Travel**: The index supports full time-travel semantics, so you can query spatial data at any historical point in time.

### Index Configuration

The S2 index is automatically created for predicates with `geo:wktLiteral` values. Configuration options:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `min_level` | 4 | Minimum S2 cell level (coarser = faster build) |
| `max_level` | 16 | Maximum S2 cell level (finer = tighter coverage) |
| `max_cells` | 8 | Maximum cells per geometry covering |

Higher `max_cells` values produce tighter coverings (fewer false positives) but increase index size and build time.

### Performance Characteristics
Performance depends on data distribution, covering configuration, and result selectivity. See [Spatial Index Design](../design/spatial-index.md) for design rationale; a benchmark suite is recommended for deployment-specific measurements.

### Supported Geometry Types

| Geometry Type | S2 Index | Notes |
|---------------|----------|-------|
| POLYGON | ✅ Yes | Most common for region queries |
| MULTIPOLYGON | ✅ Yes | Multiple disjoint regions |
| LINESTRING | ✅ Yes | Routes, boundaries |
| MULTILINESTRING | ✅ Yes | Multiple line segments |
| POINT | ⚠️ Optional | Use inline GeoPoint for proximity; S2 available with `index_points=true` |
| GEOMETRYCOLLECTION | ✅ Yes | Mixed geometry types |

### Graph Scoping

Spatial indexes are scoped by named graph. Each graph has its own spatial index, and queries automatically use the correct index based on the graph context.

**Default graph query:**
```json
{
  "from": "mydb:main",
  "where": [
    {
      "idx:spatial": "within",
      "idx:property": "ex:boundary",
      "idx:geometry": "POLYGON(...)",
      "idx:result": "?region"
    }
  ]
}
```

**Named graph query (using GRAPH pattern):**
```json
{
  "from": "mydb:main",
  "where": [
    ["graph", "http://example.org/regions",
     {
       "idx:spatial": "within",
       "idx:property": "ex:boundary",
       "idx:geometry": "POLYGON(...)",
       "idx:result": "?region"
     }
    ]
  ]
}
```

When you enter a GRAPH pattern, the spatial query automatically switches to that graph's index. This ensures results are correctly scoped—a spatial query inside `GRAPH <http://example.org/france>` only searches geometries in the France graph, not geometries from other named graphs.

**Multiple named graphs:**

If you have data across multiple named graphs (e.g., countries), you can query each independently:

```json
{
  "from": "world:main",
  "where": [
    ["graph", "http://example.org/germany",
     {
       "idx:spatial": "within",
       "idx:property": "ex:boundary",
       "idx:geometry": "POLYGON(...)",
       "idx:result": "?germanCity"
     }
    ]
  ]
}
```

The same `idx:property` (e.g., `ex:boundary`) in different named graphs will query separate spatial indexes.

### Time-Travel Support

Spatial queries support time travel via the `from` ledger selector:

```json
{
  "from": "places:main@t:100",
  "where": [
    {
      "idx:spatial": "within",
      "idx:property": "ex:boundary",
      "idx:geometry": "POLYGON(...)",
      "idx:result": "?place"
    }
  ],
  "select": ["?place"]
}
```

This returns places as they existed at transaction time 100, correctly handling:
- Geometries added after t=100 (excluded)
- Geometries retracted before t=100 (excluded)
- Geometries modified between t=100 and now

**Note**: Time travel requires `t >= index.base_t`. Queries for times before the index was built will return an error.

**Note (v1)**: The historical-view API (`query_historical`) does not execute spatial index patterns. Use a time-pinned `from` selector (as above) against the current ledger state for spatial time travel.

## Choosing Between Point Proximity and S2 Spatial Queries

Fluree provides two spatial query paths. Use this guide to pick the right one:

| Use Case | Approach | Reason |
|----------|----------|--------|
| "Find restaurants near me" | `geof:distance` bind+filter | POINT proximity with distance ranking |
| "Find cities within 100km" | `geof:distance` bind+filter | POINT data with radius filter |
| "Find buildings in this district" | `idx:spatial` (within) | POLYGONs inside a boundary |
| "Which zone contains this address?" | `idx:spatial` (contains) | POLYGON containment test |
| "Find parcels crossing this road" | `idx:spatial` (intersects) | LINESTRING intersection |
| "Find regions near this location" | `idx:spatial` (nearby) | POLYGONs with distance from point |

**Quick rule**: Use `geof:distance` bind+filter for POINT locations with radius queries. Use `idx:spatial` for polygon/linestring containment, intersection, or region-based queries.

### End-to-End Example: Points and Polygons

This example shows storing both POINT locations and POLYGON boundaries, then querying each appropriately.

**1. Insert data with both geometry types:**
```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "@graph": [
    {
      "@id": "ex:central-paris",
      "@type": "ex:District",
      "ex:name": "Central Paris",
      "ex:boundary": {
        "@value": "POLYGON((2.3 48.8, 2.4 48.8, 2.4 48.9, 2.3 48.9, 2.3 48.8))",
        "@type": "geo:wktLiteral"
      }
    },
    {
      "@id": "ex:eiffel-tower",
      "@type": "ex:Landmark",
      "ex:name": "Eiffel Tower",
      "ex:location": {
        "@value": "POINT(2.2945 48.8584)",
        "@type": "geo:wktLiteral"
      }
    },
    {
      "@id": "ex:louvre",
      "@type": "ex:Landmark",
      "ex:name": "Louvre Museum",
      "ex:location": {
        "@value": "POINT(2.3376 48.8606)",
        "@type": "geo:wktLiteral"
      }
    }
  ]
}
```

**2. Find landmarks near Eiffel Tower (POINT proximity):**
```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "from": "places:main",
  "where": [
    { "@id": "?place", "ex:location": "?loc" },
    ["bind", "?dist", "(geof:distance ?loc \"POINT(2.2945 48.8584)\")"],
    ["filter", "(<= ?dist 5000)"],
    { "@id": "?place", "ex:name": "?name" }
  ],
  "select": ["?name", "?dist"],
  "orderBy": ["?dist"]
}
```

**3. Find which district contains the Louvre (POLYGON containment):**
```json
{
  "@context": {
    "ex": "http://example.org/",
    "idx": "https://ns.flur.ee/index#"
  },
  "from": "places:main",
  "where": [
    {
      "idx:spatial": "contains",
      "idx:property": "ex:boundary",
      "idx:geometry": "POINT(2.3376 48.8606)",
      "idx:result": "?district"
    },
    { "@id": "?district", "ex:name": "?name" }
  ],
  "select": ["?name"]
}
```

### MULTIPOLYGON Example

Store regions with multiple disjoint areas (e.g., archipelagos, non-contiguous territories):

```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "@id": "ex:hawaii",
  "@type": "ex:State",
  "ex:name": "Hawaii",
  "ex:territory": {
    "@value": "MULTIPOLYGON(((-160 22, -159 22, -159 21, -160 21, -160 22)), ((-156 20, -155 20, -155 19, -156 19, -156 20)))",
    "@type": "geo:wktLiteral"
  }
}
```

Query: "Find states that contain this coordinate"
```json
{
  "where": [
    {
      "idx:spatial": "contains",
      "idx:property": "ex:territory",
      "idx:geometry": "POINT(-155.5 19.5)",
      "idx:result": "?state"
    }
  ],
  "select": ["?state"]
}
```

### LINESTRING Example

Store routes, roads, or boundaries:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "@id": "ex:route-66",
  "@type": "ex:Highway",
  "ex:name": "Route 66",
  "ex:path": {
    "@value": "LINESTRING(-118.2 34.1, -112.0 35.2, -106.6 35.1, -97.5 35.5, -90.2 38.6, -87.6 41.9)",
    "@type": "geo:wktLiteral"
  }
}
```

Query: "Find highways that cross this region"
```json
{
  "where": [
    {
      "idx:spatial": "intersects",
      "idx:property": "ex:path",
      "idx:geometry": "POLYGON((-100 34, -95 34, -95 37, -100 37, -100 34))",
      "idx:result": "?highway"
    }
  ],
  "select": ["?highway"]
}
```

## Planned Capabilities

### R-tree Index

An ephemeral R-tree is planned for:
- Spatial joins between datasets
- Range queries across multiple properties

## Examples

### Storing Multiple Locations

```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "@graph": [
    {
      "@id": "ex:paris",
      "@type": "ex:City",
      "ex:name": "Paris",
      "ex:center": { "@value": "POINT(2.3522 48.8566)", "@type": "geo:wktLiteral" }
    },
    {
      "@id": "ex:london",
      "@type": "ex:City",
      "ex:name": "London",
      "ex:center": { "@value": "POINT(-0.1278 51.5074)", "@type": "geo:wktLiteral" }
    },
    {
      "@id": "ex:tokyo",
      "@type": "ex:City",
      "ex:name": "Tokyo",
      "ex:center": { "@value": "POINT(139.6917 35.6895)", "@type": "geo:wktLiteral" }
    }
  ]
}
```

### Turtle Format

```turtle
@prefix ex: <http://example.org/> .
@prefix geo: <http://www.opengis.net/ont/geosparql#> .

ex:sensor-1 a ex:WeatherStation ;
    ex:name "Central Park Station" ;
    ex:location "POINT(-73.9654 40.7829)"^^geo:wktLiteral .

ex:sensor-2 a ex:WeatherStation ;
    ex:name "Times Square Station" ;
    ex:location "POINT(-73.9855 40.7580)"^^geo:wktLiteral .
```

### Mixed Geometry Types

Non-POINT geometries are stored as strings:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "@graph": [
    {
      "@id": "ex:central-park",
      "@type": "ex:Park",
      "ex:name": "Central Park",
      "ex:entrance": {
        "@value": "POINT(-73.9654 40.7829)",
        "@type": "geo:wktLiteral"
      },
      "ex:boundary": {
        "@value": "POLYGON((-73.9819 40.7681, -73.9580 40.8006, -73.9493 40.7969, -73.9732 40.7644, -73.9819 40.7681))",
        "@type": "geo:wktLiteral"
      }
    }
  ]
}
```

The `ex:entrance` POINT is stored as a native GeoPoint, while the `ex:boundary` POLYGON is stored as a string.

## GeoSPARQL-related support (v1)

Fluree supports the GeoSPARQL `geo:wktLiteral` datatype and `geof:distance` function. Point proximity queries use a unified `geof:distance` bind+filter pattern in both JSON-LD and SPARQL. For complex geometry queries (`within`/`contains`/`intersects`/`nearby`), use the JSON-LD `idx:spatial` pattern described above.

| Feature | Status |
|---------|--------|
| `geo:wktLiteral` datatype | ✅ Supported |
| POINT geometry | ✅ Native encoding (60-bit packed) |
| LINESTRING geometry | ✅ S2 spatial index |
| POLYGON geometry | ✅ S2 spatial index |
| MULTIPOLYGON geometry | ✅ S2 spatial index |
| `geo:asWKT` property | ✅ Use any property with wktLiteral type |
| `geof:distance` function | ✅ Supported (haversine, ~0.3% accuracy) |
| Proximity queries (radius) | ✅ Index-accelerated via geof:distance bind+filter |
| Time travel | ✅ Support via `from: "<ledger>@t:<t>"` |
| k-NN queries (nearest K) | ✅ Via ORDER BY distance + LIMIT |
| `within` spatial predicate | ✅ Via JSON-LD `idx:spatial` |
| `contains` spatial predicate | ✅ Via JSON-LD `idx:spatial` |
| `intersects` spatial predicate | ✅ Via JSON-LD `idx:spatial` |
| Spatial join (two variables) | 🔜 Planned (R-tree) |

## Best Practices

### Use geo:wktLiteral for All Geometry

Always declare the datatype explicitly:

```json
// Correct
{ "@value": "POINT(2.3522 48.8566)", "@type": "geo:wktLiteral" }

// Incorrect - stored as plain string
{ "@value": "POINT(2.3522 48.8566)" }
```

### Coordinate Precision

While Fluree stores ~0.3mm precision, consider your source data accuracy:

```json
// Excessive precision (GPS typically ±3-5m)
"POINT(2.352219834765 48.856614892341)"

// Appropriate precision for most applications
"POINT(2.3522 48.8566)"
```

### Coordinate Validation

Validate coordinates before insertion:

- Latitude: -90 to 90
- Longitude: -180 to 180
- No NaN or infinity values

Invalid coordinates are stored as strings and won't benefit from native GeoPoint indexing.

## Troubleshooting

### Query returns no results

**Check the coordinate order.** WKT uses `POINT(longitude latitude)`, not `POINT(latitude longitude)`:
```json
// Correct: Paris (lng=2.35, lat=48.86)
"POINT(2.35 48.86)"

// Wrong: coordinates swapped
"POINT(48.86 2.35)"
```

**Check the datatype.** Geometry values must use `geo:wktLiteral`:
```json
// Correct
{ "@value": "POINT(2.35 48.86)", "@type": "geo:wktLiteral" }

// Wrong - no datatype, stored as plain string
{ "@value": "POINT(2.35 48.86)" }
```

**Check the predicate.** The property in the triple pattern must match the data exactly:
```json
// If data uses ex:location, the triple must use ex:location
{ "@id": "?place", "ex:location": "?loc" }    // Correct
{ "@id": "?place", "ex:geo": "?loc" }         // Wrong - different predicate
```

For S2 spatial queries, `idx:property` must also match:
```json
"idx:property": "ex:boundary"    // Correct
"idx:property": "ex:geo"         // Wrong - different predicate
```

### "No spatial index available" error

The spatial index is built asynchronously after commits. If querying immediately after insert:
- Wait for background indexing to complete, or
- Use `from: "<ledger>@t:<t>"` to query up to the indexed `t`

### Large polygons cause slow queries

Polygons crossing the antimeridian (±180° longitude) generate many S2 cells. Consider:
- Splitting the polygon at the antimeridian
- Using a simpler bounding region for initial filtering

### SPARQL spatial predicates not accelerated

In v1, SPARQL `geof:*` spatial predicates (like `geof:sfWithin`) evaluate as filters, not index operators. For accelerated spatial queries on complex geometries, use the JSON-LD `idx:spatial` pattern instead. Note: `geof:distance` bind+filter patterns **are** automatically accelerated in both SPARQL and JSON-LD.

## Related Documentation

- [Datatypes](../concepts/datatypes.md) - Type system overview
- [Vector Search](vector-search.md) - Similarity search
- [BM25](bm25.md) - Full-text search
