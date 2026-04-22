# Iceberg / Parquet

Fluree integrates with Apache Iceberg to query data lake tables as graph sources. An [R2RML mapping](r2rml.md) defines how Iceberg table rows are materialized into RDF triples, enabling you to query large-scale analytical data stored in Parquet format using the same SPARQL / JSON-LD query interface as regular ledgers.

**Note:** Requires the `iceberg` feature flag. See [Compatibility and Feature Flags](../reference/compatibility.md#fluree-db-api-features).

## What is Apache Iceberg?

Apache Iceberg is an open table format for huge analytical datasets. It provides:
- ACID transactions on data lakes
- Time travel and versioning
- Schema evolution
- Partition management
- Optimized file organization (Parquet)

## Configuration

### Catalog Modes

Fluree supports two ways to discover Iceberg metadata:

- **REST catalog**: discover table metadata via an Iceberg REST catalog API (e.g., Polaris).
- **Direct S3 (no catalog server)**: bypass REST discovery and read `version-hint.text` from the table’s `metadata/` directory to resolve the current metadata file.

### CLI

The `fluree iceberg map` command creates Iceberg graph sources from the command line. An R2RML mapping is required to define how table rows become RDF triples.

```bash
# REST catalog with R2RML mapping
fluree iceberg map warehouse-orders \
  --catalog-uri https://polaris.example.com/api/catalog \
  --r2rml mappings/orders.ttl \
  --auth-bearer $POLARIS_TOKEN

# Direct S3 (no catalog server) with R2RML mapping
fluree iceberg map execution-log \
  --mode direct \
  --table-location s3://bucket/warehouse/logs/execution_log \
  --r2rml mappings/execution_log.ttl
```

Once mapped, graph sources appear in `fluree list`, can be inspected with `fluree info`, and removed with `fluree drop`. See [CLI iceberg reference](../cli/iceberg.md) for all options.

### Rust API

**REST catalog mode (Polaris-style):**

```rust
use fluree_db_api::R2rmlCreateConfig;

let config = R2rmlCreateConfig::new(
    "warehouse-orders",
    "https://polaris.example.com/api/catalog",
    "sales.orders",
    "fluree:file://mappings/orders.ttl",
)
.with_warehouse("my-warehouse")
.with_auth_bearer("my-token")
.with_vended_credentials(true);

fluree.create_r2rml_graph_source(config).await?;
```

**Direct S3 mode (no REST catalog):**

```rust
use fluree_db_api::R2rmlCreateConfig;

let config = R2rmlCreateConfig::new_direct(
    "execution-log",
    "s3://bucket/warehouse/logs/execution_log",
    "fluree:file://mappings/execution_log.ttl",
)
.with_s3_region("us-east-1")
.with_s3_path_style(true);

fluree.create_r2rml_graph_source(config).await?;
```

### Stored Configuration Format (Nameservice)

Iceberg graph sources are persisted as an `IcebergGsConfig` JSON document in the nameservice record’s `config` field.

Note the nesting: the graph source is “Iceberg” (this page), and `catalog.type` selects the **catalog mode** (`rest` vs `direct`) used to discover Iceberg metadata.

**REST catalog config:**

```json
{
  "catalog": {
    "type": "rest",
    "uri": "https://polaris.example.com/api/catalog",
    "warehouse": "my-warehouse",
    "auth": { "type": "bearer", "token": { "env_var": "POLARIS_TOKEN" } }
  },
  "table": "sales.orders",
  "io": {
    "vended_credentials": true,
    "s3_region": "us-east-1",
    "s3_endpoint": null,
    "s3_path_style": false
  }
}
```

**Direct S3 config:**

```json
{
  "catalog": {
    "type": "direct",
    "table_location": "s3://bucket/warehouse/logs/execution_log"
  },
  "table": "",
  "io": {
    "vended_credentials": false,
    "s3_region": "us-east-1",
    "s3_endpoint": null,
    "s3_path_style": true
  }
}
```

**Direct mode requirements:**

- `catalog.table_location` must be an S3 URI (`s3://` or `s3a://`) pointing to the table root directory.
- The table must contain a `metadata/` subdirectory with:
  - `version-hint.text` (containing the current metadata filename, e.g., `00001-abc-def.metadata.json`)
  - The referenced `.metadata.json` file
- Direct mode uses ambient AWS credentials (IAM roles, env vars, `~/.aws/credentials`). It does **not** support vended credentials.

**How Direct metadata resolution works:**

- Fluree does **not** require you to provide a path to `version-hint.text` in the config. You provide the **table root** (`table_location`), and Fluree reads:
  - `"{table_location}/metadata/version-hint.text"` to get the current metadata filename
  - `"{table_location}/metadata/{filename}"` as the table’s current metadata
- `version-hint.text` may contain a bare filename (e.g., `00001-abc.metadata.json`) or a full absolute path (`s3://...`).
- If `version-hint.text` is missing or empty, Direct mode fails with an error mentioning `version-hint.text`.

**Iceberg table setup must already exist:**

Direct mode assumes `table_location` points at a **valid Iceberg table layout** (created by `iceberg-rust`, Spark, etc.), including the `metadata/` directory and referenced metadata/manifest files. Fluree does not create or “bootstrap” Iceberg tables; it only reads them.

**When to use Direct vs REST:**
| Scenario | Recommended |
|----------|-------------|
| Shared catalog (multiple consumers) | REST |
| Writer and reader are the same system | Direct |
| `iceberg-rust` / Spark appending to known S3 path | Direct |
| Need catalog-managed credentials (vended) | REST |
| Minimizing infrastructure (no catalog server) | Direct |

## RDF Mapping (R2RML)

Every Iceberg graph source requires an [R2RML mapping](r2rml.md) (Turtle format) that defines how table rows become RDF triples — specifying subject IRI templates, predicate mappings, and type conversions. See [R2RML](r2rml.md) for the full mapping reference.

### Type Mapping

Iceberg types map to XSD types:

| Iceberg Type | RDF Type |
|--------------|----------|
| int, long | xsd:integer |
| float, double | xsd:decimal |
| string | xsd:string |
| boolean | xsd:boolean |
| date | xsd:date |
| timestamp | xsd:dateTime |
| uuid | xsd:string |

## Querying Iceberg Tables

Iceberg graph sources are queried using standard SPARQL and JSON-LD syntax. In the Rust API, mapped sources resolve transparently through the lazy query builders:

- `fluree.graph("warehouse-orders:main").query()` for a single target that may be either a native ledger or a mapped graph source
- `fluree.query_from()` when the query body itself carries the dataset (`"from"` / `FROM`) or when composing multiple sources

The lower-level materialized snapshot path (`let view = fluree.db(...).await?; fluree.query(&view, ...)`) is still native-ledger-oriented and should not be used for graph source aliases.

```rust
// Single-target lazy query
let result = fluree.graph("warehouse-orders:main")
    .query()
    .sparql("SELECT * WHERE { ?s ?p ?o } LIMIT 10")
    .execute()
    .await?;

// FROM-driven query
let result = fluree.query_from()
    .sparql("SELECT * FROM <warehouse-orders:main> WHERE { ?s ?p ?o } LIMIT 10")
    .execute()
    .await?;
```

### Basic Query

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "from": "warehouse-orders:main",
  "select": ["?orderId", "?total"],
  "where": [
    { "@id": "?order", "ex:orderId": "?orderId" },
    { "@id": "?order", "ex:total": "?total" }
  ],
  "limit": 100
}
```

### SPARQL Query

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?orderId ?total ?date
FROM <warehouse-orders:main>
WHERE {
  ?order ex:orderId ?orderId .
  ?order ex:total ?total .
  ?order ex:orderDate ?date .
  FILTER (?date >= "2024-01-01"^^xsd:date)
}
ORDER BY DESC(?date)
LIMIT 100
```

## Partition Pruning

Iceberg's partition pruning optimizes queries:

```json
{
  "from": "warehouse-orders:main",
  "select": ["?orderId", "?total"],
  "where": [
    { "@id": "?order", "ex:orderId": "?orderId" },
    { "@id": "?order", "ex:total": "?total" },
    { "@id": "?order", "ex:orderDate": "?date" }
  ],
  "filter": "?date >= '2024-01-01' && ?date < '2024-02-01'"
}
```

If `orderDate` is a partition column, Iceberg only scans January 2024 partitions.

## Combining with Fluree Data

Join Iceberg data with Fluree ledgers:

```json
{
  "from": ["customers:main", "warehouse-orders:main"],
  "select": ["?customerName", "?orderTotal", "?orderDate"],
  "where": [
    { "@id": "?customer", "schema:name": "?customerName" },
    { "@id": "?customer", "ex:customerId": "?customerId" },
    { "@id": "?order", "ex:customerId": "?customerId" },
    { "@id": "?order", "ex:total": "?orderTotal" },
    { "@id": "?order", "ex:orderDate": "?orderDate" }
  ],
  "filter": "?orderDate >= '2024-01-01'",
  "orderBy": ["-?orderDate"]
}
```

Combines customer data from Fluree with order data from Iceberg.

## Time Travel

Query historical Iceberg snapshots:

```json
{
  "from": "warehouse-orders:main@snapshot:12345",
  "select": ["?orderId", "?total"],
  "where": [
    { "@id": "?order", "ex:orderId": "?orderId" },
    { "@id": "?order", "ex:total": "?total" }
  ]
}
```

Or by timestamp:

```json
{
  "from": "warehouse-orders:main@timestamp:2024-01-01T00:00:00Z",
  "select": ["?orderId", "?total"],
  "where": [...]
}
```

## Aggregations

Aggregate Iceberg data:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?date (SUM(?total) AS ?dailyRevenue) (COUNT(?order) AS ?orderCount)
FROM <warehouse-orders:main>
WHERE {
  ?order ex:orderDate ?date .
  ?order ex:total ?total .
  FILTER (?date >= "2024-01-01"^^xsd:date)
}
GROUP BY ?date
ORDER BY ?date
```

## Performance

### Query Planning

Fluree pushes filters to Iceberg:

```text
Query: SELECT ?id WHERE { ?order ex:orderDate ?date } FILTER (?date > "2024-01-01")
  ↓
Pushed to Iceberg:
  SELECT order_id FROM sales.orders WHERE order_date > '2024-01-01'
  ↓
Iceberg optimizations:
  - Partition pruning (only scan 2024 partitions)
  - File skipping (skip files outside date range)
  - Column pruning (only read order_id, order_date)
```

### Best Practices

1. **Partition by Common Filters:**
   ```sql
   -- Partition Iceberg table by date
   PARTITIONED BY (YEAR(order_date), MONTH(order_date))
   ```

2. **Use Filters:**
   ```json
   {
     "where": [...],
     "filter": "?date >= '2024-01-01'"  // Enables partition pruning
   }
   ```

3. **Limit Results:**
   ```json
   {
     "where": [...],
     "limit": 1000
   }
   ```

4. **Project Only Needed Columns:**
   ```json
   {
     "select": ["?orderId", "?total"],  // Only these columns read from Parquet
     "where": [...]
   }
   ```

## Schema Evolution

Iceberg supports schema evolution via metadata updates. If a schema change renames/removes columns used by your R2RML mapping, update the mapping accordingly.

## Configuration Options

### AWS Credentials

For S3-backed Iceberg (both REST and Direct modes):

```bash
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_REGION=us-east-1
```

REST catalog mode also supports vended credentials (credentials issued by the catalog). Direct mode uses only ambient AWS credentials (env vars, IAM roles, `~/.aws/credentials`).

## Use Cases

### Analytics on Historical Data

Query years of historical data:

```sparql
SELECT ?year (SUM(?revenue) AS ?totalRevenue)
FROM <warehouse-sales:main>
WHERE {
  ?sale ex:year ?year .
  ?sale ex:revenue ?revenue .
  FILTER (?year >= 2020 && ?year <= 2023)
}
GROUP BY ?year
ORDER BY ?year
```

### Data Warehouse Integration

Combine real-time Fluree data with warehouse analytics:

```json
{
  "from": ["products:main", "warehouse-sales:main"],
  "select": ["?productName", "?totalSold"],
  "where": [
    { "@id": "?product", "schema:name": "?productName" },
    { "@id": "?product", "ex:productId": "?pid" },
    { "@id": "?sale", "ex:productId": "?pid" }
  ]
}
```

### Large-Scale Reporting

Generate reports from petabyte-scale data:

```sparql
SELECT ?region ?category (SUM(?amount) AS ?total)
FROM <warehouse-transactions:main>
WHERE {
  ?txn ex:region ?region .
  ?txn ex:category ?category .
  ?txn ex:amount ?amount .
  FILTER (?year = 2024)
}
GROUP BY ?region ?category
ORDER BY DESC(?total)
```

## Limitations

1. **Read-Only:** Iceberg graph sources are read-only (no writes via Fluree)
2. **Complex Joins:** Large joins between Fluree and Iceberg may be slow
3. **No Full-Text Search:** Use Fluree's BM25 for text search

## Troubleshooting

### Connection Issues

```json
{
  "error": "IcebergConnectionError",
  "message": "Cannot connect to Glue catalog"
}
```

**Solutions:**
- Check AWS credentials
- Verify IAM permissions
- Check network connectivity

### Schema Mismatch

```json
{
  "error": "SchemaMismatchError",
  "message": "Column 'order_date' not found in Iceberg table"
}
```

**Solutions:**
- Update R2RML mapping configuration (if the mapping references missing columns)
- Verify table name and catalog

### Slow Queries

**Causes:**
- Large result sets
- No partition pruning
- Scanning many files

**Solutions:**
- Add date filters to enable partition pruning
- Use LIMIT clause
- Optimize Iceberg table partitioning
- Use Iceberg file compaction

## Related Documentation

- [Graph Sources Overview](overview.md) - Graph source concepts
- [R2RML](r2rml.md) - Relational database mapping
- [Query Datasets](../query/datasets.md) - Multi-graph queries
