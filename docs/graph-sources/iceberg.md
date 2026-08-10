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

# Google Cloud Storage — see "Google Cloud Storage (GCS)" below
fluree iceberg map orders \
  --mode direct \
  --table-location s3://my-bucket/warehouse/sales/orders \
  --r2rml mappings/orders.ttl \
  --s3-endpoint https://storage.googleapis.com \
  --s3-region europe-west1 --s3-path-style
```

Once mapped, graph sources appear in `fluree list`, can be inspected with `fluree info`, and removed with `fluree drop`. See [CLI iceberg reference](../cli/iceberg.md) for all options.

#### Warehouse-root `--table-location` (multi-table, catalog-less)

`--table-location` normally points at a single table's root directory. It may instead point at a **database / namespace root** — e.g. `s3://bucket/warehouse/dw` — for a catalog-less copy whose table directories carry random suffixes (a Snowflake-managed Iceberg database writes `fact_order.UIHGsQex/`, not `fact_order/`). With an `--r2rml` mapping, each `rr:tableName` (e.g. `DW.FACT_ORDER`) is resolved to its own directory under the root via a single S3 `LIST`, matching `<name>.<suffix>/` or bare `<name>/`, case-insensitively on the name (the namespace prefix stripped). Warehouse-root mode is **auto-detected** when the location's leaf directory does not name the requested table; a bare single-table location resolves exactly as before. (A table named identically to its own parent directory reads as single-table.) No catalog or OAuth flags are needed — direct mode reads with ambient IAM credentials.

### HTTP API

When running the Fluree server (or Docker image) with the `iceberg` feature enabled, map a table by POSTing to `{api_base_url}/iceberg/map` (default: `/v1/fluree/iceberg/map`). The endpoint is admin-protected — include the admin Bearer token if admin auth is configured.

```bash
# REST catalog with R2RML mapping (mapping passed inline)
curl -X POST http://localhost:8090/v1/fluree/iceberg/map \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d @- <<'JSON'
{
  "name": "warehouse-orders",
  "mode": "rest",
  "catalog_uri": "https://polaris.example.com/api/catalog",
  "table": "sales.orders",
  "warehouse": "my-warehouse",
  "auth_bearer": "polaris-token-here",
  "r2rml": "@prefix rr: <http://www.w3.org/ns/r2rml#> . ...",
  "r2rml_type": "text/turtle"
}
JSON
```

```bash
# Direct S3 mode (no catalog server)
curl -X POST http://localhost:8090/v1/fluree/iceberg/map \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "name": "execution-log",
    "mode": "direct",
    "table_location": "s3://bucket/warehouse/logs/execution_log",
    "r2rml": "...",
    "r2rml_type": "text/turtle",
    "s3_region": "us-east-1",
    "s3_path_style": true
  }'
```

R2RML can be omitted to auto-generate a direct mapping. AWS credentials for `direct` mode are read from the server's environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, or an attached instance role). See the [Graph Source Endpoints](../api/endpoints.md#graph-source-endpoints) section in the API reference for the complete request/response schema.

### Rust API

`R2rmlCreateConfig::new` and `new_direct` take the R2RML mapping as a **content string** (Turtle or JSON-LD), not a file path — read the file yourself first. To reference an already-stored mapping by address instead, build the config directly with `R2rmlMappingInput::Address(...)`.

**REST catalog mode (Polaris-style):**

```rust
use fluree_db_api::R2rmlCreateConfig;

let mapping = std::fs::read_to_string("mappings/orders.ttl")?;

let config = R2rmlCreateConfig::new(
    "warehouse-orders",
    "https://polaris.example.com/api/catalog",
    "sales.orders",
    mapping,
)
.with_warehouse("my-warehouse")
.with_auth_bearer("my-token")
.with_vended_credentials(true);

fluree.create_r2rml_graph_source(config).await?;
```

**Direct S3 mode (no REST catalog):**

```rust
use fluree_db_api::R2rmlCreateConfig;

let mapping = std::fs::read_to_string("mappings/execution_log.ttl")?;

let config = R2rmlCreateConfig::new_direct(
    "execution-log",
    "s3://bucket/warehouse/logs/execution_log",
    mapping,
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

**Local filesystem (no catalog, no object store):**

A Direct `table_location` may also be a **local path** — a `file://` URI or a
bare absolute path — for Iceberg tables written to the local filesystem (e.g.
with pyiceberg or Spark against a local warehouse). No catalog service, no
object store, no AWS credential resolution: the table is read straight from
disk. Ideal for local development and test datasets.

> **Local tables are opt-in.** Reading the local filesystem is **disabled by
> default**: a Direct `table_location` that is a `file://` URI or an absolute
> path is refused unless the operator has named the directories that may be
> read, via `FLUREE_ICEBERG_LOCAL_ROOTS` (or `iceberg_local_roots` in the config
> file). See [Enabling local tables](#enabling-local-tables) below.

```json
{
  "catalog": {
    "type": "direct",
    "table_location": "file:///data/warehouse/logs/execution_log"
  },
  "table": "",
  "io": { "vended_credentials": false }
}
```

**Enabling local tables**

`FLUREE_ICEBERG_LOCAL_ROOTS` is a colon-separated list of absolute directories,
in the style of `PATH`:

```bash
export FLUREE_ICEBERG_LOCAL_ROOTS=/data/warehouse:/srv/lake
fluree server run --storage-path .fluree/storage
```

or in `.fluree/config.toml`:

```toml
[server]
iceberg_local_roots = "/data/warehouse:/srv/lake"
```

or in `.fluree/config.jsonld`:

```json
{
  "@context": { "@vocab": "https://ns.flur.ee/config#" },
  "server": {
    "iceberg_local_roots": "/data/warehouse:/srv/lake"
  }
}
```

The allowlist does two jobs:

1. **It enables local locations at all.** Unset, `table_location: "/data/wh/t"`
   is refused when the graph source is created, with an error naming the switch.
   Relative entries in the list are ignored, and a list that parses to nothing
   is the same as unset.
2. **It confines every path that is read.** Iceberg manifests reference data
   files by absolute URI, and that metadata is only as trustworthy as whoever
   supplied the table directory. Every resolved path — the table location,
   metadata, manifests, and data files — must land under one of the roots, so a
   reference such as `.../table/../../../etc/passwd` is refused rather than
   followed. Containment is checked both textually and against the path's
   canonical form, so a symlink out of a root does not escape it either.

`FLUREE_ICEBERG_LOCAL_ROOTS=/` allows the whole filesystem. That is a deliberate
choice for a single-tenant workstation, and a poor one for a shared deployment:
any caller who can create a graph source can then point it at any directory the
process can read.

**Why it is off by default.** Fluree is embedded by services that forward
caller-supplied `table_location` values from their own APIs. Before local
support existed, this crate rejected everything that was not `s3://`, so those
services inherited a scheme check they never had to write. Defaulting local
access to on would have removed that protection silently on a version bump —
so the capability ships closed, and an operator turns it on for the directories
they intend to expose.

**Copied and moved tables work with zero configuration.** Iceberg metadata
references data files by absolute URI, so a table copied down from an object
store (or moved on disk) carries its *original* location in every manifest.
Fluree infers the relocation automatically: when the metadata's own `location`
differs from the configured `table_location`, file references under the old
root are read from the new one. Copy the table directory, point
`table_location` at it, done — whether the manifests say `s3://bucket/...` or
`file:///old/path/...`. (Only whole-directory copies are inferred; a table
whose manifests reference files *outside* its own root is not remapped.)

**Direct mode requirements:**

- `catalog.table_location` must be an S3 URI (`s3://` or `s3a://`), a `file://` URI, or an absolute local path, pointing to the table root directory. Local paths additionally require `FLUREE_ICEBERG_LOCAL_ROOTS` to name a directory containing them (see [Enabling local tables](#enabling-local-tables)).
- The table must contain a `metadata/` subdirectory with the current `.metadata.json` file, and (for S3 locations) `version-hint.text` — the current metadata filename (e.g., `00001-abc-def.metadata.json`), a full `s3://`/`gs://` path, or a bare integer version `N` (resolving to `vN.metadata.json`)
- Direct mode uses ambient AWS credentials (IAM roles, env vars, `~/.aws/credentials`) for S3 locations. It does **not** support vended credentials. Local locations use no credentials at all.

**How Direct metadata resolution works:**

- Fluree does **not** require you to provide a path to `version-hint.text` in the config. You provide the **table root** (`table_location`), and Fluree reads:
  - `"{table_location}/metadata/version-hint.text"` to get the current metadata filename
  - `"{table_location}/metadata/{filename}"` as the table’s current metadata
- `version-hint.text` may contain a bare filename (e.g., `00001-abc.metadata.json`), a full absolute path (`s3://...` / `gs://...`), or a bare integer version `N` — the Iceberg Hadoop file-based catalog convention — which resolves to `vN.metadata.json`.
- **Local tables don't need `version-hint.text`** (pyiceberg and most non-Hadoop writers never produce it): when the hint is absent, the `metadata/` directory is listed and the highest-versioned `*.metadata.json` is used. On S3, where listing is not performed, a missing or empty `version-hint.text` fails with an error mentioning `version-hint.text`.

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

### Google Cloud Storage (GCS)

Fluree can read Iceberg tables stored in Google Cloud Storage over the GCS **S3-interoperability** endpoint. Set `s3_endpoint` to that endpoint and use path-style addressing:

```json
{
  "name": "orders",
  "mode": "direct",
  "table_location": "s3://my-bucket/warehouse/sales/orders",
  "r2rml": "...",
  "r2rml_type": "text/turtle",
  "s3_endpoint": "https://storage.googleapis.com",
  "s3_region": "europe-west1",
  "s3_path_style": true
}
```

GCS-backed tables are read through the **same AWS S3 SDK path** as any other S3-compatible store, with one adjustment: the SDK's transport is pinned to **HTTP/1.1**. Reading byte-range requests from GCS through the SDK over HTTP/2 fails (smithy-rs mishandles the partial-content response body), and the Parquet reader is range-based, so data reads would otherwise fail even though metadata reads succeed. HTTP/1.1 handles GCS range responses correctly, and only the Parquet footer plus the column chunks a query needs are fetched — never the whole object. AWS S3 and other S3-compatible stores serve range reads over HTTP/1.1 identically, so this is transparent for every endpoint. (Response-checksum validation is also disabled for these reads, because an object-level checksum cannot validate a partial byte range.)

Because reads go through the AWS SDK, GCS inherits the SDK's correct SigV4 signing — including partition directories like `event_date=2024-01-01/` and values with spaces or non-ASCII characters — plus credential refresh and retries.

**Authentication.** Requests are signed with **AWS SigV4** using GCS **HMAC interoperability keys**, resolved from the standard AWS credential chain. Set them as you would for any S3 access:

```bash
export AWS_ACCESS_KEY_ID=<gcs-hmac-access-key>
export AWS_SECRET_ACCESS_KEY=<gcs-hmac-secret>
# s3_region in the config is the SigV4 signing region (the bucket location).
```

A signing region is required and must match the bucket location — SigV4 scopes the signature to a region, and GCS interop rejects a mismatched or unsigned region. Set it via `s3_region` in the config (recommended, and what the examples use) or via the ambient `AWS_REGION` in the server environment. `s3_endpoint` must be the interop host, and `s3_path_style` must be `true`. HMAC keys do not expire; in `rest` mode, credentials vended by the catalog for a GCS-backed table are used instead (and refreshed by the SDK).

GCS-backed Iceberg tables are typically read via `direct` mode — point `table_location` at the table root. GCS-native conventions are handled automatically: `gs://` paths in metadata/manifests, a Hadoop-style integer `version-hint.text` (resolved to `vN.metadata.json`), and Snappy/GZIP-compressed Parquet. As with any direct-mode table, the Iceberg layout (the `metadata/` directory and a current `version-hint.text`) must already exist in the bucket.

**BigLake REST catalog (catalog auth, distinct from the storage HMAC above).** For tables discovered through Google's BigLake Iceberg REST catalog, the catalog `loadTable` call authenticates with a **Google OAuth token** — separate from the HMAC keys that read the `gs://` data files. A **static `auth_bearer`** (e.g. `gcloud auth print-access-token`) works for a one-shot map/query but **expires after ~1h and cannot renew**, so a long-running tracking worker starts returning 401s. For a workload running as a GCP service account (GKE **Workload Identity**), set **`auth_google_metadata: true`** instead: it mints and auto-refreshes tokens from the instance metadata server, so tracked jobs keep authenticating. (The metadata server is only reachable on GCE/GKE; locally, use a static `auth_bearer`.) The storage HMAC keys are unaffected — they don't expire.

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

## Materialization (into a native ledger)

Querying a graph source reads the Iceberg table on the fly. Native Fluree
features — **BM25 full-text search, vector / RAG, and reasoning** — operate only
on facts committed to a *native* ledger. To use those over an Iceberg table,
**materialize** it: Fluree expands the R2RML mapping over the source rows and
`upsert`s the resulting triples into a target native ledger, which you can then
index and reason over like any other ledger.

> Requires the `iceberg` feature. The endpoints are admin-protected (send the
> admin Bearer token when one is configured).

### One-shot materialize

`POST {api_base_url}/iceberg/materialize` reads the source and writes it into the
target ledger (created if it does not exist):

```bash
curl -X POST http://localhost:8090/v1/fluree/iceberg/materialize \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{ "source": "orders:main", "target": "orders-native:main" }'
```

```json
{
  "source": "orders:main",
  "target": "orders-native:main",
  "from_snapshot_id": null,
  "to_snapshot_id": 5648190075564901028,
  "incremental": false,
  "committed": true,
  "rows_read": 1200,
  "subjects_upserted": 1200,
  "subjects_retracted": 0
}
```

The materialized snapshot id is persisted as a **watermark** — one per
`(source, target, table)` — in a shared materialization-state ledger
(`fluree_materialize_state:main`, created automatically), so re-running resumes
**incrementally** — only the rows added since the last run are read. Keeping the
watermark out of the target ledger means bookkeeping never mixes with your
materialized data. You track nothing; just call it again. A run with no new data
commits nothing and returns `committed: false`. Pass `"force_full": true` to
ignore the watermark and re-read the whole table.

Incremental reads apply when the source's snapshot window is append- or
compaction-only; an `overwrite`/`delete` snapshot, or expired history, falls back
to a full re-read automatically.

### Tracking (keep the target fresh automatically)

`POST {api_base_url}/iceberg/track` registers a `source → target` job with the
in-process tracking worker, runs an immediate first sync, and then refreshes the
target on a timer (default every 30s):

```bash
curl -X POST http://localhost:8090/v1/fluree/iceberg/track \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{ "source": "orders:main", "target": "orders-native:main" }'
```

- `POST {api_base_url}/iceberg/untrack` — stop tracking (leaves materialized data in place).
- `GET {api_base_url}/iceberg/tracking` — list tracked jobs and worker stats.

The worker runs on write nodes (not peers). Tracked jobs are **persisted** in the
`fluree_materialize_state:main` state ledger, next to the watermarks, and restored
when the worker starts — so a restart resumes tracking on its own, incrementally,
with no need to re-issue `track`. `untrack` is equally durable: it clears the
record, so a restart will not resurrect the job.

### One ledger per partition (templated target)

`target` may be a **template** with `{column}` placeholders resolved from each
source row, so a single materialize/track job **fans out** into one native ledger
per partition value — e.g. isolating each `(tenant, user)` into its own ledger:

```bash
curl -X POST http://localhost:8090/v1/fluree/iceberg/materialize \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{ "source": "orders:main", "target": "orders_{tenant_id}_{user_id}:main" }'
```

Each row is routed to the ledger its columns expand to (e.g. `orders_acme_u42:main`),
creating that ledger on first sight; a row whose template columns are null is
skipped. One scan of the source feeds every target, and the job keeps **one
watermark per source table** (in the state ledger) regardless of how many ledgers
it writes. A placeholder-free `target` is the ordinary single-ledger case,
unchanged.

This is the way to give each partition its **own** ledger for per-partition
access isolation: Fluree's read policy is graph-blind, so separate access regimes
are separate ledgers (isolated by the per-ledger read gate) rather than named
graphs. Within each per-partition ledger, `rr:graphMap` named-graph routing still
applies independently.

### Multiple sources into one target (additive)

By default materialization is **additive**: it inserts and updates triples and
never removes them. Several sources can safely materialize into the **same**
target ledger — a shared knowledge graph, or a join table that only adds an edge
to a parent entity. `rdf:type` is asserted additively (via an idempotent insert),
so classes **union** across sources instead of the last writer clobbering the
rest; the remaining predicates are upserted per predicate. For example, one
source can type a subject `as:Article` while a second source adds `as:Announce`
to the *same* subject IRI, and both classes remain queryable. (The "dedicated
target" restriction under *Assumptions and limitations* applies only to
latest-by-key mode, not to additive mode.)

### Change data capture: updates and deletes (latest-by-key)

For a change-data-capture source — an append-only log where each change is a new
row and a delete is a **tombstone row** — configure two options so Fluree applies
*latest-by-key* semantics that match a
`ROW_NUMBER() OVER (PARTITION BY id ORDER BY <ts> DESC) = 1` view:

- **`order_by`** — a column that orders a key's revisions (e.g. an event
  timestamp or offset). Must be an **integer / date / timestamp** column. The
  latest row per subject wins, and a whole-subject *replace* clears fields that
  were dropped in the newer revision.
- **`delete_column`** + **`delete_values`** — how a row is recognized as a
  delete. `delete_values` lists the `delete_column` values that mean "deleted";
  a `null` entry matches a NULL column (the Debezium null-payload convention).
  When the latest row for a key is a tombstone, the **entire subject** (all its
  triples) is retracted.

Two ways to encode a delete:

```bash
# (a) value-match: an op column carries "d" on a delete (Debezium-style).
curl -X POST http://localhost:8090/v1/fluree/iceberg/map \
  -H 'Content-Type: application/json' \
  -d '{ "name": "orders", "mode": "direct",
        "table_location": "s3://my-bucket/warehouse/sales/orders",
        "r2rml": "...", "r2rml_type": "text/turtle",
        "order_by": "event_timestamp",
        "delete_column": "_op", "delete_values": ["d", "delete"] }'

# (b) null-payload: a delete row has the key set but content columns null;
#     pick a column always set on a live row but null on a delete, and list null.
curl -X POST http://localhost:8090/v1/fluree/iceberg/map \
  -H 'Content-Type: application/json' \
  -d '{ "name": "orders", "mode": "direct",
        "table_location": "s3://my-bucket/warehouse/sales/orders",
        "r2rml": "...", "r2rml_type": "text/turtle",
        "order_by": "event_timestamp",
        "delete_column": "status", "delete_values": [null] }'

# (combine both: a "d" op value OR a null column both mean delete)
#   "delete_column": "_op", "delete_values": ["d", null]
```

These live in the graph source's stored config (`IcebergGsConfig.delete` and
`order_by`); set them once at `iceberg map` time. A delete removes the **whole
entity**, not individual columns — a `null` in an ordinary column of a *live* row
just clears that one predicate.

### Assumptions and limitations

Latest-by-key mode (i.e. when `order_by` and/or a delete convention is set)
assumes the source matches the append-only, full-image CDC shape these features
target. The materializer enforces what it can and documents the rest:

- **One complete row per subject revision.** Each row is a full snapshot of its
  subject. A source that assembles one subject across multiple rows (e.g. an
  unpivoted join table) is not supported in latest-by-key mode — use additive
  mode (omit `order_by`/`delete`) or a one-row-per-subject view.
- **One triples map per logical table** (enforced — multiple would clobber under
  whole-subject replace).
- **`order_by` must be populated and value-orderable** (int/date/timestamp,
  enforced). A row with a null ordering value sorts as oldest.
- **The target ledger is dedicated to one source.** Whole-subject retraction owns
  the subject; don't mix other sources or hand-written data about the same IRIs
  into the same target. (With a templated target, each fanned-out per-partition
  ledger is likewise dedicated to that source+partition.)
- **Deletes must be expressed as tombstone rows.** A key that simply stops
  appearing — with no tombstone — is not reconciled (a set-difference pass is a
  possible future addition).
- The target data is committed first; the watermark then advances in a **separate
  commit to the state ledger**, never before the data — so an interrupted run
  re-materializes the same window on the next pass (self-healing, because the data
  writes are idempotent: whole-subject replace / idempotent insert+upsert).
- **A window is applied as several transactions**, chunked to fit the target
  ledger's novelty ceiling. An interrupted or failed pass can therefore leave a
  target *partially applied* — some subjects re-asserted, others not yet — until
  the next successful poll re-materializes the window (the watermark only
  advances after the whole window commits).
- **A window's working memory is budgeted, not unbounded.** The pass retains one
  node per distinct subject in the window; a window whose estimated accumulator
  exceeds `FLUREE_MATERIALIZE_MEMORY_BUDGET_MB` (default 1024; `0` disables)
  fails *before any commit* with a typed error naming the size and the levers —
  instead of the process being OOM-killed. An incremental window that large
  usually means the poll interval is too long; a *full* read that large needs a
  raised budget until streaming finalization lands. This failure recurs every
  poll until the budget or window changes.
- **Merge-on-read tables fail closed on both materialize paths** (incremental
  added-files scans and full reads), exactly as on the query path — see
  [Limitations](#limitations) item 4 and `FLUREE_ICEBERG_ALLOW_MOR_DELETES`.
  Materializing makes the guard *more* important, not less: a query returning
  deleted rows is a transient wrong answer, but a materialized twin commits them
  as state and advances the watermark past the window.
- **Foreign-key (`rr:refObjectMap`) edges are not materialized.** The virtual
  query path resolves them at query time; the materializer does not yet index
  parent tables, so FK edges are absent from the twin (each pass logs a warning
  with the dropped-edge count when the mapping carries them).
- **`rr:graphMap` routing is materialize-only today.** The materializer places
  rows into named graphs per the subject map's graph map; the virtual query path
  does not yet read graph maps, so a graph-scoped query returns different
  results against the source and its twin. Query-path parity is a tracked
  follow-up.
- **Compaction can silently turn incremental into a full re-read.** The
  incremental window treats `replace` (compaction) snapshots as safe when the
  writer preserves data sequence numbers (Spark's `rewrite_data_files` default).
  A compaction that *reassigns* sequence numbers makes every rewritten file look
  newly added — correctness is unaffected (the writes are idempotent), but the
  cheap incremental poll silently becomes a full-table read. If a tracked
  source shows periodic cost spikes, check the source's compaction settings.
- **In a multi-node deployment the worker runs on every write node** (every
  non-peer node with indexing enabled) with its own job set, so two nodes
  tracking the same `(source, target)` will interleave their commits;
  leader-gating is a tracked follow-up. Nodes running external-indexer mode
  (`indexing_enabled = false`) do not run the worker at all — materialization
  needs a local indexer draining novelty between chunks — and `POST
  /iceberg/track` on such a node returns an error saying so.
- **A templated target creates ledgers without bound** — one per distinct
  partition value that appears in the source. Malformed or high-cardinality
  partition columns create that many ledgers; the template columns are the
  operator's responsibility to keep bounded and well-formed.

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

**Google Cloud Storage:** when the endpoint is the GCS S3-interoperability host, reads go through the AWS S3 SDK (transport pinned to HTTP/1.1), signing requests with AWS SigV4 using GCS HMAC interop keys — the same `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` shown above (set them to your HMAC keys). See [Google Cloud Storage (GCS)](#google-cloud-storage-gcs).

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

## Materializing a Native Twin

Querying a virtual Iceberg source pays catalog + S3 latency on every query. For a stable, low-latency copy, **materialize** it into a native ledger — a *twin* — with [`fluree materialize`](../cli/materialize.md):

```bash
fluree materialize warehouse-orders:main --output ledger --verify full
```

A twin is an ordinary, fully-indexed Fluree ledger holding a point-in-time snapshot of the virtual source, so it supports the full query surface (SPARQL / JSON-LD / Cypher), time travel, branching, policy, and `.flpack` export — with none of the per-query catalog/S3 round-trips.

### The completion stamp and watermark

The build writes a **completion stamp** into the twin's *final* commit (in the `https://ns.flur.ee/materialize#` namespace): `builderVersion`, `mappingHash` (a SHA-256 of the R2RML mapping — a mapping change invalidates the twin), the `watermark` (the per-table pinned Iceberg snapshot vector captured at build time, what a delta-sync reads), and a `sampleSeed`. The contract is: **a twin is valid iff a head-walk finds this stamp** — a build that dies mid-way leaves the head commit unstamped, so a partial twin is detectable. A **pin-all pre-pass** pins every table's current snapshot up front so the watermark reflects one narrow window rather than the whole build duration.

### Verification modes

Before a twin is announced, a memory-bounded parity gate re-checks it against the source:

- **`quick`** (default) — per-class counts + a seeded 3-subjects-per-class sample against the build's *own* enumerator. A **shared oracle**: catches ingest/index corruption, but not enumerator-logic bugs (they appear identically on both sides).
- **`full`** — a whole-twin triple diff (the twin streamed in a single linear pass over the binary index), external-sorted and diffed under a bounded working set.

A failed gate drops the twin so nothing unverified stays announced. See the [`fluree materialize`](../cli/materialize.md) reference for the full flow, the machine-safety posture, and `--tmp-dir`.

## Limitations

1. **Read-Only:** Iceberg graph sources are read-only (no writes via Fluree)
2. **Complex Joins:** Large joins between Fluree and Iceberg may be slow
3. **No Full-Text Search:** Use Fluree's BM25 for text search
4. **Merge-on-read deletes are not yet applied (fail-closed):** Fluree reads the
   live data files of a snapshot but does **not** apply Iceberg *merge-on-read*
   position/equality delete files. To avoid silently returning deleted rows (or
   over-counting `COUNT(*)`/row totals), a query over a snapshot that carries
   delete files is **refused** with a `Merge-on-read deletes not applied` error.
   Copy-on-write deletes (the Snowflake-managed v2 default today) rewrite data
   files and are handled correctly — this only affects tables written with
   merge-on-read semantics (e.g. Athena `DELETE`, Flink/CDC upserts, Snowflake v3
   deletion vectors, or Snowflake v2 once `ENABLE_ICEBERG_MERGE_ON_READ` is on).
   See the switch below to override.

### Environment switches

| Variable | Default | Effect |
| --- | --- | --- |
| `FLUREE_ICEBERG_ALLOW_MOR_DELETES` | off | When truthy (`1`/`true`/`yes`/`on`), **disables** the fail-closed merge-on-read guard: delete files are ignored and the read proceeds. **Results may include deleted rows and row counts may be over-counted.** A one-time warning is logged per table. |
| `FLUREE_ICEBERG_PREDICATE_PUSHDOWN` | on | When falsy (`0`/`false`/`off`), disables row-group / row-level predicate pushdown during Parquet reads. |
| `FLUREE_ICEBERG_INFO_COUNT_BUDGET_MS` | `10000` | Wall-clock budget for the virtual `/info` row-count fetch; `0` returns structure only. |

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
