# Delta Lake Tables

Fluree queries [Delta Lake](https://delta.io/) tables in place. An
[R2RML mapping](r2rml.md) turns table rows into RDF, and the mapped source is
queried like any other graph — SPARQL or JSON-LD, joined with ledgers and other
graph sources, under the same [access policy](iceberg.md#access-policy) as
every mapped source. Tables are only ever read.

The server and CLI include Delta support by default (the `delta` build
feature). A program embedding `fluree-db-api` enables that crate's `delta`
feature to get it.

## What is read

The reader follows the [Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
through Delta Kernel, so a query sees exactly the table's logical rows:

- the transaction log and its checkpoints decide which data files are live;
- **deletion vectors** remove rows without the file being rewritten;
- **column mapping** resolves logical column names to physical Parquet
  columns, across renames, drops and re-adds;
- **partition values** are injected as columns;
- a table that requires a reader feature the reader does not implement is
  refused, never read partially.

Reading the Parquet files of a Delta directory directly would get all of these
wrong, which is why a Delta table is not addressed as a folder of files.

## Registering a source

A Delta source names its tables by **path**, or — for Databricks — by their
name in [Unity Catalog](#unity-catalog). By path, each `rr:tableName` in the
mapping resolves to:

1. its explicit `tables` entry, if there is one; otherwise
2. a directory under `root`, with the name's dots as path separators —
   `dbo.orders` → `<root>/dbo/orders`.

A location is one of:

| Store | Location |
|-------|----------|
| Amazon S3 (and S3-compatible) | `s3://bucket/prefix` |
| Azure Data Lake Storage Gen2 | `abfss://<container>@<account>.dfs.core.windows.net/<path>` |
| Microsoft Fabric OneLake | `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/<path>` — a lakehouse's tables are under `<item>/Tables`, so with that as `root`, `dbo.orders` names `Tables/dbo/orders` |
| Local filesystem | `file:///…` or an absolute path under [`FLUREE_ICEBERG_LOCAL_ROOTS`](iceberg.md#enabling-local-tables), the allowlist Iceberg local tables use. A Delta log that names a data file outside it is refused at read |

An Azure location must name its storage host in full; other `abfss://` forms
are refused, so a stored location cannot direct credentials elsewhere.

### CLI

```bash
fluree delta map sales --root s3://lake/Tables --r2rml mappings/sales.ttl
```

See [`fluree delta`](../cli/delta.md).

### HTTP API

```http
POST /v1/fluree/delta/map
Content-Type: application/json

{
  "name": "sales",
  "root": "s3://lake/Tables",
  "tables": { "orders": "s3://lake/raw/orders_v2" },
  "r2rml": "@prefix rr: <http://www.w3.org/ns/r2rml#> . …",
  "s3_region": "us-east-1"
}
```

Optional fields: `branch`, `r2rml_type`, `s3_endpoint`, `s3_path_style`,
`azure_tenant_id`, `azure_client_id`, `azure_client_secret_env` /
`azure_client_secret`, the [Unity Catalog](#unity-catalog) fields, `model`,
`default_allow`. The response reports the stored mapping, the mapped
table names, `table_versions` (the current Delta version of each table that
opened with every column its maps reference) and `table_warnings` (a table that
could not be read, or a mapped column it lacks; the source is registered
regardless).

### Rust API

```rust
use fluree_db_api::DeltaCreateConfig;

let config = DeltaCreateConfig::new("sales", "s3://lake/Tables", mapping_turtle);
let created = fluree.create_delta_graph_source(config).await?;
```

### Credentials

Credentials belong to the process that **reads** the tables — the server, or a
CLI running locally.

For creating an identity and granting it access on ADLS Gen2, OneLake or
Databricks, step by step, see
[Connecting to lakehouse platforms](lakehouse-platforms.md).

**S3.** `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (and
`AWS_SESSION_TOKEN`) from the environment, a web-identity token, or the
container / instance role. Shared-config profiles (`AWS_PROFILE`, SSO) are not
read; export the profile's credentials into the environment instead. Nothing
is stored on the graph source.

**Azure.** With no Azure options, the ambient chain: the `AZURE_*` environment
variables (`AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` + `AZURE_TENANT_ID`,
`AZURE_STORAGE_ACCOUNT_KEY`, a SAS token, `AZURE_FEDERATED_TOKEN_FILE` for
workload identity), else the managed identity of the host. To name a service
principal per source, give its tenant id, client id and secret:

```json
{
  "name": "fabric-sales",
  "root": "abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Tables",
  "azure_tenant_id": "…",
  "azure_client_id": "…",
  "azure_client_secret_env": "FABRIC_CLIENT_SECRET",
  "r2rml": "…"
}
```

`azure_client_secret_env` names an environment variable of the reading
process, so the secret is never stored; `azure_client_secret` is a literal
that is stored with the graph source. An embedding application can instead
supply a secret reference, resolved through its `SecretResolver`. Tokens are
requested for the storage audience and refreshed automatically.

What the identity needs:

- **ADLS Gen2**: the *Storage Blob Data Reader* role on the storage account or
  container. A new role assignment can take a minute or two to take effect; until
  then reads fail with 403.
- **OneLake**: a workspace role that includes OneLake data access. *Viewer*
  does not — a Viewer principal authenticates and is then refused with
  `403 … not authorized … for workspace`. *Contributor* (or a OneLake data
  access role granting read on the lakehouse) works. No tenant-wide setting is
  required for a service principal to read OneLake files.

In a schema-enabled lakehouse tables live under `Tables/<schema>/<table>`, so
with `<item>/Tables` as the root they are mapped as `rr:tableName "dbo.orders"`;
in a lakehouse without schemas they are `Tables/<table>` and mapped by bare
name.

Access control on the tables themselves (OneLake security roles, row- or
column-level rules defined in Fabric) is enforced by Azure against that
identity, not re-implemented here; use a model ledger's
[access policy](iceberg.md#access-policy) to govern what Fluree users see.

### Unity Catalog

With a Databricks workspace as `unity_uri` (`--unity-uri`), a table without a
`tables` entry is named as Databricks names it, `catalog.schema.table`. Unity
Catalog says where it lives and issues the credentials that read it, so managed
tables — whose storage path Unity owns — are reachable, and the reading process
needs no storage credentials of its own.

```json
{
  "name": "dbx-sales",
  "unity_uri": "https://<workspace>.cloud.databricks.com",
  "unity_catalog": "main",
  "oauth2_client_id": "<application-id>",
  "oauth2_client_secret_env": "DATABRICKS_CLIENT_SECRET",
  "s3_region": "us-east-1",
  "r2rml": "…"
}
```

- **Names.** `unity_catalog` and `unity_schema` complete a shorter name: with
  the config above, `rr:tableName "sales.orders"` is `main.sales.orders`. A
  `tables` entry is still read by path with the process's own credentials, so
  one source can mix the two. `root` and `unity_uri` exclude each other.
- **Auth.** A service principal (`oauth2_client_id` and its secret), whose
  token is requested and renewed automatically, or a personal access token
  (`auth_bearer_env` / `auth_bearer`), which is not. The token URL and scope
  default to the workspace's own endpoint and `all-apis`. A secret is given as
  a literal, stored with the source, or as the name of an environment variable
  of the reading process; a server accepts only names listed in
  [`FLUREE_GRAPH_SOURCE_SECRET_ENV_VARS`](iceberg.md#stored-configuration-format-nameservice).
- **Credentials.** Unity issues them per table, scoped to that table's path,
  for an hour. Each table's are kept until a few minutes before they expire
  and then requested again; concurrent queries share one request. The table's
  log and footer caches are unaffected by the change of credentials.
- **Region.** Unity does not name an S3 bucket's region: give `s3_region`, or
  set `AWS_REGION`.
- **What is read.** Delta tables with files of their own. A view, a table in
  another format, and a table the principal cannot see or Unity will not issue
  credentials for are reported by name — as a `table_warnings` entry at
  registration and as the error of a query that touches them. A location on
  the local filesystem is never followed, whatever
  `FLUREE_ICEBERG_LOCAL_ROOTS` allows.
- **Freshness of placement.** Where a name points is looked up when its table
  handle is first opened, and again when the handle is rebuilt
  (`FLUREE_ICEBERG_REST_CLIENT_TTL_SECS`, default 15 minutes); a table dropped
  and recreated elsewhere is read at its old location until then. With the
  default, credentials are in practice replaced with the handle; renewal
  within a handle serves a longer setting and scans that outlast a set.

Setting up the workspace side — external data access, the service principal
and its four privileges — is walked through in
[Connecting to lakehouse platforms](lakehouse-platforms.md#databricks-tables-through-unity-catalog).

## Mapping

The mapping is ordinary [R2RML](r2rml.md) with `rr:tableName` logical tables.
`rr:sqlQuery` is refused at registration: there is no SQL engine behind a
Delta source.

```turtle
@prefix rr:  <http://www.w3.org/ns/r2rml#> .
@prefix ex:  <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<#Order> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "dbo.orders" ] ;
    rr:subjectMap [ rr:template "http://example.org/order/{order_id}" ; rr:class ex:Order ] ;
    rr:predicateObjectMap [
        rr:predicate ex:total ;
        rr:objectMap [ rr:column "amount" ; rr:datatype xsd:decimal ]
    ] .
```

### Column types

| Delta type | Read as |
|------------|---------|
| `boolean` | boolean |
| `byte`, `short`, `integer` | 32-bit integer |
| `long` | 64-bit integer |
| `float`, `double` | floating point |
| `decimal(p,s)` | decimal (precision ≤ 38) |
| `string` | string |
| `binary` | bytes |
| `date` | date |
| `timestamp` | UTC instant |
| `timestamp_ntz` | wall-clock date-time (no zone) |

A column of any other type (`array`, `map`, `struct`, `variant`) cannot be
mapped; registration warns when a mapping references one. The table's other
columns are unaffected, because a scan projects only the columns the mapping
references.

## Querying

```json
{
  "@context": { "ex": "http://example.org/" },
  "from": "sales:main",
  "select": ["?order", "?total"],
  "where": { "@id": "?order", "ex:total": "?total" }
}
```

```sparql
PREFIX ex: <http://example.org/>
SELECT ?order ?total
FROM <sales:main>
WHERE { ?order ex:total ?total }
```

Within one query, each table is read at **one version**: the version resolved
the first time the query touches the table serves every later scan and count,
so a commit landing mid-query cannot split the result across two versions. Two
tables of one source are each consistent, but are not a cross-table snapshot —
Delta has no such thing.

## Performance

Only the columns a query's mapped predicates use are decoded, and a table's
data files are read in parallel — by default one per core, at most 32; set
`FLUREE_DELTA_SCAN_CONCURRENCY` to change it.

**Filter pushdown.** A comparison the query places on a mapped column — a
`FILTER` with `=`, `<`, `<=`, `>`, `>=` or `IN`, a `VALUES` block, a constant
object, or a bound subject whose template names the column — is pushed into
the scan, where it acts at three grains. Each data file's partition values and
min/max statistics are checked first, and a file that cannot hold a matching
row is never opened. Inside a file that is opened, the Parquet footer's
row-group statistics and page index are checked the same way, and only the row
groups and pages that may hold a match are fetched and decoded. The rows that
are decoded are then filtered before any RDF term is built for them. File skipping depends on
how the table is laid out: a filter on a partition column, or on a column the
writer clusters or sorts by, skips most of the table, while a filter on a
column whose values are spread across every file skips nothing and is left to
the row filter. Pruning inside a file compares integers, dates, doubles,
strings and timestamps stored as 64-bit microseconds; a timestamp a writer
stored in the legacy 96-bit form carries no usable bounds. A file with a
deletion vector is decoded whole, because the vector addresses rows by their
position in the file.

A comparison is pushed only when its value has the column's own type — an
integer against an integer column, a string against a string column, a zoned
`xsd:dateTime` against `timestamp` and an unzoned one against `timestamp_ntz`.
Decimal and `float` columns, comparisons against a zero `double`, and any
other expression are evaluated by the query engine over the decoded rows.
An aggregate over one table (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, with or
without `GROUP BY`) pushes its filter the same way. An aggregate that joins
tables filters the table it aggregates over after reading it.

**Counts.** `COUNT` of a whole mapped table, with no filter or grouping, is
answered from the row counts recorded in the transaction log without opening a
data file — provided every file records one, no file carries a deletion
vector, and statistics show no null in the columns the count depends on.
Otherwise the table is scanned.

**Footers.** A data file never changes, so its Parquet footer (with its page
index) is read once and kept — up to `FLUREE_DELTA_FOOTER_CACHE_MB` across all
tables (default 128, `0` keeps none). A repeat query fetches only the pages it
needs.

**The transaction log.** Every query that does not pin a version checks the
table's log for new commits, so it always reads the table's current version.
It does not read the log again from the start: the server remembers the last
version it read of each table, with that version's list of data files, and
reads only the commits added since. A repeat query of an unchanged table costs
one listing of the log directory and one metadata request, and reads nothing; the four most recently used
pinned versions of a table are remembered the same way. A table that is
deleted and written again under the same path is noticed and read afresh.
File lists are held up to a process-wide `FLUREE_DELTA_LOG_CACHE_MB` (default
256; a table whose list does not fit is planned from its log on every query,
and `0` turns the memory off). What is remembered belongs to the table's
shared handle, which is rebuilt every
[`FLUREE_ICEBERG_REST_CLIENT_TTL_SECS`](../operations/configuration.md#iceberg--r2rml-graph-source-tuning)
(default 15 minutes) so that a rotated secret takes effect; the first query
after that reads the log from its last checkpoint again.

## Time travel

An alias with a time specification reads every table of the source at the
state that specification selects, for the whole query.

| Selector | Selects |
| --- | --- |
| `@snapshot:<n>` | Delta table version `n` |
| `@time:<timestamp>` | The latest version committed at or before that instant (RFC 3339). An instant after the latest commit selects the latest version |

`@iso:` is an alias of `@time:`, and `@recorded:` a synonym: a Delta commit
carries one time.

```json
{ "from": "sales:main@snapshot:840", "select": ["?total"], "where": { "ex:total": "?total" } }
```

```sparql
SELECT ?total FROM <sales:main@time:2026-03-01T00:00:00Z> WHERE { ?o ex:total ?total }
```

```bash
fluree query sales --at snapshot:840 --sparql '…'
```

In Rust: `fluree.graph_at(alias, TimeSpec::AtSnapshot(840))` /
`TimeSpec::AtTime(iso)`.

**Which commit time.** A table written with in-commit timestamps
(`delta.enableInCommitTimestamps`) resolves instants against the times recorded
in its log. Any other table resolves them against the **modification times of
its log files**, as the protocol specifies — which a copied or restored table
does not preserve. Pin such a table by version.

**Schema.** Columns resolve against the schema of the selected version; the
R2RML mapping is always the source's current one. A pin to a version that lacks
a mapped column is an error naming the column. With column mapping, a column
dropped and later re-added under the same name is a new column: old versions
read the old values, new versions do not resurrect them.

**Errors, never a fallback.** A selection the table cannot satisfy fails:

- `@snapshot:` with a version the table never had, or one whose log entries
  have been cleaned up → `snapshot <n> not found for table '…'`.
- `@time:` before the oldest version the log can still reconstruct →
  `no snapshot of table '…' at or before <requested>; the oldest retained
  snapshot is <time>`.
- A version whose log still replays but whose **data files were removed** (by
  `VACUUM`) resolves, and then fails when the scan reaches the missing file.
  Log retention and data-file retention are separate settings on a Delta
  table; history is readable only as far back as both reach.

`@t:` and `@commit:` name Fluree ledger states and are rejected. Naming one
source at two different states in one query is rejected, as for
[Iceberg sources](iceberg.md#time-travel).

## Limitations

- **Storage**: S3 (and S3-compatible endpoints), ADLS Gen2, OneLake and the
  local filesystem. Azure sovereign clouds are not addressable.
- **`ORDER BY … LIMIT`** is not pushed down; see [Performance](#performance).
- **Materialization and tracking** (`fluree materialize`, `fluree track`) are
  not available for Delta sources.
- **Nested types** cannot be mapped.
- **Catalogs**: Unity Catalog places tables that are named in the mapping;
  catalogs, schemas and tables cannot be browsed, and no mapping is generated
  from them.

## Related Documentation

- [`fluree delta`](../cli/delta.md)
- [Connecting to lakehouse platforms](lakehouse-platforms.md) — ADLS Gen2, OneLake and Databricks, step by step
- [R2RML](r2rml.md)
- [Iceberg / Parquet](iceberg.md) — access policy, local-table allowlist
- [Time travel](../concepts/time-travel.md)
