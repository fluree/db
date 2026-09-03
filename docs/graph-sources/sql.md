# SQL Graph Sources

Query tables in a relational database or data warehouse as RDF, through an
[R2RML mapping](r2rml.md), without loading the data into a ledger. A SQL graph
source reaches its tables over HTTP through any engine that speaks the **Trino
client protocol** — so nothing in Fluree holds a database connection, no JDBC
or native driver is compiled into the binary, and the same source works from a
long-running server and from a Lambda.

## Where the SQL runs

Fluree does not talk to Postgres, MySQL, Snowflake or Oracle directly. It sends
one `POST /v1/statement` per table scan to an endpoint and pages through the
result. Anything that implements that protocol works:

| Endpoint | When to use it |
|----------|----------------|
| **Trino / Starburst** | The general answer. One Trino coordinator fronts Postgres, MySQL, SQL Server, Oracle, Snowflake, BigQuery, Redshift, Iceberg, Delta and dozens more through its connectors, and its client protocol is plain HTTP + JSON. |
| **PrestoDB** | Same protocol with the older `X-Presto-*` headers (`"protocol": "presto"`). |
| **`fluree-sql-bridge`** | A small sidecar for a single Postgres, MySQL or SQLite database when running a JVM is not wanted. It speaks the same protocol, so Fluree treats it exactly like Trino. See [Running the bridge](#running-the-bridge). |

Every page of a result is one stateless HTTP request carrying its own
credentials; dropping a result stream cancels the statement server-side.

## Registering a source

=== CLI

```bash
fluree sql map orders-db \
  --endpoint https://trino.example.com:8443 \
  --catalog hive --schema sales \
  --auth-bearer "$TRINO_TOKEN" \
  --r2rml mappings/orders.ttl
```

=== HTTP

```bash
curl -X POST http://localhost:8090/v1/fluree/sql/map \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d @- <<'JSON'
{
  "name": "orders-db",
  "endpoint": "https://trino.example.com:8443",
  "catalog": "hive",
  "schema": "sales",
  "auth_bearer": "…",
  "r2rml": "@prefix rr: <http://www.w3.org/ns/r2rml#> . …"
}
JSON
```

=== Rust

```rust
use fluree_db_api::{FlureeBuilder, SqlCreateConfig};

let fluree = FlureeBuilder::memory().build_memory();
let mut config = SqlCreateConfig::new("orders-db", "https://trino.example.com:8443", MAPPING_TTL);
config.catalog = Some("hive".into());
config.schema = Some("sales".into());
fluree.create_sql_graph_source(config).await?;
```

Registration compiles the mapping, stores it in content-addressed storage, and
probes the endpoint with `SELECT 1`. A failed probe is reported
(`connection_tested: false`) but does not block registration — credentials can
be fixed later; the first query surfaces the real error.

### Configuration

| Field | Default | Meaning |
|-------|---------|---------|
| `endpoint` | — | Base URL; `/v1/statement` is appended |
| `dialect` | `trino` | How identifiers and literals are rendered: `trino`, `postgres`, `mysql`, `sqlite`. Use the engine *behind* a bridge. |
| `protocol` | `trino` | Header family: `trino` (`X-Trino-*`) or `presto` (`X-Presto-*`) |
| `catalog`, `schema` | — | Defaults for unqualified `rr:tableName`s |
| `user` | `fluree` | The protocol's user header; required even with a bearer token |
| `auth` | none | `bearer` (static token) or `oauth2_client_credentials`; values accept the same `env_var` / `secret_ref` indirection as Iceberg catalog auth |
| `session` | `{}` | Session properties, e.g. `{"query_max_run_time": "5m"}` |
| `request_timeout_secs` | `120` | Per page fetch |

Table names in the mapping are dotted and quoted part by part:
`rr:tableName "sales.orders"` becomes `"sales"."orders"`; with `catalog`
set, an unqualified name resolves inside it.

### `rr:sqlQuery`

Unlike Iceberg sources, a SQL source accepts the R2RML `rr:sqlQuery` logical
table. The query is scanned as a derived table, with Fluree's projection and
pushed filters applied on top of it:

```turtle
<#OpenOrders> a rr:TriplesMap ;
  rr:logicalTable [ rr:sqlQuery "SELECT id, total FROM sales.orders WHERE status = 'open'" ] ;
  rr:subjectMap [ rr:template "http://example.org/order/{id}" ; rr:class ex:Order ] ;
  rr:predicateObjectMap [ rr:predicate ex:total ; rr:objectMap [ rr:column "total" ] ] .
```

```sql
-- what the engine sends for  ?o ex:total ?total
SELECT "id", "total" FROM (SELECT id, total FROM sales.orders WHERE status = 'open') AS "__fluree_q"
```

The query text is trusted as written — a mapping author already has
root-equivalent read access to the source, exactly as with `rr:tableName`.

## Querying

A SQL source is queried like any other mapped source — as a `from` target, in
`FROM <…>`, or inside `GRAPH`:

```sparql
PREFIX ex: <http://example.org/>
SELECT ?name ?total
FROM <orders-db:main>
WHERE {
  ?o a ex:Order ; ex:customer ?c ; ex:total ?total .
  ?c ex:name ?name .
  FILTER(?total > 100)
}
```

### Joining with a ledger

Put the ledger in `FROM`, the SQL source in `FROM NAMED`, and address it with
`GRAPH`. The join runs in the engine over the rows the source returns:

```sparql
PREFIX ex: <http://example.org/>
SELECT ?name ?team
FROM <teams:main>
FROM NAMED <orders-db:main>
WHERE {
  ?p ex:team ?team .
  GRAPH <orders-db:main> { ?p ex:name ?name }
}
```

Without the `FROM NAMED`, the `GRAPH` block resolves to nothing and the join is
empty — the same dataset rule that applies to Iceberg sources.

### What is pushed to SQL

The query engine asks the source for **one table at a time** — a projection,
conjunctive filters, and nothing else — and does joins, `OPTIONAL`, `UNION`,
property paths and aggregation itself over the returned rows. So each triples
map touched by a query becomes one statement of the shape:

```sql
SELECT "id", "customer_id", "total" FROM "sales"."orders" WHERE "total" > 1E2
```

Pushed as `WHERE`:

- `FILTER` comparisons and `IN` / single-variable `VALUES` on a mapped column
- constant objects (`?o ex:status "open"`)
- a bound subject (`<http://example.org/order/42> ex:total ?t`), reversed
  through the subject template to the key column

Every predicate is rendered **against the column's type**, learned from a
cached `SELECT * FROM … LIMIT 0` probe. A literal that cannot be compared
safely with the column — a string against a `bigint`, a naive timestamp
against a `timestamp with time zone`, a NaN, or (on `dialect: mysql`) a string
containing a backslash — is simply not pushed. The in-engine `FILTER` remains
the authority in every case, so a declined push costs I/O, never correctness.

`COUNT` over a single triples map is answered by an exact
`SELECT COUNT(*) … WHERE <key columns> IS NOT NULL` — exact where the Iceberg
source can only use manifest statistics. Note the trade: this is a real query
against the endpoint, so cardinality on a large table costs an aggregate scan,
where an Iceberg source answers from metadata. Most engines optimize
`COUNT(*)`, but plan for it if a query shape asks for cardinality repeatedly.

**Not pushed:** `ORDER BY … LIMIT`. A NULL in a key or required column would
consume `LIMIT` slots for rows the mapping drops, so the engine's own sort
runs over the full scan. Joins between triples maps on the same source are
also performed in the engine (a whole-query SQL rewrite in the Ontop style is
a possible later optimization, not a v1 requirement).

### Types

Trino's column types map onto Fluree's tabular types; the R2RML datatype
rules then apply as for any source.

| SQL / Trino type | Fluree column | RDF datatype (default) |
|------------------|---------------|------------------------|
| `boolean` | Boolean | `xsd:boolean` |
| `tinyint`, `smallint`, `integer` | Int32 | `xsd:integer` |
| `bigint` | Int64 | `xsd:integer` |
| `real` | Float32 | `xsd:float` |
| `double` | Float64 | `xsd:double` |
| `decimal(p,s)` | Decimal | `xsd:decimal` (exact) |
| `varchar`, `char`, `json`, `uuid`, … | String | `xsd:string` |
| `varbinary` | Bytes | `xsd:base64Binary` |
| `date` | Date | `xsd:date` |
| `timestamp(p)` | Timestamp | `xsd:dateTime` |
| `timestamp(p) with time zone` | TimestampTz | `xsd:dateTime` (UTC) |
| `array`, `map`, `row` | String (Trino's JSON rendering) | `xsd:string` |

Zoned timestamps are selected `AT TIME ZONE 'UTC'` on the Trino dialect, so a
value stored in a named region never has to be decoded client-side. Fractional
seconds beyond microseconds are truncated.

## Freshness and materialization

A SQL source has no snapshot: every query reads the tables as they are at that
moment. Consequently

- `as-of` time travel is not available on a SQL source;
- [materialization](iceberg.md#materialization) into a twin ledger and
  `fluree iceberg track` are **not yet supported** for SQL sources — both are
  built on Iceberg snapshot windows, and a mutable table has no delta between
  two reads. Attempting either returns a clear error naming the source. A
  full-rebuild materialization is a planned follow-up.

## Security

- The endpoint is admin-configured, never query-supplied. The server route
  requires the admin token like `/iceberg/map`.
- Outbound requests follow no redirects and refuse the link-local /
  cloud-metadata range (`169.254.0.0/16`, `fe80::/10`) both up front and at
  DNS resolution. Loopback and private hosts are **allowed** — a sidecar or a
  Trino on the same network is the normal deployment — which is the same
  posture as the Iceberg S3 `endpoint` override.
- Filter literals are rendered with proper quoting and typed against the
  probed schema; identifiers are quoted per dialect. `rr:sqlQuery` text is
  the mapping author's, not a query author's.
- String literals are escaped by the standard-SQL rule — `''` is an escaped
  quote, and a backslash is an ordinary character. That holds on Trino,
  Postgres and SQLite. MySQL's default `sql_mode` treats a backslash as live
  inside a literal, so on `dialect: mysql` a value containing one is **not
  pushed down** at all; the in-engine `FILTER` applies it instead. The bridge
  additionally pins the rule on the sessions it opens — `NO_BACKSLASH_ESCAPES`
  on MySQL, `standard_conforming_strings = on` on Postgres (already the
  default there, set explicitly so a server-, database- or role-level override
  cannot change it). If you point a source at some other Trino-protocol
  endpoint, ensure the equivalent holds there.
- Credentials can be indirected (`{"env_var": "TRINO_TOKEN"}` or
  `{"secret_ref": "…"}`) rather than stored inline — but only in a
  graph-source record whose config JSON is authored directly. Both
  `POST /v1/fluree/sql/map` and `fluree sql map` store what they are given as
  a literal, so a secret supplied to either lives at rest in the record, which
  should be protected accordingly. This matches how the Iceberg REST catalog
  registers; accepting a `secret_ref` through those paths is a follow-up.

## Running the bridge

`fluree-sql-bridge` is a separate small binary (not part of `fluree`) that
exposes a Postgres, MySQL or SQLite database through the Trino client
protocol. Run it next to the database:

```bash
fluree-sql-bridge --listen 0.0.0.0:8080 --database postgres://app:secret@db:5432/crm
```

then register the source with the engine's dialect:

```bash
fluree sql map crm --endpoint http://bridge:8080 --dialect postgres --schema public --r2rml crm.ttl
```

The bridge holds the connection pool; Fluree holds nothing. It answers
`POST /v1/statement` with the same paged JSON Trino returns, reporting column
types in Trino's names, so everything on this page applies unchanged.

## Comparison with Iceberg sources

| | Iceberg source | SQL source |
|-|----------------|------------|
| Reads | Parquet files directly (S3/GCS/local) | SQL through an endpoint |
| Filters | file/row-group pruning by min/max stats | exact `WHERE` |
| `COUNT` | manifest stats, when provably exact | exact `COUNT(*)` |
| `ORDER BY … LIMIT` | top-k file ordering | not pushed |
| Snapshots / time travel | pinned per query, incremental twins | none; full rebuilds |
| `rr:sqlQuery` | refused | supported |
| Extra infrastructure | none | Trino, or a bridge sidecar |

## See also

- [R2RML mappings](r2rml.md)
- [`fluree sql` CLI](../cli/sql.md)
- [`POST /sql/map`](../api/endpoints.md#post-api_base_urlsqlmap)
- [Iceberg / Parquet sources](iceberg.md)
