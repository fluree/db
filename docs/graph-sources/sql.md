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
| `model` | — | Model ledger (`name:branch`) whose default graph supplies view policies and the class/property hierarchy; must exist. See [Access policy](iceberg.md#access-policy) |
| `default_allow` | — | Fallback for governed requests that match no policy; `true` keeps the source readable under authentication without a model |

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

### The pushdown lane: one statement per block

A `GRAPH <source> { … }` block (and the whole `WHERE` clause of a query run
against the source) is compiled to **one SQL statement** when its shape
allows: triple patterns grouped by subject are one table access each, joins
between them are on the mapping's `rr:joinCondition` columns or on the
columns of an identical IRI template — never on a rendered IRI string — and
every required column is `IS NOT NULL`, which is also what makes `LIMIT`
pushable. In that statement:

- an `OPTIONAL` member of the same entity is a nullable column of the same
  access (no join); an optional entity hanging off a foreign key is a
  `LEFT JOIN`;
- a `FILTER` is pushed only when it is exact — the column's RDF datatype
  (from `rr:datatype`; `xsd:string` when un-annotated), the literal's type
  and the **probed SQL type** agree, and for strings the dialect compares
  bytes. A numeric literal against a text column the mapping reads as a
  number is not exact and stays in the engine as a residual filter over the
  returned rows; an `xsd:dateTime` literal (always a UTC instant) against a
  `timestamp with time zone` column pushes as a zoned `TIMESTAMP` literal;
- a filter the statement can only approximate is still pushed as a
  **widening** predicate, with the exact filter kept in the engine over the
  rows that come back: `STRSTARTS`, `STRENDS` and `CONTAINS` of a constant
  against a string column, and a `REGEX` anchored on a literal prefix with
  no flags, push a `LIKE` (a collation can only match more strings than a
  byte prefix, never fewer). An `xsd:dateTime` literal compared (`=`, `<`,
  `<=`, `>`, `>=`) with a `timestamp` column that has **no zone** pushes a
  window of ±14 hours around the literal, rendered as a naive `TIMESTAMP`
  so the database converts nothing: whatever zone the column was written
  in, its values lie within that span of the instant they denote (UTC-12 to
  UTC+14), so the window keeps every row the exact comparison can and the
  engine, which reads the column as UTC, applies the exact one. On SQLite,
  where a timestamp is text, the bounds are whole days (`>= '2024-01-09'`),
  which order correctly with either time separator. Inside a conjunction
  the parts that cannot be widened are simply dropped from the pushed
  predicate. A widened filter is a residual, so a `LIMIT` above it stays in
  the engine and a grouped query over it declines;
- a **sub-`SELECT`** inside the block is a derived table joined on its
  projected variables' key columns: its own block is lowered like the
  enclosing one, `GROUP BY` with `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` becomes
  the derived table's grouping (the engine decodes the aggregate outputs
  as the grouped lane does), `DISTINCT` and `ORDER BY … LIMIT` push inside
  it. A sub-select with `HAVING`, `OFFSET`, a `BIND`, a nested sub-select,
  a filter the statement cannot evaluate exactly, or one that hides an
  inner variable the enclosing block also uses is not admitted. There is
  no other lane for a sub-select over a graph source (the engine's
  subquery operator has no native index to run against), so one the lane
  does not take still refuses the query, as it did before;
- an entity whose members come from **several triples maps sharing its
  subject** (a vertically partitioned mapping: one map per column group, or
  per table, over the same `rr:template`) is one access per distinct table,
  joined on the subject's key columns; maps over the same table and subject
  share a single access;
- an entity with **several resolutions** — a predicate two maps mint, on
  the same subject or on different ones (`?s ex:name ?n` where people and
  companies both have names) — is one derived table: every resolution
  (one choice of providing map per member, the chosen maps minting the same
  subject) is lowered on its own and the branches are `UNION ALL`ed under
  shared columns, each row tagged with its branch so its terms decode
  through that branch's maps. The rest of the block joins the union once.
  Like the per-scan lane, a triple two maps mint comes back once per map.
  Filters and a top-k on a union variable push on the union's columns; a
  variable keeps its key shape (so it can be seeded or joined) only where
  every branch agrees on it. The branches must bind their columns with the
  same database types, an entity with more than eight resolutions, a
  foreign key into a union entity, an aggregate over one, and a union
  inside a sub-select decline;
- the statement has limits the lane respects: outer bindings above the
  provider's key-set cap (2000 rows, or half the statement budget) go out
  as several statements, one per chunk; a `VALUES` block or an `IN` list
  inside the block above that cap is not pushed (the block still runs on
  the lane, the `VALUES` in the engine and the `IN` as a residual); a
  `UNION` expanding to more than eight branch combinations declines;
- a constant subject or object IRI is reversed through its template into
  key predicates; a key that cannot be a value of its column (`order/abc`
  over a `bigint`) makes the block empty without a round trip. A class a
  map derives from a column (`?p a <…/kind/staff>`) is reversed the same
  way into a predicate on that column, and a class the template cannot
  produce empties the block. (The per-scan lane cannot answer a
  column-derived class constraint yet, so that shape has no fallback);
- a `VALUES` block, and bindings the outer query already holds (a ledger
  pattern joined to the block), are sent as a `VALUES` key set so the
  source does the semi-join. Once the outer side has grown past one key
  set (2000 rows), a seeded statement per outer batch stops paying: the
  lane counts the block once (`SELECT COUNT(*)`, an index-only scan on
  most tables) and, when it holds at most 100,000 rows and no more than
  four rows per outer row seen so far, fetches it whole in one statement
  and joins every outer batch to it in memory; a larger block stays
  seeded. On a 1M-row Postgres table, 50,000 outer keys against a
  100,000-row block run 2.2x faster this way, and 5,000 keys stay seeded.
  The row cap is `FLUREE_SQL_PUSHDOWN_CACHE_ROWS` (`0` keeps every batch
  seeded);
- a `BIND` in the block keeps the block on one statement: the statement
  returns the columns the expression reads and the engine computes the
  value per row, before any residual filter (so a `FILTER` over the bound
  variable is fine). When the expression is `+`, `-` and `*` over numeric
  columns the database holds natively, numeric constants and other such
  `BIND`s, a `FILTER` comparing it with a number pushes as the expression
  (`("total" * 2) > 50`) and an `ORDER BY … LIMIT` over it pushes as a
  top-k (`ORDER BY ("total" * 2) DESC LIMIT 2`); the bound value is still
  built in the engine. Division stays in the engine (SPARQL divides
  integers into a decimal, SQL into an integer), as does anything over a
  string. The `BIND` must read only variables the block bound
  before it, and nothing the statement joins or filters on may read the
  bound variable; an `EXISTS` inside the expression, or a `BIND` inside an
  `OPTIONAL` or a `UNION` branch, leaves the block to the engine;
- `LIMIT`, and `ORDER BY … LIMIT` as a top-k, are pushed when no residual
  filter could drop rows afterwards. The top-k needs every `ORDER BY` key to
  be a typed, required column (either direction); a key the statement cannot
  order on, a subject IRI say, keeps the whole `LIMIT` in the engine, because
  k rows ordered by a prefix of the keys can be the wrong k among ties;
- a `SELECT DISTINCT` directly over the block is `SELECT DISTINCT` over the
  columns of the projected variables (plus what the join and any residual
  filter read), where the dialect's string equality is byte equality; the
  engine still deduplicates the returned terms;
- a `UNION` is one block per branch combination, each joined with the rest
  of the block and carrying its own residual filters. The branches share
  **one `UNION ALL` statement** under typed columns: a variable bound on
  columns of the same database type in several branches takes one column,
  a differently typed binding takes its own, and a branch not binding it
  projects `NULL` there (on `dialect: sqlite`, whose compound columns are
  typed from the first branch alone, such padding is not sent and the
  branches run one statement each). Each row carries its branch, so its
  terms decode through that branch's maps. `ORDER BY … LIMIT` pushes onto
  the union when every branch orders on the same required column; when a
  branch lacks the ordering variable the branches run one statement each so
  that each keeps its own `LIMIT`. Branches seeded differently by the outer
  query run one statement each; a branch that can yield nothing sends
  nothing;
- a grouped query over the block (`GROUP BY` with `COUNT`, `COUNT DISTINCT`,
  `SUM`, `AVG`, `MIN`, `MAX`; or `GROUP BY` alone, which is `SELECT
  DISTINCT`) is **one grouped statement**, with SPARQL's semantics patched
  where SQL differs: `AVG` is pushed as `SUM` and `COUNT` and divided in the
  engine (databases round a decimal average to the input's scale), an empty
  `SUM` comes back `NULL` and is reported as `0`, aggregate results take the
  datatype of the mapping's `rr:datatype`, and string keys, `COUNT DISTINCT`
  of strings and `MIN`/`MAX` of strings are pushed only where the dialect
  compares bytes. `HAVING`, `ORDER BY` and `LIMIT` run in the engine over
  the grouped rows; an `ORDER BY` over aggregates and required group keys
  with a `LIMIT` and no `HAVING` is pushed as a top-k, again only when every
  key can be ordered on. Any residual filter, a
  `SUM`/`AVG` over a column whose SQL type does not match its datatype, or
  an aggregate over an IRI template declines to the engine's grouping.

**Where dialects differ**, the lane follows SPARQL's semantics (bytes, code
points, instants) and declines rather than approximate:

- String equality — a `FILTER`, an `IN` list, a key set, a constant subject
  reversed into a string key column, or a join on string columns — compares
  **bytes** on every dialect. Trino, SQLite and a deterministic Postgres
  collation do so already; on `dialect: mysql` the renderer marks every
  string literal and one side of every string join `BINARY`, since the
  default collation there folds case. The duplicate-key probe at
  registration groups `BINARY` for the same reason.
- `SELECT DISTINCT`, `GROUP BY` and `COUNT(DISTINCT …)` over a string column
  stay in the engine on MySQL: a grouping cannot be forced binary there
  (`ONLY_FULL_GROUP_BY` rejects `GROUP BY BINARY col`), and a case-folding
  collation would merge two distinct terms.
- String `ORDER BY … LIMIT` (as a top-k) and `MIN`/`MAX` of strings are
  pushed only on Trino and SQLite, which order by code point; Postgres and
  MySQL order by a locale collation, so those run in the engine.
- An `xsd:dateTime` literal against a `timestamp with time zone` column is
  rendered as the zoned literal each dialect honors — `TIMESTAMP '… UTC'`
  on Trino, `TIMESTAMP WITH TIME ZONE '… UTC'` on Postgres (a plain
  `TIMESTAMP` literal there silently drops the zone and is read in the
  session's zone), `TIMESTAMP '…+00:00'` on MySQL. A naive `timestamp`
  column is taken as UTC when its term is built, on every dialect, and a
  filter against it pushes the ±14h widening window described above.
- A decimal's lexical form follows the scale the endpoint reports for the
  column (`decimal(10,2)` gives `99.50`); the bridge reports NUMERIC /
  DECIMAL columns at the scale it was started with (`--decimal-scale`,
  default 6), and SQLite's `NUMERIC` is a double.

CI replays every lane case against SQLite, Postgres 16 and MySQL 8 behind
the bridge, the two servers deliberately running five hours off UTC, and
pins both the rows and which of these forms each statement took.

Terms are always built in the engine from the returned columns, so
datatypes come from the mapping, not from the SQL types. Shapes the lane
cannot express exactly — variable predicates, an entity with more
resolutions than the union cap or with branches of differing column types,
disconnected entities (a Cartesian product), a filter inside an optional
that is not exact, a view policy targeting both subjects and a
column-derived class — decline to the per-scan lane below, which is also
the differential oracle in the test
suite. `FLUREE_SQL_PUSHDOWN_LANE=0` disables the lane. The statement sent
is logged at `info` as `SQL block pushdown`, and a tracked query (`"meta":
true`) returns every statement the lane ran under `sql` as
`[{"source", "sql"}]` — see [Tracking and
Fuel](../query/tracking-and-fuel.md#tracked-information).

### Subject keys must be unique

Both lanes assume the columns of a subject template identify one row: a
star over a subject reads its columns from one row. R2RML itself does not
require that — the output graph is a set — so a subject minted from a
non-key column, a joined `rr:sqlQuery` view or a denormalized fact table
would return duplicate rows. Registration therefore probes every table
(`SELECT 1 … GROUP BY <subject key columns> HAVING COUNT(*) > 1 LIMIT 1`,
also over the parent columns of every foreign key pointing at the map) and
reports repeats as `mapping_warnings`; the finding is stored on the source.
The pushdown lane **refuses** a statement over a flagged table with an
error naming it. Register with `allow_duplicate_subjects` to accept the
duplicate rows instead, and run `fluree sql check <source>` to re-probe
live tables. An unreachable endpoint skips the probe with a warning.

**Not pushed** in the per-scan lane: `ORDER BY … LIMIT` (a NULL in a key or
required column would consume `LIMIT` slots for rows the mapping drops) and
joins between triples maps, which run in the engine.

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
  cannot change it). Its MySQL sessions also run with `time_zone = '+00:00'`
  (the driver's default, pinned by a test), so a `TIMESTAMP` column reads back
  as the instant it stores rather than the server's wall-clock time. If you
  point a source at some other Trino-protocol
  endpoint, ensure the equivalent holds there.
- Credentials can be indirected (`{"env_var": "TRINO_TOKEN"}` or
  `{"secret_ref": "…"}`) rather than stored inline — but only in a
  graph-source record whose config JSON is authored directly. Both
  `POST /v1/fluree/sql/map` and `fluree sql map` store what they are given as
  a literal, so a secret supplied to either lives at rest in the record, which
  should be protected accordingly. This matches how the Iceberg REST catalog
  registers; accepting a `secret_ref` through those paths is a follow-up.
- View policy is enforced in the R2RML scan exactly as for an Iceberg source
  (static `f:onProperty` / `f:onClass` / `f:onSubject` targeting, subclass
  expansion and stored policies through a `--model` ledger; `f:query` fails
  closed). See [Access policy](iceberg.md#access-policy). The pushdown lane
  prunes the mapping before it builds its statement, so a hidden column is
  never selected; the per-scan lane enforces after the rows come back. An
  `f:onClass` policy over a map that derives `rdf:type` from one column
  (`rr:template "…/kind/{kind}"` or an `rr:column` IRI map) is decided per
  targeted class and pushed as a predicate on that column: rows of a denied
  class drop out (`"kind" IS NULL OR NOT ("kind" IN ('staff'))`, a row
  without a class keeping the default), or only rows of an allowed class stay
  (`"kind" IN ('guest')`). An `f:onSubject` policy is decided per targeted
  subject the same way and pushed on the subject key columns, each subject
  reversed through the subject template (`NOT ("id" IN (1, 9))`, or
  `"id" IN (2, 3)` under a deny default); a subject the template cannot
  mint adds nothing. On an optional entity either predicate joins as a
  condition, so a hidden row leaves the optional variables unbound. A
  subject policy beside a class policy over a column-derived type, a map
  deriving classes from several columns or maps, and a policy on an
  `OPTIONAL` member of the entity it hides still leave the block to the
  per-scan lane.

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

Over SQLite, a table column is typed by its **declared** type under SQLite's
own affinity rules (`NUMERIC`, `DECIMAL(10,2)` and any other numeric-affinity
name report as `double`; `INT…` as `bigint`; `…CHAR`, `…TEXT`, `…CLOB` as
`varchar`), and every cell is converted to that type. SQLite stores each cell
in its own storage class, so a `NUMERIC` column can hold `5.00` as an integer
next to `99.50` as a real; typing by declaration keeps both as `5.0` and
`99.5` rather than letting the first row decide. An expression column
(`SUM(total)`) has no declared type and takes the driver's inference.

## Comparison with Iceberg sources

| | Iceberg source | SQL source |
|-|----------------|------------|
| Reads | Parquet files directly (S3/GCS/local) | SQL through an endpoint |
| Filters | file/row-group pruning by min/max stats | exact `WHERE` |
| Joins, OPTIONAL, VALUES, outer bindings | in the engine | one statement per block (pushdown lane) |
| `UNION`, `DISTINCT` | in the engine | one `UNION ALL` statement for the branches; `SELECT DISTINCT` (pushdown lane) |
| `COUNT` | manifest stats, when provably exact | exact `COUNT(*)` |
| `ORDER BY … LIMIT` | top-k file ordering | pushed by the pushdown lane on typed required columns |
| Snapshots / time travel | pinned per query, incremental twins | none; full rebuilds |
| `rr:sqlQuery` | refused | supported |
| View policy | static targeting in the scan, `--model` ledger | same |
| Extra infrastructure | none | Trino, or a bridge sidecar |

## See also

- [R2RML mappings](r2rml.md)
- [`fluree sql` CLI](../cli/sql.md)
- [`POST /sql/map`](../api/endpoints.md#post-api_base_urlsqlmap)
- [Iceberg / Parquet sources](iceberg.md)
