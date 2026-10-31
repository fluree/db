# Standards and Feature Flags

This document covers Fluree's compliance with standards and feature flags.

## Standards Compliance

### RDF 1.1

**Status:** Fully compliant

Fluree implements the W3C RDF 1.1 specification:
- RDF triples (subject-predicate-object)
- IRI identifiers
- Typed literals
- Language tags
- Blank nodes
- RDF datasets

**Specification:** https://www.w3.org/TR/rdf11-concepts/

### RDF 1.2

**Status:** Partial support (edge annotations / reification)

Fluree implements the RDF 1.2 reification model used for edge annotations:
- `rdf:reifies` with triple terms (`<<( s p o )>>`) as the reified object
- Reifiers identified by IRI, blank node, or variable
- Triple terms as values under any predicate, on every write surface

Fluree also exposes a non-standard extension that reads commit metadata off a
quoted triple (`<< s p o >> f:t ?t`, `f:op ?op`) for transaction-time and
assert/retract introspection.

Turtle 1.2 annotation syntax is accepted on ingest — `{| ... |}` annotation
tails, the `~` reifier, `<< s p o >>` reified triples and
`r rdf:reifies <<( s p o )>>` — on every Turtle write path (insert, upsert,
import, graph sync over the CLI or `/sync`), inside TriG `GRAPH { }` blocks, and in N-Triples and
N-Quads files. As in RDF 1.2, only the annotation syntax asserts the triple;
`<< s p o >>` and `rdf:reifies <<( s p o )>>` reify it without asserting it.
The `VERSION "1.2"` / `@version`
directive and `--ltr` / `--rtl` base-direction language tags are accepted.
The vendored W3C RDF 1.1 and RDF 1.2 Turtle, N-Triples, N-Quads and TriG
suites run in CI (`testsuite-sparql/tests/w3c_rdf.rs`), with known gaps in the
skip register.

Not yet supported:
- Annotation-of-annotation (a `{| ... |}` or `<< ... >>` inside an annotation body)

See [Edge annotations](../concepts/edge-annotations.md).

Base-direction language tags (`"text"@en--ltr`) parse and round-trip, but are
stored as a single `rdf:langString` whose language is the whole `en--ltr`
string, rather than as `rdf:dirLangString` with a separate direction. Nothing
is dropped, but `LANG()` returns `en--ltr`, JSON-LD output carries that in
`@language`, and `langMatches(?l, "en")` will not match it. First-class
`rdf:dirLangString` is a future extension.

**Specification:** https://www.w3.org/TR/rdf12-concepts/

### JSON-LD 1.1

**Status:** Compliant, with the deviations listed below

Fluree supports JSON-LD 1.1:
- @context for namespace mappings
- @id for resource identification
- @type for type specification
- @graph for multiple entities, and named graph objects (an object whose `@id` names the graph and whose `@graph` holds its nodes)
- @reverse, @included and @nest
- @value and @type for literals
- @language for language tags
- Nested objects
- Arrays
- @json literals, stored in the canonical form the Object-to-RDF transformation requires (RFC 8785). Integers past 2^53 are kept exact rather than rounded through a double

**Bulk import and transactions read some JSON-LD differently.** [Bulk import](../cli/create.md) and [`rdf::parse`](rdf-documents.md) keep an integer too large for 64 bits exact as `xsd:integer` and read a blank-node `@type` (`"@type": "_:t"`) as a blank node; a JSON-LD transaction (insert, upsert) stores such an integer as `xsd:double` and reads `_:t` as an IRI. Bulk import refuses a node with its own `@graph`, whether it names a graph or selects one; load named graphs as TriG or N-Quads.

Fluree extension: a node's own `"@graph": "<graph IRI>"` writes the node, and every node nested in it, to that graph (`"default"` and `"config"` name the default graph and the ledger's config graph). `@index` is accepted and ignored.

Deviations from the JSON-LD 1.1 expansion algorithm:
- Other `@`-keywords on a node object are refused, by a transaction and by a bulk import alike, where the algorithm drops them with a warning. A refusal names the key, so the document can drop it or rename it.
- A type-scoped context also applies to the node objects nested in the typed node (the algorithm's default is `@propagate: false`), and a node with several types applies their scoped contexts in the order written rather than in lexicographic order.
- A term defined only in a property-scoped context, used outside it, is kept as a bare key rather than dropped.

**Specification:** https://www.w3.org/TR/json-ld11/

### SPARQL 1.1 Query

**Status:** In progress toward full compliance

Supported SPARQL features:
- SELECT queries
- CONSTRUCT queries
- ASK queries
- DESCRIBE queries
- FROM and FROM NAMED clauses
- GRAPH patterns
- OPTIONAL patterns
- UNION patterns
- FILTER expressions
- BIND expressions
- Aggregations (COUNT, SUM, AVG, MIN, MAX, SAMPLE, GROUP_CONCAT) with DISTINCT modifier
- GROUP BY (variables and expressions, including bare built-in calls like `GROUP BY DATATYPE(?v)`)
- ORDER BY
- LIMIT and OFFSET
- Subqueries (evaluated independently — an inner `ORDER BY`/`LIMIT`/`OFFSET` scopes the sub-SELECT before it joins the enclosing pattern, per SPARQL 1.1 §18.2; there are no correlated/LATERAL sub-SELECTs)
- Blank-node property lists (`[ :p ?o ]`) in subject and object position
- Property paths (`+`, `*`, `?`, `^`, `|`, `/`, `!` negated sets, and transitive over a sequence including inverse steps `(^a/b)+`; see [SPARQL docs](../query/sparql.md#property-paths))
- MINUS, VALUES, and FILTER EXISTS / NOT EXISTS
- SERVICE against local ledgers (`fluree:ledger:<name>`); remote HTTP endpoints are not supported
- Result formats: SELECT/ASK as SPARQL-results JSON or XML, CSV or TSV; CONSTRUCT/DESCRIBE as JSON-LD, Turtle, N-Triples, RDF/XML, TriG or N-Quads (see [SPARQL output negotiation](../api/endpoints.md#post-queryledger))
- `GRAPH` blocks in a CONSTRUCT template, writing into named graphs (an extension, as in Apache Jena ARQ; see [CONSTRUCT](../query/construct.md#named-graphs-in-the-template))

**Aggregate result types:** COUNT and SUM of integers return `xsd:integer` (per W3C spec), not `xsd:long`. SUM of mixed types and AVG return `xsd:double`.

**Empty-group aggregates:** Over an implicit single group (no `GROUP BY`) whose pattern matches nothing, `COUNT`, `SUM`, and `AVG` return their identity `"0"^^xsd:integer` (SPARQL 1.1 §18.5.1); `MIN`, `MAX`, and `SAMPLE` have no identity element and return unbound. A query *with* `GROUP BY` over an empty pattern returns zero rows.

**Grouped queries:** A query groups when it has a `GROUP BY` or an aggregate anywhere in SELECT, HAVING or ORDER BY (§18.2.4.1). Its SELECT expressions are evaluated once per group, in SELECT order (§18.2.4.4), after HAVING unless HAVING reads them (below), and `SELECT *` projects its GROUP BY keys. In HAVING and ORDER BY, a variable that is neither a key nor aggregated means `SAMPLE(?v)` (§18.2.4.1); HAVING without grouping filters the solutions (§18.2.4.2). ASK and CONSTRUCT group the same way: ASK is true when some group passes HAVING, and CONSTRUCT instantiates its template once per group, where a template variable that is not a GROUP BY key is unbound and the triples that read it are skipped (DESCRIBE refuses GROUP BY and HAVING). Two shapes are rejected rather than evaluated literally: a projected variable that is neither a GROUP BY key nor aggregated (§11.4, including when the only aggregate is in HAVING or ORDER BY), and an aggregate over an alias of the same SELECT clause, which the spec would evaluate as unbound (`COUNT` = 0 for every group).

**HAVING reads SELECT aliases (extension of §18.2.4):** In a grouped query, HAVING can read an alias of the same SELECT clause, whether the alias names an aggregate (`(COUNT(?e) AS ?n) … HAVING (?n > 1)`) or an expression (`(COUNT(?e) + 0 AS ?n)`, `(IF(?a = "Net", "network", "other") AS ?seg) … HAVING (?seg = "network")`). The expressions HAVING reads, and the aliases they read in turn, are evaluated before HAVING; the others after it. Each is evaluated once per group either way, so HAVING tests the value the query returns, even for `RAND()` or `STRUUID()`. SPARQL 1.1 evaluates every SELECT expression after HAVING, so there such an alias is unbound and the HAVING condition fails. Many SPARQL engines make the same extension, and the W3C aggregate tests always repeat the aggregate in HAVING, so they pin neither reading. For a query that behaves the same everywhere, repeat the aggregate or expression in HAVING instead of reading its alias. JSON-LD `having` and Cypher `WITH … WHERE` follow the same rule.

**Trailing VALUES in a grouped query (deviation from §18.2.4.3):** A `VALUES` block after the WHERE clause, at the top level of any query form or in a sub-SELECT, is joined with the WHERE solutions before grouping, so it restricts what the aggregates count: `SELECT ?a (COUNT(?e) AS ?n) WHERE { ?e ex:area ?a } GROUP BY ?a VALUES ?e { ex:e1 }` counts `ex:e1` alone. The spec joins it after HAVING instead, pairing each group row with each VALUES row. The expressions the level evaluates before grouping (an aggregate's input expression, a `GROUP BY` expression, and the SELECT expressions placed before grouping: all of them in a query that does not group) run after the join, so they read the VALUES variables: `SELECT (SUM(?n * ?v) AS ?s) WHERE { ?e ex:n ?n } VALUES ?v { 2 }` doubles the sum, at the top level as in a sub-SELECT. HAVING still reads a VALUES variable as unbound, as it would in the spec's order, unless the WHERE binds it or it is a GROUP BY key. For queries that behave the same on any SPARQL processor, put the `VALUES` block inside the WHERE clause to restrict the aggregates' input, or move the grouped query into a sub-SELECT and put the `VALUES` block after the outer WHERE to join it with the grouped rows.

**Expression error semantics:** A *dynamic value* error in a `SELECT`/`BIND`/`ORDER BY` expression (e.g. arithmetic on incompatible operand types) leaves that variable unbound for the solution and the query still returns the remaining rows (SPARQL 1.1 §18.5 `Extend`); the same error in a `FILTER` eliminates the solution (§17.2). *Structural* errors — a built-in called with the wrong arity, an unknown datatype IRI — describe a malformed query and are reported as a query error. (Transactions evaluate their `WHERE` clause in strict mode, so a computed value error fails the transaction rather than silently writing an unbound value.)

**Timezone offsets are not supported (deviation from §17.4.5.5-9 and from lexical round-tripping):** Every temporal value is canonicalized the moment it is parsed, and the source offset is retained nowhere — not in novelty, not in the commit log, not in the index. An `xsd:dateTime` is a UTC instant: the offset is *applied*, so `"2010-12-21T15:38:02-08:00"` is stored, compared, and rendered as `"2010-12-21T23:38:02Z"`, and a lexical form with no offset is read as UTC. An `xsd:date`, `xsd:time`, or gYear-family value carries no designator at all: the offset is validated and then *discarded, not applied* — `"2026-01-01+05:00"` is `"2026-01-01"` and `"17:00:00-06:00"` is `"17:00:00"`, because applying it would move a calendar day, and the index has only ever stored the day / the wall clock. Consequences a caller will see: `STR()` and every result format return the canonical form, not the lexical written (`…T09:00:00+01:00` reads back as `…T08:00:00Z`; `2001-01-01Z` as `2001-01-01`; fractional seconds lose trailing zeros); the calendar accessors read UTC components, so `HOURS("…T15:38:02-08:00")` is `23`; `TZ` is always `"Z"` and `TIMEZONE` always `"PT0S"`; and two lexicals that canonicalize alike are one value for `=`, `DISTINCT`, and `GROUP BY`. Six W3C tests that assert the source offset or lexical are registered as not-supported: `functions/tz-01`, `functions/timezone-01`, `functions/hours-01`, and `open-world/date-2`, `date-3`, `date-4`.

Why: before this, a parsed value kept its offset and source lexical while it sat in novelty and lost both once indexed (the binary index stores an instant for `dateTime`, and days / microseconds-since-midnight for `date` / `time`, with no room for an offset). So every consumer that read either — comparison, equality, hashing, arithmetic, the accessors, `TZ`, rendering — answered one thing before a background reindex and another after: the same query over unchanged data returning a different result, with no write behind it and no way for a caller to predict which they would get. Those six tests passed only because the conformance harness never reindexes. Preserving the offset instead does not fit: an `ObjKey` is a `u64` already spending 59 bits on a microsecond-range instant, an arena handle would destroy the inline key ordering that dateTime range pushdown depends on, and a sidecar would keep the offset for a value the caller can render in whichever zone they want. Discarding it at the door is what makes there be exactly one representation. **Ordering, comparison and range queries are unchanged for `dateTime`** — they always used the normalized instant. Applications that need a wall-clock offset should store it as its own property.

**Fractional seconds past the microsecond are not kept (deviation from XSD's arbitrary precision):** an `xsd:dateTime` or `xsd:time` keeps six fractional digits, and digits past them are dropped the moment the value is parsed, in a write and in a query alike: `"2010-12-21T15:38:02.123456789Z"` is `"2010-12-21T15:38:02.123456Z"`. So two lexicals that differ only past the microsecond are one value for `=`, `DISTINCT`, `GROUP BY` and `ORDER BY`, and `SECONDS` returns at most six fractional digits. A query constant is held to the microsecond like every value, so it compares as the value written with it, for equality and for ordering alike: `FILTER(?t >= "2020-01-01T00:00:00.123456789Z"^^xsd:dateTime)` finds a value written with that lexical, `<` the same constant does not, and exactly one of `<`, `=` and `>` holds between a stored value and a constant. That holds for a typed literal and a cast in SPARQL, a Cypher `datetime()`/`time()` constant, and a constant bound first through `VALUES` or `BIND`, which all answer alike. A plain string compared with a temporal value (the JSON-LD filter form) is ordered as the temporal it reads as, held to the microsecond the same way. Exactly one of `<`, `=` and `>` holds for typed temporal operands only: a plain string is not value-equal to a temporal, as on earlier versions, so a string that orders equal to one satisfies `<=` and `>=` but need not satisfy `=`. Why: the commit log writes six digits and the index keys microseconds, so a value held with more matched itself only until the ledger was reloaded or indexed, and then stopped: a `DELETE DATA` naming it as written committed nothing after a restart, and a constant written past the microsecond never equalled a stored value. Dropping the digits where the value is read makes the value written the value stored. Applications that need more precision should store it as its own property.

**Date/time arithmetic (extension beyond §17.3):** The operator mapping table maps `-` only over numeric operands, so `?d1 - ?d2` on two temporal values is a type error under the published spec — and, per the expression-error rule above, that surfaces as an unbound variable rather than a failure. Fluree instead implements the XPath operators [SEP-0002](https://github.com/w3c/sparql-dev/blob/main/SEP/SEP-0002/sep-0002.md) specifies: the three *differences* (`dateTime`/`date`/`time` minus its own kind) yield a signed `xsd:dayTimeDuration`, and the *shifts* (`± xsd:dayTimeDuration` or `± xsd:yearMonthDuration`, plus duration ± duration within one family) yield the left operand's kind. Shifting is calendar-aware: month arithmetic clamps to the end of the month (`2026-01-31 + P1M` = `2026-02-28`), `xsd:time` wraps within its day, and `xsd:date ± dayTimeDuration` keeps only the date part. This matches SEP-0002's XPath semantics for every `xsd:dateTime` operand and for `xsd:date`/`xsd:time` operands written without an offset (see the next paragraph for the rest), but queries using it are **not portable** to a processor implementing only the published spec.

**Arithmetic over offset-carrying `xsd:date` / `xsd:time` operands (deviation from XPath):** follows from the paragraph above. XPath anchors each such operand in its own timezone; Fluree has already discarded it, so `"2026-01-01+05:00" - "2026-01-01Z"` is `PT0S` where XPath says `-PT5H`, and XPath's own `op:subtract-times` example `"17:00:00-06:00" - "08:00:00+09:00"` answers `PT9H` rather than `P1D`. `xsd:dateTime` arithmetic matches XPath exactly, since the instant is the value. Pinned across both storage lanes by `it_temporal_lane_stability`.

**Lists are read as their members (deviation from RDF's list structure):** A list written as `( … )` or `{"@list": [...]}` is stored in order under the subject and property that hold it, and SPARQL reads its members as values of that property, as JSON-LD does. Its `rdf:first`/`rdf:rest` cells are not triples a query can match, so `ex:items/rdf:rest*/rdf:first` and a `( ?x ?y )` pattern find nothing for it (W3C tests `list-2` to `list-4`). The Graph Store `GET`, `fluree export` and the JSON-LD crawl return each list whole and in order, and so do a CONSTRUCT whose template repeats the WHERE triple that read the list, and DESCRIBE. See [Lists](../query/sparql.md#lists).

**Dataset clauses (§13.2):** A `FROM` / `FROM NAMED` clause defines the query's dataset exhaustively — the default graph is the union of the `FROM` clauses, and `GRAPH ?g` ranges over exactly the `FROM NAMED` graphs. `FROM NAMED` with no `FROM` therefore gives an **empty default graph**. **Changed in 4.1.4:** the HTTP endpoints now implement this; earlier releases substituted a ledger's default graph, and separately enumerated the ledger alias as an extra named graph, which duplicated every `GRAPH ?g` solution. The embedded Rust API was already conformant, so this removes a divergence between surfaces rather than introducing one. The change covers all four HTTP query surfaces — ledger-scoped and connection-scoped `/query`, plus both streaming routes — and the JSON-LD `fromNamed` form follows the identical semantics, so byte-equivalent SPARQL and JSON-LD queries return the same result. A query with no dataset clause keeps its existing behavior. Requests that name only named graphs while also carrying patterns outside `GRAPH { ... }` / `["graph", ...]` receive an `x-fdb-warning` response header. See [Datasets and named graphs](../concepts/datasets-and-named-graphs.md#http-endpoints-and-default-graph-behavior).

**`x-fdb-warning` is a permanent advisory, not a migration aid.** It has no sunset and no opt-out. It describes a query *shape* whose §13.2 semantics are perennially surprising — naming only named graphs and then matching outside them — so it fires on every query surface wherever that shape appears, including the connection-scoped route where §13.2 was already in force before 4.1.4 and no behavior changed. It is named `warning` rather than `deprecation` deliberately: nothing it points at is going away. The response body and status are unaffected — the status is always the one the request earned on its own — so clients should treat the header as informational and must not key error handling on its presence.

**Graph selectors in dataset source objects (4.1.4):** A source object may narrow a source from a whole ledger to one named graph inside it. The selector is now accepted as either `graph` or `@graph` in **both** object forms — `fromNamed` entries and `from` / `to` source objects. Earlier releases read only `@graph` in `fromNamed` and only `graph` in `from`, and silently ignored the other spelling, so a source written with the wrong key resolved to the entire ledger and returned a wider result set with a `200` rather than an error. Queries that were relying on that silent widening will now see only the graph they named.

**W3C Compliance Testing:** Fluree runs the official W3C SPARQL test suite via the `testsuite-sparql` crate. The suite automatically discovers and runs 700+ test cases from W3C manifest files. See the [compliance test guide](../contributing/sparql-compliance.md) for details.

**Specification:** https://www.w3.org/TR/sparql11-query/

### SPARQL 1.1 Update

**Status:** Supported, except remote `LOAD`

Supported:
- INSERT DATA and DELETE DATA, including `GRAPH <iri> { ... }` blocks
- DELETE WHERE, including `GRAPH <iri>` and `GRAPH ?g` blocks
- DELETE/INSERT WHERE, including the `DELETE { } WHERE { }` and
  `INSERT { } WHERE { }` short forms, `WITH` / `USING` / `USING NAMED`, and
  `GRAPH <iri>` or `GRAPH ?g` blocks in templates
- Graph management: CLEAR and DROP (`GRAPH <iri>`, `DEFAULT`, `NAMED`, `ALL`),
  CREATE, ADD, COPY and MOVE, each with `SILENT`
- Several operations in one request, separated by `;`: they run in order, each
  seeing the previous one's changes, and commit as one transaction

Differences from the specification:
- Remote `LOAD` is not supported; `LOAD SILENT` is accepted as a no-op
- Fluree has no empty named graph: `DROP` behaves like `CLEAR`, and `CREATE`
  registers the graph without making it visible until it holds a triple, so
  `CREATE` of an existing graph is not an error
- The reserved `#config` and `#txn-meta` graphs cannot be the target of graph
  management operations
- A list is written and deleted through its members: deleting a value deletes
  every entry of a list that holds it, and a list's cells cannot be edited one
  by one ([Writing lists](../query/sparql.md#writing-lists))
- `ADD` of a list the destination already holds restates it, and so does
  `ADD` of the same list again beside another list there

See [SPARQL UPDATE](../query/sparql.md#sparql-update-restrictions). JSON-LD
transactions remain available as an alternative write surface.

**Specification:** https://www.w3.org/TR/sparql11-update/

### SPARQL 1.1 Graph Store HTTP Protocol

**Status:** Supported, indirect graph identification

`GET`, `HEAD`, `PUT`, `POST` and `DELETE` on `/v1/fluree/data/{ledger}` with
`?graph={iri}` or `?default`. `PUT` commits only the difference from the
graph's current contents. `GET` returns JSON-LD, Turtle, N-Triples or RDF/XML.
Direct graph identification (the request URL as the graph IRI)
is not supported. See [Graph Store Protocol](../api/graph-store.md).

**Specification:** https://www.w3.org/TR/sparql11-http-rdf-update/

### SPARQL Service Description

**Status:** Supported

A `GET` on `/v1/fluree/query` or `/v1/fluree/query/{ledger}` with no `query`
parameter returns a description of the endpoint in JSON-LD, Turtle, N-Triples
or RDF/XML, chosen by `Accept`. It names the endpoint, the SPARQL query
language (`sd:SPARQLQuery` at versions 1.0 through 1.2, and `sd:SPARQL11Query`
for older clients), the result formats, and simple entailment, plus
`sd:UnionDefaultGraph` on a ledger whose
[union default graph](../concepts/datasets-and-named-graphs.md#union-default-graph)
is on. See [Service description](../api/endpoints.md#service-description).

**Specification:** https://www.w3.org/TR/sparql11-service-description/ and the
SPARQL 1.2 Service Description draft

### SPARQL 1.2

**Status:** Partial support (annotations)

Supported query and update annotation syntax:
- Anonymous annotation blocks: `?s ?p ?o {| ?ap ?av |}`
- Named reifiers: `?s ?p ?o ~ ?r {| ... |}` (IRI, blank-node, or variable reifier)
- `rdf:reifies` form with `<<( s p o )>>` triple terms
- Annotations in `INSERT DATA` / `DELETE DATA`
- Annotations in `CONSTRUCT` templates (`~ ?r`, `{| ... |}`, `<< s p o >>` reified triples and
  `?r rdf:reifies <<( s p o )>>`),
  written by every result format

Also supported: triple terms as values, nested ones included, in patterns, `VALUES`,
`INSERT DATA` / `DELETE DATA` / `DELETE WHERE` and templates; value equality (`=`) on
triple terms; and the triple-term functions `TRIPLE()`, `SUBJECT()`, `PREDICATE()`,
`OBJECT()` and `isTRIPLE()`.

Not yet supported:
- Nested annotations (annotation-of-annotation)

**Specification:** https://www.w3.org/TR/sparql12-query/

### Turtle

**Status:** Fully supported

Fluree parses Turtle 1.1:
- @prefix declarations
- Base IRIs
- Abbreviated syntax (a, ;, ,)
- Literals with datatypes and language tags
- Collections
- Blank nodes

Resource limits: recursive constructs (property lists, collections, reified
triples) may nest at most 128 levels deep, and a single parse accepts at most
4 GiB of input; either limit produces a clean parse error.

**Specification:** https://www.w3.org/TR/turtle/

### JSON Web Signature (JWS)

**Status:** Partial (EdDSA only)

Supported algorithms:
- EdDSA (Ed25519) - **Only supported algorithm**

Not yet supported:
- ES256, ES384, ES512 (ECDSA)
- RS256 (RSA)
- HS256, HS384, HS512 (HMAC)

**Specification:** RFC 7515

**Note:** Requires the `credential` feature flag.

### Verifiable Credentials

**Status:** Planned (not yet implemented)

The credential module currently supports JWS verification only. Full VC support
(proof verification, JSON-LD canonicalization) is planned but not yet available.

**Specification:** https://www.w3.org/TR/vc-data-model/

### Decentralized Identifiers (DIDs)

**Status:** Partial support

Supported DID methods:
- did:key (Ed25519 keys only)

Not yet supported:
- did:web
- did:ion
- did:ethr

**Specification:** https://www.w3.org/TR/did-core/

**Note:** Requires the `credential` feature flag.

## Compile-Time Feature Flags (Cargo)

These features are controlled at compile time via Cargo:

### `fluree-db-api` Features

| Feature | Default | Description |
|---------|---------|-------------|
| `native` | Yes | File storage support |
| `aws` | No | AWS-backed storage support (S3, storage-backed nameservice). Enables `FlureeBuilder::s3()` and S3-based JSON-LD configs. |
| `credential` | No | DID/JWS/VerifiableCredential support for signed queries/transactions. Pulls in crypto dependencies (`ed25519-dalek`, `bs58`). |
| `iceberg` | No | Apache Iceberg/R2RML graph source support |
| `shacl` | No | SHACL constraint validation (requires fluree-db-transact + fluree-db-shacl). Default in server/CLI. |
| `graphql` | No | GraphQL endpoint over a schema derived from ledger data and SHACL shapes. Implies `shacl` (tier 2 reads shapes); pulls in `async-graphql`. Default in server/CLI. |
| `sql` | No | SQL graph sources (R2RML over a Trino-protocol endpoint). Implies `iceberg` |
| `delta` | No | Delta Lake graph sources (R2RML over Delta tables). Implies `iceberg` |
| `vector` | No | Embedded vector similarity search (HNSW indexes via usearch) |
| `ipfs` | No | IPFS-backed storage via Kubo HTTP RPC |
| `search-remote-client` | No | HTTP client for remote BM25 and vector search services |
| `residency` | No | Leaf-residency instrumentation for the query and index crates |
| `aws-testcontainers` | No | Opt-in LocalStack-backed S3/DynamoDB tests (auto-start via testcontainers) |
| `full` | No | Convenience bundle: `native`, `credential`, `iceberg`, `sql`, `shacl`, `ipfs`, `graphql` |

Example:
```toml
[dependencies]
fluree-db-api = { path = "../fluree-db-api", features = ["native", "credential"] }
```

### `fluree-db-server` Features

| Feature | Default | Description |
|---------|---------|-------------|
| `native` | Yes | File storage support (forwards to `fluree-db-api/native`) |
| `credential` | Yes | Signed request verification (forwards to `fluree-db-api/credential`) |
| `shacl` | Yes | SHACL constraint validation (forwards to `fluree-db-api/shacl`) |
| `iceberg` | Yes | Apache Iceberg/R2RML graph source support (forwards to `fluree-db-api/iceberg`) |
| `sql` | Yes | SQL graph sources over a Trino-protocol endpoint (forwards to `fluree-db-api/sql`) |
| `delta` | Yes | Delta Lake graph sources (forwards to `fluree-db-api/delta`) |
| `graphql` | Yes | GraphQL endpoints (forwards to `fluree-db-api/graphql`) |
| `bolt` | Yes | Bolt protocol listener for Neo4j drivers; binds only when `bolt_listen_addr` is configured |
| `aws` | No | AWS S3 storage + DynamoDB nameservice (forwards to `fluree-db-api/aws`) |
| `oidc` | No | OIDC JWT verification via JWKS (RS256 tokens from external IdPs) |
| `raft` | No | Raft-replicated clusters (see [Raft clusters](../operations/raft-clusters.md)) |
| `mimalloc` | No | mimalloc as the global allocator |
| `swagger-ui` | No | Swagger UI endpoint |
| `otel` | No | OpenTelemetry tracing |

To build the server without credential support (faster compile):
```bash
cargo build -p fluree-db-server --no-default-features --features native
```

## Runtime Behavior

Reasoning, SPARQL property paths, and GeoSPARQL functions are always
available in any build that links the corresponding crate features (see
the build-time feature tables above). They are not gated behind a runtime
flag.

Reasoning is opted into per query (via the `reasoning` parameter or the
SPARQL `PRAGMA reasoning` directive) or per ledger (via
`f:reasoningDefaults` in the ledger configuration graph). See
[Query-time reasoning](../query/reasoning.md) and
[Setting groups](../ledger-config/setting-groups.md).

## API Versioning

Current API version: v1. Every HTTP endpoint is under `/v1/fluree/`.

### Behavior changes

Changes within v1 that a client may need to account for.

#### Writing schema triples is checked by modify policy

Schema triples (`rdfs:subClassOf`, `rdfs:subPropertyOf`, `rdfs:domain`, `rdfs:range`, and `rdf:type` declaring a class or property) used to be exempt from `f:modify` policy, so any identity able to transact could write or retract them. A policy on a class or property also covers its subclasses and subproperties, so writing the hierarchy changes what policies allow. Schema triples are now checked like any other flake. An identity with `f:defaultAllow` or no policy is unaffected. A restricted identity that maintains the ontology needs modify permission for those triples: an `f:onProperty` policy on the four schema properties, and one on `rdf:type` whose condition admits only class and property types ([Schema writes](../security/policy-in-transactions.md#schema-writes) has the pair). This includes `CLEAR` and `DROP` of a graph holding schema triples. Reading the schema is unchanged: view policy still lets every schema triple through. See [Schema writes](../security/policy-in-transactions.md#schema-writes).

#### SHACL is enforced only where the ledger config enables it (breaking)

Shapes no longer enable SHACL on their own. A ledger that holds shapes but has no config setting `f:shaclEnabled true` used to validate every write against them in reject mode; it now validates nothing until its config enables SHACL, ledger-wide or per graph. Ledgers whose config already enables SHACL are unchanged, and so are configs without a SHACL group, which were already unenforced. The same applies to shapes published through GraphQL or `fluree model entity define`, and to branch merge, rebase, revert and push. In a GraphQL schema, `sh:minCount ≥ 1` makes a field non-null only where the config enables SHACL; elsewhere the field is nullable.

Ledger info reports an affected ledger as `shapes present; SHACL enforcement not configured` (`configDiagnostics`; `fluree info` prints it), and the ledger logs the same as a warning once each time it loads. A shape counts whether it is typed (`sh:NodeShape`, `sh:PropertyShape`) or only has a target (`sh:targetClass`, `sh:targetNode`, `sh:targetSubjectsOf`, `sh:targetObjectsOf`); a config that sets `f:shaclEnabled false` is not reported. To keep enforcing, enable SHACL in the config. For a ledger that holds shapes and no config, that is one `INSERT DATA` writing a config with `f:shaclEnabled true`; a ledger that already has a config attaches the setting to its existing config subject: [Enabling SHACL](../ledger-config/writing-config.md#enabling-shacl).

Inline shapes sent with a transaction (`opts.shapes`) are now governed by the SHACL group's `f:overrideControl` rather than by `f:shaclEnabled`: they apply without config, and under `f:OverrideAll` even where `f:shaclEnabled false`; under `f:OverrideNone` (or `f:IdentityRestricted` without a matching verified identity) the transaction is refused with a 400. They validate only themselves. They are not supported in a policy-scoped request (one with an identity or policy inputs, or on a ledger whose config sets policy defaults, ledger-wide or for a graph, other than an unrestricted `f:defaultAllow true`), which is refused with a 400; support is a follow-up. The same rule applies to inline uniqueness constraints (`opts.uniqueProperties`). See [Inline shapes](../guides/cookbook-shacl.md#inline-shapes-per-transaction). A transaction body's `opts.shapes` and `opts.uniqueProperties` are now read by the embedded API and by the CLI in local mode as well as by the server (they were ignored there), a malformed value is refused, and a build without SHACL support refuses inline shapes. Inline shapes, a requested validation mode and inline uniqueness constraints passed programmatically (`TxnOpts`) now apply to a plain Turtle insert as they did to TriG (a plain Turtle document ignored them).

#### Branch operations are validated like transactions, fast-forwards included

A fast-forward merge used to adopt the source's commits without validation. Into a target whose config enables SHACL or uniqueness (ledger-wide or for any graph), it is now validated against the target's config as it was before the merge, as a transaction writing the adopted change would be, and refused on a violation; `fluree branch diff` and `GET /merge-preview` report its `validation`. Merge, rebase and revert now enforce uniqueness constraints as well as SHACL, and a plain Turtle insert enforces uniqueness like the other write paths.

Merges (fast-forwards included), rebases and reverts also re-check the target's config groups, and refuse, as a conflict with the target's state (HTTP 409), a result that would leave a setting group's fields outside the config graph or a single-valued setting with two values. A branch that carries a split config from before these checks is refused until the config is repaired ([Repairing a config split across graphs](../ledger-config/writing-config.md#repairing-a-config-split-across-graphs)). A fast-forward into a target that enables neither SHACL nor uniqueness, whose commits carry no config settings, is unchanged.

A merge or rebase that would carry what a branch wrote to its own config graph (`urn:fluree:mydb:dev#config`) or `#txn-meta` graph into another branch is refused (HTTP 409). Such writes configure only the branch that made them; on the target they would land in an ordinary graph. Merging from a branch the target was created from is not affected. Likewise, a merge (fast-forward or not) that would change the target's own config or `#txn-meta` graph with what another branch wrote there, where that graph is an ordinary one, is refused (HTTP 409): change a branch's config on that branch.

Push validates each named graph a pushed commit writes under that graph's own config. A pushed commit that writes a graph which neither the receiving ledger nor the pushed commits register is refused (HTTP 422).

#### Config writes are checked, and config-only writes are never blocked by validation

A transaction that writes a setting group into the config graph with its fields in another graph (in that transaction or split across two), types an `f:LedgerConfig` / `f:GraphConfig` outside the config graph, writes config fields outside it on the recommended config subject `urn:fluree:{ledger_id}:config:ledger` (on a ledger with no config too), or leaves a single-valued setting (or the ledger's `f:LedgerConfig` subject) with two values is refused with a `Parse error` (HTTP 400). Writing the same value again is still accepted. See [What a config write is checked for](../ledger-config/writing-config.md#what-a-config-write-is-checked-for).

Each operation of a multi-operation SPARQL update, each clause of a Cypher write, and each write staged in a transaction (`Fluree::begin_transaction`) is validated against the shapes, uniqueness constraints and config in effect before the request, as a single transaction is: an earlier operation that changes the config, a shape or a constraint does not change how a later one in the same request is checked. A model ledger the config names (a cross-ledger `f:shapesSource`, say) is read at one head for the whole request: the one its first lookup found. The config checks run once, on the request's result. Explicit Bolt transactions are not requests of this kind: they commit each statement as it runs, so each statement is checked against the state before it, as separate transactions are. A write or transaction staged before another commit that declares a property unique (`f:enforceUnique`) stages again when it commits, and is checked against the declaration.

A transaction that writes only the config graph is no longer validated against SHACL shapes or uniqueness constraints, and no longer needs a shapes, schema or constraints source to be resolvable, so a config repair is never blocked by the source it repairs (a policy source still applies to config writes). Shapes that target nodes in the config graph are no longer checked. Where SHACL or uniqueness is enabled and its source cannot be resolved, data writes fail with an error naming it, and a config read failure now fails a write instead of letting it through without uniqueness checks. A failure to read the ledger config answers HTTP 500 with `err:storage/ReadFailure` on the write lanes, branch operations, push and queries (a transaction, a multi-operation update, a branch operation, push, the transaction policy context, a query); it was a 400 `Parse error` (a 422 on push, and `err:system/ConfigError` for the policy context and queries).

#### JSON-LD nested nodes are written to their enclosing node's graph

A node nested under a node with an `@graph` selector is now written to that graph, as JSON-LD nesting implies; it used to land in the default graph. A client that relied on the old placement should give the nested node `"@graph": "default"`, or move it to the top level. JSON-LD named graph objects (`{"@id": "<graph>", "@graph": [...]}`), whose content used to be silently dropped, are now stored. This fixes configs written with nested setting groups (#1979), which read as empty before: [Repairing a config split across graphs](../ledger-config/writing-config.md#repairing-a-config-split-across-graphs) moves the fields an older write stranded.

A term defined in a scoped (property- or type-scoped) `@context` whose `@id` is a compact IRI over a prefix only the outer context defines (`"a": {"@id": "ex:a", "@context": {"b": {"@id": "ex:b"}}}`) now expands to the full IRI. It used to stay compact, so the key was written as the string `ex:b`, an IRI with the scheme `ex`. Data already written that way stays as it is.

Related JSON-LD changes: graph names must be absolute IRIs (relative ones are refused, and a graph name no longer expands through `@vocab`); `"config"` names the ledger's config graph; a graph variable in an update template writes to the graph the WHERE clause binds; a node-level `@graph` in a `where` clause is a `GRAPH` pattern, its graph name resolved as in insert and delete templates, and in a CONSTRUCT template, the `"construct": true` shorthand included, it names the graph the node's triples are written to (it was read as a property named `@graph`, and the triples went to the default graph); `@reverse`, `@included` and `@nest` are supported and other unknown `@`-keywords on a node are refused; blank node labels are scoped to the whole document; a single object with a string `@graph` has no transaction metadata; an update's `fromNamed` list that gives one name to two graphs is refused (an alias spelled `urn:default` that names another graph among them, since that name means the default graph); `"default"` in an update's `from` list reads the default graph beside the other graphs listed (it was dropped); and `opts.unionDefaultGraph` is refused on an update, as a SPARQL update refuses the `union-default-graph` pragma. Bulk JSON-LD import reads all of this as a transaction does (see below).

#### Bulk JSON-LD import writes graph-scoped data, and refuses what it cannot place

A bulk JSON-LD import (`fluree create --from`, `.jsonld` and `.jsonl`/`.ndjson`) now writes `@graph` scoping as a transaction does: a node's graph selector puts it and the nodes nested in it in that graph, and a JSON-LD named graph puts its content there, through the same graph-name table a transaction and a TriG import use. It used to refuse such documents (and before that, dropped the named graphs' content). A few documents that imported before now fail, naming what they cannot place: an unknown `@`-keyword on a node (it used to be dropped), and a graph name a transaction refuses as well. `"config"` and `"txn-meta"` stay refused, as in a TriG import. Each `.jsonl` line is read as a document of its own under the shared context.

#### Data written by an earlier release that reads differently

No stored format changes, and no migration is needed. Two kinds of earlier data behave differently after the upgrade:

- **A config with two values for one setting-group pointer** (for example two `f:shaclDefaults` or `f:policyDefaults` groups on the config subject, typically a split config plus a corrected one written beside it). The reader now uses the non-empty group whose IRI sorts lowest; earlier releases could pick an empty one. A SHACL or policy group that had no effect therefore takes effect, including a stored `f:defaultAllow false`. Ledger info reports this as `ambiguous-pointer`. Remove the group you do not want, in the config graph.
- **Edges written through a reverse-defined term** (a context term with `"@reverse"`). With `"childOf": {"@reverse": "ex:child"}`, the node `{"@id": "ex:kid", "childOf": {"@id": "ex:mom"}}` means `ex:mom ex:child ex:kid`. Earlier releases stored `ex:kid ex:child ex:mom`, as if the term were not reversed. The term is now read as JSON-LD defines it, so the same document no longer reaches an edge an earlier release stored: deleting with it does nothing. To repair a predicate that was written only through a reverse term, flip its edges once, for example `DELETE { ?s ex:child ?o } INSERT { ?o ex:child ?s } WHERE { ?s ex:child ?o }`. If it was also written in the forward direction, select only the edges the reverse term wrote.

`@annotation` on a value of a reverse property (inside `@reverse`, or under a reverse-defined term) now annotates the edge in the direction it is stored.

#### Deleting a value a list repeats deletes every entry holding it

A delete naming a value, without a list position, used to delete the first entry of a list holding it, unless its WHERE clause handed each stored entry over (`DELETE { ex:s ex:items ?o } WHERE { ex:s ex:items ?o FILTER(?o = 1) }`), which deleted every one. It now deletes every entry of a list that holds the value, and the value where it is also stored beside the list, however it is written: SPARQL `DELETE DATA`, `DELETE WHERE`, a WHERE with `VALUES`, and a JSON-LD `delete`. A value that is no list entry is deleted as before. To remove one entry of a list that repeats a value, upsert the list.

#### `fluree-track-policy` is now parsed by the server

Previously the server recognised `fluree-track-meta`, `fluree-track-fuel`, and
`fluree-track-time` but not `fluree-track-policy` — a request carrying only
that header was not treated as tracked, and the server returned the plain
untracked body. It is now parsed like its siblings, so **a request sending only
`fluree-track-policy` receives the tracked envelope** (`{"status", "result",
"policy", ...}`) where it previously received a bare result array.

Nobody could have depended on the *tracked* shape for this header, since it was
never honored. The affected case is a client that sends the header and parses a
bare array — including any `fluree` CLI older than this release talking to a
newer server, where `--track-policy` was a silent no-op. Two ways to adapt:

- Read `result` when the body is an object and the body itself when it is an
  array. This is what the CLI does, and it works against either server.
- Or stop sending the header if you do not want the tally.

`fluree-track-meta` behavior is unchanged; it has always implied policy
tracking. See [Tracking and Fuel](../query/tracking-and-fuel.md).

#### Tracked responses carry `policy_enforcement`

Tracked responses gained an optional `policy_enforcement` sibling (and an
`x-fdb-policy-enforcement` response header; on the NDJSON streaming endpoint,
an optional field on the terminal `end` record). It is additive and present
only when a non-root policy context governed the request, so clients that
ignore unknown fields are unaffected. See
[Detecting that policy was applied](../security/policy-in-queries.md#detecting-that-policy-was-applied).

## Supported Data Formats

### JSON-LD

Supported JSON-LD versions:
- JSON-LD 1.0: Yes
- JSON-LD 1.1: Yes

### SPARQL

Supported SPARQL versions:
- SPARQL 1.0: Yes
- SPARQL 1.1: Yes

### RDF Formats

| Format | Read | Write |
|--------|------|-------|
| JSON-LD | Yes | Yes |
| Turtle | Yes | Yes (export, CONSTRUCT/DESCRIBE results, Graph Store `GET`) |
| N-Triples | Yes | Yes (export, CONSTRUCT/DESCRIBE results, Graph Store `GET`) |
| N-Quads | Yes | Yes (export, CONSTRUCT/DESCRIBE results) |
| TriG | Yes | Yes (export, CONSTRUCT/DESCRIBE results) |
| RDF/XML | No | CONSTRUCT/DESCRIBE results and Graph Store `GET` only |

Import accepts `.ttl`, `.nt`, `.nq`, `.trig`, and `.jsonld`/`.jsonl` files, each
with transparent `.gz` / `.zst` decompression.

Without a ledger, `fluree.parse` / `fluree.serialize` (Python) and
`fluree_db_api::rdf` (Rust) read and write Turtle, TriG, N-Triples, N-Quads and
JSON-LD; see [RDF documents](rdf-documents.md).

## Protocol Support

### HTTP and TLS

The server speaks HTTP/1.1 over plain TCP. It does not terminate TLS or serve
HTTP/2 itself: put it behind a reverse proxy or load balancer for HTTPS and
HTTP/2.

## Client Support

Fluree works with:

**HTTP Clients:**
- curl
- Postman
- Insomnia
- Any HTTP client library

**RDF Libraries:**
- Apache Jena (Java)
- RDF4J (Java)
- rdflib (Python)
- N3.js (JavaScript)

**SPARQL Clients:**
- Apache Jena ARQ
- RDF4J SPARQLRepository
- Any SPARQL 1.1 client

## Platform Support

### Operating Systems

**Server:**
- Linux (x86_64, aarch64)
- macOS (Intel, Apple Silicon)
- Windows (x86_64)

**Clients:**
- Any OS with HTTP support

### Cloud Platforms

- AWS (native support)
- Google Cloud Platform (via file storage)
- Azure (via file storage)
- Self-hosted / on-premises

### Container Support

- Docker: Full support
- Kubernetes: Full support
- Podman: Supported
- Docker Compose: Full support

## Database Support

### Import Sources

Fluree can import from:

**RDF Databases:**
- Apache Jena TDB
- Virtuoso
- Stardog
- GraphDB
- Any RDF export

**Graph Databases:**
- Neo4j (via RDF export)
- Amazon Neptune (via RDF export)

**Relational Databases:**
- Via R2RML mapping
- Direct SQL query

### Export Formats

Export Fluree data to:
- Turtle, N-Triples, N-Quads, TriG or JSON-LD, with `fluree export` or the export endpoint
- SPARQL CONSTRUCT / DESCRIBE results as JSON-LD, Turtle, N-Triples, RDF/XML, TriG or N-Quads
- One graph at a time through the Graph Store Protocol

## Feature Roadmap

### Planned Features

**Query:**
- SPARQL property paths: nested transitive steps inside a composite repeated unit (`(a+/b)+`); `{n,m}` depth ranges
- SPARQL 1.1 Federation: remote `SERVICE` endpoints (local-ledger `SERVICE` is supported)
- Remote `LOAD` in SPARQL UPDATE
- GeoSPARQL: remaining OGC functions (only `geof:distance` is implemented today)
- RDF 1.2 / SPARQL 1.2: annotation-of-annotation

**Storage:**
- Additional cloud providers (GCP, Azure)
- Hybrid storage modes

**Security:**
- OAuth 2.0 integration
- SAML support
- Additional DID methods

**Graph Sources:**
- BigQuery integration
- Snowflake integration
- Elasticsearch integration

### Feature Discovery

Feature availability is documented in this compatibility matrix and by
crate feature flags; the standalone server does not expose a `/features`
HTTP endpoint.

## Browser Support

For web applications using Fluree API:

**Supported Browsers:**
- Chrome/Edge 90+
- Firefox 88+
- Safari 14+

**Requirements:**
- Fetch API support
- CORS support
- WebSocket support (for future streaming)

## Tool Support

### RDF Tools

Compatible with standard RDF tools:
- Protégé (ontology editor)
- TopBraid Composer
- RDF validators
- SPARQL editors

### Data Tools

Works with data engineering tools:
- Apache Airflow (via HTTP operators)
- dbt (via SQL proxy with R2RML)
- Apache Spark (via Iceberg)
- Pandas (via query API)

## Version Requirements

### Rust Version

Building from source uses the Rust toolchain pinned in `rust-toolchain.toml`
(currently 1.97.0), which `rustup` installs automatically.

### Dependencies

Runtime dependencies:
- None (statically linked binary)

Optional dependencies:
- AWS SDK (for AWS storage)

## Related Documentation

- [Glossary](glossary.md) - Term definitions
- [Crate Map](crate-map.md) - Code architecture
- [Getting Started](../getting-started/README.md) - Installation
