# Fluree

A graph database built for data that matters. Temporal, verifiable, standards-compliant.

Fluree stores data as RDF triples with complete history, integrated search, and fine-grained access control — in a single binary with no external dependencies.

Billions of triples on commodity hardware. Over 2M triples/second bulk import. [Benchmark leader](https://labs.flur.ee) across 105 W3C SPARQL queries.

[![License: BSL 1.1](https://img.shields.io/badge/license-BSL%201.1-blue)](#license)

> [!NOTE]
> **Fluree Memory** — is part of the Fluree DB CLI.
> Persistent, searchable memory for AI coding assistants. Give Claude Code, Cursor, and other AI tools long-term project memory: facts, decisions, and preferences persist across sessions in a Fluree ledger you control — scoped per-repo or per-user, shareable via git.
> [Fluree Memory docs →](https://labs.flur.ee/docs)

## Install

**Docker** — pre-configured HTTP server, ready to accept queries on port 8090. Best for trying out the API or running Fluree as a service.

```bash
docker run -p 8090:8090 fluree/server:latest
```

**Homebrew, shell installer, or Windows PowerShell** — installs the `fluree` binary that bundles both the CLI and the embedded server (`fluree server run`).

```bash
# Homebrew (macOS / Linux)
brew install fluree/tap/fluree

# Shell installer (macOS / Linux)
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/fluree/db/releases/latest/download/fluree-db-cli-installer.sh | sh
```

```powershell
# Windows (PowerShell)
irm https://github.com/fluree/db/releases/latest/download/fluree-db-cli-installer.ps1 | iex
```

Pre-built binaries and the changelog for every release are on the [GitHub Releases page](https://github.com/fluree/db/releases).

## Zero to graph in 60 seconds

```bash
fluree init
fluree create movies

fluree insert '
@prefix schema: <http://schema.org/> .
@prefix ex:     <http://example.org/> .

ex:blade-runner  a schema:Movie ;
  schema:name        "Blade Runner" ;
  schema:dateCreated "1982-06-25"^^<http://www.w3.org/2001/XMLSchema#date> ;
  schema:director    ex:ridley-scott .

ex:ridley-scott  a schema:Person ;
  schema:name "Ridley Scott" .

ex:alien  a schema:Movie ;
  schema:name        "Alien" ;
  schema:dateCreated "1979-05-25"^^<http://www.w3.org/2001/XMLSchema#date> ;
  schema:director    ex:ridley-scott .
'

fluree query --format table 'SELECT ?title ?date WHERE {
  ?movie a <http://schema.org/Movie> ;
         <http://schema.org/name> ?title ;
         <http://schema.org/dateCreated> ?date .
} ORDER BY ?date'
```

```
┌──────────────┬────────────┐
│ title        │ date       │
├──────────────┼────────────┤
│ Alien        │ 1979-05-25 │
│ Blade Runner │ 1982-06-25 │
└──────────────┴────────────┘
```

That's a SPARQL query. The same query in JSON-LD:

```bash
fluree query --jsonld '{
  "@context": { "schema": "http://schema.org/" },
  "select": ["?title", "?date"],
  "where": [
    { "@id": "?movie", "@type": "schema:Movie",
      "schema:name": "?title", "schema:dateCreated": "?date" }
  ],
  "orderBy": "?date"
}'
```

Both languages access the same engine — same features, same performance.

Now update the data and query the past:

```bash
# Give every Ridley Scott movie a genre
fluree update '
PREFIX schema: <http://schema.org/>
PREFIX ex:     <http://example.org/>
INSERT { ?movie schema:genre "sci-fi" }
WHERE  { ?movie schema:director ex:ridley-scott }
'

# What did the data look like before that update?
fluree query --at 1 'SELECT ?title ?genre WHERE {
  ?movie a <http://schema.org/Movie> ;
         <http://schema.org/name> ?title .
  OPTIONAL { ?movie <http://schema.org/genre> ?genre }
}'
# → Blade Runner (no genre), Alien (no genre)

# And now?
fluree query 'SELECT ?title ?genre WHERE {
  ?movie a <http://schema.org/Movie> ;
         <http://schema.org/name> ?title .
  OPTIONAL { ?movie <http://schema.org/genre> ?genre }
}'
# → Blade Runner "sci-fi", Alien "sci-fi"
```

Every change is preserved. Query any point in history by transaction number, ISO timestamp, or commit ID.

## What makes Fluree different

### Time travel

Every transaction is immutable. Query data as it existed at any point in time — by transaction number, ISO-8601 timestamp, or content-addressed commit ID. No special tables, no slowly-changing dimensions. It's built into the storage model.

```bash
fluree query --at 2024-06-15T00:00:00Z 'SELECT * WHERE { ?s ?p ?o }'
```

Learn more: [Time travel concepts](docs/concepts/time-travel.md), [time-travel cookbook](docs/guides/cookbook-time-travel.md).

### Integrated search

BM25 full-text search and HNSW vector similarity are built into the query engine — not bolted-on external services. Search results participate in joins, filters, and aggregations like any other graph pattern.

```json
{
  "@context": { "ex": "http://example.org/" },
  "from": "mydb:main",
  "where": [
    { "@id": "?doc", "ex:title": "?title" },
    ["bind", "?score", "(fulltext ?title \"knowledge graph\")"]
  ],
  "select": ["?doc", "?title", "?score"],
  "orderBy": [["desc", "?score"]],
  "limit": 10
}
```

For dedicated BM25 / HNSW graph sources, the same query engine drives the `f:graphSource` / `f:searchText` / `f:queryVector` patterns and can be backed by an embedded index or a remote `fluree-search-httpd` service.

Learn more: [BM25 full-text](docs/indexing-and-search/bm25.md), [vector search](docs/indexing-and-search/vector-search.md), [search cookbook](docs/guides/cookbook-search.md).

### Git-like data management

Branch, rebase, merge, push, pull — the same workflow developers already use for code, applied to data. Fork a dataset to experiment without affecting production. Merge when ready. Rebase to catch up with upstream changes. Every branch has its own independent commit history.

```bash
fluree branch create experiment
fluree use mydb:experiment
# ... make changes safely ...
fluree branch rebase experiment    # catch up with main
fluree branch merge experiment     # fast-forward merge into main
fluree branch drop experiment      # clean up
```

Learn more: [branching cookbook](docs/guides/cookbook-branching.md), [Ledgers and the nameservice](docs/concepts/ledgers-and-nameservice.md).

### Triple-level access control

Policies are data in the ledger, enforced at query and transaction time. Users see only what they're authorized to see — not rows, not tables, individual facts. No application-layer filtering required.

See [Policy enforcement](docs/concepts/policy-enforcement.md) for the model, the [policy cookbook](docs/guides/cookbook-policies.md) for worked examples, and [Policy model and inputs](docs/security/policy-model.md) for the reference.

### Reasoning and inference

RDFS subclass/subproperty reasoning, OWL 2 RL forward-chaining, and user-defined Datalog rules. The database infers facts you didn't explicitly store.

Learn more: [Reasoning and inference](docs/concepts/reasoning.md), [OWL & RDFS support reference](docs/reference/owl-rdfs-support.md), [Datalog rules](docs/query/datalog-rules.md).

### Standards-first

Full SPARQL 1.1 with zero compliance failures against the W3C test suite. Native JSON-LD for idiomatic JSON APIs. Both query languages access the same engine with the same capabilities — time travel, policies, graph sources, and all.

Learn more: [SPARQL reference](docs/query/sparql.md), [JSON-LD Query reference](docs/query/jsonld-query.md), [Standards and feature flags](docs/reference/compatibility.md).

### Also worth knowing

- **[SHACL validation](docs/guides/cookbook-shacl.md)** — declarative shape constraints enforced at transaction time, with violations reported per-target, per-property.
- **[OWL ontology imports](docs/design/ontology-imports.md)** — pull external vocabularies into a ledger via `f:schemaSource` + `owl:imports`, materialized at commit time.
- **[Apache Iceberg / R2RML](docs/graph-sources/iceberg.md)** — query Parquet warehouses and relational stores as first-class graph sources alongside native Fluree data.

## Use it your way

**CLI** — Explore data, script pipelines, manage ledgers from the terminal.
```bash
fluree query -f report.rq --format csv > output.csv
```

**HTTP Server** — Run `fluree server` for a production API with OIDC auth, content negotiation, and OpenTelemetry.
```bash
fluree server run
curl -X POST http://localhost:8090/v1/fluree/query?ledger=mydb:main \
  -H "Content-Type: application/sparql-query" \
  -d 'SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10'
```

**Rust library** — Embed Fluree directly in your application. No server process needed.
```rust
let fluree = FlureeBuilder::memory().build_memory();
fluree.create_ledger("mydb").await?;

let result = fluree.graph("mydb:main")
    .query()
    .sparql("SELECT ?s WHERE { ?s a <http://schema.org/Person> }")
    .execute()
    .await?;
```

**MCP server** — Expose Fluree to AI assistants over the Model Context Protocol.
```bash
fluree mcp serve            # stdio transport for Claude Desktop, Cursor, etc.
```

## Capabilities

| | |
|---|---|
| **Query languages** | [SPARQL 1.1](docs/query/sparql.md), [JSON-LD Query](docs/query/jsonld-query.md) |
| **Data formats** | JSON-LD, [Turtle, TriG](docs/transactions/turtle.md), N-Triples, N-Quads |
| **Time travel** | [Transaction number, ISO timestamp, commit ID](docs/concepts/time-travel.md) |
| **Full-text search** | [Integrated BM25 with Block-Max WAND](docs/indexing-and-search/bm25.md) |
| **Vector search** | [Embedded HNSW or remote service](docs/indexing-and-search/vector-search.md) |
| **Reasoning** | [RDFS, OWL 2 QL, OWL 2 RL, Datalog rules](docs/reference/owl-rdfs-support.md) |
| **Access control** | [Triple-level policy enforcement](docs/concepts/policy-enforcement.md) |
| **Geospatial** | [GeoSPARQL, S2 cell indexing](docs/indexing-and-search/geospatial.md) |
| **Verifiability** | [JWS-signed transactions, Verifiable Credentials](docs/api/signed-requests.md) |
| **Data sources** | [Apache Iceberg](docs/graph-sources/iceberg.md), [R2RML relational mappings](docs/graph-sources/r2rml.md) |
| **Storage backends** | [Memory, file, AWS S3 + DynamoDB, IPFS](docs/operations/storage.md) |
| **Replication** | [Clone, push, pull between instances](docs/operations/query-peers.md) |
| **Branching** | [Fork ledgers, independent commit histories](docs/guides/cookbook-branching.md) |
| **Observability** | [OpenTelemetry tracing, structured logging](docs/operations/telemetry.md) |
| **Validation** | [SHACL shape constraints](docs/guides/cookbook-shacl.md) |

## Documentation

For documentation and more information, visit [labs.flur.ee/docs](https://labs.flur.ee/docs).

Full documentation also lives in [`docs/`](docs/README.md):

- [Getting started](docs/getting-started/README.md) — Install, create a ledger, write and query data
- [Fluree for SQL developers](docs/getting-started/fluree-for-sql-developers.md) — Coming from relational? Start here
- [End-to-end tutorial](docs/getting-started/tutorial-end-to-end.md) — Build a knowledge base using search, time travel, branching, and policies
- [Concepts](docs/concepts/README.md) — Time travel, graph sources, policies, verifiable data
- [Guides](docs/guides/) — Practical cookbooks for [search](docs/guides/cookbook-search.md), [time travel](docs/guides/cookbook-time-travel.md), [branching](docs/guides/cookbook-branching.md), [policies](docs/guides/cookbook-policies.md), and [SHACL validation](docs/guides/cookbook-shacl.md)
- [Query languages](docs/query/README.md) — SPARQL and JSON-LD query reference
- [Transactions](docs/transactions/README.md) — Insert, upsert, update patterns
- [CLI reference](docs/cli/README.md) — All commands and options
- [HTTP API](docs/api/README.md) — Server endpoints and authentication
- [Operations](docs/operations/README.md) — Configuration, deployment, telemetry
- [Contributing](docs/contributing/README.md) — Build from source, run tests, PR workflow
- [Benchmarking](BENCHMARKING.md) — Run, understand, and add performance benchmarks

## License

Licensed under the [Business Source License 1.1](LICENSE), with a Change Date
to Apache License 2.0 as specified in that file.
