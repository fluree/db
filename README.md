# Fluree

A graph database built for data that matters. Temporal, verifiable, standards-compliant.

Fluree stores data as RDF triples with complete history, integrated search, and fine-grained access control — in a single binary with no external dependencies.

Billions of triples on commodity hardware. Over 2M triples/second bulk import. [Benchmark leader](https://labs.flur.ee) across 105 W3C SPARQL queries.

[![License: BSL 1.1](https://img.shields.io/badge/license-BSL%201.1-blue)](#license)

## Install

```bash
# Homebrew
brew install fluree

# Cargo
cargo install fluree

# Docker
docker run -p 8090:8090 fluree/server:latest
```

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

### Integrated search

BM25 full-text search and HNSW vector similarity are built into the query engine — not bolted-on external services. Search results participate in joins, filters, and aggregations like any other graph pattern.

```sparql
SELECT ?doc ?score WHERE {
  ?doc <http://fluree.com/ns/fulltext> "knowledge graph" .
  ?doc <http://fluree.com/ns/score> ?score .
} ORDER BY DESC(?score) LIMIT 10
```

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

### Triple-level access control

Policies are data in the ledger, enforced at query time. Users see only what they're authorized to see — not rows, not tables, individual facts. No application-layer filtering required.

### Reasoning and inference

RDFS subclass/subproperty reasoning, OWL 2 RL forward-chaining, and user-defined Datalog rules. The database infers facts you didn't explicitly store.

### Standards-first

Full SPARQL 1.1 with zero compliance failures against the W3C test suite. Native JSON-LD for idiomatic JSON APIs. Both query languages access the same engine with the same capabilities — time travel, policies, graph sources, and all.

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
let ledger = fluree.create_ledger("mydb").await?;
let result = ledger.query_sparql("SELECT ?s WHERE { ?s a <http://schema.org/Person> }").await?;
```

## Capabilities

| | |
|---|---|
| **Query languages** | SPARQL 1.1, JSON-LD Query |
| **Data formats** | JSON-LD, Turtle, TriG, N-Triples, N-Quads |
| **Time travel** | Transaction number, ISO timestamp, commit ID |
| **Full-text search** | Integrated BM25 with Block-Max WAND |
| **Vector search** | Embedded HNSW or remote service |
| **Reasoning** | RDFS, OWL 2 QL, OWL 2 RL, Datalog rules |
| **Access control** | Triple-level policy enforcement |
| **Geospatial** | GeoSPARQL, S2 cell indexing |
| **Verifiability** | JWS-signed transactions, Verifiable Credentials |
| **Data sources** | Apache Iceberg, R2RML relational mappings |
| **Storage backends** | Memory, file, AWS S3 + DynamoDB, IPFS |
| **Replication** | Clone, push, pull between instances |
| **Branching** | Fork ledgers, independent commit histories |
| **Observability** | OpenTelemetry tracing, structured logging |
| **Validation** | SHACL shape constraints |

## Documentation

Full documentation lives in [`docs/`](docs/README.md):

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

## License

Licensed under the [Business Source License 1.1](LICENSE), with a Change Date
to Apache License 2.0 as specified in that file.
