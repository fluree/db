# Fluree DB

A semantic graph database with time travel, branching, and verifiable data — built on W3C standards.

Fluree DB is a single binary that stores your data as an RDF knowledge graph, queryable with SPARQL or JSON-LD Query, with every commit immutably recorded so you can travel back to any prior state. It supports git-style branching and merging, signed and policy-gated transactions, SHACL validation, OWL/RDFS reasoning, and full-text and vector search — over local files, S3, or IPFS — without bolting on external services.

## What you get

- **Semantic by default.** Your data is RDF. IRIs, JSON-LD `@context`, named graphs, and typed values are first-class. Queries are SPARQL 1.1 or the equivalent JSON-LD Query, both compiling to the same execution engine.
- **Time travel.** Every transaction is a commit on an immutable chain. Query the state of the graph at any past moment with a single `t` parameter — no snapshots to restore, no separate audit log to consult.
- **Branching and merging.** Create a branch off any commit, transact against it in isolation, then merge it back. Useful for staging changes, running what-if analyses, or maintaining environment-specific overlays.
- **Verifiable data.** Transactions and commits can be signed (JWS / W3C Verifiable Credentials). The commit chain is content-addressed, so any tampering is detectable. Pair it with policy enforcement to prove *who* changed *what* and *when* they were allowed to.
- **Policy-based access control.** Policies are written as graph data, evaluated per query and per transaction, and travel with the ledger — not bolted on at the API layer.
- **Storage your way.** Local filesystem for development, S3 + DynamoDB for production, IPFS for content-addressed distribution. The same ledger format works across all of them.
- **Search built in.** BM25 full-text indexing and HNSW vector search live alongside SPARQL — no separate search service to operate.
- **Reasoning.** OWL/RDFS inference and Datalog rules run inside the query engine, so derived facts are queryable without a materialization step.
- **Embeddable.** The same engine that powers the server runs as a Rust library, generic over storage and nameservice. Use it directly in your application or run it standalone over HTTP.

## Start here

- **New to Fluree?** → [Getting started](getting-started/README.md)
- **Run the server** → [Quickstart: run the server](getting-started/quickstart-server.md)
- **Create a ledger and write data** → [Quickstart: create a ledger](getting-started/quickstart-ledger.md) → [Quickstart: write data](getting-started/quickstart-write.md)
- **Query data** → [Quickstart: query (JSON-LD + SPARQL)](getting-started/quickstart-query.md)
- **End-to-end walkthrough** → [Tutorial: search, time travel, branching, policies](getting-started/tutorial-end-to-end.md)
- **Coming from SQL?** → [Fluree for SQL developers](getting-started/fluree-for-sql-developers.md)
- **Embedding in Rust?** → [Using Fluree as a Rust library](getting-started/rust-api.md)

## Explore the docs

- [Concepts](concepts/README.md) — ledgers, graph sources, IRIs, time travel, policy, verifiable data, reasoning
- [Guides (cookbooks)](guides/README.md) — search, time travel, branching, policies, SHACL — task-oriented recipes
- [CLI reference](cli/README.md) — every `fluree` command, flag by flag
- [HTTP API](api/README.md) — endpoints, headers, signed requests, error model
- [Query](query/README.md) — JSON-LD Query, SPARQL, output formats, CONSTRUCT, explain plans, reasoning
- [Transactions](transactions/README.md) — insert, upsert, update, conditional updates, signed transactions
- [Security and policy](security/README.md) — authentication, encryption, commit signing, policy model
- [Indexing and search](indexing-and-search/README.md) — background indexing, BM25, vector search, geospatial
- [Graph sources and integrations](graph-sources/README.md) — Iceberg/Parquet, R2RML, BM25 graph source
- [Operations](operations/README.md) — configuration, Docker, storage modes, telemetry, archive/restore
- [Design](design/README.md) — internals: query execution, storage traits, index format, nameservice
- [Reference](reference/README.md) — glossary, vocabulary, OWL/RDFS support, crate map
- [Troubleshooting](troubleshooting/README.md) — common errors, debugging queries, performance tracing
- [Contributing](contributing/README.md) — dev setup, tests, SPARQL compliance, releasing

The full table of contents is in [`SUMMARY.md`](SUMMARY.md).

## Fluree Memory

[Fluree Memory](memory/README.md) is persistent, searchable memory for AI coding assistants — built on Fluree DB and shipped in the same `fluree` binary. If you're here for the memory tooling, jump straight to the [Memory docs](memory/README.md).
