# Fluree for AI and agents

Fluree is a knowledge graph built for the way LLM agents actually work: a queryable, verifiable graph they can reason over, a Model Context Protocol surface they can call as a tool, a query output format tuned for token budgets, and persistent memory that survives across sessions.

## Why a knowledge graph for AI

Retrieval over a graph beats retrieval over flat chunks when the answer depends on *relationships*. An agent can traverse from a claim to its source, from an entity to its neighbors, or from a fact to the policy that governs it — in one query, with the joins done by the engine instead of stitched together by the model. And because every commit is immutable and content-addressed, an agent's answer can cite *exactly* the graph state it read (`t` or wallclock `iso`), and signed transactions let you prove who asserted what. See [Verifiable data](../concepts/verifiable-data.md) and [Time travel](../concepts/time-travel.md).

## The pieces

### Agent JSON — token-efficient query output

A self-describing query envelope designed for LLM consumption: datatypes declared once in a `schema` header instead of repeated per value, native JSON types for inferable values, byte-budget truncation with a `hasMore` flag, and a ready-to-run `resume` query for pagination. Request it over HTTP with `Accept: application/vnd.fluree.agent+json` and a `Fluree-Max-Bytes` budget.

→ [Output formats: Agent JSON](../query/output-formats.md#agent-json-format) · [HTTP headers](../api/headers.md)

### MCP server — Fluree as an agent tool

Fluree exposes Model Context Protocol in two distinct places:

- **Server `/mcp` endpoint** — turns a running ledger into a tool an agent can call. Exposes `sparql_query` (results returned as Agent JSON, byte-budgeted) and `get_data_model` (schema/stats discovery). Off by default; enable with `--mcp-enabled` and protect it with `--mcp-auth-trusted-issuer`. Tune the Agent JSON budget and query timeout per the config reference.

  → [MCP endpoint configuration](../operations/configuration.md#mcp-endpoint)

- **CLI `fluree mcp serve`** — a stdio MCP server for IDE agents, exposing the Fluree Memory tools (`memory_add`, `memory_recall`, `memory_update`, `memory_forget`, `memory_status`) plus `kg_query` for raw SPARQL over the memory graph.

  → [`fluree mcp`](../cli/mcp.md) · [Memory: MCP server](../memory/concepts/mcp.md)

### Fluree Memory — persistent project memory for coding agents

Long-term, searchable memory for tools like Claude Code, Cursor, and VS Code Copilot. Facts, decisions, and constraints are captured as structured memories in a local Fluree ledger, stored as plain-text TTL you can commit to git, and retrieved via ranked recall. Local-first, auditable, and tuned to keep recall small so it doesn't blow the context window.

→ [Fluree Memory](../memory/README.md) · [Getting started (per IDE)](../memory/getting-started/README.md)

### Vector and full-text search — the retrieval substrate

HNSW vector similarity and BM25 full-text search live *inside* the query engine, so semantic and keyword retrieval participate in the same joins, filters, and aggregations as the rest of your graph patterns — no separate vector store or search service to operate. This is the substrate for graph-aware RAG: retrieve by similarity, then traverse the graph from the hits.

→ [Vector search](../indexing-and-search/vector-search.md) · [BM25](../indexing-and-search/bm25.md) · [Cookbook: full-text and vector search](../guides/cookbook-search.md)

### Reasoning — derived facts without a materialization step

OWL/RDFS inference and Datalog rules run inside the query engine, so an agent querying the graph sees inferred facts (class hierarchies, transitive relationships, rule conclusions) without a separate build step. Useful when you want the model to reason over a domain ontology rather than re-deriving relationships itself.

→ [Reasoning and inference](../concepts/reasoning.md) · [Reasoning in queries](../query/reasoning.md) · [Datalog rules](../query/datalog-rules.md)

## Putting it together

A typical agent-over-Fluree stack:

1. **Ingest** your domain into a ledger as RDF (optionally with [edge annotations](../concepts/edge-annotations.md) for provenance/confidence on each statement).
2. **Index** it for [vector](../indexing-and-search/vector-search.md) and [BM25](../indexing-and-search/bm25.md) search.
3. **Expose** the ledger to the agent via the [server `/mcp` endpoint](../operations/configuration.md#mcp-endpoint), so it can call `get_data_model` to learn the schema and `sparql_query` to retrieve — getting back [Agent JSON](../query/output-formats.md#agent-json-format) sized to its context budget.
4. **Govern** access with [policy](../security/policy-in-queries.md) and prove provenance with [time travel](../concepts/time-travel.md) and [signed commits](../security/commit-signing.md).
5. For the **coding-assistant** use case, layer [Fluree Memory](../memory/README.md) so the agent remembers decisions and constraints across sessions.
