# Graph Identities and Naming
This document defines **names → things** and recommended naming conventions for Fluree.
It is split into:
- **User-facing naming**: what we say in docs, examples, and APIs.
- **Internal naming**: how we name types/components in Rust so the implementation stays clear.
\
The goal is to make these simultaneously true:
- Users can think in the familiar model: **“database as a value”** (immutable, time-travelable).
- SPARQL semantics remain correct: `GRAPH <…>` identifies a **graph by IRI**.
- Fluree can seamlessly query across:
  - graphs inside a ledger (default + named graphs),
  - across ledgers (federation),
  - and non-ledger sources (BM25, vector, Iceberg/R2RML, etc.).

## Summary: the model in one paragraph
In Fluree, you query **graphs** and often load a **graph snapshot** (an immutable point-in-time view you can query repeatedly). In SPARQL, graph scoping uses `GRAPH <iri> { … }`, where `<iri>` is a **graph identifier** (a graph IRI). Fluree supports multiple kinds of **graph sources** (ledger graphs and non-ledger sources like BM25/vector indexes and tabular mappings). Users may refer to graphs with short, friendly **aliases** that Fluree resolves against a configured base into canonical **graph IRIs**. Not all graph sources support the same time-travel semantics — time pinning and “as-of” behavior is a **graph-source capability**, not a universal guarantee.

Under the hood, this “graph snapshot” corresponds to the same semantic idea many temporal systems describe as **“database as a value”**: immutable, time-travelable, and safe to pass around.

---

## User-facing naming (recommended)

### Core terms
- **Ledger**: A durable data product with a commit chain, identified by a *ledger ID* like `mydb:main`.
  - A ledger is what users create/manage.
  - A ledger can contain multiple graphs (default graph + named graphs).
- **Graph Source ID**: A canonical `name:branch` identifier used in APIs/CLI/config to refer to a graph source, e.g. `products-search:main`.
  - This is an *alias-style* identifier (not a full IRI).
  - In SPARQL contexts it may appear inside `<…>` and can be resolved against a configured base into a canonical Graph IRI.
- **Graph**: A query scope (SPARQL term).
  - In a query, a “graph” is identified by an IRI and used to scope patterns (`GRAPH <iri> { … }`).
- **Graph Snapshot**: An immutable point-in-time view of a graph that can be queried repeatedly.
- **LedgerSnapshot (database value)**: The underlying semantic model: an immutable value at a point in time.
  - In product/docs we usually say “graph snapshot” because it aligns with SPARQL and the Rust API.
  - Internally the type is `LedgerSnapshot` in `fluree-db-core`.
- **Graph IRI**: The canonical identity of a graph. This is what SPARQL uses.
- **Graph reference (GraphRef)**: What a user types (often an alias-like string), which Fluree resolves to a Graph IRI.
- **Graph Source**: Anything addressable by a Graph IRI that can participate in query execution.
- **Federation** (preferred) / **Dataset** (SPARQL term): A query executed over a set of graphs.
  - We prefer “federation” when describing the product feature to non-SPARQL users.

### “Graph snapshot” vs “Graph IRI” (how to talk about it)
- **Graph Snapshot (value)** answers: “What immutable point-in-time graph am I querying?”
- **Graph IRI (identifier)** answers: “Which graph does this part of the query run against?”

In practice, you query a graph snapshot by naming its graph:
- When you write `FROM <…>` or `GRAPH <…>`, you are naming a **graph IRI**.
- That graph IRI resolves to a **graph snapshot** (an immutable value) at execution time.

### Time pinning syntax (“the part after `@` pins the snapshot”)
Fluree supports time pinning in graph references.

**Current syntax (implemented today):**
- `<ledger>:<branch>@t:<t>` — pin to transaction time
- `<ledger>:<branch>@iso:<rfc3339>` — pin to ISO datetime
- `<ledger>:<branch>@commit:<commit-content-id>` — pin to commit ContentId (prefix allowed)

Note: you may see an `=` form in older design notes (`@t=100`, etc.). That form is **not** the supported user-facing syntax today; use the `@t:` / `@iso:` / `@commit:` forms in docs and examples.

From a user perspective:
- The `@…` portion selects **which snapshot value** you mean for that ledger graph.

Important nuance:
- For **ledger graph sources**, `@…` selects a pinned point-in-time view.
- For **non-ledger graph sources**, `@…` support is **capability-specific**:
  - Some sources may support time pinning by selecting an appropriate snapshot/root.
  - Some sources are **head-only** and should reject time-pinned requests with a clear error.

### Named graphs within a ledger
We support multiple named graphs inside a single ledger (shared commit chain, distinct graph identities/indexes).

#### System graphs

Every ledger reserves system named graphs for internal use:

| Graph | IRI pattern | Purpose |
|-------|-------------|---------|
| Default graph | (implicit) | Application data |
| Txn-meta | `urn:fluree:{ledger_id}#txn-meta` | Commit metadata |
| Config graph | `urn:fluree:{ledger_id}#config` | Ledger configuration |

User-defined named graphs (created via TriG) are identified by their IRI and allocated after the system graphs.

The **config graph** stores ledger-level operational defaults (policy, SHACL, reasoning, uniqueness constraints) as RDF triples. See [Ledger configuration](../ledger-config/README.md) for details.

#### Naming convention

Recommended user-facing convention (alias-friendly, URL-friendly, avoids `/` as a delimiter inside the ledger namespace):

```
<ledger>:<branch>[ @time-spec ] [ #<named-graph-alias> ]
```

Examples:
- Default graph, latest: `<acme/people:main>`
- Default graph at \(t=1000\): `<acme/people:main@t:1000>`
- Txn metadata graph at latest: `<acme/people:main#txn-meta>`
- Txn metadata graph at \(t=1000\): `<acme/people:main@t:1000#txn-meta>`

#### Important note about `#` fragments
Using `#<named-graph-alias>` is idiomatic RDF identity, but **HTTP clients do not send fragments to servers**.
That’s fine for graph identity and query semantics, but if you want a dereferenceable HTTP endpoint for a named graph,
plan to expose a server-visible selector (e.g., `?graph=txn-meta`) in addition to the canonical identity.

### Full IRIs are always allowed
Semantic web users may prefer full IRIs:
- `https://data.flur.ee/acme/people:main@t:1000#txn-meta`

These should be used as-is (no resolution needed).

### Base resolution (“make aliases globally identifiable”)
Many users prefer short names like `people:main` or `acme/people:main`. To make them globally identifiable:
- Allow a configured base (SPARQL `BASE <…>` or a connection/query base configuration).
- Treat alias-style graph references as **relative IRI references** resolved against that base.

Example:
- Base: `https://data.flur.ee/`
- Ref: `<acme/people:main@t:1000#txn-meta>`
- Graph IRI: `https://data.flur.ee/acme/people:main@t:1000#txn-meta`

### Character and encoding rules (user-facing)
To avoid ambiguity and URL pitfalls:
- **Reserved delimiters**:
  - `@` separates the time specifier
  - `#` separates a named-graph alias (fragment)
  - `:` is used inside the ledger ID as `ledger:branch`
- **Do not use raw `@` or `#` inside ledger names, branch names, or named-graph aliases**.
  - If needed, percent-encode them.
- RFC3339 / ISO timestamps must be URL-safe:
  - Prefer UTC with `Z` (e.g., `2026-02-03T17:02:11Z`).
  - If offsets are used (`+05:00`), they should be percent-encoded in IRI contexts.

---

## Graph Sources (user-facing)

We use **Graph Source** as the umbrella term for anything you can name in `FROM`, `FROM NAMED`, or `GRAPH <…>` and query as part of a single execution.

Graph sources differ in capabilities:
- Some behave like RDF triple stores (ledger graphs, some mappings).
- Some provide specialized operators/patterns (BM25 and vector search).
- Some support time pinning / time travel; others are head-only.

### Categories
- **Ledger Graph Sources**: RDF graphs stored in a ledger (default graph or named graph).
- **Index Graph Sources**: persisted indexes queried through graph-integrated patterns (BM25, Vector/HNSW).
- **Mapped Graph Sources**: non-ledger data mapped into an RDF-shaped graph (R2RML, Iceberg).

---

## Conventions and examples

### SPARQL: base + pinned graphs
```sparql
BASE <https://data.flur.ee/>

SELECT ?s ?p ?o
FROM <acme/people:main@t:1000>
WHERE {
  ?s ?p ?o .
}
```

### SPARQL: txn metadata named graph
```sparql
BASE <https://data.flur.ee/>

SELECT ?commit ?t
FROM NAMED <acme/people:main@t:1000#txn-meta>
WHERE {
  GRAPH <acme/people:main@t:1000#txn-meta> {
    ?commit <https://ns.flur.ee/db#t> ?t .
  }
}
```

### JSON-LD Query: pinned graph reference
```json
{
  "from": "acme/people:main@t:1000",
  "select": ["?name"],
  "where": [{ "@id": "?p", "http://schema.org/name": "?name" }]
}
```

---

## Internal naming conventions (Rust code)

These conventions govern how we name types and variables in Rust code related to graphs, ledgers, and identifiers.

### Canonical identities: prefer explicit types, not raw `String`
Internally we should distinguish:
- **`LedgerId`**: the durable ledger identifier (e.g., `acme/people:main`)
- **`GraphIri`**: canonical graph identity used by SPARQL (`Arc<str>` or validated URL/IRI type)
- **`GraphRef`**: user input token, resolved to a `GraphIri` using base rules

Even if these are initially just `struct LedgerId(Arc<str>)` newtypes, they prevent accidental mixing and make APIs self-documenting.

### Naming rules (make each word mean one thing)
This repo reserves `_id` for **content identifiers** (e.g. `commit_id`, `index_id`, `default_context_id`) and **identity/lookup keys** (`name:branch` tokens). The recommended rule set:

- **`id`**: canonical identifier used as a cache key / stable identity.
  - For ledgers this is the full `name:branch` form (e.g., `people:main`).
  - For graph sources this is the full `name:branch` form (e.g., `products-search:main`).
- **`_id` (for content references)**: a content identifier (ContentId) used by the storage layer to fetch immutable artifacts.
  - Examples: `commit_id`, `index_id`, `default_context_id`.
- **`name`**: a base name without branch (e.g., `people`).
- **`branch`**: the branch name (e.g., `main`).
- **`alias`**: a human-friendly label, and only that. For ledger identifiers, prefer `ledger_id`.

Practical guidelines:
- If a string is used to load/cache/lookup a ledger, call it **`ledger_id`** (not `ledger_alias`, not `ledger_address`).
- If a string is used to load/cache/lookup a graph source by `name:branch`, call it **`graph_source_id`** (not `gs_alias`, not `graph_source_alias`).
- If a value identifies a content-addressed artifact (commit, index, context), call it **`*_id`** (e.g., `commit_id`, `index_id`, `default_context_id`).
- If a string is used to identify a graph in SPARQL (`FROM`, `GRAPH`), call it **`graph_iri`** (canonical) or **`graph_ref`** (user input).
- Avoid having two different meanings for the same field name across crates.

### Ledger vs Db vs Graph (internal meaning)
- **Ledger**: the durable identity + commit chain + publication state (nameservice record).
- **LedgerSnapshot**: the indexed snapshot value used for range/scan (hot path).
- **Graph**: query scoping mechanism (active graph, dataset graph selection, `GRAPH` operator).

Avoid using "graph" as a synonym for "db" in code comments. Prefer:
- "ledger graph" (RDF graph inside a ledger)
- "graph IRI" (identifier)
- "graph source" (registry/resolution concept)

### "Dataset" naming internally
SPARQL calls the set-of-graphs a "dataset", and the code already models a `DataSet`.
For product-facing APIs and docs, prefer "federation", but internally keep `DataSet` as the SPARQL-aligned term.

---

## Related docs
- `docs/concepts/time-travel.md` (time pinning syntax)
- `docs/concepts/datasets-and-named-graphs.md` (SPARQL dataset semantics)
- `docs/graph-sources/overview.md` and `docs/concepts/graph-sources.md` (graph source overview)
- `docs/reference/glossary.md` (terms glossary)

