# IRIs, Namespaces, and JSON-LD @context

## Internationalized Resource Identifiers (IRIs)

In Fluree, all data identifiers use **Internationalized Resource Identifiers (IRIs)** - the internationalized version of URIs. IRIs uniquely identify:

- **Subjects**: Entities in your data (people, products, concepts)
- **Predicates**: Relationships or properties
- **Objects**: Values or other entities
- **Graphs**: Named data partitions

### IRI Examples

```turtle
# Full IRIs
<http://example.org/person/alice> <http://xmlns.com/foaf/0.1/name> "Alice" .
<http://example.org/person/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .

# IRIs with Unicode characters
<http://例え.org/人物/アリス> <http://xmlns.com/foaf/0.1/name> "アリス" .
```

### IRI Best Practices

- **Use stable domains**: Choose domains you control or well-established standards
- **Hierarchical structure**: Organize IRIs with meaningful paths
- **Avoid query parameters**: IRIs should be clean identifiers, not URLs with parameters
- **Internationalization**: IRIs support Unicode characters for global identifiers

## Namespaces

**Namespaces** provide shorthand notation for IRIs, making data more readable and manageable. A namespace maps a prefix to a base IRI.

### Defining Namespaces

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "xsd": "http://www.w3.org/2001/XMLSchema#"
  }
}
```

### Using Namespaced IRIs

With the above context, you can write compact IRIs:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "foaf": "http://xmlns.com/foaf/0.1/"
  },
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "foaf:Person",
      "foaf:name": "Alice Smith"
    }
  ]
}
```

This expands to:

```json
{
  "@graph": [
    {
      "@id": "http://example.org/ns/alice",
      "@type": "http://xmlns.com/foaf/0.1/Person",
      "http://xmlns.com/foaf/0.1/name": "Alice Smith"
    }
  ]
}
```

## JSON-LD @context

The **@context** is a JSON-LD mechanism that defines how to interpret the data. In Fluree, @context serves multiple purposes:

### IRI Expansion/Compaction

```json
{
  "@context": {
    "name": "http://xmlns.com/foaf/0.1/name",
    "Person": "http://xmlns.com/foaf/0.1/Person"
  },
  "@graph": [
    {
      "@id": "http://example.org/alice",
      "@type": "Person",
      "name": "Alice"
    }
  ]
}
```

The @context maps `name` → `http://xmlns.com/foaf/0.1/name` and `Person` → `http://xmlns.com/foaf/0.1/Person`.

### Standard Prefixes

Fluree includes many standard prefixes by default:

```json
{
  "@context": {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "dc": "http://purl.org/dc/elements/1.1/"
  }
}
```

### @context in Queries

@context is also used in query results for compact output:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "foaf": "http://xmlns.com/foaf/0.1/"
  },
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "foaf:Person",
      "foaf:name": "Alice"
    }
  ]
}
```

## IRI Resolution Rules

Fluree follows strict IRI resolution rules:

### Absolute IRIs

These are used as-is:
- `http://example.org/person/alice`
- `https://data.example.com/product/123`

### Prefixed IRIs

These expand using @context:
- `ex:alice` → `http://example.org/ns/alice` (if `ex` maps to `http://example.org/ns/`)
- `foaf:name` → `http://xmlns.com/foaf/0.1/name`

### Relative IRIs

These are resolved relative to a base IRI:
- `alice` → `http://example.org/ns/alice` (if base is `http://example.org/ns/`)

## Strict Compact-IRI Guard

JSON-LD parsing in Fluree (queries and transactions) is **strict by default** about compact IRIs. If you write a value that *looks* like a compact IRI — `prefix:suffix` — but the prefix is not defined in `@context`, Fluree rejects the request at parse time with a clear error:

```text
Unresolved compact IRI 'ex:Person': prefix 'ex' is not defined in @context.
If this is intended as an absolute IRI, use a full form (e.g. http://...)
or add the prefix to @context.
```

### Why strict by default

Without the guard, a missing or misspelled prefix passes through silently — `ex:Person` gets stored as the literal string `"ex:Person"` instead of being expanded to a real IRI like `http://example.org/Person`. This produces incorrect data and confusing query results that are very hard to diagnose later.

The guard catches the most common cause of these bugs: forgetting an `@context`.

### What the guard accepts

- IRIs that resolve through `@context` (the normal happy path).
- Hierarchical absolute IRIs whose suffix starts with `//` — `http://...`, `https://...`, `ftp://...`, etc.
- A small allowlist of well-known non-hierarchical schemes — `urn:`, `did:`, `mailto:`, `tel:`, `data:`, `ipfs:`, `ipns:`, `geo:`, `blob:`, `magnet:`, `fluree:`. Scheme names are matched case-insensitively per RFC 3986.
- Variables (`?x`) and blank nodes (`_:b0`) bypass the guard entirely.

### Where the guard applies

The guard runs at every position that semantically expects an IRI in JSON-LD:

- `@id`, `@type`, predicates / property names
- Datatype IRIs in `@type` of `@value` objects
- Graph names and graph-crawl roots
- Selection predicates (forward and reverse)
- VALUES `@id` cells
- `@path` aliases inside `@context`

It does **not** apply to:

- SPARQL queries
- Turtle / TriG transactions
- Literal string values (only IRI positions)
- Other consumers of the underlying JSON-LD expander (e.g. connection-config parsing)

### Opting out per request

If you really need to accept unresolved compact-looking strings — for example, when migrating legacy data that uses bare `prefix:suffix` strings as opaque identifiers — set `opts.strictCompactIri: false` in the JSON-LD payload itself:

```json
{
  "@context": {"ex": "http://example.org/ns/"},
  "opts": {"strictCompactIri": false},
  "@graph": [
    {"@id": "ex:alice", "ex:name": "Alice"},
    {"@id": "legacy:bob", "ex:name": "Bob"}
  ]
}
```

The same key works on both queries and transactions. The default is `true`. Keep it on unless you have a concrete reason to disable it.

For programmatic use from Rust, transactions can also set `TxnOpts.strict_compact_iri` directly; that takes precedence over `opts.strictCompactIri` in the JSON.

## Blank Nodes and Anonymous Entities

**Blank nodes** represent entities without global identifiers:

```json
{
  "@graph": [
    {
      "@id": "_:b1",
      "foaf:name": "Anonymous Person"
    }
  ]
}
```

Blank nodes are:
- Local to a single transaction
- Cannot be referenced across transactions
- Useful for temporary or anonymous data

## Best Practices

### Namespace Organization

1. **Use stable prefixes**: Don't change prefix mappings once data is committed
2. **Standard vocabularies**: Use well-known prefixes (foaf, dc, rdf, etc.)
3. **Custom domains**: Use your own domain for application-specific terms
4. **Versioning**: Consider versioning in namespace IRIs for evolution

### IRI Design

1. **Descriptive paths**: Use meaningful hierarchical paths
2. **Avoid special characters**: Stick to URL-safe characters
3. **Consistent casing**: Use consistent capitalization conventions
4. **Future-proofing**: Design IRIs to accommodate future extensions

### @context Management

1. **Shared contexts**: Reuse @context definitions across transactions
2. **Minimal contexts**: Only define prefixes you actually use
3. **Documentation**: Document custom prefixes and their meanings
4. **Evolution**: Plan for @context changes over time

## Default Context

Each ledger can store a **default context** — a JSON object mapping prefixes to IRIs. This context is available for retrieval and can be injected into queries by compatibility surfaces (the Fluree HTTP server and CLI), but is **not** applied automatically by the core API (`fluree-db-api`).

### How it's populated

- **Bulk import:** When importing Turtle data via `fluree create --from`, all `@prefix` declarations are captured and stored as the ledger's default context, augmented with built-in prefixes (`rdf`, `rdfs`, `xsd`, `owl`, `sh`, `geo`).
- **Manual update:** Use the CLI (`fluree context set`) or HTTP API (`PUT /fluree/context/:ledger`) to set or replace the context at any time.

### Core API behavior

When using `fluree-db-api` directly (e.g., embedding Fluree in a Rust application), queries must supply their own `@context` (JSON-LD) or `PREFIX` declarations (SPARQL). If a query omits context, IRIs are not compacted and compact IRIs without a matching prefix will produce an error.

To opt in to default context injection when using the API directly, fetch the stored context and use the `with_default_context` builder:

```rust
let ctx = fluree.get_default_context("mydb").await?;
let ledger = fluree.ledger("mydb").await?;
let view = GraphDb::from_ledger_state(&ledger)
    .with_default_context(ctx);
```

Or use the convenience method:

```rust
let view = fluree.db_with_default_context("mydb").await?;
```

### Server and CLI behavior

The **CLI** automatically injects the ledger's default context into queries that don't provide their own. The HTTP API defaults this behavior off; pass `?default-context=true` on a query request to opt in.

When default context injection is enabled:

1. **Query-level `@context`** (JSON-LD) or **`PREFIX` declarations** (SPARQL) — always win
2. **Ledger default context** — applied only when the query provides no context of its own
3. **Built-in prefixes** — `rdf`, `rdfs`, `xsd`, etc. are always available

### Use with SPARQL (server/CLI)

The default context provides prefix definitions for SPARQL queries, so you don't need to repeat `PREFIX` declarations in every query when injection is enabled. If the ledger's default context includes `{"ex": "http://example.org/"}`, then you can write:

```sparql
SELECT ?name WHERE {
  ex:alice ex:name ?name .
}
```

without an explicit `PREFIX ex: <http://example.org/>` declaration. If you declare any `PREFIX` in the query, the default context is not used at all — you must declare every prefix you need.

### Use with JSON-LD queries (server/CLI)

Similarly, JSON-LD queries sent through an opt-in surface that omit `@context` receive the default context:

```json
{
  "select": ["?name"],
  "where": [["ex:alice", "ex:name", "?name"]]
}
```

### Viewing and updating

```bash
# View the default context
fluree context get mydb

# Replace it
fluree context set mydb -e '{"ex": "http://example.org/", "foaf": "http://xmlns.com/foaf/0.1/"}'
```

Via the HTTP API:

```bash
# Read
curl http://localhost:8090/fluree/context/mydb:main

# Replace
curl -X PUT http://localhost:8090/fluree/context/mydb:main \
  -H "Content-Type: application/json" \
  -d '{"ex": "http://example.org/"}'
```

See [CLI context command](../cli/context.md) and [API endpoints](../api/endpoints.md#get-flureecontextledger) for full details.

### Opting out of the default context

When using a default-context-enabled surface, you may want full, unexpanded IRIs in query results — for debugging, interoperability with other RDF tools, or simply to avoid any prefix assumptions. You can opt out of the default context:

**JSON-LD queries** — pass an empty `@context` object:

```json
{
  "@context": {},
  "select": ["?s", "?p", "?o"],
  "where": [["?s", "?p", "?o"]]
}
```

Results will contain full IRIs (e.g., `http://example.org/ns/alice`) instead of compacted forms (`ex:alice`).

**SPARQL queries** — include any `PREFIX` declaration. When a query declares its own prefixes, the default context is not injected. To opt out without defining any real prefix, use an empty default prefix:

```sparql
PREFIX : <>
SELECT ?s ?p ?o WHERE { ?s ?p ?o }
```

Or simply declare the specific prefixes you need — the default context is only injected when the query has *no* `PREFIX` declarations whatsoever.

### Storage

The default context is stored as a content-addressed blob in CAS, with a pointer (ContentId) in the nameservice config. Updates use compare-and-set semantics, so concurrent writers are safely handled. After an update, the server invalidates the cached ledger state so subsequent operations use the new context.

## Integration with Standards

Fluree's IRI system is fully compatible with:

- **RDF Standards**: Works with RDF/XML, Turtle, N-Triples
- **SPARQL**: IRIs work seamlessly in SPARQL queries
- **Linked Data**: Enables publishing and consuming linked data
- **Semantic Web**: Supports OWL ontologies and RDF Schema

This foundation enables Fluree to participate in the broader semantic web ecosystem while providing the convenience of JSON-LD's compact syntax.