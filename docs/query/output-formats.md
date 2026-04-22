# Output Formats

Fluree supports multiple output formats for query results, each optimized for different use cases. You can choose the format that best fits your application's needs.

## Supported Formats

### JSON-LD Format

**Default format** for JSON-LD Query. Provides compact, context-aware JSON with IRI expansion/compaction.

**Characteristics:**
- Uses `@context` for IRI compaction
- Compact IRIs (e.g., `ex:alice` instead of full IRIs)
- Inferable datatypes (string, long, double, boolean) rendered as bare values
- Language tags preserved

**Example (graph crawl):**

```json
[
  {
    "@id": "ex:alice",
    "schema:name": "Alice",
    "schema:age": 30,
    "schema:knows": {"@id": "ex:bob"}
  }
]
```

**Example (tabular SELECT):**

```json
[
  ["Alice", 30],
  ["Bob", 25]
]
```

### SPARQL JSON Format

Standard SPARQL 1.1 result format for SPARQL queries.

**Characteristics:**
- W3C SPARQL 1.1 compliant
- Standard `results` and `bindings` structure
- Datatype information included
- Language tags included

**Example:**

```json
{
  "head": {
    "vars": ["name", "age"]
  },
  "results": {
    "bindings": [
      {
        "name": {
          "type": "literal",
          "value": "Alice"
        },
        "age": {
          "type": "literal",
          "value": "30",
          "datatype": "http://www.w3.org/2001/XMLSchema#integer"
        }
      }
    ]
  }
}
```

### Typed JSON Format

Type-preserving JSON format with explicit datatype information on every value. Works with both tabular SELECT queries and graph crawl (entity-centric) queries.

**Characteristics:**
- Every literal includes `{"@value": ..., "@type": "..."}` — even inferable types
- References use `{"@id": "..."}`
- Language-tagged strings use `{"@value": ..., "@language": "..."}`
- `@json` values use `{"@value": <parsed>, "@type": "@json"}`
- Nested entities in graph crawl results are also fully typed
- IRIs compacted via `@context`

**Example (tabular SELECT):**

```json
[
  {
    "?name": {"@value": "Alice", "@type": "xsd:string"},
    "?age": {"@value": 30, "@type": "xsd:long"}
  }
]
```

**Example (graph crawl):**

```json
[
  {
    "@id": "ex:alice",
    "@type": ["schema:Person"],
    "schema:name": {"@value": "Alice", "@type": "xsd:string"},
    "schema:age": {"@value": 30, "@type": "xsd:long"},
    "schema:knows": {
      "@id": "ex:bob",
      "schema:name": {"@value": "Bob", "@type": "xsd:string"}
    },
    "ex:data": {"@value": {"key": "val"}, "@type": "@json"}
  }
]
```

### Agent JSON Format

**Optimized for LLM/agent consumption.** Returns a self-describing envelope with a schema header, compact object rows using native JSON types, and built-in pagination support.

**Request via HTTP:**
```http
Accept: application/vnd.fluree.agent+json
Fluree-Max-Bytes: 32768
```

**Characteristics:**
- Schema-once header: datatypes declared per variable, not repeated per value
- Native JSON types for values (strings, numbers, booleans — no wrappers for inferable types)
- Non-inferable datatypes annotated inline only where needed (`{"@value": ..., "@type": "..."}`)
- Byte-budget truncation with `hasMore` flag and resume query
- Time-pinning metadata (`t` for single-ledger, `iso` wallclock timestamp for cross-ledger)

**Example (single-ledger, no truncation):**

```json
{
  "schema": {
    "?name": "xsd:string",
    "?age": "xsd:integer",
    "?s": "uri"
  },
  "rows": [
    {"?name": "Alice", "?age": 30, "?s": "ex:alice"},
    {"?name": "Bob", "?age": 25, "?s": "ex:bob"}
  ],
  "rowCount": 2,
  "t": 5,
  "iso": "2026-03-26T14:30:00Z",
  "hasMore": false
}
```

**Example (truncated, with resume query):**

```json
{
  "schema": {
    "?name": "xsd:string",
    "?age": "xsd:integer"
  },
  "rows": [
    {"?name": "Alice", "?age": 30},
    {"?name": "Bob", "?age": 25}
  ],
  "rowCount": 2,
  "t": 5,
  "iso": "2026-03-26T14:30:00Z",
  "hasMore": true,
  "message": "Response truncated due to size limit of 32768 bytes. Use the query below to retrieve the next batch.",
  "resume": "SELECT ?name ?age FROM <mydb:main@t:5> WHERE { ?s ex:name ?name ; ex:age ?age } OFFSET 2 LIMIT 100"
}
```

**Schema types:**
- Single type → string: `"?name": "xsd:string"`
- Mixed types → array: `"?value": ["xsd:string", "xsd:integer"]`
- IRI references → `"uri"`

**Envelope fields:**

| Field | Present | Description |
|-------|---------|-------------|
| `schema` | Always | Per-variable datatype map |
| `rows` | Always | Array of `{variable: value}` objects |
| `rowCount` | Always | Number of rows included |
| `t` | Single-ledger only | Transaction number used for the query |
| `iso` | Always | ISO-8601 wallclock timestamp at query time |
| `hasMore` | Always | Whether more rows exist beyond the byte budget |
| `message` | When truncated | Human-readable truncation explanation |
| `resume` | When truncated, single-FROM only | Ready-to-execute SPARQL with `@t:` pinning and OFFSET |

**Multi-ledger queries:** The `t` field is omitted (each ledger has its own timeline). The `resume` field is also omitted; instead, the `message` instructs the caller to use `@iso:` on each FROM clause for time-pinning.

**Byte budget:** Set via the `Fluree-Max-Bytes` header. When the cumulative serialized size of rows exceeds this limit, the formatter stops adding rows and sets `hasMore: true`. The budget applies to row data only (schema and envelope overhead are excluded from the count).

## Array Normalization

By default, graph crawl results return single-valued properties as bare scalars and multi-valued properties as arrays:

```json
{"schema:name": "Alice", "ex:tags": ["rust", "wasm"]}
```

This can be problematic for typed struct deserialization (e.g., a `Vec<String>` field that receives a bare string when only one value exists).

**`normalize_arrays`** forces all property values into arrays regardless of cardinality:

```json
{"schema:name": ["Alice"], "ex:tags": ["rust", "wasm"]}
```

This is orthogonal to typed JSON and can be combined with any format:

```rust
// Typed + normalized — most predictable for struct deserialization
let config = FormatterConfig::typed_json().with_normalize_arrays();

// JSON-LD + normalized — compact values but predictable shapes
let config = FormatterConfig::jsonld().with_normalize_arrays();
```

The `@container: @set` context annotation still forces arrays per-property and works regardless of the `normalize_arrays` setting.

## Format Selection

### JSON-LD Query

JSON-LD Query defaults to JSON-LD format. You can specify the format explicitly:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?name", "?age"],
  "where": [
    { "@id": "?person", "ex:name": "?name", "ex:age": "?age" }
  ],
  "format": "jsonld"
}
```

### SPARQL

SPARQL queries return SPARQL JSON format by default:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?age
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
}
```

## Datatype Handling

### String Types

**JSON-LD:**

```json
"Hello"
```

**Typed JSON:**

```json
{"@value": "Hello", "@type": "xsd:string"}
```

**SPARQL JSON:**

```json
{"type": "literal", "value": "Hello"}
```

### Numeric Types

**JSON-LD:**

```json
42
```

**Typed JSON:**

```json
{"@value": 42, "@type": "xsd:long"}
```

**SPARQL JSON:**

```json
{"type": "literal", "value": "42", "datatype": "http://www.w3.org/2001/XMLSchema#integer"}
```

### Language-Tagged Strings

All formats use the same representation:

```json
{"@value": "Hello", "@language": "en"}
```

### IRIs

**JSON-LD / Typed JSON:**

```json
{"@id": "ex:alice"}
```

**SPARQL JSON:**

```json
{"type": "uri", "value": "http://example.org/ns/alice"}
```

## Rust API

Use `FormatterConfig` to control output format via the query builder API:

```rust
use fluree_db_api::FormatterConfig;

// Single-ledger query with explicit format
let db = fluree.db("mydb:main").await?;
let result = db.query(&fluree)
    .sparql("SELECT ?name WHERE { ?s <schema:name> ?name }")
    .format(FormatterConfig::typed_json())
    .execute_formatted()
    .await?;

// Dataset query with format
let result = dataset.query(&fluree)
    .sparql("SELECT * WHERE { ?s ?p ?o }")
    .format(FormatterConfig::sparql_json())
    .execute_formatted()
    .await?;

// Connection-level query with format
let result = fluree.query_from()
    .jsonld(&query_with_from)
    .format(FormatterConfig::jsonld())
    .execute_formatted()
    .await?;

// AgentJson with byte budget and resume support
use fluree_db_api::AgentJsonContext;

let config = FormatterConfig::agent_json()
    .with_max_bytes(32768)
    .with_agent_json_context(AgentJsonContext {
        sparql_text: Some(sparql.to_string()),
        from_count: 1,
        iso_timestamp: Some(chrono::Utc::now().to_rfc3339()),
    });
let result = db.query(&fluree)
    .sparql("SELECT ?name ?age WHERE { ?s ex:name ?name ; ex:age ?age }")
    .format(config)
    .execute_formatted()
    .await?;

// Or directly on QueryResult:
let json = result.to_agent_json(&snapshot)?;                       // no budget
let json = result.to_agent_json_with_config(&snapshot, &config)?;  // with budget
```

Available format constructors:
- `FormatterConfig::jsonld()` — JSON-LD (default for JSON-LD queries)
- `FormatterConfig::sparql_json()` — SPARQL 1.1 JSON Results (default for SPARQL queries)
- `FormatterConfig::typed_json()` — Typed JSON with explicit datatypes on every value
- `FormatterConfig::agent_json()` — Agent JSON envelope for LLM/agent consumers

Builder methods:
- `.with_normalize_arrays()` — Force array wrapping for all graph crawl properties
- `.with_pretty()` — Pretty-print JSON output
- `.with_max_bytes(n)` — Set byte budget for AgentJson truncation
- `.with_agent_json_context(ctx)` — Set SPARQL text, FROM count, and ISO timestamp for AgentJson resume queries

All three query paths (`db.query()`, `dataset.query()`, `fluree.query_from()`) support `.format()`.

### Direct formatting on QueryResult

For graph crawl queries (which require async DB access):

```rust
// Typed JSON with graph crawl support
let json = result.to_typed_json_async(db.as_graph_db_ref()).await?;

// Custom config (e.g., typed + normalize_arrays)
let config = FormatterConfig::typed_json().with_normalize_arrays();
let json = result.format_async(db.as_graph_db_ref(), &config).await?;
```

When no `.format()` is set:
- JSON-LD queries default to JSON-LD format
- SPARQL queries default to SPARQL JSON format

## CLI Usage

The `fluree query` command supports format selection via `--format`:

```bash
# Default table output
fluree query "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 5"

# JSON output
fluree query --format json '{"select": {"ex:alice": ["*"]}, "from": "mydb:main"}'

# Typed JSON output (explicit types on every value)
fluree query --format typed-json '{"select": {"ex:alice": ["*"]}, "from": "mydb:main"}'

# Normalize arrays (force all properties to arrays)
fluree query --format json --normalize-arrays '{"select": {"ex:alice": ["*"]}, "from": "mydb:main"}'

# Typed JSON + normalize arrays (most predictable for programmatic use)
fluree query --format typed-json --normalize-arrays '{"select": {"ex:alice": ["*"]}, "from": "mydb:main"}'
```

## Performance Considerations

- **JSON-LD** is the most efficient format — inferable types skip the `@value`/`@type` wrapper
- **Typed JSON** adds a constant-factor overhead per literal value (one extra JSON object allocation). Query execution is unaffected — only the formatting phase is slower.
- **normalize_arrays** adds zero overhead when disabled (default). When enabled, it skips the `len() == 1` check — no additional allocations beyond the array wrapper.
- **TSV/CSV** bypass JSON DOM construction entirely for maximum throughput

## Best Practices

1. **Use JSON-LD for human-facing apps**: Compact and readable
2. **Use Typed JSON for struct deserialization**: Unambiguous types prevent parsing surprises
3. **Use `normalize_arrays` for typed consumers**: Ensures `Vec<T>` fields always get arrays
4. **Use SPARQL JSON for standard tooling**: Interoperable with SPARQL clients
5. **Use TSV/CSV for bulk export**: Highest throughput, smallest memory footprint
6. **Use Agent JSON for LLM/agent integrations**: Schema-once + pagination prevents context window overflow

## Related Documentation

- [JSON-LD Query](jsonld-query.md): JSON-LD Query language
- [SPARQL](sparql.md): SPARQL query language
- [Datatypes](../concepts/datatypes.md): Type system details
