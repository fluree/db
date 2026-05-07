# Query-Time Reasoning

This page covers how to enable and use reasoning in your queries. For
background concepts see [Reasoning and inference](../concepts/reasoning.md); for
the full list of supported OWL/RDFS constructs see the
[OWL & RDFS reference](../reference/owl-rdfs-support.md).

## The `reasoning` parameter

Add a `"reasoning"` key to any JSON-LD query to control which inference modes
are active:

### Single mode

```json
{
  "@context": {"ex": "http://example.org/"},
  "select": ["?s"],
  "where": {"@id": "?s", "@type": "ex:Person"},
  "reasoning": "rdfs"
}
```

### Multiple modes

```json
{
  "select": ["?s"],
  "where": {"@id": "?s", "@type": "ex:Person"},
  "reasoning": ["rdfs", "owl2rl"]
}
```

### Disable reasoning

```json
{
  "select": ["?s"],
  "where": {"@id": "?s", "@type": "ex:Person"},
  "reasoning": "none"
}
```

Use `"none"` to suppress auto-enabled RDFS and any ledger-wide defaults.

### Valid mode strings

| String | Aliases | Mode |
|--------|---------|------|
| `"rdfs"` | — | RDFS subclass/subproperty expansion |
| `"owl2ql"` | `"owl-ql"`, `"owlql"` | OWL 2 QL query rewriting (includes RDFS) |
| `"owl2rl"` | `"owl-rl"`, `"owlrl"` | OWL 2 RL forward-chaining materialization |
| `"datalog"` | — | Datalog rule execution |
| `"none"` | — | Disable all reasoning |

## Default behavior

When the `reasoning` key is **absent** from a query:

- **RDFS auto-enables** if your data contains `rdfs:subClassOf` or
  `rdfs:subPropertyOf` hierarchies. This is lightweight (query rewriting only)
  and usually desirable.
- **OWL 2 QL, OWL 2 RL, and Datalog remain disabled** unless enabled via
  ledger-wide configuration.

To override ledger defaults for a single query, use `"reasoning": "none"`.

## Examples

The examples below assume this schema and data have been transacted:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#"
  },
  "insert": [
    {"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}},
    {"@id": "ex:GradStudent", "rdfs:subClassOf": {"@id": "ex:Student"}},
    {"@id": "ex:alice", "@type": "ex:GradStudent", "ex:name": "Alice"},
    {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"},

    {"@id": "ex:livesWith", "@type": "owl:SymmetricProperty"},
    {"@id": "ex:alice", "ex:livesWith": {"@id": "ex:bob"}},

    {"@id": "ex:hasAncestor", "@type": "owl:TransitiveProperty"},
    {"@id": "ex:carol", "ex:hasAncestor": {"@id": "ex:dave"}},
    {"@id": "ex:dave", "ex:hasAncestor": {"@id": "ex:eve"}},

    {"@id": "ex:hasMother", "owl:inverseOf": {"@id": "ex:childOf"}},
    {"@id": "ex:frank", "ex:hasMother": {"@id": "ex:grace"}}
  ]
}
```

### RDFS: subclass expansion

Query for all `ex:Person` instances — Alice is returned even though she was
only typed as `ex:GradStudent`:

```json
{
  "@context": {"ex": "http://example.org/"},
  "select": ["?name"],
  "where": {
    "@id": "?s", "@type": "ex:Person",
    "ex:name": "?name"
  },
  "reasoning": "rdfs"
}
```

**Result:** `["Alice", "Bob"]`

Without reasoning (or with `"reasoning": "none"`), only `"Bob"` is returned
because Alice's explicit type is `GradStudent`, not `Person`.

### OWL 2 RL: symmetric properties

Query who lives with Bob — Alice is inferred even though only
`alice livesWith bob` was asserted:

```json
{
  "@context": {"ex": "http://example.org/"},
  "select": ["?who"],
  "where": {"@id": "ex:bob", "ex:livesWith": "?who"},
  "reasoning": "owl2rl"
}
```

**Result:** `["ex:alice"]`

### OWL 2 RL: transitive properties

Query for all ancestors of Carol — Eve is inferred through transitivity:

```json
{
  "@context": {"ex": "http://example.org/"},
  "select": ["?ancestor"],
  "where": {"@id": "ex:carol", "ex:hasAncestor": "?ancestor"},
  "reasoning": "owl2rl"
}
```

**Result:** `["ex:dave", "ex:eve"]`

### OWL 2 QL: inverse properties

Query `childOf` — inferred from the `hasMother` / `inverseOf` declaration:

```json
{
  "@context": {"ex": "http://example.org/"},
  "select": ["?child"],
  "where": {"@id": "ex:grace", "ex:childOf": "?child"},
  "reasoning": "owl2ql"
}
```

**Result:** `["ex:frank"]`

### OWL 2 RL: domain and range inference

If your schema declares `rdfs:domain` and `rdfs:range`:

```json
{
  "insert": [
    {"@id": "ex:teaches", "rdfs:domain": {"@id": "ex:Professor"},
                          "rdfs:range": {"@id": "ex:Course"}},
    {"@id": "ex:alice", "ex:teaches": {"@id": "ex:cs101"}}
  ]
}
```

Then with `"reasoning": "owl2rl"`:
- `ex:alice rdf:type ex:Professor` is inferred (from domain)
- `ex:cs101 rdf:type ex:Course` is inferred (from range)

### Combined modes

Enable RDFS + OWL 2 RL + Datalog together:

```json
{
  "select": ["?s"],
  "where": {"@id": "?s", "@type": "ex:Person"},
  "reasoning": ["rdfs", "owl2rl", "datalog"],
  "rules": [
    {
      "@context": {"ex": "http://example.org/"},
      "where": {"@id": "?p", "ex:parent": {"ex:parent": "?gp"}},
      "insert": {"@id": "?p", "ex:grandparent": {"@id": "?gp"}}
    }
  ]
}
```

OWL 2 RL facts are materialized first, then Datalog rules run over the
combined base + OWL data, and finally RDFS query rewriting is applied.

## SPARQL

In SPARQL queries, reasoning is controlled via the Fluree-specific
`PRAGMA reasoning` directive. Property paths (`+`, `*`, `^`) provide a
complementary mechanism for navigating transitive and inverse relationships
directly in the query pattern — see [SPARQL](sparql.md) for details.

## Interaction with ledger configuration

If `f:reasoningDefaults` is set in the ledger configuration graph (see
[Setting groups](../ledger-config/setting-groups.md)), those modes are the
baseline for every query. The per-query `reasoning` parameter can:

- **Add modes** — the query modes are merged with the defaults.
- **Disable all** — `"reasoning": "none"` overrides the defaults entirely.

The `f:overrideControl` setting on the ledger config determines whether
query-time overrides are allowed. See
[Override control](../ledger-config/override-control.md) for details.

## Performance considerations

| Mode | Overhead | Caching |
|------|----------|---------|
| RDFS | Negligible — query rewriting only | N/A |
| OWL 2 QL | Negligible — query rewriting only | N/A |
| OWL 2 RL | First query materializes derived facts; subsequent queries use cache | LRU cache (16 entries), keyed on database state + reasoning modes |
| Datalog | Each unique rule set + database state combination is cached | Same LRU cache as OWL 2 RL |

**Tips:**
- Start with **RDFS** if you only need class/property hierarchies — it has
  virtually zero overhead.
- Use **OWL 2 QL** when you also need inverse properties and domain/range
  inference but want to stay in the query-rewriting approach.
- Use **OWL 2 RL** when you need the full rule set (transitive, symmetric,
  functional properties, `owl:sameAs`, restrictions, property chains).
- The materialization cache is invalidated when the underlying data changes
  (new transactions), so the first query after a write will re-materialize.

## Related pages

| Topic | Page |
|-------|------|
| Conceptual introduction | [Reasoning and inference](../concepts/reasoning.md) |
| Custom inference rules | [Datalog rules](datalog-rules.md) |
| Supported OWL & RDFS constructs | [OWL & RDFS reference](../reference/owl-rdfs-support.md) |
| Ledger-wide reasoning config | [Setting groups](../ledger-config/setting-groups.md) |
