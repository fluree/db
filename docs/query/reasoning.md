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

Use `"none"` to override any view- or ledger-wide reasoning defaults for
this query (with no defaults configured, it is a no-op affirmation).

### Valid mode strings

| String | Aliases | Mode |
|--------|---------|------|
| `"rdfs"` | — | RDFS subclass/subproperty expansion |
| `"owl2ql"` | `"owl-ql"`, `"owlql"` | OWL 2 QL query rewriting (includes RDFS) |
| `"owl2rl"` | `"owl-rl"`, `"owlrl"` | OWL 2 RL forward-chaining materialization |
| `"datalog"` | — | Datalog rule execution |
| `"none"` | — | Disable all reasoning |

## Default behavior

Reasoning is **opt-in**. When the `reasoning` key is absent from a query, no
reasoning runs — even if the data contains `rdfs:subClassOf` /
`rdfs:subPropertyOf` hierarchies. Plain queries match asserted triples only,
pay no reasoning-prep cost, and behave like other SPARQL engines under simple
entailment.

To enable reasoning without setting it per query, configure a default at the
view level (`GraphDb::with_reasoning(...)` in the Rust API) or in the ledger
configuration graph (`reasoningModes`). To override such a default for a
single query, use `"reasoning": "none"`.

> **Behavior change**: versions prior to this release auto-enabled RDFS when a
> schema hierarchy existed in the data (though for imported ledgers the
> auto-detection often silently failed). If you relied on automatic subclass /
> subproperty expansion, add `"reasoning": "rdfs"` to your queries or set a
> ledger default.

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

## Inline ontology per query

In addition to ontology axioms stored in the ledger (via
`f:schemaSource`), a query can supply **inline ontology axioms**
via the top-level `ontology` field. The axioms are used only for
this query's reasoning pass and never persist.

```json
{
  "@context": {"ex": "http://example.org/ns/"},
  "select":    "?name",
  "where":     {"@id": "?p", "@type": "ex:Person", "ex:name": "?name"},
  "reasoning": "rdfs",
  "ontology": {
    "@context": {
      "ex":   "http://example.org/ns/",
      "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
    },
    "@id":             "ex:Employee",
    "rdfs:subClassOf": {"@id": "ex:Person"}
  }
}
```

Semantics:

- **Additive, not replacing.** Inline axioms layer on top of
  whatever `f:schemaSource` configured for the ledger (same- or
  cross-ledger). Both contribute to the bundle the reasoner sees.
- **Transient.** Axioms never persist. The next query without
  `ontology` runs against only the configured bundle.
- **Reasoning mode still required.** Inline axioms don't enable
  reasoning on their own — set `reasoning` so the engine actually
  uses them.
- **Namespace-scoped.** IRIs the snapshot already knows reuse
  their codes; previously-unseen IRIs allocate request-scoped
  codes that are discarded with the response — the on-disk
  dictionary is untouched.
- **No audit trail.** Without persistence, "which axioms drove
  which result" can't be reconstructed from history. Store
  long-lived ontologies in a graph and reference via
  `f:schemaSource` if auditability matters.

Use cases that fit well: testing a candidate ontology before
committing it to the ledger, per-tenant axiom layers, exploratory
analytics with hypothetical sub-class chains.

## Interaction with ledger configuration

If `f:reasoningDefaults` is set in the ledger configuration graph (see
[Setting groups](../ledger-config/setting-groups.md)), those modes are the
baseline for every query. The per-query `reasoning` parameter can:

- **Add modes** — the query modes are merged with the defaults.
- **Disable all** — `"reasoning": "none"` overrides the defaults entirely.

The `f:overrideControl` setting on the ledger config determines whether
query-time overrides are allowed. See
[Override control](../ledger-config/override-control.md) for details.

## Materialization budget

OWL 2 RL materialization runs under a budget (default: 1,000,000 derived
facts / 30 seconds). When the closure exceeds the budget it is **capped**:
the query still answers, but over an incomplete closure — results may be
missing entailments. A capped run is therefore surfaced, not just logged:

- Tracked responses (`"opts": {"meta": true}` or the `fluree-track-*`
  headers) carry a top-level `reasoning` block:

  ```json
  {
    "status": 200,
    "result": [...],
    "reasoning": {
      "capped": true,
      "capped_reason": "facts",
      "derived_facts": 1000000,
      "iterations": 3,
      "duration_ms": 12450
    }
  }
  ```

- The same JSON rides the `x-fdb-reasoning` response header.
- The server logs a WARN per capped materialization.

The budget is configurable at three levels (highest precedence first):

1. **Per query** — JSON-LD `"reasoningBudget": {"maxFacts": 20000000,
   "maxSeconds": 300}`, or SPARQL `# PRAGMA reasoning-max-facts: 20000000` /
   `# PRAGMA reasoning-max-seconds: 300`. Subject to the ledger's
   `f:overrideControl` on `f:reasoningDefaults`.
2. **Per ledger** — `f:reasoningMaxFacts` / `f:reasoningMaxSeconds` in
   `f:reasoningDefaults` (see
   [Setting groups](../ledger-config/setting-groups.md)).
3. **Server-wide** — `FLUREE_REASONING_MAX_FACTS` /
   `FLUREE_REASONING_MAX_SECONDS` environment variables.

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

## Reasoning under access policy

Reasoning composes with view policy, but mind the contract when both are on:

- **OWL 2 QL** and **RDFS** rewrite the query and execute under your identity, so they are filtered like any normal query.
- **OWL 2 RL** and **datalog** materialize derived facts into the query overlay; those facts are filtered by the **same per-flake view policy as base data**. A derived flake you may not view is dropped.
- The engine filters a derived fact by its own `(subject, predicate, object)` — **not** by the base facts it was derived from. A rule or ontology axiom can therefore re-express hidden data under a viewable predicate (e.g. `ex:ssn rdfs:subPropertyOf ex:identifier`, or a rule deriving `ex:isHighEarner` from a hidden `ex:salary`) and the derived value will surface.

**If you run reasoning under a non-root policy, your policy must cover the derived properties and classes** — deny them, or use `default-allow: false` so anything not explicitly allowed (including reasoning-introduced predicates) stays hidden.

**Query-time rules are admin-only.** Under a non-root view policy, caller-supplied `rules` are stripped before execution — a restricted caller cannot inject inference rules (a rule with a viewable head could launder hidden data). Database-stored `f:rule` definitions and OWL/RDFS reasoning are administrator-controlled and still apply. See [Policy in queries → Reasoning](../security/policy-in-queries.md#reasoning-rdfs--owl--datalog).

## Related pages

| Topic | Page |
|-------|------|
| Conceptual introduction | [Reasoning and inference](../concepts/reasoning.md) |
| Custom inference rules | [Datalog rules](datalog-rules.md) |
| Supported OWL & RDFS constructs | [OWL & RDFS reference](../reference/owl-rdfs-support.md) |
| Ledger-wide reasoning config | [Setting groups](../ledger-config/setting-groups.md) |
| Reasoning under access policy | [Policy in queries](../security/policy-in-queries.md#reasoning-rdfs--owl--datalog) |
