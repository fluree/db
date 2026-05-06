# Reasoning and Inference

Fluree includes a built-in reasoning engine that can derive new facts from your
data based on ontology declarations (RDFS and OWL) or user-defined rules
(Datalog). This page introduces the core concepts; see
[Query-time reasoning](../query/reasoning.md) for usage syntax,
[Datalog rules](../query/datalog-rules.md) for custom rules, and the
[OWL & RDFS reference](../reference/owl-rdfs-support.md) for a full list of
supported constructs.

## Why reasoning?

In a plain triple store every fact must be stated explicitly. If you assert that
Alice is a `Student` and that `Student` is a subclass of `Person`, a query for
all `Person` instances will *not* return Alice — unless you also assert
`Alice rdf:type Person`.

With reasoning enabled, Fluree can **infer** the missing fact automatically:

```
Alice  rdf:type  Student       (asserted)
Student  rdfs:subClassOf  Person   (schema)
────────────────────────────────────────────
Alice  rdf:type  Person        (inferred)
```

This keeps your data clean (no redundant assertions) while giving your queries
the full power of schema-aware retrieval.

## Reasoning modes

Fluree supports four reasoning profiles that can be enabled independently or in
combination. They are listed here from lightest to most powerful:

| Mode | What it does | Cost |
|------|-------------|------|
| **RDFS** | Expands `rdfs:subClassOf` and `rdfs:subPropertyOf` hierarchies so that querying for a superclass or superproperty also returns instances of its subclasses/subproperties. | Very low — query rewriting only, no materialization. |
| **OWL 2 QL** | Everything RDFS does, plus `owl:inverseOf` expansion and `rdfs:domain`/`rdfs:range` type inference via query rewriting. Based on the OWL 2 QL profile designed for query answering. | Low — query rewriting only. |
| **OWL 2 RL** | Forward-chaining materialization of a comprehensive rule set (symmetric, transitive, and inverse properties; functional properties; property chains; class restrictions; `owl:sameAs` equivalence; and more). See the [OWL & RDFS reference](../reference/owl-rdfs-support.md) for the full list. | Medium — derives facts before query execution; results are cached. |
| **Datalog** | User-defined if/then rules expressed in a familiar JSON-LD pattern syntax. Rules run in a fixpoint loop and can chain off each other or off OWL-derived facts. See [Datalog rules](../query/datalog-rules.md). | Depends on the rules — can be lightweight or heavy. |

### Combining modes

Modes can be combined freely. For example, `["rdfs", "owl2rl", "datalog"]`
first materializes OWL 2 RL entailments, then runs your Datalog rules over the
combined base + OWL-derived data, and finally applies RDFS query rewriting on
top. This layering lets you start simple (RDFS) and add more powerful inference
only where you need it.

## How it works

Fluree uses two complementary techniques depending on the mode:

### Query rewriting (RDFS, OWL 2 QL)

The query planner rewrites your patterns at compile time. For example, a
`?x rdf:type ex:Person` pattern is expanded into a UNION over `Person` and all
of its subclasses. No extra data is stored; the rewriting is transparent to the
caller.

### Forward-chaining materialization (OWL 2 RL, Datalog)

Before your query runs, the engine:

1. **Loads the ontology** — extracts OWL/RDFS declarations (property types,
   class hierarchies, restrictions) from your data.
2. **Applies rules in a fixpoint loop** — each iteration derives new facts from
   the combination of asserted and previously-derived facts. The loop stops when
   no new facts are produced (fixpoint) or a budget limit is reached.
3. **Overlays derived facts** — the inferred triples are layered on top of your
   base data as a read-only overlay. Your original data is never modified.
4. **Caches the result** — if the same database state is queried again with the
   same reasoning modes, the cached materialization is reused instantly.

### Budget controls

To guarantee termination, materialization enforces configurable limits:

| Limit | Default | What happens when exceeded |
|-------|---------|--------------------------|
| Time | 30 seconds | Materialization stops; partial results used |
| Derived facts | 1,000,000 | Materialization stops; partial results used |
| Memory | 100 MB | Materialization stops; partial results used |

When a budget is exceeded the query still runs — it simply uses whatever facts
were derived before the limit was hit. Diagnostics are available via tracing
spans to identify when capping occurs.

## Enabling reasoning

There are two levels of control:

### 1. Ledger-wide defaults (configuration graph)

Set reasoning defaults so every query against a ledger uses a particular mode
without having to specify it each time:

```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "insert": {
    "@id": "urn:fluree:mydb:main:config:ledger",
    "@type": "f:LedgerConfig",
    "f:reasoningDefaults": {
      "f:reasoningModes": {"@id": "f:RDFS"},
      "f:overrideControl": {"@id": "f:OverrideAll"}
    }
  }
}
```

See [Setting groups — reasoningDefaults](../ledger-config/setting-groups.md)
for full configuration options.

### 2. Per-query override

Any query can specify or override the reasoning mode:

```json
{
  "select": ["?s"],
  "where": {"@id": "?s", "@type": "ex:Person"},
  "reasoning": "rdfs"
}
```

Use `"reasoning": "none"` to explicitly disable reasoning for a single query,
even if the ledger has defaults configured.

See [Query-time reasoning](../query/reasoning.md) for complete syntax and
examples.

## Key concepts

### Schema as data

Unlike systems with external schema files, Fluree stores ontology declarations
as regular triples in your graph. An `rdfs:subClassOf` assertion is just another
triple — you add it via a normal transaction:

```json
{
  "@context": {
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "ex": "http://example.org/"
  },
  "insert": {
    "@id": "ex:Student",
    "rdfs:subClassOf": {"@id": "ex:Person"}
  }
}
```

This means your schema evolves with your data, is time-travelable, and is
subject to the same policy controls as any other data.

### Derived facts are virtual

Inferred triples exist only in a query-time overlay — they are never written to
storage. This means:

- **No storage bloat** — you don't pay disk costs for derived facts.
- **Always consistent** — derived facts are recomputed from the current state,
  so they can never go stale.
- **Time-travel safe** — querying a historical point in time materializes based
  on that point's data and schema.

### owl:sameAs and identity

When OWL 2 RL is enabled, the engine tracks `owl:sameAs` equivalences using an
efficient union-find data structure. If two resources are determined to be the
same (via functional properties, inverse functional properties, or `owl:hasKey`),
all their facts are merged under a canonical representative. Queries
transparently resolve through these equivalences.

## What to read next

| Topic | Page |
|-------|------|
| Using reasoning in queries | [Query-time reasoning](../query/reasoning.md) |
| Writing custom inference rules | [Datalog rules](../query/datalog-rules.md) |
| Full list of supported OWL & RDFS constructs | [OWL & RDFS reference](../reference/owl-rdfs-support.md) |
| Configuring ledger-wide defaults | [Setting groups](../ledger-config/setting-groups.md) |
