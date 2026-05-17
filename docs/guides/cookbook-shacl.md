# Cookbook: SHACL Validation

SHACL (Shapes Constraint Language) is a W3C standard for defining constraints on graph data. In Fluree, SHACL shapes are evaluated **at transaction time** — invalid data is rejected before it's committed (or logged as a warning, depending on your config).

This guide covers:

- [When SHACL runs](#when-shacl-runs) — with and without a config graph
- [Enabling SHACL via the config graph](#enabling-shacl-via-the-config-graph)
- [Defining shapes](#defining-shapes) — node shapes, property shapes, targets
- [Constraint patterns](#constraint-patterns) — cardinality, datatype, ranges, patterns, values, class, pair, logical
- [Subclass reasoning](#rdfs-subclass-reasoning-for-shclass) for `sh:class`
- [Predicate-target shapes](#predicate-target-shapes) — `sh:targetSubjectsOf` / `sh:targetObjectsOf`
- [Per-graph enable/disable and warn vs reject](#per-graph-configuration) modes
- [Storing shapes in a named graph](#storing-shapes-in-a-named-graph) with `f:shapesSource`
- [What isn't enforced yet](#not-yet-supported)

## When SHACL runs

Fluree decides whether to run SHACL validation on each transaction using this order:

1. **If a config graph exists with `f:shaclDefaults`** — follow the configured settings per graph (enable/disable, mode).
2. **If no config graph section is present** — fall back to the **shapes-exist heuristic**: if any SHACL shapes are present in the database (as regular RDF triples), validation runs in `Reject` mode. If no shapes are present, validation is skipped entirely (zero overhead).

This means you can start using SHACL **without writing any config** — just transact shapes and they're enforced.

The `shacl` feature must be enabled at build time (it's on by default for the server and CLI binaries). See [Standards and feature flags](../reference/compatibility.md).

## Enabling SHACL via the config graph

Writing ledger config is done via transactions into the **config graph**, whose IRI is always `urn:fluree:{ledger_id}#config`. See [Writing config data](../ledger-config/writing-config.md) for the full pattern.

### Minimal config: enable SHACL, shapes in the default graph

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:config:main> a f:LedgerConfig ;
    f:shaclDefaults [
      f:shaclEnabled true ;
      f:validationMode f:ValidationReject
    ] .
}
```

Notes:
- `f:shaclEnabled` defaults to `false` when a `f:shaclDefaults` section exists without it — make the enable decision explicit.
- `f:validationMode` defaults to `f:ValidationReject`. Use `f:ValidationWarn` to log violations without failing the transaction.
- With no explicit `f:shapesSource`, shapes are compiled from the **default graph** (`f:defaultGraph`, g_id=0). See [Storing shapes in a named graph](#storing-shapes-in-a-named-graph) to load from elsewhere.

## Defining shapes

Shapes are ordinary RDF — transact them like any other data. They can be written in Turtle, TriG, or JSON-LD.

### Node shape with property constraints

```turtle
@prefix sh:     <http://www.w3.org/ns/shacl#> .
@prefix schema: <http://schema.org/> .
@prefix ex:     <http://example.org/> .
@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .

ex:PersonShape a sh:NodeShape ;
  sh:targetClass schema:Person ;
  sh:property [
    sh:path schema:name ;
    sh:datatype xsd:string ;
    sh:minCount 1 ;
    sh:maxCount 1 ;
    sh:message "Every person must have exactly one name"
  ] ;
  sh:property [
    sh:path schema:email ;
    sh:datatype xsd:string ;
    sh:pattern "^[^@]+@[^@]+\\.[^@]+$" ;
    sh:message "Email must be a valid email address"
  ] ;
  sh:property [
    sh:path ex:age ;
    sh:datatype xsd:integer ;
    sh:minInclusive 0 ;
    sh:maxInclusive 200
  ] .
```

### Target types

| Target | Effect |
|--------|--------|
| `sh:targetClass <C>` | Every subject with `rdf:type <C>` (including RDFS subclasses of `<C>` when the hierarchy is available) |
| `sh:targetNode <N>` | The specific subject `<N>` |
| `sh:targetSubjectsOf <P>` | Every subject that currently has predicate `<P>` |
| `sh:targetObjectsOf <P>` | Every node that currently appears as the object of `<P>` |

See [Predicate-target shapes](#predicate-target-shapes) for notes on how the staged-path validator discovers focus nodes for `sh:targetSubjectsOf` / `sh:targetObjectsOf`.

## Constraint patterns

### Cardinality — required and multi-valued

```turtle
ex:ArticleShape a sh:NodeShape ;
  sh:targetClass ex:Article ;
  sh:property [ sh:path ex:title ; sh:minCount 1 ; sh:maxCount 1 ] ;
  sh:property [ sh:path ex:tag   ; sh:minCount 1 ] .
```

### Datatype

```turtle
ex:ProductShape a sh:NodeShape ;
  sh:targetClass ex:Product ;
  sh:property [ sh:path ex:price   ; sh:datatype xsd:decimal ] ;
  sh:property [ sh:path ex:inStock ; sh:datatype xsd:boolean ] .
```

### Numeric ranges

```turtle
ex:OrderShape a sh:NodeShape ;
  sh:targetClass ex:Order ;
  sh:property [
    sh:path ex:quantity ;
    sh:datatype xsd:integer ;
    sh:minInclusive 1 ;
    sh:maxInclusive 10000
  ] .
```

Available: `sh:minInclusive`, `sh:maxInclusive`, `sh:minExclusive`, `sh:maxExclusive`.

### String patterns and length

```turtle
ex:UserShape a sh:NodeShape ;
  sh:targetClass ex:User ;
  sh:property [
    sh:path ex:username ;
    sh:datatype xsd:string ;
    sh:minLength 3 ;
    sh:maxLength 32 ;
    sh:pattern "^[a-zA-Z0-9_]+$"
  ] .
```

`sh:pattern` accepts an optional `sh:flags` string (e.g. `"i"` for case-insensitive).

### Node kind

```turtle
ex:RefShape sh:property [
  sh:path ex:owner ;
  sh:nodeKind sh:IRI
] .
```

Values: `sh:IRI`, `sh:BlankNode`, `sh:Literal`, `sh:BlankNodeOrIRI`, `sh:BlankNodeOrLiteral`, `sh:IRIOrLiteral`.

### Enumerated values

```turtle
ex:TaskShape a sh:NodeShape ;
  sh:targetClass ex:Task ;
  sh:property [
    sh:path ex:status ;
    sh:in ( "todo" "in-progress" "review" "done" )
  ] .
```

`sh:hasValue` requires a specific value to be present.

### Class constraint (with RDFS subclass reasoning)

```turtle
ex:OrderShape a sh:NodeShape ;
  sh:targetClass ex:Order ;
  sh:property [
    sh:path ex:customer ;
    sh:class schema:Person ;
    sh:minCount 1
  ] .
```

Each value of `ex:customer` must have `rdf:type schema:Person` — or `rdf:type` of any class that is `rdfs:subClassOf* schema:Person`. See [RDFS subclass reasoning for sh:class](#rdfs-subclass-reasoning-for-shclass).

### Pair constraints — comparing two properties

```turtle
ex:EventShape a sh:NodeShape ;
  sh:targetClass ex:Event ;
  sh:property [
    sh:path ex:startYear ;
    sh:lessThan ex:endYear
  ] ;
  sh:property [
    sh:path ex:primaryEmail ;
    sh:disjoint ex:secondaryEmail
  ] .
```

| Constraint | Semantic |
|-----------|----------|
| `sh:equals <P>` | Value sets for this path and `<P>` must be identical |
| `sh:disjoint <P>` | Value sets must not overlap |
| `sh:lessThan <P>` | Every value on this path must be strictly less than every value of `<P>` |
| `sh:lessThanOrEquals <P>` | Every value on this path must be ≤ every value of `<P>` |

### Logical constraints

```turtle
ex:ContactShape a sh:NodeShape ;
  sh:targetClass ex:Contact ;
  sh:or (
    [ sh:property [ sh:path schema:email     ; sh:minCount 1 ] ]
    [ sh:property [ sh:path schema:telephone ; sh:minCount 1 ] ]
  ) .
```

Available: `sh:not`, `sh:and`, `sh:or`, `sh:xone`.

### Closed shapes

```turtle
ex:StrictPersonShape a sh:NodeShape ;
  sh:targetClass ex:StrictPerson ;
  sh:closed true ;
  sh:ignoredProperties ( rdf:type ) ;
  sh:property [ sh:path schema:name ; sh:minCount 1 ] .
```

A closed shape forbids any property not explicitly declared (or listed in `sh:ignoredProperties`). `rdf:type` is implicitly ignored per the SHACL spec.

## RDFS subclass reasoning for `sh:class`

`sh:class` honors `rdfs:subClassOf`. Example:

```turtle
ex:Novelist rdfs:subClassOf schema:Person .
ex:pratchett rdf:type ex:Novelist .

ex:BookShape sh:property [
  sh:path ex:author ;
  sh:class schema:Person
] .
```

A book whose `ex:author` is `ex:pratchett` conforms — `ex:pratchett` is a `schema:Person` via `rdfs:subClassOf`.

Fluree resolves this in two tiers:

1. **Fast path**: the ledger's indexed schema hierarchy (`SchemaHierarchy`). Expanded at engine build time so same-class and descendant-class matches are O(1) hashmap hits.
2. **Live fallback**: when the subclass relation was asserted in the current transaction (or any earlier unindexed commit), the fast path misses. The engine then walks `rdfs:subClassOf` via a BFS on the database's SPOT index. This walk is **scoped to the default graph** regardless of the subject's own graph — matching how `SchemaHierarchy` is built and preventing cross-graph issues.

## Predicate-target shapes

`sh:targetSubjectsOf(P)` and `sh:targetObjectsOf(P)` depend on the current state of the database — a subject is a focus node iff it actually has (or is referenced by) predicate `P` in the **post-transaction** view.

Fluree does not precompute target hints from staged flakes. Instead, for each focus node being validated, the engine does a bounded existence check against the post-state:

- `sh:targetSubjectsOf(P)` → SPOT range query `(focus, P, _)`. Non-empty → shape applies.
- `sh:targetObjectsOf(P)` → OPST range query `(_, P, focus)`. Non-empty → shape applies.

This means:
- A **base-state** `(alice, ex:ssn, "123")` makes `sh:targetSubjectsOf(ex:ssn)` fire on alice even when this transaction only retracts `ex:name`.
- A **retraction-only** transaction that removes the last matching edge means the shape no longer applies — the post-state check returns empty.
- The check is **bounded** by the number of predicate-targeted shapes in the cache, not the data size.

Ref-objects of asserted flakes are pulled into the focus set for their graph, so newly-introduced inbound edges trigger validation of the referenced node.

## Per-graph configuration

Each named graph can have its own `f:shaclEnabled` and `f:validationMode` via `f:graphOverrides`:

```trig
@prefix f: <https://ns.flur.ee/db#> .
@prefix ex: <http://example.org/> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:config:main> a f:LedgerConfig ;
    # Ledger-wide: SHACL on, reject on violation.
    f:shaclDefaults [
      f:shaclEnabled true ;
      f:validationMode f:ValidationReject ;
      f:overrideControl f:OverrideAll
    ] ;
    # Per-graph: ex:scratch has SHACL off; ex:audit uses warn mode.
    f:graphOverrides
      [ a f:GraphConfig ;
        f:targetGraph ex:scratch ;
        f:shaclDefaults [ f:shaclEnabled false ]
      ],
      [ a f:GraphConfig ;
        f:targetGraph ex:audit ;
        f:shaclDefaults [ f:validationMode f:ValidationWarn ]
      ] .
}
```

With this config:

- A violating write to the **default graph** is **rejected** (ledger-wide `Reject`).
- A violating write to `ex:scratch` **passes** without validation (graph disabled).
- A violating write to `ex:audit` **passes** but emits a `tracing::warn!` (`Warn` mode).
- A single multi-graph transaction can mix modes: reject-bucket violations fail the txn; warn-bucket violations get logged.

### Monotonicity

Per-graph configs can only **tighten** the ledger-wide posture:

| Ledger-wide | Per-graph | Effective |
|-------------|-----------|-----------|
| `enabled: false`, `OverrideNone` | `enabled: true` | **disabled** (OverrideNone blocks per-graph) |
| `enabled: true`, `OverrideAll` | `enabled: false` | **disabled** for that graph |
| `mode: warn`, `OverrideAll` | `mode: reject` | **reject** for that graph |

See [Override control](../ledger-config/override-control.md) for the full ruleset.

## Storing shapes in a named graph

`f:shapesSource` points the shape compiler at a specific graph. Useful when you want schema / shapes isolated from data — even the config graph itself can be used as a shape source.

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:config:main> a f:LedgerConfig ;
    f:shaclDefaults [
      f:shaclEnabled true ;
      f:shapesSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector <http://example.org/shapes> ]
      ]
    ] .
}
```

Semantics:

- `f:shapesSource` is **authoritative, not additive**: when set, shapes come exclusively from the configured graph. Shapes in the default graph are ignored.
- `f:shapesSource` is **non-overridable** — it can only be set in the config graph, not via transaction/query-time options.
- Use `f:graphSelector f:defaultGraph` to explicitly point at the default graph (same as omitting `f:shapesSource`).
- `f:shapesSource` also supports **cross-ledger references** — set `f:ledger` on the inner `f:graphSource` to compile shapes from a different ledger at validation time. See [Cross-ledger governance — Cross-ledger SHACL shapes](../security/cross-ledger-policy.md#cross-ledger-shacl-shapes) for the end-to-end pattern.

## Inline shapes per transaction

In addition to shapes stored in a ledger, a transaction can supply
**inline shapes** via the `opts.shapes` field. The shapes are
enforced only for that one transaction and never written into the
ledger.

```json
{
  "@context": {"ex": "http://example.org/ns/"},
  "@id":   "ex:alice",
  "@type": "ex:Person",
  "opts": {
    "shapes": {
      "@context": {
        "ex":  "http://example.org/ns/",
        "sh":  "http://www.w3.org/ns/shacl#",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
      },
      "@graph": [
        {
          "@id":            "ex:PersonShape",
          "@type":          "sh:NodeShape",
          "sh:targetClass": {"@id": "ex:Person"},
          "sh:property":    {"@id": "ex:pshape_name"}
        },
        {
          "@id":         "ex:pshape_name",
          "sh:path":     {"@id": "ex:name"},
          "sh:minCount": 1,
          "sh:datatype": {"@id": "xsd:string"}
        }
      ]
    }
  }
}
```

Semantics:

- **Additive with the configured source.** Inline shapes enforce
  *alongside* whatever `f:shapesSource` resolves to. A subject
  must satisfy every shape from both sources. Note that
  `f:shapesSource` is itself singular — its `f:graphSource` is
  either local (no `f:ledger`) or cross-ledger (with `f:ledger`),
  not both at the same time. Inline shapes don't change that;
  they layer on top of whichever variant is configured.
- **Transient.** The shapes never appear in the ledger's data and
  vanish after the transaction completes. The next transaction
  without `opts.shapes` runs without them.
- **Gated by config.** If `f:shaclEnabled false` (or no graph is
  enabled), inline shapes do not bypass that posture — operator
  config wins. To use inline shapes on a fresh ledger with no
  config, the shapes-exist heuristic enables validation
  automatically.
- **No audit trail.** Because inline shapes don't persist, it
  isn't possible to reconstruct "which shapes validated which
  commit" from ledger history. If auditability matters, store
  shapes in a `f:shapesSource` graph instead.

Use cases that fit well: ad-hoc shape testing before committing
to `f:shapesSource`, per-tenant validation layers in an
application server, request-scoped governance that should not
become part of permanent ledger state.

## Validation modes

- **`f:ValidationReject`** (default): on any violation, the transaction fails with `ShaclViolation(report)`. The formatted report lists each violation's focus node, property path, and message.
- **`f:ValidationWarn`**: violations are logged via `tracing::warn!` and the transaction proceeds. Any **non-violation** error from the SHACL pipeline (compile failure, range-scan failure) still propagates — Warn mode never silently admits a broken validation pipeline.

## Working with shapes across write surfaces

SHACL validation runs consistently on every write surface:

- JSON-LD / SPARQL transactions (`fluree insert`, `fluree upsert`, `fluree update`)
- Turtle / TriG ingest (`fluree insert-turtle`, `stage_turtle_insert`)
- Commit replay (`push_commits_with_handle`, followers applying upstream commits)

All three routes go through the same post-stage helper, so the ledger's configured SHACL posture (enable/disable, mode, per-graph, shapes source) applies uniformly.

## Not yet supported

The following SHACL constructs are parsed/compiled but currently **no-ops** at validation time. Shapes using them load without error but don't constrain data:

- `sh:uniqueLang`, `sh:languageIn` — require language-tag metadata on flakes, which isn't yet threaded through the validation path.
- `sh:qualifiedValueShape` (+ `sh:qualifiedMinCount` / `sh:qualifiedMaxCount`) — requires recursive nested-shape counting.

These are tracked in the SHACL compliance effort. Contributors: see [Contributing / SHACL implementation](../contributing/shacl-implementation.md).

## Shapes are data

Because shapes live as regular RDF in your ledger:

- **Time-travelable** — `@atT` query any shape's history to see what validation was in effect at a given commit.
- **Versionable** — `delete`/`insert` constraints through ordinary transactions.
- **Queryable** — `SELECT ?shape ?target WHERE { ?shape sh:targetClass ?target }`.
- **Branchable** — test new constraints on a branch; merge when verified.

## Best practices

1. **Start with `sh:minCount`** — missing-value bugs are the most common data quality issue.
2. **Incremental rollout** — deploy shapes in `f:ValidationWarn` mode first. Watch the logs for a sprint, then flip to `f:ValidationReject`.
3. **Per-graph scratch zones** — for experimentation, disable SHACL on a named graph so exploratory transactions don't fail your CI.
4. **`sh:message` everywhere** — custom messages are what end users see when a transaction is rejected. Invest in them early.
5. **`f:shapesSource` for schema hygiene** — keep shapes out of user data graphs so deletes / retractions on user data can't accidentally touch your schema.

## Related documentation

- [Setting Groups — SHACL](../ledger-config/setting-groups.md#shacl-defaults) — Configuration reference for `f:shaclDefaults`
- [Override Control](../ledger-config/override-control.md) — Per-graph / query-time override rules
- [Writing Config Data](../ledger-config/writing-config.md) — How to transact into the config graph
- [Contributing / SHACL implementation](../contributing/shacl-implementation.md) — How the pipeline works internally (for contributors)
