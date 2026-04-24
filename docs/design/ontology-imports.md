# Ontology imports (`f:schemaSource` + `owl:imports`)

Reasoning in Fluree needs to see a ledger's **ontology** — class and
property hierarchies, OWL axioms — even when those triples don't live in
the same graph as the instance data being queried. This document describes
how that binding is configured, resolved, and plumbed into the reasoning
pipeline.

Topics:

- Config-layer contract (`f:schemaSource`, `f:followOwlImports`,
  `f:ontologyImportMap`).
- Resolution algorithm for the `owl:imports` closure.
- `SchemaBundleOverlay` — how the resolved closure is presented to the
  reasoner without changing reasoner internals.
- Caching, error semantics, and the schema-triple whitelist.

Related docs:

- [Query execution and overlay merge](query-execution.md)
- [Reasoning and inference](../concepts/reasoning.md)

## Configuration

Reasoning config is declared in the ledger's config graph (`g_id=2`), on the
`f:LedgerConfig` resource's `f:reasoningDefaults`. Three fields drive
ontology resolution:

```turtle
@prefix f:    <https://ns.flur.ee/db#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .

GRAPH <urn:fluree:myapp:main#config> {
  <urn:myapp:config> a f:LedgerConfig ;
    f:reasoningDefaults <urn:myapp:config:reasoning> .

  <urn:myapp:config:reasoning>
    f:reasoningModes ( "rdfs" "owl2-rl" ) ;
    f:schemaSource <urn:myapp:config:schema-ref> ;
    f:followOwlImports true ;
    f:ontologyImportMap <urn:myapp:config:bfo-binding> .

  <urn:myapp:config:schema-ref> a f:GraphRef ;
    f:graphSource <urn:myapp:config:schema-source> .
  <urn:myapp:config:schema-source>
    f:graphSelector <http://example.org/ontology/core> .

  <urn:myapp:config:bfo-binding>
    f:ontologyIri <http://purl.obolibrary.org/obo/bfo.owl> ;
    f:graphRef   <urn:myapp:config:bfo-ref> .
  <urn:myapp:config:bfo-ref> a f:GraphRef ;
    f:graphSource <urn:myapp:config:bfo-source> .
  <urn:myapp:config:bfo-source>
    f:graphSelector <http://example.org/ontology/local/bfo> .
}
```

Field reference:

| Field                    | Type                            | Meaning |
|--------------------------|---------------------------------|---------|
| `f:schemaSource`         | `f:GraphRef`                    | Starting graph for schema extraction. When absent, reasoning uses the default graph directly. |
| `f:followOwlImports`     | `xsd:boolean`                   | When `true`, resolve the transitive closure of `owl:imports` triples starting from `f:schemaSource`. When absent or `false`, the bundle contains only the starting graph. |
| `f:ontologyImportMap`    | list of `OntologyImportBinding` | Mapping table from external ontology IRIs to local graphs. Consulted when an `owl:imports` IRI doesn't match a named graph in the current ledger. |

An `OntologyImportBinding` has two fields:

- `f:ontologyIri` — the IRI that appears in `owl:imports` statements.
- `f:graphRef` — a nested `f:GraphRef` identifying the local graph.

The `GraphRef` shape supported for `f:schemaSource` and
`f:ontologyImportMap.graphRef` is the same-ledger shape:
`f:graphSelector` naming a local named graph, `f:defaultGraph`, or a
registered graph IRI. References are resolved at the query's effective
`to_t` — every named graph in a Fluree ledger shares the ledger's
monotonic `t`, so the entire closure is consistent at a single point in
time without per-import bookkeeping.

## Resolution algorithm

For each `owl:imports <X>` triple discovered while walking the closure, the
resolver (`fluree_db_api::ontology_imports::resolve_schema_bundle`) applies
this order:

1. **Named-graph match** — if `<X>` is registered as a graph IRI in the
   current ledger's [`GraphRegistry`], resolve to that `GraphId`.
2. **Mapping-table fallback** — if `<X>` appears in `f:ontologyImportMap`,
   resolve via the bound `GraphSourceRef`.
3. **Strict error** — otherwise, fail the query with
   `ApiError::OntologyImport`. There is no silent skip.

The walk is BFS, deduplicated by resolved `GraphId`, and cycle-safe by
construction (we only push unseen IDs onto the queue). The result is a
`ResolvedSchemaBundle { ledger_id, to_t, sources: Vec<GraphId> }`.

### System graphs are off-limits

Imports resolving to `CONFIG_GRAPH_ID` (g_id=2) or `TXN_META_GRAPH_ID`
(g_id=1) are rejected — those graphs are structurally reserved and would
leak framework triples into reasoning. The guard sits in the single
`resolve_local_graph_source` chokepoint, so **every** resolution path
(direct graph-IRI match, `f:ontologyImportMap` entry, `f:schemaSource`
selector) is covered.

### `owl:imports` discovery is subject-wildcarded

Every `?s owl:imports ?o` triple in a schema graph is treated as
authoritative, regardless of whether `?s` is typed `owl:Ontology`. This is
broader than strict OWL 2 (which restricts `owl:imports` to the ontology
header) and matches real-world OWL inputs that rely on file-level
provenance. The resolution layer's strictness still applies: a stray
`owl:imports` triple that doesn't map to a local graph fails the query
rather than silently expanding the closure.

### Reasoning-disabled queries don't trigger resolution

Queries that opt out of reasoning (`"reasoning": "none"`) skip bundle
resolution entirely — a broken ontology import in the ledger's config
shouldn't produce errors for a non-reasoning workload. The short-circuit
lives in `attach_schema_bundle` (both the single-view and dataset paths).

## Projecting the bundle into reasoning

RDFS and OWL extraction code reads schema triples out of the default graph
(`g_id=0`). The resolver feeds that code via a
[`SchemaBundleOverlay`](../../fluree-db-query/src/schema_bundle.rs) that
**projects** whitelisted triples from every bundle source onto `g_id=0`,
so the reasoner sees the full closure without being aware of it.

The projection happens in two phases:

1. **Materialize.** `build_schema_bundle_flakes` runs targeted reads against
   every source graph — one PSOT scan per schema predicate and one OPST
   scan per schema class — and collects the matching flakes into per-index
   sorted arrays (SPOT / PSOT / POST / OPST). Reads go through the normal
   `range_with_overlay` path, so both committed index data and novelty are
   visible.
2. **Overlay.** `SchemaBundleOverlay::new(base_overlay, flakes)` wraps the
   query's base overlay. For `g_id != 0` it delegates straight to the
   base. For `g_id == 0` it emits a linear merge of base flakes and
   bundle flakes in index order.

The reasoner sees: base default-graph flakes ∪ projected schema flakes,
presented as a single ordered stream at `g_id=0`. Reasoner code is
unmodified.

### Schema-triple whitelist

Only the following predicates are eligible for projection:

- **RDFS:** `rdfs:subClassOf`, `rdfs:subPropertyOf`, `rdfs:domain`, `rdfs:range`
- **OWL:** `owl:inverseOf`, `owl:equivalentClass`, `owl:equivalentProperty`,
  `owl:sameAs`, `owl:imports`

And `rdf:type` triples are projected **only when the object is** one of:
`owl:Class`, `owl:ObjectProperty`, `owl:DatatypeProperty`,
`owl:SymmetricProperty`, `owl:TransitiveProperty`, `owl:FunctionalProperty`,
`owl:InverseFunctionalProperty`, `owl:Ontology`, `rdf:Property`.

Anything else in an import graph — in particular, instance data —
**does not surface** in the reasoner's view. See
`fluree_db_core::{is_schema_predicate, is_schema_class}` for the canonical
checks and
`fluree-db-api/tests/it_reasoning_imports.rs::instance_data_in_schema_graph_does_not_leak`
for the regression test.

## Caching

`global_schema_bundle_cache()` is a process-wide `moka::sync::Cache` keyed
by:

- `ledger_id: Arc<str>`
- `to_t: i64`
- `starting_g_id: GraphId` (the resolved `f:schemaSource`)
- `follow_imports: bool`

Because config lives in the same ledger (g_id=2) and any config change
advances `t`, the `to_t` dimension is sufficient to express "config
version" — there is no separate config_epoch key, and no explicit
invalidation logic. Stale entries age out via LRU.

The cache stores the **resolution result** (`Vec<GraphId>`); the projected
flake arrays are rebuilt per query. Materialization is cheap relative to
reasoning itself, and keeping the cached value small lets many entries
coexist for many ledgers without memory pressure.

## Error semantics

`ApiError::OntologyImport` is raised when the configured closure is
invalid. Every message identifies the offending resource and suggests
remediation. Queries fail rather than silently returning reduced results,
so broken ontology references surface early. Sources of this error:

- An `owl:imports <X>` that doesn't match a local named graph and has no
  `f:ontologyImportMap` entry.
- A resolution that would land on a reserved system graph (config or
  txn-meta), whether via direct graph-IRI match, mapping table, or
  `f:schemaSource` selector.
- A `GraphRef` that targets a different ledger, uses `f:atT`, or carries a
  `f:trustPolicy` / `f:rollbackGuard`. The bundle is resolved at the
  query's single `to_t`, same-ledger scope only, and accepting these
  fields silently would create a gap between declared intent and actual
  behavior.

## Wiring at query time

`Fluree::query(&db, ...)` (and the dataset-query counterpart) call
`build_executable_for_view` → `attach_schema_bundle` on every query. The
attach step:

1. Reads `db.resolved_config().reasoning`. If there is no `f:schemaSource`,
   returns immediately — the legacy default-graph path applies unchanged.
2. Calls `resolve_schema_bundle` for the closure, consulting the cache.
3. Materializes `SchemaBundleFlakes` via `build_schema_bundle_flakes`.
4. Sets `executable.options.schema_bundle` so `prepare_execution` wraps
   `db.overlay` in a `SchemaBundleOverlay` for the reasoning_prep block.

Downstream, `schema_hierarchy_with_overlay`, `reason_owl2rl`, and
`Ontology::from_db_with_overlay` all receive the same wrapped overlay and
see the full closure on `g_id=0` reads.

## Testing

The acceptance suite lives in
`fluree-db-api/tests/it_reasoning_imports.rs` and covers:

- Same-ledger auto resolution of a named schema source.
- Transitive `A → B` with a subclass edge in `B`.
- Mapping table fallback for external IRIs.
- Unresolved imports surface as `ApiError::OntologyImport`.
- Cycle `A → B → A` terminates and still yields the correct closure.
- Mapping entries that would target a reserved system graph are rejected.
- `"reasoning": "none"` queries skip resolution entirely (no spurious
  errors from unrelated config).
- `f:atT` on a `GraphRef` is rejected with a clear message.
- Instance data in the schema graph does **not** leak into query results.
- **End-to-end OWL2-RL rule firing through a transitive import:**
  `owl:TransitiveProperty`, `owl:inverseOf`, and `rdfs:domain` axioms
  declared in an imported graph produce the expected entailments against
  instance data in the default graph.

Module-level unit tests cover the cache keys, empty-bundle passthrough,
and non-default-graph delegation.
