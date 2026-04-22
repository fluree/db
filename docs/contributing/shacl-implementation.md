# SHACL Implementation

This is the contributor-facing guide to how SHACL validation is wired into Fluree. It covers the pipeline, the crate layout, and the places you'll want to touch when fixing a bug or adding a constraint.

User-facing docs: [Cookbook: SHACL Validation](../guides/cookbook-shacl.md) and [Setting Groups — SHACL](../ledger-config/setting-groups.md#shacl-defaults).

## Pipeline at a glance

```
Transaction flakes
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ fluree-db-transact :: stage()                                   │
│   stages flakes into a LedgerView (novelty overlay)             │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ fluree-db-api :: apply_shacl_policy_to_staged_view()            │
│   (shared post-stage helper — called from every write surface)  │
│                                                                 │
│  1. load_transaction_config(ledger)                             │
│  2. build_per_graph_shacl_policy(config, graph_delta)           │
│     → HashMap<GraphId, ShaclGraphPolicy>                        │
│  3. resolve_shapes_source_g_ids(config, snapshot)               │
│     → Vec<GraphId>  (where to compile shapes from)              │
│  4. ShaclEngine::from_dbs_with_overlay(&[GraphDbRef], ledger)   │
│  5. validate_view_with_shacl(view, cache, ..., per_graph_policy)│
│     → ShaclValidationOutcome { reject, warn }                   │
│  6. log warn bucket; propagate ShaclViolation for reject bucket │
└─────────────────────────────────────────────────────────────────┘
```

## Crate layout

| Crate | Role |
|-------|------|
| `fluree-db-shacl` | SHACL engine: shape compilation, cache, per-node validation, constraint evaluators. **No transaction-layer concerns.** |
| `fluree-db-transact` | Staged-validation plumbing: `validate_view_with_shacl`, `validate_staged_nodes`. Knows about `LedgerView`, staged flakes, and graph routing. Defines the per-graph policy types. |
| `fluree-db-api` | Config resolution, policy building, and the shared helper that every write surface (JSON-LD, Turtle, commit replay) calls through. |

SHACL is feature-gated (`shacl`). See [Standards and feature flags](../reference/compatibility.md).

## The shared post-stage helper

All SHACL-enforced write surfaces route through **`apply_shacl_policy_to_staged_view`** in `fluree-db-api/src/tx.rs`:

```rust
pub(crate) async fn apply_shacl_policy_to_staged_view(
    view: &LedgerView,
    ctx: StagedShaclContext<'_>,
) -> Result<(), TransactError>
```

`StagedShaclContext` carries everything that varies between call sites:

| Field | Populated by JSON-LD txn | Populated by Turtle insert | Populated by commit replay |
|-------|-------------------------|----------------------------|----------------------------|
| `graph_delta`  | `Some(&txn.graph_delta)` (IRIs) | `None` | `Some(&routing.graph_iris)` |
| `graph_sids`   | `Some(&graph_sids)` | `None` | `Some(&routing.graph_sids)` |
| `tracker`      | `options.tracker` | `None` | `None` |

Why not fold this into `fluree-db-transact`? Config resolution (three-tier merge, override control, per-graph lookup) is API-layer policy, not a staging primitive. Keeping the helper in `tx.rs` lets `fluree-db-transact` stay focused on staging mechanics.

Call sites:
- `fluree-db-api/src/tx.rs::stage_with_config_shacl` (JSON-LD / SPARQL UPDATE txns)
- `fluree-db-api/src/tx.rs::stage_turtle_insert` (plain Turtle)
- `fluree-db-api/src/commit_transfer.rs` (push / replay)

## Config resolution

### Ledger-wide and per-graph policy

`build_per_graph_shacl_policy(config, graph_delta)` returns `Option<HashMap<GraphId, ShaclGraphPolicy>>`:

- Graphs **absent from the map** are **disabled** — their staged subjects are skipped by the validator.
- `ShaclGraphPolicy { mode: ValidationMode }` controls warn vs reject for that graph.
- The default graph (g_id=0) always gets the ledger-wide resolved policy when SHACL is enabled.
- Every graph in `graph_delta` is resolved independently via `config_resolver::resolve_effective_config(config, Some(graph_iri))`, which applies the three-tier merge (query-time → per-graph → ledger-wide) under override-control rules.
- Returns `None` when every graph resolves to disabled → the helper short-circuits before building the SHACL engine.

The transact layer's `validate_view_with_shacl` signature:

```rust
pub async fn validate_view_with_shacl(
    view: &LedgerView,
    shacl_cache: &ShaclCache,
    graph_sids: Option<&HashMap<GraphId, Sid>>,
    tracker: Option<&Tracker>,
    per_graph_policy: Option<&HashMap<GraphId, ShaclGraphPolicy>>,
) -> Result<ShaclValidationOutcome>
```

- `per_graph_policy = None`: treat every graph with staged flakes as `Reject` (legacy / shapes-exist-heuristic path).
- `per_graph_policy = Some(map)`: only graphs in the map participate; their mode drives the warn/reject split.

Output:

```rust
pub struct ShaclValidationOutcome {
    pub reject_violations: Vec<ValidationResult>,
    pub warn_violations: Vec<ValidationResult>,
}
```

The API helper logs the warn bucket and returns `TransactError::ShaclViolation` for the reject bucket.

### `f:shapesSource` resolution

`resolve_shapes_source_g_ids(config, snapshot)` in `tx.rs` is the sibling of `policy_builder::resolve_policy_source_g_ids` — identical shape, different namespace. Both:

1. Start with `[0]` (default graph) when the source field is unset.
2. Map `f:defaultGraph` → `[0]`.
3. Map a named graph IRI to its registered `GraphId` via `snapshot.graph_registry.graph_id_for_iri`.
4. Reject unsupported dimensions: `f:atT`, `f:trustPolicy`, `f:rollbackGuard`, cross-ledger `f:ledger` (these surface as `TransactError::Parse`).

`f:shapesSource` is **authoritative, not additive** — when set, shapes come exclusively from the configured graph(s). It's intentionally non-overridable at query/txn time; it can only be changed via a config-graph transaction.

## Shape compilation from multiple graphs

`ShapeCompiler::compile_from_dbs(&[GraphDbRef])` in `fluree-db-shacl/src/compile.rs` scans each input graph for every SHACL predicate (see the `shacl_predicates` list), accumulates into a single `ShapeCompiler`, then finalizes. Cross-graph `sh:and` / `sh:or` / `sh:xone` / `sh:in` list references still resolve because finalization runs once after all graphs are consumed.

`ShaclEngine::from_dbs_with_overlay(&[GraphDbRef], ledger_id)` is the corresponding engine constructor. `from_db_with_overlay(db, ledger_id)` is a single-graph convenience that delegates to the multi-graph path via `slice::from_ref(&db)`.

The engine's `SchemaHierarchy` is taken from the first graph's snapshot — hierarchy is schema-level and not graph-scoped.

## Target-type resolution

The cache (`fluree-db-shacl/src/cache.rs`) holds four indexes:

| Field | Keyed by | Used for |
|-------|----------|----------|
| `by_target_class` | class Sid (with `rdfs:subClassOf*` expansion) | `sh:targetClass` |
| `by_target_node` | subject Sid | `sh:targetNode` |
| `by_target_subjects_of` | predicate Sid | `sh:targetSubjectsOf` |
| `by_target_objects_of` | predicate Sid | `sh:targetObjectsOf` |

`ShaclEngine::validate_node` assembles applicable shapes for a focus node by:

1. `shapes_for_node(focus)` — O(1) hashmap hit.
2. `shapes_for_class(type)` for each of the focus's `rdf:type` values — O(1) per type.
3. For each key `p` in `by_target_subjects_of`: existence check `db.range(SPOT, s=focus, p=p)` — if non-empty, shape applies.
4. For each key `p` in `by_target_objects_of`: existence check `db.range(OPST, p=p, o=focus)` — if non-empty, shape applies.

Why the live db check for steps 3/4 instead of precomputed staged-flake hints? Three scenarios a hint-only approach can't cover:

- **Base-state edge**: the triggering edge is already indexed; the current txn only touches another property.
- **Retraction-only**: the staged flake set for a focus contains retractions that don't remove the last matching edge.
- **Cross-graph routing**: a subject's edge exists in graph A but we're validating the subject in graph B — the per-graph db ref sees only B.

`db.range()` returns only post-state assertions (retractions are filtered in the range pipeline — see `fluree-db-core/src/range.rs`), so the check is exactly "is this edge present in the post-txn view of this graph".

Cost is bounded by the number of predicate-targeted shapes in the cache, not by data size — typically 0–10 per ledger.

## Staged validation loop

`validate_staged_nodes` in `fluree-db-transact/src/stage.rs`:

1. Partition staged flakes into `subjects_by_graph: HashMap<GraphId, HashSet<Sid>>`.
   - Every flake's subject is added (including retractions — class/node targets still need to see them).
   - Every **assert** flake's Ref-object is also added to the graph's focus set (ensures `sh:targetObjectsOf` shapes fire on newly-referenced nodes).
2. For each `(g_id, subjects)`:
   - If `enabled_graphs` is `Some` and `g_id` is not in it: **skip**.
   - Build a per-graph `GraphDbRef` with `view` as overlay and `view.staged_t()` as `t`.
   - Attach the tracker (if any) — fuel accounting works for SHACL range scans too.
   - For each subject: fetch `rdf:type` flakes, then call `engine.validate_node(db, subject, &types)`.
   - Tag each returned `ValidationResult` with `graph_id = Some(g_id)` so the caller can partition reject vs warn.

### RDFS subclass fallback (`is_subclass_of`)

When the indexed `SchemaHierarchy` doesn't know about a `rdfs:subClassOf` edge (e.g. asserted in the same or a recent unindexed transaction), `validate_class_constraint` calls `is_subclass_of(db, start, target)` which walks `rdfs:subClassOf` upward via BFS.

Two invariants in that walk:

- **Always scope to `g_id=0`** via `rescope_to_schema_graph(db)` — schema lives in the default graph, matching how `SchemaHierarchy::from_db_root_schema` is built. Subject may be in graph G but the `subClassOf` edge must be looked up in the schema graph.
- **Preserve tracker + other `GraphDbRef` fields** — `rescope_to_schema_graph` uses `db` copy + `g_id = 0` mutation rather than `GraphDbRef::new(..)`, which would reset `tracker`, `runtime_small_dicts`, and `eager`. There's a unit test pinning this (`rescope_to_schema_graph_preserves_tracker_and_other_fields`).

## Adding a new constraint

### 1. Compile

In `fluree-db-shacl/src/compile.rs`:

- Add a variant to the `Constraint` enum (or `NodeConstraint` for node-level).
- Add the predicate name to the `shacl_predicates` array in `ShapeCompiler::compile_from_dbs`.
- Handle the predicate in `process_flake` (sets the right field on the intermediate shape builder).
- If the constraint takes arguments via an RDF list, extend `expand_rdf_lists`.

### 2. Validate

Pure per-value constraints (no db access) go in `fluree-db-shacl/src/constraints/`:

- Add a `validate_<name>(values, ..) -> Option<ConstraintViolation>` helper next to the similar ones in `cardinality.rs` / `value.rs` / etc.
- Wire it into the big match in `validate_constraint` in `fluree-db-shacl/src/validate.rs`.

Constraints that need database access (`sh:class`, pair constraints) are handled **before** the pure dispatch, inside `validate_property_shape`. Pattern:

```rust
Constraint::MyConstraint(target) => {
    let helper_violations = validate_my_constraint(db, &values, target).await?;
    for v in helper_violations {
        results.push(ValidationResult {
            focus_node: focus_node.clone(),
            result_path: Some(prop_shape.path.clone()),
            source_shape: parent_shape.id.clone(),
            source_constraint: Some(prop_shape.id.clone()),
            severity: prop_shape.severity,
            message: v.message,
            value: v.value,
            graph_id: None, // tagged later in validate_staged_nodes
        });
    }
}
```

### 3. Advertise

Update `fluree-db-shacl/src/lib.rs`:
- Add the constraint to the **Supported Constraints** list.
- Remove from the **Not Yet Supported** section if it was listed.

### 4. Test

- Add a unit test next to your `validate_<name>` helper for the pure logic.
- Add an integration test in `fluree-db-api/src/shacl_tests.rs` that transacts a shape + violating data + valid data.
- For a bug fix: temp-revert the fix, confirm the test fails, restore, confirm it passes. This pins the regression into the test.

## Testing patterns

### Integration tests

Most SHACL integration tests live in `fluree-db-api/src/shacl_tests.rs` and use the `assert_shacl_violation(err, "substring")` helper. Pattern:

```rust
let shape = json!({ /* sh:NodeShape with the constraint under test */ });
let ledger = fluree.create_ledger("shacl/foo:main").await.unwrap();
let ledger = fluree.upsert(ledger, &shape).await.unwrap().ledger;

// Negative case
let err = fluree.upsert(ledger, &violating_data).await.unwrap_err();
assert_shacl_violation(err, "expected message fragment");

// Positive case
fluree.upsert(ledger, &valid_data).await.expect("must pass");
```

### Cross-graph / per-graph tests

See `fluree-db-api/tests/it_config_graph.rs` for patterns that write config via TriG into the config graph, then stage transactions across multiple graphs. Examples:

- `shacl_shapes_source_points_to_named_graph` — `f:shapesSource` wiring.
- `shacl_per_graph_disable_honored` — per-graph `shaclEnabled: false`.
- `shacl_per_graph_mode_warn_vs_reject` — mixed modes across graphs.
- `shacl_target_subjects_of_fires_on_base_state_edge` — base-state predicate-target discovery.

### The temp-revert trick

For every correctness-fix PR, confirm the regression test actually covers the bug:

1. Apply the minimum temp-revert in the production code (comment out the fix with a `// TEMP REVERT:` marker).
2. Run the new test — it should **fail** with the expected symptom.
3. Restore the fix — test passes.
4. Commit the fix + the test together.

This is how we guard against tests that pass trivially but don't actually exercise the fix.

## Known gaps

- **`sh:uniqueLang`, `sh:languageIn`** — parsed but not evaluated. Needs language-tag metadata on flakes, which isn't yet threaded through the validation path.
- **`sh:qualifiedValueShape` (+ `sh:qualifiedMinCount` / `sh:qualifiedMaxCount`)** — parsed but not evaluated. Needs recursive nested-shape counting.
- **Cross-transaction shape cache** — every call to `from_dbs_with_overlay` recompiles from scratch. `ShaclCacheKey` has a `schema_epoch` field that's ready to drive a shared `Arc<ShaclCache>` cache on the connection, but nothing populates it yet. Low priority until perf regressions are observed.

## Where to look in the code

| What | File |
|------|------|
| Shape compilation (Turtle/JSON-LD → `CompiledShape`) | `fluree-db-shacl/src/compile.rs` |
| Shape cache with target indexes | `fluree-db-shacl/src/cache.rs` |
| Per-focus validation engine | `fluree-db-shacl/src/validate.rs` |
| Per-constraint validators (pure values) | `fluree-db-shacl/src/constraints/` |
| Staged-validation loop (per-graph) | `fluree-db-transact/src/stage.rs::validate_staged_nodes` |
| Public transact entry + outcome split | `fluree-db-transact/src/stage.rs::validate_view_with_shacl` |
| Policy types (`ShaclGraphPolicy`, `ShaclValidationOutcome`) | `fluree-db-transact/src/stage.rs` |
| Shared post-stage helper | `fluree-db-api/src/tx.rs::apply_shacl_policy_to_staged_view` |
| Per-graph policy builder | `fluree-db-api/src/tx.rs::build_per_graph_shacl_policy` |
| `f:shapesSource` resolver | `fluree-db-api/src/tx.rs::resolve_shapes_source_g_ids` |
| JSON-LD / SPARQL txn call site | `fluree-db-api/src/tx.rs::stage_with_config_shacl` |
| Turtle insert call site | `fluree-db-api/src/tx.rs::stage_turtle_insert` |
| Commit replay call site | `fluree-db-api/src/commit_transfer.rs` |
| Config field definition | `fluree-db-core/src/ledger_config.rs::ShaclDefaults` |
| Config graph parser | `fluree-db-api/src/config_resolver.rs::read_shacl_defaults` |
| Effective-config merge | `fluree-db-api/src/config_resolver.rs::merge_shacl_opts` |

## Related

- [Cookbook: SHACL Validation](../guides/cookbook-shacl.md) — user-facing usage guide
- [Setting Groups — SHACL](../ledger-config/setting-groups.md#shacl-defaults) — config reference
- [Override Control](../ledger-config/override-control.md) — three-tier precedence and monotonicity rules
- [Crate map](../reference/crate-map.md) — layering overview
