# Burn-down: named-graph (`GRAPH`) + dataset (`FROM`/`FROM NAMED`) semantics

Pre-implementation deep audit for audit §4.2 (item "Named-graph data / GRAPH ?g
binding") and §5.3 (dataset strategy). Covers the `/graph/` and `/dataset/`
entries of register `SPARQL10_QUERY_EVAL` (24 tests), plus `SPARQL11_BINDINGS`
entry `graph` and `SPARQL11_EXISTS` entry `exists-graph-variable`.

**Scope:** one small correctness cluster in the correlated `GRAPH` operator
(binds as literal, leaks the default graph, drops the graph-variable join) and
one design decision (single-ledger `FROM`/`FROM NAMED`). No source was modified
during this audit. Every claim below was verified against the code and, for the
two non-obvious cases, reproduced with the built `run-w3c-test` subprocess
binary driven by a hand-written `TestDescriptor`. Baseline: branch
`test/sparql-testsuite-full-coverage`, rdf-tests submodule `efccbc6b8`,
2026-07-06.

Key code: the whole `GRAPH` cluster lives in one operator,
`fluree-db-query/src/graph.rs` (`GraphOperator`); the dataset rejection is one
guard, `fluree-db-api/src/view/query.rs:556` (`validate_sparql_for_view`).

---

## 1. Confirmed root causes of finding (a) + per-test verdicts

Finding (a) — "`GRAPH ?g` binds `?g` for default-graph triples too, and binds it
as a plain string literal of the ledger alias" — is **two independent defects**
in `GraphOperator`, plus a **third** that the audit did not name (the
graph-variable join) and a **fourth/fifth** that surface only on the two
SPARQL 1.1 tests. All are in `fluree-db-query/src/graph.rs`.

### BUG-1 — `?g` is materialized as an `xsd:string` literal, not an IRI

`GraphOperator::execute_in_graph`, **graph.rs:314-323**, binds the graph
variable like this:

```rust
if bind_graph_var == Some(*var) {
    // Bind ?g to graph IRI using xsd:string
    let binding = Binding::Lit {
        val: FlakeValue::String(graph_iri.to_string()),
        dtc: DatatypeConstraint::Explicit(self.well_known.xsd_string.clone()),
        t: None, op: None, p_id: None,
    };
    merged_row.push(binding);
}
```

This is **materialization-time**, not a formatter problem. `Binding::Lit{String}`
renders as `{"type":"literal",…}` (`fluree-db-api/src/format/sparql.rs:333`),
whereas `Binding::Iri` renders as `{"type":"uri",…}` (sparql.rs:332, 497). The
harness therefore sees `Literal{value:"…data-g1.ttl"}` where the W3C `.ttl`
result declares `Iri(…data-g1.ttl)` (dawg-graph-03 diff). The value string is
correct — the graph registry round-trips the name faithfully; only the **term
kind** is wrong. `Binding::Iri(Arc<str>)` already exists (binding.rs:98) with a
constructor `Binding::iri()` (binding.rs:512) and is the one-line fix.

The module doc (graph.rs:10) enshrines the wrong behavior: *"?g is bound as
`Binding::Lit { val: FlakeValue::String(...), dtc: Explicit(xsd:string) }`"*.

### BUG-2 — the default graph leaks into `GRAPH ?g` (intentional extension, W3C-breaking)

In the unbound `GRAPH ?g` arm, single-db path, **graph.rs:686-711**:

```rust
// Single-db: bind ?g to each registered user graph …, then to the ledger
// alias for the default graph.
for iri in ctx.single_db_user_graph_iris() {
    self.execute_in_graph(ctx, &parent_batch, row_idx, iri, Some(*var)).await?;
}
let alias_iri: Arc<str> = Arc::from(ctx.active_snapshot.ledger_id.as_str());
self.execute_in_graph(ctx, &parent_batch, row_idx, alias_iri, Some(*var)).await?;  // <-- leak
```

The trailing `execute_in_graph(alias_iri, …)` enumerates the **default graph**
as if it were a named graph called `w3c:test` (the ledger alias). SPARQL
requires `GRAPH ?g` to range over **named graphs only**
(https://www.w3.org/TR/sparql11-query/#queryDataset). This is the source of
every "extra rows / `g: Literal{"w3c:test"}`" failure.

**This is a deliberate, documented, regression-tested Fluree extension**, not an
accident. It was added by issue #1279 ("resolve ledger named graphs in single-db
GRAPH patterns"). Two existing tests assert it:

- `fluree-db-api/tests/it_query_dataset.rs:1591`
  `sparql_single_db_graph_variable_unbound` — asserts `GRAPH ?g` binds `?g` to
  the alias `"people:main"` (and, incidentally, as a JSON string, encoding
  BUG-1 too).
- `it_query_dataset.rs:1752`
  `sparql_single_db_graph_variable_discovers_user_graphs` — comment literally
  says *"discovers user-registered named graphs (plus the ledger alias for the
  default graph)"* and cites "open decision #1".

So the fix is a **behavior change the team already flagged as open**, and it
requires updating those two tests. The concrete/bound forms (`GRAPH <alias>`,
`VALUES ?g { "alias" } GRAPH ?g`) at graph.rs:597-623 / 644-670 also treat
`is_alias` as a valid graph, but **no W3C test binds `?g` to, or names, the
ledger alias**, so those forms are W3C-safe and can stay as a benign
opt-in (see §3).

### BUG-3 — the graph variable is overwritten, never unified with inner uses

Still in the merge loop (graph.rs:312-342): for every output var that equals the
graph var, the operator **unconditionally pushes the graph IRI**, discarding
whatever the inner pattern bound to that same variable. `?g` is **not seeded**
into the inner subplan (the seed is only the parent row, graph.rs:259). So for
`GRAPH ?g { ?g :p ?o }` the inner scans `?g :p ?o` with `?g` free (matches every
subject with `:p`), then the merge overwrites the subject with the graph name —
producing rows where subject ≠ graph name. SPARQL treats the two occurrences of
`?g` as one variable: binding it to the graph name must constrain the inner
pattern. This breaks `graph-variable-join` and `graph-optional`.

### BUG-4 — a zero-variable existence solution is dropped downstream

`GRAPH <data-g1.ttl> {}` (ground body, empty projection) must yield exactly one
empty solution when the graph exists. `GraphOperator` deliberately preserves it
(graph.rs:482-489, `Batch::empty_schema_with_len`), and the sibling
variable-form `GRAPH ?g {}` returns its rows — so the operator is correct. The
one empty-schema row is lost **downstream of `GraphOperator`** (projection /
`SELECT *` serialization collapsing a 0-column, N-row batch to 0 rows). Probe
(`run-w3c-test`, graph-exist): `Expected 1 solution(s), got 0`, `Actual vars:
[]`. This is a narrow, distinct bug (empty-projection existence rows), not the
`GRAPH` binding at all. Exact drop point (projection operator vs. SPARQL-JSON
writer) should be pinned with a one-line probe during implementation.

### BUG-5 — bound `GRAPH ?g` fails when `?g` is a late-materialized `EncodedSid`

`extract_graph_iri_from_binding` (graph.rs:157-172) handles `get_iri()`
(`Iri`/`IriMatch`), `Binding::Sid`, and `Binding::Lit{String}` — but **not
`Binding::EncodedSid`** (binding.rs:155) nor `EncodedLit`. A `?g` bound from a
triple scan object (`?s :p ?g`) is, by default, a late-materialized
`EncodedSid`. So the bound arm at graph.rs:627-629 gets `None` → "binding exists
but isn't a string IRI → no output" → the graph never resolves. This is exactly
`exists-graph-variable` (`?s :p ?g . FILTER EXISTS { GRAPH ?g { … } }`). Probe:
`Expected 1 solution(s) [s1], got 0`. Corroborated by the baseline: the
`SPARQL11_BINDINGS` `graph` test binds `?g` to an IRI via `VALUES` (not a scan)
and **partially** works (the IRI-typed rows appear), i.e. IRI-bound `?g`
resolves but scan-bound `EncodedSid` `?g` does not.

### Per-test verdicts — `/graph/` (12 registered)

Data setup is from the manifest (`qt:data` → default graph, `qt:graphData` →
named graph); "leak rows" = spurious solutions from BUG-2 binding the default
graph as `w3c:test`.

| Test | Data setup | Query shape | Now | Expect | Root cause | Greened by |
|---|---|---|---|---|---|---|
| dawg-graph-03 | named data-g1 only | `GRAPH ?g {spo}` | 2 (g=Lit) | 2 (g=Iri) | BUG-1 | PR-A |
| dawg-graph-04 | default data-g1 only | `GRAPH ?g {spo}` | 2 leak | 0 | BUG-2 | PR-A |
| dawg-graph-06 | default g1 + named g2 | `GRAPH ?g {spo}` | 3 | 1 | BUG-1+BUG-2 | PR-A |
| dawg-graph-07 | default g1 + named g2 | `{spo} UNION {GRAPH ?g {spo}}` | 5 | 3 | BUG-1+BUG-2 | PR-A |
| dawg-graph-08 | default g1 + named g2 | `spo . GRAPH ?g {sqv}` | 3 | 1 | BUG-1+BUG-2 | PR-A |
| dawg-graph-09 | default g3 + named g4 (bnodes) | `spo . GRAPH ?g {sqv}` | 2 | 0 | BUG-2 | PR-A |
| dawg-graph-10b | default g3 + named g3-dup | `spo . GRAPH ?g {sqv}` | 2 | 0 | BUG-2 | PR-A |
| dawg-graph-11 | default g1 + named g1,g2 | `{spo} UNION {GRAPH ?g {spo}}` | 10 | 8 | BUG-1+BUG-2 | PR-A |
| graph-empty | named g1,g2 | `GRAPH ?g {}` | 3 (g=Lit) | 2 (g=Iri) | BUG-1+BUG-2 | PR-A |
| graph-exist | default g1 + named g1,g2 | `GRAPH <g1> {}` | 0 | 1 | **BUG-4** | PR-D |
| graph-variable-join | named data-variable-join, g1 | `GRAPH ?g { ?g :p ?o }` | 3 | 1 | **BUG-3**+BUG-1 | PR-B |
| graph-optional | named data-optional, g1 | `GRAPH ?g { spo OPTIONAL{ ?s ?p ?g } }` | 4 | 1 | **BUG-3**+BUG-1 | PR-B |

Already passing (context, not registered): dawg-graph-01/02/05, graph-not-exist,
graph-variable-scope.

### Per-test verdicts — SPARQL 1.1 graph-variable tests (2)

| Test | Query | Now | Expect | Root cause | Greened by |
|---|---|---|---|---|---|
| exists (`exists-graph-variable`) | `?s :p ?g . FILTER EXISTS { GRAPH ?g {…} }` | 0 | s1 | **BUG-5** | PR-C |
| bindings (`graph`) | `GRAPH ?g { VALUES(?g ?t){(UNDEF …)(<empty.ttl> …)} }` | 4 | 3 | BUG-1+BUG-2 **+ empty-named-graph visibility** | PR-A + open Q2 |

`bindings/graph` is **not** fully greened by PR-A: expected row `g=<empty.ttl>`
requires an *empty* named graph to be enumerable, and Fluree does not register
empty named graphs (the harness skips empty loads, `query_handler.rs:144-151`,
and the registry only holds graphs with triples). Keep it registered pending the
empty-named-graph decision (open question 2).

---

## 2. Design memo — §5.3 single-ledger `FROM` / `FROM NAMED`

All 12 `/dataset/` tests fail identically: `validate_sparql_for_view`
(view/query.rs:568) hard-rejects any dataset clause on a single-ledger `GraphDb`:

```rust
if has_dataset {
    return Err(ApiError::query(
        "SPARQL FROM/FROM NAMED clauses are not supported on a single-ledger GraphDb. \
         Use query_connection_sparql for multi-ledger queries."));
}
```

Unlike the `/graph/` tests, the `/dataset/` manifest carries **no** `qt:data`
or `qt:graphData` — the dataset is defined **entirely by the query's FROM/FROM
NAMED clauses**, each naming a test file by relative IRI (e.g.
`FROM <data-g1.ttl>`, `FROM NAMED <data-g2.ttl>`). The tests probe exactly the
delicate semantics:

- **empty default graph** when only `FROM NAMED` is given (dataset-02 → 0);
- **`FROM` does not create a named graph** (dataset-04: `GRAPH ?g` → 0);
- **default graph = union of multiple `FROM`** (dataset-12b);
- **same IRI in `FROM` and `FROM NAMED`** (dataset-11: data-g1 in both).

### How multi-ledger datasets work today (for reference)

`query_connection_sparql` (`fluree-db-api/src/query/connection.rs:688`) →
`extract_sparql_dataset_spec` (query/helpers.rs:388) → `DatasetSpec`
(dataset.rs:91). Each `FROM`/`FROM NAMED` IRI is run through
`parse_ledger_id_time_travel` (dataset.rs:881) and **treated as a ledger
identifier**; `build_dataset_view` (view/dataset_builder.rs:110) resolves each
via `self.db(identifier)` (dataset_builder.rs:212 — a **nameservice ledger
load**). The result becomes a `fluree_db_query::DataSet` of `GraphRef`s
(query/dataset.rs:174), each a `(snapshot, g_id, ledger_id)` triple, fanned out
by `DatasetOperator`. Crucially, **all single-ledger within-graph machinery is
gated on `ctx.dataset.is_none()`** (context.rs:947/961; graph.rs:556/586/…), so
the moment any `FROM` clause is present, that path turns off and every IRI is
resolved as a **separate ledger**. There is no resolver from a `FROM NAMED` IRI
to a `g_id` **inside** the current ledger.

The within-ledger registry that Option A needs already exists end-to-end:
`GraphRegistry` (`fluree-db-core/src/graph_registry.rs`), `FIRST_USER_GRAPH_ID=3`
(graph_registry.rs:41), `graph_id_for_iri`/`iter_entries` (graph_registry.rs:315,
327), surfaced as `ExecutionContext::single_db_user_graph_id` /
`single_db_user_graph_iris` (context.rs:946, 960). Issue #1279 wired it for the
**no-FROM** path only.

### Option A — within-ledger datasets (recommended)

When a single-ledger query carries `FROM`/`FROM NAMED`, resolve each clause IRI
against the ledger's own graph registry (user graph g_id, the ledger alias →
default g_id 0, or an R2RML/graph source) and build a **single-snapshot**
`DataSet`: `default_graphs` = the `FROM` g_ids (empty if there is no `FROM`
clause), `named_graphs` = the `FROM NAMED` IRI→g_id map. Pass it via the same
`ContextConfig.dataset` the multi-ledger path uses; `DatasetOperator` already
unions the defaults and scopes `GRAPH` to the named map. Because every `GraphRef`
shares one snapshot/ledger, `spans_multiple_ledgers()` is false → **no
cross-ledger provenance stamping, binary store stays enabled** (the fast scan
path is untouched). Fall back to the connection path (or today's rejection) only
when a clause IRI is not resolvable within the ledger.

- **W3C semantics:** exact. No `FROM` → `default_graphs` empty → dataset-02/04
  green. Multiple `FROM` → union → dataset-12b. Same IRI in both → same g_id in
  default and named → dataset-11.
- **g_id model:** native — a named graph *is* a g_id; this is the Fluree data
  model, not an impedance-mismatched ledger-per-graph.
- **Perf:** plan-time only (resolve IRIs → g_ids, build `GraphRef`s). Single
  snapshot ⇒ none of the multi-ledger slow paths engage.
- **Reuse:** the runtime `DataSet` + `DatasetOperator` + `dataset_query.rs`
  execution path are reused as-is; only dataset *construction* is new.
- **Product value:** fixes the actual gap — a real user can `SELECT … FROM <g>
  FROM NAMED <n>` against one ledger's named graphs.
- **Greens:** all 12 `/dataset/` tests, and `SPARQL11_CONSTRUCT` `constructwhere04`
  (same rejection, per its register comment). Requires a **harness** change:
  for dataset tests, pre-load each `FROM`/`FROM NAMED`-referenced file as a
  named graph in the single ledger (the harness already loads named graphs this
  way, `query_handler.rs:141-172`; the file list would come from the parsed
  dataset clause instead of `qt:graphData`).

### Option B — harness maps graph URLs → ledgers (`query_connection_sparql`)

Load each referenced file into its **own** in-memory ledger aliased by the file
IRI, then call `query_connection_sparql`. **Zero engine change.**

- **W3C semantics:** achievable — the existing multi-ledger path already makes
  the default the union of `FROM` and leaves it empty when only `FROM NAMED` is
  present.
- **Costs:** requires ledger aliases to admit arbitrary `https://…` IRIs; spins
  up N ledgers per query (8 for dataset-12b); needs the harness to switch from
  `fluree.query(&db, …)` to `query_connection_sparql` **for dataset tests only**,
  diverging them from every other eval test; and it **does not fix the product**
  (single-`GraphDb` `FROM` still rejected).
- **Greens:** the same 12 tests, harness-only.

### Recommendation

**Option A.** Both options green the same 12 tests, so the tie-breakers are
product value, reuse, and perf — all of which favour A. B is a harness-only
fallback that leaves the user-facing gap and forks the harness. A extends
#1279's within-ledger registry the natural way and keeps the scan hot path
byte-identical (single snapshot). The one extra cost is the harness pre-load,
which is small and reuses the existing named-graph loader.

---

## 3. Fix design for finding (a) — no per-row hot-path branching

All of (a) is fixed at **materialization / plan / seed time**; nothing lands in
a per-scan-row loop.

- **BUG-1 (IRI typing):** at graph.rs:314-323 replace the `Binding::Lit{String,
  xsd:string}` with `Binding::iri(graph_iri.clone())`. Update the module doc
  (graph.rs:10) and the assertion in
  `it_query_dataset.rs:1591`. Cost: one enum construction per *matched GRAPH
  solution* (already being built), not per scan row.

- **BUG-2 (default-graph leak):** delete the alias enumeration at
  graph.rs:700-709 in the unbound single-db arm — `GRAPH ?g` then ranges over
  `single_db_user_graph_iris()` only. Update `it_query_dataset.rs:1591` and
  `:1752`.

  *Intentional-extension handling.* The extension has two halves: (i) implicit
  **enumeration** of the default graph via unbound `GRAPH ?g`, and (ii)
  **explicit addressing** of the default graph via a concrete/bound alias IRI
  (`GRAPH <alias>`, `VALUES ?g {"alias"} GRAPH ?g`). Only (i) breaks W3C. Drop
  (i) (implicit); **keep (ii)** — no W3C test names the alias, so explicit
  addressing stays W3C-safe and preserves the useful "query the default graph by
  name" affordance. If (i) has real product value, gate it behind an opt-in
  query option rather than making it the default — the natural home is a bool on
  `QueryOpts`/`ExecutionContext` (e.g. `graph_var_includes_default`, default
  `false` = W3C), checked once where the arm decides whether to append the alias
  (graph.rs:700). Recommendation: **just fix it** (drop implicit, keep explicit);
  do not ship a toggle unless a consumer asks — the divergence is the kind #1279
  left explicitly open, and W3C-by-default is the right default.

- **BUG-3 (graph-var unification):** seed the graph var into the inner subplan.
  In `execute_in_graph`, when `bind_graph_var == Some(v)`, add `v →
  Binding::iri(graph_iri)` to the `SeedOperator` row (graph.rs:258-259) so inner
  patterns referencing `?g` are scanned with it bound; then the merge already
  carries the consistent value (the special-case push becomes redundant, or a
  cheap assert). This is **seed-time**, per parent row of a GRAPH scope, and
  *improves* perf (it constrains the inner scan instead of overwriting after a
  full scan).

- **BUG-5 (EncodedSid bound `?g`):** extend `extract_graph_iri_from_binding`
  (graph.rs:157-172) with an `EncodedSid`/`EncodedLit`-string arm that
  materializes the value against the active graph view before comparing. This
  runs **per parent row of a correlated GRAPH**, only in the bound-var arm, and
  only adds a decode when `?g` is actually bound and still encoded — off any hot
  scan path.

- **BUG-4 (empty existence row):** `GraphOperator` is already correct
  (graph.rs:482-489). Pin and fix the downstream 0-column-batch drop
  (projection or SPARQL-JSON writer); ensure the fix is preserve-only, adding no
  per-row cost.

The **dataset** fix (§2 Option A) replaces the guard at view/query.rs:568 with
a within-ledger dataset build; that is plan-time construction feeding the
existing `dataset_query.rs` execution path.

---

## 4. Hot-path classification + bench guards

| Fix | Stage | Hot path? |
|---|---|---|
| BUG-1 IRI typing | materialization (per matched GRAPH solution) | no |
| BUG-2 drop alias enumeration | enumeration/plan (removes one iteration) | no |
| BUG-3 seed graph var | seed-time (per parent row); constrains inner scan | no (net win) |
| BUG-4 preserve empty row | projection/format (preserve-only) | no |
| BUG-5 EncodedSid decode | per parent row, bound-var arm only | no |
| Dataset (Option A) | plan-time dataset construction; single snapshot | no |

None of these touch `BinaryScanOperator`, the join inner loop, or filter
evaluation. **Bench guards:** `query_hot_bsbm` and `query_hot_bsbm_bi`
(`fluree-db-api/benches/`) use BSBM data with **no named graphs and no `GRAPH`/
`FROM`**, so `GraphOperator` and dataset construction are never instantiated on
those paths — the fixes are off the guarded hot path by construction. Still run
both per PR (per §6 of the audit) to confirm the shared `context.rs` helpers
(`single_db_user_graph_iris`, `with_active_graph`) and any projection change
don't regress; expect flat within `regression-budget.json`. The single-ledger
`DataSet` guarantees the multi-ledger provenance/eager-materialization slow
paths stay dormant, so scan throughput under `GRAPH`/`FROM` is unchanged from
the concrete-graph baseline.

---

## 5. Query-surface parity (SPARQL / JSON-LD)

Fluree owns the JSON-LD query syntax, so the "SPARQL-possible ⇒ JSON-LD-possible"
rule applies, with a JSON-LD regression test named per fix. Cypher is
openCypher — Fluree does not own the grammar and will not add custom syntax —
so Cypher is **out of scope for this burn-down** and is not analyzed here.

- **JSON-LD / FQL has GRAPH patterns.** `["graph", <name>, {…}]` lowers to the
  same `Pattern::Graph` and runs through the same `GraphOperator` (tests
  `fql_graph_pattern_basic` it_query_dataset.rs:1017, `fql_graph_pattern_with_alias`
  :1070). Therefore **BUG-1/2/3/5 are IR/engine-level fixes** (parity class 1):
  one change fixes both surfaces, but the W3C submodule only guards SPARQL, so
  each PR **must add FQL regression tests**. Author in
  `fluree-db-api/tests/it_query_dataset.rs` (alongside the existing FQL graph
  tests):
  1. `["graph","?g",{…}]` over a ledger with a user named graph → `?g` comes
     back as an **IRI** term (`@id`), and the ledger's default graph is **not**
     enumerated (mirror of the BUG-1/BUG-2 SPARQL fix);
  2. `["graph","?g",{"@id":"?g", …}]` — graph variable also used inside (BUG-3);
  3. bound `["graph","?g",…]` where `?g` came from a triple scan (BUG-5).
- **JSON-LD `from`/`fromNamed` datasets already exist** (dataset.rs:76-77, the
  `ExtractedDataset` path) and flow through the same `DataSet`/`DatasetOperator`.
  Option A's within-ledger construction should be reachable from the JSON-LD
  `from`/`fromNamed` surface too, within one ledger — add an FQL within-ledger
  `from`/`fromNamed` regression test alongside the SPARQL dataset fix.

Definition of done per the team guideline: register entry removed **and** the
JSON-LD regression test authored.

---

## 6. Blast radius, PR composition, risks, open questions

### Blast radius

- `fluree-db-query/src/graph.rs` (`GraphOperator`) — BUG-1/2/3/5. Shared by
  SPARQL + FQL graph patterns. Existing behavior is pinned by
  `it_query_dataset.rs:1526-1765`; **two tests must be updated** (1591, 1752) —
  they encode the extension being removed.
- Projection / SPARQL-JSON writer — BUG-4. Shared by all queries; keep surgical.
- `fluree-db-api/src/view/query.rs` (`validate_sparql_for_view` + a new
  within-ledger dataset build) and `testsuite-sparql/src/query_handler.rs`
  (pre-load FROM-referenced files) — dataset (Option A).

### Suggested PR composition

- **PR-A — GRAPH ?g W3C conformance (BUG-1 + BUG-2).** IRI typing + drop alias
  enumeration; update tests 1591/1752; add FQL regression. Greens graph-03, 04,
  06, 07, 08, 09, 10b, 11, graph-empty (9 tests). Small, self-contained.
- **PR-B — graph-variable unification (BUG-3).** Seed `?g` into the inner
  subplan. Greens graph-variable-join, graph-optional. Depends on PR-A (needs
  IRI typing).
- **PR-C — bound EncodedSid `?g` (BUG-5).** Extend
  `extract_graph_iri_from_binding`. Greens exists-graph-variable. Tiny.
- **PR-D — empty-projection existence row (BUG-4).** Fix the downstream drop.
  Greens graph-exist. May be broader than the GRAPH cluster (projection/format);
  isolate.
- **PR-E — single-ledger FROM/FROM NAMED (Option A).** Within-ledger dataset
  construction + harness pre-load. Greens all 12 `/dataset/` tests and
  `constructwhere04`. Largest (engine view layer + harness). Independent of
  PR-A–D.

Each PR shrinks its register entries in the same change (audit §5.1
both-direction enforcement).

### Risks

1. **Removing the #1279 extension** (BUG-2) is a semantics change with two
   regression tests asserting it — needs the 2-reviewer sign-off the audit
   requires. Mitigation: keep explicit `GRAPH <alias>` addressing; only drop
   implicit enumeration.
2. **`?g` literal → IRI** changes any consumer that relied on the string form.
   The back-compat string-literal acceptance in
   `extract_graph_iri_from_binding` stays (so `VALUES ?g {"alias"}` still works);
   only the *output* term kind changes, which is the correct behavior.
3. **Option A must route through the existing dataset execution path**
   (`dataset_query.rs`, including `apply_reasoning_to_dataset`, policy, and
   R2RML graph sources) — not a parallel new path — or it will silently diverge
   on policy/reasoning. Reuse `as_runtime_dataset` (view/dataset.rs:184).
4. **BUG-4** touches shared projection/format; a careless fix could resurrect
   dropped-vs-kept empty rows elsewhere (ASK, zero-var subqueries). Add targeted
   tests.

### Open questions

1. Keep the default-graph-discovery extension behind an opt-in flag, or drop it
   outright? (Recommend: drop implicit enumeration, keep explicit alias
   addressing, no toggle unless a consumer needs it.)
2. **Empty named graphs.** Fluree does not track a declared-but-empty named
   graph (harness skips empty loads; registry holds only graphs with triples).
   This blocks `bindings/graph` (needs `g=<empty.ttl>`) and is the same gap
   behind CLEAR-vs-DROP in update-eval (query_handler.rs:440-460 comment). A
   product decision is needed: does Fluree model empty named graphs?
3. Harness: confirm Option A's pre-load of `FROM`/`FROM NAMED`-referenced files
   is acceptable (harness parses the query's dataset clause), vs. the Option B
   per-file-ledger fallback.
4. BUG-4: pin the exact 0-column-batch drop point (projection operator vs.
   SPARQL-JSON writer) with a one-line probe.
5. Cross-check **#1317** (GRAPH leaks default graph after `WITH DELETE WHERE`):
   likely shares the default-vs-named confusion behind BUG-2 and should be
   verified once BUG-2 lands.
