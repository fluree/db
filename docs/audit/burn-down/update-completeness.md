# SPARQL UPDATE completeness — pre-implementation deep audit (2026-07)

**Scope:** the two UPDATE registers in `testsuite-sparql/tests/registers/mod.rs` —
`SPARQL11_SYNTAX_UPDATE_1` (54 tests, 31 fail) and `SPARQL11_UPDATE` (157 tests,
90 fail). The syntax-update-1 IDs appear in both registers, so the de-duplicated
failure set is ~90 distinct tests.
**Baseline:** branch `test/sparql-testsuite-full-coverage`, rdf-tests @ `efccbc6b8`.
**Method:** every claim below was verified by reading code and the per-test error
JSON (`scratchpad/w3c-baseline2/sparql11_{update,syntax_update_1}_tests.json`) and
by reading the actual `.ru` test files — not by trusting the register comments.
**This audit does not modify source; it is a design input.**

## 0. TL;DR — the failures collapse to SIX root causes, not the eight the registers imply

| Class | Root cause | Failing tests | Layer |
|---|---|---|---|
| **A** | Graph-management verbs absent from grammar/AST (LOAD/CLEAR/CREATE/DROP/COPY/MOVE/ADD + SILENT/INTO/TO/DEFAULT/NAMED/ALL) | 41 eval parse-errors + ~21 positive-syntax | parse-time |
| **B** | **Multi-operation (`;`) requests unsupported — only the FIRST operation of a request runs; the rest are silently dropped** | insert-05a, insert-data-same-bnode, insert-where-same-bnode(2), dawg-delete-insert-01c, test_54 | parse-time + commit-time |
| **C** | Empty / prologue-only update request rejected | test_38, test_39, test_40 | parse-time |
| **D** | `GRAPH` blocks in `DELETE WHERE` unsupported (validate + lower) | test_36 (syntax) + dawg-delete-where-02/04/06 (eval) | parse/validate + lower-time |
| **E** | Negative-syntax gap: blank nodes not rejected in DELETE forms | test_50/51/52, dawg-delete-insert-03/03b/05/06/07/07b/08/09 | parse/validate-time |
| **F** | USING + explicit-`GRAPH` dataset-scoping semantics incomplete | dawg-delete-using-02a, dawg-delete-using-06a | prepare-time (staging) |

**Corrections to the register comments (verified):**
- **B is the single biggest correction.** The `SPARQL11_UPDATE` register attributes
  `insert-05a` and the three `insert-*-same-bnode*` tests to *"INSERT into a
  not-yet-existing named graph silently loses triples"* and `dawg-delete-insert-01c`
  to *"combined DELETE/INSERT WHERE applies inserts without deletes."* Both are
  **wrong.** All five are multi-operation requests (`…;…;…`), and their observed
  output is exactly what you get when only the first operation is applied and the
  remainder are discarded (see §1.B for the arithmetic). The "combined DELETE/INSERT"
  form actually **passes** (`dawg-delete-insert-01`, a single `DELETE{}INSERT{}WHERE{}`);
  it is the `;`-separated `INSERT ; DELETE` sibling (`-01c`) that fails.
- **test_36 is class D, not A.** Its register/audit home is "missing graph-management
  grammar," but it is `DELETE WHERE { GRAPH <G> { … } }` — rejected by the validator
  (`validate/mod.rs:154`), the same cause as the eval-side `dawg-delete-where-*`.
- **test_38/39/40 are class C, not A.** They are an empty request, a `BASE`-only
  request, and a `PREFIX`-only request — the "graph-management grammar" comment does
  not apply.
- Classes A, D, E, F otherwise match their register comments.

---

## 1. Per-test root cause with file:line evidence

### The two structural facts that drive most of this cluster

1. **The parser recognizes exactly four update operations and no request-level
   sequence.** `UpdateOperation` has only `InsertData | DeleteData | DeleteWhere |
   Modify` (`fluree-db-sparql/src/ast/update.rs:22-32`). `parse_query_body`
   dispatches update on `KwInsert | KwDelete | KwWith` only
   (`fluree-db-sparql/src/parse/query/mod.rs:308`); `parse_update_operation` errors
   on anything else (`fluree-db-sparql/src/parse/query/update.rs:78-80`). Every other
   verb falls through to `"expected query form … or update (INSERT, DELETE)"`
   (`mod.rs:312-318`), surfaced as HTTP 400 by
   `parse_and_lower_sparql_update` (`fluree-db-api/src/tx_builder.rs:42-48`). **All
   the graph-management keywords are already lexed** (`fluree-db-sparql/src/lex/token.rs`:
   `KwLoad` 255, `KwInto` 256, `KwClear` 257, `KwDrop` 258, `KwCreate` 259, `KwAdd`
   260, `KwMove` 261, `KwCopy` 262, `KwTo` 263, `KwSilent` 130, `KwDefault` 253,
   `KwAll` 254, `KwNamed` 121, `Semicolon` 317; string map at 793-806) — the work is
   parser/AST/lowering/exec, not lexing.

2. **`parse_sparql` parses one operation and never checks for trailing tokens.**
   `parse_query` runs `parse_prologue` then a single `parse_query_body()?`
   (`mod.rs:193-207`); `parse_sparql` returns as soon as that succeeds, with no EOF
   assertion (`mod.rs:40-74`). `parse_and_lower_sparql_update` calls `parse_sparql`
   exactly once and lowers the single `QueryBody::Update` to one `Txn`
   (`tx_builder.rs:41-53`, `lower_sparql_update_ast` at
   `fluree-db-transact/src/lower_sparql_update.rs:620-632`). Therefore a request like
   `INSERT{…}WHERE{…} ; DELETE{…}WHERE{…}` **parses the INSERT, then silently
   discards `; DELETE{…}WHERE{…}`.** The harness confirms this: it feeds the whole
   request text to one `.sparql_update(&sparql).commit()`
   (`testsuite-sparql/src/query_handler.rs:421-437`), and the failing tests come back
   as *evaluation mismatches*, not parse errors — proof the trailing operations were
   dropped rather than rejected.

### Class A — graph-management verbs (parse-time)

`SPARQL11_SYNTAX_UPDATE_1` positive rejects: test_1, test_2 (`BASE`/`PREFIX` prologue
followed by `LOAD` — `syntax-update-01.ru`/`-02.ru`), test_3 (`LOAD … ;`), test_4
(`LOAD … INTO GRAPH …`), test_5-7 (`DROP NAMED|DEFAULT|ALL`), test_8 (`DROP GRAPH`),
test_9-12 (`DROP SILENT …`), test_13-14 (`CREATE [SILENT] GRAPH`), test_15-22
(`CLEAR [SILENT] NAMED|DEFAULT|ALL|GRAPH`), test_37 (`CREATE GRAPH <g> ; LOAD …`).

`SPARQL11_UPDATE` eval parse-errors (41, all `"expected query form … or update
(INSERT, DELETE)"`): `add01-08` (8), `clear-{all,default,graph,named}-01` (4),
`copy0{1,2,3,4,6,7}` (6), `drop-{all,default,graph,named}-01` (4),
`move0{1,2,3,4,6,7}` (6), and all 13 `update-silent/*` (`add-silent`,
`add-to-default-silent`, `clear-default-silent`, `clear-silent`, `copy-silent`,
`copy-to-default-silent`, `create-silent`, `drop-default-silent`, `drop-silent`,
`load-into-silent`, `load-silent`, `move-silent`, `move-to-default-silent`).

### Class B — multi-operation requests (parse-time + commit-time). **Register-comment correction.**

- **`dawg-delete-insert-01c`** (`delete-insert-01c.ru`) = `INSERT{?b knows ?a}WHERE{?a
  knows ?b} ; DELETE{?a knows ?b}WHERE{?a knows ?b}`. Correct result: op1 makes the
  graph bidirectional, op2 then deletes every `knows` edge → 6 name/mbox triples.
  Observed: 12 triples = 6 name/mbox + 6 (now-bidirectional) `knows` — i.e. **op1 ran,
  op2 was dropped.** The JSON diff (expected 6, got 12, actual still contains the
  `knows` edges) matches this exactly. The sibling `-01` (single combined op) and
  `-01b` (`DELETE ; INSERT`, where op2 legitimately matches nothing after op1) both
  pass — `-01b` passes *even under the bug* because first-op-only happens to give the
  right answer, which is why the bug hid here.
- **`insert-05a`** (`insert-05a.ru`) = three `INSERT…WHERE` ops then `DROP GRAPH :g1 ;
  DROP GRAPH :g2`. Expected `g3 = :s :p 1`; observed `g3` empty ("Expected 1, got 0").
  Only op1 runs (copy `g1`→`g2`); the `g3` count-insert (op3) never executes.
- **`insert-data-same-bnode`, `insert-where-same-bnode`, `insert-where-same-bnode2`**
  — all identical shape (multi-op ending in `DROP GRAPH …`); all fail with empty `g3`
  for the same reason. Note these tests *also* require class A (their trailing ops are
  `DROP GRAPH`), but the failure you can observe today is purely B.
- **test_54** (`syntax-update-54.ru`) = `INSERT DATA {_:b1 :p :o} ; INSERT DATA {_:b1
  :p :o}` — a negative test for reusing a bnode label across operations. It "passes
  parse" (so the negative test fails) because the second operation is discarded and
  the reuse is never seen. Fixing it needs B (parse both ops) **then** E-style
  cross-operation bnode-scope validation.

Because these are single `sparql_update()` calls, none of this is a staging bug — the
data proves the AST only ever carried the first operation.

### Class C — empty / prologue-only requests (parse-time). **Register-comment correction.**

- **test_38** `# Empty` (only a comment) → tokens are empty → `parse_query_body`
  hits EOF and errors (`mod.rs:313-318`).
- **test_39** `BASE <http://example/>` only, **test_40** `PREFIX : <http://example/>`
  only → prologue parses, then `parse_query_body` finds no operation → error.

Per SPARQL 1.1 Update grammar `Update ::= Prologue (Update1 (';' Update)?)?` the
operation is **optional**; a prologue-only or empty request is a valid no-op.

### Class D — `GRAPH` blocks in `DELETE WHERE` (validate + lower)

- **test_36** (`syntax-update-36.ru`, `DELETE WHERE { GRAPH <G> { … } }`) is a
  *syntax* test; the parser accepts the GRAPH block (`update.rs:271-277`,
  `parse_quad_pattern_graph_block` 314-365) but `validate_delete_where` pushes an error
  (`fluree-db-sparql/src/validate/mod.rs:154-165`) → `has_errors` → the positive test
  fails.
- **dawg-delete-where-02/04/06** (eval) reach lowering, which hard-errors at
  `lower_sparql_update.rs:827-830` (`UnsupportedFeature { feature: "GRAPH blocks in
  DELETE WHERE" }`). Same root cause, different layer. Register comment ("engine:
  GRAPH blocks in DELETE WHERE unsupported") is correct.

### Class E — blank nodes not rejected in DELETE forms (validate)

The validator rejects only *variables* in ground DATA (`validate_ground_quad_data`,
`validate/mod.rs:214-247`) and only *GRAPH variables* in Modify templates
(`validate_update_template_quad_pattern` 183-209); it never rejects **blank nodes**.
SPARQL 1.1 §4.1.2 forbids blank nodes in `DELETE`, `DELETE DATA`, and `DELETE WHERE`
(they would be unbound wildcards). Failing negative tests:
- **test_50** `DELETE WHERE { _:a <p> <o> }`, **test_51** `DELETE { <s> <p> [] }
  WHERE{…}`, **test_52** `DELETE DATA { _:a <p> <o> }`.
- **dawg-delete-insert-03/03b/05/06/07/07b/08/09** — each has `[]` or `_:b` in the
  DELETE template (03b/09 use labelled `_:b`; the rest use `[]`). All eight are
  `mf:NegativeSyntaxTest11` with a bare `mf:action` (no data), so a validate-only
  rejection suffices. Register comment ("missing validation") is correct.

### Class F — USING + explicit-`GRAPH` dataset scoping (prepare-time)

- **dawg-delete-using-02a** (`delete-using-02.ru`) and **dawg-delete-using-06a**
  (`delete-using-06.ru`): single `DELETE{…} USING <g3> WHERE { GRAPH <g2> {…} }`
  operations. USING default-graph scoping *is* wired
  (`fluree-db-transact/src/stage.rs:1382-1399` selects the WHERE default graph from
  `sparql_where.using_default_graph_iris`, falling back to `with_graph_iri`), but the
  observed results ("Expected 5 got 3", "Expected 6 got 4") show the WHERE bindings /
  delete-target resolution are wrong when an explicit `GRAPH <g2>` block co-occurs with
  `USING <g3>` and a default-graph DELETE template. The manifest's own comment names
  the trap: *"make sure the GRAPH clause does not override the USING clause."* This is
  a genuine staging-semantics bug, single-operation, unrelated to B. Register comment
  ("USING semantics incomplete") is correct.

---

## 2. Fix design per class

### A — graph-management grammar + AST + execution mapping

**Grammar/AST.** Add variants to `UpdateOperation` (`ast/update.rs`) and a new
request-level container (see B). New AST nodes (all trivially `Clone/Debug/PartialEq`
with a `SourceSpan`):
- `Load { silent: bool, source: Iri, into: Option<Iri> }`
- `Clear { silent: bool, target: GraphRefAll }`
- `Drop  { silent: bool, target: GraphRefAll }`
- `Create { silent: bool, graph: Iri }`
- `Add | Copy | Move { silent: bool, from: GraphOrDefault, to: GraphOrDefault }`
- `GraphRefAll ::= Default | Named | All | Graph(Iri)`; `GraphOrDefault ::= Default |
  Graph(Iri)`.

Parser: extend `parse_update_operation` (`parse/query/update.rs:21`) to branch on
`KwLoad/KwClear/KwDrop/KwCreate/KwAdd/KwCopy/KwMove`, each consuming an optional
`KwSilent`, then the target grammar (`INTO GRAPH`, `TO`, `GRAPH|DEFAULT|NAMED|ALL`).
All grammar is in the SPARQL 1.1 Update spec §3.1.2–3.2. This is pure recursive-descent
work mirroring the existing `parse_using_clause` style.

**Lowering + execution mapping onto Fluree's flake/g_id model.** Fluree models a named
graph as a reserved `GraphId` (`u16`) in a per-ledger `GraphRegistry`
(`fluree-db-core/src/graph_registry.rs`: reserved 0=default, 1=txn-meta, 2=config,
≥3=user, lines 31-41); each flake is tagged with the *Sid of the graph IRI*
(`fluree-db-core/src/flake.rs:120-124`), bridged to a `GraphId` via
`build_reverse_graph` (`db.rs:446-465`) / `resolve_flake_g_id`
(`fluree-db-novelty/src/lib.rs:547-556`). Mapping:

| Verb | Execution | Existing machinery | Gap |
|---|---|---|---|
| `CLEAR GRAPH <g>` / `CLEAR DEFAULT` | scan every flake in that g_id, emit retraction (`op=false`) flakes | g_id-scoped scan (`GraphDbRef`, `fluree-db-ledger/src/lib.rs:519`); per-g_id novelty (`fluree-db-novelty/src/fact_state.rs:33-38`); retract templates as in `lower_delete_data` | need a "retract-all-in-g_id" staging primitive (none today) |
| `CLEAR ALL` / `CLEAR NAMED` | iterate registry g_ids (≥3, plus default for ALL), clear each | `GraphRegistry::iter_entries` (`graph_registry.rs:327`) | same |
| `DROP …` | **semantically = CLEAR** in Fluree's model (see empty-graph note) | same as CLEAR | true removal needs a registry-*remove* path; `graph_registry.rs` is additive-only (`apply_delta` 241, seed ctors — no `remove`/`drop`) |
| `CREATE GRAPH <g>` | register an empty graph | — | no empty-graph registration path (user g_ids only enter the registry via templates that carry triples, `lower_sparql_update.rs:1032`); **and the harness cannot observe an empty graph** — treat as near-no-op |
| `COPY/MOVE/ADD <a> → <b>` | scan g_id(a) → assert into g_id(b); `COPY`/`MOVE` first clear b; `MOVE` also clears a | g_id scan + template assertion + clear (above) | compose from the CLEAR primitive + template insert |
| `LOAD <url> [INTO GRAPH <g>]` | fetch remote RDF, parse, insert into target | Turtle/TriG parser exists (`fluree_graph_turtle`); FlakeSink | **no HTTP fetch in transact/api** (reqwest only behind `search-remote-client` for SPARQL SERVICE, `remote_service.rs`) — see recommendation |

**W3C empty-graph semantics vs Fluree (important for CLEAR/DROP/CREATE):** W3C
distinguishes `CLEAR GRAPH <g>` (graph remains, empty) from `DROP GRAPH <g>` (graph
gone) and `CREATE GRAPH <g>` (empty graph now exists). **Fluree does not track empty
named graphs**, and the harness explicitly cannot see them: `run_update_eval_test`
only compares *non-empty* graphs and lists only non-empty named graphs
(`query_handler.rs:468-479`, with an in-code note to this effect). Consequences:
- CLEAR and DROP of a graph are **indistinguishable to the test harness** — both leave
  it non-existent-because-empty. So `DROP` can be implemented as CLEAR semantics for
  W3C purposes; true unregistration is a separate, harness-invisible nicety.
- `CREATE GRAPH <g>` on a fresh graph is unobservable (creates nothing the harness can
  see) and on an existing graph is a spec no-op — so a validate-only/near-no-op
  implementation passes. Only `create-silent` is in the failing set (CREATE of an
  existing graph, swallowed by SILENT).
- The four `clear/drop-{default,graph,named,all}-01` eval tests operate on *populated*
  graphs, so they ARE observable and need the real retract-all primitive.

**SILENT.** `SILENT` must swallow the "target already/doesn't exist" errors so the op
becomes a no-op (SPARQL §3.1.2). The 13 `update-silent/*` tests all target
missing/existing graphs and expect the store **unchanged** (e.g. `clear-silent` on a
non-existent graph, `mf:result` = input `spo.ttl`). Notably, `load-silent` /
`load-into-silent` LOAD a non-resolvable/remote source; with SILENT, a LOAD that
fails-to-fetch becomes a no-op and the store is unchanged — **so both can pass even
without real HTTP fetch**, provided LOAD is parsed and its failure is swallowed.

**LOAD recommendation (open decision).** Non-SILENT `LOAD <url>` of arbitrary remote
RDF requires an HTTP client that is intentionally absent from the embedded transact/api
path. Recommend: implement parsing + SILENT-swallowed-failure (clears the two silent
tests), and **register non-SILENT remote `LOAD` as a documented divergence** in the
skip register with a spec link, unless/until an opt-in fetch hook is added. There are
no non-SILENT `LOAD` eval tests in the failing set, so this costs zero W3C coverage.

### B — multi-operation requests + sequential execution

**Parse.** Introduce a request node (e.g. `SparqlAst.body = QueryBody::UpdateRequest(
Vec<UpdateOperation>)`, or a new top-level `UpdateRequest`) and loop in `parse_query`
over `Semicolon`-separated operations, sharing one prologue and threading PREFIX/BASE
across operations per spec. Add a trailing-token/EOF assertion so a request that fails
to fully consume its input is an error (this also hardens single-op parsing).

**Execution — the subtle part.** SPARQL §3.1 requires each operation to observe the
graph-store state left by the previous one (the reason `-01c` differs from `-01`). So a
request must stage operations **sequentially against evolving state within one commit**:
each op's WHERE evaluates over the novelty overlay produced by the prior ops. Today one
commit stages one `Txn` (`tx_builder.rs:972-1007`, `stage_transaction_from_txn`). The
design choice is either (a) stage a `Vec<Txn>` sequentially, re-deriving the staged view
between ops before the single commit, or (b) N commits (simpler but changes commit
count / `t`, and is not atomic). Recommend (a) for atomicity and to match "one request =
one transaction." This is UPDATE-path-only code; it does not touch query execution.

### C — empty/prologue-only requests

Fold into B: an `UpdateRequest` with zero operations is valid and lowers to an empty
no-op `Txn` (the harness already treats an empty transaction as a valid no-op,
`query_handler.rs:431-433`). If B is deferred, a one-line fix in `parse_query_body`
(allow EOF after prologue for update context) covers C alone.

### D — `GRAPH` blocks in `DELETE WHERE`

`DELETE WHERE` is shorthand for `DELETE { P } WHERE { P }`. The Modify path already
supports concrete-IRI `GRAPH` blocks end to end (`lower_modify` 881-999, templates get
g_ids at `lower_quad_pattern_to_templates:1020-1038`). The fix is to make
`lower_delete_where` (`lower_sparql_update.rs:792-876`) route GRAPH-bearing quad
patterns through the same template + `SparqlWhereClause` machinery instead of erroring
at 827-830, and drop the validator rejection at `validate/mod.rs:154-165`. Blank-node→
existential-var rewriting must be preserved (it already is on the triple-only path).

### E — reject blank nodes in DELETE forms

Add a blank-node check to `validate_update_template_quad_pattern` (DELETE side only, for
Modify), `validate_delete_where`, and `validate_ground_quad_data` (DELETE DATA). Emit a
diagnostic mirroring the existing variable/GRAPH-var rejections. INSERT templates and
INSERT DATA must still **allow** blank nodes (CONSTRUCT-style), so the check is scoped to
delete contexts. test_54 additionally needs cross-operation bnode-label scoping, which
depends on B.

### F — USING + explicit-GRAPH scoping

Investigate `lower_sparql_where_patterns` (`stage.rs:1654-1668`) and the default-graph
selection at `stage.rs:1382-1399`: when `USING <g>` is present AND the WHERE contains an
explicit `GRAPH <h>` block, the explicit block must win for its own triples while USING
sets the default graph for un-GRAPH'd triples, and the (default-graph) DELETE template
must target the graph store's default graph — not `g`. The two failing tests are the
minimal reproductions; expected outputs are in `delete/delete-post-01f.ttl` /
`delete-post-02f.ttl`. This is the only class touching shared query-execution lowering.

---

## 3. Hot-path classification (all UPDATE work is off the query hot path)

| Class | Classification | Justification |
|---|---|---|
| A grammar/AST/parser | **parse-time** | new tokens already lexed; new AST variants + recursive-descent branches. Zero query-path code. |
| A execution (CLEAR/DROP/COPY/MOVE) | **prepare/commit-time** | a g_id-scoped read scan + retract/assert flakes, taken only by these verbs; the scan reuses read code but is not the per-row filter/join path and never runs for queries. |
| B parse | **parse-time** | request-level `;` loop + EOF check. |
| B execution | **commit-time** | sequential staging of `Vec<Txn>` within one commit; UPDATE staging/commit only. |
| C | **parse-time** | empty-request acceptance. |
| D | **parse/validate + lower-time** | reuse Modify template + SparqlWhereClause lowering; no new per-row code. |
| E | **parse/validate-time** | additional validator diagnostics. |
| F | **prepare-time** | dataset/default-graph selection is chosen once per operation in staging (`stage.rs:1382-1399`), not per-row. |

**Shared-code check (required by the brief).** The only shared surface with query
execution is UPDATE `WHERE` evaluation: `Modify`/`DELETE WHERE` WHERE clauses are lowered
through the shared SPARQL pipeline and run on the shared engine
(`stage.rs:1353-1359` → `lower_sparql_where_patterns` → planner/operators). **This is
already the case today for every `DELETE…WHERE`** — none of A/B/C/D/E adds new per-row
query code; they add parse/validate/staging code. **F** tweaks *which* graph the shared
WHERE runs against (prepare-time selection), not the operators themselves. The query hot
paths guarded by benches (`query_hot_bsbm`, `query_hot_bsbm_bi`) are not touched by any
class. Bench guardrails are therefore not strictly required for A–E; F should re-run the
query benches only to confirm the dataset-selection change is prepare-time (it is).

---

## 4. Surface parity (SPARQL + JSON-LD)

SPARQL and JSON-LD update share the `Txn` IR and engine; Fluree owns both grammars, so
the "SPARQL-possible ⇒ JSON-LD-possible" rule applies and each fix names its JSON-LD
regression test. JSON-LD update has its own parser producing the same IR, including
default- and named-graph WHERE scoping
(`fluree-db-transact/src/parse/jsonld.rs:310-446`,
`parse_update_where_{default_graph_iris,named_graphs}`).

**Cypher is out of scope for this burn-down.** Cypher is openCypher — Fluree does not
own the grammar and will not add custom syntax — so there is no support-matrix or
syntax-parity work here. Cypher benefits implicitly from every IR/engine-level fix (D,
F, and the execution half of A) because it lowers to the same `Txn`/engine
(`fluree-db-transact/src/lower_cypher_update.rs`); no per-fix Cypher assessment is
needed.

| Class | Surface category (per `sparql-compliance.md` §Query Surface Parity) | JSON-LD action |
|---|---|---|
| **A** CLEAR/DROP/CREATE/COPY/MOVE/ADD | **New IR/engine capability + SPARQL surface syntax.** Graph-store management verbs with no JSON-LD text form today. | The underlying "retract-all-in-graph" / "copy graph" capability is genuinely new and useful → **expose it as a JSON-LD/txn-builder capability + public API** (`GraphTransactBuilder::clear_graph`/`drop_graph`/`copy_graph`) in the same effort, with tests. Record decision if deferred. |
| **A** LOAD (remote) | SPARQL-only + external I/O | No JSON-LD equivalent; JSON-LD ingest is via the insert API. No parity. |
| **B** multi-op sequencing | Mostly SPARQL-only (text `;`). | If `Vec<Txn>` sequential staging is added to the IR, JSON-LD *could* gain an ordered array-of-ops form — **record as a design decision**, not required for parity. |
| **C** empty request | SPARQL-only. | N/A. |
| **D** GRAPH in DELETE WHERE | **IR/engine-level** (reuses Modify GRAPH lowering). | Fixes JSON-LD named-graph delete implicitly → **add a JSON-LD regression test** for delete-with-named-graph. |
| **E** bnode-in-DELETE validation | SPARQL surface validation. | JSON-LD delete templates should mirror the "no blank node in delete" rule → **add a JSON-LD negative test**. |
| **F** USING/graph scoping | **IR/engine-level** (staging dataset selection). | JSON-LD's `where` default/named-graph scoping shares this code → **add a JSON-LD regression test** exercising graph-scoped delete-where. |

**Exact regression-test files to add to** (per the compliance doc's "Where to add
parity tests" table, cross-checked against the tree):
- **SPARQL UPDATE** (D, F, and A execution): `fluree-db-api/tests/it_named_graphs.rs`
  (already drives `sparql_update` on named graphs) — or a new `it_sparql_update.rs`.
- **JSON-LD transactions** (D, E, F, and any A capability exposed): 
  `fluree-db-api/tests/it_transact_update.rs` and `it_named_graphs.rs`.
- A compliance fix is "done" only when the W3C register entry is removed **and** the
  corresponding JSON-LD test exists (compliance doc "Regression-test rule").

---

## 5. Blast radius, PR composition, risks, open questions

**Files touched (by class):**
- A: `fluree-db-sparql/src/ast/update.rs`, `.../parse/query/update.rs`,
  `.../parse/query/mod.rs`, `.../validate/mod.rs`;
  `fluree-db-transact/src/lower_sparql_update.rs`, `.../ir.rs`, `.../stage.rs`,
  `.../commit.rs`; `fluree-db-core/src/graph_registry.rs` (add a `remove` path only if
  true DROP is pursued); `fluree-db-api/src/{tx_builder.rs, graph_transact_builder.rs}`
  (public API for the exposed capability).
- B: `.../parse/query/mod.rs`, `ast` (request node), `fluree-db-api/src/tx_builder.rs`
  + `fluree-db-transact/src/{stage.rs, commit.rs}` (sequential staging).
- C: `.../parse/query/mod.rs` (folds into B).
- D: `.../validate/mod.rs`, `.../lower_sparql_update.rs`.
- E: `.../validate/mod.rs`.
- F: `fluree-db-transact/src/stage.rs` (+ possibly shared WHERE lowering).

**LOC ballpark:** A ≈ 500–800 (grammar+AST ~250, exec mapping ~300–500); B ≈ 250–400
(the sequential-staging design dominates); C ≈ 10 (or free with B); D ≈ 120; E ≈ 60;
F ≈ 80–200 (depends on how deep the USING/GRAPH interaction goes). Plus ~400 LOC of
JSON-LD/SPARQL parity tests.

**Suggested PR composition (each PR shrinks its register entries; both directions
enforced by CI):**
1. **PR-U1 — negative-syntax validation (E) + DELETE-WHERE-GRAPH (D).** Smallest,
   pure parse/validate/lower, no new grammar. Clears test_36, test_50/51/52,
   dawg-delete-insert-03..09, dawg-delete-where-02/04/06 (14 tests). Low risk.
2. **PR-U2 — multi-operation requests + empty request (B + C).** Parser request loop +
   sequential staging. Clears dawg-delete-insert-01c, test_38/39/40, and unblocks
   test_54; combined with A, unblocks the four basic-update tests. Medium risk
   (sequential-staging design).
3. **PR-U3 — graph-management grammar + CLEAR/DROP/CREATE + SILENT (A, most of it).**
   Grammar/AST/parser + CLEAR/DROP retract-all primitive + CREATE near-no-op + SILENT
   swallow. Clears clear/drop/create/*-silent + the syntax rejects (test_1-22,37) and
   many eval tests. Expose `clear_graph`/`drop_graph` on the builder + JSON-LD op here.
4. **PR-U4 — COPY/MOVE/ADD (A remainder).** Compose over PR-U3's primitives. Clears
   add*/copy*/move*.
5. **PR-U5 — LOAD (parse + SILENT no-op) + documented-divergence register entry for
   remote LOAD.** Clears load-silent/load-into-silent and the LOAD syntax tests.
6. **PR-U6 — USING/GRAPH scoping (F).** Isolated staging-semantics fix.

**Risks:**
- *B sequential staging* is the highest-risk item: getting "each op sees prior ops'
  writes, atomically, in one commit" right (novelty overlay between ops) without
  changing single-op behavior. Mitigate by keeping the single-op path byte-identical
  and adding the loop only when >1 op is present.
- *True DROP vs CLEAR*: implementing real graph unregistration touches the additive-only
  `GraphRegistry` and the index root; since the harness can't observe it, **do not build
  it for W3C** — ship CLEAR-semantics DROP and note the divergence.
- *F* is the only fix that risks perturbing shared WHERE lowering; keep the change at the
  prepare-time graph-selection layer and run query benches to confirm.

**Open design questions for the team:**
1. **Multi-op transaction model:** one atomic commit staging `Vec<Txn>` sequentially
   (recommended) vs N commits? Affects `t`/commit semantics and the builder API.
2. **DROP semantics:** ship DROP≡CLEAR (harness-equivalent) now and defer true
   unregistration, or add a `GraphRegistry::remove` + index-root story now?
3. **Remote LOAD:** documented divergence (recommended, zero W3C cost) vs an opt-in
   fetch hook? If a hook, where does it live given transact/api has no HTTP client?
4. **CREATE/empty graphs:** accept that Fluree cannot represent an empty named graph
   (near-no-op CREATE) as a permanent documented divergence?
5. **JSON-LD graph-management surface:** which A capabilities (clear/drop/copy) get a
   first-class JSON-LD/txn-builder form now vs later, per the parity guideline?
