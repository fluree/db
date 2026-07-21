# P2 — Bound-subject / bound-object path map (fluree/db virtual datasets)

Verification of Part 2 of the solo-side browse handoff. Read-only. Worktree `wt-parity` at rev `733cb2c8e` (confirmed `git rev-parse HEAD`). All line refs are at this rev.

## The crux: three paths, three different bound-term capabilities

The handoff's puzzle (same rev both refuses 2a and silent-empties 2b, yet bound-subject support "shipped") resolves because a virtual-dataset query can take one of THREE unrelated code paths, and each treats bound terms differently. Nothing is contradictory once you see which path each shape takes.

Path A — WHERE-pattern rewrite (`fluree-db-query/src/r2rml/rewrite.rs::convert_triple_to_r2rml`, reached for any `where` clause via the GRAPH-wrap below). Bound-IRI SUBJECTS convert and prune to their table. Bound-IRI OBJECTS convert ONLY when the predicate is a constant. VARIABLE-predicate + bound object REFUSES → this is 2a.

Path B — select-map hydration (`fluree-db-api/src/format/hydration.rs::format_subject`, reached by `{"select": {"<iri>": [...]}}` with no `where`). A constant-IRI root NEVER touches R2RML; it lowers to a `Root::Sid` hydration served by the native binary-index formatter, which finds no index on a virtual dataset and returns `{"@id": <iri>}` → this is 2b.

Path C — crawl expansion (`fluree-db-api/src/graph_source/crawl.rs::expand_wildcard_crawl`, reached by `{"select": {"?s": [...]}, "where": ...}`). This is the working "browse/detail" machinery. It silently DROPS any top-level `values` clause → this is the VALUES-workaround hazard.

How a non-GRAPH dataset query reaches Path A: `fluree-db-api/src/view/query.rs::maybe_wrap_for_graph_source` (`query.rs:28-42`) wraps every pattern of a graph-source query into a single `Pattern::Graph { name: gs_id, patterns }` when no explicit GRAPH is present. That routes the WHERE into `graph.rs::execute_in_graph` / `execute_in_graph_batched`, both of which call `rewrite_patterns_for_r2rml` and raise the 400 when `unconverted_count > 0` (`graph.rs:302-310` and the batched twin `graph.rs:506-512`).

## Task 1 — the refusal, and the admission/refusal decision tree

The refusal text lives ONLY in `fluree-db-query/src/graph.rs:302-310` (seeded path) and `graph.rs:506-512` (batched/uncorrelated fast path). Both fire on `rewrite_result.unconverted_count > 0`. The count is produced entirely by `convert_triple_to_r2rml` returning `None` for a triple (`rewrite.rs:167-171`: a `None` conversion pushes the original pattern back and increments `unconverted`).

The single decision function is `convert_triple_to_r2rml` (`rewrite.rs:1351-1481`). Decoded, per triple:

SUBJECT axis (`rewrite.rs:1358-1366`). `Ref::Var` → variable subject: CONVERTS. `Ref::Iri` → bound subject via `new_bound_subject`: CONVERTS. `Ref::Sid` that decodes → bound subject: CONVERTS; a `Ref::Sid` that does NOT decode to an IRI → `None` (UNCONVERTED). So a bound-IRI subject is NOT refused — the prose "Patterns with bound subjects (e.g. `<iri> ex:name ?o`)" in the 400 message is now STALE/misleading; those convert.

TYPE patterns (`?s a X`, `<iri> a X`, `?s a ?type`) always return `Some` (`rewrite.rs:1382-1416`): CONVERTS in every subject form, including a bound subject.

REGULAR predicate patterns (`rewrite.rs:1418-1480`). `predicate_filter` = the predicate IRI, or `None` when the predicate is a VARIABLE (`rewrite.rs:1423`). `object_constant` (`rewrite.rs:1442-1456`) is built ONLY when `predicate_filter.is_some()` — every arm is guarded on it (literal→`Scalar`, ref/IRI/Sid→`Iri`). `object_var` (`rewrite.rs:1457-1462`): a `Term::Var` object → variable object (converts); an object that is neither a var nor a resolved constant → `return None` (UNCONVERTED, comment "Bound object we cannot yet convert").

The decision tree (✓ = converts, ✗ = UNCONVERTED → 400):

`{?s ?p ?o}` full wildcard → ✓ (var subject, predicate_var, object_var).
`{<iri> ?p ?o}` bound-subject inspect → ✓ (bound subject + predicate_var + object_var); this is the subject-detail shape (see prune below).
`{<iri> <p> ?o}` bound subject + const pred → ✓.
`{?s <p> ?o}` const pred → ✓.
`{?s a <Class>}` / `{?s a ?t}` → ✓.
`{?s <p> "lit"}` const pred + loose-matchable literal object → ✓ (`ObjectConstant::Scalar`, also emits a pruning filter).
`{?s <p> <iri>}` const pred + bound IRI object → ✓ (`ObjectConstant::Iri`, operator-enforced, NO scan prune — see Task 6).
`{?s ?p <iri>}` VAR pred + bound object → ✗ 400. THIS IS 2a.
`{?s ?p "lit"}` VAR pred + bound literal → ✗ 400.
`{<iri> ?p <iri2>}` bound subject + VAR pred + bound object → ✗ 400 (object arm still fails).
`{<badsid> …}` bound Sid that won't decode → ✗ 400.

So 2a's shape `{"@id": "?referencingSubject", "?property": {"@id": "<subjectIri>"}}` = `?ref ?property <subjectIri>` = variable predicate + bound IRI object → the exact `{?s ?p <iri>}` refusal row. It is a bound-OBJECT (inbound-edge) refusal; the message's "bound subjects" clause does not describe what actually triggered here.

Single-pattern admissibility (the task's explicit question): `{<iri> ?p ?o}` qualifies (✓, bound-subject wildcard). `{?s <p> <iri>}` qualifies (✓, const pred + IRI object). `{?s ?p <iri>}` does NOT (✗). Admission needs NOTHING beyond a resolvable predicate for the object arm OR a var object — it is not a multi-predicate requirement. The W4-1/TemplateKey machinery is a PERF layer on top (below), not an admission gate.

The prune (why a bound-subject inspect is cheap, not a fan-out). Once `{<iri> ?p ?o}` converts to a `subject_constant` pattern, two prunes fire in the operator: (1) TABLE prune — `operator.rs:917-936` keeps a TriplesMap only if `subject_iri.starts_with(constant_prefix(template))`, i.e. the bound IRI must be producible by that TM's subject template (necessary condition; per-row equality at `subject_term_matches_iri` still enforces exactness). (2) FILE prune — `reverse_subject_template(template, subject_iri)` (`operator.rs:737-742`) recovers the raw key column value as a `ScanValue::TemplateKey`, pushed to Iceberg for file-level pruning. Test receipt: `crawl.rs:1431-1456` (`bound_subject_inspect_prunes_to_matching_table`) proves `{"@id":"…/person/1","?p":"?o"}` scans `people` ONLY, not `orders`.

## Task 2 — 2b root cause (the silent empty)

2b (`{"select": {"<iri>": ["*"]}}`) is NOT a swallowed refusal and NOT a template-reversal miss. It is a genuinely-unimplemented arm that defaults to native hydration over an absent index.

Chain. `{"select": {"<iri>": ["*"]}}` parses to an `UnresolvedHydrationSpec` with an `UnresolvedRoot::Iri` (`ast.rs:823-851`, the "IRI constant root (no WHERE needed)" example at `ast.rs:840-841`). Lowering encodes the IRI to a SID: `UnresolvedRoot::Iri → Root::Sid(sid)` (`lower.rs:1448-1457`; unknown IRIs fall back to `Sid::new(0, iri)`). At format time, `format_hydration_column` matches only `Root::Sid`/`Root::Var` (`hydration.rs:480-490`) → `RootRef::local(sid)` → `format_subject` (`hydration.rs:520-530`). `format_subject` emits `{"@id": …}` then appends properties read from the BINARY INDEX (`hydration.rs:1104-1169`). A virtual dataset has no binary index, so zero flakes are read → the bare `{"@id": <iri>}` stub.

The crawl interceptor cannot catch it. `detect_wildcard_crawl` (`crawl.rs:439-471`) rejects a constant-IRI root twice: the `subject_var.starts_with('?')` gate (`crawl.rs:449-451`) and the mandatory `where` gate (`crawl.rs:453`, `obj.get("where")?`). 2b has neither. So `maybe_expand_crawl` returns `None` and the query falls through. The codebase KNOWS this failure: `graph_query_builder.rs:304-308` comment — "otherwise the same crawl issued with tracking headers falls through to native hydration and returns []."

What the working detail path does differently. The chat/browse detail path sends a WHERE-pattern form — either a variable-subject crawl `{"select":{"?s":["*"]},"where":{"@id":"?s",…}}` (Path C) or the bound-subject-in-WHERE inspect `{"@id":"<iri>","?p":"?o"}` (Path A, the `crawl.rs:1425-1430` comment: "the exact lowered shape a single-subject detail view should send: a constant `@id`, which lowers to `subject_constant=Some` and hits the prune"). Both reach the R2RML operator and scan the table. The select-map hydration path (Path B) never does. Could the select-map route onto the working machinery? YES — see Option A in Task 6; it is the intended fix and reuses proven code.

## Task 3 — native contract (the parity question)

Native returns the SAME `{"@id": <iri>}` stub for a genuinely nonexistent / property-less subject. `format_subject` unconditionally seeds `@id` (`hydration.rs:1104-1106`) and then adds only the properties its index scan yields; a subject with no flakes yields none → `{"@id": <iri>}`. Native produces a full document only for a subject that is actually present with properties.

Consequence, exactly as the handoff fears: virtual's empty-for-UNSUPPORTED is byte-identical to native's empty-for-MISSING. There is no discriminator by design. This is why the correct answer to 2b is parity-shaped and is Option A, not a refusal: `customer/1` IS present in the Iceberg table, so returning the "absent" stub for it is the bug; native would return a full doc for a present subject, so virtual must too. A refusal (Option B) would DIVERGE from native, which never refuses a subject-detail select-map — it just returns what exists.

## Task 4 — the VALUES workaround (code-level prediction; sibling agent tests live)

Prediction for the EXACT handoff shape (`{"values":["?s",[{"@id":"<iri>"}]], "where":[{"@id":"?s","@type":"<Class>"}], "select":{"?s":["*"]}}`): CORRECTNESS HAZARD — the crawl path silently drops the VALUES filter and over-returns.

Why. The select is `{"?s":["*"]}` with a `where`, so `detect_wildcard_crawl` MATCHES it as a variable-subject crawl → Path C `expand_wildcard_crawl`. But `detect_wildcard_crawl` reads only `select`/`where`/`@context`/`limit`/`offset` (`crawl.rs:439-471`) and `build_flat_query` reconstructs a fresh query from only those fields (`crawl.rs:529-585`). Neither reads `values` — grep of `crawl.rs` finds zero `values` handling. So the executed flat query is `{"@type":"<Class>"} + {?s ?p ?o}` with NO VALUES constraint. `?s` is still bound (by the `@type` scan), so the crawl runs over EVERY `<Class>` instance. With no user `limit`, `flat_limit` is `None` (`crawl.rs:180-185`) and `take = usize::MAX` (`crawl.rs:329`) → an unbounded full scan of the class table (~1.74M Customers) returning all of them as node docs, not just `customer/1`.

Contrast — the OTHER VALUES shape (flat select, e.g. `{"values":["?s",…],"select":["?p","?o"],"where":{"@id":"?s","?p":"?o"}}`): NOT a crawl select-map, so VALUES is preserved and the query is CORRECT-BUT-FULL-SCAN. A subject-IRI VALUES set never lowers to a scan prune: `collect_values_pushdown` (`rewrite.rs:660-685`) requires each row to be a single `Binding::Lit` scalar (`rewrite.rs:676`); an IRI/ref binding "declines the WHOLE set" (`rewrite.rs:656-657`). So `?s` stays a post-scan join variable — the class scan runs full, VALUES joins after. Matches the `crawl.rs:1429` comment ("A VALUES-bound `?s` stays a variable and does NOT [prune]").

Bottom line: VALUES does NOT give Solo a safe client-side workaround for the subject-detail (`select`-map) shape — with a select-map it is silently WRONG; only the flat-select form is correct, and it is a full scan. Recommend Solo NOT adopt it; do Option A instead.

## Task 5 — error-envelope audit (2a's actual ask)

Today there is NO distinct machine-readable token. 2a is `QueryError::InvalidQuery(String)` (`fluree-db-query/src/error.rs:42-44`), a stringly-typed variant shared by every malformed query. Two exposure surfaces, both generic:

Buffered (normal POST) path — the only machine signal is the HTTP status from `ApiError::status_code()` (`fluree-db-api/src/error.rs:436-505`). `ApiError::Query(_)` falls into the catch-all 400 arm alongside Parse/Config/Sparql/Cypher/Turtle/Json/Batch/Format (`error.rs:473-485`). There is NO `ApiError::error_code()` body-code method — `error_code()` exists ONLY on `BuilderError` (`error.rs:39-49`, returns `err:api/*` tokens) and does not cover query errors. So 2a is an undifferentiated HTTP 400.

Streaming (ndjson) path — `query_error_code` (`stream_query.rs:438-451`) collapses `InvalidQuery | InvalidFilter | InvalidExpression` all to `"invalid_query"`. Still not distinct.

The precedent Solo referenced (R3-B). `MemoryBudgetExceeded` gets its distinct signal by being a distinct STRUCTURED variant (typed fields `used_bytes`/`budget_bytes`, `error.rs:76-80` in the query crate) mapped to a distinct HTTP 507 (`fluree-db-api/src/error.rs:460-462`; doc "distinct from the 408 timeout so the caller can degrade on it specifically"). Same mechanism for `StorageAccessDenied`/`CatalogCredentialsNotVended` → 403 (`error.rs:467-472`). The "machine-readable code" is the enum-variant → HTTP-status mapping, not a body field.

Additive typed-code design for the R2RML refusal (native-safe). Add a new structured variant, e.g. `QueryError::R2rmlUnsupportedPattern { graph_source: String, count: usize }`, raised at `graph.rs:302-310`/`:506-512` in place of the ad-hoc `InvalidQuery(format!(…))`. Keep its `#[error(...)]` Display equal to today's prose so existing prose-matchers keep working. Then add one arm to `ApiError::status_code()` mapping it to a distinct status — recommend 422 Unprocessable Entity (semantically "understood, cannot process this pattern"; cleanly outside the 400 bucket) — and one arm to `query_error_code()` → `"r2rml_unsupported_pattern"` for streaming consumers.

Additive-safety: adding an enum variant is source-additive; the two status/code matches both have `_ =>` fallbacks so the new variant is safe until explicitly mapped, and no existing variant's Display/status/code changes. Native queries never raise it (it is produced only in the R2RML rewrite path). The envelope SHAPE is unchanged — only a new status VALUE (422) appears, and only for the virtual bound-object refusal. This satisfies "no native-contract changes": nothing existing moves. (Any exhaustive `match QueryError` elsewhere is compile-checked, never a silent break.)

## Task 6 — design options (no code) and tier-3 sizing

2b options.

Option A (RECOMMENDED, parity-ideal, reuses proven code): route a constant-IRI-root select-map onto the bound-subject scan. Concretely, extend `detect_wildcard_crawl` to accept a constant-IRI root (dropping the `?`-prefix and mandatory-`where` gates for that case) and have `build_flat_query` inject a CONSTANT-subject wildcard scan `{"@id":"<iri>", ?p, ?o}` instead of `{"@id":"?s", …}`. That lowers to `subject_constant=Some` and hits the existing table-prune (`operator.rs:917-936`) + TemplateKey file-prune (`operator.rs:737-742`) — the exact machinery `crawl.rs:1431-1456` already proves. Present subject → full doc (parity with native). Non-matching / row-absent IRI → template reverses to zero TMs → empty scan → `{"@id":<iri>}` stub, which is CORRECT (native returns the same for an absent subject). So Option A gives full native parity for both present and absent subjects and needs no new operator capability.

Option B (cheap, non-parity): make Path B refuse like 2a. Consistent with 2a and stops the empty-inspector, but DIVERGES from native (which never refuses a subject-detail select-map) and still blocks the inspector. Only attractive if A is deferred.

Option C (hybrid, NOT recommended): try reversal, refuse if no template matches. The "refuse on no match" half is wrong — a non-matching IRI SHOULD return the stub (parity with native-absent), not refuse. C reintroduces a divergence A avoids. Prefer A.

2a code: the additive `R2rmlUnsupportedPattern` variant + distinct 422 (Task 5).

Tier-3 (variable-predicate inbound edges) sizing. First, a correction to the handoff's grouping: of shapes 8/9/10, only shape 8 is actually refused. Shapes 9 (`{"select":"(count ?s)","where":[{"@id":"?s","<prop>":{"@id":"<iri>"}}]}`) and 10 (`{"select":"?s","where":[{"@id":"?s","<prop>":{"@id":"<iri>"}}],"limit":5}`) have a CONSTANT predicate + bound IRI object → they CONVERT (`ObjectConstant::Iri`, `rewrite.rs:1451`) and run today as a CORRECT-BUT-FULL-SCAN of the referencing table(s) carrying `<prop>` (operator-enforced IRI match, no scan prune yet — `provider.rs:107-110`). Solo can un-gate 9/10 if a full scan per inbound-edge query is acceptable (on 1.74M rows it is slow but correct); only shape 8's "via which property?" (variable predicate) truly needs new engine work.

Shape 8 support shape and size. The refusal is precisely that a bound IRI object cannot coexist with a `predicate_var` (the object arm returns `None` before a `predicate_var` can attach). Minimum correct version: allow an `R2rmlPattern` to carry BOTH `predicate_var` and `object_constant=Iri`; the operator then wildcard-scans predicates and keeps rows whose materialized object equals `<iri>`, binding `?property` to the matching predicate. Unpruned, that scans EVERY table's every RefObjectMap and materializes every ref — impractical at 1.74M×N. The tractable version adds reverse-template pruning: reverse `<iri>` against each parent TriplesMap's subject template to identify its class, enumerate the RefObjectMaps across all TMs whose `parentTriplesMap` targets that class, and scan ONLY those referencing tables. Size: S/M for the correct-but-unpruned operator change (reuses `ObjectConstant::Iri` enforcement + the multi-TM scan already in place); M/L once you add the reverse-template-to-target-class pruning needed to make it usable at scale — and that reverse-FK-template match is the same "template reversal" follow-on already filed for FK-IRI scan pruning.
