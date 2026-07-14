# W3C SPARQL Burn-Down — Consolidated, Verified Implementation Roadmap

**Status:** Stage-3 synthesis. Inputs: the eight Stage-1 cluster audits in this
directory, eight Stage-2 adversarial verification passes (one skeptic per doc,
code-level + empirical), and a Stage-2 cross-check (ownership, movements,
conflicts, shrink accounting). Parent context:
`docs/audit/2026-07-sparql-testsuite-audit.md` (§6 perf discipline, §7 phases)
and `docs/contributing/sparql-compliance.md` (§ Query Surface Parity — JSON-LD
only; Cypher excluded).
**Baseline:** branch `test/sparql-testsuite-full-coverage`, rdf-tests
`efccbc6b8`, register `testsuite-sparql/tests/registers/mod.rs` @ 604 entries
(573 unique tests; the 31 syntax-update-1 tests are double-registered).
**Ground-truth split:** 63 not-applicable entries + 541 gap entries. The parent
audit's "538 gaps + 67 NA" headline is stale by 4; use 63/541.

---

## 1. Verification summary

55 load-bearing claims were adversarially verified across the eight docs:
**40 CONFIRMED, 13 REFUTED, 2 UNCERTAIN.** Every file:line citation spot-checked
in the CONFIRMED set matched the code; all reproduced failures reproduced
exactly. The docs are trustworthy design inputs *after* the corrections below.

| Cluster doc | Claims | Confirmed | Refuted | Uncertain |
|---|---|---|---|---|
| `update-completeness.md` | 7 | 7 | 0 | 0 |
| `parser-syntax-validation.md` | 7 | 5 | 2 | 0 |
| `named-graph-dataset.md` | 6 | 3 | 3 | 0 |
| `expression-semantics.md` | 8 | 5 | 2 | 1 |
| `lexer-formatter.md` | 6 | 4 | 2 | 0 |
| `sparql12-wave1.md` | 6 | 4 | 2 | 0 |
| `sparql12-wave2-triple-terms.md` | 7 | 6 | 1 | 0 |
| `residual-eval.md` | 8 | 6 | 1 | 1 |
| **Total** | **55** | **40** | **13** | **2** |

### 1.1 REFUTED claims and how this roadmap hedges them

1. **"P1 collection desugaring greens `construct#constructlist`"**
   (`parser-syntax-validation.md` §3). CONSTRUCT templates containing blank
   nodes currently evaluate to an **empty graph** (template bnodes lower to
   never-bound variables and are dropped); constructlist additionally needs
   per-solution bnode instantiation in the CONSTRUCT output path.
   *Hedge:* PR-1 must **not** remove the constructlist register entry (the
   both-directions CI check would fail); constructlist moves to the new
   **serialization cluster** (§2, unowned-work item W-2) alongside
   construct-3/4 and quotes-3/4. `residual-eval.md` §3.4's delegation of
   constructlist to the syntax PR is void.
2. **"An `Expression::variables()` helper already exists — reuse it"**
   (`parser-syntax-validation.md` §2 V4). No such helper (nor
   `contains_aggregate()`) exists. *Hedge:* PR-2 budgets writing both walkers
   in `ast/expr.rs`; they are shared with the nested-aggregate check
   (`sparql12-wave1.md` §1.2), which is folded into the same PR.
3. **"Removing the #1279 default-graph enumeration updates exactly two tests"**
   (`named-graph-dataset.md` §6). A third test pins the extension:
   `fluree-db-api/tests/it_upsert_duplicate_ids_repro.rs:228` (file-backed,
   binary-index path). *Hedge:* PR-G1's blast radius and reviewer sign-off
   scope include all three test updates.
4. **"#1317 plausibly shares BUG-2's root cause"** (`named-graph-dataset.md`
   §6 open Q5). #1317's signature (concrete `GRAPH <iri>`, multi-level index,
   wipe-then-upsert sequence) is incompatible with the unbound-`?g` enumeration
   arm. *Hedge:* #1317 is routed to its own graph-registry/indexing
   investigation (issue comment, §5); it is **not** closed against PR-G1;
   post-land verification covers subquery04 only.
5. **"BUG-4: the GRAPH-exist unit row is dropped downstream (projection /
   SPARQL-JSON writer)"** (`named-graph-dataset.md` §1). Empirically false —
   a 0-column/1-row batch survives projection and formatting intact.
   `graph-exist` actually fails on **relative-IRI/BASE resolution** (the query
   names `<data-g1.ttl>`, the graph is registered under the absolute URL).
   *Hedge:* PR-D is **deleted from the slate**; graph-exist reassigns to
   **PR-BASE** (new). No shared projection/format code is touched.
6. **"The harness `.rdf` DAWG parser drops solutions containing unbound
   variables"** (`expression-semantics.md` §6). Attribution to the harness is
   correct but the mechanism is backwards: `parse_rdf_dawg_result_set`
   (`result_format.rs:809`) treats `Event::Start | Event::Empty` identically
   (`:835`), so self-closing `<rs:value rdf:resource=…/>` elements skew the
   state stack — it *keeps* the unbound solution and drops the bound ones.
   *Hedge:* PR-H1 fixes the `Event::Empty` handling (pop/no-push), and audits
   the identical pattern in the SRX parser at `result_format.rs:413`.
7. **"#1319 survives the scan path via `dt_compatible`"**
   (`expression-semantics.md` §1). `dt_compatible` is asymmetric and is not
   consulted for ordinary pattern objects — they carry **no** datatype
   constraint at all (`lower/term.rs:164-166`, deliberate). The proposed fix
   (lower bare integers to `xsd:integer`) is still correct. *Hedge:* nobody
   "extends dt_compatible"; **D5b** (`open-eq-02`) is re-located to scan-path
   datatype constraints — a deliberately-disabled, per-flake, perf-sensitive
   change. It becomes an explicitly at-risk item inside PR-X2 with its own
   bench gate, deferrable with a register entry.
8. **"All RDF-lexical double output paths = the 4 cited sites"**
   (`lexer-formatter.md` §1b). Misses `fluree-db-api/src/export.rs:1236-1249`
   and `:1465-1477` (N-Triples/N-Quads export, `format!("{f:E}")` → `"1E6"`),
   and `LiteralValue::lexical()` is also consumed by the R2RML extractor
   (`fluree-db-r2rml/src/loader/extractor.rs:457,478`). *Hedge:* PR-L2 scope
   includes the export sites; the R2RML/Iceberg workstream is flagged on the
   PR; DoD requires a whole-workspace consumer grep.
9. **"The bnode-dot lexer fix strictly widens what lexes; byte-identical for
   currently-valid input"** (`lexer-formatter.md` §2). Spec-valid `_:a..b`
   lexes today and would break under the doc's proposed loop (which also does
   not compile). *Hedge:* PR-L1 uses the **greedy-scan + trailing-dot-rewind**
   design (keep the existing per-char predicate; post-scan, rewind the input by
   the count of trailing dots) — fixes `_:o6.` and keeps `_:a..b`
   byte-identical.
10. **"`EdgeKey::to_reifies_facts` is the shared encoder both ingest paths must
    use"** (`sparql12-wave1.md` §2.3). The JSON-LD path uses a JSON mirror of
    the `_jsonld_compatible` shape (omits `f:reifiesDatatype`); calling the
    full variant from Turtle would emit an extra flake and leave durable orphan
    flakes under cascade retracts. *Hedge:* PR-W15 hard-codes
    `to_reifies_facts_jsonld_compatible`; bit-identity is guarded by the
    `edgekey_roundtrip_*` equivalence tests, not "by construction".
11. **"40 of 41 eval-triple-terms fail at qt:data load"** (`sparql12-wave1.md`
    §2.1). It is 39; `update-3` fails parsing the **expected** TriG-star graph
    *after* its `{| |}` INSERT DATA already executed. *Hedge:* update-3's
    register re-point names harness expected-graph TriG-star parsing, and the
    fact that `{| |}` INSERT DATA already executes is recorded as scoping
    intel for the Option-1 epic.
12. **"inside-anonreifier/reifier-01/02 all fail on the object-position `<<>>`
    gap"** (`sparql12-wave2-triple-terms.md` §1.1). The `-02` pair sits inside
    RDF collections `( … )`, which are wholesale unparsed. *Hedge:* PR-W2A
    **depends on PR-1** (collections); the two PRs coordinate register removal
    across `SPARQL10_SYNTAX` syntax-lists and the wave-2 register so neither
    trips the stale-entry check.
13. **"agg02's fix target is the streaming `GroupAggregateOperator`
    variable-count finalize"** (`residual-eval.md` §3.3). That finalize
    already emits `xsd:integer` and is shared by the *correct* COUNT(\*) path;
    the re-typing provably happens elsewhere (plausibly shared encoded-binding
    materialization — per-row-relevant). *Hedge:* agg02 inside PR-X2 is an
    **investigation item**: probe first, classify the real site against §6,
    bench-gate if it lands in shared materialization, write the JSON-LD parity
    test against the real site.

### 1.2 UNCERTAIN claims and what settles them

1. **"The D5/D7 comparison-lattice fix keeps same-type fast paths
   byte-identical"** (`expression-semantics.md` §2/§3). It is the *mandated*
   shape (§6.2) and the infrastructure exists (prepare-time expression
   analysis, per-predicate datatype stats), but `eval.rs:147` must newly read
   `dtc` per string-literal row whenever static type info is unavailable
   (fresh/unindexed ledgers have no stats). *Roadmap hedge:* treated as a
   **bench-gated goal**, not a fact — PR-X2 acceptance is `query_hot_bsbm` +
   `query_hot_bsbm_bi` within `regression-budget.json`; if the budget blows,
   the fallback design (dtc niche-packing in `ComparableValue`) is invoked
   before any budget waiver is discussed. *Settled by:* implementing and
   running both benches.
2. **"6 residual tests are latent passes waiting only on the graph fix"**
   (`residual-eval.md` §1). Only 4–5 are cleanly latent;
   `agg-empty-group-count-graph` additionally needs enumerable empty named
   graphs (decision D-6) and `exists03` has an unverified EXISTS
   active-graph-scoping sub-check. *Roadmap hedge:* PR-G1/PR-BASE acceptance
   criteria list pp34, pp35, subquery02, subquery04 as expected greens and
   exists03 + agg-empty-group-count-graph as **conditional**. *Settled by:*
   running the graph-fixed build against all six.

Additional verified corrections that are not formal claim verdicts but bind the
plan: PR-X1's yield is ~35–36 entries, not ~40 (tP-03/04/05/21 need PR-X2 —
`expression-semantics.md` D4 contradicts its own §5); wave1 PR-4's GROUP BY
term-key (`group01`) is unimplementable as scoped because integer lexical forms
are canonicalized at ingest — it is gated on the D6 lexical-form decision;
`fluree-db-api` swallows parse-error diagnostics whenever an AST survives
recovery (`helpers.rs:271-285`), so PR-3's grammar tightening must make V1/V2
errors prevent AST production or the product API silently keeps accepting;
`parse_trig_phase1` (`trig_meta.rs:1199`, wired via `tx_builder.rs:146`)
already extracts multi-graph TriG — pure-TriG eval blockers may be harness
routing, not new parser work.

---

## 2. Final PR slate

Consolidated per the team preference for fewer, larger, thematically-coherent
PRs: **17 firm PRs + 5 decision-gated items + 2 unowned work-streams**. Per the
update-completeness skeptic, the standard bench gates (`query_hot_bsbm`,
`query_hot_bsbm_bi`, `insert_formats`, `import_bulk` within
`regression-budget.json`) run on **every** PR — no waivers; "extra gates" below
are additions. "Shrink" counts **register entries** (syntax-update-1 tests
count twice — both register copies must be deleted). Decisions D-1…D-12 are
defined in §4.

### Wave 0 — zero/near-zero engine risk, all parallel-safe

| PR | Scope | Owning doc(s) | Shrink | Risk | Extra gates / JSON-LD tests owed | Gated on |
|---|---|---|---|---|---|---|
| **PR-H1 — harness + hygiene** | Fix `parse_rdf_dawg_result_set` `Event::Empty` state-stack skew (`result_format.rs:809/:835`); audit the same pattern in the SRX parser (`:413`); apply the full register-comment correction batch (§6.1) and the parent-audit corrections (§6.3) | `expression-semantics.md` §6 (corrected per §1.1-6) | **−3** (dawg-sort-3/6/8) | None (harness-only) | None / none | — |
| **PR-1 — parser accepts valid syntax** | P1 collections (desugar to `rdf:first/rest/nil`), P4 empty IRIREF `<>`, P5a function-`NIL`, P5b bare ORDER BY constraint, P6 sub-select placement, P7 `VALUES ()`, P8 path-in-`[]`; + SPARQL 1.2 codepoint-escape pre-pass (`Cow` fast path) | `parser-syntax-validation.md` §2 PR-1; `sparql12-wave1.md` §1.1 | **−34** (24 syntax positives + list-1..4 + CODEPOINT_ESCAPES −6). **NOT constructlist** (§1.1-1). base-prefix-1 transfers to PR-BASE | Low (accept-more only) | None extra / `@list`↔first-rest parity test in `it_query.rs`; surface-only decisions recorded | — |
| **PR-L1 — Turtle bnode-dot lexer** | Greedy-scan + trailing-dot-rewind in `parse_blank_node_name` (NOT the doc's loop — §1.1-9); unit tests incl. `_:a..b` byte-identity | `lexer-formatter.md` §2 (corrected) | **−4** (JSON_RES) | Low, import-hot file | `insert_formats`+`import_bulk` are the primary gates / post-insert JSON-LD-surface query test | — |
| **PR-L2 — canonical `xsd:double`** | Shared `canonical_xsd_double` helper; route `sparql.rs`, `sparql_xml.rs`, `delimited.rs`, `LiteralValue::lexical()`, **and `export.rs:1236/:1465`** through it; zero-alloc write-into-buffer variant for `delimited.rs`; JSON-LD numeric output untouched; changelog | `lexer-formatter.md` §3 (scope extended per §1.1-8) | **−1** (csv03) | Low; user-visible output change | Whole-workspace `lexical()` consumer grep (R2RML flagged) / JSON-LD number-output guard tests stay unchanged | — |
| **PR-X1 — expression cheap high-yield** | D1 constant-FILTER placement, D2 DATATYPE/LANG expression args, D2b non-literal type error (SPARQL-scoped; covers **projexp05** and RDF11 `langstring-datatype`/`plain-string-datatype`), D3 dateTime cast, D9 BNODE per-solution, D10 regex `q`, #1319 bare-int lowering | `expression-semantics.md` §5 PR-A; `residual-eval.md` §3.3; `sparql12-wave1.md` §1.3 (DATATYPE half) | **≈−39** (~35-36 expr entries — tP-03/04/05/21 stay for PR-X2 — + projexp05 + RDF11 −2). D8/iri01 transfers to PR-BASE | Low-medium (cold/erroring paths only) | Standard gates / D1, D2/D2b, #1319 JSON-LD tests per `expression-semantics.md` §4 | — |
| **PR-PP — property-path operator** | pp16 zero-length node universe (all graph terms incl. **literals** — internal pair buffers must carry full terms, larger than the doc's "node set only" wording); pp36 `empty_schema_with_len` guard | `residual-eval.md` §3.2 | **−2** | Low-medium, contained to `property_path.rs` | Add `query_hot_property_path` bench and gate on it / record SPARQL-only decision | — |

### Wave 1 — parse/validate behavior changes + first UPDATE PRs

| PR | Scope | Owning doc(s) | Shrink | Risk | Extra gates / JSON-LD tests owed | Gated on |
|---|---|---|---|---|---|---|
| **PR-2 — semantic validation passes** | V3 bnode-scope, V4 group/projection scope, V5 BIND scope, V6 dup alias (write `contains_aggregate()` + free-var collector from scratch — §1.1-2); + nested-aggregate and duplicated-VALUES-var checks (SPARQL12_SYNTAX); + wave-2 negative (annotation blocks in INSERT/DELETE DATA) | `parser-syntax-validation.md` §2 PR-2; `sparql12-wave1.md` §1.2; `sparql12-wave2-triple-terms.md` §2/PR-D | **−27** (24 + 2 + 1) | Medium (reject-more) | Standard / JSON-LD analytical group/BIND-scope rejection tests (`it_query_analytical.rs`, `it_query_grouping.rs`); shared-checker-vs-authored-tests decision recorded | D-4 |
| **PR-3 — dot + FILTER grammar tightening** | V1 load-bearing dots, V2 FILTER Constraint. **Errors must prevent AST production** (or the API must honor error-severity diagnostics) — otherwise `fluree.query` keeps mis-executing (§1 addendum). Single owner for the trailing-token/EOF assertion shared with PR-U2 | `parser-syntax-validation.md` §2 PR-3 | **−13** | Highest parser regression surface; isolate for bisect | Standard; full query-suite reruns both directions / changelog + migration note | D-4 |
| **PR-U1 — UPDATE validation (classes D+E)** | Reject bnodes in DELETE forms; route DELETE-WHERE-GRAPH through Modify template lowering (drop validator rejection at `validate/mod.rs:154`); amend the compliance-doc parity table with an UPDATE/transact row (`it_transact_update.rs`, `it_named_graphs.rs`) | `update-completeness.md` §2 D/E, PR-U1 | **−19** entries (test_36 + test_50/51/52 ×2 registers; delete-insert-03..09; delete-where-02/04/06) | Low | Standard / JSON-LD named-graph-delete test + JSON-LD bnode-in-delete negative test | — |
| **PR-W2A — reifier-form parser extensions** | Object-position/nested/standalone `<<>>`, `~`-inside-`<<>>`, multi-reifier/annotation, richer annotation blocks; lower to existing `EdgeAnnotation` where evaluable else clean `not_implemented` | `sparql12-wave2-triple-terms.md` §7 PR-A | **−63** (60 TRIPLE_TERMS_POSITIVE + **3 SPARQL12_VERSION** — the omission the cross-check caught) | Low (parse-time; small lowering surface) | Standard / **JSON-LD regression for object-position-reifier reachability through `Pattern::EdgeAnnotation` — explicit deliverable, not prose** | **Depends on PR-1** (collections for inside-\*-02) |

### Wave 2 — decision-gated semantics + big engine PRs

| PR | Scope | Owning doc(s) | Shrink | Risk | Extra gates / JSON-LD tests owed | Gated on |
|---|---|---|---|---|---|---|
| **PR-G1 — GRAPH operator W3C conformance** | BUG-1 `?g` as IRI, BUG-2 drop implicit default-graph enumeration (keep explicit alias addressing), BUG-3 seed `?g` into inner subplan, BUG-5 EncodedSid/EncodedLit extraction; **explicitly adopts residual defects A3/A4** (subquery02/04 acceptance criteria). Updates **three** pinning tests (`it_query_dataset.rs:1591/:1752`, `it_upsert_duplicate_ids_repro.rs:228`) | `named-graph-dataset.md` §3 PR-A/B/C merged; `residual-eval.md` §3.1 | **−14** (9 /graph/ + graph-variable-join + graph-optional + exists-graph-variable + subquery02 + subquery04). `bindings#graph` stays (D-6) | Medium; semantics change, 2-reviewer sign-off | Standard / 3 FQL tests per `named-graph-dataset.md` §5 — **BUG-5's test must use the EXISTS/late-materialized shape** (top-level scan-bound `?g` passes today) | D-2 |
| **PR-BASE — base-relative IRI resolution** *(new work item; fills the ownership vacuum)* | Thread the query BASE through lowering/plan: constant GRAPH IRIs, FROM/FROM NAMED clause IRIs, `IRI()`/`URI()` (D8), output IRIs. Replaces phantom PR-D | Vacuum identified by cross-check; halves in `parser-syntax-validation.md` open-Q3 and `expression-semantics.md` D8 | **≈−7 direct** (graph-exist, iri01, base-prefix-1 eval, base-prefix-2/5) **+3 joint** with PR-G1 (pp34, pp35, exists03 — second lander removes) | Low-medium (prepare/lower-time) | Standard / IRI()-base JSON-LD test | — (but PR-G2 depends on it) |
| **PR-U2 — multi-operation UPDATE requests (classes B+C)** | Request-level `;` loop sharing one prologue; trailing-token/EOF assertion (coordinate single implementation with PR-3); sequential staging of ops against evolving novelty within **one atomic commit**; empty/prologue-only request = valid no-op; cross-op bnode-scope validation (test_54) | `update-completeness.md` §2 B/C, PR-U2 | **−9** (01c + test_38/39/40 ×2 + test_54 ×2); **unblocks** insert-05a + 3 same-bnode (green after PR-U3) | Medium-high (sequential staging design; single-op path must stay byte-identical) | Standard — this PR touches the shared parse entry and commit path, so gates are mandatory / array-of-ops JSON-LD surface decision recorded | **D-10** (Txn IR model) |
| **PR-U3 — graph-management verbs (class A)** | Grammar/AST/parser for LOAD/CLEAR/DROP/CREATE/ADD/COPY/MOVE + SILENT; retract-all-in-g_id staging primitive; DROP≡CLEAR; CREATE near-no-op; COPY/MOVE/ADD composed over CLEAR; LOAD parse + SILENT-swallow, remote LOAD as documented divergence. Expose `clear_graph`/`drop_graph`/`copy_graph` on the transact builder + JSON-LD surface (merges the docs' U3+U4+U5) | `update-completeness.md` §2 A, PR-U3/U4/U5 | **≈−91** (remaining SPARQL11_UPDATE eval + SYNTAX_UPDATE_1 ×2 entries; completes the 4 same-bnode tests jointly with PR-U2) | Medium (large but off query path) | Standard / builder + JSON-LD tests for exposed capabilities | D-5, D-6 |
| **PR-X2 — equality/EBV/promotion lattice** | D5+D7 datatype-aware comparison (`sameTerm`, IN via `=`), D-EBV, D4 float/double∘decimal (incl. tP-03/04/05/21), D11 CONCAT, D12 **SPARQL-scoped only** (STRLANG + result normalization; the ingest lower-casing option is struck); `not-not` + `plain-string-same` (folded from wave1 PR-3/4); aggregates: agg-err-01 poison-on-non-numeric, agg-count-rows-distinct new IR aggregate, **agg02 (probe-first — §1.1-13)**, subquery12 (repro-first); **at-risk carve-out:** D5b/open-eq-02 scan-path datatype constraints — own bench sign-off or defer with register entry | `expression-semantics.md` §5 PR-B; `residual-eval.md` §3.3; `sparql12-wave1.md` §1.3/1.5 | **≈−40** (~34 expr PR-B + bnode… bnode01 is in X1; + not-not + plain-string-same + agg02 + agg-err-01 + agg-count-rows-distinct + subquery12; + eq-graph-1/2/4 jointly with PR-G1 — second lander removes) | **Highest engine risk in the slate** (FILTER `=`, EBV, join residuals) | `query_hot_bsbm` **and** `query_hot_bsbm_bi` every commit; §1.2-1 byte-identical goal is the acceptance bar / D5/D7, D-EBV, D4, agg JSON-LD tests per `expression-semantics.md` §4 + `residual-eval.md` §5 | D-12 (strict-mode scoping) |
| **PR-G2 — within-ledger FROM/FROM NAMED (Option A)** | Replace the `view/query.rs:556` guard with within-ledger DataSet construction (single snapshot, existing `DatasetOperator`); harness pre-loads clause-referenced files; must route through `dataset_query.rs` (policy/reasoning), reuse `as_runtime_dataset` | `named-graph-dataset.md` §2 PR-E | **−13** (12 /dataset/ + constructwhere04) | Medium | Standard (BSBM benches never instantiate dataset code — confirm flat) / FQL within-ledger `from`/`fromNamed` test | **D-3; depends on PR-BASE** (relative clause IRIs) |
| **PR-U6 — USING + GRAPH scoping (class F)** | Fix WHERE default-graph selection when USING co-occurs with explicit GRAPH (`stage.rs:1382-1399` region); only class touching shared WHERE lowering | `update-completeness.md` §2 F, PR-U6 | **−2** (delete-using-02a/06a) | Medium (shared prepare-time lowering) | Standard, query benches emphasized / JSON-LD graph-scoped delete-where test | Sequence **after PR-G1** |
| **PR-W15 — Turtle-star ingest (asserting forms)** | Star tokens + parser hooks + sink events → **`to_reifies_facts_jsonld_compatible`** (§1.1-10); **fresh reifier per anonymous occurrence** (mirror `_:fluree_ann_N`, never dedup by EdgeKey) as an explicit requirement with an equivalence test for the repeated-anon case; reject `<<( )>>` and TriG-star with clear deferred errors; **re-run the eval suite after landing to re-baseline the 39** | `sparql12-wave1.md` §2 PR-5 (corrected) | **−2** (pattern-3, pattern-3-nomatch) | Medium (import-hot lexer) | `insert_formats`+`import_bulk` primary; byte-identical non-star corpus assertion / Turtle↔JSON-LD flake-equivalence tests next to `edgekey_roundtrip_*` | D-8 informs the TriG half |
| **PR-W2BC — triple-term syntax accept-then-defer (Option 4)** | Parse TRIPLE/SUBJECT/PREDICATE/OBJECT/isTRIPLE as builtins (arity-validated, lowering `not_implemented`); accept `<<( )>>` in subject/object/VALUES/BIND honoring the §2 negative-suite guardrails | `sparql12-wave2-triple-terms.md` §7 PR-B/C | **−27** | Low (parse-time) | Standard / record category-2 parse-only classification per compliance §Query Surface Parity | **D-1** |

### Decision-gated tail (not started until the decision lands)

| Item | Scope | Shrink if favorable | Gated on |
|---|---|---|---|
| **PR-X3 — D6 lexical-form preservation** | Lexeme column vs documented divergence; also unblocks `group01` (SPARQL12_GROUPING) and completes parts of RDF11 | −8 expr + −1 group01 | D-11 |
| **PR-W16 — base direction** | Option B encoding (lang-string `en--ltr`) + 4 new functions + both lexers + JSON-LD `@direction` in the same effort; or Option C divergence comments | −10 LANG_BASEDIR | D-7 |
| **PR-ENT — entailment pragma harness variant** | Inject `# PRAGMA reasoning: owl2rl` per named test; start rdfs03/04/09; per-test checks for rdfs05-07/10/11, parent\* | −3 firm, more candidates | D-9 |
| **Triple-term first-class-value epic (Option 1)** | New arena-handle object kind across the 8 closed enums; bucket-B eval; CONSTRUCT annotation projection; result serialization; TriG-star; JSON-LD value surface | up to −39 (EVAL_TRIPLE_TERMS) + `update-3` | D-1 (scoped after PR-W15 re-baseline) |
| **Empty-named-graph model** | Enumerable empty graphs vs permanent divergence | −2 (bindings#graph, agg-empty-group-count-graph) | D-6 |

### Unowned work-streams needing a ninth cluster doc or divergence sign-off

- **W-1 Algebra/FILTER-scope + nested-OPTIONAL cluster (9 entries):**
  algebra/filter-nested-2, join-combo-2, join-scope-1, nested-opt-1/2,
  optional/dawg-optional-complex-2/3/4, optional-filter-005. Verified real
  (filter-nested-2 reproduces as a scope bug). Its fix territory is
  `where_plan.rs` filter partitioning — **the same machinery PR-X1's D1
  touches**; land PR-X1 first and give this cluster a single owner before it
  starts.
- **W-2 Output/serialization cluster (5 entries):** construct-3/4
  (reification output), quotes-3/4 (string-escape serialization), and
  **constructlist** (per-solution bnode instantiation in CONSTRUCT templates —
  the capability gap exposed by §1.1-1; per-solution output path, bench-aware
  but not scan-hot).

### Adjusted shrink accounting

Cross-check firm total 411, adjusted for verdicts: −1 (constructlist out of
PR-1) +3 (sort-3/6/8 into PR-H1) +2 (base-prefix-2/5 into PR-BASE) +3
(eq-graph-1/2/4 into PR-G1+PR-X2) = **≈418 entries removed by the firm slate**.
Remaining ≈186 = 63 not-applicable + 50 entailment (−3 via PR-ENT) + 39
triple-term eval epic + 10 lang-basedir + 8 D6 + 2 empty-graph + **14 orphans**
(W-1: 9, W-2: 5). With all gates favorable: ≈124 remain (63 NA + 47 entailment
+ 14 orphans pending W-1/W-2). Caveats already priced in: pp34/pp35/exists03
firm only because PR-BASE + PR-G1 explicitly adopt them; wave-2's −27 assumes
D-1 = accept.

---

## 3. Sequencing plan (safest-first) + dependency map

**Wave 0 (now, all parallel):** PR-H1, PR-1, PR-L1, PR-L2, PR-X1, PR-PP.
**Wave 1:** PR-U1, PR-W2A (after PR-1); PR-2, PR-3 (after D-4).
**Wave 2:** PR-BASE, PR-G1 (after D-2), PR-U2 (after D-10 + EOF coordination
with PR-3), PR-W15.
**Wave 3:** PR-G2 (after PR-BASE + D-3), PR-U3 (after D-5/D-6), PR-X2 (after
PR-X1; agg02 probe inside), PR-U6 (after PR-G1), PR-W2BC (after D-1).
**Tail:** PR-X3, PR-W16, PR-ENT, epics, W-1/W-2 per decisions.

Hard dependencies (all others are parallel-safe):

```
PR-1 ──────────────► PR-W2A            (collections; joint register updates)
PR-3 ◄──coordinate──► PR-U2            (single trailing-token/EOF implementation
                                        in the shared parse_query entry)
D-10 ──────────────► PR-U2             (Txn IR: Vec<Txn> vs UpdateRequest —
                                        decide BEFORE the PR; JSON-LD/Cypher/
                                        pre_built_txn consumers ripple)
D-2 ───────────────► PR-G1 ──► PR-U6   (class F sits on the same default-vs-
                                        named semantics; also gates the algebra
                                        cluster's optional-complex-2)
PR-BASE ───────────► PR-G2             (dataset clause IRIs are relative)
PR-BASE + PR-G1 ───► pp34/pp35/exists03 removal (second lander removes)
PR-G1 + PR-X2 ─────► eq-graph-1/2/4 removal (second lander removes)
PR-X1 ─────────────► PR-X2             (D2 unmasks D4's tP tests)
PR-X1 (D1) ────────► W-1 algebra cluster (same where_plan.rs filter machinery)
PR-W15 ────────────► eval-triple-terms re-baseline ──► Option-1 epic scoping
D-1/D-3/D-4/D-5/D-6/D-7/D-9/D-11/D-12 ► their gated PRs (see §2/§4)
```

Cross-cluster file-collision watchlist: `validate/mod.rs` (PR-2, PR-U1, PR-2's
wave-2 negative — merge-order only, no semantic conflict; note PR-U1 *removes*
the DELETE-WHERE-GRAPH rejection while others add checks);
`parse/query/term.rs`/`pattern.rs` (PR-1's P1/P8 before PR-W2A/W2BC);
`fluree-graph-turtle/src/lex/lexer.rs` (PR-L1 vs PR-W15 — land L1 first,
tiny); `where_plan.rs` (PR-X1 D1 vs W-1); `graph.rs` (PR-G1 vs residual A1-A4
— merged by construction here).

---

## 4. Decision list for the team

> **STATUS (2026-07-06): ADOPTED AS RECOMMENDED.** AJ approved proceeding with
> the recommendation column for all of D-1 through D-12. **Standing
> requirement:** any PR that actions one of these decisions must state in its
> PR description that a decision point existed, enumerate the valid options
> that were considered (including the ones not taken), and explain why the
> adopted option was chosen — linking back to this table and the owning
> cluster doc. Silent adoption of a decision inside an implementation PR is
> not acceptable; reviewers should be able to re-litigate the choice from the
> PR description alone.

| # | Decision | Options | Audits' recommendation (verified) | Register entries gated |
|---|---|---|---|---|
| **D-1** | Triple-term syntax: accept-then-defer vs documented divergence | Option 4 accept (parse, lower=`not_implemented`) / Option 3 reject-on-principle / (Option 2 desugar is dominated — drop) | **Option 4** (`sparql12-wave2-triple-terms.md` §4); book Option 1 as a separate epic scored by EVAL_TRIPLE_TERMS | 27 (W2BC) now; 39 eval via the epic |
| **D-2** | `GRAPH ?g` W3C-by-default: drop the #1279 implicit default-graph enumeration | Drop implicit + keep explicit alias addressing / opt-in flag / keep as-is | **Drop implicit, keep explicit, no toggle** (`named-graph-dataset.md` §3); 2-reviewer sign-off; **3** pinning tests updated (§1.1-3) | 14 (PR-G1) + downstream subquery04 |
| **D-3** | Within-ledger datasets | Option A engine / Option B harness-ledger-per-graph | **Option A** (`named-graph-dataset.md` §2) — product value, reuse, single-snapshot perf; verified mechanically sound | 13 (PR-G2) |
| **D-4** | Reject-more parser behavior changes (V1–V6) shipping as hard errors | Hard error / warning-under-flag / defer | **Hard error + changelog** (`parser-syntax-validation.md` §5), with the added requirement that errors **prevent AST production** (API diagnostic-swallowing hole) | 37 (PR-2 + PR-3) |
| **D-5** | Remote (non-SILENT) LOAD | Documented divergence / opt-in fetch hook | **Documented divergence** — zero W3C cost, no HTTP client in transact (`update-completeness.md` §2 A) | LOAD subset of PR-U3 |
| **D-6** | DROP≡CLEAR observability + empty-named-graph model (one decision, three consumers) | DROP≡CLEAR now + empty graphs as divergence / build registry-remove + enumerable empty graphs | **DROP≡CLEAR now** (harness-indistinguishable); empty-graph question must be decided **once** — `update-completeness.md` recommends permanent divergence, `named-graph-dataset.md` open-Q2 wants a product call; today's DROP≡CLEAR design bakes in the divergence answer | PR-U3 shape; bindings#graph + agg-empty-group-count-graph (−2 only if enumerable) |
| **D-7** | Base-direction representation | A first-class field (storage bump) / B encode in lang string / C defer+divergence | **B if in product scope, else C** (`sparql12-wave1.md` §5); B requires JSON-LD `@direction` in the same PR | 10 (LANG_BASEDIR) |
| **D-8** | TriG parser ownership | New `fluree-graph-turtle` GRAPH-block parsing / route harness qt:data through the existing `parse_trig_phase1` builder path | **Try the builder-path routing first** (verified capability exists, `trig_meta.rs:1199`); new parser work only for star-inside-TriG | 5 eval-triple-terms (inside the 39) |
| **D-9** | Entailment owl2rl PRAGMA harness injection | Named-subset pragma variant / leave all 50 registered | **Yes, named subset** starting rdfs03/04/09 (verified green end-to-end with the pragma, `residual-eval.md` §6); RIF/OWL-DL never winnable | −3 firm + candidates of the 50 |
| **D-10** | Multi-op guard fast-track + Txn IR model | (a) fast-track a loud trailing-token error before PR-U2; (b) sequential `Vec<Txn>` in one commit vs N commits vs new `UpdateRequest` IR | **(a) yes — this is silent data loss in production** (§5 issue 1); (b) one atomic commit (recommended by `update-completeness.md`); resolve the IR shape **before** PR-U2 — it ripples to JSON-LD/Cypher lowering and `pre_built_txn` | PR-U2's 9 + unblocks 8 (with U3) |
| **D-11** | D6 lexical-form preservation vs documented value-matching divergence | Lexeme column (ingest+storage change) / divergence register | Split decision, do not block PR-X1/X2 (`expression-semantics.md` §5); note **group01 and parts of RDF11 hang on the same call** — one decision, two docs currently assume different outcomes | 8 expr + 1 group01 |
| **D-12** | SPARQL strict-type-error mode vs unified strictness across surfaces | Per-surface flag / one strict path | Needed for D2b/D5/D11 (`expression-semantics.md` open-Q2); a flag needs an explicit carve-out against compliance §Query Surface Parity **first** (the guideline's JSON-LD tests would otherwise assert behavior the flag disables) | Scoping of PR-X1 (D2b arm) + PR-X2 |

---

## 4b. Wave 0 — implementation status (2026-07-06, updated post-implementation)

All six Wave-0 PRs are implemented, review-accepted, and sitting on local
branches stacked on this coverage branch (not pushed until PR #1437 merges):

| Branch | Scope | Net register delta | Notes |
|---|---|---|---|
| `burndown/pr-h1` | harness `.rdf`/SRX self-closing-element fix (+`rdf:nodeID`), register-comment batch, parent-audit corrections | −3 | mechanism verified as the §1.1-6 corrected direction |
| `burndown/pr-1` | P1/P4/P5a/P5b/P6/P7/P8 + codepoint pre-pass, collections guarded out of quoted-triple contexts | −32 (33 removed, `test_65` added for PR-2 V6) | see deltas below |
| `burndown/pr-l1` | bnode-dot lexer (greedy-scan + trailing-dot rewind) | −5 (json-res 4 + `owlds02`) | ABAB n=100 benches within budget |
| `burndown/pr-l2` | canonical `xsd:double` across all six RDF-lexical sites | −1 | `ryu` dep dropped; changelog note in description |
| `burndown/pr-x1` | D1/D2/D2b/D3/D9/D10/#1319, per-defect commits each suite-green | −37 (39 removed, `tP-29/30` added) | D-12 transparency section in description |
| `burndown/pr-pp` | pp16 zero-length universe (full terms incl. literals), pp36 batch guard, new `query_hot_property_path` bench + budget | −2 | slower closure numbers = spec-mandated larger output on previously-wrong shapes |

Combined: ≈−80 register entries once merged (integration re-baseline at merge
time is authoritative). Merge order: PR #1437 → PR-H1 (owns the comment batch)
→ siblings in any order (trivial register-file textual conflicts against H1).

**Wave 1 — implementation status (2026-07-07):** all four PRs implemented,
review-accepted, on local branches (merge after the Wave-0 set):

| Branch | Scope | Net register delta | Notes |
|---|---|---|---|
| `burndown/pr-2` | V3-V6 validation passes + 1.2 checks (9 commits, walkers written from scratch) | −28 | V5 forced into the parser (group-simplification AST ambiguity); D-4 fallout: `GROUP BY (expr)`+reprojection (the #1362 shape = W3C agg08) now rejected — alias the key; SPARQL grouped-list extension (= group06) removed from SPARQL, kept on JSON-LD with parity pins |
| `burndown/pr-3` | V1 dot structure, V2 FILTER constraint, **API seam fix** (error diagnostics authoritative even when recovery yields an AST), EOF/trailing-token assertion (D-10a, #1438 guard, verified end-to-end) | −15 | unmasked and fixed 5 "green-by-recovery" tests; systemic caveat: every future reject-more PR will unmask more |
| `burndown/pr-u1` | GRAPH in DELETE WHERE via Modify lowering; bnode-in-DELETE rejection (validator+lowering+JSON-LD surface, `_:fdb-` exempt) | −19 | JSON-LD delete-template rejection is a surface behavior change — in the migration note |
| `burndown/pr-w2a` | RDF 1.2 reifier forms → `Pattern::EdgeAnnotation`; deferred positions lower to clean `not_implemented` (D-1) | −61 | STACKED ON `burndown/pr-1`; includes the 3 SPARQL12_VERSION entries |

Combined Waves 0+1 ≈ −200 register entries (integration re-baseline at merge
time is authoritative; expect ~340 remaining). Cross-PR coordination for
mergers: `syntax-order-07` (pr-1 ∩ pr-3, byte-identical hunk) and
`syntax-update-anonreifier-02` (pr-2 ∩ pr-3, two independent defenses) are
double-claimed — whichever lands second drops its stale removal hunk (the
both-way CI gate enforces this mechanically). Full merge order:
PR #1437 → pr-h1 → {pr-1, pr-l1, pr-l2, pr-x1, pr-pp, pr-2, pr-3, pr-u1} in
any order → pr-w2a (needs pr-1).

**Wave 2 — implementation status (2026-07-07):** all four PRs implemented and
review-accepted; the ninth cluster audit (`algebra-serialization.md`) delivered:

| Branch | Scope | Net register delta | Notes |
|---|---|---|---|
| `burndown/pr-base` | RFC 3986 resolver extracted to `fluree_vocab::iri`; all constant-IRI BASE resolution at lowering time; IRI()/URI() constant fold; `resolve_dataset_clause()` ready for PR-G2; harness supplies the query-document BASE | −6 | pp34 + exists03 greened on this branch ALONE (audit had predicted joint); variable-argument IRI() deliberately not base-resolved (documented, no W3C test) |
| `burndown/pr-g1` | D-2 semantics change: `?g` as IRI, no implicit default-graph enumeration; seed-when-produced + join-at-merge (the cluster doc's unconditional seeding REGRESSED graph-variable-scope — deviation documented); EncodedSid arms + `GraphVarCorrelated` EXISTS fallback (Semijoin keys never match across encodings) | −14 | three pinning tests updated; #1317 not claimed; eq-graph-1/2/4 + pp35 second-lander comments |
| `burndown/pr-u2` | multi-op UPDATE: `UpdateRequest` with per-op prologue snapshots; `Vec<Txn>` sequential staging via virtual commit, last-wins fold, ONE atomic commit (D-10b); real cross-op bnode validation + validator-owned annotation check replaced the guard-carried greens | −9 | STACKED ON pr-3; insert-05a + 3 same-bnode re-pointed at PR-U3 (verb-gated); benches noise-level incl. transact_commit |
| `burndown/pr-w15` | Turtle-star asserting forms → `to_reifies_facts_jsonld_compatible` in FlakeSink + ImportSink; fresh reifier per anonymous occurrence; base-triple assertion recorded as deliberate divergence (Option-1 epic owns spec-exact semantics) | −2 | eval-triple-terms re-baselined: 15 wave-2 syntax / 10 D-1 data / 5 serialization / 5 TriG / 2 harness expected-graph / 2 misc; benches within budget (6-round interleave) |

**Ninth-audit corrections (binding):** W-1's fix territory is parser/lowerer +
`subquery.rs` + `optional.rs`, NOT `where_plan.rs` — the land-PR-X1-first gate
is void; quotes-3/4 are the D5b datatype-drop (move to PR-X2), not
serialization; all three optional-complex tests are GRAPH-gated (PR-G1 to
re-verify post-merge); construct-3/4 + constructlist fall to one per-solution
bnode-instantiation fix. New slate items: **PR-W1** (FILTER-scope + subquery
correlation, low-med), **PR-W2** (CONSTRUCT bnode instantiation, SPARQL-only),
**PR-W1-OPT** (OPTIONAL independent-scope semantics — hot-path risky,
deferrable, double-bench-gated).

Running ledger: Waves 0+1 ≈ −200, Wave 2 ≈ −31 → **~307 registered entries
remain pre-integration** (of which 63 not-applicable + ~47 entailment).
Remaining big movers: PR-U3 (~91), PR-X2 (~40), PR-W2BC (27), PR-G2 (13),
PR-W1/W2 (~5). Merge order addendum: pr-u2 lands after pr-3; pr-w2a after
pr-1; BASE↔G1 joint entries resolve at whichever lands second.

**Post-implementation corrections to this roadmap:**

- **`basic#list-1..4` do NOT green via PR-1** (contra §2's PR-1 row): Turtle
  *ingest* stores object-position collections as Fluree `list_index` items and
  drops `()` objects entirely (`parse_collection_as_list`,
  fluree-graph-turtle), so the `rdf:first/rest` triples never exist in
  storage. This is a new, owned-by-no-one ingest/model gap — **decision D-13**:
  materialize first/rest at ingest vs translate at query time vs documented
  divergence. Register comments updated in PR-1.
- **`subquery12` is resolved by PR-1** (bare-`{SELECT}` misparse executing
  through error recovery) — remove it from PR-X2's scope; its probe item is
  closed, and it further evidences the diagnostic-swallowing hole PR-3 must
  fix.
- **PR-W2A's expected shrink adjusts 63 → 61**: PR-1's P8 greened
  `annotation-(anon)reifier-07` ×2.
- **PR-X2's scope gains `tP-29/30`**: PR-X1's D2 fix unmasked the D4
  promotion defect underneath (expected dynamics; entries registered with
  that comment).
- **Entailment enforced-green set is now 21** (`owlds02` was a bnode-dot
  data-load casualty, not an entailment gap); `sparqldl-03` re-pointed to
  result-mismatch.
- **PR-X1's D1 mechanism** differed from the cluster doc: the fix landed in
  `reorder_patterns` front-hoisting plus `Batch::new` zero-column length
  inference (plan-time; var-full paths byte-identical).
- **Bench-corpus gap**: the `insert_formats`/`import_bulk` corpora contain no
  `_:` tokens, so lexer changes to blank-node handling are invisible to the
  gates — add bnode-heavy content when the bench backlog is next touched.

---

## 5. Production bugs to file as GitHub issues NOW

Independent of W3C work; all confirmed end-to-end through public surfaces.

1. **SPARQL UPDATE: multi-operation (`;`) requests silently execute only the
   first operation.** `parse_sparql` returns after one operation with no
   trailing-token check (`fluree-db-sparql/src/parse/query/mod.rs:40-74,
   193-207`); `parse_and_lower_sparql_update` lowers the single body. A request
   like `INSERT …; DELETE …` commits the INSERT and silently discards
   everything after the first `;` — verified through
   `graph().transact().sparql_update().commit()` with readback. This is silent
   data loss for any client sending standard multi-op SPARQL UPDATE. Interim
   mitigation (D-10a): reject requests with trailing tokens loudly; full fix is
   request-level sequential staging (PR-U2).
2. **Variable-free FILTER eliminates every solution.** `FILTER(true)`,
   `FILTER(1=1)`, `FILTER(2 IN (1,2,3))` return zero rows through the public
   query API; the same filter with any variable reference works. Root cause:
   `required_vars = referenced_vars()` is empty, so the filter is "eligible"
   before any triple binds and is inlined against the seed row
   (`fluree-db-query/src/execute/where_plan.rs:697, :990`). Silent wrong
   results; planner-only fix (evaluate constant filters once per stream).
3. **DATATYPE()/LANG() reject any non-variable argument.**
   `eval/rdf.rs:58-62` bails unless the argument is a bare `Expression::Var`;
   `LANG` (`eval/string.rs:115-134`) silently returns `""` for expression
   arguments. `FILTER(datatype(?a + ?b) = xsd:integer)` and
   `datatype("foo"@en)` fail; 29 W3C tests and any user composing expressions
   inside these builtins are affected. Fix: evaluate the argument to a value,
   read its datatype/lang, type-error on non-literals (SPARQL-scoped per
   D-12 — the `@id` extension is deliberate on the JSON-LD surface).
4. **UPDATE `USING` + explicit `GRAPH` over-deletes from the default graph.**
   `DELETE {…} USING <g3> WHERE { GRAPH <g2> {…} }` deletes rows it must not
   (dawg-delete-using-02a: expected 5 triples remain, got 3). Single-operation,
   verified; the explicit GRAPH block is polluted by USING scoping
   (`fluree-db-transact/src/stage.rs:1382-1399` region). Data loss in a
   supported UPDATE form.
5. **`GRAPH ?g` binds the graph name as an `xsd:string` literal and enumerates
   the default graph.** `graph.rs:314-323` builds `Binding::Lit{String}` where
   an IRI term is required; `graph.rs:686-711` appends the ledger alias to the
   unbound-`?g` fan-out (deliberate #1279 extension, W3C-breaking). File as the
   tracking issue for the D-2 behavior change; note three tests pin the current
   behavior (incl. `it_upsert_duplicate_ids_repro.rs:228`).
6. **Bound `GRAPH ?g` silently returns nothing when `?g` is a late-materialized
   `EncodedSid`.** `extract_graph_iri_from_binding` (`graph.rs:157-172`) has no
   `EncodedSid`/`EncodedLit` arm; manifests inside `FILTER EXISTS { GRAPH ?g
   {…} }` where the ref-valued row is dropped while string-valued rows survive
   (verified probe). Top-level shapes work today, so the bug hides in
   correlated/EXISTS contexts.
7. **Turtle import fails on `_:label.` (blank-node label followed by the
   statement dot).** `parse_blank_node_name`
   (`fluree-graph-turtle/src/lex/lexer.rs:477-490`) greedily consumes the dot
   then hard-errors ("unexpected character '_'"), rejecting valid Turtle that
   every other store ingests. Fix design must use trailing-dot rewind so
   `_:a..b` (valid, currently accepted) keeps lexing (§1.1-9).
8. **`xsd:double` serialization is non-canonical and internally inconsistent.**
   SPARQL-JSON/XML render Rust `Display` (`"1000000"`,
   `"1000000000000000000000000000000"`), native CSV/TSV renders ryu
   (`"1000000.0"`), N-Triples/N-Quads export renders `{:E}` (`"1E6"` — missing
   the mandatory mantissa dot). Canonical is `"1.0E6"`. One helper, all sites
   (PR-L2); changelog note for downstream string-matchers.
9. **`fluree-db-sparql` fails to compile with `--no-default-features`.**
   `ast/term.rs:91` and `parse/query/term.rs:1174` reference `fluree_vocab`
   unconditionally, but it is optional behind the `lowering` feature. Any
   parse-only (Lambda/WASM) consumer is broken today.
10. **Numeric aggregates silently skip non-numeric group members.** `AVG`/`SUM`
    over a group containing a bnode/IRI/string return a value computed over the
    numeric members (`aggregate.rs:737-755`, `binding_to_numeric` → `None`);
    SPARQL requires a type error → unbound. Silent wrong aggregates on mixed
    data.

Also: **comment on #1319** with the verified mechanism (pattern objects carry
no datatype constraint; `dt_compatible` is asymmetric and not consulted on this
path — do not "fix" by extending it), and **comment on #1317** that it is a
distinct defect class from the GRAPH ?g enumeration (registry g_id
assignment/indexed-read routing under multi-level indexes; suspect
`context.rs:981-985` silent `unwrap_or(binary_g_id)` fallback) and must not be
closed against PR-G1.

---

## 6. Register hygiene

### 6.1 Register-comment corrections + reassignments (apply as a batch in PR-H1; each owning PR re-verifies on landing)

| Entries | Correction / movement |
|---|---|
| SPARQL11_UPDATE: insert-05a, insert-data-same-bnode, insert-where-same-bnode(2) | Comment "INSERT into not-yet-existing named graph silently loses triples" is **wrong** → class B multi-op `;` truncation (only first op runs). Owner: PR-U2 (+U3) |
| SPARQL11_UPDATE: dawg-delete-insert-01c | "Combined DELETE/INSERT WHERE applies inserts without deletes" is **wrong** (single combined op passes) → class B multi-op. Owner: PR-U2 |
| SPARQL11_SYNTAX_UPDATE_1: test_36 | Not graph-management grammar → class D (validator rejects GRAPH in DELETE WHERE). Owner: PR-U1 |
| SPARQL11_SYNTAX_UPDATE_1: test_38/39/40 | Not graph-management grammar → class C (empty/prologue-only request). Owner: PR-U2 |
| SPARQL11_SYNTAX_UPDATE_1: test_54 | → class B + cross-op bnode-scope validation. Owner: PR-U2 |
| SPARQL12_VERSION: version-01/02/05 | "VERSION declaration support" is **wrong** — VERSION already lexes/parses; failures are bare `<< >>` patterns. Owner: PR-W2A (−3 in that PR's math) |
| SPARQL11_PROPERTY_PATH: pp34, pp35 | Not path-cardinality → graph-cluster tests (constant-GRAPH-IRI base-expansion vs exact-key registry miss + `?g`-as-literal). Owner: PR-BASE + PR-G1 |
| SPARQL11_PROPERTY_PATH: pp16 | Comment: zero-length closure **node-universe completeness** (non-path-predicate + literal nodes), not multiplicity. Owner: PR-PP |
| SPARQL11_PROPERTY_PATH: pp36 | Comment: empty-schema `Batch::new` len-collapse **inside PropertyPathOperator**, not projection. Owner: PR-PP |
| SPARQL11_EXISTS: exists03 | → graph cluster (base resolution + EXISTS active-graph inheritance). Owner: PR-BASE + PR-G1 (conditional) |
| SPARQL11_SUBQUERY: subquery02 / subquery04 | → graph cluster (A4 correlation / A3 default-graph leak). Owner: PR-G1 |
| SPARQL11_SUBQUERY: subquery12 | → expression cluster (CONSTRUCT ↔ sub-SELECT alias visibility; repro-first). Owner: PR-X2 |
| SPARQL11_AGGREGATES: agg02, agg-err-01, agg-count-rows-distinct | → expression/aggregate cluster; agg02 comment must note the fix site is **unconfirmed** (not the group_aggregate finalize). Owner: PR-X2 |
| SPARQL11_AGGREGATES: agg-empty-group-count-graph | → graph + empty-named-graph decision (D-6); expected to remain registered after PR-G1 |
| SPARQL11_CONSTRUCT: constructlist | "Query execution error" is **wrong** → parse-time rejection today; after PR-1 it becomes the CONSTRUCT-template bnode-instantiation gap. Owner: **W-2**, stays registered through PR-1 |
| SPARQL11_CONSTRUCT: constructwhere04 | Comment correct (FROM on single-ledger). Owner: PR-G2 |
| SPARQL10_QUERY_EVAL: basic#list-1..4 | → parser cluster P1 (collection triple silently dropped today because the API swallows parse diagnostics). Owner: PR-1 |
| SPARQL10_QUERY_EVAL: basic#base-prefix-1/2/5 | → PR-BASE (relative-IRI/BASE resolution at lower time); base-prefix-1's lexer half in PR-1 |
| SPARQL10_QUERY_EVAL: sort#dawg-sort-3/6/8 | → harness `.rdf` DAWG parser (`Event::Empty` state-stack skew — engine output verified correct). Owner: PR-H1 |
| SPARQL10_QUERY_EVAL: algebra/filter-nested-2, join-combo-2, join-scope-1, nested-opt-1/2, optional-complex-2/3/4, optional-filter-005 | → **W-1 algebra cluster** (no doc yet; complex-2 also gated on PR-G1) |
| SPARQL10_QUERY_EVAL: construct-3/4, quotes-3/4 | → **W-2 serialization cluster** (no doc yet) |
| SPARQL10_QUERY_EVAL: expr-equals#eq-graph-1/2/4 | → joint PR-G1 (GRAPH-var) + PR-X2 (D5); second lander removes |
| SPARQL11_CSV_TSV: csv03 | Comment correct. Owner: PR-L2 |
| SPARQL12_RDF11: langstring-datatype, plain-string-datatype | Same D2 constant-argument defect → PR-X1 removes both clusters' entries in one fix |
| SPARQL12_EVAL_TRIPLE_TERMS: graphs-1/2, expr-1, update-1/2 | Additionally blocked on TriG GRAPH parsing (orthogonal to star; D-8); update-3 blocked on harness expected-graph TriG-star parsing (not data ingest); only pattern-3/pattern-3-nomatch green on wave-1 ingest — re-point the other 39 at wave-2 query syntax / D3 values / functions / serialization / CONSTRUCT / TriG |
| SPARQL11_BINDINGS: graph | Stays after PR-G1; gated on D-6 (empty-graph enumerability) |

**Standing rule:** the 31 syntax-update-1 tests are registered twice
(`SPARQL11_SYNTAX_UPDATE_1` + inside `SPARQL11_UPDATE`) — every UPDATE fix PR
deletes **both** register lines per test or the both-directions check fails.

### 6.2 Harness fixes discovered (land in PR-H1 unless noted)

- `.rdf` DAWG result parser: handle `Event::Empty` for self-closing
  `rs:value rdf:resource/nodeID` elements in `parse_rdf_dawg_result_set`
  (`result_format.rs:809`, `:835`); greens sort-3/6/8.
- Audit the same `Start|Empty` push-without-pop pattern in the SRX parser
  (`result_format.rs:413`) before trusting more `.srx`-based categories.
- Query-base injection: the harness prepends `@base` to **data** TriG
  (`query_handler.rs:532-538`) but passes query text with no base — PR-BASE
  needs the query's base URI plumbed (harness- or engine-side; decide in
  PR-BASE design).
- Dataset tests (PR-G2): pre-load `FROM`/`FROM NAMED`-referenced files as named
  graphs from the parsed dataset clause.
- TriG qt:data routing through the `parse_trig_phase1` builder path (D-8).
- Entailment pragma-injection harness variant (D-9, PR-ENT).
- Eval-triple-terms expected-result handling: TriG-star expected graphs +
  `"type":"triple"` result values (Option-1 epic scope).

### 6.3 Parent-audit corrections (annotate `2026-07-sparql-testsuite-audit.md`)

§4.2's "INSERT into a not-yet-existing named graph silently loses the triples"
and "Combined DELETE/INSERT WHERE applies inserts without the deletes" findings
are refuted (both are class-B multi-op truncation); §4.2 item 7 still lists
pp06 as failing (it passes; already deregistered); the §1 headline split is
63 NA / 541 gaps, not 67/538. Also amend
`docs/contributing/sparql-compliance.md` § "Where to add parity tests" with an
UPDATE/transact row (`it_transact_update.rs`, `it_named_graphs.rs`) — PR-U1.

---

## 7. Definition of done (every PR)

1. **Register shrink, both directions:** remove exactly the entries the PR
   greens (both copies for duplicated update tests; cross-register removals
   coordinated where §2 notes them — PR-1↔W2A, PR-G1↔PR-X2, PR-BASE↔PR-G1).
   CI's unexpected-pass/stale-entry policing is the enforcement.
2. **JSON-LD parity per compliance §Query Surface Parity:** classify the fix
   (IR/engine vs surface-syntax vs SPARQL-only), author the named JSON-LD
   regression tests in the same PR (`it_query.rs` / `it_query_analytical.rs` /
   `it_query_grouping.rs` / `it_transact_update.rs` / `it_named_graphs.rs`),
   or record the surface-only decision in the PR description. Cypher excluded.
3. **Bench gates, no waivers:** `query_hot_bsbm`, `query_hot_bsbm_bi`,
   `insert_formats`, `import_bulk` within `regression-budget.json` on every PR;
   plus the PR-specific gates in §2 (`query_hot_property_path` for PR-PP;
   import benches primary for PR-L1/PR-W15; both query benches every commit for
   PR-X2). Nightly bench workflow is the backstop.
4. **Behavior changes carry a changelog/migration note:** PR-2/PR-3
   (reject-more), PR-G1 (GRAPH ?g semantics), PR-L2 (double serialization),
   PR-X2 (equality/EBV strictness), PR-U2 (trailing-token errors), PR-U3
   (new verbs + DROP≡CLEAR divergence note), documented-divergence register
   comments where a decision resolves to divergence (D-5, D-6, D-7-C, D-11).
5. **Verification hygiene:** full W3C suite + `cargo test -p fluree-db-sparql`
   + the JSON-LD groups (`grp_query`, `grp_query_sparql`, `grp_transact`,
   `grp_graphsource` as applicable); PRs touching pinned extensions update the
   pinning tests named in §2; PRs with investigation items (agg02, subquery12,
   D5b) include the probe result in the PR description before the fix commit.
6. **Decision transparency (AJ, 2026-07-06):** if the PR actions any D-1…D-12
   decision, the PR description must present the decision point, all options
   considered, and the rationale for the adopted option (link §4). See the §4
   status note.
