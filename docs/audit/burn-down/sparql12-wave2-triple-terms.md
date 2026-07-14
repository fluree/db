# SPARQL 1.2 Wave 2 — Triple Terms Burn-down & D3 Decision Memo

**Cluster:** `SPARQL12_SYNTAX_TRIPLE_TERMS_POSITIVE` (87 failing) +
`SPARQL12_SYNTAX_TRIPLE_TERMS_NEGATIVE` (1 failing).
**Scope:** these two registers only. The sibling `SPARQL12_EVAL_TRIPLE_TERMS`
(41) is a *different* cluster and is referenced only where it bears on the D3
decision.
**Status:** pre-implementation audit. No source changed.
**Baseline:** branch `test/sparql-testsuite-full-coverage`, rdf-tests submodule
`efccbc6b8`. Verified against parser HEAD.

---

## 0. The single fact that reframes this whole cluster

Both registers are **syntax** suites. The harness worker
(`testsuite-sparql/src/bin/run_w3c_test.rs:83-116`) runs **only**
`parse_sparql()` + `validate(ast, Capabilities::default())`. **It never lowers
to IR and never touches storage or the query engine.** A positive test passes
iff parse+validate produce no error; a negative test passes iff they do.

Consequence: **every one of the 87 positive failures is greenable with
parser + validation work alone.** None of them requires a `FlakeValue`
variant, an index encoding, a comparator arm, or an evaluator. The heavyweight
"first-class triple term" machinery that D3 is nominally about is required only
by the *eval* cluster, which is out of scope here. This is confirmed
empirically: `basic-anonreifier-01` (`<< :a :b :c >> :p1 :o1 .`, a quoted-triple
subject with a non-`f:t` predicate) **passes today** even though that shape
hits `LowerError::not_implemented` at lower time — because syntax tests never
lower.

That fact turns D3, for *this* cluster, from "implement first-class triple
terms vs not" into a much cheaper question: **do we extend the parser to
accept SPARQL 1.2 triple-term syntax (and defer/register its evaluation), or do
we register the syntax itself as a documented divergence?** The full
first-class-value implementation is a real decision, but it belongs to the eval
cluster and is analyzed in §5/§6 so the team can price it.

---

## 1. Bucketed analysis of the 87 positive failures

Every failure is a parser rejection. I read all 113 positive `.rq`/`.ru`
files and bucketed by the exact syntactic construct the parser chokes on, and
by whether the construct is expressible in Fluree's existing edge-annotation
(reifier) model or genuinely needs a triple-term *value*.

| Bucket | What it needs | Count | Model fit |
|---|---|---|---|
| **A/D — reifier-model syntax** | `<< s p o >>` / `~ reifier` / `{\| \|}` forms in positions the parser doesn't yet accept | **60** | Expressible via existing reifier/edge-annotation model |
| **B — triple-term functions** | `TRIPLE` / `SUBJECT` / `PREDICATE` / `OBJECT` / `isTRIPLE` parse as builtins | **3** | Parse-only here; eval semantics need a decision |
| **C — triple terms as values** | `<<( s p o )>>` in subject / bare-object / `VALUES` / `BIND` | **24** | Genuinely needs a triple-term *value* (or syntax-accept-then-defer) |

Total 87 = 60 + 3 + 24.

### 1.1 Bucket A/D — reifier / annotation syntax (60)

These use only the `<<>>` reifier form, the `~` reifier operator, and the
`{| |}` annotation block — the exact constructs Fluree already committed to.
They fail because the parser accepts these constructs in a *narrower* set of
positions than SPARQL 1.2 requires. Sub-gaps, with the parser site that
rejects each (`fluree-db-sparql/src/parse/query/term.rs` unless noted):

| Sub-gap | Example test(s) | Why it fails today | Count |
|---|---|---|---|
| **Reifier `<<>>` in object position** | `basic-anonreifier-02/04`, `annotation-*reifier-03/04` | `parse_object` (~199-231) has no `TripleStart` branch — `<<>>` accepted in *subject* only (`parse_subject` ~72-75 → `SubjectTerm::QuotedTriple`) | ~8 |
| **`~ reifier` *inside* `<<>>`** | all `basic-reifier-*` (12), `bnode-reifier-*` (3) | `parse_quoted_triple` (~100-122) parses `s p o` then demands `>>`; a `~` before `>>` is unexpected | ~15 |
| **Reifier `<<>>` standalone (no predicate-object) + nested** | `basic-anonreifier-08/09/10/11/12/13`, `nested-anonreifier-02`, `nested-reifier-*` | parser requires a predicate-object list after a subject; a lone `<< s p o >> .` and a `<<>>`-inside-`<<>>` are unhandled. NB: standalone reifier **is** valid (asserts+reifies) — contrast bucket C where standalone triple-term is *invalid* (§2) | ~9 |
| **Multiple reifiers / multiple annotation blocks per triple** | `annotation-*-multiple-*` (12) | `parse_annotation_tail` (~784-829) hard-narrows to v1 "at most one reifier" (~798-801) and "at most one annotation block" (~809-813) | 12 |
| **Richer annotation-block content** (paths, nested bnode lists) | `annotation-*reifier-06/07`, `update-reifier-07` (`:q1+`), `update-*reifier-04` (`<<>>` in block) | `parse_annotation_block` (~857-924) accepts a plain predicate-object list; property paths (`:r/:q`, `:q1+`) and nested reifiers inside the block are rejected | ~8 |
| **Reifier inside collections `()` / bnode lists `[]`** | `inside-anonreifier-01/02`, `inside-reifier-01/02` | `<<>>` as an element of `( … )` or `[ … ]` object flows through `parse_object`, same object-position gap | 4 |
| **Update templates/DATA carrying the above** | `update-anonreifier-04/05`, `update-reifier-01/03/04/05` | same gaps via the CONSTRUCT/template path (`parse_construct_predicate_object_list`) | ~4 |

**All 60 are parser-grammar extensions of a model Fluree already owns.** For
the syntax suite they need parser acceptance only. For *eval* parity (separate
cluster) most map cleanly: a reifier in object position binds a plain
reifier node (an IRI/bnode) — **no new object kind** — because a reifier is a
resource, not a value.

**One design snag to flag (not blocking):** `<<>>` is overloaded. Fluree's
*legacy* `<< s p ?o >> f:t ?t` history-metadata form is lowered specially in
`lower/rdf_star.rs` (`f:t`/`f:op` → `Function::T`/`Op`); RDF 1.2 uses the same
`<<>>` tokens for a *reifier*. Extending object-position `<<>>` must reconcile
these two readings in lowering (parse is fine — the tokens are identical). This
is an eval-time concern; it does not affect greening the syntax suite.

### 1.2 Bucket B — triple-term functions (3)

`expr-tripleterm-03` (`TRIPLE(?s,?p,?o)`), `-04` (`TRIPLE(?s,?p,str(?o))`),
`-05` (`isTriple`/`SUBJECT`/`PREDICATE`/`OBJECT`). Confirmed **absent from all
four function registries**:

1. lexer keyword table — `fluree-db-sparql/src/lex/token.rs` (`TokenKind` +
   `keyword_from_str` ~733 + `keyword_str` ~550)
2. AST `FunctionName` — `fluree-db-sparql/src/ast/expr.rs:266` + `parse` ~348
3. token→`FunctionName` dispatch — `fluree-db-sparql/src/parse/expr.rs:501`
   (`check_builtin_function_keyword`)
4. IR `Function` — `fluree-db-query/src/ir/expression.rs:715`

Unqualified builtin names must be recognized or the parser tries to read
`TRIPLE` as a bad prefixed name and errors. **For this syntax cluster, only
registries 1–3 (parse-time) are needed** — parse `TRIPLE(...)`/`isTriple(...)`
as builtin calls and validate arity. The IR variant + evaluator (registry 4)
is eval work, deferable.

*Can these be implemented against the reifier model without first-class terms?*
Partially, and only with divergent semantics: if `?t` binds a **reifier node**
(not a triple value), `SUBJECT(?t)` could read the reifier's
`f:reifiesSubject` flake, `isTRIPLE(?t)` could test "is a reifier resource."
That answers a different question than the spec (which operates on triple-term
*values*), so it would green parse but diverge on eval-conformance. Recommend:
parse now, tie eval semantics to the D3 value decision.

### 1.3 Bucket C — triple terms as values (24)

`<<( s p o )>>` used as a *value*: subject, bare object (predicate ≠
`rdf:reifies`), `VALUES`, or `BIND`. The parser rejects `<<(` in every position
except object-of-`rdf:reifies`, with explicit deferred-feature errors:

- bare object — `parse_object_list` term.rs ~636-642: *"triple terms
  (`<<( s p o )>>`) are only allowed as the object of rdf:reifies in v1;
  arbitrary triple-term values are deferred"*
- subject / nested — term.rs ~81-85, ~708-720: *"nested triple terms are not
  supported in v1"*
- template/DATA — term.rs ~1064-1071; inside annotation block — ~873-879
- `BIND` — `parse/expr.rs` `parse_primary_expr` has no `TripleTermStart` branch
  (and `parse/stream.rs::is_term_start` whitelists `TripleStart` but not
  `TripleTermStart`)
- `VALUES` — `parse/query/pattern.rs::parse_values_term` accepts only UNDEF /
  IRI / literal

The 24: `basic-tripleterm-01..07`, `bnode-tripleterm-01..03`,
`compound-all`, `compound-tripleterm`, `compound-tripleterm-subject`,
`inside-tripleterm-01/02`, `nested-tripleterm-01/02`, `subject-tripleterm`,
`expr-tripleterm-01` & `-06` (BIND), `update-tripleterm-01/03/04/05`.
`basic-tripleterm-05` is the `VALUES` case.

This is the bucket D3 is really about. Two honest routes (§4).

---

## 2. The grammar constraints a parser extension must respect (guardrail)

The negative suite (64/65 passing) already encodes the RDF 1.2 rules. Any
parser extension that accepts triple terms as values **must keep rejecting**
these, or it regresses passing negatives:

- **Triple term subject may not be a triple term** — `tripleterm-subject-01/02/03`
  (`<<( <<( … )>> :q :z )>>`) are NEGATIVE. Only the *object* position of a
  triple term may nest.
- **Triple term subject may not be a literal** — `tripleterm-subject-04/05/06`.
- **A bare triple term may not stand alone** — `tripleterm-separate-01..06`
  (`<<( ?s ?p ?o )>> .`) are NEGATIVE. Critically this **differs from the
  reifier** form: `basic-anonreifier-08/09` (`<< ?s ?p ?o >> .` standalone) are
  POSITIVE. A reifier asserts+reifies; a triple term is only a value and cannot
  be a statement.
- **No paths / no collections inside a triple term** —
  `alternate-path-tripleterm`, `quoted-path-tripleterm`,
  `quoted-list-subject-tripleterm`, `list-tripleterm-01`.
- **Reifiers are not expressions; triple terms are** — `bind-reified` /
  `bind-anonreified` (`BIND(<< … >> AS ?t)`) are NEGATIVE, while
  `expr-tripleterm-01` (`BIND(<<( … )>> AS ?t)`) is POSITIVE. The parser must
  accept `<<(` in `BIND` but reject `<<` there.
- **No blank nodes in a triple term used in an expression** —
  `bindbnode-tripleterm` (`BIND(<<( [] ?p ?o )>> AS ?t)`) is NEGATIVE.

These are precisely the fiddly cases that make bucket C real grammar work
rather than a token flip. The negatives are the acceptance test for getting it
right.

### The one negative failure

`syntax-update-anonreifier-02` (`mf:NegativeUpdateSyntaxTest`):
```
DELETE DATA { :s :p :o1 {| :added 'Test' |} } ; INSERT DATA { :s :p :o2 {| :added 'Test' |} }
```
Should be rejected (annotation blocks minting anonymous reifiers are not
permitted in `DELETE DATA`/`INSERT DATA` ground quad-data); Fluree accepts it.
Fix is a small validation pass, independent of everything above. Contrast
`syntax-update-anonreifier-01` (a `DELETE … WHERE` with annotations), correctly
rejected today.

---

## 3. JSON-LD surface parity

Per `docs/contributing/sparql-compliance.md` § Query Surface Parity, scoped to
the surfaces Fluree owns. **Cypher is explicitly out of scope for this
burn-down** — it is openCypher, Fluree does not own the grammar and will not add
custom triple-term/reifier syntax to it, so there is no Cypher parity obligation
here. JSON-LD query syntax is Fluree-owned and is the only cross-surface
concern below.

- **Bucket A/D reifier syntax** is a **SPARQL-only surface feature** (category
  3, like property-path text and RDF-star syntax). The *underlying* capability
  (edge annotations) already exists in the shared IR and is already reachable
  from JSON-LD via `@annotation`
  (`fluree-db-transact/parse/edge_annotations.rs`). No JSON-LD **syntax** parity
  is owed for the `<<>>`/`~`/`{| |}` surface; record the decision and move on.
  Where a bucket-A/D fix touches *IR/eval* (object-position reifier as a bound
  node), add a JSON-LD regression test that the same annotation is reachable —
  but no new JSON-LD syntax.
- **Bucket B functions**: if `SUBJECT/PREDICATE/OBJECT/isTRIPLE/TRIPLE` become
  real evaluable functions (not just parsed), that is a **surface-syntax
  addition** — they must be exposed in JSON-LD query function syntax in the same
  effort, with tests.
- **Bucket C first-class values**: JSON-LD has **no** native "triple term as a
  value" today. If Option 1 (§4) lands, JSON-LD query needs a way to *bind and
  return* a triple-term value (a reserved object shape) and the JSON-LD result
  formatter needs to emit it; that is net-new JSON-LD surface work bundled into
  the first-class-value epic, not into wave-2 syntax. `@annotation` is the
  transact-side analogue of reifiers, **not** of triple-term values — do not
  conflate them.

---

## 4. D3 decision memo — bucket C (and the B eval tail)

The centerpiece. Three options, priced for a team decision. Test counts are
**this cluster's syntax register** unless a row says "eval."

### Option 1 — Full first-class triple terms (implement values end-to-end)

Add a triple term as a real RDF term/value everywhere.

- **Greens (syntax):** all 24 bucket-C + makes bucket-B meaningful → 27.
- **Greens (eval, separate cluster):** unblocks most of
  `SPARQL12_EVAL_TRIPLE_TERMS` (up to ~41), incl. the value-identity tests
  `results-tripleterms-1x/1j`, `expr-*`, `pattern-*`. This is the *only* option
  that greens those.
- **Cost:** a new object **kind** ripples through eight closed enums and their
  `Ord`/`Hash`/`Display`/encode/decode arms — `fluree-graph-ir::Term`
  (`term.rs:227`), `FlakeValue` (`fluree-db-core/src/value.rs:117` + discriminant
  202 + Ord 805 + Hash/canonical-hash), `ir::triple::Term`
  (`fluree-db-query/src/ir/triple.rs:117`), `Binding`
  (`fluree-db-query/src/binding.rs:45`), `OType` (`o_type.rs:30` + `decode_kind_*`
  + `DecodeKind`/`from_u8`), `ObjKind` + `ObjKey` + `ValueTypeTag`
  (`value_id.rs:41/184/561`), flake size (`flake.rs:376,421`) — **plus a new
  content-addressed arena** (see §5: `ObjKey` is a fixed 64-bit inline payload,
  too small to hold a composite triple, so a triple term must be an arena handle
  like `RDF_JSON`/`VECTOR`), plus index-selection logic, result formatters, and
  the JSON-LD surface (§3). This is a multi-crate epic, not a wave-2 PR.
- **Perf risk:** **medium, bounded — if done as an arena handle.** The on-disk
  comparators (`run_record.rs:233-281`) compare the raw `o_kind` byte then
  `o_key`, so they gain **no per-kind branch**; a new kind just needs a
  discriminant slot placed to sort correctly. The real hot-path exposure is
  enum width: `FlakeValue` and `Binding` are matched per-row in scan/filter/join
  and copied into rows — a triple term must ride as a **single Sid/u64 arena
  handle**, never an inline `[Sid;3]+dt`, or it widens the row and regresses
  `query_hot_bsbm`/`_bi`. The in-memory `FlakeValue::cmp`/`type_discriminant`
  and `graph_ir::Term::cmp` *do* switch on the closed enum and gain an arm
  (cold relative to the byte comparators). Bench guardrails per audit §6.

### Option 2 — Desugar bare triple terms to the reifier model

Parse `<<( )>>` and rewrite to the existing reifier/edge machinery.

- **Greens (syntax):** all 24 — parsing + a lowering that rewrites to reifier
  nodes. (For the syntax suite you don't even need the rewrite; see Option 4.)
- **Greens (eval):** **wrong answers, not greens.** A triple term is a *value*
  (the triple itself); a reifier is a *resource* standing for an occurrence.
  Desugaring `?t = <<( s p o )>>` to a reifier bnode makes `?t` bind a node, so
  `isTRIPLE(?t)`, `SUBJECT(?t)`, term-equality, and `ORDER BY` over `?t` all
  diverge from spec. Eval tests keyed on triple-term identity
  (`results-tripleterms-1x/1j`, `expr-2`, `pattern-*`) **fail** under desugaring.
- **Verdict:** viable as a *syntax-only* convenience, useless as an eval
  strategy. It buys nothing Option 4 doesn't buy more cheaply, and it risks
  masquerading as eval support. **Not recommended.**

### Option 3 — Documented-divergence register (reject syntax, keep skips)

Leave the 24 (and the 3 functions) rejected with the current clear
"deferred/only-as-rdf:reifies-object" errors; keep them registered as a
deliberate divergence consistent with "Fluree uses an edge-annotation model,
not first-class triple terms."

- **Greens:** 0 of bucket B/C. Bucket A/D still gets fixed separately.
- **Cost:** ~zero (write the register rationale + team sign-off).
- **Perf risk:** zero.
- **Verdict:** honest and defensible, but it permanently stops Fluree from
  **parsing** standard SPARQL 1.2 and leaves 27 register entries. Choose it only
  if the team is committing to *never* offering triple-term values.

### Option 4 — Syntax-accept-then-defer (recommended for wave 2)

Extend the parser to **accept** all of bucket B and C (respecting the §2
grammar guardrails), building AST nodes; let **lowering** reject the
not-yet-evaluable value forms with a clean `not_implemented`. Because the
harness never lowers syntax tests, this **greens all 27** with **parser +
validation work only — zero storage/index/eval/perf risk**.

- **Greens (syntax):** 24 (C) + 3 (B) = 27, plus the 60 in A/D → the entire
  87-entry positive register.
- **Greens (eval):** 0 — `SPARQL12_EVAL_TRIPLE_TERMS` stays registered,
  explicitly tied to the Option-1 decision.
- **Cost:** bounded parser grammar work + arity validation. The negatives (§2)
  are the guardrail against over-acceptance.
- **Perf risk:** zero (parse-time only).
- **Tension to name:** the parser then accepts `BIND(<<( )>> …)` etc. that error
  at query time. That is standard "syntax supported, evaluation pending" and is
  exactly what a syntax-conformance suite rewards. If the team would rather fail
  fast at parse time for product clarity, that is Option 3 for bucket B/C —
  a genuine either/or the team must pick.

### Recommendation

**Wave 2 = Option 4 for the syntax cluster** (green all 87 positive + 1
negative with parser/validation only), and **book Option 1 as a separately
scheduled first-class-value epic** owned by the engine team, gated on and
measured by `SPARQL12_EVAL_TRIPLE_TERMS`, where the perf-critical object-kind
work and its bench guardrails live. The one thing the team must actually decide
now is the **either/or for bucket B/C**: *accept-syntactically* (Option 4,
recommended — parses standard SPARQL 1.2, defers eval) **vs**
*documented-divergence* (Option 3 — rejects the syntax on principle). Bucket
A/D proceeds either way. Option 2 is dominated; drop it.

| Option | Syntax greens (this cluster) | Eval greens (other cluster) | Storage/perf risk | One-line |
|---|---|---|---|---|
| 1 Full values | 27 | up to ~41 | medium (arena handle; bench-guard) | The only real triple-term support; an epic |
| 2 Desugar | 24 | wrong answers | low | Dominated — masquerades as eval |
| 3 Register divergence | 0 | 0 | none | Honest "we don't do triple terms" |
| **4 Accept-then-defer** | **27** | 0 | **none** | **Recommended wave-2: parse now, eval later** |

---

## 5. Hot-path / index-encoding risk (for Option 1 only)

Zero relevance to greening this syntax cluster; priced here so the team can
schedule the eval epic. Verified against the core:

- **Object value is a closed enum with a discriminant-ordered sort.**
  `FlakeValue` (22 variants, `value.rs:117`) with `type_discriminant`
  (`value.rs:202`) driving `Ord` (`value.rs:805`). A new arm is required and
  touches every `match FlakeValue` — many are per-row hot (scan/filter/join).
- **Two type-tag encodings must stay in lockstep:** `OType` u16
  (`o_type.rs:30`, with reserved ranges `0x0020–0x3FFF` / `0x800D–0xBFFF`
  explicitly for new kinds) and `ObjKind` u8 + `ObjKey` u64
  (`value_id.rs:41/184`). **`ObjKey` is 64-bit inline** — a composite
  `(s,p,o,dt)` triple cannot fit, so a triple term must be a **new
  content-addressed arena** (mirror `RDF_JSON`/`VECTOR`/`NUM_BIG`), i.e. the
  object column stores an arena handle, not the triple.
- **Comparators favor us.** On-disk `cmp_*` (`run_record.rs:233-281`) and the
  overlay merge (`types.rs:110-141`) compare the raw `o_kind` byte then `o_key`
  — **no per-kind switch to extend**; cross-kind order is set by the numeric
  discriminant you assign. So the *comparator functions* need no new arm; only
  discriminant *placement* matters. In-memory `cmp_object`
  (`comparator.rs:123`) rides `FlakeValue::cmp`, which does gain the arm above.
- **The genuine hot-path exposure is enum width**, not branch count: keep the
  triple term a single `Sid`/`u64` arena handle inside `FlakeValue`/`Binding` so
  the row width and cache footprint of the scan/join path don't grow. Guard with
  `query_hot_bsbm` / `query_hot_bsbm_bi`.
- **`Opst` (object-leading index) is documented refs-only**
  (`comparator.rs:30,79-83`). Looking up a triple-term object by value needs
  index-selection work too.

**Net:** comparator functions are largely safe (raw-byte compare); the cost and
risk are in the **enum/encoder/decoder/hash/Display arms + a new arena + enum
width discipline + formatters + JSON-LD surface**. Bounded and known, but a
multi-crate epic — correctly kept out of wave 2.

---

## 6. Blast radius summary

| Change | Touches | In wave-2 syntax scope? |
|---|---|---|
| Bucket A/D parser (object-pos `<<>>`, `~`-in-`<<>>`, standalone/nested reifier, multi-reifier/-annotation, richer blocks) | `fluree-db-sparql` parse + AST + (lower to existing `EdgeAnnotation`/`AnnotationTarget` or clean `not_implemented`) | **Yes** — parse-time, zero engine risk |
| Bucket B function parsing | `fluree-db-sparql` lexer keyword + AST `FunctionName` + parse dispatch | **Yes** — parse-time |
| Bucket C triple-term-value *syntax* (Option 4) | `fluree-db-sparql` parse + AST, guarded by §2 negatives; lower = `not_implemented` | **Yes** — parse-time, zero storage risk |
| Negative `syntax-update-anonreifier-02` | `fluree-db-sparql` validation pass (annotations in `*_DATA`) | **Yes** — validate-time |
| Bucket C triple-term *values* end-to-end (Option 1) | `fluree-graph-ir`, `fluree-db-core` (value/o_type/value_id/flake/comparator), `fluree-db-binary-index` (run_record + new arena), `fluree-db-query` (ir/binding/eval/index-select), formatters, JSON-LD surface | **No** — separate epic, gated on eval cluster |
| Bucket B function *eval* semantics | IR `Function` + lowering + evaluator (+ JSON-LD parity) | **No** — tied to Option 1 decision |
| Legacy-`<<>>` vs RDF-1.2-reifier reconciliation | `fluree-db-sparql/src/lower/rdf_star.rs` | **No** — eval-time only |

---

## 7. PR composition (wave 2)

Ordered; each shrinks its register in the same change (CI enforces both
directions). All are parse/validate-time — no bench guardrails required (none
touch the engine).

1. **PR-A — reifier-form parser extensions.** Green the 60 bucket-A/D positives.
   Extend `parse_object`/quoted-triple for object-position and nested `<<>>`,
   `~`-inside-`<<>>`, standalone reifier; relax the "at most one
   reifier/annotation" narrowing; allow paths + nested reifiers in annotation
   blocks. Lower to existing `EdgeAnnotation`/`AnnotationTarget` where the model
   already evaluates; otherwise clean `not_implemented`. Remove 60 entries from
   `SPARQL12_SYNTAX_TRIPLE_TERMS_POSITIVE`. **Ships regardless of the D3
   either/or.**
2. **PR-B — triple-term function parsing.** Add `TRIPLE`/`SUBJECT`/`PREDICATE`/
   `OBJECT`/`isTRIPLE` to lexer keyword table + AST `FunctionName` + parse
   dispatch + arity validation; lowering `not_implemented`. Removes the 3
   bucket-B entries. **Gated on the D3 either/or** (only if "accept").
3. **PR-C — triple-term-value syntax acceptance (Option 4).** Accept `<<( )>>`
   in subject / object / `VALUES` / `BIND`, strictly honoring the §2 grammar
   (subject not a triple term/literal, no standalone, no paths/collections, no
   `<<` in expressions, no bnodes in expression triple terms). Lowering
   `not_implemented`. Removes the 24 bucket-C entries. **Gated on the D3
   either/or.** *If the team picks Option 3 instead, skip PR-B/PR-C and instead
   rewrite the 24+3 register comments as a reviewed documented-divergence
   rationale.*
4. **PR-D — negative validation.** Reject annotation blocks in
   `INSERT DATA`/`DELETE DATA`. Removes the sole
   `SPARQL12_SYNTAX_TRIPLE_TERMS_NEGATIVE` entry.

**Separate epic (not wave 2):** first-class triple-term values (Option 1) +
bucket-B eval semantics + CONSTRUCT annotation projection
(`lower/construct.rs:92` `not_implemented`, which bites only the eval cluster's
`construct-1..5`, none in this syntax cluster), gated on and scored by
`SPARQL12_EVAL_TRIPLE_TERMS`, with the §5 bench guardrails.

**Register math:** PR-A −60, PR-B −3, PR-C −24 → positive register 87 → 0 (if
"accept"); PR-D → negative register 1 → 0. Under Option 3: PR-A −60 → 27 remain,
re-annotated as reviewed divergence; PR-D → 0.
