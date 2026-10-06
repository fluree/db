# Burn-down: SPARQL expression & value semantics

**Cluster owner deliverable — pre-implementation deep audit. No source was
modified.** Parent audit: `docs/audit/2026-07-sparql-testsuite-audit.md`
(§4.2.2, §6 — the perf section is binding here). Register:
`testsuite-sparql/tests/registers/mod.rs` (`SPARQL10_QUERY_EVAL`,
`SPARQL11_FUNCTIONS`, `SPARQL11_CSV_TSV`). Baseline rdf-tests submodule
`efccbc6b8`. Spec references are to *SPARQL 1.1 Query Language*
(https://www.w3.org/TR/sparql11-query/) and, for numeric promotion, *XPath 2.0
/ XQuery F&O* (op:numeric-*).

**Every root cause below was reproduced against the live engine** by driving the
`run-w3c-test` subprocess binary on individual tests (and on ~10 throwaway
probe queries), so the "actual" behaviour quoted is real engine output, not
inference. Where a defect only surfaces once an upstream defect is fixed, that
is stated.

## 0. Scope and headline

This is the largest, most perf-sensitive cluster (~110 register entries across
17 categories). It decomposes into **13 engine defects plus GH #1319**, and
**~28 register entries that are not expression-semantics bugs at all** (harness
result-parsing, the algebra/OPTIONAL-scope cluster, CONSTRUCT/serialization,
the GRAPH-var-as-literal issue) and should be reassigned — see §6.

The two highest-leverage findings, both cheap and both surprising:

1. **`DATATYPE()`/`LANG()` reject any non-variable argument** (they `bail` unless
   `args[0]` is a bare `Expression::Var`). Every one of the 22 `type-promotion`
   tests and all 7 `cast` tests is a `FILTER(datatype(<expr>) = xsd:T)` — so
   **one 3-line guard fails 29 tests** (D2).
2. **A variable-free (constant) top-level `FILTER` eliminates every solution.**
   `FILTER(true)`, `FILTER(1<2)`, `FILTER(2 IN (1,2,3))`, `FILTER(2 NOT IN ())`
   all return zero rows; the moment the filter references one variable it works.
   This — not `IN` semantics — is why `in01`/`notin01`/`dawg-boolean-literal`
   fail (D1).

| ID | Defect | Tests | Locus | Time class |
|---|---|---|---|---|
| **D1** | Variable-free `FILTER` drops all rows | in01, notin01, dawg-boolean-literal (3) | `execute/where_plan.rs` filter placement | prepare-time |
| **D2** | `DATATYPE`/`LANG` reject expression args | 22 type-promotion + 7 cast (29) | `eval/rdf.rs:58`, `eval/string.rs:115` | per-row (cheap) |
| **D2b** | `DATATYPE`/`LANG` of non-literal → value, not type error | dawg-datatype-2, dawg-lang-1/2/3 (4) | `eval/rdf.rs:90`, `eval/string.rs:133` | per-row |
| **D3** | `xsd:dateTime()`/`date()`/`time()` cast unsupported | cast-dT (1) | `lower/expression.rs:307`, `eval/dispatch.rs:221` | parse+per-row |
| **D4** | Numeric promotion → wrong result datatype (float→double; double∘decimal→decimal) | tP-03/04/05/21, all expr-ops (~11) | `eval/value.rs:78-252`, `eval/cast.rs` | per-row |
| **D5** | `=`/`!=`/`<`/`>` drop datatype; unknown/incompatible datatypes compare by string; `!=` returns true not error | eq-4, eq-2-1/2-2, eq-dateTime, open-eq-04/05/06/07/08/10/11/12, date-1 (~14) | `eval.rs:147`+`eval/value.rs:593`, `eval/compare.rs:341-415` | per-row (hot) |
| **D6** | Original lexical form not preserved (values canonicalized on ingest) | open-eq-01, dawg-str-1/2, distinct-1/9, sameTerm-simple/eq/not-eq (8) | storage/ingest (`fluree-db-core`, turtle/json-ld) | ingest-time (deep) |
| **D7** | `sameTerm` / `IN` / `NOT IN` use structural (datatype-dropped) equality | sameTerm-* (w/ D6); IN/NOT IN latent | `eval/rdf.rs:146`, `eval/logical.rs:96,130` | per-row |
| **D-EBV** | EBV of a bare variable too permissive (only bool-false is falsy) | dawg-bev-1..6 (6) | `eval.rs:62` → `binding.rs:799-825`; `eval/value.rs:300` | per-row (hot) |
| **D8** | `IRI()`/`URI()` don't resolve relative IRIs against base | iri01 (1) | `eval/rdf.rs:237-263` | prepare-time (base) |
| **D9** | `BNODE(str)` identity global, not per-solution | bnode01 (1) | `eval/rdf.rs:278-296` | per-row |
| **D10** | `REGEX` `"q"` (literal) flag unsupported | regex-no-metacharacters(-ci) (2) | `eval/helpers.rs:109-129` | prepare-time (compile) |
| **D11** | `CONCAT` coerces numeric arg to string, no type error | concat02 (1) | `eval/string.rs:300`, `eval/value.rs:405` | per-row |
| **D12** | Lang-tag not lowercased; STRLANG on non-string not erroring; lang-literal output omits `rdf:langString` | strlang03-rdf11, dawg-langMatches-1/2/3/4/basic (6) | `eval/string.rs:619`, result serialization / `result_comparison.rs` | per-row + harness |
| **#1319** | VALUES/inline bare integer tagged `xsd:long`, storage uses `xsd:integer` | latent; term-identity contexts | `parse/lower.rs:528`, `lower/term.rs:455` | lowering-time |

---

## 1. Per-defect root cause, evidence, and spec

### D1 — a variable-free top-level FILTER eliminates all solutions
`ASK {}` → **true** (correct, one empty solution). `ASK { FILTER(1 = 1) }` →
**false**. `ASK { ?s ?p ?o . FILTER(1 = 1) }` (with data) → **false**. Probe
matrix (real output):

| filter | references a var? | result |
|---|---|---|
| `FILTER(?s = ?s)` | yes | pass (true) |
| `FILTER(?s = ?s && 1 = 1)` | yes | pass (true) |
| `FILTER(isIRI(?s) && 2 IN (1,2,3))` | yes | pass (true) |
| `FILTER(true)` / `FILTER(1 < 2)` / `FILTER(1 = 1)` | no | **fail (false)** |

`notin01` = `ASK { FILTER(2 NOT IN ()) }` is unconditionally true (empty list);
its returning false is *only* explicable by the FILTER over the (single, empty)
group solution yielding no passing row — i.e. the variable-free filter is
mis-placed/mis-applied so it removes every row instead of passing them. A
`FilterPattern`'s `required_vars` is `expr.referenced_vars()` (`where_plan.rs:697`);
for a constant expression that set is empty, so `required_vars.is_subset(bound)`
in `partition_eligible_filters` (`where_plan.rs:990`) is trivially true — the
filter is marked "ready" *before any triple is bound* and inlined against the
seed row, which drops the block. The correct behaviour: evaluate a constant
filter once and keep/drop the whole stream.
Tests: `in01`, `notin01` (`SPARQL11_FUNCTIONS`); `dawg-boolean-literal`
(`SPARQL10_QUERY_EVAL/boolean-effective-value`, got 0 vs 1). Spec §18.6 (FILTER).

### D2 — DATATYPE()/LANG() reject non-variable arguments (29 tests)
`eval/rdf.rs:58-62`:
```rust
let Expression::Var(var_id) = &args[0] else {
    return Err(QueryError::InvalidExpression("DATATYPE requires a variable argument"...));
};
```
`eval/string.rs:115-134` (`LANG`) is the same shape — only a `Binding` fetched
from a bare `Var` is inspected; the catch-all returns `""`. Every
`type-promotion` query is `ASK { … FILTER(datatype(?l + ?r) = xsd:T) }` and every
`cast` query is `SELECT ?s { … FILTER(datatype(xsd:T(?v)) = xsd:T) }` — the
argument is an *arithmetic expression* / *cast call*, not a variable, so
`DATATYPE` errors, the FILTER demotes to false (`error.rs:114` `can_demote_in_expression`),
and the ASK/SELECT returns false/empty. Verified: `tP-short-short` → `Expected
true, Actual false`; `cast-int` → `Expected 1, got 0`.
Tests: `type-promotion-01..22` (22) + `cast-{str,flt,dbl,dec,int,dT,bool}` (7).
Spec §17.4.2.1 (`LANG`), §17.4.2.3 (`DATATYPE`) — the argument is any expression
evaluating to an RDF term.

### D2b — DATATYPE()/LANG() of an IRI/blank node returns a value, not a type error
`eval/rdf.rs:90-92`: `DATATYPE` of a `Sid`/`Iri` returns the Fluree
`id_type` extension. `eval/string.rs:133`: `LANG` catch-all returns `""`.
`dawg-datatype-2` (`FILTER(datatype(?v) != <NotADataTypeIRI>)`, data-builtin-2
= 5 literals + 1 IRI + 1 bnode) → **Expected 5, Actual 7**: the IRI (x6) and
blank node (x7) wrongly pass because `datatype()` gives them a value instead of
raising a type error (which would exclude them). `dawg-lang-1` is identical (7
vs 5). Spec: `DATATYPE`/`LANG` require a literal argument; a non-literal is a
type error. Note the `id_type` result is a deliberate JSON-LD-surface extension
— the fix must be SPARQL-scoped, not a blanket removal (see §5 open questions).
Tests: `dawg-datatype-2`, `dawg-lang-1`, `dawg-lang-2`, `dawg-lang-3`.

### D3 — xsd:dateTime()/date()/time() casts unsupported
`lower/expression.rs:307-322` maps only `BOOLEAN/INTEGER/FLOAT/DOUBLE/DECIMAL/
STRING`; `xsd:dateTime` falls to `Function::Custom(iri)` → `eval/dispatch.rs:221`
"Unknown function" error. `cast-dT` = `FILTER(datatype(xsd:dateTime(?v)) =
xsd:dateTime)` → **Expected 1, got 0** (blocked by both D3 and D2). Spec §17.5
(XSD constructor functions). Cheap: one lowering arm + one `eval/cast.rs` arm
reusing the existing dateTime parser.

### D4 — numeric promotion produces the wrong result datatype
Two sub-defects, both proven by `expr-ops` (which SELECTs the typed result) and
by `type-promotion` once D2 is fixed:

- **D4a — `xsd:float` has no value representation; float arithmetic yields
  `xsd:double`.** `xsd:float` casts are stored as a `TypedLiteral{String}`
  (`eval/cast.rs:129-193`) and `coerce_numeric_operand` (`eval/value.rs:340-366`)
  turns any float operand into `ComparableValue::Double`; `ArithmeticOp::apply`
  (`eval/value.rs:78-252`) has no float arm, so results re-tag as `xsd:double`.
  `unplus-2` (data-numbers): expected `+("3"^^xsd:float)` = `"3"^^xsd:float`;
  **actual `"3"^^xsd:double`**.
- **D4b — `Double ∘ Decimal → Decimal`** (`eval/value.rs:234-248`) instead of
  `Double`. XPath makes `xsd:double` the widest type. `add-numbers-cast`:
  expected `decimal + double = double`; **actual `= decimal`**.

Tests (post-D2): `type-promotion-03` (double-decimal, D4b),
`-04`/`-05`/`-21` (float, D4a); `expr-ops/{add,subtract,multiply,divide}-numbers-cast`,
`unplus-2`, `unminus-2`, `add-literals`. Spec: XPath op:numeric-* type
promotion; SPARQL §17.4 operator mapping.

### D5 — value comparison drops the datatype; incompatible datatypes compare by string
`eval_to_comparable` for a `Binding::Lit { val, .. }` (`eval.rs:147`) converts via
`TryFrom<&FlakeValue>` (`eval/value.rs:593-624`), which maps
`FlakeValue::String → ComparableValue::String` and **never receives the binding's
`dtc`** (datatype/lang). So `"zzz"^^:myType`, `"zzz"^^xsd:string`, `"zzz"@en` and
plain `"zzz"` all collapse to `String("zzz")`, and `CompareOp`/`cmp_values`
(`eval/compare.rs:309-415`) compares them as equal.

- `eq-4` (`FILTER(?v = "zzz")`) → **Expected 1 (xp1 plain), Actual 2** —
  `"zzz"^^:myType` (unknown datatype) wrongly equals `"zzz"`. Spec: comparing
  literals of different/unknown datatypes is a **type error**, not `true`.
- `open-eq-07` (`FILTER(?v1 = ?v2)`, data-2) → **Expected 12, Actual 38** — all
  `xyz` variants (plain / xsd:string / xsd:integer / :unknown / @en / @EN)
  compare equal. Spec: plain = xsd:string (value-equal), `@en`/`@EN` equal
  (case-insensitive), a term equals itself reflexively even if ill-typed, but
  cross-type incomparable pairs are type errors.
- Also: for the ordering operators `cmp_values` returns `None` (incomparable) →
  `<`/`>` raise `TypeMismatch` (demoted to false), but `!=` returns **true** for
  incomparable operands (`eval/compare.rs:341-355`); per spec an incompatible
  `!=` is a type error (row excluded), not true.
- `eq-dateTime` and `date-1` show the same shape plus timezone handling
  (`date-1`: `"2006-08-23"`, `"2006-08-23Z"`, `"2006-08-23+00:00"` treated as
  equal — expected only the exact term).

**D5b (scan-level variant):** `open-eq-02` (`{ ?x :p "a"^^t:type1 }`) → **Expected
1, Actual 2** — the pattern object `"a"^^t:type1` also matches `"a"^^t:type2`,
i.e. triple-pattern object matching for unknown-datatype string literals ignores
the datatype (dictionary/scan path, not the filter path). Same root class as D5
but on the match side. Tests: `open-eq-04/05/06/07/08/10/11/12`, `eq-4`,
`eq-2-1`, `eq-2-2`, `eq-dateTime`, `date-1`. Spec §17.4.1.7 (RDFterm-equal),
§17.3 (operator mapping / type errors).

### D6 — original lexical form is not preserved (canonicalization on ingest)
Fluree stores the parsed *value*, not the original lexeme: `"001"^^xsd:integer`,
`"01"`, `"1"` all become `FlakeValue::Long(1)`; `"1.0e0"^^xsd:double`, `"1.0"`,
`"1"` all become the same `Double`, rendered canonically as `"1"`. Consequences,
all verified:

- `open-eq-01` (`{ ?x :p "001"^^xsd:integer }`) → **Expected 0, Actual 2** (z1
  `"1"`, z2 `"01"`): value match instead of simple-entailment term match.
- `dawg-str-1` (`FILTER(str(?v) = "1")`) → **Expected 4, Actual 7**: `STR` of
  `"1.0e0"^^double` and `"1.0"^^double` returns `"1"` (canonical) instead of the
  original lexeme, so they wrongly match.
- `distinct-1`/`distinct-9` (`SELECT DISTINCT ?v`, data-num) → **Expected 9,
  Actual 5**: 9 distinct terms (`"01"`/`"+1"`/`"1"` integer, `"1.0"`/`"01.0"`
  decimal, `"1.3e0"` float vs double, …) collapse to 5.
- `sameTerm-simple`/`-eq`/`-not-eq` → **26 vs 14**: distinct double lexemes are
  `sameTerm`-equal.

This is a **storage/ingest** property, not a query-layer bug: the flake stores
the canonical value and the datatype `Sid` separately (`fluree-db-core/src/value.rs`),
so the lexical form is gone before the query engine ever sees it. Spec: RDF 1.1
§3.3 (literal term equality is *lexical form × datatype × lang*); SPARQL simple
entailment matching is term-based. This is the one defect in the cluster that is
**not** a localized fix (see §4/§5) — likely a mix of an original-lexical-form
column and a documented value-matching divergence for a subset.

### D7 — sameTerm / IN / NOT IN semantics
`sameTerm` (`eval/rdf.rs:146-149`) compares two `ComparableValue`s with the
derived `PartialEq`; because D5 already dropped datatype/lang, it cannot
distinguish `"1"^^xsd:integer` from `"1"^^xsd:int` or (via D6) two double
lexemes. `IN`/`NOT IN` (`eval/logical.rs:96,130`) use the same structural `==`,
**not** the `=` operator — so no numeric promotion (`1 IN (1.0)` → false;
spec-true) and no type errors (`2 IN (3,"cat")` → false; spec-error). The IN
divergence is currently *latent* (`in01`/`notin01` fail on D1, not on IN), but
it is a real spec gap. Spec §17.4.1.8 (sameTerm), §17.4.1.9 (`IN` is defined via
`=`).

### D-EBV — Effective Boolean Value of a bare variable is too permissive
`FILTER(?v)` routes through `Expression::eval_to_bool_uncached`
(`eval.rs:62`): `Ok(row.get(*var).is_some_and(Into::into))` — i.e. the
**binding-level** `From<&Binding> for bool` (`binding.rs:799-825`), where
`Binding::Lit { .. } => true` for every literal *except* `xsd:boolean false`.
So `"0"^^xsd:integer`, `"0.0"^^xsd:double`, `NaN`, and ill-typed literals are all
truthy. `dawg-bev-1` (`FILTER(?v)`, data-1) → **Expected 4, Actual 7**: the
numeric-zero rows (y1, y2) wrongly pass. `dawg-bev-2` (`FILTER(!?v)`) → **4 vs
1** (mirror). `dawg-bev-5`/`-6` add `OPTIONAL` so `?w` is often unbound;
`FILTER(!?w)` should be a type error (unbound), but `Unbound → false` then
`!false → true` includes them (**6 vs 1**). This is *distinct* from the
`ComparableValue` EBV (`eval/value.rs:289-303`), which *does* apply the
numeric-zero rule — the two disagree, and the bare-variable path takes the wrong
one. (The `ComparableValue` EBV has its own milder deviation: `_ => true` at
`value.rs:300` makes unmodeled typed literals / durations truthy instead of a
type error.) Tests `dawg-bev-1..6`. Spec §17.2.2 (EBV).

### D8 — IRI()/URI() do not resolve relative IRIs against the base
`eval/rdf.rs:237-263` builds a `Sid`/`Iri` from the argument string with no base
resolution. `iri01` (`BASE <http://example.org/> SELECT (URI("uri") AS ?uri)
(IRI("iri") AS ?iri)`) → **actual `Iri("uri")`, `Iri("iri")`** vs expected
`http://example.org/uri`, `…/iri`. The base is known at parse/plan time; thread
it into the builtin. Spec §17.4.2.8 (`IRI`).

### D9 — BNODE(str) identity is global, not per-solution
`eval/rdf.rs:278-296` hashes only the label (`_:b{hash(label)}`), so the same
argument yields the same blank node across *all* solutions. `bnode01` expects a
fresh bnode per solution but the *same* bnode for equal args *within* one
solution; actual reuses `b3e8b8c44c3ca73b7` for `"foo"` across different rows.
Fix: fold the solution/row identity into the bnode key. Spec §17.4.2.3 (`BNODE`).

### D10 — REGEX "q" flag unsupported
`build_regex_with_flags` (`eval/helpers.rs:109-129`) errors on any flag outside
`i/m/s/x`; `regex-no-metacharacters` uses `regex(?val, "a?+*.{}()[]c", "q")`
(the XPath `q` flag = pattern is a literal string), so Fluree errors → 0 rows.
Fix: on `q`, escape the pattern (compose with `i`). Spec: SPARQL `REGEX` inherits
XPath `fn:matches` flags. Tests `regex-no-metacharacters`, `-case-insensitive`.

### D11 — CONCAT coerces a numeric argument instead of raising a type error
`concat02` (data2 has `:s7 :str 7`, an `xsd:integer`) → **49 vs 49 rows, wrong
contents**: SPARQL `CONCAT` requires string arguments (a non-string arg → type
error → the projected `?str` is unbound), but `into_string_value`
(`eval/value.rs:405`) coerces `Long → String`, so Fluree produces `"…7"` where
the row should be empty. Same theme as D2b/D11 (builtins coerce rather than
erroring). Spec §17.4.3.x (`CONCAT`).

### D12 — language-tag handling and langString output
- `strlang03-rdf11` (`STRLANG(?o,"en-US")`) → 16 vs 16, mismatch: expected lang
  is **`en-us`** (lower-cased canonical) and non-string args (`n*` numerics,
  `d*` dates) must yield *unbound*; Fluree does not lower-case the tag and/or
  binds a value for non-string args. Spec: BCP47 canonical lower-casing; STRLANG
  requires a string arg.
- `dawg-langMatches-1..4`/`-basic` → same row count, differ only in that the
  output literal is `{lang:"en-gb", datatype:None}` where the fixture is
  `{lang:"en-gb", datatype:rdf:langString}`. The `langMatches` *function* is
  correct; this is a **serialization/comparison** gap — a lang-tagged literal
  must carry `rdf:langString` (RDF 1.1), or `result_comparison.rs` must normalize
  `None ≡ rdf:langString`. Mostly harness-side (low risk, off hot path); listed
  here because the tests are in this cluster's register.

### #1319 — VALUES/inline bare integers are tagged xsd:long
`parse/lower.rs:528` (JSON-FQL inline data) and `lower/term.rs:455` (SPARQL
`VALUES` rows) default a bare integer to `xsd:long`, while storage
(`fluree-db-transact/generate/flakes.rs:471`), the Turtle parser
(`fluree-graph-turtle/parser.rs:678`), arithmetic results
(`eval/value.rs:465`), and SPARQL *triple-pattern* lowering (`lower/term.rs:210`)
all use `xsd:integer`. Verified: `SELECT ?v { VALUES ?v { 3 } }` serializes a
datatype ≠ `xsd:integer`. **However** the scan/join path survives it — a stored
`xsd:integer 3` *does* match a VALUES `xsd:long 3` because
`fluree-db-core/src/datatypes.rs:16` `dt_compatible()` treats the integer family
as compatible in the scan post-filter (probe verified: the VALUES-join returns
the row). So #1319's real blast radius is **(a)** the datatype tag on
VALUES/inline integers in results and **(b)** term-identity contexts
(`DISTINCT`/`GROUP BY`/`sameTerm`) where `dt_compatible` is not consulted — the
same number becomes two distinct group/term keys (`group_aggregate.rs`
`MaterializedLitKey` includes the datatype). No W3C test in this register fails
*solely* on #1319, but it compounds D6/D7 and is a latent correctness bug worth
fixing at the source.

---

## 2. Fix design (honoring §6: prepare-time vs per-row; fast-path preservation)

The governing rule (§6.2): keep the same-type fast paths — integer/integer,
double/double, string/string, iri/iri — **byte-identical**, and route only the
mixed / unknown-datatype / derived cases through new slow paths chosen at
**prepare time** (per-expression, per-predicate), never as a new per-row branch
in the common case.

**Prepare-time / parse-time (inherently safe — do these first):**

- **D1** — filter placement. A `FilterPattern` with empty `required_vars` must be
  recognized at plan build (`where_plan.rs`), evaluated **once** against the unit
  row, and either kept as a no-op or turned into an empty-stream short-circuit —
  not inlined into the first block's per-row path. Pure planner change; zero
  per-row cost.
- **D3** — add `xsd:dateTime/date/time` to the cast lowering table
  (`lower/expression.rs`) and a matching `eval/cast.rs` arm. Parse-time table +
  a cast that only runs when the query uses the constructor.
- **D8** — capture the query base in the plan and pass it to `Function::Iri`
  lowering/eval; relative-IRI resolution is a constant-fold when the argument is a
  literal.
- **D10** — `q`-flag handling is in `build_regex_with_flags`, already behind the
  thread-local compiled-regex cache (`eval/helpers.rs:84`) — compile-time only.
- **D2** — the promotion/derived-type *tables* are static; build a
  `datatype → numeric-class` and a `(class,class) → result-class` table once (a
  `const`/`OnceLock`), consulted only on the slow path.
- **#1319** — fix at the lowering boundary: lower a bare integer to `xsd:integer`
  in `parse/lower.rs:528` and `lower/term.rs:455` (matching storage and RDF 1.1
  §"integer" → xsd:integer). This is the cheapest correct fix and removes a
  term-identity discrepancy for free; the alternative (making comparison treat
  long≡integer everywhere) is broader and duplicates `dt_compatible`.

**Per-row, but structured to preserve the fast path:**

- **D2 (DATATYPE/LANG on expressions) / D2b (type errors on non-literals)** — replace
  the `Expression::Var` guard with "evaluate arg to a `Binding`/value, read its
  datatype/lang, error on non-literal (SPARQL scope only)". The evaluation
  already happens for expression arguments; the added cost is only for the
  previously-erroring path, so no regression on today's passing queries.
- **D4** — give `ComparableValue` a first-class `Float` (or a
  `Numeric{class}` tag on the numeric variants) so float stays float and
  `double∘decimal→double`. Keep the four existing same-type arms untouched; add
  a Float arm and correct the two mixed arms (`value.rs:234-248`). The common
  integer/double fast paths are unchanged.
- **D5 / D7 (equality lattice)** — the real fix is to **stop dropping the
  datatype**: `eval_to_comparable` must carry the `dtc` for string-valued typed
  literals (today `TryFrom<&FlakeValue>` can't, because it never sees the
  binding's `dtc` — `eval.rs:147` must pass it, producing a `TypedLiteral` for
  non-`xsd:string` string literals rather than a bare `String`). Then dispatch the
  comparison at **prepare time** by the expression's static type-pair when known
  (constant vs constant, or a predicate whose object datatype is known from
  stats), falling to a per-row "typed comparison" only for genuinely mixed
  columns. The fast paths (both sides `xsd:string`/plain, both `Long`, both
  `Double`, both `Sid`) stay on the current arms; only the "one side has a
  non-string datatype" case takes the new lattice (numeric-family promote →
  compare; same-unknown-datatype → term compare; else type error). `sameTerm`
  and `IN` then reuse the datatype-aware term-equality and the `=` operator
  respectively.
- **D-EBV** — fix `From<&Binding> for bool` (`binding.rs:799-825`) to apply
  §17.2.2: `Boolean → b`; numeric → `≠ 0 && !NaN` (the value is already inline in
  `Binding::Lit`, and `EncodedLit`'s `o_kind`/`o_key` classify numeric-zero
  without a full dictionary decode); `xsd:string`/plain → non-empty; other
  datatypes / unbound → type error (→ false in FILTER). Keep the `Sid`/`Iri`/
  path/rel truthy arms and the boolean fast path unchanged. This is on the FILTER
  hot path, so the numeric check must stay branch-light (see §3).
- **D9** — extend the `BNODE(arg)` key with the solution's row identity
  (`eval/rdf.rs:278`); per-row but trivial.
- **D11** — `CONCAT` must reject non-string args (type error) rather than calling
  `into_string_value` on them; per-arg check, cheap.
- **D12** — lower-case lang tags at STRLANG/ingest; emit `rdf:langString` on
  lang-literal output (or normalize in `result_comparison.rs`). Off hot path.

**Inherently deep (not a localized fix):**

- **D6** — preserving the original lexical form requires either storing the
  lexeme alongside the value (ingest + storage + serialization change across
  `fluree-db-core`, turtle/json-ld parsers) or accepting value-based matching as
  a **documented divergence** for the affected subset. Recommend: split D6 out of
  the burn-down, decide value-vs-term matching as a design call (mirrors the §5.3
  dataset decision), and register the un-fixed tests with a rationale rather than
  block the cluster on it. D6 alone accounts for `open-eq-01`, `dawg-str-1/2`,
  `distinct-1/9`, and the `sameTerm-*` over-matching.

---

## 3. Hot-path risk and bench gates

Gating benches (per `regression-budget.json`): `query_hot_bsbm` and
`query_hot_bsbm_bi` — budgets **10% tiny / 5% small / 3% medium** — cover the
FILTER/join hot paths this cluster touches. Every PR here must run both; the
nightly bench workflow is the backstop.

| Defect | On hot path? | Risk / mitigation |
|---|---|---|
| D1, D3, D8, D10, #1319 | No (prepare/parse/compile-time) | Zero per-row cost. Not bench-sensitive. |
| D2, D2b, D11 | Cold path only | The changed branch only runs where Fluree previously *errored*; passing queries are unaffected. Negligible. |
| D4 | Warm | Adds a `Float` variant / numeric-class tag. Risk = enum size + one extra match arm. Mitigation: keep `Long`/`Double` arms first and identical; gate on `query_hot_bsbm` (BSBM filters are integer/decimal). |
| **D5/D7** | **Hot (highest risk)** | This is FILTER `=`/`<` and join residuals. Carrying the datatype into `ComparableValue` must **not** allocate on the string/plain and numeric fast paths. Mitigation: (a) keep same-type arms byte-identical; (b) choose the typed-comparison slow path at prepare time per expression, so the per-row code for an all-`xsd:string` or all-numeric column is unchanged; (c) bench `query_hot_bsbm` **and** `query_hot_bsbm_bi` (BI variant exercises typed object filters). If the datatype must be threaded per-row, estimate ~1 extra enum-tag load + branch on the *mixed* path only. |
| **D-EBV** | **Hot** | `From<&Binding> for bool` runs for every `FILTER(?v)` and every truthiness check. Adding numeric-zero/NaN tests to the `Lit` arm is a few integer comparisons on an already-in-hand value — cheap — but the `EncodedLit` arm must classify zero from `o_kind`/`o_key` **without** a dictionary decode (decoding here would be a real regression). Mitigation: decode-free zero test; gate on `query_hot_bsbm`. |
| D9, D12 | Warm/cold | BNODE/STRLANG are not in BSBM hot loops. Low risk. |
| D6 | Ingest + hot | If solved by lexeme preservation: touches import (`insert_formats`, `import_bulk` budgets) and widens the literal representation — material. This is the main reason to defer/scope D6 separately. |

No fix in this cluster is *forced* onto the per-row path except D5/D7 and D-EBV,
and both can keep their common cases identical; there is no correctness fix here
that must accept a hot-path regression.

---

## 4. JSON-LD surface parity

*(Cypher is out of scope for this burn-down: it is openCypher — not Fluree's to
extend — and every fix here is IR/engine-level, so Cypher benefits implicitly
with no assessment required. This section covers the JSON-LD surface only.)*

All 13 defects except D3/D8/D10 are **IR/engine-level** (value model, comparison
lattice, EBV, arithmetic, storage) and therefore fix the JSON-LD query surface
implicitly — it shares `fluree-db-query` evaluation. D3 (xsd:dateTime cast), D8
(IRI base), D10 (regex q-flag) are surface-reachable functions; JSON-LD FQL has
`filter`/comparison/function syntax, so the analytical JSON-LD surface must get
the same capability in the same effort.

Per `sparql-compliance.md` "Query Surface Parity", each fix is done only when the
register entry is removed **and** a JSON-LD regression test exists for the
JSON-LD-expressible behaviour. JSON-LD tests to author (in
`fluree-db-api/tests/it_query*.rs`, run via `grp_query`/`grp_query_sparql`):

- **D2 / D2b** → `it_query.rs`: `filter` on `datatype(<arithmetic>)` and on a
  cast result; `datatype`/`lang` of an `@id`/IRI value must error, not return a
  value.
- **D4** → `it_query_analytical.rs`: arithmetic on mixed xsd:float/decimal/double
  literals asserting the result datatype (`float+float=float`,
  `double+decimal=double`).
- **D5 / D7** → `it_query.rs`: `filter` `=`/`!=` across
  plain/xsd:string/unknown-datatype/lang literals (unknown-vs-known ⇒ excluded;
  plain=xsd:string ⇒ included); `in`/`sameTerm` semantics.
- **D-EBV** → `it_query.rs`: `filter` on a bare numeric-zero / empty-string /
  ill-typed variable, and unbound-via-optional with negation.
- **D-D1** → `it_query.rs`: a constant/variable-free `filter` (e.g.
  `filter` `true` / `1 = 1`) must not drop rows.
- **D6** (if pursued) → `it_query.rs`: `DISTINCT` over `"01"`/`"1"` integer
  literals; `str()` of a non-canonical double lexeme.
- **#1319** → `it_query.rs`: a `values`/inline integer grouped/DISTINCT with a
  stored integer of the same value collapses to one group.

The SPARQL-only surfaces (`sameTerm`, `IN` as text, `BNODE`, `REGEX` flags) need
no JSON-LD syntax, but their underlying IR capability should stay reachable
wherever it already is.

---

## 5. Blast radius, PR decomposition, risks, open questions

**Blast radius.** D5/D7 and D-EBV touch the two hottest evaluation paths
(`compare.rs`, `binding.rs` EBV) and the value model (`ComparableValue`,
`TryFrom<FlakeValue>`); a mistake here regresses every FILTER and join residual.
D2/D2b/D3/D8/D9/D10/D11/D12/D1/#1319 are localized (one function or one lowering
arm each) and low-radius. D6 is repo-wide (ingest + storage + serialization).

**Suggested PR decomposition** (this cluster warrants 3, not 1):

- **PR-A — cheap high-yield, no hot-path change (land first).** D1 (constant
  filter placement), D2 + D2b (DATATYPE/LANG argument + type errors), D3
  (dateTime cast), D8 (IRI base), D10 (regex q), #1319 (bare-integer lowering).
  This clears **~40 register entries** (all 22 type-promotion + all 7 cast via
  D2, plus in01/notin01/boolean-literal, iri01, cast-dT, both regex,
  datatype-2/lang-1/2/3) with essentially zero bench risk. Gate: `query_hot_bsbm`
  as a sanity check only.
- **PR-B — the equality/EBV/promotion lattice (bench-gated).** D5 + D7
  (datatype-aware comparison, `sameTerm`/`IN`), D-EBV (bare-variable EBV), D4
  (float + double/decimal promotion), D11 (CONCAT type error), D12 (lang case /
  langString output). Clears the `expr-equals`, `open-eq-04..12`,
  `boolean-effective-value` (bev-1..6), `expr-ops`, most `expr-builtin`, and the
  functions `concat02`/`strlang03-rdf11`. Gate: **both** `query_hot_bsbm` and
  `query_hot_bsbm_bi`, every commit.
- **PR-C (design-gated) — D6 lexical-form preservation vs documented divergence.**
  Decide before coding. Covers `open-eq-01`, `dawg-str-1/2`, `distinct-1/9`,
  `sameTerm-*`. Likely a mix: a lexeme column for round-trip fidelity, and a
  registered divergence for value-based simple-entailment matching. Do not block
  A/B on this.

**Risks.**
- D5/D7 "reject-more/differ-more": making unknown-datatype `=` a type error and
  `!=` exclude will *change results* for existing non-W3C users who currently get
  string-value equality across datatypes — a behaviour change, not just a
  compliance fix. Flag for review.
- D-EBV changes truthiness of numeric-zero literals in existing filters — same
  caveat; some user queries may rely on the current (wrong) permissive behaviour.
- D4 `ComparableValue` widening risks enum-size/perf; measure.
- D2b: the `id_type`-of-IRI result is an intentional JSON-LD extension — the type
  error must be **SPARQL-scoped**, not global, or JSON-LD `datatype()` semantics
  regress.

**Open questions.**
1. D6: value-matching vs term-matching — implement lexeme preservation, or
   register a documented divergence? (Same class of call as §5.3 dataset
   strategy; needs a design decision + owner.)
2. D2b/D11/D5: does Fluree want a strict "SPARQL type-error" mode distinct from
   the lenient JSON-LD coercion mode, or one unified strict path? The three
   surfaces currently share evaluation; a per-surface strictness flag may be
   needed.
3. #1319: fix at lowering (bare integer → xsd:integer) is proposed; confirm no
   consumer depends on the current `xsd:long` tag for VALUES-sourced integers.
4. `!=` on incomparable operands: confirm the intended demotion (type error →
   excluded) matches how Fluree wants FILTER error semantics to read for the
   JSON-LD surface.

## 6. Register entries that are NOT expression-semantics bugs (reassign)

Verified by running each; these fail for reasons outside this cluster and should
move to the named owner so the register reflects the true root cause:

- **Harness — `.rdf` DAWG result-set parser** (`testsuite-sparql/src/result_format.rs`
  `parse_rdf_result`): `dawg-sort-3/6/8`. The `.rdf` fixture for `sort-3` has 4
  solutions but the parser returns **1** (it drops solutions containing an
  unbound variable — `sort-3` has an `OPTIONAL` mbox). Fluree's sort output is
  correct (4 rows, unbound-first ASC). → Phase-A / harness owner, not the engine.
- **Algebra / OPTIONAL & join variable scope**: `algebra/{filter-nested-2,
  join-scope-1, join-combo-2, nested-opt-1, nested-opt-2}`,
  `optional/{complex-2,3,4}`, `optional-filter/dawg-optional-filter-005`. These
  are FILTER-scope (`{ :x :p ?v . { FILTER(?v = 1) } }` → expected 0, actual 1 —
  Fluree lets a nested-group FILTER see an enclosing-scope variable) and
  nested-OPTIONAL/join-scope semantics — the algebra cluster, not value
  semantics. `optional/complex-2` additionally uses `GRAPH ?x` and is blocked by
  the known GRAPH-var-bound-as-literal issue (audit §8).
- **GRAPH + equality**: `expr-equals/eq-graph-1/2/4` combine `GRAPH` with `=`;
  gated on the GRAPH-var issue (algebra) plus D5.
- **CONSTRUCT/serialization**: `construct/construct-3`, `construct-4`
  (`query-reif-*`, reification output) — CONSTRUCT-graph serialization, not value
  semantics.
- **Parser / serialization ("basic")**: `basic/{list-1..4}` (RDF collection `( … )`
  syntax in patterns — parser, overlaps the parser-syntax burn-down),
  `basic/{quotes-3,quotes-4}` (string-escape serialization), `basic/{base-prefix-2,
  base-prefix-5}` (relative-IRI/base resolution in output — overlaps D8).
  (`base-prefix-1` is explicitly another agent's parser bug.)
- **CSV serialization**: `SPARQL11_CSV_TSV/csv03` — canonical `xsd:double` lexical
  form in CSV output (audit §8); a CSV serializer fix (related to D6's canonical
  form), not comparison semantics.

Removing these from the expression cluster's accounting: of ~110 register
entries nominally in scope, **~82 are genuine expression/value-semantics engine
defects** (the 13 D-items + #1319), and **~28 are misattributed** to the six
buckets above.
