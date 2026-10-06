# SPARQL 1.2 Wave 1 — Turtle-star Ingest + Mini-features Burn-down

**Clusters (this doc owns these registers):** `SPARQL12_VERSION` (3),
`SPARQL12_LANG_BASEDIR` (10), `SPARQL12_CODEPOINT_ESCAPES` (6),
`SPARQL12_RDF11` (3), `SPARQL12_GROUPING` (1), `SPARQL12_EXPRESSION` (1),
`SPARQL12_SYNTAX` (2), `SPARQL12_EVAL_TRIPLE_TERMS` (41). Total **66** tests.

**Boundary with siblings:** `docs/audit/burn-down/sparql12-wave2-triple-terms.md`
owns the two `SPARQL12_SYNTAX_TRIPLE_TERMS_*` registers (query-side `<< >>` /
`<<( )>>` parsing) and the D3 triple-terms-as-values decision. That doc
explicitly defers `SPARQL12_EVAL_TRIPLE_TERMS` (data-load) to this doc. The two
meet at exactly one seam: **most eval tests need BOTH wave-1 ingest (data loads)
AND wave-2 query syntax (`<< >>` patterns parse) to go green.** Only a small
subset passes on ingest alone (§3.4).

**Method:** every claim below is grounded in the baseline error JSONs
(`scratchpad/w3c-baseline2/sparql12_*.json`), the W3C `.rq`/`.ttl`/`.srj` files
under `testsuite-sparql/rdf-tests/sparql/sparql12/`, and the current Rust
source. Where I reversed the audit's diagnosis I say so and give the evidence.

---

## 0. Two headline corrections to the §4.3 audit

Before the per-register detail, two register comments are **wrong** and should
be fixed in the same PR that touches them:

1. **`SPARQL12_VERSION` is not a VERSION-declaration gap.** `VERSION` is already
   lexed (`fluree-db-sparql/src/lex/token.rs:790` → `KwVersion`) and parsed
   (`parse/query/mod.rs:240` `parse_version_decl`, "lex-and-accept, ignore the
   value"). Proof: version-03/04/06 — which all carry `VERSION "..."` — **pass
   today**; version-01/02/05 fail. The only difference is the WHERE body:
   the three failures all contain `<< ?s ?p ?o >>` (a bare reified-triple
   pattern), the passes contain plain `?s ?p ?o`. These 3 are **wave-2
   query-syntax** tests (they are `PositiveSyntaxTest`s that go green the moment
   wave-2's PR-A makes the parser accept bare `<< >>` patterns). Recommendation:
   re-point the register comment at wave-2 and drop the "VERSION declaration"
   rationale. **Net wave-1 work for VERSION: zero.**

2. **`SPARQL12_LANG_BASEDIR` is not a "parser-local and cheap" mini-feature.**
   The §4.3 table lumps it with VERSION/codepoint as cheap parser-locals. It is
   the **second-largest item in this cluster** after Turtle-star ingest: it
   needs a base-direction **representation decision** (the literal has no room
   for direction anywhere in the stack — §4.3 below), **4 new functions**
   (`LANGDIR`, `STRLANGDIR`, `hasLANG`, `hasLANGDIR`), langtag lexer changes in
   **both** the SPARQL and Turtle lexers, and evaluation-semantics work. Treat
   it as a design question, not a quick win.

---

## 1. Per-register root cause + evidence

### 1.1 `SPARQL12_CODEPOINT_ESCAPES` (6) — global `\u` unescape pre-pass — CLEAN WAVE 1

| test | kind | file content | today |
|---|---|---|---|
| codepoint-esc-01 | positive | `ASK {}` (whole `ASK {}` escaped) | parser rejects (valid) |
| codepoint-esc-02 | positive | `ns:id\=123` (`\`→`\`, PN_LOCAL_ESC) | parser rejects |
| codepoint-esc-06 | positive | `og:audio%3Atitle` (`%`→`%`, %-encoding in PN_LOCAL) | parser rejects |
| codepoint-esc-07 | positive | `SELECT *\U00000009WHERE` (escaped inter-token whitespace) | parser rejects |
| codepoint-esc-08 | positive | `?s ?p "value"` (`"`→`"` starts a literal) | parser rejects |
| codepoint-esc-bad-03 | **negative** | `"\\u0041"` → decodes to `\A` = invalid string escape | parser **accepts** (should reject) |

**Root cause.** SPARQL 1.2 requires `\uXXXX`/`\UXXXXXXXX` to be unescaped as a
**global first pass over the whole query string, before tokenizing** — so an
escape may produce a token boundary (esc-07), a delimiter (esc-08), or the whole
query (esc-01). Fluree's lexer only handles `\u` *inside* IRI tokens
(`lex/lexer.rs:181`) and string tokens (`:701`); there is no global pre-pass.
bad-03 is the tell: `"\\u0041"` must unescape codepoints **first** (→ `"\A"`,
then the string-escape pass rejects `\A`); Fluree instead consumes `\\`→`\` as a
string escape and accepts. The manifest comment says it verbatim: "requires
handling codepoint escaping before backslash escaping."

**Fix.** Add a pre-lex pass `unescape_codepoints(&str) -> Cow<str>` run at the
top of `Lexer::tokenize` before the winnow token pass. Fast path: if the input
contains no `\`, return `Cow::Borrowed` (no allocation). It only rewrites `\u`/
`\U`; every other `\` is left byte-identical so the downstream string/IRI escape
logic is unchanged. esc-02/06 additionally exercise PN_LOCAL `\=` / `%3A`, which
the prefixed-name lexer already accepts once the `\u` is resolved (esc-04/05,
same family, pass today). This is **parse-time, SPARQL-surface, off every hot
path** (queries are parsed once; not import-hot). Clean wave-1 green: all 6.

### 1.2 `SPARQL12_SYNTAX` (2) — missing negative-syntax validation — CLEAN WAVE 1

| test | query | required behavior | today |
|---|---|---|---|
| nested-aggregate-functions | `SELECT (COUNT(COUNT(*)) AS ?c) WHERE {}` | reject (aggregate inside aggregate is illegal) | accepts |
| duplicated-values-variable | `SELECT * WHERE { VALUES (?a ?a) { (1 1) } }` | reject (duplicate var in VALUES list) | accepts |

**Root cause.** Two missing checks in the validation pass
(`fluree-db-sparql/src/validate/mod.rs`, `validate()` → diagnostics). Neither is
in the grammar; both are semantic constraints.

**Fix.** In `validate_select`: (a) walk `SELECT`-expression aggregates and error
if an aggregate argument transitively contains another aggregate; (b) in the
VALUES handler, error on a repeated variable in the var-list. Both are AST
walks, **parse-time, zero engine risk**. JSON-LD parity: JSON-LD query has a
`values` clause — add the duplicate-variable check there too if it is
expressible; nested aggregates likely aren't reachable via the JSON-LD surface
(note the decision either way).

### 1.3 `SPARQL12_RDF11` (3) — DATATYPE-on-literal + simple-literal identity

| test | query | expected | today error |
|---|---|---|---|
| langstring-datatype | `DATATYPE("foo"@en)` | `rdf:langString` | `Query error: DATATYPE requires a variable argument` |
| plain-string-datatype | `DATATYPE("foo")` | `xsd:string` | `DATATYPE requires a variable argument` |
| plain-string-same | `ASK { FILTER(sameTerm("foo", "foo"^^xsd:string)) }` | `true` | result mismatch (Fluree ≠ true) |

**Root cause.** (1) `DATATYPE` (and, per the lang-basedir `datatype` test below,
the general expression evaluator) rejects a **constant literal** argument,
requiring a bound variable. This is an over-narrow guard in expression
lowering/eval. (2) `sameTerm("foo", "foo"^^xsd:string)` must be `true`: RDF 1.1
makes a simple literal **identical** to its `xsd:string` typed form. Fluree
treats them as distinct terms.

**Fix.** (1) Allow `DATATYPE`/other unary literal functions over constant
literal operands (evaluate at the constant). This is an IR/engine fix — add a
JSON-LD regression test. (2) Canonicalize plain literals to `xsd:string` at the
term level (RDF 1.1 "simple literal = xsd:string") so `sameTerm` and term
equality collapse them. **Caution:** #2 is a term-identity change that ripples
through equality, DISTINCT, GROUP BY, and index keys — bench-guard and verify it
does not regress the `sparql10` open-world/type-promotion cluster. Schedulable
in wave 1 but it is a semantics fix, not a parser tweak.

### 1.4 `SPARQL12_GROUPING` (1) — group key uses value, must use term

`group01`: `SELECT ?v (COUNT(*) AS ?cnt) { ?s :p ?v } GROUP BY ?v` over data
`"1"^^xsd:integer` ×2, `"001"^^xsd:integer`, `"1"^^xsd:string`. **Expected 3
groups:** `1^^integer` (cnt 2), `001^^integer` (cnt 1), `1^^string` (cnt 1).

**Root cause.** GROUP BY partitions by **RDF term** (lexical form + datatype
sensitive), so `"1"^^integer` and `"001"^^integer` — same *value*, different
*term* — are distinct groups. Fluree groups by canonical numeric value (or
normalizes `001`→`1`), collapsing them → result mismatch. Interacts with §1.3
#2: term identity, not value identity, is the grouping key.

**Fix.** Make the GROUP BY key a term key (preserve lexical form for numerics),
not a value key. **Hot-path sensitive:** grouping is on the aggregation path;
keep the common single-datatype grouping fast and only widen the key
representation. Bench `query_hot_bsbm_bi`. IR/engine fix → JSON-LD parity test.

### 1.5 `SPARQL12_EXPRESSION` (1) — EBV over mixed types

`not-not`: `BIND(!!?v AS ?ebv)` over `VALUES ?v { true 1 "a" false 0 "" "a"@en
"z"^^xsd:boolean "2020-...T..."^^xsd:dateTime :a }`. Expected: `true` for
`{true,1,"a"}`, `false` for `{false,0,""}`, and **`?ebv` unbound (error)** for
`"a"@en`, invalid `"z"^^xsd:boolean`, `dateTime`, and the IRI `:a`.

**Root cause.** Effective-boolean-value is defined only for `xsd:boolean`,
numerics, and plain/`xsd:string`. A language-tagged string, an ill-typed
boolean, a `dateTime`, and an IRI must each raise an EBV type error (→ unbound
after `!!`). Fluree diverges on at least one row (same family as the `sparql10`
`boolean-effective-value` / `dawg-bev-*` failures). Result mismatch.

**Fix.** Correct EBV type-domain handling in the expression evaluator (shared
with the sparql10 bev cluster — coordinate with the Phase C1 owner to avoid two
diverging fixes). IR/engine → JSON-LD parity test.

### 1.6 `SPARQL12_LANG_BASEDIR` (10) — base direction + 4 new functions

Two distinct lexer failures plus an evaluation/representation layer:

| test | query gist | today error | needs |
|---|---|---|---|
| datatype | `DATATYPE("foo"@en--ltr)` | lexer: `unexpected character: 'l'` | base-dir langtag lexing + `rdf:dirLangString` datatype + DATATYPE-on-literal (§1.3) |
| concat | `CONCAT("a"@en--ltr, ...)` | `'l'` | base-dir langtag; dir-propagation rules in CONCAT |
| contains | `CONTAINS("abc"@en--ltr, "b"@en--ltr)` | `'l'` | base-dir langtag; arg compat |
| langdir | `LANGDIR(?object)` | `'L'` | **new fn** LANGDIR + dir stored on data |
| langdir-literal | `LANGDIR(?v)` over `VALUES { "a"@en "l"@en--ltr "r"@en--rtl }` | `'L'` | new fn + inline base-dir langtag + dir represented |
| strlangdir | `STRLANGDIR("abc","en","ltr")` | `'S'` | **new fn** STRLANGDIR |
| haslang | `hasLANG(?object)` | `'h'` | **new fn** hasLANG |
| haslangdir | `hasLANGDIR(?object)` | `'h'` | **new fn** hasLANGDIR |
| lang | `LANG(?object)` over base-dir data | result mismatch | data must load; LANG strips direction |
| strlang | `STRLANG("abc","")` etc. | result mismatch | `STRLANG(_, "")` must error/unbind; type-guard args |

Root causes, decomposed:

- **Base-direction langtag lexing.** Both lexers stop the langtag at `en`. The
  SPARQL grammar is `'@' [a-zA-Z]+ ('-' [a-zA-Z0-9]+)*`
  (`fluree-db-sparql/src/lex/lexer.rs:840`, `parse_lang_tag`); the Turtle lexer
  is identical (`fluree-graph-turtle/src/lex/token.rs`, LangTag). `@en--ltr`:
  after `en`, the loop sees `-`, peeks the next char (`-`), it is not
  alphanumeric → stop. The residual `--ltr` then fails to lex. RDF 1.2 extends
  the production with an optional `'--' [a-zA-Z]+` base-direction suffix; both
  lexers need it, and both must still **reject** an invalid direction
  (`langdir-literal-invalid`: `"foo"@en--foo` is a `NegativeSyntaxTest` that
  passes "for free" today only because the lexer rejects all `--`; after the
  fix it must fail on `foo` ≠ `ltr`/`rtl`).
- **Four new functions** (`LANGDIR`, `STRLANGDIR`, `hasLANG`, `hasLANGDIR`).
  These are not keywords, and the SPARQL lexer has **no general identifier
  token** — a bare word that is neither a known keyword nor a `prefix:local`
  is unlexable, which is exactly the `unexpected character: 'h'/'L'/'S'` error.
  So each needs: a lexer keyword, a parser builtin-call arm, an AST node, and
  an evaluator. (`LANG`, `DATATYPE`, `CONCAT`, `CONTAINS`, `STRLANG` already
  exist; they only need base-dir-aware behavior.)
- **Representation** — the design question in §4.3.

**Data:** `data-lang.ttl` uses `"abc"@en--ltr`, `"تصميم..."@ar--rtl`. Expected
serialization (`langdir-literal.srj`) renders direction as
`"its:dir": "ltr"` alongside `"xml:lang": "en"`, and `datatype.srj` expects the
datatype IRI `rdf:dirLangString`.

---

## 2. Turtle-star ingest — the centerpiece (`SPARQL12_EVAL_TRIPLE_TERMS`, 41)

### 2.1 Ground truth: 40/41 fail at DATA LOAD, not query execution

Bucketing the 41 baseline errors: **40 fail while loading the `qt:data` file**
(`Turtle parse error: ... invalid or unterminated IRI` on `<<`, or
`expected Dot, found LBrace` on `{|`, or `expected subject, found KwGraph` on
TriG `GRAPH`). Exactly **one** (`expr-2`) fails at query parse
(`unexpected character: 'i'` = `isTriple`). The Turtle lexer treats `<<` as the
start of an IRI `<...>` and never finds the closing `>` — hence "unterminated
IRI." Confirmed: `fluree-graph-turtle/src/lex/token.rs` has **zero** star tokens
(no `<<`, `>>`, `<<(`, `)>>`, `{|`, `|}`, `~`); `fluree-graph-ir::Term`
(`term.rs:228`) is `Iri | BlankNode | Literal` only.

### 2.2 Construct inventory (read from every data file)

| construct | example (data file) | RDF 1.2 meaning | Fluree mapping |
|---|---|---|---|
| **reified triple, anon reifier** | `<<:a :b :c>> :q :z` (data-1) | assert `:a :b :c`; mint anon reifier `r`; `r :q :z` | base edge + minted reifier node + `f:reifies*` |
| **reified triple, named reifier** | `<<:s :p1 :o ~ :reifier>>` (data-2) | reifier is `:reifier` (IRI) | base edge + `:reifier` node + `f:reifies*` |
| **reified triple, bnode reifier** | `<< :a :b 9 ~ _:bnodereifier >>` (data-7) | reifier is the bnode | base edge + bnode reifier + `f:reifies*` |
| **reified triple as object** | `:z1 :q << :s1 :p :o >>` (data-5) | object slot = the reifier | ordinary edge to reifier node |
| **nested reified triple** | `<< <<:s :p2 :o>> :p3 :z >>` (data-2) | recursive | recurse: inner reifier is subject of outer |
| **annotation block** | `:a :b :c {| :q :z |}` (data-3, data-8) | assert `:a :b :c`; reifier `r`; `r :q :z` | **exact `@annotation` analog** |
| **triple term** | `:a :q <<(:a :b :c)>>` (data-0-tripleterms, data-10) | object is the triple **term**; base **not** asserted | ⚠ no Fluree representation (see D3) |
| **nested triple term** | `<<( :s :p <<(:x2 :y3 123)>> )>>` (data-0-tt) | recursive term | ⚠ D3 |
| **TriG GRAPH block** | `GRAPH :g { <<:s :p :o1>> :q1 :z1 }` (data-4/6.trig) | named graph | ⚠ **separate gap** (see 2.5) |

The first six rows are the **asserting / reifier-model** forms. They map
one-to-one onto Fluree's existing edge-annotation model. The **triple-term**
rows (`<<( )>>`) do not — that is the D3 decision the wave-2 doc owns.

### 2.3 Design: how Turtle-star should feed the reifier pipeline

**Key architectural fact:** Turtle ingest is NOT the JSON-LD path.
`insert_turtle` (`testsuite-sparql/src/query_handler.rs:149`) calls
`fluree_graph_turtle::parse(ttl, &mut sink)` — a **streaming sink** that emits
IR triples to a `FlakeSink`/`ImportSink` (`fluree-db-transact/src/import.rs`).
Only the JSON-LD path runs `edge_annotations::lower_edge_annotations`
(`parse/jsonld.rs:144`). So "feed the same reifier pipeline" does **not** mean
routing Turtle through `edge_annotations.rs` (that rewrites a `serde_json::Value`
document; Turtle streams `Term`s). It means **producing byte-identical
`f:reifies*` flakes**, and there is already one canonical encoder for that:

> `EdgeKey::to_reifies_facts(&self, ann: &Sid, t, op)` — `fluree-db-core/src/edge.rs:139`
> (and `to_reifies_facts_jsonld_compatible` at `:250`, which omits the optional
> `f:reifiesDatatype` flake exactly as the JSON-LD lowering does).

`edge_annotations.rs` is documented as **mirroring** this builder in JSON. The
Turtle path should call it **directly**. Recommended structure:

1. **Lexer (`fluree-graph-turtle/src/lex`):** add tokens `TripleStart <<`,
   `TripleEnd >>`, `TripleTermStart <<(`, `TripleTermEnd )>>`,
   `AnnotationOpen {|`, `AnnotationClose |}`, `Tilde ~`. Mirror the SPARQL
   lexer, which already has all of these (`lexer.rs:878-899, 946`) — so the
   token vocabulary and disambiguation (`<<(` before `<<` before `<`; `{|`
   before `{`) are already worked out on the SPARQL side and can be copied.
2. **Parser (`parser.rs`):** in `parse_subject` (`:473`) and `parse_object`
   (`:608`), add a `TripleStart` arm → `parse_reified_triple` returning the
   reifier `TermId`; after an object in `parse_object_list` (`:570`), add an
   optional `AnnotationOpen` tail. `parse_reified_triple` recursively parses
   `subject predicate object [~ reifier]`, emits the **base** triple to the
   sink, obtains/mints the reifier `TermId`, and signals the reifier facts.
3. **Sink boundary (the reifier encoding lives here, once):** add sink events
   `on_reified_edge(base_s, base_p, base_o, reifier)` and
   `on_annotation(base_s, base_p, base_o, reifier, props)`. The
   fluree-db-transact sink implementation resolves SIDs, builds an `EdgeKey`,
   and calls `EdgeKey::to_reifies_facts` (or the `_jsonld_compatible` variant —
   pick the one the JSON-LD path uses so both surfaces are bit-identical). The
   generic `fluree-graph-turtle` crate stays encoding-agnostic (it only knows
   "this triple has a reifier"), so the `f:reifies*` schema is never duplicated.

This keeps the reifier encoding in exactly one place (core `EdgeKey`), shared by
JSON-LD and Turtle — which is precisely the parity property §4 asks for.

The **triple-term** forms (`<<( )>>`) are deliberately **not** in this design:
they denote a term without asserting the base triple and require a first-class
triple-term value (a new `Term` variant or a durable term encoding). That is the
D3 epic owned by the wave-2 doc. Wave-1 ingest should **reject `<<( )>>` in the
Turtle parser with a clear "triple terms as values are deferred (D3)" error**,
the same posture the SPARQL parser takes (`parse/query/term.rs:699` restricts
`<<( )>>` to `rdf:reifies` object position).

### 2.4 Wave-1 vs wave-2 split of the 41 eval tests

A test goes green only if **data loads** (ingest) AND **query parses/evaluates**
AND **results serialize/compare**. Classifying by the binding constraint:

**(a) Green on wave-1 ingest alone — 2 tests.** Plain-BGP query, IRI-only
output, data uses only asserting/reifier forms:
- `pattern-3` — query `?s :b ?o . ?o :b ?z`; data-2 named reifier `:reifier`
  becomes a queryable node so `:a1 :b :reifier . :reifier :b :a2` matches
  (expected single row `:a1, :reifier, :a2`).
- `pattern-3-nomatch` — same data, `:b2` variant, expected empty.

  Caveat: data-2.ttl must fully ingest (it also contains anon reifiers and
  **nested** reified triples), so these two need the *complete* asserting-form
  ingest, not a subset.

**(b) Blocked on wave-2 QUERY syntax (bare `<< >>` patterns) — ~19.** Data
ingests (wave-1) but the query uses `<< >>` reified-triple patterns as BGP
terms, which the legacy query path does not support for arbitrary predicates
(see 2.6): `basic-2, basic-3, basic-4, basic-5, basic-6, pattern-1, pattern-2,
pattern-4, pattern-5, pattern-6, pattern-7, pattern-8, pattern-8-nomatch,
pattern-9, pattern-10` (partial), `graphs-1, graphs-2, update-1, update-2`.

**(c) Blocked on D3 triple-terms-as-values (VALUES/IN/BIND/binding/ordering) —
~12.** Query and/or data use `<<( )>>` as a value: `basic-8, basic-9,
pattern-11, op-1, op-2, expr-1, order-1, order-2` and the data files
`data-7/9/10/order/order-kind`. `op-1`/`op-2` have plain-BGP queries but their
**bindings and expected output are triple terms** (`op-1.srj` shows
`"type":"triple"`), so they need value support + result serialization.

**(d) Blocked on triple-term FUNCTIONS (D2) — 1.** `expr-2` (`isTriple`,
`SUBJECT`, `PREDICATE`, `OBJECT`) — the only query-parse failure.

**(e) Blocked on triple-term result SERIALIZATION (harness + `result_format`) —
overlaps b/c.** `results-tripleterms-1j/1x`, `results-reifiedtriples-1j/1x`,
`basic-7`, and every construct/op/order test whose expected `.srj`/`.srx`/
`.trig` embeds `"type":"triple"` (or `<< >>` in TriG). The harness's
`result_format.rs`/`result_comparison.rs` and the expected-file parser must
handle triple-term result values — otherwise even a correct engine result won't
compare. Note `basic-7` uses the **supported** `{| ?p ?o |}` query form yet is
still wave-2: its expected output binds `?o` to the `rdf:reifies` **triple term**
(one row), which needs both triple-term serialization AND exposing `rdf:reifies`
as a queryable predicate over the annotation model — Fluree hides the base edge's
reifier link behind `f:reifies*`.

**(f) CONSTRUCT annotation/triple-term projection (D4) — 5.** `construct-1..5`
build `<< >>`/`{| |}` in the template; `lower/construct.rs` returns
`UnsupportedFeature` for annotation projection today (audit §4.3 item 4).

**(g) TriG multi-graph — 5 (overlaps b/c).** `graphs-1, graphs-2, expr-1,
update-1, update-2` load `.trig` with `GRAPH` blocks (2.5).

**Bottom line for the register:** wave-1 ingest turns **2** eval tests green
(`pattern-3`, `pattern-3-nomatch`). The other 39 stay registered, re-pointed at
the specific wave-2 dependency (query-syntax / D3 / D2 / serialization / D4 /
TriG). **Recommended sequencing: land wave-1 ingest first, then re-run the eval
suite** — with data loading, the 39 will surface their *actual* query/eval
errors, giving a precise, code-verified wave-2 boundary instead of the
inferred-from-source one above. (Today no eval query has ever executed, so the
wave-2 query behavior is reasoned from source, not observed.)

### 2.5 TriG `GRAPH` blocks — a separate, orthogonal gap

`data-6.trig` is pure `GRAPH :g1 { :s1 :p1 :o1 }` with **no star at all** and
still fails `expected subject, found KwGraph`. `fluree-graph-turtle`'s
`parse_statement` (`parser.rs:364`) dispatches only prefix/base/triples — there
is **no** handling for the TriG `GRAPH` keyword block *or* the label form
`:g { }`. So the crate cannot parse multi-graph TriG at all. (Phase A's
"named graphs load via the transact builder" wraps each graph separately; it
does not parse a multi-graph `.trig` document.) This blocks graphs-1/2, expr-1,
update-1/2 independently of star support, and should be tracked as its own
work item (TriG named-graph parsing in `fluree-graph-turtle`), not folded into
the star PR.

### 2.6 Why the legacy `<< >>` query path does not cover the eval queries

`fluree-db-sparql/src/lower/rdf_star.rs` handles `<< s p o >> f:t ?t ; f:op ?op`
— the **old Fluree metadata form** (extract `f:t`/`f:op` from a quoted-triple
subject) — and explicitly **rejects** an RDF 1.2 annotation tail on a
quoted-triple subject (`not_implemented`). The eval queries use `<<:s :p :o>>
:q :z` with arbitrary predicates and expect reifier semantics, which this path
does not provide. Hence bucket (b) is genuine wave-2 work, not a
lowering-config away.

---

## 3. Hot-path classification (§6 discipline)

| change | phase | hot path? | guardrail |
|---|---|---|---|
| SPARQL codepoint pre-pass | parse-time | **No** — query parse, not import; `Cow` fast path when no `\` | none needed; add a "no-escape returns borrowed" unit test |
| SPARQL negative-syntax validators | parse-time | No | none |
| base-dir langtag in **SPARQL** lexer | parse-time | No | none |
| **new functions** (LANGDIR etc.) | parse+eval | eval per-row only when the fn is used | keep un-used-fn path untouched |
| DATATYPE-on-literal / EBV / simple-literal identity | per-row eval | **Yes** (expression + equality) | `query_hot_bsbm`, `query_hot_bsbm_bi`; keep common-type path byte-identical, route only new cases slow |
| GROUP BY term-key | aggregation | **Yes** | `query_hot_bsbm_bi`; widen key only, keep single-datatype fast |
| **Turtle-star lexer tokens** | import lexing | **YES — import-hot** | `insert_formats`, `import_bulk`; the `<`/`{`/`~` dispatch gains one peek each — must stay within `regression-budget.json` |
| Turtle-star parser/sink | import parse | import-hot but **only** when star tokens present; non-star data unaffected | same benches; verify non-star import is byte-for-byte unchanged |
| base-dir representation (§4.3) | ingest + storage | depends on option | see §4.3 cost table |

The Turtle lexer is the one genuinely import-hot change. Every IRI begins with
`<`; recognizing `<<` adds a single second-byte peek on the `<` branch. That is
cheap but not free — it is the same perf-neutrality bar as any lexer change and
must clear `insert_formats`/`import_bulk`. Design the lexer so the non-star
common path (plain `<iri>`, `{` as TriG-only, no `~`) does the minimum extra
work, and assert unchanged output on a non-star corpus.

---

## 4. JSON-LD parity (§6.6 / sparql-compliance.md)

**Scope:** JSON-LD only. Cypher is out of scope for this burn-down — Fluree does
not own the openCypher grammar and will not add custom syntax to it, so there is
no Cypher parity obligation here. (Cypher still benefits automatically from the
IR/engine-level fixes below, but that is incidental, not a deliverable.)

The parity direction here is **reversed** from the usual guideline: JSON-LD is
the *reference* implementation and the new SPARQL/Turtle surfaces must match it.

- **Turtle-star ↔ JSON-LD `@annotation` (must be bit-identical).** Both must
  produce the same `f:reifies*` flakes. Enforce by construction: route both
  through `EdgeKey::to_reifies_facts` (§2.3). **Equivalence tests to author**
  (new): for each asserting form (anon reifier, named reifier, bnode reifier,
  reified-triple-as-object, nested, annotation block), load the Turtle form and
  the equivalent JSON-LD `@annotation`/`@edge` document and assert **identical
  flake sets** (subject/predicate/object/dt/graph/lang, same reifier identity
  rules). Put these next to the existing `it_edge_annotations.rs`
  `edgekey_roundtrip_*` gate tests, which already pin the JSON-LD side.
- **Base direction has no JSON-LD story.** Grep confirms
  `fluree-graph-json-ld` has **zero** `@direction` / `dirLangString` support.
  So the lang-basedir work is not "add SPARQL syntax to an existing capability"
  — the capability is absent on every surface. If base direction is in scope,
  JSON-LD 1.1 `@direction` (and an equivalent Fluree query-syntax path) must be
  added in the **same effort**, per the guideline. This materially raises the
  cost of `SPARQL12_LANG_BASEDIR` and is a reason to treat it as its own epic.
- **Codepoint escapes / negative-syntax validators** are SPARQL-surface-only
  (text-form concerns with no JSON-LD analog) — record the decision; no JSON-LD
  work. The RDF11 / grouping / expression fixes are IR/engine-level → they fix
  all surfaces implicitly but still need JSON-LD regression tests
  (`fluree-db-api/tests/it_query*.rs`).

---

## 5. The base-direction representation decision (§4.3 — options + cost)

**The gap.** Nothing in the stack has room for base direction:
`fluree-graph-ir::Term::Literal` (`term.rs:236`) has `value` / `datatype` /
`language: Option<Arc<str>>` — **no direction**; `FlakeMeta.lang`
(`fluree-db-core/src/flake.rs:34`) and `commit::Value::LangString { value, lang }`
(`commit.rs:94`) likewise; JSON-LD has none. RDF 1.2 needs `ltr`/`rtl` on a
language-tagged string and the `rdf:dirLangString` datatype.

| option | mechanism | cost | pros / cons |
|---|---|---|---|
| **A — first-class direction field** | add `direction: Option<Dir>` to IR `Term::Literal`, `FlakeMeta`, `commit::Value`, thread through JSON-LD `@direction`, SPARQL result (`its:dir`), comparison/ordering | **High, cross-cutting** — IR + core flake meta + **on-disk commit format** + json-ld + both lexers + result_format + new fns; touches `EdgeKey`/`f:reifiesLang` (needs an `f:reifiesDir` companion for annotated dir-strings) and a **storage-format version bump** | RDF-1.2-correct; clean; but a durability-affecting change and the largest surface |
| **B — encode direction in the lang string** | store `en--ltr` as the `lang` value; `LANG` strips `--…`, `LANGDIR` extracts it, `DATATYPE` returns `dirLangString` when `lang` contains `--` | **Low–medium** — no schema/storage change (`lang` is already a string); every LANG/langMatches/equality/order site must learn to split on `--` | cheapest; term identity ("en--ltr" ≠ "en--rtl" ≠ "en") falls out for free and is *correct*; risk of leaking the encoded form through un-updated call sites; JSON-LD/result serialization must re-split |
| **C — documented divergence (defer)** | reject `@…--…` with a clear error; register all 10 lang-basedir tests as reviewed divergence | **~zero** | keeps the RDF-1.2 conformance claim honest-but-incomplete; 10 tests stay skipped; matches the wave-2 doc's "accept-then-defer" posture for triple terms |

**Recommendation.** If base direction is in product scope, **Option B** is the
pragmatic wave-1 encoding (no storage-format change, correct term identity),
with the explicit follow-up that JSON-LD `@direction` and result `its:dir`
serialization are part of the same PR (§4). If it is not a near-term product
need, **Option C** (defer + documented register) is defensible and costs
nothing — this is a **product/architecture call**, not an engineering detail,
and should be made before any lang-basedir code is written.

---

## 6. Blast radius, PR composition, risks

### 6.1 PR composition (each shrinks its register in the same change; CI enforces both directions)

1. **PR-1 Codepoint escapes** (`SPARQL12_CODEPOINT_ESCAPES` −6). Global `\u`
   pre-pass + PN_LOCAL escape confirmation. Parse-time, isolated, no engine
   risk. Independent — land first.
2. **PR-2 Negative-syntax validators** (`SPARQL12_SYNTAX` −2). Two checks in
   `validate/mod.rs` + JSON-LD `values` duplicate-var check. Parse-time.
3. **PR-3 RDF11 literal semantics** (`SPARQL12_RDF11` −3). DATATYPE-on-literal +
   simple-literal = `xsd:string`. **Bench-guarded** (term identity); coordinate
   with the sparql10 type-promotion/open-world owner. Also unblocks
   lang-basedir `datatype`.
4. **PR-4 EBV + GROUP BY term-key** (`SPARQL12_EXPRESSION` −1,
   `SPARQL12_GROUPING` −1). Shared with sparql10 bev cluster; bench-guarded.
5. **PR-5 Turtle-star ingest (asserting forms)** — the centerpiece.
   `SPARQL12_EVAL_TRIPLE_TERMS` −2 (`pattern-3`, `pattern-3-nomatch`).
   Lexer tokens + parser hooks + sink events → `EdgeKey::to_reifies_facts` +
   the JSON-LD/Turtle flake-equivalence tests (§4). **Import-hot; bench
   `insert_formats`/`import_bulk`.** Explicitly rejects `<<( )>>` (D3) and TriG
   `GRAPH` (2.5) with clear deferred errors. Sizeable — keep it to the
   asserting forms only.
6. **PR-6 base direction** — gated on the §5 decision. If Option B:
   `SPARQL12_LANG_BASEDIR` −10 (langtag lexing ×2, 4 new functions, encode/
   decode, JSON-LD `@direction`, `its:dir` serialization, LANG/STRLANG semantics
   incl. the empty-lang error). If Option C: rewrite the 10 register comments as
   documented divergence. Largest or smallest PR depending on the call.
7. **VERSION:** no PR — fix the `SPARQL12_VERSION` register comment to reference
   wave-2 PR-A (bare `<< >>` syntax); the 3 go green there.

Independent, landable now: PR-1, PR-2. Semantics (bench-guarded): PR-3, PR-4.
Big: PR-5. Decision-gated: PR-6.

### 6.2 Risks

- **Turtle lexer perf regression** (import-hot). Mitigate: minimal peek on `<`/
  `{`/`~`; assert byte-identical output on a non-star corpus; bench gate.
- **Term-identity ripple** (RDF11 simple-literal + GROUP BY term-key + Option-B
  lang encoding). Each changes what "same term" means and can silently move
  DISTINCT/GROUP BY/equality/index results. Verify against the whole sparql10
  eval suite, not just the target tests.
- **Reifier-flake divergence** between Turtle and JSON-LD. Mitigate by
  construction (single `EdgeKey` encoder) + the equivalence tests; do **not**
  re-implement the `f:reifies*` schema in the Turtle crate.
- **Inferred wave-2 boundary.** Bucket (b)/(c)/(d) rest on source reading, since
  no eval query has executed. The plan de-risks this by running the eval suite
  immediately after PR-5 to observe real query errors.
- **base-direction storage-format change** (Option A only) — durability/version
  risk; a reason to prefer B or C.

### 6.3 Open design questions (need an owner decision)

1. **Base direction: A / B / C?** (§5) — product + architecture call; blocks
   `SPARQL12_LANG_BASEDIR`. Pairs with the D3 triple-terms-as-values decision
   in the wave-2 doc (both are "first-class RDF-1.2 value" questions).
2. **`SPARQL12_VERSION` register** — confirm the reclassification (VERSION done;
   3 tests belong to wave-2 syntax) and correct the comment.
3. **TriG multi-graph parsing** in `fluree-graph-turtle` (§2.5) — own it
   separately or fold into a graph epic? Blocks 5 eval tests independent of star.
4. **Which `to_reifies_facts` variant** the JSON-LD path actually uses at
   ingest (`to_reifies_facts` vs `_jsonld_compatible`) — the Turtle sink must
   call the *same* one for bit-identical flakes; confirm before writing PR-5.
