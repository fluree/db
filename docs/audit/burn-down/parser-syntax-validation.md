# Burn-down: SPARQL query-side parser syntax + validation gaps

**Cluster owner deliverable — pre-implementation deep audit.** No source was
modified. Parent audit: `docs/audit/2026-07-sparql-testsuite-audit.md` (§4.2.3,
§4.2.4, §4.2.6). Register: `testsuite-sparql/tests/registers/mod.rs`
(`SPARQL10_SYNTAX`, `SPARQL11_SYNTAX_QUERY`, plus the negative-syntax entries in
`SPARQL11_AGGREGATES` / `SPARQL11_GROUPING`). Baseline rdf-tests submodule
`efccbc6b8`. Every root cause below was reproduced against the live parser with
a throwaway probe (`parse_sparql` + `validate`); the verdicts quoted are actual
parser output.

Spec references are to *SPARQL 1.1 Query Language*
(https://www.w3.org/TR/sparql11-query/), production numbers per its
§19.8 grammar.

## 0. Scope: 62 tests

| Sub-cluster | Tests | Kind | Fix locus |
|---|---|---|---|
| **P1** RDF collections `( … )` in patterns | 13 | positive-rejected | parser (desugar) |
| **P4** empty/relative IRIREF `<>` in PREFIX/BASE | 2 | positive-rejected | lexer + (lower for eval) |
| **P5a** extension-function call with `NIL` arg list `f()` | 3 | positive-rejected | expr parser |
| **P5b** bare `Constraint` as `ORDER BY` condition | 1 | positive-rejected | modifier parser |
| **P6** `SubSelect` placement (bare / OPTIONAL / after-UNION) | 3 | positive-rejected | pattern parser |
| **P7** `VALUES` with `NIL` var list `VALUES () { … }` | 2 | positive-rejected | pattern parser |
| **P8** property path as verb inside `[ … ]` | 1 | positive-rejected | term parser |
| **V1** BGP `.` (dot) structural validity | 12 | negative-accepted | parser (grammar) |
| **V2** `FILTER` requires a `Constraint` (not bare expr) | 1 | negative-accepted | pattern parser |
| **V3** blank-node label scope = single BGP | 11 | negative-accepted | **new validate pass** |
| **V4** GROUP BY / aggregate projection scope | 9 | negative-accepted | **new validate pass** |
| **V5** `BIND` target var must not be in-scope | 3 | negative-accepted | **new validate pass** |
| **V6** duplicate `AS` alias in `SELECT` | 1 | negative-accepted | **new validate pass** |

Positives (P*) are **accept-more** fixes (near-zero regression risk).
Negatives (V*) are **reject-more** fixes: V1/V2 tighten the grammar and V3–V6
add semantic validation — both can reject queries Fluree currently accepts, so
they carry regression risk for non-W3C users (see §5).

Per-test → sub-cluster map:

- **P1**: `syntax-sparql1#{syntax-lists-01..05, syntax-forms-01, syntax-forms-02}`,
  `syntax-sparql2#{syntax-lists-01..05}`, `syntax-query#test_pp_coll`
  (`test_pp_coll` also needs P8).
- **P4**: `syntax-sparql1#syntax-qname-05`, `basic#base-prefix-1`
  (the latter is registered under `SPARQL10_QUERY_EVAL` but is parser
  territory; see §3).
- **P5a**: `syntax-sparql2#{syntax-function-01, -02, -03}`.
- **P5b**: `syntax-sparql1#syntax-order-07`.
- **P6**: `syntax-query#{test_21, test_23, test_64}`.
- **P7**: `syntax-query#{test_35a, test_36a}`.
- **P8**: `syntax-query#test_63`.
- **V1**: `syntax-sparql3#{syn-bad-02, -03}` (missing dot) +
  `syntax-sparql3#{syn-bad-05..14}` (stray/leading/doubled dot).
- **V2**: `syntax-sparql3#filter-missing-parens`.
- **V3**: `syntax-sparql3#{blabel-cross-graph-bad, -optional-bad, -union-bad}` +
  `syntax-sparql4#{syn-bad-34..38, syn-bad-GRAPH-breaks-BGP,
  syn-bad-OPT-breaks-BGP, syn-bad-UNION-breaks-BGP}`.
- **V4**: `aggregates#{agg08, agg09, agg10, agg11, agg12}`,
  `grouping#{group06, group07}`, `syntax-query#{test_43, test_44}`.
- **V5**: `syntax-query#{test_60, test_61a, test_62a}`.
- **V6**: `syntax-query#test_45`.

---

## 1. Per-cluster root cause (file:line evidence)

### P1 — RDF collections `( … )` in triple patterns

Grammar: `Collection ::= '(' GraphNode+ ')'` (obj/subj position); empty `()`
is `NIL` = the IRI `rdf:nil`. Spec §4.2.4 mandates desugaring to
`rdf:first`/`rdf:rest`/`rdf:nil` triples with fresh blank nodes.

Root cause: collections are **explicitly stubbed out**. `parse_subject`
(`fluree-db-sparql/src/parse/query/term.rs:87-94`) and `parse_object`
(`term.rs:220-227`) both do:

```rust
if self.stream.check(&TokenKind::LParen) || self.stream.check(&TokenKind::Nil) {
    self.stream.error_at_current("RDF collection (list) syntax is not yet supported");
    self.skip_collection();               // term.rs:487-500
    return None;
}
```

`is_term_start` already routes `(`/`Nil` into the triples-block parser
(`fluree-db-sparql/src/parse/stream.rs:399-420`, includes `LParen` and `Nil`),
so a lone `( ?z )` reaches `parse_subject` and hits the same stub. Probe:
every P1 query → `PARSE-REJECT "RDF collection (list) syntax is not yet
supported"`. `syntax-forms-01/02` are the same production with blank-node
items (`( [ ?x ?y ] )`), which the collection parser must accept as
`GraphNode`s.

### P4 — empty / relative IRIREF `<>` in PREFIX/BASE

Grammar: `IRIREF ::= '<' ([^ control | <>"{}|^`\ ])* '>'` — the character
class permits **zero** characters, so `<>` is a valid (empty, relative) IRI
reference that resolves against `BASE` (spec §4.1.1).

Root cause: the SPARQL lexer rejects an empty IRI body.
`parse_iri_content` (`fluree-db-sparql/src/lex/lexer.rs:199-201`):

```rust
if result.is_empty() {
    return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
}
```

So `<>` produces no `Iri` token; `consume_iri` (`stream.rs:254-266`) returns
`None`; and `parse_prefix_decl` (`parse/query/mod.rs:264-286`) emits
`expected IRI after prefix namespace`. Probe: both `PREFIX : <>` and
`BASE <http://example.org/x/> PREFIX : <>` →
`PARSE-REJECT "expected IRI after prefix namespace"`. (This is the same string
the audit records for `basic#base-prefix-1`.) This lexer lives only in
`fluree-db-sparql`; it is **not** the Turtle/import lexer, so the fix is
SPARQL-query-surface-local.

### P5a — extension-function call with `NIL` arg list `f()`

Grammar: `iriOrFunction ::= iri ArgList?`; `ArgList ::= NIL | '(' … ')'`;
`NIL ::= '(' WS* ')'`.

Root cause: the lexer tokenizes `()` / `( )` / `(\n)` as a single `Nil`
token (`lexer.rs` NIL rule, tests at `lexer.rs:1139-1141`). But the primary
expression parser only treats an IRI as a function call when the **next token
is `LParen`** — it never checks for `Nil`:

```rust
// parse_primary_expr — fluree-db-sparql/src/parse/expr.rs
if let Some((prefix, local, pn_span)) = tokens.consume_prefixed_name() { // :260
    ...
    if tokens.check(&TokenKind::LParen) {          // :265  ← misses Nil
        return parse_function_call_with_iri(tokens, iri, start);
    }
    return Ok(Expression::iri(iri));               // :269  falls through, leaves `()`
}
```

Same at the full-IRI branch (`expr.rs:252`) and the `PrefixedNameNs` branch
(`expr.rs:281`). The downstream helper `parse_expression_list`
(`expr.rs:769-774`) *already* handles `Nil`, and built-ins like `NOW()` work
because they go through it — only the extension-IRI path is missing the `Nil`
check. Probe: `FILTER (q:name())` → `PARSE-REJECT "Expected ')' at position
63"` (the `Nil` is left un-consumed and the enclosing bracketed expression
then fails to find its `)`).

### P5b — bare `Constraint` as an `ORDER BY` condition

Grammar: `OrderCondition ::= ( ('ASC'|'DESC') BrackettedExpression ) |
( Constraint | Var )`; `Constraint ::= BrackettedExpression | BuiltInCall |
FunctionCall`. So `ORDER BY str(?o)` (a bare `BuiltInCall`) is valid.

Root cause: `parse_order_condition`
(`fluree-db-sparql/src/parse/query/modifier.rs:239-300`) accepts only
`ASC(...)`/`DESC(...)`, a bare `Var`, or a parenthesized expression; anything
else falls to `return None` (`modifier.rs:289-292`) →
`expected ordering condition`. The bare-builtin/function form is missing. Note
`parse_group_condition` already implements exactly this branch for `GROUP BY`
(`modifier.rs:142-165`, with a `position()`/`restore()` guard) — it is the
template for the fix. Probe: `ORDER BY str(?o)` → `PARSE-REJECT "expected
ordering condition"`.

### P6 — `SubSelect` placement

Grammar: `GroupGraphPattern ::= '{' ( SubSelect | GroupGraphPatternSub ) '}'`.
A group's entire content may be a sub-select. Three placements fail, two
distinct causes:

- **Bare sub-select as group content** (`test_21`
  `{ SELECT * { … } }`; `test_23` `{ {} OPTIONAL { SELECT * { … } } }`).
  `parse_group_graph_pattern`
  (`fluree-db-sparql/src/parse/query/pattern.rs:39-206`) only recognizes a
  sub-select when it is preceded by an explicit inner `{`
  (`pattern.rs:115-131`: `check(LBrace)` → advance → `check_keyword(KwSelect)`).
  When `SELECT` is the *first* token inside the group's own braces (opened by
  `WHERE`/`OPTIONAL`), no branch matches `KwSelect`, so it falls to the `else`
  (`pattern.rs:165-169`) → `unexpected token in graph pattern`. Probe: both →
  `PARSE-REJECT "unexpected token in graph pattern"`.

- **`UNION` after a sub-select group** (`test_64`
  `{ SELECT (1 AS ?X){} } UNION { SELECT (2 AS ?X){} }`). Here the sub-selects
  *are* brace-wrapped, so they parse — but the subquery branch
  (`pattern.rs:122-131`) pushes the sub-select and does **not** check for a
  trailing `UNION`, unlike the nested-group branch
  (`pattern.rs:132-141`, which calls `parse_union_continuation`). The next loop
  iteration sees `UNION` at `pattern.rs:58-61` → `UNION must follow a pattern`.
  Probe: → `PARSE-REJECT "UNION must follow a pattern"`.

Sub-select *execution* is otherwise working (only `subquery02/04/12` are result
mismatches in the subquery suite), so this is purely a parse-placement gap.

### P7 — `VALUES` with `NIL` variable list

Grammar: `InlineDataFull ::= ( NIL | '(' Var* ')' ) '{' ( '(' DataBlockValue*
')' | NIL )* '}'`. So `VALUES () { }` (zero vars, zero rows) and
`VALUES () { () }` (zero vars, one empty row) are valid.

Root cause: `parse_values_variables`
(`fluree-db-sparql/src/parse/query/pattern.rs:487-523`) recognizes only
`LParen` (multi-var) or a single bare `Var`; the `Nil` token (`()`) matches
neither → `expected variable or '(' after VALUES`. The zero-row/`NIL`-row form
in the data block (`parse_values_row`, `pattern.rs:528-566`) is likewise not
reached. Probe: both → `PARSE-REJECT "expected variable or '(' after VALUES"`.
Note `zero vars` also breaks the `multi_var = vars.len() > 1` dispatch at
`pattern.rs:438` (0 is treated as single-var), so the fix is a small
dedicated zero-variable path, not just a token check.

### P8 — property path as verb inside a blank-node property list

Grammar: inside `[ … ]` (`BlankNodePropertyList`), the verb is
`VerbPath | VerbSimple`, so `[ :p|:q|:r ?X ]` (an alternative path) is valid.

Root cause: `parse_blank_node_property_list`
(`fluree-db-sparql/src/parse/query/term.rs:438-481`) explicitly rejects paths:

```rust
Verb::Path(_) => {
    self.stream.error_at_current(
        "property paths inside a blank-node property list \
         ('[ path obj ]') are not yet supported");
    return None;                              // term.rs:453-459
}
```

Structural note: the blank-node-property-list desugarer surfaces its inner
triples via `pending_bnpl_triples: Vec<TriplePattern>` (`parse/query/mod.rs:180`),
which cannot carry a `GraphPattern::Path` (paths are not `TriplePattern`s).
Supporting paths here therefore needs a parallel `pending_bnpl_patterns`
channel (or a small refactor of how `[ … ]` results are drained). Probe:
`[ :p|:q|:r ?X ]` → `PARSE-REJECT "property paths inside a blank-node property
list … are not yet supported"`. `test_pp_coll` combines P1 + P8 (a collection
of blank-node lists each holding a path) and currently fails on P1 first.

### V1 — BGP dot (`.`) structural validity

Grammar: `GroupGraphPatternSub ::= TriplesBlock? ( GraphPatternNotTriples '.'?
TriplesBlock? )*` and `TriplesBlock ::= TriplesSameSubjectPath ( '.'
TriplesBlock? )?`. A `.` is a **separator that must follow a complete
TriplesSameSubject** (or, once, a `GraphPatternNotTriples`). Leading, doubled,
standalone, or missing dots are syntax errors.

Root cause: the parser treats `.` as a freely-skippable token.
`parse_group_graph_pattern` skips any dot anywhere
(`fluree-db-sparql/src/parse/query/pattern.rs:162-164`):

```rust
} else if self.stream.check(&TokenKind::Dot) {
    self.stream.advance();          // "Skip dots between patterns"
}
```

and `parse_triples_block` consumes at most one *optional* trailing dot then
returns (`term.rs:563-564`), re-entering the group loop for the next subject.
Consequences (all probed → `ACCEPT`):

- *stray/leading/doubled dot* (`syn-bad-05..14`): `{ . }`, `{ . ?s ?p ?o }`,
  `{ ?s ?p ?o . . }`, `{ ?s ?p ?o .. }` — the dot(s) are silently skipped.
- *missing dot between triples* (`syn-bad-02/03`):
  `{ :s1 :p1 :o1 :s2 :p2 :o2 . }` — `parse_triples_block` returns after
  `:s1 :p1 :o1` (no dot required), the loop re-enters on `is_term_start` and
  parses `:s2 :p2 :o2` as a second block with no separator.

This is the **highest-risk** item in the cluster because dot handling is
central to every BGP.

### V2 — `FILTER` requires a `Constraint`, not an arbitrary expression

Grammar: `Filter ::= 'FILTER' Constraint`;
`Constraint ::= BrackettedExpression | BuiltInCall | FunctionCall`. A bare
`Var` (or bare relational expression) is **not** a `Constraint`.

Root cause: `parse_filter_pattern`
(`fluree-db-sparql/src/parse/query/pattern.rs:355-370`) calls
`parse_expression` directly, accepting anything the expression grammar
accepts — including a bare `?x`. Probe: `{ ?s ?p ?o FILTER ?x }` → `ACCEPT`.

### V3 — blank-node label scope = a single BGP

Spec §19.6 grammar note: *"the same blank node label cannot be used in two
basic graph patterns in a query."* A `GroupGraphPattern` boundary
(`GRAPH`, `OPTIONAL`, `UNION`, `MINUS`, a nested `{ }`, a sub-select) starts a
new BGP; `FILTER`/`BIND` do **not**.

Root cause: `validate/mod.rs` has **no** blank-node-scope pass at all
(`validate_graph_pattern`, `validate/mod.rs:307-370`, only recurses and never
inspects blank-node labels). Probe: `_:who … OPTIONAL { … _:who }` and
`_:a ?p ?v . { _:a ?q 1 }` → `ACCEPT`. All 11 V3 tests reuse `_:a`/`_:who`
across `GRAPH`/`OPTIONAL`/`UNION`/nested-group boundaries.

### V4 — GROUP BY / aggregate projection scope

Spec §11 and the `SelectClause` grammar note: when a query groups (explicit
`GROUP BY` **or** an aggregate in the projection = implicit single group),
every projected variable must be a **group key** (a bare `GROUP BY ?v`, or the
`?v` of a `GROUP BY (expr AS ?v)`) or appear only **inside an aggregate**; and
`SELECT *` is **not** permitted with `GROUP BY`.

Root cause: no such pass exists (`validate_select`, `validate/mod.rs:102-104`,
validates only the WHERE pattern). Probe (all → `ACCEPT`):

| test | shape | why invalid |
|---|---|---|
| `test_43` | `SELECT * … GROUP BY ?s` | `*` with `GROUP BY` |
| `test_44`, `agg09` | project `?o`/`?P`, `GROUP BY ?s`/`?S` | projected var not a key |
| `group06` | project `?s ?v`, `GROUP BY ?s` | `?v` not a key (no aggregate) |
| `group07` | project `?eventName ?venue ?photo`, `GROUP BY ?event` | non-key vars |
| `agg10` | project `?P` + `COUNT(?O)`, no `GROUP BY` | implicit group, `?P` not aggregated |
| `agg08`, `agg11` | project `(?O1+?O2)`, `GROUP BY (?O1+?O2)` / `(?S)` | `?O1/?O2` not keys (the *expression* is the key, not its vars) |
| `agg12` | project `?O1`, `GROUP BY (?O1+?O2)` | `?O1` not a key |

Requires (a) an "expression uses only group-keys-or-aggregates" walk over
`Expression` (an aggregate-detection predicate + a free-variable collector) and
(b) the group-key set built from `GroupByClause` conditions
(`fluree-db-sparql/src/ast/query.rs` `GroupCondition::{Var, Expr{alias}}`).

### V5 — `BIND` target variable must not be in-scope

Spec §10.1 / grammar note on `Bind`: *the variable assigned by `BIND(expr AS
?v)` must not already be in-use in the group graph pattern up to that point.*

Root cause: no pass (`GraphPattern::Bind` is a no-op in the validator,
`validate/mod.rs:347-349`). Probe (all → `ACCEPT`):

- `test_60` — `?o1` bound by a preceding triple in the same group, then
  `BIND(… AS ?o1)`.
- `test_61a` — `?o1` bound inside a *nested* group, then `BIND` in the outer
  group (in-scope propagates out of a nested group).
- `test_62a` — `?Y` bound in a `UNION` branch, then `BIND(1 AS ?Y)`.

Requires an ordered walk of each group's children accumulating in-scope
variables (from BGPs, paths, `VALUES`, prior `BIND`s, and the projected/visible
variables of nested groups / unions / sub-selects per the §18.2.1 in-scope
definition), checking each `BIND` target against the set accumulated *before*
it.

### V6 — duplicate `AS` alias in `SELECT`

Spec §9.1 / `SelectClause` note: an `AS` variable must not be assigned twice
(nor collide with a variable already in scope). Root cause: `parse_select_variables`
(`fluree-db-sparql/src/parse/query/select.rs:87-95`) just collects items; no
uniqueness check, and no validate pass. Probe: `SELECT (1 AS ?X) (1 AS ?X) {}`
→ `ACCEPT`.

---

## 2. Fix design (grouped by production / rule)

### Parser-accept fixes (P*) — additive, low risk

**P1 collections (desugar; no new AST/IR).** Add `parse_collection()` to
`term.rs`, invoked from `parse_subject`/`parse_object` (replacing the two
stubs) and reachable at group level via the existing `is_term_start` routing.
Desugar per spec §4.2.4 using the **existing blank-node machinery**
(`self.bnode_counter`, `self.pending_bnpl_triples`, `parse/query/mod.rs:176-188`):

- `()` / `NIL` → the IRI `rdf:nil` (`fluree_vocab::rdf::NIL`), a plain term; no
  triples.
- `( g1 g2 … gn )` → fresh bnodes `_l1.._ln`; emit `_li rdf:first gi . _li
  rdf:rest _l(i+1) .` and `_ln rdf:rest rdf:nil .` into `pending_bnpl_triples`;
  the collection *term* is `_l1`. Items `gi` are full `GraphNode`s (vars,
  IRIs, literals, blank-node lists → recursion, nested collections →
  recursion), covering `syntax-forms-01/02`.

This adds only ordinary triples over `rdf:first`/`rdf:rest`/`rdf:nil`
(constants already in `fluree-vocab/src/lib.rs:70,73,76`) — **no** new
`GraphPattern`, **no** IR, **no** engine change. `Iri::rdf_type`
(`ast/term.rs:89`) is the pattern to copy for `Iri::rdf_first/rest/nil`
helpers.

**P4 empty IRIREF.** Delete the `result.is_empty()` rejection at
`lexer.rs:199-201` so `<>` lexes as `Iri("")`. Parse+validate then succeed for
the syntax test; the relative-IRI *resolution* needed for the `base-prefix-1`
eval test is a lowering concern (see §3).

**P5a extension-function `NIL` arg list.** In `parse_primary_expr`, change the
three `if tokens.check(&TokenKind::LParen)` guards
(`expr.rs:252, 265, 281`) to also accept `TokenKind::Nil`. `parse_expression_list`
(`expr.rs:769-774`) already consumes `Nil` → empty args. One-token change per
branch.

**P5b bare `Constraint` in `ORDER BY`.** In `parse_order_condition`
(`modifier.rs:239-300`), before the final `return None`, add a
`BuiltInCall | FunctionCall` branch that tries `parse_expression` with the
same `position()`/`restore()` guard `parse_group_condition`
(`modifier.rs:149-164`) uses, wrapping the result as `OrderExpr::Expr`.

**P6 sub-select placement.** In `parse_group_graph_pattern`:
(a) when the loop body's first token is `KwSelect` and nothing has been
accumulated yet, parse the whole group as a `SubSelect` (the grammar's
`'{' SubSelect '}'` alternative) — this covers `test_21`/`test_23`;
(b) after the `{`-detected subquery branch (`pattern.rs:122-131`), mirror the
nested-group branch and check for a trailing `KwUnion`, calling
`parse_union_continuation` — this covers `test_64`.

**P7 `VALUES` `NIL` var list.** In `parse_values_variables`
(`pattern.rs:487-523`) accept a leading `Nil` token as "zero variables"; add a
zero-variable data-block path in `parse_values_pattern` (`pattern.rs:432-482`)
that reads `NIL`/`()` rows (each an empty row) until `}`. Guard the
`multi_var` dispatch (`pattern.rs:438`) for the empty case.

**P8 path in `[ … ]`.** Replace the `Verb::Path(_)` rejection
(`term.rs:453-459`) with a real lowering: emit a `GraphPattern::Path` for the
`[ path obj ]` triple. Because `[ … ]` currently only surfaces
`TriplePattern`s, add a `pending_bnpl_patterns: Vec<GraphPattern>` companion to
`pending_bnpl_triples` and drain it wherever the triples are drained
(`parse_triples_block` `term.rs:522-567`, `parse_object_list`
`term.rs:677-679`, `parse_path_object_list` `term.rs:946-952`). This also
unblocks `test_pp_coll` once P1 lands.

### Parser-tighten fixes (V1, V2) — reject-more, grammar

**V1 dot structure.** Make dots load-bearing:
- In `parse_triples_block` (`term.rs:522-567`), after each
  `TriplesSameSubjectPath`, require a `.` before another same-subject block:
  if the next token `is_term_start()` without an intervening consumed `.`, emit
  a "missing '.'" error (fixes `syn-bad-02/03`). Keep a single trailing `.`
  optional.
- Remove the "skip any dot" branch in `parse_group_graph_pattern`
  (`pattern.rs:162-164`). Per `GroupGraphPatternSub`, a `.` is legal only
  immediately after a `GraphPatternNotTriples`; allow exactly one optional `.`
  there and treat a `.` in any other position (leading, doubled, standalone) as
  an error (fixes `syn-bad-05..14`).

**V2 `FILTER` `Constraint`.** In `parse_filter_pattern`
(`pattern.rs:355-370`), require the next token to start a `Constraint`
(`LParen` → bracketed; or a `BuiltInCall`/`FunctionCall` keyword/IRI) and
reject a bare `Var`/literal/relational expression. Reuse the same
`Constraint` predicate introduced for P5b so the two stay consistent.

### New semantic-validation passes (V3–V6) — reject-more, `validate/mod.rs`

All four are new passes on the parsed AST inside `validate()`
(`validate/mod.rs:72-76`), off the query hot path. Add `DiagCode` variants
(`diag/mod.rs`) and wire them from `validate_select` (and, where applicable,
`validate_construct`/`validate_ask`/`validate_describe` and sub-selects).

- **V3 blank-node scope.** New walk that assigns a BGP scope id per
  `GroupGraphPattern` boundary (`GRAPH`/`OPTIONAL`/`UNION`/`MINUS`/nested
  group/sub-select start a new scope; `FILTER`/`BIND` do not), collects the
  labeled blank nodes appearing in each scope, and errors if any label appears
  in ≥2 scopes. Watch the FILTER/BIND nuance (see §5 risk).
- **V4 projection scope.** In `validate_select`: detect grouping
  (`modifiers.group_by.is_some()` or any aggregate in the projection); build
  the group-key var set; then reject `SELECT *`-with-`GROUP BY`, and reject any
  projected `Var`/expression-free-var that is neither a key nor inside an
  aggregate. Needs an `Expression::contains_aggregate()` predicate and a
  free-variable collector (memory notes a `variables()` helper already exists
  on expressions — reuse it).
- **V5 BIND scope.** Ordered in-scope-variable accumulation per group; reject a
  `BIND` whose target is already in scope. Reuse the §18.2.1 in-scope
  definition; nested groups / unions / sub-selects contribute their visible
  variables.
- **V6 duplicate alias.** Collect `SELECT` `AS` targets; reject a repeat (and,
  per spec, a collision with a WHERE-visible variable — the test only needs the
  repeat case).

---

## 3. Hot-path classification

**The cluster is 100% parse-time / validate-time for the engine.** No shared
runtime (scan/join/filter/aggregate execution) code is touched by any fix. Two
lowering/prepare-time caveats, neither on the per-row path:

1. **P1 collections lower to `rdf:first`/`rdf:rest`/`rdf:nil` BGP triples** —
   ordinary triples the engine already executes. Confirmed the eval side
   matches: Fluree's Turtle ingest *also* desugars collections to the same
   predicates (`fluree-graph-turtle/src/parser.rs:18` `RDF_FIRST`, `:298`
   `rdf_first`, `:308` `rdf_rest`). So the parse-time desugaring is spec-correct
   **and** eval-compatible: it additionally turns the currently-registered eval
   failures `basic#list-1..4` (`SPARQL10_QUERY_EVAL`) and
   `construct#constructlist` (`SPARQL11_CONSTRUCT`) green. **Cross-cluster
   coordination:** those register entries live in other clusters and must be
   removed in the same PR that lands P1, or the both-directions register check
   will flag them as stale passes.

2. **P4 `base-prefix-1` eval** needs relative-IRI resolution against `BASE`
   (`<>` → the base IRI; `:x` → base + `x`) at **lower/prepare time**, not
   per-row. The lexer fix (mine) makes it parse; the resolution overlaps the
   eval cluster's `basic#base-prefix-2/5` (`SPARQL10_QUERY_EVAL`, result
   mismatch). The *syntax* test `syntax-qname-05` needs only the lexer fix
   (parse+validate). Flag base-relative resolution as shared with the eval
   cluster.

No `regression-budget.json` benches (`query_hot_bsbm*`, `insert_formats`,
`import_bulk`) exercise this cluster; the lexer change is SPARQL-query-only (not
the Turtle import lexer), so `import_bulk` is unaffected. No bench guardrail is
required beyond the standard suite run.

---

## 4. JSON-LD parity (per `sparql-compliance.md` §"Query Surface Parity")

Scope note: **Cypher is out of scope** for this burn-down — it is openCypher
(Fluree does not own the grammar and will add no custom syntax); it benefits
implicitly from any IR/engine-level fix and needs no assessment or
support-matrix work. Fluree **does** own the JSON-LD query syntax, so the
"SPARQL-possible ⇒ JSON-LD-possible" rule stands and JSON-LD regression tests
are named per fix below.

Grammar-tightening and validation fixes (V1–V6) **reject** invalid input; they
add no capability. Most (V1, V2, V3, V6) are SPARQL-surface-syntax concerns
with no JSON-LD analogue. Two carry genuine cross-surface semantics:

- **V4 (group/projection scope)** and **V5 (BIND scope)** are semantic rules
  that also apply to JSON-LD *analytical* queries (which share the IR and can
  express `groupBy`/`bind`). Decision to record: implement the check in the
  SPARQL `validate()` pass now (satisfies the W3C cluster), **and** author
  JSON-LD analytical regression tests asserting the same rejection
  (`fluree-db-api/tests/it_query_analytical.rs`,
  `it_query_grouping.rs`) — or, preferably, factor the scope/projection check
  into a shared checker the JSON-LD lowerer also calls. This is the parity
  "definition of done" per the team guideline.

The one capability-adding fix is **P1 collections**:

- It is SPARQL **surface sugar** that desugars to `rdf:first`/`rdf:rest`
  triples the engine already supports — **no new IR capability**. JSON-LD can
  already express the equivalent RDF list via `@list` / explicit
  `rdf:first`-`rdf:rest`; add a JSON-LD regression test
  (`fluree-db-api/tests/it_query.rs`) asserting a `@list`/first-rest pattern
  matches the same data a SPARQL `( … )` pattern matches, to guard the shared
  engine path.

P4/P5/P6/P7/P8 are SPARQL-only surface syntax (relative IRIs, function-call
`NIL`, sub-select placement, `VALUES` `NIL`, path-in-`[ ]`) with no JSON-LD
syntax equivalent and no new engine capability — no parity work, record as
SPARQL-surface-only.

---

## 5. Blast radius, PR composition, risks, open questions

### Files touched (all in `fluree-db-sparql/`, plus register + parity tests)

| Fix | Files | LOC (ballpark) |
|---|---|---|
| P1 | `parse/query/term.rs` (collection parser + drain), `ast/term.rs` (rdf list Iri helpers) | ~120 |
| P4 | `lex/lexer.rs` (1 guard) | ~5 |
| P5a | `parse/expr.rs` (3 guards) | ~6 |
| P5b | `parse/query/modifier.rs` | ~20 |
| P6 | `parse/query/pattern.rs` | ~40 |
| P7 | `parse/query/pattern.rs` | ~40 |
| P8 | `parse/query/{term.rs,mod.rs}` (add `pending_bnpl_patterns`) | ~40 |
| V1 | `parse/query/{pattern.rs,term.rs}` | ~60 |
| V2 | `parse/query/pattern.rs` (+ shared `Constraint` predicate) | ~25 |
| V3 | `validate/mod.rs`, `diag/mod.rs` | ~90 |
| V4 | `validate/mod.rs`, `ast/expr.rs` (aggregate/var helpers), `diag/mod.rs` | ~120 |
| V5 | `validate/mod.rs`, `diag/mod.rs` | ~100 |
| V6 | `validate/mod.rs`, `diag/mod.rs` | ~30 |

Plus register edits (`testsuite-sparql/tests/registers/mod.rs`: remove the
62 entries as they go green, plus the P1 cross-cluster `list-1..4`/`constructlist`
entries) and parity tests (§4).

### Suggested PR composition

This cluster is the first, lowest-risk burn-down PR — but the risk is **not**
uniform, so split it into three landable PRs, safest first:

- **PR-1 "parser accepts valid syntax" (P1, P4, P5a, P5b, P6, P7, P8).**
  Pure accept-more; cannot reject any currently-accepted query. Clears all 25
  positive-rejected tests plus (via P1) the cross-cluster `list-1..4` /
  `constructlist` eval entries. Ships the collection/`@list` and function-`NIL`
  JSON-LD parity tests. Lowest risk; land first.
- **PR-2 "semantic validation" (V3, V4, V5, V6).** New reject-more passes in
  `validate()`. Clears 24 negative tests (V3:11, V4:9, V5:3, V6:1). Ships the
  JSON-LD analytical group/BIND-scope parity tests. Moderate risk (rejects
  previously-accepted invalid queries — see below).
- **PR-3 "dot + FILTER grammar tightening" (V1, V2).** Highest regression
  surface; isolate so a bisect is clean. Clears 13 negative tests (V1:12, V2:1).

If a single PR is mandated, land in that internal order and call out PR-3's
scope explicitly in the description.

### Risks (regression to non-W3C users)

- **V1 (dots) — highest.** Dot handling is central to BGP parsing. Real users
  may rely on Fluree's current leniency (trailing/omitted dots, `. .`). The fix
  changes those from silently-accepted to errors. Must run the full W3C suite +
  `cargo test -p fluree-db-sparql` + the JSON-LD suite, and grep app
  corpora for lenient dot usage before landing.
- **V2 (FILTER)** — Fluree currently accepts `FILTER ?x` and
  `FILTER ?x > 5` (no parens). Tightening rejects them. Legitimate per spec but
  a behavior change; announce it.
- **V3–V6 validation** — each converts a currently-accepted (spec-invalid)
  query into an error: reused blank-node labels, non-grouped projections,
  BIND-over-bound-var, duplicate aliases. These are correctness improvements but
  are still breaking for anyone who leaned on the old behavior (esp. V4:
  `SELECT ?x ?y … GROUP BY ?x` currently returns *something*). Gate behind a
  clear diagnostic + release note; consider whether any should be a *warning*
  under a capability flag rather than a hard error.
- **P4/P5a/P6/P7 (accept-more)** — negligible; they can only turn prior errors
  into successes. One watch-item: P6's "bare `SELECT` = sub-select" must fire
  only when the group is genuinely a sub-select (SELECT is a reserved keyword
  that cannot begin a triple, so ambiguity is nil).
- **P8 structural** — adding `pending_bnpl_patterns` touches the shared
  `[ … ]` drain sites (`term.rs:522-567, 677-679, 946-952`); keep the existing
  triple path byte-identical and only *add* the pattern channel.

### Open questions

1. **V3 BGP boundary + FILTER/BIND.** The correct scope unit is the *BGP*
   (join scope), not each `GraphPattern::Bgp` node — the parser emits a fresh
   `Bgp` node across a `FILTER`/`BIND` in the *same* group, so a naive
   per-`Bgp`-node check would wrongly reject `_:a … FILTER(…) … _:a` (legal,
   one BGP). No failing test exercises this, but the pass must merge
   same-group `Bgp` nodes to avoid a false-positive regression. Confirm the
   intended boundary set (GRAPH/OPTIONAL/UNION/MINUS/nested-group/sub-select
   only).
2. **V4/V5 parity locus.** Implement group/BIND-scope validation in the
   SPARQL-only `validate()` (fastest for the W3C cluster) or in a shared
   IR-level checker the JSON-LD lowerer also invokes (parity across SPARQL +
   JSON-LD)? Recommendation: shared checker if cheap; otherwise SPARQL pass +
   authored JSON-LD analytical regression tests. Needs a call from the team.
3. **P4 base-relative resolution ownership.** The `base-prefix-1` *eval* pass
   depends on relative-IRI-against-`BASE` resolution at lower time, shared with
   the eval cluster's `base-prefix-2/5`. Confirm which PR owns that resolution
   so `base-prefix-1`'s eval-register entry is removed by the right change (the
   *syntax* `syntax-qname-05` is fully mine via the lexer fix).
4. **V4 aggregate detection** needs `Expression::contains_aggregate()` and a
   reliable free-variable collector. Confirm the existing expression
   `variables()` helper covers aggregates/nested function args before building
   on it.
