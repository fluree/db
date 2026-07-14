# Burn-down: Turtle lexer bnode-dot + xsd:double canonical lexical form

Pre-implementation deep audit for audit §4.2 items 5 (Turtle lexer) and 6
(CSV double), plus the §8 finding "CSV output does not use canonical
`xsd:double` lexical form (csv03)". Covers registers `SPARQL11_JSON_RES`
(jsonres01–04) and `SPARQL11_CSV_TSV` (csv03).

**Scope:** two independent, small, spec-correct engine fixes. No source was
modified during this audit; all claims below were verified against the code and
with throwaway probes (winnow 0.7.15 blank-node lexer replica; `rustc` f64
formatting probes). Baseline: branch `test/sparql-testsuite-full-coverage`,
rdf-tests submodule `efccbc6b8`.

---

## 1. Confirmed root causes

### 1a. Turtle lexer — blank-node label immediately followed by `.`

**Defect:** `fluree-graph-turtle/src/lex/lexer.rs:477-490` `parse_blank_node_name`
greedily consumes a trailing `.` into the label, notices the label ends in `.`,
and **returns a hard backtrack error** instead of leaving the dot as the
statement terminator:

```rust
fn parse_blank_node_name<'a>(input: &mut Input<'a>) -> ModalResult<&'a str> {
    let result: &str = (
        take_while(1, |c: char| is_pn_chars_u(c) || c.is_ascii_digit()),
        take_while(0.., |c: char| is_pn_chars(c) || c == '.'),   // <-- eats trailing '.'
    ).take().parse_next(input)?;
    if result.ends_with('.') {
        return Err(winnow::error::ErrMode::Backtrack(ContextError::new())); // <-- gives up
    }
    Ok(result)
}
```

Called from `parse_blank_node_label` (lexer.rs:470-474). When this backtracks,
the top-level `next_token` `alt` (lexer.rs:188-213) has no other arm that can
start with `_`, so the whole lexer fails with
`unexpected character '_'`. That is exactly the reported failure on
`testsuite-sparql/rdf-tests/sparql/sparql11/json-res/data.ttl` line 10
(`:s6 :p6 _:o6.`), which blocks the data load for all four json-res tests.

Per the Turtle grammar, `BLANK_NODE_LABEL ::= '_:' (PN_CHARS_U | [0-9])
((PN_CHARS | '.')* PN_CHARS)?` — interior dots are allowed but the label MUST
NOT end in a dot. So `_:o6.` is the label `o6` followed by the `.` terminator.
The bug is that the lexer treats "label technically ended in a dot" as a lexical
error rather than as "the dot wasn't part of the label."

**Verified (standalone winnow 0.7 replica of the two functions):**

| input after `_:` | current | fixed (below) |
|---|---|---|
| `o6.`  | ERR (→ lexer error) | label `o6`,  remaining `.` |
| `abc.` | ERR | label `abc`, remaining `.` |
| `a.b ` | OK `a.b` | OK `a.b` (interior dot kept) |
| `a.b.c.` | ERR | label `a.b.c`, remaining `.` |
| `b1 ` / `1 ` / `x-y ` | OK (unchanged) | OK (unchanged) |

The correct pattern **already exists twice in the same file**: `parse_pn_local`
(lexer.rs:407-460) and `parse_prefixed_name_or_keyword` (lexer.rs:357-368) both
use a single-char dot-lookahead — consume a `.` only if the next char continues
the name, otherwise stop and leave the dot. That is why `xsd:decimal.` on line 9
of the same data file lexes fine but `_:o6.` on line 10 does not: prefixed-name
local parsing has the lookahead, blank-node parsing does not.

Nearby-lexer sweep (requested): **no other trailing-dot defect.** Numeric
lexing is already correct — `parse_integer` (lexer.rs:749-754) and
`parse_decimal`/`parse_double` (via the `peek`/`starts_with('.')` guards)
leave a trailing `.` as a terminator, so `5.5.` lexes as `Decimal("5.5")` +
`Dot` and `5.` as `Integer(5)` + `Dot`. The blank-node name parser is the sole
place that greedily eats the dot and then errors. The sparql10
syntax-lists/forms/qname failures are **parser/grammar**, not lexer (collections
`( )` and blank-node property-list forms) — owned by the parser-syntax work,
not this cluster.

### 1b. `xsd:double` non-canonical lexical form (csv03)

**Defect:** every RDF-lexical output path renders a finite `f64` with Rust's
`f64::Display` (`d.to_string()`), which is neither the W3C canonical
`xsd:double` form nor even scientific notation:

| site | code |
|---|---|
| `fluree-db-api/src/format/sparql.rs:413-425` `scalar_lexical` (SPARQL-JSON DOM) | `d.to_string()` |
| `fluree-db-api/src/format/sparql.rs:547-559` `format_binding` (SPARQL-JSON) | `d.to_string()` |
| `fluree-db-api/src/format/sparql_xml.rs:365-373` `write_double` (SPARQL-XML) | `d.to_string()` |
| `fluree-db-api/src/format/delimited.rs:538-541` native CSV/TSV | `ryu::Buffer::format(d)` |
| `fluree-graph-ir/src/term.rs:90-101` `LiteralValue::lexical()` | `d.to_string()` (→ Term `Display` term.rs:482, `rdf_xml.rs:169`) |

Rust `Display` produces (probe-verified): `1000000.0 → "1000000"`,
`0.001 → "0.001"`, `1e30 → "1000000000000000000000000000000"`,
`1e-10 → "0.0000000001"`. The W3C canonical `xsd:double` form is
mantissa-in-[1,10) with a mandatory `.` and mandatory uppercase `E` exponent:
`1000000.0 → "1.0E6"`.

**Why csv03 fails but tsv03 passes** (this is the whole mechanism, and it tells
us the fix is engine-side, not harness-side):

`data2.ttl` stores `:s6 :p6 "1.0E6"^^xsd:double`. Fluree parses that to
`FlakeValue::Double(1000000.0)` on ingest (the original lexical string is not
retained), so all output is at the mercy of the formatter.

- **csv03** — the harness formats actual results as SPARQL-JSON, then
  `project_to_csv_space` (`result_format.rs:189-227`) **drops the datatype**
  (CSV is lossy by design). Comparison in `result_comparison.rs:160-173` then
  falls to a **pure lexical string compare** — the numeric-value fallback is
  gated on the datatype still being present, which the CSV projection removed.
  Fluree emits `"1000000"`; the expected `csvtsv03.csv` has `1.0E6`; strings
  differ → fail.
- **tsv03** — TSV is not projected, so the `xsd:double` datatype survives. The
  same comparison hits `numeric_values_equal(...,"double")`
  (result_comparison.rs:243-255) which parses both sides to `f64` and compares
  by value: `1.0e6 == 1000000` numerically → **pass**, regardless of lexical
  form.

So the defect is real and engine-side: Fluree's serialized `xsd:double`
lexical form is wrong (non-canonical). csv03 is simply the only W3C
output-format test whose comparison path is lexical rather than numeric, so
it's the one that surfaces the bug.

---

## 2. Fix design — Turtle lexer (perf-neutral)

Replace the greedy-then-error body of `parse_blank_node_name` with the same
single-char dot-lookahead loop already used by `parse_pn_local`:

```rust
fn parse_blank_node_name<'a>(input: &mut Input<'a>) -> ModalResult<&'a str> {
    (
        // first char: PN_CHARS_U | [0-9]  (exactly one — take_while(1,..) is exactly-1, probe-confirmed)
        take_while(1, |c: char| is_pn_chars_u(c) || c.is_ascii_digit()),
        |input: &mut Input<'a>| -> ModalResult<()> {
            loop {
                let _: &str = take_while(0.., is_pn_chars).parse_next(input)?;
                if input.starts_with('.') {
                    let rest = &input.as_ref()[1..];
                    if rest.chars().next().is_some_and(is_pn_chars) {
                        '.'.parse_next(input)?;   // interior dot: keep going
                        continue;
                    }
                }
                break;                            // trailing/terminator dot: leave it
            }
            Ok(())
        },
    ).take().map(|_| ()).parse_next(input)   // span-only token; the &str name is unused downstream
}
```

**Exact loop/table touched:** only `parse_blank_node_name` (lexer.rs:477-490).
The char-class predicates in `fluree-graph-turtle/src/lex/chars.rs`
(`is_pn_chars`, `is_pn_chars_u`, `is_pn_chars_base`) are **untouched** — they
are already branch-light `matches!` range tables; the fix reuses them verbatim.

**Perf-neutrality argument (no added branching on the common path):**

- The bulk character scan stays a single `take_while(0.., is_pn_chars)`. In fact
  the fix **removes** a comparison from the hot per-char predicate: the current
  code scans `is_pn_chars(c) || c == '.'`; the fix scans `is_pn_chars` alone.
- The fix also **removes** the post-scan `.take()` materialization and the
  `result.ends_with('.')` re-scan of the label.
- It **adds** exactly one `starts_with('.')` branch per label, executed once
  (not per character). For the overwhelmingly common blank node — no interior
  dot, followed by whitespace/`.`/`;`/`,` — the loop body runs exactly once and
  that branch is not taken (or taken once at the terminator and immediately
  breaks). Net instruction count on the common path is flat-to-slightly-lower.
- No backtracking, no regex, no allocation, no extra lookahead beyond one byte.
  Behavior for every currently-valid input is byte-identical; only inputs that
  previously errored (`_:x.`) now lex.
- **Bench guardrails (audit §6/§4.2.5):** blank nodes are frequent in bulk RDF
  import, so run `insert_formats` and `import_bulk` (fluree-db-api/benches) under
  `regression-budget.json`. Expectation: flat or marginal improvement.

**Known residual (out of scope, and pre-existing):** consecutive interior dots
(`_:a..b`) still error, because the single-char lookahead only continues a dot
when the *immediately* following char is `PN_CHARS`. This is the identical
limitation that `parse_pn_local` already has for `ex:a..b`; no failing W3C test
exercises it, and matching the sibling parser's behavior is the
consistency-preserving choice for a perf-first codebase. Flag it in a code
comment; do not expand the lookahead.

---

## 3. Fix design — canonical `xsd:double` lexical form

**Where the canonical mapping lives:** add one shared helper (a
`canonical_xsd_double(d: f64) -> String`, plus a write-into-buffer variant for
the XML/delimited paths). Natural home: `fluree-graph-ir` next to
`LiteralValue` (so `term.rs` can use it too), re-exported for the
`fluree-db-api` formatters. Algorithm (probe-verified against all W3C forms):

```rust
fn canonical_xsd_double(d: f64) -> String {
    if d.is_nan() { return "NaN".into(); }
    if d.is_infinite() { return if d.is_sign_positive() { "INF".into() } else { "-INF".into() }; }
    let s = format!("{:e}", d);                 // Rust shortest round-trip: "1e6","2.2e0","1e-3"
    let (mant, exp) = s.split_once('e').unwrap();
    let mant = if mant.contains('.') { mant.to_string() } else { format!("{mant}.0") };
    format!("{mant}E{exp}")                      // uppercase E, no '+', no leading zeros
}
```

Verified transforms: `1000000.0→1.0E6`, `1.0→1.0E0`, `2.2→2.2E0`, `0.001→1.0E-3`,
`0.0→0.0E0`, `1e30→1.0E30`, `1e-10→1.0E-10`, `123.456→1.23456E2`,
`6.02e23→6.02E23`, `-3.0→-3.0E0`; `NaN/INF/-INF` preserved. It reuses Rust's
built-in shortest-round-trip float formatter (`{:e}`), so it is correct and
allocation-light; the special-value spellings already match what all four sites
emit today.

**Which formats change behavior (route these sites through the helper):**

- `sparql.rs:413-425` and `sparql.rs:547-559` — **SPARQL Results JSON**
  (this is the exact path csv03 exercises, since the harness formats actual
  results via `FormatterConfig::sparql_json()`).
- `sparql_xml.rs:365-373` `write_double` — **SPARQL Results XML**.
- `delimited.rs:538-541` — **native CSV/TSV** (currently `ryu` → `"1000000.0"`,
  also non-canonical; the harness does not hit this path, but real users do, so
  fix it for consistency).
- Optionally `LiteralValue::lexical()` (term.rs) → flows to Term `Display` and
  RDF/XML (`rdf_xml.rs:169`). Correct to canonicalize, but `lexical()` is a
  general-purpose accessor; changing it is a slightly wider blast radius.
  Recommended: canonicalize `lexical()` too (it is only consumed by Display and
  RDF/XML serialization — grep-confirmed it is **not** used in query/scan/storage
  hot paths, indexing, or comparison keys), so all serialized double lexical
  forms are consistent.

**Do NOT touch the JSON-LD path** (`jsonld.rs:357-365` `push_f64`,
`jsonld.rs:506-517` `json!(d)`). JSON-LD emits doubles as **native JSON
numbers**, not lexical strings; forcing a `"1.0E6"` string there would corrupt
JSON-LD number semantics. This is the key parity nuance (see §4).

**User-visible change → changelog required.** SPARQL-JSON, SPARQL-XML, and
CSV/TSV serialization of `xsd:double` values changes from Rust-Display form
(`"1000000"`, `"0.0000000001"`, `"1000000000000000000000000000000"`) to W3C
canonical form (`"1.0E6"`, `"1.0E-10"`, `"1.0E30"`). This is spec-correct and
matches Jena / RDF4J / Oxigraph, but any downstream consumer that string-matches
Fluree's double output must be told. Add a `CHANGELOG`/compatibility note
(fixed-behavior, not breaking-grammar). JSON-LD numeric output is explicitly
unchanged.

---

## 4. Query-surface parity (JSON-LD)

Both fixes are **IR/engine-level**, not surface-syntax additions, so per
`docs/contributing/sparql-compliance.md` §"Query Surface Parity" each needs a
JSON-LD regression test authored alongside (the W3C submodule only guards the
SPARQL surface). Cypher is out of scope: Fluree does not own the openCypher
grammar and adds no custom Cypher syntax, so the burn-down does not assess
Cypher parity — JSON-LD is the only owned non-SPARQL surface here.

**Lexer fix** — this is the shared **Turtle/TriG ingest** path
(`fluree-graph-turtle`), used by data loading for every surface, not a
query-surface feature. Tests:
- Unit (fluree-graph-turtle): `tokenize("_:o6.")` → `[BlankNodeLabel, Dot, Eof]`;
  add `_:a.b` (interior dot kept) and confirm `_:o6.` label span is `_:o6`.
  Co-locate with the existing `test_blank_node` (lexer.rs:927).
- Integration (fluree-db-api): `insert_turtle` of `:s :p _:o6.` round-trips —
  the blank node loads and is queryable. JSON-LD has no `_:x.` surface syntax
  (blank-node-dot is Turtle-specific), so the "JSON-LD equivalent" here is:
  after the Turtle insert, query the same data through the **JSON-LD query
  surface** and confirm the blank-node object is returned. That exercises the
  shared IR/engine on ingested `_:x.`-shaped data.

**Double fix** — the canonical lexical form is a serialization property of the
RDF-oriented output formats. Tests:
- SPARQL surface: `fluree-db-api/tests/it_query_sparql.rs` — SELECT a stored
  `xsd:double` (e.g. `1.0E6`), assert SPARQL-JSON `value == "1.0E6"` and
  SPARQL-XML `<literal>1.0E6</literal>`; add a CSV assertion for `1.0E6`.
- JSON-LD surface (parity, `fluree-db-api/tests/it_query.rs`): assert the SAME
  double comes back as a **native JSON number** `@value` (not the canonical
  string) — i.e. the JSON-LD contract is "same numeric value," and the fix must
  NOT regress it into a string. This test is the guardrail that keeps the
  canonicalization scoped away from `jsonld.rs`.

Existing formatter unit tests that will need their expected strings updated to
canonical form (finite values only; NaN/INF unchanged): `sparql.rs`
`parity_double_special_and_normal` (~981) and
`test_format_binding_double_special_values` (~1121); `sparql_xml.rs`
`double_special_values` (~633); `delimited.rs` `test_tsv_boolean_and_double`
(~798). The `jsonld.rs` double tests (`test_format_binding_double` ~764,
`parity_double_special_and_large` ~890) assert **number** output and must stay
unchanged — they double as the parity guard.

---

## 5. Blast radius, PR recommendation, risks

**Blast radius — lexer:** contained to one function in `fluree-graph-turtle`.
No token-boundary change for any currently-valid input (probe-confirmed);
strictly widens what lexes. Unblocks jsonres01–04 data load. Any user Turtle/
TriG data of the `_:x.` shape now imports. Removes 4 entries from
`SPARQL11_JSON_RES`.

**Blast radius — double:** wider but shallow — 3-4 formatter call sites plus a
handful of existing unit-test expectations. Changes user-visible `xsd:double`
serialization in SPARQL-JSON/XML and CSV/TSV. **Does not regress other W3C
output-format tests:** for srj/srx/tsv comparisons the datatype survives and the
numeric-value fallback already makes them pass regardless of lexical form; after
the fix they pass on the exact-lexical fast path instead (strictly tighter, not
looser). Only csv03 (datatype-stripped, lexical compare) is affected — it goes
green. Removes 1 entry from `SPARQL11_CSV_TSV`.

**Perf:** lexer fix is perf-neutral-to-better on the import hot path (§2); double
fix is output-serialization only (not query/scan/join hot path), a few extra
instructions per double cell, no named hot-path bench covers it. Run
`insert_formats`/`import_bulk` for the lexer; the double change needs no bench
beyond the standard suite.

**PR recommendation:** these are Phase B engine fixes (audit §7: B4 = Turtle
lexer bnode-dot; the double item is the §8 csv03 finding). They are logically
independent of each other and of the parser-syntax gaps (B3), but all three are
small, off-hot-path, and bench-guarded the same way. Recommended packaging:

- **Fold the lexer bnode-dot fix into the parser-syntax PR** (B3+B4 together):
  both are `fluree-graph-*` grammar/lexing corrections, share the
  `insert_formats`/`import_bulk` bench gate, and each drops a small register
  block. Shrinks `SPARQL11_JSON_RES` (−4) in the same change.
- **Keep the double-canonicalization as its own small PR** (or a clearly
  separate commit): it touches the `fluree-db-api` format layer and
  `fluree-graph-ir`, carries a user-visible behavior change + changelog note,
  and its review concern (JSON-LD must stay numeric; downstream double
  consumers) is different from grammar review. Shrinks `SPARQL11_CSV_TSV` (−1).

Each PR must remove the register entries it fixes in the same change (CI's
stale-skip detection enforces this) and add the JSON-LD parity tests from §4.

**Risks / watch-items:**
1. Scoping the double fix away from `jsonld.rs` is load-bearing — a naive
   "canonicalize all double sites" sweep would break JSON-LD number output.
   The parity test in §4 is the guard.
2. If `LiteralValue::lexical()` is canonicalized, re-confirm no
   storage/index/comparison consumer depends on the Display form (grep shows
   only Display + RDF/XML today, but re-verify with `--all-features`).
3. Downstream/external consumers string-matching Fluree's `xsd:double` output
   change — mitigate with the changelog note; behavior is now spec-aligned.
4. The lexer residual (`_:a..b`) is intentionally left erroring to match
   `parse_pn_local`; note it so a future reader doesn't mistake it for an
   oversight.
