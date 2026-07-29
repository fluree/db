//! Per-suite registers of KNOWN failures in the W3C RDF syntax suites.
//!
//! Each entry is a test that currently fails for a understood reason. The
//! suites themselves always run in CI; `check_testsuite` enforces the register
//! in BOTH directions:
//!
//! - a test that fails and is not listed here fails the suite (regression);
//! - a test that passes but IS listed here fails the suite (stale entry —
//!   remove it in the same change that fixes the cause).
//!
//! So the register can only shrink, and the pass rate it produces is honest:
//! registered tests stay in the denominator and out of the numerator (see
//! `report.rs`), which means registering a failure LOWERS the reported rate.
//!
//! LIMITATION, same as its SPARQL sibling: this mechanism cannot catch an
//! *unregistered test that passes for the wrong reason*. One such blind spot
//! is live here and needs manual vigilance — see `NEGATIVE_SYNTAX_BLIND_SPOT`
//! below.
//!
//! Grouping comments name the root cause; the letters match the burn-down
//! categories in the workstream report. There are SEVEN groups, not six —
//! it is easy to count the Turtle causes and forget E, which is the largest
//! single group in the whole register:
//!
//! - **A1** repeated `;` as empty predicateObjectList items (Turtle)
//! - **A2** PN_LOCAL interior dot runs (Turtle)
//! - **A3** relative `@base` resolution (Turtle)
//! - **B**  directive keyword case, both directions (Turtle)
//! - **C**  IRI / language-tag validation — the H-8 workstream (Turtle + NT)
//! - **D**  boolean keyword vs longhand IR duality (Turtle)
//! - **E**  no strict N-Triples reader, so the NT suites run through the
//!   Turtle parser and a negative test whose document is valid Turtle cannot
//!   fail (N-Triples only)
//!
//! E is a product gap rather than a parser bug, which is why it reads as a
//! footnote and then turns out to account for most of the N-Triples column.
//!
//! Baseline: rdf-tests submodule @ efccbc6b8, captured 2026-07-28 against the
//! `feat/graphsink-protocol` base (#1552).

/// A negative-syntax test passes when the parser rejects the document — for
/// ANY reason. A parser that rejected a valid construct would still be scored
/// green on every negative test that happens to contain it. The positive
/// suites are the counterweight (they fail on over-rejection), but the two are
/// disjoint sets of documents, so the guarantee is statistical, not exact.
/// Treat a large negative-suite pass rate next to a low positive-suite one as
/// evidence of over-rejection, never as conformance.
pub const NEGATIVE_SYNTAX_BLIND_SPOT: &str =
    "negative-syntax tests do not check WHY the parser rejected the document";

/// RDF 1.1 Turtle — 16 known failures out of 313.
pub const RDF11_TURTLE: &[&str] = &[
    // A1 (repeated `;` as empty predicateObjectList items) — FIXED, entries
    // removed with the fix.
    // A2 (PN_LOCAL interior dots) — FIXED, entry removed with the fix.
    // A3 (relative @base resolution) — FIXED, entry removed with the fix.
    // ---------------------------------------------------------------------
    // B. PARSER BUG — directive keyword case. Turtle has two directive
    // spellings with DIFFERENT case rules: `@base`/`@prefix` are lowercase
    // only, while the SPARQL-style `BASE`/`PREFIX` (no `@`) are
    // case-insensitive. The parser has it backwards in both directions: it
    // rejects `base <...>` and `PreFIX : <...>` (valid), and accepts
    // `@BASE <...>` (invalid). One localized fix clears all three. (3)
    // ---------------------------------------------------------------------
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#turtle-syntax-base-04",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#turtle-syntax-prefix-02",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#turtle-syntax-bad-base-02",
    // ---------------------------------------------------------------------
    // C. TERM VALIDATION NOT IMPLEMENTED — the H-8 workstream ("IRI
    // validation ships in the light crates"), not yet landed. The parser
    // accepts IRIs that are ill-formed after `\uXXXX` expansion (a space,
    // a `<`, a `>`) and language tags that are not `[a-zA-Z]+('-'...)`.
    // These are precisely the `TestTurtleNegativeEval` class: the document
    // lexes, but the terms it denotes are not RDF terms. Expect this group
    // to clear wholesale when H-8 lands, together with the N-Triples
    // language-tag entry. (4)
    // ---------------------------------------------------------------------
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#turtle-eval-bad-01",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#turtle-eval-bad-02",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#turtle-eval-bad-03",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#turtle-syntax-bad-lang-01",
    // ---------------------------------------------------------------------
    // D. IR LIMITATION — one RDF term, two unequal IR representations. The
    // Turtle keywords `true`/`false` still become `LiteralValue::Boolean`,
    // while `"true"^^xsd:boolean` (the same term, written longhand, and what
    // every gold `.nt` file uses) becomes `LiteralValue::String`. The IR's
    // `PartialEq`/`Hash` are variant-sensitive (`term.rs` `_ => false`), so
    // the two compare unequal and hash apart even though `Display` renders
    // both as `"true"^^<...#boolean>`.
    //
    // This is the boolean lane of exactly what `NumericStyle::PreserveLexical`
    // fixed for integers and doubles — the conformant preset simply does not
    // reach the `KwTrue`/`KwFalse` arms of `parse_literal`. It is NOT only a
    // harness concern: a document stating a fact both ways denotes ONE
    // triple, and today's IR would hold two, so graph dedup and set semantics
    // are wrong wherever a boolean is written longhand.
    //
    // Deliberately NOT papered over by normalizing inside the isomorphism
    // checker — that would hide a live defect behind a green suite. (3)
    // ---------------------------------------------------------------------
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#literal_true",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#literal_false",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl#turtle-subm-22",
];

/// RDF 1.1 N-Triples — 15 known failures out of 70.
///
/// Every entry is a NEGATIVE syntax test, and 14 of the 15 share one cause:
/// there is no N-Triples parser. N-Triples is a syntactic subset of Turtle, so
/// the suite runs through the Turtle parser; positive tests are therefore
/// meaningful, but a negative test whose document is *valid Turtle* cannot
/// fail. Read the 78.6% with that attached.
pub const RDF11_NTRIPLES: &[&str] = &[
    // ---------------------------------------------------------------------
    // E. HARNESS/PRODUCT GAP — no strict N-Triples mode. Each document below
    // is invalid N-Triples and valid Turtle, so the Turtle parser accepts it
    // as specified. Closing these needs an N-Triples reader (or a strict
    // profile on the shared one) that refuses: directives, `,`/`;` lists,
    // relative IRIs (N-Triples has no base), single/triple-quoted strings,
    // and bare numeric literals. That reader is M1 scope — `fluree rdf
    // convert` must read N-Triples natively for the parallel path anyway.
    // Until then these are unenforceable, not broken. (14)
    // ---------------------------------------------------------------------
    // relative IRIs (subject, predicate, object, datatype)
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-uri-06",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-uri-07",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-uri-08",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-uri-09",
    // Turtle directives
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-prefix-01",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-base-01",
    // object list (`,`)
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-struct-01",
    // Turtle-only literal spellings: decimal, double, ''' and """ strings
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-string-02",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-string-03",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-string-04",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-string-05",
    // bare integers
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-num-01",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-num-02",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-num-03",
    // ---------------------------------------------------------------------
    // C. TERM VALIDATION NOT IMPLEMENTED — the N-Triples half of the H-8
    // language-tag gap; `"string"@1` is invalid in both grammars, so unlike
    // the entries above this one is a genuine miss that a strict N-Triples
    // reader would NOT fix. Clears with its Turtle twin
    // (`turtle-syntax-bad-lang-01`). (1)
    // ---------------------------------------------------------------------
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl#nt-syntax-bad-lang-01",
];
