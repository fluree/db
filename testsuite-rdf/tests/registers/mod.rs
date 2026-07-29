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
//! categories in the workstream report:
//!
//! - **A1** repeated `;` as empty predicateObjectList items (Turtle) — closed
//! - **A2** PN_LOCAL interior dot runs (Turtle) — closed
//! - **A3** relative `@base` resolution (Turtle) — closed
//! - **B**  directive keyword case, both directions (Turtle) — closed
//! - **C**  IRI / language-tag validation — the H-8 workstream — closed for
//!   Turtle and N-Triples; the TriG members are the last live entries
//! - **D**  boolean keyword vs longhand IR duality (Turtle) — closed
//! - **E**  no strict N-Triples reader, so the NT suites ran through the
//!   Turtle parser and a negative test whose document is valid Turtle could
//!   not fail — CLOSED by the strict line reader in
//!   `fluree-graph-turtle::nquads`; N-Triples and N-Quads are both 100%
//! - **F**  TriG eval blocked on an N-Quads reader — closed by that same
//!   reader, with no TriG change
//!
//! Two workstreams closed the last two causes from opposite directions: the
//! strict line reader (M2) closed E and F, and term validation (H-8) closed C.
//! So the live taxonomy is C, with entries in the TriG register only.
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

/// RDF 1.1 Turtle — EMPTY. 313 of 313, in conformant mode.
///
/// Every cause the burn-down opened is closed: A1, A2, A3, B and D during the
/// burn-down itself, and C — term validation — with the H-8 workstream. The
/// register having nothing left in it is the claim; `check_testsuite` polices
/// it in both directions, so a single Turtle regression re-populates this list
/// or fails the suite.
///
/// Read it with the caveat above attached: 100% here is 100% of the RDF 1.1
/// Turtle suite in `ParserOptions::conformant` mode. Ingest defaults score
/// 89.8% BY DESIGN (indexed list items, canonicalized numerics), and the RDF
/// 1.2 suites are informational and far lower.
pub const RDF11_TURTLE: &[&str] = &[
    // A1 (repeated `;` as empty predicateObjectList items) — FIXED, entries
    // removed with the fix.
    // A2 (PN_LOCAL interior dots) — FIXED, entry removed with the fix.
    // A3 (relative @base resolution) — FIXED, entry removed with the fix.
    // B (directive keyword case, both directions) — FIXED, entries removed
    // with the fix.
    // C (IRI / language-tag validation) — FIXED by H-8, entries removed with
    // the fix. The parser now checks that a resolved IRI is an IRI after
    // `\uXXXX` expansion and that a language tag matches the LANGTAG
    // production, under `ParserOptions::validate`.
    // D (boolean keyword vs longhand IR duality) — FIXED, entries removed
    // with the fix.
];

/// RDF 1.1 N-Triples — CLEAN, 70/70.
pub const RDF11_NTRIPLES: &[&str] = &[
    // EMPTY, and it took both workstreams to get here.
    //
    // Cause E is closed: the suites now run through the strict line reader in
    // `fluree-graph-turtle::nquads`, not the Turtle parser, so every negative
    // test whose document is valid Turtle and invalid N-Triples now fails as
    // it should — directives, `,`/`;` lists, relative IRIs, single/triple
    // quoted strings and bare numerics, 14 entries in all.
    //
    // Cause C is closed too. `nt-syntax-bad-lang-01` (`"string"@1`) was a
    // genuine miss rather than a consequence of reading N-Triples with the
    // Turtle parser, and a strict N-Triples reader would NOT have fixed it: it
    // needed the H-8 language-tag predicate, and cleared with its Turtle twin
    // `turtle-syntax-bad-lang-01`. Both readers call the same predicates.
];

/// RDF 1.1 TriG — 4 known failures out of 356, all cause C.
pub const RDF11_TRIG: &[&str] = &[
    // C — IRI / language-tag validation (H-8), the exact four-test shape the
    // Turtle suite has: three ill-formed IRIs behind `\uXXXX` escapes and one
    // bad language tag.
    //
    // Cause F (eval blocked on an N-Quads reader) is closed: the reader landed
    // and the eval family cleared with no TriG change. Precisely — because the
    // earlier wording here overstated it — all 143 POSITIVE `TestTrigEval`
    // tests pass, and 144 of the 147 eval-family tests do. The 3 that remain
    // are `TestTrigNegativeEval`, and they are listed right below as cause C:
    // they are ill-formed-IRI tests, not eval-machinery tests, which is why
    // they did not clear with the reader.
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-syntax-bad-lang-01",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-eval-bad-01",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-eval-bad-02",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-eval-bad-03",
];

/// RDF 1.1 N-Quads — CLEAN, 87/87.
pub const RDF11_NQUADS: &[&str] = &[];
