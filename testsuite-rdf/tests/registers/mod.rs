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

/// RDF 1.1 Turtle — 4 known failures out of 313, all cause C.
///
/// Causes A1, A2, A3, B and D were fixed in the burn-down; only the H-8
/// term-validation work remains.
pub const RDF11_TURTLE: &[&str] = &[
    // A1 (repeated `;` as empty predicateObjectList items) — FIXED, entries
    // removed with the fix.
    // A2 (PN_LOCAL interior dots) — FIXED, entry removed with the fix.
    // A3 (relative @base resolution) — FIXED, entry removed with the fix.
    // B (directive keyword case, both directions) — FIXED, entries removed
    // with the fix.
    // D (boolean keyword vs longhand IR duality) — FIXED, entries removed
    // with the fix.
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

/// RDF 1.1 TriG — 148 known failures out of 356.
///
/// Two groups, and the split is the whole story:
///
/// - **F (147)** — every `TestTrigEval` / `TestTrigNegativeEval`. These
///   compare the parsed dataset against an N-Quads gold file, and no N-Quads
///   reader exists yet. The TriG parser is NOT what is missing: the syntax
///   half of this same suite passes 208/209. They clear as a block when the
///   N-Quads reader lands, with no TriG change expected.
/// - **C (1)** — `trig-syntax-bad-lang-01`, the TriG member of the
///   IRI/language-tag validation gap that also holds 4 Turtle tests and 1
///   N-Triples test. H-8 workstream.
pub const RDF11_TRIG: &[&str] = &[
    // C — language-tag validation (H-8), same cause as its Turtle twin.
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-syntax-bad-lang-01",
    // F — awaiting the N-Quads reader (see above); no TriG defect implied.
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#anonymous_blank_node_graph",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#labeled_blank_node_graph",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#alternating_iri_graphs",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#alternating_bnode_graphs",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#IRI_subject",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#IRI_with_four_digit_numeric_escape",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#IRI_with_eight_digit_numeric_escape",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#IRI_with_all_punctuation",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#bareword_a_predicate",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#old_style_prefix",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#SPARQL_style_prefix",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#prefixed_IRI_predicate",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#prefixed_IRI_object",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#prefix_only_IRI",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#prefix_with_PN_CHARS_BASE_character_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#prefix_with_non_leading_extras",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#default_namespace_IRI",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#prefix_reassigned_and_used",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#reserved_escaped_localName",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#percent_escaped_localName",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#HYPHEN_MINUS_in_localName",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#underscore_in_localName",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#localname_with_COLON",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#localName_with_assigned_nfc_bmp_PN_CHARS_BASE_character_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#localName_with_assigned_nfc_PN_CHARS_BASE_character_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#localName_with_nfc_PN_CHARS_BASE_character_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#localName_with_leading_underscore",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#localName_with_leading_digit",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#localName_with_non_leading_extras",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#old_style_base",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#SPARQL_style_base",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#labeled_blank_node_subject",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#labeled_blank_node_object",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#labeled_blank_node_with_PN_CHARS_BASE_character_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#labeled_blank_node_with_leading_underscore",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#labeled_blank_node_with_leading_digit",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#labeled_blank_node_with_non_leading_extras",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#anonymous_blank_node_subject",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#anonymous_blank_node_object",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#sole_blankNodePropertyList",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#blankNodePropertyList_as_subject",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#blankNodePropertyList_as_object",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#blankNodePropertyList_as_object_containing_objectList",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#blankNodePropertyList_as_object_containing_objectList_of_two_objects",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#blankNodePropertyList_with_multiple_triples",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#nested_blankNodePropertyLists",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#blankNodePropertyList_containing_collection",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#collection_subject",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#collection_object",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#empty_collection",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#nested_collection",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#first",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#last",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL1",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL1_ascii_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL1_with_UTF8_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL1_all_controls",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL1_all_punctuation",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL_LONG1",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL_LONG1_ascii_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL_LONG1_with_UTF8_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL_LONG1_with_1_squote",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL_LONG1_with_2_squotes",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL2",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL2_ascii_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL2_with_UTF8_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL_LONG2",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL_LONG2_ascii_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL_LONG2_with_UTF8_boundaries",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL_LONG2_with_1_squote",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL_LONG2_with_2_squotes",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_CHARACTER_TABULATION",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_BACKSPACE",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_LINE_FEED",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_CARRIAGE_RETURN",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_FORM_FEED",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_REVERSE_SOLIDUS",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_escaped_CHARACTER_TABULATION",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_escaped_BACKSPACE",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_escaped_LINE_FEED",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_escaped_CARRIAGE_RETURN",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_escaped_FORM_FEED",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_numeric_escape4",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_with_numeric_escape8",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#IRIREF_datatype",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#prefixed_name_datatype",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#bareword_integer",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#bareword_decimal",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#bareword_double",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#double_lower_case_e",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#negative_numeric",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#positive_numeric",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#numeric_with_leading_0",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_true",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#literal_false",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#langtagged_non_LONG",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#langtagged_LONG",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#lantag_with_subtag",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#objectList_with_two_objects",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#predicateObjectList_with_two_objectLists",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#predicateObjectList_with_blankNodePropertyList_as_object",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#repeated_semis_at_end",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#repeated_semis_not_at_end",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#comment_following_localName",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#number_sign_following_localName",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#comment_following_PNAME_NS",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#number_sign_following_PNAME_NS",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#LITERAL_LONG2_with_REVERSE_SOLIDUS",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#two_LITERAL_LONG2s",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#langtagged_LONG_with_subtag",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-eval-struct-01",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-eval-struct-02",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-01",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-02",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-03",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-04",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-05",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-06",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-07",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-08",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-09",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-10",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-11",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-12",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-13",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-14",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-15",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-16",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-17",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-18",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-19",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-20",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-21",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-22",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-23",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-24",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-25",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-26",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-subm-27",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-eval-bad-01",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-eval-bad-02",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-eval-bad-03",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#trig-eval-bad-04",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#IRI-resolution-01",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#IRI-resolution-02",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#IRI-resolution-07",
    "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl#IRI-resolution-08",
];
