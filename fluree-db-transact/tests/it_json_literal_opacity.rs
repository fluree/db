//! `@json` literals are opaque documents, for every walker in
//! `parse/edge_annotations.rs`.
//!
//! JSON-LD 1.1 defines the `@value` of a `@json` literal as a document carried
//! verbatim — the transactor serializes the whole thing into one `rdf:JSON`
//! flake value. Nothing inside it is a keyword this parser owns. Four walkers
//! in that module read into documents, and the rule they need is a property of
//! the document, not of any one of them: the first fix for this guarded a
//! single call site, and the other three kept reading.
//!
//! These run at the lowering entry point, so each shape is one assertion
//! rather than a ledger round-trip. The end-to-end contract (a stored rule is
//! storable, and deletable afterwards) is pinned in
//! `fluree-db-api/tests/it_datalog_rules_annotations.rs`.

use fluree_db_transact::parse::edge_annotations::{
    document_has_annotation_keys, lower_edge_annotations,
};
use serde_json::{json, Value};

/// A rule body that reads claims: `@annotation` appears inside it, and it is
/// the documented shape for a stored datalog rule.
fn rule_payload() -> Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "where": [
            {"@id": "?a", "ex:knows": {"@id": "?b", "@annotation": {"ex:confidence": "?conf"}}},
            ["filter", "(> ?conf 0.85)"]
        ],
        "insert": {"@id": "?a", "ex:trustedKnows": {"@id": "?b"}}
    })
}

fn wrapper(type_key: &str, type_val: &str) -> Value {
    json!({ type_key: type_val, "@value": rule_payload() })
}

fn lower(mut doc: Value) -> Result<(), String> {
    lower_edge_annotations(&mut doc).map_err(|e| e.to_string())
}

/// The same opaque payload in the five positions a document can put it in.
/// A fix sited at one walker's call site covers some of these and not others;
/// which ones depends on which call site, which is the point.
fn five_positions(type_key: &str, type_val: &str, ctx: Value) -> Vec<(&'static str, Value)> {
    let w = || wrapper(type_key, type_val);
    vec![
        (
            "direct",
            json!({"@context": ctx, "@id": "ex:r", "ex:rule": w()}),
        ),
        (
            "nested",
            json!({"@context": ctx, "@id": "ex:r", "ex:inner": {"@id": "ex:i", "ex:rule": w()}}),
        ),
        (
            "array",
            json!({"@context": ctx, "@id": "ex:r", "ex:rule": [w()]}),
        ),
        (
            "@list",
            json!({"@context": ctx, "@id": "ex:r", "ex:rule": {"@list": [w()]}}),
        ),
        (
            "annotation-body",
            json!({"@context": ctx, "@id": "ex:a",
                   "ex:knows": {"@id": "ex:b",
                                "@annotation": {"@id": "ex:c", "ex:rule": w()}}}),
        ),
    ]
}

fn assert_all_accepted(label: &str, cases: Vec<(&'static str, Value)>) {
    let refused: Vec<String> = cases
        .into_iter()
        .filter_map(|(name, doc)| lower(doc).err().map(|e| format!("{name}: {e}")))
        .collect();
    assert!(
        refused.is_empty(),
        "{label}: the payload is opaque in every position, but these were refused:\n  {}",
        refused.join("\n  ")
    );
}

#[test]
fn an_opaque_payload_is_opaque_in_every_position() {
    assert_all_accepted(
        "@type: @json",
        five_positions("@type", "@json", json!({"ex": "http://example.org/"})),
    );
}

#[test]
fn every_deferred_keyword_is_opaque_inside_a_payload_not_just_annotation() {
    // The skip is on the `@value` key, so it is keyword-agnostic by
    // construction — but "by construction" is what the first fix also
    // claimed. `@reifies` and `@edge` reach the same refusals as
    // `@annotation`, and a stored rule body is free to contain any of them.
    for keyword in ["@annotation", "@edge", "@reifies"] {
        let doc = json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:r",
            "ex:rule": {"@type": "@json", "@value": {
                "where": [{"@id": "?a", "ex:knows": {"@id": "?b", keyword: {"@id": "ex:x"}}}]
            }}
        });
        lower(doc).unwrap_or_else(|e| panic!("{keyword} inside an opaque payload: {e}"));
    }
}

#[test]
fn a_context_aliased_type_key_still_names_a_json_literal() {
    // `{"type": "@type"}` is among the most ordinary things a JSON-LD context
    // does. The expander resolves the key through the context before deciding
    // the literal is `@json`; a predicate that compares the raw key `"@type"`
    // disagrees with it on exactly this document.
    assert_all_accepted(
        "aliased type key",
        five_positions(
            "type",
            "@json",
            json!({"ex": "http://example.org/", "type": "@type"}),
        ),
    );
}

#[test]
fn a_datatype_iri_that_expands_to_rdf_json_still_names_a_json_literal() {
    // The transactor's own literal parser accepts `@json` OR any IRI that
    // expands to `rdf:JSON`. Matching only the keyword leaves the compact and
    // absolute spellings behind.
    assert_all_accepted(
        "rdf:JSON datatype",
        five_positions(
            "@type",
            "rdf:JSON",
            json!({"ex": "http://example.org/",
                   "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}),
        ),
    );
}

#[test]
fn a_deferred_keyword_in_a_non_value_sibling_is_still_refused() {
    // Only `@value` is skipped. A sibling of the payload is ordinary JSON-LD
    // the parser owns, so a deferred keyword nested in one is still deferred.
    // This is what makes the skip safe to put inside the scanner and delete
    // from the call site: the scanner distinguishes the payload from its
    // siblings, where a wholesale "skip this wrapper" does not.
    let doc = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:r",
        "ex:rule": {
            "@type": "@json",
            "@value": {"anything": 1},
            "ex:sibling": {"ex:inner": {"@reifies": {"@id": "ex:e"}}}
        }
    });
    let err = lower(doc).expect_err("a nested @reifies beside the payload must stay refused");
    assert!(
        err.contains("@reifies"),
        "must name the keyword found: {err}"
    );
}

#[test]
fn an_annotation_key_on_a_wrapper_is_still_refused() {
    // The scanner checks the map's own keys before it skips the payload, so an
    // `@annotation` **on** a wrapper is caught. These three shapes reach the
    // wrapper without passing through `intercept_annotations_for_predicate`
    // (which strips the supported literal-annotation form first), so the check
    // is live rather than vestigial — the reason the scanner takes the map
    // rather than skipping wrappers wholesale.
    //
    // The `@json` rows matter on their own: the first attempt at this fix
    // scanned only the *values* of a `@json` wrapper's non-`@value` keys and
    // never the keys themselves, so it accepted these three where `main`
    // refuses them. Making the payload opaque must not widen what the wrapper
    // around it may be.
    for (name, doc) in [
        (
            "clause value",
            json!({"insert": {"@value": 1, "@annotation": {"ex:c": 1}}}),
        ),
        (
            "envelope item",
            json!({"@graph": [{"@value": 1, "@annotation": {"ex:c": 1}}]}),
        ),
        (
            "whole document",
            json!({"@value": 1, "@annotation": {"ex:c": 1}}),
        ),
        (
            "clause value, @json",
            json!({"insert": {"@type": "@json", "@value": 1, "@annotation": {"ex:c": 1}}}),
        ),
        (
            "envelope item, @json",
            json!({"@graph": [{"@type": "@json", "@value": 1, "@annotation": {"ex:c": 1}}]}),
        ),
        (
            "whole document, @json",
            json!({"@type": "@json", "@value": 1, "@annotation": {"ex:c": 1}}),
        ),
    ] {
        let err = lower(doc).expect_err(&format!(
            "{name}: an @annotation on a wrapper must be refused"
        ));
        assert!(
            err.contains("@annotation"),
            "{name}: refusal must name the key it found: {err}"
        );
    }
}

#[test]
fn the_firewall_does_not_read_into_an_opaque_payload() {
    // Walker 1 rejects user-authored `f:reifies*` IRIs before lowering runs.
    // Inside a `@json` document there is nothing to guard: the payload is
    // serialized whole into one literal and never becomes triples. Both of
    // these are refused without the gate, and neither refusal is about
    // anything the user did wrong.
    let system_predicate_in_payload = json!({
        "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
        "@id": "ex:r",
        "ex:rule": {"@type": "@json", "@value": {"f:reifiesSubject": "whatever"}}
    });
    lower(system_predicate_in_payload)
        .expect("an f:reifies* key inside an opaque payload is just JSON");

    let application_context_in_payload = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:r",
        "ex:rule": {"@type": "@json", "@value": {"@context": 42, "k": 1}}
    });
    lower(application_context_in_payload)
        .expect("a payload key named @context is not this parser's @context");
}

#[test]
fn the_lowering_gate_is_not_opened_by_a_keyword_inside_an_opaque_payload() {
    // Walker 2 decides whether the mutating lowering passes run at all. A
    // keyword that exists only inside a `@json` document is not a block those
    // passes would rewrite, so counting it buys a document clone and two walks
    // that find nothing.
    let opaque_only = json!({
        "@id": "ex:r",
        "ex:rule": {"@type": "@json", "@value": {"@annotation": {"ex:c": 1}}}
    });
    assert!(
        !document_has_annotation_keys(&opaque_only),
        "a keyword inside an opaque payload must not open the lowering pass"
    );

    let opaque_plus_real = json!({
        "@id": "ex:r",
        "ex:rule": {"@type": "@json", "@value": {"@annotation": {"ex:c": 1}}},
        "ex:knows": {"@id": "ex:b", "@annotation": {"ex:confidence": 0.9}}
    });
    assert!(
        document_has_annotation_keys(&opaque_plus_real),
        "a real annotation beside an opaque payload must still open the pass"
    );
}
