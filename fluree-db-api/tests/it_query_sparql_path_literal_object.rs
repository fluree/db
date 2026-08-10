//! Property paths whose object is a literal.
//!
//! SPARQL 1.1 §19.8 reaches a path's object through `GraphNodePath`, which
//! admits `RDFLiteral` / `NumericLiteral` / `BooleanLiteral`, so
//! `?s ex:ofMaker/ex:name "Acme"` is an ordinary matchable shape. Lowering used
//! to force every path object through `Ref`, rejecting all of them with
//! `err:db/InvalidQuery`.
//!
//! These are execution tests on purpose. The W3C syntax suite only parses
//! (`evaluate_positive_syntax_test` never calls `lower_sparql`), so it is
//! structurally unable to see a lowering-time rejection — `syn-pp-in-collection`
//! was green the whole time this was broken.

use crate::support;
use crate::support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// 2 makers, 3 models (maker1 has 2, maker2 has 1). `ex:alias` gives the
/// alternative-path tests a second branch that also ends at `"Acme"`.
async fn seed_makers(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:maker1", "@type": "ex:Maker", "ex:name": "Acme", "ex:foundedYear": 1990},
            {"@id": "ex:maker2", "@type": "ex:Maker", "ex:name": "Globex", "ex:alias": "Acme"},
            {"@id": "ex:m1", "@type": "ex:Model", "ex:ofMaker": {"@id": "ex:maker1"}},
            {"@id": "ex:m2", "@type": "ex:Model", "ex:ofMaker": {"@id": "ex:maker1"}},
            {"@id": "ex:m3", "@type": "ex:Model", "ex:ofMaker": {"@id": "ex:maker2"}}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

/// The headline case: a sequence path ending at a string literal.
#[tokio::test]
async fn sparql_path_sequence_string_literal_object() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/path-lit-seq:main").await;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s ex:ofMaker/ex:name "Acme" }"#;

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("a sequence path with a literal object is legal SPARQL 1.1")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:m1"], ["ex:m2"]]))
    );
}

/// Same shape with a numeric literal — the W3C `syn-pp-in-collection` form
/// (`:p*/:q 123`) puts an integer in exactly this position.
#[tokio::test]
async fn sparql_path_sequence_numeric_literal_object() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/path-lit-num:main").await;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s ex:ofMaker/ex:foundedYear 1990 }";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("a numeric literal is a legal path object")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:m1"], ["ex:m2"]]))
    );
}

/// A transitive *leading* step still works when the final hop carries the
/// literal — only the path's own endpoint has to be a `Ref`.
#[tokio::test]
async fn sparql_path_transitive_step_then_literal_object() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/path-lit-star-seq:main").await;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s ex:ofMaker*/ex:name "Acme" }"#;

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("transitive first hop, literal final hop")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    // maker1 via the zero-length prefix, m1/m2 via one ex:ofMaker hop.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:m1"], ["ex:m2"], ["ex:maker1"]]))
    );
}

/// Both branches of an alternative feed the literal into the object slot.
#[tokio::test]
async fn sparql_path_alternative_literal_object() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/path-lit-alt:main").await;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s ex:name|ex:alias "Acme" }"#;

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("an alternative path with a literal object is legal")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["ex:maker1"], ["ex:maker2"]]))
    );
}

/// `^p` with a literal object puts the literal in *subject* position. RDF has
/// no literal subjects, so the answer is the empty solution sequence — not an
/// error. This is the `[ ^:r "hello" ]` half of `syn-pp-in-collection`.
#[tokio::test]
async fn sparql_path_inverse_literal_object_matches_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/path-lit-inv:main").await;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s ^ex:name "Acme" }"#;

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("inverse-onto-literal is zero solutions, not an error")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(normalize_rows(&jsonld), Vec::<serde_json::Value>::new());
}

/// The same statically-empty lowering with a constant subject, where the arm
/// contributes no variables at all.
#[tokio::test]
async fn sparql_path_inverse_literal_object_constant_subject() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/path-lit-inv-const:main").await;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?n WHERE { ex:maker1 ex:name ?n . ex:maker1 ^ex:name "Acme" }"#;

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("inverse-onto-literal is zero solutions, not an error")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(normalize_rows(&jsonld), Vec::<serde_json::Value>::new());
}

/// A sequence whose *final* hop is an inverse onto a literal: same
/// zero-solutions outcome, reached through the sequence-chain arm.
#[tokio::test]
async fn sparql_path_sequence_trailing_inverse_literal_matches_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/path-lit-seq-inv:main").await;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s ex:ofMaker/^ex:name "Acme" }"#;

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("trailing inverse onto a literal is zero solutions, not an error")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(normalize_rows(&jsonld), Vec::<serde_json::Value>::new());
}

/// W3C `syn-pp-in-collection` (`sparql11/syntax-query`, `mf:PositiveSyntaxTest11`)
/// EXECUTED rather than merely parsed. It combines both halves: `:p*/:q 123`
/// (sequence ending at a numeric literal) and `^:r "hello"` (inverse onto a
/// literal). Nothing in the ledger matches, so the contract is 200/empty.
#[tokio::test]
async fn sparql_path_in_collection_executes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/path-lit-coll:main").await;

    let query = "PREFIX : <http://example.org/>\n\
        SELECT * WHERE {\n\
        \t?s ?p ( [:p*/:q 123 ] [ ^:r \"hello\"] )\n\
        }";

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("syn-pp-in-collection must execute, not just parse")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(normalize_rows(&jsonld), Vec::<serde_json::Value>::new());
}

/// Interim divergence, pinned deliberately: the path IR's own endpoints are
/// typed `Ref`, so a transitive path cannot end at a literal. `?s ex:p+ "lit"`
/// and `?s ex:p* "lit"` are both evaluable under SPARQL 1.1 (the latter via the
/// zero-length path, which binds `?s` to the literal), so this must fail with a
/// message that says the limitation is ours — not with the old blanket
/// "Property path object cannot be a literal value".
#[tokio::test]
async fn sparql_path_transitive_literal_object_narrow_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/path-lit-transitive:main").await;

    for path in ["ex:name+", "ex:name*", "ex:name?", "^ex:name+"] {
        let query = format!(
            r#"PREFIX ex: <http://example.org/>
               SELECT ?s WHERE {{ ?s {path} "Acme" }}"#
        );
        let err = support::query_sparql(&fluree, &ledger, &query)
            .await
            .err()
            .unwrap_or_else(|| panic!("{path} onto a literal is still unsupported"))
            .to_string();
        assert!(
            err.contains("Transitive property paths") && err.contains("not yet supported"),
            "{path} should name the unsupported sub-case, got: {err}"
        );
    }
}

/// Same divergence inside a sequence step, which reaches the limitation through
/// a different arm.
#[tokio::test]
async fn sparql_path_sequence_transitive_tail_literal_narrow_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/path-lit-seq-transitive:main").await;

    for path in ["ex:ofMaker/ex:name+", "ex:ofMaker/^ex:name+"] {
        let query = format!(
            r#"PREFIX ex: <http://example.org/>
               SELECT ?s WHERE {{ ?s {path} "Acme" }}"#
        );
        let err = support::query_sparql(&fluree, &ledger, &query)
            .await
            .err()
            .unwrap_or_else(|| panic!("{path} onto a literal is still unsupported"))
            .to_string();
        assert!(
            err.contains("Transitive property paths") && err.contains("not yet supported"),
            "{path} should name the unsupported sub-case, got: {err}"
        );
    }
}

/// Negated property sets split into a forward and an inverse branch, so their
/// endpoint must be a `Ref` too. Also a deliberate divergence, also narrow.
#[tokio::test]
async fn sparql_path_negated_set_literal_object_narrow_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/path-lit-nps:main").await;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s !(ex:alias) "Acme" }"#;

    let err = support::query_sparql(&fluree, &ledger, query)
        .await
        .err()
        .expect("negated set onto a literal is still unsupported")
        .to_string();
    assert!(
        err.contains("Negated property sets") && err.contains("not yet supported"),
        "should name the unsupported sub-case, got: {err}"
    );
}

/// A path with a literal object must agree with the hand-written triple chain
/// it is shorthand for — including how loosely the literal's datatype is
/// matched.
#[tokio::test]
async fn sparql_path_literal_object_agrees_with_triple_chain() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_makers(&fluree, "sparql/path-lit-parity:main").await;

    let via_path = support::query_sparql(
        &fluree,
        &ledger,
        r#"PREFIX ex: <http://example.org/>
           SELECT ?s WHERE { ?s ex:ofMaker/ex:name "Acme" }"#,
    )
    .await
    .expect("path form")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");

    let via_triples = support::query_sparql(
        &fluree,
        &ledger,
        r#"PREFIX ex: <http://example.org/>
           SELECT ?s WHERE { ?s ex:ofMaker ?mk . ?mk ex:name "Acme" }"#,
    )
    .await
    .expect("triple form")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");

    assert_eq!(normalize_rows(&via_path), normalize_rows(&via_triples));
}
