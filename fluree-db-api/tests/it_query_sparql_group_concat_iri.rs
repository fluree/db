//! `GROUP_CONCAT` over IRI-valued variables.
//!
//! `agg_group_concat` read only `Binding::Lit`, so IRI-valued group members
//! were silently skipped: an all-IRI group concatenated nothing and returned
//! Unbound (rendered `null`), and a mixed IRI/literal group dropped its IRI
//! members from the result without any error. `STR(?s)` on the same term has
//! always produced the full IRI, so the two disagreed.

use crate::support;
use crate::support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// maker1 has two models, maker2 one. `ex:tag` gives maker1 a group whose
/// members are an IRI and a string, for the mixed case.
async fn seed(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:maker1", "@type": "ex:Maker", "ex:name": "Acme"},
            {"@id": "ex:maker2", "@type": "ex:Maker", "ex:name": "Globex"},
            {"@id": "ex:m1", "ex:ofMaker": {"@id": "ex:maker1"}},
            {"@id": "ex:m2", "ex:ofMaker": {"@id": "ex:maker1"}},
            {"@id": "ex:m3", "ex:ofMaker": {"@id": "ex:maker2"}}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

/// A non-key IRI variable: the group's models must concatenate as their IRIs
/// rather than collapsing to null.
#[tokio::test]
async fn sparql_group_concat_over_non_key_iri_var() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "sparql/gc-iri-nonkey:main").await;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?c (GROUP_CONCAT(?m; SEPARATOR="|") AS ?g)
        WHERE { ?m ex:ofMaker ?c } GROUP BY ?c"#;

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("GROUP_CONCAT over an IRI variable")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    // maker1's two models arrive in an unspecified order (SPARQL §18.5.1.7).
    let rows = normalize_rows(&jsonld);
    assert_eq!(rows.len(), 2, "one row per maker: {rows:?}");
    let maker1 = rows
        .iter()
        .find(|r| r[0] == "ex:maker1")
        .expect("maker1 row");
    let g = maker1[1].as_str().expect("maker1 concat is a string");
    assert!(
        g == "http://example.org/m1|http://example.org/m2"
            || g == "http://example.org/m2|http://example.org/m1",
        "expected both model IRIs, got {g:?}"
    );
    let maker2 = rows
        .iter()
        .find(|r| r[0] == "ex:maker2")
        .expect("maker2 row");
    assert_eq!(maker2[1], json!("http://example.org/m3"));
}

/// The grouping key itself, read as an aggregate input. This rides on the
/// GROUP-BY-key copy, so it exercises a different column than the case above.
#[tokio::test]
async fn sparql_group_concat_over_key_iri_var() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "sparql/gc-iri-key:main").await;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?c (GROUP_CONCAT(?c; SEPARATOR="|") AS ?g)
        WHERE { ?m ex:ofMaker ?c } GROUP BY ?c"#;

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("GROUP_CONCAT over the grouping key")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    // Every row in a group repeats the key, so this is order-independent.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            [
                "ex:maker1",
                "http://example.org/maker1|http://example.org/maker1"
            ],
            ["ex:maker2", "http://example.org/maker2"]
        ]))
    );
}

/// A mixed group: the IRI member used to vanish from the result while the
/// string member survived, which is the silent-wrong-answer half of this bug.
#[tokio::test]
async fn sparql_group_concat_over_mixed_iri_and_literal() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "sparql/gc-iri-mixed:main").await;

    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT (GROUP_CONCAT(?v; SEPARATOR="|") AS ?g)
        WHERE { { ex:maker1 ex:name ?v } UNION { ex:m1 ex:ofMaker ?v } }"#;

    let jsonld = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("GROUP_CONCAT over mixed IRI and literal values")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    let rows = normalize_rows(&jsonld);
    let g = rows[0][0].as_str().expect("concat is a string");
    let parts: std::collections::HashSet<&str> = g.split('|').collect();
    assert_eq!(
        parts,
        ["Acme", "http://example.org/maker1"].into_iter().collect(),
        "both members must appear, got {g:?}"
    );
}

/// `GROUP_CONCAT(?s)` and `GROUP_CONCAT(STR(?s))` must agree — the explicit
/// coercion was the workaround, and it is now redundant rather than required.
#[tokio::test]
async fn sparql_group_concat_iri_agrees_with_explicit_str() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "sparql/gc-iri-str:main").await;

    let bare = support::query_sparql(
        &fluree,
        &ledger,
        r#"PREFIX ex: <http://example.org/>
           SELECT ?c (GROUP_CONCAT(?c; SEPARATOR="|") AS ?g)
           WHERE { ?m ex:ofMaker ?c } GROUP BY ?c"#,
    )
    .await
    .expect("bare IRI form")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");

    let via_str = support::query_sparql(
        &fluree,
        &ledger,
        r#"PREFIX ex: <http://example.org/>
           SELECT ?c (GROUP_CONCAT(STR(?c); SEPARATOR="|") AS ?g)
           WHERE { ?m ex:ofMaker ?c } GROUP BY ?c"#,
    )
    .await
    .expect("explicit STR form")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");

    assert_eq!(normalize_rows(&bare), normalize_rows(&via_str));
}
