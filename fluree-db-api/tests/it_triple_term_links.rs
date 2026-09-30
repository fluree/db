//! The RDF 1.2 link form on the bulk-import path.
//!
//! Alongside the `f:reifies*` bundle, import writes one `rdf:reifies` flake
//! per reifier whose object is a triple-term handle, and persists the term
//! dictionary that gives the handle its meaning. Reading the link back
//! through a query exercises the whole chain: sink → chunk term table →
//! global remap and interning → dictionary upload → root section → store
//! load → handle decode.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState};
use serde_json::{json, Value as JsonValue};

const CLAIMS: &str = r#"VERSION "1.2"
@prefix ex: <http://example.org/> .

ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 ; ex:source ex:hr |} .
ex:alice ex:knows ex:carol {| ex:source ex:linkedin |} .
<< ex:bob ex:knows ex:dave >> ex:source ex:crm .
ex:carol ex:age 42 ~ ex:claim2 .
"#;

async fn import(files: &[(&str, &str)], alias: &str) -> (fluree_db_api::Fluree, LedgerState) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    for (name, content) in files {
        std::fs::write(data_dir.path().join(name), content).expect("write fixture");
    }
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    fluree
        .create(alias)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import of Turtle-star must succeed");
    let ledger = fluree.ledger(alias).await.expect("load ledger");
    std::mem::forget(db_dir);
    std::mem::forget(data_dir);
    (fluree, ledger)
}

fn rows(result: &JsonValue) -> Vec<Vec<String>> {
    result
        .as_array()
        .expect("row array")
        .iter()
        .map(|row| {
            row.as_array()
                .expect("row")
                .iter()
                .map(|cell| match cell {
                    JsonValue::String(s) => s.clone(),
                    JsonValue::Number(n) => n.to_string(),
                    other => other
                        .get("@id")
                        .or_else(|| other.get("@value"))
                        .map(|v| match v {
                            JsonValue::String(s) => s.clone(),
                            v => v.to_string(),
                        })
                        .unwrap_or_else(|| other.to_string()),
                })
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn imported_reifiers_carry_a_decodable_triple_term_link() {
    let (fluree, ledger) = import(&[("claims.ttl", CLAIMS)], "it/triple-term-links:claims").await;

    // One link per reifier, and each link's object decodes through the term
    // dictionary to the base edge it reifies.
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
                  SELECT ?r ?t WHERE { ?r rdf:reifies ?t } ORDER BY ?r";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("link query over an imported ledger");
    let got = rows(&result);
    assert_eq!(got.len(), 4, "{got:#?}");

    let term_of = |reifier: &str| -> String {
        got.iter()
            .find(|r| r[0].ends_with(reifier))
            .unwrap_or_else(|| panic!("no link for {reifier}: {got:#?}"))[1]
            .clone()
    };
    let t1 = term_of("claim1");
    assert!(
        t1.contains("alice") && t1.contains("knows") && t1.contains("bob"),
        "claim1 must reify alice knows bob: {t1}"
    );
    let t2 = term_of("claim2");
    assert!(
        t2.contains("carol") && t2.contains("age") && t2.contains("42"),
        "claim2 must reify carol age 42 (literal object): {t2}"
    );

    // While the link form is index-internal, wildcard scans keep hiding it
    // exactly as they hide the `f:reifies*` bundle.
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  SELECT ?p WHERE { ex:claim1 ?p ?o } ORDER BY ?p";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("wildcard predicate scan");
    let preds: Vec<String> = rows(&result).into_iter().map(|r| r[0].clone()).collect();
    assert!(
        !preds.iter().any(|p| p.contains("reifies")),
        "wildcard scan must hide rdf:reifies and f:reifies*: {preds:?}"
    );
    assert!(
        preds.iter().any(|p| p.ends_with("confidence")),
        "the claim body stays visible: {preds:?}"
    );
}

/// A full rebuild from commits re-synthesizes the links and re-interns the
/// dictionary: the commits carry only the `f:reifies*` bundles, so this is
/// the resolver-side assembler, not the import-side term table, at work.
#[tokio::test]
async fn reindex_rebuilds_the_links_and_the_dictionary() {
    let alias = "it/triple-term-links:reindex";
    let (fluree, _ledger) = import(&[("claims.ttl", CLAIMS)], alias).await;
    fluree
        .reindex(alias, fluree_db_api::ReindexOptions::default())
        .await
        .expect("reindex of an imported annotated ledger");
    let ledger = fluree.ledger(alias).await.expect("reload after reindex");

    let sparql = "PREFIX ex: <http://example.org/>\n\
                  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
                  SELECT ?r ?t WHERE { ?r rdf:reifies ?t } ORDER BY ?r";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("link query after reindex");
    let got = rows(&result);
    assert_eq!(got.len(), 4, "{got:#?}");
    let t1 = &got
        .iter()
        .find(|r| r[0].ends_with("claim1"))
        .unwrap_or_else(|| panic!("no link for claim1 after reindex: {got:#?}"))[1];
    assert!(
        t1.contains("alice") && t1.contains("knows") && t1.contains("bob"),
        "claim1 must still reify alice knows bob: {t1}"
    );
}

async fn links(fluree: &fluree_db_api::Fluree, ledger: &LedgerState) -> Vec<Vec<String>> {
    let sparql = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
                  SELECT ?r ?t WHERE { ?r rdf:reifies ?t } ORDER BY ?r";
    let result = support::query_sparql_formatted(fluree, ledger, sparql)
        .await
        .expect("link query");
    rows(&result)
}

/// The incremental path. The first index of the ledger interns its reifier;
/// the next window adds a second reifier on the same edge, whose handle must
/// come from the base dictionary, and a reifier on a new edge, whose handle
/// is allocated above the base watermark. Both are answered from the index
/// after the incremental build appends to the dictionary.
#[tokio::test]
async fn incremental_index_appends_new_terms_and_reuses_existing_handles() {
    use fluree_db_indexer::IndexerConfig;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/triple-term-links:incremental";
    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());
    let ctx = json!({ "ex": "http://example.org/" });

    local
        .run_until(async {
            let ledger0 = support::genesis_ledger(&fluree, ledger_id);
            let first = fluree
                .insert(
                    ledger0,
                    &json!({
                        "@context": ctx,
                        "@id": "ex:alice",
                        "ex:knows": {
                            "@id": "ex:bob",
                            "@annotation": { "@id": "ex:claim1", "ex:source": { "@id": "ex:hr" } }
                        }
                    }),
                )
                .await
                .expect("first annotated insert");
            support::trigger_index_and_wait(&handle, ledger_id, first.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, first.receipt.t).await;
            let ledger1 = fluree.ledger(ledger_id).await.expect("reload after first index");
            let got = links(&fluree, &ledger1).await;
            assert_eq!(got.len(), 1, "first index must intern the first reifier: {got:#?}");

            let second = fluree
                .insert(
                    ledger1,
                    &json!({
                        "@context": ctx,
                        "@graph": [
                            {
                                "@id": "ex:alice",
                                "ex:knows": {
                                    "@id": "ex:bob",
                                    "@annotation": { "@id": "ex:claim2", "ex:source": { "@id": "ex:crm" } }
                                }
                            },
                            {
                                "@id": "ex:carol",
                                "ex:knows": {
                                    "@id": "ex:dave",
                                    "@annotation": { "@id": "ex:claim3", "ex:source": { "@id": "ex:hr" } }
                                }
                            }
                        ]
                    }),
                )
                .await
                .expect("second annotated insert");
            support::trigger_index_and_wait(&handle, ledger_id, second.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, second.receipt.t).await;
            let ledger2 = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after incremental index");
            let got = links(&fluree, &ledger2).await;
            assert_eq!(got.len(), 3, "{got:#?}");
            let term = |reifier: &str| -> String {
                got.iter()
                    .find(|r| r[0].ends_with(reifier))
                    .unwrap_or_else(|| panic!("no link for {reifier}: {got:#?}"))[1]
                    .clone()
            };
            assert_eq!(
                term("claim1"),
                term("claim2"),
                "two reifiers of one edge must share its term"
            );
            let t3 = term("claim3");
            assert!(
                t3.contains("carol") && t3.contains("dave"),
                "the new edge gets its own term: {t3}"
            );
        })
        .await;
}

/// The link-based lowering (`FLUREE_ANNOTATION_TERMS=1`): reified-triple
/// patterns scan `rdf:reifies` and decompose or constrain the term instead
/// of walking the bundle chain. Every shape the design doc's access-path
/// table names, on the same imported claims.
#[tokio::test]
async fn link_lowering_answers_reified_triple_shapes() {
    std::env::set_var("FLUREE_ANNOTATION_TERMS", "1");
    let (fluree, ledger) = import(&[("claims.ttl", CLAIMS)], "it/triple-term-links:lowering").await;
    let q = |body: &str| {
        format!(
            "PREFIX ex: <http://example.org/>\n\
             PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n{body}"
        )
    };
    let run = |body: &str| {
        let sparql = q(body);
        let fluree = &fluree;
        let ledger = &ledger;
        async move {
            let result = support::query_sparql_formatted(fluree, ledger, &sparql)
                .await
                .unwrap_or_else(|e| panic!("{sparql}: {e}"));
            rows(&result)
        }
    };

    // Wildcard: every reifier's edge, decomposed from the term.
    let got =
        run("SELECT ?s ?p ?o WHERE { ?r rdf:reifies <<( ?s ?p ?o )>> } ORDER BY ?s ?p ?o").await;
    assert_eq!(got.len(), 4, "{got:#?}");
    assert!(
        got.iter()
            .any(|r| r[0].ends_with("alice") && r[1].ends_with("knows") && r[2].ends_with("bob")),
        "{got:#?}"
    );
    assert!(
        got.iter()
            .any(|r| r[0].ends_with("carol") && r[1].ends_with("age") && r[2] == "42"),
        "{got:#?}"
    );

    // Predicate-bound with no body join: the handle interval alone must
    // exclude carol's `ex:age` claim.
    let got =
        run("SELECT ?s ?o WHERE { ?r rdf:reifies <<( ?s ex:knows ?o )>> } ORDER BY ?s ?o").await;
    assert_eq!(got.len(), 3, "{got:#?}");
    assert!(got.iter().all(|r| r[1] != "42"), "{got:#?}");

    // Predicate-bound: the handle interval.
    let got =
        run("SELECT ?s ?o WHERE { << ?s ex:knows ?o >> ex:source ?src } ORDER BY ?s ?o").await;
    assert_eq!(got.len(), 3, "{got:#?}");
    assert!(
        got.iter()
            .all(|r| r[0].ends_with("alice") || r[0].ends_with("bob")),
        "{got:#?}"
    );

    // Fully bound: the composed constant term.
    let got = run("SELECT ?src WHERE { << ex:alice ex:knows ex:bob >> ex:source ?src }").await;
    assert_eq!(got.len(), 1, "{got:#?}");
    assert!(got[0][0].ends_with("hr"), "{got:#?}");

    // Subject-bound with a literal object.
    let got = run("SELECT ?p ?o WHERE { ?r rdf:reifies <<( ex:carol ?p ?o )>> }").await;
    assert_eq!(got.len(), 1, "{got:#?}");
    assert!(got[0][0].ends_with("age") && got[0][1] == "42", "{got:#?}");

    // Object- and predicate-bound with the reifier's body joined.
    let got = run(
        "SELECT ?s ?src WHERE { ?r rdf:reifies <<( ?s ex:knows ex:bob )>> . ?r ex:source ?src }",
    )
    .await;
    assert_eq!(got.len(), 1, "{got:#?}");
    assert!(
        got[0][0].ends_with("alice") && got[0][1].ends_with("hr"),
        "{got:#?}"
    );

    // A component variable bound earlier joins instead of being rebound.
    let got = run("SELECT ?o2 WHERE { ?r1 rdf:reifies <<( ex:alice ex:knows ?o1 )>> . ?r2 rdf:reifies <<( ?o1 ex:knows ?o2 )>> }").await;
    assert_eq!(got.len(), 1, "{got:#?}");
    assert!(
        got[0][0].ends_with("dave"),
        "bob knows dave via alice knows bob: {got:#?}"
    );
}
