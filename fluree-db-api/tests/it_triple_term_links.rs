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

const LITERAL_CLAIMS: &str = r#"VERSION "1.2"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:doc ex:title "chat"@fr {| ex:source ex:fr |} .
ex:doc ex:title "chat"@en {| ex:source ex:en |} .
ex:doc ex:title "chat" {| ex:source ex:plain |} .
ex:doc ex:size "5"^^xsd:int {| ex:source ex:int |} .
ex:doc ex:size 5 {| ex:source ex:integer |} .
"#;

async fn run_link_query(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    body: String,
) -> Vec<Vec<String>> {
    let sparql = format!(
        "PREFIX ex: <http://example.org/>\n\
         PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
         PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n{body}"
    );
    let result = support::query_sparql_formatted(fluree, ledger, &sparql)
        .await
        .unwrap_or_else(|e| panic!("{sparql}: {e}"));
    rows(&result)
}

/// A reified pattern's literal object is a term: its language tag or
/// datatype is part of the match, both when the whole edge composes to a
/// constant term and when only the object is constant. Decomposing a term
/// keeps the tag and datatype too.
#[tokio::test]
async fn link_lowering_matches_literal_objects_by_term() {
    std::env::set_var("FLUREE_ANNOTATION_TERMS", "1");
    let (fluree, ledger) = import(
        &[("literals.ttl", LITERAL_CLAIMS)],
        "it/triple-term-links:literals",
    )
    .await;
    let run = |body: &str| run_link_query(&fluree, &ledger, body.to_string());

    for (object, source) in [
        ("\"chat\"@fr", "fr"),
        ("\"chat\"@en", "en"),
        ("\"chat\"", "plain"),
    ] {
        for subject in ["ex:doc", "?s"] {
            let got = run(&format!(
                "SELECT ?src WHERE {{ << {subject} ex:title {object} >> ex:source ?src }}"
            ))
            .await;
            assert_eq!(got.len(), 1, "{subject} {object}: {got:#?}");
            assert!(got[0][0].ends_with(source), "{subject} {object}: {got:#?}");
        }
    }
    for (object, source) in [("\"5\"^^xsd:int", "int"), ("5", "integer")] {
        for subject in ["ex:doc", "?s"] {
            let got = run(&format!(
                "SELECT ?src WHERE {{ << {subject} ex:size {object} >> ex:source ?src }}"
            ))
            .await;
            assert_eq!(got.len(), 1, "{subject} {object}: {got:#?}");
            assert!(got[0][0].ends_with(source), "{subject} {object}: {got:#?}");
        }
    }

    let got = run("SELECT ?o (LANG(?o) AS ?l) WHERE { ?r rdf:reifies <<( ex:doc ex:title ?o )>> } ORDER BY ?l").await;
    assert_eq!(got.len(), 3, "{got:#?}");
    let langs: Vec<&str> = got.iter().map(|r| r[1].as_str()).collect();
    assert_eq!(langs, ["", "en", "fr"], "{got:#?}");

    let got = run("SELECT (DATATYPE(?o) AS ?dt) WHERE { ?r rdf:reifies <<( ex:doc ex:size ?o )>> } ORDER BY ?dt").await;
    assert_eq!(got.len(), 2, "{got:#?}");
    assert!(got[0][0].ends_with("int"), "{got:#?}");
    assert!(got[1][0].ends_with("integer"), "{got:#?}");
}

/// A component variable is always bound with `BIND`, whose agreement check
/// is the join: the same variable in two UNION branches is fresh in each,
/// a VALUES row or the reifier itself constrains it, and the value the row
/// already holds may be in any representation.
#[tokio::test]
async fn link_lowering_joins_component_variables_in_every_scope() {
    std::env::set_var("FLUREE_ANNOTATION_TERMS", "1");
    let (fluree, ledger) = import(&[("claims.ttl", CLAIMS)], "it/triple-term-links:scopes").await;
    let run = |body: &str| run_link_query(&fluree, &ledger, body.to_string());

    let got = run(
        "SELECT ?o WHERE { { << ex:alice ex:knows ?o >> ex:source ex:hr } \
         UNION { << ex:bob ex:knows ?o >> ex:source ex:crm } } ORDER BY ?o",
    )
    .await;
    assert_eq!(got.len(), 2, "{got:#?}");
    assert!(
        got[0][0].ends_with("bob") && got[1][0].ends_with("dave"),
        "{got:#?}"
    );

    let got = run(
        "SELECT ?s ?o WHERE { VALUES ?s { ex:alice ex:bob } << ?s ex:knows ?o >> ex:source ?src } \
         ORDER BY ?s ?o",
    )
    .await;
    assert_eq!(got.len(), 3, "{got:#?}");
    assert!(
        got[2][0].ends_with("bob") && got[2][1].ends_with("dave"),
        "{got:#?}"
    );

    let got = run("SELECT ?x WHERE { ?x rdf:reifies <<( ?x ?p ?o )>> }").await;
    assert!(
        got.is_empty(),
        "no reifier is its own edge's subject: {got:#?}"
    );
    let got = run("SELECT ?x WHERE { ?x rdf:reifies <<( ?y ?p ?o )>> }").await;
    assert_eq!(got.len(), 4, "{got:#?}");

    let got = run("SELECT ?s WHERE { ?s ex:age ?age . << ?s ?p ?o >> ex:source ?src }").await;
    assert!(got.is_empty(), "carol's claim has no source: {got:#?}");
    let got = run(
        "SELECT ?s ?o WHERE { ?s ex:knows ?o . ?r rdf:reifies <<( ?s ex:knows ?o )>> } \
         ORDER BY ?s ?o",
    )
    .await;
    assert_eq!(got.len(), 3, "{got:#?}");
    assert!(
        got[2][0].ends_with("bob") && got[2][1].ends_with("dave"),
        "{got:#?}"
    );
}

/// Two inner-predicate constraints on one term that name different
/// predicates admit no handle: the second filter stays in the plan.
#[tokio::test]
async fn link_lowering_keeps_contradictory_predicate_filters() {
    std::env::set_var("FLUREE_ANNOTATION_TERMS", "1");
    let (fluree, ledger) = import(
        &[("claims.ttl", CLAIMS)],
        "it/triple-term-links:contradiction",
    )
    .await;
    let run = |body: &str| run_link_query(&fluree, &ledger, body.to_string());

    let got = run("SELECT ?r WHERE { ?r rdf:reifies ?t . FILTER(PREDICATE(?t) = ex:knows) }").await;
    assert_eq!(got.len(), 3, "{got:#?}");
    let got = run(
        "SELECT ?r WHERE { ?r rdf:reifies ?t . FILTER(PREDICATE(?t) = ex:knows) \
         FILTER(PREDICATE(?t) = ex:age) }",
    )
    .await;
    assert!(got.is_empty(), "{got:#?}");
}

/// Positions the query never reads are not decomposed at all; the count is
/// still the count of matching links.
#[tokio::test]
async fn link_lowering_counts_without_decomposing_unread_positions() {
    std::env::set_var("FLUREE_ANNOTATION_TERMS", "1");
    let (fluree, ledger) = import(&[("claims.ttl", CLAIMS)], "it/triple-term-links:count").await;
    let got = run_link_query(
        &fluree,
        &ledger,
        "SELECT (COUNT(*) AS ?n) WHERE { << ?s ?p ?o >> ex:source ?src }".to_string(),
    )
    .await;
    assert_eq!(got, vec![vec!["3".to_string()]], "{got:#?}");
    let got = run_link_query(
        &fluree,
        &ledger,
        "SELECT (COUNT(*) AS ?n) WHERE { << ?s ex:knows ?o >> ex:source ?src }".to_string(),
    )
    .await;
    assert_eq!(got, vec![vec!["3".to_string()]], "{got:#?}");
}

/// A re-point that changes one slot writes only that slot's retract and
/// assert (sync and upsert cancel the unchanged slots). A rebuild replays
/// the reifier's whole history, so the link moves with it.
#[tokio::test]
async fn reindex_follows_a_partial_repoint() {
    let alias = "it/triple-term-links:reindex-repoint";
    let (fluree, ledger) = import(&[("claims.ttl", CLAIMS)], alias).await;
    fluree
        .upsert_turtle(
            ledger,
            "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\nex:carol ex:age 43 ~ ex:claim2 .\n",
        )
        .await
        .expect("re-pointing the claim's object through upsert");
    fluree
        .reindex(alias, fluree_db_api::ReindexOptions::default())
        .await
        .expect("reindex after the re-point");
    let ledger = fluree.ledger(alias).await.expect("reload after reindex");
    let got = links(&fluree, &ledger).await;
    assert_eq!(got.len(), 4, "{got:#?}");
    let t2 = &got
        .iter()
        .find(|r| r[0].ends_with("claim2"))
        .unwrap_or_else(|| panic!("no link for claim2: {got:#?}"))[1];
    assert!(
        t2.contains("carol") && t2.contains("43") && !t2.contains("42"),
        "claim2 must reify the re-pointed edge: {t2}"
    );
}

/// The incremental twin: the base index holds the reifier's attachment, the
/// next window carries only the changed slot, and the link must still move.
#[tokio::test]
async fn incremental_index_follows_a_partial_repoint() {
    use fluree_db_indexer::IndexerConfig;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/triple-term-links:incremental-repoint";
    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());
    let turtle =
        |body: &str| format!("VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n{body}\n");

    local
        .run_until(async {
            let ledger0 = support::genesis_ledger(&fluree, ledger_id);
            let first = fluree
                .upsert_turtle(
                    ledger0,
                    &turtle("ex:alice ex:age 42 ~ ex:claim1 {| ex:source ex:hr |} ."),
                )
                .await
                .expect("first claim");
            support::trigger_index_and_wait(&handle, ledger_id, first.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, first.receipt.t).await;
            let ledger1 = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after first index");
            let got = links(&fluree, &ledger1).await;
            assert_eq!(got.len(), 1, "{got:#?}");
            assert!(got[0][1].contains("42"), "{got:#?}");

            let second = fluree
                .upsert_turtle(
                    ledger1,
                    &turtle("ex:alice ex:age 43 ~ ex:claim1 {| ex:source ex:hr |} ."),
                )
                .await
                .expect("re-pointing the claim's object through upsert");
            support::trigger_index_and_wait(&handle, ledger_id, second.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, second.receipt.t).await;
            let ledger2 = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after incremental index");
            let got = links(&fluree, &ledger2).await;
            assert_eq!(got.len(), 1, "one live link after the re-point: {got:#?}");
            assert!(
                got[0][1].contains("43") && !got[0][1].contains("42"),
                "the link must follow the re-point: {got:#?}"
            );
        })
        .await;
}

async fn assert_object_types_survive(fluree: &fluree_db_api::Fluree, ledger: &LedgerState) {
    let got = run_link_query(
        fluree,
        ledger,
        "SELECT (DATATYPE(OBJECT(?t)) AS ?dt) WHERE { ?r rdf:reifies ?t . \
         FILTER(PREDICATE(?t) = ex:size) } ORDER BY ?dt"
            .to_string(),
    )
    .await;
    assert_eq!(got.len(), 2, "{got:#?}");
    assert!(
        got[0][0].ends_with("int") && got[1][0].ends_with("integer"),
        "{got:#?}"
    );
    let got = run_link_query(
        fluree,
        ledger,
        "SELECT (LANG(OBJECT(?t)) AS ?l) WHERE { ?r rdf:reifies ?t . \
         FILTER(PREDICATE(?t) = ex:title) } ORDER BY ?l"
            .to_string(),
    )
    .await;
    let langs: Vec<&str> = got.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(langs, ["", "en", "fr"], "{got:#?}");
    let got = run_link_query(
        fluree,
        ledger,
        "SELECT (DATATYPE(?o) AS ?dt) WHERE { ?r rdf:reifies <<( ex:doc ex:size ?o )>> } \
         ORDER BY ?dt"
            .to_string(),
    )
    .await;
    assert_eq!(got.len(), 2, "{got:#?}");
    assert!(
        got[0][0].ends_with("int") && got[1][0].ends_with("integer"),
        "{got:#?}"
    );
}

/// `DATATYPE` and `LANG` over an accessor read the component the way a
/// bound variable is read: on the indexed path, and on the materialized
/// path once an unrelated unindexed commit turns late materialization off.
#[tokio::test]
async fn link_lowering_keeps_object_types_through_accessors_and_novelty() {
    std::env::set_var("FLUREE_ANNOTATION_TERMS", "1");
    let (fluree, ledger) = import(
        &[("literals.ttl", LITERAL_CLAIMS)],
        "it/triple-term-links:accessor-types",
    )
    .await;
    assert_object_types_survive(&fluree, &ledger).await;

    let after = fluree
        .insert(
            ledger,
            &json!({
                "@context": { "ex": "http://example.org/" },
                "@id": "ex:other",
                "ex:note": "unrelated"
            }),
        )
        .await
        .expect("unrelated insert");
    assert_object_types_survive(&fluree, &after.ledger).await;
}

/// `rdf:reifies` names a triple term. An ordinary object is refused on the
/// transactional path and on import, so every `rdf:reifies` row is a link
/// and a scan of the predicate never has to second-guess its object.
#[tokio::test]
async fn rdf_reifies_with_an_ordinary_object_is_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, "it/triple-term-links:reifies-firewall");
    let bad = "@prefix ex: <http://example.org/> .\n\
               @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
               ex:r rdf:reifies ex:x ; ex:source ex:hr .\n";
    let err = fluree
        .upsert_turtle(ledger0, bad)
        .await
        .expect_err("rdf:reifies with an IRI object must be refused");
    assert!(format!("{err}").contains("reifies"), "{err}");

    fluree
        .create_ledger("it/triple-term-links:reifies-firewall-sparql")
        .await
        .expect("create ledger");
    let handle = fluree
        .ledger_cached("it/triple-term-links:reifies-firewall-sparql")
        .await
        .expect("ledger handle");
    let err = fluree
        .stage(&handle)
        .sparql_update(
            "PREFIX ex: <http://example.org/>\n\
             PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
             INSERT DATA { ex:r rdf:reifies ex:x ; ex:source ex:hr . }",
        )
        .execute()
        .await
        .expect_err("SPARQL UPDATE of rdf:reifies with an IRI object must be refused");
    assert!(format!("{err}").contains("reifies"), "{err}");

    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    std::fs::write(data_dir.path().join("bad.ttl"), bad).expect("write fixture");
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let result = fluree
        .create("it/triple-term-links:reifies-firewall-import")
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await;
    let err = match result {
        Ok(_) => panic!("import of rdf:reifies with an IRI object must fail"),
        Err(e) => e,
    };
    assert!(format!("{err}").contains("reifies"), "{err}");
}

/// Every incremental build appends a pack to each inner predicate's term
/// stream that gained terms; compaction keeps that stream bounded, records
/// the packs it merged away as garbage, and leaves every handle resolvable.
#[tokio::test]
async fn incremental_term_packs_are_compacted() {
    use fluree_db_binary_index::format::index_root::IndexRoot;
    use fluree_db_core::{ContentId, ContentStore};
    use fluree_db_indexer::IndexerConfig;

    const CYCLES: u64 = 12;
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/triple-term-links:term-pack-compaction";
    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local
        .run_until(async {
            let mut ledger = support::genesis_ledger(&fluree, ledger_id);
            let mut roots: Vec<ContentId> = Vec::new();
            for cycle in 0..CYCLES {
                let turtle = format!(
                    "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n\
                     ex:s{cycle} ex:val {cycle} ~ ex:r{cycle} {{| ex:src ex:x |}} .\n"
                );
                let r = fluree.upsert_turtle(ledger, &turtle).await.expect("claim");
                support::trigger_index_and_wait(&handle, ledger_id, r.receipt.t).await;
                support::wait_for_index_application(&fluree, ledger_id, r.receipt.t).await;
                ledger = fluree.ledger(ledger_id).await.expect("reload");
                let record = fluree
                    .nameservice()
                    .lookup(ledger_id)
                    .await
                    .expect("ns lookup")
                    .expect("ns record");
                roots.push(record.index_head_id.expect("index root"));
            }

            let cs = fluree.content_store(ledger_id);
            let mut decoded = Vec::new();
            for cid in &roots {
                decoded.push(IndexRoot::decode(&cs.get(cid).await.expect("root")).expect("decode"));
            }
            let term_packs = |root: &IndexRoot| -> Vec<ContentId> {
                root.term_dict
                    .iter()
                    .flat_map(|td| td.forward_packs.iter())
                    .flat_map(|(_, refs)| refs.iter().map(|r| r.pack_cid.clone()))
                    .collect()
            };
            let final_packs = term_packs(decoded.last().unwrap());
            assert!(
                final_packs.len() < CYCLES as usize,
                "{} term packs after {CYCLES} appending builds: nothing was compacted",
                final_packs.len()
            );

            let consumed: Vec<ContentId> = decoded
                .iter()
                .flat_map(term_packs)
                .filter(|cid| !final_packs.contains(cid))
                .collect();
            assert!(!consumed.is_empty(), "no term pack was merged away");
            let mut garbage = std::collections::HashSet::new();
            for root in &decoded {
                let Some(g) = root.garbage.as_ref() else {
                    continue;
                };
                let record: fluree_db_indexer::GarbageRecord =
                    serde_json::from_slice(&cs.get(&g.id).await.expect("garbage")).expect("parse");
                garbage.extend(record.garbage);
            }
            for cid in &consumed {
                assert!(
                    garbage.contains(&cid.to_string()),
                    "merged-away term pack {cid} reached no garbage record"
                );
            }

            let got = links(&fluree, &ledger).await;
            assert_eq!(got.len(), CYCLES as usize, "{got:#?}");
            for cycle in 0..CYCLES {
                assert!(
                    got.iter().any(|row| row[0].ends_with(&format!("r{cycle}"))
                        && row[1].contains(&format!("s{cycle}"))),
                    "reifier r{cycle} lost its term: {got:#?}"
                );
            }
        })
        .await;
}
