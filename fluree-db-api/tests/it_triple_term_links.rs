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

/// The link lowering: reified-triple patterns scan `rdf:reifies` and
/// decompose or constrain the term instead of walking the bundle chain.
/// Every shape the design doc's access-path table names, on the same
/// imported claims.
#[tokio::test]
async fn link_lowering_answers_reified_triple_shapes() {
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
    // The accessor's argument need not be a bare variable.
    for select in [
        "(DATATYPE(OBJECT(COALESCE(?t))) AS ?dt)",
        "(DATATYPE(?o) AS ?dt)",
    ] {
        let got = run_link_query(
            fluree,
            ledger,
            format!(
                "SELECT {select} WHERE {{ ?r rdf:reifies ?t . \
                 FILTER(PREDICATE(?t) = ex:size) BIND(OBJECT(COALESCE(?t)) AS ?o) }} \
                 ORDER BY ?dt"
            ),
        )
        .await;
        assert_eq!(got.len(), 2, "{select}: {got:#?}");
        assert!(
            got[0][0].ends_with("int") && got[1][0].ends_with("integer"),
            "{select}: {got:#?}"
        );
    }
}

/// `DATATYPE` and `LANG` over an accessor read the component the way a
/// bound variable is read: on the indexed path, and on the materialized
/// path once an unrelated unindexed commit turns late materialization off.
#[tokio::test]
async fn link_lowering_keeps_object_types_through_accessors_and_novelty() {
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

    // A predicate variable reaches the predicate only per solution row.
    let err = fluree
        .stage(&handle)
        .sparql_update(
            "PREFIX ex: <http://example.org/>\n\
             PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
             INSERT { ex:r ?p ex:x } WHERE { VALUES ?p { rdf:reifies } }",
        )
        .execute()
        .await
        .expect_err("a predicate variable bound to rdf:reifies must be refused too");
    assert!(format!("{err}").contains("reifies"), "{err}");
    let ledger = fluree
        .ledger("it/triple-term-links:reifies-firewall-sparql")
        .await
        .expect("reload");
    let err = fluree
        .update(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                },
                "where": [["values", ["?p", [{ "@type": "@id", "@value": "rdf:reifies" }]]]],
                "insert": { "@id": "ex:r", "?p": { "@id": "ex:x" } }
            }),
        )
        .await
        .expect_err("a JSON-LD predicate variable bound to rdf:reifies must be refused");
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

const CHAINED_CLAIMS: &str = r#"VERSION "1.2"
@prefix ex: <http://example.org/> .
ex:a ex:p ex:b ~ ex:r1 {| ex:src ex:x |} .
ex:b ex:q ex:c ~ ex:r2 {| ex:src ex:y |} .
ex:d ex:q ex:e ~ ex:r3 {| ex:src ex:z |} .
"#;

/// Two quoted patterns joined through a component variable: the second
/// edge's subject must be the first edge's object. Both positions lower to a
/// `BIND` of the same variable, so the second is the join; the planner must
/// carry the first one's column to it although only a count reads it.
#[tokio::test]
async fn link_lowering_joins_chained_edges_when_only_counted() {
    let (fluree, ledger) = import(
        &[("chained.ttl", CHAINED_CLAIMS)],
        "it/triple-term-links:chained",
    )
    .await;
    let chain = "<< ex:a ex:p ?o1 >> ex:src ?s1 . << ?o1 ex:q ?o2 >> ex:src ?s2";
    let count = |select: &str| format!("SELECT {select} WHERE {{ {chain} }}");
    for select in ["(COUNT(*) AS ?n)", "(COUNT(DISTINCT ?o1) AS ?n)"] {
        let got = run_link_query(&fluree, &ledger, count(select)).await;
        assert_eq!(got, vec![vec!["1".to_string()]], "{select}: {got:#?}");
    }
    let got = run_link_query(&fluree, &ledger, count("?o1 ?o2")).await;
    assert_eq!(
        got,
        vec![vec!["ex:b".to_string(), "ex:c".to_string()]],
        "{got:#?}"
    );
}

/// A quoted edge whose subject a previous edge bound is found through its
/// components (the reverse tree's subject prefix) and its link read as a
/// bound-object lookup, rather than every link of the predicate being read
/// and decoded per input row. (Which edge leads is a cost decision; on three
/// links the first edge's scan is cheapest.)
#[tokio::test]
async fn link_lowering_drives_chained_edges_through_their_subjects() {
    let alias = "it/triple-term-links:chained-plan";
    let (fluree, _ledger) = import(&[("chained.ttl", CHAINED_CLAIMS)], alias).await;
    let view = fluree.db(alias).await.expect("view");
    let plan = fluree
        .explain_sparql(
            &view,
            "PREFIX ex: <http://example.org/>\n\
             SELECT (COUNT(*) AS ?n) WHERE { \
             << ex:a ex:p ?o1 >> ex:src ?s1 . << ?o1 ex:q ?o2 >> ex:src ?s2 }",
        )
        .await
        .expect("explain");

    /// The `TermComponentsOperator` for `term` in this subtree, if any.
    fn components_for<'a>(node: &'a JsonValue, term: &str) -> Option<&'a JsonValue> {
        if node["op"] == "TermComponentsOperator" && node["details"]["term"] == term {
            return Some(node);
        }
        node["children"]
            .as_array()
            .into_iter()
            .flatten()
            .find_map(|c| components_for(&c["node"], term))
    }
    fn link_joins(node: &JsonValue, out: &mut Vec<JsonValue>) {
        if node["op"] == "NestedLoopJoinOperator"
            && node["details"]["right"]
                .as_str()
                .is_some_and(|r| r.contains("reifies"))
        {
            out.push(node.clone());
        }
        for c in node["children"].as_array().into_iter().flatten() {
            link_joins(&c["node"], out);
        }
    }
    let mut joins = Vec::new();
    link_joins(&plan["plan"]["physical"], &mut joins);
    assert!(
        !joins.is_empty(),
        "the chained link must be a lookup: {plan:#}"
    );
    for join in &joins {
        let right = join["details"]["right"].as_str().unwrap();
        let term = right.rsplit(' ').next().unwrap();
        let components = components_for(join, term)
            .unwrap_or_else(|| panic!("link {right} was read without its components: {plan:#}"));
        assert_eq!(components["details"]["access"], "subject", "{plan:#}");
    }
}

/// What every link query below must answer once the claims are imported and
/// three unindexed transactions have changed them: a new annotated edge, a
/// partial re-point of `ex:claim2`'s object, a second edge out of `ex:bob`,
/// and a retract of `ex:alice ex:knows ex:carol`, whose annotation the
/// retract cascades away.
async fn assert_novelty_links(fluree: &fluree_db_api::Fluree, ledger: &LedgerState) {
    let got = links(fluree, ledger).await;
    assert_eq!(got.len(), 5, "{got:#?}");
    let term = |reifier: &str| -> String {
        got.iter()
            .find(|r| r[0].ends_with(reifier))
            .unwrap_or_else(|| panic!("no link for {reifier}: {got:#?}"))[1]
            .clone()
    };
    assert!(
        term("claim2").contains("43") && !term("claim2").contains("42"),
        "the link follows an unindexed re-point: {got:#?}"
    );
    assert!(
        term("claim9").contains("erin") && term("claim9").contains("frank"),
        "{got:#?}"
    );
    assert!(
        !got.iter()
            .any(|r| r[1].contains("carol") && r[1].contains("knows")),
        "the cascade retracts the link: {got:#?}"
    );

    let got = run_link_query(
        fluree,
        ledger,
        "SELECT ?s ?o WHERE { << ?s ex:knows ?o >> ex:source ?src } ORDER BY ?s ?o".to_string(),
    )
    .await;
    let pairs: Vec<(String, String)> = got.iter().map(|r| (r[0].clone(), r[1].clone())).collect();
    assert_eq!(
        pairs,
        [
            ("ex:alice", "ex:bob"),
            ("ex:bob", "ex:dave"),
            ("ex:bob", "ex:erin"),
            ("ex:erin", "ex:frank"),
        ]
        .map(|(s, o)| (s.to_string(), o.to_string())),
        "{got:#?}"
    );

    let got = run_link_query(
        fluree,
        ledger,
        "SELECT ?o WHERE { << ex:erin ex:knows ?o >> ex:source ?src }".to_string(),
    )
    .await;
    assert_eq!(got, vec![vec!["ex:frank".to_string()]], "{got:#?}");

    let got = run_link_query(
        fluree,
        ledger,
        "SELECT ?age WHERE { ?r rdf:reifies <<( ex:carol ex:age ?age )>> }".to_string(),
    )
    .await;
    assert_eq!(got, vec![vec!["43".to_string()]], "{got:#?}");

    // The second edge's subject is bound by the first: one indexed term and
    // one only novelty holds.
    let got = run_link_query(
        fluree,
        ledger,
        "SELECT ?x WHERE { << ex:alice ex:knows ?y >> ex:source ?s1 . \
         << ?y ex:knows ?x >> ex:source ?s2 } ORDER BY ?x"
            .to_string(),
    )
    .await;
    assert_eq!(
        got,
        vec![vec!["ex:dave".to_string()], vec!["ex:erin".to_string()]],
        "{got:#?}"
    );
}

async fn change_claims_without_indexing(
    fluree: &fluree_db_api::Fluree,
    ledger: LedgerState,
) -> LedgerState {
    let turtle =
        |body: &str| format!("VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n{body}\n");
    let ledger = fluree
        .upsert_turtle(
            ledger,
            &turtle(
                "ex:erin ex:knows ex:frank ~ ex:claim9 {| ex:source ex:web |} .\n\
                 ex:carol ex:age 43 ~ ex:claim2 .",
            ),
        )
        .await
        .expect("new claim and re-point")
        .ledger;
    let ledger = fluree
        .insert_turtle(
            ledger,
            &turtle("ex:bob ex:knows ex:erin {| ex:source ex:web |} ."),
        )
        .await
        .expect("second edge out of bob")
        .ledger;
    fluree
        .update(
            ledger,
            &json!({
                "@context": { "ex": "http://example.org/" },
                "delete": { "@id": "ex:alice", "ex:knows": { "@id": "ex:carol" } }
            }),
        )
        .await
        .expect("retract an annotated edge")
        .ledger
}

/// Links for commits no index covers come from novelty: new annotations, a
/// partial re-point and a cascaded retract show in link queries before any
/// index build, and again after a reload rebuilds novelty from the commits,
/// where the links wait for the index store before they can be derived.
#[tokio::test]
async fn novelty_carries_links_until_the_index_covers_them() {
    let alias = "it/triple-term-links:novelty";
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    std::fs::write(data_dir.path().join("claims.ttl"), CLAIMS).expect("write fixture");
    let open = || {
        FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
            .build()
            .expect("build file-backed Fluree")
    };
    let fluree = open();
    fluree
        .create(alias)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let ledger = fluree.ledger(alias).await.expect("load ledger");
    let ledger = change_claims_without_indexing(&fluree, ledger).await;
    assert!(
        ledger.t() > ledger.index_t(),
        "the changes must stay unindexed"
    );
    assert_novelty_links(&fluree, &ledger).await;
    drop(ledger);
    drop(fluree);

    let fluree = open();
    let ledger = fluree.ledger(alias).await.expect("reload");
    assert!(
        ledger.t() > ledger.index_t(),
        "the reload must replay novelty"
    );
    assert_novelty_links(&fluree, &ledger).await;

    // Time travel to the first change replays novelty on its own.
    let view = fluree
        .db_at_t(alias, ledger.index_t() + 1)
        .await
        .expect("historical view");
    let result = fluree
        .query(
            &view,
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
             SELECT ?r ?t WHERE { ?r rdf:reifies ?t } ORDER BY ?r",
        )
        .await
        .expect("historical link query");
    let got = rows(
        &result
            .to_jsonld_async(view.as_graph_db_ref())
            .await
            .expect("format"),
    );
    assert_eq!(got.len(), 5, "{got:#?}");
    assert!(
        got.iter()
            .any(|r| r[1].contains("carol") && r[1].contains("knows")),
        "the retract comes later: {got:#?}"
    );
    assert!(
        got.iter()
            .any(|r| r[0].ends_with("claim2") && r[1].contains("43")),
        "{got:#?}"
    );
}

/// A ledger that was never indexed holds every link in novelty and has no
/// term dictionary at all.
#[tokio::test]
async fn novelty_carries_links_on_a_ledger_never_indexed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "it/triple-term-links:never-indexed");
    let ledger = fluree
        .insert_turtle(ledger, CLAIMS)
        .await
        .expect("claims")
        .ledger;
    assert_eq!(ledger.index_t(), 0);
    let ledger = change_claims_without_indexing(&fluree, ledger).await;
    assert_novelty_links(&fluree, &ledger).await;
}

/// An index publish in the middle of unindexed re-points: novelty's links
/// must then derive from the new index. Against the old one, the next
/// re-point would retract a term the new index never linked and leave the
/// published link live beside the new one. Every write and read goes through
/// the cached handle, which is what the publish updates in place.
#[tokio::test]
async fn novelty_links_follow_the_index_they_were_published_over() {
    use fluree_db_indexer::IndexerConfig;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/triple-term-links:novelty-over-publish";
    let (local, indexer) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());
    let claim = |age: u32| {
        format!(
            "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n\
             ex:alice ex:age {age} ~ ex:claim1 {{| ex:source ex:hr |}} .\n"
        )
    };

    local
        .run_until(async {
            fluree.create_ledger(ledger_id).await.expect("create");
            let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
            let upsert = |age: u32| {
                let fluree = &fluree;
                let handle = &handle;
                let turtle = claim(age);
                async move {
                    fluree
                        .stage(handle)
                        .upsert_turtle(&turtle)
                        .execute()
                        .await
                        .expect("upsert through the cached handle")
                        .receipt
                        .t
                }
            };
            let live = || async {
                let state = handle.snapshot().await.to_ledger_state();
                let got = links(&fluree, &state).await;
                (state.t(), state.index_t(), got)
            };
            let assert_age = |got: &Vec<Vec<String>>, age: u32| {
                assert_eq!(got.len(), 1, "one live link: {got:#?}");
                assert!(got[0][1].contains(&age.to_string()), "{got:#?}");
            };

            let t1 = upsert(41).await;
            support::trigger_index_and_wait(&indexer, ledger_id, t1).await;
            support::wait_for_index_application(&fluree, ledger_id, t1).await;

            let t2 = upsert(42).await;
            let (t, index_t, got) = live().await;
            assert_eq!((t, index_t), (t2, t1));
            assert_age(&got, 42);
            support::trigger_index_and_wait(&indexer, ledger_id, t2).await;
            support::wait_for_index_application(&fluree, ledger_id, t2).await;

            let t3 = upsert(43).await;
            let (t, index_t, got) = live().await;
            assert_eq!(
                (t, index_t),
                (t3, t2),
                "the publish must reach the cached handle"
            );
            assert_age(&got, 43);
        })
        .await;
}

/// A count over links whose terms only novelty holds stays on the count plan:
/// those terms get provisional handles, so their links join the encoded
/// overlay instead of the raw-flake lane the plan declines on.
#[tokio::test(flavor = "current_thread")]
async fn novelty_terms_keep_link_counts_on_the_count_plan() {
    let (fluree, ledger) = import(
        &[("claims.ttl", CLAIMS)],
        "it/triple-term-links:novelty-count",
    )
    .await;
    let ledger = change_claims_without_indexing(&fluree, ledger).await;
    let count = || "SELECT (COUNT(*) AS ?n) WHERE { << ?s ?p ?o >> ex:source ?src }".to_string();
    // Register the stamp callsite before this thread's subscriber reads it.
    run_link_query(&fluree, &ledger, count()).await;
    let (store, _guard) = support::span_capture::init_test_tracing();
    tracing::callsite::rebuild_interest_cache();

    let got = run_link_query(&fluree, &ledger, count()).await;
    assert_eq!(got, vec![vec!["4".to_string()]], "{got:#?}");
    let outcomes: Vec<String> = store
        .find_events("fast-path outcome")
        .iter()
        .filter(|e| e.fields.get("site").map(String::as_str) == Some("count-plan"))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect();
    assert_eq!(outcomes, ["proceed"], "{outcomes:?}");
}

const ARENA_CLAIMS: &str = r#"VERSION "1.2"
@prefix ex: <http://example.org/> .
@prefix f: <https://ns.flur.ee/db#> .

ex:acct ex:balance 1.50 ~ ex:c1 {| ex:source ex:ledger |} .
ex:acct ex:serial 123456789012345678901234567890 ~ ex:c2 {| ex:source ex:registry |} .
ex:acct ex:embedding "[0.5, -1.25]"^^f:embeddingVector ~ ex:c3 {| ex:source ex:model |} .
"#;

/// Decimal, big-integer and vector objects, which the main index keys by
/// per-graph arena handles: each annotated edge has a link, a constant term
/// composes to it, and its object decomposes with its datatype; a decimal's
/// joins the asserted edge.
async fn assert_arena_links(fluree: &fluree_db_api::Fluree, ledger: &LedgerState) {
    let got = links(fluree, ledger).await;
    let term = |reifier: &str| -> String {
        got.iter()
            .find(|r| r[0].ends_with(reifier))
            .unwrap_or_else(|| panic!("no link for {reifier}: {got:#?}"))[1]
            .clone()
    };
    assert!(term("c1").contains("1.5"), "{got:#?}");
    assert!(
        term("c2").contains("123456789012345678901234567890"),
        "{got:#?}"
    );
    assert!(
        term("c3").contains("0.5") && term("c3").contains("-1.25"),
        "{got:#?}"
    );

    let run = |body: &str| run_link_query(fluree, ledger, body.to_string());
    for (edge, source) in [
        ("ex:acct ex:balance 1.5", "ledger"),
        ("?s ex:balance 1.50", "ledger"),
        (
            "ex:acct ex:serial 123456789012345678901234567890",
            "registry",
        ),
        (
            "?s ex:embedding \"[0.5, -1.25]\"^^<https://ns.flur.ee/db#embeddingVector>",
            "model",
        ),
    ] {
        let got = run(&format!(
            "SELECT ?src WHERE {{ << {edge} >> ex:source ?src }}"
        ))
        .await;
        if edge.starts_with("ex:acct ") {
            assert_eq!(got.len(), 1, "{edge}: {got:#?}");
        }
        assert!(
            got.iter().any(|r| r[0].ends_with(source)),
            "{edge}: {got:#?}"
        );
    }

    for (p, dt) in [
        ("balance", "decimal"),
        ("serial", "integer"),
        ("embedding", "embeddingVector"),
    ] {
        let got = run(&format!(
            "SELECT (DATATYPE(?o) AS ?dt) WHERE {{ << ex:acct ex:{p} ?o >> ex:source ?src }}"
        ))
        .await;
        assert_eq!(got.len(), 1, "{p}: {got:#?}");
        assert!(got[0][0].ends_with(dt), "{p}: {got:#?}");
    }
    for body in [
        "SELECT ?src WHERE { << ex:acct ex:balance ?o >> ex:source ?src . ex:acct ex:balance ?o }",
        "SELECT ?src WHERE { ex:acct ex:balance ?o . << ex:acct ex:balance ?o >> ex:source ?src }",
    ] {
        let got = run(body).await;
        assert_eq!(got, vec![vec!["ex:ledger".to_string()]], "{body}: {got:#?}");
    }
}

/// Import interns these terms through the chunk term table, a rebuild
/// through the resolver's attachment replay. The first file is its own chunk
/// with strings that sort before the forms, so the second chunk's local
/// string ids are not the global ones.
#[tokio::test]
async fn arena_kind_objects_link_through_import_and_reindex() {
    let alias = "it/triple-term-links:arena";
    let pad = "@prefix ex: <http://example.org/> .\nex:pad ex:label \"!pad\" , \"0pad\" .\n";
    let (fluree, ledger) = import(&[("a.ttl", pad), ("b.ttl", ARENA_CLAIMS)], alias).await;
    assert_arena_links(&fluree, &ledger).await;

    // A term only novelty holds whose object form the index already interned.
    let ledger = fluree
        .insert_turtle(
            ledger,
            "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n\
             ex:acct2 ex:balance 1.5 ~ ex:c4 {| ex:source ex:late |} .\n",
        )
        .await
        .expect("unindexed claim")
        .ledger;
    let got = run_link_query(
        &fluree,
        &ledger,
        "SELECT ?src WHERE { << ?s ex:balance 1.5 >> ex:source ?src } ORDER BY ?src".to_string(),
    )
    .await;
    assert_eq!(
        got,
        vec![vec!["ex:late".to_string()], vec!["ex:ledger".to_string()]],
        "{got:#?}"
    );

    fluree
        .reindex(alias, fluree_db_api::ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(alias).await.expect("reload after reindex");
    assert_eq!(ledger.t(), ledger.index_t());
    assert_arena_links(&fluree, &ledger).await;
    assert_eq!(links(&fluree, &ledger).await.len(), 4);
}

/// A ledger never indexed holds these links in novelty.
#[tokio::test]
async fn arena_kind_objects_link_in_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "it/triple-term-links:arena-novelty");
    let ledger = fluree
        .insert_turtle(ledger, ARENA_CLAIMS)
        .await
        .expect("claims")
        .ledger;
    assert_eq!(ledger.index_t(), 0);
    assert_arena_links(&fluree, &ledger).await;
}

/// Incremental builds over these objects, in the default graph and a named
/// one. The base index holds each attachment's object as an arena handle;
/// a re-point must still retract the term the base linked (an object
/// change) and carry the base object into the new term (a subject change).
#[tokio::test]
async fn arena_kind_links_follow_incremental_repoints_in_every_graph() {
    use fluree_db_indexer::IndexerConfig;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/triple-term-links:arena-incremental";
    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());
    let trig = |body: &str| {
        format!(
            "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n\
             @prefix f: <https://ns.flur.ee/db#> .\n{body}\n"
        )
    };
    let audit_links = |ledger: LedgerState| {
        let fluree = &fluree;
        async move {
            let result = support::query_sparql_formatted(
                fluree,
                &ledger,
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
                 SELECT ?r ?t WHERE { GRAPH <http://example.org/audit> { ?r rdf:reifies ?t } }",
            )
            .await
            .expect("named-graph link query");
            rows(&result)
        }
    };

    local
        .run_until(async {
            fluree.create_ledger(ledger_id).await.expect("create");
            let commit = |body: String| {
                let fluree = &fluree;
                async move {
                    fluree
                        .graph(ledger_id)
                        .transact()
                        .upsert_turtle(&body)
                        .commit()
                        .await
                        .expect("commit claims")
                        .receipt
                        .t
                }
            };
            let first = commit(trig(
                "ex:acct ex:balance 1.50 ~ ex:c1 {| ex:source ex:ledger |} .\n\
                 ex:acct ex:embedding \"[0.5, -1.25]\"^^f:embeddingVector \
                 ~ ex:c3 {| ex:source ex:model |} .\n\
                 GRAPH ex:audit { ex:acct ex:balance 1.5 ~ ex:c2 {| ex:source ex:audit |} . }",
            ))
            .await;
            support::trigger_index_and_wait(&handle, ledger_id, first).await;
            support::wait_for_index_application(&fluree, ledger_id, first).await;
            let ledger1 = fluree.ledger(ledger_id).await.expect("reload");
            assert_eq!(ledger1.t(), ledger1.index_t());
            let got = links(&fluree, &ledger1).await;
            assert_eq!(got.len(), 2, "{got:#?}");
            let got = audit_links(ledger1).await;
            assert_eq!(got.len(), 1, "{got:#?}");
            assert!(got[0][1].contains("1.5"), "{got:#?}");

            let second = commit(trig(
                "ex:acct ex:balance 2.25 ~ ex:c1 .\n\
                 ex:other ex:embedding \"[0.5, -1.25]\"^^f:embeddingVector ~ ex:c3 .\n\
                 GRAPH ex:audit { ex:acct ex:balance 3.75 ~ ex:c2 . }",
            ))
            .await;
            support::trigger_index_and_wait(&handle, ledger_id, second).await;
            support::wait_for_index_application(&fluree, ledger_id, second).await;
            let ledger2 = fluree.ledger(ledger_id).await.expect("reload");
            assert_eq!(ledger2.t(), ledger2.index_t());

            let got = links(&fluree, &ledger2).await;
            assert_eq!(got.len(), 2, "one live link per reifier: {got:#?}");
            let term = |reifier: &str| -> String {
                got.iter()
                    .find(|r| r[0].ends_with(reifier))
                    .unwrap_or_else(|| panic!("no link for {reifier}: {got:#?}"))[1]
                    .clone()
            };
            assert!(
                term("c1").contains("2.25") && !term("c1").contains("1.5"),
                "{got:#?}"
            );
            assert!(
                term("c3").contains("other") && term("c3").contains("-1.25"),
                "{got:#?}"
            );
            let got = audit_links(ledger2.clone()).await;
            assert_eq!(got.len(), 1, "{got:#?}");
            assert!(
                got[0][1].contains("3.75") && !got[0][1].contains("1.5"),
                "{got:#?}"
            );

            let got = run_link_query(
                &fluree,
                &ledger2,
                "SELECT ?src WHERE { << ex:other ex:embedding \
                 \"[0.5, -1.25]\"^^<https://ns.flur.ee/db#embeddingVector> >> ex:source ?src }"
                    .to_string(),
            )
            .await;
            assert_eq!(got, vec![vec!["ex:model".to_string()]], "{got:#?}");
        })
        .await;
}

/// Live links per inner predicate, as the index root's stats carry them.
fn link_counts(ledger: &LedgerState) -> Option<Vec<(String, u64)>> {
    let links = ledger.snapshot.stats.as_ref()?.links.as_ref()?;
    Some(links.iter().map(|l| (l.sid.1.clone(), l.count)).collect())
}

/// The link scan's planner estimate, read from the explain plan.
async fn link_scan_estimate(fluree: &fluree_db_api::Fluree, ledger: &LedgerState) -> i64 {
    let db = fluree_db_api::GraphDb::from_ledger_state(ledger);
    let plan = fluree
        .explain_sparql(
            &db,
            "PREFIX ex: <http://example.org/>\n\
             SELECT ?src WHERE { << ?s ex:knows ?o >> ex:source ?src }",
        )
        .await
        .expect("explain");
    plan["plan"]["logical"]
        .as_array()
        .expect("logical plan")
        .iter()
        .find(|n| {
            n["pattern"]["property"]
                .as_str()
                .is_some_and(|p| p.ends_with("#reifies"))
        })
        .unwrap_or_else(|| panic!("no link scan: {plan:#}"))["estimate"]["row-count"]
        .as_i64()
        .expect("row count")
}

/// Every build publishes live links per inner predicate, and the planner
/// ranks a link scan pinned to an inner predicate by its count: import and
/// reindex count from the whole history, an incremental build moves the
/// base's counts by the window's link asserts and retracts.
#[tokio::test]
async fn link_counts_follow_every_build() {
    use fluree_db_indexer::IndexerConfig;
    let expected = |knows: u64| Some(vec![("age".to_string(), 1), ("knows".to_string(), knows)]);

    let alias = "it/triple-term-links:link-counts";
    // A second file restates claim1: the merge collapses the duplicate link
    // row, and the count must not keep it.
    let restated = "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n\
                    ex:alice ex:knows ex:bob ~ ex:claim1 .\n";
    let (fluree, ledger) = import(&[("a.ttl", CLAIMS), ("b.ttl", restated)], alias).await;
    assert_eq!(link_counts(&ledger), expected(3));
    assert_eq!(link_scan_estimate(&fluree, &ledger).await, 3);
    fluree
        .reindex(alias, fluree_db_api::ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(alias).await.expect("reload");
    assert_eq!(link_counts(&ledger), expected(3));

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/triple-term-links:link-counts-incremental";
    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());
    local
        .run_until(async {
            let ledger = fluree
                .insert_turtle(support::genesis_ledger(&fluree, ledger_id), CLAIMS)
                .await
                .expect("claims")
                .ledger;
            support::trigger_index_and_wait(&handle, ledger_id, ledger.t()).await;
            support::wait_for_index_application(&fluree, ledger_id, ledger.t()).await;
            let ledger = fluree.ledger(ledger_id).await.expect("reload");
            assert_eq!(link_counts(&ledger), expected(3));

            // +2 knows (claim9, bob knows erin), -1 knows (the cascaded
            // retract), and an age re-point that keeps age at one.
            let ledger = change_claims_without_indexing(&fluree, ledger).await;
            let t = ledger.t();
            support::trigger_index_and_wait(&handle, ledger_id, t).await;
            support::wait_for_index_application(&fluree, ledger_id, t).await;
            let ledger = fluree.ledger(ledger_id).await.expect("reload");
            assert_eq!(ledger.index_t(), t);
            assert_eq!(link_counts(&ledger), expected(4));
            assert_eq!(link_scan_estimate(&fluree, &ledger).await, 4);
        })
        .await;
}

/// A ledger indexed before its first annotation: the incremental build that
/// meets it starts the term dictionary over a base that has none.
#[tokio::test]
async fn first_annotation_after_an_index_without_terms() {
    use fluree_db_indexer::IndexerConfig;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/triple-term-links:first-annotation-incremental";
    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());
    let (store, _guard) = support::span_capture::init_test_tracing();
    local
        .run_until(async {
            let ledger = fluree
                .insert_turtle(
                    support::genesis_ledger(&fluree, ledger_id),
                    "@prefix ex: <http://example.org/> .\nex:alice ex:knows ex:bob .\n",
                )
                .await
                .expect("plain data")
                .ledger;
            support::trigger_index_and_wait(&handle, ledger_id, ledger.t()).await;
            support::wait_for_index_application(&fluree, ledger_id, ledger.t()).await;
            let ledger = fluree.ledger(ledger_id).await.expect("reload");
            let ledger = fluree
                .insert_turtle(ledger, CLAIMS)
                .await
                .expect("claims")
                .ledger;
            let t = ledger.t();
            support::trigger_index_and_wait(&handle, ledger_id, t).await;
            support::wait_for_index_application(&fluree, ledger_id, t).await;
            let ledger = fluree.ledger(ledger_id).await.expect("reload");
            assert_eq!(ledger.index_t(), t);
            assert_eq!(links(&fluree, &ledger).await.len(), 4);
        })
        .await;
    let fallbacks = store.find_events("incremental indexing failed, falling back to full rebuild");
    assert!(
        fallbacks.is_empty(),
        "{:?}",
        fallbacks.iter().map(|e| &e.fields).collect::<Vec<_>>()
    );
}

/// Object-bound reified triples answered by the object-first tree, with
/// what each object lookup stamped.
async fn object_bound_answers(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
) -> (Vec<Vec<Vec<String>>>, Vec<Vec<String>>) {
    let queries = [
        "SELECT ?s ?src WHERE { << ?s ex:knows ex:bob >> ex:source ?src } ORDER BY ?s",
        "SELECT (COUNT(*) AS ?n) WHERE { << ?s ?p ex:bob >> ex:source ?src }",
        "SELECT ?src WHERE { << ?s ex:size 5 >> ex:source ?src }",
        "SELECT ?o ?src WHERE { ex:alice ex:knows ?o . << ?s ex:knows ?o >> ex:source ?src } \
         ORDER BY ?o ?src",
    ];
    // Register the stamp callsite before this thread's subscriber reads it.
    run_link_query(fluree, ledger, queries[0].to_string()).await;
    let (store, _guard) = support::span_capture::init_test_tracing();
    tracing::callsite::rebuild_interest_cache();
    let stamps = || -> Vec<String> {
        store
            .find_events("fast-path outcome")
            .iter()
            .filter(|e| e.fields.get("site").map(String::as_str) == Some("term-object"))
            .filter_map(|e| e.fields.get("outcome").cloned())
            .collect()
    };
    let mut answers = Vec::new();
    let mut outcomes = Vec::new();
    for q in queries {
        let before = stamps().len();
        answers.push(run_link_query(fluree, ledger, q.to_string()).await);
        outcomes.push(stamps().split_off(before));
    }
    (answers, outcomes)
}

fn strings(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(std::string::ToString::to_string).collect())
        .collect()
}

/// A constant or bound term object reads its terms from the object-first
/// reverse tree (a range on the object, and the predicate when fixed), after
/// import, beside novelty, after a reindex and after an incremental build.
#[tokio::test(flavor = "current_thread")]
async fn object_bound_terms_read_the_object_tree() {
    use fluree_db_indexer::IndexerConfig;
    let expected = |extra: bool| {
        let mut knows_bob = vec![vec!["ex:alice".to_string(), "ex:hr".to_string()]];
        if extra {
            knows_bob.push(vec!["ex:erin".to_string(), "ex:web".to_string()]);
        }
        vec![
            knows_bob,
            vec![vec![if extra { "2" } else { "1" }.to_string()]],
            strings(&[&["ex:integer"]]),
            if extra {
                strings(&[
                    &["ex:bob", "ex:hr"],
                    &["ex:bob", "ex:web"],
                    &["ex:carol", "ex:linkedin"],
                ])
            } else {
                strings(&[&["ex:bob", "ex:hr"], &["ex:carol", "ex:linkedin"]])
            },
        ]
    };
    let erin = "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n\
                ex:erin ex:knows ex:bob {| ex:source ex:web |} .\n";
    let all_proceed = |outcomes: &[Vec<String>]| {
        for (i, per_query) in outcomes.iter().enumerate() {
            assert!(
                !per_query.is_empty(),
                "query {i} never read the object tree"
            );
            assert!(
                per_query.iter().all(|o| o == "proceed"),
                "query {i}: {per_query:?}"
            );
        }
    };

    // Enough knows links elsewhere that the object range, not the
    // predicate's link interval, drives.
    let padding: String = std::iter::once("@prefix ex: <http://example.org/> .\n".to_string())
        .chain((0..50).map(|i| {
            format!(
                "ex:p{i} ex:knows ex:q{i} {{| ex:source ex:pad |}} .\n\
                 ex:p{i} ex:size {} {{| ex:source ex:pad |}} .\n",
                i + 100
            )
        }))
        .collect();
    let alias = "it/triple-term-links:object-tree";
    let (fluree, ledger) = import(
        &[
            ("claims.ttl", CLAIMS),
            ("literals.ttl", LITERAL_CLAIMS),
            ("padding.ttl", &padding),
        ],
        alias,
    )
    .await;
    let (answers, outcomes) = object_bound_answers(&fluree, &ledger).await;
    assert_eq!(answers, expected(false));
    all_proceed(&outcomes);

    let ledger = fluree
        .insert_turtle(ledger, erin)
        .await
        .expect("novelty")
        .ledger;
    let (answers, outcomes) = object_bound_answers(&fluree, &ledger).await;
    assert_eq!(answers, expected(true), "novelty terms beside the tree");
    all_proceed(&outcomes);

    fluree
        .reindex(alias, fluree_db_api::ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(alias).await.expect("reload");
    let (answers, outcomes) = object_bound_answers(&fluree, &ledger).await;
    assert_eq!(answers, expected(true), "after reindex");
    all_proceed(&outcomes);

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/triple-term-links:object-tree-incremental";
    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());
    local
        .run_until(async {
            // The first build sees no annotation, so the next one writes the
            // term dictionary over a base that has none.
            let plain = "@prefix ex: <http://example.org/> .\nex:zed ex:knows ex:bob .\n";
            let mut ledger = support::genesis_ledger(&fluree, ledger_id);
            for body in [plain, CLAIMS, LITERAL_CLAIMS, &padding, erin] {
                ledger = fluree
                    .insert_turtle(ledger, body)
                    .await
                    .expect("insert")
                    .ledger;
                let t = ledger.t();
                support::trigger_index_and_wait(&handle, ledger_id, t).await;
                support::wait_for_index_application(&fluree, ledger_id, t).await;
                ledger = fluree.ledger(ledger_id).await.expect("reload");
                assert_eq!(ledger.index_t(), t);
            }
        })
        .await;
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let (answers, outcomes) = object_bound_answers(&fluree, &ledger).await;
    assert_eq!(answers, expected(true), "after incremental builds");
    all_proceed(&outcomes);
}

/// `TRIPLE(s, p, o)` builds the term a link holds: it joins, filters and
/// groups with links whether the index or novelty holds them, a literal
/// component keeps its datatype or tag, and a component of the wrong kind
/// leaves the result unbound.
#[tokio::test]
async fn triple_constructs_the_terms_links_hold() {
    let (fluree, ledger) = import(
        &[("claims.ttl", CLAIMS), ("literals.ttl", LITERAL_CLAIMS)],
        "it/triple-term-links:triple-fn",
    )
    .await;
    let check = |ledger: LedgerState| {
        let fluree = &fluree;
        async move {
            let run = |body: &str| run_link_query(fluree, &ledger, body.to_string());
            for body in [
                "SELECT ?r WHERE { BIND(TRIPLE(ex:alice, ex:knows, ex:bob) AS ?t) ?r rdf:reifies ?t }",
                "SELECT ?r WHERE { ?r rdf:reifies ?t FILTER(?t = TRIPLE(ex:alice, ex:knows, ex:bob)) }",
                "SELECT ?r WHERE { BIND(<<( ex:alice ex:knows ex:bob )>> AS ?t) ?r rdf:reifies ?t }",
            ] {
                assert_eq!(run(body).await, strings(&[&["ex:claim1"]]), "{body}");
            }
            for (object, source) in [
                ("\"5\"^^xsd:int", "ex:int"),
                ("5", "ex:integer"),
                ("\"chat\"@fr", "ex:fr"),
            ] {
                let body = format!(
                    "SELECT ?src WHERE {{ BIND(TRIPLE(ex:doc, ?p, {object}) AS ?t) \
                     ?r rdf:reifies ?t ; ex:source ?src \
                     VALUES ?p {{ ex:size ex:title }} }}"
                );
                assert_eq!(run(&body).await, strings(&[&[source]]), "{object}");
            }
            // The constructed term and the link's are one group.
            let got = run(
                "SELECT (COUNT(*) AS ?n) WHERE { { ?r rdf:reifies ?t } UNION \
                 { BIND(TRIPLE(ex:alice, ex:knows, ex:bob) AS ?t) } } \
                 GROUP BY ?t HAVING (COUNT(*) > 1)",
            )
            .await;
            assert_eq!(got, strings(&[&["2"]]), "{got:?}");
        }
    };
    check(ledger.clone()).await;
    let ledger = fluree
        .insert_turtle(
            ledger,
            "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n\
             ex:erin ex:knows ex:frank ~ ex:claim9 {| ex:source ex:web |} .\n",
        )
        .await
        .expect("novelty")
        .ledger;
    check(ledger.clone()).await;
    let got = run_link_query(
        &fluree,
        &ledger,
        "SELECT ?r WHERE { BIND(TRIPLE(ex:erin, ex:knows, ex:frank) AS ?t) ?r rdf:reifies ?t }"
            .to_string(),
    )
    .await;
    assert_eq!(got, strings(&[&["ex:claim9"]]), "a term only novelty holds");

    let got = run_link_query(
        &fluree,
        &ledger,
        "SELECT (isTRIPLE(?t) AS ?is) (SUBJECT(?t) AS ?s) (DATATYPE(OBJECT(?t)) AS ?dt) \
         (isTRIPLE(OBJECT(?n)) AS ?nested) (BOUND(?bad1) AS ?b1) (BOUND(?bad2) AS ?b2) \
         (BOUND(?bad3) AS ?b3) WHERE { \
         BIND(TRIPLE(ex:a, ex:b, \"5\"^^xsd:int) AS ?t) \
         BIND(TRIPLE(ex:r, ex:says, ?t) AS ?n) \
         BIND(TRIPLE(\"lit\", ex:b, ex:c) AS ?bad1) \
         BIND(TRIPLE(ex:a, \"x\", ex:c) AS ?bad2) \
         BIND(TRIPLE(ex:a, BNODE(), ex:c) AS ?bad3) }"
            .to_string(),
    )
    .await;
    assert_eq!(
        got,
        strings(&[&["true", "ex:a", "xsd:int", "true", "false", "false", "false"]])
    );
}

/// A SHACL SPARQL constraint over a quoted pattern validates the post-state
/// with the transaction's own annotations in it, as a committed read would.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_sparql_constraints_see_the_transactions_links() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "it/triple-term-links:shacl-staged");
    let ledger = fluree
        .insert_turtle(
            ledger,
            r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .
ex:AliceShape a sh:NodeShape ;
    sh:targetNode ex:alice ;
    sh:sparql ex:noMallory .
ex:noMallory sh:message "no claim may say alice knows mallory" ;
    sh:select "SELECT $this WHERE { << $this <http://example.org/knows> <http://example.org/mallory> >> <http://example.org/source> ?src }" .
"#,
        )
        .await
        .expect("shape")
        .ledger;
    let claim = |o: &str| {
        format!(
            "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n\
             ex:alice ex:knows ex:{o} {{| ex:source ex:web |}} .\n"
        )
    };
    let ledger = fluree
        .insert_turtle(ledger, &claim("bob"))
        .await
        .expect("a conforming claim")
        .ledger;
    let rejected = fluree.insert_turtle(ledger, &claim("mallory")).await;
    assert!(
        rejected.is_err(),
        "the staged annotation must violate the shape"
    );
}

/// A preview of a staged transaction reads the links of its own
/// annotations, over an index and over a ledger never indexed.
#[tokio::test]
async fn previews_read_the_links_of_their_own_annotations() {
    let (fluree, indexed) = import(&[("claims.ttl", CLAIMS)], "it/triple-term-links:preview").await;
    let memory = FlureeBuilder::memory().build_memory();
    let never_indexed = memory
        .insert_turtle(
            support::genesis_ledger(&memory, "it/triple-term-links:preview-memory"),
            CLAIMS,
        )
        .await
        .expect("claims")
        .ledger;
    for (fluree, ledger) in [(&fluree, indexed), (&memory, never_indexed)] {
        let staged = fluree
            .stage_owned(ledger)
            .upsert_turtle(
                "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n\
                 ex:erin ex:knows ex:frank ~ ex:claim9 {| ex:source ex:web |} .\n",
            )
            .stage()
            .await
            .expect("stage");
        let preview = fluree_db_api::GraphDb::from_staged(&staged).expect("preview");
        let result = fluree
            .query(
                &preview,
                "PREFIX ex: <http://example.org/>\n\
                 SELECT ?s WHERE { << ?s ex:knows ex:frank >> ex:source ex:web }",
            )
            .await
            .expect("preview query");
        let got = rows(
            &result
                .to_jsonld_async(preview.as_graph_db_ref())
                .await
                .expect("format"),
        );
        assert_eq!(got, strings(&[&["ex:erin"]]));
    }
}

/// A triple term renders as SPARQL 1.2's `triple` term in the SPARQL result
/// formats, as `<<( s p o )>>` in delimited text, and as a JSON-LD-star
/// embedded node in the JSON-LD formats, whichever writer (DOM or streaming)
/// produces it.
#[tokio::test]
async fn triple_terms_render_in_every_result_format() {
    use fluree_db_api::format::{format_results_string, FormatterConfig};
    let (fluree, ledger) = import(
        &[("claims.ttl", CLAIMS), ("literals.ttl", LITERAL_CLAIMS)],
        "it/triple-term-links:formats",
    )
    .await;
    let select = "PREFIX ex: <http://example.org/>\n\
                  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
                  SELECT ?src ?t WHERE { ?r rdf:reifies ?t ; ex:source ?src \
                  VALUES ?src { ex:hr ex:fr ex:int } } ORDER BY ?src";
    let result = support::query_sparql(&fluree, &ledger, select)
        .await
        .expect("link query");
    let string = |config: FormatterConfig| {
        format_results_string(&result, &result.context, &ledger.snapshot, &config).expect("format")
    };
    let parsed = |config: FormatterConfig| -> JsonValue {
        serde_json::from_str(&string(config)).expect("JSON output")
    };

    let iri = |l: &str| json!({"type": "uri", "value": format!("http://example.org/{l}")});
    let triple = |s: &str, p: &str, o: JsonValue| json!({"type": "triple", "value": {"subject": iri(s), "predicate": iri(p), "object": o}});
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("SPARQL JSON");
    let terms: Vec<&JsonValue> = sparql_json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| &b["t"])
        .collect();
    assert_eq!(
        terms,
        [
            &triple(
                "doc",
                "title",
                json!({"type": "literal", "value": "chat", "xml:lang": "fr"})
            ),
            &triple("alice", "knows", iri("bob")),
            &triple(
                "doc",
                "size",
                json!({"type": "literal", "value": "5",
                       "datatype": "http://www.w3.org/2001/XMLSchema#int"})
            ),
        ]
    );
    assert_eq!(parsed(FormatterConfig::sparql_json()), sparql_json);

    let xml = string(FormatterConfig::sparql_xml());
    for term in [
        "<triple><subject><uri>http://example.org/doc</uri></subject>\
         <predicate><uri>http://example.org/title</uri></predicate>\
         <object><literal xml:lang=\"fr\">chat</literal></object></triple>",
        "<triple><subject><uri>http://example.org/alice</uri></subject>\
         <predicate><uri>http://example.org/knows</uri></predicate>\
         <object><uri>http://example.org/bob</uri></object></triple>",
        "<object><literal datatype=\"http://www.w3.org/2001/XMLSchema#int\">5</literal></object>",
    ] {
        assert!(xml.contains(term), "{term} in {xml}");
    }

    let tsv = result.to_tsv(&ledger.snapshot).expect("TSV");
    assert!(
        tsv.contains(
            "\t<<( http://example.org/alice http://example.org/knows http://example.org/bob )>>\n"
        ),
        "{tsv}"
    );
    assert!(
        tsv.contains("\t<<( http://example.org/doc http://example.org/title \"chat\"@fr )>>\n"),
        "{tsv}"
    );
    let csv = result.to_csv(&ledger.snapshot).expect("CSV");
    assert!(
        csv.contains(
            ",\"<<( http://example.org/doc http://example.org/title \"\"chat\"\"@fr )>>\""
        ),
        "{csv}"
    );

    let node = |s: &str, p: &str, o: JsonValue| json!({"@id": {"@id": s, p: o}});
    let int = json!({"@value": 5, "@type": "http://www.w3.org/2001/XMLSchema#int"});
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("JSON-LD");
    assert_eq!(
        jsonld,
        json!([
            [
                "ex:fr",
                node(
                    "ex:doc",
                    "ex:title",
                    json!({"@value": "chat", "@language": "fr"})
                )
            ],
            [
                "ex:hr",
                node("ex:alice", "ex:knows", json!({"@id": "ex:bob"}))
            ],
            ["ex:int", node("ex:doc", "ex:size", int.clone())],
        ])
    );
    assert_eq!(parsed(FormatterConfig::jsonld()), jsonld);
    let typed = result.to_typed_json(&ledger.snapshot).expect("typed JSON");
    assert_eq!(
        typed[1]["?t"],
        node("ex:alice", "ex:knows", json!({"@id": "ex:bob"}))
    );
    assert_eq!(typed[2]["?t"], node("ex:doc", "ex:size", int));
    assert_eq!(parsed(FormatterConfig::typed_json()), typed);
}

/// `?r rdf:reifies ?t` in a CONSTRUCT template, with a triple term bound to
/// ?t, writes what the explicit `<<( s p o )>>` template writes. Under any
/// other predicate the term (which the graph model cannot hold as an object)
/// is written as its N-Triples text.
#[tokio::test]
async fn construct_writes_triple_terms_as_reifications() {
    const REIFIES: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies>";
    use fluree_db_api::format::{format_results_string, FormatterConfig};
    let (fluree, ledger) = import(
        &[("claims.ttl", CLAIMS), ("literals.ttl", LITERAL_CLAIMS)],
        "it/triple-term-links:construct",
    )
    .await;
    let render = |result: &fluree_db_api::QueryResult, config: FormatterConfig| {
        format_results_string(result, &result.context, &ledger.snapshot, &config).expect("format")
    };
    let construct = |template: &'static str, filter: &'static str| {
        let fluree = &fluree;
        let ledger = &ledger;
        async move {
            let sparql = format!(
                "PREFIX ex: <http://example.org/>\n\
                 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
                 CONSTRUCT {{ {template} }} WHERE {{ {filter} }}"
            );
            support::query_sparql(fluree, ledger, &sparql)
                .await
                .expect("construct")
        }
    };
    let sorted = |result: &fluree_db_api::QueryResult| {
        let mut lines: Vec<String> = render(result, FormatterConfig::ntriples())
            .lines()
            .map(str::to_string)
            .collect();
        lines.sort();
        lines
    };

    for (term_form, template_form) in [
        (
            "?r rdf:reifies ?t ; ex:source ex:hr",
            "?r rdf:reifies <<( ?s ?p ?o )>> ; ex:source ex:hr",
        ),
        (
            "?r rdf:reifies ?t ; ex:source ex:fr",
            "?r rdf:reifies <<( ?s ?p ?o )>> ; ex:source ex:fr",
        ),
    ] {
        let by_term = construct("?r rdf:reifies ?t", term_form).await;
        let by_template = construct("?r rdf:reifies <<( ?s ?p ?o )>>", template_form).await;
        let lines = sorted(&by_term);
        assert_eq!(
            lines.len(),
            2,
            "the base triple and its reification: {lines:#?}"
        );
        assert_eq!(lines, sorted(&by_template), "{term_form}");
        assert_eq!(
            by_term.to_construct(&ledger.snapshot).expect("JSON-LD"),
            by_template.to_construct(&ledger.snapshot).expect("JSON-LD"),
            "{term_form}"
        );
    }
    let hr = construct("?r rdf:reifies ?t", "?r rdf:reifies ?t ; ex:source ex:hr").await;
    assert_eq!(
        hr.to_construct(&ledger.snapshot).expect("JSON-LD")["@graph"],
        json!([{"@id": "ex:alice", "ex:knows": [{"@id": "ex:bob", "@annotation": {"@id": "ex:claim1"}}]}])
    );

    // The shorthand's template is the WHERE clause: a constant quoted triple
    // lowers to a link with a constant term, written the same way.
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  CONSTRUCT WHERE { << ex:alice ex:knows ex:bob ~ ?r >> ex:source ?src }";
    let shorthand = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("construct where");
    let ex = |l: &str| format!("<http://example.org/{l}>");
    let mut expected = vec![
        format!("{} {} {} .", ex("alice"), ex("knows"), ex("bob")),
        format!(
            "{} {REIFIES} <<( {} {} {} )>> .",
            ex("claim1"),
            ex("alice"),
            ex("knows"),
            ex("bob")
        ),
        format!("{} {} {} .", ex("claim1"), ex("source"), ex("hr")),
    ];
    expected.sort();
    assert_eq!(sorted(&shorthand), expected);

    let about = construct("?r ex:about ?t", "?r rdf:reifies ?t ; ex:source ex:fr").await;
    let nt = render(&about, FormatterConfig::ntriples());
    assert!(
        nt.contains(
            r#" <http://example.org/about> "<<( <http://example.org/doc> <http://example.org/title> \"chat\"@fr )>>"^^"#
        ),
        "{nt}"
    );
}

/// An annotated ledger whose index predates links (simulated by dropping the
/// root's term dictionary) refuses link reads until a full rebuild links its
/// annotations. An incremental build in between does not start a term
/// dictionary, which would cover its window alone and lift the refusal.
#[tokio::test]
async fn link_reads_refuse_an_index_built_before_links() {
    use fluree_db_binary_index::format::index_root::IndexRoot;
    use fluree_db_core::{ContentKind, ContentStore};

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/triple-term-links:pre-link";
    let claim = |n: u32| {
        format!(
            "VERSION \"1.2\"\n@prefix ex: <http://example.org/> .\n\
             ex:s{n} ex:knows ex:o{n} ~ ex:claim{n} {{| ex:src ex:x |}} .\n"
        )
    };
    let current_root = || async {
        let record = fluree
            .nameservice()
            .lookup(ledger_id)
            .await
            .expect("ns lookup")
            .expect("ns record");
        let cid = record.index_head_id.clone().expect("index root");
        let bytes = fluree
            .content_store(ledger_id)
            .get(&cid)
            .await
            .expect("root");
        (IndexRoot::decode(&bytes).expect("decode"), record.index_t)
    };
    let query = "PREFIX ex: <http://example.org/>\n\
                 SELECT ?r WHERE { << ?s ex:knows ?o ~ ?r >> ex:src ex:x } ORDER BY ?r";

    let ledger = support::genesis_ledger(&fluree, ledger_id);
    fluree
        .upsert_turtle(ledger, &claim(1))
        .await
        .expect("claim 1");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;

    let (mut root, index_t) = current_root().await;
    assert!(root.has_annotations && root.term_dict.is_some());
    root.term_dict = None;
    let cid = fluree
        .content_store(ledger_id)
        .put(ContentKind::IndexRoot, &root.encode())
        .await
        .expect("put root");
    fluree
        .publisher()
        .expect("read-write nameservice")
        .publish_index_allow_equal(ledger_id, index_t, &cid)
        .await
        .expect("publish root");

    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let err = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect_err("a pre-link index refuses link reads");
    assert!(err.to_string().contains("fluree reindex"), "{err}");

    fluree
        .upsert_turtle(ledger, &claim(2))
        .await
        .expect("claim 2");
    support::build_and_publish_index(&fluree, ledger_id).await;
    let (root, _) = current_root().await;
    assert!(
        root.term_dict.is_none(),
        "an incremental build over a pre-link index must not start a term dictionary"
    );
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    assert!(support::query_sparql(&fluree, &ledger, query)
        .await
        .is_err());

    // Same `t` as the incremental root, so it publishes with allow-equal.
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("ns record");
    let rebuilt = fluree_db_indexer::rebuild_index_from_commits(
        fluree.content_store(ledger_id),
        ledger_id,
        &record,
        fluree_db_indexer::IndexerConfig::default(),
    )
    .await
    .expect("rebuild");
    fluree
        .publisher()
        .expect("read-write nameservice")
        .publish_index_allow_equal(ledger_id, rebuilt.index_t, &rebuilt.root_id)
        .await
        .expect("publish rebuild");
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let result = support::query_sparql_formatted(&fluree, &ledger, query)
        .await
        .expect("a rebuilt index answers");
    assert_eq!(rows(&result), strings(&[&["ex:claim1"], &["ex:claim2"]]));
}

/// The JSON-LD twin of `triple_constructs_the_terms_links_hold`: the
/// triple-term functions under their JSON-LD names, in `bind` and `filter`.
#[tokio::test]
async fn jsonld_triple_term_functions() {
    let (fluree, ledger) =
        import(&[("claims.ttl", CLAIMS)], "it/triple-term-links:jsonld-fns").await;
    let ctx = json!({
        "ex": "http://example.org/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    });
    let run = |query: JsonValue| {
        let fluree = &fluree;
        let ledger = &ledger;
        async move {
            let result = support::query_jsonld_formatted(fluree, ledger, &query)
                .await
                .unwrap_or_else(|e| panic!("{query}: {e:?}"));
            rows(&result)
        }
    };

    let built = run(json!({
        "@context": ctx,
        "select": ["?r"],
        "where": [
            ["bind", "?t", "(triple ex:alice ex:knows ex:bob)"],
            {"@id": "?r", "rdf:reifies": "?t"}
        ]
    }))
    .await;
    assert_eq!(built, strings(&[&["ex:claim1"]]));

    let decomposed = run(json!({
        "@context": ctx,
        "select": ["?s", "?p", "?o"],
        "where": [
            {"@id": "ex:claim1", "rdf:reifies": "?t"},
            ["bind", "?s", "(subject ?t)"],
            ["bind", "?p", "(predicate ?t)"],
            ["bind", "?o", "(object ?t)"]
        ]
    }))
    .await;
    assert_eq!(decomposed, strings(&[&["ex:alice", "ex:knows", "ex:bob"]]));

    let filtered = run(json!({
        "@context": ctx,
        "select": ["?r"],
        "where": [
            {"@id": "?r", "rdf:reifies": "?t"},
            ["filter", "(isTriple ?t)"],
            ["filter", "(sameTerm (predicate ?t) ex:age)"]
        ]
    }))
    .await;
    assert_eq!(filtered, strings(&[&["ex:claim2"]]));
}

/// TriG import writes an annotation inside a `GRAPH` block through its own
/// named-graph loop; that loop spools the link as the default-graph sink does.
#[tokio::test]
async fn trig_import_links_named_graph_annotations() {
    const TRIG: &str = r#"@prefix ex: <http://example.org/> .
ex:alice ex:name "Alice" .
GRAPH <http://example.org/graphs/audit> {
    ex:event1 ex:actor ex:alice {| ex:confidence "high" |} .
    ex:event2 ex:actor ex:alice ~ ex:claim2 {| ex:confidence "low"@en |} .
}
"#;
    let (fluree, ledger) = import(&[("data.trig", TRIG)], "it/triple-term-links:trig").await;
    let got = run_link_query(
        &fluree,
        &ledger,
        "SELECT ?s WHERE { GRAPH <http://example.org/graphs/audit> \
         { ?r rdf:reifies ?t BIND(SUBJECT(?t) AS ?s) } } ORDER BY ?s"
            .to_string(),
    )
    .await;
    assert_eq!(got, strings(&[&["ex:event1"], &["ex:event2"]]));
}
