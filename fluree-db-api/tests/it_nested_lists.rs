//! Nested lists: a list holding lists is stored as one tree, its values
//! values of the property that holds it, whatever syntax writes it.

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState, QueryExecutionOptions};
use serde_json::{json, Value as JsonValue};

const PREFIX: &str = "PREFIX ex: <http://example.org/>\n\
                      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n";

async fn create(fluree: &fluree_db_api::Fluree, name: &str) -> String {
    let id = format!("it/nested-lists/{name}:main");
    fluree.create_ledger(&id).await.expect("create");
    id
}

async fn turtle(fluree: &fluree_db_api::Fluree, id: &str, ttl: &str) {
    fluree
        .graph(id)
        .transact()
        .insert_turtle(&format!("@prefix ex: <http://example.org/> .\n{ttl}"))
        .commit()
        .await
        .unwrap_or_else(|e| panic!("insert: {e}\n{ttl}"));
}

async fn update(fluree: &fluree_db_api::Fluree, id: &str, body: &str) {
    fluree
        .graph(id)
        .transact()
        .sparql_update(&format!("{PREFIX}{body}"))
        .commit()
        .await
        .unwrap_or_else(|e| panic!("update: {e}\n{body}"));
}

async fn insert_jsonld(fluree: &fluree_db_api::Fluree, id: &str, node: JsonValue) {
    let txn = json!({"@context": {"ex": "http://example.org/"}, "insert": node});
    fluree
        .graph(id)
        .transact()
        .update(&txn)
        .commit()
        .await
        .unwrap_or_else(|e| panic!("insert: {e}\n{txn}"));
}

async fn load(fluree: &fluree_db_api::Fluree, id: &str) -> LedgerState {
    fluree.ledger(id).await.expect("load")
}

/// `ex:s`'s `ex:p`, as a JSON-LD crawl returns it.
async fn crawl(fluree: &fluree_db_api::Fluree, id: &str) -> JsonValue {
    let ledger = load(fluree, id).await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": {"ex:s": ["*"]}
    });
    let out = support::query_jsonld_formatted(fluree, &ledger, &query)
        .await
        .unwrap_or_else(|e| panic!("crawl: {e}"));
    out[0]["ex:p"].clone()
}

/// The rows of a SPARQL query, each a sorted array of its values.
async fn sparql(fluree: &fluree_db_api::Fluree, id: &str, text: &str) -> Vec<JsonValue> {
    let ledger = load(fluree, id).await;
    let text = format!("{PREFIX}{text}");
    let result = support::query_sparql(fluree, &ledger, &text)
        .await
        .unwrap_or_else(|e| panic!("query: {e}\n{text}"))
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    support::normalize_rows(&result)
}

fn tree() -> JsonValue {
    json!({"@list": [{"@list": [1, 2]}, 3]})
}

/// Turtle (inserted or upserted), a SPARQL update and JSON-LD write
/// `((1 2) 3)` as one list: its
/// values are values of `ex:p`, and no `rdf:first` triple is stored.
#[tokio::test]
async fn every_syntax_stores_a_nested_list_as_one_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ttl = create(&fluree, "ttl").await;
    turtle(&fluree, &ttl, "ex:s ex:p ((1 2) 3) .").await;
    let sparql_update = create(&fluree, "sparql").await;
    update(
        &fluree,
        &sparql_update,
        "INSERT DATA { ex:s ex:p ((1 2) 3) }",
    )
    .await;
    let upsert = create(&fluree, "upsert").await;
    fluree
        .upsert_turtle(
            load(&fluree, &upsert).await,
            "@prefix ex: <http://example.org/> .\nex:s ex:p ((1 2) 3) .",
        )
        .await
        .expect("upsert");
    let jsonld = create(&fluree, "jsonld").await;
    insert_jsonld(&fluree, &jsonld, json!({"@id": "ex:s", "ex:p": tree()})).await;
    let spelled = create(&fluree, "spelled").await;
    turtle(
        &fluree,
        &spelled,
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         ex:s ex:p _:c0 .\n\
         _:c0 rdf:first _:d0 ; rdf:rest _:c1 .\n\
         _:c1 rdf:first 3 ; rdf:rest rdf:nil .\n\
         _:d0 rdf:first 1 ; rdf:rest _:d1 .\n\
         _:d1 rdf:first 2 ; rdf:rest rdf:nil .",
    )
    .await;

    for id in [&ttl, &upsert, &sparql_update, &jsonld, &spelled] {
        assert_eq!(crawl(&fluree, id).await, tree(), "{id}");
        let values = sparql(&fluree, id, "SELECT ?o WHERE { ex:s ex:p ?o }").await;
        assert_eq!(values, [json!([1]), json!([2]), json!([3])], "{id}");
        let cells = sparql(&fluree, id, "SELECT ?c WHERE { ?c rdf:first ?o }").await;
        assert!(cells.is_empty(), "{id}: {cells:?}");
    }
}

/// Depth alone does not tell `((1 2))` from `((1) (2))`.
#[tokio::test]
async fn lists_side_by_side_stay_apart() {
    let fluree = FlureeBuilder::memory().build_memory();
    let one = create(&fluree, "one-inner").await;
    turtle(&fluree, &one, "ex:s ex:p ((1 2)) .").await;
    let two = create(&fluree, "two-inner").await;
    turtle(&fluree, &two, "ex:s ex:p ((1) (2)) .").await;
    assert_eq!(
        crawl(&fluree, &one).await,
        json!({"@list": [{"@list": [1, 2]}]})
    );
    assert_eq!(
        crawl(&fluree, &two).await,
        json!({"@list": [{"@list": [1]}, {"@list": [2]}]})
    );
}

/// Writing a nested list a property holds again restates it; one that
/// differs only in its shape is another list.
#[tokio::test]
async fn writing_a_nested_list_again_restates_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "restate").await;
    turtle(&fluree, &id, "ex:s ex:p ((1 2) 3) .").await;
    turtle(&fluree, &id, "ex:s ex:p ((1 2) 3) .").await;
    insert_jsonld(&fluree, &id, json!({"@id": "ex:s", "ex:p": tree()})).await;
    assert_eq!(crawl(&fluree, &id).await, tree());
    turtle(&fluree, &id, "ex:s ex:p ((1) 2 3) .").await;
    let lists = crawl(&fluree, &id).await;
    let lists = lists.as_array().expect("two lists");
    assert_eq!(lists.len(), 2, "{lists:?}");
    assert!(lists.contains(&tree()), "{lists:?}");
    assert!(
        lists.contains(&json!({"@list": [{"@list": [1]}, 2, 3]})),
        "{lists:?}"
    );
}

/// An empty inner list is `rdf:nil`, a value of the list.
#[tokio::test]
async fn an_empty_inner_list_is_rdf_nil() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ttl = create(&fluree, "nil-ttl").await;
    turtle(&fluree, &ttl, "ex:s ex:p ((() 1) 2) .").await;
    let jsonld = create(&fluree, "nil-jsonld").await;
    insert_jsonld(
        &fluree,
        &jsonld,
        json!({"@id": "ex:s", "ex:p": {"@list": [{"@list": [{"@list": []}, 1]}, 2]}}),
    )
    .await;
    let want = crawl(&fluree, &ttl).await;
    assert_eq!(crawl(&fluree, &jsonld).await, want);
    let inner = &want["@list"][0]["@list"];
    assert_eq!(inner.as_array().map(Vec::len), Some(2), "{want}");
    assert_eq!(inner[1], json!(1), "{want}");
}

/// A JSON-LD list nested deeper than a tree can hold is refused.
#[tokio::test]
async fn a_list_nested_too_deep_is_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "too-deep").await;
    let mut deep = json!(1);
    for _ in 0..=16 {
        deep = json!({"@list": [deep]});
    }
    let txn = json!({
        "@context": {"ex": "http://example.org/"},
        "insert": {"@id": "ex:s", "ex:p": deep}
    });
    let err = fluree
        .graph(&id)
        .transact()
        .update(&txn)
        .commit()
        .await
        .expect_err("refused");
    assert!(err.to_string().contains("more than 15 deep"), "{err}");
}

/// A ledger holding `ttl` in novelty, and one holding it in its index.
async fn novelty_and_index(fluree: &fluree_db_api::Fluree, name: &str, ttl: &str) -> [String; 2] {
    let novelty = create(fluree, &format!("{name}-novelty")).await;
    turtle(fluree, &novelty, ttl).await;
    let indexed = create(fluree, &format!("{name}-index")).await;
    turtle(fluree, &indexed, ttl).await;
    support::rebuild_and_publish_index(fluree, &indexed).await;
    [novelty, indexed]
}

/// A walk along `rdf:rest*/rdf:first` reaches a nested list's items as its
/// RDF does: the inner list is a cell.
#[tokio::test]
async fn a_walk_reads_a_nested_list_as_its_rdf() {
    let fluree = FlureeBuilder::memory().build_memory();
    for id in novelty_and_index(&fluree, "walk", "ex:s ex:p ((1 2) 3) .").await {
        let top = sparql(
            &fluree,
            &id,
            "SELECT ?x WHERE { ex:s ex:p/rdf:rest*/rdf:first ?x }",
        )
        .await;
        assert_eq!(top.len(), 2, "{id}: {top:?}");
        assert!(top.contains(&json!([3])), "{id}: {top:?}");
        assert!(
            top.iter()
                .any(|r| r[0].as_str().is_some_and(|s| s.starts_with("_:"))),
            "{id}: the inner list is a cell: {top:?}"
        );
    }
}

/// The RDF view, which the Graph Store `GET` reads through, holds a nested
/// list's owner edge and both lists' cells.
#[tokio::test]
async fn the_rdf_view_holds_a_nested_lists_cells() {
    let fluree = FlureeBuilder::memory().build_memory();
    for id in novelty_and_index(&fluree, "view", "ex:s ex:p ((1 2) 3) .").await {
        let ledger = load(&fluree, &id).await;
        let rows = |text: &str| {
            let text = format!("{PREFIX}{text}");
            let ledger = &ledger;
            let fluree = &fluree;
            async move {
                let result = support::graphdb_from_ledger(ledger)
                    .query(fluree)
                    .sparql(&text)
                    .execution_options(QueryExecutionOptions::new().with_rdf_list_view(true))
                    .execute()
                    .await
                    .unwrap_or_else(|e| panic!("query: {e}\n{text}"))
                    .to_jsonld(&ledger.snapshot)
                    .expect("to_jsonld");
                support::normalize_rows(&result)
            }
        };
        let all = rows("SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await;
        assert_eq!(all.len(), 1 + 2 * 2 + 2 * 2, "{id}: {all:?}");
        let spelled = rows("SELECT ?a ?b ?c WHERE { ex:s ex:p ((?a ?b) ?c) }").await;
        assert_eq!(spelled, [json!([1, 2, 3])], "{id}");
        let ends = rows("SELECT ?c WHERE { ?c rdf:rest rdf:nil }").await;
        assert_eq!(ends.len(), 2, "{id}: each list's last cell: {ends:?}");
        let inner =
            rows("SELECT ?x WHERE { ex:s ex:p/rdf:rest*/rdf:first/rdf:rest*/rdf:first ?x }").await;
        assert_eq!(inner, [json!([1]), json!([2])], "{id}");
        let head = rows("SELECT ?x WHERE { ex:s ex:p/rdf:first/rdf:first ?x }").await;
        assert_eq!(head, [json!([1])], "{id}");
        let back = rows("SELECT ?o WHERE { ?o ex:p/rdf:first/rdf:rest/rdf:first 2 }").await;
        assert_eq!(back, [json!(["ex:s"])], "{id}");
        let up = rows("SELECT ?o WHERE { ?o ^rdf:first/^ex:p ex:s }").await;
        assert_eq!(up.len(), 1, "{id}: the inner list's head: {up:?}");
    }
}

/// Export writes a nested list as Turtle, JSON-LD and N-Triples spell one,
/// and each reads back as the same list.
#[tokio::test]
async fn export_writes_a_nested_list_that_reads_back() {
    use fluree_db_api::export::ExportFormat;
    let fluree = FlureeBuilder::memory().build_memory();
    for id in novelty_and_index(&fluree, "export", "ex:s ex:p ((1 2) 3) .").await {
        for format in [
            ExportFormat::Turtle,
            ExportFormat::JsonLd,
            ExportFormat::NTriples,
        ] {
            let mut out = Vec::new();
            fluree
                .export(&id)
                .format(format)
                .context(&json!({"ex": "http://example.org/"}))
                .write_to(&mut out)
                .await
                .expect("export");
            let text = String::from_utf8(out).expect("UTF-8");
            let copy = create(
                &fluree,
                &format!("{}-{format:?}", id.replace([':', '/'], "-")),
            )
            .await;
            match format {
                ExportFormat::JsonLd => {
                    let doc: JsonValue = serde_json::from_str(&text).expect("JSON-LD");
                    fluree
                        .graph(&copy)
                        .transact()
                        .insert(&doc)
                        .commit()
                        .await
                        .unwrap_or_else(|e| panic!("{format:?}: {e}\n{text}"));
                }
                _ => {
                    fluree
                        .graph(&copy)
                        .transact()
                        .insert_turtle(&text)
                        .commit()
                        .await
                        .unwrap_or_else(|e| panic!("{format:?}: {e}\n{text}"));
                }
            }
            if format == ExportFormat::Turtle {
                assert!(text.contains("ex:p ( ( "), "{id}: {text}");
            }
            assert_eq!(
                crawl(&fluree, &copy).await,
                tree(),
                "{id} {format:?}: {text}"
            );
        }
    }
}

/// Deleting a value repairs the shape around it: the leaf after it keeps
/// the lists it was in.
#[tokio::test]
async fn deleting_a_value_keeps_the_lists_around_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "delete-repair").await;
    turtle(&fluree, &id, "ex:s ex:p ((1) (2 3)) .").await;
    update(&fluree, &id, "DELETE DATA { ex:s ex:p 2 }").await;
    assert_eq!(
        crawl(&fluree, &id).await,
        json!({"@list": [{"@list": [1]}, {"@list": [3]}]})
    );
}

/// A JSON-LD delete naming a nested list deletes the list with its shape,
/// not another with the same values.
#[tokio::test]
async fn deleting_a_nested_list_matches_its_shape() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "delete-shape").await;
    turtle(&fluree, &id, "ex:s ex:p ((1 2) 3), (1 (2 3)) .").await;
    let txn = json!({
        "@context": {"ex": "http://example.org/"},
        "delete": {"@id": "ex:s", "ex:p": tree()}
    });
    fluree
        .graph(&id)
        .transact()
        .update(&txn)
        .commit()
        .await
        .expect("delete");
    assert_eq!(
        crawl(&fluree, &id).await,
        json!({"@list": [1, {"@list": [2, 3]}]})
    );
}

/// An inner list whose cell says more than `rdf:first`/`rdf:rest` keeps its
/// cell, so what the cell says stays reachable.
#[tokio::test]
async fn a_cell_that_says_more_is_not_folded() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "annotated-cell").await;
    turtle(
        &fluree,
        &id,
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         ex:s ex:p (_:inner 3) .\n\
         _:inner rdf:first 1 ; rdf:rest rdf:nil ; ex:note \"keep\" .",
    )
    .await;
    let rows = sparql(
        &fluree,
        &id,
        "SELECT ?note WHERE { ex:s ex:p ?inner . ?inner ex:note ?note }",
    )
    .await;
    assert_eq!(rows, [json!(["keep"])]);
}

/// A CONSTRUCT restating the triple that reads a nested list writes it as
/// the nested list.
#[tokio::test]
async fn construct_writes_a_nested_list() {
    use fluree_db_api::format::{format_results_string, FormatterConfig};
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "construct").await;
    turtle(&fluree, &id, "ex:s ex:p ((1 2) 3) .").await;
    let ledger = load(&fluree, &id).await;
    let text = format!("{PREFIX}CONSTRUCT {{ ex:s ex:p ?v }} WHERE {{ ex:s ex:p ?v }}");
    let result = support::query_sparql(&fluree, &ledger, &text)
        .await
        .expect("construct");
    let ttl = format_results_string(
        &result,
        &result.context,
        &ledger.snapshot,
        &FormatterConfig::turtle(),
    )
    .expect("format");
    let copy = create(&fluree, "construct-copy").await;
    turtle(&fluree, &copy, &ttl).await;
    assert_eq!(crawl(&fluree, &copy).await, tree(), "construct: {ttl}");
}

/// A policy hiding a value reads the tree as if the value were deleted.
#[tokio::test]
async fn a_hidden_value_reads_as_deleted() {
    use fluree_db_api::{policy_builder, GovernanceOptions};
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "policy").await;
    turtle(&fluree, &id, "ex:s ex:p ((1) (2 3)) .").await;
    let ledger = load(&fluree, &id).await;
    let opts = GovernanceOptions {
        policy: Some(json!([
            {"@id": "ex:viewAll", "f:action": "f:view", "f:allow": true},
            {"@id": "ex:gate", "f:required": true,
             "f:onProperty": [{"@id": "http://example.org/p"}],
             "f:action": "f:view",
             "f:query": {"@type": "f:sparql", "@value": "ASK { FILTER($value != 2) }"}}
        ])),
        default_allow: Some(false),
        ..Default::default()
    };
    let policy = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
        &[0],
    )
    .await
    .expect("policy");
    let query = json!({"@context": {"ex": "http://example.org/"}, "select": {"ex:s": ["*"]}});
    let result = support::query_jsonld_with_policy(&fluree, &ledger, &query, &policy)
        .await
        .expect("query");
    let db = support::graphdb_from_ledger(&ledger);
    let out = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("format");
    assert_eq!(
        out[0]["ex:p"],
        json!({"@list": [{"@list": [1]}, {"@list": [3]}]})
    );
}

/// Deleting a run of values repairs the leaf after the whole run, from
/// novelty and from the index; an inner list left with no value is gone.
#[tokio::test]
async fn deleting_a_run_of_values_repairs_the_leaf_after_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    for id in novelty_and_index(&fluree, "delete-run", "ex:s ex:p ((1) (2 3 4)) .").await {
        update(&fluree, &id, "DELETE DATA { ex:s ex:p 2, 3 }").await;
        assert_eq!(
            crawl(&fluree, &id).await,
            json!({"@list": [{"@list": [1]}, {"@list": [4]}]}),
            "{id}"
        );
        update(&fluree, &id, "DELETE DATA { ex:s ex:p 1 }").await;
        assert_eq!(
            crawl(&fluree, &id).await,
            json!({"@list": [{"@list": [4]}]}),
            "{id}"
        );
    }
}

/// A CONSTRUCT whose WHERE filters entries out writes the tree without
/// them, as if they were deleted.
#[tokio::test]
async fn a_filtered_construct_writes_the_tree_without_the_entries() {
    use fluree_db_api::format::{format_results_string, FormatterConfig};
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "construct-filtered").await;
    turtle(&fluree, &id, "ex:s ex:p ((1) (2 3 4)) .").await;
    let ledger = load(&fluree, &id).await;
    let text = format!(
        "{PREFIX}CONSTRUCT {{ ex:s ex:p ?v }} WHERE {{ ex:s ex:p ?v FILTER(?v != 2 && ?v != 3) }}"
    );
    let result = support::query_sparql(&fluree, &ledger, &text)
        .await
        .expect("construct");
    let ttl = format_results_string(
        &result,
        &result.context,
        &ledger.snapshot,
        &FormatterConfig::turtle(),
    )
    .expect("format");
    let copy = create(&fluree, "construct-filtered-copy").await;
    turtle(&fluree, &copy, &ttl).await;
    assert_eq!(
        crawl(&fluree, &copy).await,
        json!({"@list": [{"@list": [1]}, {"@list": [4]}]}),
        "construct: {ttl}"
    );
}

/// A nested collection in a `DELETE WHERE` matches the nested list it
/// spells, and deletes it whole; another with the same values stays.
#[tokio::test]
async fn delete_where_matches_a_nested_collection() {
    let fluree = FlureeBuilder::memory().build_memory();
    for id in novelty_and_index(&fluree, "delete-where", "ex:s ex:p ((1 2) 3), (1 (2 3)) .").await {
        update(&fluree, &id, "DELETE WHERE { ex:s ex:p ((1 2) 3) }").await;
        assert_eq!(
            crawl(&fluree, &id).await,
            json!({"@list": [1, {"@list": [2, 3]}]}),
            "{id}"
        );
        update(&fluree, &id, "DELETE WHERE { ex:s ex:p (?a (?b ?c)) }").await;
        assert_eq!(crawl(&fluree, &id).await, JsonValue::Null, "{id}");
    }
}

/// An upsert replaces a nested list with the one it writes.
#[tokio::test]
async fn an_upsert_replaces_a_nested_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "upsert-replace").await;
    turtle(&fluree, &id, "ex:s ex:p ((1 2) 3) .").await;
    let data = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:s",
        "ex:p": {"@list": [{"@list": [3]}, {"@list": [2, 1]}]}
    });
    fluree
        .graph(&id)
        .transact()
        .upsert(&data)
        .commit()
        .await
        .expect("upsert");
    assert_eq!(
        crawl(&fluree, &id).await,
        json!({"@list": [{"@list": [3]}, {"@list": [2, 1]}]})
    );
}

/// `ADD` and `COPY` copy a nested list into another graph with its shape.
#[tokio::test]
async fn add_and_copy_keep_a_nested_lists_shape() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "add-copy").await;
    update(
        &fluree,
        &id,
        "INSERT DATA { GRAPH ex:g { ex:s ex:p ((1 2) 3) } }",
    )
    .await;
    update(&fluree, &id, "ADD ex:g TO DEFAULT").await;
    assert_eq!(crawl(&fluree, &id).await, tree());
    update(&fluree, &id, "ADD ex:g TO DEFAULT").await;
    assert_eq!(crawl(&fluree, &id).await, tree(), "ADD again restates it");
    update(&fluree, &id, "COPY ex:g TO ex:h").await;
    let rows = sparql(
        &fluree,
        &id,
        "SELECT ?o WHERE { GRAPH ex:h { ex:s ex:p ?o } }",
    )
    .await;
    assert_eq!(rows, [json!([1]), json!([2]), json!([3])]);
}

/// DESCRIBE writes a nested list the resource holds as the nested list.
#[tokio::test]
async fn describe_writes_a_nested_list() {
    use fluree_db_api::format::{format_results_string, FormatterConfig};
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "describe").await;
    turtle(&fluree, &id, "ex:s ex:p ((1 2) 3) .").await;
    let ledger = load(&fluree, &id).await;
    let result = support::query_sparql(&fluree, &ledger, &format!("{PREFIX}DESCRIBE ex:s"))
        .await
        .expect("describe");
    let ttl = format_results_string(
        &result,
        &result.context,
        &ledger.snapshot,
        &FormatterConfig::turtle(),
    )
    .expect("format");
    let copy = create(&fluree, "describe-copy").await;
    turtle(&fluree, &copy, &ttl).await;
    assert_eq!(crawl(&fluree, &copy).await, tree(), "describe: {ttl}");
}

/// A list of lists stored before nested lists were compact holds its inner
/// lists as chains (4.2.5 to 5.0 wrote a Turtle `((1 2) 3)` so): JSON-LD
/// reads it as the nested list.
#[tokio::test]
async fn a_list_holding_a_stored_chain_reads_as_a_nested_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "legacy-chain").await;
    turtle(&fluree, &id, "ex:s ex:p ( _:x 3 ) .").await;
    let head = sparql(
        &fluree,
        &id,
        "SELECT ?o WHERE { ex:s ex:p ?o FILTER(isBlank(?o)) }",
    )
    .await;
    let head = head[0][0]
        .as_str()
        .expect("the member's blank node")
        .to_string();
    turtle(
        &fluree,
        &id,
        &format!(
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
             {head} rdf:first 1 ; rdf:rest _:y .\n\
             _:y rdf:first 2 ; rdf:rest rdf:nil ."
        ),
    )
    .await;
    let cells = sparql(&fluree, &id, "SELECT ?c WHERE { ?c rdf:first ?v }").await;
    assert_eq!(
        cells.len(),
        2,
        "the inner list is stored as its triples: {cells:?}"
    );
    assert_eq!(crawl(&fluree, &id).await, tree());
}
