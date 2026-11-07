//! SPARQL reads a list's members as values of the property that holds it,
//! as JSON-LD does. A request reads a list's RDF cells only when it asks to
//! (`# PRAGMA rdf-lists: true`, JSON-LD `opts.rdfLists`), and the Graph Store
//! `GET` always does (`it_sparql_lists.rs`). Edits to cells are in
//! `it_list_cell_edits.rs`.

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState};
use serde_json::{json, Value as JsonValue};

const PREFIX: &str = "PREFIX ex: <http://example.org/>\n";

async fn create(fluree: &fluree_db_api::Fluree, name: &str, ttl: &str) -> String {
    let id = format!("it/sparql-list-members/{name}:main");
    fluree.create_ledger(&id).await.expect("create");
    if !ttl.is_empty() {
        fluree
            .graph(&id)
            .transact()
            .insert_turtle(&format!("@prefix ex: <http://example.org/> .\n{ttl}"))
            .commit()
            .await
            .unwrap_or_else(|e| panic!("insert: {e}"));
    }
    id
}

async fn load(fluree: &fluree_db_api::Fluree, id: &str) -> LedgerState {
    fluree.ledger(id).await.expect("load")
}

async fn sparql(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    text: &str,
) -> Vec<JsonValue> {
    let result = support::query_sparql(fluree, ledger, text)
        .await
        .unwrap_or_else(|e| panic!("query: {e}\n{text}"))
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    support::normalize_rows(&result)
}

async fn jsonld(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    query: JsonValue,
) -> Vec<JsonValue> {
    let result = support::query_jsonld(fluree, ledger, &query)
        .await
        .unwrap_or_else(|e| panic!("query: {e}\n{query}"))
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    support::normalize_rows(&result)
}

/// `subject ex:items ?o` in SPARQL.
async fn sparql_items(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    subject: &str,
) -> Vec<JsonValue> {
    let text = format!("{PREFIX}SELECT ?o WHERE {{ {subject} ex:items ?o }}");
    sparql(fluree, ledger, &text).await
}

/// The JSON-LD twin of [`sparql_items`].
async fn jsonld_items(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    subject: &str,
) -> Vec<JsonValue> {
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?o"],
        "where": {"@id": subject, "ex:items": "?o"}
    });
    jsonld(fluree, ledger, query).await
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

async fn lists(ledger: &LedgerState, subject: &str) -> Vec<Vec<i64>> {
    support::stored_int_lists(
        ledger,
        &format!("http://example.org/{subject}"),
        "http://example.org/items",
    )
    .await
}

#[tokio::test]
async fn a_lists_members_are_values_of_its_property() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "values", "ex:s ex:items (1 2 3) ; ex:items 9 .").await;
    let ledger = load(&fluree, &id).await;

    let members = vec![json!([1]), json!([2]), json!([3]), json!([9])];
    assert_eq!(sparql_items(&fluree, &ledger, "ex:s").await, members);
    assert_eq!(jsonld_items(&fluree, &ledger, "ex:s").await, members);
}

/// `# PRAGMA rdf-lists: true` reads each list as the RDF list it denotes,
/// an owner edge to a chain of cells, as `opts.rdfLists` does in JSON-LD;
/// without it a cell is nothing a pattern matches.
#[tokio::test]
async fn the_rdf_lists_pragma_reads_a_lists_cells() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(
        &fluree,
        "rdf-lists",
        "ex:s ex:items (1 2), (3) ; ex:items 9 .",
    )
    .await;
    let ledger = load(&fluree, &id).await;
    let rdf = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n";
    let heads = format!(
        "{PREFIX}{rdf}SELECT ?first WHERE {{ ex:s ex:items ?head . ?head rdf:first ?first }}"
    );
    let firsts = vec![json!([1]), json!([3])];
    let cells = sparql(
        &fluree,
        &ledger,
        &format!("# PRAGMA rdf-lists: true\n{heads}"),
    )
    .await;
    assert_eq!(cells, firsts);
    assert!(sparql(&fluree, &ledger, &heads).await.is_empty());
    let off = format!("# PRAGMA rdf-lists: false\n{heads}");
    assert!(sparql(&fluree, &ledger, &off).await.is_empty());

    // The second cell of each list, by its position.
    let second = format!(
        "# PRAGMA rdf-lists: true\n{PREFIX}{rdf}\
         SELECT ?second WHERE {{ ex:s ex:items/rdf:rest/rdf:first ?second }}"
    );
    assert_eq!(sparql(&fluree, &ledger, &second).await, vec![json!([2])]);

    // A plain value is a value; a list is its head cell, a blank node.
    let values = format!(
        "# PRAGMA rdf-lists: true\n{PREFIX}\
         SELECT ?o WHERE {{ ex:s ex:items ?o FILTER(!isBlank(?o)) }}"
    );
    assert_eq!(sparql(&fluree, &ledger, &values).await, vec![json!([9])]);

    let query = |opts: JsonValue| {
        json!({
            "@context": {"ex": "http://example.org/", "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"},
            "select": ["?first"],
            "where": [
                {"@id": "ex:s", "ex:items": "?head"},
                {"@id": "?head", "rdf:first": "?first"}
            ],
            "opts": opts
        })
    };
    assert_eq!(
        jsonld(&fluree, &ledger, query(json!({"rdfLists": true}))).await,
        firsts
    );
    assert!(jsonld(&fluree, &ledger, query(json!({}))).await.is_empty());
}

/// Where a request cannot read lists as RDF it is refused, never answered
/// with the members as values: a history range, a dataset spanning ledgers,
/// a JSON-LD graph crawl, a policy query, and a malformed option.
#[tokio::test]
async fn rdf_lists_is_refused_where_it_cannot_be_honored() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "rdf-lists-refused", "ex:s ex:items (1 2) .").await;
    let other = create(&fluree, "rdf-lists-other", "ex:t ex:items (3) .").await;
    let ledger = load(&fluree, &id).await;
    fn refused<T>(result: fluree_db_api::Result<T>, expected: &str) {
        let err = result
            .err()
            .unwrap_or_else(|| panic!("refused: {expected}"));
        assert!(err.to_string().contains(expected), "{expected}: {err}");
        assert_eq!(err.status_code(), 400, "{err}");
    }

    let history = format!(
        "# PRAGMA rdf-lists: true\n{PREFIX}SELECT ?o FROM <{id}@t:1> TO <{id}@t:latest> \
         WHERE {{ ex:s ex:items ?o }}"
    );
    refused(
        fluree
            .query_from()
            .sparql(&history)
            .execute_formatted()
            .await,
        "history query",
    );

    let across = format!(
        "# PRAGMA rdf-lists: true\n{PREFIX}SELECT ?o FROM <{id}> FROM <{other}> \
         WHERE {{ ?s ex:items ?o }}"
    );
    refused(
        fluree
            .query_from()
            .sparql(&across)
            .execute_formatted()
            .await,
        "several ledgers",
    );
    let across = across.replace("# PRAGMA rdf-lists: true\n", "");
    let members = fluree
        .query_from()
        .sparql(&across)
        .execute_formatted()
        .await
        .expect("members across ledgers");
    let rows = members["results"]["bindings"].as_array().map(Vec::len);
    assert_eq!(rows, Some(3), "{members}");

    // One ledger at two times: the list is only at t:2.
    let times = create(&fluree, "rdf-lists-times", "ex:s ex:marker 0 .").await;
    fluree
        .graph(&times)
        .transact()
        .insert_turtle("@prefix ex: <http://example.org/> .\nex:s ex:items (1 2) .")
        .commit()
        .await
        .expect("t:2");
    let firsts = |from: &str| {
        format!(
            "# PRAGMA rdf-lists: true\n{PREFIX}\
             PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
             SELECT ?v {from} WHERE {{ ex:s ex:items ?h . ?h rdf:first ?v }}"
        )
    };
    let at_two = fluree
        .query_from()
        .sparql(&firsts(&format!("FROM <{times}@t:2>")))
        .execute_formatted()
        .await
        .expect("one time");
    let rows = at_two["results"]["bindings"].as_array().map(Vec::len);
    assert_eq!(rows, Some(1), "{at_two}");
    refused(
        fluree
            .query_from()
            .sparql(&firsts(&format!("FROM <{times}@t:1> FROM <{times}@t:2>")))
            .execute_formatted()
            .await,
        "several points in time",
    );

    let crawl = json!({
        "@context": {"ex": "http://example.org/"},
        "select": {"?s": ["*"]},
        "where": {"@id": "?s", "ex:items": "?o"},
        "opts": {"rdfLists": true}
    });
    refused(
        support::query_jsonld(&fluree, &ledger, &crawl).await,
        "graph crawl",
    );
    let malformed = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?o"],
        "where": {"@id": "ex:s", "ex:items": "?o"},
        "opts": {"rdfLists": "yes"}
    });
    refused(
        support::query_jsonld(&fluree, &ledger, &malformed).await,
        "opts.rdfLists",
    );
}

#[tokio::test]
async fn a_repeated_member_is_a_value_each_time() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "repeated", "ex:s ex:items (1 1 2) .").await;
    let ledger = load(&fluree, &id).await;

    let members = vec![json!([1]), json!([1]), json!([2])];
    assert_eq!(sparql_items(&fluree, &ledger, "ex:s").await, members);
    assert_eq!(jsonld_items(&fluree, &ledger, "ex:s").await, members);
}

/// A value read over an index keeps each member, a repeated value as many:
/// at the head; at an earlier `t`, where the leaflet's history replays and
/// undoing the two `1`s of the lists written later leaves the first list's;
/// and with novelty merged over the index.
#[tokio::test]
async fn indexed_reads_keep_each_member_at_an_earlier_t() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "indexed-at-t", "ex:s ex:items (1 2) .").await;
    update(&fluree, &id, "INSERT DATA { ex:s ex:items (1), (1 3) }").await;
    support::rebuild_and_publish_index(&fluree, &id).await;
    let ledger = load(&fluree, &id).await;
    assert_eq!(lists(&ledger, "s").await.len(), 3);
    let before = fluree.db_at_t(&id, 1).await.expect("at t 1");
    for pattern in ["ex:s ex:items ?o", "?s ex:items ?o"] {
        let text = format!("{PREFIX}SELECT ?o WHERE {{ {pattern} }}");
        let now = sparql(&fluree, &ledger, &text).await;
        let five = [json!([1]), json!([1]), json!([1]), json!([2]), json!([3])];
        assert_eq!(now, five, "{pattern}");
        let rows = fluree
            .query(&before, text.as_str())
            .await
            .unwrap_or_else(|e| panic!("query: {e}\n{text}"))
            .to_jsonld(&before.snapshot)
            .expect("to_jsonld");
        assert_eq!(
            support::normalize_rows(&rows),
            [json!([1]), json!([2])],
            "{pattern} at t 1"
        );
    }
    let twin = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?o"],
        "where": {"@id": "ex:s", "ex:items": "?o"}
    });
    let rows = fluree
        .query(&before, &twin)
        .await
        .expect("query")
        .to_jsonld(&before.snapshot)
        .expect("to_jsonld");
    assert_eq!(support::normalize_rows(&rows), [json!([1]), json!([2])]);

    update(&fluree, &id, "DELETE DATA { ex:s ex:items 2 }").await;
    update(&fluree, &id, "INSERT DATA { ex:s ex:items (1 4) }").await;
    let ledger = load(&fluree, &id).await;
    let merged = [1, 1, 1, 1, 3, 4].map(|v| json!([v]));
    for pattern in ["ex:s ex:items ?o", "?s ex:items ?o"] {
        let text = format!("{PREFIX}SELECT ?o WHERE {{ {pattern} }}");
        assert_eq!(sparql(&fluree, &ledger, &text).await, merged, "{pattern}");
    }
}

/// Counts read the stored rows, a member each, from novelty and from an
/// index.
#[tokio::test]
async fn counts_count_each_member() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "counts", "ex:s ex:items (1 1 2) ; ex:name \"s\" .").await;
    for indexed in [false, true] {
        if indexed {
            support::rebuild_and_publish_index(&fluree, &id).await;
        }
        let ledger = load(&fluree, &id).await;
        let total = sparql(
            &fluree,
            &ledger,
            "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }",
        )
        .await;
        assert_eq!(total, [json!([4])], "indexed {indexed}");
        let by_predicate = sparql(
            &fluree,
            &ledger,
            &format!("{PREFIX}SELECT (COUNT(*) AS ?n) WHERE {{ ?s ex:items ?o }}"),
        )
        .await;
        assert_eq!(by_predicate, [json!([3])], "indexed {indexed}");
        let twin = jsonld(
            &fluree,
            &ledger,
            json!({
                "@context": {"ex": "http://example.org/"},
                "select": ["(count ?o)"],
                "where": {"@id": "?s", "ex:items": "?o"}
            }),
        )
        .await;
        assert_eq!(twin, [json!([3])], "indexed {indexed}");
    }
}

#[tokio::test]
async fn insert_data_writes_a_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "insert", "").await;
    update(&fluree, &id, "INSERT DATA { ex:s ex:items (1 2 3) }").await;
    let ledger = load(&fluree, &id).await;

    assert_eq!(lists(&ledger, "s").await, [vec![1, 2, 3]]);
    let members = vec![json!([1]), json!([2]), json!([3])];
    assert_eq!(sparql_items(&fluree, &ledger, "ex:s").await, members);
    assert_eq!(jsonld_items(&fluree, &ledger, "ex:s").await, members);
}

/// A member is a value, so a delete naming the value reaches it, as the
/// same JSON-LD delete does.
#[tokio::test]
async fn delete_data_deletes_a_member() {
    let fluree = FlureeBuilder::memory().build_memory();
    let sparql_id = create(&fluree, "delete-sparql", "ex:s ex:items (1 2 3) .").await;
    update(&fluree, &sparql_id, "DELETE DATA { ex:s ex:items 2 }").await;

    let jsonld_id = create(&fluree, "delete-jsonld", "ex:s ex:items (1 2 3) .").await;
    jsonld_delete(&fluree, &jsonld_id, json!(2)).await;

    for id in [&sparql_id, &jsonld_id] {
        let ledger = load(&fluree, id).await;
        assert_eq!(lists(&ledger, "s").await, [vec![1, 3]], "{id}");
        assert_eq!(
            sparql_items(&fluree, &ledger, "ex:s").await,
            [json!([1]), json!([3])],
            "{id}"
        );
    }
}

async fn jsonld_delete(fluree: &fluree_db_api::Fluree, id: &str, value: JsonValue) {
    fluree
        .graph(id)
        .transact()
        .update(&json!({
            "@context": {"ex": "http://example.org/"},
            "delete": {"@id": "ex:s", "ex:items": value}
        }))
        .commit()
        .await
        .expect("JSON-LD delete");
}

/// Deleting a value a list repeats deletes every entry holding it, and the
/// value stored beside the list too, however the delete is spelled: the same
/// answer from a WHERE that hands each stored entry over and from one that
/// names the value.
#[tokio::test]
async fn deleting_a_repeated_member_deletes_each_entry() {
    let fluree = FlureeBuilder::memory().build_memory();
    let forms = [
        ("data", "DELETE DATA { ex:s ex:items 1 }"),
        (
            "filter",
            "DELETE { ex:s ex:items ?o } WHERE { ex:s ex:items ?o FILTER(?o = 1) }",
        ),
        (
            "values",
            "DELETE { ex:s ex:items ?o } WHERE { VALUES ?o { 1 } ex:s ex:items ?o }",
        ),
        ("where", "DELETE WHERE { ex:s ex:items 1 }"),
        (
            "constant",
            "DELETE { ex:s ex:items 1 } WHERE { ex:s ex:items 1 }",
        ),
    ];
    for indexed in [false, true] {
        let mut ids = Vec::new();
        for (name, body) in forms {
            let id = create(
                &fluree,
                &format!("repeat-{name}-{indexed}"),
                "ex:s ex:items (1 1 2 1) , 1 .",
            )
            .await;
            if indexed {
                support::rebuild_and_publish_index(&fluree, &id).await;
            }
            update(&fluree, &id, body).await;
            ids.push((name, id));
        }
        let id = create(
            &fluree,
            &format!("repeat-jsonld-{indexed}"),
            "ex:s ex:items (1 1 2 1) , 1 .",
        )
        .await;
        if indexed {
            support::rebuild_and_publish_index(&fluree, &id).await;
        }
        jsonld_delete(&fluree, &id, json!(1)).await;
        ids.push(("jsonld", id));

        for (name, id) in ids {
            let ledger = load(&fluree, &id).await;
            assert_eq!(
                lists(&ledger, "s").await,
                [vec![2]],
                "{name}, indexed {indexed}"
            );
            assert_eq!(
                sparql_items(&fluree, &ledger, "ex:s").await,
                [json!([2])],
                "{name}, indexed {indexed}: the plain 1 goes too"
            );
        }
    }
}

/// A list the property holds, written again, is restated as stored: a
/// delete leaves its positions apart, and writing the same members again
/// adds none. By insert, and by ADD from a graph whose list lost a member.
#[tokio::test]
async fn restating_a_list_keeps_its_positions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "restate", "ex:s ex:items (1 2 3) .").await;
    update(&fluree, &id, "DELETE DATA { ex:s ex:items 2 }").await;
    fluree
        .graph(&id)
        .transact()
        .insert(&json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:s",
            "ex:items": {"@list": [1, 3]}
        }))
        .commit()
        .await
        .expect("insert");
    let ledger = load(&fluree, &id).await;
    assert_eq!(lists(&ledger, "s").await, [vec![1, 3]], "insert");

    let id = create(
        &fluree,
        "restate-add",
        "GRAPH ex:g1 { ex:s ex:items (1 2 3) . } GRAPH ex:g2 { ex:s ex:items (1 3) . }",
    )
    .await;
    update(
        &fluree,
        &id,
        "DELETE DATA { GRAPH ex:g1 { ex:s ex:items 2 } }",
    )
    .await;
    update(&fluree, &id, "ADD ex:g1 TO ex:g2").await;
    let ledger = load(&fluree, &id).await;
    let in_g2 = format!("{PREFIX}SELECT ?o WHERE {{ GRAPH ex:g2 {{ ex:s ex:items ?o }} }}");
    assert_eq!(
        sparql(&fluree, &ledger, &in_g2).await,
        [json!([1]), json!([3])],
        "ADD"
    );
}

/// ADD and COPY leave the source's list as it is and put the same list in
/// the destination; ADD again changes nothing.
#[tokio::test]
async fn a_list_added_or_copied_stays_in_its_source() {
    let data = "GRAPH ex:g1 { ex:s ex:items (1 2) . } GRAPH ex:g2 { ex:t ex:name ex:n . }";
    let in_graph =
        |g: &str| format!("{PREFIX}SELECT ?o WHERE {{ GRAPH {g} {{ ex:s ex:items ?o }} }}");
    for op in ["ADD", "COPY"] {
        let fluree = FlureeBuilder::memory().build_memory();
        let id = create(&fluree, &format!("transfer-{op}"), data).await;
        update(&fluree, &id, &format!("{op} ex:g1 TO ex:g2")).await;
        let ledger = load(&fluree, &id).await;
        for g in ["ex:g1", "ex:g2"] {
            assert_eq!(
                sparql(&fluree, &ledger, &in_graph(g)).await,
                [json!([1]), json!([2])],
                "{op}: {g}"
            );
        }
        let t = ledger.t();
        update(&fluree, &id, "ADD ex:g1 TO ex:g2").await;
        assert_eq!(load(&fluree, &id).await.t(), t, "{op}, then ADD again");

        // The source's members are values still: a delete reaches them.
        update(
            &fluree,
            &id,
            "DELETE DATA { GRAPH ex:g1 { ex:s ex:items 1 } }",
        )
        .await;
        let ledger = load(&fluree, &id).await;
        assert_eq!(
            sparql(&fluree, &ledger, &in_graph("ex:g1")).await,
            [json!([2])],
            "{op}"
        );
        assert_eq!(
            sparql(&fluree, &ledger, &in_graph("ex:g2")).await,
            [json!([1]), json!([2])],
            "{op}: the copy is its own"
        );
    }
}

/// ADD into a graph that holds another list on the same subject and
/// property keeps that list, and adds the source's beside it, as a list of
/// its own.
#[tokio::test]
async fn adding_beside_another_list_keeps_both() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(
        &fluree,
        "transfer-beside",
        "GRAPH ex:g1 { ex:s ex:items (1 2) . } GRAPH ex:g2 { ex:s ex:items (3 4) . }",
    )
    .await;
    update(&fluree, &id, "ADD ex:g1 TO ex:g2").await;
    let ledger = load(&fluree, &id).await;
    let source = sparql(
        &fluree,
        &ledger,
        &format!("{PREFIX}SELECT ?o WHERE {{ GRAPH ex:g1 {{ ex:s ex:items ?o }} }}"),
    )
    .await;
    assert_eq!(source, [json!([1]), json!([2])], "the source");
    let members = sparql(
        &fluree,
        &ledger,
        &format!("{PREFIX}SELECT ?o WHERE {{ GRAPH ex:g2 {{ ex:s ex:items ?o }} }}"),
    )
    .await;
    assert_eq!(
        members,
        [json!([1]), json!([2]), json!([3]), json!([4])],
        "both lists' members"
    );
    let g2 = ledger
        .snapshot
        .graph_registry
        .assigned_graph_id("http://example.org/g2")
        .expect("g2");
    let held = support::stored_int_lists_in(
        &ledger,
        g2,
        "http://example.org/s",
        "http://example.org/items",
    )
    .await;
    assert_eq!(held, [[1, 2], [3, 4]], "two lists");

    let t = ledger.t();
    update(&fluree, &id, "ADD ex:g1 TO ex:g2").await;
    assert_eq!(load(&fluree, &id).await.t(), t, "ADD again changes nothing");
}

/// An update's WHERE binds members, as a query's does.
#[tokio::test]
async fn an_update_reads_members() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "update-where", "ex:s ex:items (1 2 3) .").await;
    update(
        &fluree,
        &id,
        "INSERT { ex:t ex:items ?o } WHERE { ex:s ex:items ?o FILTER(?o > 1) }",
    )
    .await;
    let ledger = load(&fluree, &id).await;

    let copied = vec![json!([2]), json!([3])];
    assert_eq!(sparql_items(&fluree, &ledger, "ex:t").await, copied);
    assert_eq!(jsonld_items(&fluree, &ledger, "ex:t").await, copied);
    assert_eq!(lists(&ledger, "s").await, [vec![1, 2, 3]]);
}

async fn turtle(fluree: &fluree_db_api::Fluree, ledger: &LedgerState, text: &str) -> String {
    use fluree_db_api::format::{format_results_string, FormatterConfig};
    let result = support::query_sparql(fluree, ledger, text)
        .await
        .unwrap_or_else(|e| panic!("query: {e}\n{text}"));
    format_results_string(
        &result,
        &result.context,
        &ledger.snapshot,
        &FormatterConfig::turtle(),
    )
    .expect("turtle")
}

/// A CONSTRUCT whose template restates a triple of its WHERE writes the
/// list entries it reads as the list, in order and with its repeats: `( … )`
/// in Turtle, `@list` in JSON-LD, from novelty and from an index.
#[tokio::test]
async fn construct_writes_the_lists_it_reads() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(
        &fluree,
        "construct",
        "ex:s ex:items (ex:a ex:b ex:a) ; ex:nums (3 1 3) ; ex:name \"s\" .",
    )
    .await;
    for indexed in [false, true] {
        if indexed {
            support::rebuild_and_publish_index(&fluree, &id).await;
        }
        let ledger = load(&fluree, &id).await;
        let everything = format!("{PREFIX}CONSTRUCT {{ ?s ?p ?o }} WHERE {{ ?s ?p ?o }}");
        let ttl = turtle(&fluree, &ledger, &everything).await;
        assert!(
            ttl.contains("ex:items ( ex:a ex:b ex:a )"),
            "indexed {indexed}: {ttl}"
        );
        assert!(
            ttl.contains("ex:nums ( 3 1 3 )"),
            "indexed {indexed}: {ttl}"
        );

        let result = support::query_sparql(&fluree, &ledger, &everything)
            .await
            .expect("construct");
        let graph = result.to_construct(&ledger.snapshot).expect("jsonld");
        assert_eq!(
            graph["@graph"][0]["ex:nums"],
            json!({"@list": [3, 1, 3]}),
            "indexed {indexed}: {graph}"
        );

        let shorthand = format!("{PREFIX}CONSTRUCT WHERE {{ ex:s ex:nums ?o }}");
        let ttl = turtle(&fluree, &ledger, &shorthand).await;
        assert!(
            ttl.contains("ex:s ex:nums ( 3 1 3 ) ."),
            "indexed {indexed}: {ttl}"
        );

        let ttl = turtle(&fluree, &ledger, &format!("{PREFIX}DESCRIBE ex:s")).await;
        assert!(
            ttl.contains("ex:items ( ex:a ex:b ex:a )"),
            "indexed {indexed}: {ttl}"
        );
        assert!(
            ttl.contains("ex:nums ( 3 1 3 )"),
            "indexed {indexed}: {ttl}"
        );
    }
}

/// Each list on one property is written as its own collection, and reads
/// back as the lists it came from.
#[tokio::test]
async fn construct_writes_each_list_on_a_property() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(
        &fluree,
        "construct-several",
        "ex:s ex:items (3 1) , (1 2) .",
    )
    .await;
    for indexed in [false, true] {
        if indexed {
            support::rebuild_and_publish_index(&fluree, &id).await;
        }
        let ledger = load(&fluree, &id).await;
        let everything = format!("{PREFIX}CONSTRUCT {{ ?s ?p ?o }} WHERE {{ ?s ?p ?o }}");
        let ttl = turtle(&fluree, &ledger, &everything).await;
        assert!(
            ttl.contains("( 3 1 )") && ttl.contains("( 1 2 )"),
            "indexed {indexed}: {ttl}"
        );
        let filtered = format!(
            "{PREFIX}CONSTRUCT {{ ?s ex:items ?o }} WHERE {{ ?s ex:items ?o FILTER(?o != 2) }}"
        );
        let ttl = turtle(&fluree, &ledger, &filtered).await;
        assert!(
            ttl.contains("( 3 1 )") && ttl.contains("( 1 )"),
            "indexed {indexed}: {ttl}"
        );

        let copy = format!("it/sparql-list-members/construct-several-copy-{indexed}:main");
        fluree.create_ledger(&copy).await.expect("create");
        let ttl = turtle(&fluree, &ledger, &everything).await;
        fluree
            .graph(&copy)
            .transact()
            .insert_turtle(&ttl)
            .commit()
            .await
            .unwrap_or_else(|e| panic!("insert: {e}\n{ttl}"));
        assert_eq!(
            lists(&load(&fluree, &copy).await, "s").await,
            [[1, 2], [3, 1]],
            "{ttl}"
        );
    }
}

/// A value names every entry holding it, as a delete naming it does: a
/// FILTER keeps the entries holding the values it keeps, in order.
#[tokio::test]
async fn construct_writes_the_entries_holding_each_value() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "construct-filter", "ex:s ex:nums (3 1 2 3) .").await;
    let ledger = load(&fluree, &id).await;
    let text =
        format!("{PREFIX}CONSTRUCT {{ ?s ex:nums ?o }} WHERE {{ ?s ex:nums ?o FILTER(?o != 1) }}");
    let ttl = turtle(&fluree, &ledger, &text).await;
    assert!(ttl.contains("ex:s ex:nums ( 3 2 3 ) ."), "{ttl}");

    let text = format!(
        "{PREFIX}CONSTRUCT {{ ?s ex:nums ?o }} WHERE {{ ?s ex:nums ?o }} ORDER BY DESC(?o) LIMIT 1"
    );
    let ttl = turtle(&fluree, &ledger, &text).await;
    assert!(ttl.contains("ex:s ex:nums ( 3 3 ) ."), "{ttl}");
}

/// A template that writes the values under another subject writes values,
/// though that subject holds a list of them: the WHERE read the values from
/// its owner's list, not the template subject's.
#[tokio::test]
async fn construct_under_another_subject_writes_values() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(
        &fluree,
        "construct-other",
        "ex:s ex:nums (3 1) ; ex:twin ex:t . ex:t ex:nums (1 3) .",
    )
    .await;
    let ledger = load(&fluree, &id).await;
    for text in [
        "CONSTRUCT { ex:t ex:nums ?o } WHERE { ex:s ex:nums ?o }",
        "CONSTRUCT { ?x ex:nums ?o } WHERE { ?s ex:twin ?x . ?s ex:nums ?o }",
    ] {
        let ttl = turtle(&fluree, &ledger, &format!("{PREFIX}{text}")).await;
        assert!(!ttl.contains('('), "{text}: {ttl}");
        assert!(ttl.contains("ex:t ex:nums 1, 3 ."), "{text}: {ttl}");
    }
}

/// What a CONSTRUCT writes reads back as what it read: each list whole and
/// in order, and a value held beside a list beside it.
#[tokio::test]
async fn constructed_lists_round_trip() {
    let fluree = FlureeBuilder::memory().build_memory();
    let data = "ex:s ex:items (1 2 1) ; ex:refs (ex:a ex:b) ; ex:name \"s\" .\n\
                ex:u ex:items (1 2) , 1 .";
    let id = create(&fluree, "round-trip-source", data).await;
    let ledger = load(&fluree, &id).await;
    let everything = format!("{PREFIX}CONSTRUCT {{ ?s ?p ?o }} WHERE {{ ?s ?p ?o }}");
    let ttl = turtle(&fluree, &ledger, &everything).await;

    let copy = "it/sparql-list-members/round-trip-copy:main";
    fluree.create_ledger(copy).await.expect("create");
    fluree
        .graph(copy)
        .transact()
        .insert_turtle(&ttl)
        .commit()
        .await
        .unwrap_or_else(|e| panic!("insert: {e}\n{ttl}"));
    let copied = load(&fluree, copy).await;
    assert_eq!(lists(&copied, "s").await, [vec![1, 2, 1]], "{ttl}");
    assert_eq!(
        sparql_items(&fluree, &copied, "ex:u").await,
        [json!([1]), json!([1]), json!([2])],
        "{ttl}"
    );
    assert_eq!(turtle(&fluree, &copied, &everything).await, ttl);
}

/// The JSON-LD twin: a `construct` restating its `where` writes `@list`.
#[tokio::test]
async fn jsonld_construct_writes_the_lists_it_reads() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "jsonld-construct", "ex:s ex:nums (3 1 3) .").await;
    let ledger = load(&fluree, &id).await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "?s", "ex:nums": "?o"},
        "construct": [{"@id": "?s", "ex:nums": "?o"}]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("construct");
    let graph = result.to_construct(&ledger.snapshot).expect("jsonld");
    assert_eq!(
        graph["@graph"],
        json!([{"@id": "ex:s", "ex:nums": {"@list": [3, 1, 3]}}]),
        "{graph}"
    );
}

/// The list is the one the query's default graph holds: a FROM picks the
/// graph, and a default graph holding a list on the owner in two of its
/// graphs writes values.
#[tokio::test]
async fn construct_reads_the_list_its_default_graph_holds() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(
        &fluree,
        "construct-from",
        "ex:s ex:nums (1 2) . GRAPH ex:g1 { ex:s ex:nums (3 4) . } GRAPH ex:g2 { ex:s ex:nums (5 6) . }",
    )
    .await;
    let ledger = load(&fluree, &id).await;
    let from =
        |graphs: &str| format!("{PREFIX}CONSTRUCT {{ ?s ?p ?o }} {graphs} WHERE {{ ?s ?p ?o }}");
    let ttl = turtle(&fluree, &ledger, &from("")).await;
    assert!(ttl.contains("ex:s ex:nums ( 1 2 ) ."), "{ttl}");
    let ttl = turtle(&fluree, &ledger, &from("FROM ex:g1")).await;
    assert!(ttl.contains("ex:s ex:nums ( 3 4 ) ."), "{ttl}");
    let ttl = turtle(&fluree, &ledger, &from("FROM ex:g1 FROM ex:g2")).await;
    assert!(!ttl.contains('('), "{ttl}");
}

/// Each value's entries are bound once, on the first solution naming it: a
/// list repeating a value N times binds N positions in all, not N².
#[tokio::test]
async fn construct_binds_each_values_entries_once() {
    use fluree_db_query::binding::Binding;

    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(
        &fluree,
        "construct-repeats",
        "ex:s ex:nums (7 7 7 7 7 7 7 7) .",
    )
    .await;
    let ledger = load(&fluree, &id).await;
    let text = format!("{PREFIX}CONSTRUCT {{ ?s ex:nums ?o }} WHERE {{ ?s ex:nums ?o }}");
    let result = support::query_sparql(&fluree, &ledger, &text)
        .await
        .expect("construct");
    let vars = result
        .output
        .construct_template()
        .and_then(|t| t.list_entries(0))
        .expect("the template restates the WHERE");
    let (mut rows, mut positions) = (0, 0);
    for batch in &result.batches {
        for row in 0..batch.len() {
            if let Some(Binding::List(at)) = batch.get(row, vars.positions) {
                rows += 1;
                positions += at.len();
            }
        }
    }
    assert_eq!((rows, positions), (8, 8));
    let ttl = turtle(&fluree, &ledger, &text).await;
    assert!(ttl.contains("ex:s ex:nums ( 7 7 7 7 7 7 7 7 ) ."), "{ttl}");
}

/// The variables a list's entries are bound to are new to the query: one
/// the query names as they would be named is its own.
#[tokio::test]
async fn construct_variables_alias_none_of_the_querys() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "construct-names", "ex:s ex:nums (3 1 3) .").await;
    let ledger = load(&fluree, &id).await;
    for var in [
        "?__list_positions_0",
        "?__list_beside_1",
        "?__list_positions_2",
    ] {
        let text = format!("{PREFIX}CONSTRUCT {{ ?s ex:nums {var} }} WHERE {{ ?s ex:nums {var} }}");
        let ttl = turtle(&fluree, &ledger, &text).await;
        assert!(ttl.contains("ex:s ex:nums ( 3 1 3 ) ."), "{var}: {ttl}");
    }
}

/// ADD again after an index build changes nothing, though the build holds
/// the list's `-0.0` as `0.0`: the list ADD put beside another is found by
/// its members.
#[tokio::test]
async fn adding_again_after_an_index_build_changes_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(
        &fluree,
        "transfer-indexed",
        "GRAPH ex:g1 { ex:s ex:items (-0.0e0 1.5e0) . } GRAPH ex:g2 { ex:s ex:items (3 4) . }",
    )
    .await;
    update(&fluree, &id, "ADD ex:g1 TO ex:g2").await;
    support::rebuild_and_publish_index(&fluree, &id).await;
    let t = load(&fluree, &id).await.t();
    update(&fluree, &id, "ADD ex:g1 TO ex:g2").await;
    assert_eq!(load(&fluree, &id).await.t(), t);
}

/// Two lists ADDed into one destination beside its own list stay two lists,
/// however alike their members print.
#[tokio::test]
async fn lists_added_beside_another_stay_apart() {
    let fluree = FlureeBuilder::memory().build_memory();
    let point = |lat: &str| {
        format!("\"POINT(2 {lat})\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>")
    };
    let data = format!(
        "GRAPH ex:g1 {{ ex:s ex:items ({}) . }} GRAPH ex:g2 {{ ex:s ex:items ({}) . }} \
         GRAPH ex:g3 {{ ex:s ex:items (3 4) . }}",
        point("1.0000001"),
        point("1.0000004"),
    );
    let id = create(&fluree, "transfer-apart", &data).await;
    update(&fluree, &id, "ADD ex:g1 TO ex:g3").await;
    update(&fluree, &id, "ADD ex:g2 TO ex:g3").await;
    let ledger = load(&fluree, &id).await;
    let members = sparql(
        &fluree,
        &ledger,
        &format!(
            "{PREFIX}SELECT (COUNT(*) AS ?members) \
             WHERE {{ GRAPH ex:g3 {{ ex:s ex:items ?x }} }}"
        ),
    )
    .await;
    assert_eq!(
        members,
        [json!([4])],
        "a member each, beside the list there"
    );
}

/// `p/rdf:rest*/rdf:first` reaches each member of every list `p` holds, once
/// per entry, as the lists' RDF would: not a plain value of `p`, and a list
/// stored as its triples, complete or not, through those triples. The query's
/// other patterns read members as values. From novelty and from an index.
#[tokio::test]
async fn a_list_walk_reads_each_member() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(
        &fluree,
        "list-walk",
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
         ex:s ex:items (1 1 2), (3), 9 .
         ex:t ex:items _:c . _:c rdf:first 7 ; rdf:rest _:d ; ex:note 0 .
         _:d rdf:first 8 ; rdf:rest rdf:nil .
         ex:u ex:items _:e . _:e rdf:first 5 .",
    )
    .await;
    let rdf = format!("{PREFIX}PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
    let walk = |subject: &str| {
        format!("{rdf}SELECT ?x WHERE {{ {subject} ex:items/rdf:rest*/rdf:first ?x }}")
    };
    let values = |vs: &[i64]| vs.iter().map(|v| json!([v])).collect::<Vec<_>>();
    for indexed in [false, true] {
        if indexed {
            support::rebuild_and_publish_index(&fluree, &id).await;
        }
        let ledger = load(&fluree, &id).await;
        let lane = format!("indexed {indexed}");
        assert_eq!(
            sparql(&fluree, &ledger, &walk("ex:s")).await,
            values(&[1, 1, 2, 3]),
            "{lane}"
        );
        assert_eq!(
            sparql(&fluree, &ledger, &walk("ex:t")).await,
            values(&[7, 8]),
            "{lane}"
        );
        assert_eq!(
            sparql(&fluree, &ledger, &walk("ex:u")).await,
            values(&[5]),
            "{lane}"
        );
        let all = sparql(&fluree, &ledger, &walk("?s")).await;
        assert_eq!(all, values(&[1, 1, 2, 3, 5, 7, 8]), "{lane}");
        let holders = sparql(
            &fluree,
            &ledger,
            &format!("{rdf}SELECT ?s WHERE {{ ?s ex:items/rdf:rest*/rdf:first 2 }}"),
        )
        .await;
        assert_eq!(holders, [json!(["ex:s"])], "{lane}");
        let beside = sparql(
            &fluree,
            &ledger,
            &format!(
                "{rdf}SELECT (COUNT(?x) AS ?n) WHERE {{ ex:s ex:items ?x \
                 FILTER EXISTS {{ ex:s ex:items/rdf:rest*/rdf:first ?x }} }}"
            ),
        )
        .await;
        assert_eq!(beside, [json!([4])], "the plain 9 is no member; {lane}");
        let twin = jsonld(
            &fluree,
            &ledger,
            json!({
                "@context": {
                    "ex": "http://example.org/",
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                    "members": {"@path": "ex:items/rdf:rest*/rdf:first"}
                },
                "select": ["?x"],
                "where": {"@id": "ex:s", "members": "?x"}
            }),
        )
        .await;
        assert_eq!(twin, values(&[1, 1, 2, 3]), "{lane}");
    }
    // An update's WHERE reads the walk as a query's does.
    update(
        &fluree,
        &id,
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         INSERT { ?s ex:member ?x } WHERE { ?s ex:items/rdf:rest*/rdf:first ?x }",
    )
    .await;
    let ledger = load(&fluree, &id).await;
    let members = sparql(
        &fluree,
        &ledger,
        &format!("{PREFIX}SELECT ?x WHERE {{ ex:s ex:member ?x }}"),
    )
    .await;
    assert_eq!(members, values(&[1, 2, 3]));
}

/// The rows of a SPARQL query over the dataset it names: each row's values,
/// in projection order.
async fn dataset_rows(
    fluree: &fluree_db_api::Fluree,
    text: &str,
) -> fluree_db_api::Result<Vec<Vec<String>>> {
    let result = fluree.query_from().sparql(text).execute_formatted().await?;
    let vars: Vec<&str> = result["head"]["vars"]
        .as_array()
        .expect("vars")
        .iter()
        .filter_map(JsonValue::as_str)
        .collect();
    let rows = result["results"]["bindings"].as_array().expect("bindings");
    Ok(rows
        .iter()
        .map(|row| {
            vars.iter()
                .map(|v| row[*v]["value"].as_str().unwrap_or("").to_string())
                .collect()
        })
        .collect())
}

/// A walk reads the lists of each graph a dataset names, in that graph's
/// ledger: one ledger at two times in either order, and named graphs of two
/// ledgers at one `t`, whether or not the first holds a list or codes the
/// property as the second does. A default graph spanning ledgers is refused,
/// as it is for any path, and so is a history range over compact lists; lists
/// stored as triples walk in history as before.
#[tokio::test]
async fn a_list_walk_reads_each_graph_of_a_dataset() {
    let fluree = FlureeBuilder::memory().build_memory();
    let insert = |id: String, ttl: &'static str| {
        let fluree = &fluree;
        async move {
            fluree
                .graph(&id)
                .transact()
                .insert_turtle(&format!("@prefix ex: <http://example.org/> .\n{ttl}"))
                .commit()
                .await
                .expect("t:2");
            id
        }
    };
    // Both lists are at t:2. `b` met another namespace at t:1 (a declared
    // prefix takes a code too), so it codes `ex:` apart from `a`.
    let a = create(&fluree, "walk-a", "ex:q ex:name 0 .").await;
    let a = insert(a, "ex:s ex:items (1 2) .").await;
    let b = create(&fluree, "walk-b", "").await;
    fluree
        .graph(&b)
        .transact()
        .insert_turtle("<http://other.example/x> <http://other.example/y> 0 .")
        .commit()
        .await
        .expect("t:1");
    let b = insert(b, "ex:t ex:items (3) .").await;
    // `c` codes `ex:` as `a` does, at the same `t`.
    let c = create(&fluree, "walk-c", "ex:q ex:name 0 .").await;
    let c = insert(c, "ex:t ex:items (3) .").await;
    // One `ex:s` with a list, and one without, at the same `t`; and one that
    // codes `ex:` apart.
    let bare = create(&fluree, "walk-bare", "ex:s ex:name 0 .").await;
    let held = create(&fluree, "walk-held", "ex:s ex:name 0 ; ex:items (5) .").await;
    let far = create(&fluree, "walk-far", "").await;
    fluree
        .graph(&far)
        .transact()
        .insert_turtle("<http://other.example/x> <http://other.example/y> 0 .")
        .commit()
        .await
        .expect("t:1");
    let far = insert(far, "ex:s ex:items (6) .").await;
    let nolist = create(&fluree, "walk-nolist", "ex:q ex:name 1 .").await;
    let times = create(&fluree, "walk-times", "ex:q ex:name 1 .").await;
    let times = insert(times, "ex:s ex:items (1 2) .").await;
    let chains = create(
        &fluree,
        "walk-chains",
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
         ex:u ex:items _:c . _:c rdf:first 7 ; rdf:rest rdf:nil ; ex:note 0 .",
    )
    .await;
    let code = |ledger: &LedgerState| {
        ledger
            .snapshot
            .encode_iri("http://example.org/items")
            .map(|p| p.namespace_code)
    };
    for (one, other) in [(&a, &b), (&bare, &far)] {
        assert_ne!(
            code(&load(&fluree, one).await),
            code(&load(&fluree, other).await),
            "{one} and {other} code ex: apart"
        );
    }

    let rdf = format!("{PREFIX}PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n");
    let walk = "?s ex:items/rdf:rest*/rdf:first ?x";
    let rows = |text: String| {
        let fluree = &fluree;
        async move {
            dataset_rows(fluree, &text)
                .await
                .unwrap_or_else(|e| panic!("query: {e}\n{text}"))
        }
    };
    let row = |cells: &[&str]| cells.iter().map(ToString::to_string).collect::<Vec<_>>();
    let refused = |result: fluree_db_api::Result<Vec<Vec<String>>>, expected: &str| {
        let err = result
            .err()
            .unwrap_or_else(|| panic!("refused: {expected}"));
        assert!(err.to_string().contains(expected), "{expected}: {err}");
        assert_eq!(err.status_code(), 400, "{err}");
    };
    for indexed in [false, true] {
        if indexed {
            for id in [&a, &b, &c, &bare, &held, &far, &nolist, &times, &chains] {
                support::rebuild_and_publish_index(&fluree, id).await;
            }
        }
        let lane = format!("indexed {indexed}");

        for from in [
            format!("FROM <{times}@t:1> FROM <{times}@t:2>"),
            format!("FROM <{times}@t:2> FROM <{times}@t:1>"),
        ] {
            assert_eq!(
                rows(format!(
                    "{rdf}SELECT ?x {from} WHERE {{ {walk} }} ORDER BY ?x"
                ))
                .await,
                [row(&["1"]), row(&["2"])],
                "{lane}: {from}"
            );
        }

        let named = |first: &str, second: &str, graph: &str| {
            format!(
                "{rdf}SELECT ?s ?x FROM NAMED <{first}> FROM NAMED <{second}> \
                 WHERE {{ GRAPH {graph} {{ {walk} }} }} ORDER BY ?x"
            )
        };
        for other in [&b, &c] {
            assert_eq!(
                rows(named(&a, other, "?g")).await,
                [
                    row(&["http://example.org/s", "1"]),
                    row(&["http://example.org/s", "2"]),
                    row(&["http://example.org/t", "3"]),
                ],
                "{lane}: each ledger's own lists, beside {other}"
            );
        }
        for (first, second) in [(&bare, &held), (&held, &bare)] {
            let text = format!(
                "{rdf}SELECT ?x FROM NAMED <{first}> FROM NAMED <{second}> \
                 WHERE {{ GRAPH ?g {{ ?s ex:name 0 . {walk} }} }}"
            );
            assert_eq!(rows(text).await, [row(&["5"])], "{lane}: {first}, {second}");
        }
        // A subject bound in one ledger walks in another by its IRI.
        let joined = format!(
            "{rdf}SELECT ?x FROM NAMED <{bare}> FROM NAMED <{far}> \
             WHERE {{ GRAPH <{bare}> {{ ?s ex:name 0 }} GRAPH <{far}> {{ {walk} }} }}"
        );
        assert_eq!(rows(joined).await, [row(&["6"])], "{lane}");
        let constant = format!(
            "{rdf}SELECT ?x FROM NAMED <{a}> FROM NAMED <{b}> \
             WHERE {{ GRAPH <{b}> {{ ex:t ex:items/rdf:rest*/rdf:first ?x }} }}"
        );
        assert_eq!(rows(constant).await, [row(&["3"])], "{lane}");
        for graph in ["?g".to_string(), format!("<{b}>")] {
            assert_eq!(
                rows(named(&nolist, &b, &graph)).await,
                [row(&["http://example.org/t", "3"])],
                "{lane}: GRAPH {graph} past a ledger holding no list"
            );
        }
        let members = fluree
            .query_from()
            .jsonld(&json!({
                "@context": {
                    "ex": "http://example.org/",
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                    "members": {"@path": "ex:items/rdf:rest*/rdf:first"}
                },
                "fromNamed": [a, b],
                "select": ["?x"],
                "where": [["graph", "?g", {"@id": "?s", "members": "?x"}]],
                "orderBy": ["?x"]
            }))
            .execute_formatted()
            .await
            .expect("JSON-LD walk over named graphs");
        assert_eq!(members, json!([[1], [2], [3]]), "{lane}");

        let across = format!("{rdf}SELECT ?x FROM <{a}> FROM <{b}> WHERE {{ {walk} }}");
        refused(dataset_rows(&fluree, &across).await, "spanning ledgers");
        let across = fluree
            .query_from()
            .jsonld(&json!({
                "@context": {
                    "ex": "http://example.org/",
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                    "members": {"@path": "ex:items/rdf:rest*/rdf:first"}
                },
                "from": [a, b],
                "select": ["?x"],
                "where": {"@id": "?s", "members": "?x"}
            }))
            .execute_formatted()
            .await
            .map(|_| Vec::new());
        refused(across, "spanning ledgers");

        let history = |id: &str| {
            format!("{rdf}SELECT ?x FROM <{id}@t:1> TO <{id}@t:latest> WHERE {{ {walk} }}")
        };
        refused(dataset_rows(&fluree, &history(&a)).await, "history query");
        let chained = rows(history(&chains)).await;
        assert!(chained.contains(&row(&["7"])), "{lane}: {chained:?}");
    }
}
