//! SPARQL reads a list's members as values of the property that holds it,
//! as JSON-LD does. Only the Graph Store `GET` reads a list's RDF cells
//! (`it_sparql_lists.rs`).

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

/// SPARQL has no way to read a list's cells: the RDF view is not a request
/// option.
#[tokio::test]
async fn no_pragma_reads_a_lists_cells() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = create(&fluree, "no-pragma", "ex:s ex:items (1 2) .").await;
    let ledger = load(&fluree, &id).await;
    let text = format!("# PRAGMA rdf-lists: true\n{PREFIX}SELECT ?o WHERE {{ ex:s ex:items ?o }}");
    let err = support::query_sparql(&fluree, &ledger, &text)
        .await
        .expect_err("an unknown pragma is refused");
    assert!(err.to_string().contains("unknown pragma"), "{err}");
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
/// property keeps that list, and adds the source's beside it.
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
    let firsts = sparql(
        &fluree,
        &ledger,
        &format!(
            "{PREFIX}PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
             SELECT ?x WHERE {{ GRAPH ex:g2 {{ ?c rdf:first ?x }} }}"
        ),
    )
    .await;
    assert_eq!(
        firsts,
        [json!([1]), json!([2])],
        "the added list, as its triples"
    );
    let members = sparql(
        &fluree,
        &ledger,
        &format!("{PREFIX}SELECT ?o WHERE {{ GRAPH ex:g2 {{ ex:s ex:items ?o FILTER(isLiteral(?o)) }} }}"),
    )
    .await;
    assert_eq!(
        members,
        [json!([3]), json!([4])],
        "the destination's own list"
    );

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
