//! Constant IRIs bound in a query, once the namespace table is full.
//!
//! A write stores an IRI whose prefix has no code whole, as
//! `Sid(OVERFLOW, iri)`. A query must encode the same IRI the same way: a
//! constant encoded as `Sid(EMPTY, iri)` still finds a persisted subject,
//! whose dictionary lookup goes by IRI, but matches nothing in novelty, which
//! compares Sids. Every shape below binds an overflow IRI; `OLD` is indexed
//! and `NEW` is only in novelty, and `OLD` also takes a new value in novelty.
//! A named graph's IRI and a datatype's take the same encoding: before it, a
//! write into an overflow-IRI graph was refused as naming an unknown graph.
//! The planner and the schema catalogs find an overflow predicate's and
//! class's stats under that Sid too, and a policy identity's IRI is found
//! under it, so its policies apply.

use crate::support;
use fluree_db_api::{Batch, Fluree, FlureeBuilder, LedgerState};
use fluree_vocab::namespaces;
use serde_json::{json, Value};
use std::sync::Arc;

const LABEL: &str = "http://example.org/label";
const LINK: &str = "http://example.org/link";
const OLD: &str = "http://old.example/s";
const OLD_P: &str = "http://old.example/p";
const OLD_O: &str = "http://old.example/o";
const NEW: &str = "http://new.example/s";
const NEW_P: &str = "http://new.example/p";
const NEW_O: &str = "http://new.example/o";
const GRAPH: &str = "http://graphs.example/g";
const CODE: &str = "http://example.org/code";
const DT: &str = "http://types.example/code";
const CLASS: &str = "http://old.example/C";
const CLASS_P: &str = "http://old.example/q";

fn row_count(result: &fluree_db_api::QueryResult) -> usize {
    result.batches.iter().map(Batch::len).sum()
}

/// One subject per namespace, enough to take every user code: an IRI with
/// a new prefix written after these is stored whole, under `OVERFLOW`.
fn table_fillers() -> Vec<Value> {
    (0..namespaces::OVERFLOW as usize)
        .map(|i| json!({"@id": format!("http://ns{i}.example/s"), LABEL: "filler"}))
        .collect()
}

async fn sparql_update(fluree: &Fluree, ledger: LedgerState, body: &str) -> LedgerState {
    let parsed = fluree_db_sparql::parse_sparql(body);
    assert!(!parsed.has_errors(), "{body}: {:?}", parsed.diagnostics);
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &parsed.ast.expect("ast"),
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect("SPARQL update")
        .ledger
}

#[tokio::test]
async fn constant_overflow_iris_match_in_novelty_and_the_index() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("file fluree");
    let id = "it/overflow-iri-bound:main";
    let ledger = fluree.create_ledger(id).await.expect("create");

    let mut graph = table_fillers();
    graph.push(json!({
        "@id": OLD, LABEL: "old", OLD_P: 1, LINK: {"@id": OLD_O},
        CODE: {"@value": "x", "@type": DT}
    }));
    graph.push(json!({"@id": "http://old.example/t", "@type": CLASS, CLASS_P: "v"}));
    fluree
        .insert(ledger, &json!({"@graph": graph}))
        .await
        .expect("seed");
    support::rebuild_and_publish_index(&fluree, id).await;
    let ledger = fluree.ledger(id).await.expect("reload");

    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@id": NEW, LABEL: "new", NEW_P: 2, LINK: {"@id": NEW_O},
                CODE: {"@value": "y", "@type": DT}
            }),
        )
        .await
        .expect("novel subject")
        .ledger;
    let ledger = fluree
        .insert(ledger, &json!({"@id": OLD, OLD_P: 3}))
        .await
        .expect("new value on an indexed subject")
        .ledger;
    let ledger = sparql_update(
        &fluree,
        ledger,
        &format!("INSERT DATA {{ GRAPH <{GRAPH}> {{ <{NEW}> <{NEW_P}> 4 }} }}"),
    )
    .await;
    assert!(
        ledger
            .snapshot
            .namespaces()
            .contains_key(&(namespaces::OVERFLOW - 1))
            && !ledger
                .snapshot
                .namespace_reverse()
                .contains_key("http://new.example/"),
        "the fixture must fill the namespace table before its IRIs"
    );

    let mut failures = Vec::new();

    // The planner finds an overflow predicate's stats under the Sid a query
    // gives it: the indexed count, with novelty's counts merged into it.
    // Missing, it plans the predicate as absent, scanning it first in full.
    let explained = fluree
        .explain_sparql(
            &support::graphdb_from_ledger(&ledger),
            &format!("SELECT ?s ?n WHERE {{ ?s <{OLD_P}> ?n }}"),
        )
        .await
        .expect("explain");
    let estimate = &explained["plan"]["logical"][0]["estimate"]["row-count"];
    if *estimate != json!(1) {
        failures.push(format!(
            "indexed estimate for <{OLD_P}>: {estimate}, want 1"
        ));
    }
    let merged = fluree_db_core::StatsView::from_db_stats_with_namespaces(
        &Arc::new(ledger.current_stats()),
        &ledger.snapshot,
    );
    let count = ledger
        .snapshot
        .encode_iri(OLD_P)
        .and_then(|p| merged.get_property(&p))
        .map(|p| p.count);
    if count != Some(2) {
        failures.push(format!(
            "merged count for <{OLD_P}>: {count:?}, want Some(2)"
        ));
    }

    // Catalogs built from the stats name overflow classes and predicates.
    let db = support::graphdb_from_ledger(&ledger);
    for (call, want) in [
        ("CALL db.labels()", CLASS),
        ("CALL db.propertyKeys()", OLD_P),
        ("CALL db.propertyKeys()", CLASS_P),
    ] {
        let rows = fluree
            .query_cypher(&db, call)
            .await
            .expect(call)
            .to_jsonld_async(db.as_graph_db_ref())
            .await
            .expect("jsonld");
        if !rows
            .as_array()
            .expect("rows")
            .iter()
            .any(|row| row[0] == want)
        {
            failures.push(format!("{call} lacks {want}: {rows}"));
        }
    }
    #[cfg(feature = "graphql")]
    {
        match fluree_db_api::graphql::schema_sdl(&db).await {
            Ok(sdl) if sdl.contains("type C {") && sdl.contains("q: [String!]") => {}
            other => failures.push(format!(
                "GraphQL schema lacks type C with field q: {other:?}"
            )),
        }
    }

    let sparql = [
        (format!("SELECT ?p ?o WHERE {{ <{NEW}> ?p ?o }}"), 4),
        (
            format!("SELECT ?p ?o WHERE {{ VALUES ?s {{ <{NEW}> }} ?s ?p ?o }}"),
            4,
        ),
        (format!("SELECT ?s ?o WHERE {{ ?s <{NEW_P}> ?o }}"), 1),
        (format!("SELECT ?s WHERE {{ ?s <{LINK}> <{NEW_O}> }}"), 1),
        (format!("SELECT ?o WHERE {{ <{NEW}> <{NEW_P}> ?o }}"), 1),
        (format!("SELECT ?p WHERE {{ <{NEW}> ?p <{NEW_O}> }}"), 1),
        (format!("SELECT ?p ?o WHERE {{ <{OLD}> ?p ?o }}"), 5),
        (format!("SELECT ?n WHERE {{ <{OLD}> <{OLD_P}> ?n }}"), 2),
        (
            format!("SELECT ?n WHERE {{ <{OLD}> <{OLD_P}> ?n FILTER(?n = 3) }}"),
            1,
        ),
        (format!("SELECT ?s WHERE {{ ?s <{OLD_P}> 3 }}"), 1),
        (format!("SELECT ?p WHERE {{ <{OLD}> ?p 3 }}"), 1),
        (format!("SELECT ?s WHERE {{ ?s <{LINK}> <{OLD_O}> }}"), 1),
        (format!("SELECT DISTINCT ?p WHERE {{ <{OLD}> ?p ?o }}"), 4),
        (
            format!("SELECT ?s WHERE {{ ?s <{CODE}> \"x\"^^<{DT}> }}"),
            1,
        ),
        (
            format!("SELECT ?s WHERE {{ ?s <{CODE}> \"y\"^^<{DT}> }}"),
            1,
        ),
        (
            format!("SELECT ?s WHERE {{ ?s <{CODE}> ?c FILTER(DATATYPE(?c) = <{DT}>) }}"),
            2,
        ),
        (
            format!("SELECT ?s WHERE {{ VALUES ?s {{ <{OLD}> <{NEW}> }} ?s <{LINK}> <{OLD_O}> }}"),
            1,
        ),
        (
            format!("SELECT ?s WHERE {{ VALUES ?s {{ <{OLD}> <{NEW}> }} ?s <{LINK}> <{NEW_O}> }}"),
            1,
        ),
        (
            format!("SELECT ?s WHERE {{ ?s <{OLD_P}> ?n . ?s <{LINK}> <{OLD_O}> }}"),
            2,
        ),
        (
            format!("SELECT ?p WHERE {{ <{OLD}> ?p 1 . <{OLD}> ?p 3 }}"),
            1,
        ),
        (
            format!("SELECT ?s ?o WHERE {{ GRAPH <{GRAPH}> {{ ?s <{NEW_P}> ?o }} }}"),
            1,
        ),
        (
            format!("SELECT ?o WHERE {{ GRAPH <{GRAPH}> {{ <{NEW}> <{NEW_P}> ?o }} }}"),
            1,
        ),
    ];
    for (q, want) in &sparql {
        let got = row_count(&support::query_sparql(&fluree, &ledger, q).await.expect(q));
        if got != *want {
            failures.push(format!("{q}: {got} rows, want {want}"));
        }
    }
    for q in [
        format!("ASK {{ <{NEW}> <{NEW_P}> 2 }}"),
        format!("ASK {{ <{OLD}> <{OLD_P}> 3 }}"),
    ] {
        let got = support::query_sparql(&fluree, &ledger, &q)
            .await
            .expect(&q)
            .to_sparql_json(&ledger.snapshot)
            .expect("sparql json");
        if got["boolean"] != Value::Bool(true) {
            failures.push(format!("{q}: {}", got["boolean"]));
        }
    }

    let jsonld = [
        (
            json!({"select": ["?p", "?o"], "where": {"@id": NEW, "?p": "?o"}}),
            4,
        ),
        (
            json!({"select": ["?s", "?o"], "where": {"@id": "?s", NEW_P: "?o"}}),
            1,
        ),
        (
            json!({"select": "?s", "where": {"@id": "?s", LINK: {"@id": NEW_O}}}),
            1,
        ),
        (
            json!({"select": "?n", "where": {"@id": OLD, OLD_P: "?n"}}),
            2,
        ),
        (json!({"select": "?s", "where": {"@id": "?s", OLD_P: 3}}), 1),
    ];
    for (q, want) in &jsonld {
        let got = row_count(
            &support::query_jsonld(&fluree, &ledger, q)
                .await
                .expect("jsonld"),
        );
        if got != *want {
            failures.push(format!("{q}: {got} rows, want {want}"));
        }
    }

    // A retraction written with the constants deletes the novel fact.
    let ledger = sparql_update(
        &fluree,
        ledger,
        &format!("DELETE DATA {{ <{NEW}> <{NEW_P}> 2 . <{OLD}> <{OLD_P}> 3 }}"),
    )
    .await;
    for (q, want) in [
        (format!("SELECT ?o WHERE {{ <{NEW}> <{NEW_P}> ?o }}"), 0),
        (format!("SELECT ?n WHERE {{ <{OLD}> <{OLD_P}> ?n }}"), 1),
    ] {
        let got = row_count(&support::query_sparql(&fluree, &ledger, &q).await.expect(&q));
        if got != want {
            failures.push(format!("after delete: {q}: {got} rows, want {want}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// An identity whose IRI is stored under `OVERFLOW` has its `f:policyClass`
/// policies applied to its queries, while it is only in novelty and once it
/// is indexed. Its IRI must encode as the write stored it for the identity
/// to be found at all.
#[tokio::test]
async fn an_overflow_identity_has_its_policies_applied() {
    const SECRET: &str = "http://example.org/secret";
    const USER: &str = "http://example.org/user";
    const IDENTITY: &str = "http://identities.example/alice";
    const POLICY_CLASS: &str = "http://identities.example/EmployeePolicy";

    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("file fluree");
    let id = "it/overflow-iri-identity:main";
    let ledger = fluree.create_ledger(id).await.expect("create");
    let mut graph = table_fillers();
    graph.push(json!({"@id": OLD, SECRET: "old's"}));
    graph.push(json!({"@id": OLD_O, SECRET: "someone else's"}));
    let ledger = fluree
        .insert(ledger, &json!({"@graph": graph}))
        .await
        .expect("seed")
        .ledger;

    // Only the identity's own user's secret is visible to it.
    let only_its_user = serde_json::to_string(&json!({
        "where": {"@id": "?$identity", USER: {"@id": "?$this"}}
    }))
    .unwrap();
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@graph": [
                    {
                        "@id": IDENTITY,
                        "f:policyClass": {"@id": POLICY_CLASS},
                        USER: {"@id": OLD}
                    },
                    {
                        "@id": "http://identities.example/secretPolicy",
                        "@type": ["f:AccessPolicy", POLICY_CLASS],
                        "f:required": true,
                        "f:onProperty": [{"@id": SECRET}],
                        "f:action": {"@id": "f:view"},
                        "f:query": {"@value": only_its_user, "@type": "http://www.w3.org/2001/XMLSchema#string"}
                    }
                ]
            }),
        )
        .await
        .expect("identity and policy")
        .ledger;
    assert!(
        ledger
            .snapshot
            .encode_iri(IDENTITY)
            .is_some_and(|sid| sid.namespace_code == namespaces::OVERFLOW),
        "the fixture must store the identity under OVERFLOW"
    );

    let secrets_seen = || async {
        let query = json!({
            "from": id,
            "opts": {"identity": IDENTITY, "default-allow": true},
            "select": "?secret",
            "where": {"@id": "?s", SECRET: "?secret"}
        });
        row_count(&fluree.query_connection(&query).await.expect("query"))
    };
    assert_eq!(secrets_seen().await, 1, "identity only in novelty");
    support::rebuild_and_publish_index(&fluree, id).await;
    fluree.disconnect_ledger(id).await;
    assert_eq!(secrets_seen().await, 1, "identity indexed");
}
