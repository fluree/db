//! `Transaction`: operations staged one at a time, readable before commit,
//! committed as one commit.

use fluree_db_api::{
    CommitOpts, Fluree, FlureeBuilder, GovernanceOptions, GraphDb, TrackingOptions, Transaction,
    TransactionOptions, TxnOperation,
};
use serde_json::{json, Value as JsonValue};

const LEDGER: &str = "it/transaction:main";
const PREFIX: &str = "PREFIX ex: <http://example.org/> ";

async fn fluree() -> Fluree {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger(LEDGER).await.expect("create");
    fluree
}

async fn select(fluree: &Fluree, db: &GraphDb, sparql: &str) -> Vec<JsonValue> {
    let result = fluree
        .query(db, format!("{PREFIX}{sparql}").as_str())
        .await
        .expect("query");
    let mut rows = match result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("format")
    {
        JsonValue::Array(rows) => rows,
        other => vec![other],
    };
    rows.sort_by_key(ToString::to_string);
    rows
}

async fn begin(fluree: &Fluree) -> Transaction {
    fluree
        .begin_transaction(LEDGER, TransactionOptions::default())
        .await
        .expect("begin")
}

async fn head(fluree: &Fluree) -> GraphDb {
    fluree.db(LEDGER).await.expect("db")
}

async fn people(fluree: &Fluree, db: &GraphDb) -> Vec<JsonValue> {
    select(
        fluree,
        db,
        "SELECT ?name ?age WHERE { ?s ex:name ?name OPTIONAL { ?s ex:age ?age } }",
    )
    .await
}

fn insert(id: &str, name: &str, age: i64) -> TxnOperation {
    TxnOperation::Insert(json!({
        "@context": { "ex": "http://example.org/" },
        "@id": format!("ex:{id}"), "ex:name": name, "ex:age": age,
    }))
}

const BIRTHDAY: &str = "PREFIX ex: <http://example.org/> \
    DELETE { ex:alice ex:age ?age } INSERT { ex:alice ex:age ?next } \
    WHERE { ex:alice ex:age ?age BIND(?age + 1 AS ?next) }";

#[tokio::test]
async fn operations_see_earlier_ones_and_commit_once() {
    let fluree = fluree().await;
    let mut txn = begin(&fluree).await;
    txn.stage(insert("alice", "Alice", 30)).await.unwrap();
    // Reads alice's age from the insert above.
    txn.stage(TxnOperation::SparqlUpdate(BIRTHDAY.into()))
        .await
        .unwrap();
    txn.stage(TxnOperation::InsertTurtle(
        "@prefix ex: <http://example.org/> . ex:bob ex:name \"Bob\" .".into(),
    ))
    .await
    .unwrap();

    let staged = txn.db().await.unwrap();
    assert_eq!(
        people(&fluree, &staged).await,
        vec![json!(["Alice", 31]), json!(["Bob", null])]
    );
    assert!(people(&fluree, &head(&fluree).await).await.is_empty());

    let result = txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!(result.receipt.t, 1);
    // alice's age 30 was asserted and retracted within the transaction, so
    // the commit carries only the net facts.
    assert_eq!(
        (result.receipt.assert_count, result.receipt.retract_count),
        (3, 0)
    );
    assert_eq!(
        people(&fluree, &head(&fluree).await).await,
        vec![json!(["Alice", 31]), json!(["Bob", null])]
    );
}

#[tokio::test]
async fn a_failed_operation_leaves_the_transaction_as_it_was() {
    let fluree = fluree().await;
    let mut txn = begin(&fluree).await;
    txn.stage(insert("alice", "Alice", 30)).await.unwrap();
    txn.stage(TxnOperation::SparqlUpdate(
        "INSERT DATA { not sparql".into(),
    ))
    .await
    .expect_err("parse error");
    assert_eq!(txn.len(), 1);
    txn.stage(insert("bob", "Bob", 40)).await.unwrap();
    assert_eq!(
        people(&fluree, &txn.db().await.unwrap()).await,
        vec![json!(["Alice", 30]), json!(["Bob", 40])]
    );
    txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!(
        people(&fluree, &head(&fluree).await).await,
        vec![json!(["Alice", 30]), json!(["Bob", 40])]
    );
}

#[tokio::test]
async fn rollback_to_a_savepoint_discards_the_operations_after_it() {
    let fluree = fluree().await;
    let mut txn = begin(&fluree).await;
    txn.stage(insert("alice", "Alice", 30)).await.unwrap();
    let savepoint = txn.savepoint();
    txn.stage(insert("bob", "Bob", 40)).await.unwrap();
    txn.stage(insert("cy", "Cy", 50)).await.unwrap();
    txn.rollback_to(savepoint).await.unwrap();
    assert_eq!(txn.len(), 1);
    // The staged state is rebuilt too, so later operations see only alice.
    txn.stage(TxnOperation::SparqlUpdate(BIRTHDAY.into()))
        .await
        .unwrap();
    assert_eq!(
        people(&fluree, &txn.db().await.unwrap()).await,
        vec![json!(["Alice", 31])]
    );
    txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!(
        people(&fluree, &head(&fluree).await).await,
        vec![json!(["Alice", 31])]
    );
}

#[tokio::test]
async fn net_zero_and_empty_transactions_commit_nothing() {
    let fluree = fluree().await;
    let mut txn = begin(&fluree).await;
    txn.stage(insert("alice", "Alice", 30)).await.unwrap();
    txn.stage(TxnOperation::SparqlUpdate(format!(
        "{PREFIX}DELETE DATA {{ ex:alice ex:name \"Alice\" ; ex:age 30 }}"
    )))
    .await
    .unwrap();
    let result = txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!((result.receipt.t, result.receipt.flake_count), (0, 0));

    let txn = begin(&fluree).await;
    let result = txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!((result.receipt.t, result.receipt.flake_count), (0, 0));
    assert_eq!(fluree.commit_log(LEDGER, None).await.unwrap().1, 0);
}

/// A commit that lands after the transaction began, on other subjects: the
/// staged result re-bases over it.
#[tokio::test]
async fn commits_over_a_concurrent_write_to_other_subjects() {
    let fluree = fluree().await;
    let mut txn = begin(&fluree).await;
    txn.stage(insert("alice", "Alice", 30)).await.unwrap();

    fluree
        .graph(LEDGER)
        .transact()
        .insert(&json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:carol", "ex:name": "Carol",
        }))
        .commit()
        .await
        .unwrap();

    let result = txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!(result.receipt.t, 2);
    assert_eq!(
        people(&fluree, &head(&fluree).await).await,
        vec![json!(["Alice", 30]), json!(["Carol", null])]
    );
}

/// A commit that changes what an operation's `WHERE` matches: the operations
/// stage again over it, as a single write does when it loses a race.
#[tokio::test]
async fn restages_over_a_concurrent_write_its_update_matches() {
    let fluree = fluree().await;
    insert_alice(&fluree, 30).await;

    let mut txn = begin(&fluree).await;
    txn.stage(TxnOperation::SparqlUpdate(BIRTHDAY.into()))
        .await
        .unwrap();
    set_alice_age(&fluree, 50).await;

    txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!(
        people(&fluree, &head(&fluree).await).await,
        vec![json!(["Alice", 51])]
    );
}

/// A transaction that was read may have decided its writes from what it read;
/// staging them again over a commit it never saw would lose that commit's
/// update. It refuses instead.
#[tokio::test]
async fn a_read_transaction_conflicts_when_the_ledger_moved() {
    let fluree = fluree().await;
    insert_alice(&fluree, 30).await;

    let mut txn = begin(&fluree).await;
    let ages = people(&fluree, &txn.db().await.unwrap()).await;
    assert_eq!(ages, vec![json!(["Alice", 30])]);
    // Decided from the read: 30 + 1.
    txn.stage(TxnOperation::Upsert(json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:alice", "ex:age": 31,
    })))
    .await
    .unwrap();
    set_alice_age(&fluree, 50).await;

    let err = txn.commit(CommitOpts::default()).await.unwrap_err();
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_api::TransactError::CommitConflict { .. })
        ),
        "{err}"
    );
    assert_eq!(
        people(&fluree, &head(&fluree).await).await,
        vec![json!(["Alice", 50])]
    );

    // Unmoved, a read transaction commits as usual.
    let mut txn = begin(&fluree).await;
    txn.db().await.unwrap();
    txn.stage(TxnOperation::SparqlUpdate(BIRTHDAY.into()))
        .await
        .unwrap();
    txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!(
        people(&fluree, &head(&fluree).await).await,
        vec![json!(["Alice", 51])]
    );
}

async fn insert_alice(fluree: &Fluree, age: i64) {
    fluree
        .graph(LEDGER)
        .transact()
        .insert(&json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:alice", "ex:name": "Alice", "ex:age": age,
        }))
        .commit()
        .await
        .unwrap();
}

async fn set_alice_age(fluree: &Fluree, age: i64) {
    fluree
        .graph(LEDGER)
        .transact()
        .upsert(&json!({
            "@context": { "ex": "http://example.org/" },
            "@id": "ex:alice", "ex:age": age,
        }))
        .commit()
        .await
        .unwrap();
}

async fn count(fluree: &Fluree, sparql: &str) -> usize {
    select(fluree, &head(fluree).await, sparql).await.len()
}

/// Cypher and SPARQL writes in one transaction see one another and land in
/// one commit.
#[tokio::test]
async fn cypher_and_sparql_mix_in_one_commit() {
    let fluree = fluree().await;
    let mut txn = begin(&fluree).await;
    let returned = txn
        .stage_cypher(r#"CREATE (:Person {name: "Ann"})"#, None)
        .await
        .unwrap();
    assert!(returned.is_none());
    txn.stage(TxnOperation::SparqlUpdate(
        r#"INSERT { ?p <age> 30 } WHERE { ?p <name> "Ann" }"#.into(),
    ))
    .await
    .unwrap();
    // Matches only if it sees the SPARQL update's age.
    txn.stage_cypher(
        "MATCH (p:Person {name: $name}) WHERE p.age = 30 SET p.checked = true",
        Some(serde_json::from_value(json!({"name": "Ann"})).unwrap()),
    )
    .await
    .unwrap();

    let result = txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!(result.receipt.t, 1);
    assert_eq!(
        select(
            &fluree,
            &head(&fluree).await,
            r#"SELECT ?age WHERE { ?p <name> "Ann" ; <age> ?age ; <checked> true }"#
        )
        .await,
        vec![json!([30])]
    );
}

/// A Cypher write's RETURN rows are a read: the transaction then refuses a
/// ledger that moved.
#[tokio::test]
async fn cypher_return_rows_count_as_a_read() {
    let fluree = fluree().await;
    let mut txn = begin(&fluree).await;
    let (columns, rows) = txn
        .stage_cypher(r#"CREATE (p:Person {name: "Bo"}) RETURN p"#, None)
        .await
        .unwrap()
        .expect("RETURN rows");
    assert_eq!((columns, rows.len()), (vec!["p".to_string()], 1));
    insert_alice(&fluree, 30).await;
    let err = txn.commit(CommitOpts::default()).await.unwrap_err();
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_api::TransactError::CommitConflict { .. })
        ),
        "{err}"
    );
}

/// A MERGE staged before another commit created its node is staged again
/// over that commit, so it matches instead of creating a duplicate.
#[tokio::test]
async fn a_merge_restages_over_a_concurrent_create() {
    let fluree = fluree().await;
    let mut txn = begin(&fluree).await;
    txn.stage_cypher(r#"MERGE (p:Person {name: "Cy"}) SET p.seen = true"#, None)
        .await
        .unwrap();
    fluree
        .graph(LEDGER)
        .transact()
        .sparql_update(r#"INSERT DATA { <cy> a <Person> ; <name> "Cy" }"#)
        .commit()
        .await
        .unwrap();

    txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!(
        count(
            &fluree,
            r#"SELECT ?p WHERE { ?p a <Person> ; <name> "Cy" }"#
        )
        .await,
        1
    );
    assert_eq!(
        count(&fluree, "SELECT ?p WHERE { <cy> <seen> true }").await,
        1
    );
}

/// A failed operation leaves the operations before it as they were read:
/// their blank nodes and the values `STRUUID()` computed don't change.
#[tokio::test]
async fn a_failed_operation_keeps_the_values_already_staged() {
    const ORDER: &str = "SELECT ?item ?token WHERE { ex:order ex:item ?item ; ex:token ?token }";
    let fluree = fluree().await;
    let mut txn = begin(&fluree).await;
    txn.stage(TxnOperation::Insert(json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:order", "ex:item": { "ex:sku": "A1" },
    })))
    .await
    .unwrap();
    txn.stage(TxnOperation::SparqlUpdate(format!(
        "{PREFIX}INSERT {{ ex:order ex:token ?t }} WHERE {{ BIND(STRUUID() AS ?t) }}"
    )))
    .await
    .unwrap();
    let staged = select(&fluree, &txn.db().await.unwrap(), ORDER).await;
    assert_eq!(staged.len(), 1);

    txn.stage(TxnOperation::SparqlUpdate(
        "INSERT DATA { not sparql".into(),
    ))
    .await
    .expect_err("parse error");
    assert_eq!(
        select(&fluree, &txn.db().await.unwrap(), ORDER).await,
        staged
    );
    txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!(select(&fluree, &head(&fluree).await, ORDER).await, staged);
}

/// A Cypher write that staged but whose `RETURN` failed is not added.
#[tokio::test]
async fn a_cypher_write_whose_return_fails_is_not_staged() {
    let fluree = fluree().await;
    let mut txn = begin(&fluree).await;
    txn.stage_cypher("UNWIND range(1, 4097) AS i CREATE (:Item {i: i})", None)
        .await
        .unwrap();
    // One row per item: more than a write's RETURN may produce.
    txn.stage_cypher("MATCH (i:Item) CREATE (c:Copy) RETURN c", None)
        .await
        .expect_err("too many RETURN rows");
    assert_eq!(txn.len(), 1);
    txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!(count(&fluree, "SELECT ?c WHERE { ?c a <Copy> }").await, 0);
    assert_eq!(
        count(&fluree, "SELECT ?i WHERE { ?i a <Item> }").await,
        4097
    );
}

/// A transaction staged again over a newer head is checked against the
/// policy the ledger has now: a deny rule committed while it was open
/// refuses it.
#[tokio::test]
async fn a_restage_is_checked_against_the_current_policy() {
    let fluree = fluree().await;
    fluree
        .graph(LEDGER)
        .transact()
        .sparql_update(&format!("{PREFIX}INSERT DATA {{ ex:WritePolicy a <http://www.w3.org/2000/01/rdf-schema#Class> }}"))
        .commit()
        .await
        .unwrap();
    let options = TransactionOptions {
        governance: GovernanceOptions {
            policy_class: Some(vec!["http://example.org/WritePolicy".to_string()]),
            default_allow: Some(true),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut txn = fluree.begin_transaction(LEDGER, options).await.unwrap();
    txn.stage(TxnOperation::Insert(json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:bob", "ex:ssn": "999-99-9999",
    })))
    .await
    .unwrap();

    fluree
        .graph(LEDGER)
        .transact()
        .insert(&json!({
            "@context": { "f": "https://ns.flur.ee/db#", "ex": "http://example.org/" },
            "@id": "ex:noSsn",
            "@type": ["f:AccessPolicy", "ex:WritePolicy"],
            "f:action": { "@id": "f:modify" },
            "f:required": true,
            "f:onProperty": [{ "@id": "ex:ssn" }],
            "f:allow": false,
        }))
        .commit()
        .await
        .unwrap();

    let err = txn.commit(CommitOpts::default()).await.unwrap_err();
    assert!(err.to_string().contains("Policy enforcement"), "{err}");
    assert_eq!(
        count(&fluree, "SELECT ?s WHERE { ?s ex:ssn ?ssn }").await,
        0
    );
}

/// A transaction that began with no policy is checked against one the
/// ledger's config gained before it commits.
#[tokio::test]
async fn a_policy_configured_while_open_applies_to_the_commit() {
    let fluree = fluree().await;
    let mut txn = begin(&fluree).await;
    txn.stage(insert("alice", "Alice", 30)).await.unwrap();

    fluree
        .graph(LEDGER)
        .transact()
        .upsert_turtle(&format!(
            "@prefix f: <https://ns.flur.ee/db#> .
             GRAPH <urn:fluree:{LEDGER}#config> {{
                 <urn:config:main> a f:LedgerConfig ;
                     f:policyDefaults <urn:config:policy> .
                 <urn:config:policy> f:defaultAllow false .
             }}"
        ))
        .commit()
        .await
        .unwrap();

    let err = txn.commit(CommitOpts::default()).await.unwrap_err();
    assert!(err.to_string().contains("Policy enforcement"), "{err}");
    assert!(people(&fluree, &head(&fluree).await).await.is_empty());
}

/// A rollback stages the operations before the savepoint again, and their
/// blank nodes keep the identities the caller already read.
#[tokio::test]
async fn a_rollback_keeps_the_blank_nodes_staged_before_it() {
    const ITEM: &str = "SELECT ?item WHERE { ex:order ex:item ?item }";
    let fluree = fluree().await;
    let mut txn = begin(&fluree).await;
    txn.stage(TxnOperation::Insert(json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:order", "ex:item": { "ex:sku": "A1" },
    })))
    .await
    .unwrap();
    let item = select(&fluree, &txn.db().await.unwrap(), ITEM).await;
    let savepoint = txn.savepoint();
    txn.stage(insert("bob", "Bob", 40)).await.unwrap();
    txn.rollback_to(savepoint).await.unwrap();
    assert_eq!(select(&fluree, &txn.db().await.unwrap(), ITEM).await, item);
}

fn tracked(max_fuel: Option<u64>) -> TransactionOptions {
    TransactionOptions {
        tracking: TrackingOptions {
            track_fuel: true,
            max_fuel,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn fuel_limit(fuel: f64) -> u64 {
    (fuel * fluree_db_core::tracking::MICRO_FUEL_PER_FUEL as f64) as u64
}

/// Commit one insert in a tracked transaction and return the fuel it used.
async fn fuel_of_one_insert(fluree: &Fluree) -> f64 {
    let mut txn = fluree
        .begin_transaction(LEDGER, tracked(None))
        .await
        .unwrap();
    txn.stage(insert("zed", "Zed", 1)).await.unwrap();
    let fuel = txn
        .commit(CommitOpts::default())
        .await
        .unwrap()
        .tally
        .and_then(|tally| tally.fuel)
        .expect("fuel");
    assert!(fuel > 0.0);
    fuel
}

/// The fuel a transaction's operations use adds up across all of them, so
/// a limit bounds the transaction as a whole. An operation that runs out
/// is not staged, and the transaction carries on without it.
#[tokio::test]
async fn fuel_is_tracked_across_the_transaction() {
    let fluree = fluree().await;
    let one = fuel_of_one_insert(&fluree).await;
    // Room for one insert, not two.
    let mut txn = fluree
        .begin_transaction(LEDGER, tracked(Some(fuel_limit(one * 1.5))))
        .await
        .unwrap();
    txn.stage(insert("bob", "Bob", 40)).await.unwrap();
    let err = txn.stage(insert("cy", "Cy", 50)).await.unwrap_err();
    assert!(err.to_string().contains("Fuel limit exceeded"), "{err}");
    assert_eq!(txn.len(), 1);
    assert_eq!(
        people(&fluree, &txn.db().await.unwrap()).await,
        vec![json!(["Bob", 40]), json!(["Zed", 1])]
    );
    let tally = txn.commit(CommitOpts::default()).await.unwrap().tally;
    assert!(tally.and_then(|tally| tally.fuel).expect("fuel") > one);
}

/// A rollback that fails part way — here, staging the operations before
/// the savepoint again runs out of fuel — leaves the transaction as it was:
/// it commits the operations it reports.
#[tokio::test]
async fn a_failed_rollback_leaves_the_transaction_as_it_was() {
    let fluree = fluree().await;
    let one = fuel_of_one_insert(&fluree).await;
    // Room for two inserts and part of a third.
    let mut txn = fluree
        .begin_transaction(LEDGER, tracked(Some(fuel_limit(one * 2.5))))
        .await
        .unwrap();
    txn.stage(insert("alice", "Alice", 30)).await.unwrap();
    let savepoint = txn.savepoint();
    txn.stage(insert("bob", "Bob", 40)).await.unwrap();
    let err = txn.rollback_to(savepoint).await.unwrap_err();
    assert!(err.to_string().contains("Fuel limit exceeded"), "{err}");
    assert_eq!(txn.len(), 2);
    txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!(
        people(&fluree, &head(&fluree).await).await,
        vec![json!(["Alice", 30]), json!(["Bob", 40]), json!(["Zed", 1])]
    );
}
