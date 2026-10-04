//! `Transaction`: operations staged one at a time, readable before commit,
//! committed as one commit.

use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, GraphDb, TxnOperation};
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
    let mut txn = fluree.begin_transaction(LEDGER, None).await.unwrap();
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
    let mut txn = fluree.begin_transaction(LEDGER, None).await.unwrap();
    txn.stage(insert("alice", "Alice", 30)).await.unwrap();
    txn.stage(TxnOperation::SparqlUpdate(
        "INSERT DATA { not sparql".into(),
    ))
    .await
    .expect_err("parse error");
    assert_eq!(txn.operations().len(), 1);
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
async fn net_zero_and_empty_transactions_commit_nothing() {
    let fluree = fluree().await;
    let mut txn = fluree.begin_transaction(LEDGER, None).await.unwrap();
    txn.stage(insert("alice", "Alice", 30)).await.unwrap();
    txn.stage(TxnOperation::SparqlUpdate(format!(
        "{PREFIX}DELETE DATA {{ ex:alice ex:name \"Alice\" ; ex:age 30 }}"
    )))
    .await
    .unwrap();
    let result = txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!((result.receipt.t, result.receipt.flake_count), (0, 0));

    let txn = fluree.begin_transaction(LEDGER, None).await.unwrap();
    let result = txn.commit(CommitOpts::default()).await.unwrap();
    assert_eq!((result.receipt.t, result.receipt.flake_count), (0, 0));
    assert_eq!(fluree.commit_log(LEDGER, None).await.unwrap().1, 0);
}

/// A commit that lands after the transaction began, on other subjects: the
/// staged result re-bases over it.
#[tokio::test]
async fn commits_over_a_concurrent_write_to_other_subjects() {
    let fluree = fluree().await;
    let mut txn = fluree.begin_transaction(LEDGER, None).await.unwrap();
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

    let mut txn = fluree.begin_transaction(LEDGER, None).await.unwrap();
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

    let mut txn = fluree.begin_transaction(LEDGER, None).await.unwrap();
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
    let mut txn = fluree.begin_transaction(LEDGER, None).await.unwrap();
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
