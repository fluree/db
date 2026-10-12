//! Interactive Cypher transactions (the engine behind Bolt explicit
//! transactions).

use fluree_db_api::{FlureeBuilder, GovernanceOptions, TrackingOptions};

/// A ledger created but never written has an unborn head ref (no id, `t`
/// 0). The transaction's publish CAS must expect exactly that, as the
/// autocommit path does, or its first commit is refused as a conflict.
#[tokio::test]
async fn commits_on_a_ledger_with_no_commits_yet() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = "it/cypher-txn/empty:main";
    fluree.create_ledger(ledger).await.unwrap();

    let mut txn = fluree
        .begin_cypher_transaction(
            ledger,
            GovernanceOptions::default(),
            TrackingOptions::default(),
        )
        .await
        .unwrap();
    fluree
        .cypher_transaction_write(&mut txn, r#"CREATE (:Person {name: "Alice"})"#, None)
        .await
        .unwrap();
    let committed = fluree.commit_cypher_transaction(txn).await.unwrap();
    assert_eq!(committed.receipt.t, 1);

    let db = fluree.db(ledger).await.unwrap();
    let result = fluree
        .query_cypher(&db, "MATCH (p:Person) RETURN p.name")
        .await
        .unwrap();
    let (_, rows) = result.to_cypher_typed_table(&db).await.unwrap();
    assert_eq!(rows.len(), 1);
}
