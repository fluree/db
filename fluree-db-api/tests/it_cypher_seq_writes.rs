// Cypher query strings are written as raw strings (`r#"..."#`) for consistency
// even when a given query has no inner quotes.
#![allow(clippy::needless_raw_string_hashes)]

//! Multi-clause Cypher write statements (the sequential driver).
//!
//! Each statement stages clause-by-clause into ONE commit; later clauses
//! observe earlier clauses' writes (MERGE guards included), rows pipe
//! between clauses, and a trailing RETURN answers from the final row table.
//!
//! See `docs/design/cypher-sequential-writes.md`.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, graphdb_from_ledger};

async fn count(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    cypher: &str,
) -> i64 {
    let db = graphdb_from_ledger(ledger);
    let cj = fluree
        .query_cypher(&db, cypher)
        .await
        .unwrap_or_else(|e| panic!("count query failed: {cypher}: {e}"))
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    cj["results"][0]["data"][0]["row"][0]
        .as_i64()
        .unwrap_or_else(|| panic!("count query returned no integer: {cypher}: {cj}"))
}

fn params(v: serde_json::Value) -> fluree_db_cypher::ParamMap {
    v.as_object().expect("params object").clone()
}

#[tokio::test]
async fn merge_chain_creates_nodes_and_edge_once() {
    // The headline shape: two node MERGEs and a relationship MERGE in one
    // statement — the second and third guards must observe the first's
    // creations within the same commit.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:chain");

    let stmt = r#"MERGE (a:Person {name: "Alice"}) MERGE (b:City {name: "Paris"}) MERGE (a)-[:LIVES_IN]->(b)"#;
    let r1 = fluree.transact_cypher(l, stmt).await.expect("first run");
    assert!(r1.receipt.flake_count > 0, "first run stages flakes");
    let t1 = r1.ledger.t();

    assert_eq!(
        count(&fluree, &r1.ledger, "MATCH (n:Person) RETURN count(n)").await,
        1
    );
    assert_eq!(
        count(&fluree, &r1.ledger, "MATCH (n:City) RETURN count(n)").await,
        1
    );
    assert_eq!(
        count(
            &fluree,
            &r1.ledger,
            "MATCH (:Person)-[r:LIVES_IN]->(:City) RETURN count(r)"
        )
        .await,
        1
    );

    // Idempotence: everything exists now, so the statement is a no-op.
    let r2 = fluree
        .transact_cypher(r1.ledger, stmt)
        .await
        .expect("second run");
    assert_eq!(r2.receipt.flake_count, 0, "second run is a no-op");
    assert_eq!(r2.ledger.t(), t1, "no-op does not advance t");
    assert_eq!(
        count(&fluree, &r2.ledger, "MATCH (n:Person) RETURN count(n)").await,
        1
    );
    assert_eq!(
        count(
            &fluree,
            &r2.ledger,
            "MATCH (:Person)-[r:LIVES_IN]->(:City) RETURN count(r)"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn merge_chain_reuses_existing_nodes() {
    // Alice pre-exists: the chain must bind her, create only the city and
    // the edge.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:partial");
    let l = fluree
        .transact_cypher(l, r#"CREATE (a:Person {name: "Alice", age: 30})"#)
        .await
        .expect("seed alice")
        .ledger;

    let r = fluree
        .transact_cypher(
            l,
            r#"MERGE (a:Person {name: "Alice"}) MERGE (b:City {name: "Paris"}) MERGE (a)-[:LIVES_IN]->(b)"#,
        )
        .await
        .expect("chain");

    // Still one Person (the original, with her age), one City, one edge off
    // the ORIGINAL node.
    assert_eq!(
        count(&fluree, &r.ledger, "MATCH (n:Person) RETURN count(n)").await,
        1
    );
    assert_eq!(
        count(
            &fluree,
            &r.ledger,
            r#"MATCH (a:Person {age: 30})-[r:LIVES_IN]->(:City {name: "Paris"}) RETURN count(r)"#
        )
        .await,
        1,
        "edge hangs off the pre-existing Alice"
    );
}

#[tokio::test]
async fn unwind_batch_relationship_upsert() {
    // The canonical loading idiom: per-row endpoint upsert + relationship
    // MERGE. Shared endpoints across rows must create once.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:batch");

    let stmt = r#"UNWIND $rows AS row
        MERGE (a:Person {id: row.src})
        MERGE (b:Person {id: row.dst})
        MERGE (a)-[:KNOWS]->(b)"#;
    let p = params(json!({
        "rows": [
            {"src": "alice", "dst": "bob"},
            {"src": "alice", "dst": "carol"},
            {"src": "bob",   "dst": "carol"},
        ]
    }));
    let r1 = fluree
        .transact_cypher_with_params(l, stmt, Some(&p))
        .await
        .expect("batch upsert");

    assert_eq!(
        count(&fluree, &r1.ledger, "MATCH (n:Person) RETURN count(n)").await,
        3,
        "alice/bob/carol created once each despite repeats across rows"
    );
    assert_eq!(
        count(
            &fluree,
            &r1.ledger,
            "MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(r)"
        )
        .await,
        3
    );

    // Idempotent re-run.
    let r2 = fluree
        .transact_cypher_with_params(r1.ledger, stmt, Some(&p))
        .await
        .expect("re-run");
    assert_eq!(r2.receipt.flake_count, 0, "everything already exists");
    assert_eq!(
        count(&fluree, &r2.ledger, "MATCH (n:Person) RETURN count(n)").await,
        3
    );
}

#[tokio::test]
async fn match_merge_node_creates_once_per_distinct_key() {
    // Previously rejected ("cartesian footgun"): a node MERGE under a plain
    // MATCH. The driver creates once per distinct absent key and binds every
    // row — Cypher's actual semantics.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:match-merge");
    let l = fluree
        .transact_cypher_with_params(
            l,
            r#"UNWIND $rows AS row CREATE (p:Player {name: row.name, team: row.team})"#,
            Some(&params(json!({"rows": [
                {"name": "Ann", "team": "Red"},
                {"name": "Ben", "team": "Red"},
                {"name": "Cal", "team": "Blue"},
            ]}))),
        )
        .await
        .expect("seed players")
        .ledger;

    let r = fluree
        .transact_cypher(
            l,
            r#"MATCH (p:Player) MERGE (t:Team {name: p.team}) MERGE (p)-[:PLAYS_FOR]->(t)"#,
        )
        .await
        .expect("match-merge chain");

    assert_eq!(
        count(&fluree, &r.ledger, "MATCH (t:Team) RETURN count(t)").await,
        2,
        "two distinct teams — Red created once for two players"
    );
    assert_eq!(
        count(
            &fluree,
            &r.ledger,
            "MATCH (:Player)-[r:PLAYS_FOR]->(:Team) RETURN count(r)"
        )
        .await,
        3,
        "every player got an edge to their team"
    );
    assert_eq!(
        count(
            &fluree,
            &r.ledger,
            r#"MATCH (:Player)-[r:PLAYS_FOR]->(:Team {name: "Red"}) RETURN count(r)"#
        )
        .await,
        2
    );
}

#[tokio::test]
async fn create_bound_vars_thread_into_merge() {
    // CREATE-bound variables thread as skolem Sids: each created Item must
    // connect to ITS row's category (pins VALUES-row ↔ solution order).
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:create-thread");

    let r = fluree
        .transact_cypher_with_params(
            l,
            r#"UNWIND $rows AS row
               CREATE (n:Item {sku: row.sku})
               MERGE (c:Cat {name: row.cat})
               MERGE (n)-[:IN]->(c)"#,
            Some(&params(json!({"rows": [
                {"sku": "a1", "cat": "tools"},
                {"sku": "a2", "cat": "toys"},
                {"sku": "a3", "cat": "tools"},
            ]}))),
        )
        .await
        .expect("create+merge chain");

    assert_eq!(
        count(&fluree, &r.ledger, "MATCH (n:Item) RETURN count(n)").await,
        3
    );
    assert_eq!(
        count(&fluree, &r.ledger, "MATCH (c:Cat) RETURN count(c)").await,
        2
    );
    // Row-exact wiring, not just counts.
    for (sku, cat) in [("a1", "tools"), ("a2", "toys"), ("a3", "tools")] {
        assert_eq!(
            count(
                &fluree,
                &r.ledger,
                &format!(
                    r#"MATCH (:Item {{sku: "{sku}"}})-[r:IN]->(:Cat {{name: "{cat}"}}) RETURN count(r)"#
                ),
            )
            .await,
            1,
            "item {sku} wired to {cat}"
        );
    }
}

#[tokio::test]
async fn on_create_and_on_match_fire_per_branch_in_chain() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:branches");

    let stmt = r#"MERGE (a:P {id: 1}) ON CREATE SET a.created = true ON MATCH SET a.matched = true
                  MERGE (b:P {id: 2}) ON CREATE SET b.created = true ON MATCH SET b.matched = true"#;
    let r1 = fluree.transact_cypher(l, stmt).await.expect("first");
    assert_eq!(
        count(
            &fluree,
            &r1.ledger,
            "MATCH (n:P {created: true}) RETURN count(n)"
        )
        .await,
        2,
        "both created on first run"
    );
    assert_eq!(
        count(
            &fluree,
            &r1.ledger,
            "MATCH (n:P {matched: true}) RETURN count(n)"
        )
        .await,
        0
    );

    let r2 = fluree
        .transact_cypher(r1.ledger, stmt)
        .await
        .expect("second");
    assert_eq!(
        count(
            &fluree,
            &r2.ledger,
            "MATCH (n:P {matched: true}) RETURN count(n)"
        )
        .await,
        2,
        "both matched on second run"
    );
}

#[tokio::test]
async fn in_batch_duplicate_key_first_creates_later_matches() {
    // Two rows share id=7 (absent): the first creates (+ON CREATE), the
    // second matches the just-created node (+ON MATCH) — sequential per-row
    // semantics inside one vectorized statement.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:dupkey");

    let r = fluree
        .transact_cypher_with_params(
            l,
            // The second MERGE makes the statement multi-clause, routing it
            // to the sequential driver (a single-clause UNWIND-MERGE keeps
            // its existing desugar-dedup path).
            r#"UNWIND $rows AS row
               MERGE (n:P {id: row.id}) ON CREATE SET n.c = row.v ON MATCH SET n.m = row.v
               MERGE (t:Tag {of: row.id})"#,
            Some(&params(json!({"rows": [
                {"id": 7, "v": "first"},
                {"id": 7, "v": "second"},
            ]}))),
        )
        .await
        .expect("dup-key batch");

    assert_eq!(
        count(&fluree, &r.ledger, "MATCH (n:P) RETURN count(n)").await,
        1
    );
    assert_eq!(
        count(
            &fluree,
            &r.ledger,
            r#"MATCH (n:P {c: "first"}) RETURN count(n)"#
        )
        .await,
        1,
        "ON CREATE ran with the first row's values"
    );
    assert_eq!(
        count(
            &fluree,
            &r.ledger,
            r#"MATCH (n:P {m: "second"}) RETURN count(n)"#
        )
        .await,
        1,
        "ON MATCH ran for the duplicate row"
    );
    assert_eq!(
        count(&fluree, &r.ledger, "MATCH (t:Tag) RETURN count(t)").await,
        1
    );
}

#[tokio::test]
async fn interleaved_set_applies_to_all_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:set");

    let r = fluree
        .transact_cypher(
            l,
            r#"MERGE (a:Cfg {name: "main"}) SET a.version = 2 MERGE (b:Cfg {name: "aux"}) SET b.version = 1"#,
        )
        .await
        .expect("merge+set chain");

    assert_eq!(
        count(
            &fluree,
            &r.ledger,
            r#"MATCH (n:Cfg {name: "main", version: 2}) RETURN count(n)"#
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fluree,
            &r.ledger,
            r#"MATCH (n:Cfg {name: "aux", version: 1}) RETURN count(n)"#
        )
        .await,
        1
    );
}

#[tokio::test]
async fn trailing_return_answers_from_final_row_table() {
    // RETURN on a multi-clause write: full read surface off the final row
    // table — including the MATCHED (not created) branch, which the
    // created-entity-only reconstruction could never answer.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:return");
    let l = fluree
        .transact_cypher(l, r#"CREATE (a:Person {name: "Alice", age: 42})"#)
        .await
        .expect("seed")
        .ledger;

    let (result, rows) = fluree
        .transact_cypher_returning(
            l,
            r#"MERGE (a:Person {name: "Alice"}) MERGE (b:City {name: "Paris"}) MERGE (a)-[:LIVES_IN]->(b) RETURN a.age AS age, b"#,
            None,
        )
        .await
        .expect("chain with RETURN");
    let rows = rows.expect("RETURN produces an envelope");

    let columns = rows["results"][0]["columns"].as_array().expect("columns");
    assert_eq!(columns, &[json!("age"), json!("b")], "envelope: {rows}");
    let data = rows["results"][0]["data"].as_array().expect("data");
    assert_eq!(data.len(), 1, "one row: {rows}");
    assert_eq!(
        data[0]["row"][0],
        json!(42),
        "a.age reads the MATCHED node's property: {rows}"
    );
    assert!(result.receipt.flake_count > 0);
}

#[tokio::test]
async fn zero_matching_rows_is_a_clean_no_op() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:zero-rows");
    let l = fluree
        .transact_cypher(l, r#"CREATE (a:Person {name: "Alice"})"#)
        .await
        .expect("seed")
        .ledger;
    let t0 = l.t();

    let r = fluree
        .transact_cypher(
            l,
            r#"MATCH (p:Person {name: "Nobody"}) MERGE (t:Team {name: p.name}) MERGE (p)-[:IN]->(t)"#,
        )
        .await
        .expect("zero-row chain succeeds");
    assert_eq!(r.receipt.flake_count, 0);
    assert_eq!(r.ledger.t(), t0, "no commit for a zero-row statement");
    assert_eq!(
        count(&fluree, &r.ledger, "MATCH (t:Team) RETURN count(t)").await,
        0
    );
}

#[tokio::test]
async fn anonymous_read_prefix_preserves_row_multiplicity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:anonymous-prefix");
    let l = fluree
        .transact_cypher(l, r#"UNWIND [1, 2, 3] AS id CREATE (:Person {id: id})"#)
        .await
        .expect("seed people")
        .ledger;

    let r = fluree
        .transact_cypher(
            l,
            r#"MATCH (:Person) CREATE (:Audit) MERGE (:Sentinel {id: 1})"#,
        )
        .await
        .expect("anonymous-prefix sequential write");

    assert_eq!(
        count(&fluree, &r.ledger, "MATCH (n:Audit) RETURN count(n)").await,
        3,
        "the zero-column read relation still carries one row per match"
    );
    assert_eq!(
        count(&fluree, &r.ledger, "MATCH (n:Sentinel) RETURN count(n)").await,
        1
    );
}

#[tokio::test]
async fn with_star_preserves_visible_scope_before_sequential_write() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:with-star");
    let l = fluree
        .transact_cypher(
            l,
            r#"UNWIND ["a", "b"] AS name CREATE (:Person {name: name})"#,
        )
        .await
        .expect("seed people")
        .ledger;

    let r = fluree
        .transact_cypher(
            l,
            r#"MATCH (p:Person) WITH * MERGE (t:Tag {name: p.name}) MERGE (p)-[:TAGGED]->(t)"#,
        )
        .await
        .expect("WITH * sequential write");

    assert_eq!(
        count(&fluree, &r.ledger, "MATCH (n:Tag) RETURN count(n)").await,
        2
    );
    assert_eq!(
        count(
            &fluree,
            &r.ledger,
            "MATCH (:Person)-[r:TAGGED]->(:Tag) RETURN count(r)"
        )
        .await,
        2
    );
}

#[tokio::test]
async fn computed_on_match_values_observe_prior_duplicate_updates() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:computed-on-match");
    let l = fluree
        .transact_cypher(l, r#"CREATE (:Counter {id: "counter", count: 0})"#)
        .await
        .expect("seed counter")
        .ledger;

    let r = fluree
        .transact_cypher_with_params(
            l,
            r#"UNWIND $rows AS row
               MERGE (n:Counter {id: row.id}) ON MATCH SET n.count = n.count + 1
               MERGE (s:Sentinel {id: 1})"#,
            Some(&params(
                json!({"rows": [{"id": "counter"}, {"id": "counter"}]}),
            )),
        )
        .await
        .expect("computed ON MATCH");

    let db = graphdb_from_ledger(&r.ledger);
    let returned = fluree
        .query_cypher(
            &db,
            r#"MATCH (n:Counter {id: "counter"}) RETURN n.count AS count"#,
        )
        .await
        .expect("read counter")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("counter JSON");
    assert_eq!(
        returned["results"][0]["data"][0]["row"][0],
        json!(2),
        "the second duplicate row reads the first row's increment: {returned}"
    );
}

#[tokio::test]
async fn computed_on_create_values_can_read_the_created_binding() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:computed-on-create");

    let r = fluree
        .transact_cypher(
            l,
            r#"MERGE (n:Counter {id: "new"})
               ON CREATE SET n.copy = n.id
               MERGE (s:Sentinel {id: 1})"#,
        )
        .await
        .expect("computed ON CREATE");

    assert_eq!(
        count(
            &fluree,
            &r.ledger,
            r#"MATCH (n:Counter {id: "new", copy: "new"}) RETURN count(n)"#
        )
        .await,
        1
    );
}

#[tokio::test]
async fn path_bindings_round_trip_through_sequential_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:path-row");
    let l = fluree
        .transact_cypher(
            l,
            r#"CREATE (:Person {name: "A"})-[:KNOWS]->(:Person {name: "B"})"#,
        )
        .await
        .expect("seed path")
        .ledger;

    let (_r, returned) = fluree
        .transact_cypher_returning(
            l,
            r#"MATCH p = (a:Person)-[:KNOWS]->(b:Person)
               MERGE (x:Seen {name: a.name})
               MERGE (x)-[:POINTS_TO]->(b)
               RETURN length(p) AS hops"#,
            None,
        )
        .await
        .expect("path-threading sequential write");
    let returned = returned.expect("RETURN envelope");
    assert_eq!(returned["results"][0]["data"][0]["row"][0], json!(1));
}

#[tokio::test]
async fn failing_clause_commits_nothing() {
    // All-or-nothing: the third clause is rejected (multi-hop MERGE), so the
    // first two clauses' staging must not surface anywhere.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:atomic");
    let t0 = l.t();

    let err = fluree
        .transact_cypher(
            l.clone(),
            r#"MERGE (a:Person {name: "X"}) MERGE (b:Person {name: "Y"}) MERGE (a)-[:K]->()-[:K]->(b)"#,
        )
        .await
        .expect_err("multi-hop MERGE clause must fail the statement");
    let msg = err.to_string();
    assert!(msg.contains("multi-hop"), "clear error, got: {msg}");

    assert_eq!(l.t(), t0);
    assert_eq!(
        count(&fluree, &l, "MATCH (n:Person) RETURN count(n)").await,
        0,
        "nothing committed from the failed statement"
    );
}

#[tokio::test]
async fn delete_in_chain_rejected_clearly() {
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:delete-chain");

    let err = fluree
        .transact_cypher(l, r#"MERGE (a:P {id: 1}) MATCH (b:Old) DELETE b"#)
        .await
        .expect_err("DELETE in a multi-clause statement is deferred");
    assert!(
        err.to_string().contains("DELETE in a multi-clause"),
        "clear error, got: {err}"
    );
}

#[tokio::test]
async fn bolt_transaction_pipelines_sequential_statements() {
    // Explicit-transaction path: a multi-clause statement against the
    // private state, read-your-writes inside the transaction, atomic COMMIT.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:bolt");
    // Committing publishes the nameservice record, so the cached-handle
    // machinery behind `begin_cypher_transaction` can resolve the ledger.
    let l = fluree
        .transact_cypher(l, r#"CREATE (s:Seed {name: "s"})"#)
        .await
        .expect("seed commit")
        .ledger;

    let governance = fluree_db_api::GovernanceOptions::default();
    let mut txn = fluree
        .begin_cypher_transaction(
            "it/cyseq:bolt",
            governance,
            fluree_db_api::TrackingOptions::default(),
        )
        .await
        .expect("begin");

    let out = fluree
        .cypher_transaction_write(
            &mut txn,
            r#"MERGE (a:Person {name: "Alice"}) MERGE (b:City {name: "Rome"}) MERGE (a)-[:LIVES_IN]->(b) RETURN a, b"#,
            None,
        )
        .await
        .expect("sequential statement in txn");
    assert!(out.flake_count > 0);
    let (cols, rows) = out.return_table.expect("typed RETURN rows");
    assert_eq!(cols, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(rows.len(), 1);

    // Read-your-writes inside the transaction: a second statement's MERGE
    // must observe the first statement's nodes.
    let out2 = fluree
        .cypher_transaction_write(
            &mut txn,
            r#"MERGE (a:Person {name: "Alice"}) MERGE (c:City {name: "Oslo"}) MERGE (a)-[:VISITED]->(c)"#,
            None,
        )
        .await
        .expect("second sequential statement");
    assert!(out2.flake_count > 0);

    let committed = fluree.commit_cypher_transaction(txn).await.expect("commit");
    assert!(committed.receipt.t > l.t());
    assert!(
        committed.receipt.flake_count > 0,
        "flake count summed across both statements"
    );

    let handle = fluree
        .ledger_cached("it/cyseq:bolt")
        .await
        .expect("cached handle");
    let state = handle.snapshot().await.to_ledger_state();
    assert_eq!(
        count(&fluree, &state, "MATCH (n:Person) RETURN count(n)").await,
        1,
        "one Alice across both statements"
    );
    assert_eq!(
        count(&fluree, &state, "MATCH (n:City) RETURN count(n)").await,
        2
    );
}

#[tokio::test]
async fn commit_result_carries_fuel_and_indexing_signals() {
    // The commit result reaches parity with the autocommit `execute()` path:
    // a fuel tally accumulated across every statement, plus the indexing
    // signals a transactor needs to trigger background reindexing.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:tracked");
    let _ = fluree
        .transact_cypher(l, r#"CREATE (s:Seed {name: "s"})"#)
        .await
        .expect("seed commit")
        .ledger;

    let mut txn = fluree
        .begin_cypher_transaction(
            "it/cyseq:tracked",
            fluree_db_api::GovernanceOptions::default(),
            fluree_db_api::TrackingOptions::all_enabled(),
        )
        .await
        .expect("begin");

    // Two separate statements: their fuel must sum into one tally.
    fluree
        .cypher_transaction_write(
            &mut txn,
            r#"MERGE (a:Person {name: "Alice"}) MERGE (b:City {name: "Rome"}) MERGE (a)-[:LIVES_IN]->(b)"#,
            None,
        )
        .await
        .expect("first statement");
    fluree
        .cypher_transaction_write(
            &mut txn,
            r#"MERGE (a:Person {name: "Alice"}) MERGE (c:City {name: "Oslo"}) MERGE (a)-[:VISITED]->(c)"#,
            None,
        )
        .await
        .expect("second statement");

    let committed = fluree.commit_cypher_transaction(txn).await.expect("commit");

    let fuel = committed
        .tally
        .as_ref()
        .and_then(|t| t.fuel)
        .expect("fuel tally present when tracking is enabled");
    assert!(
        fuel > 0.0,
        "fuel accumulated across both statements: {fuel}"
    );
    assert!(committed.receipt.flake_count > 0);
    // Indexing signals are populated (novelty reflects the staged flakes).
    assert!(committed.indexing.novelty_size > 0);
    assert_eq!(committed.indexing.commit_t, committed.receipt.t);
}

#[tokio::test]
async fn commit_without_tracking_reports_no_tally() {
    // Opting out of tracking leaves the tally empty — no accidental overhead.
    let fluree = FlureeBuilder::memory().build_memory();
    let l = genesis_ledger(&fluree, "it/cyseq:untracked");
    let _ = fluree
        .transact_cypher(l, r#"CREATE (s:Seed {name: "s"})"#)
        .await
        .expect("seed commit")
        .ledger;

    let mut txn = fluree
        .begin_cypher_transaction(
            "it/cyseq:untracked",
            fluree_db_api::GovernanceOptions::default(),
            fluree_db_api::TrackingOptions::default(),
        )
        .await
        .expect("begin");
    fluree
        .cypher_transaction_write(&mut txn, r#"CREATE (a:Person {name: "Bob"})"#, None)
        .await
        .expect("write");

    let committed = fluree.commit_cypher_transaction(txn).await.expect("commit");
    assert!(committed.tally.is_none(), "no tally without tracking");
    assert!(committed.receipt.flake_count > 0);
}
