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
async fn all_reads_commit_on_uncommitted_ledger_is_a_no_op() {
    // `create_ledger` registers the nameservice record but writes no genesis
    // commit, so the transaction's base head id is None and base t is 0. An
    // all-reads COMMIT on such a ledger must succeed as a no-op — matching the
    // autocommit no-op receipt — not error.
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_ledger("it/cyseq:uncommitted")
        .await
        .expect("create ledger");

    let txn = fluree
        .begin_cypher_transaction(
            "it/cyseq:uncommitted",
            fluree_db_api::GovernanceOptions::default(),
            fluree_db_api::TrackingOptions::default(),
        )
        .await
        .expect("begin on a ledger with no genesis commit");
    assert!(!txn.has_writes());

    let committed = fluree
        .commit_cypher_transaction(txn)
        .await
        .expect("all-reads commit succeeds on an uncommitted ledger");
    assert_eq!(committed.receipt.t, 0, "base t of an uncommitted ledger");
    assert_eq!(committed.receipt.flake_count, 0);
    assert!(!committed.indexing.needed, "a no-op needs no reindex");

    // The ledger stays usable: data can still be added after the fact.
    let handle = fluree
        .ledger_cached("it/cyseq:uncommitted")
        .await
        .expect("cached handle");
    let state = handle.snapshot().await.to_ledger_state();
    let r = fluree
        .transact_cypher(state, r#"CREATE (p:Person {name: "Ada"})"#)
        .await
        .expect("write after an all-reads no-op commit");
    assert_eq!(
        count(&fluree, &r.ledger, "MATCH (n:Person) RETURN count(n)").await,
        1
    );
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

// ---- Clauses that write one fact: the last write holds -------------------
//
// Each case runs a Cypher statement and the SPARQL UPDATE that means the same
// (one operation per clause, `;`-separated) on twin ledgers seeded alike, and
// requires both to end in the same expected state with the same commit.

const TWIN_SEED: &str = r#"INSERT DATA {
  <a> a <P> ; <id> "a" ; <name> "x" ; <tags> "t1", "t2" .
  <b> a <P> ; <id> "b" ; <knows> <a> .
}"#;

/// Every triple, with subjects the seed didn't name (created nodes) shown as
/// `new`, sorted.
async fn twin_state(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
) -> Vec<String> {
    let json = support::query_sparql(fluree, ledger, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
        .await
        .expect("state query")
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    let mut rows: Vec<String> = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| {
            let term = |k: &str| {
                let v = b[k]["value"].as_str().unwrap_or("?");
                match b[k]["type"].as_str() {
                    Some("bnode") => "new".to_string(),
                    _ if k == "s" && !["a", "b"].contains(&v) => "new".to_string(),
                    _ => v.to_string(),
                }
            };
            format!("{} {} {}", term("s"), term("p"), term("o"))
        })
        .collect();
    rows.sort();
    rows
}

/// Run `cypher` and `sparql` on twin seeded ledgers: each side's state and
/// commit (asserts, retracts).
async fn twins(cypher: &str, sparql: &str) -> [(Vec<String>, (usize, usize)); 2] {
    let mut out = Vec::new();
    for side in 0..2 {
        let fluree = FlureeBuilder::memory().build_memory();
        let id = format!("it/cyseq-twin-{side}:main");
        fluree.create_ledger(&id).await.expect("create");
        fluree
            .graph(&id)
            .transact()
            .sparql_update(TWIN_SEED)
            .commit()
            .await
            .expect("seed");
        let receipt = if side == 0 {
            let l = fluree.ledger(&id).await.expect("ledger");
            fluree
                .transact_cypher(l, cypher)
                .await
                .expect("cypher")
                .receipt
        } else {
            fluree
                .graph(&id)
                .transact()
                .sparql_update(sparql)
                .commit()
                .await
                .expect("sparql")
                .receipt
        };
        let ledger = fluree.ledger(&id).await.expect("ledger");
        let state = twin_state(&fluree, &ledger).await;
        out.push((state, (receipt.assert_count, receipt.retract_count)));
    }
    let sparql_side = out.pop().expect("sparql");
    let cypher_side = out.pop().expect("cypher");
    [cypher_side, sparql_side]
}

fn seeded_with(changes: &[(&str, Option<&str>)]) -> Vec<String> {
    let mut rows: Vec<String> = [
        "a http://www.w3.org/1999/02/22-rdf-syntax-ns#type P",
        "a id a",
        "a name x",
        "a tags t1",
        "a tags t2",
        "b http://www.w3.org/1999/02/22-rdf-syntax-ns#type P",
        "b id b",
        "b knows a",
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect();
    for (row, add) in changes {
        match add {
            None => rows.retain(|r| r != row),
            Some(_) => rows.push((*row).to_string()),
        }
    }
    rows.sort();
    rows
}

fn assert_twins(
    label: &str,
    got: [(Vec<String>, (usize, usize)); 2],
    want: Vec<String>,
    commit: (usize, usize),
) {
    let [cypher, sparql] = got;
    assert_eq!(sparql.0, want, "{label}: SPARQL state");
    assert_eq!(
        sparql.1, commit,
        "{label}: SPARQL commit (asserts, retracts)"
    );
    assert_eq!(cypher.0, want, "{label}: Cypher state");
    assert_eq!(
        cypher.1, commit,
        "{label}: Cypher commit (asserts, retracts)"
    );
}

#[tokio::test]
async fn remove_then_set_of_the_same_value_keeps_it() {
    let got = twins(
        r#"MATCH (n:P {id: "a"}) REMOVE n.name SET n.name = "x""#,
        r#"DELETE { ?n <name> ?o } WHERE { ?n a <P> ; <id> "a" ; <name> ?o } ;
           DELETE { ?n <name> ?o } INSERT { ?n <name> "x" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } }"#,
    )
    .await;
    assert_twins("REMOVE then SET same", got, seeded_with(&[]), (0, 0));
}

#[tokio::test]
async fn the_last_of_two_sets_holds() {
    let got = twins(
        r#"MATCH (n:P {id: "a"}) SET n.name = "y" SET n.name = "w""#,
        r#"DELETE { ?n <name> ?o } INSERT { ?n <name> "y" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } } ;
           DELETE { ?n <name> ?o } INSERT { ?n <name> "w" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } }"#,
    )
    .await;
    assert_twins(
        "SET y then SET w",
        got,
        seeded_with(&[("a name x", None), ("a name w", Some(""))]),
        (1, 1),
    );
}

#[tokio::test]
async fn a_set_back_to_the_stored_value_writes_nothing() {
    let got = twins(
        r#"MATCH (n:P {id: "a"}) SET n.name = "y" SET n.name = "x""#,
        r#"DELETE { ?n <name> ?o } INSERT { ?n <name> "y" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } } ;
           DELETE { ?n <name> ?o } INSERT { ?n <name> "x" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } }"#,
    )
    .await;
    assert_twins("SET y then SET x", got, seeded_with(&[]), (0, 0));
}

#[tokio::test]
async fn a_set_to_the_stored_value_writes_nothing() {
    let got = twins(
        r#"MATCH (n:P {id: "a"}) SET n.name = "x""#,
        r#"DELETE { ?n <name> ?o } INSERT { ?n <name> "x" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } }"#,
    )
    .await;
    assert_twins("SET same", got, seeded_with(&[]), (0, 0));
}

#[tokio::test]
async fn two_items_of_one_set_on_one_property_keep_the_last() {
    let got = twins(
        r#"MATCH (n:P {id: "a"}) SET n.name = "y", n.name = "w""#,
        r#"DELETE { ?n <name> ?o } INSERT { ?n <name> "y" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } } ;
           DELETE { ?n <name> ?o } INSERT { ?n <name> "w" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } }"#,
    )
    .await;
    assert_twins(
        "SET y, w",
        got,
        seeded_with(&[("a name x", None), ("a name w", Some(""))]),
        (1, 1),
    );
}

#[tokio::test]
async fn a_label_set_then_removed_is_gone() {
    let got = twins(
        r#"MATCH (n:P {id: "a"}) SET n:P REMOVE n:P"#,
        r#"INSERT { ?n a <P> } WHERE { ?n a <P> ; <id> "a" } ;
           DELETE { ?n a <P> } WHERE { ?n <id> "a" }"#,
    )
    .await;
    assert_twins(
        "SET :P then REMOVE :P",
        got,
        seeded_with(&[("a http://www.w3.org/1999/02/22-rdf-syntax-ns#type P", None)]),
        (0, 1),
    );
}

#[tokio::test]
async fn a_detach_delete_then_create_creates_once() {
    let got = twins(
        r#"MATCH (n:P {id: "a"}) DETACH DELETE n CREATE (:P {id: "a", name: "x"})"#,
        r#"DELETE { ?n ?p ?o . ?s ?q ?n } WHERE { ?n a <P> ; <id> "a" { ?n ?p ?o } UNION { ?s ?q ?n } } ;
           INSERT DATA { _:c a <P> ; <id> "a" ; <name> "x" }"#,
    )
    .await;
    let want = {
        let mut rows = seeded_with(&[
            ("a http://www.w3.org/1999/02/22-rdf-syntax-ns#type P", None),
            ("a id a", None),
            ("a name x", None),
            ("a tags t1", None),
            ("a tags t2", None),
            ("b knows a", None),
        ]);
        rows.extend(
            [
                "new http://www.w3.org/1999/02/22-rdf-syntax-ns#type P",
                "new id a",
                "new name x",
            ]
            .map(String::from),
        );
        rows.sort();
        rows
    };
    assert_twins("DETACH DELETE then CREATE", got, want, (3, 6));
}

#[tokio::test]
async fn a_set_over_a_multivalued_property_then_create_creates_once() {
    let got = twins(
        r#"MATCH (n:P {id: "a"}) SET n.tags = "t3" CREATE (:Q {id: "q"})"#,
        r#"DELETE { ?n <tags> ?o } INSERT { ?n <tags> "t3" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <tags> ?o } } ;
           INSERT DATA { _:q a <Q> ; <id> "q" }"#,
    )
    .await;
    let want = {
        let mut rows = seeded_with(&[
            ("a tags t1", None),
            ("a tags t2", None),
            ("a tags t3", Some("")),
        ]);
        rows.extend(
            [
                "new http://www.w3.org/1999/02/22-rdf-syntax-ns#type Q",
                "new id q",
            ]
            .map(String::from),
        );
        rows.sort();
        rows
    };
    assert_twins("SET multi-valued then CREATE", got, want, (3, 2));
}

#[tokio::test]
async fn two_variables_on_one_node_keep_the_last_write() {
    let got = twins(
        r#"MATCH (m:P {id: "a"}), (n:P {id: "a"}) SET m.name = "y" SET n.name = "w""#,
        r#"DELETE { ?n <name> ?o } INSERT { ?n <name> "y" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } } ;
           DELETE { ?n <name> ?o } INSERT { ?n <name> "w" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } }"#,
    )
    .await;
    assert_twins(
        "aliased SETs",
        got,
        seeded_with(&[("a name x", None), ("a name w", Some(""))]),
        (1, 1),
    );
}

/// A row OPTIONAL MATCH leaves unbound writes nothing: its SET must not
/// reach another node through the SET's own lookup of the stored value.
#[tokio::test]
async fn an_optional_match_that_finds_nothing_writes_nothing() {
    let got = twins(
        r#"OPTIONAL MATCH (n:P {id: "zz"}) SET n.name = "w""#,
        r#"DELETE { ?n <name> ?o } INSERT { ?n <name> "w" }
           WHERE { ?n a <P> ; <id> "zz" OPTIONAL { ?n <name> ?o } }"#,
    )
    .await;
    assert_twins("OPTIONAL MATCH misses", got, seeded_with(&[]), (0, 0));
}

#[tokio::test]
async fn an_optional_match_before_two_sets_keeps_the_last() {
    let got = twins(
        r#"OPTIONAL MATCH (n:P {id: "a"}) SET n.name = "y" SET n.name = "w""#,
        r#"DELETE { ?n <name> ?o } INSERT { ?n <name> "y" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } } ;
           DELETE { ?n <name> ?o } INSERT { ?n <name> "w" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } }"#,
    )
    .await;
    assert_twins(
        "OPTIONAL MATCH, SET y then SET w",
        got,
        seeded_with(&[("a name x", None), ("a name w", Some(""))]),
        (1, 1),
    );
}

/// The same with two clauses: a row the OPTIONAL MATCH leaves unbound is not
/// seeded, so neither clause's lookup of the stored value can bind its target.
#[tokio::test]
async fn an_optional_match_that_finds_nothing_before_two_sets_writes_nothing() {
    let got = twins(
        r#"OPTIONAL MATCH (n:P {id: "zz"}) SET n.name = "y" SET n.name = "w""#,
        r#"DELETE { ?n <name> ?o } INSERT { ?n <name> "y" }
           WHERE { ?n a <P> ; <id> "zz" OPTIONAL { ?n <name> ?o } } ;
           DELETE { ?n <name> ?o } INSERT { ?n <name> "w" }
           WHERE { ?n a <P> ; <id> "zz" OPTIONAL { ?n <name> ?o } }"#,
    )
    .await;
    assert_twins(
        "OPTIONAL MATCH misses, two SETs",
        got,
        seeded_with(&[]),
        (0, 0),
    );
}

#[tokio::test]
async fn a_set_then_detach_delete_leaves_nothing() {
    let got = twins(
        r#"MATCH (n:P {id: "a"}) SET n.name = "y" DETACH DELETE n"#,
        r#"DELETE { ?n <name> ?o } INSERT { ?n <name> "y" }
           WHERE { ?n a <P> ; <id> "a" OPTIONAL { ?n <name> ?o } } ;
           DELETE { ?n ?p ?o . ?s ?q ?n } WHERE { ?n a <P> ; <id> "a" { ?n ?p ?o } UNION { ?s ?q ?n } }"#,
    )
    .await;
    let want = seeded_with(&[
        ("a http://www.w3.org/1999/02/22-rdf-syntax-ns#type P", None),
        ("a id a", None),
        ("a name x", None),
        ("a tags t1", None),
        ("a tags t2", None),
        ("b knows a", None),
    ]);
    assert_twins("SET then DETACH DELETE", got, want, (0, 6));
}

/// A SET on a node the statement creates: the single `Txn` refuses it (no
/// MATCH binds the target), and the driver stages it after the CREATE. Its
/// lookup of the stored value must reach the created node alone.
#[tokio::test]
async fn a_set_on_a_node_the_statement_creates_writes_that_node_once() {
    let got = twins(
        r#"CREATE (n:P {name: "z"}) SET n.id = "c""#,
        r#"INSERT DATA { _:c a <P> ; <name> "z" ; <id> "c" }"#,
    )
    .await;
    let want = {
        let mut rows = seeded_with(&[]);
        rows.extend(
            [
                "new http://www.w3.org/1999/02/22-rdf-syntax-ns#type P",
                "new id c",
                "new name z",
            ]
            .map(String::from),
        );
        rows.sort();
        rows
    };
    assert_twins("CREATE then SET of the created node", got, want, (3, 0));
}

// ---- The state after the last clause --------------------------------------
//
// Only a trailing RETURN reads the virtual state after a statement's last
// write clause, so without one the driver doesn't build it. The commit reads
// the merged result, which is the same either way.

/// Two SETs of one key on node `a`; the last write is `"w"`.
const TWO_SETS: &str = r#"MATCH (n:P {id: "a"}) SET n.name = "y" SET n.name = "w""#;

/// The `cypher_unread_final_state` stamps since event `from`.
fn final_state_stamps(store: &support::span_capture::SpanStore, from: usize) -> Vec<String> {
    let site = fluree_db_api::cypher_seq::UNREAD_FINAL_STATE_SITE;
    store.find_events("fast-path outcome")[from..]
        .iter()
        .filter(|e| e.fields.get("site").map(String::as_str) == Some(site))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect()
}

/// A fresh ledger `id` seeded with [`TWIN_SEED`].
async fn twin_seeded(fluree: &fluree_db_api::Fluree, id: &str) {
    fluree.create_ledger(id).await.expect("create");
    fluree
        .graph(id)
        .transact()
        .sparql_update(TWIN_SEED)
        .commit()
        .await
        .expect("seed");
}

/// A statement commits the same facts whether or not a RETURN reads the
/// state after its last clause: the run without one leaves that state
/// unbuilt, the run with one builds it, and both commit the merged result.
#[tokio::test]
async fn a_write_commits_the_same_facts_with_or_without_a_return() {
    let (store, _guard) = support::span_capture::init_test_tracing();
    let cases = [
        (
            r#"MATCH (n:P) SET n.name = "y" SET n.name = "w""#,
            " RETURN n.name AS name",
        ),
        (
            r#"MATCH (n:P {id: "a"}) DETACH DELETE n CREATE (:P {id: "a"})"#,
            " RETURN 1 AS done",
        ),
        (
            r#"MATCH (n:P {id: "a"}) SET n.name = "y" DETACH DELETE n"#,
            " RETURN 1 AS done",
        ),
        (
            r#"MATCH (n:P {id: "a"}) REMOVE n.name SET n.name = "x""#,
            " RETURN n.name AS name",
        ),
    ];
    for (i, (statement, ret)) in cases.into_iter().enumerate() {
        let mut runs = Vec::new();
        for (with_return, expected_stamp) in [(false, "proceed"), (true, "fallback:gate_declined")]
        {
            let fluree = FlureeBuilder::memory().build_memory();
            let id = format!("it/cyseq-unread-{i}-{with_return}:main");
            twin_seeded(&fluree, &id).await;
            let ledger = fluree.ledger(&id).await.expect("ledger");
            let before = store.find_events("fast-path outcome").len();
            let (result, rows) = if with_return {
                fluree
                    .transact_cypher_returning(ledger, &format!("{statement}{ret}"), None)
                    .await
                    .expect("with RETURN")
            } else {
                let result = fluree
                    .transact_cypher(ledger, statement)
                    .await
                    .expect("without RETURN");
                (result, None)
            };
            assert_eq!(rows.is_some(), with_return, "{statement}");
            assert_eq!(
                final_state_stamps(&store, before),
                [expected_stamp],
                "{statement} (RETURN: {with_return})"
            );
            let commit = (result.receipt.assert_count, result.receipt.retract_count);
            let ledger = fluree.ledger(&id).await.expect("ledger");
            runs.push((twin_state(&fluree, &ledger).await, commit));
        }
        assert_eq!(runs[0], runs[1], "{statement}: without vs with a RETURN");
    }
}

/// A trailing RETURN reads the last clause's write, on every path that
/// formats one: autocommit, the transaction builder (which the consensus
/// committers use), a `Transaction`, and a Bolt transaction.
#[tokio::test]
async fn a_trailing_return_reads_the_last_clauses_write() {
    use fluree_db_api::format::cypher_typed::CypherCell;

    let (store, _guard) = support::span_capture::init_test_tracing();
    let statement = format!("{TWO_SETS} RETURN n.name AS name");
    let declined = ["fallback:gate_declined"];

    // Autocommit.
    let fluree = FlureeBuilder::memory().build_memory();
    twin_seeded(&fluree, "it/cyseq-return:auto").await;
    let ledger = fluree.ledger("it/cyseq-return:auto").await.expect("ledger");
    let before = store.find_events("fast-path outcome").len();
    let (_, rows) = fluree
        .transact_cypher_returning(ledger, &statement, None)
        .await
        .expect("autocommit");
    let rows = rows.expect("RETURN rows");
    assert_eq!(
        rows["results"][0]["data"][0]["row"][0],
        json!("w"),
        "{rows}"
    );
    assert_eq!(final_state_stamps(&store, before), declined);

    // The transaction builder.
    let id = "it/cyseq-return:builder";
    twin_seeded(&fluree, id).await;
    let ast = fluree_db_cypher::parse_cypher(&statement)
        .ast
        .expect("parse");
    let plan = fluree_db_api::cypher_seq::detect_sequential(&ast).expect("sequential");
    let handle = fluree.ledger_cached(id).await.expect("handle");
    let before = store.find_events("fast-path outcome").len();
    let committed = fluree
        .stage(&handle)
        .cypher_sequential(fluree_db_api::cypher_seq::CypherSeqInput {
            plan,
            governance: Default::default(),
            skolem_txn_id: None,
        })
        .execute()
        .await
        .expect("builder");
    let rows = committed.cypher_return.expect("RETURN rows");
    assert_eq!(
        rows["results"][0]["data"][0]["row"][0],
        json!("w"),
        "{rows}"
    );
    assert_eq!(final_state_stamps(&store, before), declined);

    // A `Transaction`.
    let id = "it/cyseq-return:transaction";
    twin_seeded(&fluree, id).await;
    let mut txn = fluree
        .begin_transaction(id, fluree_db_api::TransactionOptions::default())
        .await
        .expect("begin");
    let before = store.find_events("fast-path outcome").len();
    let (_, rows) = txn
        .stage_cypher(&statement, None)
        .await
        .expect("stage")
        .expect("RETURN rows");
    assert_eq!(rows, vec![vec![CypherCell::Value(json!("w"))]]);
    assert_eq!(final_state_stamps(&store, before), declined);

    // A Bolt transaction.
    let id = "it/cyseq-return:bolt";
    twin_seeded(&fluree, id).await;
    let mut txn = fluree
        .begin_cypher_transaction(
            id,
            fluree_db_api::GovernanceOptions::default(),
            fluree_db_api::TrackingOptions::default(),
        )
        .await
        .expect("begin");
    let before = store.find_events("fast-path outcome").len();
    let out = fluree
        .cypher_transaction_write(&mut txn, &statement, None)
        .await
        .expect("write");
    let (_, rows) = out.return_table.expect("RETURN rows");
    assert_eq!(rows, vec![vec![CypherCell::Value(json!("w"))]]);
    assert_eq!(final_state_stamps(&store, before), declined);
}

/// A statement with no RETURN leaves the state after its last clause
/// unbuilt; the next statement in the same transaction still reads its
/// writes, from the transaction's own state.
#[tokio::test]
async fn the_next_statement_in_a_transaction_reads_a_write_with_no_return() {
    let (store, _guard) = support::span_capture::init_test_tracing();
    let fluree = FlureeBuilder::memory().build_memory();
    let next = r#"MATCH (n:P {name: "w"}) SET n.seen = "yes""#;
    let want = seeded_with(&[
        ("a name x", None),
        ("a name w", Some("")),
        ("a seen yes", Some("")),
    ]);

    // A Bolt transaction.
    let id = "it/cyseq-next:bolt";
    twin_seeded(&fluree, id).await;
    let mut txn = fluree
        .begin_cypher_transaction(
            id,
            fluree_db_api::GovernanceOptions::default(),
            fluree_db_api::TrackingOptions::default(),
        )
        .await
        .expect("begin");
    let before = store.find_events("fast-path outcome").len();
    fluree
        .cypher_transaction_write(&mut txn, TWO_SETS, None)
        .await
        .expect("two SETs");
    assert_eq!(final_state_stamps(&store, before), ["proceed"]);
    let out = fluree
        .cypher_transaction_write(&mut txn, next, None)
        .await
        .expect("next statement");
    assert_eq!(out.flake_count, 1, "the next MATCH finds the written name");
    fluree.commit_cypher_transaction(txn).await.expect("commit");
    let ledger = fluree.ledger(id).await.expect("ledger");
    assert_eq!(twin_state(&fluree, &ledger).await, want, "Bolt");

    // A `Transaction`.
    let id = "it/cyseq-next:transaction";
    twin_seeded(&fluree, id).await;
    let mut txn = fluree
        .begin_transaction(id, fluree_db_api::TransactionOptions::default())
        .await
        .expect("begin");
    let before = store.find_events("fast-path outcome").len();
    txn.stage_cypher(TWO_SETS, None).await.expect("two SETs");
    assert_eq!(final_state_stamps(&store, before), ["proceed"]);
    txn.stage_cypher(next, None).await.expect("next statement");
    txn.commit(fluree_db_api::CommitOpts::default())
        .await
        .expect("commit");
    let ledger = fluree.ledger(id).await.expect("ledger");
    assert_eq!(twin_state(&fluree, &ledger).await, want, "Transaction");
}

// ---- A relationship's DELETE beside other clauses -------------------------
//
// `DELETE r` retracts the edge `r` reifies, and every relationship on that
// edge goes with it. Beside other write clauses it stages in the driver, after
// the clauses before it, and refuses when the edge carries another
// relationship too (a parallel sibling), as a `DELETE r` of its own refuses.

const PARALLEL_REFUSAL: &str = "parallel relationships share its underlying edge";

/// Nodes `a`, `b`, `c` (`:P`, by `id`) and `(a)-[:K {w: 1}]->(b)`; with
/// `parallel`, a second relationship `(a)-[:K {w: 2}]->(b)` on the same edge.
async fn rel_seeded(
    fluree: &fluree_db_api::Fluree,
    id: &str,
    parallel: bool,
) -> fluree_db_api::LedgerState {
    let mut seed = vec![
        r#"CREATE (:P {id: "a"})"#,
        r#"CREATE (:P {id: "b"})"#,
        r#"CREATE (:P {id: "c"})"#,
        r#"MATCH (a:P {id: "a"}), (b:P {id: "b"}) CREATE (a)-[:K {w: 1}]->(b)"#,
    ];
    if parallel {
        seed.push(r#"MATCH (a:P {id: "a"}), (b:P {id: "b"}) CREATE (a)-[:K {w: 2}]->(b)"#);
    }
    let mut l = fluree.create_ledger(id).await.expect("create");
    for s in seed {
        l = fluree.transact_cypher(l, s).await.expect(s).ledger;
    }
    l
}

/// The `:K` relationships as Cypher reads them (`[from, w, v, to]`, sorted),
/// and every triple, with nodes named by their `id`, any other subject (a
/// relationship's annotation) as `rel`, and IRIs by their last segment.
async fn rel_state(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
) -> (Vec<String>, Vec<String>) {
    let db = graphdb_from_ledger(ledger);
    let rels = fluree
        .query_cypher(
            &db,
            "MATCH (s:P)-[r:K]->(o:P) RETURN s.id AS from, r.w AS w, r.v AS v, o.id AS to",
        )
        .await
        .expect("relationships")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("format");
    let mut rels: Vec<String> = rels["results"][0]["data"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|d| d["row"].to_string())
        .collect();
    rels.sort();
    let json = support::query_sparql(fluree, ledger, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
        .await
        .expect("state query")
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    let bindings = json["results"]["bindings"].as_array().expect("bindings");
    let last = |v: &str| v.rsplit(['/', '#']).next().unwrap_or(v).to_string();
    let ids: std::collections::HashMap<String, String> = bindings
        .iter()
        .filter(|b| b["p"]["value"].as_str().map(last).as_deref() == Some("id"))
        .map(|b| {
            (
                b["s"]["value"].as_str().unwrap_or("?").to_string(),
                b["o"]["value"].as_str().unwrap_or("?").to_string(),
            )
        })
        .collect();
    let name = |b: &serde_json::Value, k: &str| {
        let v = b[k]["value"].as_str().unwrap_or("?");
        match ids.get(v) {
            Some(id) => id.clone(),
            None if k == "s" => "rel".to_string(),
            None => last(v),
        }
    };
    let mut triples: Vec<String> = bindings
        .iter()
        .map(|b| format!("{} {} {}", name(b, "s"), name(b, "p"), name(b, "o")))
        .collect();
    triples.sort();
    (rels, triples)
}

/// The seeded nodes' own triples (no relationship), plus `extra`, sorted.
fn nodes_and(extra: &[&str]) -> Vec<String> {
    let mut rows: Vec<String> = ["a", "b", "c"]
        .iter()
        .flat_map(|n| [format!("{n} id {n}"), format!("{n} type P")])
        .chain(extra.iter().map(|s| (*s).to_string()))
        .collect();
    rows.sort();
    rows
}

/// Run `statement` on a fresh seeded ledger: its outcome, and the state after.
async fn rel_run(
    label: &str,
    parallel: bool,
    statement: &str,
) -> (Result<(), String>, (Vec<String>, Vec<String>)) {
    rel_run_seeded(label, parallel, &[], statement).await
}

/// [`rel_run`] with the `extra` statements run on the seed first.
async fn rel_run_seeded(
    label: &str,
    parallel: bool,
    extra: &[&str],
    statement: &str,
) -> (Result<(), String>, (Vec<String>, Vec<String>)) {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = format!("it/cyseq-rel-{label}:main");
    let mut ledger = rel_seeded(&fluree, &id, parallel).await;
    for s in extra {
        ledger = fluree.transact_cypher(ledger, s).await.expect(s).ledger;
    }
    let outcome = fluree
        .transact_cypher(ledger, statement)
        .await
        .map(drop)
        .map_err(|e| e.to_string());
    let ledger = fluree.ledger(&id).await.expect("ledger");
    (outcome, rel_state(&fluree, &ledger).await)
}

/// A SET on a relationship, then its DELETE: the relationship goes with
/// everything it carries. In one `Txn`, the DELETE cleared only the
/// properties stored before the statement, and the SET's value was left on
/// the deleted relationship.
#[tokio::test]
async fn a_set_on_a_relationship_then_its_delete_leaves_nothing() {
    for set in ["SET r.v = 5", "SET r.w = 9"] {
        let statement =
            format!(r#"MATCH (a:P {{id: "a"}})-[r:K]->(b:P {{id: "b"}}) {set} DELETE r"#);
        let (outcome, (rels, triples)) = rel_run("set-delete", false, &statement).await;
        assert_eq!(outcome, Ok(()), "{statement}");
        assert!(rels.is_empty(), "{statement}: {rels:?}");
        assert_eq!(triples, nodes_and(&[]), "{statement}");
    }
}

/// A relationship's DELETE, then a CREATE of the same edge: the new
/// relationship replaces the old. In one `Txn` the edge's retraction and its
/// re-assertion cancelled, the old relationship's properties were never
/// cleared, and both relationships stood on the edge.
#[tokio::test]
async fn a_relationship_deleted_then_created_again_is_the_new_one() {
    for (create, rel, extra) in [
        (
            "CREATE (a)-[:K {w: 7}]->(b)",
            r#"["a",7,null,"b"]"#,
            &["a K b", "rel w 7"][..],
        ),
        (
            "CREATE (a)-[:K]->(b)",
            r#"["a",null,null,"b"]"#,
            &["a K b"][..],
        ),
    ] {
        let statement =
            format!(r#"MATCH (a:P {{id: "a"}})-[r:K]->(b:P {{id: "b"}}) DELETE r {create}"#);
        let (outcome, (rels, triples)) = rel_run("delete-create", false, &statement).await;
        assert_eq!(outcome, Ok(()), "{statement}");
        assert_eq!(rels, vec![rel.to_string()], "{statement}");
        assert_eq!(triples, nodes_and(extra), "{statement}");
    }
}

/// A relationship's DELETE beside another clause, on an edge that carries a
/// second relationship: refused, and nothing is written. In one `Txn` it
/// skipped the parallel-relationship check and deleted both.
#[tokio::test]
async fn a_multi_clause_delete_of_a_parallel_relationship_refuses() {
    let seeded = rel_run(
        "parallel-seed",
        true,
        r#"MATCH (n:P {id: "c"}) SET n.id = "c""#,
    )
    .await
    .1;
    for clauses in [
        "SET a.seen = true DELETE r",
        "DELETE r SET a.seen = true",
        "SET r.v = 5 DELETE r",
    ] {
        let statement =
            format!(r#"MATCH (a:P {{id: "a"}})-[r:K {{w: 1}}]->(b:P {{id: "b"}}) {clauses}"#);
        let (outcome, state) = rel_run("parallel-multi", true, &statement).await;
        let err = outcome.expect_err(&statement);
        assert!(err.contains(PARALLEL_REFUSAL), "{statement}: {err}");
        assert_eq!(state, seeded, "{statement}: nothing written");
    }
}

/// A DELETE r of its own, filtered down to one of two relationships on an
/// edge: refused, as the unfiltered form is. The check counted only the
/// relationships the MATCH kept, so the filter hid the sibling and both were
/// deleted.
#[tokio::test]
async fn a_filtered_delete_of_a_parallel_relationship_refuses() {
    let seeded = rel_run(
        "parallel-seed-1",
        true,
        r#"MATCH (n:P {id: "c"}) SET n.id = "c""#,
    )
    .await
    .1;
    for statement in [
        r#"MATCH (a:P {id: "a"})-[r:K {w: 1}]->(b:P {id: "b"}) DELETE r"#,
        r#"MATCH (a:P {id: "a"})-[r:K]->(b:P {id: "b"}) WHERE r.w = 1 DELETE r"#,
    ] {
        let (outcome, state) = rel_run("parallel-single", true, statement).await;
        let err = outcome.expect_err(statement);
        assert!(err.contains(PARALLEL_REFUSAL), "{statement}: {err}");
        assert_eq!(state, seeded, "{statement}: nothing written");
    }
}

/// With no second relationship on the edge, the same statements commit.
#[tokio::test]
async fn a_relationship_delete_with_no_sibling_commits() {
    for (statement, extra) in [
        (
            r#"MATCH (a:P {id: "a"})-[r:K {w: 1}]->(b:P {id: "b"}) DELETE r"#,
            &[][..],
        ),
        (
            r#"MATCH (a:P {id: "a"})-[r:K {w: 1}]->(b:P {id: "b"}) SET a.seen = true DELETE r"#,
            &["a seen true"][..],
        ),
        (
            r#"MATCH (a:P {id: "a"})-[r:K]->(b:P {id: "b"}) DELETE r SET a.seen = true"#,
            &["a seen true"][..],
        ),
    ] {
        let (outcome, (rels, triples)) = rel_run("no-sibling", false, statement).await;
        assert_eq!(outcome, Ok(()), "{statement}");
        assert!(rels.is_empty(), "{statement}: {rels:?}");
        assert_eq!(triples, nodes_and(extra), "{statement}");
    }
    let statement = r#"MATCH (a:P {id: "a"})-[r:K]->(b:P {id: "b"}) DELETE r DETACH DELETE b"#;
    let (outcome, (rels, triples)) = rel_run("no-sibling-detach", false, statement).await;
    assert_eq!(outcome, Ok(()), "{statement}");
    assert!(rels.is_empty(), "{statement}: {rels:?}");
    let mut want = nodes_and(&[]);
    want.retain(|t| !t.starts_with("b "));
    assert_eq!(triples, want, "{statement}");
}

/// The check reads the state the clauses before the DELETE leave: a second
/// relationship an earlier clause created on the edge refuses the DELETE, as
/// a stored one does. (An earlier clause can't remove a sibling without
/// taking the edge, since that DELETE is refused itself.)
#[tokio::test]
async fn a_sibling_an_earlier_clause_created_refuses_the_delete() {
    let seeded = rel_run(
        "created-seed",
        false,
        r#"MATCH (n:P {id: "c"}) SET n.id = "c""#,
    )
    .await
    .1;
    let statement =
        r#"MATCH (a:P {id: "a"})-[r:K]->(b:P {id: "b"}) CREATE (a)-[:K {w: 2}]->(b) DELETE r"#;
    let (outcome, state) = rel_run("created-sibling", false, statement).await;
    let err = outcome.expect_err(statement);
    assert!(err.contains(PARALLEL_REFUSAL), "{statement}: {err}");
    assert_eq!(state, seeded, "nothing written");
}

// ---- A DELETE clause with a node and a relationship ------------------------
//
// The parallel-relationship check covers the edges a relationship's DELETE
// retracts, and no others. A `DETACH DELETE` of a node beside it takes every
// relationship on the node, as it does on its own.

/// A second relationship `(a)-[:K {w: 3}]->(c)`, on an edge of its own.
const A_TO_C: &str = r#"MATCH (a:P {id: "a"}), (c:P {id: "c"}) CREATE (a)-[:K {w: 3}]->(c)"#;

/// `DETACH DELETE a, r` takes every relationship on `a`, whether or not one of
/// `a`'s edges carries two of them, and whether or not that edge is `r`'s. The
/// same holds for a `DETACH DELETE` of `r`'s other endpoint, and beside other
/// clauses.
#[tokio::test]
async fn a_detach_delete_beside_a_relationship_takes_every_relationship_on_the_node() {
    let mut a_gone = nodes_and(&[]);
    a_gone.retain(|t| !t.starts_with("a "));
    for statement in [
        r#"MATCH (a:P {id: "a"})-[r:K]->(c:P {id: "c"}) DETACH DELETE a, r"#,
        r#"MATCH (a:P {id: "a"})-[r:K {w: 1}]->(b:P {id: "b"}) DETACH DELETE a, r"#,
        r#"MATCH (a:P {id: "a"})-[r:K {w: 1}]->(b:P {id: "b"}) DETACH DELETE r, a"#,
        r#"MATCH (a:P {id: "a"})-[r:K]->(c:P {id: "c"}) SET a.seen = true DETACH DELETE a, r"#,
        r#"MATCH (a:P {id: "a"})-[r:K]->(c:P {id: "c"}) SET r.v = 5 DETACH DELETE a, r"#,
    ] {
        let (outcome, (rels, triples)) =
            rel_run_seeded("detach-beside-rel", true, &[A_TO_C], statement).await;
        assert_eq!(outcome, Ok(()), "{statement}");
        assert!(rels.is_empty(), "{statement}: {rels:?}");
        assert_eq!(triples, a_gone, "{statement}");
    }
    let statement = r#"MATCH (a:P {id: "a"})-[r:K {w: 1}]->(b:P {id: "b"}) DETACH DELETE b, r"#;
    let (outcome, (rels, triples)) =
        rel_run_seeded("detach-other-end", true, &[A_TO_C], statement).await;
    assert_eq!(outcome, Ok(()), "{statement}");
    assert_eq!(rels, vec![r#"["a",3,null,"c"]"#.to_string()], "{statement}");
    let mut want = nodes_and(&["a K c", "rel w 3"]);
    want.retain(|t| !t.starts_with("b "));
    assert_eq!(triples, want, "{statement}");
}

/// The check still covers a relationship's own edge when a `DETACH DELETE` in
/// the same clause doesn't take that edge, and a `DETACH DELETE` of a
/// relationship is its DELETE.
#[tokio::test]
async fn a_parallel_relationship_the_detach_delete_does_not_reach_still_refuses() {
    let seeded = rel_run_seeded(
        "parallel-seed-2",
        true,
        &[A_TO_C],
        r#"MATCH (n:P {id: "c"}) SET n.id = "c""#,
    )
    .await
    .1;
    for statement in [
        r#"MATCH (a:P {id: "a"})-[r:K {w: 1}]->(b:P {id: "b"}), (c:P {id: "c"}) DETACH DELETE c, r"#,
        r#"MATCH (a:P {id: "a"})-[r:K {w: 1}]->(b:P {id: "b"}) DETACH DELETE r"#,
        r#"MATCH (a:P {id: "a"})-[r:K {w: 1}]->(b:P {id: "b"}) SET r.v = 5 DETACH DELETE r"#,
    ] {
        let (outcome, state) = rel_run_seeded("parallel-detach", true, &[A_TO_C], statement).await;
        let err = outcome.expect_err(statement);
        assert!(err.contains(PARALLEL_REFUSAL), "{statement}: {err}");
        assert_eq!(state, seeded, "{statement}: nothing written");
    }
}

/// A `DETACH DELETE` of a relationship beside other clauses stages as its
/// DELETE does: a SET on it first leaves nothing behind.
#[tokio::test]
async fn a_detach_delete_of_a_relationship_beside_a_set_leaves_nothing() {
    let statement = r#"MATCH (a:P {id: "a"})-[r:K]->(b:P {id: "b"}) SET r.v = 5 DETACH DELETE r"#;
    let (outcome, (rels, triples)) = rel_run("set-detach-rel", false, statement).await;
    assert_eq!(outcome, Ok(()), "{statement}");
    assert!(rels.is_empty(), "{statement}: {rels:?}");
    assert_eq!(triples, nodes_and(&[]), "{statement}");
}

// ---- Which edge a relationship's DELETE retracts ---------------------------
//
// Beside other clauses, a relationship's DELETE re-matches its edge in the
// state the clauses before it leave. It finds the edge through the
// relationship itself, so neither the names a `WITH` gives the endpoints nor
// the pattern's direction can point it at another edge, and the endpoints need
// no names.

/// A `WITH` that rebinds an endpoint's name to another node (through a later
/// `MATCH`), drops the endpoints, or renames the relationship: the
/// DELETE still takes the relationship the SET wrote to.
#[tokio::test]
async fn a_relationship_delete_after_a_with_deletes_that_relationship() {
    for statement in [
        r#"MATCH (a:P {id: "a"})-[r:K]->(b:P {id: "b"}) WITH r MATCH (a:P {id: "c"}) SET r.v = 5 DELETE r"#,
        r#"MATCH (a:P {id: "a"})-[r:K]->(b:P {id: "b"}) WITH r SET r.v = 5 DELETE r"#,
        r#"MATCH (a:P {id: "a"})-[r:K]->(b:P {id: "b"}) WITH DISTINCT r SET r.v = 5 DELETE r"#,
        r#"MATCH (a:P {id: "a"})-[r:K]->(b:P {id: "b"}) WITH r, count(*) AS n SET r.v = n DELETE r"#,
        r#"MATCH (:P {id: "a"})-[r:K]->(:P {id: "b"}) WITH r SET r.v = 5 DELETE r"#,
        r#"MATCH (a:P {id: "a"})-[r:K]->(b:P {id: "b"}) WITH a, b, r AS q SET q.v = 5 DELETE q"#,
    ] {
        let (outcome, (rels, triples)) = rel_run("with-delete", false, statement).await;
        assert_eq!(outcome, Ok(()), "{statement}");
        assert!(rels.is_empty(), "{statement}: {rels:?}");
        assert_eq!(triples, nodes_and(&[]), "{statement}");
    }
}

/// Unnamed endpoints, and a pattern read against the edge's direction: the
/// DELETE takes the relationship, on its own and beside a SET.
#[tokio::test]
async fn a_relationship_delete_needs_no_endpoint_names_or_direction() {
    for (statement, extra) in [
        (
            r#"MATCH (:P {id: "a"})-[r:K]->(:P {id: "b"}) DELETE r"#,
            &[][..],
        ),
        (
            r#"MATCH (:P {id: "a"})-[r:K]->(:P {id: "b"}) SET r.v = 5 DELETE r"#,
            &[][..],
        ),
        (
            r#"MATCH (x:P {id: "b"})-[r:K]-(y:P {id: "a"}) SET x.seen = true DELETE r"#,
            &["b seen true"][..],
        ),
    ] {
        let (outcome, (rels, triples)) = rel_run("unnamed-delete", false, statement).await;
        assert_eq!(outcome, Ok(()), "{statement}");
        assert!(rels.is_empty(), "{statement}: {rels:?}");
        assert_eq!(triples, nodes_and(extra), "{statement}");
    }
    let seeded = rel_run(
        "parallel-seed-3",
        true,
        r#"MATCH (n:P {id: "c"}) SET n.id = "c""#,
    )
    .await
    .1;
    for statement in [
        r#"MATCH (:P {id: "a"})-[r:K {w: 1}]->(:P {id: "b"}) DELETE r"#,
        r#"MATCH (:P {id: "a"})-[r:K {w: 1}]->(:P {id: "b"}) SET r.v = 5 DELETE r"#,
    ] {
        let (outcome, state) = rel_run("unnamed-parallel", true, statement).await;
        let err = outcome.expect_err(statement);
        assert!(err.contains(PARALLEL_REFUSAL), "{statement}: {err}");
        assert_eq!(state, seeded, "{statement}: nothing written");
    }
}

// ---- A relationship's DELETE beside a clause the driver doesn't stage -----

/// A statement with a clause the driver defers keeps the single `Txn`, a
/// relationship's DELETE beside it included, and the single `Txn`'s lowering
/// refuses it as it refuses the same statement without that DELETE.
#[tokio::test]
async fn a_relationship_delete_beside_a_deferred_clause_keeps_one_txn() {
    for (statement, refusal) in [
        (
            r#"MATCH (a:P {id: "a"})-[r:K]->(b:P {id: "b"}), (c:P {id: "c"}) SET a.seen = true DELETE r, c"#,
            "bare DELETE n",
        ),
        (
            r#"MATCH (a:P {id: "a"})-[r:K]->(b:P {id: "b"}) OPTIONAL MATCH (c:P {id: "c"}) SET a.seen = true DELETE r CREATE (c)-[:K]->(a)"#,
            "OPTIONAL MATCH before CREATE",
        ),
    ] {
        let ast = fluree_db_cypher::parse_cypher(statement)
            .ast
            .expect("parse");
        assert!(
            fluree_db_api::cypher_seq::detect_sequential(&ast).is_none(),
            "{statement}: keeps the single Txn"
        );
        let (outcome, _) = rel_run("deferred-delete", false, statement).await;
        let err = outcome.expect_err(statement);
        assert!(err.contains(refusal), "{statement}: {err}");
    }
}
