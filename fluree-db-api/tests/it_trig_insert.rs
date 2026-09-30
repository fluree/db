//! TriG on insert (#1849).
//!
//! `insert_turtle` streams Turtle straight to flakes, and that parser has no
//! graph-block production, so a TriG document used to fail with a Turtle
//! syntax error on every insert lane. A document the streaming parser rejects
//! is now read as TriG and, when it has graph blocks or `<#txn-meta>`, staged
//! through the named-graph path TriG upsert already uses — with insert
//! semantics. Plain Turtle never leaves the streaming parser.
//!
//! Also pins two lanes that dropped a TriG upsert's graph blocks: the owned
//! builder with a policy, and `stage()` on the graph builder. Every builder now
//! stages through one dispatch, so each lane below is a separate entry point
//! into it, not a separate implementation.

use crate::support::span_capture::{init_test_tracing, SpanStore};
use crate::support::{genesis_ledger, MemoryFluree};
use fluree_db_api::policy_builder::build_policy_context_from_opts;
use fluree_db_api::tx::TURTLE_INSERT_SITE;
use fluree_db_api::{FlureeBuilder, GovernanceOptions, GraphDb, LedgerState, PolicyContext};
use serde_json::{json, Value as JsonValue};

const G1: &str = "http://example.org/g1";
const G2: &str = "http://example.org/g2";

/// Default-graph triples, both graph-block spellings, and commit metadata.
const TRIG: &str = r#"
@prefix ex: <http://example.org/> .
@prefix fluree: <https://ns.flur.ee/db#> .

ex:alice ex:name "Alice" .

GRAPH <http://example.org/g1> { ex:alice ex:knows ex:bob . }

<http://example.org/g2> { ex:bob ex:knows ex:carol . }

GRAPH <#txn-meta> { fluree:commit:this ex:batch "b-42" . }
"#;

/// Rows of a SPARQL SELECT, each cell flattened to its IRI or lexical form,
/// sorted so assertions don't depend on result order.
async fn select(fluree: &MemoryFluree, db: &GraphDb, sparql: &str) -> Vec<Vec<String>> {
    let result = fluree.query(db, sparql).await.expect("query");
    let formatted = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("format");
    let cell = |v: &JsonValue| match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Object(o) => o
            .get("@id")
            .or_else(|| o.get("@value"))
            .map(|x| {
                x.as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| x.to_string())
            })
            .unwrap_or_else(|| v.to_string()),
        other => other.to_string(),
    };
    let mut rows: Vec<Vec<String>> = formatted
        .as_array()
        .expect("row array")
        .iter()
        .map(|row| match row.as_array() {
            Some(cells) => cells.iter().map(cell).collect(),
            None => vec![cell(row)],
        })
        .collect();
    rows.sort();
    rows
}

/// `?s ?o` of `ex:knows` in `graph`, or `ex:name` in the default graph.
async fn knows_in(fluree: &MemoryFluree, db: &GraphDb, graph: &str) -> Vec<Vec<String>> {
    let sparql = format!(
        "PREFIX ex: <http://example.org/>\n\
         SELECT ?s ?o WHERE {{ GRAPH <{graph}> {{ ?s ex:knows ?o }} }}"
    );
    select(fluree, db, &sparql).await
}

fn row(cells: &[&str]) -> Vec<String> {
    cells.iter().map(ToString::to_string).collect()
}

/// The data half of [`TRIG`] landed where it says, and nowhere else.
async fn assert_trig_landed(fluree: &MemoryFluree, db: &GraphDb, lane: &str) {
    let default = select(
        fluree,
        db,
        "PREFIX ex: <http://example.org/>\nSELECT ?s ?p ?o WHERE { ?s ?p ?o }",
    )
    .await;
    assert_eq!(
        default,
        vec![row(&["ex:alice", "ex:name", "Alice"])],
        "{lane}: the default graph holds only the default-graph triple"
    );
    assert_eq!(
        knows_in(fluree, db, G1).await,
        vec![row(&["ex:alice", "ex:bob"])],
        "{lane}: the `GRAPH <g> {{ }}` block landed in g1"
    );
    assert_eq!(
        knows_in(fluree, db, G2).await,
        vec![row(&["ex:bob", "ex:carol"])],
        "{lane}: the compact `<g> {{ }}` block landed in g2"
    );
}

/// The `<#txn-meta>` block became commit metadata.
async fn assert_txn_meta(fluree: &MemoryFluree, ledger_id: &str, lane: &str) {
    let query = json!({
        "from": format!("{ledger_id}#txn-meta"),
        "select": "?o",
        "where": {"@id": "?s", "http://example.org/batch": "?o"}
    });
    let result = fluree.query_connection(&query).await.expect("txn-meta");
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let rows = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        rows,
        json!(["b-42"]),
        "{lane}: the txn-meta block must reach the commit"
    );
}

/// A ledger whose namespace table has `http://example.org/`, which a policy
/// needs to encode `ex:ssn` at all (see `it_policy_tx.rs`). The seed triple
/// sits in its own graph, so the default graph and g1/g2 start empty.
async fn seeded(fluree: &MemoryFluree, id: &str) -> LedgerState {
    fluree.create_ledger(id).await.expect("create");
    fluree
        .graph(id)
        .transact()
        .upsert_turtle(
            "@prefix ex: <http://example.org/> .\n\
             GRAPH <http://example.org/seed> { ex:seed ex:ssn \"0\" . }\n",
        )
        .commit()
        .await
        .expect("seed");
    fluree.ledger(id).await.expect("load")
}

/// A non-root policy that allows every write except one to `ex:ssn`. Build it
/// on a [`seeded`] ledger.
async fn deny_ssn(ledger: &LedgerState) -> PolicyContext {
    let opts = GovernanceOptions {
        policy: Some(json!([{
            "@id": "http://example.org/denySsn",
            "f:required": true,
            "f:onProperty": [{"@id": "http://example.org/ssn"}],
            "f:action": "f:modify",
            "f:allow": false
        }])),
        default_allow: Some(true),
        ..Default::default()
    };
    build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
        &[0],
    )
    .await
    .expect("build policy context")
}

fn memory() -> MemoryFluree {
    FlureeBuilder::memory().build_memory()
}

// =============================================================================
// Each insert lane
// =============================================================================

#[tokio::test]
async fn fluree_insert_turtle_accepts_trig() {
    let fluree = memory();
    let id = "it/trig-insert-direct:main";
    let result = fluree
        .insert_turtle(genesis_ledger(&fluree, id), TRIG)
        .await
        .expect("Fluree::insert_turtle with TriG");
    assert_trig_landed(
        &fluree,
        &GraphDb::from_ledger_state(&result.ledger),
        "direct",
    )
    .await;
    assert_txn_meta(&fluree, id, "direct").await;
}

#[tokio::test]
async fn owned_builder_execute_accepts_trig() {
    let fluree = memory();
    let id = "it/trig-insert-owned:main";
    let result = fluree
        .stage_owned(genesis_ledger(&fluree, id))
        .insert_turtle(TRIG)
        .execute()
        .await
        .expect("stage_owned().insert_turtle(trig).execute()");
    assert_trig_landed(
        &fluree,
        &GraphDb::from_ledger_state(&result.ledger),
        "owned",
    )
    .await;
    assert_txn_meta(&fluree, id, "owned").await;
}

#[tokio::test]
async fn owned_builder_stage_accepts_trig() {
    let fluree = memory();
    let staged = fluree
        .stage_owned(genesis_ledger(&fluree, "it/trig-insert-owned-stage:main"))
        .insert_turtle(TRIG)
        .stage()
        .await
        .expect("stage_owned().insert_turtle(trig).stage()");
    let db = GraphDb::from_staged(&staged).expect("staged view");
    assert_trig_landed(&fluree, &db, "owned stage").await;
}

/// No policy: the cached-handle commit stages outside the write lock.
#[tokio::test]
async fn graph_builder_commit_accepts_trig() {
    let fluree = memory();
    let id = "it/trig-insert-graph:main";
    fluree.create_ledger(id).await.expect("create");
    fluree
        .graph(id)
        .transact()
        .insert_turtle(TRIG)
        .commit()
        .await
        .expect("graph().transact().insert_turtle(trig).commit()");
    let ledger = fluree.ledger(id).await.expect("load");
    assert_trig_landed(
        &fluree,
        &GraphDb::from_ledger_state(&ledger),
        "graph commit",
    )
    .await;
    assert_txn_meta(&fluree, id, "graph commit").await;
}

/// A policy moves the cached-handle commit under the write lock.
#[tokio::test]
async fn graph_builder_commit_with_policy_accepts_trig() {
    let fluree = memory();
    let id = "it/trig-insert-graph-policy:main";
    let ledger = seeded(&fluree, id).await;
    fluree
        .graph(id)
        .transact()
        .insert_turtle(TRIG)
        .policy(deny_ssn(&ledger).await)
        .commit()
        .await
        .expect("graph().transact().insert_turtle(trig).policy(..).commit()");
    let ledger = fluree.ledger(id).await.expect("load");
    assert_trig_landed(
        &fluree,
        &GraphDb::from_ledger_state(&ledger),
        "graph commit + policy",
    )
    .await;
    assert_txn_meta(&fluree, id, "graph commit + policy").await;
}

#[tokio::test]
async fn graph_builder_stage_accepts_trig() {
    let fluree = memory();
    let id = "it/trig-insert-graph-stage:main";
    fluree.create_ledger(id).await.expect("create");
    let staged = fluree
        .graph(id)
        .transact()
        .insert_turtle(TRIG)
        .stage()
        .await
        .expect("graph().transact().insert_turtle(trig).stage()");
    let db = GraphDb::from_staged(staged.staged()).expect("staged view");
    assert_trig_landed(&fluree, &db, "graph stage").await;
}

// =============================================================================
// Semantics
// =============================================================================

/// A document of graph blocks alone has an empty default graph.
#[tokio::test]
async fn trig_with_only_graph_blocks_is_accepted() {
    let fluree = memory();
    let trig = "@prefix ex: <http://example.org/> .\n\
                GRAPH <http://example.org/g1> { ex:alice ex:knows ex:bob . }\n";
    let result = fluree
        .insert_turtle(
            genesis_ledger(&fluree, "it/trig-insert-blocks-only:main"),
            trig,
        )
        .await
        .expect("graph blocks only");
    let db = GraphDb::from_ledger_state(&result.ledger);
    assert_eq!(
        knows_in(&fluree, &db, G1).await,
        vec![row(&["ex:alice", "ex:bob"])]
    );
}

/// Insert adds to a named graph; upsert replaces. Same document, so this pins
/// that the TriG path runs with the caller's transaction type.
#[tokio::test]
async fn trig_insert_adds_where_upsert_replaces() {
    let fluree = memory();
    let first = "@prefix ex: <http://example.org/> .\n\
                 GRAPH <http://example.org/g1> { ex:alice ex:status \"one\" . }\n";
    let second = "@prefix ex: <http://example.org/> .\n\
                  GRAPH <http://example.org/g1> { ex:alice ex:status \"two\" . }\n";
    let statuses = "PREFIX ex: <http://example.org/>\n\
                    SELECT ?o WHERE { GRAPH <http://example.org/g1> { ex:alice ex:status ?o } }";

    let base = |id: &'static str| {
        let fluree = &fluree;
        async move {
            fluree
                .insert_turtle(genesis_ledger(fluree, id), first)
                .await
                .expect("seed")
                .ledger
        }
    };

    let inserted = fluree
        .insert_turtle(base("it/trig-insert-vs-upsert-i:main").await, second)
        .await
        .expect("insert")
        .ledger;
    assert_eq!(
        select(&fluree, &GraphDb::from_ledger_state(&inserted), statuses).await,
        vec![row(&["one"]), row(&["two"])],
        "insert keeps the existing value"
    );

    let upserted = fluree
        .stage_owned(base("it/trig-insert-vs-upsert-u:main").await)
        .upsert_turtle(second)
        .execute()
        .await
        .expect("upsert")
        .ledger;
    assert_eq!(
        select(&fluree, &GraphDb::from_ledger_state(&upserted), statuses).await,
        vec![row(&["two"])],
        "upsert replaces it"
    );
}

/// TriG blank-node labels are document-scoped: `_:x` in the default graph and
/// in a block is one node.
#[tokio::test]
async fn trig_insert_shares_a_blank_node_across_graphs() {
    let fluree = memory();
    let trig = "@prefix ex: <http://example.org/> .\n\
                _:x ex:name \"X\" .\n\
                GRAPH <http://example.org/g1> { _:x ex:knows ex:bob . }\n";
    let result = fluree
        .insert_turtle(genesis_ledger(&fluree, "it/trig-insert-bnode:main"), trig)
        .await
        .expect("insert");
    let db = GraphDb::from_ledger_state(&result.ledger);
    let named = select(
        &fluree,
        &db,
        "PREFIX ex: <http://example.org/>\nSELECT ?s WHERE { ?s ex:name \"X\" }",
    )
    .await;
    let knows = knows_in(&fluree, &db, G1).await;
    assert_eq!(named.len(), 1, "one named node: {named:?}");
    assert_eq!(knows.len(), 1, "one edge in g1: {knows:?}");
    assert_eq!(
        named[0][0], knows[0][0],
        "the default-graph node and the g1 subject must be the same node"
    );
}

/// TriG-star: an annotation inside a graph block is written into that graph.
#[tokio::test]
async fn trig_insert_annotation_lands_in_its_graph() {
    let fluree = memory();
    let trig = "@prefix ex: <http://example.org/> .\n\
                GRAPH <http://example.org/g1> {\n\
                  ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 |} .\n\
                }\n";
    let result = fluree
        .insert_turtle(genesis_ledger(&fluree, "it/trig-insert-star:main"), trig)
        .await
        .expect("insert");
    let db = GraphDb::from_ledger_state(&result.ledger);
    let in_g1 = "PREFIX ex: <http://example.org/>\n\
                 SELECT ?r ?c WHERE { GRAPH <http://example.org/g1> \
                 { ex:alice ex:knows ex:bob ~ ?r {| ex:confidence ?c |} } }";
    assert_eq!(
        select(&fluree, &db, in_g1).await,
        vec![row(&["ex:claim1", "0.9"])]
    );
    let in_default = "PREFIX ex: <http://example.org/>\n\
                      SELECT ?c WHERE { ex:alice ex:knows ex:bob {| ex:confidence ?c |} }";
    assert!(
        select(&fluree, &db, in_default).await.is_empty(),
        "the annotation belongs to g1, not the default graph"
    );
}

/// The default graph of a TriG document goes through the named-graph path
/// rather than the streaming parser; it must store exactly what plain Turtle
/// stores.
#[tokio::test]
async fn trig_default_graph_matches_plain_turtle() {
    let default_graph = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice a ex:Person ;
    ex:name "Alice" , "Alicia"@es ;
    ex:age 42 ;
    ex:height 1.68 ;
    ex:weight "61.5"^^xsd:double ;
    ex:born "1990-05-01"^^xsd:date ;
    ex:active true ;
    ex:knows ex:bob ;
    ex:tags ( "a" "b" "c" ) .
ex:alice ex:knows ex:carol {| ex:since 2020 |} .
"#;
    let trig = format!("{default_graph}\nGRAPH <{G1}> {{ ex:alice ex:knows ex:dave . }}\n");
    let all = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";

    let fluree = memory();
    let plain = fluree
        .insert_turtle(
            genesis_ledger(&fluree, "it/trig-parity-turtle:main"),
            default_graph,
        )
        .await
        .expect("plain Turtle")
        .ledger;
    let via_trig = fluree
        .insert_turtle(genesis_ledger(&fluree, "it/trig-parity-trig:main"), &trig)
        .await
        .expect("TriG")
        .ledger;

    // Anonymous reifiers mint a fresh node per transaction; compare them by
    // shape, not by id.
    let normalize = |rows: Vec<Vec<String>>| -> Vec<Vec<String>> {
        let mut rows: Vec<Vec<String>> = rows
            .into_iter()
            .map(|r| {
                r.into_iter()
                    .map(|c| {
                        if c.starts_with("_:") {
                            "_:".to_string()
                        } else {
                            c
                        }
                    })
                    .collect()
            })
            .collect();
        rows.sort();
        rows
    };
    let expected = normalize(select(&fluree, &GraphDb::from_ledger_state(&plain), all).await);
    let actual = normalize(select(&fluree, &GraphDb::from_ledger_state(&via_trig), all).await);
    assert!(!expected.is_empty());
    assert_eq!(
        actual, expected,
        "a TriG document's default graph must match the same triples as plain Turtle"
    );
}

// =============================================================================
// Routing and errors
// =============================================================================

/// Routing outcomes at the Turtle insert site since `before`.
fn outcomes(store: &SpanStore, before: usize) -> Vec<String> {
    store.find_events("fast-path outcome")[before..]
        .iter()
        .filter(|e| e.fields.get("site").map(String::as_str) == Some(TURTLE_INSERT_SITE))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect()
}

/// Plain Turtle stays on the streaming parser even when it contains what a
/// cheap scan would take for TriG (an annotation brace, the word "GRAPH");
/// TriG takes the fallback.
#[tokio::test(flavor = "current_thread")]
async fn plain_turtle_stays_on_the_streaming_parser() {
    let (store, _guard) = init_test_tracing();
    let fluree = memory();

    // A sibling test can register the stamp's callsite under the no-op global
    // dispatcher while this subscriber is being installed, pinning its
    // interest to "never". Hit the callsite once, then rebuild the cache.
    let ledger = fluree
        .insert_turtle(
            genesis_ledger(&fluree, "it/trig-routing:main"),
            "<http://example.org/warm> <http://example.org/up> 1 .",
        )
        .await
        .expect("warm-up")
        .ledger;
    tracing::callsite::rebuild_interest_cache();

    let turtle = "@prefix ex: <http://example.org/> .\n\
                  ex:alice ex:knows ex:bob {| ex:note \"GRAPH { }\" |} .\n\
                  ex:alice ex:page <http://example.org/graph/1> .\n";
    let before = store.find_events("fast-path outcome").len();
    let ledger = fluree
        .insert_turtle(ledger, turtle)
        .await
        .expect("plain Turtle")
        .ledger;
    assert_eq!(
        outcomes(&store, before),
        vec!["proceed"],
        "MustFire: plain Turtle must take the streaming parser"
    );

    let before = store.find_events("fast-path outcome").len();
    fluree.insert_turtle(ledger, TRIG).await.expect("TriG");
    assert_eq!(
        outcomes(&store, before),
        vec!["fallback:gate_declined"],
        "MustNotFire: TriG must leave the streaming parser"
    );
}

/// Malformed plain Turtle keeps the streaming parser's error; malformed TriG
/// reports the graph block.
#[tokio::test]
async fn parse_errors_name_the_real_problem() {
    let fluree = memory();
    let ledger = genesis_ledger(&fluree, "it/trig-errors:main");

    // Contains `{` and "graph", so the TriG reader looks, finds no block, and
    // defers to the Turtle error.
    let bad_turtle = "@prefix ex: <http://example.org/> .\n\
                      ex:alice ex:knows ex:bob {| ex:note \"graph\" |} .\n\
                      ex:alice ex:name .\n";
    let err = fluree
        .insert_turtle(ledger.clone(), bad_turtle)
        .await
        .map(|_| ())
        .expect_err("malformed Turtle")
        .to_string();
    assert!(
        err.contains("Turtle parse error"),
        "expected the streaming parser's error, got: {err}"
    );

    let unclosed = "@prefix ex: <http://example.org/> .\n\
                    GRAPH <http://example.org/g1> { ex:alice ex:knows ex:bob .\n";
    let err = fluree
        .insert_turtle(ledger, unclosed)
        .await
        .map(|_| ())
        .expect_err("malformed TriG")
        .to_string();
    assert!(
        err.contains("expected '}' to close GRAPH block"),
        "a malformed block must be reported as one, not as Turtle choking on GRAPH, got: {err}"
    );
}

// =============================================================================
// Policy on named-graph flakes
// =============================================================================

/// A modify policy governs a TriG insert's named-graph flakes on both policy
/// lanes, and an allowed TriG insert still lands its blocks.
#[tokio::test]
async fn policy_governs_trig_insert_named_graphs() {
    let fluree = memory();
    let denied = "@prefix ex: <http://example.org/> .\n\
                  GRAPH <http://example.org/g1> { ex:bob ex:ssn \"999\" . }\n";

    let ledger = seeded(&fluree, "it/trig-insert-policy-owned:main").await;
    let ctx = deny_ssn(&ledger).await;
    let err = fluree
        .stage_owned(ledger.clone())
        .insert_turtle(denied)
        .policy(ctx.clone())
        .execute()
        .await
        .map(|_| ())
        .expect_err("owned: ex:ssn in a named graph must be denied");
    assert!(
        err.to_string()
            .contains("Policy enforcement prevents modification"),
        "owned: expected a policy refusal, got: {err}"
    );
    let result = fluree
        .stage_owned(ledger)
        .insert_turtle(TRIG)
        .policy(ctx)
        .execute()
        .await
        .expect("owned: an allowed TriG insert commits");
    assert_trig_landed(
        &fluree,
        &GraphDb::from_ledger_state(&result.ledger),
        "owned + policy",
    )
    .await;

    let id = "it/trig-insert-policy-graph:main";
    let ledger = seeded(&fluree, id).await;
    let err = fluree
        .graph(id)
        .transact()
        .insert_turtle(denied)
        .policy(deny_ssn(&ledger).await)
        .commit()
        .await
        .map(|_| ())
        .expect_err("graph: ex:ssn in a named graph must be denied");
    assert!(
        err.to_string()
            .contains("Policy enforcement prevents modification"),
        "graph: expected a policy refusal, got: {err}"
    );
}

// =============================================================================
// TriG upsert lanes that dropped graph blocks
// =============================================================================

const UPSERT_TRIG: &str = "@prefix ex: <http://example.org/> .\n\
                           ex:alice ex:name \"Alice\" .\n\
                           GRAPH <http://example.org/g1> { ex:alice ex:knows ex:bob . }\n";

async fn assert_upsert_landed(fluree: &MemoryFluree, db: &GraphDb, lane: &str) {
    assert_eq!(
        knows_in(fluree, db, G1).await,
        vec![row(&["ex:alice", "ex:bob"])],
        "{lane}: the graph block must not be dropped"
    );
}

/// The owned builder's policy lane parsed the default graph and ignored the
/// graph blocks.
#[tokio::test]
async fn owned_builder_upsert_with_policy_keeps_graph_blocks() {
    let fluree = memory();
    let ledger = seeded(&fluree, "it/trig-upsert-owned-policy:main").await;
    let ctx = deny_ssn(&ledger).await;

    let result = fluree
        .stage_owned(ledger.clone())
        .upsert_turtle(UPSERT_TRIG)
        .policy(ctx.clone())
        .execute()
        .await
        .expect("execute");
    assert_upsert_landed(
        &fluree,
        &GraphDb::from_ledger_state(&result.ledger),
        "owned execute + policy",
    )
    .await;

    let staged = fluree
        .stage_owned(ledger)
        .upsert_turtle(UPSERT_TRIG)
        .policy(ctx)
        .stage()
        .await
        .expect("stage");
    assert_upsert_landed(
        &fluree,
        &GraphDb::from_staged(&staged).expect("staged view"),
        "owned stage + policy",
    )
    .await;
}

/// The graph builder's `stage()` never passed the graph blocks on, with or
/// without a policy, while its `commit()` did.
#[tokio::test]
async fn graph_builder_stage_keeps_upsert_graph_blocks() {
    let fluree = memory();
    let id = "it/trig-upsert-graph-stage:main";
    let ledger = seeded(&fluree, id).await;

    let staged = fluree
        .graph(id)
        .transact()
        .upsert_turtle(UPSERT_TRIG)
        .stage()
        .await
        .expect("stage");
    assert_upsert_landed(
        &fluree,
        &GraphDb::from_staged(staged.staged()).expect("staged view"),
        "graph stage",
    )
    .await;

    let staged = fluree
        .graph(id)
        .transact()
        .upsert_turtle(UPSERT_TRIG)
        .policy(deny_ssn(&ledger).await)
        .stage()
        .await
        .expect("stage + policy");
    assert_upsert_landed(
        &fluree,
        &GraphDb::from_staged(staged.staged()).expect("staged view"),
        "graph stage + policy",
    )
    .await;
}

// =============================================================================
// A directive applies only to what follows it
// =============================================================================

/// Every triple as `[graph, s, p, o]` with full IRIs; `graph` is empty for
/// the default graph.
async fn quads(fluree: &MemoryFluree, db: &GraphDb) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = select(fluree, db, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
        .await
        .into_iter()
        .map(|r| [vec![String::new()], r].concat())
        .collect();
    rows.extend(
        select(
            fluree,
            db,
            "SELECT ?g ?s ?p ?o WHERE { GRAPH ?g { ?s ?p ?o } }",
        )
        .await,
    );
    rows.sort();
    rows
}

/// TriG phase 1 rebuilt a document's default graph with every directive
/// first and expanded every graph block with the document's final prefix
/// map, so a later `@prefix`/`@base` silently rewrote the IRIs before it.
/// Upsert takes phase 1 whenever the text holds `{` or the bytes "graph",
/// even inside a literal; insert takes it for TriG. The last case is a
/// control with no redefinition.
#[tokio::test]
async fn a_redefined_prefix_or_base_applies_only_after_it() {
    let fluree = memory();
    let (g1, g2) = (G1, G2);
    let q = |g: &str, s: &str, p: &str, o: &str| row(&[g, s, p, o]);
    let cases = [
        (
            "a prefix redefined after a default triple that mentions \"graph\"",
            "@prefix ex: <http://a.org/> .\n\
             ex:x ex:p \"mentions the graph word\" .\n\
             @prefix ex: <http://b.org/> .\n\
             ex:y ex:p \"second\" .\n"
                .to_string(),
            vec![
                q(
                    "",
                    "http://a.org/x",
                    "http://a.org/p",
                    "mentions the graph word",
                ),
                q("", "http://b.org/y", "http://b.org/p", "second"),
            ],
        ),
        (
            "graph blocks before and after a prefix redefinition",
            format!(
                "@prefix ex: <http://a.org/> .\n\
                 GRAPH <{g1}> {{ ex:x ex:p \"1\" . }}\n\
                 @prefix ex: <http://b.org/> .\n\
                 GRAPH <{g2}> {{ ex:y ex:p \"2\" . }}\n"
            ),
            vec![
                q(g1, "http://a.org/x", "http://a.org/p", "1"),
                q(g2, "http://b.org/y", "http://b.org/p", "2"),
            ],
        ),
        (
            "a base redefined after a default triple",
            format!(
                "@base <http://a.org/> .\n\
                 <x> <p> \"mentions the graph word\" .\n\
                 GRAPH <{g1}> {{ <y> <p> \"1\" . }}\n\
                 @base <http://b.org/> .\n\
                 <z> <p> \"2\" .\n"
            ),
            vec![
                q(
                    "",
                    "http://a.org/x",
                    "http://a.org/p",
                    "mentions the graph word",
                ),
                q("", "http://b.org/z", "http://b.org/p", "2"),
                q(g1, "http://a.org/y", "http://a.org/p", "1"),
            ],
        ),
        (
            "control: no redefinition",
            format!(
                "@prefix ex: <http://a.org/> .\n\
                 ex:x ex:p \"graph\" .\n\
                 GRAPH <{g1}> {{ ex:y ex:p \"1\" . }}\n"
            ),
            vec![
                q("", "http://a.org/x", "http://a.org/p", "graph"),
                q(g1, "http://a.org/y", "http://a.org/p", "1"),
            ],
        ),
    ];

    let mut failures = Vec::new();
    for (i, (case, doc, expected)) in cases.iter().enumerate() {
        for lane in ["upsert", "insert"] {
            let ledger = genesis_ledger(&fluree, &format!("it/trig-directives-{i}-{lane}:main"));
            let result = if lane == "upsert" {
                fluree
                    .stage_owned(ledger)
                    .upsert_turtle(doc)
                    .execute()
                    .await
            } else {
                fluree.insert_turtle(ledger, doc).await
            };
            match result {
                Ok(r) => {
                    let got = quads(&fluree, &GraphDb::from_ledger_state(&r.ledger)).await;
                    if got != *expected {
                        failures.push(format!("{case} ({lane}): {got:?}"));
                    }
                }
                Err(e) => failures.push(format!("{case} ({lane}): {e}")),
            }
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
