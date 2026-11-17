//! One `PolicyContext` is handed to every op of a `;`-separated SPARQL
//! UPDATE while the sequential stager advances `t` between ops. The class
//! cache is write-once, so it must be keyed on `t`: op 1 reads ex:doc1
//! (caching its classes) and then makes it ex:Secret; op 2's WHERE runs at
//! `t+1` and must not see it.

use crate::support;

use fluree_db_api::{policy_builder, FlureeBuilder, GovernanceOptions, PolicyContext};
use serde_json::json;

const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n";
const OP1: &str = "INSERT { ?d a ex:Secret } WHERE { ?d ex:title ?t }";
const OP2: &str = "INSERT { ?d ex:leaked ?t } WHERE { ?d ex:title ?t }";

async fn setup(fluree: &fluree_db_api::Fluree, id: &str) {
    fluree.create_ledger(id).await.expect("create ledger");
    fluree
        .graph(id)
        .transact()
        .sparql_update(&format!(
            "{PREFIX}INSERT DATA {{ ex:doc1 a ex:Doc ; ex:title \"T1\" . ex:doc0 a ex:Secret ; ex:title \"T0\" }}"
        ))
        .commit()
        .await
        .expect("seed");
}

/// default-allow, except members of ex:Secret are not viewable.
async fn secret_hidden_ctx(fluree: &fluree_db_api::Fluree, id: &str) -> PolicyContext {
    let ledger = fluree.ledger(id).await.expect("ledger");
    let opts = GovernanceOptions {
        policy: Some(json!([{
            "@id": "ex:secretHidden",
            "f:required": true,
            "f:onClass": [{"@id": "http://example.org/ns/Secret"}],
            "f:action": [{"@id": "f:view"}],
            "f:allow": false
        }])),
        default_allow: Some(true),
        ..Default::default()
    };
    policy_builder::build_policy_context_from_opts(
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

async fn leaked_count(fluree: &fluree_db_api::Fluree, id: &str) -> usize {
    let ledger = fluree.ledger(id).await.expect("ledger");
    let rows = support::query_sparql(
        fluree,
        &ledger,
        &format!("{PREFIX}SELECT ?d WHERE {{ ?d ex:leaked ?t }}"),
    )
    .await
    .expect("root query")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    rows.as_array().map(Vec::len).unwrap_or(0)
}

async fn secret_count(fluree: &fluree_db_api::Fluree, id: &str) -> usize {
    let ledger = fluree.ledger(id).await.expect("ledger");
    let rows = support::query_sparql(
        fluree,
        &ledger,
        &format!("{PREFIX}SELECT ?d WHERE {{ ?d a ex:Secret }}"),
    )
    .await
    .expect("root query")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    rows.as_array().map(Vec::len).unwrap_or(0)
}

/// Oracle: the two ops as two requests, a fresh context each.
#[tokio::test]
async fn control_two_requests() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "repro/class-cache-seq-control:main";
    setup(&fluree, id).await;

    let ctx = secret_hidden_ctx(&fluree, id).await;
    fluree
        .graph(id)
        .transact()
        .sparql_update(&format!("{PREFIX}{OP1}"))
        .policy(ctx)
        .commit()
        .await
        .expect("op 1 commits");
    assert_eq!(
        secret_count(&fluree, id).await,
        2,
        "op 1 made doc1 Secret (doc0 already was)"
    );

    let ctx = secret_hidden_ctx(&fluree, id).await;
    // Op 2's WHERE matches nothing once doc1 is Secret; an empty txn may error.
    let _ = fluree
        .graph(id)
        .transact()
        .sparql_update(&format!("{PREFIX}{OP2}"))
        .policy(ctx)
        .commit()
        .await;
    assert_eq!(
        leaked_count(&fluree, id).await,
        0,
        "control: a Secret doc's title must not be readable by op 2's WHERE"
    );
}

/// Same two ops as ONE request: op 2 must observe op 1's class change.
#[tokio::test]
async fn one_request_two_ops() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "repro/class-cache-seq:main";
    setup(&fluree, id).await;

    let ctx = secret_hidden_ctx(&fluree, id).await;
    fluree
        .graph(id)
        .transact()
        .sparql_update(&format!("{PREFIX}{OP1} ;\n{OP2}"))
        .policy(ctx)
        .commit()
        .await
        .expect("multi-op request commits");
    assert_eq!(
        secret_count(&fluree, id).await,
        2,
        "op 1 made doc1 Secret (doc0 already was)"
    );
    assert_eq!(
        leaked_count(&fluree, id).await,
        0,
        "op 2's WHERE ran at t+1 where doc1 is Secret, so it must bind nothing; \
         a stale class-cache entry from op 1 lets it through"
    );
}

/// Discriminator: op 1 has NO WHERE, so nothing is cached before op 2. If this
/// still leaks, the cause is not cache staleness.
#[tokio::test]
async fn one_request_two_ops_nothing_cached_by_op1() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "repro/class-cache-seq-nocache:main";
    setup(&fluree, id).await;

    let ctx = secret_hidden_ctx(&fluree, id).await;
    fluree
        .graph(id)
        .transact()
        .sparql_update(&format!(
            "{PREFIX}INSERT DATA {{ ex:doc1 a ex:Secret }} ;\n{OP2}"
        ))
        .policy(ctx)
        .commit()
        .await
        .expect("multi-op request commits");
    assert_eq!(
        secret_count(&fluree, id).await,
        2,
        "op 1 made doc1 Secret (doc0 already was)"
    );
    assert_eq!(
        leaked_count(&fluree, id).await,
        0,
        "nothing was cached before op 2, so a leak here is not staleness"
    );
}
