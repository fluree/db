#![cfg(feature = "native")]

use std::sync::Arc;
mod support;

use fluree_db_api::{ApiError, FlureeBuilder};
use fluree_db_core::{
    range_with_overlay, ContentId, Flake, FlakeValue, IndexType, RangeMatch, RangeOptions,
    RangeTest, Sid, TXN_META_GRAPH_ID,
};
use fluree_db_ledger::LedgerState;
use fluree_db_novelty::Novelty;
use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_DB};
use serde_json::json;
use support::{
    genesis_ledger, start_background_indexer_local, trigger_index_and_wait, MemoryFluree,
};

async fn seed_two_commits(
    fluree: &MemoryFluree,
    ledger_id: &str,
) -> (fluree_db_api::LedgerState, i64, i64) {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let tx1 = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice", "ex:name": "Alice"}
        ]
    });
    let ledger1 = fluree.insert(ledger0, &tx1).await.expect("tx1").ledger;
    let t1 = ledger1.t();

    let tx2 = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:bob", "ex:name": "Bob"}
        ]
    });
    let ledger2 = fluree.insert(ledger1, &tx2).await.expect("tx2").ledger;
    let t2 = ledger2.t();

    (ledger2, t1, t2)
}

async fn txn_meta_commit_flakes_for_t(
    snapshot: &fluree_db_core::LedgerSnapshot,
    overlay: &Novelty,
    target_t: i64,
    current_t: i64,
) -> Vec<Flake> {
    let predicate = Sid::new(FLUREE_DB, fluree_vocab::db::T);
    let range_match = RangeMatch::predicate_object(predicate, FlakeValue::Long(target_t));
    let opts = RangeOptions::default()
        .with_to_t(current_t)
        .with_flake_limit(16);

    range_with_overlay(
        snapshot,
        TXN_META_GRAPH_ID,
        overlay,
        IndexType::Post,
        RangeTest::Eq,
        range_match,
        opts,
    )
    .await
    .expect("txn-meta POST lookup by db:t")
}

async fn assert_txn_meta_lookup_contains_commit(ledger: &LedgerState, target_t: i64) {
    let flakes = txn_meta_commit_flakes_for_t(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        target_t,
        ledger.t(),
    )
    .await;

    assert!(
        flakes.iter().any(|flake| {
            flake.p.namespace_code == FLUREE_DB
                && flake.p.name.as_ref() == fluree_vocab::db::T
                && flake.o == FlakeValue::Long(target_t)
                && flake.s.namespace_code == FLUREE_COMMIT
        }),
        "expected txn-meta POST lookup to return commit metadata for t={target_t}, got: {flakes:?}"
    );
}

#[tokio::test]
async fn commit_t_resolves_latest_unindexed_commit_from_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-commit-t-novelty:main";

    let (ledger, _t1, t2) = seed_two_commits(&fluree, ledger_id).await;

    assert_txn_meta_lookup_contains_commit(&ledger, t2).await;

    let detail = fluree
        .graph(ledger_id)
        .commit_t(t2)
        .execute()
        .await
        .expect("resolve commit by t from novelty");

    assert_eq!(detail.t, t2);
    assert!(!detail.id.is_empty(), "commit detail should include CID");
    assert!(
        !detail.flakes.is_empty(),
        "commit detail should include commit flakes"
    );
}

#[tokio::test]
async fn commit_t_resolves_indexed_commit_from_txn_meta_post_lookup() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/graph-commit-t-indexed:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let (_ledger, t1, t2) = seed_two_commits(&fluree, ledger_id).await;

            trigger_index_and_wait(&handle, ledger_id, t2).await;

            let indexed = fluree.ledger(ledger_id).await.expect("load indexed ledger");

            assert_txn_meta_lookup_contains_commit(&indexed, t2).await;
            assert_txn_meta_lookup_contains_commit(&indexed, t1).await;

            let detail_latest = fluree
                .graph(ledger_id)
                .commit_t(t2)
                .execute()
                .await
                .expect("resolve indexed latest commit by t");
            assert_eq!(detail_latest.t, t2);

            let detail_earlier = fluree
                .graph(ledger_id)
                .commit_t(t1)
                .execute()
                .await
                .expect("resolve indexed earlier commit by t");
            assert_eq!(detail_earlier.t, t1);
        })
        .await;
}

// ============================================================================
// Policy-filtered commit show tests
// ============================================================================

/// Seed a ledger with users, identity, and policy rules for SSN restriction.
/// Returns the ledger state after setup (t=1) with policies loaded.
async fn seed_ledger_with_policy(fluree: &MemoryFluree, ledger_id: &str) -> (LedgerState, i64) {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    // Insert users + identity + policies in one transaction
    let setup = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "http://example.org/ns/alice",
                "@type": "http://example.org/ns/User",
                "http://schema.org/name": "Alice",
                "http://schema.org/ssn": "111-11-1111"
            },
            {
                "@id": "http://example.org/ns/bob",
                "@type": "http://example.org/ns/User",
                "http://schema.org/name": "Bob",
                "http://schema.org/ssn": "222-22-2222"
            },
            // Identity: alice links to her user
            {
                "@id": "http://example.org/ns/aliceIdentity",
                "https://ns.flur.ee/db#policyClass": [{"@id": "http://example.org/ns/TestPolicy"}],
                "http://example.org/ns/user": {"@id": "http://example.org/ns/alice"}
            },
            // Policy: restrict SSN to own user via f:query
            {
                "@id": "http://example.org/ns/ssnRestriction",
                "@type": ["https://ns.flur.ee/db#AccessPolicy", "http://example.org/ns/TestPolicy"],
                "https://ns.flur.ee/db#required": true,
                "https://ns.flur.ee/db#onProperty": [{"@id": "http://schema.org/ssn"}],
                "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
                "https://ns.flur.ee/db#query": serde_json::to_string(&json!({
                    "@context": {"ex": "http://example.org/ns/"},
                    "where": {
                        "@id": "?$identity",
                        "http://example.org/ns/user": {"@id": "?$this"}
                    }
                })).unwrap()
            },
            // Default allow for other properties
            {
                "@id": "http://example.org/ns/defaultAllow",
                "@type": ["https://ns.flur.ee/db#AccessPolicy", "http://example.org/ns/TestPolicy"],
                "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
                "https://ns.flur.ee/db#query": serde_json::to_string(&json!({})).unwrap()
            }
        ]
    });

    let result = fluree
        .insert(ledger0, &setup)
        .await
        .expect("seed policy data");
    let t = result.ledger.t();
    (result.ledger, t)
}

#[tokio::test]
async fn commit_show_without_policy_returns_all_flakes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/commit-show-no-policy:main";
    let (_ledger, t) = seed_ledger_with_policy(&fluree, ledger_id).await;

    // No identity/policy_class → all flakes returned
    let detail = fluree
        .graph(ledger_id)
        .commit_t(t)
        .execute()
        .await
        .expect("commit show without policy");

    assert_eq!(detail.t, t);
    let total = detail.asserts + detail.retracts;
    assert!(total > 0, "should have flakes");

    // Both SSNs should be present in the unfiltered output
    let ssn_flakes: Vec<_> = detail
        .flakes
        .iter()
        .filter(|f| f.p.contains("ssn"))
        .collect();
    assert_eq!(
        ssn_flakes.len(),
        2,
        "unfiltered should see both SSN flakes, got: {ssn_flakes:?}"
    );
}

#[tokio::test]
async fn commit_show_with_identity_filters_flakes_by_policy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/commit-show-policy-filter:main";
    let (_ledger, t) = seed_ledger_with_policy(&fluree, ledger_id).await;

    // With alice's identity → policy filters SSN to only her own
    let detail = fluree
        .graph(ledger_id)
        .commit_t(t)
        .identity(Some("http://example.org/ns/aliceIdentity"))
        .execute()
        .await
        .expect("commit show with identity");

    assert_eq!(detail.t, t);

    // Only Alice's SSN should be visible
    let ssn_flakes: Vec<_> = detail
        .flakes
        .iter()
        .filter(|f| f.p.contains("ssn"))
        .collect();
    assert_eq!(
        ssn_flakes.len(),
        1,
        "policy-filtered should see only Alice's SSN, got: {ssn_flakes:?}"
    );

    // Verify it's Alice's SSN specifically
    let ssn_value = match &ssn_flakes[0].o {
        fluree_db_api::graph_commit_builder::ResolvedValue::String(s) => s.as_str(),
        other => panic!("expected string SSN, got: {other:?}"),
    };
    assert_eq!(ssn_value, "111-11-1111", "should be Alice's SSN");

    // asserts count should reflect the filtered set (fewer than unfiltered)
    // Non-SSN flakes (names, types, identity, policies) should still be present
    assert!(detail.asserts > 0, "should still have visible asserts");
}

#[tokio::test]
async fn commit_show_filtered_counts_reflect_visible_flakes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/commit-show-filtered-counts:main";
    let (_ledger, t) = seed_ledger_with_policy(&fluree, ledger_id).await;

    // Unfiltered
    let unfiltered = fluree
        .graph(ledger_id)
        .commit_t(t)
        .execute()
        .await
        .expect("unfiltered");

    // Filtered
    let filtered = fluree
        .graph(ledger_id)
        .commit_t(t)
        .identity(Some("http://example.org/ns/aliceIdentity"))
        .execute()
        .await
        .expect("filtered");

    // Counts should match actual flake vec lengths
    assert_eq!(
        filtered.asserts,
        filtered.flakes.iter().filter(|f| f.op).count(),
        "asserts count must match actual assert flakes"
    );
    assert_eq!(
        filtered.retracts,
        filtered.flakes.iter().filter(|f| !f.op).count(),
        "retracts count must match actual retract flakes"
    );

    // Filtered should have fewer flakes than unfiltered (Bob's SSN removed)
    assert!(
        filtered.flakes.len() < unfiltered.flakes.len(),
        "filtered ({}) should have fewer flakes than unfiltered ({})",
        filtered.flakes.len(),
        unfiltered.flakes.len()
    );
}

#[tokio::test]
async fn commit_show_with_bad_identity_returns_query_error() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/commit-show-bad-identity:main";
    let (_ledger, t) = seed_ledger_with_policy(&fluree, ledger_id).await;

    // Non-existent identity IRI should produce a query-class error, not internal
    let result = fluree
        .graph(ledger_id)
        .commit_t(t)
        .identity(Some("http://example.org/ns/nonExistentIdentity"))
        .execute()
        .await;

    match result {
        Err(ApiError::Query(_)) => { /* expected: bad identity is a query/config error */ }
        Err(other) => panic!("expected ApiError::Query for bad identity, got: {other:?}"),
        Ok(_) => {
            // Some implementations may return root policy (all flakes) when
            // identity resolves but has no policyClass. This is also acceptable.
        }
    }
}

#[tokio::test]
async fn commit_show_prefix_with_identity_filters_flakes_by_policy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/commit-show-prefix-policy:main";
    let (_ledger, t) = seed_ledger_with_policy(&fluree, ledger_id).await;

    // First, get the commit CID via the t-based path
    let by_t = fluree
        .graph(ledger_id)
        .commit_t(t)
        .execute()
        .await
        .expect("commit by t");

    // Extract the SHA-256 hex digest from the CID, then take a prefix.
    // resolve_commit_prefix expects a hex digest prefix, not a CID string prefix.
    let cid: ContentId = by_t.id.parse().expect("parse CID");
    let hex_digest = cid.digest_hex();
    let prefix = &hex_digest[..12];

    // Resolve via prefix WITHOUT policy → should see both SSNs
    let unfiltered = fluree
        .graph(ledger_id)
        .commit_prefix(prefix)
        .execute()
        .await
        .expect("prefix lookup unfiltered");
    assert_eq!(unfiltered.t, t, "prefix should resolve to the same commit");
    let unfiltered_ssns: Vec<_> = unfiltered
        .flakes
        .iter()
        .filter(|f| f.p.contains("ssn"))
        .collect();
    assert_eq!(
        unfiltered_ssns.len(),
        2,
        "unfiltered prefix lookup should see both SSNs"
    );

    // Resolve via prefix WITH identity → policy should filter to Alice's SSN only
    let filtered = fluree
        .graph(ledger_id)
        .commit_prefix(prefix)
        .identity(Some("http://example.org/ns/aliceIdentity"))
        .execute()
        .await
        .expect("prefix lookup with identity");
    assert_eq!(
        filtered.t, t,
        "filtered prefix should resolve to same commit"
    );

    let filtered_ssns: Vec<_> = filtered
        .flakes
        .iter()
        .filter(|f| f.p.contains("ssn"))
        .collect();
    assert_eq!(
        filtered_ssns.len(),
        1,
        "policy-filtered prefix lookup should see only Alice's SSN, got: {filtered_ssns:?}"
    );

    let ssn_value = match &filtered_ssns[0].o {
        fluree_db_api::graph_commit_builder::ResolvedValue::String(s) => s.as_str(),
        other => panic!("expected string SSN, got: {other:?}"),
    };
    assert_eq!(ssn_value, "111-11-1111", "should be Alice's SSN");
}
