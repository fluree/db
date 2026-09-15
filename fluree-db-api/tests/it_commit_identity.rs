//! Commit identity: the spellings a commit reference may be written in.
//!
//! A commit has two canonical spellings and they are not interchangeable.
//! [`ContentId`] *displays* as a base32 CIDv1 — that is the JSON API wire
//! format and the `db:address` flake value — but the indexed commit subject is
//! minted from `ContentId::digest_hex()`, so both prefix resolvers scan hex.
//!
//! `ledger_view::normalize_commit_ref` is the single place that reconciles the
//! two, and these tests pin it through the surfaces that reach it, not through
//! the helper directly. The unit tests beside the function cover the spelling
//! rules; what is checked here is that the resolvers are actually wired to it.
//!
//! The surfaces exercised are `@commit:<x>` in a `from` clause and
//! `Fluree::db_at(_, TimeSpec::AtCommit)`. Both route through
//! `time_resolve::commit_to_t` — the copy that serves `--at`, `fluree history`
//! and `branch create --at`, and that until now accepted only a bare hex
//! prefix.

#![cfg(feature = "native")]

use crate::support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::{FlureeBuilder, TimeSpec};
use serde_json::{json, Value as JsonValue};

fn ctx_test() -> JsonValue {
    json!({
        "test": "http://example.org/test#",
        "name": "test:name",
        "Person": "test:Person"
    })
}

/// Two commits. Returns the ledger plus the t=1 commit id, which every
/// spelling below names.
async fn seed(fluree: &MemoryFluree, ledger_id: &str) -> (MemoryLedger, fluree_db_core::ContentId) {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let tx1 = json!({
        "@context": ctx_test(),
        "@graph": [{"@id":"test:person1","@type":"Person","name":"Alice"}]
    });
    let out1 = fluree.insert(ledger0, &tx1).await.expect("insert t=1");
    let commit_t1 = out1.receipt.commit_id.clone();

    let tx2 = json!({
        "@context": ctx_test(),
        "@graph": [{"@id":"test:person2","@type":"Person","name":"Bob"}]
    });
    let ledger2 = fluree
        .insert(out1.ledger, &tx2)
        .await
        .expect("insert t=2")
        .ledger;

    (ledger2, commit_t1)
}

async fn names_at(
    fluree: &MemoryFluree,
    db_for_formatting: fluree_db_core::GraphDbRef<'_>,
    from_spec: &str,
) -> Result<Vec<JsonValue>, String> {
    let q = json!({
        "@context": ctx_test(),
        "from": [from_spec],
        "select": ["?name"],
        "where": [{"@id":"?s","name":"?name"}],
        "orderBy": ["?name"]
    });

    let result = fluree
        .query_connection(&q)
        .await
        .map_err(|e| e.to_string())?;
    let jsonld = result
        .to_jsonld_async(db_for_formatting)
        .await
        .map_err(|e| e.to_string())?;
    Ok(normalize_rows(&jsonld))
}

/// Every spelling of one commit selects the same state.
///
/// The full-CID and `sha256:`-wrapped forms are the new ones: before the two
/// resolvers shared a normalizer, `@commit:<full CID>` failed with "No commit
/// found with prefix", because the scan is keyed on the hex digest and a CID
/// string is base32.
#[tokio::test]
async fn every_spelling_of_a_commit_resolves_to_the_same_state() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/commit-identity";
    let (ledger, commit_t1) = seed(&fluree, ledger_id).await;

    let hex = commit_t1.digest_hex();
    let at_t1 = normalize_rows(&json!([["Alice"]]));

    for spelling in [
        hex.clone(),                           // full hex digest
        hex[..12].to_string(),                 // what `fluree log` prints
        hex[..6].to_string(),                  // the resolver's floor
        format!("sha256:{hex}"),               // wrapped
        format!("fluree:commit:sha256:{hex}"), // the #txn-meta IRI
        commit_t1.to_string(),                 // the full base32 CID
    ] {
        let rows = names_at(
            &fluree,
            ledger.as_graph_db_ref(0),
            &format!("{ledger_id}@commit:{spelling}"),
        )
        .await
        .unwrap_or_else(|e| panic!("@commit:{spelling} should resolve, got: {e}"));

        assert_eq!(rows, at_t1, "@commit:{spelling} selected the wrong state");
    }
}

/// An abbreviated CID is refused by name, not reported as a missing prefix.
///
/// This is the shape a user gets by copying an id out of a JSON API response
/// and trimming it, or out of any `fluree log` built before the output was
/// switched to hex. It cannot be scanned for — the leading characters are a
/// constant — so the diagnostic has to say that rather than imply the commit
/// does not exist.
///
/// Both surfaces are asserted, because they do not share an error path:
/// `build_source_view` rewrites any `is_not_found()` coming out of `db_at`
/// into "ledger not found", so a commit-resolution failure only survives the
/// `from` clause while it is typed the way its neighbours are. Pinning both
/// makes that coupling visible if someone retypes the error later.
#[tokio::test]
async fn an_abbreviated_cid_is_refused_by_name() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/commit-identity-abbrev";
    let (ledger, commit_t1) = seed(&fluree, ledger_id).await;

    let cid = commit_t1.to_string();

    // 11 characters is what the width-from-screen abbreviation printed for a
    // single-commit log; 15 is what it printed for a handful.
    for len in [7usize, 11, 15] {
        let short = &cid[..len];

        let via_db_at = fluree
            .db_at(ledger_id, TimeSpec::AtCommit(short.to_string()))
            .await
            .expect_err("an abbreviated CID cannot be resolved")
            .to_string();
        assert!(
            via_db_at.contains("abbreviated CID"),
            "db_at at {len} characters should name the cause, got: {via_db_at}"
        );

        let via_from = names_at(
            &fluree,
            ledger.as_graph_db_ref(0),
            &format!("{ledger_id}@commit:{short}"),
        )
        .await
        .expect_err("an abbreviated CID cannot be resolved");
        assert!(
            via_from.contains("abbreviated CID"),
            "the from clause at {len} characters should name the cause, got: {via_from}"
        );
    }

    // The full CID, through the same surface, resolves.
    fluree
        .db_at(ledger_id, TimeSpec::AtCommit(cid.clone()))
        .await
        .expect("a full CID resolves");
}
