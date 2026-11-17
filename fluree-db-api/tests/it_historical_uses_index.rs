//! Regression test for the historical-query gate.
//!
//! Before the fix, `HistoricalLedgerView::load_at_with_store` required
//! `target_t >= index_t` to use the binary index, forcing any query at
//! `target_t < index_t` to fall back to overlay-only reconstruction
//! (genesis snapshot + replay of every prior commit). Combined with the
//! expansion `select {"?s": ["*"]}` path — which issues one `range()`
//! call per subject and per reverse property — this turned historical
//! queries into an O(N × total-overlay) scan that OOMed on real ledgers.
//!
//! The correct condition is `base_t <= target_t`: the index's FIR6
//! Region 3 history covers everything in `base_t..=index_t`. Novelty
//! replay handles `(index_t, target_t]`.
//!
//! This test reproduces the incident: index the ledger, add a commit on
//! top, then query at `target_t < index_t` with expansion `select *`.
//! The historical view must use the index (not fall back to genesis),
//! and results must match what was present at `target_t`.

#![cfg(feature = "native")]

use crate::support::hooked_storage::{HookedStorage, StorageHooks};
use async_trait::async_trait;
use fluree_db_api::{FlureeBuilder, FormatterConfig, NameServiceMode, ReindexOptions};
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

fn ctx() -> serde_json::Value {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

#[tokio::test]
async fn historical_view_uses_index_when_target_below_index_t() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();

    let fluree = FlureeBuilder::file(path).build().expect("build");
    let ledger_id = "test/historical-index:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");

    // t=1: 20 Contacts in one transaction.
    let mut contacts = Vec::with_capacity(20);
    for i in 0..20 {
        contacts.push(json!({
            "@id": format!("ex:contact{:02}", i),
            "@type": "ex:Contact",
            "ex:name": format!("Contact {:02}", i),
            "ex:email": format!("contact{:02}@example.org", i),
        }));
    }
    let tx1 = json!({
        "@context": ctx(),
        "@graph": contacts,
    });
    let r1 = fluree.insert(ledger0, &tx1).await.expect("tx1");
    assert_eq!(r1.receipt.t, 1);

    // t=2: One extra contact so `commit_t = 2` after the index.
    let tx2 = json!({
        "@context": ctx(),
        "@id": "ex:contact20",
        "@type": "ex:Contact",
        "ex:name": "Contact 20",
        "ex:email": "contact20@example.org",
    });
    let r2 = fluree.insert(r1.ledger, &tx2).await.expect("tx2");
    assert_eq!(r2.receipt.t, 2);

    // Index at t=2.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");

    // t=3: One more contact lands in novelty, so `commit_t (3) > index_t (2)`.
    let tx3 = json!({
        "@context": ctx(),
        "@id": "ex:contact21",
        "@type": "ex:Contact",
        "ex:name": "Contact 21",
        "ex:email": "contact21@example.org",
    });
    let r3 = fluree.insert(r2.ledger, &tx3).await.expect("tx3");
    assert_eq!(r3.receipt.t, 3);

    // Sanity: ledger advanced and has an index behind the head.
    let status = fluree.index_status(ledger_id).await.expect("index_status");
    assert_eq!(status.index_t, 2, "index should be at t=2");
    assert_eq!(status.commit_t, 3, "commit should be at t=3");

    // ---- Structural assertion: querying below index_t uses the index ----
    //
    // Before the fix, `load_at_with_store` returned a genesis snapshot
    // (`snapshot.t == 0`) whenever `target_t < index_t`, forcing every
    // subsequent range() call through the overlay-only replay path.
    //
    // After the fix, the historical view wraps the indexed snapshot
    // (`snapshot.t == index_t`) and lets FIR6 history answer the query.
    let view_at_1 = fluree
        .ledger_view_at(ledger_id, 1)
        .await
        .expect("ledger_view_at t=1");
    assert_eq!(
        view_at_1.index_t(),
        2,
        "historical view at t=1 must wrap the indexed snapshot \
         (t=2), not fall back to genesis (t=0). \
         Genesis fallback forces overlay-only replay of the full ledger \
         history per range() call — the bug this test locks in."
    );
    assert_eq!(view_at_1.to_t(), 1, "view's query bound should be t=1");

    // Also exercise t=2 (boundary: `target_t == index_t`). This worked
    // under the old gate too, but is cheap insurance against regressions
    // in the gate's boundary handling.
    let view_at_2 = fluree
        .ledger_view_at(ledger_id, 2)
        .await
        .expect("ledger_view_at t=2");
    assert_eq!(view_at_2.index_t(), 2);

    // ---- Functional assertion: expansion `select *` at t<index_t ----
    //
    // This is the exact query shape that OOMed in production. At t=1 we
    // should see all 20 Contacts from tx1 and none from tx2/tx3.
    let crawl = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:1"),
        "where": {"@id": "?s", "@type": "ex:Contact"},
        "select": {"?s": ["*"]},
    });
    let result = fluree
        .query_from()
        .jsonld(&crawl)
        .format(FormatterConfig::typed_json().with_normalize_arrays())
        .execute_tracked()
        .await
        .expect("expansion at t=1");

    let value = serde_json::to_value(&result.result).expect("serialize");
    let rows = value.as_array().expect("crawl result is array");
    assert_eq!(
        rows.len(),
        20,
        "t=1 expansion must return exactly the 20 contacts from tx1; got {}",
        rows.len()
    );

    // t=2 should include the 21st contact that was indexed in the reindex.
    let crawl_t2 = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:2"),
        "where": {"@id": "?s", "@type": "ex:Contact"},
        "select": {"?s": ["*"]},
    });
    let result_t2 = fluree
        .query_from()
        .jsonld(&crawl_t2)
        .format(FormatterConfig::typed_json().with_normalize_arrays())
        .execute_tracked()
        .await
        .expect("expansion at t=2");
    let value_t2 = serde_json::to_value(&result_t2.result).expect("serialize");
    assert_eq!(value_t2.as_array().expect("array").len(), 21);
}

/// Pause one root read after the historical loader has captured its nameservice
/// record, so an index can publish before the binary store is attached (#2265).
#[derive(Debug, Default)]
struct HistoricalRootGate {
    digest: Mutex<Option<String>>,
    parked: Notify,
    release: Notify,
}

#[async_trait]
impl StorageHooks for HistoricalRootGate {
    async fn before_read(&self, address: &str) {
        let pause = {
            let mut digest = self.digest.lock().unwrap();
            if digest.as_ref().is_some_and(|hex| address.contains(hex)) {
                digest.take();
                true
            } else {
                false
            }
        };
        if pause {
            self.parked.notify_one();
            tokio::time::timeout(Duration::from_secs(60), self.release.notified())
                .await
                .expect("release historical root read");
        }
    }
}

fn historical_items(t: i64) -> Vec<serde_json::Value> {
    (0..2)
        .map(|i| {
            let key = format!("item-{t}-{i}");
            json!({
                "@id": format!("ex:{key}"),
                "@type": "ex:Item",
                "ex:group": "g1",
                "ex:key": key,
                "ex:digest": format!("digest-{key}-0123456789abcdef"),
                "ex:doc": format!(r#"{{"key":"{key}","txn":{t}}}"#),
                "ex:version": "v1",
            })
        })
        .collect()
}

async fn assert_historical_publish_keeps_strings(sparql: bool) {
    let storage = HookedStorage::new(HistoricalRootGate::default());
    let fluree = FlureeBuilder::memory().build_with(
        storage.clone(),
        NameServiceMode::ReadWrite(Arc::new(MemoryNameService::new())),
    );
    let ledger_id = "test/historical-publish:main";
    let mut ledger = fluree.create_ledger(ledger_id).await.expect("create");
    ledger = fluree
        .insert(
            ledger,
            &json!({"@context": ctx(), "@graph": historical_items(1)}),
        )
        .await
        .expect("t=1")
        .ledger;
    let old_root = fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("index t=1")
        .root_id;
    for t in [2, 3] {
        ledger = fluree
            .insert(
                ledger,
                &json!({"@context": ctx(), "@graph": historical_items(t)}),
            )
            .await
            .expect("insert")
            .ledger;
        assert_eq!(ledger.t(), t);
    }
    // The head path must be warm before arming: the gate belongs specifically
    // to the historical snapshot load, not the cached handle's initial load.
    let handle = fluree.ledger_cached(ledger_id).await.expect("cached head");
    assert_eq!(handle.t().await, 3);
    let query = json!({
        "@context": ctx(),
        "select": {"?s": ["ex:group", "ex:key", "ex:digest", "ex:doc", "ex:version"]},
        "where": {"@id": "?s", "@type": "ex:Item", "ex:group": "g1"},
        "orderBy": "?s",
    });
    let sparql_query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?group ?key ?digest ?doc ?version WHERE {
            ?s a ex:Item; ex:group ?group; ex:key ?key; ex:digest ?digest;
                ex:doc ?doc; ex:version ?version.
            FILTER (?group = "g1")
        } ORDER BY ?s
    "#;
    // Pin through the public historical loader directly so query preflight
    // (e.g. virtual-dataset probes) cannot consume the one-shot race gate.
    let read = || async {
        let view = fluree.db_at_t(ledger_id, 2).await.expect("historical view");
        let answer = if sparql {
            fluree.query(&view, sparql_query).await
        } else {
            fluree.query(&view, &query).await
        }
        .expect("historical query");
        answer
            .to_jsonld_async(view.as_graph_db_ref())
            .await
            .expect("query JSON")
    };
    // Check against the actual transacted values, not just two reads agreeing.
    let fields = ["ex:group", "ex:key", "ex:digest", "ex:doc", "ex:version"];
    let expected = serde_json::Value::Array(
        (1..=2)
            .flat_map(historical_items)
            .map(|mut item| {
                if sparql {
                    serde_json::Value::Array(fields.iter().map(|key| item[*key].clone()).collect())
                } else {
                    let object = item.as_object_mut().unwrap();
                    object.remove("@id");
                    object.remove("@type");
                    item
                }
            })
            .collect(),
    );
    assert_eq!(read().await, expected, "before publication");

    *storage.hooks().digest.lock().unwrap() = Some(old_root.digest_hex());
    let publish = async {
        tokio::time::timeout(Duration::from_secs(60), storage.hooks().parked.notified())
            .await
            .expect("historical loader must read the old root");
        let new_root = fluree
            .reindex(ledger_id, ReindexOptions::default())
            .await
            .expect("publish index t=3")
            .root_id;
        assert_ne!(new_root, old_root, "index must actually advance");
        let record = fluree
            .nameservice()
            .lookup(ledger_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.index_t, 3);
        assert_eq!(record.index_head_id.as_ref(), Some(&new_root));
        storage.hooks().release.notify_one();
    };
    let (during_publish, ()) = tokio::join!(read(), publish);
    assert_eq!(
        during_publish, expected,
        "publishing an index must not replace historical data strings with commit metadata"
    );
    assert_eq!(read().await, expected, "the next pinned read must agree");
}

#[tokio::test]
async fn historical_publish_keeps_jsonld_strings() {
    assert_historical_publish_keeps_strings(false).await;
}

#[tokio::test]
async fn historical_publish_keeps_sparql_strings() {
    assert_historical_publish_keeps_strings(true).await;
}
