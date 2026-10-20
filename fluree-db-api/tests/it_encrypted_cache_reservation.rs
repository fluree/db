//! How much of the cache budget encrypted file storage keeps reserved for
//! reading dictionaries by range. It starts reserved; data in version 1
//! envelopes, decrypted whole, gives it back, keeping only what covers the
//! version 2 packs opened before it; what is given back is not taken again,
//! so version 2 data opened later reads by range without promotion.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{Fluree, FlureeBuilder};
use fluree_db_core::{StorageRead, StorageWrite};
use serde_json::json;
use std::path::Path;

const KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const CACHE_MB: u64 = 1024;
const BUDGET: u64 = CACHE_MB * 1024 * 1024;
/// The allowance's default share of the budget.
const CAP: u64 = BUDGET * 60 / 100;
/// The fenced-leaf pool: a thirty-second of what the allowance leaves.
const FENCES: u64 = (BUDGET - CAP) / 32;

async fn client(path: &Path) -> Fluree {
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "storage",
                "@type": "Storage",
                "filePath": path.to_string_lossy(),
                "AES256Key": KEY
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "storage"},
                "cacheMaxMb": CACHE_MB,
                "defaults": {"indexing": {"indexingEnabled": false}}
            }
        ]
    });
    FlureeBuilder::from_json_ld(&config)
        .expect("config")
        .build_client()
        .await
        .expect("build_client")
}

fn things(name: &str, ids: std::ops::Range<usize>) -> serde_json::Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": ids.map(|i| json!({
            "@id": format!("ex:{name}-{i}"),
            "@type": "ex:Thing",
            "ex:name": format!("{name} {i}")
        })).collect::<Vec<_>>()
    })
}

async fn seed(fluree: &Fluree, name: &str, n: usize) {
    let ledger = fluree.create_ledger(name).await.expect("create");
    fluree
        .insert(ledger, &things(name, 0..n))
        .await
        .expect("insert");
    support::build_and_publish_index(fluree, &format!("{name}:main")).await;
}

/// Read every thing's name, through the forward dictionaries.
async fn count_things(fluree: &Fluree, name: &str) -> usize {
    let ledger = fluree
        .ledger(&format!("{name}:main"))
        .await
        .expect("ledger");
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s", "?name"],
        "where": {"@id": "?s", "@type": "ex:Thing", "ex:name": "?name"}
    });
    support::query_jsonld(fluree, &ledger, &query)
        .await
        .expect("query")
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld")
        .as_array()
        .map(Vec::len)
        .unwrap_or(0)
}

/// Rewrite every envelope under `root` as version 1, as releases before
/// segmented envelopes wrote them.
async fn rewrite_as_version_1(root: &Path) {
    use fluree_db_crypto::{EncryptedStorage, EncryptionKey, StaticKeyProvider};
    let storage = EncryptedStorage::new(
        fluree_db_core::FileStorage::new(root),
        StaticKeyProvider::new(EncryptionKey::from_base64(KEY, 0).expect("key")),
    )
    .writing_version_1_envelopes();
    for address in storage.list_prefix("fluree:file://").await.expect("list") {
        let Ok(plaintext) = storage.read_bytes(&address).await else {
            continue;
        };
        storage
            .write_bytes(&address, &plaintext)
            .await
            .expect("rewrite");
    }
}

fn reserved(fluree: &Fluree) -> (u64, u64) {
    let cache = fluree.leaflet_cache();
    (
        cache.reserved_bytes(),
        cache.dictionary_allowance().capacity(),
    )
}

/// Version 2 data keeps the whole reservation: its packs and reverse leaves
/// are read by range.
#[tokio::test(flavor = "multi_thread")]
async fn version_2_data_keeps_its_reservation() {
    let data = tempfile::TempDir::new().expect("tempdir");
    seed(&client(data.path()).await, "fresh", 40).await;
    let fluree = client(data.path()).await;
    assert_eq!(reserved(&fluree), (FENCES + CAP, CAP), "reserved up front");
    assert_eq!(count_things(&fluree, "fresh").await, 40);
    assert_eq!(reserved(&fluree), (FENCES + CAP, CAP));
}

/// A store of version 1 envelopes gives the reservation back when it opens
/// a pack, and a version 2 ledger opened after it on the same connection
/// does not take it again: it reads by range, without promotion.
#[tokio::test(flavor = "multi_thread")]
async fn version_1_data_gives_the_reservation_back_for_good() {
    let data = tempfile::TempDir::new().expect("tempdir");
    seed(&client(data.path()).await, "old", 40).await;
    rewrite_as_version_1(data.path()).await;
    seed(&client(data.path()).await, "new", 25).await;

    let fluree = client(data.path()).await;
    assert_eq!(count_things(&fluree, "old").await, 40);
    assert_eq!(reserved(&fluree), (0, 0), "version 1 alone");
    assert_eq!(count_things(&fluree, "new").await, 25);
    assert_eq!(reserved(&fluree), (0, 0), "not taken again");
    assert_eq!(count_things(&fluree, "old").await, 40);
}

/// Version 2 data opened first keeps, once legacy data appears, the
/// smallest part of the allowance covering its packs, not the whole share.
#[tokio::test(flavor = "multi_thread")]
async fn legacy_data_after_version_2_data_keeps_what_its_packs_need() {
    let data = tempfile::TempDir::new().expect("tempdir");
    seed(&client(data.path()).await, "old", 40).await;
    rewrite_as_version_1(data.path()).await;
    seed(&client(data.path()).await, "new", 25).await;

    let fluree = client(data.path()).await;
    assert_eq!(count_things(&fluree, "new").await, 25);
    assert_eq!(reserved(&fluree), (FENCES + CAP, CAP));
    assert_eq!(count_things(&fluree, "old").await, 40);
    let (bytes, allowance) = reserved(&fluree);
    assert!(
        allowance > 0 && allowance <= CAP / 64,
        "a small level: {allowance}"
    );
    assert!(bytes >= allowance && bytes < FENCES + CAP, "{bytes}");
    assert_eq!(count_things(&fluree, "new").await, 25);
}

/// A ledger of version 1 data that takes new writes holds both kinds of
/// pack; reading it gives back the reservation beyond what its version 2
/// packs opened first could need.
#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_ledger_gives_back_what_it_cannot_use() {
    let data = tempfile::TempDir::new().expect("tempdir");
    seed(&client(data.path()).await, "mixed", 40).await;
    rewrite_as_version_1(data.path()).await;
    {
        let fluree = client(data.path()).await;
        assert_eq!(count_things(&fluree, "mixed").await, 40);
        assert_eq!(reserved(&fluree), (0, 0), "before any version 2 write");
        let ledger = fluree.ledger("mixed:main").await.expect("ledger");
        fluree
            .insert(ledger, &things("mixed", 40..50))
            .await
            .expect("insert");
        support::build_and_publish_index(&fluree, "mixed:main").await;
    }
    let fluree = client(data.path()).await;
    assert_eq!(count_things(&fluree, "mixed").await, 50);
    let (bytes, allowance) = reserved(&fluree);
    assert!(allowance <= CAP / 64, "{allowance}");
    assert!(bytes < FENCES + CAP, "{bytes}");
}
