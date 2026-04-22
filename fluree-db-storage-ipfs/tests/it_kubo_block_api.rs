//! Integration tests for IPFS block storage via Kubo HTTP RPC.
//!
//! These tests require a running Kubo node at `http://127.0.0.1:5001`.
//! Run with: `cargo test -p fluree-db-storage-ipfs --features integration-tests`
//!
//! To start a Kubo node:
//!   ipfs daemon
//!
//! Or via Docker:
//!   docker run -d --name ipfs -p 5001:5001 ipfs/kubo:latest

#![cfg(feature = "integration-tests")]

use fluree_db_core::content_id::ContentId;
use fluree_db_core::content_kind::ContentKind;
use fluree_db_core::storage::ContentStore;
use fluree_db_storage_ipfs::{IpfsConfig, IpfsStorage, KuboClient};

const KUBO_URL: &str = "http://127.0.0.1:5001";

/// Helper: skip test if Kubo is not available.
///
/// Returns `None` when no Kubo daemon is reachable, allowing tests to
/// silently return early (same pattern as `it_file_backed.rs`).
async fn require_kubo() -> Option<KuboClient> {
    let client = KuboClient::new(KUBO_URL);
    if !client.is_available().await {
        eprintln!(
            "Kubo node not available at {KUBO_URL} — skipping test. \
             Start with `ipfs daemon` or \
             `docker run -d -p 5001:5001 ipfs/kubo:latest`"
        );
        return None;
    }
    Some(client)
}

// ============================================================================
// 1. Basic block put/get with default (raw) codec
// ============================================================================

#[tokio::test]
async fn test_block_put_get_raw_codec() {
    let Some(kubo) = require_kubo().await else {
        return;
    };
    let data = b"hello fluree ipfs test";

    // Put with default raw codec
    let put_resp = kubo.block_put(data, None, Some("sha2-256")).await.unwrap();
    println!("Raw codec put → CID: {}", put_resp.key);
    assert!(put_resp.size > 0);

    // Get it back
    let retrieved = kubo.block_get(&put_resp.key).await.unwrap();
    assert_eq!(retrieved, data);
}

// ============================================================================
// 2. Can Kubo accept a block with a numeric custom codec?
// ============================================================================

#[tokio::test]
async fn test_block_put_custom_codec_numeric() {
    let Some(kubo) = require_kubo().await else {
        return;
    };
    let data = b"fluree commit test payload";

    // Try putting with Fluree's commit codec (0x300001) as a numeric string.
    // Kubo's `cid-codec` parameter accepts multicodec *names* — let's see
    // if it also handles numeric values or if it rejects them.
    let result = kubo
        .block_put(data, Some("0x300001"), Some("sha2-256"))
        .await;

    match result {
        Ok(resp) => {
            println!("Custom numeric codec ACCEPTED → CID: {}", resp.key);
            // Verify we can get it back
            let retrieved = kubo.block_get(&resp.key).await.unwrap();
            assert_eq!(retrieved, data);
        }
        Err(e) => {
            println!("Custom numeric codec REJECTED: {e}");
            println!("This is expected — Kubo only accepts named multicodec strings.");
            println!("We'll use 'raw' codec for storage and our own CID for identity.");
        }
    }
}

// ============================================================================
// 3. Core question: can we PUT with raw codec and GET with Fluree's CID?
//
// IPFS block storage is keyed by multihash internally. If we put a block
// with raw codec (0x55) and then ask for it using a CID with Fluree's
// custom codec (0x300001) but the SAME multihash, does Kubo resolve it?
// ============================================================================

#[tokio::test]
async fn test_cross_codec_retrieval() {
    let Some(kubo) = require_kubo().await else {
        return;
    };
    let data = b"cross-codec retrieval test";

    // 1. Compute Fluree's CID (custom codec + SHA256)
    let fluree_cid = ContentId::new(ContentKind::Commit, data);
    println!("Fluree CID (codec 0x300001): {fluree_cid}");

    // 2. Put with raw codec — this should always work
    let put_resp = kubo.block_put(data, None, Some("sha2-256")).await.unwrap();
    println!("IPFS raw CID:               {}", put_resp.key);

    // 3. Try to GET using Fluree's CID string
    let result = kubo.block_get(&fluree_cid.to_string()).await;
    match result {
        Ok(retrieved) => {
            assert_eq!(retrieved, data);
            println!("CROSS-CODEC RETRIEVAL WORKS! Kubo resolves by multihash, not full CID.");
            println!("This means we can use Fluree's native CIDs with IPFS storage.");
        }
        Err(e) => {
            println!("CROSS-CODEC RETRIEVAL FAILED: {e}");
            println!("Kubo requires exact CID match (including codec).");
            println!("We'll need to maintain a CID mapping: fluree_cid <-> raw_cid");

            // Verify the block IS there with the raw CID
            let raw_data = kubo.block_get(&put_resp.key).await.unwrap();
            assert_eq!(raw_data, data);
            println!("Block is accessible with raw CID: {}", put_resp.key);
        }
    }
}

// ============================================================================
// 4. ContentStore trait round-trip through IpfsStorage
// ============================================================================

#[tokio::test]
async fn test_content_store_roundtrip() {
    let store = IpfsStorage::new(IpfsConfig {
        api_url: KUBO_URL.to_string(),
        pin_on_put: false, // Skip pinning for test speed
    });

    if !store.is_available().await {
        eprintln!("Kubo not available at {KUBO_URL} — skipping test");
        return;
    }

    let test_data = b"ContentStore round-trip test payload";

    // Put via ContentStore trait
    let cid = store.put(ContentKind::Commit, test_data).await.unwrap();
    println!("ContentStore put → Fluree CID: {cid}");

    // Verify the CID matches what we'd compute locally
    let expected_cid = ContentId::new(ContentKind::Commit, test_data);
    assert_eq!(cid, expected_cid);

    // Check has()
    // Note: this may fail if Kubo can't resolve by Fluree's CID.
    // We'll test and report either way.
    match store.has(&cid).await {
        Ok(true) => println!("has() with Fluree CID: true"),
        Ok(false) => {
            println!("has() with Fluree CID: false (Kubo can't resolve custom codec CIDs)");
        }
        Err(e) => println!("has() with Fluree CID: error — {e}"),
    }

    // Try get()
    match store.get(&cid).await {
        Ok(retrieved) => {
            assert_eq!(retrieved, test_data);
            println!("get() with Fluree CID: SUCCESS — bytes match!");
        }
        Err(e) => {
            println!("get() with Fluree CID: FAILED — {e}");
            println!("We need a CID translation layer (fluree CID ↔ raw CID).");
        }
    }
}

// ============================================================================
// 5. Test multiple content kinds
// ============================================================================

#[tokio::test]
async fn test_multiple_content_kinds() {
    let Some(kubo) = require_kubo().await else {
        return;
    };

    let kinds = [
        ("Commit", ContentKind::Commit, 0x0030_0001_u64),
        ("Txn", ContentKind::Txn, 0x0030_0002),
        ("IndexRoot", ContentKind::IndexRoot, 0x0030_0003),
        ("IndexBranch", ContentKind::IndexBranch, 0x0030_0004),
        ("IndexLeaf", ContentKind::IndexLeaf, 0x0030_0005),
        ("LedgerConfig", ContentKind::LedgerConfig, 0x0030_0008),
    ];

    println!("\n=== Testing IPFS block storage with Fluree content kinds ===\n");
    println!(
        "{:<15} {:<10} {:<60} Cross-codec?",
        "Kind", "Codec", "Fluree CID"
    );
    println!("{}", "-".repeat(100));

    for (name, kind, codec) in kinds {
        let data = format!("test payload for {name}");
        let fluree_cid = ContentId::new(kind, data.as_bytes());

        // Put with raw codec
        let put_resp = kubo
            .block_put(data.as_bytes(), None, Some("sha2-256"))
            .await
            .unwrap();

        // Try cross-codec retrieval
        let cross_codec = match kubo.block_get(&fluree_cid.to_string()).await {
            Ok(bytes) if bytes == data.as_bytes() => "YES",
            Ok(_) => "CORRUPTED",
            Err(_) => "NO",
        };

        println!(
            "{:<15} 0x{:06x}  {:<60} {}",
            name,
            codec,
            fluree_cid.to_string(),
            cross_codec
        );

        // Verify raw CID always works
        let raw_data = kubo.block_get(&put_resp.key).await.unwrap();
        assert_eq!(raw_data, data.as_bytes());
    }
}

// ============================================================================
// 6. Test put_with_id (import/replication path)
// ============================================================================

#[tokio::test]
async fn test_put_with_id() {
    let store = IpfsStorage::new(IpfsConfig {
        api_url: KUBO_URL.to_string(),
        pin_on_put: false,
    });

    if !store.is_available().await {
        eprintln!("Kubo not available at {KUBO_URL} — skipping test");
        return;
    }

    let data = b"put_with_id verification test";
    let id = ContentId::new(ContentKind::Txn, data);

    // Should succeed — CID matches bytes
    store.put_with_id(&id, data).await.unwrap();
    println!("put_with_id succeeded for CID: {id}");

    // Should fail — CID doesn't match bytes
    let wrong_data = b"this data doesn't match the CID";
    let result = store.put_with_id(&id, wrong_data).await;
    assert!(
        result.is_err(),
        "put_with_id should reject mismatched bytes"
    );
    println!("put_with_id correctly rejected mismatched bytes");
}

// ============================================================================
// 7. Pin lifecycle with raw-codec CIDs
//
// Kubo rejects pin/add for CIDs with unregistered codecs ("no decoder
// registered for multicodec code 0x300001"). So IpfsStorage pins blocks
// using the raw-codec (0x55) CID derived from the same multihash.
//
// This test verifies the full lifecycle that IpfsStorage actually uses:
//   1. block_put with Fluree custom codec (stores the block)
//   2. pin_add with raw-codec CID (pins the block without decode)
//   3. pin_rm with raw-codec CID (unpins cleanly)
//
// It also verifies that pin/add with a Fluree-codec CID is rejected,
// confirming that the raw-codec pin strategy is necessary.
// ============================================================================

#[tokio::test]
async fn test_pin_lifecycle_raw_codec() {
    let Some(kubo) = require_kubo().await else {
        return;
    };
    let data = b"pin lifecycle test payload";

    // 1. Put with Fluree custom codec
    let fluree_cid = ContentId::new(ContentKind::Commit, data);
    let codec_hex = format!("0x{:x}", ContentKind::Commit.to_codec());
    let put_resp = kubo
        .block_put(data, Some(&codec_hex), Some("sha2-256"))
        .await
        .unwrap();
    let fluree_cid_str = put_resp.key.clone();
    println!("Fluree-codec CID (from put): {fluree_cid_str}");

    // 2. Verify that pinning with Fluree-codec CID FAILS
    let pin_fluree_result = kubo.pin_add(&fluree_cid_str).await;
    assert!(
        pin_fluree_result.is_err(),
        "pin/add should reject Fluree-codec CIDs (no decoder registered)"
    );
    println!(
        "pin/add with Fluree CID correctly rejected: {}",
        pin_fluree_result.unwrap_err()
    );

    // 3. Construct raw-codec CID (same multihash, codec 0x55)
    let raw_cid_str =
        fluree_db_storage_ipfs::address::hash_hex_to_cid_string(&fluree_cid.digest_hex()).unwrap();
    println!("Raw-codec CID (for pin):     {raw_cid_str}");

    // 4. Pin with raw-codec CID — this is what IpfsStorage::maybe_pin does
    kubo.pin_add(&raw_cid_str).await.unwrap();
    assert!(
        kubo.is_pinned(&raw_cid_str).await.unwrap(),
        "block should be pinned with raw-codec CID"
    );
    println!("pin/add with raw CID succeeded");

    // 5. Unpin with raw-codec CID — this is what IpfsStorage::delete does
    kubo.pin_rm(&raw_cid_str).await.unwrap();
    assert!(
        !kubo.is_pinned(&raw_cid_str).await.unwrap(),
        "block should be unpinned after pin/rm"
    );
    println!("pin/rm with raw CID succeeded — block is unpinned");

    // 6. Verify the block data is still accessible (just unpinned, not deleted)
    let retrieved = kubo.block_get(&fluree_cid_str).await.unwrap();
    assert_eq!(retrieved, data);
    println!("Block data still accessible after unpin (until GC)");
}

// ============================================================================
// 8. Verify SHA-256 digest matches between Fluree and IPFS
// ============================================================================

#[tokio::test]
async fn test_sha256_digest_matches() {
    let Some(kubo) = require_kubo().await else {
        return;
    };
    let data = b"SHA-256 interop verification";

    // Compute locally
    let fluree_cid = ContentId::new(ContentKind::Commit, data);
    let fluree_digest = fluree_cid.digest_hex();

    // Put to IPFS and inspect the returned CID
    let put_resp = kubo.block_put(data, None, Some("sha2-256")).await.unwrap();

    // Parse the IPFS CID to extract its digest
    let ipfs_cid: ContentId = put_resp
        .key
        .parse()
        .expect("IPFS CID should be parseable as ContentId");
    let ipfs_digest = ipfs_cid.digest_hex();

    println!("Fluree digest: {fluree_digest}");
    println!("IPFS digest:   {ipfs_digest}");

    assert_eq!(
        fluree_digest, ipfs_digest,
        "SHA-256 digests must match between Fluree and IPFS"
    );
    println!("SHA-256 digests match!");
}
