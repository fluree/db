//! Integration tests for encrypted storage
//!
//! Tests that FlureeBuilder::build_memory_encrypted() works correctly
//! with the full Fluree API.

use crate::support;
use fluree_db_api::FlureeBuilder;
use fluree_db_core::prelude::*; // For storage traits
use serde_json::json;

/// Test that we can create an encrypted memory instance and perform basic operations
#[tokio::test]
async fn test_encrypted_memory_create_and_query() {
    // 32-byte test key (in production, use a secure key)
    let key: [u8; 32] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ];

    let fluree = FlureeBuilder::new().build_memory_encrypted(key);

    // Create a ledger
    let ledger = fluree
        .create_ledger("test/encrypted")
        .await
        .expect("Failed to create ledger");

    assert_eq!(ledger.ledger_id(), "test/encrypted:main");

    // Insert some data
    let txn = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:alice",
        "ex:name": "Alice",
        "ex:age": 30
    });

    let result = fluree.insert(ledger, &txn).await;
    assert!(result.is_ok(), "Insert should succeed: {:?}", result.err());
    let ledger = result.unwrap().ledger;

    // Query the data back
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "ex:alice"},
        "select": {"ex:alice": ["*"]}
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("Query should succeed");

    // Convert to JSON-LD for easy inspection
    let jsonld = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .unwrap();

    // Should find Alice - jsonld is a JSON array
    let jsonld_array = jsonld.as_array().expect("Should be array");
    assert_eq!(jsonld_array.len(), 1, "Should find one result");

    let alice = &jsonld_array[0];
    assert_eq!(alice["ex:name"], json!("Alice"));
    assert_eq!(alice["ex:age"], json!(30));
}

/// Test that encrypted and non-encrypted storage are isolated
/// (data encrypted with one key can't be read without the key)
#[tokio::test]
async fn test_encrypted_data_requires_key() {
    use fluree_db_api::{EncryptedStorage, EncryptionKey, StaticKeyProvider};
    use fluree_db_core::prelude::*;

    let key: [u8; 32] = [0x42; 32];

    // Create encrypted storage and write data
    let storage = MemoryStorage::new();
    let encryption_key = EncryptionKey::new(key, 0);
    let key_provider = StaticKeyProvider::new(encryption_key);
    let encrypted = EncryptedStorage::new(storage.clone(), key_provider);

    let plaintext = b"sensitive data";
    encrypted.write_bytes("test/data", plaintext).await.unwrap();

    // Raw storage should have encrypted (different) bytes
    let raw_bytes = storage.read_bytes("test/data").await.unwrap();
    assert_ne!(
        raw_bytes.as_slice(),
        plaintext,
        "Raw bytes should be encrypted"
    );

    // Encrypted storage should decrypt correctly
    let decrypted = encrypted.read_bytes("test/data").await.unwrap();
    assert_eq!(
        decrypted.as_slice(),
        plaintext,
        "Should decrypt to original"
    );
}

/// Test that the encryption envelope is portable (magic bytes present)
#[tokio::test]
async fn test_encryption_envelope_format() {
    use fluree_db_api::{EncryptedStorage, EncryptionKey, StaticKeyProvider};
    use fluree_db_core::prelude::*;

    let key: [u8; 32] = [0x42; 32];

    let storage = MemoryStorage::new();
    let encryption_key = EncryptionKey::new(key, 0);
    let key_provider = StaticKeyProvider::new(encryption_key);
    let encrypted = EncryptedStorage::new(storage.clone(), key_provider);

    encrypted.write_bytes("test/data", b"hello").await.unwrap();

    // Check envelope format: magic bytes "FLU\0"
    let raw_bytes = storage.read_bytes("test/data").await.unwrap();
    assert!(raw_bytes.len() >= 22 + 16, "Should have header + tag");
    assert_eq!(&raw_bytes[0..4], b"FLU\x00", "Should have magic bytes");
    assert_eq!(raw_bytes[4], 0x01, "Version should be 1");
    assert_eq!(raw_bytes[5], 0x01, "Algorithm should be AES-256-GCM (1)");
}

/// Test that different keys produce different ciphertext
#[tokio::test]
async fn test_different_keys_different_ciphertext() {
    use fluree_db_api::{EncryptedStorage, EncryptionKey, StaticKeyProvider};

    let key1: [u8; 32] = [0x01; 32];
    let key2: [u8; 32] = [0x02; 32];
    let plaintext = b"same plaintext";

    // Encrypt with key1
    let storage1 = MemoryStorage::new();
    let enc1 = EncryptedStorage::new(
        storage1.clone(),
        StaticKeyProvider::new(EncryptionKey::new(key1, 0)),
    );
    enc1.write_bytes("data", plaintext).await.unwrap();

    // Encrypt with key2
    let storage2 = MemoryStorage::new();
    let enc2 = EncryptedStorage::new(
        storage2.clone(),
        StaticKeyProvider::new(EncryptionKey::new(key2, 0)),
    );
    enc2.write_bytes("data", plaintext).await.unwrap();

    // Ciphertexts should be different (different keys + random nonces)
    let ct1 = storage1.read_bytes("data").await.unwrap();
    let ct2 = storage2.read_bytes("data").await.unwrap();
    assert_ne!(
        ct1, ct2,
        "Different keys should produce different ciphertext"
    );
}

/// Test FlureeBuilder with base64 encryption key
#[tokio::test]
async fn test_builder_with_base64_key() {
    use fluree_db_api::FlureeBuilder;

    // Base64-encoded 32-byte key (all zeros for testing)
    let base64_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    let builder = FlureeBuilder::new()
        .with_encryption_key_base64(base64_key)
        .expect("Should parse valid base64 key");

    assert!(
        builder.has_encryption_key(),
        "Builder should have encryption key"
    );
}

/// Test FlureeBuilder rejects invalid base64 keys
#[tokio::test]
async fn test_builder_rejects_invalid_base64_key() {
    use fluree_db_api::FlureeBuilder;

    // Invalid base64
    let result = FlureeBuilder::new().with_encryption_key_base64("not-valid-base64!!!");
    assert!(result.is_err(), "Should reject invalid base64");

    // Valid base64 but wrong length (16 bytes instead of 32)
    let short_key = "AAAAAAAAAAAAAAAAAAAAAA=="; // 16 bytes
    let result = FlureeBuilder::new().with_encryption_key_base64(short_key);
    assert!(result.is_err(), "Should reject key that's not 32 bytes");
    assert!(result.unwrap_err().to_string().contains("32 bytes"));
}

// ============================================================================
// Coverage: a configured key is honoured by every build path, and encryption
// at rest leaves no plaintext copy in the binary-index disk cache.
// ============================================================================

const KEY_B64: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const ENVELOPE_MAGIC: &[u8] = b"FLU\x00";

fn regular_files_under(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

fn jsonld_storage_config(mut storage: serde_json::Value) -> serde_json::Value {
    storage["@id"] = json!("storage");
    json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            storage,
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "storage"}
            }
        ]
    })
}

fn cache_config(cache_dir: &std::path::Path) -> fluree_db_api::LedgerManagerConfig {
    fluree_db_api::LedgerManagerConfig {
        cache_dir: cache_dir.to_path_buf(),
        ..Default::default()
    }
}

/// Create a ledger, insert enough subjects for a real index, build and
/// publish that index with the indexer's artifact cache under `data_dir`,
/// then reload and query through the index so leaves are read.
async fn seed_index_and_query(
    fluree: &fluree_db_api::Fluree,
    ledger_name: &str,
    data_dir: &std::path::Path,
) {
    let ledger_id = format!("{ledger_name}:main");
    let ledger = fluree.create_ledger(ledger_name).await.expect("create");
    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": (0..120).map(|i| json!({
            "@id": format!("ex:person{i}"),
            "@type": "ex:Person",
            "ex:name": format!("Person {i}")
        })).collect::<Vec<_>>()
    });
    fluree.insert(ledger, &tx).await.expect("insert");

    let record = fluree
        .nameservice()
        .lookup(&ledger_id)
        .await
        .expect("lookup")
        .expect("record");
    let result = fluree_db_indexer::build_index_for_record(
        fluree.content_store(&ledger_id),
        &record,
        fluree_db_indexer::IndexerConfig::default().with_data_dir(data_dir),
    )
    .await
    .expect("index build");
    fluree
        .publisher()
        .expect("read-write nameservice")
        .publish_index(&ledger_id, result.index_t, &result.root_id)
        .await
        .expect("publish index");

    let indexed = fluree.ledger(&ledger_id).await.expect("reload");
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s"],
        "where": {"@id": "?s", "@type": "ex:Person"}
    });
    let rows = support::query_jsonld(fluree, &indexed, &query)
        .await
        .expect("query")
        .to_jsonld_async(indexed.as_graph_db_ref(0))
        .await
        .expect("to_jsonld");
    assert_eq!(rows.as_array().map(Vec::len), Some(120));
}

/// `build_client()` on file storage with `AES256Key` in JSON-LD: every
/// commit and index blob at rest carries the encryption envelope, and
/// neither the reader's nor the indexer's disk cache holds a plaintext
/// copy of any of them.
#[tokio::test]
async fn build_client_file_honours_key_and_spills_no_plaintext() {
    let data = tempfile::TempDir::new().expect("tempdir");
    let cache = tempfile::TempDir::new().expect("tempdir");
    let config = jsonld_storage_config(json!({
        "@type": "Storage",
        "filePath": data.path().to_string_lossy(),
        "AES256Key": KEY_B64
    }));
    let builder = FlureeBuilder::from_json_ld(&config).expect("config");
    assert!(builder.has_encryption_key());
    let fluree = builder
        .with_ledger_cache_config(cache_config(cache.path()))
        .build_client()
        .await
        .expect("build_client");

    seed_index_and_query(&fluree, "enc-file", data.path()).await;

    let indexer_cache = data.path().join("binary_artifact_cache");
    let mut blobs = 0;
    for path in regular_files_under(data.path()) {
        let rel = path.strip_prefix(data.path()).unwrap();
        let first = rel
            .components()
            .next()
            .unwrap()
            .as_os_str()
            .to_string_lossy();
        // The file nameservice is documented plaintext; the WAL and the
        // indexer's cache are not content blobs.
        if first.starts_with("ns@") || first.starts_with('.') || path.starts_with(&indexer_cache) {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        assert!(
            bytes.starts_with(ENVELOPE_MAGIC),
            "plaintext blob at rest: {}",
            rel.display()
        );
        blobs += 1;
    }
    assert!(blobs > 1, "expected commit and index blobs on disk");

    assert!(
        regular_files_under(cache.path()).is_empty(),
        "reader disk cache holds artifacts: {:?}",
        regular_files_under(cache.path())
    );
    assert!(
        regular_files_under(&indexer_cache).is_empty(),
        "indexer artifact cache holds artifacts: {:?}",
        regular_files_under(&indexer_cache)
    );
}

/// Memory storage has no local path, so its readers go through the disk
/// cache: an unencrypted client populates it (which is what makes the
/// encrypted assertion non-vacuous), an encrypted one leaves it empty.
#[tokio::test]
async fn build_client_memory_honours_key_and_bypasses_disk_cache() {
    async fn cached_files_for(key: Option<&str>) -> Vec<std::path::PathBuf> {
        let data = tempfile::TempDir::new().expect("tempdir");
        let cache = tempfile::TempDir::new().expect("tempdir");
        let mut storage = json!({"@type": "Storage"});
        if let Some(key) = key {
            storage["AES256Key"] = json!(key);
        }
        let builder = FlureeBuilder::from_json_ld(&jsonld_storage_config(storage)).expect("config");
        assert_eq!(builder.has_encryption_key(), key.is_some());
        let fluree = builder
            .with_ledger_cache_config(cache_config(cache.path()))
            .build_client()
            .await
            .expect("build_client");
        seed_index_and_query(&fluree, "enc-mem", data.path()).await;
        let mut files = regular_files_under(cache.path());
        files.extend(regular_files_under(
            &data.path().join("binary_artifact_cache"),
        ));
        files
    }

    let plain = cached_files_for(None).await;
    assert!(
        !plain.is_empty(),
        "unencrypted reads should populate the disk cache"
    );

    let encrypted = cached_files_for(Some(KEY_B64)).await;
    assert!(
        encrypted.is_empty(),
        "encrypted reads spilled plaintext to the disk cache: {encrypted:?}"
    );
}

/// The S3 branch of the JSON-LD parser carries `AES256Key` like the file
/// branch does; the builder sees the key before any AWS client exists.
#[tokio::test]
async fn from_json_ld_s3_config_carries_encryption_key() {
    let config = jsonld_storage_config(json!({
        "@type": "Storage",
        "s3Bucket": "my-bucket",
        "AES256Key": KEY_B64
    }));
    let builder = FlureeBuilder::from_json_ld(&config).expect("config");
    assert!(builder.has_encryption_key());
}
