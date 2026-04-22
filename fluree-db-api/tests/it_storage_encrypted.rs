//! Integration tests for encrypted storage
//!
//! Tests that FlureeBuilder::build_memory_encrypted() works correctly
//! with the full Fluree API.

mod support;

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
