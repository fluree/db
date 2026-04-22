//! IPFS storage backend for Fluree DB.
//!
//! Implements [`ContentStore`] (CID-first) backed by IPFS via the Kubo HTTP
//! RPC API (`/api/v0/block/*`). Use via
//! `StorageBackend::Permanent(Arc::new(IpfsStorage::new(...)))` to integrate
//! with a `Fluree` instance.
//!
//! ## Architecture
//!
//! Fluree's [`ContentId`] is a CIDv1 with SHA2-256 multihash and Fluree-specific
//! multicodec values (private-use range 0x300001–0x30000B). Kubo accepts these
//! custom codecs in `block/put` and resolves blocks by multihash — so Fluree's
//! native CIDs work directly with IPFS, no translation layer needed.
//!
//! Unlike file/S3/memory backends, IPFS does **not** implement the address-based
//! [`Storage`] trait. IPFS is natively content-addressed and cannot meaningfully
//! support arbitrary address-based writes or prefix listing. It is therefore
//! represented as a [`StorageBackend::Permanent`] in the Fluree API.
//!
//! ## Limitations
//!
//! - **No true deletion**: IPFS content is immutable. [`ContentStore::release`]
//!   unpins a block, making it eligible for Kubo's garbage collector — but the
//!   block may persist if reachable from other pins or pinned remotely.
//! - **No prefix listing**: IPFS has no concept of enumeration. Admin
//!   operations that depend on listing (e.g., fast-path ledger drop) fall back
//!   to CID-walking, which is slower but correct.
//! - **No mutable nameservice**: `IpfsStorage` is suitable for storing
//!   content-addressed data (commits, indexes) but not for nameservice records
//!   that require mutable, address-based writes. Pair it with a separate
//!   nameservice (e.g., [`MemoryNameService`] for tests, or a dedicated DB
//!   for production).
//!
//! [`Storage`]: fluree_db_core::Storage
//! [`StorageBackend::Permanent`]: fluree_db_core::StorageBackend::Permanent
//! [`MemoryNameService`]: fluree_db_nameservice::memory::MemoryNameService
//!
//! ## Usage
//!
//! Requires a running Kubo node with the HTTP RPC API enabled (default port 5001).

pub mod address;
pub mod error;
pub mod kubo;

pub use error::{IpfsStorageError, Result};
pub use kubo::KuboClient;

use async_trait::async_trait;
use fluree_db_core::content_id::ContentId;
use fluree_db_core::content_kind::ContentKind;
use fluree_db_core::storage::ContentStore;

/// IPFS storage backend backed by a Kubo node.
///
/// Implements [`ContentStore`] directly — IPFS is natively content-addressed,
/// so operations take `ContentId` rather than address strings. Use via
/// `StorageBackend::Permanent(Arc::new(IpfsStorage::new(...)))` to integrate
/// with a `Fluree` instance.
#[derive(Debug, Clone)]
pub struct IpfsStorage {
    kubo: KuboClient,
    /// If true, pin every block on put. Defaults to true.
    pin_on_put: bool,
}

/// Configuration for `IpfsStorage`.
#[derive(Debug, Clone)]
pub struct IpfsConfig {
    /// Kubo RPC API base URL (e.g., `http://127.0.0.1:5001`).
    pub api_url: String,
    /// Pin blocks on put. Default: true.
    pub pin_on_put: bool,
}

impl Default for IpfsConfig {
    fn default() -> Self {
        Self {
            api_url: "http://127.0.0.1:5001".to_string(),
            pin_on_put: true,
        }
    }
}

impl IpfsStorage {
    /// Create a new IPFS storage backend.
    pub fn new(config: IpfsConfig) -> Self {
        Self {
            kubo: KuboClient::new(config.api_url),
            pin_on_put: config.pin_on_put,
        }
    }

    /// Create from an existing `KuboClient`.
    pub fn from_client(kubo: KuboClient, pin_on_put: bool) -> Self {
        Self { kubo, pin_on_put }
    }

    /// Check if the backing Kubo node is reachable.
    pub async fn is_available(&self) -> bool {
        self.kubo.is_available().await
    }

    /// Access the underlying Kubo client.
    pub fn kubo(&self) -> &KuboClient {
        &self.kubo
    }

    /// Format the Fluree multicodec as a hex string for Kubo's `cid-codec` parameter.
    fn codec_hex(kind: ContentKind) -> String {
        format!("0x{:x}", kind.to_codec())
    }

    /// Pin a block if pin_on_put is enabled.
    ///
    /// Converts the CID to raw codec (0x55) before pinning. Kubo rejects
    /// `pin/add` for CIDs with unregistered codecs (like Fluree's private-use
    /// range) because it tries to decode the block for DAG traversal. Using
    /// raw codec avoids this — Kubo treats the block as opaque bytes and pins
    /// it by multihash, which is all we need.
    async fn maybe_pin(&self, cid: &str) {
        if self.pin_on_put {
            let pin_cid = match Self::to_raw_cid(cid) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(cid = %cid, error = %e, "failed to convert CID for pinning");
                    return;
                }
            };
            if let Err(e) = self.kubo.pin_add(&pin_cid).await {
                tracing::warn!(cid = %cid, raw_cid = %pin_cid, error = %e, "failed to pin block");
            }
        }
    }

    /// Convert any CID string to its raw-codec (0x55) equivalent with the same
    /// multihash. Used for pin operations since Kubo can't pin custom codecs.
    fn to_raw_cid(cid_str: &str) -> std::result::Result<String, String> {
        let cid = cid::Cid::try_from(cid_str).map_err(|e| format!("invalid CID: {e}"))?;
        let raw = cid::Cid::new_v1(0x55, *cid.hash());
        Ok(raw.to_string())
    }
}

// ============================================================================
// ContentStore (CID-first interface)
// ============================================================================

#[async_trait]
impl ContentStore for IpfsStorage {
    async fn has(&self, id: &ContentId) -> fluree_db_core::error::Result<bool> {
        let cid_str = id.to_string();
        match self.kubo.block_stat(&cid_str).await {
            Ok(_) => Ok(true),
            Err(IpfsStorageError::NotFound(_)) => Ok(false),
            Err(e) => Err(fluree_db_core::error::Error::storage(e.to_string())),
        }
    }

    async fn get(&self, id: &ContentId) -> fluree_db_core::error::Result<Vec<u8>> {
        let cid_str = id.to_string();
        self.kubo
            .block_get(&cid_str)
            .await
            .map_err(fluree_db_core::error::Error::from)
    }

    async fn put(
        &self,
        kind: ContentKind,
        bytes: &[u8],
    ) -> fluree_db_core::error::Result<ContentId> {
        let expected_id = ContentId::new(kind, bytes);
        let codec = Self::codec_hex(kind);

        let response = self
            .kubo
            .block_put(bytes, Some(&codec), Some("sha2-256"))
            .await
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;

        tracing::debug!(
            fluree_cid = %expected_id,
            ipfs_cid = %response.key,
            size = response.size,
            "block put to IPFS"
        );

        self.maybe_pin(&response.key).await;
        Ok(expected_id)
    }

    async fn release(&self, id: &ContentId) -> fluree_db_core::error::Result<()> {
        let cid_str = id.to_string();
        let raw_cid = Self::to_raw_cid(&cid_str).map_err(fluree_db_core::error::Error::storage)?;
        self.kubo
            .pin_rm(&raw_cid)
            .await
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::error::Result<()> {
        if !id.verify(bytes) {
            return Err(fluree_db_core::error::Error::storage(format!(
                "CID verification failed: provided CID {id} does not match bytes"
            )));
        }

        // Use the codec from the CID itself
        let codec = format!("0x{:x}", id.codec());

        let response = self
            .kubo
            .block_put(bytes, Some(&codec), Some("sha2-256"))
            .await
            .map_err(|e| fluree_db_core::error::Error::storage(e.to_string()))?;

        tracing::debug!(
            fluree_cid = %id,
            ipfs_cid = %response.key,
            size = response.size,
            "block put_with_id to IPFS"
        );

        self.maybe_pin(&response.key).await;
        Ok(())
    }
}
