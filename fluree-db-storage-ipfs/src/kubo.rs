//! Thin client for the Kubo IPFS HTTP RPC API.
//!
//! We use `reqwest` directly rather than the `ipfs-api` crate because:
//! 1. We need fine-grained control over `cid-codec` parameters (custom codecs)
//! 2. The API surface we need is tiny (block/put, block/get, block/stat)
//! 3. Fewer transitive dependencies

use crate::error::{IpfsStorageError, Result};
use reqwest::multipart;

/// Response from `block/put`.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BlockPutResponse {
    /// CID string that Kubo assigned to the block.
    pub key: String,
    /// Size in bytes.
    pub size: u64,
}

/// Response from `block/stat`.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BlockStatResponse {
    /// CID of the block.
    pub key: String,
    /// Size in bytes.
    pub size: u64,
}

/// Kubo IPFS HTTP RPC client.
#[derive(Debug, Clone)]
pub struct KuboClient {
    client: reqwest::Client,
    /// Base URL of the Kubo RPC API, e.g. `http://127.0.0.1:5001`.
    base_url: String,
}

impl KuboClient {
    /// Create a new client pointing at a Kubo RPC endpoint.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    /// Create a client with a custom `reqwest::Client` (for timeouts, etc.).
    pub fn with_client(client: reqwest::Client, base_url: impl Into<String>) -> Self {
        Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    /// Check if the Kubo node is reachable.
    pub async fn is_available(&self) -> bool {
        let url = format!("{}/api/v0/id", self.base_url);
        self.client.post(&url).send().await.is_ok()
    }

    /// `POST /api/v0/block/put` — store a raw block.
    ///
    /// # Parameters
    ///
    /// - `data`: raw block bytes
    /// - `cid_codec`: multicodec name OR numeric string for the CID codec.
    ///   Use `None` for Kubo's default (`raw`, codec 0x55).
    /// - `mhtype`: multihash type name. Use `None` for default (`sha2-256`).
    pub async fn block_put(
        &self,
        data: &[u8],
        cid_codec: Option<&str>,
        mhtype: Option<&str>,
    ) -> Result<BlockPutResponse> {
        let mut url = format!("{}/api/v0/block/put", self.base_url);

        // Build query parameters
        let mut params = Vec::new();
        if let Some(codec) = cid_codec {
            params.push(format!("cid-codec={codec}"));
        }
        if let Some(mh) = mhtype {
            params.push(format!("mhtype={mh}"));
        }
        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }

        let part = multipart::Part::bytes(data.to_vec()).file_name("data");
        let form = multipart::Form::new().part("data", part);

        let response = self
            .client
            .post(&url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| IpfsStorageError::ConnectionFailed(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            return Err(IpfsStorageError::Rpc(format!(
                "block/put failed ({status}): {body}"
            )));
        }

        response
            .json::<BlockPutResponse>()
            .await
            .map_err(|e| IpfsStorageError::Rpc(format!("failed to parse block/put response: {e}")))
    }

    /// `POST /api/v0/block/get` — retrieve a block by CID.
    pub async fn block_get(&self, cid: &str) -> Result<Vec<u8>> {
        let url = format!("{}/api/v0/block/get?arg={}", self.base_url, cid);

        let response = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| IpfsStorageError::ConnectionFailed(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            if status.as_u16() == 500 && body.contains("block was not found") {
                return Err(IpfsStorageError::NotFound(cid.to_string()));
            }
            return Err(IpfsStorageError::Rpc(format!(
                "block/get failed ({status}): {body}"
            )));
        }

        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| IpfsStorageError::Rpc(format!("failed to read block/get body: {e}")))
    }

    /// `POST /api/v0/block/stat` — get block metadata without downloading.
    pub async fn block_stat(&self, cid: &str) -> Result<BlockStatResponse> {
        let url = format!("{}/api/v0/block/stat?arg={}", self.base_url, cid);

        let response = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| IpfsStorageError::ConnectionFailed(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            if status.as_u16() == 500 && body.contains("block was not found") {
                return Err(IpfsStorageError::NotFound(cid.to_string()));
            }
            return Err(IpfsStorageError::Rpc(format!(
                "block/stat failed ({status}): {body}"
            )));
        }

        response
            .json::<BlockStatResponse>()
            .await
            .map_err(|e| IpfsStorageError::Rpc(format!("failed to parse block/stat response: {e}")))
    }

    /// `POST /api/v0/pin/add` — pin a block by CID.
    ///
    /// **Important**: Kubo rejects `pin/add` for CIDs with unregistered codecs
    /// (like Fluree's private-use range 0x300001–0x30000B) because it tries
    /// to decode the block for DAG traversal. Callers must pass a raw-codec
    /// (0x55) CID with the same multihash — see `IpfsStorage::to_raw_cid()`.
    pub async fn pin_add(&self, cid: &str) -> Result<()> {
        let url = format!("{}/api/v0/pin/add?arg={}", self.base_url, cid);

        let response = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| IpfsStorageError::ConnectionFailed(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            return Err(IpfsStorageError::Rpc(format!(
                "pin/add failed ({status}): {body}"
            )));
        }

        // Consume response body
        let _ = response.bytes().await;
        Ok(())
    }

    /// `POST /api/v0/pin/ls?arg={cid}` — check if a specific CID is pinned.
    ///
    /// Returns `true` if the CID is directly or recursively pinned, `false` otherwise.
    pub async fn is_pinned(&self, cid: &str) -> Result<bool> {
        let url = format!("{}/api/v0/pin/ls?arg={}", self.base_url, cid);

        let response = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| IpfsStorageError::ConnectionFailed(e.to_string()))?;

        if response.status().is_success() {
            // Consume response body
            let _ = response.bytes().await;
            return Ok(true);
        }

        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "unknown".to_string());

        // Kubo returns 500 with "is not pinned" when the CID isn't pinned
        if body.contains("is not pinned") {
            return Ok(false);
        }

        Err(IpfsStorageError::Rpc(format!("pin/ls failed: {body}")))
    }

    /// `POST /api/v0/pin/rm` — unpin a block by CID.
    ///
    /// Idempotent for drop/GC use: returns `Ok(())` if Kubo reports the CID is
    /// "not pinned" or the block is "not found" in the blockstore. Other errors
    /// (connection failures, RPC errors) are propagated.
    pub async fn pin_rm(&self, cid: &str) -> Result<()> {
        let url = format!("{}/api/v0/pin/rm?arg={}", self.base_url, cid);

        let response = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| IpfsStorageError::ConnectionFailed(e.to_string()))?;

        if !response.status().is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            // Tolerate "not pinned" (already unpinned) and "not found" (block
            // missing from blockstore) — both are expected during idempotent
            // drop/GC operations.
            if body.contains("not pinned") || body.contains("not found") {
                return Ok(());
            }
            return Err(IpfsStorageError::Rpc(format!("pin/rm failed: {body}")));
        }

        // Consume response body
        let _ = response.bytes().await;
        Ok(())
    }
}
