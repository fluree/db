//! Proxy storage implementation for peer mode
//!
//! Fetches ledger content over HTTP from another Fluree server instead of
//! direct storage access. This allows peers to operate without storage
//! credentials (no S3 access, no filesystem mount).
//!
//! Two read modes are supported, selected at construction via [`ProxyReadMode`]:
//!
//! ## `ProxyReadMode::Raw` — full-access CAS reads
//!
//! Fetches canonical CAS bytes via `GET /v1/fluree/storage/objects/{cid}` and
//! verifies every payload against its CID client-side before returning it.
//! Leaf blocks arrive as raw FLI3, so the binary index reader consumes them
//! directly. Requires a token whose scope grants **full read access** to the
//! ledger — the server serves raw index content without policy filtering on
//! this endpoint.
//!
//! ## `ProxyReadMode::Filtered` — policy-filtered reads
//!
//! Fetches through `POST /v1/fluree/storage/block`, where the server always
//! returns decoded, policy-filtered flakes (FLKB format) for leaf blocks —
//! raw FLI3 leaf bytes are never returned. Both `read_bytes()` and
//! `read_bytes_hint()` use flakes-first content negotiation: they request
//! `application/x-fluree-flakes` first, falling back to
//! `application/octet-stream` on 406 (for non-leaf blocks like commits and
//! branches). Payloads are identity-specific and NOT verifiable against their
//! CID; integrity rests on TLS + bearer auth. Note that no in-tree reader
//! currently decodes FLKB leaves — this tier is the transport for future
//! fine-grained (row-level filtered) peer access.

use crate::transport::{HttpTransport, TransportError, TransportRequest};
use async_trait::async_trait;
use bytes::Bytes;
use fluree_db_core::error::{Error as CoreError, Result};
use fluree_db_core::format_ledger_id;
use fluree_db_core::storage::{ReadHint, GRAPH_SOURCES_PATH_SEGMENT};
use fluree_db_core::{
    ContentAddressedWrite, ContentId, ContentKind, ContentWriteResult, StorageRead, StorageWrite,
    CODEC_FLUREE_COMMIT, CODEC_FLUREE_DICT_BLOB, CODEC_FLUREE_GARBAGE,
    CODEC_FLUREE_GRAPH_SOURCE_MAPPING, CODEC_FLUREE_GRAPH_SOURCE_SNAPSHOT,
    CODEC_FLUREE_HISTORY_SIDECAR, CODEC_FLUREE_INDEX_BRANCH, CODEC_FLUREE_INDEX_LEAF,
    CODEC_FLUREE_INDEX_ROOT, CODEC_FLUREE_LEDGER_CONFIG, CODEC_FLUREE_SPATIAL_INDEX,
    CODEC_FLUREE_STATS_SKETCH, CODEC_FLUREE_TXN,
};
use http::StatusCode;
use serde::Serialize;
use std::fmt::Debug;
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

/// How [`ProxyStorage`] fetches ledger content from the origin server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyReadMode {
    /// Raw CAS reads via `GET /storage/objects/{cid}`: canonical bytes,
    /// integrity-verified against the CID client-side. Requires full read
    /// access to the ledger.
    Raw,
    /// Policy-filtered reads via `POST /storage/block`: leaf blocks arrive
    /// as FLKB (decoded, filtered flakes), non-leaf blocks as raw bytes.
    /// Payloads are not CID-verifiable.
    Filtered,
}

/// Storage implementation that proxies reads through the transaction server
///
/// All network I/O goes through the [`HttpTransport`] seam; the wire
/// protocol (URLs, address↔CID parsing, status mapping, CID verification)
/// lives here and is transport-agnostic.
#[derive(Clone)]
pub struct ProxyStorage {
    transport: Arc<dyn HttpTransport>,
    api_base: String,
    token: String,
    mode: ProxyReadMode,
    /// Mount prefix under which the remote's ledgers appear locally
    /// (e.g. `"acme"` makes remote `inventory:main` appear as
    /// `acme/inventory:main`). Stripped from locally-derived aliases
    /// before requests go to the remote.
    local_prefix: Option<String>,
}

impl Debug for ProxyStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyStorage")
            .field("api_base", &self.api_base)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

/// Request body for the `/storage/block` endpoint.
///
/// Both fields are required. The `cid` and `ledger` are derived from
/// the storage address via [`cid_and_ledger_from_address()`].
#[derive(Debug, Serialize)]
struct BlockRequest {
    cid: String,
    ledger: String,
}

/// Try to derive a CID string and ledger alias from a Fluree address.
///
/// Parses the canonical `fluree:{method}://{alias_path}/{kind_dir}/{hash}.{ext}`
/// format produced by [`fluree_db_core::content_address()`].
///
/// Returns `(cid, ledger_alias)` on success, or `None` if the format
/// is unrecognized (in which case the caller should fall back to address-based).
///
/// Public so cache layers wrapping [`ProxyStorage`] (the browser peer's
/// CAS cache) can key by CID without re-implementing the address layout;
/// the mapping is pinned by the round-trip tests in this module.
pub fn cid_and_ledger_from_address(address: &str) -> Option<(ContentId, String)> {
    // Strip `fluree:{method}://` prefix
    let rest = address.strip_prefix("fluree:")?;
    let sep_pos = rest.find("://")?;
    let path = &rest[sep_pos + 3..];

    let parts: Vec<&str> = path.split('/').collect();
    let n = parts.len();
    if n < 3 {
        return None;
    }

    // Extract hash hex from the filename stem (before extension)
    let filename = parts[n - 1];
    let (hash_hex, _ext) = filename.rsplit_once('.')?;

    // Determine the multicodec and the alias slice `parts[alias_start..alias_end]`
    // based on directory structure. These patterns mirror those generated by
    // `content_path()` in fluree-db-core. `alias_start` is 0 for the usual
    // "{ledger}/…" layouts and 1 for the graph-source kinds, whose addresses
    // lead with a literal `graph-sources` segment and carry the ledger *after*
    // it. `branch_in_path` is false only for the @shared dict layout, whose
    // addresses carry the ledger name with no branch segment.
    let (codec, alias_start, alias_end, branch_in_path) = if n >= 2 && parts[n - 2] == "commit" {
        (CODEC_FLUREE_COMMIT, 0, n - 2, true)
    } else if n >= 2 && parts[n - 2] == "txn" {
        (CODEC_FLUREE_TXN, 0, n - 2, true)
    } else if n >= 2 && parts[n - 2] == "config" {
        (CODEC_FLUREE_LEDGER_CONFIG, 0, n - 2, true)
    } else if n >= 3 && parts[n - 3] == "index" && parts[n - 2] == "roots" {
        (CODEC_FLUREE_INDEX_ROOT, 0, n - 3, true)
    } else if n >= 3 && parts[n - 3] == "index" && parts[n - 2] == "garbage" {
        (CODEC_FLUREE_GARBAGE, 0, n - 3, true)
    } else if n >= 3 && parts[n - 3] == "index" && parts[n - 2] == "stats" {
        // StatsSketch: {name}/{branch}/index/stats/{hash}.hll
        (CODEC_FLUREE_STATS_SKETCH, 0, n - 3, true)
    } else if n >= 3 && parts[n - 3] == "index" && parts[n - 2] == "spatial" {
        // SpatialIndex: {name}/{branch}/index/spatial/{hash}.bin
        (CODEC_FLUREE_SPATIAL_INDEX, 0, n - 3, true)
    } else if n >= 4 && parts[n - 4] == "index" && parts[n - 3] == "objects" {
        match parts[n - 2] {
            "branches" => (CODEC_FLUREE_INDEX_BRANCH, 0, n - 4, true),
            "leaves" => (CODEC_FLUREE_INDEX_LEAF, 0, n - 4, true),
            // HistorySidecar: {name}/{branch}/index/objects/history/{hash}.fhs1
            "history" => (CODEC_FLUREE_HISTORY_SIDECAR, 0, n - 4, true),
            // Legacy per-branch dict layout (pre-@shared):
            // {name}/{branch}/index/objects/dicts/{hash}.dict
            "dicts" => (CODEC_FLUREE_DICT_BLOB, 0, n - 4, true),
            _ => return None,
        }
    } else if n >= 3 && parts[n - 2] == "dicts" && parts[n - 3] == "@shared" {
        // DictBlob: {name}/@shared/dicts/{hash}.{ext} — no branch in the
        // path; the default branch stands in and the server resolves it to
        // a live branch of the name.
        (CODEC_FLUREE_DICT_BLOB, 0, n - 3, false)
    } else if n >= 5 && parts[0] == GRAPH_SOURCES_PATH_SEGMENT && parts[n - 2] == "snapshots" {
        // GraphSourceSnapshot: graph-sources/{name}/{branch}/snapshots/{hash}.gssnap
        // The alias is not the leading segment here, so the alias slice
        // starts at 1 (after the literal `graph-sources` prefix).
        //
        // NOTE: for the two graph-source kinds the recovered "ledger" is
        // actually a **graph_source_id** — that is what `content_path` was
        // given at write time (see bm25.rs / r2rml.rs). Callers pass it as
        // the server's `?ledger=` parameter, which the object endpoint cannot
        // resolve today (`ns.lookup()` skips graph-source records); serving
        // these kinds is tracked in fluree/db#1539. Parsing them here is
        // still correct and forward-compatible: the client forms the right
        // request as soon as the server learns to answer it.
        (CODEC_FLUREE_GRAPH_SOURCE_SNAPSHOT, 1, n - 2, true)
    } else if n >= 5 && parts[0] == GRAPH_SOURCES_PATH_SEGMENT && parts[n - 2] == "mapping" {
        // GraphSourceMapping: graph-sources/{name}/{branch}/mapping/{hash}.ttl
        // (same graph_source_id caveat as the snapshots arm above)
        (CODEC_FLUREE_GRAPH_SOURCE_MAPPING, 1, n - 2, true)
    } else {
        return None;
    };

    // Reconstruct ledger ID from the alias segments `parts[alias_start..alias_end]`
    // (the segments before the kind directory, after any leading prefix).
    let ledger_id = if branch_in_path {
        if alias_end < alias_start + 2 {
            return None;
        }
        let branch = parts[alias_end - 1];
        let name = parts[alias_start..alias_end - 1].join("/");
        format_ledger_id(&name, branch)
    } else {
        if alias_end < alias_start + 1 {
            return None;
        }
        let name = parts[alias_start..alias_end].join("/");
        format_ledger_id(&name, fluree_db_core::DEFAULT_BRANCH)
    };

    // Build CID from codec + hex digest
    let cid = ContentId::from_hex_digest(codec, hash_hex)?;
    Some((cid, ledger_id))
}

/// Internal result type for fetch operations
///
/// This avoids using CoreError for the 406 case, which is an internal
/// retry condition rather than a user-facing error.
enum FetchOutcome {
    /// Successfully fetched bytes
    Success(Vec<u8>),
    /// Server returned 406 Not Acceptable (format not available)
    NotAcceptable,
    /// Fetch failed with an error
    Error(CoreError),
}

impl ProxyStorage {
    /// Create a new proxy storage client
    ///
    /// # Arguments
    ///
    /// * `base_url` - Base URL of the transaction server (e.g., `https://tx.fluree.internal:8090`)
    /// * `token` - Bearer token for authentication (with `fluree.storage.*` claims)
    /// * `mode` - Read mode: [`ProxyReadMode::Raw`] for CID-verified canonical
    ///   bytes (full-access tokens), [`ProxyReadMode::Filtered`] for
    ///   policy-filtered FLKB leaf payloads
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(base_url: String, token: String, mode: ProxyReadMode) -> Self {
        // Server root → default versioned API base.
        let api_base = format!("{}/v1/fluree", base_url.trim_end_matches('/'));
        Self::from_api_base(api_base, token, mode)
    }

    /// Create a proxy storage client from a full API base URL (e.g.
    /// `https://data.example.com/v1/fluree`), as stored by `fluree remote add`
    /// or advertised via discovery's `api_base_url`. Use this instead of
    /// [`new`](Self::new) when the API may be mounted under a non-default
    /// prefix.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_api_base(api_base: String, token: String, mode: ProxyReadMode) -> Self {
        let transport = Arc::new(crate::transport::ReqwestTransport::with_timeout(
            Duration::from_secs(60), // 1 minute for block reads
        ));
        Self::from_api_base_with_transport(api_base, token, mode, transport)
    }

    /// Create a proxy storage client over a caller-supplied [`HttpTransport`].
    ///
    /// This is the constructor for non-native environments (e.g. a browser
    /// fetch transport); [`new`](Self::new) and
    /// [`from_api_base`](Self::from_api_base) are conveniences that plug in
    /// the default reqwest transport.
    pub fn from_api_base_with_transport(
        api_base: String,
        token: String,
        mode: ProxyReadMode,
        transport: Arc<dyn HttpTransport>,
    ) -> Self {
        Self {
            transport,
            api_base: api_base.trim_end_matches('/').to_string(),
            token,
            mode,
            local_prefix: None,
        }
    }

    /// Set the mount prefix under which the remote's ledgers appear locally.
    ///
    /// With prefix `"acme"`, a read of address
    /// `fluree:proxy://acme/inventory/main/commit/x.fc` is requested from the
    /// remote as ledger `inventory:main`.
    #[must_use]
    pub fn with_local_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.local_prefix = Some(prefix.into());
        self
    }

    /// Rewrite a locally-derived ledger alias to the remote's own alias by
    /// stripping the mount prefix (`acme/inventory:main` → `inventory:main`).
    fn remote_ledger(&self, ledger: String) -> String {
        match &self.local_prefix {
            Some(prefix) => ledger
                .strip_prefix(prefix.as_str())
                .and_then(|rest| rest.strip_prefix('/'))
                .map(str::to_string)
                .unwrap_or(ledger),
            None => ledger,
        }
    }

    /// Build the storage block endpoint URL
    fn block_url(&self) -> String {
        format!("{}/storage/block", self.api_base)
    }

    /// Build the CAS object endpoint URL for a CID
    fn object_url(&self, cid: &ContentId) -> String {
        format!("{}/storage/objects/{}", self.api_base, cid)
    }

    /// Build the object endpoint URL including the `ledger` query parameter.
    fn object_url_for(&self, cid: &ContentId, ledger: &str) -> String {
        format!(
            "{}?ledger={}",
            self.object_url(cid),
            urlencoding::encode(ledger)
        )
    }

    /// Map a transport failure to the caller-facing storage error, keeping
    /// the historical message per failure class.
    fn transport_error(address: &str, err: TransportError) -> CoreError {
        match err {
            TransportError::Timeout(e) => {
                CoreError::io(format!("Storage proxy timeout for {address}: {e}"))
            }
            TransportError::Connect(e) => CoreError::io(format!(
                "Storage proxy connection failed for {address}: {e}"
            )),
            TransportError::Request(e) => {
                CoreError::io(format!("Storage proxy request failed for {address}: {e}"))
            }
            TransportError::Body(e) => {
                CoreError::io(format!("Failed to read response body for {address}: {e}"))
            }
        }
    }

    /// Fetch canonical CAS bytes via `GET /storage/objects/{cid}` and verify
    /// them against the CID before returning.
    ///
    /// Always uses the raw object endpoint regardless of [`ProxyReadMode`]
    /// (it is what [`ProxyReadMode::Raw`] reads dispatch to). Returns the
    /// transport's buffer without copying; callers that need an owned
    /// `Vec<u8>` convert at their own boundary — a cache layer that keeps
    /// the bytes resident (the browser peer) takes the [`Bytes`] directly.
    pub async fn read_object_bytes(&self, address: &str) -> Result<Bytes> {
        let (cid, ledger) = cid_and_ledger_from_address(address).ok_or_else(|| {
            CoreError::storage(format!("Cannot derive CID from address: {address}"))
        })?;
        let ledger = self.remote_ledger(ledger);

        let request = TransportRequest::get(self.object_url_for(&cid, &ledger))
            .header("authorization", format!("Bearer {}", self.token));
        let response = self
            .transport
            .execute(request)
            .await
            .map_err(|e| Self::transport_error(address, e))?;

        let status = response.status;
        match status {
            StatusCode::OK => {
                let bytes = response.body;
                if !crate::integrity::verify_object_integrity(&cid, &bytes) {
                    return Err(CoreError::storage(format!(
                        "Integrity verification failed for {address} (cid {cid})"
                    )));
                }
                Ok(bytes)
            }
            // 403 → NotFound parity with the server's no-existence-leak behavior
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN => Err(CoreError::not_found(address)),
            StatusCode::UNAUTHORIZED => Err(CoreError::storage(format!(
                "Storage proxy authentication failed for {address}: check token validity"
            ))),
            s if s.is_server_error() => Err(CoreError::io(format!(
                "Storage proxy server error for {address}: {status}"
            ))),
            _ => Err(CoreError::storage(format!(
                "Storage proxy unexpected status {status} for {address}"
            ))),
        }
    }

    /// Fetch with flakes-first content negotiation
    ///
    /// Tries `application/x-fluree-flakes` first. If the server returns 406
    /// (format not available for this block type), falls back to raw bytes.
    async fn fetch_prefer_flakes(&self, address: &str) -> Result<Vec<u8>> {
        // Try flakes format first
        match self
            .fetch_with_accept(address, "application/x-fluree-flakes")
            .await
        {
            FetchOutcome::Success(bytes) => Ok(bytes),
            FetchOutcome::NotAcceptable => {
                // 406 = not a leaf or policy filtering not applicable
                // Fall back to raw bytes
                tracing::debug!(
                    address = %address,
                    "Flakes format not available, falling back to raw bytes"
                );
                match self
                    .fetch_with_accept(address, "application/octet-stream")
                    .await
                {
                    FetchOutcome::Success(bytes) => Ok(bytes),
                    FetchOutcome::NotAcceptable => {
                        // Should not happen for octet-stream, but handle anyway
                        Err(CoreError::storage(format!(
                            "Storage proxy: octet-stream also rejected for {address}"
                        )))
                    }
                    FetchOutcome::Error(e) => Err(e),
                }
            }
            FetchOutcome::Error(e) => Err(e),
        }
    }

    /// Internal fetch with a specific Accept header
    ///
    /// Returns `FetchOutcome` to distinguish between success, 406, and errors
    /// without using string-based error detection.
    ///
    /// Derives CID+ledger from the address. Failure to parse is a hard error
    /// (all storage addresses in the system use the canonical format).
    async fn fetch_with_accept(&self, address: &str, accept: &str) -> FetchOutcome {
        let url = self.block_url();
        let (cid, ledger) = match cid_and_ledger_from_address(address) {
            Some(pair) => pair,
            None => {
                return FetchOutcome::Error(CoreError::storage(format!(
                    "Cannot derive CID from address: {address}"
                )));
            }
        };
        let body = BlockRequest {
            cid: cid.to_string(),
            ledger: self.remote_ledger(ledger),
        };
        let body_bytes = match serde_json::to_vec(&body) {
            Ok(b) => b,
            Err(e) => {
                return FetchOutcome::Error(CoreError::storage(format!(
                    "Failed to encode block request for {address}: {e}"
                )));
            }
        };

        let request = TransportRequest::post(url)
            .header("authorization", format!("Bearer {}", self.token))
            .header("accept", accept)
            .header("content-type", "application/json")
            .body(body_bytes);
        let response = match self.transport.execute(request).await {
            Ok(r) => r,
            Err(e) => {
                return FetchOutcome::Error(Self::transport_error(address, e));
            }
        };

        let status = response.status;

        match status {
            StatusCode::OK => FetchOutcome::Success(response.body.to_vec()),
            StatusCode::NOT_FOUND => FetchOutcome::Error(CoreError::not_found(address)),
            StatusCode::NOT_ACCEPTABLE => {
                // 406 - format not available, signal for retry with different Accept
                FetchOutcome::NotAcceptable
            }
            StatusCode::UNAUTHORIZED => FetchOutcome::Error(CoreError::storage(format!(
                "Storage proxy authentication failed for {address}: check token validity"
            ))),
            StatusCode::FORBIDDEN => {
                // Address not in token scope - treat as not found (no existence leak)
                FetchOutcome::Error(CoreError::not_found(address))
            }
            s if s.is_server_error() => FetchOutcome::Error(CoreError::io(format!(
                "Storage proxy server error for {address}: {status}"
            ))),
            _ => FetchOutcome::Error(CoreError::storage(format!(
                "Storage proxy unexpected status {status} for {address}"
            ))),
        }
    }
}

#[async_trait]
impl StorageRead for ProxyStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
        match self.mode {
            // Raw mode: canonical CAS bytes, CID-verified client-side.
            ProxyReadMode::Raw => self.read_object_bytes(address).await.map(|b| b.to_vec()),
            // Filtered mode: flakes-first negotiation for deterministic
            // behavior across block types:
            // - Leaves → FLKB (policy-filtered flakes)
            // - Non-leaves → raw bytes (via 406 fallback to octet-stream)
            ProxyReadMode::Filtered => self.fetch_prefer_flakes(address).await,
        }
    }

    async fn read_byte_range(&self, address: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        // Filtered payloads are transport-encoded (FLKB), so byte ranges
        // don't apply — fall back to full read + slice.
        if self.mode != ProxyReadMode::Raw {
            let full = self.read_bytes(address).await?;
            let start = range.start as usize;
            if start >= full.len() {
                return Ok(Vec::new());
            }
            let end = (range.end as usize).min(full.len());
            return Ok(full[start..end].to_vec());
        }

        let (cid, ledger) = cid_and_ledger_from_address(address).ok_or_else(|| {
            CoreError::storage(format!("Cannot derive CID from address: {address}"))
        })?;
        let ledger = self.remote_ledger(ledger);

        let request = TransportRequest::get(self.object_url_for(&cid, &ledger))
            .header("authorization", format!("Bearer {}", self.token))
            // HTTP ranges are inclusive; ours are half-open.
            .header("range", format!("bytes={}-{}", range.start, range.end - 1));
        let response = self.transport.execute(request).await.map_err(|e| match e {
            // This path historically reported all send failures uniformly;
            // only body-read failures carry their own message.
            TransportError::Body(e) => {
                CoreError::io(format!("Failed to read response body for {address}: {e}"))
            }
            TransportError::Timeout(e)
            | TransportError::Connect(e)
            | TransportError::Request(e) => {
                CoreError::io(format!("Storage proxy request failed for {address}: {e}"))
            }
        })?;

        let status = response.status;
        match status {
            // Partial payloads can't be CID-verified client-side; the server
            // verifies the full object against the CID before slicing.
            StatusCode::PARTIAL_CONTENT => Ok(response.body.to_vec()),
            // Server ignored the Range header (older server): verify the
            // full object and slice locally.
            StatusCode::OK => {
                let bytes = response.body;
                if !crate::integrity::verify_object_integrity(&cid, &bytes) {
                    return Err(CoreError::storage(format!(
                        "Integrity verification failed for {address} (cid {cid})"
                    )));
                }
                let start = range.start as usize;
                if start >= bytes.len() {
                    return Ok(Vec::new());
                }
                let end = (range.end as usize).min(bytes.len());
                Ok(bytes[start..end].to_vec())
            }
            // Range start past the object length — matches the default
            // implementation's empty-slice semantics.
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(Vec::new()),
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN => Err(CoreError::not_found(address)),
            StatusCode::UNAUTHORIZED => Err(CoreError::storage(format!(
                "Storage proxy authentication failed for {address}: check token validity"
            ))),
            s if s.is_server_error() => Err(CoreError::io(format!(
                "Storage proxy server error for {address}: {status}"
            ))),
            _ => Err(CoreError::storage(format!(
                "Storage proxy unexpected status {status} for {address}"
            ))),
        }
    }

    fn supports_ranged_reads(&self) -> bool {
        // Raw mode issues true HTTP Range requests; the filtered tier
        // full-fetches and slices.
        self.mode == ProxyReadMode::Raw
    }

    async fn read_bytes_hint(&self, address: &str, hint: ReadHint) -> Result<Vec<u8>> {
        match self.mode {
            // Raw mode always returns canonical bytes; the FLKB preference
            // only applies to the filtered tier.
            ProxyReadMode::Raw => self.read_object_bytes(address).await.map(|b| b.to_vec()),
            ProxyReadMode::Filtered => match hint {
                ReadHint::AnyBytes => self.read_bytes(address).await,
                ReadHint::PreferLeafFlakes => self.fetch_prefer_flakes(address).await,
                // Future ReadHint variants fall back to default
                _ => self.read_bytes(address).await,
            },
        }
    }

    async fn exists(&self, address: &str) -> Result<bool> {
        // In v1, implement exists as try-read
        // This is correct but slightly less efficient than a HEAD request
        // The server currently only supports POST for blocks anyway
        match self.read_bytes(address).await {
            Ok(_) => Ok(true),
            Err(CoreError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn list_prefix(&self, _prefix: &str) -> Result<Vec<String>> {
        // ProxyStorage is read-only and doesn't support listing
        Err(CoreError::storage(
            "ProxyStorage does not support list_prefix".to_string(),
        ))
    }
}

#[async_trait]
impl StorageWrite for ProxyStorage {
    async fn write_bytes(&self, _address: &str, _bytes: &[u8]) -> Result<()> {
        Err(CoreError::storage(
            "ProxyStorage is read-only (writes must go to the transaction server)".to_string(),
        ))
    }

    async fn delete(&self, _address: &str) -> Result<()> {
        Err(CoreError::storage(
            "ProxyStorage is read-only (deletes must go to the transaction server)".to_string(),
        ))
    }
}

#[async_trait]
impl ContentAddressedWrite for ProxyStorage {
    async fn content_write_bytes_with_hash(
        &self,
        _kind: ContentKind,
        _ledger_alias: &str,
        _content_hash_hex: &str,
        _bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        Err(CoreError::storage(
            "ProxyStorage is read-only (writes must go to the transaction server)".to_string(),
        ))
    }

    async fn content_write_bytes(
        &self,
        _kind: ContentKind,
        _ledger_alias: &str,
        _bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        Err(CoreError::storage(
            "ProxyStorage is read-only (writes must go to the transaction server)".to_string(),
        ))
    }
}

impl fluree_db_core::StorageMethod for ProxyStorage {
    fn storage_method(&self) -> &'static str {
        "proxy"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;

    /// A canned-response transport: records the requests it receives and
    /// replays a fixed response. This is the injection shape a browser
    /// (fetch-backed) transport uses, so it doubles as a seam proof.
    #[derive(Debug)]
    struct CannedTransport {
        requests: std::sync::Mutex<Vec<TransportRequest>>,
        status: StatusCode,
        body: Vec<u8>,
    }

    impl CannedTransport {
        fn new(status: StatusCode, body: Vec<u8>) -> Arc<Self> {
            Arc::new(Self {
                requests: std::sync::Mutex::new(Vec::new()),
                status,
                body,
            })
        }
    }

    #[async_trait]
    impl HttpTransport for CannedTransport {
        async fn execute(
            &self,
            req: TransportRequest,
        ) -> std::result::Result<crate::transport::TransportResponse, TransportError> {
            self.requests.lock().unwrap().push(req);
            Ok(crate::transport::TransportResponse {
                status: self.status,
                headers: http::HeaderMap::new(),
                body: bytes::Bytes::from(self.body.clone()),
            })
        }
    }

    /// End-to-end through an injected transport: the raw path forms the
    /// object URL + bearer header, verifies the payload against the CID,
    /// and returns the canonical bytes — no reqwest involved.
    #[tokio::test]
    async fn raw_read_via_injected_transport_verifies_and_returns_bytes() {
        let payload = b"commit payload".to_vec();
        let id = ContentId::new(ContentKind::Txn, &payload);
        let address = fluree_db_core::content_address(
            "file",
            ContentKind::Txn,
            "mydb:main",
            &id.digest_hex(),
        );

        let transport = CannedTransport::new(StatusCode::OK, payload.clone());
        let storage = ProxyStorage::from_api_base_with_transport(
            "http://origin.example/v1/fluree".to_string(),
            "tok".to_string(),
            ProxyReadMode::Raw,
            transport.clone(),
        );

        let bytes = storage.read_bytes(&address).await.expect("raw read");
        assert_eq!(bytes, payload);

        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let req = &requests[0];
        assert_eq!(req.method, crate::transport::TransportMethod::Get);
        assert_eq!(
            req.url,
            format!("http://origin.example/v1/fluree/storage/objects/{id}?ledger=mydb%3Amain")
        );
        assert_eq!(
            req.headers,
            vec![("authorization", "Bearer tok".to_string())]
        );
    }

    /// Integrity failures are detected client-side regardless of transport:
    /// a payload that doesn't hash to the CID is rejected.
    #[tokio::test]
    async fn raw_read_via_injected_transport_rejects_corrupt_bytes() {
        let payload = b"commit payload".to_vec();
        let id = ContentId::new(ContentKind::Txn, &payload);
        let address = fluree_db_core::content_address(
            "file",
            ContentKind::Txn,
            "mydb:main",
            &id.digest_hex(),
        );

        let transport = CannedTransport::new(StatusCode::OK, b"tampered bytes".to_vec());
        let storage = ProxyStorage::from_api_base_with_transport(
            "http://origin.example/v1/fluree".to_string(),
            "tok".to_string(),
            ProxyReadMode::Raw,
            transport,
        );

        let err = storage
            .read_bytes(&address)
            .await
            .expect_err("corrupt payload must be rejected");
        assert!(
            err.to_string().contains("Integrity verification failed"),
            "got: {err}"
        );
    }

    /// 403 maps to NotFound (no existence leak) through any transport.
    #[tokio::test]
    async fn raw_read_via_injected_transport_maps_forbidden_to_not_found() {
        let payload = b"x".to_vec();
        let id = ContentId::new(ContentKind::Txn, &payload);
        let address = fluree_db_core::content_address(
            "file",
            ContentKind::Txn,
            "mydb:main",
            &id.digest_hex(),
        );

        let transport = CannedTransport::new(StatusCode::FORBIDDEN, Vec::new());
        let storage = ProxyStorage::from_api_base_with_transport(
            "http://origin.example/v1/fluree".to_string(),
            "tok".to_string(),
            ProxyReadMode::Raw,
            transport,
        );

        let err = storage.read_bytes(&address).await.expect_err("403");
        assert!(matches!(err, CoreError::NotFound(_)), "got: {err:?}");
    }

    #[test]
    fn test_proxy_storage_debug() {
        let storage = ProxyStorage::new(
            "http://localhost:8090".to_string(),
            "test-token".to_string(),
            ProxyReadMode::Filtered,
        );
        let debug = format!("{storage:?}");
        assert!(debug.contains("ProxyStorage"));
        assert!(debug.contains("localhost:8090"));
        // Token should NOT be in debug output
        assert!(!debug.contains("test-token"));
    }

    #[test]
    fn test_block_url() {
        let storage = ProxyStorage::new(
            "http://localhost:8090".to_string(),
            "test-token".to_string(),
            ProxyReadMode::Raw,
        );
        assert_eq!(
            storage.block_url(),
            "http://localhost:8090/v1/fluree/storage/block"
        );
        let id = ContentId::new(ContentKind::Commit, b"url test");
        assert_eq!(
            storage.object_url(&id),
            format!("http://localhost:8090/v1/fluree/storage/objects/{id}")
        );
    }

    #[test]
    fn test_block_url_with_trailing_slash() {
        let storage = ProxyStorage::new(
            "http://localhost:8090/".to_string(),
            "test-token".to_string(),
            ProxyReadMode::Raw,
        );
        // Should work but might have double slash - that's okay for URLs
        assert!(storage.block_url().contains("/v1/fluree/storage/block"));
    }

    // ========================================================================
    // cid_and_ledger_from_address tests
    // ========================================================================

    /// Round-trip helper: build an address via `content_address`, then verify
    /// `cid_and_ledger_from_address` recovers the correct CID and ledger.
    fn assert_roundtrip(kind: ContentKind, alias: &str, data: &[u8]) {
        let id = ContentId::new(kind, data);
        let address = fluree_db_core::content_address("file", kind, alias, &id.digest_hex());
        let (cid, ledger) = cid_and_ledger_from_address(&address).expect("should parse address");
        assert_eq!(cid, id, "CID mismatch for {address}");
        assert_eq!(ledger, alias, "ledger mismatch for {address}");
    }

    #[test]
    fn test_parse_commit_address() {
        assert_roundtrip(ContentKind::Commit, "mydb:main", b"commit data");
    }

    #[test]
    fn test_parse_txn_address() {
        assert_roundtrip(ContentKind::Txn, "mydb:main", b"txn data");
    }

    #[test]
    fn test_parse_index_root_address() {
        assert_roundtrip(ContentKind::IndexRoot, "mydb:main", b"root data");
    }

    #[test]
    fn test_parse_index_branch_address() {
        assert_roundtrip(ContentKind::IndexBranch, "mydb:main", b"branch data");
    }

    #[test]
    fn test_parse_index_leaf_address() {
        assert_roundtrip(ContentKind::IndexLeaf, "mydb:main", b"leaf data");
    }

    #[test]
    fn test_parse_dict_blob_address() {
        use fluree_db_core::DictKind;
        assert_roundtrip(
            ContentKind::DictBlob {
                dict: DictKind::Graphs,
            },
            "mydb:main",
            b"dict data",
        );
    }

    #[test]
    fn test_parse_config_address() {
        assert_roundtrip(ContentKind::LedgerConfig, "mydb:main", b"config data");
    }

    #[test]
    fn test_parse_garbage_address() {
        assert_roundtrip(ContentKind::GarbageRecord, "mydb:main", b"gc data");
    }

    #[test]
    fn test_parse_stats_sketch_address() {
        assert_roundtrip(ContentKind::StatsSketch, "mydb:main", b"hll sketch");
    }

    #[test]
    fn test_parse_spatial_index_address() {
        assert_roundtrip(ContentKind::SpatialIndex, "mydb:main", b"spatial index");
    }

    #[test]
    fn test_parse_history_sidecar_address() {
        assert_roundtrip(ContentKind::HistorySidecar, "mydb:main", b"history sidecar");
    }

    #[test]
    fn test_parse_graph_source_snapshot_address() {
        assert_roundtrip(
            ContentKind::GraphSourceSnapshot,
            "mydb:main",
            b"bm25 snapshot",
        );
    }

    #[test]
    fn test_parse_graph_source_mapping_address() {
        assert_roundtrip(
            ContentKind::GraphSourceMapping,
            "mydb:main",
            b"r2rml mapping",
        );
    }

    /// The graph-source layout is asymmetric (`graph-sources/{name…}/{branch}/…`,
    /// ledger not the leading segment). Cover a multi-segment ledger name there.
    #[test]
    fn test_parse_graph_source_multi_segment_ledger() {
        assert_roundtrip(
            ContentKind::GraphSourceSnapshot,
            "org/team/db:main",
            b"snapshot",
        );
    }

    /// Exhaustiveness guard: every content kind that `content_path` gives a
    /// distinct address MUST survive the mount reverse-parser (forward → reverse
    /// recovers the same CID + ledger). The four annotation kinds are a KNOWN
    /// forward-side gap — `content_path` routes all four to the shared
    /// `{prefix}/blob/{hash}.bin` catch-all, which carries no codec
    /// discriminator, so no reverse parser can recover them until the forward
    /// layout gives them distinct paths. This test pins both facts: adding a
    /// kind (or giving annotations distinct paths) without updating the parser
    /// fails here instead of silently shipping a mount hole.
    #[test]
    fn test_all_content_path_kinds_roundtrip_or_are_documented_gaps() {
        use fluree_db_core::DictKind;

        let recoverable = [
            ContentKind::Commit,
            ContentKind::Txn,
            ContentKind::IndexRoot,
            ContentKind::GarbageRecord,
            ContentKind::DictBlob {
                dict: DictKind::Graphs,
            },
            ContentKind::IndexBranch,
            ContentKind::IndexLeaf,
            ContentKind::LedgerConfig,
            ContentKind::StatsSketch,
            ContentKind::SpatialIndex,
            ContentKind::HistorySidecar,
            ContentKind::GraphSourceSnapshot,
            ContentKind::GraphSourceMapping,
        ];
        for kind in recoverable {
            let id = ContentId::new(kind, b"x");
            let address =
                fluree_db_core::content_address("file", kind, "mydb:main", &id.digest_hex());
            let (cid, ledger) = cid_and_ledger_from_address(&address)
                .unwrap_or_else(|| panic!("no round-trip for {kind:?} at {address}"));
            assert_eq!(cid, id, "CID mismatch for {kind:?}");
            assert_eq!(ledger, "mydb:main", "ledger mismatch for {kind:?}");
        }

        // Annotation kinds are forward-lossy today (shared blob/.bin), so the
        // reverse parser MUST return None. If `content_path` ever gives them
        // distinct paths, update the parser AND move them into `recoverable`.
        for kind in [
            ContentKind::AnnotationForwardBranch,
            ContentKind::AnnotationForwardLeaf,
            ContentKind::AnnotationReverseBranch,
            ContentKind::AnnotationReverseLeaf,
        ] {
            let id = ContentId::new(kind, b"x");
            let address =
                fluree_db_core::content_address("file", kind, "mydb:main", &id.digest_hex());
            assert!(
                cid_and_ledger_from_address(&address).is_none(),
                "{kind:?} unexpectedly parsed — update this test AND the parser"
            );
        }
    }

    #[test]
    fn test_parse_address_with_s3_method() {
        let id = ContentId::new(ContentKind::Commit, b"s3 test");
        let address = fluree_db_core::content_address(
            "s3",
            ContentKind::Commit,
            "prod:main",
            &id.digest_hex(),
        );
        let (cid, ledger) = cid_and_ledger_from_address(&address).expect("should parse s3 address");
        assert_eq!(cid, id);
        assert_eq!(ledger, "prod:main");
    }

    #[test]
    fn test_remote_ledger_prefix_strip() {
        let storage = ProxyStorage::new(
            "http://localhost:8090".to_string(),
            "test-token".to_string(),
            ProxyReadMode::Raw,
        )
        .with_local_prefix("acme");
        assert_eq!(
            storage.remote_ledger("acme/inventory:main".to_string()),
            "inventory:main"
        );
        // Non-matching aliases pass through unchanged.
        assert_eq!(
            storage.remote_ledger("other:main".to_string()),
            "other:main"
        );
        // A name that merely starts with the prefix string (no separator)
        // is not stripped.
        assert_eq!(
            storage.remote_ledger("acmecorp:main".to_string()),
            "acmecorp:main"
        );
        // Without a prefix configured, everything passes through.
        let plain = ProxyStorage::new(
            "http://localhost:8090".to_string(),
            "test-token".to_string(),
            ProxyReadMode::Raw,
        );
        assert_eq!(
            plain.remote_ledger("acme/inventory:main".to_string()),
            "acme/inventory:main"
        );
    }

    #[test]
    fn test_parse_address_not_fluree() {
        assert!(cid_and_ledger_from_address("https://example.com/foo").is_none());
    }

    #[test]
    fn test_parse_address_too_short() {
        assert!(cid_and_ledger_from_address("fluree:file://a/b").is_none());
    }

    #[test]
    fn test_parse_address_unknown_kind_dir() {
        assert!(cid_and_ledger_from_address("fluree:file://mydb/main/unknown/abc.bin").is_none());
    }
}
