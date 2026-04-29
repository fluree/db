//! Wire types shared between `fluree-db-server` and `fluree-db-cli`.
//!
//! These structs define the JSON contract for admin HTTP endpoints. Putting
//! them in `fluree-db-api` (a crate both server and CLI already depend on)
//! gives both ends a single typed source and eliminates brittle
//! `serde_json::Value::get(...)` decoding in the client.
//!
//! Internal library types (e.g. `ReindexResult` with `ContentId`) stay in
//! their respective modules. Conversions from internal → wire types live
//! here via `From` impls.
//!
//! Currently covers: `/reindex`. Other admin endpoints (`/create`, `/drop`,
//! `/branch`, etc.) can be moved into this module incrementally.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::ReindexResult;

/// Request body for `POST /reindex`.
///
/// `opts` is reserved for future per-request overrides (e.g. indexer tuning).
/// It is accepted but ignored today — the server always reindexes using the
/// indexer settings it is configured with.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReindexRequest {
    /// Ledger alias (e.g. `"mydb"` or `"mydb:main"`).
    pub ledger: String,
    /// Reserved for future use — currently ignored by the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opts: Option<JsonValue>,
}

/// Build statistics included in `ReindexResponse`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReindexStats {
    pub flake_count: usize,
    pub leaf_count: usize,
    pub branch_count: usize,
    pub total_bytes: usize,
}

/// Response body for `POST /reindex`.
///
/// Mirrors the library's `ReindexResult`, with `root_id` serialized as a
/// `String` (the `ContentId` display form) for wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReindexResponse {
    pub ledger_id: String,
    pub index_t: i64,
    pub root_id: String,
    pub stats: ReindexStats,
}

impl From<ReindexResult> for ReindexResponse {
    fn from(r: ReindexResult) -> Self {
        Self {
            ledger_id: r.ledger_id,
            index_t: r.index_t,
            root_id: r.root_id.to_string(),
            stats: ReindexStats {
                flake_count: r.stats.flake_count,
                leaf_count: r.stats.leaf_count,
                branch_count: r.stats.branch_count,
                total_bytes: r.stats.total_bytes,
            },
        }
    }
}
