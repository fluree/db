//! DynamoDB table schema constants (composite-key layout v2)
//!
//! Defines the attribute names and values used in the fluree-nameservice table.
//!
//! ## Table Schema
//!
//! ```text
//! Table: fluree-nameservice (configurable)
//!
//! Primary Key:
//!   - pk (String, Partition Key): alias in `name:branch` form (e.g., "mydb:main")
//!   - sk (String, Sort Key): concern discriminator ("meta", "head", "index", "config", "status")
//!
//! GSI1 (gsi1-kind):
//!   - PK: kind (String) — "ledger" | "graph_source"
//!   - SK: pk (String)
//!   - Projection: INCLUDE (name, branch, source_type, dependencies, retracted)
//!
//! Items per alias (ledger):
//!   - (pk, "meta")   — kind, name, branch, retracted
//!   - (pk, "head")   — commit_id, commit_t
//!   - (pk, "index")  — index_id, index_t
//!   - (pk, "config") — default_context_address, config_v, config_meta
//!   - (pk, "status") — status, status_v, status_meta
//!
//! Items per alias (graph source):
//!   - (pk, "meta")   — kind, source_type, name, branch, dependencies, retracted
//!   - (pk, "config") — config_json, config_v
//!   - (pk, "index")  — index_id, index_t
//!   - (pk, "status") — status, status_v, status_meta
//! ```

// ── Primary key ─────────────────────────────────────────────────────────────
/// Partition key: alias in `name:branch` form (e.g., "mydb:main")
pub const ATTR_PK: &str = "pk";
/// Sort key: concern discriminator
pub const ATTR_SK: &str = "sk";

// ── Sort key values ─────────────────────────────────────────────────────────
pub const SK_META: &str = "meta";
pub const SK_HEAD: &str = "head";
pub const SK_INDEX: &str = "index";
pub const SK_CONFIG: &str = "config";
pub const SK_STATUS: &str = "status";

// ── Meta item attributes ────────────────────────────────────────────────────
/// "ledger" | "graph_source"
pub const ATTR_KIND: &str = "kind";
/// Ledger/graph-source name (DynamoDB reserved word — use `#name`)
pub const ATTR_NAME: &str = "name";
/// Branch name
pub const ATTR_BRANCH: &str = "branch";
/// Whether this record has been retracted
pub const ATTR_RETRACTED: &str = "retracted";
/// Graph-source type string (e.g., "f:Bm25Index")
pub const ATTR_SOURCE_TYPE: &str = "source_type";
/// Branch point: source branch name (e.g., "main")
pub const ATTR_BP_SOURCE: &str = "bp_source";
/// Branch point: commit CID at branch time
pub const ATTR_BP_COMMIT_ID: &str = "bp_commit_id";
/// Branch point: transaction time at branch time
pub const ATTR_BP_T: &str = "bp_t";
/// Number of child branches (reference count for safe deletion)
pub const ATTR_BRANCHES: &str = "branches";
/// Dependent ledger aliases (List<String>)
pub const ATTR_DEPENDENCIES: &str = "dependencies";

// ── Kind values ─────────────────────────────────────────────────────────────
pub const KIND_LEDGER: &str = "ledger";
pub const KIND_GRAPH_SOURCE: &str = "graph_source";

// ── Head item attributes (ledger only) ──────────────────────────────────────
pub const ATTR_COMMIT_ID: &str = "commit_id";
pub const ATTR_COMMIT_T: &str = "commit_t";

// ── Index item attributes (ledger + graph source) ───────────────────────────
pub const ATTR_INDEX_ID: &str = "index_id";
pub const ATTR_INDEX_T: &str = "index_t";

// ── Config item attributes ──────────────────────────────────────────────────
/// Default JSON-LD context address (ledger config)
pub const ATTR_DEFAULT_CONTEXT_ADDRESS: &str = "default_context_address";
/// Opaque JSON config string (graph source only)
pub const ATTR_CONFIG_JSON: &str = "config_json";
/// Config version watermark (monotonically increasing)
pub const ATTR_CONFIG_V: &str = "config_v";
/// Extensible config metadata map (ledger config)
pub const ATTR_CONFIG_META: &str = "config_meta";

// ── Status item attributes ──────────────────────────────────────────────────
/// Status state string (DynamoDB reserved word — use `#st`)
pub const ATTR_STATUS: &str = "status";
/// Status version watermark (monotonically increasing)
pub const ATTR_STATUS_V: &str = "status_v";
/// Extensible status metadata map
pub const ATTR_STATUS_META: &str = "status_meta";

// ── Common attributes ───────────────────────────────────────────────────────
/// Last update timestamp (epoch milliseconds)
pub const ATTR_UPDATED_AT_MS: &str = "updated_at_ms";
/// Schema version number
pub const ATTR_SCHEMA: &str = "schema";
/// Current schema version
pub const SCHEMA_VERSION: i64 = 2;

// ── Status values ───────────────────────────────────────────────────────────
pub const STATUS_READY: &str = "ready";
pub const STATUS_RETRACTED: &str = "retracted";

// ── GSI1 ────────────────────────────────────────────────────────────────────
pub const GSI1_NAME: &str = "gsi1-kind";

// ── Table name ──────────────────────────────────────────────────────────────
pub const DEFAULT_TABLE_NAME: &str = "fluree-nameservice";
