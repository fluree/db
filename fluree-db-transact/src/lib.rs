//! # Fluree DB Transact
//!
//! Transaction support for Fluree DB, including staging and committing changes.
//!
//! This crate provides:
//! - Transaction parsing (JSON-LD → IR)
//! - Flake generation from templates
//! - Staging (creates a `StagedLedger` with uncommitted changes)
//! - Commit (persists to storage and publishes to nameservice)
//!
//! ## Transaction Types
//!
//! - **Insert**: Add new triples (fails if subject already exists)
//! - **Upsert**: Insert or update (deletes existing values for provided predicates)
//! - **Update**: SPARQL-style conditional update with WHERE/DELETE/INSERT
//!
//! ## Example
//!
//! ```ignore
//! use fluree_db_transact::{stage_update, commit};
//! use serde_json::json;
//!
//! // Stage an update
//! let view = stage_update(ledger, &json!({
//!     "where": { "@id": "?s", "ex:name": "?name" },
//!     "delete": { "@id": "?s", "ex:name": "?name" },
//!     "insert": { "@id": "?s", "ex:name": "New Name" }
//! }), opts).await?;
//!
//! // Commit the changes
//! let (receipt, new_state) = commit(view, &storage, &nameservice, commit_opts).await?;
//! ```

pub mod address;
pub mod commit;
pub mod commit_flakes;
pub mod error;
pub mod flake_sink;
pub mod generate;
pub mod ir;
pub mod lower_sparql_update;
pub mod namespace;
pub mod parse;
pub mod raw_txn_upload;
pub mod stage;
mod value_convert;

#[cfg(feature = "import")]
pub mod import;
pub mod import_sink;
/// Re-export from `fluree_graph_turtle::splitter` for backwards compatibility.
#[cfg(feature = "import")]
pub use fluree_graph_turtle::splitter as turtle_splitter;

// Re-exports
pub use address::parse_commit_id;
pub use commit::{commit, CommitOpts, CommitReceipt};
pub use commit_flakes::generate_commit_flakes;
pub use error::{Result, TransactError};
pub use flake_sink::FlakeSink;
pub use generate::{apply_cancellation, FlakeGenerator};
pub use ir::{InlineValues, TemplateTerm, TripleTemplate, Txn, TxnOpts, TxnType};
pub use lower_sparql_update::{lower_sparql_update, lower_sparql_update_ast, LowerError};
pub use namespace::{NamespaceRegistry, SharedNamespaceAllocator, BLANK_NODE_PREFIX};
pub use parse::{
    parse_transaction, parse_trig_phase1, resolve_trig_meta, NamedGraphBlock, RawObject, RawTerm,
    RawTrigMeta, RawTriple, TrigPhase1Result,
};
pub use raw_txn_upload::PendingRawTxnUpload;
pub use stage::{generate_txn_id, stage, stage_flakes, StageOptions};

#[cfg(feature = "shacl")]
pub use stage::{
    stage_with_shacl, validate_view_with_shacl, ShaclGraphPolicy, ShaclValidationOutcome,
};

pub mod commit_v2;
