//! Commit metadata flakes generation (re-export)
//!
//! The canonical implementation lives in `fluree-db-novelty` so it can be used both:
//! - during commit (transact) to inject metadata into novelty immediately
//! - during ledger load/replay (ledger) to regenerate metadata flakes from commit records
//!
//! This module is kept for API stability / convenience.

pub use fluree_db_novelty::{generate_commit_flakes, stamp_graph_on_commit_flakes};
