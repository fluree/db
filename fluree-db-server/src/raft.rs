//! Raft integration, re-exported from `fluree-db-consensus`.
//!
//! The assembly used to live here and is kept at this path so
//! `fluree_db_server::raft::{RaftIntegration, RaftBootstrapConfig}`
//! keep resolving. It moved because nothing in it depended on the
//! server: an embedding process builds the same node the same way.

pub use fluree_db_consensus::raft::integration::*;
