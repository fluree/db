//! Cross-ledger model enforcement.
//!
//! Resolution of `f:GraphRef` whose `f:ledger` targets a different
//! ledger on the same instance: the model ledger holds governance
//! artifacts (policy / shapes / schema / rules / constraints) that
//! are applied to requests against the data ledger.
//!
//! Contract and semantics: see
//! `docs/design/cross-ledger-model-enforcement.md`.

pub mod error;

pub use error::CrossLedgerError;
