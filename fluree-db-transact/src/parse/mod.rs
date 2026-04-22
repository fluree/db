//! Transaction parsing
//!
//! This module provides parsers for converting JSON-LD transaction
//! representations into the internal Transaction IR.

pub mod jsonld;
pub mod trig_meta;
pub mod txn_meta;

pub use jsonld::parse_transaction;
pub use trig_meta::{
    extract_trig_txn_meta, parse_trig_phase1, resolve_trig_meta, NamedGraphBlock, RawObject,
    RawTerm, RawTrigMeta, RawTriple, TrigMetaResult, TrigPhase1Result, TXN_META_GRAPH_IRI,
};
pub use txn_meta::extract_txn_meta;
