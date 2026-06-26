//! Transaction parsing
//!
//! This module provides parsers for converting JSON-LD transaction
//! representations into the internal Transaction IR.

pub mod edge_annotations;
pub mod jsonld;
pub mod nquads;
pub mod trig_meta;
pub mod txn_meta;

/// Top-level keys reserved by the transactor — control and dataset-scoping
/// keys that are NEVER interpreted as RDF data predicates.
///
/// Centralized here because several independent stages must agree on this
/// set. Past drift between them silently corrupted data: when a routing key
/// (e.g. the body-form `ledger`) rode along in a document, the annotation
/// lowering pass misclassified the envelope as a plain data node, skipped
/// `@graph`, and dropped `@annotation` reification with no error. The stages:
///
/// - [`jsonld::strip_opts_for_expansion`] removes these before JSON-LD
///   expansion so the single-object form does not leak them as predicates.
/// - [`edge_annotations::is_envelope`] / [`edge_annotations::is_transaction_wrapper`]
///   tolerate them when classifying a document as an envelope / wrapper
///   rather than a data node-map, so annotation lowering still recurses into
///   `@graph` / clause values.
///
/// Roles: `opts` / `txn-meta` are parse-time sidecars; `ledger` is HTTP
/// routing; `from` / `fromNamed` / `from-named` / `graph` are dataset /
/// default-graph selectors consumed by UPDATE (and pure routing noise on
/// INSERT / UPSERT, where the strip runs).
pub(crate) const RESERVED_TXN_KEYS: &[&str] = &[
    "opts",
    "txn-meta",
    "ledger",
    "from",
    "fromNamed",
    "from-named",
    "graph",
];

/// Clause keys that carry an UPDATE wrapper's data sub-payloads. Distinct
/// from [`RESERVED_TXN_KEYS`]: these hold data and must be recursed into,
/// never stripped.
pub(crate) const CLAUSE_KEYS: &[&str] = &["where", "delete", "insert", "upsert", "values"];

pub use jsonld::parse_transaction;
pub use nquads::nquads_to_trig;
pub use trig_meta::{
    extract_trig_txn_meta, parse_trig_phase1, resolve_trig_meta, NamedGraphBlock, RawObject,
    RawTerm, RawTrigMeta, RawTriple, TrigMetaResult, TrigPhase1Result, TXN_META_GRAPH_IRI,
};
pub use txn_meta::extract_txn_meta;
