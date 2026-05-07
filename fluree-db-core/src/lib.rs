//! # Fluree DB Core
//!
//! Runtime-agnostic core library for Fluree DB queries.
//!
//! This crate provides:
//! - Core types: `Sid`, `FlakeValue`, `Flake`
//! - Index comparators for all 4 orderings (SPOT, PSOT, POST, OPST)
//! - Storage and cache trait interfaces
//! - Range query implementation
//!
//! ## Design Principles
//!
//! 1. **Runtime-agnostic**: No tokio, no forced `Send + Sync`
//! 2. **Async at I/O seam only**: Synchronous traversal once data is in memory
//! 3. **Strict total ordering**: No nil-as-wildcard; use explicit min/max bounds
//!
//! ## Example
//!
//! ```ignore
//! use fluree_db_core::{LedgerSnapshot, range, IndexType, RangeTest};
//!
//! // Apps provide their own Storage implementation
//! let db = load_ledger_snapshot(&storage, &root_id, "ledger/main").await?;
//! let flakes = range(&db, IndexType::Spot, RangeTest::Eq, match_val, opts).await?;
//! ```

pub mod address;
pub mod address_path;
pub mod coerce;
pub mod commit;
pub mod comparator;
pub mod conflict_key;
pub mod content_id;
pub mod content_kind;
pub mod datatype_constraint;
pub mod datatypes;
pub mod db;
pub mod dict_novelty;
pub mod error;
pub mod flake;
pub mod geo;
pub mod graph_db_ref;
pub mod graph_registry;
pub mod ids;
pub mod index_schema;
pub mod index_stats;
pub mod ledger_config;
pub mod ledger_id;
pub mod namespaces;
pub mod ns_encoding;
pub mod nonempty;
pub mod ns_vec_bi_dict;
pub mod o_type;
pub mod o_type_registry;
pub mod overlay;
pub mod pack;
pub mod prefix_trie;
pub mod query_bounds;
pub mod range;
pub mod range_provider;
pub mod runtime_small_dicts;
pub mod schema_hierarchy;
pub mod serde;
pub mod sid;
pub mod stats_view;
pub mod stats_wire;
pub mod storage;
pub mod subject_id;
pub mod temporal;
pub mod tracking;
pub mod value;
pub mod value_id;
pub mod vec_bi_dict;

// Re-export main types
pub use address::{
    extract_identifier, extract_ledger_prefix, extract_path, parse_fluree_address,
    ParsedFlureeAddress,
};
pub use coerce::{coerce_json_value, coerce_value, CoercionError, CoercionResult};
pub use commit::{
    collect_dag_cids, collect_dag_cids_with_split_mode, commit_to_summary, find_common_ancestor,
    first_t_where_graph_registered, load_commit_by_id, load_commit_envelope_by_id,
    trace_commit_envelopes_by_id, trace_commits_by_id, walk_commit_summaries, Commit,
    CommitEnvelope, CommitSummary, CommonAncestor, GraphRegistrationProbe, TxnMetaEntry,
    TxnMetaValue, TxnSignature, MAX_TXN_META_BYTES, MAX_TXN_META_ENTRIES,
};
pub use comparator::IndexType;
pub use conflict_key::ConflictKey;
pub use content_id::{CommitId, ContentId, IndexRootId, TxnId};
pub use content_kind::{
    ContentKind, DictKind, CODEC_FLUREE_COMMIT, CODEC_FLUREE_DICT_BLOB, CODEC_FLUREE_GARBAGE,
    CODEC_FLUREE_GRAPH_SOURCE_SNAPSHOT, CODEC_FLUREE_HISTORY_SIDECAR, CODEC_FLUREE_INDEX_BRANCH,
    CODEC_FLUREE_INDEX_LEAF, CODEC_FLUREE_INDEX_ROOT, CODEC_FLUREE_LEDGER_CONFIG,
    CODEC_FLUREE_STATS_SKETCH, CODEC_FLUREE_TXN,
};
pub use datatype_constraint::DatatypeConstraint;
pub use datatypes::dt_compatible;
pub use db::{load_ledger_snapshot, LedgerSnapshot, LedgerSnapshotMetadata};
pub use dict_novelty::DictNovelty;
pub use error::{Error, Result};
pub use flake::{Flake, FlakeMeta};
pub use graph_db_ref::GraphDbRef;
pub use graph_registry::{
    config_graph_iri, txn_meta_graph_iri, GraphRegistry, CONFIG_GRAPH_ID, DEFAULT_GRAPH_ID,
    FIRST_USER_GRAPH_ID, TXN_META_GRAPH_ID,
};
pub use ids::{
    DatatypeDictId, GraphId, LangId, ListIndex, PredicateId, RuntimeDatatypeId, RuntimePredicateId,
    StringId, TxnT,
};
pub use index_schema::{IndexSchema, SchemaPredicateInfo, SchemaPredicates};
pub use index_stats::{
    ClassPropertyUsage, ClassRefCount, ClassStatEntry, GraphPropertyStatEntry, GraphStatsEntry,
    IndexStats, PropertyStatEntry,
};
pub use ledger_id::{
    format_ledger_id, normalize_ledger_id, parse_ledger_id_with_time, split_ledger_id,
    split_time_travel_suffix, validate_branch_name, LedgerIdParseError, LedgerIdTimeSpec,
    ParsedLedgerId, DEFAULT_BRANCH,
};
pub use namespaces::{
    default_namespace_codes, is_owl_class_class, is_owl_datatype_property_class,
    is_owl_equivalent_class, is_owl_equivalent_property, is_owl_functional_property,
    is_owl_imports, is_owl_inverse_functional_property, is_owl_inverse_of,
    is_owl_object_property_class, is_owl_ontology_class, is_owl_same_as, is_owl_symmetric_property,
    is_owl_transitive_property, is_rdf_first, is_rdf_nil, is_rdf_property_class, is_rdf_rest,
    is_rdf_type, is_rdfs_domain, is_rdfs_range, is_rdfs_subclass_of, is_rdfs_subproperty_of,
    is_schema_class, is_schema_predicate,
};
pub use nonempty::NonEmpty;
pub use ns_encoding::{
    builtin_prefix_trie, canonical_split, NamespaceCodes, NsAllocError, NsLookup, NsSplitMode,
    HOST_PLUS_N_MAX,
};
pub use o_type::{DecodeKind, OType};
pub use o_type_registry::OTypeRegistry;
pub use overlay::{NoOverlay, OverlayProvider};
pub use prefix_trie::PrefixTrie;
pub use range::{
    flake_matches_range_eq, range, range_bounded_with_overlay, range_with_overlay, ObjectBounds,
    RangeMatch, RangeOptions, RangeTest, BATCHED_JOIN_SIZE,
};
pub use range_provider::{RangeProvider, RangeQuery};
pub use runtime_small_dicts::RuntimeSmallDicts;
pub use schema_hierarchy::SchemaHierarchy;
pub use sid::{Sid, SidInterner};
pub use stats_view::{PropertyStatData, StatsView};
pub use storage::{
    bridge_content_store,
    content_address,
    content_path,
    content_store_for,
    // Helper functions for storage implementations
    ledger_id_prefix_for_path,
    sha256_hex,
    BranchedContentStore,
    CasAction,
    CasOutcome,
    ContentAddressedWrite,
    ContentStore,
    ContentWriteResult,
    ListResult,
    MemoryContentStore,
    MemoryStorage,
    ReadHint,
    RemoteObject,
    Storage,
    StorageBackend,
    StorageCas,
    StorageContentStore,
    StorageDelete,
    StorageExtError,
    StorageExtResult,
    StorageList,
    StorageMethod,
    StorageRead,
    StorageWrite,
    STORAGE_METHOD_IPFS,
    STORAGE_METHOD_MEMORY,
    STORAGE_METHOD_S3,
};
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub use storage::{FileStorage, STORAGE_METHOD_FILE};
pub use subject_id::{SubjectId, SubjectIdColumn, SubjectIdEncoding};
pub use temporal::{Date, DateTime, Time};
pub use tracking::{FuelExceededError, PolicyStats, Tracker, TrackingOptions, TrackingTally};
pub use value::{
    parse_decimal, parse_decimal_string, parse_double, parse_integer, parse_integer_string,
    FlakeValue, GeoPointBits,
};
pub use value_id::{ObjKey, ObjKeyError, ObjKind, ObjPair, ValueTypeTag};

/// Prelude module for convenient imports of storage traits and common types.
///
/// # Example
///
/// ```ignore
/// use fluree_db_core::prelude::*;
///
/// async fn example<S: Storage>(storage: &S) {
///     let bytes = storage.read_bytes("address").await?;
///     storage.write_bytes("address", &bytes).await?;
/// }
/// ```
pub mod prelude {
    pub use crate::content_id::ContentId;
    pub use crate::content_kind::{ContentKind, DictKind};
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    pub use crate::storage::FileStorage;
    pub use crate::storage::{
        CasAction, CasOutcome, ContentAddressedWrite, ContentStore, ContentWriteResult, ListResult,
        MemoryContentStore, MemoryStorage, ReadHint, RemoteObject, Storage, StorageCas,
        StorageContentStore, StorageDelete, StorageExtError, StorageExtResult, StorageList,
        StorageRead, StorageWrite,
    };
}
