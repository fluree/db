//! Shared types for the build pipeline.

use fluree_db_binary_index::format::branch::LeafEntry;
use fluree_db_binary_index::format::index_root::NamedGraphRouting;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::{DictRefs, DictTreeRefs, VectorDictRef};
use fluree_db_core::ContentId;
use std::collections::BTreeMap;

/// Result of uploading index artifacts to CAS (FLI3/FBR3/FHS1).
///
/// Default graph (g_id=0) collects inline `LeafEntry` for root embedding.
/// Named graphs upload branch manifests and return branch CIDs.
pub struct UploadedIndexes {
    /// Default graph (g_id=0): inline leaf entries per sort order.
    pub default_graph_orders: Vec<(RunSortOrder, Vec<LeafEntry>)>,
    /// Named graphs (g_id!=0): branch CIDs per sort order per graph.
    pub named_graphs: Vec<NamedGraphRouting>,
}

/// Result of uploading persisted dict flat files to CAS.
///
/// Contains the CAS addresses for all dictionary artifacts plus derived metadata
/// needed for building the index root.
#[derive(Debug)]
pub struct UploadedDicts {
    pub dict_refs: DictRefs,
    pub subject_id_encoding: fluree_db_core::SubjectIdEncoding,
    pub subject_watermarks: Vec<u64>,
    pub string_watermark: u32,
    /// Graph IRIs by dict_index (0-based). `g_id = dict_index + 1`.
    pub graph_iris: Vec<String>,
    /// Datatype IRIs by dt_id (0-based).
    pub datatype_iris: Vec<String>,
    /// Language tags by (lang_id - 1). `lang_id = index + 1`, 0 = "no tag".
    pub language_tags: Vec<String>,
    /// Numbig arena CIDs: g_id_str → (p_id_str → CID).
    pub numbig: BTreeMap<String, BTreeMap<String, ContentId>>,
    /// Vector arena refs: g_id_str → (p_id_str → VectorDictRef).
    pub vectors: BTreeMap<String, BTreeMap<String, VectorDictRef>>,
}

/// Result of uploading an incrementally-updated reverse dictionary tree.
pub(crate) struct UpdatedReverseTree {
    pub(crate) tree_refs: DictTreeRefs,
    pub(crate) replaced_cids: Vec<ContentId>,
}
