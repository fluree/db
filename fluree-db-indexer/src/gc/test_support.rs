//! Shared fixtures for garbage-collection and sweep tests.

use fluree_db_binary_index::{
    BinaryGarbageRef, BinaryPrevIndexRef, DictPackRefs, DictRefs, DictTreeRefs, IndexRoot,
};
use fluree_db_core::{content_address, ContentId, ContentKind};
use std::collections::BTreeMap;

/// Build a minimal FIR6 root.
///
/// `dict_branch` becomes the reverse dictionary tree's branch CID, which is
/// the root's only CAS reference — tests that need a root to hold a live
/// artifact reference supply a real one.
pub(crate) fn minimal_fir6_for(
    ledger_id: &str,
    t: i64,
    prev_index: Option<BinaryPrevIndexRef>,
    garbage: Option<BinaryGarbageRef>,
    dict_branch: ContentId,
) -> Vec<u8> {
    let dict_tree = DictTreeRefs {
        branch: dict_branch,
        leaves: Vec::new(),
    };
    let root = IndexRoot {
        ledger_id: ledger_id.to_string(),
        index_t: t,
        base_t: 0,
        subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
        namespace_codes: BTreeMap::new(),
        predicate_sids: Vec::new(),
        graph_iris: Vec::new(),
        datatype_iris: Vec::new(),
        language_tags: Vec::new(),
        dict_refs: DictRefs {
            forward_packs: DictPackRefs {
                string_fwd_packs: Vec::new(),
                subject_fwd_ns_packs: Vec::new(),
            },
            subject_reverse: dict_tree.clone(),
            string_reverse: dict_tree,
        },
        subject_watermarks: Vec::new(),
        string_watermark: 0,
        lex_sorted_string_ids: false,
        total_commit_size: 0,
        total_asserts: 0,
        total_retracts: 0,
        graph_arenas: Vec::new(),
        default_graph_orders: Vec::new(),
        named_graphs: Vec::new(),
        stats: None,
        schema: None,
        prev_index,
        garbage,
        sketch_ref: None,
        has_annotations: false,
        annotation_index: None,
        had_annotation_arena: false,
        has_list_meta: None,
        o_type_table: IndexRoot::build_o_type_table(&[], &[]),
        ns_split_mode: fluree_db_core::ns_encoding::NsSplitMode::default(),
    };
    root.encode()
}

/// A CID and the memory-storage address its blob belongs at.
pub(crate) fn cid_and_addr_for(
    ledger_id: &str,
    kind: ContentKind,
    data: &[u8],
) -> (ContentId, String) {
    let cid = ContentId::new(kind, data);
    let addr = content_address("memory", kind, ledger_id, &cid.digest_hex());
    (cid, addr)
}
