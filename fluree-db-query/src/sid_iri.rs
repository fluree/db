use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::Sid;
use std::io;

/// Translate a query-space `Sid` into a persisted subject ID filter (`s_id`) for the binary store.
///
/// Under canonical encoding (immutable code↔prefix bimap), the snapshot and store
/// namespace tables agree, so we can look up directly by parts without IRI reconstruction.
///
/// Returns `Ok(None)` when the subject is not present in the persisted dictionaries
/// (common for novelty-only subjects or post-index namespace allocations).
#[inline]
pub(crate) fn sid_to_store_s_id(store: &BinaryIndexStore, sid: &Sid) -> io::Result<Option<u64>> {
    if let Some(s_id) = store.find_subject_id_by_parts(sid.namespace_code, &sid.name)? {
        return Ok(Some(s_id));
    }
    match store.sid_to_iri(sid) {
        Some(iri) => store.find_subject_id(&iri),
        None => Ok(None),
    }
}

/// Translate a query-space `Sid` into a persisted predicate ID filter (`p_id`) for the binary store.
///
/// Under canonical encoding (immutable code↔prefix bimap), the snapshot and store
/// namespace tables agree, so we can use `sid_to_p_id` directly without re-encoding
/// through a "store-space" SID.
///
/// Returns `None` when the namespace code is unknown or the predicate is not in the
/// persisted dictionary (novelty-only predicate).
#[inline]
pub(crate) fn sid_to_store_p_id(store: &BinaryIndexStore, sid: &Sid) -> Option<u32> {
    store.sid_to_p_id(sid)
}
