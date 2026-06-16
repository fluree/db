use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::Sid;
use std::io;

/// Translate a **snapshot-space** `Sid` into a persisted subject ID (`s_id`) in
/// the binary store.
///
/// # Namespace-space contract
///
/// Implements invariants #1/#2 documented on
/// `BinaryScanOperator::extract_bound_terms_snapshot`:
///
/// 1. **Fast path (canonical agreement).** For a prefix present at index time,
///    the snapshot and store assign the same code, so we look the Sid up
///    directly by parts (`find_subject_id_by_parts`) — no IRI reconstruction.
/// 2. **Fallback (post-index namespace).** If the by-parts lookup misses, the
///    code may be a snapshot-only post-index allocation the store never saw; we
///    decode the Sid to its full IRI via the store's table and re-find it.
///
/// Returns `Ok(None)` when the subject is genuinely not persisted (novelty-only,
/// or a post-index namespace the store can't reconstruct) — the caller then
/// serves it from novelty, never as an empty result.
#[inline]
pub(crate) fn sid_to_store_s_id(store: &BinaryIndexStore, sid: &Sid) -> io::Result<Option<u64>> {
    if let Some(s_id) = store.find_subject_id_by_parts(sid.namespace_code.as_u16(), &sid.name)? {
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
