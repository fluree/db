//! Fast-path: `COUNT(*)` for a 2-pattern multicolumn join on `(?s, ?o)`.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE { ?s <p1> ?o . ?s <p2> ?o }
//! ```
//!
//! This is a natural join on both variables (not a cartesian star join).
//! We can compute it as the size of the intersection of the two predicate relations
//! on the composite key `(s_id, o_type, o_key)` using a streaming merge join in PSOT order.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, fast_path_store, leaf_entries_for_predicate, normalize_pred_sid,
    projection_sid_otype_okey, FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::ir::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;
use std::cmp::Ordering;

/// Create a fused operator that outputs a single-row batch with the COUNT(*) result.
pub fn multicolumn_join_count_all_operator(
    p1: Ref,
    p2: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let n = count_multicolumn_join_psot(store, ctx.binary_g_id, &p1, &p2)?;
            let n_i64 = i64::try_from(n).map_err(|_| {
                QueryError::execution("COUNT(*) exceeds i64 in multicolumn join fast-path")
            })?;
            Ok(Some(build_count_batch(out_var, n_i64)?))
        },
        fallback,
        "multicolumn-join COUNT(*)",
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SoKey {
    s: u64,
    o_type: u16,
    o_key: u64,
}

impl Ord for SoKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.s
            .cmp(&other.s)
            .then_with(|| self.o_type.cmp(&other.o_type))
            .then_with(|| self.o_key.cmp(&other.o_key))
    }
}

impl PartialOrd for SoKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct PsotSoIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    leaf_entries: &'a [fluree_db_binary_index::format::branch::LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<fluree_db_binary_index::ColumnBatch>,
    projection: fluree_db_binary_index::ColumnProjection,
}

impl<'a> PsotSoIter<'a> {
    fn new(store: &'a BinaryIndexStore, g_id: GraphId, p_id: u32) -> Self {
        let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
        Self {
            store,
            p_id,
            leaf_entries: leaves,
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            projection: projection_sid_otype_okey(),
        }
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        loop {
            if self.handle.is_none() {
                if self.leaf_pos >= self.leaf_entries.len() {
                    return Ok(None);
                }
                let leaf_entry = &self.leaf_entries[self.leaf_pos];
                self.leaf_pos += 1;
                self.leaflet_idx = 0;
                self.row = 0;
                self.batch = None;
                self.handle = Some(
                    self.store
                        .open_leaf_handle(
                            &leaf_entry.leaf_cid,
                            leaf_entry.sidecar_cid.as_ref(),
                            false,
                        )
                        .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                );
            }

            let handle = self.handle.as_ref().unwrap();
            let dir = handle.dir();
            while self.leaflet_idx < dir.entries.len() {
                let entry = &dir.entries[self.leaflet_idx];
                let idx = self.leaflet_idx;
                self.leaflet_idx += 1;
                if entry.row_count == 0 || entry.p_const != Some(self.p_id) {
                    continue;
                }
                let batch = handle
                    .load_columns(idx, &self.projection, RunSortOrder::Psot)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                self.row = 0;
                self.batch = Some(batch);
                return Ok(Some(()));
            }

            self.handle = None;
        }
    }

    fn next_row(&mut self) -> Result<Option<SoKey>> {
        loop {
            if self.batch.is_none() && self.load_next_batch()?.is_none() {
                return Ok(None);
            }
            let batch = self.batch.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }
            let key = SoKey {
                s: batch.s_id.get(self.row),
                o_type: batch.o_type.get(self.row),
                o_key: batch.o_key.get(self.row),
            };
            self.row += 1;
            return Ok(Some(key));
        }
    }
}

fn count_multicolumn_join_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
) -> Result<u64> {
    let p1_sid = normalize_pred_sid(store, p1)?;
    let p2_sid = normalize_pred_sid(store, p2)?;

    let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
        return Ok(0);
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(0);
    };

    let mut it1 = PsotSoIter::new(store, g_id, p1_id);
    let mut it2 = PsotSoIter::new(store, g_id, p2_id);

    let mut a = it1.next_row()?;
    let mut b = it2.next_row()?;
    let mut count: u64 = 0;

    while let (Some(ka), Some(kb)) = (a, b) {
        match ka.cmp(&kb) {
            Ordering::Less => a = it1.next_row()?,
            Ordering::Greater => b = it2.next_row()?,
            Ordering::Equal => {
                count = count.saturating_add(1);
                a = it1.next_row()?;
                b = it2.next_row()?;
            }
        }
    }

    Ok(count)
}
