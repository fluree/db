//! Distinct-group counts from leaflet directories.
//!
//! A leaflet's `lead_group_count` is the number of distinct groups (the key
//! prefix [`live_group_len`] names) among its rows. Over a run of leaflets in
//! key order the distinct groups are the sum of those counts, less one wherever
//! a leaflet's first group is the previous leaflet's last group.
//!
//! That boundary rule needs the groups of the rows themselves. A leaflet's
//! routing keys do not give them: the writer widens the keys over the history
//! the leaflet holds, which can name a group the leaflet holds no row of. A v5
//! leaflet stores its rows' first and last groups ([`LiveEdges::Exact`]); a v4
//! leaflet stores none, so where it holds history and its routing keys meet a
//! neighbour's, this walk reads the group from its lead column.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{parallel_dir_walk_min_leaves, parallel_leaf_chunk_reduce_at};
use fluree_db_binary_index::format::branch::LeafEntry;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::leaf::{LeafletDirEntryV3, LiveEdges};
use fluree_db_binary_index::format::leaflet::LiveGroup;
use fluree_db_binary_index::format::run_record::{RunSortOrder, LIST_INDEX_NONE};
use fluree_db_binary_index::format::run_record_v2::{
    write_ordered_key_v2, RunRecordV2, ORDERED_KEY_V2_SIZE,
};
use fluree_db_binary_index::read::column_types::{ColumnBatch, ColumnProjection, ColumnSet};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::subject_id::SubjectId;

/// What a walk does with one leaflet.
pub(crate) enum Role {
    /// Not part of the count.
    Skip,
    /// Not counted as groups; its rows are tallied instead.
    Tally(u64),
    /// Counted by its `lead_group_count` and live edges.
    Groups,
    /// Counted by a run the caller read from its rows: `count` distinct groups
    /// from `first` to `last`.
    Run {
        count: u64,
        first: LiveGroup,
        last: LiveGroup,
    },
}

/// Result of a walk: distinct groups, and the rows of leaflets the role
/// tallied instead of counting.
pub(crate) struct LeadWalk {
    pub count: u64,
    pub tally: u64,
}

/// One end of a counted run of leaflets.
#[derive(Clone, Copy)]
struct Edge {
    group: LiveGroup,
    /// For a v4 leaflet whose routing keys may be widened: where its row is
    /// (leaf index in the walk's leaves, leaflet index, which end). `group` is
    /// then a bound, not a row's group.
    unproven: Option<(usize, usize, End)>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum End {
    First,
    Last,
}

struct Partial {
    count: u64,
    tally: u64,
    first: Option<Edge>,
    last: Option<Edge>,
    /// Chunk seams whose routing groups meet but whose rows' groups were not
    /// read yet: resolved after the reduce, so `combine` stays free of I/O.
    pending: Vec<(Edge, Edge)>,
}

impl Partial {
    fn new() -> Self {
        Self {
            count: 0,
            tally: 0,
            first: None,
            last: None,
            pending: Vec::new(),
        }
    }
}

/// Distinct groups of `order` over `leaves`, from their directories, with
/// `role` saying what to do with each leaflet. `None` when a leaflet it counts
/// by [`Role::Groups`] predates `lead_group_count`.
pub(crate) fn distinct_lead_walk<R>(
    store: &BinaryIndexStore,
    order: RunSortOrder,
    leaves: &[LeafEntry],
    role: R,
) -> Result<Option<LeadWalk>>
where
    R: Fn(&LeafEntry, usize, &LeafletDirEntryV3) -> Result<Role> + Sync + Send,
{
    if leaves.is_empty() {
        return Ok(Some(LeadWalk { count: 0, tally: 0 }));
    }

    let map = |start: usize, chunk: &[LeafEntry]| -> Result<Option<Partial>> {
        let mut partial = Partial::new();
        for (offset, leaf) in chunk.iter().enumerate() {
            let leaf_idx = start + offset;
            let dir = store
                .open_leaf_dir(&leaf.leaf_cid)
                .map_err(|e| QueryError::Internal(format!("leaf dir open: {e}")))?;
            for (idx, entry) in dir.entries.iter().enumerate() {
                let (count, first, last) = match role(leaf, idx, entry)? {
                    Role::Skip => continue,
                    Role::Tally(rows) => {
                        partial.tally = partial.tally.saturating_add(rows);
                        continue;
                    }
                    Role::Run { count, first, last } => (
                        count,
                        Edge {
                            group: first,
                            unproven: None,
                        },
                        Edge {
                            group: last,
                            unproven: None,
                        },
                    ),
                    Role::Groups => {
                        if entry.row_count == 0 {
                            continue;
                        }
                        if entry.lead_group_count == 0 {
                            return Ok(None);
                        }
                        let (first, last) = leaflet_edges(order, leaf_idx, idx, entry);
                        (u64::from(entry.lead_group_count), first, last)
                    }
                };
                partial.count = partial.count.saturating_add(count);
                if let Some(prev) = partial.last {
                    if seam_shares_group(store, order, leaves, prev, first)? {
                        partial.count = partial.count.saturating_sub(1);
                    }
                }
                partial.first.get_or_insert(first);
                partial.last = Some(last);
            }
        }
        Ok(Some(partial))
    };

    let combine = |mut left: Partial, right: Partial| -> Partial {
        left.tally = left.tally.saturating_add(right.tally);
        left.pending.extend(right.pending);
        let (Some(last), Some(first)) = (left.last, right.first) else {
            if left.first.is_none() {
                left.first = right.first;
            }
            if right.last.is_some() {
                left.last = right.last;
            }
            left.count = left.count.saturating_add(right.count);
            return left;
        };
        left.count = left.count.saturating_add(right.count);
        if last.group == first.group {
            if last.unproven.is_none() && first.unproven.is_none() {
                left.count = left.count.saturating_sub(1);
            } else {
                left.pending.push((last, first));
            }
        }
        left.last = right.last;
        left
    };

    let parallel = leaves.len() >= parallel_dir_walk_min_leaves();
    let Some(mut partial) = parallel_leaf_chunk_reduce_at(leaves, parallel, map, combine)? else {
        return Ok(None);
    };
    for (last, first) in std::mem::take(&mut partial.pending) {
        if seam_shares_group(store, order, leaves, last, first)? {
            partial.count = partial.count.saturating_sub(1);
        }
    }
    Ok(Some(LeadWalk {
        count: partial.count,
        tally: partial.tally,
    }))
}

/// The first and last edges of a leaflet with rows.
fn leaflet_edges(
    order: RunSortOrder,
    leaf: usize,
    idx: usize,
    entry: &LeafletDirEntryV3,
) -> (Edge, Edge) {
    match entry.live_edges {
        LiveEdges::Exact { first, last } => (
            Edge {
                group: first,
                unproven: None,
            },
            Edge {
                group: last,
                unproven: None,
            },
        ),
        LiveEdges::Empty | LiveEdges::Legacy => {
            let widened = entry.holds_history();
            let edge = |key: &[u8; ORDERED_KEY_V2_SIZE], end| Edge {
                group: LiveGroup::of_key(order, key),
                unproven: widened.then_some((leaf, idx, end)),
            };
            (
                edge(&entry.first_key, End::First),
                edge(&entry.last_key, End::Last),
            )
        }
    }
}

/// Whether the leaflet ending in `last` and the one starting at `first` share
/// a group, so that the walk counted it twice.
///
/// Routing groups that differ settle it: a leaflet's routing keys bound its
/// rows, and neighbouring leaflets' routing ranges do not overlap, so rows of
/// one group on both sides put both routing groups at that group. Only when
/// they meet does an unproven side need its row's group read.
fn seam_shares_group(
    store: &BinaryIndexStore,
    order: RunSortOrder,
    leaves: &[LeafEntry],
    last: Edge,
    first: Edge,
) -> Result<bool> {
    if last.group != first.group {
        return Ok(false);
    }
    let right = match first.unproven {
        Some((leaf, idx, end)) => read_row_group(store, order, &leaves[leaf], idx, end)?,
        None => first.group,
    };
    if right != first.group {
        return Ok(false);
    }
    let left = match last.unproven {
        Some((leaf, idx, end)) => read_row_group(store, order, &leaves[leaf], idx, end)?,
        None => last.group,
    };
    Ok(left == right)
}

/// The group of a leaflet's first row, or its last when `last`: stored in a
/// v5 leaflet; a v4 leaflet's routing key gives it where the leaflet holds no
/// history, and its lead column otherwise. `None` for a leaflet with no rows.
pub(crate) fn leaflet_row_group(
    store: &BinaryIndexStore,
    order: RunSortOrder,
    leaf: &LeafEntry,
    idx: usize,
    entry: &LeafletDirEntryV3,
    last: bool,
) -> Result<Option<LiveGroup>> {
    let end = if last { End::Last } else { End::First };
    match entry.live_edges {
        LiveEdges::Exact { first, last: l } => Ok(Some(if last { l } else { first })),
        LiveEdges::Empty => Ok(None),
        LiveEdges::Legacy if entry.row_count == 0 => Ok(None),
        LiveEdges::Legacy if !entry.holds_history() => Ok(Some(LiveGroup::of_key(
            order,
            if last {
                &entry.last_key
            } else {
                &entry.first_key
            },
        ))),
        LiveEdges::Legacy => read_row_group(store, order, leaf, idx, end).map(Some),
    }
}

/// The object `(o_type, o_key)` a POST group names, after its predicate.
pub(crate) fn post_group_object(group: &LiveGroup) -> (u16, u64) {
    let b = group.as_bytes();
    (
        u16::from_be_bytes([b[4], b[5]]),
        u64::from_be_bytes(b[6..14].try_into().expect("a POST group is 14 bytes")),
    )
}

/// The group of a v4 leaflet's first or last row, read from its lead column.
fn read_row_group(
    store: &BinaryIndexStore,
    order: RunSortOrder,
    leaf: &LeafEntry,
    idx: usize,
    end: End,
) -> Result<LiveGroup> {
    let dir = store
        .open_leaf_dir(&leaf.leaf_cid)
        .map_err(|e| QueryError::Internal(format!("leaf dir open: {e}")))?;
    let entry = dir
        .entries
        .get(idx)
        .ok_or_else(|| QueryError::Internal(format!("leaflet {idx} not in its leaf")))?;
    let handle = store
        .open_leaf_handle(&leaf.leaf_cid, leaf.sidecar_cid.as_ref(), false)
        .map_err(|e| QueryError::from_io("leaf open", e))?;
    let batch = handle
        .load_columns(idx, &group_projection(order), order)
        .map_err(|e| QueryError::Internal(format!("leaflet columns: {e}")))?;
    let row = match end {
        _ if batch.row_count == 0 => {
            return Err(QueryError::Internal(
                "a leaflet with rows decoded to none".to_string(),
            ))
        }
        End::First => 0,
        End::Last => batch.row_count - 1,
    };
    Ok(group_of_row(order, entry, &batch, row))
}

/// The group of `row` in `batch`, a leaflet's columns loaded with
/// [`group_projection`], filling what the leaflet holds constant from `entry`.
fn group_of_row(
    order: RunSortOrder,
    entry: &LeafletDirEntryV3,
    batch: &ColumnBatch,
    row: usize,
) -> LiveGroup {
    let rec = RunRecordV2 {
        s_id: SubjectId(batch.s_id.get_or(row, 0)),
        o_key: batch.o_key.get_or(row, 0),
        p_id: batch.p_id.get_or(row, entry.p_const.unwrap_or(0)),
        t: 0,
        o_i: LIST_INDEX_NONE,
        o_type: batch.o_type.get_or(row, entry.o_type_const.unwrap_or(0)),
        g_id: batch.g_id.get_or(row, 0),
    };
    let mut key = [0u8; ORDERED_KEY_V2_SIZE];
    write_ordered_key_v2(order, &rec, &mut key);
    LiveGroup::of_key(order, &key)
}

/// The columns that hold `order`'s group beyond the leaflet's constants.
fn group_projection(order: RunSortOrder) -> ColumnProjection {
    let mut internal = ColumnSet::EMPTY;
    match order.base() {
        RunSortOrder::Spot | RunSortOrder::Psot => internal.insert(ColumnId::SId),
        // POST and OPST: the object.
        _ => {
            internal.insert(ColumnId::OType);
            internal.insert(ColumnId::OKey);
        }
    }
    if order == RunSortOrder::Gspo {
        internal.insert(ColumnId::GId);
    }
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_binary_index::format::leaf::{
        decode_leaf_dir_v3_with_base, decode_leaf_header_v3, LeafWriter,
    };
    use fluree_db_binary_index::format::run_record_v2::cmp_v2_for_order;
    use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
    use fluree_db_core::o_type::OType;

    fn rec(g_id: u32, s_id: u64, p_id: u32, o_type: u16, o_key: u64) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t: 1,
            o_i: LIST_INDEX_NONE,
            o_type,
            g_id,
        }
    }

    /// What a v4 leaflet's lead column gives for its first and last rows is
    /// the group this build's writer stores for them, in every order, so a
    /// v4 index's boundaries are judged by the same groups a v5 index stores.
    #[test]
    fn a_row_group_read_from_columns_is_the_group_the_writer_stores() {
        let int = OType::XSD_INTEGER.as_u16();
        let string = OType::XSD_STRING.as_u16();
        let mut records = Vec::new();
        for g in [1, 2] {
            for s in 1..=5u64 {
                for p in [3u32, 7] {
                    records.push(rec(g, s * 11, p, int, s * 100 + u64::from(p)));
                    records.push(rec(g, s * 11, p, string, s + u64::from(p)));
                }
            }
        }
        for order in [
            RunSortOrder::Spot,
            RunSortOrder::Psot,
            RunSortOrder::Post,
            RunSortOrder::Opst,
            RunSortOrder::Gspo,
            RunSortOrder::PostG,
        ] {
            let mut sorted = records.clone();
            if !order.is_pooled() {
                sorted.retain(|r| r.g_id == 1);
                sorted.iter_mut().for_each(|r| r.g_id = 0);
            }
            sorted.sort_by(cmp_v2_for_order(order));
            let mut writer = LeafWriter::new(order, 3, 10_000, 1);
            writer.set_skip_history(true);
            for r in sorted {
                writer.push_record(r).unwrap();
            }
            let mut checked = 0;
            for leaf in writer.finish().unwrap() {
                let bytes = &leaf.leaf_bytes;
                let header = decode_leaf_header_v3(bytes).unwrap();
                let dir = decode_leaf_dir_v3_with_base(bytes, &header).unwrap();
                for entry in &dir.entries {
                    let LiveEdges::Exact { first, last } = entry.live_edges else {
                        continue;
                    };
                    let batch = load_leaflet_columns(
                        bytes,
                        entry,
                        dir.payload_base,
                        &group_projection(order),
                        order,
                    )
                    .unwrap();
                    let n = batch.row_count;
                    assert_eq!(group_of_row(order, entry, &batch, 0), first, "{order:?}");
                    assert_eq!(group_of_row(order, entry, &batch, n - 1), last, "{order:?}");
                    checked += 1;
                }
            }
            assert!(checked > 1, "{order:?}: several leaflets checked");
        }
    }

    /// A v4 leaflet's routing keys stand for its rows unless it holds history:
    /// an empty history segment (4 bytes) or none at all leaves them exact.
    #[test]
    fn a_v4_leaflets_edges_are_unproven_only_where_it_holds_history() {
        let mut key = [0u8; ORDERED_KEY_V2_SIZE];
        write_ordered_key_v2(RunSortOrder::Spot, &rec(0, 9, 1, 0, 0), &mut key);
        let entry = |live_edges, history_len| LeafletDirEntryV3 {
            row_count: 1,
            lead_group_count: 1,
            first_key: key,
            last_key: key,
            live_edges,
            p_const: None,
            o_type_const: None,
            flags: 0,
            payload_offset: 0,
            payload_len: 0,
            column_refs: Vec::new(),
            history_offset: 0,
            history_len,
            history_min_t: 0,
            history_max_t: 0,
        };
        let unproven = |e: &LeafletDirEntryV3| {
            let (first, last) = leaflet_edges(RunSortOrder::Spot, 0, 0, e);
            (first.unproven.is_some(), last.unproven.is_some())
        };
        assert_eq!(unproven(&entry(LiveEdges::Legacy, 0)), (false, false));
        assert_eq!(unproven(&entry(LiveEdges::Legacy, 4)), (false, false));
        assert_eq!(unproven(&entry(LiveEdges::Legacy, 40)), (true, true));
        let group = LiveGroup::of_key(RunSortOrder::Spot, &key);
        let exact = LiveEdges::Exact {
            first: group,
            last: group,
        };
        assert_eq!(unproven(&entry(exact, 40)), (false, false));
    }
}
