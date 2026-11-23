//! Distinct-group counts from leaflet directories.
//!
//! A leaflet's `lead_group_count` is the number of distinct groups (the key
//! prefix [`live_group_len`] names) among its rows. Over a run of leaflets in
//! key order the distinct groups are the sum of those counts, less one wherever
//! a leaflet's first group is the previous leaflet's last group.
//!
//! That boundary rule needs the groups of the rows themselves. A leaflet's
//! routing keys do not give them: the writer widens the keys over the history
//! the leaflet holds, which can name a group the leaflet holds no row of. So
//! where a leaflet holds history and its routing keys meet a neighbour's, this
//! walk reads the group from its lead column. A leaflet that holds no history
//! has routing keys that are its rows' keys.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    load_leaflet_projection, parallel_dir_walk_min_leaves, parallel_leaf_chunk_reduce_at,
};
use fluree_db_binary_index::format::branch::LeafEntry;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::leaf::LeafletDirEntryV3;
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
    /// As [`Role::Tally`], in the walk's tally of vector rows.
    TallyVectors(u64),
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
    pub vector_tally: u64,
}

/// Where a leaflet is: its leaf's index in the walk's leaves, and its index in
/// that leaf.
type At = (usize, usize);

/// The first and last groups of a counted leaflet.
#[derive(Clone, Copy)]
struct Edges {
    first: LiveGroup,
    last: LiveGroup,
    at: At,
    /// Whether `first` and `last` are its rows' groups. A leaflet that holds
    /// history has only its routing keys' groups, bounds of its rows' groups,
    /// until a boundary reads its rows.
    exact: bool,
}

impl Edges {
    /// Makes `first` and `last` the rows' groups, reading them if they are
    /// not. A leaflet is read at most once: the walk keeps the proven edges.
    fn prove(&mut self, read: &mut impl FnMut(At) -> Result<(LiveGroup, LiveGroup)>) -> Result<()> {
        if !self.exact {
            (self.first, self.last) = read(self.at)?;
            self.exact = true;
        }
        Ok(())
    }
}

struct Partial {
    count: u64,
    tally: u64,
    vector_tally: u64,
    first: Option<Edges>,
    last: Option<Edges>,
    /// Chunk seams whose routing groups meet but where a side was not proven
    /// yet: settled after the reduce, so `join` stays free of I/O.
    pending: Vec<(Edges, Edges)>,
}

impl Partial {
    fn new() -> Self {
        Self {
            count: 0,
            tally: 0,
            vector_tally: 0,
            first: None,
            last: None,
            pending: Vec::new(),
        }
    }

    /// Adds the next counted leaflet in key order: its `count` groups, less
    /// one where it starts with the group the previous leaflet ends with.
    fn push(
        &mut self,
        count: u64,
        mut edges: Edges,
        read: &mut impl FnMut(At) -> Result<(LiveGroup, LiveGroup)>,
    ) -> Result<()> {
        self.count = self.count.saturating_add(count);
        let Some(mut prev) = self.last.take() else {
            self.first = Some(edges);
            self.last = Some(edges);
            return Ok(());
        };
        if seam_shares_group(&mut prev, &mut edges, read)? {
            self.count = self.count.saturating_sub(1);
        }
        // What this seam proved of the first leaflet serves its chunk seam.
        if self.first.is_some_and(|first| first.at == prev.at) {
            self.first = Some(prev);
        }
        self.last = Some(edges);
        Ok(())
    }

    /// `self` followed by the partial of the leaves after it.
    fn join(mut self, right: Partial) -> Self {
        self.count = self.count.saturating_add(right.count);
        self.tally = self.tally.saturating_add(right.tally);
        self.vector_tally = self.vector_tally.saturating_add(right.vector_tally);
        if let (Some(last), Some(first)) = (self.last, right.first) {
            if last.last == first.first {
                if last.exact && first.exact {
                    self.count = self.count.saturating_sub(1);
                } else {
                    self.pending.push((last, first));
                }
            }
        }
        self.pending.extend(right.pending);
        if self.first.is_none() {
            self.first = right.first;
        }
        if right.last.is_some() {
            self.last = right.last;
        }
        self
    }

    /// Settles the pending seams, in key order. A chunk with one counted
    /// leaflet puts that leaflet on two seams, so what one seam proved of its
    /// right side serves the next seam's left.
    fn settle(
        mut self,
        read: &mut impl FnMut(At) -> Result<(LiveGroup, LiveGroup)>,
    ) -> Result<LeadWalk> {
        let mut proved: Option<Edges> = None;
        for (mut last, mut first) in std::mem::take(&mut self.pending) {
            if let Some(edges) = proved.filter(|edges| edges.at == last.at) {
                last = edges;
            }
            if seam_shares_group(&mut last, &mut first, read)? {
                self.count = self.count.saturating_sub(1);
            }
            proved = Some(first);
        }
        Ok(LeadWalk {
            count: self.count,
            tally: self.tally,
            vector_tally: self.vector_tally,
        })
    }
}

/// Distinct groups of `order` over `leaves`, from their directories, with
/// `role` saying what to do with each leaflet. `None` when a leaflet it counts
/// by [`Role::Groups`] predates `lead_group_count`.
pub(crate) fn distinct_lead_walk<R>(
    store: &BinaryIndexStore,
    order: RunSortOrder,
    leaves: &[LeafEntry],
    tracker: &fluree_db_core::Tracker,
    role: R,
) -> Result<Option<LeadWalk>>
where
    R: Fn(&LeafEntry, usize, &LeafletDirEntryV3) -> Result<Role> + Sync + Send,
{
    if leaves.is_empty() {
        return Ok(Some(LeadWalk {
            count: 0,
            tally: 0,
            vector_tally: 0,
        }));
    }

    let read = |(leaf, idx): At| read_row_groups(store, order, &leaves[leaf], idx, tracker);
    let map = |start: usize, chunk: &[LeafEntry]| -> Result<Option<Partial>> {
        let mut partial = Partial::new();
        let mut read = read;
        for (offset, leaf) in chunk.iter().enumerate() {
            let leaf_idx = start + offset;
            let dir = store
                .open_leaf_dir(&leaf.leaf_cid)
                .map_err(|e| QueryError::Internal(format!("leaf dir open: {e}")))?;
            for (idx, entry) in dir.entries.iter().enumerate() {
                let at = (leaf_idx, idx);
                let (count, edges) = match role(leaf, idx, entry)? {
                    Role::Skip => continue,
                    Role::Tally(rows) => {
                        partial.tally = partial.tally.saturating_add(rows);
                        continue;
                    }
                    Role::TallyVectors(rows) => {
                        partial.vector_tally = partial.vector_tally.saturating_add(rows);
                        continue;
                    }
                    Role::Run { count, first, last } => (
                        count,
                        Edges {
                            first,
                            last,
                            at,
                            exact: true,
                        },
                    ),
                    Role::Groups => {
                        if entry.row_count == 0 {
                            continue;
                        }
                        if entry.lead_group_count == 0 {
                            return Ok(None);
                        }
                        (
                            u64::from(entry.lead_group_count),
                            leaflet_edges(order, at, entry),
                        )
                    }
                };
                // Two exact edges settle by one comparison.
                match partial.last {
                    Some(prev) if prev.exact && edges.exact => {
                        partial.count = partial.count.saturating_add(count);
                        if prev.last == edges.first {
                            partial.count = partial.count.saturating_sub(1);
                        }
                        partial.last = Some(edges);
                    }
                    _ => partial.push(count, edges, &mut read)?,
                }
            }
        }
        Ok(Some(partial))
    };

    let parallel = leaves.len() >= parallel_dir_walk_min_leaves();
    let Some(partial) = parallel_leaf_chunk_reduce_at(leaves, parallel, map, Partial::join)? else {
        return Ok(None);
    };
    let mut read = read;
    partial.settle(&mut read).map(Some)
}

/// The edges of a leaflet with rows: its routing keys' groups, which are its
/// rows' groups where it holds no history.
fn leaflet_edges(order: RunSortOrder, at: At, entry: &LeafletDirEntryV3) -> Edges {
    Edges {
        first: LiveGroup::of_key(order, &entry.first_key),
        last: LiveGroup::of_key(order, &entry.last_key),
        at,
        exact: !entry.holds_history(),
    }
}

/// Whether the leaflet `left` ends with the group the leaflet `right` starts
/// with, so that the walk counted it twice. Proves a side it needs to read.
///
/// Groups that differ settle it, even where one side is a routing group: a
/// leaflet's routing keys bound its rows, and neighbouring leaflets' routing
/// ranges do not overlap, so rows of one group on both sides put both sides'
/// groups at that group. Only when they meet is an unproven side read: the
/// right one first, whose last group the next seam needs.
fn seam_shares_group(
    left: &mut Edges,
    right: &mut Edges,
    read: &mut impl FnMut(At) -> Result<(LiveGroup, LiveGroup)>,
) -> Result<bool> {
    let group = left.last;
    if group != right.first {
        return Ok(false);
    }
    right.prove(read)?;
    if right.first != group {
        return Ok(false);
    }
    left.prove(read)?;
    Ok(left.last == group)
}

/// The group of a leaflet's first row, or its last when `last`: its routing
/// key gives it where the leaflet holds no history, and its lead column
/// otherwise. `None` for a leaflet with no rows.
pub(crate) fn leaflet_row_group(
    store: &BinaryIndexStore,
    order: RunSortOrder,
    leaf: &LeafEntry,
    idx: usize,
    entry: &LeafletDirEntryV3,
    last: bool,
    tracker: &fluree_db_core::Tracker,
) -> Result<Option<LiveGroup>> {
    if entry.row_count == 0 {
        return Ok(None);
    }
    if !entry.holds_history() {
        let key = if last {
            &entry.last_key
        } else {
            &entry.first_key
        };
        return Ok(Some(LiveGroup::of_key(order, key)));
    }
    read_row_groups(store, order, leaf, idx, tracker)
        .map(|(first, l)| Some(if last { l } else { first }))
}

/// The object `(o_type, o_key)` a POST group names, after its predicate.
pub(crate) fn post_group_object(group: &LiveGroup) -> (u16, u64) {
    let b = group.as_bytes();
    (
        u16::from_be_bytes([b[4], b[5]]),
        u64::from_be_bytes(b[6..14].try_into().expect("a POST group is 14 bytes")),
    )
}

/// The groups of a leaflet's first and last rows, read from its lead
/// column: the columns [`group_projection`] names, through the leaflet cache.
/// A remote leaf is range-read, as the store's other walks that read a few
/// leaflets of many leaves do.
fn read_row_groups(
    store: &BinaryIndexStore,
    order: RunSortOrder,
    leaf: &LeafEntry,
    idx: usize,
    tracker: &fluree_db_core::Tracker,
) -> Result<(LiveGroup, LiveGroup)> {
    let handle = store
        .open_leaf_handle_ranged(&leaf.leaf_cid)
        .map_err(|e| QueryError::from_io("leaf open", e))?;
    let entry = handle
        .dir()
        .entries
        .get(idx)
        .ok_or_else(|| QueryError::Internal(format!("leaflet {idx} not in its leaf")))?;
    let batch = load_leaflet_projection(
        store,
        handle.as_ref(),
        idx,
        &group_projection(order),
        order,
        tracker,
    )?;
    if batch.row_count == 0 {
        return Err(QueryError::Internal(
            "a leaflet with rows decoded to none".to_string(),
        ));
    }
    Ok((
        group_of_row(order, entry, &batch, 0),
        group_of_row(order, entry, &batch, batch.row_count - 1),
    ))
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
        g_id: 0,
    };
    let mut key = [0u8; ORDERED_KEY_V2_SIZE];
    write_ordered_key_v2(order, &rec, &mut key);
    LiveGroup::of_key(order, &key)
}

/// The columns that hold `order`'s group beyond the leaflet's constants.
fn group_projection(order: RunSortOrder) -> ColumnProjection {
    let mut internal = ColumnSet::EMPTY;
    match order {
        RunSortOrder::Spot | RunSortOrder::Psot => internal.insert(ColumnId::SId),
        RunSortOrder::Post | RunSortOrder::Opst => {
            internal.insert(ColumnId::OType);
            internal.insert(ColumnId::OKey);
        }
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

    fn rec(s_id: u64, p_id: u32, o_type: u16, o_key: u64) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t: 1,
            o_i: LIST_INDEX_NONE,
            o_type,
            g_id: 0,
        }
    }

    /// What a leaflet's lead column gives for its first and last rows is the
    /// group its routing keys name where it holds no history, in every order,
    /// so a boundary read judges by the same groups a history-free leaflet's
    /// routing keys give.
    #[test]
    fn a_row_group_read_from_columns_is_the_group_of_an_exact_routing_key() {
        let int = OType::XSD_INTEGER.as_u16();
        let string = OType::XSD_STRING.as_u16();
        let mut records = Vec::new();
        for s in 1..=5u64 {
            for p in [3u32, 7] {
                records.push(rec(s * 11, p, int, s * 100 + u64::from(p)));
                records.push(rec(s * 11, p, string, s + u64::from(p)));
            }
        }
        for order in [
            RunSortOrder::Spot,
            RunSortOrder::Psot,
            RunSortOrder::Post,
            RunSortOrder::Opst,
        ] {
            let mut sorted = records.clone();
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
                    if entry.row_count == 0 {
                        continue;
                    }
                    assert!(!entry.holds_history(), "{order:?}");
                    let batch = load_leaflet_columns(
                        bytes,
                        entry,
                        dir.payload_base,
                        &group_projection(order),
                        order,
                    )
                    .unwrap();
                    let n = batch.row_count;
                    assert_eq!(
                        group_of_row(order, entry, &batch, 0),
                        LiveGroup::of_key(order, &entry.first_key),
                        "{order:?}"
                    );
                    assert_eq!(
                        group_of_row(order, entry, &batch, n - 1),
                        LiveGroup::of_key(order, &entry.last_key),
                        "{order:?}"
                    );
                    checked += 1;
                }
            }
            assert!(checked > 1, "{order:?}: several leaflets checked");
        }
    }

    /// A leaflet's routing keys stand for its rows unless it holds history:
    /// an empty history segment (4 bytes) or none at all leaves them exact.
    #[test]
    fn a_leaflets_edges_are_unproven_only_where_it_holds_history() {
        let mut key = [0u8; ORDERED_KEY_V2_SIZE];
        write_ordered_key_v2(RunSortOrder::Spot, &rec(9, 1, 0, 0), &mut key);
        let entry = |history_len| LeafletDirEntryV3 {
            row_count: 1,
            lead_group_count: 1,
            first_key: key,
            last_key: key,
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
        let exact = |e: &LeafletDirEntryV3| leaflet_edges(RunSortOrder::Spot, (0, 0), e).exact;
        assert!(exact(&entry(0)));
        assert!(exact(&entry(4)));
        assert!(!exact(&entry(40)));
    }

    fn group(s_id: u64) -> LiveGroup {
        let mut key = [0u8; ORDERED_KEY_V2_SIZE];
        write_ordered_key_v2(RunSortOrder::Spot, &rec(s_id, 1, 0, 0), &mut key);
        LiveGroup::of_key(RunSortOrder::Spot, &key)
    }

    /// A leaflet the walk sees: its group count, its routing groups and its
    /// rows' groups, all as subject ids.
    struct Seen {
        count: u64,
        routing: (u64, u64),
        rows: (u64, u64),
    }

    /// Walks `leaflets`, all holding history, cut into chunks of
    /// `sizes` and joined as the parallel reduce joins them, reading rows'
    /// groups through a loader that counts reads per leaflet.
    fn walk(leaflets: &[Seen], sizes: &[usize]) -> (u64, Vec<usize>) {
        let reads = std::cell::RefCell::new(vec![0; leaflets.len()]);
        let mut read = |(i, _): At| {
            reads.borrow_mut()[i] += 1;
            let (first, last) = leaflets[i].rows;
            Ok((group(first), group(last)))
        };
        let mut start = 0;
        let mut partials = Vec::new();
        for &size in sizes {
            let mut partial = Partial::new();
            for (i, seen) in leaflets.iter().enumerate().skip(start).take(size) {
                let edges = Edges {
                    first: group(seen.routing.0),
                    last: group(seen.routing.1),
                    at: (i, 0),
                    exact: false,
                };
                partial.push(seen.count, edges, &mut read).unwrap();
            }
            partials.push(partial);
            start += size;
        }
        let joined = partials.into_iter().reduce(Partial::join).unwrap();
        let count = joined.settle(&mut read).unwrap().count;
        (count, reads.into_inner())
    }

    /// Every way to cut `n` leaflets into chunks of one or more.
    fn cuts(n: usize) -> Vec<Vec<usize>> {
        (0..1u32 << (n - 1))
            .map(|mask| {
                let mut sizes = vec![1];
                for i in 0..n - 1 {
                    if mask & (1 << i) == 0 {
                        *sizes.last_mut().unwrap() += 1;
                    } else {
                        sizes.push(1);
                    }
                }
                sizes
            })
            .collect()
    }

    /// Where every boundary of a run needs its rows read, the walk reads
    /// each leaflet once, serially and however the leaves are cut into
    /// chunks, and counts the group the run shares once.
    #[test]
    fn a_walk_reads_each_leaflet_once_however_it_is_chunked() {
        let one_group: Vec<Seen> = (0..5)
            .map(|_| Seen {
                count: 1,
                routing: (7, 7),
                rows: (7, 7),
            })
            .collect();
        for sizes in cuts(one_group.len()) {
            let (count, reads) = walk(&one_group, &sizes);
            assert_eq!(count, 1, "{sizes:?}");
            assert_eq!(reads, vec![1; 5], "{sizes:?}: reads per leaflet");
        }
    }

    /// Routing groups that meet where the rows' groups do not: the walk reads
    /// the rows and subtracts only where they share a group, and reads no
    /// leaflet twice. Groups 1 to 5 in all.
    #[test]
    fn a_walk_subtracts_only_where_rows_share_a_group() {
        let leaflets = [
            // Widened over history up to group 3.
            Seen {
                count: 2,
                routing: (1, 3),
                rows: (1, 2),
            },
            Seen {
                count: 1,
                routing: (3, 3),
                rows: (3, 3),
            },
            // Widened over history up to group 5.
            Seen {
                count: 2,
                routing: (3, 5),
                rows: (3, 4),
            },
            Seen {
                count: 1,
                routing: (5, 5),
                rows: (5, 5),
            },
        ];
        for sizes in cuts(leaflets.len()) {
            let (count, reads) = walk(&leaflets, &sizes);
            assert_eq!(count, 5, "{sizes:?}");
            assert!(reads.iter().all(|&n| n <= 1), "{sizes:?}: reads {reads:?}");
        }
    }
}
