//! The rows of one key range in one index order, read from every member of a
//! [`MemberSet`] and merged into one sorted stream that holds each fact once.
//!
//! A default graph of two or more members is the set union of their facts
//! (SPARQL §13.2). A fact is storage's own identity: one fact per value and
//! list position, so a fact present in two members is one row, while
//! two items of one `@list` that repeat a value stay two facts. This cursor
//! drops a row when its fact ([`Key::identity`], the one definition) equals
//! the row emitted before it.
//!
//! **Why adjacency is exact.** The identity `(s_id, p_id, o_type, o_key, o_i)`
//! is every key column of every index order, so after a merge in any order a
//! fact's copies are adjacent, and one member's rows are distinct facts: only
//! the first row of a run can repeat the row before it.
//!
//! **Arena types.** `(o_type, o_key)` names one term across members for every
//! `o_type` except the arena-backed ones, whose `o_key` is a handle into an
//! arena per `(graph, predicate)` (big numbers, vectors, spatial values): one
//! handle can mean two values in two graphs, and a merged row no longer says
//! which graph would decode it. The cursor declines
//! ([`MergeDecline::ArenaType`]) at the first such row.
//!
//! **No novelty.** Each member's index must answer for it alone
//! ([`MemberSet::all_current`]); the cursor merges no overlay and declines a
//! set that has novelty or reads below the index ([`MergeDecline::Novelty`]).
//!
//! **Cost.** The merge works on integer keys only, with no dictionary access.
//! The input that holds the smallest row emits a run up to the smallest row of
//! the other inputs, found by galloping, so disjoint stretches cost a few
//! comparisons per run, and interleaved or repeated rows one key read and one
//! comparison each against the runner-up, plus the choice of the next input
//! (a scan of the heads for up to four inputs, a heap above). A row is
//! compared with the previous fact only at the start of a run. The output is
//! gathered column by column from the runs: a run that is a whole input batch
//! is shared, not copied.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io;
use std::marker::PhantomData;

use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::{
    BinaryCursor, BinaryFilter, ColumnBatch, ColumnData, ColumnProjection, ColumnSet,
};
use fluree_db_core::ids::GraphId;
use fluree_db_core::{DecodeKind, OType, Tracker};
use smallvec::SmallVec;

use crate::error::{QueryError, Result};
use crate::fast_path_outcome::FastPathFallback;
use crate::index_scope::MemberSet;

/// Inputs merged by a scan of their heads; above this, by a heap.
const LINEAR_MERGE_MAX_INPUTS: usize = 4;

/// Why a merge cannot give the exact union, so the lane that asked must fall
/// back to the general plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeDecline {
    /// A row's object is a handle into an arena per `(graph, predicate)`:
    /// big numbers, vectors and spatial values. Its encoded key does not name
    /// one value across members.
    ArenaType { o_type: u16 },
    /// A member has no index branch for the order.
    NoBranch,
    /// A member's index does not answer for it alone: it has novelty, or it
    /// is read below the index's `t`.
    Novelty,
}

impl MergeDecline {
    /// The decline reason a lane stamps.
    pub fn reason(&self) -> &'static str {
        match self {
            MergeDecline::ArenaType { .. } => "ArenaType",
            MergeDecline::NoBranch => "NoBranch",
            MergeDecline::Novelty => "Novelty",
        }
    }

    /// The fallback a lane's site stamps for this decline.
    pub fn fallback(&self) -> FastPathFallback {
        match self {
            MergeDecline::ArenaType { .. } => FastPathFallback::ArenaType,
            MergeDecline::NoBranch => FastPathFallback::NoBranch,
            MergeDecline::Novelty => FastPathFallback::Novelty,
        }
    }
}

/// Whether `o_type`'s `o_key` is a handle into an arena per
/// `(graph, predicate)`.
#[inline]
pub(crate) fn is_graph_scoped_arena(o_type: u16) -> bool {
    matches!(
        OType::from_u16(o_type).decode_kind(),
        DecodeKind::NumBigArena | DecodeKind::VectorArena | DecodeKind::SpatialArena
    )
}

/// The rows of one key range in one index order, read from every member of a
/// [`MemberSet`] and merged: rows come out in `order`, each fact once (see the
/// module docs).
pub struct MergedRangeCursor {
    order: RunSortOrder,
    inputs: SmallVec<[MemberInput; 4]>,
    min: Key,
    max: Key,
    /// The columns each output batch carries: the caller's projection.
    output: ColumnSet,
    /// The fact of the last row emitted, across batches.
    last: Option<FactKey>,
    /// Sticky: once declined, every later call declines the same way.
    declined: Option<MergeDecline>,
    /// The runs the current round emits, reused across rounds.
    runs: Vec<Run>,
}

impl MergedRangeCursor {
    /// A merge of `set`'s members over `[min, max]` (inclusive, in `order`),
    /// each read with `filter`. Output batches carry `projection`'s columns;
    /// the inputs read the identity columns as well, which the merge needs.
    /// With no column (an empty projection), a batch is a row count alone and
    /// the merge records nothing to gather: what a counting lane reads.
    ///
    /// Members whose index holds no leaf in the range are left out. Declines
    /// [`MergeDecline::Novelty`] unless every member is current, and
    /// [`MergeDecline::NoBranch`] when a member has no branch for `order`.
    pub fn new(
        set: &MemberSet<'_>,
        order: RunSortOrder,
        min: &RunRecordV2,
        max: &RunRecordV2,
        filter: BinaryFilter,
        projection: ColumnProjection,
    ) -> std::result::Result<Self, MergeDecline> {
        if !set.all_current() {
            return Err(MergeDecline::Novelty);
        }
        let input_projection = input_projection(projection);
        let cmp = fluree_db_binary_index::format::run_record_v2::cmp_v2_for_order(order);
        let mut inputs = SmallVec::new();
        for member in set.members() {
            let store = member.store();
            let branch = store
                .branch_for_order(member.g_id(), order)
                .ok_or(MergeDecline::NoBranch)?;
            if branch.find_leaves_in_range(min, max, cmp).is_empty() {
                continue;
            }
            let mut cursor = BinaryCursor::new(
                std::sync::Arc::clone(store),
                order,
                std::sync::Arc::clone(branch),
                min,
                max,
                filter.clone(),
                input_projection,
            );
            cursor.set_to_t(member.to_t());
            inputs.push(MemberInput::new(member.g_id(), Source::Index(cursor)));
        }
        Ok(Self::from_inputs(order, inputs, min, max, projection))
    }

    fn from_inputs(
        order: RunSortOrder,
        inputs: SmallVec<[MemberInput; 4]>,
        min: &RunRecordV2,
        max: &RunRecordV2,
        projection: ColumnProjection,
    ) -> Self {
        Self {
            order,
            inputs,
            min: Key::of(min),
            max: Key::of(max),
            output: projection.effective(),
            last: None,
            declined: None,
            runs: Vec::new(),
        }
    }

    /// Charge `tracker` one index touch per input leaflet read, as a single
    /// cursor does.
    pub fn with_tracker(mut self, tracker: &Tracker) -> Self {
        if tracker.is_enabled() {
            self.inputs = std::mem::take(&mut self.inputs)
                .into_iter()
                .map(|mut input| {
                    input.source = match input.source {
                        Source::Index(c) => Source::Index(c.with_tracker(tracker.clone())),
                        #[cfg(test)]
                        other => other,
                    };
                    input
                })
                .collect();
        }
        self
    }

    /// The order rows come out in.
    pub fn order(&self) -> RunSortOrder {
        self.order
    }

    /// The members whose index holds a leaf in the range: the others were
    /// left out at construction. None means the range is empty in every
    /// member; one means a single member can hold rows there. The leaf test
    /// is a superset, so a member listed here may still hold no row.
    pub fn members_in_range(&self) -> SmallVec<[GraphId; 4]> {
        self.inputs.iter().map(|input| input.g_id).collect()
    }

    /// The next merged batch, or `None` when every input is drained. A batch
    /// ends at the first input leaflet boundary, so it holds at most the rows
    /// of the inputs' current leaflets; it is never empty.
    ///
    /// `Err(MergeDecline)` when the merge meets a row it cannot merge exactly
    /// (see [`MergeDecline`]); it may come after batches were returned, and
    /// every later call returns it again. An I/O error leaves the cursor as it
    /// was, so the call can be retried.
    pub fn next_batch(&mut self) -> Result<std::result::Result<Option<ColumnBatch>, MergeDecline>> {
        loop {
            if let Some(decline) = self.declined {
                return Ok(Err(decline));
            }
            if let Some(decline) = self.refill()? {
                self.declined = Some(decline);
                return Ok(Err(decline));
            }
            self.runs.clear();
            let rows = match self.order {
                RunSortOrder::Spot => self.merge_round::<Spot>(),
                RunSortOrder::Psot => self.merge_round::<Psot>(),
                RunSortOrder::Post => self.merge_round::<Post>(),
                RunSortOrder::Opst => self.merge_round::<Opst>(),
            };
            if rows > 0 {
                return Ok(Ok(Some(self.gather(rows))));
            }
            if self.inputs.iter().all(|input| input.done) {
                return Ok(Ok(None));
            }
            // Every row of this round was a duplicate: merge on.
        }
    }

    /// Give every input that has used up its batch its next batch, trimmed to
    /// the range and checked for rows the merge must decline.
    fn refill(&mut self) -> Result<Option<MergeDecline>> {
        let order = self.order;
        let (min, max) = (self.min, self.max);
        for input in &mut self.inputs {
            while !input.done && input.pos >= input.end {
                if input.last_batch {
                    input.finish();
                    break;
                }
                let Some(batch) = input
                    .source
                    .next_batch()
                    .map_err(|e| QueryError::from_io("merged range cursor", e))?
                else {
                    input.finish();
                    break;
                };
                let (start, end, past_max) = match order {
                    RunSortOrder::Spot => trim::<Spot>(&batch, &min, &max),
                    RunSortOrder::Psot => trim::<Psot>(&batch, &min, &max),
                    RunSortOrder::Post => trim::<Post>(&batch, &min, &max),
                    RunSortOrder::Opst => trim::<Opst>(&batch, &min, &max),
                };
                input.last_batch = past_max;
                if start >= end {
                    continue;
                }
                if let Some(o_type) = first_arena_type(&batch.o_type, start, end) {
                    return Ok(Some(MergeDecline::ArenaType { o_type }));
                }
                input.batch = Some(batch);
                input.pos = start;
                input.end = end;
            }
        }
        Ok(None)
    }

    /// Merge the inputs' current batches until one of them is used up,
    /// recording the rows to emit in `self.runs` when a column is wanted.
    /// Returns how many rows it emits.
    fn merge_round<O: MergeOrder>(&mut self) -> usize {
        let record = self.output != ColumnSet::EMPTY;
        let Self {
            inputs, runs, last, ..
        } = self;
        // Each live input's batch, borrowed for the round; positions are
        // written back at its end.
        let mut lanes: SmallVec<[Lane<'_>; 4]> = inputs
            .iter()
            .enumerate()
            .filter(|(_, input)| input.pos < input.end)
            .map(|(i, input)| Lane {
                input: i,
                batch: input.batch(),
                pos: input.pos,
                end: input.end,
            })
            .collect();
        // The round ends when the lane whose batch ends lowest runs out, so
        // it emits nothing past that end (the horizon). A lane whose head is
        // past it can be neither the smallest head nor a bound this round:
        // leave it out, so lanes that only wait cost nothing per step.
        if lanes.len() > 2 {
            let horizon = lanes
                .iter()
                .map(|lane| Key::at(lane.batch, lane.end - 1))
                .min_by(|x, y| O::cmp(x, y))
                .expect("two or more lanes");
            lanes.retain(|lane| O::cmp(&lane.head(), &horizon) != Ordering::Greater);
        }
        let mut out = Emitted {
            runs,
            last,
            record,
            rows: 0,
        };
        match lanes.as_mut_slice() {
            [] => {}
            [only] => {
                let head = only.head();
                advance::<O>(only, head, None, &mut out);
            }
            [a, b] => merge_two::<O>(a, b, &mut out),
            lanes if lanes.len() <= LINEAR_MERGE_MAX_INPUTS => merge_linear::<O>(lanes, &mut out),
            lanes => merge_heap::<O>(lanes, &mut out),
        }
        let positions: SmallVec<[(usize, usize); 4]> =
            lanes.iter().map(|lane| (lane.input, lane.pos)).collect();
        drop(lanes);
        for (i, pos) in positions {
            inputs[i].pos = pos;
        }
        out.rows
    }

    /// The batch of this round's `row_count` rows, gathered column by column
    /// from its runs; a row count alone when no column is wanted (no runs
    /// were recorded).
    fn gather(&self, row_count: usize) -> ColumnBatch {
        let inputs = &self.inputs;
        let runs = &self.runs;
        let wants = |col| self.output.contains(col);
        ColumnBatch {
            row_count,
            s_id: gather_column(wants(ColumnId::SId), runs, inputs, row_count, 0, |b| {
                &b.s_id
            }),
            o_key: gather_column(wants(ColumnId::OKey), runs, inputs, row_count, 0, |b| {
                &b.o_key
            }),
            p_id: gather_column(wants(ColumnId::PId), runs, inputs, row_count, 0, |b| {
                &b.p_id
            }),
            o_type: gather_column(wants(ColumnId::OType), runs, inputs, row_count, 0, |b| {
                &b.o_type
            }),
            o_i: gather_column(
                wants(ColumnId::OI),
                runs,
                inputs,
                row_count,
                u32::MAX,
                |b| &b.o_i,
            ),
            t: gather_column(wants(ColumnId::T), runs, inputs, row_count, 0, |b| &b.t),
        }
    }
}

/// What each input reads: the caller's columns plus the identity the merge
/// compares and dedups on.
fn input_projection(projection: ColumnProjection) -> ColumnProjection {
    let mut internal = projection.internal.union(ColumnSet::CORE);
    internal.insert(ColumnId::OI);
    ColumnProjection {
        output: projection.output,
        internal,
    }
}

/// One input's batch during a merge round, borrowed out of its
/// [`MemberInput`] so the merge loop works on locals.
struct Lane<'b> {
    input: usize,
    batch: &'b ColumnBatch,
    /// The head; rows `[pos, end)` remain.
    pos: usize,
    end: usize,
}

impl Lane<'_> {
    #[inline(always)]
    fn head(&self) -> Key {
        Key::at(self.batch, self.pos)
    }
}

/// Where a round's rows go: the runs to gather, and the identity of the last
/// row emitted.
struct Emitted<'a> {
    runs: &'a mut Vec<Run>,
    last: &'a mut Option<FactKey>,
    /// Record runs to gather from. Without an output column, rows are only
    /// counted.
    record: bool,
    /// The rows emitted this round.
    rows: usize,
}

impl Emitted<'_> {
    /// Emit `lane`'s row `row` alone, whose key is `key`, unless it is the
    /// fact emitted last.
    #[inline(always)]
    fn one(&mut self, lane: &Lane<'_>, row: usize, key: Key) {
        let fact = key.identity();
        if *self.last != Some(fact) {
            if self.record {
                push_run(self.runs, lane.input, row, row + 1);
            }
            self.rows += 1;
            *self.last = Some(fact);
        }
    }

    /// Emit `lane`'s rows `[from, to)`, the first of which has key `first`.
    /// One member's rows are distinct facts, so only the first can be the
    /// fact emitted last.
    #[inline]
    fn run(&mut self, lane: &Lane<'_>, mut from: usize, to: usize, first: Key) {
        if from < to && *self.last == Some(first.identity()) {
            from += 1;
        }
        if from < to {
            if self.record {
                push_run(self.runs, lane.input, from, to);
            }
            self.rows += to - from;
            *self.last = Some(Key::at(lane.batch, to - 1).identity());
        }
    }
}

/// Emit `lane`'s run: from its head, whose key is `head`, through its last
/// row that sorts before `bound` (the smallest of the other heads; `None`,
/// the whole batch). The head itself is always emitted: it is the smallest,
/// the earlier input winning a tie. A row equal to `bound` ends the run, so
/// the input holding the other copy emits next and drops it as a repeat, a
/// run of one each. Returns the new head's key, or `None` when the run used
/// up the batch.
#[inline(always)]
fn advance<O: MergeOrder>(
    lane: &mut Lane<'_>,
    head: Key,
    bound: Option<&Key>,
    out: &mut Emitted<'_>,
) -> Option<Key> {
    let next = lane.pos + 1;
    let run_end = match bound {
        Some(bound) if next < lane.end => {
            let probe = Key::at(lane.batch, next);
            if O::cmp(&probe, bound) != Ordering::Less {
                // A run of one, the commonest where members interleave or
                // repeat each other: the probed row is the new head.
                out.one(lane, lane.pos, head);
                lane.pos = next;
                return Some(probe);
            }
            gallop::<O>(lane.batch, next, lane.end, bound)
        }
        _ => lane.end,
    };
    out.run(lane, lane.pos, run_end, head);
    lane.pos = run_end;
    (run_end < lane.end).then(|| lane.head())
}

/// The merge of two inputs, its heads in locals.
fn merge_two<O: MergeOrder>(a: &mut Lane<'_>, b: &mut Lane<'_>, out: &mut Emitted<'_>) {
    let (mut head_a, mut head_b) = (a.head(), b.head());
    loop {
        match O::cmp(&head_b, &head_a) {
            Ordering::Less => match advance::<O>(b, head_b, Some(&head_a), out) {
                Some(next) => head_b = next,
                None => return,
            },
            Ordering::Greater => match advance::<O>(a, head_a, Some(&head_b), out) {
                Some(next) => head_a = next,
                None => return,
            },
            // Rows both members hold, the commonest where they overlap.
            Ordering::Equal => match shared_stretch::<O>(a, b, head_a, None, out) {
                Some((next_a, next_b)) => (head_a, head_b) = (next_a, next_b),
                None => return,
            },
        }
    }
}

/// `a` and `b` both hold the row at their heads, whose key is `head`: walk the
/// rows they both hold in lockstep, while the two stay equal and sort before
/// `bound` (the smallest of any other heads), `a` emitting each and `b`
/// stepping past its copy, which would be dropped as a repeat. One key read
/// per lane and one comparison per row pair. Returns the two new heads, or
/// `None` when either batch is used up.
#[inline]
fn shared_stretch<O: MergeOrder>(
    a: &mut Lane<'_>,
    b: &mut Lane<'_>,
    head: Key,
    bound: Option<&Key>,
    out: &mut Emitted<'_>,
) -> Option<(Key, Key)> {
    let from = a.pos;
    if let Some(shared) = constant_shared_prefix::<O>(a, b, bound) {
        a.pos += shared;
        b.pos += shared;
        out.run(a, from, a.pos, head);
        return (a.pos < a.end && b.pos < b.end).then(|| (a.head(), b.head()));
    }
    let next = loop {
        a.pos += 1;
        b.pos += 1;
        if a.pos == a.end || b.pos == b.end {
            break None;
        }
        let (next_a, next_b) = (a.head(), b.head());
        if next_a != next_b || bound.is_some_and(|t| O::cmp(&next_a, t) != Ordering::Less) {
            break Some((next_a, next_b));
        }
    };
    out.run(a, from, a.pos, head);
    next
}

/// [`shared_stretch`]'s walk on slices: how many rows from both heads (equal)
/// are equal and, after the first, sort before `bound`, when both batches
/// carry `p_id`, `o_type` and `o_i` as one constant they agree on (one
/// predicate's leaflets of one type, without list rows, the common case).
/// Two rows then differ only in `s_id` and `o_key`, compared as slices.
/// `None` when the columns are not of that shape.
#[inline]
fn constant_shared_prefix<O: MergeOrder>(
    a: &Lane<'_>,
    b: &Lane<'_>,
    bound: Option<&Key>,
) -> Option<usize> {
    let (ab, bb) = (a.batch, b.batch);
    let same_const = matches!((&ab.p_id, &bb.p_id), (ColumnData::Const(x), ColumnData::Const(y)) if x == y)
        && matches!((&ab.o_type, &bb.o_type), (ColumnData::Const(x), ColumnData::Const(y)) if x == y)
        && matches!((&ab.o_i, &bb.o_i), (ColumnData::Const(x), ColumnData::Const(y)) if x == y);
    if !same_const {
        return None;
    }
    let (
        ColumnData::Block(a_s),
        ColumnData::Block(b_s),
        ColumnData::Block(a_o),
        ColumnData::Block(b_o),
    ) = (&ab.s_id, &bb.s_id, &ab.o_key, &bb.o_key)
    else {
        return None;
    };
    let mut span = (a.end - a.pos).min(b.end - b.pos);
    if let Some(bound) = bound {
        // The head always goes; the rows after it only while below `bound`.
        let below = partition_point(a.pos + 1, a.end, |row| {
            O::cmp(&Key::at(ab, row), bound) == Ordering::Less
        });
        span = span.min(below - a.pos);
    }
    let shared = a_s[a.pos..a.pos + span]
        .iter()
        .zip(&b_s[b.pos..b.pos + span])
        .zip(
            a_o[a.pos..a.pos + span]
                .iter()
                .zip(&b_o[b.pos..b.pos + span]),
        )
        .take_while(|((sa, sb), (oa, ob))| sa == sb && oa == ob)
        .count();
    Some(shared.max(1))
}

/// Two distinct elements of `slice`, mutably.
fn two_mut<T>(slice: &mut [T], i: usize, j: usize) -> (&mut T, &mut T) {
    debug_assert_ne!(i, j);
    if i < j {
        let (left, right) = slice.split_at_mut(j);
        (&mut left[i], &mut right[0])
    } else {
        let (left, right) = slice.split_at_mut(i);
        (&mut right[0], &mut left[j])
    }
}

/// The merge for a few inputs: the next input is found by scanning the
/// heads.
fn merge_linear<O: MergeOrder>(lanes: &mut [Lane<'_>], out: &mut Emitted<'_>) {
    let mut heads: SmallVec<[Key; LINEAR_MERGE_MAX_INPUTS]> =
        lanes.iter().map(Lane::head).collect();
    loop {
        // The smallest head and the smallest of the others (the earlier lane
        // on a tie).
        let mut min_at = 0;
        let mut second_at: Option<usize> = None;
        for at in 1..heads.len() {
            if O::cmp(&heads[at], &heads[min_at]) == Ordering::Less {
                second_at = Some(min_at);
                min_at = at;
            } else if second_at.is_none_or(|s| O::cmp(&heads[at], &heads[s]) == Ordering::Less) {
                second_at = Some(at);
            }
        }
        let second_at = second_at.expect("two or more lanes");
        if heads[min_at] == heads[second_at] {
            // Rows two members hold: walk them in lockstep below the third
            // head.
            let third = (0..heads.len())
                .filter(|&at| at != min_at && at != second_at)
                .map(|at| heads[at])
                .min_by(|x, y| O::cmp(x, y));
            let (a, b) = two_mut(lanes, min_at, second_at);
            match shared_stretch::<O>(a, b, heads[min_at], third.as_ref(), out) {
                Some((next_a, next_b)) => (heads[min_at], heads[second_at]) = (next_a, next_b),
                None => return,
            }
        } else {
            let bound = heads[second_at];
            match advance::<O>(&mut lanes[min_at], heads[min_at], Some(&bound), out) {
                Some(next) => heads[min_at] = next,
                None => return,
            }
        }
    }
}

/// The merge for many inputs: the next input comes off a heap of heads.
fn merge_heap<O: MergeOrder>(lanes: &mut [Lane<'_>], out: &mut Emitted<'_>) {
    let mut heap: BinaryHeap<HeapHead<O>> = lanes
        .iter()
        .enumerate()
        .map(|(at, lane)| HeapHead::new(lane.head(), at))
        .collect();
    while let Some(top) = heap.pop() {
        let bound = heap.peek().map(|next| next.key);
        match advance::<O>(&mut lanes[top.input], top.key, bound.as_ref(), out) {
            Some(next) => heap.push(HeapHead::new(next, top.input)),
            None => return,
        }
    }
}

/// One member's stream: its cursor and the part of its current batch not yet
/// merged.
struct MemberInput {
    g_id: GraphId,
    source: Source,
    batch: Option<ColumnBatch>,
    /// The next row to merge; rows `[pos, end)` of `batch` remain.
    pos: usize,
    end: usize,
    /// The current batch reaches past the range: none after it can be in it.
    last_batch: bool,
    done: bool,
}

impl MemberInput {
    fn new(g_id: GraphId, source: Source) -> Self {
        Self {
            g_id,
            source,
            batch: None,
            pos: 0,
            end: 0,
            last_batch: false,
            done: false,
        }
    }

    fn finish(&mut self) {
        self.done = true;
        self.batch = None;
        self.pos = 0;
        self.end = 0;
    }

    #[inline]
    fn batch(&self) -> &ColumnBatch {
        self.batch.as_ref().expect("a live input holds a batch")
    }
}

/// Where an input's batches come from. The test variant is the small one;
/// production builds have only the cursor.
#[cfg_attr(test, allow(clippy::large_enum_variant))]
enum Source {
    Index(BinaryCursor),
    /// Fixed batches, for the unit tests.
    #[cfg(test)]
    Batches(std::collections::VecDeque<io::Result<ColumnBatch>>),
}

impl Source {
    fn next_batch(&mut self) -> io::Result<Option<ColumnBatch>> {
        match self {
            Source::Index(cursor) => cursor.next_batch(),
            #[cfg(test)]
            Source::Batches(batches) => batches.pop_front().transpose(),
        }
    }
}

/// Rows `[from, to)` of input `input`'s current batch, emitted in order.
#[derive(Debug, Clone, Copy)]
struct Run {
    input: usize,
    from: usize,
    to: usize,
}

#[inline]
fn push_run(runs: &mut Vec<Run>, input: usize, from: usize, to: usize) {
    if let Some(last) = runs.last_mut() {
        if last.input == input && last.to == from {
            last.to = to;
            return;
        }
    }
    runs.push(Run { input, from, to });
}

/// One output column from the runs: shared when one run is a whole input
/// batch, a constant when every run's column is the same constant, else
/// copied run by run. `AbsentDefault` when the caller did not ask for it.
fn gather_column<T: Copy + PartialEq>(
    wanted: bool,
    runs: &[Run],
    inputs: &[MemberInput],
    row_count: usize,
    default: T,
    column: impl Fn(&ColumnBatch) -> &ColumnData<T>,
) -> ColumnData<T> {
    if !wanted {
        return ColumnData::AbsentDefault;
    }
    if let [run] = runs {
        let batch = inputs[run.input].batch();
        if run.from == 0 && run.to == batch.row_count {
            return column(batch).clone();
        }
    }
    let mut constant = None;
    let all_constant = runs
        .iter()
        .all(|run| match column(inputs[run.input].batch()) {
            ColumnData::Const(v) => *constant.get_or_insert(*v) == *v,
            _ => false,
        });
    if all_constant {
        if let Some(v) = constant {
            return ColumnData::Const(v);
        }
    }
    let mut values = Vec::with_capacity(row_count);
    for run in runs {
        match column(inputs[run.input].batch()) {
            // A run of one row, the commonest where members interleave.
            ColumnData::Block(block) if run.to - run.from == 1 => values.push(block[run.from]),
            ColumnData::Block(block) => values.extend_from_slice(&block[run.from..run.to]),
            ColumnData::Const(v) => values.resize(values.len() + (run.to - run.from), *v),
            ColumnData::AbsentDefault => {
                values.resize(values.len() + (run.to - run.from), default);
            }
        }
    }
    ColumnData::Block(values.into())
}

/// The first arena-backed `o_type` in rows `[from, to)`.
fn first_arena_type(o_type: &ColumnData<u16>, from: usize, to: usize) -> Option<u16> {
    match o_type {
        ColumnData::Const(v) => is_graph_scoped_arena(*v).then_some(*v),
        ColumnData::Block(block) => block[from..to]
            .iter()
            .copied()
            .find(|&v| is_graph_scoped_arena(v)),
        ColumnData::AbsentDefault => None,
    }
}

/// The rows of `batch` within `[min, max]` as `[start, end)`, and whether the
/// batch reaches past `max`. A cursor emits whole leaflets, so its first and
/// last ones can hold rows outside its range.
fn trim<O: MergeOrder>(batch: &ColumnBatch, min: &Key, max: &Key) -> (usize, usize, bool) {
    let n = batch.row_count;
    if n == 0 {
        return (0, 0, false);
    }
    let start = if O::cmp(&Key::at(batch, 0), min) == Ordering::Less {
        partition_point(0, n, |row| {
            O::cmp(&Key::at(batch, row), min) == Ordering::Less
        })
    } else {
        0
    };
    let past_max = O::cmp(&Key::at(batch, n - 1), max) == Ordering::Greater;
    let end = if past_max {
        partition_point(start, n, |row| {
            O::cmp(&Key::at(batch, row), max) != Ordering::Greater
        })
    } else {
        n
    };
    (start, end, past_max)
}

/// The first row in `(pos, end]` whose key does not sort before `bound`,
/// given that row `pos`'s does: an exponential probe, then a binary search.
#[inline]
fn gallop<O: MergeOrder>(batch: &ColumnBatch, pos: usize, end: usize, bound: &Key) -> usize {
    let after = |row: usize| O::cmp(&Key::at(batch, row), bound) != Ordering::Less;
    let mut lo = pos + 1;
    let mut step = 1;
    let hi = loop {
        let probe = pos + step;
        if probe >= end {
            break end;
        }
        if after(probe) {
            break probe;
        }
        lo = probe + 1;
        step *= 2;
    };
    partition_point(lo, hi, |row| !after(row))
}

/// The first index in `[lo, hi)` where `pred` fails, `pred` holding on a
/// prefix of the range.
#[inline]
fn partition_point(mut lo: usize, mut hi: usize, pred: impl Fn(usize) -> bool) -> usize {
    while lo < hi {
        let mid = usize::midpoint(lo, hi);
        if pred(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Every sort column of every order, for one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Key {
    s_id: u64,
    o_key: u64,
    p_id: u32,
    o_i: u32,
    o_type: u16,
}

impl Key {
    fn of(record: &RunRecordV2) -> Self {
        Self {
            s_id: record.s_id.as_u64(),
            o_key: record.o_key,
            p_id: record.p_id,
            o_i: record.o_i,
            o_type: record.o_type,
        }
    }

    /// Row `row` of `batch`, read with the defaults the cursor reads absent
    /// columns with. Forced inline: the merge reads a key or two per row, and
    /// an out-of-line call returning the key through memory was over a third
    /// of the merge's time.
    #[inline(always)]
    fn at(batch: &ColumnBatch, row: usize) -> Self {
        Self {
            s_id: batch.s_id.get(row),
            o_key: batch.o_key.get(row),
            p_id: batch.p_id.get_or(row, 0),
            o_i: batch.o_i.get_or(row, u32::MAX),
            o_type: batch.o_type.get_or(row, 0),
        }
    }

    /// The fact this row stores: the one definition of what the merge
    /// deduplicates on. Storage's fact identity is one fact per value
    /// and list position, and for a non-arena `o_type` the index key is the
    /// value, so the identity is the whole key.
    #[inline(always)]
    fn identity(&self) -> FactKey {
        FactKey {
            s_id: self.s_id,
            o_key: self.o_key,
            p_id: self.p_id,
            o_i: self.o_i,
            o_type: self.o_type,
        }
    }
}

/// A fact's identity in the merge ([`Key::identity`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FactKey {
    s_id: u64,
    o_key: u64,
    p_id: u32,
    o_i: u32,
    o_type: u16,
}

/// An index order's comparator over [`Key`], the same as
/// `cmp_v2_for_order`'s, resolved at compile time.
trait MergeOrder {
    fn cmp(a: &Key, b: &Key) -> Ordering;
}

enum Spot {}
enum Psot {}
enum Post {}
enum Opst {}

impl MergeOrder for Spot {
    #[inline(always)]
    fn cmp(a: &Key, b: &Key) -> Ordering {
        (a.s_id, a.p_id, a.o_type, a.o_key, a.o_i).cmp(&(b.s_id, b.p_id, b.o_type, b.o_key, b.o_i))
    }
}

impl MergeOrder for Psot {
    #[inline(always)]
    fn cmp(a: &Key, b: &Key) -> Ordering {
        (a.p_id, a.s_id, a.o_type, a.o_key, a.o_i).cmp(&(b.p_id, b.s_id, b.o_type, b.o_key, b.o_i))
    }
}

impl MergeOrder for Post {
    #[inline(always)]
    fn cmp(a: &Key, b: &Key) -> Ordering {
        (a.p_id, a.o_type, a.o_key, a.o_i, a.s_id).cmp(&(b.p_id, b.o_type, b.o_key, b.o_i, b.s_id))
    }
}

impl MergeOrder for Opst {
    #[inline(always)]
    fn cmp(a: &Key, b: &Key) -> Ordering {
        (a.o_type, a.o_key, a.o_i, a.p_id, a.s_id).cmp(&(b.o_type, b.o_key, b.o_i, b.p_id, b.s_id))
    }
}

/// A lane's head on the heap: a min-heap on the key, the earlier lane first
/// on a tie.
struct HeapHead<O> {
    key: Key,
    input: usize,
    order: PhantomData<O>,
}

impl<O> HeapHead<O> {
    fn new(key: Key, input: usize) -> Self {
        Self {
            key,
            input,
            order: PhantomData,
        }
    }
}

impl<O: MergeOrder> Ord for HeapHead<O> {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` pops the greatest: reverse both keys.
        O::cmp(&other.key, &self.key).then(other.input.cmp(&self.input))
    }
}

impl<O: MergeOrder> PartialOrd for HeapHead<O> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<O: MergeOrder> PartialEq for HeapHead<O> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<O: MergeOrder> Eq for HeapHead<O> {}

#[cfg(test)]
impl MergedRangeCursor {
    /// A merge of fixed batches, one list per member, over `[min, max]`.
    fn from_batches(
        order: RunSortOrder,
        members: Vec<Vec<io::Result<ColumnBatch>>>,
        min: &RunRecordV2,
        max: &RunRecordV2,
        projection: ColumnProjection,
    ) -> Self {
        let inputs = members
            .into_iter()
            .enumerate()
            .map(|(g, batches)| {
                MemberInput::new(g as GraphId, Source::Batches(batches.into_iter().collect()))
            })
            .collect();
        Self::from_inputs(order, inputs, min, max, projection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_binary_index::format::run_record_v2::cmp_v2_for_order;
    use fluree_db_binary_index::BinaryIndexStore;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::NoOverlay;
    use std::collections::HashSet;
    use std::sync::Arc;

    /// `(s_id, p_id, o_type, o_key, o_i)`.
    type Row = (u64, u32, u16, u64, u32);

    const NO_LIST: u32 = u32::MAX;
    const STR: u16 = OType::XSD_STRING.as_u16();
    const IRI: u16 = OType::IRI_REF.as_u16();
    const INT: u16 = OType::XSD_INTEGER.as_u16();
    const ORDERS: [RunSortOrder; 4] = [
        RunSortOrder::Spot,
        RunSortOrder::Psot,
        RunSortOrder::Post,
        RunSortOrder::Opst,
    ];

    fn record(row: Row) -> RunRecordV2 {
        let (s_id, p_id, o_type, o_key, o_i) = row;
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t: 0,
            o_i,
            o_type,
            g_id: 0,
        }
    }

    fn full_range() -> (RunRecordV2, RunRecordV2) {
        (
            record((0, 0, 0, 0, 0)),
            record((u64::MAX, u32::MAX, u16::MAX, u64::MAX, u32::MAX)),
        )
    }

    /// Rows sorted by the index's own comparator for `order`.
    fn sorted(order: RunSortOrder, mut rows: Vec<Row>) -> Vec<Row> {
        let cmp = cmp_v2_for_order(order);
        rows.sort_by(|a, b| cmp(&record(*a), &record(*b)));
        rows.dedup();
        rows
    }

    /// A batch shaped as a leaflet decodes: `s_id`/`o_key` as blocks,
    /// `p_id`/`o_type` constant when one value, `o_i` the sentinel constant
    /// without list rows, `t` the row's position.
    fn leaflet(rows: &[Row]) -> ColumnBatch {
        fn column<T: Copy + PartialEq>(values: Vec<T>) -> ColumnData<T> {
            match values.first() {
                Some(&v) if values.iter().all(|&x| x == v) => ColumnData::Const(v),
                _ => ColumnData::Block(values.into()),
            }
        }
        ColumnBatch {
            row_count: rows.len(),
            s_id: ColumnData::Block(rows.iter().map(|r| r.0).collect::<Vec<_>>().into()),
            o_key: ColumnData::Block(rows.iter().map(|r| r.3).collect::<Vec<_>>().into()),
            p_id: column(rows.iter().map(|r| r.1).collect()),
            o_type: column(rows.iter().map(|r| r.2).collect()),
            o_i: if rows.iter().all(|r| r.4 == NO_LIST) {
                ColumnData::Const(NO_LIST)
            } else {
                ColumnData::Block(rows.iter().map(|r| r.4).collect::<Vec<_>>().into())
            },
            t: ColumnData::Block((0..rows.len() as u32).collect::<Vec<_>>().into()),
        }
    }

    /// One member read as the given leaflets, each sorted for `order`.
    fn member(order: RunSortOrder, leaflets: &[&[Row]]) -> Vec<io::Result<ColumnBatch>> {
        leaflets
            .iter()
            .map(|rows| Ok(leaflet(&sorted(order, rows.to_vec()))))
            .collect()
    }

    fn merge(order: RunSortOrder, members: Vec<Vec<io::Result<ColumnBatch>>>) -> MergedRangeCursor {
        let (min, max) = full_range();
        MergedRangeCursor::from_batches(order, members, &min, &max, ColumnProjection::all())
    }

    fn rows_of(batch: &ColumnBatch) -> Vec<Row> {
        (0..batch.row_count)
            .map(|i| {
                (
                    batch.s_id.get(i),
                    batch.p_id.get_or(i, 0),
                    batch.o_type.get_or(i, 0),
                    batch.o_key.get(i),
                    batch.o_i.get_or(i, NO_LIST),
                )
            })
            .collect()
    }

    /// Every row the cursor emits, or its decline.
    fn drain(cursor: &mut MergedRangeCursor) -> std::result::Result<Vec<Row>, MergeDecline> {
        let mut out = Vec::new();
        loop {
            match cursor.next_batch().expect("no I/O error") {
                Ok(Some(batch)) => {
                    assert!(batch.row_count > 0, "a merged batch is never empty");
                    out.extend(rows_of(&batch));
                }
                Ok(None) => return Ok(out),
                Err(decline) => return Err(decline),
            }
        }
    }

    /// A row's fact: every column, the list index included.
    fn fact(row: &Row) -> Row {
        *row
    }

    /// The merge's contract, checked without its comparator: rows strictly
    /// ascending by the index's own order, no fact twice, and exactly the
    /// facts of every member.
    fn assert_is_the_union(order: RunSortOrder, members: &[Vec<Row>], out: &[Row]) {
        let cmp = cmp_v2_for_order(order);
        for pair in out.windows(2) {
            assert_eq!(
                cmp(&record(pair[0]), &record(pair[1])),
                Ordering::Less,
                "{order:?}: rows out of order: {pair:?}"
            );
        }
        let emitted: Vec<_> = out.iter().map(fact).collect();
        let distinct: HashSet<_> = emitted.iter().copied().collect();
        assert_eq!(distinct.len(), emitted.len(), "{order:?}: a fact twice");
        let expected: HashSet<_> = members.iter().flatten().map(fact).collect();
        assert_eq!(distinct, expected, "{order:?}: not the members' facts");
    }

    /// A deterministic xorshift generator.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Splits sorted rows into leaflets of 1 to 7 rows.
    fn split_leaflets(rng: &mut Rng, rows: &[Row]) -> Vec<io::Result<ColumnBatch>> {
        let mut out = Vec::new();
        let mut at = 0;
        while at < rows.len() {
            let len = (1 + rng.below(7) as usize).min(rows.len() - at);
            out.push(Ok(leaflet(&rows[at..at + len])));
            at += len;
        }
        out
    }

    #[test]
    fn merge_orders_match_the_index_comparators() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let mut pick = |n: u64| rng.below(n);
        for _ in 0..20_000 {
            let mut row = || -> Row {
                (
                    pick(3),
                    pick(3) as u32,
                    pick(3) as u16,
                    pick(3),
                    [0, 1, NO_LIST][pick(3) as usize],
                )
            };
            let (a, b) = (row(), row());
            let (ka, kb) = (Key::of(&record(a)), Key::of(&record(b)));
            for order in ORDERS {
                let ours = match order {
                    RunSortOrder::Spot => Spot::cmp(&ka, &kb),
                    RunSortOrder::Psot => Psot::cmp(&ka, &kb),
                    RunSortOrder::Post => Post::cmp(&ka, &kb),
                    RunSortOrder::Opst => Opst::cmp(&ka, &kb),
                };
                assert_eq!(ours, cmp_v2_for_order(order)(&record(a), &record(b)));
            }
        }
    }

    #[test]
    fn identical_members_emit_each_fact_once() {
        let rows: Vec<Row> = (1..=6).map(|s| (s, 7, STR, 100 + s % 3, NO_LIST)).collect();
        for order in ORDERS {
            for k in [2, 3, 5] {
                let members: Vec<_> = (0..k).map(|_| member(order, &[&rows[..]])).collect();
                let out = drain(&mut merge(order, members)).unwrap();
                assert_eq!(out, sorted(order, rows.clone()), "{order:?} k={k}");
            }
        }
    }

    #[test]
    fn overlapping_members_merge_into_one_sorted_set() {
        // Shared subjects with different objects, and one identical triple.
        let g1: Vec<Row> = vec![
            (1, 5, STR, 10, NO_LIST),
            (2, 5, STR, 11, NO_LIST),
            (3, 5, IRI, 2, NO_LIST),
        ];
        let g2: Vec<Row> = vec![
            (1, 5, STR, 12, NO_LIST),
            (2, 5, STR, 11, NO_LIST),
            (4, 5, INT, 9, NO_LIST),
        ];
        for order in ORDERS {
            let members = vec![member(order, &[&g1[..]]), member(order, &[&g2[..]])];
            let out = drain(&mut merge(order, members)).unwrap();
            assert_eq!(out.len(), 5, "{order:?}: {out:?}");
            assert_is_the_union(order, &[g1.clone(), g2.clone()], &out);
        }
    }

    #[test]
    fn a_disjoint_member_and_empty_members_pass_through() {
        let g1: Vec<Row> = vec![(1, 5, STR, 10, NO_LIST), (2, 5, STR, 11, NO_LIST)];
        // A predicate only g2 holds.
        let g2: Vec<Row> = vec![(1, 9, IRI, 3, NO_LIST), (8, 9, IRI, 1, NO_LIST)];
        for order in ORDERS {
            let members = vec![
                member(order, &[&g1[..]]),
                Vec::new(),
                member(order, &[&g2[..]]),
                vec![Ok(ColumnBatch::empty())],
            ];
            let out = drain(&mut merge(order, members)).unwrap();
            assert_eq!(out.len(), 4, "{order:?}");
            assert_is_the_union(order, &[g1.clone(), g2.clone()], &out);
            // No member at all.
            assert_eq!(
                drain(&mut merge(order, vec![Vec::new(), Vec::new()])).unwrap(),
                Vec::new()
            );
        }
    }

    #[test]
    fn list_items_are_facts_of_their_own_in_every_order() {
        // g1 holds "a" twice in one list (o_i 0 and 2) and "b" once (o_i 1);
        // g2 holds "a" at o_i 0 (the fact g1 holds too), at o_i 1, and as a
        // plain value. Five facts: one fact per value and list position.
        let g1: Vec<Row> = vec![(1, 5, STR, 10, 0), (1, 5, STR, 11, 1), (1, 5, STR, 10, 2)];
        let g2: Vec<Row> = vec![
            (1, 5, STR, 10, 0),
            (1, 5, STR, 10, 1),
            (1, 5, STR, 10, NO_LIST),
        ];
        // In POST and OPST, (o, o_i 0, s1), (o, o_i 0, s2), (o, o_i 1, s1):
        // one subject's two items of one value are not adjacent, and the fact
        // both members hold is.
        let h1: Vec<Row> = vec![(1, 6, IRI, 3, 0), (2, 6, IRI, 3, 0), (1, 6, IRI, 3, 1)];
        let h2: Vec<Row> = vec![(1, 6, IRI, 3, 1)];
        for order in ORDERS {
            let members = vec![
                member(order, &[&g1[..], &h1[..]]),
                member(order, &[&g2[..], &h2[..]]),
            ];
            let out = drain(&mut merge(order, members)).unwrap();
            let mut want: Vec<Row> = g1
                .iter()
                .chain(&g2)
                .chain(&h1)
                .chain(&h2)
                .copied()
                .collect();
            want = sorted(order, want);
            assert_eq!(out, want, "{order:?}");
            assert_eq!(out.len(), 8, "{order:?}");
        }
    }

    #[test]
    fn arena_types_decline_in_every_order() {
        let numbig = OType::NUM_BIG_OVERFLOW.as_u16();
        let vector = OType::VECTOR.as_u16();
        let spatial = OType::SPATIAL_COMPLEX.as_u16();
        let plain: Vec<Row> = vec![(1, 5, STR, 10, NO_LIST)];
        for order in ORDERS {
            for arena in [numbig, vector, spatial] {
                // A leaflet of one arena type (a constant column) ...
                let alone: Vec<Row> = vec![(2, 5, arena, 0, NO_LIST)];
                let members = vec![member(order, &[&plain[..]]), member(order, &[&alone[..]])];
                assert_eq!(
                    drain(&mut merge(order, members)),
                    Err(MergeDecline::ArenaType { o_type: arena }),
                    "{order:?}"
                );
                // ... and one mixed with others (a block).
                let mixed: Vec<Row> = vec![(2, 5, STR, 4, NO_LIST), (3, 5, arena, 0, NO_LIST)];
                let members = vec![member(order, &[&plain[..]]), member(order, &[&mixed[..]])];
                assert_eq!(
                    drain(&mut merge(order, members)),
                    Err(MergeDecline::ArenaType { o_type: arena }),
                    "{order:?}"
                );
            }
            // JSON and full-text values are string-dictionary ids: they merge.
            let dict: Vec<Row> = vec![
                (2, 5, OType::RDF_JSON.as_u16(), 4, NO_LIST),
                (3, 5, OType::FULLTEXT.as_u16(), 4, NO_LIST),
            ];
            let members = vec![member(order, &[&plain[..]]), member(order, &[&dict[..]])];
            assert_eq!(drain(&mut merge(order, members)).unwrap().len(), 3);
        }
        assert!(!is_graph_scoped_arena(STR) && !is_graph_scoped_arena(IRI));
    }

    #[test]
    fn a_decline_is_sticky() {
        let order = RunSortOrder::Psot;
        let g1: Vec<Row> = vec![(1, 5, OType::VECTOR.as_u16(), 0, NO_LIST)];
        let g2: Vec<Row> = vec![(2, 5, STR, 1, NO_LIST)];
        let mut cursor = merge(
            order,
            vec![member(order, &[&g1[..]]), member(order, &[&g2[..]])],
        );
        let decline = MergeDecline::ArenaType {
            o_type: OType::VECTOR.as_u16(),
        };
        assert_eq!(cursor.next_batch().unwrap().err(), Some(decline));
        assert_eq!(cursor.next_batch().unwrap().err(), Some(decline));
    }

    /// k = 2, 3 and 5 (past the heap threshold), random members over a small
    /// universe so they overlap, cut into leaflets of 1 to 7 rows, with list
    /// rows in every order: a value repeated within one member's list, and
    /// one list item held by several members.
    #[test]
    fn random_members_merge_to_the_set_union() {
        let objects = [(STR, 10), (STR, 11), (IRI, 3), (INT, 7)];
        for seed in 1..=40u64 {
            let mut rng = Rng(seed.wrapping_mul(0x2545_f491_4f6c_dd1d));
            for order in ORDERS {
                for k in [2usize, 3, 5] {
                    let mut members = Vec::new();
                    for _ in 0..k {
                        let mut rows = Vec::new();
                        for s in 1..=12u64 {
                            for p in 1..=3u32 {
                                for &(o_type, o_key) in &objects {
                                    if rng.below(10) < 4 {
                                        let o_i = if rng.below(4) == 0 {
                                            rng.below(3) as u32
                                        } else {
                                            NO_LIST
                                        };
                                        rows.push((s, p, o_type, o_key, o_i));
                                        // A list that repeats the value
                                        // within this member.
                                        if rng.below(6) == 0 {
                                            let again = 3 + rng.below(3) as u32;
                                            rows.push((s, p, o_type, o_key, again));
                                        }
                                    }
                                }
                            }
                        }
                        members.push(sorted(order, rows));
                    }
                    let batches = members
                        .iter()
                        .map(|rows| split_leaflets(&mut rng, rows))
                        .collect();
                    let out = drain(&mut merge(order, batches))
                        .unwrap_or_else(|d| panic!("{order:?} k={k} seed={seed}: {d:?}"));
                    assert_is_the_union(order, &members, &out);
                    // Counting alone (no output column) emits as many rows,
                    // over leaflets cut differently.
                    let batches = members
                        .iter()
                        .map(|rows| split_leaflets(&mut rng, rows))
                        .collect();
                    let (min, max) = full_range();
                    let mut counting = MergedRangeCursor::from_batches(
                        order,
                        batches,
                        &min,
                        &max,
                        ColumnProjection {
                            output: ColumnSet::EMPTY,
                            internal: ColumnSet::EMPTY,
                        },
                    );
                    let mut counted = 0;
                    while let Some(batch) = counting.next_batch().unwrap().unwrap() {
                        assert!(batch.row_count > 0 && batch.s_id.is_absent());
                        counted += batch.row_count;
                    }
                    assert_eq!(
                        counted,
                        out.len(),
                        "{order:?} k={k} seed={seed}: count only"
                    );
                }
            }
        }
    }

    #[test]
    fn a_duplicate_straddling_leaflets_is_dropped() {
        let order = RunSortOrder::Psot;
        let r = |s: u64, o: u64, o_i: u32| -> Row { (s, 5, STR, o, o_i) };
        // g1's leaflet ends where g2's second leaflet starts.
        let g1 = member(
            order,
            &[&[r(1, 1, NO_LIST), r(3, 1, NO_LIST)], &[r(5, 1, NO_LIST)]],
        );
        let g2 = member(
            order,
            &[&[r(2, 1, NO_LIST)], &[r(3, 1, NO_LIST), r(4, 1, NO_LIST)]],
        );
        let out = drain(&mut merge(order, vec![g1, g2])).unwrap();
        assert_eq!(
            out.iter().map(|r| r.0).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );

        // One value's two list items split across g1's leaflets, g2 holding
        // the second item and the plain value: four facts, the shared item
        // once.
        let g1 = member(order, &[&[r(1, 1, 0)], &[r(1, 1, 1), r(2, 1, NO_LIST)]]);
        let g2 = member(order, &[&[r(1, 1, 1), r(1, 1, NO_LIST)]]);
        let out = drain(&mut merge(order, vec![g1, g2])).unwrap();
        assert_eq!(
            out,
            vec![r(1, 1, 0), r(1, 1, 1), r(1, 1, NO_LIST), r(2, 1, NO_LIST)]
        );

        // The same row closes one member's leaflet and the other's, in every
        // order, with a third member in between.
        for order in ORDERS {
            let row = (3, 5, STR, 1, NO_LIST);
            let before = (1, 5, STR, 1, NO_LIST);
            let after = (9, 5, STR, 1, NO_LIST);
            let g1 = member(order, &[&[before, row], &[after]]);
            let g2 = member(order, &[&[row], &[after]]);
            let g3 = member(order, &[&[before], &[row]]);
            let out = drain(&mut merge(order, vec![g1, g2, g3])).unwrap();
            assert_eq!(out, sorted(order, vec![before, row, after]), "{order:?}");
        }
    }

    #[test]
    fn rows_outside_the_range_are_trimmed() {
        let order = RunSortOrder::Psot;
        let r = |p: u32, s: u64| -> Row { (s, p, STR, 1, NO_LIST) };
        // p 5's range; the leaflets around it hold p 4 and p 6 rows, and a
        // later leaflet holds an arena row the merge must never reach.
        let min = record((0, 5, 0, 0, 0));
        let max = record((u64::MAX, 5, u16::MAX, u64::MAX, u32::MAX));
        let past = (1, 7, OType::NUM_BIG_OVERFLOW.as_u16(), 0, NO_LIST);
        let g1 = member(
            order,
            &[&[r(4, 9), r(5, 1), r(5, 3)], &[r(5, 4), r(6, 1)], &[past]],
        );
        let g2 = member(order, &[&[r(4, 2), r(5, 2)], &[r(5, 3), r(6, 0)], &[past]]);
        let mut cursor = MergedRangeCursor::from_batches(
            order,
            vec![g1, g2],
            &min,
            &max,
            ColumnProjection::all(),
        );
        let out = drain(&mut cursor).unwrap();
        assert_eq!(out, vec![r(5, 1), r(5, 2), r(5, 3), r(5, 4)]);
    }

    #[test]
    fn an_io_error_leaves_the_cursor_reenterable() {
        let order = RunSortOrder::Spot;
        let r = |s: u64| -> Row { (s, 5, STR, 1, NO_LIST) };
        let g1 = vec![
            Ok(leaflet(&[r(1), r(3)])),
            Err(io::Error::other("transient")),
            Ok(leaflet(&[r(5)])),
        ];
        let g2 = vec![Ok(leaflet(&[r(2), r(3), r(4)]))];
        let mut cursor = merge(order, vec![g1, g2]);
        let mut out = Vec::new();
        let mut errors = 0;
        loop {
            match cursor.next_batch() {
                Ok(Ok(Some(batch))) => out.extend(rows_of(&batch)),
                Ok(Ok(None)) => break,
                Ok(Err(decline)) => panic!("{decline:?}"),
                Err(_) => errors += 1,
            }
        }
        assert_eq!(errors, 1);
        assert_eq!(out, (1..=5).map(r).collect::<Vec<_>>());
    }

    #[test]
    fn a_whole_leaflet_is_shared_and_constant_columns_stay_constant() {
        let order = RunSortOrder::Psot;
        let rows: Vec<Row> = (1..=4).map(|s| (s, 5, STR, s, NO_LIST)).collect();
        let only = leaflet(&rows);
        let ColumnData::Block(s_ids) = &only.s_id else {
            unreachable!()
        };
        let s_ids = Arc::clone(s_ids);
        let mut cursor = merge(order, vec![vec![Ok(only)], Vec::new()]);
        let batch = cursor.next_batch().unwrap().unwrap().unwrap();
        let ColumnData::Block(out) = &batch.s_id else {
            panic!("s_id: {:?}", batch.s_id)
        };
        assert!(
            Arc::ptr_eq(out, &s_ids),
            "one whole leaflet is shared, not copied"
        );

        // Two interleaved members: p_id, o_type and o_i stay constant.
        let even: Vec<Row> = (1..=4).map(|s| (2 * s, 5, STR, 1, NO_LIST)).collect();
        let odd: Vec<Row> = (1..=4).map(|s| (2 * s - 1, 5, STR, 1, NO_LIST)).collect();
        let mut cursor = merge(
            order,
            vec![member(order, &[&even[..]]), member(order, &[&odd[..]])],
        );
        let batch = cursor.next_batch().unwrap().unwrap().unwrap();
        assert!(matches!(batch.p_id, ColumnData::Const(5)));
        assert!(matches!(batch.o_type, ColumnData::Const(STR)));
        assert!(matches!(batch.o_i, ColumnData::Const(NO_LIST)));
        assert!(matches!(batch.s_id, ColumnData::Block(_)));

        // Columns the caller did not ask for are left out.
        let (min, max) = full_range();
        let mut cursor = MergedRangeCursor::from_batches(
            order,
            vec![member(order, &[&even[..]]), member(order, &[&odd[..]])],
            &min,
            &max,
            ColumnProjection {
                output: ColumnSet::single(ColumnId::SId),
                internal: ColumnSet::EMPTY,
            },
        );
        // A batch ends where an input's leaflet does: odd's 7 closes the
        // first, and 8 comes in the second.
        let mut sizes = Vec::new();
        while let Some(batch) = cursor.next_batch().unwrap().unwrap() {
            assert!(batch.o_key.is_absent() && batch.t.is_absent() && batch.o_i.is_absent());
            assert!(!batch.s_id.is_absent());
            sizes.push(batch.row_count);
        }
        assert_eq!(sizes, vec![7, 1]);
    }

    /// Construction over a real (empty) store: a set that is not current
    /// declines `Novelty`; a current one with no branch declines `NoBranch`.
    #[test]
    fn construction_declines_novelty_and_a_missing_branch() {
        use crate::index_scope::{IndexMember, MemberSet};
        let store = Arc::new(BinaryIndexStore::empty(std::env::temp_dir()));
        let (min, max) = full_range();
        let new = |set: &MemberSet<'_>| {
            MergedRangeCursor::new(
                set,
                RunSortOrder::Psot,
                &min,
                &max,
                BinaryFilter::default(),
                ColumnProjection::all(),
            )
            .map(|_| ())
        };
        let at = |to_t: i64| -> MemberSet<'_> {
            MemberSet::new(
                &store,
                [3, 4]
                    .into_iter()
                    .map(|g| IndexMember::new(&store, g, &NoOverlay, to_t))
                    .collect(),
            )
            .expect("two members")
        };
        assert_eq!(new(&at(store.max_t() + 1)), Err(MergeDecline::Novelty));
        assert_eq!(new(&at(store.max_t())), Err(MergeDecline::NoBranch));
    }

    /// The merge's cost per input row, in isolation. Ignored: run it on the
    /// release profile, as
    /// `cargo test --release -p fluree-db-query --lib merged_cursor::tests::merge_cost -- --ignored --nocapture`.
    /// `MERGE_COST=<scenario>` runs one scenario; `<scenario>:setup` runs its
    /// setup alone, so the difference of two runs' instructions retired
    /// (`/usr/bin/time -l`) is the merge's. `MERGE_COST_REPS` sets the
    /// repetitions (10); `MERGE_COST_COUNT_ONLY=1` projects no column (the
    /// default projects `s_id`).
    #[test]
    #[ignore]
    fn merge_cost_per_row() {
        const ROWS: u64 = 1 << 20;
        const LEAFLET: usize = 4096;
        let reps: usize = std::env::var("MERGE_COST_REPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);
        // `MERGE_COST_COUNT_ONLY=1`: no output column, as a counting lane reads.
        let count_only = std::env::var("MERGE_COST_COUNT_ONLY").is_ok();
        // Each scenario: members' rows, in PSOT over one predicate.
        let scenarios: Vec<(&str, Vec<Vec<Row>>)> = {
            let r = |s: u64| -> Row { (s, 5, STR, s % 97, NO_LIST) };
            let interleaved = |k: u64| -> Vec<Vec<Row>> {
                (0..k)
                    .map(|m| (0..ROWS / k).map(|i| r(i * k + m)).collect())
                    .collect()
            };
            vec![
                ("one_member", vec![(0..ROWS).map(r).collect(), Vec::new()]),
                (
                    "k2_disjoint_halves",
                    vec![
                        (0..ROWS / 2).map(r).collect(),
                        (ROWS / 2..ROWS).map(r).collect(),
                    ],
                ),
                ("k2_interleaved", interleaved(2)),
                ("k2_identical", vec![(0..ROWS / 2).map(r).collect(); 2]),
                (
                    "k2_half_overlap",
                    vec![
                        (0..ROWS / 2).map(r).collect(),
                        (ROWS / 4..3 * ROWS / 4).map(r).collect(),
                    ],
                ),
                ("k3_interleaved", interleaved(3)),
                ("k5_interleaved", interleaved(5)),
            ]
        };
        let only = std::env::var("MERGE_COST").ok();
        for (name, members) in scenarios {
            let (wanted, setup_only) = match only.as_deref() {
                None => (true, false),
                Some(sel) => match sel.strip_suffix(":setup") {
                    Some(base) => (base == name, true),
                    None => (sel == name, false),
                },
            };
            if !wanted {
                continue;
            }
            let leaflets: Vec<Vec<ColumnBatch>> = members
                .iter()
                .map(|rows| rows.chunks(LEAFLET).map(leaflet).collect())
                .collect();
            let input_rows: usize = members.iter().map(Vec::len).sum();
            let mut best = std::time::Duration::MAX;
            let mut emitted = 0;
            for _ in 0..reps {
                let batches = leaflets
                    .iter()
                    .map(|m| m.iter().cloned().map(Ok).collect())
                    .collect();
                let (min, max) = full_range();
                let mut cursor = MergedRangeCursor::from_batches(
                    RunSortOrder::Psot,
                    batches,
                    &min,
                    &max,
                    ColumnProjection {
                        output: if count_only {
                            ColumnSet::EMPTY
                        } else {
                            ColumnSet::single(ColumnId::SId)
                        },
                        internal: ColumnSet::EMPTY,
                    },
                );
                if setup_only {
                    std::hint::black_box(&mut cursor);
                    continue;
                }
                let start = std::time::Instant::now();
                emitted = 0;
                while let Ok(Some(batch)) = cursor.next_batch().unwrap() {
                    emitted += batch.row_count;
                    std::hint::black_box(&batch);
                }
                best = best.min(start.elapsed());
            }
            if !setup_only {
                println!(
                    "{name:>20}: {input_rows} input rows, {emitted} emitted, {:.2} ns/input row (best of {reps})",
                    best.as_nanos() as f64 / input_rows as f64
                );
            }
        }
    }
}
