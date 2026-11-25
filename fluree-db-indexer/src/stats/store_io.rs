//! The statistics store as an index build reads and writes it.
//!
//! An incremental build reads the hot object whole and, for each touched key
//! it lacks, that key's base segment; it never reads a segment it does not
//! need. It writes the hot object back with the touched entries' new state.

use std::collections::{BTreeMap, HashMap, HashSet};

use fluree_db_binary_index::format::stats_store::{
    encode_entries, DropMarker, EntryRef, EntrySummary, Registers, StatsEntry, StatsKey,
    StoreObject, StoreObjectKind,
};
use fluree_db_binary_index::{StatsSegmentRef, StatsStoreRefs};
use fluree_db_core::index_stats::GraphPropertyStatEntry;
use fluree_db_core::{ContentId, ContentKind, ContentStore, GraphId};

use crate::config::StatsCompactionPolicy;
use crate::error::{IndexerError, Result};
use crate::hll::HllSketch256;

use super::id_hook::IdPropertyHll;

/// The hot object is rewritten on every publish, so it favors speed.
pub(crate) const HOT_ZSTD_LEVEL: i32 = 1;
/// Base segments are written rarely and read on first touch: smaller wins.
pub(crate) const SEGMENT_ZSTD_LEVEL: i32 = 3;
/// Segments fetched at once: remote reads pay latency, not bandwidth, and
/// each segment is at most `SEGMENT_MAX_BYTES` compressed.
const SEGMENT_FETCH_WIDTH: usize = 8;
/// A segment's encoded size cap: entries vary widely in size, so a segment
/// over it is split rather than trusted to its entry count.
pub(crate) const SEGMENT_MAX_BYTES: usize = 1 << 20;

/// The segment owning `key`: the last whose first key is at or before it,
/// the first segment owning everything before its own first key.
pub(crate) fn owning_segment(segments: &[StatsSegmentRef], key: &StatsKey) -> Option<usize> {
    if segments.is_empty() {
        return None;
    }
    let at = segments.partition_point(|s| s.first <= (key.g_id, key.p_id));
    Some(at.saturating_sub(1))
}

/// One drop marker per graph, the latest floor: it hides everything an
/// earlier one did.
pub(crate) fn merge_markers(
    old: impl IntoIterator<Item = DropMarker>,
    new: impl IntoIterator<Item = DropMarker>,
) -> Vec<DropMarker> {
    let mut by_graph: BTreeMap<_, i64> = BTreeMap::new();
    for m in old.into_iter().chain(new) {
        let floor = by_graph.entry(m.g_id).or_insert(m.floor);
        *floor = (*floor).max(m.floor);
    }
    by_graph
        .into_iter()
        .map(|(g_id, floor)| DropMarker { g_id, floor })
        .collect()
}

fn hidden(markers: &[DropMarker], entry: &EntrySummary) -> bool {
    markers
        .binary_search_by_key(&entry.key.g_id, |m| m.g_id)
        .is_ok_and(|i| markers[i].hides(entry))
}

/// What an incremental build reads of the store.
#[derive(Default)]
pub(crate) struct LoadedStore {
    /// The hot object's visible entries.
    pub hot: BTreeMap<StatsKey, StatsEntry>,
    /// The base's markers merged with this build's.
    pub markers: Vec<DropMarker>,
    /// Each touched key's visible state before this build, from hot or its
    /// segment.
    pub prior: HashMap<StatsKey, StatsEntry>,
    pub objects_read: usize,
    pub bytes_read: usize,
}

async fn fetch(
    cs: &dyn ContentStore,
    cid: &ContentId,
    what: &str,
) -> Result<fluree_db_core::ContentBytes> {
    cs.get(cid)
        .await
        .map_err(|e| IndexerError::StorageRead(format!("stats store {what} {cid}: {e}")))
}

/// A segment fetch that owns what it carries along, so concurrent fetches
/// borrow nothing but the store.
async fn fetch_owned<T>(
    cs: &dyn ContentStore,
    cid: ContentId,
    carry: T,
) -> Result<(ContentId, T, fluree_db_core::ContentBytes)> {
    let bytes = fetch(cs, &cid, "segment").await?;
    Ok((cid, carry, bytes))
}

fn decode_error(what: &str, cid: &ContentId, e: impl std::fmt::Display) -> IndexerError {
    IndexerError::StorageRead(format!("stats store {what} {cid}: {e}"))
}

/// Read the hot object and the segments of the touched keys it lacks.
/// `new_markers` are the drops this build purges.
pub(crate) async fn load(
    cs: &dyn ContentStore,
    refs: Option<&StatsStoreRefs>,
    touched: &HashSet<StatsKey>,
    new_markers: &[DropMarker],
) -> Result<LoadedStore> {
    let mut out = LoadedStore::default();
    let Some(refs) = refs else {
        out.markers = merge_markers([], new_markers.iter().copied());
        return Ok(out);
    };

    let mut hot_entries = Vec::new();
    let mut old_markers = Vec::new();
    if let Some(cid) = &refs.hot {
        let bytes = fetch(cs, cid, "hot object").await?;
        out.objects_read += 1;
        out.bytes_read += bytes.len();
        let hot = StoreObject::decode(&bytes).map_err(|e| decode_error("hot object", cid, e))?;
        if hot.kind != StoreObjectKind::Hot {
            return Err(decode_error("hot object", cid, "not a hot object"));
        }
        hot_entries = hot.entries;
        old_markers = hot.markers;
    }
    out.markers = merge_markers(old_markers, new_markers.iter().copied());
    for e in hot_entries {
        if !hidden(&out.markers, &e.summary) {
            out.hot.insert(e.summary.key.clone(), e);
        }
    }

    let mut by_segment: BTreeMap<usize, HashSet<StatsKey>> = BTreeMap::new();
    for key in touched {
        match out.hot.get(key) {
            Some(e) => {
                out.prior.insert(key.clone(), e.clone());
            }
            None => {
                if let Some(i) = owning_segment(&refs.segments, key) {
                    by_segment.entry(i).or_default().insert(key.clone());
                }
            }
        }
    }
    use futures::StreamExt;
    let reads: Vec<_> = by_segment
        .into_iter()
        .map(|(i, keys)| fetch_owned(cs, refs.segments[i].cid.clone(), keys))
        .collect();
    let mut fetched = futures::stream::iter(reads).buffer_unordered(SEGMENT_FETCH_WIDTH);
    while let Some(read) = fetched.next().await {
        let (cid, keys, bytes) = read?;
        out.objects_read += 1;
        out.bytes_read += bytes.len();
        let segment = StoreObject::decode_selected(&bytes, |k| keys.contains(k))
            .map_err(|e| decode_error("segment", &cid, e))?;
        for e in segment.entries {
            if !hidden(&out.markers, &e.summary) {
                out.prior.insert(e.summary.key.clone(), e);
            }
        }
    }
    Ok(out)
}

/// The hook's working state for an entry read from the store.
pub(crate) fn to_hll(e: &StatsEntry) -> IdPropertyHll {
    let s = &e.summary;
    let mut hll = IdPropertyHll::from_sketches(
        s.count as i64,
        HllSketch256::from_bytes(&e.values),
        HllSketch256::from_bytes(&e.subjects),
        s.last_modified_t,
        s.datatypes.iter().map(|&(t, n)| (t, n as i64)).collect(),
    );
    hll.seed_historical_tags(s.historical_datatypes.iter().copied());
    hll
}

/// An entry's summary from its finalized statistics.
fn summary_of(key: StatsKey, stat: &GraphPropertyStatEntry) -> EntrySummary {
    let mut datatypes: Vec<(u8, u64)> = stat
        .datatypes
        .iter()
        .copied()
        .filter(|&(_, n)| n > 0)
        .collect();
    datatypes.sort_unstable();
    let mut historical_datatypes = stat.historical_datatypes.clone();
    historical_datatypes.sort_unstable();
    historical_datatypes.dedup();
    EntrySummary {
        key,
        count: stat.count,
        ndv_values: stat.ndv_values,
        ndv_subjects: stat.ndv_subjects,
        last_modified_t: stat.last_modified_t,
        datatypes,
        historical_datatypes,
    }
}

/// An entry's state after this build: the finalized statistics and the
/// hook's registers.
pub(crate) fn to_entry(
    key: StatsKey,
    stat: &GraphPropertyStatEntry,
    registers: &(HllSketch256, HllSketch256),
) -> StatsEntry {
    StatsEntry {
        summary: summary_of(key, stat),
        values: *registers.0.registers(),
        subjects: *registers.1.registers(),
    }
}

/// Encode and store a hot object; `None` when it holds nothing.
pub(crate) async fn write_hot(
    cs: &dyn ContentStore,
    entries: BTreeMap<StatsKey, StatsEntry>,
    markers: Vec<DropMarker>,
) -> Result<Option<(ContentId, Vec<u8>)>> {
    if entries.is_empty() && markers.is_empty() {
        return Ok(None);
    }
    let object = StoreObject {
        kind: StoreObjectKind::Hot,
        entries: entries.into_values().collect(),
        markers,
    };
    let bytes = object
        .encode(HOT_ZSTD_LEVEL)
        .map_err(|e| IndexerError::StorageWrite(format!("stats hot object: {e}")))?;
    let cid = cs
        .put(ContentKind::StatsSketch, &bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    Ok(Some((cid, bytes)))
}

/// A base segment encoded but not yet stored.
#[derive(Debug)]
pub struct EncodedSegment {
    first: (u32, u32),
    entries: u32,
    bytes: Vec<u8>,
}

static NO_REGISTERS: Registers = [0; 256];

/// Encode every entry as base segments of at most `target_entries`, in key
/// order, chunks in parallel. Entries are encoded straight from the
/// finalized stats and `registers`, never copied into dense entries, and only
/// the compressed segments are kept: a caller can free the registers at once.
/// `graphs` are sorted by graph, their properties by property, as finalize
/// leaves them.
pub fn encode_stats_segments(
    graphs: &[fluree_db_core::index_stats::GraphStatsEntry],
    registers: &HashMap<StatsKey, (HllSketch256, HllSketch256)>,
    target_entries: usize,
) -> Result<Vec<EncodedSegment>> {
    use rayon::prelude::*;
    let flat: Vec<(GraphId, &GraphPropertyStatEntry)> = graphs
        .iter()
        .flat_map(|g| g.properties.iter().map(move |p| (g.g_id, p)))
        .collect();
    let chunks: Vec<Vec<EncodedSegment>> = flat
        .par_chunks(target_entries.max(1))
        .map(|chunk| {
            let keyed: Vec<(StatsKey, EntrySummary)> = chunk
                .iter()
                .map(|&(g_id, stat)| {
                    let key = StatsKey {
                        g_id,
                        p_id: stat.p_id,
                    };
                    (key.clone(), summary_of(key, stat))
                })
                .collect();
            let refs: Vec<EntryRef<'_>> = keyed
                .iter()
                .map(|(key, summary)| {
                    let (values, subjects) = registers
                        .get(key)
                        .map_or((&NO_REGISTERS, &NO_REGISTERS), |(v, s)| {
                            (v.registers(), s.registers())
                        });
                    EntryRef {
                        summary,
                        values,
                        subjects,
                    }
                })
                .collect();
            let mut out = Vec::new();
            encode_capped(&refs, &mut out)?;
            Ok(out)
        })
        .collect::<Result<_>>()?;
    Ok(chunks.into_iter().flatten().collect())
}

/// Encode `refs` as one segment, or as halves while it encodes past the byte
/// cap.
fn encode_capped(refs: &[EntryRef<'_>], out: &mut Vec<EncodedSegment>) -> Result<()> {
    let bytes = encode_entries(StoreObjectKind::Segment, refs, &[], SEGMENT_ZSTD_LEVEL)
        .map_err(|e| IndexerError::StorageWrite(format!("stats segment: {e}")))?;
    if bytes.len() > SEGMENT_MAX_BYTES && refs.len() > 1 {
        let (front, back) = refs.split_at(refs.len() / 2);
        encode_capped(front, out)?;
        return encode_capped(back, out);
    }
    let first = &refs[0].summary.key;
    out.push(EncodedSegment {
        first: (first.g_id, first.p_id),
        entries: refs.len() as u32,
        bytes,
    });
    Ok(())
}

/// Store encoded segments, a few at a time, in order. Returns the
/// references and the bytes written.
pub async fn put_encoded_segments(
    cs: &dyn ContentStore,
    segments: Vec<EncodedSegment>,
) -> Result<(StatsStoreRefs, usize)> {
    use futures::StreamExt;
    let written = segments.iter().map(|s| s.bytes.len()).sum();
    let puts: Vec<_> = segments
        .into_iter()
        .map(|seg| put_encoded(cs, seg))
        .collect();
    let segments = futures::stream::iter(puts)
        .buffered(SEGMENT_FETCH_WIDTH)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    Ok((
        StatsStoreRefs {
            hot: None,
            segments,
        },
        written,
    ))
}

async fn put_encoded(cs: &dyn ContentStore, seg: EncodedSegment) -> Result<StatsSegmentRef> {
    let cid = cs
        .put(ContentKind::StatsSketch, &seg.bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    Ok(StatsSegmentRef {
        first: seg.first,
        entries: seg.entries,
        cid,
    })
}

/// Store `entries` (in key order) as segments of at most `target_entries`,
/// appended to `refs`. With `first`, the first new segment keeps that
/// boundary: a rewritten segment still owns the range it owned.
async fn put_chunked(
    cs: &dyn ContentStore,
    entries: Vec<StatsEntry>,
    target_entries: usize,
    first: Option<(u32, u32)>,
    refs: &mut Vec<StatsSegmentRef>,
) -> Result<usize> {
    let start = refs.len();
    let mut written = 0;
    let mut entries = entries.into_iter().peekable();
    while entries.peek().is_some() {
        let chunk: Vec<_> = entries.by_ref().take(target_entries.max(1)).collect();
        written += put_segments(cs, chunk, refs).await?;
    }
    if let (Some(first), Some(seg)) = (first, refs.get_mut(start)) {
        seg.first = first;
    }
    Ok(written)
}

/// Store `entries` as one segment, or as halves while it encodes past the
/// byte cap.
async fn put_segments(
    cs: &dyn ContentStore,
    entries: Vec<StatsEntry>,
    refs: &mut Vec<StatsSegmentRef>,
) -> Result<usize> {
    let mut pending = vec![entries];
    let mut written = 0;
    while let Some(entries) = pending.pop() {
        let object = StoreObject {
            kind: StoreObjectKind::Segment,
            entries,
            markers: Vec::new(),
        };
        let bytes = object
            .encode(SEGMENT_ZSTD_LEVEL)
            .map_err(|e| IndexerError::StorageWrite(format!("stats segment: {e}")))?;
        let mut entries = object.entries;
        if bytes.len() > SEGMENT_MAX_BYTES && entries.len() > 1 {
            let back = entries.split_off(entries.len() / 2);
            pending.push(back);
            pending.push(entries);
            continue;
        }
        let first = &entries[0].summary.key;
        let cid = cs
            .put(ContentKind::StatsSketch, &bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        refs.push(StatsSegmentRef {
            first: (first.g_id, first.p_id),
            entries: entries.len() as u32,
            cid,
        });
        written += bytes.len();
    }
    Ok(written)
}

/// What a publish's compaction did.
#[derive(Debug, Default)]
pub(crate) struct CompactionOutcome {
    pub steps: usize,
    pub segments_read: usize,
    pub segments_written: usize,
    pub bytes_read: usize,
    pub bytes_written: usize,
}

/// The segments that could hold `g_id`'s entries.
fn graph_segments(segments: &[StatsSegmentRef], g_id: GraphId) -> Option<std::ops::Range<usize>> {
    let first = owning_segment(segments, &StatsKey { g_id, p_id: 0 })?;
    let last = owning_segment(
        segments,
        &StatsKey {
            g_id,
            p_id: u32::MAX,
        },
    )?;
    Some(first..last + 1)
}

/// The next segments to rewrite: the group draining the most hot entries per
/// entry rewritten. A dropped graph's segments form one group, so its marker
/// retires when they are rewritten together.
fn pick_group(
    segments: &[StatsSegmentRef],
    hot: &BTreeMap<StatsKey, StatsEntry>,
    markers: &[DropMarker],
) -> Option<std::ops::Range<usize>> {
    let mut drained = vec![0u64; segments.len()];
    for key in hot.keys() {
        if let Some(i) = owning_segment(segments, key) {
            drained[i] += 1;
        }
    }
    let groups = (0..segments.len()).map(|i| i..i + 1).chain(
        markers
            .iter()
            .filter_map(|m| graph_segments(segments, m.g_id)),
    );
    groups
        .filter_map(|g| {
            let gain: u64 = drained[g.clone()].iter().sum();
            let cost: u64 = segments[g.clone()]
                .iter()
                .map(|s| u64::from(s.entries))
                .sum();
            (gain > 0).then(|| (u128::from(gain) * 1_000_000 / u128::from(cost + 1), g))
        })
        .max_by(|(a, ga), (b, gb)| a.cmp(b).then(gb.start.cmp(&ga.start)))
        .map(|(_, g)| g)
}

/// Fold hot entries into base segments while `hot` is over the policy's
/// threshold: `normal_steps` steps, or as many as it takes to get under the
/// ceiling. Each step rewrites one group of segments with the hot entries
/// they own, dropping entries the markers hide. With no base segments yet,
/// the hot object becomes the first ones. Updates `segments`, `hot` and
/// `markers` in place; the root builder retires the segments replaced.
pub(crate) async fn compact(
    cs: &dyn ContentStore,
    policy: &StatsCompactionPolicy,
    segments: &mut Vec<StatsSegmentRef>,
    hot: &mut BTreeMap<StatsKey, StatsEntry>,
    markers: &mut Vec<DropMarker>,
) -> Result<CompactionOutcome> {
    let mut out = CompactionOutcome::default();
    if hot.len() <= policy.hot_threshold_entries {
        return Ok(out);
    }
    if segments.is_empty() {
        let entries = std::mem::take(hot).into_values().collect();
        out.bytes_written =
            put_chunked(cs, entries, policy.segment_target_entries, None, segments).await?;
        out.segments_written = segments.len();
        out.steps = 1;
        // Nothing in base for a marker to hide.
        markers.clear();
        return Ok(out);
    }
    while hot.len() > policy.hot_threshold_entries
        && (out.steps < policy.normal_steps || hot.len() > policy.hot_ceiling_entries)
    {
        let Some(group) = pick_group(segments, hot, markers) else {
            break;
        };
        let mut merged: BTreeMap<StatsKey, StatsEntry> = BTreeMap::new();
        let reads: Vec<_> = segments[group.clone()]
            .iter()
            .map(|seg| fetch_owned(cs, seg.cid.clone(), ()))
            .collect();
        for (cid, (), bytes) in futures::future::try_join_all(reads).await? {
            out.segments_read += 1;
            out.bytes_read += bytes.len();
            let object =
                StoreObject::decode(&bytes).map_err(|e| decode_error("segment", &cid, e))?;
            for e in object.entries {
                if !hidden(markers, &e.summary) {
                    merged.insert(e.summary.key.clone(), e);
                }
            }
        }
        let owned: Vec<StatsKey> = hot
            .keys()
            .filter(|k| owning_segment(segments, k).is_some_and(|i| group.contains(&i)))
            .cloned()
            .collect();
        for key in owned {
            let entry = hot.remove(&key).expect("an owned key is in hot");
            merged.insert(key, entry);
        }
        markers.retain(|m| {
            !graph_segments(segments, m.g_id)
                .is_some_and(|r| r.start >= group.start && r.end <= group.end)
        });
        let first = segments[group.start].first;
        let mut rewritten = Vec::new();
        out.bytes_written += put_chunked(
            cs,
            merged.into_values().collect(),
            policy.segment_target_entries,
            Some(first),
            &mut rewritten,
        )
        .await?;
        out.segments_written += rewritten.len();
        segments.splice(group, rewritten);
        out.steps += 1;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(first: (u32, u32)) -> StatsSegmentRef {
        StatsSegmentRef {
            first,
            entries: 1,
            cid: ContentId::new(ContentKind::StatsSketch, format!("{first:?}").as_bytes()),
        }
    }

    #[test]
    fn a_key_belongs_to_the_last_segment_starting_at_or_before_it() {
        let segments = [seg((0, 0)), seg((10, 5)), seg((20, 0))];
        let owner = |g, p| owning_segment(&segments, &StatsKey { g_id: g, p_id: p });
        assert_eq!(owner(0, 0), Some(0));
        assert_eq!(owner(10, 4), Some(0));
        assert_eq!(owner(10, 5), Some(1));
        assert_eq!(owner(19, 99), Some(1));
        assert_eq!(
            owner(500, 0),
            Some(2),
            "past the last first key: the last segment"
        );
        let later_start = [seg((5, 0))];
        assert_eq!(
            owning_segment(&later_start, &StatsKey { g_id: 1, p_id: 0 }),
            Some(0),
            "before the first segment's first key: the first segment"
        );
        assert_eq!(owning_segment(&[], &StatsKey { g_id: 1, p_id: 0 }), None);
    }

    fn entry(g_id: GraphId, p_id: u32, count: u64, last_t: i64) -> StatsEntry {
        StatsEntry {
            summary: EntrySummary {
                key: StatsKey { g_id, p_id },
                count,
                ndv_values: count,
                ndv_subjects: count,
                last_modified_t: last_t,
                datatypes: vec![(3, count)],
                historical_datatypes: vec![3],
            },
            values: [0; 256],
            subjects: [0; 256],
        }
    }

    fn policy(threshold: usize, ceiling: usize, target: usize) -> StatsCompactionPolicy {
        StatsCompactionPolicy {
            hot_threshold_entries: threshold,
            hot_ceiling_entries: ceiling,
            normal_steps: 1,
            segment_target_entries: target,
        }
    }

    async fn segment_entries(cs: &dyn ContentStore, seg: &StatsSegmentRef) -> Vec<StatsEntry> {
        StoreObject::decode(&cs.get(&seg.cid).await.unwrap())
            .unwrap()
            .entries
    }

    fn hot_of(entries: impl IntoIterator<Item = StatsEntry>) -> BTreeMap<StatsKey, StatsEntry> {
        entries
            .into_iter()
            .map(|e| (e.summary.key.clone(), e))
            .collect()
    }

    /// Three segments of ten graphs' entries each, at t = 1.
    async fn base(cs: &dyn ContentStore) -> Vec<StatsSegmentRef> {
        let mut segments = Vec::new();
        let entries: Vec<_> = (0..30).map(|g| entry(g, 1, 10, 1)).collect();
        put_chunked(cs, entries, 10, None, &mut segments)
            .await
            .unwrap();
        assert_eq!(segments.len(), 3);
        segments
    }

    #[tokio::test]
    async fn the_first_compaction_turns_the_hot_object_into_segments() {
        let cs = fluree_db_core::MemoryContentStore::new();
        let mut segments = Vec::new();
        let mut hot = hot_of((0..7).map(|g| entry(g, 1, 1, 5)));
        let mut markers = vec![DropMarker { g_id: 99, floor: 3 }];
        let out = compact(
            &cs,
            &policy(5, 20, 4),
            &mut segments,
            &mut hot,
            &mut markers,
        )
        .await
        .unwrap();
        assert_eq!((out.steps, segments.len()), (1, 2));
        assert!(hot.is_empty() && markers.is_empty());
        assert_eq!(segment_entries(&cs, &segments[1]).await.len(), 3);
    }

    #[tokio::test]
    async fn a_step_drains_the_segment_owning_most_hot_entries_and_keeps_its_range() {
        let cs = fluree_db_core::MemoryContentStore::new();
        let mut segments = base(&cs).await;
        let untouched = (segments[0].clone(), segments[2].clone());
        // Segment 1 owns graphs 10..20: four updates and a new key; segment 2
        // one update.
        let mut hot = hot_of(
            [12, 13, 14, 15]
                .map(|g| entry(g, 1, 50, 9))
                .into_iter()
                .chain([entry(17, 2, 1, 9), entry(25, 1, 50, 9)]),
        );
        let mut markers = Vec::new();
        let out = compact(
            &cs,
            &policy(3, 100, 20),
            &mut segments,
            &mut hot,
            &mut markers,
        )
        .await
        .unwrap();
        assert_eq!(
            out.steps, 1,
            "one step between the threshold and the ceiling"
        );
        assert_eq!(hot.keys().map(|k| k.g_id).collect::<Vec<_>>(), vec![25]);
        assert_eq!((segments[0].clone(), segments[2].clone()), untouched);
        assert_eq!(segments[1].first, (10, 1));
        let merged = segment_entries(&cs, &segments[1]).await;
        assert_eq!(merged.len(), 11, "ten base entries and the new key");
        assert!(merged
            .iter()
            .filter(|e| (12..=15).contains(&e.summary.key.g_id))
            .all(|e| e.summary.count == 50));
    }

    #[tokio::test]
    async fn compaction_drops_what_a_marker_hides_and_retires_it() {
        let cs = fluree_db_core::MemoryContentStore::new();
        let mut segments = base(&cs).await;
        // Graph 11 dropped at t = 4 and recreated at the floor; graph 12
        // dropped with nothing recreated.
        let mut hot = hot_of([entry(11, 1, 2, 4), entry(13, 1, 50, 9), entry(14, 1, 50, 9)]);
        let mut markers = vec![
            DropMarker { g_id: 11, floor: 4 },
            DropMarker { g_id: 12, floor: 4 },
        ];
        compact(
            &cs,
            &policy(2, 100, 20),
            &mut segments,
            &mut hot,
            &mut markers,
        )
        .await
        .unwrap();
        assert!(
            markers.is_empty(),
            "both graphs' only segment was rewritten"
        );
        let merged = segment_entries(&cs, &segments[1]).await;
        let graph = |g| merged.iter().find(|e| e.summary.key.g_id == g);
        assert!(graph(12).is_none(), "the dropped graph's entry is gone");
        assert_eq!(graph(11).unwrap().summary.count, 2, "the recreated graph's");
    }

    #[tokio::test]
    async fn above_the_ceiling_steps_continue_until_under_it() {
        let cs = fluree_db_core::MemoryContentStore::new();
        let mut segments = base(&cs).await;
        let mut hot = hot_of((0..30).map(|g| entry(g, 2, 1, 9)));
        let mut markers = Vec::new();
        let out = compact(
            &cs,
            &policy(5, 12, 100),
            &mut segments,
            &mut hot,
            &mut markers,
        )
        .await
        .unwrap();
        assert_eq!(out.steps, 2, "30 → 20 → 10, under the ceiling of 12");
        assert_eq!(hot.len(), 10);
    }

    #[tokio::test]
    async fn a_segment_that_outgrows_its_target_splits_and_keeps_its_first_boundary() {
        let cs = fluree_db_core::MemoryContentStore::new();
        let mut segments = base(&cs).await;
        let mut hot = hot_of((0..12).map(|p| entry(10, p + 2, 1, 9)));
        let mut markers = Vec::new();
        compact(
            &cs,
            &policy(5, 100, 10),
            &mut segments,
            &mut hot,
            &mut markers,
        )
        .await
        .unwrap();
        assert_eq!(segments.len(), 5, "22 entries in segments of 10: three");
        assert_eq!(segments[1].first, (10, 1));
        assert!(segments.windows(2).all(|w| w[0].first < w[1].first));
        let total: u32 = segments.iter().map(|s| s.entries).sum();
        assert_eq!(total, 42);
    }

    #[test]
    fn merged_markers_keep_each_graphs_latest_floor() {
        let m = |g_id, floor| DropMarker { g_id, floor };
        assert_eq!(
            merge_markers([m(2, 10), m(5, 3)], [m(2, 7), m(9, 1), m(5, 8)]),
            vec![m(2, 10), m(5, 8), m(9, 1)]
        );
    }
}
