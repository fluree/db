//! The statistics store as an index build reads and writes it.
//!
//! An incremental build reads the hot object whole and, for each touched key
//! it lacks, that key's base segment; it never reads a segment it does not
//! need. It writes the hot object back with the touched entries' new state.

use std::collections::{BTreeMap, HashMap, HashSet};

use fluree_db_binary_index::format::stats_store::{
    DropMarker, EntrySummary, StatsEntry, StatsKey, StoreObject, StoreObjectKind,
};
use fluree_db_binary_index::{StatsSegmentRef, StatsStoreRefs};
use fluree_db_core::index_stats::GraphPropertyStatEntry;
use fluree_db_core::{ContentId, ContentKind, ContentStore};

use crate::error::{IndexerError, Result};
use crate::hll::HllSketch256;

use super::id_hook::IdPropertyHll;

/// The hot object is rewritten on every publish, so it favors speed.
pub(crate) const HOT_ZSTD_LEVEL: i32 = 1;
/// Base segments are written rarely and read on first touch: smaller wins.
pub(crate) const SEGMENT_ZSTD_LEVEL: i32 = 3;
/// Provisional sizing: about 500 KB at the ~10 bytes an entry measured on
/// docgraphs, large enough that remote reads pay latency rarely.
pub(crate) const SEGMENT_TARGET_ENTRIES: usize = 50_000;
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

    let mut by_segment: BTreeMap<usize, HashSet<&StatsKey>> = BTreeMap::new();
    for key in touched {
        match out.hot.get(key) {
            Some(e) => {
                out.prior.insert(key.clone(), e.clone());
            }
            None => {
                if let Some(i) = owning_segment(&refs.segments, key) {
                    by_segment.entry(i).or_default().insert(key);
                }
            }
        }
    }
    for (i, keys) in by_segment {
        let cid = &refs.segments[i].cid;
        let bytes = fetch(cs, cid, "segment").await?;
        out.objects_read += 1;
        out.bytes_read += bytes.len();
        let segment = StoreObject::decode_selected(&bytes, |k| keys.contains(k))
            .map_err(|e| decode_error("segment", cid, e))?;
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

/// An entry's state after this build: the finalized statistics and the
/// hook's registers.
pub(crate) fn to_entry(
    key: StatsKey,
    stat: &GraphPropertyStatEntry,
    registers: &(HllSketch256, HllSketch256),
) -> StatsEntry {
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
    StatsEntry {
        summary: EntrySummary {
            key,
            count: stat.count,
            ndv_values: stat.ndv_values,
            ndv_subjects: stat.ndv_subjects,
            last_modified_t: stat.last_modified_t,
            datatypes,
            historical_datatypes,
        },
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

/// Write every entry as base segments, in key order, one segment's entries
/// in memory at a time. `graphs` are sorted by graph, their properties by
/// property, as finalize leaves them; `registers` has every entry's HLLs.
/// Returns the references and the bytes written.
pub(crate) async fn write_segments(
    cs: &dyn ContentStore,
    graphs: &[fluree_db_core::index_stats::GraphStatsEntry],
    mut registers: HashMap<StatsKey, (HllSketch256, HllSketch256)>,
) -> Result<(StatsStoreRefs, usize)> {
    let mut refs = StatsStoreRefs::default();
    let mut written = 0;
    let mut chunk = Vec::with_capacity(SEGMENT_TARGET_ENTRIES);
    let props = graphs
        .iter()
        .flat_map(|g| g.properties.iter().map(move |p| (g.g_id, p)));
    for (g_id, prop) in props {
        let key = StatsKey {
            g_id,
            p_id: prop.p_id,
        };
        let regs = registers.remove(&key).unwrap_or_default();
        chunk.push(to_entry(key, prop, &regs));
        if chunk.len() == SEGMENT_TARGET_ENTRIES {
            written += put_segments(cs, std::mem::take(&mut chunk), &mut refs).await?;
        }
    }
    if !chunk.is_empty() {
        written += put_segments(cs, chunk, &mut refs).await?;
    }
    Ok((refs, written))
}

/// Store `entries` as one segment, or as halves while it encodes past the
/// byte cap.
async fn put_segments(
    cs: &dyn ContentStore,
    entries: Vec<StatsEntry>,
    refs: &mut StatsStoreRefs,
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
        refs.segments.push(StatsSegmentRef {
            first: (first.g_id, first.p_id),
            entries: entries.len() as u32,
            cid,
        });
        written += bytes.len();
    }
    Ok(written)
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

    #[test]
    fn merged_markers_keep_each_graphs_latest_floor() {
        let m = |g_id, floor| DropMarker { g_id, floor };
        assert_eq!(
            merge_markers([m(2, 10), m(5, 3)], [m(2, 7), m(9, 1), m(5, 8)]),
            vec![m(2, 10), m(5, 8), m(9, 1)]
        );
    }
}
