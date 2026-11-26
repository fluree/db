//! The statistics store as a reader holds it: every segment's summaries and
//! the hot object's, from which the per-graph property stats are rebuilt.
//!
//! Segments change only when compaction rewrites them, so a view loaded for
//! a new root takes the segments it shares with the previous view from it,
//! by content id, and decodes only the hot object and new segments.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use fluree_db_core::index_stats::{GraphPropertyStatEntry, GraphStatsEntry, PropertyStatEntry};
use fluree_db_core::{ContentId, ContentStore};

use crate::format::stats_store::{
    DropMarker, EntrySummary, StoreObject, StoreObjectKind, StoreSummaries,
};
use crate::StatsStoreRefs;

/// Segments fetched at once: remote reads pay latency, not bandwidth.
const FETCH_WIDTH: usize = 8;

/// Decoded summaries of one root's statistics store.
#[derive(Debug, Default)]
pub struct StatsStoreView {
    /// In key order, as the root lists them.
    segments: Vec<(ContentId, Arc<StoreSummaries>)>,
    hot: Option<StoreSummaries>,
}

/// What loading a view read.
#[derive(Debug, Default, Clone, Copy)]
pub struct ViewLoad {
    /// Objects fetched and decoded: the hot object and new segments.
    pub objects_read: usize,
    pub bytes_read: usize,
    /// Segments taken from the previous view.
    pub segments_reused: usize,
}

fn invalid(what: &str, cid: &ContentId, e: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("stats store {what} {cid}: {e}"),
    )
}

async fn summaries(
    cs: &dyn ContentStore,
    cid: ContentId,
    kind: StoreObjectKind,
) -> io::Result<(ContentId, StoreSummaries, usize)> {
    let what = match kind {
        StoreObjectKind::Hot => "hot object",
        StoreObjectKind::Segment => "segment",
    };
    let bytes = cs
        .get(&cid)
        .await
        .map_err(|e| io::Error::other(format!("stats store {what} {cid}: {e}")))?;
    let decoded = StoreObject::decode_summaries(&bytes).map_err(|e| invalid(what, &cid, e))?;
    if decoded.kind != kind {
        return Err(invalid(what, &cid, "wrong object kind"));
    }
    Ok((cid, decoded, bytes.len()))
}

impl StatsStoreView {
    /// Load the summaries `refs` names, taking every segment `prev` already
    /// holds from it.
    pub async fn load(
        cs: &dyn ContentStore,
        refs: &StatsStoreRefs,
        prev: Option<&StatsStoreView>,
    ) -> io::Result<(Self, ViewLoad)> {
        use futures::StreamExt;
        let mut stats = ViewLoad::default();
        let known: HashMap<&ContentId, &Arc<StoreSummaries>> = prev
            .map(|p| p.segments.iter().map(|(cid, s)| (cid, s)).collect())
            .unwrap_or_default();

        let mut segments: Vec<Option<Arc<StoreSummaries>>> =
            Vec::with_capacity(refs.segments.len());
        let mut wanted = Vec::new();
        for (i, seg) in refs.segments.iter().enumerate() {
            match known.get(&seg.cid) {
                Some(s) => {
                    stats.segments_reused += 1;
                    segments.push(Some(Arc::clone(s)));
                }
                None => {
                    segments.push(None);
                    wanted.push((i, seg.cid.clone()));
                }
            }
        }

        let hot = async {
            match &refs.hot {
                Some(cid) => summaries(cs, cid.clone(), StoreObjectKind::Hot)
                    .await
                    .map(Some),
                None => Ok(None),
            }
        };
        let fetched = futures::stream::iter(wanted.into_iter().map(|(i, cid)| async move {
            summaries(cs, cid, StoreObjectKind::Segment)
                .await
                .map(|read| (i, read))
        }))
        .buffer_unordered(FETCH_WIDTH)
        .collect::<Vec<_>>();
        let (hot, fetched) = futures::join!(hot, fetched);

        let hot = hot?.map(|(_, hot, len)| {
            stats.objects_read += 1;
            stats.bytes_read += len;
            hot
        });
        for read in fetched {
            let (i, (_, decoded, len)) = read?;
            stats.objects_read += 1;
            stats.bytes_read += len;
            segments[i] = Some(Arc::new(decoded));
        }
        let segments = refs
            .segments
            .iter()
            .zip(segments)
            .map(|(seg, s)| (seg.cid.clone(), s.expect("every segment reused or fetched")))
            .collect();
        Ok((Self { segments, hot }, stats))
    }

    /// A view of `segments` (in key order, any subset of a store's) under
    /// `hot`.
    pub fn from_parts(
        segments: Vec<(ContentId, Arc<StoreSummaries>)>,
        hot: Option<StoreSummaries>,
    ) -> Self {
        Self { segments, hot }
    }

    /// This view with `hot` in place of its hot object: a writer's, before
    /// it is stored.
    pub fn with_hot(mut self, hot: Option<StoreSummaries>) -> Self {
        self.hot = hot;
        self
    }

    /// Every visible entry in key order: hot entries over the segments', less
    /// what a drop marker hides.
    pub fn visible(&self) -> Vec<&EntrySummary> {
        let (hot, markers): (&[EntrySummary], &[DropMarker]) = match &self.hot {
            Some(h) => (&h.summaries, &h.markers),
            None => (&[], &[]),
        };
        let hidden = |e: &EntrySummary| {
            markers
                .binary_search_by_key(&e.key.g_id, |m| m.g_id)
                .is_ok_and(|i| markers[i].hides(e))
        };
        let base = self.segments.iter().flat_map(|(_, s)| s.summaries.iter());
        let mut out = Vec::with_capacity(
            hot.len()
                + self
                    .segments
                    .iter()
                    .map(|(_, s)| s.summaries.len())
                    .sum::<usize>(),
        );
        let mut hot = hot.iter().peekable();
        for e in base {
            while let Some(h) = hot.next_if(|h| h.key < e.key) {
                out.push(h);
            }
            match hot.next_if(|h| h.key == e.key) {
                Some(h) => out.push(h),
                None => out.push(e),
            }
        }
        out.extend(hot);
        out.retain(|e| !hidden(e));
        out
    }

    /// The per-graph property stats, sorted by graph then property; a
    /// graph's flakes are its entries' counts summed, as a build counts them.
    /// Sizes and class tables are the caller's (`IndexStats::fill_graphs`).
    pub fn graph_stats(&self) -> Vec<GraphStatsEntry> {
        let mut graphs: Vec<GraphStatsEntry> = Vec::new();
        for e in self.visible() {
            if graphs.last().is_none_or(|g| g.g_id != e.key.g_id) {
                graphs.push(GraphStatsEntry {
                    g_id: e.key.g_id,
                    flakes: 0,
                    size: 0,
                    properties: Vec::new(),
                    classes: None,
                });
            }
            let g = graphs.last_mut().expect("pushed above");
            g.flakes += e.count;
            g.properties.push(GraphPropertyStatEntry {
                p_id: e.key.p_id,
                count: e.count,
                ndv_values: e.ndv_values,
                ndv_subjects: e.ndv_subjects,
                last_modified_t: e.last_modified_t,
                observed_datatypes: PropertyStatEntry::tags_of(&e.datatypes),
                historical_datatypes: e.historical_datatypes.clone(),
                datatypes: e.datatypes.clone(),
            });
        }
        graphs
    }
}

/// `root`'s statistics with the per-graph stats filled from its statistics
/// store: what a reader of the root alone (a tool, a test) sees of them.
pub async fn root_stats_with_graphs(
    cs: &dyn ContentStore,
    root: &crate::format::index_root::IndexRoot,
) -> io::Result<Option<fluree_db_core::IndexStats>> {
    let Some(mut stats) = root.stats.clone() else {
        return Ok(None);
    };
    if let (true, Some(refs)) = (stats.graphs_unfilled(), &root.stats_store) {
        let (view, _) = StatsStoreView::load(cs, refs, None).await?;
        stats.fill_graphs(view.graph_stats());
    }
    Ok(Some(stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::stats_store::{StatsEntry, StatsKey};
    use crate::StatsSegmentRef;
    use fluree_db_core::{ContentKind, MemoryContentStore};

    fn entry(g_id: u32, p_id: u32, count: u64, t: i64) -> StatsEntry {
        StatsEntry {
            summary: EntrySummary {
                key: StatsKey { g_id, p_id },
                count,
                ndv_values: count,
                ndv_subjects: 1,
                last_modified_t: t,
                datatypes: vec![(1, count)],
                historical_datatypes: vec![1],
            },
            values: [0; 256],
            subjects: [0; 256],
        }
    }

    async fn put(
        cs: &MemoryContentStore,
        kind: StoreObjectKind,
        entries: Vec<StatsEntry>,
        markers: Vec<DropMarker>,
    ) -> ContentId {
        let bytes = StoreObject {
            kind,
            entries,
            markers,
        }
        .encode(1)
        .unwrap();
        cs.put(ContentKind::StatsSketch, &bytes).await.unwrap()
    }

    fn seg(first: (u32, u32), last: (u32, u32), entries: u32, cid: ContentId) -> StatsSegmentRef {
        StatsSegmentRef {
            first,
            last,
            entries,
            cid,
        }
    }

    fn flat(graphs: &[GraphStatsEntry]) -> Vec<(u32, u32, u64, i64)> {
        graphs
            .iter()
            .flat_map(|g| {
                g.properties
                    .iter()
                    .map(move |p| (g.g_id, p.p_id, p.count, p.last_modified_t))
            })
            .collect()
    }

    #[tokio::test]
    async fn hot_entries_shadow_segments_and_markers_hide_a_dropped_graph() {
        let cs = MemoryContentStore::new();
        let a = put(
            &cs,
            StoreObjectKind::Segment,
            vec![entry(1, 1, 5, 1), entry(1, 2, 6, 1), entry(2, 1, 7, 1)],
            vec![],
        )
        .await;
        let b = put(
            &cs,
            StoreObjectKind::Segment,
            vec![entry(3, 1, 8, 1), entry(3, 2, 9, 2)],
            vec![],
        )
        .await;
        // Hot: (1,2) changed, (2,5) new, graph 3 dropped at 2 (its (3,2)
        // written at the floor survives), (4,1) new after the last segment.
        let hot = put(
            &cs,
            StoreObjectKind::Hot,
            vec![entry(1, 2, 60, 3), entry(2, 5, 1, 3), entry(4, 1, 2, 3)],
            vec![DropMarker { g_id: 3, floor: 2 }],
        )
        .await;
        let refs = StatsStoreRefs {
            hot: Some(hot),
            segments: vec![seg((0, 0), (2, 1), 3, a), seg((3, 0), (3, 2), 2, b)],
        };
        let (view, load) = StatsStoreView::load(&cs, &refs, None).await.unwrap();
        assert_eq!(load.objects_read, 3);
        let graphs = view.graph_stats();
        assert_eq!(
            flat(&graphs),
            vec![
                (1, 1, 5, 1),
                (1, 2, 60, 3),
                (2, 1, 7, 1),
                (2, 5, 1, 3),
                (3, 2, 9, 2),
                (4, 1, 2, 3),
            ]
        );
        let flakes: Vec<(u32, u64)> = graphs.iter().map(|g| (g.g_id, g.flakes)).collect();
        assert_eq!(flakes, vec![(1, 65), (2, 8), (3, 9), (4, 2)]);
        assert_eq!(graphs[0].properties[1].observed_datatypes, vec![1]);
    }

    #[tokio::test]
    async fn a_reload_reads_only_the_hot_object_and_new_segments() {
        let cs = MemoryContentStore::new();
        let a = put(
            &cs,
            StoreObjectKind::Segment,
            vec![entry(1, 1, 5, 1)],
            vec![],
        )
        .await;
        let b = put(
            &cs,
            StoreObjectKind::Segment,
            vec![entry(2, 1, 5, 1)],
            vec![],
        )
        .await;
        let hot1 = put(&cs, StoreObjectKind::Hot, vec![entry(1, 1, 6, 2)], vec![]).await;
        let refs = StatsStoreRefs {
            hot: Some(hot1),
            segments: vec![seg((0, 0), (1, 1), 1, a.clone()), seg((2, 0), (2, 1), 1, b)],
        };
        let (prev, _) = StatsStoreView::load(&cs, &refs, None).await.unwrap();

        let c = put(
            &cs,
            StoreObjectKind::Segment,
            vec![entry(2, 1, 9, 3)],
            vec![],
        )
        .await;
        let hot2 = put(&cs, StoreObjectKind::Hot, vec![entry(1, 1, 7, 3)], vec![]).await;
        let refs = StatsStoreRefs {
            hot: Some(hot2),
            segments: vec![seg((0, 0), (1, 1), 1, a), seg((2, 0), (2, 1), 1, c)],
        };
        let (view, load) = StatsStoreView::load(&cs, &refs, Some(&prev)).await.unwrap();
        assert_eq!((load.objects_read, load.segments_reused), (2, 1));
        assert_eq!(flat(&view.graph_stats()), vec![(1, 1, 7, 3), (2, 1, 9, 3)]);
    }
}
