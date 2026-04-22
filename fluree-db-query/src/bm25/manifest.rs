//! BM25-owned manifest for snapshot history.
//!
//! The manifest is stored as JSON in content-addressed storage (CAS).
//! Nameservice stores only a head pointer to the latest manifest CID.
//! BM25 owns time-travel selection logic via [`Bm25Manifest::select_snapshot`].

use fluree_db_core::ContentId;
use serde::{Deserialize, Serialize};

/// A single snapshot entry in the BM25 manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bm25SnapshotEntry {
    /// Transaction time (watermark) for this snapshot.
    pub index_t: i64,
    /// Content identifier of the serialized BM25 index blob.
    pub snapshot_id: ContentId,
}

impl Bm25SnapshotEntry {
    pub fn new(index_t: i64, snapshot_id: ContentId) -> Self {
        Self {
            index_t,
            snapshot_id,
        }
    }
}

/// BM25-owned manifest for snapshot history and time-travel.
///
/// Each manifest is immutable and content-addressed (keyed by latest `index_t`).
/// The nameservice head pointer stores the CAS CID of the latest manifest.
///
/// # Append semantics
///
/// Entries must be strictly monotonically increasing by `index_t`, with one
/// exception: the last entry may be **replaced** when `index_t == last.index_t`
/// (idempotent reindex).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bm25Manifest {
    /// Graph source ID this manifest belongs to (e.g., "my-search:main").
    pub graph_source_id: String,
    /// Ordered list of snapshots (ascending by `index_t`).
    pub snapshots: Vec<Bm25SnapshotEntry>,
}

impl Bm25Manifest {
    /// Create a new empty manifest.
    pub fn new(graph_source_id: impl Into<String>) -> Self {
        Self {
            graph_source_id: graph_source_id.into(),
            snapshots: Vec::new(),
        }
    }

    /// Select the best snapshot for a given `as_of_t`.
    ///
    /// Returns the snapshot with `index_t = max { t | t <= as_of_t }`,
    /// or `None` if no suitable snapshot exists.
    pub fn select_snapshot(&self, as_of_t: i64) -> Option<&Bm25SnapshotEntry> {
        self.snapshots.iter().rev().find(|s| s.index_t <= as_of_t)
    }

    /// Get the most recent snapshot (head).
    pub fn head(&self) -> Option<&Bm25SnapshotEntry> {
        self.snapshots.last()
    }

    /// Check if a snapshot exists at exactly the given `t`.
    pub fn has_snapshot_at(&self, t: i64) -> bool {
        self.snapshots.iter().any(|s| s.index_t == t)
    }

    /// Append a snapshot entry.
    ///
    /// Entries must be strictly monotonically increasing, except that
    /// the last entry may be replaced if `index_t == last.index_t`
    /// (idempotent reindex). Returns `true` if added/replaced, `false`
    /// if rejected (lower `t` than existing).
    pub fn append(&mut self, entry: Bm25SnapshotEntry) -> bool {
        if let Some(last) = self.snapshots.last() {
            if entry.index_t < last.index_t {
                return false; // Reject: going backwards
            }
            if entry.index_t == last.index_t {
                // Idempotent reindex: replace last entry
                *self.snapshots.last_mut().unwrap() = entry;
                return true;
            }
        }
        self.snapshots.push(entry);
        true
    }

    /// Get all snapshot CIDs (for cleanup/deletion on drop).
    pub fn all_snapshot_ids(&self) -> Vec<&ContentId> {
        self.snapshots.iter().map(|s| &s.snapshot_id).collect()
    }

    /// Trim old snapshots beyond the retention limit.
    ///
    /// Keeps the most recent `keep` snapshots and returns the CIDs of
    /// removed entries so the caller can delete the old blobs from storage.
    /// Returns an empty vec if no trimming was needed.
    pub fn trim(&mut self, keep: usize) -> Vec<ContentId> {
        if self.snapshots.len() <= keep {
            return Vec::new();
        }
        let remove_count = self.snapshots.len() - keep;
        let removed: Vec<ContentId> = self.snapshots[..remove_count]
            .iter()
            .map(|s| s.snapshot_id.clone())
            .collect();
        self.snapshots.drain(..remove_count);
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ContentKind;

    /// Helper: create a test ContentId for a snapshot.
    fn test_cid(label: &[u8]) -> ContentId {
        ContentId::new(ContentKind::GraphSourceSnapshot, label)
    }

    #[test]
    fn test_empty_manifest() {
        let m = Bm25Manifest::new("test:main");
        assert_eq!(m.graph_source_id, "test:main");
        assert!(m.snapshots.is_empty());
        assert!(m.head().is_none());
        assert!(m.select_snapshot(100).is_none());
        assert!(!m.has_snapshot_at(1));
        assert!(m.all_snapshot_ids().is_empty());
    }

    #[test]
    fn test_append_and_head() {
        let mut m = Bm25Manifest::new("test:main");
        assert!(m.append(Bm25SnapshotEntry::new(5, test_cid(b"snap-5"))));
        assert!(m.append(Bm25SnapshotEntry::new(10, test_cid(b"snap-10"))));
        assert!(m.append(Bm25SnapshotEntry::new(20, test_cid(b"snap-20"))));

        assert_eq!(m.snapshots.len(), 3);
        assert_eq!(m.head().unwrap().index_t, 20);
    }

    #[test]
    fn test_append_monotonic_rejection() {
        let mut m = Bm25Manifest::new("test:main");
        assert!(m.append(Bm25SnapshotEntry::new(10, test_cid(b"snap-10"))));
        assert!(!m.append(Bm25SnapshotEntry::new(5, test_cid(b"snap-5")))); // Rejected
        assert_eq!(m.snapshots.len(), 1);
    }

    #[test]
    fn test_append_same_t_replaces_last() {
        let mut m = Bm25Manifest::new("test:main");
        let cid_5 = test_cid(b"snap-5");
        let cid_10_v2 = test_cid(b"snap-10-v2");
        assert!(m.append(Bm25SnapshotEntry::new(5, cid_5.clone())));
        assert!(m.append(Bm25SnapshotEntry::new(10, test_cid(b"snap-10-v1"))));
        assert!(m.append(Bm25SnapshotEntry::new(10, cid_10_v2.clone()))); // Idempotent reindex

        assert_eq!(m.snapshots.len(), 2);
        assert_eq!(m.head().unwrap().snapshot_id, cid_10_v2);
        // Earlier entry is untouched
        assert_eq!(m.snapshots[0].snapshot_id, cid_5);
    }

    #[test]
    fn test_select_snapshot() {
        let mut m = Bm25Manifest::new("test:main");
        m.append(Bm25SnapshotEntry::new(5, test_cid(b"snap-5")));
        m.append(Bm25SnapshotEntry::new(10, test_cid(b"snap-10")));
        m.append(Bm25SnapshotEntry::new(20, test_cid(b"snap-20")));

        // Before any snapshot
        assert!(m.select_snapshot(3).is_none());

        // Exact match
        assert_eq!(m.select_snapshot(5).unwrap().index_t, 5);
        assert_eq!(m.select_snapshot(10).unwrap().index_t, 10);
        assert_eq!(m.select_snapshot(20).unwrap().index_t, 20);

        // Between snapshots: returns largest <= as_of_t
        assert_eq!(m.select_snapshot(7).unwrap().index_t, 5);
        assert_eq!(m.select_snapshot(15).unwrap().index_t, 10);

        // After all snapshots
        assert_eq!(m.select_snapshot(100).unwrap().index_t, 20);
    }

    #[test]
    fn test_has_snapshot_at() {
        let mut m = Bm25Manifest::new("test:main");
        m.append(Bm25SnapshotEntry::new(5, test_cid(b"snap-5")));
        m.append(Bm25SnapshotEntry::new(10, test_cid(b"snap-10")));

        assert!(m.has_snapshot_at(5));
        assert!(m.has_snapshot_at(10));
        assert!(!m.has_snapshot_at(7));
        assert!(!m.has_snapshot_at(1));
    }

    #[test]
    fn test_all_snapshot_ids() {
        let mut m = Bm25Manifest::new("test:main");
        let cid_5 = test_cid(b"snap-5");
        let cid_10 = test_cid(b"snap-10");
        m.append(Bm25SnapshotEntry::new(5, cid_5.clone()));
        m.append(Bm25SnapshotEntry::new(10, cid_10.clone()));

        let ids = m.all_snapshot_ids();
        assert_eq!(ids, vec![&cid_5, &cid_10]);
    }

    #[test]
    fn test_trim_removes_oldest() {
        let mut m = Bm25Manifest::new("test:main");
        let cid_1 = test_cid(b"snap-1");
        let cid_2 = test_cid(b"snap-2");
        m.append(Bm25SnapshotEntry::new(1, cid_1.clone()));
        m.append(Bm25SnapshotEntry::new(2, cid_2.clone()));
        m.append(Bm25SnapshotEntry::new(3, test_cid(b"snap-3")));
        m.append(Bm25SnapshotEntry::new(4, test_cid(b"snap-4")));
        m.append(Bm25SnapshotEntry::new(5, test_cid(b"snap-5")));

        let removed = m.trim(3);
        assert_eq!(removed, vec![cid_1, cid_2]);
        assert_eq!(m.snapshots.len(), 3);
        assert_eq!(m.snapshots[0].index_t, 3);
        assert_eq!(m.head().unwrap().index_t, 5);
    }

    #[test]
    fn test_trim_no_op_when_under_limit() {
        let mut m = Bm25Manifest::new("test:main");
        m.append(Bm25SnapshotEntry::new(1, test_cid(b"snap-1")));
        m.append(Bm25SnapshotEntry::new(2, test_cid(b"snap-2")));

        let removed = m.trim(5);
        assert!(removed.is_empty());
        assert_eq!(m.snapshots.len(), 2);
    }

    #[test]
    fn test_trim_to_one() {
        let mut m = Bm25Manifest::new("test:main");
        let cid_1 = test_cid(b"snap-1");
        let cid_2 = test_cid(b"snap-2");
        m.append(Bm25SnapshotEntry::new(1, cid_1.clone()));
        m.append(Bm25SnapshotEntry::new(2, cid_2.clone()));
        m.append(Bm25SnapshotEntry::new(3, test_cid(b"snap-3")));

        let removed = m.trim(1);
        assert_eq!(removed, vec![cid_1, cid_2]);
        assert_eq!(m.snapshots.len(), 1);
        assert_eq!(m.head().unwrap().index_t, 3);
    }

    #[test]
    fn test_serde_roundtrip() {
        let mut m = Bm25Manifest::new("my-search:main");
        m.append(Bm25SnapshotEntry::new(5, test_cid(b"snap-5")));
        m.append(Bm25SnapshotEntry::new(10, test_cid(b"snap-10")));

        let json = serde_json::to_string(&m).unwrap();
        let deserialized: Bm25Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(m, deserialized);
    }
}
