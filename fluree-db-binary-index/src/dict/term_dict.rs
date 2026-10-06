//! Triple-term dictionary: `OType::TRIPLE_TERM` handles ↔ encoded base edges.
//!
//! One reification link is a main-index flake `_:r rdf:reifies <handle>`.
//! This dictionary gives the handle its meaning: a [`TermKey`], the base
//! edge's `(s_id, p_id, o_type, o_key)` exactly as the main index stores it.
//!
//! Layout mirrors the subject dictionary with the inner predicate in the
//! namespace role: one forward pack stream per inner predicate keyed by the
//! per-predicate sequence (the low half of the handle), and one reverse tree
//! keyed by the big-endian `TermKey` bytes (subject-first, so a subject-bound
//! reified-triple pattern is one key range).

use crate::dict::builder::{build_reverse_tree, finalize_branch, DEFAULT_TARGET_LEAF_BYTES};
use crate::dict::forward_pack::{encode_forward_pack, KIND_TERM_FWD};
use crate::dict::pack_builder::{DEFAULT_TARGET_PACK_BYTES, DEFAULT_TARGET_PAGE_BYTES};
use crate::dict::pack_reader::ForwardPackReader;
use crate::dict::reader::DictTreeReader;
use crate::dict::reverse_leaf::ReverseEntry;
use crate::format::wire_helpers::{DictTreeRefs, PackBranchEntry, TermDictRefs};
use crate::read::leaflet_cache::LeafletCache;
use fluree_db_core::triple_term::{term_handle, term_handle_p_id, term_handle_seq, TermKey};
use fluree_db_core::{ContentId, ContentKind, ContentStore, DictKind};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::Path;
use std::sync::Arc;

/// The pack header's `ns_code` slot is 16 bits; the low half of the inner
/// predicate id goes there as a read-time consistency check.
#[inline]
pub fn pack_ns_code(p_id: u32) -> u16 {
    (p_id & 0xFFFF) as u16
}

/// The smallest reverse key past every key of subject `s_id`.
fn next_subject(s_id: u64) -> [u8; TermKey::LEN] {
    match s_id.checked_add(1) {
        Some(next) => {
            let mut end = [0u8; TermKey::LEN];
            end[..8].copy_from_slice(&next.to_be_bytes());
            end
        }
        None => [0xFF; TermKey::LEN],
    }
}

// ── Reader ──────────────────────────────────────────────────────────────────

/// Read side of the triple-term dictionary.
pub struct TermDictReader {
    forward: BTreeMap<u32, ForwardPackReader>,
    reverse: Option<Arc<DictTreeReader>>,
    object_reverse: Option<Arc<DictTreeReader>>,
    watermarks: HashMap<u32, u32>,
    term_count: u64,
}

impl TermDictReader {
    /// Open the dictionary named by `refs`, reusing `prev`'s open handles
    /// for packs and leaves whose CIDs are unchanged.
    pub async fn from_refs_reusing(
        cs: Arc<dyn ContentStore>,
        cache_dir: &Path,
        refs: &TermDictRefs,
        leaflet_cache: Option<&Arc<LeafletCache>>,
        prev: Option<&TermDictReader>,
    ) -> io::Result<Self> {
        let mut forward = BTreeMap::new();
        for (p_id, packs) in &refs.forward_packs {
            let reader = ForwardPackReader::from_pack_refs_reusing(
                Arc::clone(&cs),
                cache_dir,
                packs,
                KIND_TERM_FWD,
                pack_ns_code(*p_id),
                prev.and_then(|p| p.forward.get(p_id)),
            )
            .await?;
            forward.insert(*p_id, reader);
        }
        let reverse = Some(
            DictTreeReader::from_refs_reusing(
                &cs,
                &refs.reverse,
                leaflet_cache,
                Some(cache_dir),
                prev.and_then(|p| p.reverse.as_ref()),
            )
            .await?,
        );
        let object_reverse = match &refs.object_reverse {
            Some(tree) => Some(
                DictTreeReader::from_refs_reusing(
                    &cs,
                    tree,
                    leaflet_cache,
                    Some(cache_dir),
                    prev.and_then(|p| p.object_reverse.as_ref()),
                )
                .await?,
            ),
            None => None,
        };
        Ok(Self {
            forward,
            reverse,
            object_reverse,
            watermarks: refs.watermarks.iter().copied().collect(),
            term_count: refs.term_count,
        })
    }

    /// Pre-warm forward packs into the OS page cache, predicate by predicate,
    /// up to `budget_bytes`; returns the bytes touched. Blocking, like
    /// [`ForwardPackReader::prewarm`].
    pub fn prewarm(&self, budget_bytes: u64) -> u64 {
        let mut warmed = 0;
        for reader in self.forward.values() {
            if warmed >= budget_bytes {
                break;
            }
            warmed += reader.prewarm(budget_bytes - warmed);
        }
        warmed
    }

    /// Distinct terms in the dictionary.
    pub fn term_count(&self) -> u64 {
        self.term_count
    }

    /// Highest sequence allocated under `p_id`, if any term exists there.
    pub fn watermark(&self, p_id: u32) -> Option<u32> {
        self.watermarks.get(&p_id).copied()
    }

    /// Inner predicates that have at least one term.
    pub fn predicates(&self) -> impl Iterator<Item = u32> + '_ {
        self.forward.keys().copied()
    }

    /// The handle for an encoded base edge, if it has been interned.
    pub fn find_handle(&self, key: &TermKey) -> io::Result<Option<u64>> {
        match &self.reverse {
            Some(tree) => tree.reverse_lookup(&key.to_be_bytes()),
            None => Ok(None),
        }
    }

    /// [`Self::find_handle`] for many keys, reading each reverse-tree leaf once.
    pub fn find_handles(&self, keys: &[TermKey]) -> io::Result<Vec<Option<u64>>> {
        match &self.reverse {
            Some(tree) => {
                let bytes: Vec<[u8; TermKey::LEN]> =
                    keys.iter().map(TermKey::to_be_bytes).collect();
                tree.reverse_lookup_many(bytes.iter().map(|b| &b[..]))
            }
            None => Ok(vec![None; keys.len()]),
        }
    }

    /// Every term whose base edge has subject `s_id` (and predicate `p_id`,
    /// when given), with its handle: one range over the subject-first reverse
    /// tree, which yields the keys themselves, so nothing is decoded.
    pub fn terms_with_subject(
        &self,
        s_id: u64,
        p_id: Option<u32>,
    ) -> io::Result<Vec<(TermKey, u64)>> {
        let Some(tree) = &self.reverse else {
            return Ok(Vec::new());
        };
        let mut start = [0u8; TermKey::LEN];
        start[..8].copy_from_slice(&s_id.to_be_bytes());
        let mut end = [0u8; TermKey::LEN];
        // Exclusive end: the next subject, or the next predicate of this one.
        match p_id {
            Some(p) => {
                start[8..12].copy_from_slice(&p.to_be_bytes());
                match p.checked_add(1) {
                    Some(next) => {
                        end[..8].copy_from_slice(&s_id.to_be_bytes());
                        end[8..12].copy_from_slice(&next.to_be_bytes());
                    }
                    None => end = next_subject(s_id),
                }
            }
            None => end = next_subject(s_id),
        }
        tree.reverse_range_scan(&start, &end)?
            .into_iter()
            .map(|(bytes, handle)| {
                TermKey::from_be_bytes(&bytes)
                    .map(|key| (key, handle))
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "term reverse key has the wrong width",
                        )
                    })
            })
            .collect()
    }

    /// Every term whose base edge has object `(o_type, o_key)` (and predicate
    /// `p_id`, when given), with its handle: one range over the object-first
    /// tree. `None` when the dictionary predates that tree.
    pub fn terms_with_object(
        &self,
        o_type: u16,
        o_key: u64,
        p_id: Option<u32>,
    ) -> io::Result<Option<Vec<(TermKey, u64)>>> {
        let Some(tree) = &self.object_reverse else {
            return Ok(None);
        };
        let mut start = [0u8; TermKey::LEN];
        start[..2].copy_from_slice(&o_type.to_be_bytes());
        start[2..10].copy_from_slice(&o_key.to_be_bytes());
        if let Some(p) = p_id {
            start[10..14].copy_from_slice(&p.to_be_bytes());
        }
        // Exclusive end: the key prefix plus one, at its last byte.
        let prefix_len = if p_id.is_some() { 14 } else { 10 };
        let mut end = [0u8; TermKey::LEN];
        end[..prefix_len].copy_from_slice(&start[..prefix_len]);
        let Some(last) = end[..prefix_len].iter().rposition(|b| *b != 0xFF) else {
            return Ok(Some(Vec::new()));
        };
        end[last] += 1;
        end[last + 1..].fill(0);
        tree.reverse_range_scan(&start, &end)?
            .into_iter()
            .map(|(bytes, handle)| {
                TermKey::from_object_first_bytes(&bytes)
                    .map(|key| (key, handle))
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "term object key has the wrong width",
                        )
                    })
            })
            .collect::<io::Result<Vec<_>>>()
            .map(Some)
    }

    /// Every term of inner predicate `p_id`, with its handle, read through the
    /// forward packs in sequence order.
    pub fn terms_of_predicate(&self, p_id: u32) -> io::Result<Vec<(TermKey, u64)>> {
        let Some(watermark) = self.watermark(p_id) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::with_capacity(watermark as usize + 1);
        for seq in 0..=watermark {
            let handle = term_handle(p_id, seq);
            if let Some(key) = self.resolve(handle)? {
                out.push((key, handle));
            }
        }
        Ok(out)
    }

    /// The encoded base edge behind `handle`, or `None` for an unknown handle.
    pub fn resolve(&self, handle: u64) -> io::Result<Option<TermKey>> {
        let Some(reader) = self.forward.get(&term_handle_p_id(handle)) else {
            return Ok(None);
        };
        let mut buf = Vec::with_capacity(TermKey::LEN);
        if !reader.forward_lookup_into(term_handle_seq(handle) as u64, &mut buf)? {
            return Ok(None);
        }
        TermKey::from_be_bytes(&buf)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "term handle {handle:#x}: malformed forward entry ({} bytes)",
                        buf.len()
                    ),
                )
            })
            .map(Some)
    }
}

// ── Builder ─────────────────────────────────────────────────────────────────

/// In-memory interner used while a build assigns handles.
///
/// Sequences continue above any watermarks the builder was created with, so
/// an incremental build allocates fresh handles without touching existing
/// ones.
#[derive(Default)]
pub struct TermDictBuilder {
    map: HashMap<TermKey, u64>,
    next_seq: HashMap<u32, u32>,
}

impl TermDictBuilder {
    /// A builder whose first allocation per predicate starts at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// A builder that continues each predicate's sequence above `watermarks`.
    pub fn above_watermarks(watermarks: &[(u32, u32)]) -> Self {
        Self {
            map: HashMap::new(),
            next_seq: watermarks
                .iter()
                .map(|(p_id, wm)| (*p_id, wm.saturating_add(1)))
                .collect(),
        }
    }

    /// The handle already assigned to `key` in this builder.
    pub fn get(&self, key: &TermKey) -> Option<u64> {
        self.map.get(key).copied()
    }

    /// The handle for `key`, allocating one under its inner predicate if new.
    pub fn get_or_insert(&mut self, key: TermKey) -> io::Result<u64> {
        if let Some(h) = self.map.get(&key) {
            return Ok(*h);
        }
        let next = self.next_seq.entry(key.p_id).or_insert(0);
        // The sequences above are novelty's provisional handles.
        if *next >= fluree_db_core::triple_term::NOVELTY_TERM_SEQ_BASE {
            return Err(io::Error::other(format!(
                "triple-term dictionary: predicate {} exhausted its {} indexed sequences",
                key.p_id,
                fluree_db_core::triple_term::NOVELTY_TERM_SEQ_BASE
            )));
        }
        let handle = term_handle(key.p_id, *next);
        *next += 1;
        self.map.insert(key, handle);
        Ok(handle)
    }

    /// Distinct terms interned so far.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True when nothing has been interned.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Every interned term as `(handle, key)`, ascending by handle, which is
    /// `(p_id, seq)` order: what an incremental pack append consumes.
    pub fn entries_sorted(&self) -> Vec<(u64, TermKey)> {
        let mut v: Vec<(u64, TermKey)> = self.map.iter().map(|(k, h)| (*h, *k)).collect();
        v.sort_unstable_by_key(|(h, _)| *h);
        v
    }

    /// `(p_id, highest seq allocated)` per predicate with at least one term.
    pub fn watermarks(&self) -> Vec<(u32, u32)> {
        let mut wms: Vec<(u32, u32)> = self
            .next_seq
            .iter()
            .filter(|(_, next)| **next > 0)
            .map(|(p_id, next)| (*p_id, next - 1))
            .collect();
        wms.sort_unstable();
        wms
    }

    /// Persist everything interned so far as a complete dictionary and
    /// return its root references.
    ///
    /// Forward packs are written per inner predicate in sequence order, split
    /// at the default pack size; the reverse tree is built from every term in
    /// key order. Both are content-addressed through `cs`.
    pub async fn upload(self, cs: &dyn ContentStore) -> io::Result<TermDictRefs> {
        let term_count = self.map.len() as u64;
        let watermarks = self.watermarks();

        // Group by predicate, ordered by seq, so each stream is contiguous from 0.
        let mut by_pred: BTreeMap<u32, Vec<(u32, [u8; TermKey::LEN])>> = BTreeMap::new();
        let mut reverse_entries: Vec<ReverseEntry> = Vec::with_capacity(self.map.len());
        let mut object_entries: Vec<ReverseEntry> = Vec::with_capacity(self.map.len());
        for (key, handle) in &self.map {
            let bytes = key.to_be_bytes();
            by_pred
                .entry(term_handle_p_id(*handle))
                .or_default()
                .push((term_handle_seq(*handle), bytes));
            reverse_entries.push(ReverseEntry {
                key: bytes.to_vec(),
                id: *handle,
            });
            object_entries.push(ReverseEntry {
                key: key.to_object_first_bytes().to_vec(),
                id: *handle,
            });
        }

        let mut forward_packs = Vec::with_capacity(by_pred.len());
        for (p_id, mut entries) in by_pred {
            entries.sort_unstable_by_key(|(seq, _)| *seq);
            let kind = ContentKind::DictBlob {
                dict: DictKind::TermForward { p_id },
            };
            let per_pack = (DEFAULT_TARGET_PACK_BYTES / (TermKey::LEN + 4)).max(1);
            let mut packs = Vec::new();
            for chunk in entries.chunks(per_pack) {
                let refs: Vec<(u64, &[u8])> = chunk
                    .iter()
                    .map(|(seq, bytes)| (*seq as u64, bytes.as_slice()))
                    .collect();
                let bytes = encode_forward_pack(
                    &refs,
                    KIND_TERM_FWD,
                    pack_ns_code(p_id),
                    DEFAULT_TARGET_PAGE_BYTES,
                )?;
                let pack_cid = put(cs, kind, &bytes).await?;
                packs.push(PackBranchEntry {
                    first_id: refs[0].0,
                    last_id: refs[refs.len() - 1].0,
                    pack_cid,
                });
            }
            forward_packs.push((p_id, packs));
        }

        reverse_entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        let reverse = upload_reverse_tree(cs, reverse_entries).await?;
        object_entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        let object_reverse = upload_reverse_tree(cs, object_entries).await?;

        Ok(TermDictRefs {
            forward_packs,
            reverse,
            watermarks,
            term_count,
            object_reverse: Some(object_reverse),
        })
    }
}

async fn put(cs: &dyn ContentStore, kind: ContentKind, bytes: &[u8]) -> io::Result<ContentId> {
    cs.put(kind, bytes).await.map_err(io::Error::other)
}

/// Build, upload and finalize a term reverse tree from key-sorted entries.
pub async fn upload_reverse_tree(
    cs: &dyn ContentStore,
    entries: Vec<ReverseEntry>,
) -> io::Result<DictTreeRefs> {
    let kind = ContentKind::DictBlob {
        dict: DictKind::TermReverse,
    };
    let built = build_reverse_tree(entries, DEFAULT_TARGET_LEAF_BYTES)?;
    let mut hash_to_address: HashMap<String, String> = HashMap::new();
    let mut address_to_cid: HashMap<String, ContentId> = HashMap::new();
    for leaf in &built.leaves {
        let cid = put(cs, kind, &leaf.bytes).await?;
        let addr = cid.to_string();
        address_to_cid.insert(addr.clone(), cid);
        hash_to_address.insert(leaf.hash.clone(), addr);
    }
    let (branch, branch_bytes, _) = finalize_branch(built.branch, &hash_to_address)?;
    let branch_cid = put(cs, kind, &branch_bytes).await?;
    let mut leaves = Vec::with_capacity(branch.leaves.len());
    for entry in &branch.leaves {
        let cid = address_to_cid.get(&entry.address).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "term reverse tree: no CID for leaf address {}",
                    entry.address
                ),
            )
        })?;
        leaves.push(cid.clone());
    }
    Ok(DictTreeRefs {
        branch: branch_cid,
        leaves,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::o_type::OType;

    fn key(s: u64, p: u32, o: u64) -> TermKey {
        TermKey {
            s_id: s,
            p_id: p,
            o_type: OType::IRI_REF,
            o_key: o,
        }
    }

    #[test]
    fn builder_partitions_by_predicate_and_dedups() {
        let mut b = TermDictBuilder::new();
        let h1 = b.get_or_insert(key(1, 5, 2)).unwrap();
        let h2 = b.get_or_insert(key(1, 5, 3)).unwrap();
        let h3 = b.get_or_insert(key(1, 9, 2)).unwrap();
        assert_eq!(b.get_or_insert(key(1, 5, 2)).unwrap(), h1);
        assert_eq!(term_handle_p_id(h1), 5);
        assert_eq!(term_handle_seq(h1), 0);
        assert_eq!(term_handle_seq(h2), 1);
        assert_eq!((term_handle_p_id(h3), term_handle_seq(h3)), (9, 0));
        assert_eq!(b.len(), 3);
        assert_eq!(b.watermarks(), vec![(5, 1), (9, 0)]);
    }

    /// The server's background warmer reaches term packs through this:
    /// every predicate's stream, in order, within the budget.
    #[test]
    fn prewarm_covers_every_predicate_stream_within_the_budget() {
        let stream = |p_id: u32, n: u64| {
            let keys: Vec<[u8; TermKey::LEN]> =
                (0..n).map(|i| key(i + 1, p_id, i).to_be_bytes()).collect();
            let entries: Vec<(u64, &[u8])> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| (i as u64, &k[..]))
                .collect();
            let packs = crate::dict::pack_builder::build_forward_packs_for_stream(
                KIND_TERM_FWD,
                pack_ns_code(p_id),
                &entries,
                DEFAULT_TARGET_PAGE_BYTES,
                DEFAULT_TARGET_PACK_BYTES,
            )
            .unwrap()
            .packs;
            let len: u64 = packs.iter().map(|p| p.bytes.len() as u64).sum();
            let reader = ForwardPackReader::from_memory(
                packs
                    .into_iter()
                    .map(|p| Arc::from(p.bytes.into_boxed_slice()))
                    .collect(),
            )
            .unwrap();
            (reader, len)
        };
        let (a, len_a) = stream(5, 40);
        let (b, len_b) = stream(9, 70);
        let reader = TermDictReader {
            forward: BTreeMap::from([(5, a), (9, b)]),
            reverse: None,
            object_reverse: None,
            watermarks: HashMap::new(),
            term_count: 110,
        };
        assert_eq!(reader.prewarm(u64::MAX), len_a + len_b);
        assert_eq!(reader.prewarm(len_a + len_b / 2), len_a + len_b / 2);
        assert_eq!(reader.prewarm(0), 0);
    }

    /// A subject prefix (and subject + predicate prefix) selects exactly its
    /// terms, across leaves, at the edges of the key space too.
    #[test]
    fn terms_with_subject_reads_the_prefix_range() {
        let keys = [
            key(4, 9, 1),
            key(5, 2, 7),
            key(5, 9, 1),
            key(5, 9, 2),
            key(5, u32::MAX, 3),
            key(6, 0, 0),
            key(u64::MAX, 1, 1),
        ];
        let reader = reader_over(&keys);
        let handles = |s: u64, p: Option<u32>| -> Vec<u64> {
            reader
                .terms_with_subject(s, p)
                .unwrap()
                .into_iter()
                .map(|(k, h)| {
                    assert_eq!(k, keys[(h - 100) as usize]);
                    h
                })
                .collect()
        };
        assert_eq!(handles(5, None), vec![101, 102, 103, 104]);
        assert_eq!(handles(5, Some(9)), vec![102, 103]);
        assert_eq!(handles(5, Some(u32::MAX)), vec![104]);
        assert_eq!(handles(5, Some(3)), Vec::<u64>::new());
        assert_eq!(handles(u64::MAX, None), vec![106]);
        assert_eq!(handles(7, None), Vec::<u64>::new());
    }

    /// Both reverse trees over `keys`, with handle `100 + i`, small leaves so
    /// ranges cross them.
    fn reader_over(keys: &[TermKey]) -> TermDictReader {
        use crate::dict::builder::build_reverse_tree;
        use crate::dict::reader::DictTreeReader;
        let tree = |encode: fn(&TermKey) -> [u8; TermKey::LEN]| {
            let mut entries: Vec<ReverseEntry> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| ReverseEntry {
                    key: encode(k).to_vec(),
                    id: i as u64 + 100,
                })
                .collect();
            entries.sort_by(|a, b| a.key.cmp(&b.key));
            let built = build_reverse_tree(entries, 64).unwrap();
            assert!(built.branch.leaves.len() > 1, "the range must cross leaves");
            let leaves = built
                .leaves
                .iter()
                .zip(&built.branch.leaves)
                .map(|(leaf, entry)| (entry.address.clone(), leaf.bytes.clone()))
                .collect();
            Some(Arc::new(DictTreeReader::from_memory(built.branch, leaves)))
        };
        TermDictReader {
            forward: BTreeMap::new(),
            reverse: tree(TermKey::to_be_bytes),
            object_reverse: tree(TermKey::to_object_first_bytes),
            watermarks: HashMap::new(),
            term_count: keys.len() as u64,
        }
    }

    /// An object prefix (and object + predicate prefix) selects exactly its
    /// terms, at the top of the key space too.
    #[test]
    fn terms_with_object_reads_the_prefix_range() {
        let keys = [
            key(4, 9, 1),
            key(5, 2, 7),
            key(5, 9, 1),
            key(6, 9, 1),
            key(3, 1, 1),
            key(5, u32::MAX, u64::MAX),
            key(9, u32::MAX, u64::MAX),
            key(1, 0, 2),
        ];
        let reader = reader_over(&keys);
        let handles = |o: u64, p: Option<u32>| -> Vec<u64> {
            let mut found: Vec<u64> = reader
                .terms_with_object(OType::IRI_REF.as_u16(), o, p)
                .unwrap()
                .expect("object tree")
                .into_iter()
                .map(|(k, h)| {
                    assert_eq!(k, keys[(h - 100) as usize]);
                    h
                })
                .collect();
            found.sort_unstable();
            found
        };
        assert_eq!(handles(1, None), vec![100, 102, 103, 104]);
        assert_eq!(handles(1, Some(9)), vec![100, 102, 103]);
        assert_eq!(handles(7, None), vec![101]);
        assert_eq!(handles(u64::MAX, None), vec![105, 106]);
        assert_eq!(handles(u64::MAX, Some(u32::MAX)), vec![105, 106]);
        assert_eq!(handles(3, None), Vec::<u64>::new());
        let other_type = reader
            .terms_with_object(OType::XSD_STRING.as_u16(), 1, None)
            .unwrap();
        assert_eq!(other_type, Some(Vec::new()));
        let no_tree = TermDictReader {
            object_reverse: None,
            ..reader_over(&keys)
        };
        assert!(no_tree
            .terms_with_object(OType::IRI_REF.as_u16(), 1, None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn builder_continues_above_watermarks() {
        let mut b = TermDictBuilder::above_watermarks(&[(5, 41)]);
        let h = b.get_or_insert(key(1, 5, 2)).unwrap();
        assert_eq!(term_handle_seq(h), 42);
        let h0 = b.get_or_insert(key(1, 6, 2)).unwrap();
        assert_eq!(term_handle_seq(h0), 0);
    }
}
