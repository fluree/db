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

// ── Reader ──────────────────────────────────────────────────────────────────

/// Read side of the triple-term dictionary.
pub struct TermDictReader {
    forward: BTreeMap<u32, ForwardPackReader>,
    reverse: Option<Arc<DictTreeReader>>,
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
        Ok(Self {
            forward,
            reverse,
            watermarks: refs.watermarks.iter().copied().collect(),
            term_count: refs.term_count,
        })
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
        if *next == u32::MAX {
            return Err(io::Error::other(format!(
                "triple-term dictionary: predicate {} exhausted its {}-bit sequence space",
                key.p_id,
                fluree_db_core::triple_term::TERM_SEQ_BITS
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

        Ok(TermDictRefs {
            forward_packs,
            reverse,
            watermarks,
            term_count,
        })
    }
}

async fn put(cs: &dyn ContentStore, kind: ContentKind, bytes: &[u8]) -> io::Result<ContentId> {
    cs.put(kind, bytes).await.map_err(io::Error::other)
}

/// Build, upload and finalize a reverse tree from key-sorted entries.
async fn upload_reverse_tree(
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

    #[test]
    fn builder_continues_above_watermarks() {
        let mut b = TermDictBuilder::above_watermarks(&[(5, 41)]);
        let h = b.get_or_insert(key(1, 5, 2)).unwrap();
        assert_eq!(term_handle_seq(h), 42);
        let h0 = b.get_or_insert(key(1, 6, 2)).unwrap();
        assert_eq!(term_handle_seq(h0), 0);
    }
}
