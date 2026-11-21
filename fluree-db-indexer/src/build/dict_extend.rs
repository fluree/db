//! Extend a published dictionary with a bulk import's new terms, streaming.
//!
//! [`apply_dict_novelty`](super::incremental::apply_dict_novelty) holds every
//! new entry and every reverse leaf it touches in memory, and splits an
//! overgrown leaf in two at most: right for one index cycle's novelty, not
//! for an import that brings tens of millions of terms. Here the terms stream
//! from the files a seeded vocabulary merge wrote, each reverse tree is
//! rewritten a leaf at a time into leaves of the target size, and forward
//! packs are cut as they fill. Memory stays at a few leaves and one pack
//! whatever the import's size.

use fluree_db_binary_index::dict::branch::{BranchLeafEntry, DictBranch};
use fluree_db_binary_index::dict::key_scheme::{GroupCounter, KeyOrder, KeyScheme, RawGroups};
use fluree_db_binary_index::dict::reverse_leaf::{LeafFormat, ReverseEntry, ReverseLeaf};
use fluree_db_binary_index::{DictRefs, DictTreeRefs, PackBranchEntry};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{ContentId, ContentKind, ContentStore, DictKind};
use futures::StreamExt;

use crate::error::{IndexerError, Result};
use crate::run_index::NewTerms;

use super::incremental::{
    compact_forward_streams, subject_ns_packs_mut, CompactionBudget, DictNoveltyUpdate,
    ForwardPackAppends,
};
use super::types::UpdatedReverseTree;

/// Reverse leaves fetched ahead of the merge.
const LEAF_PREFETCH: usize = 8;

/// Leaf and pack sizes to cut at.
#[derive(Clone, Copy)]
struct Targets {
    leaf_bytes: usize,
    pack_bytes: usize,
}

/// Add the new terms of a seeded vocabulary merge to the dictionaries
/// `base_dict_refs` names: merge them into both reverse trees and append
/// (then compact) forward packs.
///
/// `new_subjects` holds sid64 → namespace-stripped suffix and
/// `new_strings` string id → value, each sorted by id with every id above
/// the base watermark, and so sorted by reverse key too. `base_index_t`
/// rotates which untouched streams the leftover compaction budget sweeps.
pub async fn extend_dicts(
    content_store: &dyn ContentStore,
    base_dict_refs: &DictRefs,
    base_index_t: i64,
    new_subjects: &NewTerms,
    new_strings: &NewTerms,
) -> Result<DictNoveltyUpdate> {
    let targets = Targets {
        leaf_bytes: fluree_db_binary_index::dict::builder::DEFAULT_TARGET_LEAF_BYTES,
        pack_bytes: fluree_db_binary_index::dict::pack_builder::pack_target_bytes(
            new_subjects.value_bytes.max(new_strings.value_bytes),
        ),
    };
    extend_dicts_with(
        content_store,
        base_dict_refs,
        base_index_t,
        new_subjects,
        new_strings,
        targets,
    )
    .await
}

async fn extend_dicts_with(
    content_store: &dyn ContentStore,
    base_dict_refs: &DictRefs,
    base_index_t: i64,
    new_subjects: &NewTerms,
    new_strings: &NewTerms,
    targets: Targets,
) -> Result<DictNoveltyUpdate> {
    let mut dict_refs = base_dict_refs.clone();
    let mut replaced_cids = Vec::new();

    if !new_subjects.is_empty() {
        let updated = extend_reverse_tree(
            content_store,
            DictKind::SubjectReverse,
            &base_dict_refs.subject_reverse,
            new_subjects,
            subject_key,
            targets.leaf_bytes,
        )
        .await?;
        replaced_cids.extend(updated.replaced_cids);
        dict_refs.subject_reverse = updated.tree_refs;
    }
    if !new_strings.is_empty() {
        let updated = extend_reverse_tree(
            content_store,
            DictKind::StringReverse,
            &base_dict_refs.string_reverse,
            new_strings,
            string_key,
            targets.leaf_bytes,
        )
        .await?;
        replaced_cids.extend(updated.replaced_cids);
        dict_refs.string_reverse = updated.tree_refs;
    }

    let mut appends = ForwardPackAppends::default();
    if !new_strings.is_empty() {
        append_forward_packs(
            content_store,
            &mut dict_refs,
            &mut appends,
            new_strings,
            ForwardStream::Strings,
            targets.pack_bytes,
        )
        .await?;
    }
    if !new_subjects.is_empty() {
        append_forward_packs(
            content_store,
            &mut dict_refs,
            &mut appends,
            new_subjects,
            ForwardStream::Subjects,
            targets.pack_bytes,
        )
        .await?;
    }
    replaced_cids.extend(
        compact_forward_streams(
            content_store,
            &mut dict_refs,
            &base_dict_refs.forward_packs,
            base_index_t,
            appends,
            &mut CompactionBudget::for_cycle(),
        )
        .await?,
    );

    Ok(DictNoveltyUpdate {
        dict_refs,
        replaced_cids,
    })
}

/// A subject's reverse key: `[ns_code BE][suffix]`.
fn subject_key(sid64: u64, suffix: &[u8], key: &mut Vec<u8>) {
    key.clear();
    key.extend_from_slice(&SubjectId::from_u64(sid64).ns_code().to_be_bytes());
    key.extend_from_slice(suffix);
}

/// A string's reverse value: the string, which the tree's scheme keys.
fn string_key(_id: u64, value: &[u8], key: &mut Vec<u8>) {
    key.clear();
    key.extend_from_slice(value);
}

type KeyOf = fn(u64, &[u8], &mut Vec<u8>);

fn invalid(message: String) -> IndexerError {
    IndexerError::StorageWrite(message)
}

fn read_error(e: std::io::Error) -> IndexerError {
    IndexerError::StorageRead(format!("new terms: {e}"))
}

// ============================================================================
// Reverse trees
// ============================================================================

/// The new terms as reverse entries, in key order, one ahead.
///
/// Terms arrive in value order; the tree's scheme may key a run of values
/// sharing a prefix in another order, which [`KeyOrder`] restores. Groups
/// in `raw` are keyed by value; every other group must bring at most the
/// cap, which [`groups_filled_by`] sees to.
struct EntryCursor {
    reader: crate::run_index::NewTermReader,
    key_of: KeyOf,
    order: KeyOrder,
    value: Vec<u8>,
    drained: bool,
    next: Option<ReverseEntry>,
}

impl EntryCursor {
    fn open(terms: &NewTerms, key_of: KeyOf, scheme: KeyScheme, raw: &RawGroups) -> Result<Self> {
        let mut cursor = Self {
            reader: terms.reader().map_err(read_error)?,
            key_of,
            order: KeyOrder::new(scheme, raw.clone()),
            value: Vec::new(),
            drained: false,
            next: None,
        };
        cursor.next = cursor.read()?;
        Ok(cursor)
    }

    fn read(&mut self) -> Result<Option<ReverseEntry>> {
        loop {
            if let Some((key, id)) = self.order.pop() {
                return Ok(Some(ReverseEntry { key, id }));
            }
            if self.drained {
                return Ok(None);
            }
            match self.reader.next_term().map_err(read_error)? {
                Some((id, value)) => {
                    (self.key_of)(id, value, &mut self.value);
                    self.order
                        .push(self.value.clone(), id)
                        .map_err(read_error)?;
                }
                None => {
                    self.order.finish();
                    self.drained = true;
                }
            }
        }
    }

    fn peek(&self) -> Option<&ReverseEntry> {
        self.next.as_ref()
    }

    fn take(&mut self) -> Result<ReverseEntry> {
        let entry = self.next.take().expect("take follows a peek");
        self.next = self.read()?;
        if let Some(next) = &self.next {
            if next.key <= entry.key {
                return Err(invalid(
                    "new dictionary terms are not in reverse-key order".to_string(),
                ));
            }
        }
        Ok(entry)
    }
}

/// The existing leaves that own at least one new key, in order.
///
/// Leaf `i` owns `[first_key[i], first_key[i + 1])`; the first leaf also
/// owns everything below it and the last everything above.
fn leaves_receiving(
    branch: &DictBranch,
    terms: &NewTerms,
    key_of: KeyOf,
    scheme: KeyScheme,
    raw: &RawGroups,
) -> Result<Vec<usize>> {
    let mut touched = Vec::new();
    if branch.leaves.is_empty() {
        return Ok(touched);
    }
    let mut cursor = EntryCursor::open(terms, key_of, scheme, raw)?;
    let mut leaf = 0usize;
    while let Some(entry) = cursor.peek() {
        while leaf + 1 < branch.leaves.len()
            && entry.key.as_slice() >= branch.leaves[leaf + 1].first_key.as_slice()
        {
            leaf += 1;
        }
        if touched.last() != Some(&leaf) {
            touched.push(leaf);
        }
        cursor.take()?;
    }
    Ok(touched)
}

/// The groups the new terms alone take past the scheme's cap.
fn groups_filled_by(terms: &NewTerms, key_of: KeyOf, scheme: KeyScheme) -> Result<Vec<Vec<u8>>> {
    let mut counter = GroupCounter::new(scheme);
    let mut reader = terms.reader().map_err(read_error)?;
    let mut value = Vec::new();
    while let Some((id, term)) = reader.next_term().map_err(read_error)? {
        key_of(id, term, &mut value);
        counter.push(&value);
    }
    Ok(counter.finish())
}

/// Counts each unlisted group's hashed keys as the merge writes them, to
/// list the groups the merge took past the cap. A group's hashed keys sit
/// together, but the merge reads only the leaves receiving new keys: a
/// group that runs into a leaf it skips is counted as full if it received
/// any. Also notes the listed groups found holding hashed keys.
struct GroupWatch<'a> {
    scheme: KeyScheme,
    raw: &'a RawGroups,
    /// The group being counted, if any.
    group: Option<Vec<u8>>,
    count: usize,
    added: bool,
    unseen: bool,
    filled: Vec<Vec<u8>>,
    listed_hashed: Vec<Vec<u8>>,
}

impl<'a> GroupWatch<'a> {
    fn new(scheme: KeyScheme, raw: &'a RawGroups) -> Self {
        Self {
            scheme,
            raw,
            group: None,
            count: 0,
            added: false,
            unseen: false,
            filled: Vec::new(),
            listed_hashed: Vec::new(),
        }
    }

    /// The merge wrote `key`, a new one or one the tree held.
    fn see(&mut self, key: &[u8], new: bool) {
        let Some(group) = self.scheme.hashed_key_group(key) else {
            self.close();
            return;
        };
        if self.raw.get(group).is_some() {
            self.close();
            if !new && self.listed_hashed.last().map(Vec::as_slice) != Some(group) {
                self.listed_hashed.push(group.to_vec());
            }
            return;
        }
        if self.group.as_deref() != Some(group) {
            self.close();
            self.group = Some(group.to_vec());
        }
        self.count += 1;
        self.added |= new;
    }

    /// The merge skips the leaves from one holding `first` to one holding
    /// `last`.
    fn skip(&mut self, first: &[u8], last: &[u8]) {
        if self.group.is_some() && self.scheme.hashed_key_group(first) == self.group.as_deref() {
            self.unseen = true;
        }
        self.close();
        if let Some(group) = self.scheme.hashed_key_group(last) {
            if self.raw.get(group).is_none() {
                self.group = Some(group.to_vec());
                self.unseen = true;
            }
        }
    }

    fn close(&mut self) {
        if let Some(group) = self.group.take() {
            if self.added && (self.unseen || self.count > self.scheme.cap()) {
                self.filled.push(group);
            }
        }
        self.count = 0;
        self.added = false;
        self.unseen = false;
    }
}

/// Cuts a sorted entry stream into leaves near `target` bytes.
///
/// Each range (one existing leaf's share of the merge) ends on a leaf of
/// its own. A range's last full leaf is held back until the range ends, so
/// that a short tail joins it rather than standing as a sliver.
struct LeafCutter {
    target: usize,
    held: Vec<ReverseEntry>,
    current: Vec<ReverseEntry>,
    current_bytes: usize,
    ready: Vec<Vec<ReverseEntry>>,
}

impl LeafCutter {
    fn new(target: usize) -> Self {
        Self {
            target,
            held: Vec::new(),
            current: Vec::new(),
            current_bytes: 0,
            ready: Vec::new(),
        }
    }

    fn push(&mut self, entry: ReverseEntry) {
        // Offset slot + key length + id, as a `DLR1` leaf lays it out: the
        // measure leaves are cut by in either format.
        self.current_bytes += 16 + entry.key.len();
        self.current.push(entry);
        if self.current_bytes >= self.target {
            if !self.held.is_empty() {
                self.ready.push(std::mem::take(&mut self.held));
            }
            self.held = std::mem::take(&mut self.current);
            self.current_bytes = 0;
        }
    }

    fn end_range(&mut self) {
        let tail = std::mem::take(&mut self.current);
        let short = self.current_bytes < self.target / 2;
        self.current_bytes = 0;
        if self.held.is_empty() {
            if !tail.is_empty() {
                self.ready.push(tail);
            }
        } else if short {
            let mut joined = std::mem::take(&mut self.held);
            joined.extend(tail);
            self.ready.push(joined);
        } else {
            self.ready.push(std::mem::take(&mut self.held));
            if !tail.is_empty() {
                self.ready.push(tail);
            }
        }
    }
}

/// The leaves of the tree being written, in key order.
struct TreeWriter<'a> {
    content_store: &'a dyn ContentStore,
    kind: ContentKind,
    format: LeafFormat,
    leaves: Vec<BranchLeafEntry>,
    leaf_cids: Vec<ContentId>,
}

impl TreeWriter<'_> {
    async fn upload(&mut self, cutter: &mut LeafCutter) -> Result<()> {
        for entries in cutter.ready.drain(..) {
            let bytes = self.format.encode(&entries);
            let cid = self
                .content_store
                .put(self.kind, &bytes)
                .await
                .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            self.leaves.push(BranchLeafEntry {
                first_key: entries
                    .first()
                    .expect("a cut leaf is non-empty")
                    .key
                    .clone(),
                last_key: entries.last().expect("a cut leaf is non-empty").key.clone(),
                entry_count: entries.len() as u32,
                address: cid.to_string(),
            });
            self.leaf_cids.push(cid);
        }
        Ok(())
    }

    fn keep(&mut self, leaf: &BranchLeafEntry, cid: &ContentId) {
        self.leaves.push(leaf.clone());
        self.leaf_cids.push(cid.clone());
    }
}

/// Merge `terms` into the reverse tree `existing`, writing only the leaves
/// that receive new keys. None of `terms` may be in the tree already.
async fn extend_reverse_tree(
    content_store: &dyn ContentStore,
    dict: DictKind,
    existing: &DictTreeRefs,
    terms: &NewTerms,
    key_of: KeyOf,
    target_leaf_bytes: usize,
) -> Result<UpdatedReverseTree> {
    let kind = ContentKind::DictBlob { dict };
    let branch_bytes = content_store
        .get(&existing.branch)
        .await
        .map_err(|e| IndexerError::StorageRead(format!("load reverse branch: {e}")))?;
    let branch = DictBranch::decode(&branch_bytes)
        .map_err(|e| IndexerError::StorageRead(format!("decode reverse branch: {e}")))?;
    // An empty tree takes a new tree's scheme; any other keeps its own.
    let scheme = if branch.leaves.is_empty() {
        KeyScheme::for_new_tree(dict)
    } else {
        branch.key_scheme
    };
    if branch.leaves.len() != existing.leaves.len() {
        return Err(IndexerError::StorageRead(format!(
            "reverse branch names {} leaves, its refs {}",
            branch.leaves.len(),
            existing.leaves.len()
        )));
    }

    // Groups the import alone takes past the cap are keyed by value. One
    // whose hashed range lies only in leaves the merge reads is marked as
    // holding hashed keys if the merge finds some; any other, as maybe.
    let mut raw = branch.raw_groups.clone();
    let filled_by_terms: Vec<Vec<u8>> = if scheme.reorders() {
        groups_filled_by(terms, key_of, scheme)?
            .into_iter()
            .filter(|group| raw.get(group).is_none())
            .collect()
    } else {
        Vec::new()
    };
    for group in &filled_by_terms {
        raw.insert(group, false);
    }
    let touched = leaves_receiving(&branch, terms, key_of, scheme, &raw)?;
    for group in &filled_by_terms {
        let (start, end) = scheme.hashed_range(group);
        let unread = branch
            .leaves_overlapping(&start, end.as_deref())
            .any(|i| touched.binary_search(&i).is_err());
        if unread {
            raw.insert(group, true);
        }
    }
    // Owned ids: a stream of futures borrowing through a closure argument
    // is not `Send` for every lifetime, which a spawned import needs.
    let to_fetch: Vec<ContentId> = touched
        .iter()
        .map(|&i| existing.leaves[i].clone())
        .collect();
    let mut fetched = futures::stream::iter(to_fetch)
        .map(|cid| async move { content_store.get(&cid).await })
        .buffered(LEAF_PREFETCH);

    let mut cursor = EntryCursor::open(terms, key_of, scheme, &raw)?;
    let mut cutter = LeafCutter::new(target_leaf_bytes);
    let mut watch = GroupWatch::new(scheme, &raw);
    let mut writer = TreeWriter {
        content_store,
        kind,
        format: LeafFormat::for_tree(dict, scheme),
        leaves: Vec::with_capacity(branch.leaves.len()),
        leaf_cids: Vec::with_capacity(branch.leaves.len()),
    };
    let mut replaced_cids = vec![existing.branch.clone()];

    if branch.leaves.is_empty() {
        while cursor.peek().is_some() {
            let entry = cursor.take()?;
            watch.see(&entry.key, true);
            cutter.push(entry);
            if !cutter.ready.is_empty() {
                writer.upload(&mut cutter).await?;
            }
        }
        cutter.end_range();
        writer.upload(&mut cutter).await?;
    }

    let mut touched = touched.iter().copied().peekable();
    // The first leaf of the run of skipped leaves the merge is in, if any.
    let mut skipped_from: Option<usize> = None;
    for (i, leaf_entry) in branch.leaves.iter().enumerate() {
        if touched.next_if_eq(&i).is_none() {
            writer.keep(leaf_entry, &existing.leaves[i]);
            skipped_from.get_or_insert(i);
            continue;
        }
        if let Some(from) = skipped_from.take() {
            watch.skip(
                &branch.leaves[from].first_key,
                &branch.leaves[i - 1].last_key,
            );
        }
        let leaf_bytes = fetched
            .next()
            .await
            .expect("one fetch per touched leaf")
            .map_err(|e| IndexerError::StorageRead(format!("fetch reverse leaf: {e}")))?;
        let leaf = ReverseLeaf::from_bytes(&leaf_bytes)
            .map_err(|e| IndexerError::StorageRead(format!("decode reverse leaf: {e}")))?;
        let upper = branch
            .leaves
            .get(i + 1)
            .map(|next| next.first_key.as_slice());

        let mut old = leaf.iter().peekable();
        loop {
            let new = cursor
                .peek()
                .filter(|e| upper.is_none_or(|upper| e.key.as_slice() < upper));
            let take_new = match (old.peek(), new) {
                (None, None) => break,
                (Some(_), None) => false,
                (None, Some(_)) => true,
                (Some((old_key, _)), Some(new)) => match new.key.as_slice().cmp(old_key.as_ref()) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Greater => false,
                    std::cmp::Ordering::Equal => {
                        return Err(invalid(format!(
                            "a new {dict:?} term is already in the dictionary"
                        )));
                    }
                },
            };
            let entry = if take_new {
                cursor.take()?
            } else {
                let (key, id) = old.next().expect("peeked");
                ReverseEntry {
                    key: key.into_owned(),
                    id,
                }
            };
            watch.see(&entry.key, take_new);
            cutter.push(entry);
            if !cutter.ready.is_empty() {
                writer.upload(&mut cutter).await?;
            }
        }
        cutter.end_range();
        writer.upload(&mut cutter).await?;
        replaced_cids.push(existing.leaves[i].clone());
    }
    if cursor.peek().is_some() {
        return Err(invalid(format!(
            "new {dict:?} terms left over after the merge"
        )));
    }
    if let Some(from) = skipped_from {
        let last = &branch.leaves[branch.leaves.len() - 1].last_key;
        watch.skip(&branch.leaves[from].first_key, last);
    }
    watch.close();
    let GroupWatch {
        filled,
        listed_hashed,
        ..
    } = watch;
    let mut raw_groups = raw.clone();
    for group in filled.iter().chain(&listed_hashed) {
        raw_groups.insert(group, true);
    }

    let new_branch = DictBranch {
        leaves: writer.leaves,
        key_scheme: scheme,
        raw_groups,
    };
    let branch_cid = content_store
        .put(kind, &new_branch.encode())
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

    tracing::debug!(
        ?dict,
        new_terms = terms.count,
        base_leaves = branch.leaves.len(),
        rewritten = replaced_cids.len() - 1,
        leaves = new_branch.leaves.len(),
        "reverse dictionary extended"
    );

    Ok(UpdatedReverseTree {
        tree_refs: DictTreeRefs {
            branch: branch_cid,
            leaves: writer.leaf_cids,
        },
        replaced_cids,
    })
}

// ============================================================================
// Forward packs
// ============================================================================

#[derive(Clone, Copy)]
enum ForwardStream {
    /// One stream, ids are string ids.
    Strings,
    /// A stream per namespace, ids are sid64s.
    Subjects,
}

/// The entries of the pack being cut.
struct PackBuffer {
    ids: Vec<u64>,
    ends: Vec<usize>,
    data: Vec<u8>,
}

impl PackBuffer {
    fn push(&mut self, id: u64, value: &[u8]) {
        self.ids.push(id);
        self.data.extend_from_slice(value);
        self.ends.push(self.data.len());
    }

    /// Value bytes plus an offset slot per entry, as the pack builder sizes it.
    fn estimate(&self) -> usize {
        self.data.len() + 4 * self.ids.len()
    }
}

/// Append packs for `terms` to their forward streams, recording what was
/// uploaded and which streams grew in `appends`.
async fn append_forward_packs(
    content_store: &dyn ContentStore,
    dict_refs: &mut DictRefs,
    appends: &mut ForwardPackAppends,
    terms: &NewTerms,
    stream: ForwardStream,
    target_pack_bytes: usize,
) -> Result<()> {
    let mut reader = terms.reader().map_err(read_error)?;
    let mut buffer = PackBuffer {
        ids: Vec::new(),
        ends: Vec::new(),
        data: Vec::new(),
    };
    let mut current_ns: Option<u16> = None;

    while let Some((id, value)) = reader.next_term().map_err(read_error)? {
        let (ns_code, local_id) = match stream {
            ForwardStream::Strings => (0, id),
            ForwardStream::Subjects => {
                let sid = SubjectId::from_u64(id);
                (sid.ns_code(), sid.local_id())
            }
        };
        if current_ns != Some(ns_code) {
            if let Some(ns) = current_ns {
                flush_pack(content_store, dict_refs, appends, &mut buffer, stream, ns).await?;
            }
            current_ns = Some(ns_code);
            appends.streams.push(match stream {
                ForwardStream::Strings => None,
                ForwardStream::Subjects => Some(ns_code),
            });
        }
        buffer.push(local_id, value);
        if buffer.estimate() >= target_pack_bytes {
            flush_pack(
                content_store,
                dict_refs,
                appends,
                &mut buffer,
                stream,
                ns_code,
            )
            .await?;
        }
    }
    if let Some(ns) = current_ns {
        flush_pack(content_store, dict_refs, appends, &mut buffer, stream, ns).await?;
    }
    Ok(())
}

async fn flush_pack(
    content_store: &dyn ContentStore,
    dict_refs: &mut DictRefs,
    appends: &mut ForwardPackAppends,
    buffer: &mut PackBuffer,
    stream: ForwardStream,
    ns_code: u16,
) -> Result<()> {
    use fluree_db_binary_index::dict::forward_pack::{
        encode_forward_pack, KIND_STRING_FWD, KIND_SUBJECT_FWD,
    };
    use fluree_db_binary_index::dict::pack_builder::DEFAULT_TARGET_PAGE_BYTES;

    let (Some(&first_id), Some(&last_id)) = (buffer.ids.first(), buffer.ids.last()) else {
        return Ok(());
    };
    let (pack_kind, dict, refs) = match stream {
        ForwardStream::Strings => (
            KIND_STRING_FWD,
            DictKind::StringForward,
            &mut dict_refs.forward_packs.string_fwd_packs,
        ),
        ForwardStream::Subjects => (
            KIND_SUBJECT_FWD,
            DictKind::SubjectForward,
            subject_ns_packs_mut(dict_refs, ns_code),
        ),
    };
    if let Some(last) = refs.last() {
        if first_id <= last.last_id {
            return Err(invalid(format!(
                "new {dict:?} ids start at {first_id}, inside the dictionary's {}..={}",
                last.first_id, last.last_id
            )));
        }
    }

    let mut start = 0;
    let entries: Vec<(u64, &[u8])> = buffer
        .ids
        .iter()
        .zip(&buffer.ends)
        .map(|(&id, &end)| {
            let value = &buffer.data[start..end];
            start = end;
            (id, value)
        })
        .collect();
    let bytes = encode_forward_pack(&entries, pack_kind, ns_code, DEFAULT_TARGET_PAGE_BYTES)
        .map_err(|e| invalid(format!("{dict:?} pack build: {e}")))?;
    let pack_cid = content_store
        .put(ContentKind::DictBlob { dict }, &bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    appends.record(&pack_cid, bytes.len());
    refs.push(PackBranchEntry {
        first_id,
        last_id,
        pack_cid,
    });

    buffer.ids.clear();
    buffer.ends.clear();
    buffer.data.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_binary_index::dict::forward_pack::ForwardPack;
    use fluree_db_binary_index::DictPackRefs;
    use fluree_db_core::storage::MemoryContentStore;

    use crate::run_index::NewTermWriter;

    const LEAF: usize = 512;

    fn targets() -> Targets {
        Targets {
            leaf_bytes: LEAF,
            pack_bytes: 1024,
        }
    }

    fn terms(dir: &std::path::Path, name: &str, entries: &[(u64, Vec<u8>)]) -> NewTerms {
        let mut writer = NewTermWriter::create(&dir.join(name)).unwrap();
        for (id, value) in entries {
            writer.push(*id, value).unwrap();
        }
        writer.finish().unwrap()
    }

    /// A string reverse tree over `entries`, cut by the bulk builder.
    async fn upload_tree(cs: &dyn ContentStore, entries: &[(Vec<u8>, u64)]) -> DictTreeRefs {
        use fluree_db_binary_index::dict::builder::{build_reverse_tree, finalize_branch};
        let kind = ContentKind::DictBlob {
            dict: DictKind::StringReverse,
        };
        let tree = build_reverse_tree(
            entries
                .iter()
                .map(|(key, id)| ReverseEntry {
                    key: key.clone(),
                    id: *id,
                })
                .collect(),
            LEAF,
        )
        .unwrap();
        let mut addresses = std::collections::HashMap::new();
        let mut by_address = std::collections::HashMap::new();
        for leaf in &tree.leaves {
            let cid = cs.put(kind, &leaf.bytes).await.unwrap();
            addresses.insert(leaf.hash.clone(), cid.to_string());
            by_address.insert(cid.to_string(), cid);
        }
        let (branch, bytes, _) = finalize_branch(tree.branch, &addresses).unwrap();
        DictTreeRefs {
            branch: cs.put(kind, &bytes).await.unwrap(),
            leaves: branch
                .leaves
                .iter()
                .map(|leaf| by_address[&leaf.address].clone())
                .collect(),
        }
    }

    /// Every entry of a tree in order, checking that the branch describes
    /// its leaves and routes each key to the leaf that holds it. Returns the
    /// entries and each leaf's encoded size.
    async fn read_tree(
        cs: &dyn ContentStore,
        refs: &DictTreeRefs,
    ) -> (Vec<(Vec<u8>, u64)>, Vec<usize>) {
        let branch = DictBranch::decode(&cs.get(&refs.branch).await.unwrap()).unwrap();
        assert_eq!(branch.leaves.len(), refs.leaves.len());
        let mut entries = Vec::new();
        let mut sizes = Vec::new();
        for (i, (leaf_entry, cid)) in branch.leaves.iter().zip(&refs.leaves).enumerate() {
            assert_eq!(leaf_entry.address, cid.to_string());
            let bytes = cs.get(cid).await.unwrap();
            let leaf = ReverseLeaf::from_bytes(&bytes).unwrap();
            let leaf_entries: Vec<(Vec<u8>, u64)> =
                leaf.iter().map(|(k, id)| (k.to_vec(), id)).collect();
            assert_eq!(leaf_entries.len() as u32, leaf_entry.entry_count);
            assert_eq!(leaf_entries.first().unwrap().0, leaf_entry.first_key);
            assert_eq!(leaf_entries.last().unwrap().0, leaf_entry.last_key);
            for (key, id) in &leaf_entries {
                assert_eq!(branch.find_leaf(key), Some(i));
                assert_eq!(leaf.lookup(key), Some(*id));
            }
            sizes.push(bytes.len());
            entries.extend(leaf_entries);
        }
        assert!(entries.windows(2).all(|w| w[0].0 < w[1].0), "keys ascend");
        (entries, sizes)
    }

    fn key(n: u32) -> Vec<u8> {
        format!("s{n:05}").into_bytes()
    }

    #[tokio::test]
    async fn new_keys_land_in_the_leaves_that_own_them() {
        let cs = MemoryContentStore::new();
        let dir = tempfile::tempdir().unwrap();
        // Even keys, ids 0..: enough for a couple of dozen 512-byte leaves.
        let base: Vec<(Vec<u8>, u64)> = (0..600u32)
            .step_by(2)
            .map(|n| (key(n), u64::from(n / 2)))
            .collect();
        let base_refs = upload_tree(&cs, &base).await;
        assert!(base_refs.leaves.len() > 10);

        // New keys below the tree, in one stretch of the middle, and above it.
        let mut new: Vec<(Vec<u8>, u64)> = vec![(b"a".to_vec(), 0)];
        new.extend((201..260u32).step_by(2).map(|n| (key(n), 0)));
        new.push((b"z".to_vec(), 0));
        let new: Vec<(u64, Vec<u8>)> = new
            .into_iter()
            .enumerate()
            .map(|(i, (k, _))| (1000 + i as u64, k))
            .collect();
        let new_terms = terms(dir.path(), "strings.new", &new);

        let updated = extend_reverse_tree(
            &cs,
            DictKind::StringReverse,
            &base_refs,
            &new_terms,
            string_key,
            LEAF,
        )
        .await
        .unwrap();

        let (entries, sizes) = read_tree(&cs, &updated.tree_refs).await;
        let mut expected = base.clone();
        expected.extend(new.iter().map(|(id, k)| (k.clone(), *id)));
        expected.sort();
        assert_eq!(entries, expected);
        assert!(sizes.iter().all(|&s| s <= LEAF * 3 / 2 + 64), "{sizes:?}");

        // Only the first leaf, the last, and those over the middle stretch
        // were rewritten; the rest are the base's own artifacts.
        let kept: Vec<&ContentId> = updated
            .tree_refs
            .leaves
            .iter()
            .filter(|cid| base_refs.leaves.contains(cid))
            .collect();
        let rewritten = updated.replaced_cids.len() - 1;
        assert_eq!(updated.replaced_cids[0], base_refs.branch);
        assert_eq!(kept.len() + rewritten, base_refs.leaves.len());
        assert!(
            rewritten >= 3 && rewritten < base_refs.leaves.len() / 2,
            "{rewritten}"
        );
        assert!(updated.replaced_cids[1..]
            .iter()
            .all(|cid| !updated.tree_refs.leaves.contains(cid)));
    }

    /// A subject tree's rewritten leaves come out front-coded; the leaves no
    /// new key reaches stay the plain artifacts they were.
    #[tokio::test]
    async fn rewritten_subject_leaves_are_front_coded() {
        let cs = MemoryContentStore::new();
        let dir = tempfile::tempdir().unwrap();
        let suffix = |n: u32| format!("Offer{n:05}").into_bytes();
        let entry = |id: u64, suffix: &[u8]| {
            let mut key = Vec::new();
            subject_key(id, suffix, &mut key);
            (key, id)
        };
        let base: Vec<(Vec<u8>, u64)> = (0..600u32)
            .step_by(2)
            .map(|n| entry(SubjectId::new(5, u64::from(n)).as_u64(), &suffix(n)))
            .collect();
        let base_refs = upload_tree(&cs, &base).await;
        assert!(base_refs.leaves.len() > 10);

        let new: Vec<(u64, Vec<u8>)> = (201..260u32)
            .step_by(2)
            .map(|n| (SubjectId::new(5, 1_000 + u64::from(n)).as_u64(), suffix(n)))
            .collect();
        let new_terms = terms(dir.path(), "subjects.new", &new);
        let updated = extend_reverse_tree(
            &cs,
            DictKind::SubjectReverse,
            &base_refs,
            &new_terms,
            subject_key,
            LEAF,
        )
        .await
        .unwrap();

        let (entries, _) = read_tree(&cs, &updated.tree_refs).await;
        let mut expected = base.clone();
        expected.extend(new.iter().map(|(id, suffix)| entry(*id, suffix)));
        expected.sort();
        assert_eq!(entries, expected);
        let mut front_coded = 0;
        for cid in &updated.tree_refs.leaves {
            let bytes = cs.get(cid).await.unwrap();
            let leaf = ReverseLeaf::from_bytes(&bytes).unwrap();
            assert_eq!(leaf.is_front_coded(), !base_refs.leaves.contains(cid));
            front_coded += usize::from(leaf.is_front_coded());
        }
        assert!(front_coded >= 2, "{front_coded}");
    }

    #[tokio::test]
    async fn a_large_batch_into_one_leaf_is_cut_to_size() {
        let cs = MemoryContentStore::new();
        let dir = tempfile::tempdir().unwrap();
        let base = vec![(key(0), 0), (key(99_999), 1)];
        let base_refs = upload_tree(&cs, &base).await;
        assert_eq!(base_refs.leaves.len(), 1);

        let new: Vec<(u64, Vec<u8>)> = (1..5000u32).map(|n| (u64::from(n) + 1, key(n))).collect();
        let new_terms = terms(dir.path(), "strings.new", &new);
        let updated = extend_reverse_tree(
            &cs,
            DictKind::StringReverse,
            &base_refs,
            &new_terms,
            string_key,
            LEAF,
        )
        .await
        .unwrap();

        let (entries, sizes) = read_tree(&cs, &updated.tree_refs).await;
        assert_eq!(entries.len(), 5001);
        // Every leaf near the target; none a sliver, none overgrown.
        assert!(sizes.len() > 100);
        assert!(
            sizes
                .iter()
                .all(|s| (LEAF / 2..=LEAF * 3 / 2 + 64).contains(s)),
            "{sizes:?}"
        );
    }

    #[tokio::test]
    async fn an_empty_tree_is_built_from_the_new_terms() {
        let cs = MemoryContentStore::new();
        let dir = tempfile::tempdir().unwrap();
        let empty = DictTreeRefs {
            branch: cs
                .put(
                    ContentKind::DictBlob {
                        dict: DictKind::StringReverse,
                    },
                    &DictBranch::default().encode(),
                )
                .await
                .unwrap(),
            leaves: Vec::new(),
        };
        let new: Vec<(u64, Vec<u8>)> = (0..300u32).map(|n| (u64::from(n), key(n))).collect();
        let updated = extend_reverse_tree(
            &cs,
            DictKind::StringReverse,
            &empty,
            &terms(dir.path(), "strings.new", &new),
            string_key,
            LEAF,
        )
        .await
        .unwrap();
        let (entries, _) = read_tree(&cs, &updated.tree_refs).await;
        assert_eq!(
            entries,
            new.iter()
                .map(|(id, k)| (k.clone(), *id))
                .collect::<Vec<_>>()
        );
    }

    /// An import lists the groups it takes past the cap: `b`, which it
    /// brings more of than the cap, keyed by value from the start, and `a`,
    /// which passes the cap only with the hashed keys the tree holds, and
    /// keeps them. After every import an unlisted group holds at most the
    /// cap of hashed keys, a listed one holding any says so, and every
    /// string resolves to its id, singly and batched.
    #[tokio::test]
    async fn an_import_lists_the_groups_it_fills() {
        use fluree_db_binary_index::dict::key_scheme::DEFAULT_GROUP_HASH_CAP;
        use fluree_db_binary_index::dict::reader::DictTreeReader;
        use std::collections::BTreeMap;
        use std::sync::Arc;

        let cap = usize::from(DEFAULT_GROUP_HASH_CAP);
        let cs: Arc<dyn ContentStore> = Arc::new(MemoryContentStore::new());
        let dir = tempfile::tempdir().unwrap();
        let kind = ContentKind::DictBlob {
            dict: DictKind::StringReverse,
        };
        let mut refs = DictTreeRefs {
            branch: cs.put(kind, &DictBranch::default().encode()).await.unwrap(),
            leaves: Vec::new(),
        };
        let group = |c: char| c.to_string().repeat(48).into_bytes();
        let long = |c: char, i: usize| {
            let mut v = group(c);
            v.extend(format!("{i:040}").bytes());
            v
        };
        let imports: Vec<Vec<Vec<u8>>> = vec![
            (0..200)
                .map(key)
                .chain((0..cap - 4).map(|i| long('a', i)))
                .chain((0..cap + 8).map(|i| long('b', i)))
                .chain((0..10).map(|i| long('c', i)))
                .collect(),
            (cap - 4..cap + 6)
                .map(|i| long('a', i))
                .chain((cap + 8..cap + 12).map(|i| long('b', i)))
                .collect(),
            vec![long('a', 900), long('c', 900)],
        ];

        let mut all: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut next_id = 0u64;
        for (n, mut values) in imports.into_iter().enumerate() {
            values.sort();
            let batch: Vec<(u64, Vec<u8>)> = values
                .into_iter()
                .map(|v| {
                    next_id += 1;
                    (next_id, v)
                })
                .collect();
            all.extend(batch.iter().map(|(id, v)| (v.clone(), *id)));
            let new_terms = terms(dir.path(), &format!("strings.{n}"), &batch);
            refs = extend_reverse_tree(
                cs.as_ref(),
                DictKind::StringReverse,
                &refs,
                &new_terms,
                string_key,
                LEAF,
            )
            .await
            .unwrap()
            .tree_refs;

            let branch = DictBranch::decode(&cs.get(&refs.branch).await.unwrap()).unwrap();
            let (entries, _) = read_tree(cs.as_ref(), &refs).await;
            let mut hashed = BTreeMap::<Vec<u8>, usize>::new();
            for (key, _) in &entries {
                if let Some(g) = branch.key_scheme.hashed_key_group(key) {
                    *hashed.entry(g.to_vec()).or_default() += 1;
                }
            }
            for (g, count) in &hashed {
                match branch.raw_groups.get(g) {
                    None => assert!(*count <= cap, "import {n}: {count}"),
                    Some(listed) => assert!(listed.hashed && *count <= 2 * cap),
                }
            }
            let reader = DictTreeReader::from_refs(&cs, &refs, None).await.unwrap();
            for (value, id) in &all {
                assert_eq!(reader.reverse_lookup(value).unwrap(), Some(*id));
            }
            let found = reader
                .reverse_lookup_many(all.iter().map(|(v, _)| v.as_slice()))
                .unwrap();
            assert!(found.iter().zip(&all).all(|(f, (_, id))| *f == Some(*id)));
            assert_eq!(reader.reverse_lookup(&long('a', 999)).unwrap(), None);

            if n == 0 {
                assert!(branch.raw_groups.get(&group('a')).is_none());
                assert_eq!(hashed[&group('a')], cap - 4);
                assert!(!branch.raw_groups.get(&group('b')).unwrap().hashed);
                assert!(!hashed.contains_key(&group('b')));
            }
        }
        let branch = DictBranch::decode(&cs.get(&refs.branch).await.unwrap()).unwrap();
        assert!(branch.raw_groups.get(&group('a')).unwrap().hashed);
        assert!(!branch.raw_groups.get(&group('b')).unwrap().hashed);
    }

    /// A hashed string tree written through each path the indexer takes:
    /// imports, incremental writes and erasures. It checks after each step
    /// that every string it holds resolves to its id, singly and batched,
    /// that erased ones don't, and that the listed groups describe the
    /// hashed keys: an unlisted group holds at most the cap, and a listed
    /// one holding any says so.
    struct StringTree {
        cs: std::sync::Arc<dyn ContentStore>,
        dir: tempfile::TempDir,
        refs: DictTreeRefs,
        live: std::collections::BTreeMap<Vec<u8>, u64>,
        erased: Vec<Vec<u8>>,
        next_id: u64,
        imports: usize,
    }

    impl StringTree {
        async fn new() -> Self {
            let cs: std::sync::Arc<dyn ContentStore> =
                std::sync::Arc::new(MemoryContentStore::new());
            let kind = ContentKind::DictBlob {
                dict: DictKind::StringReverse,
            };
            let refs = DictTreeRefs {
                branch: cs.put(kind, &DictBranch::default().encode()).await.unwrap(),
                leaves: Vec::new(),
            };
            Self {
                cs,
                dir: tempfile::tempdir().unwrap(),
                refs,
                live: Default::default(),
                erased: Vec::new(),
                next_id: 0,
                imports: 0,
            }
        }

        /// The values not yet held, each with a new id, in value order.
        fn fresh(&mut self, mut values: Vec<Vec<u8>>) -> Vec<(u64, Vec<u8>)> {
            values.sort();
            values.dedup();
            values.retain(|v| !self.live.contains_key(v) && !self.erased.contains(v));
            values
                .into_iter()
                .map(|v| {
                    self.next_id += 1;
                    (self.next_id, v)
                })
                .collect()
        }

        async fn import(&mut self, values: Vec<Vec<u8>>, leaf_bytes: usize) {
            let batch = self.fresh(values);
            if batch.is_empty() {
                return;
            }
            self.imports += 1;
            let new_terms = terms(
                self.dir.path(),
                &format!("strings.{}", self.imports),
                &batch,
            );
            self.refs = extend_reverse_tree(
                self.cs.as_ref(),
                DictKind::StringReverse,
                &self.refs,
                &new_terms,
                string_key,
                leaf_bytes,
            )
            .await
            .unwrap()
            .tree_refs;
            self.live.extend(batch.into_iter().map(|(id, v)| (v, id)));
        }

        async fn write(&mut self, values: Vec<Vec<u8>>) {
            let batch = self.fresh(values);
            if batch.is_empty() {
                return;
            }
            let strings = batch
                .iter()
                .map(|(id, v)| (*id as u32, v.clone()))
                .collect();
            // The hashes the resolver would carry: those of the values it
            // looks up by their hashed keys.
            let branch =
                DictBranch::decode(&self.cs.get(&self.refs.branch).await.unwrap()).unwrap();
            let rules = branch.keys();
            let hashes: super::super::dicts::StringHashes = batch
                .iter()
                .filter_map(|(id, v)| Some((*id as u32, rules.lookup_keys(v).hash()?)))
                .collect();
            self.refs = super::super::dicts::upload_incremental_reverse_tree_async_strings(
                self.cs.as_ref(),
                DictKind::StringReverse,
                &self.refs,
                strings,
                &hashes,
                None,
            )
            .await
            .unwrap()
            .tree_refs;
            self.live.extend(batch.into_iter().map(|(id, v)| (v, id)));
        }

        async fn erase(&mut self, mut values: Vec<Vec<u8>>) {
            values.retain(|v| self.live.contains_key(v));
            values.sort();
            values.dedup();
            if values.is_empty() {
                return;
            }
            self.refs = super::super::dicts::remove_from_reverse_tree_async(
                self.cs.as_ref(),
                DictKind::StringReverse,
                &self.refs,
                values.clone(),
            )
            .await
            .unwrap()
            .tree_refs;
            for v in values {
                self.live.remove(&v);
                self.erased.push(v);
            }
        }

        async fn check(&self, step: &str) {
            use fluree_db_binary_index::dict::key_scheme::DEFAULT_GROUP_HASH_CAP;
            use fluree_db_binary_index::dict::reader::DictTreeReader;
            let cap = usize::from(DEFAULT_GROUP_HASH_CAP);
            let branch =
                DictBranch::decode(&self.cs.get(&self.refs.branch).await.unwrap()).unwrap();
            let entries = if self.refs.leaves.is_empty() {
                Vec::new()
            } else {
                read_tree(self.cs.as_ref(), &self.refs).await.0
            };
            assert_eq!(entries.len(), self.live.len(), "{step}: entries");
            let mut hashed = std::collections::BTreeMap::<Vec<u8>, usize>::new();
            for (key, _) in &entries {
                if let Some(g) = branch.key_scheme.hashed_key_group(key) {
                    *hashed.entry(g.to_vec()).or_default() += 1;
                }
            }
            for (g, count) in &hashed {
                match branch.raw_groups.get(g) {
                    None => assert!(*count <= cap, "{step}: unlisted group holds {count}"),
                    Some(listed) => assert!(
                        listed.hashed && *count <= 2 * cap,
                        "{step}: listed group holds {count}, flagged {}",
                        listed.hashed
                    ),
                }
            }
            let reader = DictTreeReader::from_refs(&self.cs, &self.refs, None)
                .await
                .unwrap();
            for (value, id) in &self.live {
                assert_eq!(reader.reverse_lookup(value).unwrap(), Some(*id), "{step}");
            }
            let values: Vec<&[u8]> = self.live.keys().map(Vec::as_slice).collect();
            let found = reader.reverse_lookup_many(values.iter().copied()).unwrap();
            assert!(
                found
                    .iter()
                    .zip(self.live.values())
                    .all(|(f, id)| *f == Some(*id)),
                "{step}: batched"
            );
            for value in &self.erased {
                assert_eq!(
                    reader.reverse_lookup(value).unwrap(),
                    None,
                    "{step}: erased"
                );
            }
        }
    }

    /// A value of `group` (48 bytes): long, keyed by hash unless the group
    /// is listed, or of middling length, always keyed by value.
    fn grouped(group: u8, i: u64, long: bool) -> Vec<u8> {
        let mut v = vec![b'g'; 47];
        v.push(b'0' + group);
        v.extend(format!("{i:06}").bytes());
        if long {
            v.extend(std::iter::repeat_n(b'x', 40));
        }
        v
    }

    /// An import that alone takes a group past the cap lists it, and keeps
    /// the hashed keys the tree already holds findable: in one leaf the
    /// merge reads, and in small leaves it doesn't, past value keys of the
    /// group that sit between the new values and the hashed range.
    #[tokio::test]
    async fn an_import_past_the_cap_keeps_the_hashed_keys_the_tree_holds() {
        use fluree_db_binary_index::dict::key_scheme::DEFAULT_GROUP_HASH_CAP;
        let cap = u64::from(DEFAULT_GROUP_HASH_CAP);
        for leaf_bytes in [1 << 20, LEAF] {
            let mut tree = StringTree::new().await;
            let mut first: Vec<Vec<u8>> = (0..200).map(key).collect();
            first.extend((0..cap - 12).map(|i| grouped(0, i, true)));
            first.extend((0..40).map(|i| grouped(0, 500 + i, false)));
            tree.import(first, leaf_bytes).await;
            tree.check("first import").await;
            tree.import(
                (100..100 + cap + 8).map(|i| grouped(0, i, true)).collect(),
                leaf_bytes,
            )
            .await;
            tree.check(&format!("second import, {leaf_bytes}-byte leaves"))
                .await;
        }
    }

    /// A seeded run of imports, incremental writes and erasures over small
    /// leaves, mixing long and middling values of a few groups with short
    /// ones, checked after every step.
    #[tokio::test]
    async fn a_seeded_mix_of_writes_keeps_every_string_findable() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move |n: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % n
        };
        let mut tree = StringTree::new().await;
        let mut serial = 0u64;
        for step in 0..60 {
            let mut values = Vec::new();
            for _ in 0..=next(30) {
                serial += 1;
                values.push(match next(4) {
                    0 => key(serial as u32),
                    1 => grouped(next(3) as u8, serial, false),
                    _ => grouped(next(3) as u8, serial, true),
                });
            }
            let what = match next(3) {
                0 => {
                    tree.import(values, LEAF).await;
                    "import"
                }
                1 => {
                    tree.write(values).await;
                    "write"
                }
                _ => {
                    let held: Vec<Vec<u8>> = tree.live.keys().cloned().collect();
                    let gone = (0..=next(12))
                        .filter_map(|_| held.get(next(held.len().max(1) as u64) as usize).cloned())
                        .collect();
                    tree.erase(gone).await;
                    "erase"
                }
            };
            tree.check(&format!("step {step} ({what})")).await;
        }
    }

    #[tokio::test]
    async fn a_term_the_tree_already_holds_is_refused() {
        let cs = MemoryContentStore::new();
        let dir = tempfile::tempdir().unwrap();
        let base_refs = upload_tree(&cs, &[(key(1), 0), (key(3), 1)]).await;
        let new = vec![(5, key(2)), (6, key(3))];
        let result = extend_reverse_tree(
            &cs,
            DictKind::StringReverse,
            &base_refs,
            &terms(dir.path(), "strings.new", &new),
            string_key,
            LEAF,
        )
        .await;
        assert!(result.is_err());
    }

    /// Resolve `id` through a forward stream's routing table.
    async fn forward_lookup(
        cs: &dyn ContentStore,
        refs: &[PackBranchEntry],
        id: u64,
    ) -> Option<Vec<u8>> {
        let pack = refs.iter().find(|p| p.first_id <= id && id <= p.last_id)?;
        let bytes = cs.get(&pack.pack_cid).await.unwrap();
        ForwardPack::from_bytes(&bytes)
            .unwrap()
            .lookup(id)
            .unwrap()
            .map(std::borrow::Cow::into_owned)
    }

    #[tokio::test]
    async fn forward_packs_continue_each_stream() {
        use fluree_db_binary_index::dict::forward_pack::{
            encode_forward_pack, KIND_STRING_FWD, KIND_SUBJECT_FWD,
        };
        let cs = MemoryContentStore::new();
        let dir = tempfile::tempdir().unwrap();
        let empty_tree = DictTreeRefs {
            branch: cs
                .put(
                    ContentKind::DictBlob {
                        dict: DictKind::SubjectReverse,
                    },
                    &DictBranch::default().encode(),
                )
                .await
                .unwrap(),
            leaves: Vec::new(),
        };

        // Namespace 5 holds local ids 0..=2 and the strings 0..=3.
        let pack = |kind, ns, n: u64| {
            let values: Vec<Vec<u8>> = (0..n).map(|i| format!("old{i}").into_bytes()).collect();
            let entries: Vec<(u64, &[u8])> = values
                .iter()
                .enumerate()
                .map(|(i, v)| (i as u64, v.as_slice()))
                .collect();
            encode_forward_pack(&entries, kind, ns, 4096).unwrap()
        };
        let subject_pack = cs
            .put(
                ContentKind::DictBlob {
                    dict: DictKind::SubjectForward,
                },
                &pack(KIND_SUBJECT_FWD, 5, 3),
            )
            .await
            .unwrap();
        let string_pack = cs
            .put(
                ContentKind::DictBlob {
                    dict: DictKind::StringForward,
                },
                &pack(KIND_STRING_FWD, 0, 4),
            )
            .await
            .unwrap();
        let base = DictRefs {
            forward_packs: DictPackRefs {
                string_fwd_packs: vec![PackBranchEntry {
                    first_id: 0,
                    last_id: 3,
                    pack_cid: string_pack,
                }],
                subject_fwd_ns_packs: vec![(
                    5,
                    vec![PackBranchEntry {
                        first_id: 0,
                        last_id: 2,
                        pack_cid: subject_pack,
                    }],
                )],
            },
            subject_reverse: empty_tree.clone(),
            string_reverse: empty_tree,
        };

        let sid = |ns, local| SubjectId::new(ns, local).as_u64();
        let mut subjects: Vec<(u64, Vec<u8>)> = (3..400)
            .map(|l| (sid(5, l), format!("five{l:04}").into_bytes()))
            .collect();
        subjects.extend((1..50).map(|l| (sid(9, l), format!("nine{l:04}").into_bytes())));
        let strings: Vec<(u64, Vec<u8>)> = (4..300)
            .map(|i| (i, format!("str{i:04}").into_bytes()))
            .collect();
        let updated = extend_dicts_with(
            &cs,
            &base,
            7,
            &terms(dir.path(), "subjects.new", &subjects),
            &terms(dir.path(), "strings.new", &strings),
            targets(),
        )
        .await
        .unwrap();

        let packs = &updated.dict_refs.forward_packs;
        let ns = |code| {
            &packs
                .subject_fwd_ns_packs
                .iter()
                .find(|(n, _)| *n == code)
                .unwrap()
                .1
        };
        for refs in [&packs.string_fwd_packs, ns(5), ns(9)] {
            assert!(refs.windows(2).all(|w| w[0].last_id < w[1].first_id));
        }
        assert_eq!(forward_lookup(&cs, ns(5), 1).await.unwrap(), b"old1");
        assert_eq!(
            forward_lookup(&cs, &packs.string_fwd_packs, 2)
                .await
                .unwrap(),
            b"old2"
        );
        for (id, value) in &subjects {
            let s = SubjectId::from_u64(*id);
            assert_eq!(
                forward_lookup(&cs, ns(s.ns_code()), s.local_id())
                    .await
                    .as_ref(),
                Some(value)
            );
        }
        for (id, value) in &strings {
            assert_eq!(
                forward_lookup(&cs, &packs.string_fwd_packs, *id)
                    .await
                    .as_ref(),
                Some(value)
            );
        }

        // The reverse trees name every new term too.
        let (subject_entries, _) = read_tree(&cs, &updated.dict_refs.subject_reverse).await;
        assert_eq!(subject_entries.len(), subjects.len());
        let (string_entries, _) = read_tree(&cs, &updated.dict_refs.string_reverse).await;
        assert_eq!(string_entries.len(), strings.len());
    }

    // ---- Memory stress ----

    /// Process peak resident set in bytes (high-water mark).
    fn peak_rss_bytes() -> u64 {
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
            return 0;
        }
        let maxrss = usage.ru_maxrss.max(0) as u64;
        // Linux reports kibibytes; macOS reports bytes.
        if cfg!(target_os = "macos") {
            maxrss
        } else {
            maxrss * 1024
        }
    }

    /// Keeps small objects (the base's empty branches) and discards the
    /// rest, so the measurement reflects the update's own working set
    /// rather than retained artifacts.
    #[derive(Debug, Default)]
    struct SmallObjectStore {
        kept: std::sync::Mutex<std::collections::HashMap<ContentId, Vec<u8>>>,
    }

    #[async_trait::async_trait]
    impl ContentStore for SmallObjectStore {
        async fn has(&self, id: &ContentId) -> fluree_db_core::error::Result<bool> {
            Ok(self.kept.lock().unwrap().contains_key(id))
        }
        async fn get(
            &self,
            id: &ContentId,
        ) -> fluree_db_core::error::Result<fluree_db_core::ContentBytes> {
            self.kept
                .lock()
                .unwrap()
                .get(id)
                .cloned()
                .map(fluree_db_core::ContentBytes::from)
                .ok_or_else(|| fluree_db_core::error::Error::not_found(id.to_string()))
        }
        async fn put(
            &self,
            kind: ContentKind,
            bytes: &[u8],
        ) -> fluree_db_core::error::Result<ContentId> {
            let id = ContentId::new(kind, bytes);
            if bytes.len() <= 64 {
                self.kept.lock().unwrap().insert(id.clone(), bytes.to_vec());
            }
            Ok(id)
        }
        async fn put_with_id(
            &self,
            _id: &ContentId,
            _bytes: &[u8],
        ) -> fluree_db_core::error::Result<()> {
            Ok(())
        }
        async fn release(&self, _id: &ContentId) -> fluree_db_core::error::Result<()> {
            Ok(())
        }
    }

    /// Peak memory of adding `FLUREE_EXTEND_TERMS` new subjects and as many
    /// new strings to an empty dictionary. `FLUREE_EXTEND_STREAMING=0` runs
    /// the in-memory update over the same terms for comparison.
    #[tokio::test]
    #[ignore = "memory stress; run explicitly"]
    async fn stress_extend_memory() {
        let env = |k: &str, d: usize| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        };
        let n = env("FLUREE_EXTEND_TERMS", 3_000_000) as u64;
        let streaming = env("FLUREE_EXTEND_STREAMING", 1) != 0;
        let dir = tempfile::tempdir().unwrap();

        let mut subjects = NewTermWriter::create(&dir.path().join("subjects.new")).unwrap();
        let mut strings = NewTermWriter::create(&dir.path().join("strings.new")).unwrap();
        for i in 0..n {
            let value = format!("term-{i:012}-padding-to-forty-bytes");
            subjects
                .push(SubjectId::new(5, i + 1).as_u64(), value.as_bytes())
                .unwrap();
            strings.push(i + 1, value.as_bytes()).unwrap();
        }
        let (subjects, strings) = (subjects.finish().unwrap(), strings.finish().unwrap());

        let store = SmallObjectStore::default();
        let empty_branch = |dict| {
            let store = &store;
            async move {
                DictTreeRefs {
                    branch: store
                        .put(
                            ContentKind::DictBlob { dict },
                            &DictBranch::default().encode(),
                        )
                        .await
                        .unwrap(),
                    leaves: Vec::new(),
                }
            }
        };
        let base = DictRefs {
            forward_packs: DictPackRefs {
                string_fwd_packs: Vec::new(),
                subject_fwd_ns_packs: Vec::new(),
            },
            subject_reverse: empty_branch(DictKind::SubjectReverse).await,
            string_reverse: empty_branch(DictKind::StringReverse).await,
        };

        let before = peak_rss_bytes();
        if streaming {
            extend_dicts(&store, &base, 1, &subjects, &strings)
                .await
                .unwrap();
        } else {
            let read = |terms: &NewTerms| {
                let mut reader = terms.reader().unwrap();
                let mut out = Vec::new();
                while let Some((id, value)) = reader.next_term().unwrap() {
                    out.push((id, value.to_vec()));
                }
                out
            };
            let new_subjects: Vec<(u16, u64, Vec<u8>)> = read(&subjects)
                .into_iter()
                .map(|(id, v)| (5, SubjectId::from_u64(id).local_id(), v))
                .collect();
            let new_strings: Vec<(u32, Vec<u8>)> = read(&strings)
                .into_iter()
                .map(|(id, v)| (id as u32, v))
                .collect();
            super::super::incremental::apply_dict_novelty(
                &store,
                &base,
                1,
                &new_subjects,
                &new_strings,
                &Default::default(),
                None,
                &mut super::super::incremental::CompactionBudget::for_cycle(),
            )
            .await
            .unwrap();
        }
        let after = peak_rss_bytes();
        eprintln!(
            "extend-stress: streaming={streaming} terms={n} (x2) peak_rss_growth={}MiB",
            after.saturating_sub(before) / (1024 * 1024)
        );
    }
}
