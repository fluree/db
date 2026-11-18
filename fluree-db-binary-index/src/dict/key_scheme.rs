//! How a reverse tree turns a value into its key.
//!
//! A [`KeyScheme::Raw`] tree keys a value by its bytes. A
//! [`KeyScheme::PrefixHash`] tree does too, except for a value longer than
//! `prefix_len + 1 + KEY_HASH_LEN` bytes, which it keys by its first
//! `prefix_len` bytes, a [`HASHED_KEY_MARKER`] byte and the value's SHA-256.
//! Long values then cost a fixed-size key instead of a second copy of
//! themselves.
//!
//! Values are UTF-8, which never contains `0xFF`, so a key with the marker
//! at `prefix_len` can only be a hashed one, and a key keeps its value's
//! first `prefix_len` bytes either way: every value starting with a prefix
//! of at most `prefix_len` bytes lies in that prefix's key range. A longer
//! prefix's hashed candidates share only the first `prefix_len` bytes and
//! must be checked against their values.
//!
//! Equal keys for different values would take a SHA-256 collision, the
//! same assumption content addresses make; the tree writers refuse equal
//! keys rather than replace one value with another.

use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use fluree_db_core::DictKind;
use sha2::{Digest, Sha256};

/// Hash bytes in a hashed key: the whole SHA-256.
pub const KEY_HASH_LEN: usize = 32;

/// The byte between a hashed key's prefix and its hash.
pub const HASHED_KEY_MARKER: u8 = 0xFF;

/// Prefix a new string tree keeps before the hash.
pub const DEFAULT_STRING_KEY_PREFIX: u8 = 48;

/// Hashed values a spilling [`KeyOrder`] holds before writing a sorted
/// chunk to disk: 40 bytes each, so about 40 MiB.
pub const KEY_ORDER_SPILL_ENTRIES: usize = 1 << 20;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KeyScheme {
    /// The key is the value.
    #[default]
    Raw,
    /// Long values are keyed by a prefix and a hash.
    PrefixHash { prefix_len: u8 },
}

/// A key range `[start, end)` (`end: None` runs to the last key) and
/// whether a value found in it must be checked against the prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefixRange {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    pub check: bool,
}

/// The least byte string above every string that starts with `prefix`;
/// `None` when nothing is.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 0xFF {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

fn digest(value: &[u8]) -> [u8; KEY_HASH_LEN] {
    Sha256::digest(value).into()
}

impl KeyScheme {
    /// The scheme a new tree of `dict` is written in.
    pub fn for_new_tree(dict: DictKind) -> Self {
        match dict {
            DictKind::StringReverse => Self::PrefixHash {
                prefix_len: DEFAULT_STRING_KEY_PREFIX,
            },
            _ => Self::Raw,
        }
    }

    /// The prefix a hashed key keeps, if this scheme hashes.
    fn prefix_len(self) -> Option<usize> {
        match self {
            Self::Raw => None,
            Self::PrefixHash { prefix_len } => Some(prefix_len as usize),
        }
    }

    /// The tree key for `value`.
    pub fn key(self, value: &[u8]) -> Cow<'_, [u8]> {
        let Some(k) = self.prefix_len() else {
            return Cow::Borrowed(value);
        };
        if value.len() <= k + 1 + KEY_HASH_LEN {
            return Cow::Borrowed(value);
        }
        debug_assert!(
            !value[..k].contains(&HASHED_KEY_MARKER),
            "reverse-tree values are UTF-8"
        );
        let mut key = Vec::with_capacity(k + 1 + KEY_HASH_LEN);
        key.extend_from_slice(&value[..k]);
        key.push(HASHED_KEY_MARKER);
        key.extend_from_slice(&digest(value));
        Cow::Owned(key)
    }

    /// The key ranges that hold every value starting with `prefix`.
    pub fn prefix_ranges(self, prefix: &[u8]) -> Vec<PrefixRange> {
        let Some(k) = self.prefix_len() else {
            // A raw tree's values are its keys, and UTF-8 never holds 0xFF.
            let mut end = prefix.to_vec();
            end.push(0xFF);
            return vec![PrefixRange {
                start: prefix.to_vec(),
                end: Some(end),
                check: false,
            }];
        };
        let exact = PrefixRange {
            start: prefix.to_vec(),
            end: prefix_end(prefix),
            check: false,
        };
        if prefix.len() <= k {
            return vec![exact];
        }
        let mut hashed = prefix[..k].to_vec();
        hashed.push(HASHED_KEY_MARKER);
        vec![
            exact,
            PrefixRange {
                end: prefix_end(&hashed),
                start: hashed,
                check: true,
            },
        ]
    }

    /// Whether this scheme may order keys differently from their values.
    pub fn reorders(self) -> bool {
        self != Self::Raw
    }
}

/// A hashed value as a run holds it: its hash and id.
type Hashed = ([u8; KEY_HASH_LEN], u64);

const RECORD_LEN: usize = KEY_HASH_LEN + 8;

static SPILL_SEQ: AtomicU64 = AtomicU64::new(0);

/// Settled entries, in key order.
enum Segment {
    Key(Vec<u8>, u64),
    /// A closed run's hashed values, sorted; `prefix` ends in the marker.
    Held {
        prefix: Vec<u8>,
        entries: std::vec::IntoIter<Hashed>,
    },
    /// A closed run too large to hold, merged from its sorted chunks.
    Spilled {
        prefix: Vec<u8>,
        merge: SpillMerge,
    },
}

fn hashed_key(prefix: &[u8], hash: &[u8; KEY_HASH_LEN]) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + KEY_HASH_LEN);
    key.extend_from_slice(prefix);
    key.extend_from_slice(hash);
    key
}

/// Puts `(value, id)` pairs arriving in value order into key order.
///
/// Only values sharing their first `prefix_len` bytes can change order, and
/// those arrive together as a run. A run's whole-keyed values sort before
/// its hashed ones and keep their order, so they pass straight through;
/// its hashed values are held, as hash and id, until the run ends, then
/// released sorted by hash. A spilling order writes a sorted chunk to disk
/// whenever it holds [`KEY_ORDER_SPILL_ENTRIES`], and merges the chunks as
/// the run is read out, so a run of any size stays within that memory.
pub struct KeyOrder {
    scheme: KeyScheme,
    spill_dir: Option<PathBuf>,
    spill_entries: usize,
    run_prefix: Option<Vec<u8>>,
    held: Vec<Hashed>,
    chunks: Vec<PathBuf>,
    settled: VecDeque<Segment>,
}

impl KeyOrder {
    /// An order that holds each run in memory, however large.
    pub fn new(scheme: KeyScheme) -> Self {
        Self {
            scheme,
            spill_dir: None,
            spill_entries: KEY_ORDER_SPILL_ENTRIES,
            run_prefix: None,
            held: Vec::new(),
            chunks: Vec::new(),
            settled: VecDeque::new(),
        }
    }

    /// An order that spills large runs to sorted chunks in `dir`.
    pub fn spilling(scheme: KeyScheme, dir: impl Into<PathBuf>) -> Self {
        let mut order = Self::new(scheme);
        order.spill_dir = Some(dir.into());
        order
    }

    #[cfg(test)]
    fn with_spill_entries(mut self, entries: usize) -> Self {
        self.spill_entries = entries;
        self
    }

    /// Add the next value; values must arrive in ascending byte order.
    pub fn push(&mut self, value: &[u8], id: u64) -> io::Result<()> {
        let Some(k) = self.scheme.prefix_len() else {
            self.settled.push_back(Segment::Key(value.to_vec(), id));
            return Ok(());
        };
        if value.len() <= k {
            self.close_run()?;
            self.settled.push_back(Segment::Key(value.to_vec(), id));
            return Ok(());
        }
        if self.run_prefix.as_deref() != Some(&value[..k]) {
            self.close_run()?;
            self.run_prefix = Some(value[..k].to_vec());
        }
        if value.len() <= k + 1 + KEY_HASH_LEN {
            self.settled.push_back(Segment::Key(value.to_vec(), id));
        } else {
            self.held.push((digest(value), id));
            if self.spill_dir.is_some() && self.held.len() >= self.spill_entries {
                self.spill()?;
            }
        }
        Ok(())
    }

    /// No more values: settle whatever is held.
    pub fn finish(&mut self) -> io::Result<()> {
        self.close_run()
    }

    /// The next `(key, id)` in key order, once it is settled.
    pub fn pop(&mut self) -> io::Result<Option<(Vec<u8>, u64)>> {
        loop {
            let next = match self.settled.front_mut() {
                None => return Ok(None),
                Some(Segment::Key(..)) => match self.settled.pop_front() {
                    Some(Segment::Key(key, id)) => return Ok(Some((key, id))),
                    _ => unreachable!("the front is a key"),
                },
                Some(Segment::Held { prefix, entries }) => entries
                    .next()
                    .map(|(hash, id)| (hashed_key(prefix, &hash), id)),
                Some(Segment::Spilled { prefix, merge }) => merge
                    .next()?
                    .map(|(hash, id)| (hashed_key(prefix, &hash), id)),
            };
            match next {
                Some(entry) => return Ok(Some(entry)),
                None => {
                    self.settled.pop_front();
                }
            }
        }
    }

    fn close_run(&mut self) -> io::Result<()> {
        let Some(mut prefix) = self.run_prefix.take() else {
            return Ok(());
        };
        prefix.push(HASHED_KEY_MARKER);
        if self.chunks.is_empty() {
            self.held.sort_unstable();
            let entries = std::mem::take(&mut self.held).into_iter();
            self.settled.push_back(Segment::Held { prefix, entries });
            return Ok(());
        }
        if !self.held.is_empty() {
            self.spill()?;
        }
        let merge = SpillMerge::open(std::mem::take(&mut self.chunks))?;
        self.settled.push_back(Segment::Spilled { prefix, merge });
        Ok(())
    }

    fn spill(&mut self) -> io::Result<()> {
        let dir = self
            .spill_dir
            .as_ref()
            .expect("only a spilling order spills");
        let path = dir.join(format!(
            ".key-order-{}-{}.chunk",
            std::process::id(),
            SPILL_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        self.held.sort_unstable();
        // Recorded before writing, so a failed write is still cleaned up.
        self.chunks.push(path.clone());
        let mut out = BufWriter::new(File::create(&path)?);
        for (hash, id) in self.held.drain(..) {
            out.write_all(&hash)?;
            out.write_all(&id.to_le_bytes())?;
        }
        out.flush()
    }
}

impl Drop for KeyOrder {
    fn drop(&mut self) {
        for path in &self.chunks {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Merges a run's sorted chunk files, removing them when dropped.
struct SpillMerge {
    readers: Vec<BufReader<File>>,
    heap: BinaryHeap<Reverse<(Hashed, usize)>>,
    paths: Vec<PathBuf>,
}

impl SpillMerge {
    fn open(paths: Vec<PathBuf>) -> io::Result<Self> {
        let mut merge = Self {
            readers: Vec::with_capacity(paths.len()),
            heap: BinaryHeap::with_capacity(paths.len()),
            paths,
        };
        for i in 0..merge.paths.len() {
            merge
                .readers
                .push(BufReader::new(File::open(&merge.paths[i])?));
            if let Some(entry) = Self::read(&mut merge.readers[i])? {
                merge.heap.push(Reverse((entry, i)));
            }
        }
        Ok(merge)
    }

    fn read(reader: &mut BufReader<File>) -> io::Result<Option<Hashed>> {
        let mut record = [0u8; RECORD_LEN];
        match reader.read_exact(&mut record) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let mut hash = [0u8; KEY_HASH_LEN];
        hash.copy_from_slice(&record[..KEY_HASH_LEN]);
        let id = u64::from_le_bytes(record[KEY_HASH_LEN..].try_into().expect("eight bytes"));
        Ok(Some((hash, id)))
    }

    fn next(&mut self) -> io::Result<Option<Hashed>> {
        let Some(Reverse((entry, i))) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(next) = Self::read(&mut self.readers[i])? {
            self.heap.push(Reverse((next, i)));
        }
        Ok(Some(entry))
    }
}

impl Drop for SpillMerge {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEME: KeyScheme = KeyScheme::PrefixHash { prefix_len: 8 };

    /// Values near and across the hashing threshold, sharing prefixes of
    /// every length, and some that are prefixes of others.
    fn values() -> Vec<Vec<u8>> {
        let mut values: Vec<Vec<u8>> = Vec::new();
        for stem in ["", "a", "abcdefg", "abcdefgh", "abcdefghi", "abcdefgz", "b"] {
            for tail in 0..60usize {
                let mut v = stem.as_bytes().to_vec();
                v.extend(std::iter::repeat_n(b'a' + (tail % 26) as u8, tail));
                values.push(v);
                let mut w = stem.as_bytes().to_vec();
                w.extend(format!("{tail}-é-{}", "q".repeat(tail)).bytes());
                values.push(w);
            }
        }
        values.sort();
        values.dedup();
        values
    }

    fn drain(order: &mut KeyOrder, out: &mut Vec<(Vec<u8>, u64)>) {
        while let Some(e) = order.pop().unwrap() {
            out.push(e);
        }
    }

    fn sorted_keys(scheme: KeyScheme, values: &[Vec<u8>]) -> Vec<(Vec<u8>, u64)> {
        let mut want: Vec<(Vec<u8>, u64)> = values
            .iter()
            .enumerate()
            .map(|(id, v)| (scheme.key(v).into_owned(), id as u64))
            .collect();
        want.sort();
        want
    }

    #[test]
    fn long_values_are_keyed_by_prefix_marker_and_hash() {
        let limit = 8 + 1 + KEY_HASH_LEN;
        let short = vec![b'x'; limit];
        assert!(matches!(SCHEME.key(&short), Cow::Borrowed(k) if k == short));
        let long = vec![b'x'; limit + 1];
        let key = SCHEME.key(&long);
        assert_eq!(key.len(), limit);
        assert_eq!(&key[..8], &long[..8]);
        assert_eq!(key[8], HASHED_KEY_MARKER);
        assert_eq!(&key[9..], Sha256::digest(&long).as_slice());
        assert_eq!(KeyScheme::Raw.key(&long), Cow::Borrowed(long.as_slice()));
    }

    /// Every prefix's ranges hold exactly the keys of the values starting
    /// with it, once the checked ranges' values are checked.
    #[test]
    fn prefix_ranges_cover_exactly_the_matching_values() {
        let values = values();
        let mut prefixes: Vec<Vec<u8>> = vec![vec![]];
        for v in &values {
            for n in [1, 7, 8, 9, 12] {
                if v.len() >= n {
                    prefixes.push(v[..n].to_vec());
                }
            }
        }
        for scheme in [SCHEME, KeyScheme::Raw] {
            let keyed: Vec<(Vec<u8>, &Vec<u8>)> = values
                .iter()
                .map(|v| (scheme.key(v).into_owned(), v))
                .collect();
            for prefix in &prefixes {
                let mut found: Vec<&Vec<u8>> = Vec::new();
                for range in scheme.prefix_ranges(prefix) {
                    for (key, value) in &keyed {
                        let inside = key.as_slice() >= range.start.as_slice()
                            && range.end.as_ref().is_none_or(|end| key < end);
                        if inside && (!range.check || value.starts_with(prefix)) {
                            found.push(value);
                        }
                    }
                }
                found.sort();
                let want: Vec<&Vec<u8>> = values.iter().filter(|v| v.starts_with(prefix)).collect();
                assert_eq!(found, want, "{scheme:?} {prefix:?}");
            }
        }
    }

    /// Values pushed in byte order come out in key order, each once.
    #[test]
    fn key_order_turns_value_order_into_key_order() {
        let values = values();
        for scheme in [SCHEME, KeyScheme::Raw] {
            let mut order = KeyOrder::new(scheme);
            let mut out = Vec::new();
            for (id, v) in values.iter().enumerate() {
                order.push(v, id as u64).unwrap();
                drain(&mut order, &mut out);
            }
            order.finish().unwrap();
            drain(&mut order, &mut out);
            assert_eq!(out, sorted_keys(scheme, &values), "{scheme:?}");
        }
    }

    /// A run larger than the spill limit holds no more than the limit,
    /// spills sorted chunks, comes out in the same order as a held run,
    /// and leaves no chunk files behind.
    #[test]
    fn a_large_run_spills_and_merges_in_key_order() {
        let dir = std::env::temp_dir().join(format!("key-order-spill-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut values = values();
        let shared = "abcdefgh-shared-by-every-value-in-the-run-";
        values.extend((0..2_000).map(|i| format!("{shared}{i:05}").into_bytes()));
        values.extend((0..50).map(|i| format!("{shared}{i:05}é").into_bytes()));
        values.sort();
        values.dedup();

        let mut order = KeyOrder::spilling(SCHEME, &dir).with_spill_entries(64);
        let mut out = Vec::new();
        let mut spilled = 0;
        for (id, v) in values.iter().enumerate() {
            order.push(v, id as u64).unwrap();
            assert!(order.held.len() < 64);
            spilled = spilled.max(order.chunks.len());
            drain(&mut order, &mut out);
        }
        order.finish().unwrap();
        drain(&mut order, &mut out);
        assert!(spilled > 20, "{spilled}");
        assert_eq!(out, sorted_keys(SCHEME, &values));
        drop(order);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        std::fs::remove_dir(&dir).unwrap();
    }

    #[test]
    fn only_string_trees_hash() {
        assert_eq!(
            KeyScheme::for_new_tree(DictKind::StringReverse),
            KeyScheme::PrefixHash {
                prefix_len: DEFAULT_STRING_KEY_PREFIX
            }
        );
        assert_eq!(
            KeyScheme::for_new_tree(DictKind::SubjectReverse),
            KeyScheme::Raw
        );
        assert_eq!(
            KeyScheme::for_new_tree(DictKind::TermReverse),
            KeyScheme::Raw
        );
    }
}
