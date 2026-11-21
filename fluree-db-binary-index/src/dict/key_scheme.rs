//! How a reverse tree turns a value into its key.
//!
//! A [`KeyScheme::Raw`] tree keys a value by its bytes. A
//! [`KeyScheme::PrefixHash`] tree does too, except that it may key a *long*
//! value, one longer than `prefix_len + 1 + KEY_HASH_LEN` bytes, by its first
//! `prefix_len` bytes, a [`HASHED_KEY_MARKER`] byte and the value's SHA-256.
//! Long values then cost a fixed-size key instead of a second copy of
//! themselves.
//!
//! Values are UTF-8, which never contains `0xFF`, so a key with the marker
//! at `prefix_len` can only be a hashed one, and a key keeps its value's
//! first `prefix_len` bytes either way: every value starting with a prefix
//! of at most `prefix_len` bytes lies in that prefix's key range.
//!
//! The long values sharing their first `prefix_len` bytes are a *group*. A
//! longer prefix's hashed candidates are its group's hashed values, each of
//! which must be checked against its value, so a group hashes at most `cap`
//! values. Past that the tree lists the group in its [`RawGroups`] and keys
//! the group's new values by value, where a prefix finds them exactly:
//!
//! - a full build counts each group first, so a group it lists holds no
//!   hashed value;
//! - an incremental write counts the hashed values a group already holds,
//!   and lists the group before it would pass the cap;
//! - an import into an existing tree sees a group's existing hashed values
//!   only as it merges past them, so it can add up to `cap` before listing
//!   it.
//!
//! A group listed after it began hashing keeps those values, and says so,
//! so a lookup there tries the value key and then the hashed one.
//!
//! Equal keys for different values would take a SHA-256 collision, the
//! same assumption content addresses make; the tree writers refuse equal
//! keys rather than replace one value with another.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::io;

use fluree_db_core::DictKind;
use sha2::{Digest, Sha256};

/// Hash bytes in a hashed key: the whole SHA-256.
pub const KEY_HASH_LEN: usize = 32;

/// The byte between a hashed key's prefix and its hash.
pub const HASHED_KEY_MARKER: u8 = 0xFF;

/// Prefix a new string tree keeps before the hash.
pub const DEFAULT_STRING_KEY_PREFIX: u8 = 48;

/// Hashed values a new string tree lets a group hold.
pub const DEFAULT_GROUP_HASH_CAP: u16 = 32;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KeyScheme {
    /// The key is the value.
    #[default]
    Raw,
    /// Long values are keyed by a prefix and a hash, at most `cap` to a
    /// group.
    PrefixHash { prefix_len: u8, cap: u16 },
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

fn hashed_key(group: &[u8], hash: &[u8; KEY_HASH_LEN]) -> Vec<u8> {
    let mut key = Vec::with_capacity(group.len() + 1 + KEY_HASH_LEN);
    key.extend_from_slice(group);
    key.push(HASHED_KEY_MARKER);
    key.extend_from_slice(hash);
    key
}

impl KeyScheme {
    /// The scheme a new tree of `dict` is written in.
    pub fn for_new_tree(dict: DictKind) -> Self {
        match dict {
            DictKind::StringReverse => Self::PrefixHash {
                prefix_len: DEFAULT_STRING_KEY_PREFIX,
                cap: DEFAULT_GROUP_HASH_CAP,
            },
            _ => Self::Raw,
        }
    }

    /// The bytes a group shares, `None` for a raw tree.
    pub(crate) fn prefix_len(self) -> Option<usize> {
        match self {
            Self::Raw => None,
            Self::PrefixHash { prefix_len, .. } => Some(prefix_len as usize),
        }
    }

    /// Hashed values a group may hold.
    pub fn cap(self) -> usize {
        match self {
            Self::Raw => 0,
            Self::PrefixHash { cap, .. } => cap as usize,
        }
    }

    /// A long value's group: its first `prefix_len` bytes.
    pub fn group(self, value: &[u8]) -> Option<&[u8]> {
        let k = self.prefix_len()?;
        (value.len() > k + 1 + KEY_HASH_LEN).then(|| &value[..k])
    }

    /// The group of a hashed key; `None` for a value key.
    pub fn hashed_key_group(self, key: &[u8]) -> Option<&[u8]> {
        let k = self.prefix_len()?;
        (key.len() == k + 1 + KEY_HASH_LEN && key[k] == HASHED_KEY_MARKER).then(|| &key[..k])
    }

    /// A long value's hashed key.
    pub fn hashed_key(self, value: &[u8]) -> Vec<u8> {
        let group = self.group(value).expect("only a long value hashes");
        debug_assert!(
            !group.contains(&HASHED_KEY_MARKER),
            "reverse-tree values are UTF-8"
        );
        hashed_key(group, &digest(value))
    }

    /// The key range `[start, end)` of every key starting with `group`:
    /// its values of any length, by value or hashed.
    pub fn group_range(self, group: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
        (group.to_vec(), prefix_end(group))
    }

    /// The group whose key range ([`Self::group_range`]) holds `key`, by
    /// value or hashed: its first `prefix_len` bytes, if it has that many.
    /// A value of exactly `prefix_len` bytes is no member of its group, but
    /// its key spells the group's prefix and lies in its range.
    pub fn key_group(self, key: &[u8]) -> Option<&[u8]> {
        let k = self.prefix_len()?;
        (key.len() >= k).then(|| &key[..k])
    }

    /// The key range `[start, end)` of a group's hashed values.
    pub fn hashed_range(self, group: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
        let mut start = group.to_vec();
        start.push(HASHED_KEY_MARKER);
        let end = prefix_end(&start);
        (start, end)
    }

    /// Whether this scheme may order keys differently from their values.
    pub fn reorders(self) -> bool {
        self != Self::Raw
    }
}

/// A group whose long values the tree keys by value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawGroup {
    pub prefix: Vec<u8>,
    /// Whether it also holds hashed values, from before it filled.
    pub hashed: bool,
}

/// The groups a tree keys by value, sorted by prefix.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RawGroups(Vec<RawGroup>);

impl RawGroups {
    /// From groups sorted by prefix, each listed once.
    pub fn from_sorted(groups: Vec<RawGroup>) -> io::Result<Self> {
        if groups.windows(2).any(|w| w[0].prefix >= w[1].prefix) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raw key groups are not sorted",
            ));
        }
        Ok(Self(groups))
    }

    pub fn get(&self, prefix: &[u8]) -> Option<&RawGroup> {
        self.0
            .binary_search_by(|g| g.prefix.as_slice().cmp(prefix))
            .ok()
            .map(|i| &self.0[i])
    }

    /// List `prefix`; `hashed` if it may already hold hashed values.
    pub fn insert(&mut self, prefix: &[u8], hashed: bool) {
        match self.0.binary_search_by(|g| g.prefix.as_slice().cmp(prefix)) {
            Ok(i) => self.0[i].hashed |= hashed,
            Err(i) => self.0.insert(
                i,
                RawGroup {
                    prefix: prefix.to_vec(),
                    hashed,
                },
            ),
        }
    }

    /// Stop listing `prefix`.
    pub fn remove(&mut self, prefix: &[u8]) {
        if let Ok(i) = self.0.binary_search_by(|g| g.prefix.as_slice().cmp(prefix)) {
            self.0.remove(i);
        }
    }

    /// Record that `prefix`, listed, holds no hashed value.
    pub fn clear_hashed(&mut self, prefix: &[u8]) {
        if let Ok(i) = self.0.binary_search_by(|g| g.prefix.as_slice().cmp(prefix)) {
            self.0[i].hashed = false;
        }
    }

    pub fn iter(&self) -> std::slice::Iter<'_, RawGroup> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// How one tree keys values: its scheme and the groups it keys by value.
#[derive(Clone, Copy, Debug)]
pub struct KeyRules<'a> {
    pub scheme: KeyScheme,
    pub raw: &'a RawGroups,
}

/// The keys a value may be held under, in the order to try them.
#[derive(Debug, PartialEq, Eq)]
pub struct LookupKeys<'v> {
    pub first: Cow<'v, [u8]>,
    /// The value, when its group filled after it began hashing, so it may
    /// be held under its hashed key instead.
    fallback: Option<(KeyScheme, &'v [u8])>,
}

impl LookupKeys<'_> {
    /// Whether the value may also be held under [`Self::then`].
    pub fn has_then(&self) -> bool {
        self.fallback.is_some()
    }

    /// The key to try when `first` misses: the hashed key of a value whose
    /// group filled after it began hashing. Hashed on asking, so a hit on
    /// `first` never hashes the value.
    pub fn then(&self) -> Option<Vec<u8>> {
        self.fallback
            .map(|(scheme, value)| scheme.hashed_key(value))
    }
}

impl KeyRules<'_> {
    /// The key a new `value` is written under.
    pub fn key<'v>(&self, value: &'v [u8]) -> Cow<'v, [u8]> {
        match self.scheme.group(value) {
            Some(group) if self.raw.get(group).is_none() => {
                Cow::Owned(self.scheme.hashed_key(value))
            }
            _ => Cow::Borrowed(value),
        }
    }

    /// The hash a new `value` is keyed by, if it is keyed by one.
    pub fn key_hash(&self, value: &[u8]) -> Option<[u8; KEY_HASH_LEN]> {
        let group = self.scheme.group(value)?;
        self.raw.get(group).is_none().then(|| digest(value))
    }

    /// The keys `value` may be held under.
    pub fn lookup_keys<'v>(&self, value: &'v [u8]) -> LookupKeys<'v> {
        let Some(group) = self.scheme.group(value) else {
            return LookupKeys {
                first: Cow::Borrowed(value),
                fallback: None,
            };
        };
        match self.raw.get(group) {
            None => LookupKeys {
                first: Cow::Owned(self.scheme.hashed_key(value)),
                fallback: None,
            },
            Some(raw) => LookupKeys {
                first: Cow::Borrowed(value),
                fallback: raw.hashed.then_some((self.scheme, value)),
            },
        }
    }

    /// The key ranges that hold every value starting with `prefix`.
    pub fn prefix_ranges(&self, prefix: &[u8]) -> Vec<PrefixRange> {
        let Some(k) = self.scheme.prefix_len() else {
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
        let group = &prefix[..k];
        if self.raw.get(group).is_some_and(|g| !g.hashed) {
            return vec![exact];
        }
        let (start, end) = self.scheme.hashed_range(group);
        vec![
            exact,
            PrefixRange {
                start,
                end,
                check: true,
            },
        ]
    }
}

/// The groups holding more long values than the cap, from values arriving
/// in byte order.
pub struct GroupCounter {
    scheme: KeyScheme,
    group: Vec<u8>,
    count: usize,
    over: Vec<Vec<u8>>,
}

impl GroupCounter {
    pub fn new(scheme: KeyScheme) -> Self {
        Self {
            scheme,
            group: Vec::new(),
            count: 0,
            over: Vec::new(),
        }
    }

    pub fn push(&mut self, value: &[u8]) {
        let Some(group) = self.scheme.group(value) else {
            return;
        };
        if self.count > 0 && group == self.group.as_slice() {
            self.count += 1;
            return;
        }
        self.close();
        self.group.clear();
        self.group.extend_from_slice(group);
        self.count = 1;
    }

    fn close(&mut self) {
        if self.count > self.scheme.cap() {
            self.over.push(std::mem::take(&mut self.group));
        }
        self.count = 0;
    }

    /// The groups over the cap, in byte order.
    pub fn finish(mut self) -> Vec<Vec<u8>> {
        self.close();
        self.over
    }
}

/// A hashed value as a run holds it: its hash and id.
type Hashed = ([u8; KEY_HASH_LEN], u64);

/// Settled entries, in key order.
enum Segment<V> {
    Key(V, u64),
    /// A closed run's hashed values, sorted.
    Held {
        group: Vec<u8>,
        entries: std::vec::IntoIter<Hashed>,
    },
}

/// Puts `(value, id)` pairs arriving in value order into key order.
///
/// Only values sharing their first `prefix_len` bytes can change order, and
/// those arrive together as a run. A run's value-keyed values sort before
/// its hashed ones and keep their order, so they pass straight through; its
/// hashed values are held, as hash and id, until the run ends, then
/// released sorted by hash. A group not in `raw` may hold at most the cap,
/// so a run holds no more.
///
/// `V` is how a value is held until it is released: a slice of a buffer
/// that outlives the order costs no copy.
pub struct KeyOrder<V = Vec<u8>> {
    scheme: KeyScheme,
    raw: RawGroups,
    run_prefix: Option<Vec<u8>>,
    held: Vec<Hashed>,
    settled: VecDeque<Segment<V>>,
}

impl<V: AsRef<[u8]>> KeyOrder<V> {
    /// An order keying the groups in `raw` by value and hashing the rest.
    pub fn new(scheme: KeyScheme, raw: RawGroups) -> Self {
        Self {
            scheme,
            raw,
            run_prefix: None,
            held: Vec::new(),
            settled: VecDeque::new(),
        }
    }

    /// Add the next value; values must arrive in ascending byte order.
    pub fn push(&mut self, value: V, id: u64) -> io::Result<()> {
        self.push_hashed(value, id, None)
    }

    /// [`Self::push`], with the value's hash when it is keyed by one
    /// ([`KeyRules::key_hash`]), computed ahead; `None` computes it here.
    pub fn push_hashed(
        &mut self,
        value: V,
        id: u64,
        hash: Option<[u8; KEY_HASH_LEN]>,
    ) -> io::Result<()> {
        let bytes = value.as_ref();
        let Some(k) = self.scheme.prefix_len() else {
            self.settled.push_back(Segment::Key(value, id));
            return Ok(());
        };
        if bytes.len() <= k {
            self.close_run();
            self.settled.push_back(Segment::Key(value, id));
            return Ok(());
        }
        if self.run_prefix.as_deref() != Some(&bytes[..k]) {
            self.close_run();
            self.run_prefix = Some(bytes[..k].to_vec());
        }
        match self.scheme.group(bytes) {
            Some(group) if self.raw.get(group).is_none() => {
                if self.held.len() >= self.scheme.cap() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "a hashed key group holds more values than its cap",
                    ));
                }
                let hash = hash.unwrap_or_else(|| digest(bytes));
                self.held.push((hash, id));
            }
            _ => self.settled.push_back(Segment::Key(value, id)),
        }
        Ok(())
    }

    /// No more values: settle whatever is held.
    pub fn finish(&mut self) {
        self.close_run();
    }

    /// The next settled key, written into `key`, and its id.
    pub fn pop_into(&mut self, key: &mut Vec<u8>) -> Option<u64> {
        loop {
            match self.settled.front_mut()? {
                Segment::Key(value, id) => {
                    let id = *id;
                    key.clear();
                    key.extend_from_slice(value.as_ref());
                    self.settled.pop_front();
                    return Some(id);
                }
                Segment::Held { group, entries } => {
                    if let Some((hash, id)) = entries.next() {
                        key.clear();
                        key.extend_from_slice(group);
                        key.push(HASHED_KEY_MARKER);
                        key.extend_from_slice(&hash);
                        return Some(id);
                    }
                }
            }
            self.settled.pop_front();
        }
    }

    /// The next `(key, id)` in key order, once it is settled.
    pub fn pop(&mut self) -> Option<(Vec<u8>, u64)>
    where
        V: Into<Vec<u8>>,
    {
        loop {
            let next = match self.settled.front_mut()? {
                Segment::Key(..) => match self.settled.pop_front() {
                    Some(Segment::Key(key, id)) => return Some((key.into(), id)),
                    _ => unreachable!("the front is a key"),
                },
                Segment::Held { group, entries } => entries
                    .next()
                    .map(|(hash, id)| (hashed_key(group, &hash), id)),
            };
            match next {
                Some(entry) => return Some(entry),
                None => {
                    self.settled.pop_front();
                }
            }
        }
    }

    fn close_run(&mut self) {
        let Some(group) = self.run_prefix.take() else {
            return;
        };
        self.held.sort_unstable();
        let entries = std::mem::take(&mut self.held).into_iter();
        self.settled.push_back(Segment::Held { group, entries });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEME: KeyScheme = KeyScheme::PrefixHash {
        prefix_len: 8,
        cap: 4,
    };

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

    fn over_cap(values: &[Vec<u8>]) -> RawGroups {
        let mut counter = GroupCounter::new(SCHEME);
        for v in values {
            counter.push(v);
        }
        let groups = counter
            .finish()
            .into_iter()
            .map(|prefix| RawGroup {
                prefix,
                hashed: false,
            })
            .collect();
        RawGroups::from_sorted(groups).unwrap()
    }

    fn drain(order: &mut KeyOrder<&[u8]>, out: &mut Vec<(Vec<u8>, u64)>) {
        let mut key = Vec::new();
        while let Some(id) = order.pop_into(&mut key) {
            out.push((key.clone(), id));
        }
    }

    #[test]
    fn long_values_are_keyed_by_prefix_marker_and_hash() {
        let none = RawGroups::default();
        let rules = KeyRules {
            scheme: SCHEME,
            raw: &none,
        };
        let limit = 8 + 1 + KEY_HASH_LEN;
        let short = vec![b'x'; limit];
        assert!(matches!(rules.key(&short), Cow::Borrowed(k) if k == short));
        let long = vec![b'x'; limit + 1];
        let key = rules.key(&long);
        assert_eq!(key.len(), limit);
        assert_eq!(&key[..8], &long[..8]);
        assert_eq!(key[8], HASHED_KEY_MARKER);
        assert_eq!(&key[9..], Sha256::digest(&long).as_slice());
        assert_eq!(SCHEME.hashed_key_group(&key), Some(&long[..8]));
        assert_eq!(SCHEME.hashed_key_group(&long), None);
        let raw = KeyRules {
            scheme: KeyScheme::Raw,
            raw: &none,
        };
        assert_eq!(raw.key(&long), Cow::Borrowed(long.as_slice()));
    }

    /// A listed group's values are keyed by value; a lookup tries the value
    /// key, then the hashed key only where the group began hashing.
    #[test]
    fn a_listed_group_is_keyed_and_looked_up_by_value() {
        let long = |c: u8| -> Vec<u8> {
            let mut v = vec![c; 8];
            v.extend_from_slice(&[b'z'; 40]);
            v
        };
        let (pure, mixed, hashed) = (long(b'p'), long(b'm'), long(b'h'));
        let mut raw = RawGroups::default();
        raw.insert(&mixed[..8], true);
        raw.insert(&pure[..8], false);
        raw.insert(&pure[..8], false);
        assert_eq!(raw.len(), 2);
        let rules = KeyRules {
            scheme: SCHEME,
            raw: &raw,
        };

        assert_eq!(rules.key(&pure), Cow::Borrowed(pure.as_slice()));
        assert_eq!(rules.key(&mixed), Cow::Borrowed(mixed.as_slice()));
        assert_eq!(rules.key(&hashed).into_owned(), SCHEME.hashed_key(&hashed));

        let probe = |v: &[u8]| {
            let keys = rules.lookup_keys(v);
            let then = keys.then();
            (keys.first.into_owned(), then)
        };
        assert_eq!(probe(&pure), (pure.clone(), None));
        assert_eq!(
            probe(&mixed),
            (mixed.clone(), Some(SCHEME.hashed_key(&mixed)))
        );
        assert_eq!(probe(&hashed), (SCHEME.hashed_key(&hashed), None));

        // A group listed as holding no hashed value has no range to check.
        assert_eq!(rules.prefix_ranges(&pure[..9]).len(), 1);
        assert_eq!(rules.prefix_ranges(&mixed[..9]).len(), 2);
        assert_eq!(rules.prefix_ranges(&hashed[..9]).len(), 2);
    }

    /// Every prefix's ranges hold exactly the keys of the values starting
    /// with it, once the checked ranges' values are checked: with every
    /// group hashed, with full groups keyed by value, and with full groups
    /// whose earliest values kept their hashed keys.
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
        let full = over_cap(&values);
        assert!(!full.is_empty());
        let mut mixed = RawGroups::default();
        for g in full.iter() {
            mixed.insert(&g.prefix, true);
        }
        let none = RawGroups::default();
        let trees: [(KeyScheme, &RawGroups); 4] = [
            (SCHEME, &none),
            (SCHEME, &full),
            (SCHEME, &mixed),
            (KeyScheme::Raw, &none),
        ];
        for (scheme, raw) in trees {
            let rules = KeyRules { scheme, raw };
            let mut seen_in_group = std::collections::HashMap::<Vec<u8>, usize>::new();
            let keyed: Vec<(Vec<u8>, &Vec<u8>)> = values
                .iter()
                .map(|v| {
                    // A mixed group's first two values kept their hashed keys.
                    let mixed_group = scheme
                        .group(v)
                        .filter(|g| raw.get(g).is_some_and(|r| r.hashed));
                    if let Some(g) = mixed_group {
                        let seen = seen_in_group.entry(g.to_vec()).or_default();
                        *seen += 1;
                        if *seen <= 2 {
                            return (scheme.hashed_key(v), v);
                        }
                    }
                    (rules.key(v).into_owned(), v)
                })
                .collect();
            for prefix in &prefixes {
                let mut found: Vec<&Vec<u8>> = Vec::new();
                for range in rules.prefix_ranges(prefix) {
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
                assert_eq!(found, want, "{scheme:?} {raw:?} {prefix:?}");
            }
        }
    }

    /// Values pushed in byte order come out in key order, each once, with
    /// the over-cap groups keyed by value and no run holding past the cap.
    #[test]
    fn key_order_turns_value_order_into_key_order() {
        let values = values();
        for (scheme, raw) in [
            (SCHEME, over_cap(&values)),
            (KeyScheme::Raw, RawGroups::default()),
        ] {
            let rules = KeyRules { scheme, raw: &raw };
            let mut want: Vec<(Vec<u8>, u64)> = values
                .iter()
                .enumerate()
                .map(|(id, v)| (rules.key(v).into_owned(), id as u64))
                .collect();
            want.sort();

            let mut order = KeyOrder::new(scheme, raw.clone());
            let mut out = Vec::new();
            for (id, v) in values.iter().enumerate() {
                // Every other value with its hash computed ahead, as the
                // full build does.
                if id % 2 == 0 {
                    order
                        .push_hashed(v.as_slice(), id as u64, rules.key_hash(v))
                        .unwrap();
                } else {
                    order.push(v.as_slice(), id as u64).unwrap();
                }
                assert!(order.held.len() <= scheme.cap());
                drain(&mut order, &mut out);
            }
            order.finish();
            drain(&mut order, &mut out);
            assert_eq!(out, want, "{scheme:?}");
        }
    }

    /// A group past the cap that is not listed is refused rather than held.
    #[test]
    fn an_unlisted_group_past_the_cap_is_refused() {
        let mut order = KeyOrder::new(SCHEME, RawGroups::default());
        let values: Vec<String> = (0..5).map(|i| format!("abcdefgh{i:040}")).collect();
        for (id, v) in values.iter().enumerate().take(4) {
            order.push(v.as_bytes(), id as u64).unwrap();
        }
        assert!(order.push(values[4].as_bytes(), 4).is_err());
    }

    #[test]
    fn the_counter_finds_the_groups_over_the_cap() {
        let mut values: Vec<String> = Vec::new();
        values.extend((0..5).map(|i| format!("aaaaaaaa{i:040}")));
        values.extend((0..4).map(|i| format!("bbbbbbbb{i:040}")));
        // Short values between a group's long ones neither count nor split it.
        values.extend((0..9).map(|i| format!("cccccccc{i}")));
        values.extend((0..5).map(|i| format!("cccccccc{i}{}", "x".repeat(40))));
        values.sort();
        let mut counter = GroupCounter::new(SCHEME);
        for v in &values {
            counter.push(v.as_bytes());
        }
        assert_eq!(
            counter.finish(),
            vec![b"aaaaaaaa".to_vec(), b"cccccccc".to_vec()]
        );
    }

    #[test]
    fn only_string_trees_hash() {
        assert_eq!(
            KeyScheme::for_new_tree(DictKind::StringReverse),
            KeyScheme::PrefixHash {
                prefix_len: DEFAULT_STRING_KEY_PREFIX,
                cap: DEFAULT_GROUP_HASH_CAP,
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
