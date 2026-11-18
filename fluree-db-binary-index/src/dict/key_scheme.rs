//! How a reverse tree turns a value into its key.
//!
//! A [`KeyScheme::Raw`] tree keys a value by its bytes. A
//! [`KeyScheme::PrefixHash`] tree does too, except for a value longer than
//! `prefix_len + 1 + KEY_HASH_LEN` bytes, which it keys by its first
//! `prefix_len` bytes, a [`HASHED_KEY_MARKER`] byte and the first
//! [`KEY_HASH_LEN`] bytes of the value's SHA-256. Long values then cost a
//! fixed-size key instead of a second copy of themselves.
//!
//! Values are UTF-8, which never contains `0xFF`, so a key with the marker
//! at `prefix_len` can only be a hashed one, and a key keeps its value's
//! first `prefix_len` bytes either way: every value starting with a prefix
//! of at most `prefix_len` bytes lies in that prefix's key range. A longer
//! prefix's hashed candidates share only the first `prefix_len` bytes and
//! must be checked against their values.
//!
//! Equal keys for different values would take a SHA-256 collision; the
//! tree writers refuse equal keys rather than replace one with the other.

use std::borrow::Cow;
use std::collections::VecDeque;

use fluree_db_core::DictKind;
use sha2::{Digest, Sha256};

/// Hash bytes in a hashed key.
pub const KEY_HASH_LEN: usize = 16;

/// The byte between a hashed key's prefix and its hash.
pub const HASHED_KEY_MARKER: u8 = 0xFF;

/// Prefix a new string tree keeps before the hash.
pub const DEFAULT_STRING_KEY_PREFIX: u8 = 48;

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
        key.extend_from_slice(&Sha256::digest(value)[..KEY_HASH_LEN]);
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

/// Puts `(value, id)` pairs arriving in value order into key order.
///
/// Only values sharing their first `prefix_len` bytes can change order, and
/// those arrive together, so each such run is held until the next value
/// leaves it, then released sorted by key.
pub struct KeyOrder {
    scheme: KeyScheme,
    run_prefix: Vec<u8>,
    held: Vec<(Vec<u8>, u64)>,
    ready: VecDeque<(Vec<u8>, u64)>,
}

impl KeyOrder {
    pub fn new(scheme: KeyScheme) -> Self {
        Self {
            scheme,
            run_prefix: Vec::new(),
            held: Vec::new(),
            ready: VecDeque::new(),
        }
    }

    /// Add the next value; values must arrive in ascending byte order.
    pub fn push(&mut self, value: &[u8], id: u64) {
        let key = self.scheme.key(value).into_owned();
        match self.scheme.prefix_len() {
            Some(k) if value.len() > k => {
                if self.held.is_empty() || value[..k] != self.run_prefix[..] {
                    self.release();
                    self.run_prefix.clear();
                    self.run_prefix.extend_from_slice(&value[..k]);
                }
                self.held.push((key, id));
            }
            _ => {
                self.release();
                self.ready.push_back((key, id));
            }
        }
    }

    /// No more values: release whatever is held.
    pub fn finish(&mut self) {
        self.release();
    }

    /// The next `(key, id)` in key order, once it is settled.
    pub fn pop(&mut self) -> Option<(Vec<u8>, u64)> {
        self.ready.pop_front()
    }

    fn release(&mut self) {
        self.held.sort_by(|a, b| a.0.cmp(&b.0));
        self.ready.extend(self.held.drain(..));
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
            for tail in 0..40usize {
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
        assert_ne!(SCHEME.key(&[b'x'; 40]), SCHEME.key(&[b'x'; 41]));
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
                order.push(v, id as u64);
                while let Some(e) = order.pop() {
                    out.push(e);
                }
            }
            order.finish();
            while let Some(e) = order.pop() {
                out.push(e);
            }
            let mut want: Vec<(Vec<u8>, u64)> = values
                .iter()
                .enumerate()
                .map(|(id, v)| (scheme.key(v).into_owned(), id as u64))
                .collect();
            want.sort();
            assert_eq!(out, want, "{scheme:?}");
        }
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
