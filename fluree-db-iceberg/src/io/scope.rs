//! Which storage identity a cached object was read under.
//!
//! Every cache keyed by an object's location (parsed table metadata, the scan
//! file list, the `COUNT(*)` manifest stats, the Parquet data-file cache and its
//! single-flight fill) must also key on the identity that read it. Otherwise
//! bytes fetched under one source's identity are handed to another source that
//! names the same path, without that source's identity ever being asked: a
//! cache hit reads no storage.
//!
//! A [`StorageScope`] is a stable, non-secret fingerprint of that identity: its
//! kind, its principal, the endpoint it reads through, and the instance's
//! identity context. The configuration every install had before scopes existed
//! (the host's ambient AWS chain, no endpoint override, the default identity
//! context) is the literal [`StorageScope::shared`], so the keys of caches
//! written then are unchanged and stay warm.

use std::borrow::Cow;
use std::sync::Arc;

use sha2::{Digest, Sha256};

/// The scope every cache key had before scopes existed.
const SHARED: &str = "shared";

/// The identity a storage read was made under, for cache keys.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct StorageScope {
    /// The fingerprint (`shared`, or the identity's description).
    repr: Arc<str>,
    /// A filename-safe digest of `repr` (`shared` for the shared scope).
    segment: Arc<str>,
}

/// Redacting `Debug`: a scope's description carries digests of the stored
/// auth it was built from, so only whether it is the shared scope is shown.
impl std::fmt::Debug for StorageScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let shown = if self.is_shared() { SHARED } else { "<scoped>" };
        f.debug_tuple("StorageScope").field(&shown).finish()
    }
}

impl StorageScope {
    /// The scope of the host's ambient AWS identity with no endpoint override,
    /// under the default identity context.
    pub fn shared() -> Self {
        Self {
            repr: Arc::from(SHARED),
            segment: Arc::from(SHARED),
        }
    }

    /// The scope of an identity described by `parts` (kind, principal,
    /// endpoint, identity context, …), each non-secret. The order is part of
    /// the key.
    pub fn of(parts: &[(&str, &str)]) -> Self {
        let mut repr = String::from("v1");
        for (name, value) in parts {
            repr.push('|');
            repr.push_str(name);
            repr.push('=');
            repr.push_str(value);
        }
        // 128 bits of SHA-256: the description is partly chosen by whoever
        // configures a source (a catalog URI, a principal), so a digest an
        // adversary could collide would let one source's files be named as
        // another's in the data-file cache, which keeps no key of its own.
        let digest = Sha256::digest(repr.as_bytes());
        let segment: String = digest[..16].iter().map(|b| format!("{b:02x}")).collect();
        Self {
            repr: Arc::from(repr),
            segment: Arc::from(format!("s{segment}")),
        }
    }

    /// Whether this is [`StorageScope::shared`].
    pub fn is_shared(&self) -> bool {
        &*self.repr == SHARED
    }

    /// `key` qualified by this scope, for in-memory caches and verified
    /// on-disk keys: `key` itself in the shared scope (so existing entries are
    /// still found), else the scope and `key` joined.
    pub fn qualify<'a>(&self, key: &'a str) -> Cow<'a, str> {
        if self.is_shared() {
            Cow::Borrowed(key)
        } else {
            Cow::Owned(format!("{}\u{1f}{key}", self.repr))
        }
    }

    /// A filename-safe segment naming this scope (`shared`, or `s` and 32 hex
    /// digits).
    pub fn file_segment(&self) -> &str {
        &self.segment
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shared_scope_leaves_keys_unchanged() {
        let shared = StorageScope::shared();
        assert!(shared.is_shared());
        assert_eq!(shared.qualify("s3://b/m.json"), "s3://b/m.json");
        assert_eq!(shared.file_segment(), "shared");
    }

    #[test]
    fn distinct_identities_get_distinct_keys_and_segments() {
        let a = StorageScope::of(&[("kind", "google"), ("principal", "a@p.iam")]);
        let b = StorageScope::of(&[("kind", "google"), ("principal", "b@p.iam")]);
        assert!(!a.is_shared());
        assert_ne!(a, b);
        assert_ne!(a.qualify("gs://b/k"), b.qualify("gs://b/k"));
        assert_ne!(a.file_segment(), b.file_segment());
        assert_eq!(a.file_segment().len(), 33);
        assert!(a.file_segment().bytes().all(|c| c.is_ascii_alphanumeric()));
        // Stable: the same description is the same scope.
        assert_eq!(
            a,
            StorageScope::of(&[("kind", "google"), ("principal", "a@p.iam")])
        );
    }

    #[test]
    fn debug_shows_no_part_of_the_description() {
        let scoped = StorageScope::of(&[("auth", "d1g3st"), ("principal", "a@p.iam")]);
        let shown = format!("{scoped:?}");
        assert_eq!(shown, "StorageScope(\"<scoped>\")");
        assert_eq!(
            format!("{:?}", StorageScope::shared()),
            "StorageScope(\"shared\")"
        );
    }

    #[test]
    fn a_description_never_reads_as_the_shared_scope() {
        assert!(!StorageScope::of(&[]).is_shared());
        assert!(!StorageScope::of(&[("kind", "shared")]).is_shared());
    }
}
