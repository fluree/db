//! Document-scoped skolemization keys for bulk import.
//!
//! RDF scopes a blank-node label to the document that contains it. Bulk import
//! cuts a document into many chunks and commits each separately, so the
//! skolemization key cannot be derived from the commit — it has to name the
//! *document*. This module defines the key format and is the only place that
//! builds one.
//!
//! # Format
//!
//! ```text
//! fdb-d<13 lowercase base36 digits>-<label>
//!     ^ ^                          ^
//!     | |                          `- the label as written in the source
//!     | `- xxh64(namespace ‖ 0x00 ‖ doc_key), zero-padded to a fixed width
//!     `- the document-scope marker
//! ```
//!
//! Everything after `fdb-` up to the first `-` is the **document scope**: a
//! [`doc_scope`] rendering of [`doc_id`]. The whole label is what
//! `_:fdb-…` renders as in query results.
//!
//! ## Why a hash and not the ledger id
//!
//! The obvious key — `{ledger_id}-{document}` — embeds a ledger id, which
//! contains `/` and `:`. Neither is legal inside a SPARQL `BLANK_NODE_LABEL`,
//! so ids minted that way could be read out of a query but never written back
//! into a SPARQL `DELETE`/`INSERT`. Hashing produces `[0-9a-z]` only, which is
//! writable in every surface Fluree exposes (SPARQL, Turtle, JSON-LD) with no
//! escaping.
//!
//! The ledger id does not disappear — it becomes the hash **namespace**, so two
//! ledgers importing the same file still mint different ids. Callers that
//! deliberately want them to agree pass an explicit shared namespace.
//!
//! ## Why a fixed width
//!
//! 13 base36 digits hold any `u64` (`36^13 > 2^64`), so the rendering is
//! lossless and the document scope is always exactly [`DOC_SCOPE_LEN`] bytes.
//! Fixed width is what makes the scope recoverable from a minted id: the first
//! [`DOC_SCOPE_LEN`] bytes of the local name are the scope, and the rest —
//! after one `-` — is the source label, however many `-` it contains itself.
//! [`split_doc_scope`] does that, and the import manifest in the `txn-meta`
//! graph maps the scope back to the file it came from.
//!
//! ## Disjointness from the other `fdb-` minters
//!
//! Three subsystems mint `fdb-` labels, and they must not collide:
//!
//! | minter | shape | first `-` after `fdb-` |
//! |---|---|---|
//! | bulk import (here) | `d` + 13 base36 + `-` + label | offset 14 |
//! | staged transaction | `{nanos:x}` + `-` + solution + `-` + label | offset 16 |
//! | SPARQL `BNODE()` | hyphenated UUIDv4 | offset 8 |
//!
//! The offsets are structural, not coincidental: `BNODE()` emits RFC-4122 text,
//! whose first hyphen is always at offset 8; the staged-transaction key is
//! nanoseconds-since-epoch in hex, which is 16 digits for every wall clock
//! between 2004-11 and 2154-07. A staged key could only reach offset 14 on a
//! host whose clock reads within about four days of 1970-01-01.

use xxhash_rust::xxh64::Xxh64;

/// Marker character that opens an import document scope, immediately after the
/// `fdb-` stable-label prefix.
pub const DOC_SCOPE_MARKER: char = 'd';

/// Number of base36 digits in a rendered document scope.
///
/// `36^13 ≈ 1.7e20 > u64::MAX`, so 13 digits render any `u64` losslessly.
pub const DOC_SCOPE_DIGITS: usize = 13;

/// Byte length of a rendered document scope (`DOC_SCOPE_MARKER` + digits).
pub const DOC_SCOPE_LEN: usize = 1 + DOC_SCOPE_DIGITS;

/// Byte separating the namespace from the document key in the hash input.
///
/// A key cannot contain NUL (paths and object addresses are UTF-8 text), so no
/// `(namespace, doc_key)` pair can be re-cut at a different boundary to produce
/// the same hash input. Without it, namespace `"a"` + key `"b/c"` and namespace
/// `"a/b"` + key `"c"` would hash identically.
const NAMESPACE_SEPARATOR: u8 = 0;

const BASE36: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

/// Hash a document key into its import-stable document id.
///
/// `namespace` is the ledger id by default (see the module docs); `doc_key`
/// identifies the document within the import — a path relative to the import
/// root, a remote address relative to its listing prefix, or a file's basename.
///
/// Pure and allocation-free: parse workers on different threads derive the same
/// id for the same document with no coordination.
#[must_use]
pub fn doc_id(namespace: &str, doc_key: &str) -> u64 {
    let mut hasher = Xxh64::new(0);
    hasher.update(namespace.as_bytes());
    hasher.update(&[NAMESPACE_SEPARATOR]);
    hasher.update(doc_key.as_bytes());
    hasher.digest()
}

/// Render a document id as its fixed-width document scope.
#[must_use]
pub fn doc_scope(id: u64) -> String {
    let mut buf = [b'0'; DOC_SCOPE_LEN];
    buf[0] = DOC_SCOPE_MARKER as u8;
    let mut rest = id;
    let mut i = DOC_SCOPE_LEN;
    while rest > 0 {
        i -= 1;
        buf[i] = BASE36[(rest % 36) as usize];
        rest /= 36;
    }
    // Every byte is ASCII by construction.
    String::from_utf8(buf.to_vec()).expect("base36 rendering is ASCII")
}

/// Build the skolem base for a document: [`doc_id`] then [`doc_scope`].
///
/// The result is the `{base}` in `fdb-{base}-{label}`.
#[must_use]
pub fn skolem_base(namespace: &str, doc_key: &str) -> String {
    doc_scope(doc_id(namespace, doc_key))
}

/// Split a stable blank-node local name minted by bulk import into its
/// `(document scope, source label)` halves.
///
/// `local` is the name *after* `_:` — e.g. `fdb-d0000000000abc-shared`, which
/// splits into `("d0000000000abc", "shared")`. Returns `None` for any local
/// name not minted by this module (a staged-transaction key, a `BNODE()` uuid,
/// or an id minted before this format existed).
#[must_use]
pub fn split_doc_scope(local: &str) -> Option<(&str, &str)> {
    let rest = local.strip_prefix(crate::ns_encoding::STABLE_BLANK_NODE_LABEL_PREFIX)?;
    let (scope, label) = rest.split_at_checked(DOC_SCOPE_LEN)?;
    let mut chars = scope.chars();
    if chars.next() != Some(DOC_SCOPE_MARKER)
        || !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    {
        return None;
    }
    Some((scope, label.strip_prefix('-')?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_scope_is_fixed_width_lowercase_base36() {
        for id in [0u64, 1, 35, 36, u64::MAX, 0x0123_4567_89ab_cdef] {
            let rendered = doc_scope(id);
            assert_eq!(rendered.len(), DOC_SCOPE_LEN, "{id}");
            assert!(rendered.starts_with(DOC_SCOPE_MARKER), "{rendered}");
            assert!(
                rendered[1..]
                    .bytes()
                    .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase()),
                "{rendered}"
            );
        }
    }

    #[test]
    fn doc_scope_renders_u64_max_losslessly() {
        // If 13 digits were not enough, u64::MAX would wrap onto some smaller
        // id's rendering. Decode and compare.
        let rendered = doc_scope(u64::MAX);
        let decoded = rendered[1..].bytes().fold(0u128, |acc, b| {
            acc * 36 + u128::from(BASE36.iter().position(|&c| c == b).unwrap() as u32)
        });
        assert_eq!(decoded, u128::from(u64::MAX));
    }

    // The whole point of hashing: a minted id must be writable as a SPARQL
    // BLANK_NODE_LABEL, which admits neither `/` nor `:`.
    #[test]
    fn skolem_base_is_free_of_characters_sparql_forbids() {
        let base = skolem_base("my/ledger:main", "sub dir/data (1).ttl");
        assert!(
            base.bytes()
                .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase()),
            "{base}"
        );
    }

    #[test]
    fn namespace_separates_ledger_from_document() {
        // Without the NUL, ("a", "b/c") and ("a/b", "c") would collide.
        assert_ne!(doc_id("a", "b/c"), doc_id("a/b", "c"));
    }

    #[test]
    fn different_ledgers_mint_different_scopes_for_one_file() {
        assert_ne!(
            skolem_base("one:main", "data.ttl"),
            skolem_base("two:main", "data.ttl")
        );
    }

    #[test]
    fn split_doc_scope_recovers_labels_containing_hyphens() {
        let base = skolem_base("l:main", "data.ttl");
        let local = format!("fdb-{base}-my-label-with-hyphens");
        assert_eq!(
            split_doc_scope(&local),
            Some((base.as_str(), "my-label-with-hyphens"))
        );
    }

    #[test]
    fn split_doc_scope_rejects_the_other_minters() {
        // Staged transaction: `fdb-{nanos:x}-{solution}-{label}`.
        assert_eq!(split_doc_scope("fdb-1857f4a2b9c3d0e1-0-b0"), None);
        // SPARQL BNODE(): `fdb-{uuid}`.
        assert_eq!(
            split_doc_scope("fdb-67e55044-10b1-426f-9247-bb680e5fe0c8"),
            None
        );
        // Pre-C2 import id: `fdb-{ledger}:{branch}-{t}-{label}`.
        assert_eq!(split_doc_scope("fdb-lubm:main-1-genid10"), None);
    }

    // Structural disjointness from the other two `fdb-` minters, expressed as
    // the offset of the first `-` after the `fdb-` prefix (see module docs).
    #[test]
    fn first_hyphen_offset_distinguishes_the_minters() {
        let import = skolem_base("l:main", "data.ttl");
        assert_eq!(import.find('-'), None, "a scope contains no hyphen");
        assert_eq!(import.len(), 14, "so the first hyphen lands at offset 14");

        let staged = format!("{:x}", 1_750_000_000_000_000_000u64);
        assert_eq!(staged.len(), 16, "nanos-since-epoch is 16 hex digits");

        let bnode = "67e55044-10b1-426f-9247-bb680e5fe0c8";
        assert_eq!(bnode.find('-'), Some(8), "uuid hyphen is at offset 8");
    }
}
