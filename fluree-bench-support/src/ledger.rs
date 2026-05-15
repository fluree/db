//! Atomic ledger-alias generation.
//!
//! Replaces the per-bench-file `LEDGER_COUNTER: AtomicU64` pattern that was
//! duplicated in `insert_formats.rs`, `vector_query.rs`, and
//! `fulltext_query.rs` before this chassis existed. Every bench iteration
//! that calls `Fluree::create_ledger(&alias)` needs a fresh, never-reused
//! alias; this helper supplies one.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use fluree_bench_support::next_ledger_alias;
//!
//! let alias = next_ledger_alias("jld");
//! // -> "bench/jld-0:main", "bench/jld-1:main", ...
//! ```
//!
//! The shape `bench/{prefix}-{n}:main` matches what existing benches were
//! producing by hand. The `:main` suffix is the default branch name in
//! Fluree; benches that need a different branch can construct their own
//! alias from the counter value via [`next_ledger_id()`].

use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Atomically increment the bench-wide ledger counter and return the value.
///
/// Useful when a bench needs to construct a custom alias (e.g., a different
/// branch name, or a non-`bench/` prefix).
pub fn next_ledger_id() -> u64 {
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Build a unique ledger alias of the form `bench/{prefix}-{n}:main`.
///
/// The atomic counter is shared across all benches in the same process, so
/// aliases never collide even when criterion runs multiple bench groups
/// concurrently.
///
/// Panics if `prefix` contains `/` or `:` (those characters partition the
/// alias namespace and would corrupt the resulting ledger ID).
pub fn next_ledger_alias(prefix: &str) -> String {
    assert!(
        !prefix.contains('/') && !prefix.contains(':'),
        "ledger alias prefix must not contain '/' or ':'; got {prefix:?}"
    );
    let n = next_ledger_id();
    format!("bench/{prefix}-{n}:main")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_are_unique() {
        let a = next_ledger_alias("test_unique");
        let b = next_ledger_alias("test_unique");
        assert_ne!(a, b);
        assert!(a.starts_with("bench/test_unique-"));
        assert!(a.ends_with(":main"));
    }

    #[test]
    fn ids_increment_monotonically() {
        let a = next_ledger_id();
        let b = next_ledger_id();
        assert!(b > a);
    }

    #[test]
    #[should_panic(expected = "must not contain")]
    fn rejects_slash_in_prefix() {
        let _ = next_ledger_alias("bad/prefix");
    }

    #[test]
    #[should_panic(expected = "must not contain")]
    fn rejects_colon_in_prefix() {
        let _ = next_ledger_alias("bad:prefix");
    }
}
