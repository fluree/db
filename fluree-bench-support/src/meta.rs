//! Per-scenario corpus identity and phase timings.
//!
//! Criterion measures one number per scenario: wall-clock. Two other facts
//! decide whether that number *means* anything, and neither is recoverable
//! after the fact:
//!
//! - **What was measured.** A conversion bench reads a corpus off disk. Compare
//!   a run over `dblp.ttl` against a baseline captured over `dblp.nt` and the
//!   gate reports a 40% "regression" that is really a different file. This is
//!   the most expensive silent failure the baseline gate has, because the output
//!   looks exactly like a real result.
//! - **Where the time went.** A pipeline that gets 5% slower overall while its
//!   parse share moves from 60% to 75% has a parse regression masked by a write
//!   improvement. Absolute per-phase nanoseconds don't survive a machine change;
//!   the *share* does, which makes share drift the one per-phase signal that
//!   still gates across host classes.
//!
//! Both travel the same route as [`crate::mem`]: the bench records them into a
//! JSON sidecar under `target/fluree-bench-meta/<bench>.json`, keyed by the same
//! `<group>/<function>/<scale>` ID criterion uses, and `bench-baseline capture`
//! merges them into the captured [`crate::baseline::BaselineFile`].
//!
//! ```no_run
//! use fluree_bench_support::meta::{CorpusInfo, ScenarioMeta};
//!
//! let corpus = CorpusInfo::from_path("corpora/dblp.ttl", "turtle", "nquads", 8)?
//!     .with_element_count(561_500_000);
//! let meta = ScenarioMeta::new()
//!     .with_corpus(corpus)
//!     .with_phase_ns([("read", 1.2e9), ("parse", 8.0e9), ("write", 2.8e9)]);
//! fluree_bench_support::meta::record_scenario("convert", "convert/ttl_to_nq/small", &meta);
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Hashing is SHA-256 (`sha2`, already a workspace dependency). BLAKE3 would be
//! faster, but it is not in the tree and a corpus is hashed once per capture —
//! the strategy note's preference for "no new deps" wins over the throughput
//! difference.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Minimum share movement, in percentage points, that the default gate treats
/// as drift. Overridable per compare run (`--share-drift-pp`).
pub const DEFAULT_SHARE_DRIFT_PP: f64 = 5.0;

/// Identity of the input a scenario measured.
///
/// Every field is part of "is this the same measurement?" except
/// `byte_len`/`element_count`, which are implied by the hash and kept for
/// human-readable reports (throughput needs a denominator).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorpusInfo {
    /// Human name — a file name, a generator spec (`bsbm:pc=1000`), whatever
    /// identifies the input to a reader. Not part of the identity check: two
    /// paths to identical bytes are the same corpus.
    pub name: String,
    /// Lowercase hex SHA-256 of the input bytes.
    pub sha256: String,
    pub byte_len: u64,
    /// Triples/quads/rows in the input, when the bench knows it. `0` = unknown.
    #[serde(default)]
    pub element_count: u64,
    /// Syntax read, e.g. `"turtle"`, `"ntriples"`, `"jsonld"`.
    pub input_syntax: String,
    /// Syntax written, e.g. `"nquads"`. `"none"` for parse-only scenarios.
    pub output_syntax: String,
    /// Worker threads the scenario ran with. A K=8 number is not comparable to
    /// a K=16 baseline, so this is part of the identity.
    pub thread_count: u32,
}

impl CorpusInfo {
    /// Hash `bytes` and describe them as a corpus.
    pub fn from_bytes(
        name: impl Into<String>,
        bytes: &[u8],
        input_syntax: impl Into<String>,
        output_syntax: impl Into<String>,
        thread_count: u32,
    ) -> Self {
        Self {
            name: name.into(),
            sha256: sha256_hex(bytes),
            byte_len: bytes.len() as u64,
            element_count: 0,
            input_syntax: input_syntax.into(),
            output_syntax: output_syntax.into(),
            thread_count,
        }
    }

    /// Hash a file on disk, streaming so a multi-GB corpus doesn't have to be
    /// resident. `name` defaults to the path's file name.
    pub fn from_path(
        path: impl AsRef<Path>,
        input_syntax: impl Into<String>,
        output_syntax: impl Into<String>,
        thread_count: u32,
    ) -> std::io::Result<Self> {
        use std::io::Read;
        let path = path.as_ref();
        let mut file = std::fs::File::open(path)?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 1 << 20];
        let mut byte_len = 0u64;
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            byte_len += n as u64;
        }
        Ok(Self {
            name: path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| path.display().to_string()),
            sha256: hex_encode(&hasher.finalize()),
            byte_len,
            element_count: 0,
            input_syntax: input_syntax.into(),
            output_syntax: output_syntax.into(),
            thread_count,
        })
    }

    pub fn with_element_count(mut self, count: u64) -> Self {
        self.element_count = count;
        self
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Reject a corpus block whose hash could not have been written by this
    /// crate. A malformed `sha256` cannot establish identity — comparing
    /// against it would assert "same input" on the strength of a field nothing
    /// validated — so the caller must refuse rather than proceed.
    pub fn validate(&self) -> Result<(), String> {
        if !is_well_formed_sha256(&self.sha256) {
            return Err(format!(
                "corpus '{}' has a malformed sha256 ({:?}): expected 64 lowercase hex digits, got \
                 {} character(s). A hash that cannot be parsed cannot establish corpus identity.",
                self.name,
                short_hash(&self.sha256),
                self.sha256.chars().count(),
            ));
        }
        Ok(())
    }

    /// Describe how this corpus differs from `other` in a way that makes the
    /// two measurements incomparable, or `None` when they are the same
    /// measurement. Order the checks cheapest-signal-first so the message names
    /// the *cause* a reader can act on (a syntax swap reads very differently
    /// from a corpus refresh, even though both change the hash).
    pub fn identity_mismatch(&self, other: &Self) -> Option<String> {
        if self.input_syntax != other.input_syntax || self.output_syntax != other.output_syntax {
            return Some(format!(
                "syntax: baseline {}→{}, current {}→{}",
                self.input_syntax, self.output_syntax, other.input_syntax, other.output_syntax
            ));
        }
        if self.thread_count != other.thread_count {
            return Some(format!(
                "thread_count: baseline {}, current {}",
                self.thread_count, other.thread_count
            ));
        }
        if self.sha256 != other.sha256 {
            return Some(format!(
                "sha256: baseline {} ({}, {} bytes), current {} ({}, {} bytes)",
                short_hash(&self.sha256),
                self.name,
                self.byte_len,
                short_hash(&other.sha256),
                other.name,
                other.byte_len,
            ));
        }
        None
    }
}

/// One phase of a pipeline: how long it took, and what fraction of the
/// scenario's measured phases it was.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PhaseTiming {
    pub ns: f64,
    /// `ns / Σns × 100` across the scenario's measured phases.
    ///
    /// [`ScenarioMeta::with_phase_ns`] computes this from the nanoseconds, so
    /// anything built through that constructor is self-consistent. That is a
    /// property of the constructor, **not an invariant of the type**: both
    /// fields are public, and a `PhaseTiming` deserialized from a baseline file
    /// is untrusted input that can say whatever the file says. `compare`
    /// therefore reports a scenario whose shares don't sum to 100% instead of
    /// assuming they do.
    pub share_pct: f64,
}

/// Corpus identity plus phase timings for one scenario.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScenarioMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub corpus: Option<CorpusInfo>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub phases: BTreeMap<String, PhaseTiming>,
}

impl ScenarioMeta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_corpus(mut self, corpus: CorpusInfo) -> Self {
        self.corpus = Some(corpus);
        self
    }

    /// Record phases from their raw nanosecond costs, deriving each share from
    /// the total. Phases need not cover the whole scenario — shares are of the
    /// *measured* phases, which is what makes derived-by-subtraction phases
    /// (per the `cypher_phase_profile.rs` model) safe to include.
    ///
    /// **Merges** into any phases already recorded, and recomputes every share
    /// over the combined total, so a bench that reports its phases in more than
    /// one call still ends up with one consistent set. A repeated phase name
    /// overwrites its previous nanoseconds. Replacing instead of merging would
    /// silently discard the earlier call's phases while leaving the shares
    /// looking perfectly well-formed — the failure would be invisible in the
    /// output.
    ///
    /// A zero total leaves every share at 0.0 rather than dividing by zero.
    pub fn with_phase_ns<I, S>(mut self, phases: I) -> Self
    where
        I: IntoIterator<Item = (S, f64)>,
        S: Into<String>,
    {
        for (name, ns) in phases {
            self.phases
                .insert(name.into(), PhaseTiming { ns, share_pct: 0.0 });
        }
        let total: f64 = self.phases.values().map(|p| p.ns).sum();
        for timing in self.phases.values_mut() {
            timing.share_pct = if total > 0.0 {
                timing.ns / total * 100.0
            } else {
                0.0
            };
        }
        self
    }

    pub fn is_empty(&self) -> bool {
        self.corpus.is_none() && self.phases.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Sidecar IO — same shape as `crate::mem`, different directory.
// ---------------------------------------------------------------------------

/// Sidecar directory under the cargo target dir.
pub fn sidecar_dir() -> PathBuf {
    crate::budget::workspace_root()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("target")
        .join("fluree-bench-meta")
}

/// Record meta for one scenario of `bench`, merging into the bench's sidecar
/// file. `scenario_id` must match the criterion ID path —
/// `"<group>/<function>/<scale>"` — so baseline capture can join the rows.
///
/// Best-effort, like [`crate::mem::record_scenario`]: a bench must not fail
/// because a sidecar write did.
pub fn record_scenario(bench: &str, scenario_id: &str, meta: &ScenarioMeta) {
    let dir = sidecar_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("  [bench-meta] create {}: {e}", dir.display());
        return;
    }
    let path = dir.join(format!("{bench}.json"));
    let mut entries = read_sidecar(&path).unwrap_or_default();
    entries.insert(scenario_id.to_string(), meta.clone());
    match serde_json::to_string_pretty(&entries) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&path, json) {
                eprintln!("  [bench-meta] write {}: {e}", path.display());
            }
        }
        Err(e) => eprintln!("  [bench-meta] serialize {}: {e}", path.display()),
    }
}

/// Read one sidecar file. Returns `None` when missing or unparsable.
pub fn read_sidecar(path: &Path) -> Option<BTreeMap<String, ScenarioMeta>> {
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

/// Read every sidecar in [`sidecar_dir`] (or the given dir), merged into a
/// single scenario-ID → meta map. Used by `bench-baseline capture`.
pub fn read_all_sidecars(dir: &Path) -> BTreeMap<String, ScenarioMeta> {
    let mut merged = BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return merged;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            if let Some(map) = read_sidecar(&path) {
                merged.extend(map);
            }
        }
    }
    merged
}

/// Remove all sidecars so a fresh bench run starts clean. Stale meta from an
/// earlier run would otherwise be merged into a new baseline — and stale
/// *corpus* meta is worse than stale timings, since it would assert the wrong
/// input identity for a run that never read that input.
pub fn clear_sidecars(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("json") {
            let _ = std::fs::remove_file(&path);
        }
    }
}

// ---------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------

/// Lowercase hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// First 12 characters, for report lines. Never used for comparison.
///
/// Slices on a CHARACTER boundary, not a byte offset. A baseline file is
/// untrusted input, so a `sha256` field can hold anything — and this is called
/// from the refusal path, where a panic would turn a documented exit-2 refusal
/// into an exit-101 crash. [`CorpusInfo::validate`] rejects such a hash outright;
/// this is the second line of defence for the report line that names it.
pub(crate) fn short_hash(hash: &str) -> &str {
    match hash.char_indices().nth(12) {
        Some((byte_idx, _)) => &hash[..byte_idx],
        None => hash,
    }
}

/// A SHA-256 as this crate writes it: exactly 64 lowercase ASCII hex digits.
fn is_well_formed_sha256(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn corpus() -> CorpusInfo {
        CorpusInfo::from_bytes("dblp.ttl", b"<a> <b> <c> .\n", "turtle", "nquads", 8)
    }

    #[test]
    fn sha256_matches_known_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn from_path_matches_from_bytes() {
        let dir = std::env::temp_dir().join(format!(
            "fluree-bench-meta-hash-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("corpus.ttl");
        let bytes = b"<a> <b> <c> .\n".repeat(100_000);
        std::fs::write(&path, &bytes).unwrap();

        let from_path = CorpusInfo::from_path(&path, "turtle", "nquads", 8).unwrap();
        let from_bytes = CorpusInfo::from_bytes("corpus.ttl", &bytes, "turtle", "nquads", 8);
        // Streams across many 1MiB reads — the chunk boundary must not change
        // the digest.
        assert!(from_path.byte_len > (1 << 20));
        assert_eq!(from_path, from_bytes);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn identical_corpora_do_not_mismatch() {
        assert!(corpus().identity_mismatch(&corpus()).is_none());
    }

    #[test]
    fn differing_bytes_mismatch_on_hash() {
        let other = CorpusInfo::from_bytes("dblp.ttl", b"<a> <b> <d> .\n", "turtle", "nquads", 8);
        let msg = corpus().identity_mismatch(&other).unwrap();
        assert!(msg.starts_with("sha256:"), "{msg}");
    }

    #[test]
    fn differing_syntax_mismatches_before_hash() {
        let mut other = corpus();
        other.input_syntax = "ntriples".into();
        let msg = corpus().identity_mismatch(&other).unwrap();
        assert!(msg.starts_with("syntax:"), "{msg}");
    }

    #[test]
    fn differing_thread_count_mismatches() {
        let mut other = corpus();
        other.thread_count = 16;
        let msg = corpus().identity_mismatch(&other).unwrap();
        assert!(msg.starts_with("thread_count:"), "{msg}");
    }

    #[test]
    fn name_and_element_count_are_not_identity() {
        let other = corpus().with_name("renamed.ttl").with_element_count(1);
        assert!(corpus().identity_mismatch(&other).is_none());
    }

    #[test]
    fn shares_are_derived_from_ns_and_sum_to_100() {
        let meta = ScenarioMeta::new().with_phase_ns([("read", 1.0e9), ("parse", 3.0e9)]);
        assert_eq!(meta.phases["read"].share_pct, 25.0);
        assert_eq!(meta.phases["parse"].share_pct, 75.0);
        let total: f64 = meta.phases.values().map(|p| p.share_pct).sum();
        assert!((total - 100.0).abs() < 1e-9);
    }

    #[test]
    fn with_phase_ns_merges_and_recomputes_shares_over_the_union() {
        // A bench that reports phases in two calls must end up with one
        // consistent set. Replacing would drop `read` while leaving the
        // remaining shares looking perfectly well-formed — invisible in output.
        let meta = ScenarioMeta::new()
            .with_phase_ns([("read", 1.0e9)])
            .with_phase_ns([("parse", 3.0e9)]);
        assert_eq!(meta.phases.len(), 2);
        assert_eq!(meta.phases["read"].share_pct, 25.0);
        assert_eq!(meta.phases["parse"].share_pct, 75.0);
    }

    #[test]
    fn with_phase_ns_overwrites_a_repeated_phase_name() {
        let meta = ScenarioMeta::new()
            .with_phase_ns([("parse", 1.0e9), ("write", 1.0e9)])
            .with_phase_ns([("parse", 3.0e9)]);
        assert_eq!(meta.phases["parse"].ns, 3.0e9);
        assert_eq!(meta.phases["parse"].share_pct, 75.0);
        assert_eq!(meta.phases["write"].share_pct, 25.0);
    }

    #[test]
    fn zero_total_phases_do_not_divide_by_zero() {
        let meta = ScenarioMeta::new().with_phase_ns([("read", 0.0), ("parse", 0.0)]);
        assert!(meta.phases.values().all(|p| p.share_pct == 0.0));
    }

    #[test]
    fn sidecar_round_trip() {
        let dir = std::env::temp_dir().join(format!(
            "fluree-bench-meta-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("convert.json");

        let mut entries: BTreeMap<String, ScenarioMeta> = BTreeMap::new();
        entries.insert(
            "convert/ttl_to_nq/small".into(),
            ScenarioMeta::new()
                .with_corpus(corpus())
                .with_phase_ns([("parse", 2.0e9)]),
        );
        std::fs::write(&path, serde_json::to_string(&entries).unwrap()).unwrap();

        let read = read_sidecar(&path).unwrap();
        assert_eq!(
            read["convert/ttl_to_nq/small"].corpus.as_ref().unwrap(),
            &corpus()
        );
        assert_eq!(read_all_sidecars(&dir).len(), 1);

        clear_sidecars(&dir);
        assert!(read_all_sidecars(&dir).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// bplatz review item 7. `short_hash` sliced at byte 12, which panics when
    /// byte 12 is not a UTF-8 boundary — turning a documented exit-2 refusal
    /// into an exit-101 crash on untrusted input.
    #[test]
    fn short_hash_never_panics_on_multibyte_input() {
        for s in [
            "üüüüüüüüüüüüüüüü",
            "日本語のハッシュ値です",
            "",
            "abc",
            "🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥",
        ] {
            let out = short_hash(s);
            assert!(s.starts_with(out), "{s:?} -> {out:?}");
            assert!(out.chars().count() <= 12);
        }
    }

    #[test]
    fn validate_accepts_a_real_sha256() {
        assert!(corpus().validate().is_ok());
        assert_eq!(corpus().sha256.len(), 64);
    }

    #[test]
    fn validate_rejects_malformed_hashes() {
        let cases = [
            ("", "empty"),
            ("abc", "too short"),
            (&"a".repeat(63), "63 chars"),
            (&"a".repeat(65), "65 chars"),
            (&"A".repeat(64), "uppercase"),
            (&"g".repeat(64), "non-hex letter"),
            (&"ü".repeat(64), "multibyte"),
        ];
        for (hash, why) in cases {
            let mut c = corpus();
            c.sha256 = hash.to_string();
            assert!(c.validate().is_err(), "should reject {why}: {hash:?}");
        }
    }

    #[test]
    fn empty_meta_serializes_without_null_fields() {
        let json = serde_json::to_string(&ScenarioMeta::new()).unwrap();
        assert_eq!(json, "{}");
    }
}
