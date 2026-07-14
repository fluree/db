//! Blessed baselines: a per-query **expected** correctness oracle (from the
//! native target) and a per-target **perf** reference, plus the pure comparison
//! core behind `vbench compare`.
//!
//! Layout under `baselines/`:
//! - `expected/<qid>.json` — one file per query: the canonical result hash, row
//!   count, and first-20 canonical rows, blessed from the **native** run. This is
//!   the oracle a virtual run's result is checked against. Queries the manifest
//!   declares are *expected to error on virtual* (`expected_status.virtual =
//!   error`, i.e. q043/q044) get **no** expected file — there is no correct
//!   result to compare a (correctly) erroring virtual query against, so a missing
//!   expected file is the intended state, not an omission.
//! - `perf/<target>.json` — one file per target: per-query hot median wall,
//!   optional cold wall, and the pathway counters, with run provenance.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::budgets::EffectiveBudget;
use crate::corpus::{Corpus, ExpectedOutcome, HashGate};
use crate::schema::{Counters, RunMeta, RunRecord, Status};

pub const EXPECTED_SCHEMA_VERSION: u32 = 1;
pub const PERF_SCHEMA_VERSION: u32 = 1;

/// Provenance stamped on a blessed artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlessedFrom {
    pub target: String,
    pub run_id: String,
    pub commit: String,
}

/// The correctness oracle for one query (blessed from native).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedEntry {
    pub schema_version: u32,
    pub query_id: String,
    pub result_hash: String,
    pub rows: usize,
    pub head_rows: Vec<String>,
    pub blessed_from: BlessedFrom,
}

/// One query's perf reference on one target.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerfEntry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hot_wall_ms_median: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cold_wall_ms: Option<u64>,
    pub counters: Counters,
}

/// A target's perf reference, keyed by query id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfBaseline {
    pub schema_version: u32,
    pub target: String,
    pub blessed_from: BlessedFrom,
    pub entries: BTreeMap<String, PerfEntry>,
}

fn expected_dir(baselines: &Path) -> PathBuf {
    baselines.join("expected")
}
fn perf_dir(baselines: &Path) -> PathBuf {
    baselines.join("perf")
}

/// Native target ids in a run's meta.
fn native_target_ids(meta: &RunMeta) -> Vec<String> {
    meta.targets
        .iter()
        .filter(|t| t.kind == "native")
        .map(|t| t.id.clone())
        .collect()
}

/// Outcome of a `baseline --expected` bless pass.
#[derive(Debug, Default)]
pub struct BlessSummary {
    /// Oracles written (new, plus any force-overwrites).
    pub written: Vec<String>,
    /// Subset of `written` that replaced a *differing* oracle (`--force` only).
    pub overwritten: Vec<String>,
    /// Existing oracles this run reproduced exactly — left untouched (original
    /// provenance kept, diff-free re-bless).
    pub unchanged: Vec<String>,
    /// Queries deliberately omitted (expected-to-error-on-virtual) or unusable
    /// (non-ok native status).
    pub skipped: Vec<String>,
}

/// An about-to-be-blessed oracle that differs from what is on disk.
struct OracleDelta {
    query_id: String,
    old_rows: usize,
    new_rows: usize,
    old_hash: String,
    new_hash: String,
}

impl std::fmt::Display for OracleDelta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn short(h: &str) -> &str {
            &h[..h.len().min(12)]
        }
        write!(
            f,
            "{}: rows {} → {}, hash {}… → {}…",
            self.query_id,
            self.old_rows,
            self.new_rows,
            short(&self.old_hash),
            short(&self.new_hash)
        )
    }
}

/// Bless per-query expected oracles from the native records of a run.
///
/// The oracles are the corpus's ground truth, so re-blessing is guarded: an
/// existing oracle whose hash/rows differ from this run is **refused** (with
/// the delta printed, nothing written) unless `force` is set — otherwise a
/// native correctness regression that still exits ok would be silently
/// blessed as the new truth. An oracle this run reproduces exactly is left
/// untouched; `RowsOnly`-gated queries (nondeterministic selection) compare
/// row count only, since their hash legitimately varies between native runs.
pub fn write_expected(
    meta: &RunMeta,
    records: &[RunRecord],
    corpus: &Corpus,
    baselines: &Path,
    force: bool,
) -> Result<BlessSummary> {
    let dir = expected_dir(baselines);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let natives = native_target_ids(meta);
    if natives.is_empty() {
        anyhow::bail!("run has no native target — cannot bless expected oracles");
    }

    // Pass 1: classify everything before writing anything, so a refusal
    // leaves the blessed state exactly as it was (no partial bless).
    let mut summary = BlessSummary::default();
    let mut to_write: Vec<(ExpectedEntry, bool)> = Vec::new(); // (entry, replaces_existing)
    let mut refused: Vec<OracleDelta> = Vec::new();
    for r in records.iter().filter(|r| natives.contains(&r.target)) {
        // A query the manifest expects to *error on virtual* has no cross-target
        // result to compare against, so we deliberately write no expected file.
        if let Some(q) = corpus.get(&r.query_id) {
            if q.expected_status.for_target(true) == ExpectedOutcome::Error {
                summary
                    .skipped
                    .push(format!("{} (expected virtual error)", r.query_id));
                continue;
            }
        }
        if r.status != Status::Ok {
            summary
                .skipped
                .push(format!("{} (native status {:?})", r.query_id, r.status));
            continue;
        }
        let entry = ExpectedEntry {
            schema_version: EXPECTED_SCHEMA_VERSION,
            query_id: r.query_id.clone(),
            result_hash: r.result_hash.clone(),
            rows: r.rows,
            head_rows: r.heads.clone().unwrap_or_default(),
            blessed_from: BlessedFrom {
                target: r.target.clone(),
                run_id: meta.run_id.clone(),
                commit: meta.git_commit.clone(),
            },
        };
        match load_expected(baselines, &r.query_id)? {
            None => to_write.push((entry, false)),
            Some(existing) => {
                let gate = corpus
                    .get(&r.query_id)
                    .map(|q| q.hash_gate)
                    .unwrap_or_default();
                let same = match gate {
                    HashGate::Full => {
                        existing.result_hash == entry.result_hash && existing.rows == entry.rows
                    }
                    HashGate::RowsOnly => existing.rows == entry.rows,
                };
                if same {
                    summary.unchanged.push(r.query_id.clone());
                } else if force {
                    to_write.push((entry, true));
                } else {
                    refused.push(OracleDelta {
                        query_id: r.query_id.clone(),
                        old_rows: existing.rows,
                        new_rows: entry.rows,
                        old_hash: existing.result_hash.clone(),
                        new_hash: entry.result_hash.clone(),
                    });
                }
            }
        }
    }
    if !refused.is_empty() {
        let mut msg = format!(
            "refusing to overwrite {} blessed oracle(s) that differ from this run (nothing written):\n",
            refused.len()
        );
        for d in &refused {
            msg.push_str(&format!("  {d}\n"));
        }
        msg.push_str(
            "if the change is intended (e.g. an amended query), re-bless with --force; \
             if not, this run may carry a native correctness regression — investigate \
             before blessing",
        );
        anyhow::bail!(msg);
    }

    // Pass 2: write.
    for (entry, replaces) in to_write {
        let path = dir.join(format!("{}.json", entry.query_id));
        std::fs::write(&path, serde_json::to_string_pretty(&entry)?)
            .with_context(|| format!("writing {}", path.display()))?;
        if replaces {
            summary.overwritten.push(entry.query_id.clone());
        }
        summary.written.push(entry.query_id);
    }
    Ok(summary)
}

/// Bless per-target perf references. Merges into any existing `perf/<target>.json`
/// so a hot run and a later cold run can both populate a query's entry.
pub fn write_perf(meta: &RunMeta, records: &[RunRecord], baselines: &Path) -> Result<Vec<String>> {
    let dir = perf_dir(baselines);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;

    let mut by_target: BTreeMap<&str, Vec<&RunRecord>> = BTreeMap::new();
    for r in records {
        by_target.entry(r.target.as_str()).or_default().push(r);
    }

    let mut written = Vec::new();
    for (target, recs) in by_target {
        let mut baseline = load_perf(baselines, target)?.unwrap_or_else(|| PerfBaseline {
            schema_version: PERF_SCHEMA_VERSION,
            target: target.to_string(),
            blessed_from: BlessedFrom {
                target: target.to_string(),
                run_id: meta.run_id.clone(),
                commit: meta.git_commit.clone(),
            },
            entries: BTreeMap::new(),
        });
        baseline.blessed_from.run_id = meta.run_id.clone();
        baseline.blessed_from.commit = meta.git_commit.clone();
        for r in recs {
            let entry = baseline.entries.entry(r.query_id.clone()).or_default();
            if r.cache_state == "cold" {
                entry.cold_wall_ms = Some(r.wall_ms);
            } else {
                entry.hot_wall_ms_median = Some(r.wall_ms);
            }
            // Prefer the hot record's counters; fall back to whatever we have.
            if r.cache_state != "cold" || entry.counters == Counters::default() {
                entry.counters = r.counters.clone();
            }
        }
        let path = dir.join(format!("{target}.json"));
        std::fs::write(&path, serde_json::to_string_pretty(&baseline)?)
            .with_context(|| format!("writing {}", path.display()))?;
        written.push(path.display().to_string());
    }
    Ok(written)
}

pub fn load_expected(baselines: &Path, query_id: &str) -> Result<Option<ExpectedEntry>> {
    let path = expected_dir(baselines).join(format!("{query_id}.json"));
    if !path.exists() {
        return Ok(None);
    }
    let raw =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    Ok(Some(
        serde_json::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?,
    ))
}

pub fn load_perf(baselines: &Path, target: &str) -> Result<Option<PerfBaseline>> {
    let path = perf_dir(baselines).join(format!("{target}.json"));
    if !path.exists() {
        return Ok(None);
    }
    let raw =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    Ok(Some(
        serde_json::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?,
    ))
}

/// Correctness verdict for one record vs its expected oracle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum HashCheck {
    Pass,
    /// No expected file — unblessed, or a deliberately-omitted virtual-error query.
    NoExpected,
    /// The record is a non-ok status, so there's no hash to compare.
    NotApplicable,
    /// A `Full`-gated query whose result hash changed.
    Mismatch {
        expected: String,
        observed: String,
    },
    /// A `RowsOnly`-gated query (nondeterministic-selection LIMIT) whose row
    /// count changed — the hash is deliberately not compared for these.
    RowsMismatch {
        expected: usize,
        observed: usize,
    },
}

impl HashCheck {
    pub fn is_fail(&self) -> bool {
        matches!(self, Self::Mismatch { .. } | Self::RowsMismatch { .. })
    }
}

/// Perf verdict for one record vs its perf baseline.
#[derive(Debug, Clone, Serialize)]
pub struct PerfCheck {
    pub baseline_ms: u64,
    pub observed_ms: u64,
    pub ratio: f64,
    pub budget: Option<EffectiveBudget>,
    pub violated: bool,
}

/// One record's full comparison.
#[derive(Debug, Clone, Serialize)]
pub struct CompareOutcome {
    pub query_id: String,
    pub target: String,
    pub cache_state: String,
    pub hash: HashCheck,
    pub perf: Option<PerfCheck>,
    /// Set when the perf verdict was confirmed by an in-process auto-rerun.
    pub reran: bool,
}

/// Whether an observed wall exceeds the baseline by more than the budget's
/// percent **and** by at least its `min_delta_ms` absolute floor (so a percent
/// of a tiny baseline can't gate on scheduler noise). The single source of
/// truth for a perf verdict — used both by `compare_one` and by the compare
/// auto-rerun re-evaluation.
pub fn over_budget(baseline_ms: u64, observed_ms: u64, budget: EffectiveBudget) -> bool {
    let ratio = if baseline_ms == 0 {
        1.0
    } else {
        observed_ms as f64 / baseline_ms as f64
    };
    (ratio - 1.0) * 100.0 > budget.pct
        && observed_ms.saturating_sub(baseline_ms) >= budget.min_delta_ms
}

/// Pure comparison of one record against its blessed oracle + perf entry, under a
/// budget. `hash_gate` selects the correctness check: `Full` compares the result
/// hash; `RowsOnly` (a nondeterministic-selection LIMIT — any k rows are valid)
/// compares only the row count. `budget == None` means advisory (cold): a
/// ratio is reported but `violated` is always false.
pub fn compare_one(
    record: &RunRecord,
    expected: Option<&ExpectedEntry>,
    perf: Option<&PerfEntry>,
    budget: Option<EffectiveBudget>,
    hash_gate: HashGate,
) -> CompareOutcome {
    // Correctness.
    let hash = match expected {
        None => HashCheck::NoExpected,
        Some(_) if record.status != Status::Ok => HashCheck::NotApplicable,
        Some(e) => match hash_gate {
            HashGate::RowsOnly => {
                if e.rows == record.rows {
                    HashCheck::Pass
                } else {
                    HashCheck::RowsMismatch {
                        expected: e.rows,
                        observed: record.rows,
                    }
                }
            }
            HashGate::Full if e.result_hash == record.result_hash => HashCheck::Pass,
            HashGate::Full => HashCheck::Mismatch {
                expected: e.result_hash.clone(),
                observed: record.result_hash.clone(),
            },
        },
    };

    // Perf: pick the baseline wall matching this record's cache state.
    let cold = record.cache_state == "cold";
    let baseline_ms = perf.and_then(|p| {
        if cold {
            p.cold_wall_ms
        } else {
            p.hot_wall_ms_median
        }
    });
    let perf_check = baseline_ms.map(|base| {
        let ratio = if base == 0 {
            1.0
        } else {
            record.wall_ms as f64 / base as f64
        };
        let violated = match budget {
            Some(b) => over_budget(base, record.wall_ms, b),
            None => false, // advisory
        };
        PerfCheck {
            baseline_ms: base,
            observed_ms: record.wall_ms,
            ratio,
            budget,
            violated,
        }
    });

    CompareOutcome {
        query_id: record.query_id.clone(),
        target: record.target.clone(),
        cache_state: record.cache_state.clone(),
        hash,
        perf: perf_check,
        reran: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(query: &str, target: &str, cache: &str, hash: &str, wall: u64) -> RunRecord {
        RunRecord {
            query_id: query.into(),
            target: target.into(),
            cache_state: cache.into(),
            rep: 0,
            reps: 1,
            wall_ms: wall,
            all_walls_ms: vec![wall],
            status: Status::Ok,
            rows: 1,
            result_hash: hash.into(),
            counters: Counters::default(),
            spans_missing: vec![],
            error: None,
            heads: None,
        }
    }

    /// A 20% budget with no absolute floor — the pure-percent behavior most
    /// tests exercise; the floor has its own test.
    fn pct20() -> EffectiveBudget {
        EffectiveBudget {
            pct: 20.0,
            min_delta_ms: 0,
        }
    }

    fn expected(query: &str, hash: &str) -> ExpectedEntry {
        ExpectedEntry {
            schema_version: 1,
            query_id: query.into(),
            result_hash: hash.into(),
            rows: 1,
            head_rows: vec![],
            blessed_from: BlessedFrom {
                target: "native-sf01".into(),
                run_id: "R".into(),
                commit: "abc".into(),
            },
        }
    }

    #[test]
    fn compare_passes_on_match_and_within_budget() {
        let r = rec("q001", "virtual-sf01", "hot", "H", 110);
        let e = expected("q001", "H");
        let p = PerfEntry {
            hot_wall_ms_median: Some(100),
            cold_wall_ms: None,
            counters: Counters::default(),
        };
        let o = compare_one(&r, Some(&e), Some(&p), Some(pct20()), HashGate::Full);
        assert_eq!(o.hash, HashCheck::Pass);
        assert!(!o.perf.unwrap().violated, "110 vs 100 is within 20%");
    }

    #[test]
    fn compare_flags_hash_mismatch_and_perf_violation() {
        let r = rec("q001", "virtual-sf01", "hot", "OTHER", 200);
        let e = expected("q001", "H");
        let p = PerfEntry {
            hot_wall_ms_median: Some(100),
            cold_wall_ms: None,
            counters: Counters::default(),
        };
        let o = compare_one(&r, Some(&e), Some(&p), Some(pct20()), HashGate::Full);
        assert!(o.hash.is_fail());
        assert!(
            o.perf.unwrap().violated,
            "200 vs 100 is 100% over a 20% budget"
        );
    }

    #[test]
    fn rows_only_gate_ignores_hash_and_checks_rows() {
        // A rows_only query: different hash but same row count → PASS.
        let mut r = rec("q029", "virtual-sf01", "hot", "DIFFERENT_HASH", 100);
        r.rows = 100;
        let mut e = expected("q029", "NATIVE_HASH");
        e.rows = 100;
        let o = compare_one(&r, Some(&e), None, Some(pct20()), HashGate::RowsOnly);
        assert_eq!(
            o.hash,
            HashCheck::Pass,
            "rows_only ignores the hash difference"
        );
        // Same query, wrong row count → RowsMismatch (a fail).
        r.rows = 99;
        let o = compare_one(&r, Some(&e), None, Some(pct20()), HashGate::RowsOnly);
        assert!(matches!(o.hash, HashCheck::RowsMismatch { .. }));
        assert!(o.hash.is_fail());
    }

    #[test]
    fn cold_is_advisory_never_violates() {
        let r = rec("q001", "virtual-sf01", "cold", "H", 9999);
        let e = expected("q001", "H");
        let p = PerfEntry {
            hot_wall_ms_median: Some(100),
            cold_wall_ms: Some(1000),
            counters: Counters::default(),
        };
        // budget None == advisory.
        let o = compare_one(&r, Some(&e), Some(&p), None, HashGate::Full);
        let perf = o.perf.unwrap();
        assert_eq!(
            perf.baseline_ms, 1000,
            "cold compares against cold baseline"
        );
        assert!(!perf.violated, "cold is advisory");
    }

    #[test]
    fn no_expected_file_is_not_a_fail() {
        let r = rec("q043", "virtual-sf01", "hot", "H", 100);
        let o = compare_one(&r, None, None, Some(pct20()), HashGate::Full);
        assert_eq!(o.hash, HashCheck::NoExpected);
        assert!(!o.hash.is_fail());
    }

    /// The absolute floor: a percent-over record whose absolute slowdown is
    /// under `min_delta_ms` must not violate (10% of 8 ms is a 0.8 ms
    /// tripwire), while a real regression clears both conditions.
    #[test]
    fn min_delta_floor_suppresses_micro_baseline_noise() {
        let floored = EffectiveBudget {
            pct: 10.0,
            min_delta_ms: 50,
        };
        // 8 ms -> 10 ms is +25% but only 2 ms absolute: not a violation.
        assert!(!over_budget(8, 10, floored), "under the absolute floor");
        // 8 ms -> 58 ms crosses both the percent and the floor.
        assert!(over_budget(8, 58, floored), "a real micro-query regression");
        // 1000 ms -> 1200 ms: +20% and 200 ms absolute — unaffected by the floor.
        assert!(
            over_budget(1000, 1200, floored),
            "large baselines gate as before"
        );
        // A zero floor restores pure-percent behavior.
        assert!(over_budget(
            8,
            10,
            EffectiveBudget {
                pct: 10.0,
                min_delta_ms: 0
            }
        ));
    }

    // --- bless overwrite guard -------------------------------------------

    use crate::corpus::{ExpectedRows, ExpectedStatus, OrderSensitivity, QueryDef, Tag};
    use crate::schema::{RunMeta, TargetFingerprint, SCHEMA_VERSION};

    fn bless_meta(run_id: &str) -> RunMeta {
        RunMeta {
            schema_version: SCHEMA_VERSION,
            run_id: run_id.into(),
            timestamp: "2026-07-13T00:00:00Z".into(),
            git_commit: "abc1234".into(),
            git_dirty: false,
            build_profile: "debug".into(),
            host: "test".into(),
            runtime: "tokio-multi-thread(worker_threads=1)".into(),
            subset: None,
            corpus_version: 2,
            survey: false,
            targets: vec![TargetFingerprint {
                id: "native-sf01".into(),
                kind: "native".into(),
                alias: "enterprise-sf01".into(),
                fluree_home: "/nonexistent".into(),
            }],
        }
    }

    fn bless_corpus(gates: &[(&str, HashGate)]) -> Corpus {
        let queries = gates
            .iter()
            .map(|(id, gate)| QueryDef {
                id: (*id).to_string(),
                file: PathBuf::from(format!("queries/{id}.rq")),
                bi_question: String::new(),
                tags: vec![Tag::Count],
                tables: vec![],
                class: "dims-only".to_string(),
                expected_rows: ExpectedRows::Range([0, usize::MAX]),
                order_sensitive: OrderSensitivity::None,
                timeout_s: 120,
                subsets: vec!["smoke".to_string()],
                expected_status: ExpectedStatus::default(),
                hash_gate: *gate,
            })
            .collect();
        Corpus {
            dir: PathBuf::from("/nonexistent"),
            corpus_version: 2,
            queries,
        }
    }

    fn bless_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("vbench-bless-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn bless_refuses_differing_oracle_without_force() {
        let baselines = bless_dir("refuse");
        let corpus = bless_corpus(&[("q001", HashGate::Full)]);
        let mut r1 = rec("q001", "native-sf01", "hot", "HASH_A", 10);
        r1.rows = 5;
        let s = write_expected(&bless_meta("R1"), &[r1], &corpus, &baselines, false).unwrap();
        assert_eq!(s.written, vec!["q001"]);

        // A later run with a different hash/rows must refuse (and change nothing).
        let mut r2 = rec("q001", "native-sf01", "hot", "HASH_B", 10);
        r2.rows = 0;
        let err = write_expected(&bless_meta("R2"), &[r2], &corpus, &baselines, false)
            .expect_err("differing oracle must refuse without --force");
        let msg = err.to_string();
        assert!(msg.contains("q001"), "delta names the query: {msg}");
        assert!(
            msg.contains("rows 5 → 0"),
            "delta shows the row change: {msg}"
        );
        let kept = load_expected(&baselines, "q001").unwrap().unwrap();
        assert_eq!(kept.result_hash, "HASH_A", "oracle untouched after refusal");
        assert_eq!(kept.blessed_from.run_id, "R1");
        let _ = std::fs::remove_dir_all(&baselines);
    }

    #[test]
    fn bless_force_overwrites_and_reports() {
        let baselines = bless_dir("force");
        let corpus = bless_corpus(&[("q001", HashGate::Full)]);
        let r1 = rec("q001", "native-sf01", "hot", "HASH_A", 10);
        write_expected(&bless_meta("R1"), &[r1], &corpus, &baselines, false).unwrap();

        let r2 = rec("q001", "native-sf01", "hot", "HASH_B", 10);
        let s = write_expected(&bless_meta("R2"), &[r2], &corpus, &baselines, true).unwrap();
        assert_eq!(s.written, vec!["q001"]);
        assert_eq!(s.overwritten, vec!["q001"]);
        let now = load_expected(&baselines, "q001").unwrap().unwrap();
        assert_eq!(now.result_hash, "HASH_B");
        assert_eq!(now.blessed_from.run_id, "R2");
        let _ = std::fs::remove_dir_all(&baselines);
    }

    #[test]
    fn bless_identical_rerun_is_unchanged_and_diff_free() {
        let baselines = bless_dir("unchanged");
        let corpus = bless_corpus(&[("q001", HashGate::Full)]);
        let r1 = rec("q001", "native-sf01", "hot", "HASH_A", 10);
        write_expected(
            &bless_meta("R1"),
            std::slice::from_ref(&r1),
            &corpus,
            &baselines,
            false,
        )
        .unwrap();

        // Same hash/rows from a new run: no rewrite, original provenance kept.
        let s = write_expected(&bless_meta("R2"), &[r1], &corpus, &baselines, false).unwrap();
        assert!(s.written.is_empty());
        assert_eq!(s.unchanged, vec!["q001"]);
        let kept = load_expected(&baselines, "q001").unwrap().unwrap();
        assert_eq!(kept.blessed_from.run_id, "R1", "diff-free re-bless");
        let _ = std::fs::remove_dir_all(&baselines);
    }

    #[test]
    fn bless_rows_only_gate_tolerates_hash_drift() {
        let baselines = bless_dir("rowsonly");
        let corpus = bless_corpus(&[("q029", HashGate::RowsOnly)]);
        let mut r1 = rec("q029", "native-sf01", "hot", "HASH_A", 10);
        r1.rows = 100;
        write_expected(&bless_meta("R1"), &[r1], &corpus, &baselines, false).unwrap();

        // A rows_only query's hash legitimately varies between native runs;
        // same row count must be treated as unchanged, not refused.
        let mut r2 = rec("q029", "native-sf01", "hot", "HASH_B", 10);
        r2.rows = 100;
        let s = write_expected(&bless_meta("R2"), &[r2], &corpus, &baselines, false).unwrap();
        assert_eq!(s.unchanged, vec!["q029"]);
        // A row-count change still refuses.
        let mut r3 = rec("q029", "native-sf01", "hot", "HASH_C", 10);
        r3.rows = 99;
        assert!(write_expected(&bless_meta("R3"), &[r3], &corpus, &baselines, false).is_err());
        let _ = std::fs::remove_dir_all(&baselines);
    }

    #[test]
    fn auto_rerun_recovers_from_a_noisy_violation() {
        // The compare auto-rerun path: a first pass flags a violation (200ms vs a
        // 100ms baseline under a 20% budget), then the in-process rerun comes back
        // at 105ms — within budget — so the query is not declared red.
        assert!(over_budget(100, 200, pct20()), "first pass is over budget");
        assert!(
            !over_budget(100, 105, pct20()),
            "rerun recovers within budget"
        );
        // A genuine regression stays red on rerun too.
        assert!(
            over_budget(100, 180, pct20()),
            "a real 80% regression stays red"
        );
    }
}
