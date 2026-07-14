//! Corpus manifest: the tagged query catalog and its validation.
//!
//! `corpus/manifest.json` lists every query with its file, tags, tables, BI
//! question, expected-row bound, and subset membership. [`Corpus::load`]
//! validates the manifest before any run: ids are unique, every `.rq` file
//! exists, tags are drawn from the closed [`Tag`] enum, and the `smoke` subset
//! covers every tag that appears anywhere in the corpus (so a smoke run
//! exercises the full pathway surface).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Closed set of pathway tags (the SPARQL-feature enum from
/// `docs/audit/2026-07-virtual-dataset-perf/03-corpus-design.md` §2). Adding a
/// query pathway means adding a variant here — an unknown tag in the manifest is
/// a load error, not a silent pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tag {
    BgpStar,
    Join,
    FkChain,
    FilterRange,
    FilterString,
    FilterDate,
    /// IRI / `=` / `IN` equality on a term-typed value (distinct from lexical
    /// `FilterString` and numeric/date `FilterRange`).
    FilterIri,
    Optional,
    Union,
    Aggregate,
    Count,
    GroupBy,
    Having,
    OrderBy,
    Distinct,
    Subquery,
    Values,
    Negation,
    PropertyPath,
    Construct,
}

/// A per-target-kind expected terminal outcome. Defaults to [`Self::Ok`], so a
/// query with no `expected_status` in the manifest is expected to succeed
/// everywhere (the common case).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExpectedOutcome {
    #[default]
    Ok,
    Error,
}

/// Optional per-target-kind expected status. The error-boundary queries (a
/// lang-tagged / custom-datatype bound object) return **0 rows on the native
/// materialized ledger** but **error on a virtual R2RML target** (the router
/// fails the whole GRAPH scope), so a single `expected_rows` cannot describe
/// both. When present, an `Error` outcome that matches the expectation for the
/// running target's kind is recorded as [`crate::schema::Status::ExpectedError`]
/// (a gating pass) rather than [`crate::schema::Status::Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedStatus {
    #[serde(default)]
    pub native: ExpectedOutcome,
    /// `virtual` is a Rust keyword, hence the trailing underscore + serde rename.
    #[serde(default, rename = "virtual")]
    pub virtual_: ExpectedOutcome,
}

impl Default for ExpectedStatus {
    fn default() -> Self {
        Self {
            native: ExpectedOutcome::Ok,
            virtual_: ExpectedOutcome::Ok,
        }
    }
}

impl ExpectedStatus {
    /// The expected outcome for a target of the given kind.
    pub fn for_target(&self, is_virtual: bool) -> ExpectedOutcome {
        if is_virtual {
            self.virtual_
        } else {
            self.native
        }
    }
}

/// Whether row-order carries meaning for a query (metadata only — the result
/// hash is always an order-independent multiset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSensitivity {
    None,
    ByKeys,
}

/// How a query's result is gated in native-vs-virtual comparison.
///
/// `Full` (the default) requires an exact result-hash match. `RowsOnly` gates on
/// row count (plus any invariants) only — for queries whose result is a
/// **nondeterministic selection**: an unordered `LIMIT` that truncates a larger
/// set (any `k` rows are a valid answer), which two engines can satisfy with
/// different-but-equally-correct rows and therefore cannot be hash-compared. An
/// `ORDER BY … LIMIT` top-k stays `Full` because the corpus appends a unique
/// tiebreaker to its sort key (see `03-corpus-design.md` §5). Consumed by the
/// compare/bless gate: on `RowsOnly` it must skip the hash equality check.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HashGate {
    #[default]
    Full,
    RowsOnly,
}

/// Expected row count: an exact value or an inclusive `[min, max]` range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExpectedRows {
    Exact(usize),
    Range([usize; 2]),
}

impl ExpectedRows {
    /// Whether `n` satisfies this bound.
    pub fn contains(&self, n: usize) -> bool {
        match self {
            Self::Exact(x) => n == *x,
            Self::Range([lo, hi]) => n >= *lo && n <= *hi,
        }
    }
}

impl std::fmt::Display for ExpectedRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exact(x) => write!(f, "{x}"),
            Self::Range([lo, hi]) => write!(f, "[{lo},{hi}]"),
        }
    }
}

fn default_timeout_s() -> u64 {
    120
}

/// One catalogued query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDef {
    pub id: String,
    /// Path to the `.rq` file, relative to the corpus directory.
    pub file: PathBuf,
    pub bi_question: String,
    pub tags: Vec<Tag>,
    pub tables: Vec<String>,
    pub class: String,
    pub expected_rows: ExpectedRows,
    pub order_sensitive: OrderSensitivity,
    #[serde(default = "default_timeout_s")]
    pub timeout_s: u64,
    pub subsets: Vec<String>,
    /// Per-target-kind expected status. Absent ⇒ expected `ok` everywhere.
    #[serde(default)]
    pub expected_status: ExpectedStatus,
    /// How native-vs-virtual parity is gated. Absent ⇒ `Full` (exact hash).
    #[serde(default)]
    pub hash_gate: HashGate,
}

impl QueryDef {
    pub fn in_subset(&self, subset: &str) -> bool {
        self.subsets.iter().any(|s| s == subset)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct Manifest {
    corpus_version: u32,
    #[allow(dead_code)]
    #[serde(default)]
    description: String,
    queries: Vec<QueryDef>,
}

/// The validated corpus plus the directory its query files live in.
pub struct Corpus {
    pub dir: PathBuf,
    /// The manifest's `corpus_version` — recorded in every run's meta so a run
    /// made against an amended corpus stays distinguishable.
    pub corpus_version: u32,
    pub queries: Vec<QueryDef>,
}

impl Corpus {
    /// Load and validate `<dir>/manifest.json`.
    pub fn load(dir: &Path) -> Result<Self> {
        let manifest_path = dir.join("manifest.json");
        let raw = std::fs::read_to_string(&manifest_path)
            .with_context(|| format!("reading manifest {}", manifest_path.display()))?;
        let manifest: Manifest = serde_json::from_str(&raw)
            .with_context(|| format!("parsing manifest {}", manifest_path.display()))?;
        let corpus = Self {
            dir: dir.to_path_buf(),
            corpus_version: manifest.corpus_version,
            queries: manifest.queries,
        };
        corpus.validate()?;
        Ok(corpus)
    }

    /// Read the SPARQL text for a query.
    pub fn read_query(&self, def: &QueryDef) -> Result<String> {
        let path = self.dir.join(&def.file);
        std::fs::read_to_string(&path).with_context(|| format!("reading query {}", path.display()))
    }

    /// Queries in a subset (or all queries when `subset` is `None`).
    pub fn select(&self, subset: Option<&str>) -> Vec<&QueryDef> {
        self.queries
            .iter()
            .filter(|q| subset.is_none_or(|s| q.in_subset(s)))
            .collect()
    }

    /// Select an explicit set of queries by id, returned in **corpus order**
    /// (stable regardless of the order the ids were listed). Errors if any id is
    /// unknown. Used to resume a partial run (e.g. the remaining `q019..q054`
    /// after a crash) in-process so timings stay hot-protocol comparable.
    pub fn select_by_ids(&self, ids: &[String]) -> Result<Vec<&QueryDef>> {
        let want: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
        let found: Vec<&QueryDef> = self
            .queries
            .iter()
            .filter(|q| want.contains(q.id.as_str()))
            .collect();
        let found_ids: BTreeSet<&str> = found.iter().map(|q| q.id.as_str()).collect();
        let missing: Vec<&&str> = want.iter().filter(|id| !found_ids.contains(**id)).collect();
        if !missing.is_empty() {
            anyhow::bail!("unknown query id(s): {missing:?}");
        }
        Ok(found)
    }

    /// Look up a single query by id.
    pub fn get(&self, id: &str) -> Option<&QueryDef> {
        self.queries.iter().find(|q| q.id == id)
    }

    fn validate(&self) -> Result<()> {
        // Unique ids.
        let mut seen = BTreeSet::new();
        for q in &self.queries {
            if !seen.insert(q.id.as_str()) {
                anyhow::bail!("duplicate query id '{}' in manifest", q.id);
            }
        }

        // Every query file exists and is non-empty.
        for q in &self.queries {
            let path = self.dir.join(&q.file);
            let text = std::fs::read_to_string(&path).with_context(|| {
                format!(
                    "query '{}' references missing file {}",
                    q.id,
                    path.display()
                )
            })?;
            if text.trim().is_empty() {
                anyhow::bail!("query '{}' file {} is empty", q.id, path.display());
            }
        }

        // Smoke covers every tag that appears anywhere in the corpus.
        let all_tags: BTreeSet<Tag> = self
            .queries
            .iter()
            .flat_map(|q| q.tags.iter().copied())
            .collect();
        let smoke_tags: BTreeSet<Tag> = self
            .queries
            .iter()
            .filter(|q| q.in_subset("smoke"))
            .flat_map(|q| q.tags.iter().copied())
            .collect();
        let uncovered: Vec<Tag> = all_tags.difference(&smoke_tags).copied().collect();
        if !uncovered.is_empty() {
            anyhow::bail!(
                "smoke subset does not cover every tag present in the corpus; uncovered: {uncovered:?}"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shipped corpus is valid (unique ids, files present, smoke covers all
    /// tags). This is the corpus-validation unit test the quality bar requires.
    #[test]
    fn shipped_corpus_is_valid() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus");
        let corpus = Corpus::load(&dir).expect("shipped corpus must validate");
        assert_eq!(
            corpus.queries.len(),
            54,
            "full corpus has 54 queries (design Q01-Q54)"
        );
        // The smoke subset is a cheap, dims-heavy cover of every feature tag.
        let smoke = corpus.select(Some("smoke"));
        assert!(
            (12..=18).contains(&smoke.len()),
            "smoke is a ~12-15 query cover, got {}",
            smoke.len()
        );
        // `validate()` already guarantees smoke covers every tag; assert the
        // load-bearing count so a future trim can't silently shrink coverage.
        let smoke_tags: BTreeSet<Tag> = corpus
            .queries
            .iter()
            .filter(|q| q.in_subset("smoke"))
            .flat_map(|q| q.tags.iter().copied())
            .collect();
        assert_eq!(
            smoke_tags.len(),
            20,
            "smoke must exercise all 20 feature tags"
        );
    }

    /// `select_by_ids` returns the requested queries in corpus order and rejects
    /// an unknown id — the resume path's guardrail.
    #[test]
    fn select_by_ids_resumes_in_corpus_order_and_rejects_unknown() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus");
        let corpus = Corpus::load(&dir).expect("corpus loads");
        // Requested out of order, returned in corpus order.
        let sel = corpus
            .select_by_ids(&["q054".to_string(), "q019".to_string(), "q027".to_string()])
            .unwrap();
        let ids: Vec<&str> = sel.iter().map(|q| q.id.as_str()).collect();
        assert_eq!(ids, vec!["q019", "q027", "q054"]);
        // A typo is a hard error, not a silent skip.
        assert!(corpus
            .select_by_ids(&["q019".to_string(), "q999".to_string()])
            .is_err());
    }

    /// The error-boundary probes (q043 lang-tag, q044 custom-datatype) carry
    /// NO expected_status override: the audit predicted a whole-GRAPH error on
    /// virtual, but observed behavior (2026-07-11 baseline) is q043 ok/0-rows
    /// and q044 dnf — neither errors. Both must still admit 0 rows on native.
    ///
    /// PR-0 (findings F1/F2) then flipped the non-lowered sub-scope queries to
    /// loud-refuse on virtual: exactly q034 (transitive property path) and
    /// q051 (subquery) declare `virtual = error`, and both must stay `ok` on
    /// native, where the same shapes evaluate fine against the ledger index.
    #[test]
    fn error_boundary_queries_match_observed_behavior() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus");
        let corpus = Corpus::load(&dir).expect("corpus loads");
        let declared_error: Vec<&str> = corpus
            .queries
            .iter()
            .filter(|q| q.expected_status.for_target(true) == ExpectedOutcome::Error)
            .map(|q| q.id.as_str())
            .collect();
        assert_eq!(
            declared_error,
            vec!["q034", "q051"],
            "exactly the PR-0 loud-refuse queries declare virtual=error"
        );
        for id in ["q034", "q051"] {
            let q = corpus.queries.iter().find(|q| q.id == id).expect(id);
            assert_eq!(
                q.expected_status.for_target(false),
                ExpectedOutcome::Ok,
                "{id} must stay ok on native"
            );
        }
        for id in ["q043", "q044"] {
            let q = corpus.queries.iter().find(|q| q.id == id).expect(id);
            assert_eq!(q.expected_status.for_target(false), ExpectedOutcome::Ok);
            assert!(q.expected_rows.contains(0), "{id} native-expects 0 rows");
        }
    }

    /// The determinism amendment (§5): the nondeterministic-selection queries
    /// carry `hash_gate = rows_only`; everything else defaults to `Full` (exact
    /// hash). A default-constructed gate is `Full`.
    #[test]
    fn rows_only_hash_gate_marks_nondeterministic_limits() {
        assert_eq!(HashGate::default(), HashGate::Full);
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus");
        let corpus = Corpus::load(&dir).expect("corpus loads");
        let rows_only: BTreeSet<&str> = corpus
            .queries
            .iter()
            .filter(|q| q.hash_gate == HashGate::RowsOnly)
            .map(|q| q.id.as_str())
            .collect();
        let expected: BTreeSet<&str> = [
            "q015", "q016", "q028", "q029", "q031", "q045", "q048", "q049", "q053",
        ]
        .into_iter()
        .collect();
        assert_eq!(rows_only, expected, "rows_only set must match the §5 audit");
    }

    #[test]
    fn expected_status_defaults_to_ok() {
        // A query with no `expected_status` key is expected `ok` on both kinds.
        let es = ExpectedStatus::default();
        assert_eq!(es.for_target(true), ExpectedOutcome::Ok);
        assert_eq!(es.for_target(false), ExpectedOutcome::Ok);
    }

    #[test]
    fn expected_rows_bounds() {
        assert!(ExpectedRows::Exact(1).contains(1));
        assert!(!ExpectedRows::Exact(1).contains(2));
        assert!(ExpectedRows::Range([1, 10]).contains(1));
        assert!(ExpectedRows::Range([1, 10]).contains(10));
        assert!(!ExpectedRows::Range([1, 10]).contains(11));
    }

    #[test]
    fn duplicate_ids_rejected() {
        let q = QueryDef {
            id: "dup".to_string(),
            file: PathBuf::from("queries/q001_count_class.rq"),
            bi_question: String::new(),
            tags: vec![Tag::Count],
            tables: vec![],
            class: "dims-only".to_string(),
            expected_rows: ExpectedRows::Exact(1),
            order_sensitive: OrderSensitivity::None,
            timeout_s: 120,
            subsets: vec!["smoke".to_string()],
            expected_status: ExpectedStatus::default(),
            hash_gate: HashGate::default(),
        };
        let corpus = Corpus {
            dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus"),
            corpus_version: 2,
            queries: vec![q.clone(), q],
        };
        assert!(corpus.validate().is_err(), "duplicate ids must be rejected");
    }
}
