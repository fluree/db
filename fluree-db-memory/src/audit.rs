//! Checkpoint audit: mechanical signals over a memory store.
//!
//! The editorial rubric in `docs/memory/guides/hygiene-and-auditing.md` — is a
//! memory durable, shared, non-derivable, actionable, well-formed? — is a
//! judgment call and stays with the reviewer. What can be mechanized is the
//! pre-pass: content over the cap, effort narration, unportable paths, refs that
//! no longer resolve or that the code moved out from under, and branch changes
//! nothing remembers. This module computes exactly those signals and nothing
//! else; it never mutates the store.

use crate::types::{Memory, MAX_CONTENT_LENGTH};
use chrono::DateTime;
use regex::Regex;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

/// Base ref a branch audit diffs against when the caller names none.
pub const DEFAULT_BASE_REF: &str = "main";

/// Cap on the changed-files-with-no-coverage list, so the report stays readable
/// on a branch that touched hundreds of files.
pub const MAX_UNREFERENCED_FILES: usize = 25;

/// File extensions the coverage pass considers worth remembering something about.
const COVERAGE_EXTENSIONS: [&str; 3] = ["rs", "md", "toml"];

/// Churned refs named individually in [`MemoryAuditEntry::refs_status`] before
/// the rest are folded into a count.
const CHURN_NAMES_SHOWN: usize = 2;

/// Which memories an audit covers.
#[derive(Debug, Clone)]
pub enum AuditScope {
    /// Memories captured on this branch, plus any memory (from any branch) that
    /// references a file the branch changed — those are the ones this branch can
    /// invalidate.
    Branch(String),
    /// Every memory handed to the audit.
    All,
}

/// Inputs to [`audit_memories`].
#[derive(Debug, Clone)]
pub struct AuditOptions {
    pub scope: AuditScope,
    /// Base ref for the branch diff; [`DEFAULT_BASE_REF`] when `None`.
    pub base_ref: Option<String>,
}

impl Default for AuditOptions {
    fn default() -> Self {
        Self {
            scope: AuditScope::All,
            base_ref: None,
        }
    }
}

/// The full audit result. Serializable so the CLI can emit it verbatim.
#[derive(Debug, Clone, Serialize)]
pub struct AuditReport {
    /// `all`, or the branch name being audited.
    pub scope: String,
    pub base_ref: String,
    pub total_memories: usize,
    pub entries: Vec<MemoryAuditEntry>,
    /// `None` when git could not resolve the merge base — a non-git root, an
    /// unknown base ref, or an `All`-scope audit. Coverage is a branch question.
    pub coverage: Option<CoverageReport>,
    /// True when no project root was available (global-mode store): refs are
    /// repo-relative, so missing/churned checks and coverage were skipped
    /// rather than resolved against an unrelated working directory.
    pub ref_checks_skipped: bool,
}

/// Signals for one memory.
#[derive(Debug, Clone, Serialize)]
pub struct MemoryAuditEntry {
    pub id: String,
    pub kind: String,
    pub branch: Option<String>,
    pub content_len: usize,
    pub over_cap: bool,
    /// Progress/status language: this memory reads as news about an effort.
    pub narration_markers: Vec<String>,
    /// Absolute paths and person-ish handles: this memory won't travel.
    pub portability_markers: Vec<String>,
    pub tag_issues: Vec<String>,
    pub refs_total: usize,
    pub missing_refs: Vec<String>,
    pub churned_refs: Vec<ChurnedRef>,
}

/// A ref whose file changed after the memory was last written or re-verified.
#[derive(Debug, Clone, Serialize)]
pub struct ChurnedRef {
    pub path: String,
    /// Commit date of the file's most recent commit, RFC 3339.
    pub last_touched: String,
}

/// What this branch changed that no in-scope memory points at.
#[derive(Debug, Clone, Serialize)]
pub struct CoverageReport {
    pub merge_base: String,
    pub changed_files: usize,
    pub unreferenced_changed_files: Vec<String>,
    /// The list was cut at [`MAX_UNREFERENCED_FILES`].
    pub truncated: bool,
}

impl MemoryAuditEntry {
    /// Whether every signal came back clean.
    pub fn is_clean(&self) -> bool {
        !self.over_cap
            && self.narration_markers.is_empty()
            && self.portability_markers.is_empty()
            && self.tag_issues.is_empty()
            && self.missing_refs.is_empty()
            && self.churned_refs.is_empty()
    }

    /// Compact labels for one-line rendering, e.g. `over-cap:812`.
    pub fn flags(&self) -> Vec<String> {
        let mut flags = Vec::new();
        if self.over_cap {
            flags.push(format!("over-cap:{}", self.content_len));
        }
        if !self.narration_markers.is_empty() {
            flags.push(format!("narration:{}", self.narration_markers.join(",")));
        }
        if !self.portability_markers.is_empty() {
            flags.push(format!(
                "portability:{}",
                self.portability_markers.join(",")
            ));
        }
        if !self.tag_issues.is_empty() {
            flags.push(format!("tags:{}", self.tag_issues.join(",")));
        }
        flags
    }

    /// One-phrase summary of this memory's refs.
    ///
    /// Churned refs are named up to [`CHURN_NAMES_SHOWN`] and then counted: a
    /// memory pointing at a dozen files that all moved is one finding, not
    /// twelve, and the full list is in the JSON form.
    pub fn refs_status(&self) -> String {
        if self.refs_total == 0 {
            return "none".to_string();
        }
        let mut parts = vec![format!(
            "{} ok",
            self.refs_total - self.missing_refs.len().min(self.refs_total)
        )];
        if !self.missing_refs.is_empty() {
            parts.push(format!("MISSING {}", self.missing_refs.join(" ")));
        }
        if !self.churned_refs.is_empty() {
            let named: Vec<String> = self
                .churned_refs
                .iter()
                .take(CHURN_NAMES_SHOWN)
                .map(|c| format!("{} {}", c.path, c.last_touched))
                .collect();
            let rest = self.churned_refs.len().saturating_sub(CHURN_NAMES_SHOWN);
            let more = if rest > 0 {
                format!(", +{rest}")
            } else {
                String::new()
            };
            parts.push(format!(
                "{} churned since last verified ({}{more})",
                self.churned_refs.len(),
                named.join(", ")
            ));
        }
        parts.join(", ")
    }
}

/// Audit `memories` against the working tree at `repo_root`.
///
/// Read-only: the store is never mutated and git is only ever queried.
pub fn audit_memories(
    memories: &[Memory],
    repo_root: Option<&Path>,
    opts: &AuditOptions,
) -> AuditReport {
    let base_ref = opts
        .base_ref
        .as_deref()
        .unwrap_or(DEFAULT_BASE_REF)
        .to_string();

    // A branch audit needs the diff twice: to widen the scope to memories that
    // reference changed files, and to compute coverage. Resolve it once.
    let diff = match (&opts.scope, repo_root) {
        (AuditScope::Branch(_), Some(root)) => branch_diff(root, &base_ref),
        _ => None,
    };

    let in_scope: Vec<&Memory> = match &opts.scope {
        AuditScope::All => memories.iter().collect(),
        AuditScope::Branch(branch) => {
            let changed: BTreeSet<&str> = diff
                .as_ref()
                .map(|d| d.files.iter().map(String::as_str).collect())
                .unwrap_or_default();
            memories
                .iter()
                .filter(|m| {
                    m.branch.as_deref() == Some(branch.as_str())
                        || m.artifact_refs.iter().any(|r| changed.contains(r.as_str()))
                })
                .collect()
        }
    };

    let churn = match repo_root {
        Some(root) => last_commit_dates(root, &in_scope),
        None => BTreeMap::new(),
    };

    let mut entries: Vec<MemoryAuditEntry> = in_scope
        .iter()
        .map(|m| audit_one(m, repo_root, &churn))
        .collect();
    // Flagged first, so a renderer that caps its list can never hide a finding
    // behind a run of clean memories.
    entries.sort_by_key(MemoryAuditEntry::is_clean);

    let coverage = diff.map(|d| coverage_report(d, &in_scope));

    AuditReport {
        scope: match &opts.scope {
            AuditScope::All => "all".to_string(),
            AuditScope::Branch(b) => b.clone(),
        },
        base_ref,
        total_memories: memories.len(),
        entries,
        coverage,
        ref_checks_skipped: repo_root.is_none(),
    }
}

fn audit_one(
    mem: &Memory,
    repo_root: Option<&Path>,
    churn: &BTreeMap<String, String>,
) -> MemoryAuditEntry {
    let mut text = mem.content.clone();
    if let Some(rationale) = &mem.rationale {
        text.push(' ');
        text.push_str(rationale);
    }

    let mut missing_refs = Vec::new();
    let mut churned_refs = Vec::new();
    let effective = effective_timestamp(mem);
    // No root means repo-relative refs can't be resolved at all; reporting
    // them MISSING against an unrelated cwd would be a confident wrong answer.
    for r in repo_root.map(|_| &mem.artifact_refs).into_iter().flatten() {
        let root = repo_root.expect("guarded by the map above");
        if !root.join(r).exists() {
            missing_refs.push(r.clone());
            continue;
        }
        // A ref whose file moved on after the memory was last written or
        // re-verified is the cheapest available "this claim may have rotted".
        if let (Some(last), Some(effective)) = (churn.get(r), effective.as_ref()) {
            if DateTime::parse_from_rfc3339(last).is_ok_and(|l| l.to_utc() > *effective) {
                churned_refs.push(ChurnedRef {
                    path: r.clone(),
                    last_touched: last.split('T').next().unwrap_or(last).to_string(),
                });
            }
        }
    }

    MemoryAuditEntry {
        id: mem.id.clone(),
        kind: mem.kind.to_string(),
        branch: mem.branch.clone(),
        content_len: mem.content.chars().count(),
        over_cap: mem.content.chars().count() > MAX_CONTENT_LENGTH,
        narration_markers: narration_markers(&text),
        portability_markers: portability_markers(&text),
        tag_issues: tag_issues(&mem.tags),
        refs_total: mem.artifact_refs.len(),
        missing_refs,
        churned_refs,
    }
}

/// Last write or re-verification of a memory, as a UTC instant.
fn effective_timestamp(mem: &Memory) -> Option<chrono::DateTime<chrono::Utc>> {
    let raw = mem.updated_at.as_deref().unwrap_or(&mem.created_at);
    DateTime::parse_from_rfc3339(raw).ok().map(|d| d.to_utc())
}

// ---------------------------------------------------------------------------
// Content markers
// ---------------------------------------------------------------------------

struct Markers {
    narration: Vec<(&'static str, Regex)>,
    portability: Vec<(&'static str, Regex)>,
    handle: Regex,
}

/// `@`-prefixed words that are JSON-LD/RDF keywords rather than people. Without
/// this list every memory about `@context` or `@fulltext` reads as personal.
const NON_HANDLE_ATS: [&str; 21] = [
    "context",
    "prefix",
    "id",
    "type",
    "value",
    "graph",
    "fulltext",
    "list",
    "set",
    "language",
    "base",
    "vocab",
    "container",
    "none",
    "json",
    "default",
    "index",
    "reverse",
    "nest",
    "included",
    "embed",
];

fn markers() -> &'static Markers {
    static MARKERS: OnceLock<Markers> = OnceLock::new();
    MARKERS.get_or_init(|| Markers {
        narration: vec![
            // Case-sensitive where the shout is the signal: "shipped a fix" is
            // prose, "SHIPPED" is a status board.
            ("SHIPPED", Regex::new(r"\bSHIPPED\b").unwrap()),
            ("WIP", Regex::new(r"\bWIP\b").unwrap()),
            ("landed", Regex::new(r"(?i)\blanded\b").unwrap()),
            ("merged-as", Regex::new(r"(?i)\bmerged as\b").unwrap()),
            ("awaiting", Regex::new(r"(?i)\bawaiting\b").unwrap()),
            ("pending", Regex::new(r"(?i)\bpending\b").unwrap()),
            ("next-step", Regex::new(r"(?i)\bnext steps?\b").unwrap()),
            ("hand-off", Regex::new(r"(?i)\bhand-?off\b").unwrap()),
            ("resume", Regex::new(r"(?i)\bresume").unwrap()),
            ("pr-number", Regex::new(r"(?i)\bPR ?#?\d+|#\d{4,}").unwrap()),
            ("iso-date", Regex::new(r"\b20\d\d-\d\d-\d\d\b").unwrap()),
        ],
        portability: vec![(
            "absolute-path",
            Regex::new(r"(/Users/|/home/|~/|[A-Za-z]:\\)").unwrap(),
        )],
        handle: Regex::new(r"@[A-Za-z][A-Za-z0-9_.-]{2,}").unwrap(),
    })
}

/// Progress/status language over content + rationale.
pub fn narration_markers(text: &str) -> Vec<String> {
    let mut found: Vec<String> = markers()
        .narration
        .iter()
        .filter(|(label, _)| *label != "iso-date")
        .filter(|(_, re)| re.is_match(text))
        .map(|(label, _)| (*label).to_string())
        .collect();
    // A bare date is often legitimate provenance ("the spec froze 2025-03-01");
    // it only reads as a status board next to other narration language.
    if !found.is_empty()
        && markers()
            .narration
            .iter()
            .any(|(l, re)| *l == "iso-date" && re.is_match(text))
    {
        found.push("iso-date".to_string());
    }
    found
}

/// Markers that a memory won't travel to another contributor's checkout.
pub fn portability_markers(text: &str) -> Vec<String> {
    let m = markers();
    let mut found: Vec<String> = m
        .portability
        .iter()
        .filter(|(_, re)| re.is_match(text))
        .map(|(label, _)| (*label).to_string())
        .collect();
    if m.handle.find_iter(text).any(|hit| {
        // Compare only the leading word so hyphenated prose like
        // "@context-driven" is still recognized as the JSON-LD keyword.
        let name = hit.as_str().trim_start_matches('@').to_lowercase();
        let head = name.split(['-', '.', '_']).next().unwrap_or(&name);
        !NON_HANDLE_ATS.contains(&head)
    }) {
        found.push("handle".to_string());
    }
    found
}

/// Tags that aren't lowercase single-word recall keys.
pub fn tag_issues(tags: &[String]) -> Vec<String> {
    let mut issues = Vec::new();
    for tag in tags {
        if tag.trim().is_empty() {
            issues.push("empty".to_string());
        } else if tag.chars().any(char::is_whitespace) {
            issues.push(format!("whitespace:{tag}"));
        } else if *tag != tag.to_lowercase() {
            issues.push(format!("uppercase:{tag}"));
        }
    }
    issues
}

// ---------------------------------------------------------------------------
// Git
// ---------------------------------------------------------------------------

struct BranchDiff {
    merge_base: String,
    files: Vec<String>,
}

fn git(repo_root: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(repo_root)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Files this branch changed relative to its merge base with `base_ref`.
///
/// `None` on any git failure — a missing repo or an unknown base must degrade
/// the audit to "no coverage section", never fail it.
fn branch_diff(repo_root: &Path, base_ref: &str) -> Option<BranchDiff> {
    // User-supplied; a leading '-' would parse as a git option, not a ref.
    if base_ref.starts_with('-') {
        return None;
    }
    let merge_base = git(repo_root, &["merge-base", base_ref, "HEAD"])?;
    if merge_base.is_empty() {
        return None;
    }
    let range = format!("{merge_base}..HEAD");
    let out = git(repo_root, &["diff", "--name-only", &range])?;
    Some(BranchDiff {
        merge_base,
        files: out.lines().map(str::to_string).collect(),
    })
}

/// Commit date of the most recent commit touching each distinct existing ref.
///
/// One `git log` walk for the whole set, bounded to commits newer than the
/// oldest effective timestamp in scope — churn older than every memory can't
/// flag anything, and per-ref subprocesses made `--scope all` fork O(refs)
/// processes.
fn last_commit_dates(repo_root: &Path, memories: &[&Memory]) -> BTreeMap<String, String> {
    let distinct: BTreeSet<&str> = memories
        .iter()
        .flat_map(|m| m.artifact_refs.iter().map(String::as_str))
        // A missing ref is already reported as missing; asking git about it
        // would spend effort to say the same thing twice.
        .filter(|r| repo_root.join(r).exists())
        .collect();
    if distinct.is_empty() {
        return BTreeMap::new();
    }
    let Some(oldest) = memories.iter().filter_map(|m| effective_timestamp(m)).min() else {
        return BTreeMap::new();
    };
    let since = format!("--since={}", oldest.to_rfc3339());
    let out = git(
        repo_root,
        &["log", &since, "--format=%x01%cI", "--name-only"],
    );
    let mut dates = BTreeMap::new();
    let mut current: Option<&str> = None;
    for line in out.as_deref().unwrap_or_default().lines() {
        if let Some(date) = line.strip_prefix('\x01') {
            current = Some(date);
        } else if !line.is_empty() {
            if let Some(date) = current {
                if distinct.contains(line) && !dates.contains_key(line) {
                    // Newest-first walk: the first commit naming a path is its
                    // most recent touch.
                    dates.insert(line.to_string(), date.to_string());
                }
            }
        }
    }
    dates
}

fn coverage_report(diff: BranchDiff, in_scope: &[&Memory]) -> CoverageReport {
    let referenced: BTreeSet<&str> = in_scope
        .iter()
        .flat_map(|m| m.artifact_refs.iter().map(String::as_str))
        .collect();

    let mut unreferenced: Vec<String> = diff
        .files
        .iter()
        .filter(|f| !f.starts_with(".fluree-memory/"))
        .filter(|f| {
            Path::new(f)
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| COVERAGE_EXTENSIONS.contains(&e))
        })
        .filter(|f| !referenced.contains(f.as_str()))
        .cloned()
        .collect();

    let truncated = unreferenced.len() > MAX_UNREFERENCED_FILES;
    unreferenced.truncate(MAX_UNREFERENCED_FILES);

    CoverageReport {
        merge_base: diff.merge_base,
        changed_files: diff.files.len(),
        unreferenced_changed_files: unreferenced,
        truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MemoryKind, Scope};

    fn mem(id: &str, content: &str) -> Memory {
        Memory {
            id: id.to_string(),
            kind: MemoryKind::Fact,
            content: content.to_string(),
            tags: vec!["testing".to_string()],
            scope: Scope::Repo,
            severity: None,
            artifact_refs: Vec::new(),
            branch: None,
            created_at: "2026-01-01T00:00:00+00:00".to_string(),
            updated_at: None,
            rationale: None,
            alternatives: None,
        }
    }

    #[test]
    fn narration_markers_catch_progress_language() {
        assert_eq!(narration_markers("SHIPPED as part of wave 2"), ["SHIPPED"]);
        assert_eq!(
            narration_markers("merged as #1450"),
            ["merged-as", "pr-number"]
        );
        assert_eq!(narration_markers("awaiting review"), ["awaiting"]);
        assert_eq!(
            narration_markers("resume from the handoff"),
            ["hand-off", "resume"]
        );
        // A bare date is provenance, not narration (review R5): it only flags
        // alongside another narration marker.
        assert!(narration_markers("verified on 2026-07-12").is_empty());
        assert_eq!(
            narration_markers("SHIPPED 2026-07-18"),
            ["SHIPPED", "iso-date"]
        );
        assert_eq!(
            narration_markers("landed on main; next steps are in the tracker"),
            ["landed", "next-step"]
        );
    }

    #[test]
    fn narration_markers_leave_durable_prose_alone() {
        assert!(narration_markers(
            "PSOT range queries return supersets; callers must re-filter on the object."
        )
        .is_empty());
        // Lowercase "shipped"/"wip" inside ordinary prose is not a status board.
        assert!(narration_markers("the shipped binaries omit HNSW").is_empty());
    }

    #[test]
    fn portability_markers_catch_paths_and_handles() {
        assert_eq!(
            portability_markers("see /Users/dev/checkout/notes.md"),
            ["absolute-path"]
        );
        assert_eq!(
            portability_markers("~/scratch holds the corpus"),
            ["absolute-path"]
        );
        assert_eq!(
            portability_markers(r"C:\work\db is the checkout"),
            ["absolute-path"]
        );
        assert_eq!(portability_markers("confirmed by @someone"), ["handle"]);
    }

    #[test]
    fn json_ld_keywords_are_not_handles() {
        assert!(portability_markers(
            "compact_id is @context-driven, so @fulltext and @id survive the round trip"
        )
        .is_empty());
    }

    #[test]
    fn tag_issues_flag_non_keys() {
        assert!(tag_issues(&["indexer".to_string()]).is_empty());
        assert_eq!(tag_issues(&["Indexer".to_string()]), ["uppercase:Indexer"]);
        assert_eq!(
            tag_issues(&["two words".to_string()]),
            ["whitespace:two words"]
        );
        assert_eq!(tag_issues(&["  ".to_string()]), ["empty"]);
    }

    #[test]
    fn over_cap_and_refs_are_reported() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("Cargo.toml"), "").unwrap();

        let mut m = mem("mem:fact-a", &"x".repeat(MAX_CONTENT_LENGTH + 1));
        m.artifact_refs = vec!["Cargo.toml".to_string(), "src/gone.rs".to_string()];

        let report = audit_memories(&[m], Some(root.path()), &AuditOptions::default());
        let entry = &report.entries[0];
        assert!(entry.over_cap);
        assert_eq!(entry.content_len, MAX_CONTENT_LENGTH + 1);
        assert_eq!(entry.missing_refs, ["src/gone.rs"]);
        assert_eq!(entry.refs_total, 2);
        assert!(!entry.is_clean());
    }

    #[test]
    fn clean_memory_has_no_flags() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("Cargo.toml"), "").unwrap();

        let mut m = mem(
            "mem:fact-a",
            "Ledger ids normalize to `<name>:<branch>` before any lookup.",
        );
        m.artifact_refs = vec!["Cargo.toml".to_string()];

        let report = audit_memories(&[m], Some(root.path()), &AuditOptions::default());
        assert!(report.entries[0].is_clean());
        assert!(report.entries[0].flags().is_empty());
        assert_eq!(report.entries[0].refs_status(), "1 ok");
    }

    #[test]
    fn branch_scope_keeps_own_branch_memories() {
        let root = tempfile::tempdir().unwrap();

        let mut on_branch = mem("mem:fact-a", "on the audited branch");
        on_branch.branch = Some("feat/x".to_string());
        let mut elsewhere = mem("mem:fact-b", "on another branch");
        elsewhere.branch = Some("feat/y".to_string());

        let report = audit_memories(
            &[on_branch, elsewhere],
            Some(root.path()),
            &AuditOptions {
                scope: AuditScope::Branch("feat/x".to_string()),
                base_ref: None,
            },
        );

        assert_eq!(report.total_memories, 2);
        assert_eq!(report.entries.len(), 1);
        assert_eq!(report.entries[0].id, "mem:fact-a");
        assert_eq!(report.scope, "feat/x");
    }

    #[test]
    fn coverage_is_none_outside_a_git_repo() {
        let root = tempfile::tempdir().unwrap();
        let report = audit_memories(
            &[mem("mem:fact-a", "anything")],
            Some(root.path()),
            &AuditOptions {
                scope: AuditScope::Branch("feat/x".to_string()),
                base_ref: None,
            },
        );
        assert!(report.coverage.is_none());
        assert_eq!(report.base_ref, DEFAULT_BASE_REF);
    }

    #[test]
    fn no_root_skips_ref_checks_instead_of_reporting_missing() {
        let mut m = mem("mem:fact-a", "cites a real path");
        m.artifact_refs = vec!["src/definitely/not/here.rs".to_string()];
        let report = audit_memories(&[m], None, &AuditOptions::default());
        assert!(report.ref_checks_skipped);
        assert!(report.entries[0].missing_refs.is_empty());
        assert!(report.entries[0].churned_refs.is_empty());
    }

    #[test]
    fn all_scope_never_reports_coverage() {
        let report = audit_memories(
            &[mem("mem:fact-a", "anything")],
            Some(Path::new(".")),
            &AuditOptions::default(),
        );
        assert!(report.coverage.is_none());
        assert_eq!(report.scope, "all");
    }
}
