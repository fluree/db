//! Branches: list, create, drop, merge, rebase, revert, and the read-only
//! merge and revert previews.
//!
//! Every function takes a canonical `name:branch` id and returns plain dicts;
//! `fluree/_records.py` builds the result objects.

use crate::convert::{commit_ref, commit_summary, flake};
use crate::error::{api_error, fluree_error, invalid_request};
use crate::runtime::block_on;
use fluree_db_api::format::iri::IriCompactor;
use fluree_db_api::{
    ConflictDetail, ConflictStrategy, Fluree, MergePreview, MergePreviewOpts, RevertPreview,
    RevertPreviewOpts, TimeSpec, ValidationSummary,
};
use fluree_db_core::ledger_id::split_ledger_id;
use fluree_db_core::{CommitSummary, ConflictKey};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// `(ledger name, branch)` of a canonical id.
fn parts(ledger: &str) -> PyResult<(String, String)> {
    split_ledger_id(ledger).map_err(|e| invalid_request(e.to_string()))
}

fn strategy(name: &str) -> PyResult<ConflictStrategy> {
    ConflictStrategy::parse_canonical(name).map_err(invalid_request)
}

/// The commits named by `commits` (each a `t`, id, or digest prefix).
fn commit_refs(commits: &Bound<'_, PyList>) -> PyResult<Vec<fluree_db_api::CommitRef>> {
    commits.iter().map(|c| commit_ref(&c)).collect()
}

pub(crate) fn list<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    ledger: &str,
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let (name, _) = parts(ledger)?;
    let mut records = block_on(py, fluree.list_branches(&name))?.map_err(api_error)?;
    records.sort_by(|a, b| a.branch.cmp(&b.branch));
    records
        .iter()
        .map(|record| {
            let branch = PyDict::new(py);
            branch.set_item("name", &record.branch)?;
            branch.set_item("id", record.ledger_id.to_string())?;
            branch.set_item("source", record.source_branch.as_deref())?;
            branch.set_item("t", record.commit_t)?;
            branch.set_item(
                "head",
                record.commit_head_id.as_ref().map(ToString::to_string),
            )?;
            Ok(branch)
        })
        .collect()
}

/// Create branch `name` from `ledger`'s branch, at its head or at `at`.
/// Returns the new branch's id.
pub(crate) fn create(
    py: Python<'_>,
    fluree: &Fluree,
    ledger: &str,
    name: &str,
    at: TimeSpec,
) -> PyResult<String> {
    let (ledger_name, source) = parts(ledger)?;
    let at = match at {
        TimeSpec::Latest => None,
        at => Some(at),
    };
    let record = block_on(
        py,
        fluree.create_branch(&ledger_name, name, Some(&source), at),
    )?
    .map_err(api_error)?;
    Ok(record.ledger_id.to_string())
}

pub(crate) fn drop(py: Python<'_>, fluree: &Fluree, ledger: &str) -> PyResult<()> {
    let (ledger_name, branch) = parts(ledger)?;
    let dropped = block_on(py, async {
        // The engine refuses the root too, but names its Rust API in the error.
        let records = fluree.list_branches(&ledger_name).await?;
        if records
            .iter()
            .any(|r| r.branch == branch && r.source_branch.is_none())
        {
            return Ok(false);
        }
        fluree
            .drop_branch(&ledger_name, &branch)
            .await
            .map(|_| true)
    })?
    .map_err(api_error)?;
    if dropped {
        Ok(())
    } else {
        Err(invalid_request(format!(
            "{ledger:?} is the ledger's first branch; drop the whole ledger with \
             drop({ledger_name:?})"
        )))
    }
}

/// Merge branch `source` into `target`.
pub(crate) fn merge<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    target: &str,
    source: &str,
    strategy_name: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let (ledger_name, target_branch) = parts(target)?;
    let strategy = strategy(strategy_name)?;
    let report = block_on(
        py,
        fluree.merge_branch(&ledger_name, source, Some(&target_branch), strategy),
    )?
    .map_err(api_error)?;
    let merge = PyDict::new(py);
    merge.set_item("source", &report.source)?;
    merge.set_item("target", &report.target)?;
    merge.set_item("fast_forward", report.fast_forward)?;
    merge.set_item("t", report.new_head_t)?;
    merge.set_item("id", report.new_head_id.to_string())?;
    merge.set_item("digest", report.new_head_id.digest_hex())?;
    merge.set_item("conflicts", report.conflict_count)?;
    merge.set_item("strategy", report.strategy.as_deref())?;
    Ok(merge)
}

/// Replay `ledger`'s own commits onto the current head of the branch it was
/// created from.
pub(crate) fn rebase<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    ledger: &str,
    strategy_name: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let (ledger_name, branch) = parts(ledger)?;
    let strategy = strategy(strategy_name)?;
    let report =
        block_on(py, fluree.rebase_branch(&ledger_name, &branch, strategy))?.map_err(api_error)?;
    let rebase = PyDict::new(py);
    rebase.set_item("fast_forward", report.fast_forward)?;
    rebase.set_item("replayed", report.replayed)?;
    rebase.set_item("skipped", report.skipped)?;
    rebase.set_item("total", report.total_commits)?;
    rebase.set_item("source_t", report.source_head_t)?;
    let conflicts = report
        .conflicts
        .iter()
        .map(|c| (c.original_t, c.conflict_count, c.resolution))
        .collect::<Vec<_>>();
    rebase.set_item("conflicts", conflicts)?;
    let failures = report
        .failures
        .iter()
        .map(|f| (f.original_t, f.error.as_str()))
        .collect::<Vec<_>>();
    rebase.set_item("failures", failures)?;
    Ok(rebase)
}

/// Write one commit on `ledger` that undoes `commits`.
pub(crate) fn revert<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    ledger: &str,
    commits: &Bound<'py, PyList>,
    strategy_name: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let (ledger_name, branch) = parts(ledger)?;
    let strategy = strategy(strategy_name)?;
    let refs = commit_refs(commits)?;
    let report = block_on(
        py,
        fluree.revert_commits(&ledger_name, &branch, refs, strategy),
    )?
    .map_err(api_error)?;
    let revert = PyDict::new(py);
    revert.set_item("committed", report.wrote_commit)?;
    revert.set_item("t", report.new_head_t)?;
    revert.set_item("id", report.new_head_id.to_string())?;
    revert.set_item("digest", report.new_head_id.digest_hex())?;
    let reverted = report
        .reverted_commits
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    revert.set_item("reverted", reverted)?;
    revert.set_item("conflicts", report.conflict_count)?;
    revert.set_item("strategy", &report.strategy)?;
    Ok(revert)
}

/// Options for [`merge_preview`], as `Ledger.merge_preview` passes them.
#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct MergePreviewArgs {
    strategy: String,
    conflicts: bool,
    details: bool,
    changes: bool,
    changes_after: Option<String>,
    validate: bool,
    max_commits: Option<usize>,
    max_conflicts: Option<usize>,
    max_changes: Option<usize>,
}

pub(crate) fn merge_preview<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    target: &str,
    source: &str,
    args: MergePreviewArgs,
) -> PyResult<Bound<'py, PyDict>> {
    let (ledger_name, target_branch) = parts(target)?;
    let opts = MergePreviewOpts {
        max_commits: args.max_commits,
        max_conflict_keys: args.max_conflicts,
        include_conflicts: args.conflicts,
        include_conflict_details: args.details,
        conflict_strategy: strategy(&args.strategy)?,
        include_changes: args.changes,
        max_changes: args.max_changes,
        changes_after_subject: args.changes_after,
        include_validation: args.validate,
    };
    let (preview, compactors) = block_on(py, async {
        let preview = fluree
            .merge_preview_with(&ledger_name, source, Some(&target_branch), opts)
            .await?;
        // Conflict keys are namespace-coded; a key the target has never seen
        // takes the source's code for its namespace.
        let compactors = if preview.conflicts.keys.is_empty() {
            Vec::new()
        } else {
            let source_id = format!("{ledger_name}:{source}");
            vec![
                compactor(fluree, target).await?,
                compactor(fluree, &source_id).await?,
            ]
        };
        Ok::<_, fluree_db_api::ApiError>((preview, compactors))
    })?
    .map_err(api_error)?;
    merge_preview_to_py(py, &preview, &compactors)
}

fn merge_preview_to_py<'py>(
    py: Python<'py>,
    preview: &MergePreview,
    compactors: &[IriCompactor],
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("source", &preview.source)?;
    out.set_item("target", &preview.target)?;
    out.set_item("fast_forward", preview.fast_forward)?;
    out.set_item("mergeable", preview.mergeable)?;
    out.set_item("ancestor_t", preview.ancestor.as_ref().map(|a| a.t))?;
    out.set_item("ahead", summaries(py, &preview.ahead.commits)?)?;
    out.set_item("ahead_count", preview.ahead.count)?;
    out.set_item("behind", summaries(py, &preview.behind.commits)?)?;
    out.set_item("behind_count", preview.behind.count)?;
    out.set_item(
        "conflicts",
        conflicts(
            py,
            &preview.conflicts.keys,
            &preview.conflicts.details,
            compactors,
        )?,
    )?;
    out.set_item("conflict_count", preview.conflicts.count)?;
    out.set_item("violations", violations(preview.validation.as_ref()))?;
    match &preview.changes {
        Some(changes) => {
            let flakes = PyList::empty(py);
            for subject in &changes.entries {
                for f in subject.retracts.iter().chain(&subject.asserts) {
                    flakes.append(flake(py, f)?)?;
                }
            }
            out.set_item("changes", flakes)?;
            out.set_item("changes_after", changes.next_cursor.as_deref())?;
        }
        None => {
            out.set_item("changes", py.None())?;
            out.set_item("changes_after", py.None())?;
        }
    }
    Ok(out)
}

/// Options for [`revert_preview`], as `Ledger.revert_preview` passes them.
#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct RevertPreviewArgs {
    strategy: String,
    conflicts: bool,
    validate: bool,
    max_commits: Option<usize>,
    max_conflicts: Option<usize>,
}

pub(crate) fn revert_preview<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    ledger: &str,
    commits: &Bound<'py, PyList>,
    args: RevertPreviewArgs,
) -> PyResult<Bound<'py, PyDict>> {
    let (ledger_name, branch) = parts(ledger)?;
    let refs = commit_refs(commits)?;
    let opts = RevertPreviewOpts {
        max_commits: args.max_commits,
        max_conflict_keys: args.max_conflicts,
        include_conflicts: args.conflicts,
        conflict_strategy: strategy(&args.strategy)?,
        include_validation: args.validate,
    };
    let (preview, compactors) = block_on(py, async {
        let preview = fluree
            .revert_commits_preview_with(&ledger_name, &branch, refs, opts)
            .await?;
        let compactors = if preview.conflicts.keys.is_empty() {
            Vec::new()
        } else {
            vec![compactor(fluree, ledger).await?]
        };
        Ok::<_, fluree_db_api::ApiError>((preview, compactors))
    })?
    .map_err(api_error)?;
    revert_preview_to_py(py, &preview, &compactors)
}

fn revert_preview_to_py<'py>(
    py: Python<'py>,
    preview: &RevertPreview,
    compactors: &[IriCompactor],
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("revertable", preview.revertable)?;
    out.set_item("commits", summaries(py, &preview.reverted_commits)?)?;
    out.set_item("commit_count", preview.reverted_count)?;
    out.set_item(
        "conflicts",
        conflicts(py, &preview.conflicts.keys, &[], compactors)?,
    )?;
    out.set_item("conflict_count", preview.conflicts.count)?;
    out.set_item("violations", violations(preview.validation.as_ref()))?;
    Ok(out)
}

/// Decodes namespace-coded ids to whole IRIs.
async fn compactor(fluree: &Fluree, ledger: &str) -> fluree_db_api::Result<IriCompactor> {
    let db = fluree.db(ledger).await?;
    Ok(IriCompactor::from_namespaces(
        db.snapshot.shared_namespaces(),
    ))
}

fn summaries<'py>(py: Python<'py>, commits: &[CommitSummary]) -> PyResult<Vec<Bound<'py, PyDict>>> {
    commits.iter().map(|c| commit_summary(py, c)).collect()
}

fn violations(validation: Option<&ValidationSummary>) -> Option<&str> {
    validation.filter(|v| !v.conforms).map(|v| {
        v.report
            .as_deref()
            .unwrap_or("does not conform to the ledger's shapes")
    })
}

/// `{subject, predicate, graph, source, target}` per key; `source` and
/// `target` are the flakes each side wrote to it, when `details` has them.
fn conflicts<'py>(
    py: Python<'py>,
    keys: &[ConflictKey],
    details: &[ConflictDetail],
    compactors: &[IriCompactor],
) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let decode = |sid: &fluree_db_core::Sid| {
        compactors
            .iter()
            .find_map(|c| c.decode_sid(sid).ok())
            .ok_or_else(|| fluree_error(format!("cannot decode conflict key {sid:?}")))
    };
    keys.iter()
        .map(|key| {
            let conflict = PyDict::new(py);
            conflict.set_item("subject", decode(&key.s)?)?;
            conflict.set_item("predicate", decode(&key.p)?)?;
            conflict.set_item("graph", key.g.as_ref().map(decode).transpose()?)?;
            let detail = details.iter().find(|d| &d.key == key);
            let side = |values: Option<&Vec<fluree_db_api::ResolvedFlake>>| {
                values
                    .map(|flakes| {
                        flakes
                            .iter()
                            .map(|f| flake(py, f))
                            .collect::<PyResult<Vec<_>>>()
                    })
                    .transpose()
            };
            conflict.set_item("source", side(detail.map(|d| &d.source_values))?)?;
            conflict.set_item("target", side(detail.map(|d| &d.target_values))?)?;
            Ok(conflict)
        })
        .collect()
}
