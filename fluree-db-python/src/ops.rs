//! Operations: SHACL validation, indexing, integrity checks, index storage
//! sweeps. Every function takes a canonical `name:branch` id and returns
//! plain dicts; `fluree/_records.py` builds the result objects.

use crate::convert::{from_json, to_jsonld};
use crate::error::{api_error, fluree_error, invalid_request, raise_status};
use crate::query::{check_max_fuel, timeout_duration};
use crate::runtime::{block_on, block_on_cancellable};
use fluree_db_api::validate::{ShapesSource, ValidateOptions};
use fluree_db_api::{
    ApiError, Fluree, IndexPhase, ReindexOptions, TriggerIndexOptions, VerifySeverity,
};
use fluree_db_core::ledger_id::split_ledger_id;
use fluree_db_core::tracking::FuelExceededError;
use fluree_db_core::QueryCancellation;
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Options for [`validate`], as `Ledger.validate` passes them.
#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct ValidateArgs {
    /// `"attached"`, `"graph"` (`shapes` is its IRI), `"jsonld"`, or `"turtle"`.
    shapes_kind: String,
    shapes: Option<Py<PyAny>>,
    graph: Option<String>,
    include_attached: bool,
    max_fuel: Option<f64>,
    timeout: Option<f64>,
}

pub(crate) fn validate<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    ledger: &str,
    args: ValidateArgs,
) -> PyResult<Bound<'py, PyAny>> {
    let shapes = args.shapes.as_ref().map(|s| s.bind(py));
    let text = || -> PyResult<String> {
        shapes
            .ok_or_else(|| invalid_request("shapes are missing"))?
            .extract()
    };
    let shapes = match args.shapes_kind.as_str() {
        "attached" => ShapesSource::Attached,
        "graph" => ShapesSource::Graph(text()?),
        "turtle" => ShapesSource::InlineTurtle(text()?),
        "jsonld" => ShapesSource::InlineJsonLd(to_jsonld(
            shapes.ok_or_else(|| invalid_request("shapes are missing"))?,
        )?),
        other => return Err(invalid_request(format!("unknown shapes source {other:?}"))),
    };
    check_max_fuel(args.max_fuel)?;
    let cancellation = QueryCancellation::new();
    let options = ValidateOptions {
        graph: args.graph,
        shapes,
        include_attached: args.include_attached,
        max_fuel: args.max_fuel.map(fluree_db_core::tracking::fuel_to_micro),
        cancellation: Some(cancellation.clone()),
    };
    let timeout = timeout_duration(args.timeout)?;
    let report = block_on_cancellable(
        py,
        &cancellation,
        timeout,
        fluree.validate_ledger(ledger, &options),
    )?
    .map_err(|e| {
        // Reported as a 400, as a tracked query's overrun is; the typed
        // cause is what says it was the fuel limit.
        if fuel_exhausted(&e) {
            raise_status("ResourceLimitError", e.to_string(), e.status_code())
        } else {
            api_error(e)
        }
    })?;
    let json = serde_json::to_value(&report).map_err(|e| fluree_error(e.to_string()))?;
    from_json(py, &json)
}

fn fuel_exhausted(e: &ApiError) -> bool {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(error) = cause {
        if error.is::<FuelExceededError>() {
            return true;
        }
        cause = error.source();
    }
    false
}

pub(crate) fn index_status<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    ledger: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let status = block_on(py, fluree.index_status(ledger))?.map_err(api_error)?;
    let out = PyDict::new(py);
    out.set_item("index_t", status.index_t)?;
    out.set_item("commit_t", status.commit_t)?;
    out.set_item("enabled", status.indexing_enabled)?;
    let phase = match status.phase {
        IndexPhase::Idle => "idle",
        IndexPhase::Pending => "pending",
        IndexPhase::InProgress => "in_progress",
    };
    out.set_item("phase", phase)?;
    out.set_item("error", status.last_error)?;
    Ok(out)
}

/// Index everything committed so far and wait for it; returns the index `t`.
pub(crate) fn index(
    py: Python<'_>,
    fluree: &Fluree,
    ledger: &str,
    timeout: Option<f64>,
) -> PyResult<i64> {
    let opts = TriggerIndexOptions {
        timeout_ms: timeout_duration(timeout)?.map(|t| {
            t.as_nanos()
                .div_ceil(1_000_000)
                .try_into()
                .unwrap_or(u64::MAX)
        }),
    };
    block_on(py, fluree.trigger_index(ledger, opts))?
        .map(|result| result.index_t)
        .map_err(api_error)
}

/// Rebuild the index from the commit chain; returns the index `t`.
pub(crate) fn reindex(py: Python<'_>, fluree: &Fluree, ledger: &str) -> PyResult<i64> {
    block_on(py, fluree.reindex(ledger, ReindexOptions::default()))?
        .map(|result| result.index_t)
        .map_err(api_error)
}

pub(crate) fn verify<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    ledger: &str,
    max_commits: Option<usize>,
) -> PyResult<Bound<'py, PyDict>> {
    let report = block_on(py, fluree.verify_ledger(ledger, max_commits))?.map_err(api_error)?;
    let out = PyDict::new(py);
    out.set_item("severity", severity_name(report.severity()))?;
    out.set_item("head_t", report.head_t)?;
    out.set_item("index_t", report.index_t)?;
    out.set_item("commits_checked", report.commits_checked)?;
    out.set_item("truncated", report.truncated)?;
    let problems =
        serde_json::to_value(&report.problems).map_err(|e| fluree_error(e.to_string()))?;
    out.set_item("problems", from_json(py, &problems)?)?;
    Ok(out)
}

fn severity_name(severity: VerifySeverity) -> &'static str {
    match severity {
        VerifySeverity::Healthy => "healthy",
        VerifySeverity::Provenance => "provenance",
        VerifySeverity::Chain => "chain",
    }
}

/// Delete index files no index root references any more, across every branch
/// of `ledger`'s ledger (they share dictionary files). A dry run only counts
/// them.
pub(crate) fn sweep<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    ledger: &str,
    dry_run: bool,
) -> PyResult<Bound<'py, PyDict>> {
    let (name, _) = split_ledger_id(ledger).map_err(|e| invalid_request(e.to_string()))?;
    let out = PyDict::new(py);
    out.set_item("dry_run", dry_run)?;
    if dry_run {
        let plan = block_on(py, fluree.plan_index_sweep(&name))?.map_err(api_error)?;
        out.set_item("orphans", plan.orphans.len())?;
        out.set_item("reclaimed", 0)?;
        out.set_item("failures", Vec::<(String, String)>::new())?;
    } else {
        let result = block_on(py, fluree.sweep_index_storage(&name))?.map_err(api_error)?;
        out.set_item("orphans", result.reclaimed + result.failures.len())?;
        out.set_item("reclaimed", result.reclaimed)?;
        out.set_item("failures", result.failures)?;
    }
    Ok(out)
}
