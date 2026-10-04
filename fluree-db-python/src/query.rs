//! Query execution controls (fuel limit, timeout, stats) shared by every
//! query entry point.

use crate::convert::{from_json, sparql_to_py, SparqlResult};
use crate::error::{api_error, raise_status};
use crate::runtime::block_on_cancellable;
use fluree_db_api::QueryCancellation;
use fluree_db_api::{ApiError, TrackedErrorResponse, TrackedQueryResponse, TrackingOptions};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json::Value as JsonValue;
use std::future::Future;
use std::time::Duration;

/// `{"max_fuel": float | None, "timeout": float | None, "stats": bool}`
#[derive(FromPyObject, Clone, Copy, Default)]
#[pyo3(from_item_all)]
pub(crate) struct Controls {
    max_fuel: Option<f64>,
    timeout: Option<f64>,
    stats: bool,
}

impl Controls {
    /// Engine tracking for a fuel limit or stats; `None` runs untracked.
    pub(crate) fn tracking(&self) -> Option<TrackingOptions> {
        (self.stats || self.max_fuel.is_some()).then(|| TrackingOptions {
            track_time: self.stats,
            track_fuel: true,
            track_policy: self.stats,
            max_fuel: self.max_fuel.map(fluree_db_core::tracking::fuel_to_micro),
        })
    }

    pub(crate) fn timeout(&self) -> Option<f64> {
        self.timeout
    }

    /// Run a query future built around a cancellation handle, enforcing the
    /// timeout and Ctrl-C through it.
    pub(crate) fn run<F>(
        self,
        py: Python<'_>,
        query: impl FnOnce(QueryCancellation) -> F,
    ) -> PyResult<Answer>
    where
        F: Future<Output = Result<Answer, Failure>> + Send,
    {
        let cancellation = QueryCancellation::new();
        let timeout = self.timeout.map(Duration::from_secs_f64);
        block_on_cancellable(py, &cancellation, timeout, query(cancellation.clone()))?
            .map_err(PyErr::from)
    }

    pub(crate) fn answer(
        &self,
        result: Result<TrackedQueryResponse, TrackedErrorResponse>,
    ) -> Result<Answer, Failure> {
        match result {
            Ok(response) => Ok(Answer {
                json: response.result,
                stats: self.stats.then_some(Stats {
                    fuel: response.fuel,
                    time: response.time,
                }),
            }),
            Err(e) => {
                // A fuel overrun reports 400 with no error type; the fuel it
                // reached is what identifies it.
                let fuel_exhausted = matches!(
                    (e.fuel, self.max_fuel),
                    (Some(used), Some(limit)) if used >= limit
                );
                Err(Failure::Tracked {
                    status: e.status,
                    message: e.error,
                    fuel_exhausted,
                })
            }
        }
    }
}

pub(crate) struct Stats {
    fuel: Option<f64>,
    time: Option<String>,
}

/// A formatted query result, with stats when they were asked for.
pub(crate) struct Answer {
    pub(crate) json: JsonValue,
    pub(crate) stats: Option<Stats>,
}

impl Answer {
    pub(crate) fn plain(json: JsonValue) -> Self {
        Self { json, stats: None }
    }

    /// The Python result — `(result, stats)` when stats were asked for. A
    /// SPARQL result decodes against its query text.
    pub(crate) fn into_py<'py>(
        self,
        py: Python<'py>,
        sparql: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let result = match sparql {
            Some(sparql) => sparql_to_py(py, &SparqlResult::new(sparql, self.json))?,
            None => from_json(py, &self.json)?,
        };
        match self.stats {
            None => Ok(result),
            Some(stats) => {
                let dict = PyDict::new(py);
                dict.set_item("fuel", stats.fuel)?;
                dict.set_item("time", stats.time)?;
                (result, dict).into_pyobject(py).map(Bound::into_any)
            }
        }
    }
}

pub(crate) enum Failure {
    Api(ApiError),
    Tracked {
        status: u16,
        message: String,
        fuel_exhausted: bool,
    },
}

impl From<ApiError> for Failure {
    fn from(e: ApiError) -> Self {
        Self::Api(e)
    }
}

impl From<Failure> for PyErr {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Api(e) => api_error(e),
            Failure::Tracked {
                message,
                fuel_exhausted: true,
                status,
            } => raise_status("ResourceLimitError", message, status),
            Failure::Tracked {
                status, message, ..
            } => raise_status(crate::error::class_for_status(status), message, status),
        }
    }
}

/// Execute a query builder under `controls` and `cancellation`: tracked when
/// a fuel limit or stats were asked for, formatted otherwise.
macro_rules! execute {
    ($controls:expr, $cancellation:expr, $builder:expr) => {{
        let controls: crate::query::Controls = $controls;
        let builder = $builder.cancellation($cancellation);
        match controls.tracking() {
            Some(tracking) => controls.answer(builder.tracking(tracking).execute_tracked().await),
            None => Ok(crate::query::Answer::plain(
                builder.execute_formatted().await?,
            )),
        }
    }};
}
pub(crate) use execute;
