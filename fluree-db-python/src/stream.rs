//! Streaming SELECT results: rows arrive as the query produces them.
//!
//! The engine's producer runs on the runtime and writes NDJSON records (`head`,
//! `row`..., then `end` or `error`) into a bounded channel; a full channel
//! pauses it. Python pulls rows in batches. Dropping the stream, or closing it,
//! cancels the producer.

use crate::convert::term;
use crate::error::{class_for_status, raise_status};
use crate::runtime::{runtime, InRuntime};
use bytes::Bytes;
use fluree_db_api::QueryCancellation;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use serde_json::Value as JsonValue;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Records buffered between the producer and Python.
pub(crate) const CHANNEL_DEPTH: usize = 64;

/// How often a waiting `next_batch` checks its deadline and Python's signals.
const POLL: Duration = Duration::from_millis(50);

#[pyclass(frozen, module = "fluree._fluree")]
pub(crate) struct RowStream {
    records: Mutex<Option<InRuntime<mpsc::Receiver<Bytes>>>>,
    cancellation: QueryCancellation,
    columns: Mutex<Option<Vec<String>>>,
    timeout: Option<f64>,
    deadline: Option<Instant>,
}

impl RowStream {
    pub(crate) fn new(
        records: mpsc::Receiver<Bytes>,
        cancellation: QueryCancellation,
        columns: Option<Vec<String>>,
        timeout: Option<f64>,
    ) -> Self {
        Self {
            records: Mutex::new(Some(InRuntime::new(records))),
            cancellation,
            columns: Mutex::new(columns),
            timeout,
            deadline: timeout.map(|t| Instant::now() + Duration::from_secs_f64(t)),
        }
    }

    fn finish(&self) {
        self.cancellation.cancel();
        self.records.lock().expect("stream lock").take();
    }
}

impl Drop for RowStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[pymethods]
impl RowStream {
    /// Column names in projection order. Known once the first batch is read
    /// for `SELECT *`.
    #[getter]
    fn columns(&self) -> Option<Vec<String>> {
        self.columns.lock().expect("stream lock").clone()
    }

    /// Up to `max` rows, waiting for at least one; `None` once the stream ends.
    fn next_batch<'py>(
        &self,
        py: Python<'py>,
        max: usize,
    ) -> PyResult<Option<Vec<Bound<'py, PyTuple>>>> {
        let runtime = runtime()?;
        let mut guard = self.records.lock().expect("stream lock");
        let Some(records) = guard.as_mut() else {
            return Ok(None);
        };
        // A stream that never idles would otherwise never see its deadline.
        if self.deadline.is_some_and(|d| Instant::now() >= d) {
            drop(guard);
            self.finish();
            return Err(timeout_error(self.timeout));
        }
        let records = records.get_mut();

        let mut lines: Vec<Bytes> = Vec::new();
        let mut closed = false;
        // Wait for one record, then take whatever else is already buffered.
        let waited = py.detach(|| -> PyResult<()> {
            loop {
                let next =
                    runtime.block_on(async { tokio::time::timeout(POLL, records.recv()).await });
                match next {
                    Ok(Some(line)) => {
                        lines.push(line);
                        break;
                    }
                    Ok(None) => {
                        closed = true;
                        break;
                    }
                    Err(_) => {
                        if self.deadline.is_some_and(|d| Instant::now() >= d) {
                            return Err(timeout_error(self.timeout));
                        }
                        // A method path is not lifetime-generic enough for `attach`.
                        #[allow(clippy::redundant_closure_for_method_calls)]
                        Python::attach(|py| py.check_signals())?;
                    }
                }
            }
            while lines.len() < max {
                match records.try_recv() {
                    Ok(line) => lines.push(line),
                    Err(_) => break,
                }
            }
            Ok(())
        });
        if let Err(e) = waited {
            drop(guard);
            self.finish();
            return Err(e);
        }
        drop(guard);

        let mut rows = Vec::with_capacity(lines.len());
        let mut ended = false;
        // A chunk carries one or more newline-terminated records.
        let records = lines
            .iter()
            .flat_map(|chunk| chunk.split(|b| *b == b'\n'))
            .filter(|line| !line.is_empty());
        for line in records {
            let record: JsonValue = serde_json::from_slice(line)
                .map_err(|e| raise_status("FlureeError", format!("bad stream record: {e}"), 500))?;
            match record["type"].as_str() {
                Some("head") => {
                    let mut columns = self.columns.lock().expect("stream lock");
                    if columns.is_none() {
                        *columns = Some(
                            record["vars"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .filter_map(|v| v.as_str().map(str::to_string))
                                .collect(),
                        );
                    }
                }
                Some("row") => {
                    let columns = self.columns.lock().expect("stream lock");
                    let cells = columns
                        .iter()
                        .flatten()
                        .map(|c| record["row"].get(c).map(|t| term(py, t)).transpose())
                        .collect::<PyResult<Vec<_>>>()?;
                    rows.push(PyTuple::new(py, cells)?);
                }
                Some("end") => ended = true,
                Some("error") => {
                    self.finish();
                    return Err(stream_error(&record["error"], self.timeout));
                }
                _ => {}
            }
        }
        if closed && !ended {
            // Only an `end` or `error` record completes a stream.
            self.finish();
            return Err(raise_status(
                "FlureeError",
                "query stream ended without completing".to_string(),
                500,
            ));
        }
        if ended {
            self.finish();
        }
        Ok((!rows.is_empty() || !ended).then_some(rows))
    }

    /// Stop the query and release its resources.
    fn close(&self) {
        self.finish();
    }
}

fn timeout_error(timeout: Option<f64>) -> PyErr {
    let seconds = timeout.unwrap_or_default();
    raise_status(
        "QueryTimeoutError",
        format!("query exceeded its {seconds}s timeout and was cancelled"),
        408,
    )
}

fn stream_error(error: &JsonValue, timeout: Option<f64>) -> PyErr {
    let message = error["message"]
        .as_str()
        .unwrap_or("query failed")
        .to_string();
    match error["code"].as_str() {
        Some("fuel_exhausted" | "resource_limit") => {
            raise_status("ResourceLimitError", message, 400)
        }
        Some("timeout") => timeout_error(timeout),
        Some("invalid_query" | "r2rml_unsupported_pattern") => {
            raise_status("InvalidRequestError", message, 400)
        }
        _ => raise_status(class_for_status(500), message, 500),
    }
}
