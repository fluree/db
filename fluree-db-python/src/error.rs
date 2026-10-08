//! Engine errors raised as the exception classes in `fluree/errors.py`.
//!
//! The classes live in Python so they can also derive from the matching
//! builtin (`LookupError`, `ValueError`, ...), which `create_exception!` cannot.

use crate::convert::iri;
use fluree_db_api::{ApiError, TransactError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

/// An engine error as its Python exception, carrying the details the engine
/// keeps structured: a SHACL rejection's violations, a unique-constraint
/// clash, the `t`s of a commit conflict.
pub(crate) fn api_error(e: ApiError) -> PyErr {
    let status = e.status_code();
    let message = e.to_string();
    Python::attach(|py| {
        let details = PyDict::new(py);
        let class = match &e {
            ApiError::Transact(TransactError::ShaclViolation(violations)) => {
                details.set_item("_results", pythonize::pythonize(py, violations.results())?)?;
                "ShaclViolationError"
            }
            ApiError::Transact(TransactError::UniqueConstraintViolation {
                property,
                value,
                graph,
                existing_subject,
                new_subject,
            }) => {
                details.set_item("property", iri(py, property)?)?;
                details.set_item("value", value)?;
                let graph = (graph != "default").then(|| iri(py, graph)).transpose()?;
                details.set_item("graph", graph)?;
                details.set_item("existing_subject", iri(py, existing_subject)?)?;
                details.set_item("new_subject", iri(py, new_subject)?)?;
                "UniqueConstraintError"
            }
            ApiError::Transact(TransactError::CommitConflict { expected_t, head_t }) => {
                details.set_item("expected_t", expected_t)?;
                details.set_item("head_t", head_t)?;
                "ConflictError"
            }
            _ => class_for_status(status),
        };
        Ok::<_, PyErr>(raise_with(class, message, Some(status), &details))
    })
    .unwrap_or_else(|err| err)
}

pub(crate) fn class_for_status(status: u16) -> &'static str {
    match status {
        400 | 422 => "InvalidRequestError",
        401 | 403 => "PermissionDeniedError",
        404 => "NotFoundError",
        408 | 504 => "QueryTimeoutError",
        409 => "ConflictError",
        413 | 429 | 507 => "ResourceLimitError",
        _ => "FlureeError",
    }
}

pub(crate) fn raise_status(class: &str, message: String, status: u16) -> PyErr {
    raise(class, message, Some(status))
}

pub(crate) fn fluree_error(message: impl Into<String>) -> PyErr {
    raise("FlureeError", message.into(), None)
}

pub(crate) fn not_found(message: impl Into<String>) -> PyErr {
    raise("NotFoundError", message.into(), None)
}

pub(crate) fn invalid_request(message: impl Into<String>) -> PyErr {
    raise("InvalidRequestError", message.into(), None)
}

fn raise(class: &str, message: String, status: Option<u16>) -> PyErr {
    Python::attach(|py| raise_with(class, message, status, &PyDict::new(py)))
}

/// `class(message)` with `status` and each of `details` set on it.
fn raise_with(
    class: &str,
    message: String,
    status: Option<u16>,
    details: &Bound<'_, PyDict>,
) -> PyErr {
    let exception = details
        .py()
        .import("fluree.errors")
        .and_then(|errors| errors.getattr(class))
        .and_then(|class| class.call1((message,)))
        .and_then(|exception| {
            exception.setattr("status", status)?;
            for (name, value) in details.iter() {
                exception.setattr(name.cast::<PyString>()?, value)?;
            }
            Ok(exception)
        });
    match exception {
        Ok(exception) => PyErr::from_value(exception),
        Err(e) => e,
    }
}
