//! Engine errors raised as the exception classes in `fluree/errors.py`.
//!
//! The classes live in Python so they can also derive from the matching
//! builtin (`LookupError`, `ValueError`, ...), which `create_exception!` cannot.

use fluree_db_api::ApiError;
use pyo3::prelude::*;

pub(crate) fn api_error(e: ApiError) -> PyErr {
    let status = e.status_code();
    raise(class_for_status(status), e.to_string(), Some(status))
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
    Python::attach(|py| {
        let exception = py
            .import("fluree.errors")
            .and_then(|errors| errors.getattr(class))
            .and_then(|class| class.call1((message,)))
            .and_then(|exception| {
                exception.setattr("status", status)?;
                Ok(exception)
            });
        match exception {
            Ok(exception) => PyErr::from_value(exception),
            Err(e) => e,
        }
    })
}
