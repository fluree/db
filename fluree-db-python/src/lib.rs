//! Native core of the `fluree` Python package.
//!
//! The public API lives in Python (`python/fluree/`); this module is the thin
//! layer under it. Design:
//!
//! - **Plain data across the boundary.** Methods take ledger ids, JSON-able
//!   objects and strings, and return dicts, lists and term tuples. Python owns
//!   argument handling, defaults, and the result objects users see.
//! - **One engine runtime per process.** A multi-thread tokio runtime starts on
//!   first use and serves every connection. The engine needs one even for
//!   synchronous builder calls, and its index reads bridge sync to async in a
//!   way that is only cheap on a multi-thread runtime.
//! - **The GIL is released for every engine call**, and the call wakes
//!   periodically to deliver signals, so Ctrl-C cancels a running query.
//! - **Snapshots are frozen.** `Snapshot` pins one `GraphDb`; every query on it
//!   sees the same state however the ledger moves on.

mod branch;
mod connection;
mod convert;
mod cypher;
mod error;
mod ops;
mod query;
mod runtime;
mod stream;
mod transaction;

use pyo3::prelude::*;

#[pymodule]
fn _fluree(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<connection::Connection>()?;
    m.add_class::<connection::Snapshot>()?;
    m.add_class::<stream::RowStream>()?;
    m.add_class::<transaction::Transaction>()?;
    m.add_class::<query::Canceller>()?;
    m.add_class::<cypher::CypherTransaction>()?;
    Ok(())
}
