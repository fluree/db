//! The engine's tokio runtime, shared by every connection in the process.

use crate::error::{fluree_error, raise_status};
use fluree_db_api::{QueryCancellation, QueryCancellationReason};
use pyo3::prelude::*;
use std::future::Future;
use std::ops::Deref;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::runtime::{EnterGuard, Runtime};

/// How often a waiting query checks its deadline and Python's signals.
const POLL: Duration = Duration::from_millis(50);

struct Engine {
    runtime: Runtime,
    pid: u32,
}

static ENGINE: OnceLock<Result<Engine, String>> = OnceLock::new();

fn engine() -> PyResult<&'static Engine> {
    let engine = ENGINE
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("fluree-engine")
                .build()
                .map(|runtime| Engine {
                    runtime,
                    pid: std::process::id(),
                })
                .map_err(|e| format!("failed to start the Fluree engine runtime: {e}"))
        })
        .as_ref()
        .map_err(|e| fluree_error(e.clone()))?;
    // A forked child inherits the runtime's state but none of its threads.
    if engine.pid != std::process::id() {
        return Err(fluree_error(
            "Fluree cannot be used in a process forked after the engine started; \
             use the multiprocessing 'spawn' or 'forkserver' start method",
        ));
    }
    Ok(engine)
}

/// Run an engine future to completion with the GIL released.
///
/// The call cannot be interrupted part way: a signal that arrives meanwhile
/// (Ctrl-C) is handled by Python as soon as it returns.
pub(crate) fn block_on<F>(py: Python<'_>, fut: F) -> PyResult<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    let runtime = &engine()?.runtime;
    Ok(py.detach(|| runtime.block_on(fut)))
}

/// Run a query future that observes `cancellation`, cancelling it at the
/// `timeout` or when a signal handler raises (Ctrl-C).
///
/// Query operators can run long stretches without yielding, so a timer racing
/// the future cannot interrupt them. The query instead runs on a worker thread
/// while this thread watches the clock and Python's signals, and stops it
/// through the engine's cooperative cancellation checkpoints.
pub(crate) fn block_on_cancellable<F>(
    py: Python<'_>,
    cancellation: &QueryCancellation,
    timeout: Option<Duration>,
    fut: F,
) -> PyResult<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    let runtime = &engine()?.runtime;
    let deadline = timeout.map(|t| Instant::now() + t);
    py.detach(|| {
        std::thread::scope(|scope| {
            let (done, finished) = mpsc::sync_channel(1);
            scope.spawn(move || {
                let _ = done.send(runtime.block_on(fut));
            });
            let mut interrupted: Option<PyErr> = None;
            loop {
                match finished.recv_timeout(POLL) {
                    Ok(output) => return interrupted.map_or(Ok(output), Err),
                    // The worker panicked; leaving the scope re-raises it.
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err(
                            interrupted.unwrap_or_else(|| fluree_error("query worker failed"))
                        )
                    }
                    Err(RecvTimeoutError::Timeout) if interrupted.is_none() => {
                        if deadline.is_some_and(|d| Instant::now() >= d) {
                            cancellation.cancel_with(QueryCancellationReason::Timeout);
                            let seconds = timeout.unwrap_or_default().as_secs_f64();
                            interrupted = Some(raise_status(
                                "QueryTimeoutError",
                                format!("query exceeded its {seconds}s timeout and was cancelled"),
                                408,
                            ));
                        } else {
                            // A method path is not lifetime-generic enough for `attach`.
                            #[allow(clippy::redundant_closure_for_method_calls)]
                            let signals = Python::attach(|py| py.check_signals());
                            if let Err(signal) = signals {
                                cancellation.cancel();
                                interrupted = Some(signal);
                            }
                        }
                    }
                    // Cancelled; wait for the query to reach a checkpoint.
                    Err(RecvTimeoutError::Timeout) => {}
                }
            }
        })
    })
}

pub(crate) fn runtime() -> PyResult<&'static Runtime> {
    Ok(&engine()?.runtime)
}

/// Enter the runtime for synchronous engine calls that spawn tasks (builders).
pub(crate) fn enter() -> PyResult<EnterGuard<'static>> {
    Ok(engine()?.runtime.enter())
}

/// An engine value dropped inside the runtime: engine `Drop` impls hand their
/// cleanup to `Handle::try_current()` and skip it outside one.
pub(crate) struct InRuntime<T>(Option<T>);

impl<T> InRuntime<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(Some(value))
    }

    pub(crate) fn get_mut(&mut self) -> &mut T {
        self.0.as_mut().expect("taken only by drop")
    }
}

impl<T> Deref for InRuntime<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.0.as_ref().expect("taken only by drop")
    }
}

impl<T> Drop for InRuntime<T> {
    fn drop(&mut self) {
        let _guard = ENGINE
            .get()
            .and_then(|engine| engine.as_ref().ok())
            .map(|engine| engine.runtime.enter());
        drop(self.0.take());
    }
}
