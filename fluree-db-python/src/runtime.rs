//! The engine's tokio runtime, shared by every connection in the process.

use crate::error::{fluree_error, raise_status};
use fluree_db_api::{QueryCancellation, QueryCancellationReason};
use pyo3::prelude::*;
use std::future::Future;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};
use tokio::runtime::{EnterGuard, Runtime};

/// How often a waiting query checks its deadline and Python's signals.
const POLL: Duration = Duration::from_millis(50);

/// The engine runtime of one process. A forked child inherits its parent's
/// runtime without the threads that drive it, so a child that uses the engine
/// starts a runtime of its own (except on macOS; see [`engine`]); the
/// inherited one is leaked, never dropped, since dropping it would wait on
/// threads that do not exist.
struct Engine {
    runtime: Runtime,
    pid: u32,
}

static ENGINE: AtomicPtr<Engine> = AtomicPtr::new(std::ptr::null_mut());
static STARTING: Mutex<()> = Mutex::new(());

fn engine() -> PyResult<&'static Engine> {
    let pid = std::process::id();
    if let Some(engine) = current(pid) {
        return Ok(engine);
    }
    // On macOS the system dispatch library, which Rust's timed waits use,
    // aborts in a child forked from a process that used it, so a forked child
    // cannot run an engine of its own.
    if cfg!(target_os = "macos") && !ENGINE.load(Ordering::Acquire).is_null() {
        return Err(fluree_error(
            "on macOS, a process forked after Fluree started cannot use it; use the \
             multiprocessing 'spawn' start method (the macOS default) or 'forkserver'",
        ));
    }
    let _starting = STARTING.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(engine) = current(pid) {
        return Ok(engine);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("fluree-engine")
        .build()
        .map_err(|e| fluree_error(format!("failed to start the Fluree engine runtime: {e}")))?;
    let engine: &'static Engine = Box::leak(Box::new(Engine { runtime, pid }));
    ENGINE.store(std::ptr::from_ref(engine).cast_mut(), Ordering::Release);
    Ok(engine)
}

/// This process's engine, if it has started one.
fn current(pid: u32) -> Option<&'static Engine> {
    // SAFETY: the pointer is null or a leaked `Engine`, never freed.
    let engine = unsafe { ENGINE.load(Ordering::Acquire).as_ref() }?;
    (engine.pid == pid).then_some(engine)
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

/// An engine value, usable only in the process that created it, and dropped
/// inside the runtime: engine `Drop` impls hand their cleanup to
/// `Handle::try_current()` and skip it outside one.
///
/// A forked child inherits the value but not the threads and locks its
/// state relies on, so there [`get`](Self::get) refuses it and dropping
/// leaks it.
pub(crate) struct InRuntime<T> {
    value: Option<T>,
    pid: u32,
}

impl<T> InRuntime<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            value: Some(value),
            pid: std::process::id(),
        }
    }

    pub(crate) fn get(&self) -> PyResult<&T> {
        self.check()?;
        Ok(self
            .value
            .as_ref()
            .expect("taken only by into_inner or drop"))
    }

    pub(crate) fn get_mut(&mut self) -> PyResult<&mut T> {
        self.check()?;
        Ok(self
            .value
            .as_mut()
            .expect("taken only by into_inner or drop"))
    }

    /// The value, for a caller that will drop it inside the runtime itself.
    pub(crate) fn into_inner(mut self) -> PyResult<T> {
        self.check()?;
        Ok(self.value.take().expect("taken only by into_inner or drop"))
    }

    fn check(&self) -> PyResult<()> {
        if self.pid == std::process::id() {
            return Ok(());
        }
        Err(fluree_error(
            "this was opened in the parent of a forked process; open a new connection \
             in this process",
        ))
    }
}

impl<T> Drop for InRuntime<T> {
    fn drop(&mut self) {
        let Some(value) = self.value.take() else {
            return;
        };
        match current(self.pid) {
            Some(engine) if self.pid == std::process::id() => {
                let _guard = engine.runtime.enter();
                drop(value);
            }
            Some(_) => std::mem::forget(value),
            None => drop(value),
        }
    }
}
