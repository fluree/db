//! Engine log events, forwarded to Python's `logging` as the `fluree.engine`
//! logger.
//!
//! An engine thread never waits on the GIL: an event goes onto a bounded
//! queue that a forwarding thread drains, and is dropped when the queue is
//! full or the interpreter is shutting down. Events below the configured
//! level are disabled at their callsites, so a quiet level costs nothing on
//! the engine's hot paths.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyDict;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::Mutex;
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::metadata::Metadata;
use tracing::subscriber::Interest;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

/// How many events may wait for the forwarding thread before more are dropped.
const QUEUE: usize = 1024;

/// The most verbose level forwarded: 0 off, 1 error … 5 trace.
static LEVEL: AtomicU8 = AtomicU8::new(2);

struct Record {
    level: Level,
    target: String,
    message: String,
}

/// The queue to this process's forwarding thread, started on the first event
/// (and again in a forked child, which inherits no thread).
static QUEUE_TX: Mutex<Option<(u32, SyncSender<Record>)>> = Mutex::new(None);

static LOGGER: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn level_filter() -> LevelFilter {
    match LEVEL.load(Ordering::Relaxed) {
        0 => LevelFilter::OFF,
        1 => LevelFilter::ERROR,
        2 => LevelFilter::WARN,
        3 => LevelFilter::INFO,
        4 => LevelFilter::DEBUG,
        _ => LevelFilter::TRACE,
    }
}

fn python_level(level: Level) -> u8 {
    match level {
        Level::ERROR => 40,
        Level::WARN => 30,
        Level::INFO => 20,
        Level::DEBUG => 10,
        Level::TRACE => 5,
    }
}

struct Forward;

impl<S: Subscriber> Layer<S> for Forward {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if level_filter() >= *metadata.level() {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>, _: Context<'_, S>) -> bool {
        level_filter() >= *metadata.level()
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(level_filter())
    }

    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut text = Text::default();
        event.record(&mut text);
        send(Record {
            level: *event.metadata().level(),
            target: event.metadata().target().to_string(),
            message: text.0,
        });
    }
}

/// An event's message, then its other fields as `key=value`.
#[derive(Default)]
struct Text(String);

impl Visit for Text {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let rest = std::mem::take(&mut self.0);
            let _ = write!(self.0, "{value:?}");
            self.0.push_str(&rest);
        } else {
            let _ = write!(self.0, " {}={value:?}", field.name());
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0.insert_str(0, value);
        } else {
            let _ = write!(self.0, " {}={value}", field.name());
        }
    }
}

fn send(record: Record) {
    let pid = std::process::id();
    let mut queue = QUEUE_TX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if queue.as_ref().is_none_or(|(owner, _)| *owner != pid) {
        let (tx, rx) = mpsc::sync_channel::<Record>(QUEUE);
        let started = std::thread::Builder::new()
            .name("fluree-logging".into())
            .spawn(move || {
                for record in rx {
                    let forwarded = Python::try_attach(|py| forward(py, &record));
                    if forwarded.is_none() {
                        break; // The interpreter is gone.
                    }
                }
            });
        if started.is_err() {
            return;
        }
        *queue = Some((pid, tx));
    }
    if let Some((_, tx)) = queue.as_ref() {
        let _ = tx.try_send(record);
    }
}

fn forward(py: Python<'_>, record: &Record) {
    let logged = (|| -> PyResult<()> {
        let logger = LOGGER.get_or_try_init(py, || -> PyResult<_> {
            Ok(py
                .import("logging")?
                .call_method1("getLogger", ("fluree.engine",))?
                .unbind())
        })?;
        let extra = PyDict::new(py);
        extra.set_item("target", &record.target)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("extra", extra)?;
        logger.bind(py).call_method(
            "log",
            (python_level(record.level), &record.message),
            Some(&kwargs),
        )?;
        Ok(())
    })();
    // A failing handler must not take the forwarding thread down with it.
    if let Err(e) = logged {
        e.print(py);
    }
}

/// Route engine events to Python logging. Called once, at import.
pub(crate) fn install() {
    let _ = tracing_subscriber::registry().with(Forward).try_init();
}

/// Forward engine events at `level` and above: 0 off, 1 error, 2 warning,
/// 3 info, 4 debug, 5 trace.
#[pyfunction]
pub(crate) fn set_log_level(level: u8) {
    LEVEL.store(level.min(5), Ordering::Relaxed);
    tracing::callsite::rebuild_interest_cache();
}
