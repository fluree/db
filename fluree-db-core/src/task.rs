//! Faithful reporting of why a joined tokio task produced no value.
//!
//! `tokio::task::JoinError` is either a panic or a cancellation. Its `Display`
//! already says which (`task N was cancelled` / `task N panicked with message
//! ...`), but a call site that writes `format!("{task} panicked: {e}")` turns
//! the routine cancellation of a `spawn_blocking` job during runtime shutdown
//! into a phantom panic in the logs. Classify first, then label.

use std::any::Any;

/// Why a spawned task did not return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskFailure {
    /// The task was aborted, or never started because the runtime was
    /// shutting down when it was spawned or before it was picked up.
    Cancelled,
    /// The task panicked. Carries the payload text for `&str` and `String`
    /// payloads, a fixed marker otherwise.
    Panicked(String),
}

impl From<tokio::task::JoinError> for TaskFailure {
    fn from(e: tokio::task::JoinError) -> Self {
        match e.try_into_panic() {
            Ok(payload) => Self::Panicked(panic_message(payload)),
            Err(_) => Self::Cancelled,
        }
    }
}

impl TaskFailure {
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    /// One-line description naming `task` (e.g. `"index build task"`). Only
    /// the panic arm contains the word "panicked", so log filters keyed on it
    /// stay meaningful.
    pub fn describe(&self, task: &str) -> String {
        match self {
            Self::Cancelled => {
                format!("{task} was cancelled before it finished (runtime shutdown or abort)")
            }
            Self::Panicked(message) => format!("{task} panicked: {message}"),
        }
    }
}

/// Best-effort text of a panic payload (from `catch_unwind` or
/// `JoinError::into_panic`): `panic!("literal")` yields `&'static str`,
/// `panic!("{fmt}")` yields `String`; anything else gets a fixed marker.
pub fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "non-string panic payload".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panic_message_extracts_static_str() {
        let payload: Box<dyn Any + Send> = Box::new("kaboom");
        assert_eq!(panic_message(payload), "kaboom");
    }

    #[test]
    fn panic_message_extracts_string() {
        let payload: Box<dyn Any + Send> = Box::new(String::from("formatted: 42"));
        assert_eq!(panic_message(payload), "formatted: 42");
    }

    #[test]
    fn panic_message_falls_back_for_unknown_payload() {
        let payload: Box<dyn Any + Send> = Box::new(42u32);
        assert_eq!(panic_message(payload), "non-string panic payload");
    }

    #[tokio::test]
    async fn aborted_task_is_cancelled_not_panicked() {
        let handle = tokio::spawn(std::future::pending::<()>());
        handle.abort();
        let failure = TaskFailure::from(handle.await.unwrap_err());
        assert!(failure.is_cancelled());
        let text = failure.describe("probe task");
        assert!(text.contains("cancelled"), "{text}");
        assert!(!text.contains("panicked"), "{text}");
    }

    /// The production shape: a `spawn_blocking` issued while the runtime is
    /// shutting down is refused by the blocking pool and its handle resolves
    /// `Cancelled` immediately — nothing panicked.
    #[test]
    fn spawn_blocking_during_shutdown_is_cancelled() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let handle = rt.handle().clone();
        rt.shutdown_background();
        let join = handle.spawn_blocking(|| 1);
        let failure = TaskFailure::from(futures::executor::block_on(join).unwrap_err());
        assert_eq!(failure, TaskFailure::Cancelled);
    }

    #[tokio::test]
    async fn str_panic_keeps_payload() {
        let err = tokio::task::spawn_blocking(|| -> () { panic!("kaboom") })
            .await
            .unwrap_err();
        let failure = TaskFailure::from(err);
        assert_eq!(failure, TaskFailure::Panicked("kaboom".into()));
        assert_eq!(
            failure.describe("probe task"),
            "probe task panicked: kaboom"
        );
    }

    #[tokio::test]
    async fn string_panic_keeps_payload() {
        let err = tokio::task::spawn_blocking(|| -> () { panic!("code {}", 7) })
            .await
            .unwrap_err();
        assert_eq!(
            TaskFailure::from(err),
            TaskFailure::Panicked("code 7".into())
        );
    }

    #[tokio::test]
    async fn non_string_panic_is_still_reported_as_panic() {
        let err = tokio::task::spawn_blocking(|| -> () { std::panic::panic_any(42u32) })
            .await
            .unwrap_err();
        let failure = TaskFailure::from(err);
        assert!(!failure.is_cancelled());
        assert!(failure.describe("probe task").contains("panicked"));
    }
}
