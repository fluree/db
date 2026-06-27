use fluree_db_api::{QueryCancellation, QueryCancellationReason, QueryExecutionOptions};
use std::future::Future;
use tokio::task::AbortHandle;

use crate::error::ServerError;

tokio::task_local! {
    static QUERY_EXECUTION_OPTIONS: QueryExecutionOptions;
}

struct QueryTimeoutGuard {
    abort: AbortHandle,
}

impl Drop for QueryTimeoutGuard {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// Fires `ClientDisconnected` on the shared cancellation handle when dropped
/// while still armed. Used by `run_query_task` for buffered requests, and by
/// the streaming endpoint hung off the response body stream so a client that
/// disconnects mid-execution cancels the producer at the next checkpoint.
pub(crate) struct QueryDisconnectGuard {
    cancellation: QueryCancellation,
    disarmed: bool,
}

impl QueryDisconnectGuard {
    pub(crate) fn new(cancellation: QueryCancellation) -> Self {
        Self {
            cancellation,
            disarmed: false,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for QueryDisconnectGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            self.cancellation
                .cancel_with(QueryCancellationReason::ClientDisconnected);
        }
    }
}

struct ServerQueryControl {
    cancellation: QueryCancellation,
    options: QueryExecutionOptions,
}

fn query_execution_control(timeout_ms: u64) -> ServerQueryControl {
    let cancellation = QueryCancellation::new();
    let mut options = QueryExecutionOptions::new().with_cancellation(cancellation.clone());

    if timeout_ms != 0 {
        let timer_cancellation = cancellation.clone();
        let timeout_task = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(timeout_ms)).await;
            timer_cancellation.cancel_with(QueryCancellationReason::Timeout);
        });
        let guard = QueryTimeoutGuard {
            abort: timeout_task.abort_handle(),
        };
        options = options.with_lifecycle_guard(guard);
    }

    ServerQueryControl {
        cancellation,
        options,
    }
}

/// Return the request-scoped execution options for the current server query task.
///
/// Most route code reaches this through small local wrappers, which keeps the
/// existing builder call sites simple while still sharing one cancellation
/// handle across all query execution performed for the request.
///
/// The `timeout_ms` fallback is used only when this is called outside a
/// [`run_query_task`] scope. Inside that scope, the already-installed
/// request-scoped options win and this argument is intentionally ignored.
pub(crate) fn current_query_execution_options(timeout_ms: u64) -> QueryExecutionOptions {
    QUERY_EXECUTION_OPTIONS
        .try_with(Clone::clone)
        .unwrap_or_else(|_| query_execution_control(timeout_ms).options)
}

/// Run server query work in a spawned task that can outlive the HTTP/MCP waiter.
///
/// If the waiter future is dropped before the query task completes, the
/// disconnect guard signals `ClientDisconnected` on the same handle installed in
/// [`QueryExecutionOptions`]. The spawned query task can then observe that signal
/// at cooperative cancellation checkpoints.
pub(crate) async fn run_query_task<T, Fut, Build>(
    timeout_ms: u64,
    build: Build,
) -> Result<T, ServerError>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, ServerError>> + Send + 'static,
    Build: FnOnce() -> Fut,
{
    let control = query_execution_control(timeout_ms);
    let mut disconnect_guard = QueryDisconnectGuard::new(control.cancellation.clone());
    let options = control.options;

    let handle = tokio::spawn(QUERY_EXECUTION_OPTIONS.scope(options, build()));
    let joined = handle.await;
    disconnect_guard.disarm();

    match joined {
        Ok(result) => result,
        Err(e) => Err(ServerError::Api(fluree_db_api::ApiError::Internal(
            format!("query task failed: {e}"),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{current_query_execution_options, run_query_task};
    use crate::error::ServerError;
    use fluree_db_core::QueryCancellation;
    use fluree_db_core::QueryCancellationReason;
    use tokio::sync::oneshot;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn dropping_waiter_signals_client_disconnect_to_query_task() {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (reason_tx, reason_rx) = oneshot::channel();

        let waiter = tokio::spawn(async move {
            run_query_task(0, || async move {
                let cancellation = current_query_execution_options(0)
                    .cancellation
                    .expect("server query task has cancellation handle");
                let _ = ready_tx.send(());

                loop {
                    if let Some(reason) = cancellation.reason() {
                        let _ = reason_tx.send(reason);
                        return Ok(());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
        });

        ready_rx.await.expect("query task should start");
        waiter.abort();

        assert_eq!(
            reason_rx
                .await
                .expect("query task should observe cancellation"),
            QueryCancellationReason::ClientDisconnected
        );
    }

    #[tokio::test]
    async fn completed_query_does_not_signal_client_disconnect() {
        let (handle_tx, handle_rx) = oneshot::channel();

        let result: Result<(), ServerError> = run_query_task(0, || async move {
            let cancellation = current_query_execution_options(0)
                .cancellation
                .expect("server query task has cancellation handle");
            let _ = handle_tx.send(cancellation);
            Ok(())
        })
        .await;

        result.expect("query should complete normally");
        let cancellation: QueryCancellation = handle_rx
            .await
            .expect("query should send cancellation handle");
        assert_eq!(cancellation.reason(), None);
    }

    #[tokio::test]
    async fn timeout_signals_timeout_to_query_task() {
        let reason = timeout(Duration::from_secs(1), async {
            run_query_task(1, || async move {
                let cancellation = current_query_execution_options(0)
                    .cancellation
                    .expect("server query task has cancellation handle");

                loop {
                    if let Some(reason) = cancellation.reason() {
                        return Ok(reason);
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
        })
        .await
        .expect("timeout test should not hang")
        .expect("query task should return observed reason");

        assert_eq!(reason, QueryCancellationReason::Timeout);
    }
}
