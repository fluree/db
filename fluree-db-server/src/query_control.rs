use fluree_db_api::{QueryCancellation, QueryCancellationReason, QueryExecutionOptions};
use tokio::task::AbortHandle;

struct QueryTimeoutGuard {
    abort: AbortHandle,
}

impl Drop for QueryTimeoutGuard {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// Build execution options for server-owned query requests.
///
/// The timeout is signalled by an external Tokio task, and the guard aborts that
/// timer when the request finishes. Client disconnects are handled by normal
/// HTTP/MCP request-future cancellation rather than this cooperative signal.
pub(crate) fn query_execution_options(timeout_ms: u64) -> QueryExecutionOptions {
    if timeout_ms == 0 {
        return QueryExecutionOptions::default();
    }

    let cancellation = QueryCancellation::new();
    let timer_cancellation = cancellation.clone();
    let timeout_task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(timeout_ms)).await;
        timer_cancellation.cancel_with(QueryCancellationReason::Timeout);
    });
    let guard = QueryTimeoutGuard {
        abort: timeout_task.abort_handle(),
    };

    QueryExecutionOptions::new()
        .with_cancellation(cancellation)
        .with_lifecycle_guard(guard)
}
