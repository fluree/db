//! One Iceberg data file's read, run on a task of its own for as long as the
//! scan that wants it is still there.

use std::future::Future;

use fluree_db_iceberg::io::ColumnBatch;
use fluree_db_query::error::{QueryError, Result as QueryResult};
use tokio::task::JoinHandle;

/// Aborts its task when dropped.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Run one data file's read, built by `read`, on its own task, so a scan's
/// reads run in parallel, and wait for it.
///
/// Dropping the returned future aborts the task. A scan streams its files'
/// reads through `buffer_unordered`, so dropping the scan's stream (its
/// consumer has the rows it needs, or the query was cancelled or timed out)
/// drops the reads in flight. A bare `tokio::spawn` left each of those reads
/// running to the end of its file, for no one.
///
/// The read is built on the first poll and spawned in the same step, so until
/// then the stream holds only what builds it: a scan's stream must be `Sync`,
/// and a pending read is not.
pub(crate) async fn read_in_own_task<R, F>(read: R) -> QueryResult<Vec<ColumnBatch>>
where
    R: FnOnce() -> F,
    F: Future<Output = QueryResult<Vec<ColumnBatch>>> + Send + 'static,
{
    let mut task = AbortOnDrop(tokio::spawn(read()));
    (&mut task.0)
        .await
        .map_err(|e| QueryError::Internal(format!("Parquet read worker failed: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// Counts the reads whose future was dropped before it finished.
    struct CountOnDrop(Arc<AtomicUsize>);

    impl Drop for CountOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A read that never finishes on its own, counting itself when started and
    /// when dropped.
    fn endless_read(
        started: &Arc<AtomicUsize>,
        dropped: &Arc<AtomicUsize>,
    ) -> impl Future<Output = QueryResult<Vec<ColumnBatch>>> + Send + 'static {
        let started = Arc::clone(started);
        let dropped = CountOnDrop(Arc::clone(dropped));
        async move {
            let _dropped = dropped;
            started.fetch_add(1, Ordering::SeqCst);
            futures::future::pending::<()>().await;
            unreachable!("an endless read never finishes")
        }
    }

    async fn wait_for(count: &AtomicUsize, n: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while count.load(Ordering::SeqCst) < n {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("count stayed at {}", count.load(Ordering::SeqCst)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_read_aborts_its_task() {
        let (started, dropped) = (Arc::default(), Arc::default());
        let mut read = Box::pin(read_in_own_task(|| endless_read(&started, &dropped)));
        assert!(futures::poll!(&mut read).is_pending());
        wait_for(&started, 1).await;

        drop(read);
        wait_for(&dropped, 1).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_finished_read_returns_its_batches_and_errors() {
        let ok = read_in_own_task(|| async { Ok(Vec::new()) }).await.unwrap();
        assert!(ok.is_empty());
        let err = read_in_own_task(|| async { Err(QueryError::Internal("no file".into())) })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no file"), "{err}");
    }

    /// The shape every scan reads its files in: up to `concurrency` reads in
    /// flight under `buffer_unordered`. Dropping the stream aborts all of them,
    /// and starts no more.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_scan_stream_aborts_the_reads_in_flight() {
        let (started, dropped) = (Arc::<AtomicUsize>::default(), Arc::default());
        let reads = {
            let (started, dropped) = (Arc::clone(&started), Arc::clone(&dropped));
            futures::stream::iter(0..16)
                .map(move |_| {
                    let (started, dropped) = (Arc::clone(&started), Arc::clone(&dropped));
                    read_in_own_task(move || endless_read(&started, &dropped))
                })
                .buffer_unordered(4)
        };
        let mut reads = Box::pin(reads);
        assert!(futures::poll!(reads.next()).is_pending());
        wait_for(&started, 4).await;

        drop(reads);
        wait_for(&dropped, 4).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            started.load(Ordering::SeqCst),
            4,
            "no read starts after the drop"
        );
    }
}
