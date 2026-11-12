//! One Iceberg data file's read, run on a task of its own for as long as the
//! scan that wants it is still there.

use std::future::Future;

use fluree_db_iceberg::io::ColumnBatch;
use fluree_db_query::error::{QueryError, Result as QueryResult};
use tokio_util::task::AbortOnDropHandle;

/// Spawn one data file's `read` on its own task, so a scan's reads run in
/// parallel, and return a future of its result that aborts the task when it is
/// dropped.
///
/// A scan streams its files' reads through `buffer_unordered`, so dropping the
/// scan's stream (its consumer has the rows it needs, or a cancelled query
/// unwinds) drops the reads in flight. A bare `tokio::spawn` left each of those
/// reads running to the end of its file, for no one.
///
/// It spawns the read it is given right away, so the future it returns holds
/// only the task's handle and is `Sync` whatever the read is: a scan's stream
/// must be `Sync`, and a pending read is not. That fits both a fan-out that
/// builds the read where it spawns it and one that builds it ahead and holds
/// it in a `Mutex` until then, as #2315's `read_files_in_order` does. Once
/// #2315 lands, all four fan-outs read through `read_files_in_order`, and this
/// call (and `a_dropped_scan_stream_aborts_the_reads_in_flight`, driving
/// `read_files_in_order` under both of its combinators) belongs there.
pub(crate) fn spawn_file_read<F>(
    read: F,
) -> impl Future<Output = QueryResult<Vec<ColumnBatch>>> + Send + Sync + 'static
where
    F: Future<Output = QueryResult<Vec<ColumnBatch>>> + Send + 'static,
{
    let task = AbortOnDropHandle::new(tokio::spawn(read));
    async move {
        task.await
            .map_err(|e| QueryError::Internal(format!("Parquet read worker failed: {e}")))?
    }
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
        let read = spawn_file_read(endless_read(&started, &dropped));
        wait_for(&started, 1).await;

        drop(read);
        wait_for(&dropped, 1).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_finished_read_returns_its_batches_and_errors() {
        let ok = spawn_file_read(async { Ok(Vec::new()) }).await.unwrap();
        assert!(ok.is_empty());
        let err = spawn_file_read(async { Err(QueryError::Internal("no file".into())) })
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
                .map(move |_| spawn_file_read(endless_read(&started, &dropped)))
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

    fn assert_send_sync<T: Send + Sync>(_: &T) {}

    /// A read that is `Send` but not `Sync` (it holds a `Cell` across an
    /// await) still gives a `Sync` future, both called directly and from a
    /// future that keeps the built read in a `Mutex` until it spawns it, the
    /// way #2315's `read_files_in_order` keeps its reads.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_read_future_is_sync_for_a_read_that_is_not() {
        async fn not_sync_read() -> QueryResult<Vec<ColumnBatch>> {
            let held = std::cell::Cell::new(0);
            tokio::task::yield_now().await;
            held.set(1);
            Ok(Vec::new())
        }

        let direct = spawn_file_read(not_sync_read());
        assert_send_sync(&direct);
        assert!(direct.await.unwrap().is_empty());

        let pending = std::sync::Mutex::new(not_sync_read());
        let held_ahead = async move {
            let read = pending
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            spawn_file_read(read).await
        };
        assert_send_sync(&held_ahead);
        assert!(held_ahead.await.unwrap().is_empty());
    }
}
