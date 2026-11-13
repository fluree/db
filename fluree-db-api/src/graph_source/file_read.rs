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
/// Every scan reads its files through `r2rml::read_files_in_order`, which
/// spawns each read through here and awaits it under `buffered` or
/// `buffer_unordered`. Dropping the scan's stream (its consumer has the rows it
/// needs, or a cancelled query unwinds) drops the reads in flight. A bare
/// `tokio::spawn` left each of those reads running to the end of its file, for
/// no one.
///
/// It spawns the read it is given right away, so the future it returns holds
/// only the task's handle and is `Sync` whatever the read is: a scan's stream
/// must be `Sync`, and a pending read is not. `read_files_in_order` builds each
/// read ahead and holds it in a `Mutex` until it spawns it here.
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

/// Reads that count when they start and when they are dropped, for the tests
/// here and `read_files_in_order`'s in `r2rml`.
#[cfg(test)]
pub(crate) mod test_reads {
    use super::*;
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
    pub(crate) fn endless_read(
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

    /// Wait (up to 10 s) until `count` reaches `n`.
    pub(crate) async fn wait_for(count: &AtomicUsize, n: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while count.load(Ordering::SeqCst) < n {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("count stayed at {}", count.load(Ordering::SeqCst)));
    }
}

#[cfg(test)]
mod tests {
    use super::test_reads::{endless_read, wait_for};
    use super::*;
    use std::sync::Arc;

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

    fn assert_send_sync<T: Send + Sync>(_: &T) {}

    /// A read that is `Send` but not `Sync` (it holds a `Cell` across an
    /// await) still gives a `Sync` future, both called directly and from a
    /// future that keeps the built read in a `Mutex` until it spawns it, the
    /// way `read_files_in_order` keeps its reads.
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
