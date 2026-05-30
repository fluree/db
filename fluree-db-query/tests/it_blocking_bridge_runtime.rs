//! Regression guard for the multi-query worker-starvation fix.
//!
//! `BinaryScanOperator::next_batch` relocates its synchronous produce+decode
//! region onto a blocking context via `tokio::task::block_in_place` for remote
//! (CAS/S3-backed) stores. The decode region reaches the dictionary-tree
//! bridge in `fluree-db-binary-index` (`dict/reader.rs::fetch_remote_leaf_bytes`),
//! which spawns an OS thread that calls `Handle::block_on(cs.get(...))` on the
//! **outer** runtime and parks the caller on `rx.recv()` — with no
//! `block_in_place` of its own.
//!
//! On a small (e.g. AWS Lambda 2-worker) multi-thread runtime, running that
//! bridge inline parks every worker on `rx.recv()` with no thread left to
//! drive the IO reactor / time wheel, so the re-injected S3 futures never get
//! polled — a hard deadlock (the production 900s wedge). Wrapping the region
//! in `block_in_place` converts the calling worker to a blocking thread and
//! lets tokio promote a replacement that keeps driving the reactor/timer, so
//! the bridge futures complete.
//!
//! This test reproduces that exact bridge pattern (independent of fluree
//! internals) and proves the `block_in_place` arm keeps a 2-worker runtime
//! live under N >> 2 concurrent blocked bridges. Without `block_in_place` the
//! same pattern deadlocks a 2-worker runtime — see `bridge_*` below.

use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Mirrors `dict/reader.rs::fetch_remote_leaf_bytes`: spawn an OS thread that
/// drives an async fetch on the **outer** runtime handle, while the caller
/// parks on `rx.recv()`. This is the bridge that starves a small runtime when
/// run without `block_in_place` coverage.
fn outer_handle_bridge(handle: tokio::runtime::Handle, delay: Duration) -> u64 {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        // `block_on` on the outer runtime: the awaited future only makes
        // progress if some worker is left to drive the reactor/timer.
        let v = handle.block_on(async move {
            tokio::time::sleep(delay).await;
            42u64
        });
        let _ = tx.send(v);
    });
    rx.recv().unwrap()
}

/// The fix: N concurrent sub-queries (>> worker count), each running the
/// outer-handle bridge under `block_in_place` (exactly what `next_batch` does
/// for a remote scan's decode region). A concurrent timer task proves the
/// runtime stays live; all bridges must complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn block_in_place_keeps_two_worker_runtime_live_under_bridge_fanout() {
    let handle = tokio::runtime::Handle::current();

    // If the runtime wedges, this timer never fires.
    let timer = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        true
    });

    // 8 concurrent bridges on a 2-worker runtime.
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let h = handle.clone();
        tasks.push(tokio::spawn(async move {
            tokio::task::block_in_place(|| outer_handle_bridge(h, Duration::from_millis(50)))
        }));
    }

    let started = Instant::now();
    for t in tasks {
        assert_eq!(t.await.unwrap(), 42, "every bridge must complete");
    }
    assert!(
        timer.await.unwrap(),
        "concurrent timer must fire — runtime stayed live under block_in_place"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "8 concurrent bridges under block_in_place must complete promptly on 2 workers, \
         got {:?}",
        started.elapsed()
    );
}

/// Control: a single bridge under `block_in_place` resolves fine (sanity that
/// the bridge mechanism itself works and the test harness is sound).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_bridge_resolves() {
    let handle = tokio::runtime::Handle::current();
    let v = tokio::task::block_in_place(|| outer_handle_bridge(handle, Duration::from_millis(10)));
    assert_eq!(v, 42);
}

/// Negative proof (the pre-fix bug): the SAME bridge fan-out WITHOUT
/// `block_in_place` deadlocks a 2-worker runtime — both workers park on
/// `rx.recv()`, no thread drives the timer, the `sleep`s never fire.
///
/// `#[ignore]` because it intentionally wedges a runtime (the work threads
/// leak until process exit); run on demand with
/// `cargo test -p fluree-db-query --test it_blocking_bridge_runtime -- --ignored`.
/// Driven on a dedicated runtime in a child thread so the main test thread can
/// detect the wedge via `recv_timeout` instead of hanging.
#[test]
#[ignore = "intentionally wedges a 2-worker runtime to prove the pre-fix bug"]
fn without_block_in_place_two_worker_runtime_wedges() {
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let handle = tokio::runtime::Handle::current();
            let mut tasks = Vec::new();
            for _ in 0..8 {
                let h = handle.clone();
                // NOTE: no block_in_place — the bug.
                tasks.push(tokio::spawn(async move {
                    outer_handle_bridge(h, Duration::from_millis(50))
                }));
            }
            for t in tasks {
                let _ = t.await;
            }
        });
        let _ = done_tx.send(());
    });

    match done_rx.recv_timeout(Duration::from_secs(3)) {
        Err(mpsc::RecvTimeoutError::Timeout) => { /* expected: the runtime wedged */ }
        Ok(()) => panic!(
            "expected the 2-worker runtime to WEDGE without block_in_place, but it completed — \
             the starvation mechanism (or the fix's premise) has changed"
        ),
        Err(e) => panic!("unexpected channel error: {e}"),
    }
}
