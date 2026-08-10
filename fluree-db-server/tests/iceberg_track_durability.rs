//! Materialization tracking jobs must survive a server restart.
//!
//! Before this, the job set lived only in `MaterializeWorkerHandle`'s in-memory
//! map: a restart silently stopped every materialization until a client noticed
//! and re-issued `POST /iceberg/track`. Nothing surfaced the gap — the worker
//! reported itself running with zero jobs, which is indistinguishable from
//! "nobody has tracked anything yet".
//!
//! These tests drive the durable record directly rather than through
//! `POST /iceberg/track`, because that route also runs an immediate materialize
//! and would need a reachable Iceberg catalog. The restore path under test —
//! `MaterializeTrackingWorker::restore_jobs`, run at the top of `run()` — is the
//! real one either way.
#![cfg(feature = "iceberg")]

use axum::body::Body;
use axum::Router;
use fluree_db_api::PersistedMaterializeJob;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tower::ServiceExt;

const SOURCE: &str = "silveractor:main";
const TARGET: &str = "silver_{tenant_id}_{user_id}:main";

/// Restore runs at the top of the worker's `run()`, so it races the test's first
/// poll. Generous — a failure here means restore didn't happen, not that it was slow.
const SETTLE: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(50);

async fn state_in(tmp: &TempDir) -> Arc<AppState> {
    let cfg = ServerConfig {
        cors_enabled: false,
        // The tracking worker only spawns where a local indexer runs (it relies
        // on the indexer draining novelty between materialize chunks), and the
        // worker is what these tests exercise — so indexing stays at its
        // shipped default (enabled). `worker_requires_local_indexing` below
        // pins the disabled-indexing behavior.
        indexing_enabled: true,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"))
}

/// A node with `indexing_enabled = false` (external-indexer mode) must NOT run
/// the tracking worker: materialize chunking relies on the local indexer
/// draining novelty between chunks, and without one novelty only grows until
/// every large sync parks on backpressure. The `/track` route surfaces this as
/// its "worker is not running" error rather than accepting a job that can
/// never drain.
#[tokio::test]
async fn worker_requires_local_indexing() {
    let tmp = TempDir::new().unwrap();
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).await.expect("AppState::new"));
    assert!(
        state.materialize_worker.is_none(),
        "tracking worker must not spawn without a local indexer"
    );
}

async fn tracking(app: &Router) -> Value {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/fluree/iceberg/tracking")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).expect("valid JSON");
    assert_eq!(status, StatusCode::OK, "tracking status: {json}");
    json
}

/// Is `(SOURCE, TARGET)` in this node's live job set?
fn has_job(body: &Value) -> bool {
    body.get("jobs")
        .and_then(Value::as_array)
        .is_some_and(|jobs| {
            jobs.iter().any(|j| {
                j.get("source").and_then(Value::as_str) == Some(SOURCE)
                    && j.get("target").and_then(Value::as_str) == Some(TARGET)
            })
        })
}

async fn poll_until(app: &Router, what: &str, pred: impl Fn(&Value) -> bool) -> Value {
    let deadline = std::time::Instant::now() + SETTLE;
    loop {
        let body = tracking(app).await;
        if pred(&body) {
            return body;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}; last /iceberg/tracking body: {body}"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// The headline property: a job tracked before a restart is running after it,
/// with its own poll interval intact, and nobody re-issued `track`.
#[tokio::test]
async fn a_tracked_job_survives_a_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");

    let first = state_in(&tmp).await;
    first
        .fluree
        .persist_materialize_job(&PersistedMaterializeJob {
            source: SOURCE.to_string(),
            target: TARGET.to_string(),
            poll_interval_secs: 300,
        })
        .await
        .expect("persist");
    drop(first);

    // Second instance over the same storage — nothing calls track.
    let state = state_in(&tmp).await;
    let app = build_router(state.clone());

    let body = poll_until(&app, "the persisted job to be restored", has_job).await;
    assert_eq!(
        body.get("running").and_then(Value::as_bool),
        Some(true),
        "{body}"
    );

    let job = body
        .get("jobs")
        .and_then(Value::as_array)
        .and_then(|j| j.first())
        .expect("job");
    assert_eq!(
        job.get("poll_interval_secs").and_then(Value::as_u64),
        Some(300),
        "the job's own interval must survive, not the worker default: {body}"
    );
}

/// ...and untracking is durable too, or a restart would resurrect the job.
#[tokio::test]
async fn an_untracked_job_is_not_restored() {
    let tmp = tempfile::tempdir().expect("tempdir");

    let first = state_in(&tmp).await;
    first
        .fluree
        .persist_materialize_job(&PersistedMaterializeJob {
            source: SOURCE.to_string(),
            target: TARGET.to_string(),
            poll_interval_secs: 300,
        })
        .await
        .expect("persist");
    first
        .fluree
        .forget_materialize_job(SOURCE, TARGET)
        .await
        .expect("forget");
    assert!(
        first
            .fluree
            .tracked_materialize_jobs()
            .await
            .expect("list")
            .is_empty(),
        "forget must clear the durable record"
    );
    drop(first);

    let state = state_in(&tmp).await;
    let app = build_router(state.clone());

    // Give restore its chance, then assert it stayed out.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let body = tracking(&app).await;
    assert_eq!(body.get("running").and_then(Value::as_bool), Some(true));
    assert!(
        !has_job(&body),
        "an untracked job must not be restored: {body}"
    );
}

/// Re-tracking the same pair updates in place rather than accumulating rows —
/// the restore path must not resurrect a stale interval alongside the new one.
#[tokio::test]
async fn re_tracking_replaces_rather_than_duplicates() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let state = state_in(&tmp).await;

    for interval in [30u64, 600] {
        state
            .fluree
            .persist_materialize_job(&PersistedMaterializeJob {
                source: SOURCE.to_string(),
                target: TARGET.to_string(),
                poll_interval_secs: interval,
            })
            .await
            .expect("persist");
    }

    let jobs = state.fluree.tracked_materialize_jobs().await.expect("list");
    assert_eq!(jobs.len(), 1, "one row per (source, target): {jobs:?}");
    assert_eq!(jobs[0].poll_interval_secs, 600, "last write wins");
}

/// A server that has never tracked anything has no state ledger — restore must
/// treat that as "no jobs", not as an error that kills the worker.
#[tokio::test]
async fn restore_is_a_noop_with_no_state_ledger() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let state = state_in(&tmp).await;

    assert!(state
        .fluree
        .tracked_materialize_jobs()
        .await
        .expect("listing with no state ledger must not error")
        .is_empty());

    let app = build_router(state.clone());
    let body = tracking(&app).await;
    assert_eq!(body.get("running").and_then(Value::as_bool), Some(true));
    assert_eq!(
        body.get("jobs").and_then(Value::as_array).map(Vec::len),
        Some(0),
        "{body}"
    );
}
