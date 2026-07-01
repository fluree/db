//! HTTP client with cluster-aware routing.
//!
//! Holds a pool of target URLs and a shared `reqwest::Client`. Each
//! dispatched op picks the next URL round-robin; on 503-leader-change
//! the response's leader-hint header (if present) drives the retry,
//! otherwise the next URL in the pool gets it. Sustained failures
//! against one URL trigger a short-window blacklist so the round-robin
//! doesn't keep hammering a known-bad target.

use crate::ops::{Op, OpKind, OpResult, Outcome};
use reqwest::{Client, StatusCode, Url};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-target health state. Updated on every dispatch.
struct TargetHealth {
    /// Monotonic count of consecutive failures. Reset on any success.
    consecutive_failures: AtomicU64,
    /// Unix-millis-since-this-process-start (saturating) until which
    /// the target is skipped during round-robin selection. Zero means
    /// healthy.
    blacklist_until: AtomicU64,
}

impl TargetHealth {
    fn new() -> Self {
        Self {
            consecutive_failures: AtomicU64::new(0),
            blacklist_until: AtomicU64::new(0),
        }
    }
}

/// HTTP client + routing pool.
///
/// Cheap to clone (everything inside is `Arc`'d), so every worker
/// task gets its own handle.
#[derive(Clone)]
pub struct ClusterClient {
    http: Client,
    targets: Arc<Vec<Url>>,
    health: Arc<Vec<TargetHealth>>,
    next_target: Arc<AtomicUsize>,
    /// Number of consecutive failures before a target gets blacklisted.
    blacklist_threshold: u64,
    /// How long a blacklisted target sits out.
    blacklist_window: Duration,
    /// Process start, used as the clock-zero for blacklist windows.
    start: Instant,
}

impl ClusterClient {
    pub fn new(
        targets: Vec<Url>,
        request_timeout: Duration,
        blacklist_threshold: u64,
        blacklist_window: Duration,
    ) -> Result<Self, ClientBuildError> {
        if targets.is_empty() {
            return Err(ClientBuildError::NoTargets);
        }
        let http = Client::builder()
            .timeout(request_timeout)
            .pool_max_idle_per_host(64)
            // Don't follow redirects automatically — a 3xx with a
            // leader-change body would normally be a 503, but if a
            // future server starts using redirects for leader-forward
            // we want the load tool to see them, not silently follow.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(ClientBuildError::Reqwest)?;
        let health = targets.iter().map(|_| TargetHealth::new()).collect();
        Ok(Self {
            http,
            targets: Arc::new(targets),
            health: Arc::new(health),
            next_target: Arc::new(AtomicUsize::new(0)),
            blacklist_threshold,
            blacklist_window,
            start: Instant::now(),
        })
    }

    /// Pick a target round-robin, skipping blacklisted ones. Falls
    /// back to "use the next slot regardless" if every target is
    /// currently blacklisted — better to keep firing than to stall
    /// the workload entirely.
    fn pick_target(&self) -> usize {
        let n = self.targets.len();
        let now_ms = self.start.elapsed().as_millis() as u64;
        for _ in 0..n {
            let idx = self.next_target.fetch_add(1, Ordering::Relaxed) % n;
            let until = self.health[idx].blacklist_until.load(Ordering::Relaxed);
            if until <= now_ms {
                return idx;
            }
        }
        // Every target is blacklisted; pick whichever's next.
        self.next_target.fetch_add(1, Ordering::Relaxed) % n
    }

    fn record_outcome(&self, idx: usize, outcome: Outcome) {
        let health = &self.health[idx];
        if outcome.is_landed() {
            health.consecutive_failures.store(0, Ordering::Relaxed);
            return;
        }
        // Connection-level failures and 5xx are the only things worth
        // counting toward a blacklist — a 4xx ClientError reflects the
        // request, not the target's health, so it shouldn't trip the
        // cool-off.
        let counts = matches!(
            outcome,
            Outcome::NetworkError | Outcome::Timeout | Outcome::ServerError
        );
        if !counts {
            return;
        }
        let prev = health.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        if prev + 1 >= self.blacklist_threshold {
            let now_ms = self.start.elapsed().as_millis() as u64;
            let until = now_ms.saturating_add(self.blacklist_window.as_millis() as u64);
            health.blacklist_until.store(until, Ordering::Relaxed);
        }
    }

    /// Dispatch one op and return the classified result. Latency is
    /// the wall-clock time of the underlying HTTP exchange (including
    /// any leader-change retry; that's the latency the client
    /// actually experiences, which is what the operator cares about).
    pub async fn dispatch(&self, op: Op) -> OpResult {
        let started = Instant::now();
        let outcome = self.dispatch_inner(&op).await;
        let latency_ns = started.elapsed().as_nanos() as u64;
        OpResult {
            kind: op.kind,
            ledger: op.ledger,
            outcome,
            latency_ns,
        }
    }

    async fn dispatch_inner(&self, op: &Op) -> Outcome {
        // First attempt — round-robin target.
        let idx = self.pick_target();
        let url = self.build_url(idx, op);

        let res = self.build_request(url, op).send().await;
        let outcome = match res {
            Ok(resp) => self.classify_response(resp).await,
            Err(e) => classify_send_error(&e),
        };

        // 503-leader-change is the only outcome worth retrying
        // internally — overloaded / timeout / network all need the
        // caller to back off, and a retry on the same body would
        // double-count latency. One leader-change retry against the
        // next target is conservative but useful: it stops a single
        // election from sinking every request in flight.
        let outcome = if matches!(outcome, Outcome::LeaderChange) {
            self.record_outcome(idx, outcome);
            let retry_idx = self.pick_target();
            let retry_url = self.build_url(retry_idx, op);
            match self.build_request(retry_url, op).send().await {
                Ok(resp) => {
                    let o = self.classify_response(resp).await;
                    self.record_outcome(retry_idx, o);
                    o
                }
                Err(e) => {
                    let o = classify_send_error(&e);
                    self.record_outcome(retry_idx, o);
                    o
                }
            }
        } else {
            self.record_outcome(idx, outcome);
            outcome
        };

        outcome
    }

    /// Assemble the request builder from an op. Consolidates the
    /// two places we dispatch (first attempt + leader-change retry)
    /// so the header set stays consistent.
    fn build_request(&self, url: Url, op: &Op) -> reqwest::RequestBuilder {
        let mut req = self.http.post(url).json(&op.body);
        if let Some(key) = &op.idempotency_key {
            req = req.header("Idempotency-Key", key);
        }
        req
    }

    fn build_url(&self, idx: usize, op: &Op) -> Url {
        let base = &self.targets[idx];
        let path = match op.kind {
            OpKind::CreateLedger => "/v1/fluree/create".to_string(),
            // Use the ledger-tail form for transact + query so the
            // ledger sits in the URL rather than the body — keeps
            // the body purely about the data.
            OpKind::Transact => format!("/v1/fluree/insert/{}", op.ledger),
            OpKind::Query => format!("/v1/fluree/query/{}", op.ledger),
        };
        let mut url = base.clone();
        url.set_path(&path);
        url
    }

    async fn classify_response(&self, resp: reqwest::Response) -> Outcome {
        let status = resp.status();
        if status.is_success() {
            // Distinguish IdempotencyHit from Success by sniffing the
            // response body for the `idempotency_hit` marker. The
            // current server doesn't emit such a marker for non-keyed
            // requests, so all our submissions land as plain Success
            // — leaving the bucket here for when we add per-request
            // idempotency keys.
            let body = resp.text().await.unwrap_or_default();
            if body.contains("\"idempotency_hit\":true") {
                Outcome::IdempotencyHit
            } else {
                Outcome::Success
            }
        } else if status == StatusCode::SERVICE_UNAVAILABLE {
            // Distinguish leader-change from overloaded by body text.
            // The server returns 503 for both; the body hint is what
            // separates them.
            let body = resp.text().await.unwrap_or_default();
            let lower = body.to_ascii_lowercase();
            if lower.contains("not the leader") || lower.contains("leader") {
                Outcome::LeaderChange
            } else if lower.contains("overload") || lower.contains("admission") {
                Outcome::Overloaded
            } else {
                Outcome::ServerError
            }
        } else if status == StatusCode::TOO_MANY_REQUESTS {
            Outcome::Overloaded
        } else if status.is_client_error() {
            Outcome::ClientError
        } else {
            Outcome::ServerError
        }
    }
}

fn classify_send_error(err: &reqwest::Error) -> Outcome {
    if err.is_timeout() {
        Outcome::Timeout
    } else {
        Outcome::NetworkError
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientBuildError {
    #[error("no target URLs supplied")]
    NoTargets,
    #[error("reqwest client build failed: {0}")]
    Reqwest(#[from] reqwest::Error),
}
