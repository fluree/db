//! Operation kinds the load tool issues.
//!
//! Each variant corresponds to one HTTP endpoint shape on the
//! Fluree server. `Outcome` classifies the response into categories
//! the reporter cares about (success, idempotency hit, leader
//! change, overloaded, timeout, etc.), so per-class counters and
//! per-class latency stats can be tracked separately from the
//! aggregate.

use serde::{Deserialize, Serialize};
use std::fmt;

/// One issued request. Carries enough context for the client to
/// dispatch and for the metrics layer to attribute the result.
#[derive(Debug, Clone, Serialize)]
pub struct Op {
    pub kind: OpKind,
    pub ledger: String,
    pub body: serde_json::Value,
    /// Value to send in the `Idempotency-Key` HTTP header, or `None`
    /// for anonymous submissions. Only meaningful on write ops
    /// (`CreateLedger` / `Query` don't carry keys through the
    /// `Committer` trait); the workload composer decides whether to
    /// populate this per op.
    pub idempotency_key: Option<String>,
}

/// Coarse operation taxonomy. Stays small so per-kind counter arrays
/// don't blow up; finer detail (which body shape, which workload)
/// lives on the workload side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OpKind {
    CreateLedger,
    Transact,
    Query,
}

impl OpKind {
    pub const ALL: &'static [OpKind] = &[OpKind::CreateLedger, OpKind::Transact, OpKind::Query];

    pub fn label(self) -> &'static str {
        match self {
            OpKind::CreateLedger => "create-ledger",
            OpKind::Transact => "transact",
            OpKind::Query => "query",
        }
    }
}

impl fmt::Display for OpKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Result of dispatching a single op.
///
/// The `Outcome` enum captures everything the reporter needs to
/// bucket the result without leaking HTTP details. The latency is
/// measured from request send to response received (or error
/// raised); a `NetworkError` carries no latency-of-success meaning,
/// but recording it lets us count timeouts vs successes by duration
/// bucket.
#[derive(Debug, Clone)]
pub struct OpResult {
    pub kind: OpKind,
    pub ledger: String,
    pub outcome: Outcome,
    pub latency_ns: u64,
}

/// Classification of a single op's response.
///
/// `Success` is the only "the request did what the caller wanted"
/// path. `IdempotencyHit` is success-but-recognized (a prior
/// submission with the same key landed; the cache replied with the
/// recorded receipt). The rest are failure modes worth distinguishing
/// — the user's stated goal is to characterise the cluster under
/// load, and "leader changed" and "overloaded" are operationally
/// different signals from "the network blinked."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Outcome {
    /// 2xx response, no idempotency-cache marker on the body.
    Success,
    /// 2xx, but the response indicates this submission was
    /// short-circuited by the replicated idempotency cache (the
    /// original submission's receipt is being returned). Distinguished
    /// because it doesn't go through propose → apply on this request.
    IdempotencyHit,
    /// 503 with a leader-change hint in the body or `Location` header.
    /// Caller retried against the named leader; the latency reported
    /// is the duration of the original attempt only.
    LeaderChange,
    /// 503 / 429 indicating the in-flight admission cap was hit.
    Overloaded,
    /// Request or response timed out at the HTTP layer.
    Timeout,
    /// Connection refused, reset, or otherwise broken before a
    /// response was received. Distinct from `Timeout`.
    NetworkError,
    /// 4xx other than the recognised retry-friendly cases above.
    /// Usually means the request itself is malformed against the
    /// current cluster state (missing ledger, key collision).
    ClientError,
    /// 5xx other than the recognised retry-friendly cases above.
    /// Server-side bug or invariant break — worth surfacing.
    ServerError,
}

impl Outcome {
    pub const ALL: &'static [Outcome] = &[
        Outcome::Success,
        Outcome::IdempotencyHit,
        Outcome::LeaderChange,
        Outcome::Overloaded,
        Outcome::Timeout,
        Outcome::NetworkError,
        Outcome::ClientError,
        Outcome::ServerError,
    ];

    /// `true` when the request landed durably (either the work
    /// happened this submission or it was already done). Used by
    /// the reporter to compute the success rate the operator cares
    /// about — vs. the wire-level 2xx rate which would conflate
    /// leader-change retries.
    pub fn is_landed(self) -> bool {
        matches!(self, Outcome::Success | Outcome::IdempotencyHit)
    }

    pub fn label(self) -> &'static str {
        match self {
            Outcome::Success => "success",
            Outcome::IdempotencyHit => "idempotency-hit",
            Outcome::LeaderChange => "leader-change",
            Outcome::Overloaded => "overloaded",
            Outcome::Timeout => "timeout",
            Outcome::NetworkError => "network-error",
            Outcome::ClientError => "client-error",
            Outcome::ServerError => "server-error",
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}
