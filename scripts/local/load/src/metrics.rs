//! Latency histograms + per-class counters.
//!
//! The reporter consumes a `MetricsSnapshot` produced by
//! `Metrics::snapshot()` to print the live one-line-per-second view
//! and the final summary. Snapshots are cheap to produce so the
//! reporter can pull at whatever cadence it wants.

use crate::ops::{OpKind, Outcome};
use hdrhistogram::Histogram;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

const HIST_MIN_NS: u64 = 1_000; // 1 µs floor
const HIST_MAX_NS: u64 = 60_000_000_000; // 60 s ceiling
const HIST_SIGNIFICANT_DIGITS: u8 = 3;

/// Aggregate metrics. Internally synchronized via a single `Mutex`
/// because the recording path is a microsecond or two — fine even
/// at hundreds of thousands of ops per second from a few dozen
/// concurrent workers. If contention shows up we can shard later;
/// for now simplicity wins.
pub struct Metrics {
    inner: Mutex<MetricsInner>,
}

struct MetricsInner {
    /// Histogram over all op kinds combined. Useful for top-line TPS
    /// and aggregate p99.
    aggregate_hist: Histogram<u64>,
    /// Per-kind histograms. Op-mix is what the user explicitly cares
    /// about — `CreateLedger` p99 vs `Transact` p99 vs `Query` p99
    /// is the load-bearing breakdown.
    per_kind_hist: HashMap<OpKind, Histogram<u64>>,
    /// Counter per outcome class. Sum over a kind gives total issued
    /// for that kind; ratio gives success rate.
    per_kind_outcome: HashMap<(OpKind, Outcome), u64>,
    /// Per-ledger total counts (across all kinds). Surfaces hot-spots
    /// when the workload distribution isn't perfectly uniform.
    per_ledger: HashMap<String, u64>,
    /// Total samples recorded. Convenient for "did we issue anything"
    /// checks without iterating per-kind counters.
    total: u64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let aggregate_hist = make_hist();
        let mut per_kind_hist = HashMap::with_capacity(OpKind::ALL.len());
        for &kind in OpKind::ALL {
            per_kind_hist.insert(kind, make_hist());
        }
        Self {
            inner: Mutex::new(MetricsInner {
                aggregate_hist,
                per_kind_hist,
                per_kind_outcome: HashMap::new(),
                per_ledger: HashMap::new(),
                total: 0,
            }),
        }
    }

    /// Record one op result. Latency is clamped to the histogram's
    /// recordable range; the histogram's own `record_correct` would
    /// be the more accurate choice for coordinated-omission-adjusted
    /// stats, but at our cadence the raw recording is fine and easier
    /// to reason about.
    pub fn record(&self, kind: OpKind, ledger: &str, outcome: Outcome, latency_ns: u64) {
        let mut g = self.inner.lock().expect("metrics lock poisoned");
        let clamped = latency_ns.clamp(HIST_MIN_NS, HIST_MAX_NS);
        let _ = g.aggregate_hist.record(clamped);
        if let Some(h) = g.per_kind_hist.get_mut(&kind) {
            let _ = h.record(clamped);
        }
        *g.per_kind_outcome.entry((kind, outcome)).or_insert(0) += 1;
        *g.per_ledger.entry(ledger.to_string()).or_insert(0) += 1;
        g.total += 1;
    }

    /// Snapshot the current state. Returns owned data so the reporter
    /// can release the lock before formatting / printing.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let g = self.inner.lock().expect("metrics lock poisoned");
        let aggregate = HistogramSnapshot::from(&g.aggregate_hist);
        let per_kind = g
            .per_kind_hist
            .iter()
            .map(|(&kind, h)| (kind, HistogramSnapshot::from(h)))
            .collect();
        MetricsSnapshot {
            total: g.total,
            aggregate,
            per_kind,
            per_kind_outcome: g.per_kind_outcome.clone(),
            per_ledger: g.per_ledger.clone(),
        }
    }
}

fn make_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(HIST_MIN_NS, HIST_MAX_NS, HIST_SIGNIFICANT_DIGITS)
        .expect("static hist bounds are valid")
}

/// Immutable snapshot of metrics state at one moment.
pub struct MetricsSnapshot {
    pub total: u64,
    pub aggregate: HistogramSnapshot,
    pub per_kind: HashMap<OpKind, HistogramSnapshot>,
    pub per_kind_outcome: HashMap<(OpKind, Outcome), u64>,
    pub per_ledger: HashMap<String, u64>,
}

impl MetricsSnapshot {
    /// Total ops recorded for a kind (sum of every outcome class).
    pub fn count_for(&self, kind: OpKind) -> u64 {
        self.per_kind_outcome
            .iter()
            .filter(|((k, _), _)| *k == kind)
            .map(|(_, c)| *c)
            .sum()
    }

    /// Ops for a kind that landed durably (Success + IdempotencyHit).
    pub fn landed_for(&self, kind: OpKind) -> u64 {
        self.per_kind_outcome
            .iter()
            .filter(|((k, o), _)| *k == kind && o.is_landed())
            .map(|(_, c)| *c)
            .sum()
    }
}

/// Histogram-derived stats. We pre-compute the percentiles the
/// reporter wants so the reporter doesn't keep the histogram alive
/// across thread boundaries (and so percentile selection lives in
/// one place).
pub struct HistogramSnapshot {
    pub count: u64,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub p999: Duration,
    pub max: Duration,
}

impl HistogramSnapshot {
    fn from(h: &Histogram<u64>) -> Self {
        let count = h.len();
        if count == 0 {
            let zero = Duration::ZERO;
            return Self {
                count: 0,
                p50: zero,
                p95: zero,
                p99: zero,
                p999: zero,
                max: zero,
            };
        }
        Self {
            count,
            p50: Duration::from_nanos(h.value_at_quantile(0.50)),
            p95: Duration::from_nanos(h.value_at_quantile(0.95)),
            p99: Duration::from_nanos(h.value_at_quantile(0.99)),
            p999: Duration::from_nanos(h.value_at_quantile(0.999)),
            max: Duration::from_nanos(h.max()),
        }
    }
}
