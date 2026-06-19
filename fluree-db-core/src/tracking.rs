//! Query/transaction execution tracking
//!
//! Internal accumulator stores **micro-fuel** (1 fuel = 1000 micro-fuel).
//! The user-facing fuel value is decimal: micro-fuel / 1000, rounded to 3 places.
//!
//! All `TrackingOptions::max_fuel` and `FuelExceededError` field values are
//! micro-fuel. Use the helper methods (`limit_fuel`, `used_fuel`) for
//! user-facing decimal representations.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use thiserror::Error;

/// Conversion factor between fuel and micro-fuel.
pub const MICRO_FUEL_PER_FUEL: u64 = 1000;

/// Round a fuel value to 3 decimal places (matches the user-facing precision).
#[inline]
pub fn round_fuel(fuel: f64) -> f64 {
    (fuel * 1000.0).round() / 1000.0
}

/// Convert micro-fuel to fuel (rounded to 3 decimals).
#[inline]
pub fn micro_to_fuel(micro: u64) -> f64 {
    round_fuel(micro as f64 / MICRO_FUEL_PER_FUEL as f64)
}

/// Convert a decimal fuel value to micro-fuel (rounded).
#[inline]
pub fn fuel_to_micro(fuel: f64) -> u64 {
    (fuel * MICRO_FUEL_PER_FUEL as f64).round().max(0.0) as u64
}

/// Fuel schedule — the central, named source for the engine's structural fuel
/// charges: the query floor, I/O "touches", the per-row/per-flake rate, and the
/// transaction baseline. All values are **micro-fuel** (1 fuel = 1000 micro-fuel).
///
/// Not (yet) centralized here: per-call expression/function micro-charges
/// (1–5 µf for hashing, UUID, geo, vector, fulltext, etc.) and R2RML row
/// charges, which are applied inline at their `eval`/r2rml sites. The public
/// cost ladder in `docs/query/tracking-and-fuel.md` lists all of them.
///
/// To re-scale a charge defined here, change it once and every call site picks
/// it up. History: I/O "touches" were rescaled from 1000 µf (1.000 fuel) to
/// 10 µf (0.010 fuel) so scan-dominated queries report fuel proportionate to a
/// transaction's flat baseline. The per-row/per-flake rate (1 µf) is the floor
/// of the integer unit and was left unchanged.
pub mod schedule {
    /// One-time floor charged once at query entry (before parsing). Guarantees
    /// a fuel-tracked query reports at least 1.000 fuel and that parse/plan
    /// errors still reflect a non-zero cost.
    pub const QUERY_FLOOR_MICRO_FUEL: u64 = 1000;

    /// Per index-leaflet batch read during a binary cursor scan, charged once
    /// per batch returned regardless of cache state.
    pub const INDEX_TOUCH_MICRO_FUEL: u64 = 10;

    /// Per persisted forward-dict decode (id → value) during result
    /// materialization.
    pub const DICT_TOUCH_MICRO_FUEL: u64 = 10;

    /// Base charge per history-scan leaflet. Per-row costs (base rows +
    /// in-range sidecar rows, at [`PER_ROW_MICRO_FUEL`] each) are added on top
    /// at the call site.
    pub const HISTORY_LEAF_TOUCH_MICRO_FUEL: u64 = 10;

    /// Per row/flake materialized from in-memory state: `db.range` flakes,
    /// overlay/novelty rows, and history rows. The same 1 µf-per-unit rate also
    /// applies to staged flakes during transactions and bulk imports, where it
    /// is charged as a raw count (`flakes.len()`) at those call sites.
    pub const PER_ROW_MICRO_FUEL: u64 = 1;

    /// Transaction/commit baseline, charged once per `stage` and once per
    /// bulk-import commit chunk.
    pub const TXN_BASELINE_MICRO_FUEL: u64 = 10_000;

    /// Per successful indexer CAS write. Charged once per `put`, `put_with_id`,
    /// or `content_write_bytes` call made by the indexer. For `IndexLeaf` writes
    /// an additional charge of this same rate is applied per *re-encoded*
    /// leaflet inside the leaf (passthrough leaflets are byte-copied and not
    /// charged).
    pub const INDEX_CAS_WRITE_MICRO_FUEL: u64 = 1000;
}

/// Tracking options parsed from query `opts`
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrackingOptions {
    pub track_time: bool,
    pub track_fuel: bool,
    pub track_policy: bool,
    /// Micro-fuel limit. None or Some(0) = unlimited.
    pub max_fuel: Option<u64>,
}

impl TrackingOptions {
    /// Parse tracking options from a JSON `opts` object value.
    ///
    /// Expected shapes:
    /// - `"opts": {"meta": true}` enables all tracking
    /// - `"opts": {"meta": {"time": true, "fuel": true, "policy": true}}` selective
    /// - `"opts": {"max-fuel": 1000}` (decimal allowed) implicitly enables fuel tracking
    ///
    /// Also accepts camel/snake variants for max-fuel (`max_fuel`, `maxFuel`).
    pub fn from_opts_value(opts: Option<&JsonValue>) -> Self {
        let Some(opts) = opts.and_then(|v| v.as_object()) else {
            return Self::default();
        };

        let meta = opts.get("meta");
        let max_fuel = opts
            .get("max-fuel")
            .or_else(|| opts.get("max_fuel"))
            .or_else(|| opts.get("maxFuel"))
            .and_then(serde_json::Value::as_f64)
            .map(fuel_to_micro);

        let track_all = matches!(meta, Some(JsonValue::Bool(true)));
        let meta_obj = meta.and_then(|v| v.as_object());

        let meta_flag = |k: &str| -> bool {
            meta_obj
                .and_then(|m| m.get(k))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        };

        Self {
            track_time: track_all || meta_flag("time"),
            track_fuel: max_fuel.is_some() || track_all || meta_flag("fuel"),
            track_policy: track_all || meta_flag("policy"),
            max_fuel,
        }
    }

    #[inline]
    pub fn any_enabled(&self) -> bool {
        self.track_time || self.track_fuel || self.track_policy
    }

    /// Returns tracking options with all tracking enabled (time, fuel, policy).
    ///
    /// This is the default for "tracked" query endpoints where the user expects
    /// tracking information in the response.
    pub fn all_enabled() -> Self {
        Self {
            track_time: true,
            track_fuel: true,
            track_policy: true,
            max_fuel: None,
        }
    }
}

/// Policy execution statistics: `{:executed N :allowed M}`
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyStats {
    pub executed: u64,
    pub allowed: u64,
}

/// Fuel limit exceeded. Field values are micro-fuel; use the helpers for fuel decimals.
#[derive(Debug, Clone, Error)]
#[error("Fuel limit exceeded")]
pub struct FuelExceededError {
    /// Micro-fuel consumed when the limit was hit.
    pub used_micro_fuel: u64,
    /// Configured micro-fuel limit.
    pub limit_micro_fuel: u64,
}

impl FuelExceededError {
    /// User-facing fuel decimal (rounded to 3 places).
    pub fn used_fuel(&self) -> f64 {
        micro_to_fuel(self.used_micro_fuel)
    }
    /// User-facing fuel decimal (rounded to 3 places).
    pub fn limit_fuel(&self) -> f64 {
        micro_to_fuel(self.limit_micro_fuel)
    }
}

struct TrackerInner {
    // Time tracking
    start_time: Option<Instant>,

    // Fuel tracking (micro-fuel internally)
    fuel_total: AtomicU64,
    fuel_limit: u64, // 0 = unlimited

    // Policy tracking
    policy_stats: RwLock<HashMap<String, PolicyStats>>,

    // Reasoning materialization outcome (recorded by query prepare when a
    // reasoning mode ran). Not gated by an option: a capped materialization
    // is a correctness signal, so any enabled tracker reports it.
    reasoning: RwLock<Option<ReasoningTally>>,

    options: TrackingOptions,
}

/// Execution tracker.
///
/// When disabled, this is a single `None` pointer (cheap to clone and pass around).
#[derive(Clone, Default)]
pub struct Tracker(Option<Arc<TrackerInner>>);

impl Tracker {
    /// Create a tracker from options. Returns a disabled tracker if no tracking is enabled.
    pub fn new(options: TrackingOptions) -> Self {
        if !options.any_enabled() {
            return Self(None);
        }

        Self(Some(Arc::new(TrackerInner {
            start_time: options.track_time.then(Instant::now),
            fuel_total: AtomicU64::new(0),
            fuel_limit: options.max_fuel.unwrap_or(0),
            policy_stats: RwLock::new(HashMap::new()),
            reasoning: RwLock::new(None),
            options,
        })))
    }

    /// Disabled tracker (zero overhead beyond a null check at call sites).
    #[inline]
    pub fn disabled() -> Self {
        Self(None)
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    #[inline]
    pub fn tracks_fuel(&self) -> bool {
        self.0
            .as_ref()
            .map(|i| i.options.track_fuel)
            .unwrap_or(false)
    }

    #[inline]
    pub fn tracks_policy(&self) -> bool {
        self.0
            .as_ref()
            .map(|i| i.options.track_policy)
            .unwrap_or(false)
    }

    /// Consume `units` of micro-fuel.
    ///
    /// Allows total consumption up to and including the limit; errors when the
    /// total would strictly exceed the limit.
    #[inline]
    pub fn consume_fuel(&self, units: u64) -> Result<(), FuelExceededError> {
        let Some(inner) = &self.0 else {
            return Ok(());
        };
        if !inner.options.track_fuel || units == 0 {
            return Ok(());
        }

        let new_total = inner.fuel_total.fetch_add(units, Ordering::Relaxed) + units;
        if inner.fuel_limit > 0 && new_total > inner.fuel_limit {
            return Err(FuelExceededError {
                used_micro_fuel: new_total,
                limit_micro_fuel: inner.fuel_limit,
            });
        }
        Ok(())
    }

    /// Record a policy evaluation attempt (increments for every policy considered).
    #[inline]
    pub fn policy_executed(&self, policy_id: &str) {
        let Some(inner) = &self.0 else {
            return;
        };
        if !inner.options.track_policy || policy_id.is_empty() {
            return;
        }

        if let Ok(mut stats) = inner.policy_stats.write() {
            stats.entry(policy_id.to_string()).or_default().executed += 1;
        }
    }

    /// Record a policy allow decision (only when that policy grants access).
    #[inline]
    pub fn policy_allowed(&self, policy_id: &str) {
        let Some(inner) = &self.0 else {
            return;
        };
        if !inner.options.track_policy || policy_id.is_empty() {
            return;
        }

        if let Ok(mut stats) = inner.policy_stats.write() {
            stats.entry(policy_id.to_string()).or_default().allowed += 1;
        }
    }

    /// Record the outcome of an OWL2-RL materialization for this request.
    ///
    /// Last write wins (a request runs at most one materialization per
    /// prepared query; dataset queries record the primary graph's run).
    pub fn record_reasoning(&self, tally: ReasoningTally) {
        let Some(inner) = &self.0 else {
            return;
        };
        if let Ok(mut slot) = inner.reasoning.write() {
            *slot = Some(tally);
        }
    }

    /// Finalize tracking into a serializable tally.
    pub fn tally(&self) -> Option<TrackingTally> {
        let inner = self.0.as_ref()?;

        Some(TrackingTally {
            time: inner.start_time.map(|t| format_time_ms(t.elapsed())),
            fuel: inner
                .options
                .track_fuel
                .then(|| micro_to_fuel(inner.fuel_total.load(Ordering::Relaxed))),
            policy: if inner.options.track_policy {
                inner.policy_stats.read().ok().map(|m| m.clone())
            } else {
                None
            },
            reasoning: inner.reasoning.read().ok().and_then(|r| r.clone()),
        })
    }
}

/// Tracking tally returned on completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackingTally {
    /// Formatted time string like `"12.34ms"`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    /// Total fuel consumed (decimal, rounded to 3 places).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuel: Option<f64>,
    /// Policy stats: `{policy-id -> {executed, allowed}}`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<HashMap<String, PolicyStats>>,
    /// Reasoning materialization outcome, when a reasoning mode ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<ReasoningTally>,
}

/// Outcome of an OWL2-RL materialization, reported per request.
///
/// `capped: true` means the closure hit its budget before reaching fixpoint —
/// query results may be missing entailments. Clients should treat capped
/// results as incomplete, not merely slow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReasoningTally {
    /// Whether materialization was capped before reaching fixpoint.
    pub capped: bool,
    /// Why materialization was capped (e.g. budget kind), if it was.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capped_reason: Option<String>,
    /// Number of facts derived.
    pub derived_facts: u64,
    /// Fixpoint iterations performed.
    pub iterations: u64,
    /// Wall-clock materialization time in milliseconds. For a cached
    /// materialization this reports the original computation, not this
    /// request's (near-zero) cache hit.
    pub duration_ms: u64,
}

fn format_time_ms(duration: Duration) -> String {
    let ms = duration.as_secs_f64() * 1000.0;
    format!("{ms:.2}ms")
}

#[cfg(test)]
mod tests {
    use super::schedule::*;
    use super::*;

    fn fuel_tracker(max_fuel: Option<u64>) -> Tracker {
        Tracker::new(TrackingOptions {
            track_fuel: true,
            max_fuel,
            ..Default::default()
        })
    }

    #[test]
    fn query_floor_reports_one_fuel() {
        let t = fuel_tracker(None);
        t.consume_fuel(QUERY_FLOOR_MICRO_FUEL).unwrap();
        assert_eq!(t.tally().unwrap().fuel, Some(1.0));
    }

    #[test]
    fn index_dict_history_touches_are_one_hundredth_fuel() {
        for touch in [
            INDEX_TOUCH_MICRO_FUEL,
            DICT_TOUCH_MICRO_FUEL,
            HISTORY_LEAF_TOUCH_MICRO_FUEL,
        ] {
            let t = fuel_tracker(None);
            t.consume_fuel(touch).unwrap();
            assert_eq!(t.tally().unwrap().fuel, Some(0.01), "touch={touch}");
        }
    }

    #[test]
    fn floor_plus_one_touch_reports_one_point_zero_one() {
        let t = fuel_tracker(None);
        t.consume_fuel(QUERY_FLOOR_MICRO_FUEL).unwrap();
        t.consume_fuel(INDEX_TOUCH_MICRO_FUEL).unwrap();
        assert_eq!(t.tally().unwrap().fuel, Some(1.01));
    }

    #[test]
    fn floor_exceeds_max_fuel_below_one() {
        // max-fuel: 0.5 leaves no room for the 1.000 floor.
        let t = fuel_tracker(Some(fuel_to_micro(0.5)));
        let err = t.consume_fuel(QUERY_FLOOR_MICRO_FUEL).unwrap_err();
        assert_eq!(err.limit_fuel(), 0.5);
        assert_eq!(err.used_fuel(), 1.0);
    }

    #[test]
    fn max_fuel_one_admits_floor_but_not_a_touch() {
        // max-fuel: 1 permits exactly the floor; the next persisted touch fails.
        let t = fuel_tracker(Some(fuel_to_micro(1.0)));
        t.consume_fuel(QUERY_FLOOR_MICRO_FUEL).unwrap();
        assert!(t.consume_fuel(INDEX_TOUCH_MICRO_FUEL).is_err());
    }
}
