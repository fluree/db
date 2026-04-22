//! Query/transaction execution tracking
//!
//! Internal accumulator stores **micro-fuel** (1 fuel = 1000 micro-fuel).
//! The user-facing fuel value is decimal: micro-fuel / 1000, rounded to 3 places.
//!
//! All `TrackingOptions::max_fuel` and `FuelExceededError` field values are
//! micro-fuel. Use the helper methods (`limit_fuel`, `used_fuel`) for
//! user-facing decimal representations.

use serde::Serialize;
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

/// Tracking options parsed from query `opts`
#[derive(Debug, Clone, Default)]
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
#[derive(Debug, Clone, Default, Serialize)]
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
        })
    }
}

/// Tracking tally returned on completion.
#[derive(Debug, Clone, Serialize)]
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
}

fn format_time_ms(duration: Duration) -> String {
    let ms = duration.as_secs_f64() * 1000.0;
    format!("{ms:.2}ms")
}
