//! Monotonic / wall-clock time sources that work on every target.
//!
//! On native targets these are plain re-exports of `std::time` — zero-cost
//! type aliases, so call sites compile to exactly the code they had before
//! this module existed. On `wasm32-unknown-unknown` — where
//! `std::time::Instant::now()` and `SystemTime::now()` abort at runtime
//! ("time not implemented on this platform") — they come from `web-time`,
//! which reads `performance.now()` / `Date.now()` through wasm-bindgen while
//! exposing the same API surface (`now`, `elapsed`, `duration_since`,
//! `saturating_duration_since`, arithmetic with `Duration`, ordering).
//!
//! `Duration` needs no shim: it is pure data and `std::time::Duration` works
//! everywhere. Only the *clock reads* differ per target.

#[cfg(not(target_arch = "wasm32"))]
pub use std::time::{Instant, SystemTime};

#[cfg(target_arch = "wasm32")]
pub use web_time::{Instant, SystemTime};
