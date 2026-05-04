//! Flake generation from transaction templates
//!
//! This module provides functionality for materializing triple templates
//! into concrete flakes, handling variable substitution, blank node
//! skolemization, and assertion/retraction cancellation.

pub mod accumulator;
pub mod cancellation;
pub mod flakes;

pub use accumulator::FlakeAccumulator;
pub use cancellation::{apply_cancellation, dedup_retractions};
pub use flakes::{infer_datatype, FlakeGenerator};
pub(crate) use flakes::{
    validate_value_dt_pair, DT_BOOLEAN, DT_DATE, DT_DATE_TIME, DT_DAY_TIME_DURATION, DT_DECIMAL,
    DT_DOUBLE, DT_DURATION, DT_G_DAY, DT_G_MONTH, DT_G_MONTH_DAY, DT_G_YEAR, DT_G_YEAR_MONTH,
    DT_ID, DT_INTEGER, DT_JSON, DT_LANG_STRING, DT_STRING, DT_TIME, DT_WKT_LITERAL,
    DT_YEAR_MONTH_DURATION,
};
