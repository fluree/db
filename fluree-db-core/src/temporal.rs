//! Temporal types for XSD dateTime, date, and time
//!
//! This module provides structured temporal types that:
//! - Preserve the original lexical form for round-trip serialization
//! - Normalize to UTC instants for consistent comparison
//! - Support SPARQL accessor functions (YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS, TZ)
//!
//! ## Comparison Semantics
//!
//! Temporal values are compared by their normalized UTC instant, not by lexical form.
//! This means `"2024-01-01T05:00:00Z"` equals `"2024-01-01T00:00:00-05:00"` (same instant).
//!
//! ## Timezone Handling
//!
//! - DateTime: Normalize to UTC instant; preserve original timezone for output
//! - Date: If timezone present, compare by instant at midnight in that offset
//! - Time: If timezone present, compare by UTC-normalized time-of-day
//!
//! Values without timezone are treated as UTC for comparison purposes.

use chrono::{
    DateTime as ChronoDateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime,
    Timelike, Utc,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::fmt;

/// Serde helper for Option<FixedOffset> - serializes as Option<i32> (seconds from UTC)
mod tz_offset_serde {
    use super::*;

    pub fn serialize<S>(offset: &Option<FixedOffset>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match offset {
            Some(o) => serializer.serialize_some(&o.local_minus_utc()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<FixedOffset>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<i32> = Option::deserialize(deserializer)?;
        Ok(opt.and_then(FixedOffset::east_opt))
    }
}

/// XSD dateTime with timezone preservation
///
/// Stores both the normalized UTC instant (for comparison) and the original
/// string representation (for serialization).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DateTime {
    /// Normalized UTC instant for comparison
    instant: ChronoDateTime<Utc>,
    /// Original timezone offset (None = no timezone in input, treated as UTC)
    #[serde(with = "tz_offset_serde")]
    tz_offset: Option<FixedOffset>,
    /// Original string for round-trip serialization
    original: String,
}

impl DateTime {
    /// Parse an XSD dateTime string
    ///
    /// Accepts:
    /// - RFC3339/ISO8601 with timezone: `2024-01-15T10:30:00Z`, `2024-01-15T10:30:00+05:00`
    /// - Without timezone (treated as UTC): `2024-01-15T10:30:00`
    /// - With fractional seconds: `2024-01-15T10:30:00.123Z`
    pub fn parse(s: &str) -> Result<Self, String> {
        // Try RFC3339/ISO8601 with timezone
        if let Ok(dt) = ChronoDateTime::parse_from_rfc3339(s) {
            return Ok(Self {
                instant: dt.with_timezone(&Utc),
                tz_offset: Some(*dt.offset()),
                original: s.to_string(),
            });
        }

        // Try with explicit timezone offset formats not covered by RFC3339
        // e.g., "2024-01-15T10:30:00+0500" (no colon in offset)
        for fmt in &["%Y-%m-%dT%H:%M:%S%.f%z", "%Y-%m-%dT%H:%M:%S%z"] {
            if let Ok(dt) = ChronoDateTime::parse_from_str(s, fmt) {
                return Ok(Self {
                    instant: dt.with_timezone(&Utc),
                    tz_offset: Some(*dt.offset()),
                    original: s.to_string(),
                });
            }
        }

        // Try without timezone - multiple formats
        for fmt in &[
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
        ] {
            if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
                return Ok(Self {
                    instant: ndt.and_utc(),
                    tz_offset: None,
                    original: s.to_string(),
                });
            }
        }

        Err(format!("Cannot parse dateTime: {s}"))
    }

    /// Get the normalized UTC instant
    pub fn instant(&self) -> ChronoDateTime<Utc> {
        self.instant
    }

    /// Get the original timezone offset (if any)
    pub fn tz_offset(&self) -> Option<FixedOffset> {
        self.tz_offset
    }

    /// Get the original string representation
    pub fn original(&self) -> &str {
        &self.original
    }

    // === SPARQL accessor functions ===

    /// Get the year component
    pub fn year(&self) -> i32 {
        self.instant.year()
    }

    /// Get the month component (1-12)
    pub fn month(&self) -> u32 {
        self.instant.month()
    }

    /// Get the day component (1-31)
    pub fn day(&self) -> u32 {
        self.instant.day()
    }

    /// Get the hour component (0-23)
    pub fn hours(&self) -> u32 {
        self.instant.hour()
    }

    /// Get the minute component (0-59)
    pub fn minutes(&self) -> u32 {
        self.instant.minute()
    }

    /// Get the seconds component with fractional part
    pub fn seconds(&self) -> f64 {
        self.instant.second() as f64 + self.instant.nanosecond() as f64 / 1e9
    }

    /// Get the timezone string (e.g., "+05:00", "Z") or None if no timezone
    pub fn timezone(&self) -> Option<String> {
        self.tz_offset.map(|tz| {
            let secs = tz.local_minus_utc();
            if secs == 0 {
                "Z".to_string()
            } else {
                let hours = secs.abs() / 3600;
                let mins = (secs.abs() % 3600) / 60;
                let sign = if secs >= 0 { '+' } else { '-' };
                format!("{sign}{hours:02}:{mins:02}")
            }
        })
    }

    /// Get epoch milliseconds (for Parquet storage)
    pub fn epoch_millis(&self) -> i64 {
        self.instant.timestamp_millis()
    }

    /// Get epoch microseconds (for higher precision Parquet storage)
    pub fn epoch_micros(&self) -> i64 {
        self.instant.timestamp_micros()
    }
}

impl PartialEq for DateTime {
    fn eq(&self, other: &Self) -> bool {
        self.instant == other.instant
    }
}

impl Eq for DateTime {}

impl Ord for DateTime {
    fn cmp(&self, other: &Self) -> Ordering {
        self.instant.cmp(&other.instant)
    }
}

impl PartialOrd for DateTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for DateTime {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.instant.timestamp_nanos_opt().hash(state);
    }
}

impl fmt::Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

/// XSD date (year-month-day with optional timezone)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Date {
    /// The date value
    date: NaiveDate,
    /// Original timezone offset (None = no timezone in input)
    #[serde(with = "tz_offset_serde")]
    tz_offset: Option<FixedOffset>,
    /// Original string for round-trip serialization
    original: String,
}

impl Date {
    /// Parse an XSD date string
    ///
    /// Accepts:
    /// - With timezone: `2024-01-15Z`, `2024-01-15+05:00`
    /// - Without timezone: `2024-01-15`
    pub fn parse(s: &str) -> Result<Self, String> {
        if !is_strict_date_lexical(s) {
            return Err(format!("Cannot parse date: {s}"));
        }

        // Try parsing with timezone suffix
        if let Some(date_part) = s.strip_suffix('Z') {
            if let Ok(date) = NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
                return Ok(Self {
                    date,
                    tz_offset: Some(FixedOffset::east_opt(0).unwrap()),
                    original: s.to_string(),
                });
            }
        }

        // Try parsing with explicit offset (e.g., +05:00 or -05:00)
        if let Some(offset_start) = s.rfind(['+', '-']) {
            // Make sure this is actually a timezone, not just a negative year
            if offset_start > 0 && s[offset_start..].contains(':') {
                let date_part = &s[..offset_start];
                let offset_part = &s[offset_start..];

                if let Ok(date) = NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
                    // Parse the offset
                    let sign = if offset_part.starts_with('-') { -1 } else { 1 };
                    let offset_str = &offset_part[1..];
                    if let Some((hours_str, mins_str)) = offset_str.split_once(':') {
                        if let (Ok(hours), Ok(mins)) =
                            (hours_str.parse::<i32>(), mins_str.parse::<i32>())
                        {
                            let total_secs = sign * (hours * 3600 + mins * 60);
                            if let Some(offset) = FixedOffset::east_opt(total_secs) {
                                return Ok(Self {
                                    date,
                                    tz_offset: Some(offset),
                                    original: s.to_string(),
                                });
                            }
                        }
                    }
                }
            }
        }

        // Try without timezone
        if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            return Ok(Self {
                date,
                tz_offset: None,
                original: s.to_string(),
            });
        }

        Err(format!("Cannot parse date: {s}"))
    }

    /// Get the date value
    pub fn date(&self) -> NaiveDate {
        self.date
    }

    /// Get the original timezone offset (if any)
    pub fn tz_offset(&self) -> Option<FixedOffset> {
        self.tz_offset
    }

    /// Get the original string representation
    pub fn original(&self) -> &str {
        &self.original
    }

    /// Convert to UTC instant at midnight for timezone-aware comparison
    fn to_instant(&self) -> ChronoDateTime<Utc> {
        let midnight = self.date.and_hms_opt(0, 0, 0).unwrap();
        match self.tz_offset {
            Some(offset) => midnight
                .and_local_timezone(offset)
                .single()
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|| midnight.and_utc()),
            None => midnight.and_utc(),
        }
    }

    // === SPARQL accessor functions ===

    pub fn year(&self) -> i32 {
        self.date.year()
    }

    pub fn month(&self) -> u32 {
        self.date.month()
    }

    pub fn day(&self) -> u32 {
        self.date.day()
    }

    pub fn timezone(&self) -> Option<String> {
        self.tz_offset.map(|tz| {
            let secs = tz.local_minus_utc();
            if secs == 0 {
                "Z".to_string()
            } else {
                let hours = secs.abs() / 3600;
                let mins = (secs.abs() % 3600) / 60;
                let sign = if secs >= 0 { '+' } else { '-' };
                format!("{sign}{hours:02}:{mins:02}")
            }
        })
    }

    /// Get days since epoch (for Parquet storage)
    pub fn days_since_epoch(&self) -> i32 {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        self.date.signed_duration_since(epoch).num_days() as i32
    }
}

impl PartialEq for Date {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Date {}

impl Ord for Date {
    fn cmp(&self, other: &Self) -> Ordering {
        // If either side has a timezone, compare by instant at midnight UTC
        match (self.tz_offset, other.tz_offset) {
            (Some(_) | None, Some(_)) | (Some(_), None) => {
                self.to_instant().cmp(&other.to_instant())
            }
            (None, None) => self.date.cmp(&other.date),
        }
    }
}

impl PartialOrd for Date {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for Date {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.date.hash(state);
        self.tz_offset.map(|o| o.local_minus_utc()).hash(state);
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

/// XSD time (hour:minute:second with optional timezone)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Time {
    /// The time value
    time: NaiveTime,
    /// Original timezone offset (None = no timezone in input)
    #[serde(with = "tz_offset_serde")]
    tz_offset: Option<FixedOffset>,
    /// Original string for round-trip serialization
    original: String,
}

impl Time {
    /// Parse an XSD time string
    ///
    /// Accepts:
    /// - With timezone: `10:30:00Z`, `10:30:00+05:00`
    /// - Without timezone: `10:30:00`
    /// - With fractional seconds: `10:30:00.123Z`
    pub fn parse(s: &str) -> Result<Self, String> {
        if !is_strict_time_lexical(s) {
            return Err(format!("Cannot parse time: {s}"));
        }

        // Try parsing with Z suffix
        if let Some(time_part) = s.strip_suffix('Z') {
            for fmt in &["%H:%M:%S%.f", "%H:%M:%S"] {
                if let Ok(time) = NaiveTime::parse_from_str(time_part, fmt) {
                    return Ok(Self {
                        time,
                        tz_offset: Some(FixedOffset::east_opt(0).unwrap()),
                        original: s.to_string(),
                    });
                }
            }
        }

        // Try parsing with explicit offset
        if let Some(offset_start) = s.rfind(['+', '-']) {
            if s[offset_start..].contains(':') {
                let time_part = &s[..offset_start];
                let offset_part = &s[offset_start..];

                for fmt in &["%H:%M:%S%.f", "%H:%M:%S"] {
                    if let Ok(time) = NaiveTime::parse_from_str(time_part, fmt) {
                        let sign = if offset_part.starts_with('-') { -1 } else { 1 };
                        let offset_str = &offset_part[1..];
                        if let Some((hours_str, mins_str)) = offset_str.split_once(':') {
                            if let (Ok(hours), Ok(mins)) =
                                (hours_str.parse::<i32>(), mins_str.parse::<i32>())
                            {
                                let total_secs = sign * (hours * 3600 + mins * 60);
                                if let Some(offset) = FixedOffset::east_opt(total_secs) {
                                    return Ok(Self {
                                        time,
                                        tz_offset: Some(offset),
                                        original: s.to_string(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        // Try without timezone
        for fmt in &["%H:%M:%S%.f", "%H:%M:%S", "%H:%M"] {
            if let Ok(time) = NaiveTime::parse_from_str(s, fmt) {
                return Ok(Self {
                    time,
                    tz_offset: None,
                    original: s.to_string(),
                });
            }
        }

        Err(format!("Cannot parse time: {s}"))
    }

    /// Get the time value
    pub fn time(&self) -> NaiveTime {
        self.time
    }

    /// Get the original timezone offset (if any)
    pub fn tz_offset(&self) -> Option<FixedOffset> {
        self.tz_offset
    }

    /// Get the original string representation
    pub fn original(&self) -> &str {
        &self.original
    }

    /// Normalize to UTC time-of-day for timezone-aware comparison
    fn to_utc_time(&self) -> NaiveTime {
        match self.tz_offset {
            Some(offset) => {
                let secs = self.time.num_seconds_from_midnight() as i32 - offset.local_minus_utc();
                let normalized_secs = secs.rem_euclid(86400) as u32;
                NaiveTime::from_num_seconds_from_midnight_opt(
                    normalized_secs,
                    self.time.nanosecond(),
                )
                .unwrap_or(self.time)
            }
            None => self.time,
        }
    }

    // === SPARQL accessor functions ===

    pub fn hours(&self) -> u32 {
        self.time.hour()
    }

    pub fn minutes(&self) -> u32 {
        self.time.minute()
    }

    pub fn seconds(&self) -> f64 {
        self.time.second() as f64 + self.time.nanosecond() as f64 / 1e9
    }

    pub fn timezone(&self) -> Option<String> {
        self.tz_offset.map(|tz| {
            let secs = tz.local_minus_utc();
            if secs == 0 {
                "Z".to_string()
            } else {
                let hours = secs.abs() / 3600;
                let mins = (secs.abs() % 3600) / 60;
                let sign = if secs >= 0 { '+' } else { '-' };
                format!("{sign}{hours:02}:{mins:02}")
            }
        })
    }

    /// Get microseconds since midnight (for Parquet storage)
    pub fn micros_since_midnight(&self) -> i64 {
        let secs = self.time.num_seconds_from_midnight() as i64;
        let nanos = self.time.nanosecond() as i64;
        secs * 1_000_000 + nanos / 1000
    }
}

fn is_strict_date_lexical(s: &str) -> bool {
    let (date_part, tz_part) = if let Some(stripped) = s.strip_suffix('Z') {
        (stripped, Some("Z"))
    } else if let Some(idx) = s.rfind(['+', '-']) {
        if idx == 10 {
            (&s[..idx], Some(&s[idx..]))
        } else {
            (s, None)
        }
    } else {
        (s, None)
    };

    if date_part.len() != 10 {
        return false;
    }
    let bytes = date_part.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return false;
    }
    if !bytes[0..4].iter().all(u8::is_ascii_digit)
        || !bytes[5..7].iter().all(u8::is_ascii_digit)
        || !bytes[8..10].iter().all(u8::is_ascii_digit)
    {
        return false;
    }

    if let Some(tz) = tz_part {
        if tz == "Z" {
            return true;
        }
        if tz.len() != 6 {
            return false;
        }
        let tzb = tz.as_bytes();
        if (tzb[0] != b'+' && tzb[0] != b'-') || tzb[3] != b':' {
            return false;
        }
        return tzb[1..3].iter().all(u8::is_ascii_digit)
            && tzb[4..6].iter().all(u8::is_ascii_digit);
    }

    true
}

fn is_strict_time_lexical(s: &str) -> bool {
    let (time_part, tz_part) = if let Some(stripped) = s.strip_suffix('Z') {
        (stripped, Some("Z"))
    } else if s.len() >= 6 {
        let tail = &s[s.len() - 6..];
        if (tail.starts_with('+') || tail.starts_with('-')) && tail.as_bytes()[3] == b':' {
            (&s[..s.len() - 6], Some(tail))
        } else {
            (s, None)
        }
    } else {
        (s, None)
    };

    let (main, _frac) = if let Some((m, f)) = time_part.split_once('.') {
        if f.is_empty() || !f.chars().all(|c| c.is_ascii_digit()) {
            return false;
        }
        (m, Some(f))
    } else {
        (time_part, None)
    };

    if main.len() != 8 {
        return false;
    }
    let bytes = main.as_bytes();
    if bytes[2] != b':' || bytes[5] != b':' {
        return false;
    }
    if !bytes[0..2].iter().all(u8::is_ascii_digit)
        || !bytes[3..5].iter().all(u8::is_ascii_digit)
        || !bytes[6..8].iter().all(u8::is_ascii_digit)
    {
        return false;
    }

    if let Some(tz) = tz_part {
        if tz == "Z" {
            return true;
        }
        if tz.len() != 6 {
            return false;
        }
        let tzb = tz.as_bytes();
        if (tzb[0] != b'+' && tzb[0] != b'-') || tzb[3] != b':' {
            return false;
        }
        return tzb[1..3].iter().all(u8::is_ascii_digit)
            && tzb[4..6].iter().all(u8::is_ascii_digit);
    }

    true
}

impl PartialEq for Time {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Time {}

impl Ord for Time {
    fn cmp(&self, other: &Self) -> Ordering {
        // If either side has a timezone, compare by normalized UTC time-of-day
        match (self.tz_offset, other.tz_offset) {
            (Some(_) | None, Some(_)) | (Some(_), None) => {
                self.to_utc_time().cmp(&other.to_utc_time())
            }
            (None, None) => self.time.cmp(&other.time),
        }
    }
}

impl PartialOrd for Time {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for Time {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.time.hash(state);
        self.tz_offset.map(|o| o.local_minus_utc()).hash(state);
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

// ============================================================================
// Common helpers
// ============================================================================

/// Parse an optional timezone suffix from the end of a string.
/// Returns (value_part, optional_timezone_offset).
fn parse_tz_suffix(s: &str) -> (&str, Option<FixedOffset>) {
    if let Some(stripped) = s.strip_suffix('Z') {
        (stripped, Some(FixedOffset::east_opt(0).unwrap()))
    } else if s.len() >= 6 {
        let tail = &s[s.len() - 6..];
        if (tail.starts_with('+') || tail.starts_with('-')) && tail.as_bytes()[3] == b':' {
            let sign = if tail.starts_with('-') { -1 } else { 1 };
            let hours: i32 = tail[1..3].parse().unwrap_or(0);
            let mins: i32 = tail[4..6].parse().unwrap_or(0);
            let total_secs = sign * (hours * 3600 + mins * 60);
            if let Some(offset) = FixedOffset::east_opt(total_secs) {
                return (&s[..s.len() - 6], Some(offset));
            }
            (s, None)
        } else {
            (s, None)
        }
    } else {
        (s, None)
    }
}

/// Format a timezone offset as a string suffix.
fn format_tz_suffix(tz: Option<FixedOffset>) -> String {
    match tz {
        None => String::new(),
        Some(offset) => {
            let secs = offset.local_minus_utc();
            if secs == 0 {
                "Z".to_string()
            } else {
                let hours = secs.abs() / 3600;
                let mins = (secs.abs() % 3600) / 60;
                let sign = if secs >= 0 { '+' } else { '-' };
                format!("{sign}{hours:02}:{mins:02}")
            }
        }
    }
}

// ============================================================================
// 1. GYear
// ============================================================================

/// XSD gYear — a year with optional timezone
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GYear {
    year: i32,
    #[serde(with = "tz_offset_serde")]
    tz_offset: Option<FixedOffset>,
    original: String,
}

impl GYear {
    /// Parse an XSD gYear string.
    ///
    /// Accepts: `"2024"`, `"2024Z"`, `"2024+05:00"`, `"-0050"`, `"-0050Z"`
    pub fn parse(s: &str) -> Result<Self, String> {
        let (value, tz) = parse_tz_suffix(s);

        // Handle negative years: the year string itself starts with '-'
        let year: i32 = value
            .parse()
            .map_err(|_| format!("Cannot parse gYear: {s}"))?;

        Ok(Self {
            year,
            tz_offset: tz,
            original: s.to_string(),
        })
    }

    /// Construct from a year value with no timezone.
    pub fn from_year(year: i32) -> Self {
        let original = Self::canonical_string(year, None);
        Self {
            year,
            tz_offset: None,
            original,
        }
    }

    /// Canonical string representation.
    pub fn canonical(&self) -> String {
        Self::canonical_string(self.year, self.tz_offset)
    }

    fn canonical_string(year: i32, tz: Option<FixedOffset>) -> String {
        let tz_str = format_tz_suffix(tz);
        if year >= 10000 || year <= -10000 {
            format!("{year}{tz_str}")
        } else if year < 0 {
            format!("-{:04}{}", year.unsigned_abs(), tz_str)
        } else {
            format!("{year:04}{tz_str}")
        }
    }

    pub fn year(&self) -> i32 {
        self.year
    }

    pub fn tz_offset(&self) -> Option<FixedOffset> {
        self.tz_offset
    }

    pub fn original(&self) -> &str {
        &self.original
    }
}

impl PartialEq for GYear {
    fn eq(&self, other: &Self) -> bool {
        self.year == other.year
    }
}

impl Eq for GYear {}

impl Ord for GYear {
    fn cmp(&self, other: &Self) -> Ordering {
        self.year.cmp(&other.year)
    }
}

impl PartialOrd for GYear {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for GYear {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.year.hash(state);
    }
}

impl fmt::Display for GYear {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

// ============================================================================
// 2. GYearMonth
// ============================================================================

/// XSD gYearMonth — a year-month with optional timezone
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GYearMonth {
    year: i32,
    month: u32,
    #[serde(with = "tz_offset_serde")]
    tz_offset: Option<FixedOffset>,
    original: String,
}

impl GYearMonth {
    /// Parse an XSD gYearMonth string.
    ///
    /// Accepts: `"2024-01"`, `"2024-01Z"`, `"2024-01+05:00"`, `"-0050-06"`
    pub fn parse(s: &str) -> Result<Self, String> {
        let (value, tz) = parse_tz_suffix(s);

        // Handle negative years. The format is YYYY-MM or -YYYY-MM.
        // For negative years, the string starts with '-', so we need to find
        // the separator '-' that is NOT the leading negative sign.
        let (year, month) = if let Some(rest) = value.strip_prefix('-') {
            // Negative year: find the '-' separator after position 0
            // skip leading '-'
            let dash_pos = rest
                .rfind('-')
                .ok_or_else(|| format!("Cannot parse gYearMonth: {s}"))?;
            let year_str = &rest[..dash_pos];
            let month_str = &rest[dash_pos + 1..];
            let year: i32 = year_str
                .parse::<i32>()
                .map_err(|_| format!("Cannot parse gYearMonth year: {s}"))?;
            let month: u32 = month_str
                .parse()
                .map_err(|_| format!("Cannot parse gYearMonth month: {s}"))?;
            (-year, month)
        } else {
            let dash_pos = value
                .rfind('-')
                .ok_or_else(|| format!("Cannot parse gYearMonth: {s}"))?;
            let year: i32 = value[..dash_pos]
                .parse()
                .map_err(|_| format!("Cannot parse gYearMonth year: {s}"))?;
            let month: u32 = value[dash_pos + 1..]
                .parse()
                .map_err(|_| format!("Cannot parse gYearMonth month: {s}"))?;
            (year, month)
        };

        if !(1..=12).contains(&month) {
            return Err(format!("Invalid month {month} in gYearMonth: {s}"));
        }

        Ok(Self {
            year,
            month,
            tz_offset: tz,
            original: s.to_string(),
        })
    }

    /// Construct from year and month with no timezone.
    pub fn from_components(year: i32, month: u32) -> Self {
        let tz_str = format_tz_suffix(None);
        let original = if year < 0 {
            format!("-{:04}-{:02}{}", year.unsigned_abs(), month, tz_str)
        } else {
            format!("{year:04}-{month:02}{tz_str}")
        };
        Self {
            year,
            month,
            tz_offset: None,
            original,
        }
    }

    pub fn year(&self) -> i32 {
        self.year
    }

    pub fn month(&self) -> u32 {
        self.month
    }

    pub fn tz_offset(&self) -> Option<FixedOffset> {
        self.tz_offset
    }

    pub fn original(&self) -> &str {
        &self.original
    }
}

impl PartialEq for GYearMonth {
    fn eq(&self, other: &Self) -> bool {
        self.year == other.year && self.month == other.month
    }
}

impl Eq for GYearMonth {}

impl Ord for GYearMonth {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.year, self.month).cmp(&(other.year, other.month))
    }
}

impl PartialOrd for GYearMonth {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for GYearMonth {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.year.hash(state);
        self.month.hash(state);
    }
}

impl fmt::Display for GYearMonth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

// ============================================================================
// 3. GMonth
// ============================================================================

/// XSD gMonth — a month with optional timezone
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GMonth {
    month: u32,
    #[serde(with = "tz_offset_serde")]
    tz_offset: Option<FixedOffset>,
    original: String,
}

impl GMonth {
    /// Parse an XSD gMonth string.
    ///
    /// Accepts: `"--01"`, `"--01Z"`, `"--01+05:00"`
    pub fn parse(s: &str) -> Result<Self, String> {
        if !s.starts_with("--") {
            return Err(format!("gMonth must start with '--': {s}"));
        }

        let after_prefix = &s[2..];
        let (value, tz) = parse_tz_suffix(after_prefix);

        let month: u32 = value
            .parse()
            .map_err(|_| format!("Cannot parse gMonth: {s}"))?;

        if !(1..=12).contains(&month) {
            return Err(format!("Invalid month {month} in gMonth: {s}"));
        }

        Ok(Self {
            month,
            tz_offset: tz,
            original: s.to_string(),
        })
    }

    /// Construct from a month value with no timezone.
    pub fn from_month(month: u32) -> Self {
        let original = format!("--{month:02}");
        Self {
            month,
            tz_offset: None,
            original,
        }
    }

    pub fn month(&self) -> u32 {
        self.month
    }

    pub fn tz_offset(&self) -> Option<FixedOffset> {
        self.tz_offset
    }

    pub fn original(&self) -> &str {
        &self.original
    }
}

impl PartialEq for GMonth {
    fn eq(&self, other: &Self) -> bool {
        self.month == other.month
    }
}

impl Eq for GMonth {}

impl Ord for GMonth {
    fn cmp(&self, other: &Self) -> Ordering {
        self.month.cmp(&other.month)
    }
}

impl PartialOrd for GMonth {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for GMonth {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.month.hash(state);
    }
}

impl fmt::Display for GMonth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

// ============================================================================
// 4. GDay
// ============================================================================

/// XSD gDay — a day with optional timezone
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GDay {
    day: u32,
    #[serde(with = "tz_offset_serde")]
    tz_offset: Option<FixedOffset>,
    original: String,
}

impl GDay {
    /// Parse an XSD gDay string.
    ///
    /// Accepts: `"---15"`, `"---15Z"`, `"---15+05:00"`
    pub fn parse(s: &str) -> Result<Self, String> {
        if !s.starts_with("---") {
            return Err(format!("gDay must start with '---': {s}"));
        }

        let after_prefix = &s[3..];
        let (value, tz) = parse_tz_suffix(after_prefix);

        let day: u32 = value
            .parse()
            .map_err(|_| format!("Cannot parse gDay: {s}"))?;

        if !(1..=31).contains(&day) {
            return Err(format!("Invalid day {day} in gDay: {s}"));
        }

        Ok(Self {
            day,
            tz_offset: tz,
            original: s.to_string(),
        })
    }

    /// Construct from a day value with no timezone.
    pub fn from_day(day: u32) -> Self {
        let original = format!("---{day:02}");
        Self {
            day,
            tz_offset: None,
            original,
        }
    }

    pub fn day(&self) -> u32 {
        self.day
    }

    pub fn tz_offset(&self) -> Option<FixedOffset> {
        self.tz_offset
    }

    pub fn original(&self) -> &str {
        &self.original
    }
}

impl PartialEq for GDay {
    fn eq(&self, other: &Self) -> bool {
        self.day == other.day
    }
}

impl Eq for GDay {}

impl Ord for GDay {
    fn cmp(&self, other: &Self) -> Ordering {
        self.day.cmp(&other.day)
    }
}

impl PartialOrd for GDay {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for GDay {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.day.hash(state);
    }
}

impl fmt::Display for GDay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

// ============================================================================
// 5. GMonthDay
// ============================================================================

/// XSD gMonthDay — a month-day with optional timezone
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GMonthDay {
    month: u32,
    day: u32,
    #[serde(with = "tz_offset_serde")]
    tz_offset: Option<FixedOffset>,
    original: String,
}

impl GMonthDay {
    /// Parse an XSD gMonthDay string.
    ///
    /// Accepts: `"--01-15"`, `"--01-15Z"`, `"--01-15+05:00"`
    pub fn parse(s: &str) -> Result<Self, String> {
        if !s.starts_with("--") {
            return Err(format!("gMonthDay must start with '--': {s}"));
        }

        let after_prefix = &s[2..];
        let (value, tz) = parse_tz_suffix(after_prefix);

        // value should be "MM-DD"
        let dash_pos = value
            .find('-')
            .ok_or_else(|| format!("Cannot parse gMonthDay (expected MM-DD): {s}"))?;

        let month: u32 = value[..dash_pos]
            .parse()
            .map_err(|_| format!("Cannot parse gMonthDay month: {s}"))?;
        let day: u32 = value[dash_pos + 1..]
            .parse()
            .map_err(|_| format!("Cannot parse gMonthDay day: {s}"))?;

        if !(1..=12).contains(&month) {
            return Err(format!("Invalid month {month} in gMonthDay: {s}"));
        }
        if !(1..=31).contains(&day) {
            return Err(format!("Invalid day {day} in gMonthDay: {s}"));
        }

        Ok(Self {
            month,
            day,
            tz_offset: tz,
            original: s.to_string(),
        })
    }

    /// Construct from month and day with no timezone.
    pub fn from_components(month: u32, day: u32) -> Self {
        let original = format!("--{month:02}-{day:02}");
        Self {
            month,
            day,
            tz_offset: None,
            original,
        }
    }

    pub fn month(&self) -> u32 {
        self.month
    }

    pub fn day(&self) -> u32 {
        self.day
    }

    pub fn tz_offset(&self) -> Option<FixedOffset> {
        self.tz_offset
    }

    pub fn original(&self) -> &str {
        &self.original
    }
}

impl PartialEq for GMonthDay {
    fn eq(&self, other: &Self) -> bool {
        self.month == other.month && self.day == other.day
    }
}

impl Eq for GMonthDay {}

impl Ord for GMonthDay {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.month, self.day).cmp(&(other.month, other.day))
    }
}

impl PartialOrd for GMonthDay {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for GMonthDay {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.month.hash(state);
        self.day.hash(state);
    }
}

impl fmt::Display for GMonthDay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

// ============================================================================
// 6. YearMonthDuration
// ============================================================================

/// XSD yearMonthDuration — months-only duration (totally orderable)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct YearMonthDuration {
    months: i32,
    original: String,
}

impl YearMonthDuration {
    /// Parse an XSD yearMonthDuration string.
    ///
    /// Accepts ISO 8601 durations with only year and/or month components:
    /// `"P1Y2M"`, `"P0Y"`, `"-P1Y"`, `"P14M"`, `"P1Y"`, `"P6M"`
    ///
    /// No day (`D`) or time (`T`) components are allowed.
    pub fn parse(s: &str) -> Result<Self, String> {
        let (negative, rest) = s.strip_prefix('-').map(|r| (true, r)).unwrap_or((false, s));

        let body = rest
            .strip_prefix('P')
            .ok_or_else(|| format!("yearMonthDuration must start with 'P' (or '-P'): {s}"))?;

        // Reject if it contains 'D' or 'T' — those are day/time components
        if body.contains('D') || body.contains('T') {
            return Err(format!(
                "yearMonthDuration must not contain day or time components: {s}"
            ));
        }

        let mut years: i32 = 0;
        let mut months_part: i32 = 0;
        let mut found_any = false;
        let mut remaining = body;

        // Parse optional nY
        if let Some(y_pos) = remaining.find('Y') {
            years = remaining[..y_pos]
                .parse()
                .map_err(|_| format!("Invalid year component in yearMonthDuration: {s}"))?;
            remaining = &remaining[y_pos + 1..];
            found_any = true;
        }

        // Parse optional nM
        if let Some(m_pos) = remaining.find('M') {
            months_part = remaining[..m_pos]
                .parse()
                .map_err(|_| format!("Invalid month component in yearMonthDuration: {s}"))?;
            remaining = &remaining[m_pos + 1..];
            found_any = true;
        }

        if !found_any {
            return Err(format!(
                "yearMonthDuration must have at least Y or M component: {s}"
            ));
        }

        if !remaining.is_empty() {
            return Err(format!(
                "Unexpected trailing content in yearMonthDuration: {s}"
            ));
        }

        let total_months = years * 12 + months_part;
        let total_months = if negative {
            -total_months
        } else {
            total_months
        };

        Ok(Self {
            months: total_months,
            original: s.to_string(),
        })
    }

    /// Construct from a total number of months.
    pub fn from_months(months: i32) -> Self {
        let original = Self::make_canonical(months);
        Self { months, original }
    }

    /// Canonical string representation (e.g. "P1Y2M", "P0M", "-P1Y2M").
    pub fn to_canonical_string(&self) -> String {
        Self::make_canonical(self.months)
    }

    fn make_canonical(months: i32) -> String {
        let negative = months < 0;
        let abs = months.unsigned_abs();
        let y = abs / 12;
        let m = abs % 12;
        let prefix = if negative { "-P" } else { "P" };
        match (y, m) {
            (0, _) => format!("{prefix}{m}M"),
            (_, 0) => format!("{prefix}{y}Y"),
            _ => format!("{prefix}{y}Y{m}M"),
        }
    }

    pub fn months(&self) -> i32 {
        self.months
    }

    /// Get the original string representation
    pub fn original(&self) -> &str {
        &self.original
    }

    /// Whole years (truncated toward zero).
    pub fn years(&self) -> i32 {
        self.months / 12
    }

    /// Remaining months after extracting whole years (always non-negative).
    pub fn remaining_months(&self) -> u32 {
        (self.months.abs() % 12) as u32
    }
}

impl PartialEq for YearMonthDuration {
    fn eq(&self, other: &Self) -> bool {
        self.months == other.months
    }
}

impl Eq for YearMonthDuration {}

impl Ord for YearMonthDuration {
    fn cmp(&self, other: &Self) -> Ordering {
        self.months.cmp(&other.months)
    }
}

impl PartialOrd for YearMonthDuration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for YearMonthDuration {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.months.hash(state);
    }
}

impl fmt::Display for YearMonthDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

// ============================================================================
// 7. DayTimeDuration
// ============================================================================

/// XSD dayTimeDuration — time-only duration in microseconds (totally orderable)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DayTimeDuration {
    micros: i64,
    original: String,
}

impl DayTimeDuration {
    /// Parse an XSD dayTimeDuration string.
    ///
    /// Accepts ISO 8601 durations with day and/or time components only:
    /// `"P3DT4H5M6.789S"`, `"PT1H30M"`, `"-PT1S"`, `"P1D"`, `"PT0.5S"`
    ///
    /// No year (`Y`) or month (`M` before `T`) components are allowed.
    pub fn parse(s: &str) -> Result<Self, String> {
        let (negative, rest) = s.strip_prefix('-').map(|r| (true, r)).unwrap_or((false, s));

        let body = rest
            .strip_prefix('P')
            .ok_or_else(|| format!("dayTimeDuration must start with 'P' (or '-P'): {s}"))?;

        // Reject year component
        if body.contains('Y') {
            return Err(format!(
                "dayTimeDuration must not contain year component: {s}"
            ));
        }

        // Split on 'T' to separate date-part (days) from time-part
        let (date_part, time_part) = if let Some(t_pos) = body.find('T') {
            (&body[..t_pos], Some(&body[t_pos + 1..]))
        } else {
            (body, None)
        };

        // Reject month component in the date part (M before T means months)
        if date_part.contains('M') {
            return Err(format!(
                "dayTimeDuration must not contain month component: {s}"
            ));
        }

        let mut total_micros: i64 = 0;
        let mut found_any = false;

        // Parse optional nD from date part
        if !date_part.is_empty() {
            if let Some(d_pos) = date_part.find('D') {
                let days: i64 = date_part[..d_pos]
                    .parse()
                    .map_err(|_| format!("Invalid day component in dayTimeDuration: {s}"))?;
                total_micros += days * 86_400_000_000;
                found_any = true;

                let after_d = &date_part[d_pos + 1..];
                if !after_d.is_empty() {
                    return Err(format!(
                        "Unexpected content after D in dayTimeDuration: {s}"
                    ));
                }
            } else {
                return Err(format!("Expected 'D' in date part of dayTimeDuration: {s}"));
            }
        }

        // Parse time components
        if let Some(tp) = time_part {
            if tp.is_empty() {
                return Err(format!("Empty time part after 'T' in dayTimeDuration: {s}"));
            }

            let mut remaining = tp;

            // Parse optional nH
            if let Some(h_pos) = remaining.find('H') {
                let hours: i64 = remaining[..h_pos]
                    .parse()
                    .map_err(|_| format!("Invalid hour component in dayTimeDuration: {s}"))?;
                total_micros += hours * 3_600_000_000;
                remaining = &remaining[h_pos + 1..];
                found_any = true;
            }

            // Parse optional nM (minutes, since we are in the T section)
            if let Some(m_pos) = remaining.find('M') {
                let minutes: i64 = remaining[..m_pos]
                    .parse()
                    .map_err(|_| format!("Invalid minute component in dayTimeDuration: {s}"))?;
                total_micros += minutes * 60_000_000;
                remaining = &remaining[m_pos + 1..];
                found_any = true;
            }

            // Parse optional n.nS
            if let Some(s_pos) = remaining.find('S') {
                let sec_str = &remaining[..s_pos];
                let sec_micros = parse_seconds_to_micros(sec_str)
                    .map_err(|e| format!("Invalid seconds in dayTimeDuration '{s}': {e}"))?;
                total_micros += sec_micros;
                remaining = &remaining[s_pos + 1..];
                found_any = true;
            }

            if !remaining.is_empty() {
                return Err(format!(
                    "Unexpected trailing content in dayTimeDuration time part: {s}"
                ));
            }
        }

        if !found_any {
            return Err(format!(
                "dayTimeDuration must have at least one of D, H, M, or S: {s}"
            ));
        }

        let total_micros = if negative {
            -total_micros
        } else {
            total_micros
        };

        Ok(Self {
            micros: total_micros,
            original: s.to_string(),
        })
    }

    /// Construct from total microseconds.
    pub fn from_micros(micros: i64) -> Self {
        let original = Self::make_canonical(micros);
        Self { micros, original }
    }

    /// Canonical string representation.
    pub fn to_canonical_string(&self) -> String {
        Self::make_canonical(self.micros)
    }

    fn make_canonical(micros: i64) -> String {
        let negative = micros < 0;
        let abs = micros.unsigned_abs();
        let prefix = if negative { "-P" } else { "P" };

        let total_secs = abs / 1_000_000;
        let frac_micros = abs % 1_000_000;

        let days = total_secs / 86400;
        let rem = total_secs % 86400;
        let hours = rem / 3600;
        let rem = rem % 3600;
        let minutes = rem / 60;
        let secs = rem % 60;

        let mut result = prefix.to_string();
        let mut has_date = false;
        if days > 0 {
            result.push_str(&format!("{days}D"));
            has_date = true;
        }

        let has_time = hours > 0 || minutes > 0 || secs > 0 || frac_micros > 0;
        if has_time {
            result.push('T');
            if hours > 0 {
                result.push_str(&format!("{hours}H"));
            }
            if minutes > 0 {
                result.push_str(&format!("{minutes}M"));
            }
            if secs > 0 || frac_micros > 0 {
                if frac_micros > 0 {
                    // Format fractional seconds, trimming trailing zeros
                    let frac_str = format!("{frac_micros:06}");
                    let trimmed = frac_str.trim_end_matches('0');
                    result.push_str(&format!("{secs}.{trimmed}S"));
                } else {
                    result.push_str(&format!("{secs}S"));
                }
            }
        } else if !has_date {
            // Zero duration
            result.push_str("T0S");
        }

        result
    }

    pub fn micros(&self) -> i64 {
        self.micros
    }

    /// Get the original string representation
    pub fn original(&self) -> &str {
        &self.original
    }

    /// Total whole days (truncated toward zero).
    pub fn days(&self) -> i64 {
        self.micros / 86_400_000_000
    }

    /// Hours component after extracting whole days (0-23).
    pub fn hours(&self) -> i64 {
        (self.micros.abs() % 86_400_000_000) / 3_600_000_000
    }

    /// Minutes component after extracting hours (0-59).
    pub fn minutes(&self) -> i64 {
        (self.micros.abs() % 3_600_000_000) / 60_000_000
    }

    /// Fractional seconds component (0.0 .. 60.0).
    pub fn fractional_seconds(&self) -> f64 {
        let rem = self.micros.abs() % 60_000_000;
        rem as f64 / 1_000_000.0
    }
}

impl PartialEq for DayTimeDuration {
    fn eq(&self, other: &Self) -> bool {
        self.micros == other.micros
    }
}

impl Eq for DayTimeDuration {}

impl Ord for DayTimeDuration {
    fn cmp(&self, other: &Self) -> Ordering {
        self.micros.cmp(&other.micros)
    }
}

impl PartialOrd for DayTimeDuration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for DayTimeDuration {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.micros.hash(state);
    }
}

impl fmt::Display for DayTimeDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

// ============================================================================
// 8. Duration
// ============================================================================

/// XSD duration — general duration with both year-month and day-time components
///
/// NOT totally orderable. Two durations are indeterminate when months and micros
/// disagree in direction (e.g., P1M vs P31D — is a month longer than 31 days?).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Duration {
    months: i32,
    micros: i64,
    original: String,
}

impl Duration {
    /// Parse a full XSD duration string.
    ///
    /// Accepts: `"P1Y2M3DT4H5M6S"`, `"P1Y"`, `"PT1H"`, `"-P1Y2M3DT4H5M6.789S"`
    pub fn parse(s: &str) -> Result<Self, String> {
        let (negative, rest) = s.strip_prefix('-').map(|r| (true, r)).unwrap_or((false, s));

        let body = rest
            .strip_prefix('P')
            .ok_or_else(|| format!("Duration must start with 'P' (or '-P'): {s}"))?;

        // Split on 'T' to separate date-part from time-part
        let (date_part, time_part) = if let Some(t_pos) = body.find('T') {
            (&body[..t_pos], Some(&body[t_pos + 1..]))
        } else {
            (body, None)
        };

        let mut total_months: i32 = 0;
        let mut total_micros: i64 = 0;
        let mut found_any = false;

        // Parse date components: nY nM nD
        let mut remaining = date_part;

        if let Some(y_pos) = remaining.find('Y') {
            let years: i32 = remaining[..y_pos]
                .parse()
                .map_err(|_| format!("Invalid year component in duration: {s}"))?;
            total_months += years * 12;
            remaining = &remaining[y_pos + 1..];
            found_any = true;
        }

        // 'M' in date part means months
        if let Some(m_pos) = remaining.find('M') {
            let months_val: i32 = remaining[..m_pos]
                .parse()
                .map_err(|_| format!("Invalid month component in duration: {s}"))?;
            total_months += months_val;
            remaining = &remaining[m_pos + 1..];
            found_any = true;
        }

        if let Some(d_pos) = remaining.find('D') {
            let days: i64 = remaining[..d_pos]
                .parse()
                .map_err(|_| format!("Invalid day component in duration: {s}"))?;
            total_micros += days * 86_400_000_000;
            remaining = &remaining[d_pos + 1..];
            found_any = true;
        }

        if !remaining.is_empty() {
            return Err(format!("Unexpected content in date part of duration: {s}"));
        }

        // Parse time components: nH nM nS
        if let Some(tp) = time_part {
            if tp.is_empty() {
                return Err(format!("Empty time part after 'T' in duration: {s}"));
            }

            let mut remaining = tp;

            if let Some(h_pos) = remaining.find('H') {
                let hours: i64 = remaining[..h_pos]
                    .parse()
                    .map_err(|_| format!("Invalid hour component in duration: {s}"))?;
                total_micros += hours * 3_600_000_000;
                remaining = &remaining[h_pos + 1..];
                found_any = true;
            }

            // 'M' in time part means minutes
            if let Some(m_pos) = remaining.find('M') {
                let minutes: i64 = remaining[..m_pos]
                    .parse()
                    .map_err(|_| format!("Invalid minute component in duration: {s}"))?;
                total_micros += minutes * 60_000_000;
                remaining = &remaining[m_pos + 1..];
                found_any = true;
            }

            if let Some(s_pos) = remaining.find('S') {
                let sec_str = &remaining[..s_pos];
                let sec_micros = parse_seconds_to_micros(sec_str)
                    .map_err(|e| format!("Invalid seconds in duration '{s}': {e}"))?;
                total_micros += sec_micros;
                remaining = &remaining[s_pos + 1..];
                found_any = true;
            }

            if !remaining.is_empty() {
                return Err(format!(
                    "Unexpected trailing content in duration time part: {s}"
                ));
            }
        }

        if !found_any {
            return Err(format!(
                "Duration must have at least one component (Y, M, D, H, M, S): {s}"
            ));
        }

        if negative {
            total_months = -total_months;
            total_micros = -total_micros;
        }

        Ok(Self {
            months: total_months,
            micros: total_micros,
            original: s.to_string(),
        })
    }

    /// Canonical string representation combining year-month and day-time parts.
    ///
    /// E.g., months=14, micros=90_000_000_000 -> "P1Y2MT1H30M"
    pub fn to_canonical_string(&self) -> String {
        let neg_months = self.months < 0;
        let neg_micros = self.micros < 0;
        // If both are zero or agree in sign, produce a single canonical form.
        // If they disagree in sign, we still produce a string but note it is
        // not standard — this shouldn't happen with valid XSD durations.
        let negative = neg_months || neg_micros;

        let abs_months = self.months.unsigned_abs();
        let abs_micros = self.micros.unsigned_abs();

        let years = abs_months / 12;
        let months = abs_months % 12;

        let total_secs = abs_micros / 1_000_000;
        let frac_micros = abs_micros % 1_000_000;
        let days = total_secs / 86400;
        let rem = total_secs % 86400;
        let hours = rem / 3600;
        let rem = rem % 3600;
        let minutes = rem / 60;
        let secs = rem % 60;

        let prefix = if negative { "-P" } else { "P" };
        let mut result = prefix.to_string();

        let mut has_date = false;
        if years > 0 {
            result.push_str(&format!("{years}Y"));
            has_date = true;
        }
        if months > 0 {
            result.push_str(&format!("{months}M"));
            has_date = true;
        }
        if days > 0 {
            result.push_str(&format!("{days}D"));
            has_date = true;
        }

        let has_time = hours > 0 || minutes > 0 || secs > 0 || frac_micros > 0;
        if has_time {
            result.push('T');
            if hours > 0 {
                result.push_str(&format!("{hours}H"));
            }
            if minutes > 0 {
                result.push_str(&format!("{minutes}M"));
            }
            if secs > 0 || frac_micros > 0 {
                if frac_micros > 0 {
                    let frac_str = format!("{frac_micros:06}");
                    let trimmed = frac_str.trim_end_matches('0');
                    result.push_str(&format!("{secs}.{trimmed}S"));
                } else {
                    result.push_str(&format!("{secs}S"));
                }
            }
        } else if !has_date {
            // Zero duration
            result.push_str("T0S");
        }

        result
    }

    pub fn months(&self) -> i32 {
        self.months
    }

    pub fn micros(&self) -> i64 {
        self.micros
    }

    /// Get the original string representation
    pub fn original(&self) -> &str {
        &self.original
    }

    /// True if this duration has only year-month components (micros == 0).
    pub fn is_year_month_duration(&self) -> bool {
        self.micros == 0
    }

    /// True if this duration has only day-time components (months == 0).
    pub fn is_day_time_duration(&self) -> bool {
        self.months == 0
    }
}

impl PartialEq for Duration {
    fn eq(&self, other: &Self) -> bool {
        self.months == other.months && self.micros == other.micros
    }
}

impl Eq for Duration {}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for Duration {
    /// Partial ordering for durations.
    ///
    /// Returns `None` (indeterminate) when months and micros components disagree
    /// in sign direction between the two durations being compared. For example,
    /// P1M vs P31D is indeterminate because a month can be 28, 29, 30, or 31 days.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let month_cmp = self.months.cmp(&other.months);
        let micro_cmp = self.micros.cmp(&other.micros);

        match (month_cmp, micro_cmp) {
            (Ordering::Equal, _) => Some(micro_cmp),
            (_, Ordering::Equal) => Some(month_cmp),
            (Ordering::Less, Ordering::Less) => Some(Ordering::Less),
            (Ordering::Greater, Ordering::Greater) => Some(Ordering::Greater),
            // Components disagree — ordering is indeterminate
            _ => None,
        }
    }
}

impl Ord for Duration {
    /// Storage order only — not semantic comparison. Use PartialOrd for
    /// semantically correct comparison.
    fn cmp(&self, other: &Self) -> Ordering {
        (self.months, self.micros).cmp(&(other.months, other.micros))
    }
}

impl std::hash::Hash for Duration {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.months.hash(state);
        self.micros.hash(state);
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.original)
    }
}

// ============================================================================
// Shared helper for parsing fractional seconds
// ============================================================================

/// Parse a seconds string (possibly fractional) into microseconds.
///
/// Examples: `"6"` -> 6_000_000, `"6.789"` -> 6_789_000, `"0.5"` -> 500_000
fn parse_seconds_to_micros(s: &str) -> Result<i64, String> {
    if let Some((whole_str, frac_str)) = s.split_once('.') {
        let whole: i64 = if whole_str.is_empty() {
            0
        } else {
            whole_str
                .parse()
                .map_err(|_| format!("Invalid whole seconds: {s}"))?
        };

        // Pad or truncate fractional part to exactly 6 digits (microseconds)
        let padded = if frac_str.len() >= 6 {
            frac_str[..6].to_string()
        } else {
            format!("{frac_str:0<6}")
        };

        let frac_micros: i64 = padded
            .parse()
            .map_err(|_| format!("Invalid fractional seconds: {s}"))?;

        Ok(whole * 1_000_000 + frac_micros)
    } else {
        let whole: i64 = s.parse().map_err(|_| format!("Invalid seconds: {s}"))?;
        Ok(whole * 1_000_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datetime_parse_rfc3339() {
        let dt = DateTime::parse("2024-01-15T10:30:00Z").unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hours(), 10);
        assert_eq!(dt.minutes(), 30);
        assert_eq!(dt.timezone(), Some("Z".to_string()));
    }

    #[test]
    fn test_datetime_parse_with_offset() {
        let dt = DateTime::parse("2024-01-15T10:30:00+05:00").unwrap();
        assert_eq!(dt.hours(), 5); // Normalized to UTC
        assert_eq!(dt.timezone(), Some("+05:00".to_string()));
    }

    #[test]
    fn test_datetime_parse_no_timezone() {
        let dt = DateTime::parse("2024-01-15T10:30:00").unwrap();
        assert_eq!(dt.hours(), 10);
        assert!(dt.timezone().is_none());
    }

    #[test]
    fn test_datetime_equality_same_instant() {
        // These represent the same instant
        let dt1 = DateTime::parse("2024-01-01T05:00:00Z").unwrap();
        let dt2 = DateTime::parse("2024-01-01T00:00:00-05:00").unwrap();
        assert_eq!(dt1, dt2);
    }

    #[test]
    fn test_datetime_ordering() {
        let dt1 = DateTime::parse("2024-01-01T00:00:00Z").unwrap();
        let dt2 = DateTime::parse("2024-01-01T01:00:00Z").unwrap();
        assert!(dt1 < dt2);
    }

    #[test]
    fn test_date_parse() {
        let d = Date::parse("2024-01-15").unwrap();
        assert_eq!(d.year(), 2024);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 15);
        assert!(d.timezone().is_none());
    }

    #[test]
    fn test_date_parse_with_timezone() {
        let d = Date::parse("2024-01-15Z").unwrap();
        assert_eq!(d.timezone(), Some("Z".to_string()));

        let d2 = Date::parse("2024-01-15+05:00").unwrap();
        assert_eq!(d2.timezone(), Some("+05:00".to_string()));
    }

    #[test]
    fn test_time_parse() {
        let t = Time::parse("10:30:00").unwrap();
        assert_eq!(t.hours(), 10);
        assert_eq!(t.minutes(), 30);
        assert!(t.timezone().is_none());
    }

    #[test]
    fn test_time_parse_with_timezone() {
        let t = Time::parse("10:30:00Z").unwrap();
        assert_eq!(t.timezone(), Some("Z".to_string()));

        let t2 = Time::parse("10:30:00+05:00").unwrap();
        assert_eq!(t2.timezone(), Some("+05:00".to_string()));
    }

    #[test]
    fn test_time_ordering_with_timezone() {
        // 10:00:00Z and 15:00:00+05:00 are the same UTC time
        let t1 = Time::parse("10:00:00Z").unwrap();
        let t2 = Time::parse("15:00:00+05:00").unwrap();
        assert_eq!(t1, t2);
    }

    // ---- parse_tz_suffix / format_tz_suffix ----

    #[test]
    fn test_parse_tz_suffix_z() {
        let (val, tz) = parse_tz_suffix("2024Z");
        assert_eq!(val, "2024");
        assert_eq!(tz.unwrap().local_minus_utc(), 0);
    }

    #[test]
    fn test_parse_tz_suffix_positive_offset() {
        let (val, tz) = parse_tz_suffix("2024+05:00");
        assert_eq!(val, "2024");
        assert_eq!(tz.unwrap().local_minus_utc(), 5 * 3600);
    }

    #[test]
    fn test_parse_tz_suffix_negative_offset() {
        let (val, tz) = parse_tz_suffix("2024-05:30");
        assert_eq!(val, "2024");
        assert_eq!(tz.unwrap().local_minus_utc(), -(5 * 3600 + 30 * 60));
    }

    #[test]
    fn test_parse_tz_suffix_none() {
        let (val, tz) = parse_tz_suffix("2024");
        assert_eq!(val, "2024");
        assert!(tz.is_none());
    }

    #[test]
    fn test_format_tz_suffix_none() {
        assert_eq!(format_tz_suffix(None), "");
    }

    #[test]
    fn test_format_tz_suffix_utc() {
        let tz = FixedOffset::east_opt(0).unwrap();
        assert_eq!(format_tz_suffix(Some(tz)), "Z");
    }

    #[test]
    fn test_format_tz_suffix_positive() {
        let tz = FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
        assert_eq!(format_tz_suffix(Some(tz)), "+05:30");
    }

    #[test]
    fn test_format_tz_suffix_negative() {
        let tz = FixedOffset::east_opt(-8 * 3600).unwrap();
        assert_eq!(format_tz_suffix(Some(tz)), "-08:00");
    }

    // ---- GYear ----

    #[test]
    fn test_gyear_parse_plain() {
        let y = GYear::parse("2024").unwrap();
        assert_eq!(y.year(), 2024);
        assert!(y.tz_offset().is_none());
        assert_eq!(y.original(), "2024");
    }

    #[test]
    fn test_gyear_parse_with_z() {
        let y = GYear::parse("2024Z").unwrap();
        assert_eq!(y.year(), 2024);
        assert_eq!(y.tz_offset().unwrap().local_minus_utc(), 0);
    }

    #[test]
    fn test_gyear_parse_with_offset() {
        let y = GYear::parse("2024+05:00").unwrap();
        assert_eq!(y.year(), 2024);
        assert_eq!(y.tz_offset().unwrap().local_minus_utc(), 5 * 3600);
    }

    #[test]
    fn test_gyear_parse_negative() {
        let y = GYear::parse("-0050").unwrap();
        assert_eq!(y.year(), -50);
    }

    #[test]
    fn test_gyear_parse_negative_with_z() {
        let y = GYear::parse("-0050Z").unwrap();
        assert_eq!(y.year(), -50);
        assert_eq!(y.tz_offset().unwrap().local_minus_utc(), 0);
    }

    #[test]
    fn test_gyear_from_year() {
        let y = GYear::from_year(2024);
        assert_eq!(y.year(), 2024);
        assert_eq!(y.canonical(), "2024");
    }

    #[test]
    fn test_gyear_from_year_negative() {
        let y = GYear::from_year(-50);
        assert_eq!(y.canonical(), "-0050");
    }

    #[test]
    fn test_gyear_from_year_large() {
        let y = GYear::from_year(12345);
        assert_eq!(y.canonical(), "12345");
    }

    #[test]
    fn test_gyear_equality_ignores_tz() {
        let y1 = GYear::parse("2024").unwrap();
        let y2 = GYear::parse("2024Z").unwrap();
        assert_eq!(y1, y2);
    }

    #[test]
    fn test_gyear_ordering() {
        let y1 = GYear::parse("-0050").unwrap();
        let y2 = GYear::parse("2024").unwrap();
        assert!(y1 < y2);
    }

    #[test]
    fn test_gyear_display() {
        let y = GYear::parse("2024+05:00").unwrap();
        assert_eq!(format!("{y}"), "2024+05:00");
    }

    #[test]
    fn test_gyear_parse_invalid() {
        assert!(GYear::parse("abc").is_err());
        assert!(GYear::parse("").is_err());
    }

    // ---- GYearMonth ----

    #[test]
    fn test_gyearmonth_parse_plain() {
        let ym = GYearMonth::parse("2024-01").unwrap();
        assert_eq!(ym.year(), 2024);
        assert_eq!(ym.month(), 1);
        assert!(ym.tz_offset().is_none());
    }

    #[test]
    fn test_gyearmonth_parse_with_z() {
        let ym = GYearMonth::parse("2024-01Z").unwrap();
        assert_eq!(ym.year(), 2024);
        assert_eq!(ym.month(), 1);
        assert_eq!(ym.tz_offset().unwrap().local_minus_utc(), 0);
    }

    #[test]
    fn test_gyearmonth_parse_with_offset() {
        let ym = GYearMonth::parse("2024-06+05:00").unwrap();
        assert_eq!(ym.year(), 2024);
        assert_eq!(ym.month(), 6);
        assert_eq!(ym.tz_offset().unwrap().local_minus_utc(), 5 * 3600);
    }

    #[test]
    fn test_gyearmonth_parse_negative_year() {
        let ym = GYearMonth::parse("-0050-06").unwrap();
        assert_eq!(ym.year(), -50);
        assert_eq!(ym.month(), 6);
    }

    #[test]
    fn test_gyearmonth_from_components() {
        let ym = GYearMonth::from_components(2024, 3);
        assert_eq!(ym.year(), 2024);
        assert_eq!(ym.month(), 3);
        assert_eq!(ym.original(), "2024-03");
    }

    #[test]
    fn test_gyearmonth_from_components_negative() {
        let ym = GYearMonth::from_components(-50, 6);
        assert_eq!(ym.original(), "-0050-06");
    }

    #[test]
    fn test_gyearmonth_equality() {
        let ym1 = GYearMonth::parse("2024-01").unwrap();
        let ym2 = GYearMonth::parse("2024-01Z").unwrap();
        assert_eq!(ym1, ym2);
    }

    #[test]
    fn test_gyearmonth_ordering() {
        let ym1 = GYearMonth::parse("2024-01").unwrap();
        let ym2 = GYearMonth::parse("2024-06").unwrap();
        let ym3 = GYearMonth::parse("2025-01").unwrap();
        assert!(ym1 < ym2);
        assert!(ym2 < ym3);
    }

    #[test]
    fn test_gyearmonth_invalid_month() {
        assert!(GYearMonth::parse("2024-13").is_err());
        assert!(GYearMonth::parse("2024-00").is_err());
    }

    #[test]
    fn test_gyearmonth_display() {
        let ym = GYearMonth::parse("2024-01+05:00").unwrap();
        assert_eq!(format!("{ym}"), "2024-01+05:00");
    }

    // ---- GMonth ----

    #[test]
    fn test_gmonth_parse_plain() {
        let m = GMonth::parse("--01").unwrap();
        assert_eq!(m.month(), 1);
        assert!(m.tz_offset().is_none());
    }

    #[test]
    fn test_gmonth_parse_with_z() {
        let m = GMonth::parse("--07Z").unwrap();
        assert_eq!(m.month(), 7);
        assert_eq!(m.tz_offset().unwrap().local_minus_utc(), 0);
    }

    #[test]
    fn test_gmonth_parse_with_offset() {
        let m = GMonth::parse("--12+05:00").unwrap();
        assert_eq!(m.month(), 12);
        assert_eq!(m.tz_offset().unwrap().local_minus_utc(), 5 * 3600);
    }

    #[test]
    fn test_gmonth_from_month() {
        let m = GMonth::from_month(3);
        assert_eq!(m.month(), 3);
        assert_eq!(m.original(), "--03");
    }

    #[test]
    fn test_gmonth_equality() {
        let m1 = GMonth::parse("--01").unwrap();
        let m2 = GMonth::parse("--01Z").unwrap();
        assert_eq!(m1, m2);
    }

    #[test]
    fn test_gmonth_ordering() {
        let m1 = GMonth::parse("--01").unwrap();
        let m2 = GMonth::parse("--12").unwrap();
        assert!(m1 < m2);
    }

    #[test]
    fn test_gmonth_invalid() {
        assert!(GMonth::parse("--00").is_err());
        assert!(GMonth::parse("--13").is_err());
        assert!(GMonth::parse("01").is_err()); // no -- prefix
    }

    #[test]
    fn test_gmonth_display() {
        let m = GMonth::parse("--07Z").unwrap();
        assert_eq!(format!("{m}"), "--07Z");
    }

    // ---- GDay ----

    #[test]
    fn test_gday_parse_plain() {
        let d = GDay::parse("---15").unwrap();
        assert_eq!(d.day(), 15);
        assert!(d.tz_offset().is_none());
    }

    #[test]
    fn test_gday_parse_with_z() {
        let d = GDay::parse("---01Z").unwrap();
        assert_eq!(d.day(), 1);
        assert_eq!(d.tz_offset().unwrap().local_minus_utc(), 0);
    }

    #[test]
    fn test_gday_parse_with_offset() {
        let d = GDay::parse("---31+05:00").unwrap();
        assert_eq!(d.day(), 31);
        assert_eq!(d.tz_offset().unwrap().local_minus_utc(), 5 * 3600);
    }

    #[test]
    fn test_gday_from_day() {
        let d = GDay::from_day(7);
        assert_eq!(d.day(), 7);
        assert_eq!(d.original(), "---07");
    }

    #[test]
    fn test_gday_equality() {
        let d1 = GDay::parse("---15").unwrap();
        let d2 = GDay::parse("---15Z").unwrap();
        assert_eq!(d1, d2);
    }

    #[test]
    fn test_gday_ordering() {
        let d1 = GDay::parse("---01").unwrap();
        let d2 = GDay::parse("---31").unwrap();
        assert!(d1 < d2);
    }

    #[test]
    fn test_gday_invalid() {
        assert!(GDay::parse("---00").is_err());
        assert!(GDay::parse("---32").is_err());
        assert!(GDay::parse("15").is_err()); // no --- prefix
    }

    #[test]
    fn test_gday_display() {
        let d = GDay::parse("---15+05:00").unwrap();
        assert_eq!(format!("{d}"), "---15+05:00");
    }

    // ---- GMonthDay ----

    #[test]
    fn test_gmonthday_parse_plain() {
        let md = GMonthDay::parse("--01-15").unwrap();
        assert_eq!(md.month(), 1);
        assert_eq!(md.day(), 15);
        assert!(md.tz_offset().is_none());
    }

    #[test]
    fn test_gmonthday_parse_with_z() {
        let md = GMonthDay::parse("--12-25Z").unwrap();
        assert_eq!(md.month(), 12);
        assert_eq!(md.day(), 25);
        assert_eq!(md.tz_offset().unwrap().local_minus_utc(), 0);
    }

    #[test]
    fn test_gmonthday_parse_with_offset() {
        let md = GMonthDay::parse("--06-15+05:00").unwrap();
        assert_eq!(md.month(), 6);
        assert_eq!(md.day(), 15);
        assert_eq!(md.tz_offset().unwrap().local_minus_utc(), 5 * 3600);
    }

    #[test]
    fn test_gmonthday_from_components() {
        let md = GMonthDay::from_components(3, 14);
        assert_eq!(md.month(), 3);
        assert_eq!(md.day(), 14);
        assert_eq!(md.original(), "--03-14");
    }

    #[test]
    fn test_gmonthday_equality() {
        let md1 = GMonthDay::parse("--01-15").unwrap();
        let md2 = GMonthDay::parse("--01-15Z").unwrap();
        assert_eq!(md1, md2);
    }

    #[test]
    fn test_gmonthday_ordering() {
        let md1 = GMonthDay::parse("--01-15").unwrap();
        let md2 = GMonthDay::parse("--02-01").unwrap();
        let md3 = GMonthDay::parse("--01-20").unwrap();
        assert!(md1 < md2); // different months
        assert!(md1 < md3); // same month, different day
    }

    #[test]
    fn test_gmonthday_invalid() {
        assert!(GMonthDay::parse("--13-01").is_err());
        assert!(GMonthDay::parse("--01-32").is_err());
        assert!(GMonthDay::parse("01-15").is_err()); // no -- prefix
    }

    #[test]
    fn test_gmonthday_display() {
        let md = GMonthDay::parse("--12-25Z").unwrap();
        assert_eq!(format!("{md}"), "--12-25Z");
    }

    // ---- YearMonthDuration ----

    #[test]
    fn test_ymd_parse_years_and_months() {
        let d = YearMonthDuration::parse("P1Y2M").unwrap();
        assert_eq!(d.months(), 14);
        assert_eq!(d.years(), 1);
        assert_eq!(d.remaining_months(), 2);
    }

    #[test]
    fn test_ymd_parse_years_only() {
        let d = YearMonthDuration::parse("P3Y").unwrap();
        assert_eq!(d.months(), 36);
        assert_eq!(d.years(), 3);
        assert_eq!(d.remaining_months(), 0);
    }

    #[test]
    fn test_ymd_parse_months_only() {
        let d = YearMonthDuration::parse("P14M").unwrap();
        assert_eq!(d.months(), 14);
    }

    #[test]
    fn test_ymd_parse_zero() {
        let d = YearMonthDuration::parse("P0Y").unwrap();
        assert_eq!(d.months(), 0);

        let d2 = YearMonthDuration::parse("P0M").unwrap();
        assert_eq!(d2.months(), 0);
    }

    #[test]
    fn test_ymd_parse_negative() {
        let d = YearMonthDuration::parse("-P1Y").unwrap();
        assert_eq!(d.months(), -12);

        let d2 = YearMonthDuration::parse("-P1Y2M").unwrap();
        assert_eq!(d2.months(), -14);
    }

    #[test]
    fn test_ymd_from_months() {
        let d = YearMonthDuration::from_months(14);
        assert_eq!(d.months(), 14);
        assert_eq!(d.to_canonical_string(), "P1Y2M");
    }

    #[test]
    fn test_ymd_from_months_zero() {
        let d = YearMonthDuration::from_months(0);
        assert_eq!(d.to_canonical_string(), "P0M");
    }

    #[test]
    fn test_ymd_from_months_negative() {
        let d = YearMonthDuration::from_months(-14);
        assert_eq!(d.to_canonical_string(), "-P1Y2M");
    }

    #[test]
    fn test_ymd_from_months_years_only() {
        let d = YearMonthDuration::from_months(24);
        assert_eq!(d.to_canonical_string(), "P2Y");
    }

    #[test]
    fn test_ymd_from_months_months_only() {
        let d = YearMonthDuration::from_months(5);
        assert_eq!(d.to_canonical_string(), "P5M");
    }

    #[test]
    fn test_ymd_equality() {
        let d1 = YearMonthDuration::parse("P1Y2M").unwrap();
        let d2 = YearMonthDuration::parse("P14M").unwrap();
        assert_eq!(d1, d2);
    }

    #[test]
    fn test_ymd_ordering() {
        let d1 = YearMonthDuration::parse("-P1Y").unwrap();
        let d2 = YearMonthDuration::parse("P0M").unwrap();
        let d3 = YearMonthDuration::parse("P2Y").unwrap();
        assert!(d1 < d2);
        assert!(d2 < d3);
    }

    #[test]
    fn test_ymd_invalid_has_day() {
        assert!(YearMonthDuration::parse("P1Y2M3D").is_err());
    }

    #[test]
    fn test_ymd_invalid_has_time() {
        assert!(YearMonthDuration::parse("P1YT1H").is_err());
    }

    #[test]
    fn test_ymd_invalid_no_component() {
        assert!(YearMonthDuration::parse("P").is_err());
    }

    #[test]
    fn test_ymd_invalid_no_p() {
        assert!(YearMonthDuration::parse("1Y2M").is_err());
    }

    #[test]
    fn test_ymd_display() {
        let d = YearMonthDuration::parse("P1Y2M").unwrap();
        assert_eq!(format!("{d}"), "P1Y2M");
    }

    // ---- DayTimeDuration ----

    #[test]
    fn test_dtd_parse_full() {
        let d = DayTimeDuration::parse("P3DT4H5M6S").unwrap();
        let expected = 3 * 86_400_000_000_i64 + 4 * 3_600_000_000 + 5 * 60_000_000 + 6 * 1_000_000;
        assert_eq!(d.micros(), expected);
    }

    #[test]
    fn test_dtd_parse_fractional_seconds() {
        let d = DayTimeDuration::parse("P3DT4H5M6.789S").unwrap();
        let expected =
            3 * 86_400_000_000_i64 + 4 * 3_600_000_000 + 5 * 60_000_000 + 6 * 1_000_000 + 789_000;
        assert_eq!(d.micros(), expected);
    }

    #[test]
    fn test_dtd_parse_hours_minutes() {
        let d = DayTimeDuration::parse("PT1H30M").unwrap();
        assert_eq!(d.micros(), 3_600_000_000_i64 + 30 * 60_000_000);
    }

    #[test]
    fn test_dtd_parse_days_only() {
        let d = DayTimeDuration::parse("P1D").unwrap();
        assert_eq!(d.micros(), 86_400_000_000);
    }

    #[test]
    fn test_dtd_parse_seconds_only() {
        let d = DayTimeDuration::parse("PT1S").unwrap();
        assert_eq!(d.micros(), 1_000_000);
    }

    #[test]
    fn test_dtd_parse_negative() {
        let d = DayTimeDuration::parse("-PT1S").unwrap();
        assert_eq!(d.micros(), -1_000_000);
    }

    #[test]
    fn test_dtd_parse_sub_second() {
        let d = DayTimeDuration::parse("PT0.5S").unwrap();
        assert_eq!(d.micros(), 500_000);
    }

    #[test]
    fn test_dtd_from_micros() {
        let d = DayTimeDuration::from_micros(90_000_000_000);
        assert_eq!(d.days(), 1);
        assert_eq!(d.hours(), 1);
        assert_eq!(d.minutes(), 0);
        assert_eq!(d.to_canonical_string(), "P1DT1H");
    }

    #[test]
    fn test_dtd_from_micros_zero() {
        let d = DayTimeDuration::from_micros(0);
        assert_eq!(d.to_canonical_string(), "PT0S");
    }

    #[test]
    fn test_dtd_from_micros_negative() {
        let d = DayTimeDuration::from_micros(-3_661_500_000);
        assert_eq!(d.to_canonical_string(), "-PT1H1M1.5S");
    }

    #[test]
    fn test_dtd_from_micros_fractional() {
        let d = DayTimeDuration::from_micros(1_500_000);
        assert_eq!(d.to_canonical_string(), "PT1.5S");
    }

    #[test]
    fn test_dtd_accessors() {
        let d = DayTimeDuration::parse("P2DT3H4M5.678S").unwrap();
        assert_eq!(d.days(), 2);
        assert_eq!(d.hours(), 3);
        assert_eq!(d.minutes(), 4);
        let fsec = d.fractional_seconds();
        assert!((fsec - 5.678).abs() < 0.0001);
    }

    #[test]
    fn test_dtd_equality() {
        let d1 = DayTimeDuration::parse("PT90M").unwrap();
        let d2 = DayTimeDuration::parse("PT1H30M").unwrap();
        assert_eq!(d1, d2);
    }

    #[test]
    fn test_dtd_ordering() {
        let d1 = DayTimeDuration::parse("-PT1S").unwrap();
        let d2 = DayTimeDuration::parse("PT0S").unwrap();
        let d3 = DayTimeDuration::parse("P1D").unwrap();
        assert!(d1 < d2);
        assert!(d2 < d3);
    }

    #[test]
    fn test_dtd_invalid_has_year() {
        assert!(DayTimeDuration::parse("P1Y2DT3H").is_err());
    }

    #[test]
    fn test_dtd_invalid_has_month() {
        assert!(DayTimeDuration::parse("P1M2D").is_err());
    }

    #[test]
    fn test_dtd_invalid_no_component() {
        assert!(DayTimeDuration::parse("P").is_err());
    }

    #[test]
    fn test_dtd_invalid_empty_time() {
        assert!(DayTimeDuration::parse("PT").is_err());
    }

    #[test]
    fn test_dtd_display() {
        let d = DayTimeDuration::parse("P3DT4H5M6.789S").unwrap();
        assert_eq!(format!("{d}"), "P3DT4H5M6.789S");
    }

    // ---- Duration ----

    #[test]
    fn test_duration_parse_full() {
        let d = Duration::parse("P1Y2M3DT4H5M6S").unwrap();
        assert_eq!(d.months(), 14);
        let expected_micros =
            3 * 86_400_000_000_i64 + 4 * 3_600_000_000 + 5 * 60_000_000 + 6 * 1_000_000;
        assert_eq!(d.micros(), expected_micros);
    }

    #[test]
    fn test_duration_parse_year_only() {
        let d = Duration::parse("P1Y").unwrap();
        assert_eq!(d.months(), 12);
        assert_eq!(d.micros(), 0);
        assert!(d.is_year_month_duration());
    }

    #[test]
    fn test_duration_parse_time_only() {
        let d = Duration::parse("PT1H").unwrap();
        assert_eq!(d.months(), 0);
        assert_eq!(d.micros(), 3_600_000_000);
        assert!(d.is_day_time_duration());
    }

    #[test]
    fn test_duration_parse_negative() {
        let d = Duration::parse("-P1Y2M3DT4H5M6.789S").unwrap();
        assert_eq!(d.months(), -14);
        let expected_micros = -(3 * 86_400_000_000_i64
            + 4 * 3_600_000_000
            + 5 * 60_000_000
            + 6 * 1_000_000
            + 789_000);
        assert_eq!(d.micros(), expected_micros);
    }

    #[test]
    fn test_duration_parse_fractional_seconds() {
        let d = Duration::parse("PT0.001S").unwrap();
        assert_eq!(d.micros(), 1000);
    }

    #[test]
    fn test_duration_canonical_year_month_only() {
        let d = Duration::parse("P1Y2M").unwrap();
        assert_eq!(d.to_canonical_string(), "P1Y2M");
    }

    #[test]
    fn test_duration_canonical_day_time_only() {
        let d = Duration::parse("P3DT4H").unwrap();
        assert_eq!(d.to_canonical_string(), "P3DT4H");
    }

    #[test]
    fn test_duration_canonical_full() {
        let d = Duration::parse("P1Y2M3DT4H5M6S").unwrap();
        let canonical = d.to_canonical_string();
        assert_eq!(canonical, "P1Y2M3DT4H5M6S");
    }

    #[test]
    fn test_duration_canonical_negative() {
        let d = Duration::parse("-P1Y2M").unwrap();
        assert_eq!(d.to_canonical_string(), "-P1Y2M");
    }

    #[test]
    fn test_duration_canonical_zero() {
        let d = Duration::parse("P0Y").unwrap();
        assert_eq!(d.to_canonical_string(), "PT0S");
    }

    #[test]
    fn test_duration_equality() {
        let d1 = Duration::parse("P1Y2M").unwrap();
        let d2 = Duration::parse("P14M").unwrap();
        assert_eq!(d1, d2);
    }

    #[test]
    fn test_duration_inequality() {
        let d1 = Duration::parse("P1Y").unwrap();
        let d2 = Duration::parse("P1Y1D").unwrap();
        assert_ne!(d1, d2);
    }

    #[test]
    fn test_duration_partial_ord_both_agree() {
        let d1 = Duration::parse("P1Y").unwrap();
        let d2 = Duration::parse("P2Y").unwrap();
        assert_eq!(d1.partial_cmp(&d2), Some(Ordering::Less));
    }

    #[test]
    fn test_duration_partial_ord_equal() {
        let d1 = Duration::parse("P1Y2M3DT4H").unwrap();
        let d2 = Duration::parse("P14M3DT4H").unwrap();
        assert_eq!(d1.partial_cmp(&d2), Some(Ordering::Equal));
    }

    #[test]
    fn test_duration_partial_ord_indeterminate() {
        // P1M has months=1, micros=0
        // P31D has months=0, micros=31*86400*1e6
        // months: 1 > 0, micros: 0 < big => disagree => None
        let d1 = Duration::parse("P1M").unwrap();
        let d2 = Duration::parse("P31D").unwrap();
        assert_eq!(d1.partial_cmp(&d2), None);
    }

    #[test]
    fn test_duration_partial_ord_months_equal_micros_differ() {
        let d1 = Duration::parse("P1YT1H").unwrap();
        let d2 = Duration::parse("P1YT2H").unwrap();
        assert_eq!(d1.partial_cmp(&d2), Some(Ordering::Less));
    }

    #[test]
    fn test_duration_partial_ord_micros_equal_months_differ() {
        let d1 = Duration::parse("P1Y3D").unwrap();
        let d2 = Duration::parse("P2Y3D").unwrap();
        assert_eq!(d1.partial_cmp(&d2), Some(Ordering::Less));
    }

    #[test]
    fn test_duration_ord_storage_order() {
        // Ord is for storage, not semantic comparison
        let d1 = Duration::parse("P1M").unwrap();
        let d2 = Duration::parse("P31D").unwrap();
        // d1: months=1, micros=0; d2: months=0, micros=big
        // Storage order: compare (months, micros) as tuple
        // (1, 0) > (0, big) because months are compared first
        assert_eq!(d1.cmp(&d2), Ordering::Greater);
    }

    #[test]
    fn test_duration_is_year_month() {
        let d = Duration::parse("P1Y2M").unwrap();
        assert!(d.is_year_month_duration());
        assert!(!d.is_day_time_duration());
    }

    #[test]
    fn test_duration_is_day_time() {
        let d = Duration::parse("P3DT4H").unwrap();
        assert!(!d.is_year_month_duration());
        assert!(d.is_day_time_duration());
    }

    #[test]
    fn test_duration_invalid_no_component() {
        assert!(Duration::parse("P").is_err());
    }

    #[test]
    fn test_duration_invalid_no_p() {
        assert!(Duration::parse("1Y2M").is_err());
    }

    #[test]
    fn test_duration_invalid_empty_time() {
        assert!(Duration::parse("PT").is_err());
    }

    #[test]
    fn test_duration_display() {
        let d = Duration::parse("P1Y2M3DT4H5M6S").unwrap();
        assert_eq!(format!("{d}"), "P1Y2M3DT4H5M6S");
    }

    // ---- parse_seconds_to_micros ----

    #[test]
    fn test_parse_seconds_whole() {
        assert_eq!(parse_seconds_to_micros("6").unwrap(), 6_000_000);
    }

    #[test]
    fn test_parse_seconds_fractional() {
        assert_eq!(parse_seconds_to_micros("6.789").unwrap(), 6_789_000);
    }

    #[test]
    fn test_parse_seconds_short_fraction() {
        assert_eq!(parse_seconds_to_micros("0.5").unwrap(), 500_000);
    }

    #[test]
    fn test_parse_seconds_long_fraction() {
        // 6.1234567 -> truncated to 6 digits -> 123456 micros
        assert_eq!(parse_seconds_to_micros("6.1234567").unwrap(), 6_123_456);
    }

    #[test]
    fn test_parse_seconds_zero() {
        assert_eq!(parse_seconds_to_micros("0").unwrap(), 0);
    }

    #[test]
    fn test_parse_seconds_just_fraction() {
        // ".5" -> 0 whole + 500000
        assert_eq!(parse_seconds_to_micros(".5").unwrap(), 500_000);
    }

    #[test]
    fn test_parse_seconds_invalid() {
        assert!(parse_seconds_to_micros("abc").is_err());
    }

    // ---- Cross-type comparisons / edge cases ----

    #[test]
    fn test_dtd_zero_seconds() {
        let d = DayTimeDuration::parse("PT0S").unwrap();
        assert_eq!(d.micros(), 0);
        assert_eq!(d.to_canonical_string(), "PT0S");
    }

    #[test]
    fn test_dtd_minutes_only() {
        let d = DayTimeDuration::parse("PT45M").unwrap();
        assert_eq!(d.micros(), 45 * 60_000_000);
        assert_eq!(d.to_canonical_string(), "PT45M");
    }

    #[test]
    fn test_duration_mixed_ym_dt() {
        let d = Duration::parse("P1Y6MT12H").unwrap();
        assert_eq!(d.months(), 18);
        assert_eq!(d.micros(), 12 * 3_600_000_000);
        assert!(!d.is_year_month_duration());
        assert!(!d.is_day_time_duration());
    }

    #[test]
    fn test_dtd_canonical_complex() {
        // 100_000 seconds = 1 day, 3 hours, 46 minutes, 40 seconds
        let d = DayTimeDuration::from_micros(100_000_000_000);
        assert_eq!(d.to_canonical_string(), "P1DT3H46M40S");
    }

    #[test]
    fn test_ymd_canonical_roundtrip() {
        let inputs = ["P1Y2M", "P0M", "-P3Y", "P10M", "-P1Y11M"];
        for input in &inputs {
            let d = YearMonthDuration::parse(input).unwrap();
            let canonical = d.to_canonical_string();
            let d2 = YearMonthDuration::parse(&canonical).unwrap();
            assert_eq!(d, d2, "Roundtrip failed for {input}");
        }
    }

    #[test]
    fn test_dtd_canonical_roundtrip() {
        let inputs = ["P1D", "PT1H", "PT1M", "PT1S", "PT0.5S", "P1DT1H1M1S"];
        for input in &inputs {
            let d = DayTimeDuration::parse(input).unwrap();
            let canonical = d.to_canonical_string();
            let d2 = DayTimeDuration::parse(&canonical).unwrap();
            assert_eq!(d, d2, "Roundtrip failed for {input}");
        }
    }

    #[test]
    fn test_duration_hash_consistent_with_eq() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let d1 = Duration::parse("P1Y2M").unwrap();
        let d2 = Duration::parse("P14M").unwrap();
        assert_eq!(d1, d2);

        let mut h1 = DefaultHasher::new();
        d1.hash(&mut h1);
        let mut h2 = DefaultHasher::new();
        d2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_gyear_hash_consistent_with_eq() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let y1 = GYear::parse("2024").unwrap();
        let y2 = GYear::parse("2024Z").unwrap();
        assert_eq!(y1, y2);

        let mut h1 = DefaultHasher::new();
        y1.hash(&mut h1);
        let mut h2 = DefaultHasher::new();
        y2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_dtd_parse_minutes_not_months() {
        // Inside the T section, M means minutes, not months
        let d = DayTimeDuration::parse("PT5M").unwrap();
        assert_eq!(d.micros(), 5 * 60_000_000);
        assert_eq!(d.minutes(), 5);
    }

    #[test]
    fn test_duration_parse_months_vs_minutes() {
        // P2M = 2 months, PT2M = 2 minutes
        let d_months = Duration::parse("P2M").unwrap();
        assert_eq!(d_months.months(), 2);
        assert_eq!(d_months.micros(), 0);

        let d_minutes = Duration::parse("PT2M").unwrap();
        assert_eq!(d_minutes.months(), 0);
        assert_eq!(d_minutes.micros(), 2 * 60_000_000);
    }

    #[test]
    fn test_dtd_negative_days_and_time() {
        let d = DayTimeDuration::parse("-P1DT2H").unwrap();
        let expected = -(86_400_000_000_i64 + 2 * 3_600_000_000);
        assert_eq!(d.micros(), expected);
    }

    #[test]
    fn test_duration_negative_full() {
        let d = Duration::parse("-P1Y2M3DT4H5M6S").unwrap();
        assert_eq!(d.months(), -14);
        let expected =
            -(3 * 86_400_000_000_i64 + 4 * 3_600_000_000 + 5 * 60_000_000 + 6 * 1_000_000);
        assert_eq!(d.micros(), expected);
    }

    #[test]
    fn test_duration_canonical_fractional() {
        let d = Duration::parse("PT1.5S").unwrap();
        assert_eq!(d.to_canonical_string(), "PT1.5S");
    }

    #[test]
    fn test_duration_canonical_mixed_with_fractional() {
        let d = Duration::parse("P1Y2M3DT4H5M6.789S").unwrap();
        assert_eq!(d.to_canonical_string(), "P1Y2M3DT4H5M6.789S");
    }
}
