//! The accepted forms of a TTL instruction, shared by every write surface.
//!
//! A relative lifetime (`@ttl`, `f:ttl`, `fluree.ttl`) is whole seconds: a
//! non-negative integer, or a fixed day/time duration such as `"PT1H"` or
//! `"P1DT2H"`, as a string or a typed `xsd:dayTimeDuration`. An absolute
//! deadline is an RFC 3339 timestamp with `Z` or a known numeric
//! offset, normalized to UTC. Anything else is refused rather than guessed:
//! calendar durations (`P1M`, `P1Y`), negative or fractional lifetimes,
//! numeric strings, date-only values and local date-times.

use chrono::{DateTime, SecondsFormat, Utc};

/// A JSON TTL value: a non-negative whole number of seconds, or a day/time
/// duration string.
pub fn ttl_seconds_from_json(value: &serde_json::Value) -> Result<u64, String> {
    match value {
        serde_json::Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| format!("a TTL is a non-negative whole number of seconds, not {n}")),
        serde_json::Value::String(s) => duration_seconds(s),
        other => Err(format!(
            "a TTL is a whole number of seconds or a duration such as \"PT1H\", not {other}"
        )),
    }
}

/// An RDF TTL value: a non-negative whole number of seconds, or a day/time
/// duration as a string or a typed `xsd:dayTimeDuration` or `xsd:duration`.
pub fn ttl_seconds_from_value(value: &fluree_db_core::FlakeValue) -> Result<u64, String> {
    use fluree_db_core::FlakeValue;
    match value {
        FlakeValue::Long(n) => {
            u64::try_from(*n).map_err(|_| format!("a TTL may not be negative, not {n}"))
        }
        FlakeValue::String(text) => duration_seconds(text),
        FlakeValue::DayTimeDuration(d) => duration_seconds(&d.to_canonical_string()),
        FlakeValue::Duration(d) => duration_seconds(&d.to_canonical_string()),
        _ => Err("a TTL is a whole number of seconds or a duration such as \"PT1H\"".to_string()),
    }
}

/// Seconds in a fixed day/time duration (`P[nD][T[nH][nM][nS]]`): days are
/// 86,400 seconds, and there are no months, years or weeks.
pub fn duration_seconds(text: &str) -> Result<u64, String> {
    let refuse = || {
        format!(
            "\"{text}\" is not a supported TTL duration: use whole days, hours, minutes and \
             seconds such as \"PT1H\" or \"P1DT2H\" (no months, years, weeks, fractions or \
             negative durations), or a number of seconds"
        )
    };
    let rest = text.strip_prefix('P').ok_or_else(refuse)?;
    if rest.is_empty() {
        return Err(refuse());
    }
    let (date, time) = match rest.split_once('T') {
        Some((date, time)) if !time.is_empty() => (date, Some(time)),
        Some(_) => return Err(refuse()),
        None => (rest, None),
    };
    let mut total: u64 = 0;
    let mut add = |part: &str, units: &[(char, u64)]| -> Result<(), String> {
        let mut digits = String::new();
        let mut last_unit = None;
        for c in part.chars() {
            if c.is_ascii_digit() {
                digits.push(c);
                continue;
            }
            let position = units.iter().position(|(u, _)| *u == c).ok_or_else(refuse)?;
            if digits.is_empty() || last_unit.is_some_and(|last| position <= last) {
                return Err(refuse());
            }
            let n: u64 = digits.parse().map_err(|_| refuse())?;
            total = n
                .checked_mul(units[position].1)
                .and_then(|s| total.checked_add(s))
                .ok_or_else(refuse)?;
            digits.clear();
            last_unit = Some(position);
        }
        if !digits.is_empty() {
            return Err(refuse());
        }
        Ok(())
    };
    add(date, &[('D', 86_400)])?;
    if let Some(time) = time {
        add(time, &[('H', 3_600), ('M', 60), ('S', 1)])?;
    }
    Ok(total)
}

/// An absolute deadline: RFC 3339 with `Z` or a numeric offset, at most
/// microsecond precision. Returns the instant in UTC.
pub fn expires_at(text: &str) -> Result<DateTime<Utc>, String> {
    let parsed = DateTime::parse_from_rfc3339(text).map_err(|_| {
        format!(
            "\"{text}\" is not a supported TTL deadline: use an RFC 3339 timestamp with a time \
             zone, such as \"2026-10-21T12:00:00Z\""
        )
    })?;
    if text.ends_with("-00:00") {
        return Err(format!(
            "\"{text}\" has an unknown local offset (-00:00); give the deadline's offset"
        ));
    }
    let utc = parsed.with_timezone(&Utc);
    if utc.timestamp_subsec_nanos() % 1_000 != 0 {
        return Err(format!(
            "\"{text}\" is more precise than a microsecond; a TTL deadline is not rounded"
        ));
    }
    Ok(utc)
}

/// The canonical `xsd:dateTime` lexical a deadline is committed as.
pub fn expires_at_lexical(instant: &DateTime<Utc>) -> String {
    instant.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

/// Validate and normalize a transaction's TTL default in its txn-meta: at
/// most one `f:ttl` (whole seconds, or a duration) or
/// `f:expiresAt` (an RFC 3339 deadline, stored as a UTC `xsd:dateTime`).
pub fn normalize_txn_meta_ttl(entries: &mut [fluree_db_core::TxnMetaEntry]) -> Result<(), String> {
    use fluree_db_core::TxnMetaValue;
    use fluree_vocab::db;
    use fluree_vocab::namespaces::{FLUREE_DB, XSD};
    let mut seen = 0;
    for entry in entries.iter_mut() {
        if entry.predicate_ns != FLUREE_DB {
            continue;
        }
        if entry.predicate_name == db::TTL {
            seen += 1;
            let seconds = match &entry.value {
                TxnMetaValue::Long(n) => u64::try_from(*n)
                    .map_err(|_| "txn-meta f:ttl may not be negative".to_string())?,
                TxnMetaValue::String(text) => duration_seconds(text)?,
                TxnMetaValue::TypedLiteral {
                    value,
                    dt_ns,
                    dt_name,
                } if *dt_ns == XSD
                    && matches!(
                        dt_name.as_str(),
                        fluree_vocab::xsd_names::DAY_TIME_DURATION
                            | fluree_vocab::xsd_names::DURATION
                    ) =>
                {
                    duration_seconds(value)?
                }
                _ => {
                    return Err(
                        "txn-meta f:ttl is a whole number of seconds or a duration".to_string()
                    )
                }
            };
            let seconds =
                i64::try_from(seconds).map_err(|_| "txn-meta f:ttl is too large".to_string())?;
            entry.value = TxnMetaValue::Long(seconds);
        } else if entry.predicate_name == db::EXPIRES_AT {
            seen += 1;
            let text = match &entry.value {
                TxnMetaValue::String(text) => text,
                TxnMetaValue::TypedLiteral { value, .. } => value,
                _ => {
                    return Err("txn-meta f:expiresAt is an RFC 3339 timestamp".to_string());
                }
            };
            let instant = expires_at(text)?;
            entry.value = TxnMetaValue::TypedLiteral {
                value: expires_at_lexical(&instant),
                dt_ns: XSD,
                dt_name: fluree_vocab::xsd_names::DATE_TIME.to_string(),
            };
        }
    }
    if seen > 1 {
        return Err(
            "a transaction takes one TTL default: one f:ttl or one f:expiresAt".to_string(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn whole_seconds_and_day_time_durations() {
        assert_eq!(ttl_seconds_from_json(&json!(3600)).unwrap(), 3600);
        assert_eq!(ttl_seconds_from_json(&json!(0)).unwrap(), 0);
        assert_eq!(ttl_seconds_from_json(&json!("PT1H")).unwrap(), 3600);
        assert_eq!(ttl_seconds_from_json(&json!("P1DT2H")).unwrap(), 93_600);
        assert_eq!(ttl_seconds_from_json(&json!("PT0S")).unwrap(), 0);
        assert_eq!(ttl_seconds_from_json(&json!("P2D")).unwrap(), 172_800);
        assert_eq!(ttl_seconds_from_json(&json!("PT1H30M15S")).unwrap(), 5_415);
    }

    #[test]
    fn rdf_values_take_seconds_or_a_duration() {
        use fluree_db_core::temporal::{DayTimeDuration, Duration};
        use fluree_db_core::FlakeValue;
        let day_time =
            |s: &str| FlakeValue::DayTimeDuration(Box::new(DayTimeDuration::parse(s).unwrap()));
        let generic = |s: &str| FlakeValue::Duration(Box::new(Duration::parse(s).unwrap()));
        for (value, seconds) in [
            (FlakeValue::Long(3600), 3600),
            (FlakeValue::Long(0), 0),
            (FlakeValue::String("PT1H".into()), 3600),
            (day_time("P1DT2H"), 93_600),
            (day_time("PT0S"), 0),
            (generic("PT15M"), 900),
        ] {
            assert_eq!(ttl_seconds_from_value(&value), Ok(seconds), "{value:?}");
        }
        for bad in [
            FlakeValue::Long(-1),
            FlakeValue::String("3600".into()),
            FlakeValue::Boolean(true),
            day_time("-PT1H"),
            day_time("PT1.5S"),
            generic("P1Y"),
            generic("P1M"),
        ] {
            assert!(ttl_seconds_from_value(&bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn refuses_what_it_would_have_to_guess() {
        for bad in [
            json!(-1),
            json!(1.5),
            json!("3600"),
            json!(true),
            json!(null),
            json!("P1M"),
            json!("P1Y"),
            json!("P1W"),
            json!("-PT1H"),
            json!("PT1.5S"),
            json!("P"),
            json!("PT"),
            json!("PT1M1H"),
            json!("PT1H1H"),
        ] {
            assert!(ttl_seconds_from_json(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn deadlines_need_a_known_offset() {
        let utc = expires_at("2026-10-21T12:00:00Z").unwrap();
        assert_eq!(expires_at_lexical(&utc), "2026-10-21T12:00:00Z");
        let shifted = expires_at("2026-10-21T14:00:00+02:00").unwrap();
        assert_eq!(shifted, utc);
        for bad in [
            "2026-10-21",
            "2026-10-21T12:00:00",
            "2026-10-21T12:00:00-00:00",
            "2026-10-21T12:00:00.123456789Z",
            "tomorrow",
        ] {
            assert!(expires_at(bad).is_err(), "{bad}");
        }
        assert!(expires_at("2026-10-21T12:00:00.123456Z").is_ok());
    }
}
