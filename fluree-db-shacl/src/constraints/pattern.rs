//! String pattern constraint validators

use super::{Constraint, ConstraintViolation};
use crate::error::{Result, ShaclError};
use fluree_db_core::FlakeValue;
use regex::Regex;

/// Validate sh:pattern constraint
///
/// Checks that a string value matches the regular expression pattern.
pub fn validate_pattern(
    value: &FlakeValue,
    pattern: &str,
    flags: Option<&str>,
) -> Result<Option<ConstraintViolation>> {
    let string_value = match value {
        FlakeValue::String(s) => s.as_str(),
        _ => {
            // Non-string values fail pattern matching
            return Ok(Some(ConstraintViolation {
                constraint: Constraint::Pattern(pattern.to_string(), flags.map(String::from)),
                value: Some(value.clone()),
                message: "Pattern constraint requires a string value".to_string(),
            }));
        }
    };

    // Build regex with optional flags
    let regex_pattern = if let Some(f) = flags {
        let mut prefix = String::from("(?");
        for c in f.chars() {
            match c {
                'i' => prefix.push('i'),
                'm' => prefix.push('m'),
                's' => prefix.push('s'),
                'x' => prefix.push('x'),
                _ => {} // Ignore unknown flags
            }
        }
        prefix.push(')');
        format!("{prefix}{pattern}")
    } else {
        pattern.to_string()
    };

    let regex = Regex::new(&regex_pattern).map_err(|e| ShaclError::InvalidPattern {
        pattern: pattern.to_string(),
        message: e.to_string(),
    })?;

    if regex.is_match(string_value) {
        Ok(None)
    } else {
        Ok(Some(ConstraintViolation {
            constraint: Constraint::Pattern(pattern.to_string(), flags.map(String::from)),
            value: Some(value.clone()),
            message: format!("Value '{string_value}' does not match pattern '{pattern}'"),
        }))
    }
}

/// Validate sh:minLength constraint
pub fn validate_min_length(value: &FlakeValue, min: usize) -> Option<ConstraintViolation> {
    let len = string_length(value);

    if len < min {
        Some(ConstraintViolation {
            constraint: Constraint::MinLength(min),
            value: Some(value.clone()),
            message: format!("String length {len} is less than minimum {min}"),
        })
    } else {
        None
    }
}

/// Validate sh:maxLength constraint
pub fn validate_max_length(value: &FlakeValue, max: usize) -> Option<ConstraintViolation> {
    let len = string_length(value);

    if len > max {
        Some(ConstraintViolation {
            constraint: Constraint::MaxLength(max),
            value: Some(value.clone()),
            message: format!("String length {len} exceeds maximum {max}"),
        })
    } else {
        None
    }
}

/// Get the length of a value as a string
fn string_length(value: &FlakeValue) -> usize {
    match value {
        FlakeValue::String(s) => s.chars().count(),
        FlakeValue::Long(n) => n.to_string().len(),
        FlakeValue::Double(n) => n.to_string().len(),
        FlakeValue::Boolean(b) => {
            if *b {
                4
            } else {
                5
            }
        } // "true" or "false"
        FlakeValue::Ref(sid) => sid.name.len(),
        FlakeValue::Vector(v) => v.len(), // Length of vector
        FlakeValue::Null => 0,
        FlakeValue::Json(s) => s.chars().count(),
        FlakeValue::BigInt(n) => n.to_string().len(),
        FlakeValue::Decimal(d) => d.to_string().len(),
        FlakeValue::DateTime(dt) => dt.original().len(),
        FlakeValue::Date(d) => d.original().len(),
        FlakeValue::Time(t) => t.original().len(),
        FlakeValue::GYear(v) => v.original().len(),
        FlakeValue::GYearMonth(v) => v.original().len(),
        FlakeValue::GMonth(v) => v.original().len(),
        FlakeValue::GDay(v) => v.original().len(),
        FlakeValue::GMonthDay(v) => v.original().len(),
        FlakeValue::YearMonthDuration(v) => v.original().len(),
        FlakeValue::DayTimeDuration(v) => v.original().len(),
        FlakeValue::Duration(v) => v.original().len(),
        FlakeValue::GeoPoint(v) => v.to_string().len(), // "POINT(lng lat)"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_match() {
        let value = FlakeValue::String("hello123".to_string());
        let result = validate_pattern(&value, r"^hello\d+$", None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_pattern_no_match() {
        let value = FlakeValue::String("hello".to_string());
        let result = validate_pattern(&value, r"^\d+$", None).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_pattern_case_insensitive() {
        let value = FlakeValue::String("HELLO".to_string());
        let result = validate_pattern(&value, "hello", Some("i")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_min_length_satisfied() {
        let value = FlakeValue::String("hello".to_string());
        assert!(validate_min_length(&value, 5).is_none());
        assert!(validate_min_length(&value, 3).is_none());
    }

    #[test]
    fn test_min_length_violated() {
        let value = FlakeValue::String("hi".to_string());
        let violation = validate_min_length(&value, 5);
        assert!(violation.is_some());
    }

    #[test]
    fn test_max_length_satisfied() {
        let value = FlakeValue::String("hello".to_string());
        assert!(validate_max_length(&value, 5).is_none());
        assert!(validate_max_length(&value, 10).is_none());
    }

    #[test]
    fn test_max_length_violated() {
        let value = FlakeValue::String("hello world".to_string());
        let violation = validate_max_length(&value, 5);
        assert!(violation.is_some());
    }
}
