//! String pattern constraint validators

use super::{Constraint, ConstraintViolation};
use crate::error::{Result, ShaclError};
use fluree_db_core::FlakeValue;
use regex::Regex;

/// The lexical form a literal is matched against for `sh:pattern`, mirroring
/// SPARQL `STR()`. `None` for non-literals: blank nodes always violate per
/// spec, and IRI matching would need namespace decoding that this pure path
/// doesn't have.
fn pattern_lexical_form(value: &FlakeValue) -> Option<String> {
    match value {
        FlakeValue::String(s) => Some(s.clone()),
        FlakeValue::Long(n) => Some(n.to_string()),
        FlakeValue::Double(n) => Some(n.to_string()),
        FlakeValue::Boolean(b) => Some(b.to_string()),
        FlakeValue::BigInt(n) => Some(n.to_string()),
        FlakeValue::Decimal(d) => Some(d.to_string()),
        FlakeValue::DateTime(v) => Some(v.original().to_string()),
        FlakeValue::Date(v) => Some(v.original().to_string()),
        FlakeValue::Time(v) => Some(v.original().to_string()),
        FlakeValue::GYear(v) => Some(v.original().to_string()),
        FlakeValue::GYearMonth(v) => Some(v.original().to_string()),
        FlakeValue::GMonth(v) => Some(v.original().to_string()),
        FlakeValue::GDay(v) => Some(v.original().to_string()),
        FlakeValue::GMonthDay(v) => Some(v.original().to_string()),
        FlakeValue::YearMonthDuration(v) => Some(v.original().to_string()),
        FlakeValue::DayTimeDuration(v) => Some(v.original().to_string()),
        FlakeValue::Duration(v) => Some(v.original().to_string()),
        FlakeValue::Json(s) => Some(s.clone()),
        FlakeValue::GeoPoint(v) => Some(v.to_string()),
        FlakeValue::Ref(_) | FlakeValue::Vector(_) | FlakeValue::Null => None,
    }
}

/// Validate sh:pattern constraint
///
/// Matches the value's lexical form (per SPARQL `STR()`) against the regular
/// expression — so numeric, boolean, and date/time literals participate, not
/// just strings.
pub fn validate_pattern(
    value: &FlakeValue,
    pattern: &str,
    flags: Option<&str>,
) -> Result<Option<ConstraintViolation>> {
    let Some(string_value) = pattern_lexical_form(value) else {
        // Blank nodes / IRIs / non-literals fail pattern matching
        return Ok(Some(ConstraintViolation {
            constraint: Constraint::Pattern(pattern.to_string(), flags.map(String::from)),
            value: Some(value.clone()),
            value_index: None,
            message: "Pattern constraint cannot be applied to a non-literal value".to_string(),
        }));
    };
    let string_value = string_value.as_str();

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
            value_index: None,
            message: format!("Value '{string_value}' does not match pattern '{pattern}'"),
        }))
    }
}

/// Validate sh:minLength constraint
///
/// Applies to the value's lexical form (per SPARQL `STR()`); non-literals
/// (blank nodes, undecodable IRIs) violate per spec.
pub fn validate_min_length(value: &FlakeValue, min: usize) -> Option<ConstraintViolation> {
    let Some(len) = lexical_length(value) else {
        return Some(ConstraintViolation {
            constraint: Constraint::MinLength(min),
            value: Some(value.clone()),
            value_index: None,
            message: "Length constraint cannot be applied to a non-literal value".to_string(),
        });
    };

    if len < min {
        Some(ConstraintViolation {
            constraint: Constraint::MinLength(min),
            value: Some(value.clone()),
            value_index: None,
            message: format!("String length {len} is less than minimum {min}"),
        })
    } else {
        None
    }
}

/// Validate sh:maxLength constraint
///
/// Applies to the value's lexical form (per SPARQL `STR()`); non-literals
/// (blank nodes, undecodable IRIs) violate per spec.
pub fn validate_max_length(value: &FlakeValue, max: usize) -> Option<ConstraintViolation> {
    let Some(len) = lexical_length(value) else {
        return Some(ConstraintViolation {
            constraint: Constraint::MaxLength(max),
            value: Some(value.clone()),
            value_index: None,
            message: "Length constraint cannot be applied to a non-literal value".to_string(),
        });
    };

    if len > max {
        Some(ConstraintViolation {
            constraint: Constraint::MaxLength(max),
            value: Some(value.clone()),
            value_index: None,
            message: format!("String length {len} exceeds maximum {max}"),
        })
    } else {
        None
    }
}

/// Character count of the value's lexical form; `None` for non-literals.
fn lexical_length(value: &FlakeValue) -> Option<usize> {
    pattern_lexical_form(value).map(|s| s.chars().count())
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
    fn test_pattern_numeric_literal_matches_lexical_form() {
        // Non-string literals match on their lexical form (SPARQL STR()),
        // not unconditionally violate.
        let year = FlakeValue::Long(2024);
        assert!(validate_pattern(&year, r"^\d{4}$", None).unwrap().is_none());
        let too_long = FlakeValue::Long(12345);
        assert!(validate_pattern(&too_long, r"^\d{4}$", None)
            .unwrap()
            .is_some());
    }

    #[test]
    fn test_pattern_ref_still_violates() {
        let value = FlakeValue::Ref(fluree_db_core::Sid::new(100, "thing"));
        assert!(validate_pattern(&value, ".*", None).unwrap().is_some());
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
