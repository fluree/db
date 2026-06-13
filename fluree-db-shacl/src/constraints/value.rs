//! Value constraint validators (sh:hasValue, sh:in, value ranges)

use super::{Constraint, ConstraintViolation};
use fluree_db_core::FlakeValue;

/// Validate sh:hasValue constraint
///
/// Checks that the value set contains the expected value.
pub fn validate_has_value(
    values: &[FlakeValue],
    expected: &FlakeValue,
) -> Option<ConstraintViolation> {
    if values.contains(expected) {
        None
    } else {
        Some(ConstraintViolation {
            constraint: Constraint::HasValue(expected.clone()),
            value: None,
            message: format!("Required value {expected:?} not found"),
        })
    }
}

/// Validate sh:in constraint
///
/// Checks that a value is in the allowed set.
pub fn validate_in(value: &FlakeValue, allowed: &[FlakeValue]) -> Option<ConstraintViolation> {
    if allowed.contains(value) {
        None
    } else {
        Some(ConstraintViolation {
            constraint: Constraint::In(allowed.to_vec()),
            value: Some(value.clone()),
            message: format!(
                "Value {:?} is not in the allowed set of {} values",
                value,
                allowed.len()
            ),
        })
    }
}

/// Validate sh:minInclusive constraint
pub fn validate_min_inclusive(value: &FlakeValue, min: &FlakeValue) -> Option<ConstraintViolation> {
    match compare_values(value, min) {
        Some(ord) if ord >= std::cmp::Ordering::Equal => None,
        _ => Some(ConstraintViolation {
            constraint: Constraint::MinInclusive(min.clone()),
            value: Some(value.clone()),
            message: format!("Value {value:?} is less than minimum {min:?}"),
        }),
    }
}

/// Validate sh:maxInclusive constraint
pub fn validate_max_inclusive(value: &FlakeValue, max: &FlakeValue) -> Option<ConstraintViolation> {
    match compare_values(value, max) {
        Some(ord) if ord <= std::cmp::Ordering::Equal => None,
        _ => Some(ConstraintViolation {
            constraint: Constraint::MaxInclusive(max.clone()),
            value: Some(value.clone()),
            message: format!("Value {value:?} exceeds maximum {max:?}"),
        }),
    }
}

/// Validate sh:minExclusive constraint
pub fn validate_min_exclusive(value: &FlakeValue, min: &FlakeValue) -> Option<ConstraintViolation> {
    match compare_values(value, min) {
        Some(std::cmp::Ordering::Greater) => None,
        _ => Some(ConstraintViolation {
            constraint: Constraint::MinExclusive(min.clone()),
            value: Some(value.clone()),
            message: format!("Value {value:?} must be greater than {min:?}"),
        }),
    }
}

/// Validate sh:maxExclusive constraint
pub fn validate_max_exclusive(value: &FlakeValue, max: &FlakeValue) -> Option<ConstraintViolation> {
    match compare_values(value, max) {
        Some(std::cmp::Ordering::Less) => None,
        _ => Some(ConstraintViolation {
            constraint: Constraint::MaxExclusive(max.clone()),
            value: Some(value.clone()),
            message: format!("Value {value:?} must be less than {max:?}"),
        }),
    }
}

/// Compare two FlakeValues for ordering
///
/// Returns None if values are not comparable (different types).
fn compare_values(a: &FlakeValue, b: &FlakeValue) -> Option<std::cmp::Ordering> {
    // Numeric pairs (Long/Double/BigInt/Decimal in any combination) compare
    // via core's exact cross-type ordering — a per-variant match here
    // previously rejected every Decimal/BigInt value as "not comparable",
    // which the range facets count as an unconditional violation.
    if a.is_numeric() && b.is_numeric() {
        return a.numeric_cmp(b);
    }
    match (a, b) {
        (FlakeValue::String(a), FlakeValue::String(b)) => Some(a.cmp(b)),
        (FlakeValue::Boolean(a), FlakeValue::Boolean(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> FlakeValue {
        FlakeValue::Decimal(Box::new(s.parse().expect("decimal")))
    }

    #[test]
    fn test_range_facets_compare_decimals() {
        // Decimal values previously hit the "not comparable" catch-all,
        // which every range facet counts as a violation.
        assert!(validate_min_inclusive(&dec("10.5"), &dec("10.5")).is_none());
        assert!(validate_min_inclusive(&dec("10.4"), &dec("10.5")).is_some());
        assert!(validate_max_inclusive(&dec("10.5"), &dec("10.5")).is_none());
        assert!(validate_max_exclusive(&dec("10.5"), &dec("10.5")).is_some());
        assert!(validate_min_exclusive(&dec("10.6"), &dec("10.5")).is_none());
    }

    #[test]
    fn test_range_facets_compare_decimal_against_integer_constraint() {
        // sh:minInclusive 10 (xsd:integer) against an xsd:decimal value.
        assert!(validate_min_inclusive(&dec("10.00"), &FlakeValue::Long(10)).is_none());
        assert!(validate_min_inclusive(&dec("9.99"), &FlakeValue::Long(10)).is_some());
        // And a Long value against a decimal constraint.
        assert!(validate_max_inclusive(&FlakeValue::Long(10), &dec("10.5")).is_none());
    }

    #[test]
    fn test_range_facets_compare_bigint() {
        let big = FlakeValue::BigInt(Box::new(
            "123456789012345678901234567890".parse().expect("bigint"),
        ));
        assert!(validate_min_inclusive(&big, &FlakeValue::Long(1)).is_none());
        assert!(validate_max_inclusive(&big, &FlakeValue::Long(1)).is_some());
    }

    #[test]
    fn test_has_value_found() {
        let values = vec![
            FlakeValue::String("a".to_string()),
            FlakeValue::String("b".to_string()),
        ];
        let expected = FlakeValue::String("b".to_string());
        assert!(validate_has_value(&values, &expected).is_none());
    }

    #[test]
    fn test_has_value_not_found() {
        let values = vec![FlakeValue::String("a".to_string())];
        let expected = FlakeValue::String("b".to_string());
        assert!(validate_has_value(&values, &expected).is_some());
    }

    #[test]
    fn test_in_valid() {
        let value = FlakeValue::Long(2);
        let allowed = vec![
            FlakeValue::Long(1),
            FlakeValue::Long(2),
            FlakeValue::Long(3),
        ];
        assert!(validate_in(&value, &allowed).is_none());
    }

    #[test]
    fn test_in_invalid() {
        let value = FlakeValue::Long(4);
        let allowed = vec![
            FlakeValue::Long(1),
            FlakeValue::Long(2),
            FlakeValue::Long(3),
        ];
        assert!(validate_in(&value, &allowed).is_some());
    }

    #[test]
    fn test_min_inclusive() {
        let min = FlakeValue::Long(5);
        assert!(validate_min_inclusive(&FlakeValue::Long(5), &min).is_none());
        assert!(validate_min_inclusive(&FlakeValue::Long(6), &min).is_none());
        assert!(validate_min_inclusive(&FlakeValue::Long(4), &min).is_some());
    }

    #[test]
    fn test_max_inclusive() {
        let max = FlakeValue::Long(10);
        assert!(validate_max_inclusive(&FlakeValue::Long(10), &max).is_none());
        assert!(validate_max_inclusive(&FlakeValue::Long(9), &max).is_none());
        assert!(validate_max_inclusive(&FlakeValue::Long(11), &max).is_some());
    }

    #[test]
    fn test_min_exclusive() {
        let min = FlakeValue::Long(5);
        assert!(validate_min_exclusive(&FlakeValue::Long(6), &min).is_none());
        assert!(validate_min_exclusive(&FlakeValue::Long(5), &min).is_some());
        assert!(validate_min_exclusive(&FlakeValue::Long(4), &min).is_some());
    }

    #[test]
    fn test_max_exclusive() {
        let max = FlakeValue::Long(10);
        assert!(validate_max_exclusive(&FlakeValue::Long(9), &max).is_none());
        assert!(validate_max_exclusive(&FlakeValue::Long(10), &max).is_some());
        assert!(validate_max_exclusive(&FlakeValue::Long(11), &max).is_some());
    }

    #[test]
    fn test_compare_mixed_numeric() {
        // Long vs Double comparison
        let long = FlakeValue::Long(5);
        let double = FlakeValue::Double(5.0);
        assert_eq!(
            compare_values(&long, &double),
            Some(std::cmp::Ordering::Equal)
        );

        let double_higher = FlakeValue::Double(5.5);
        assert_eq!(
            compare_values(&long, &double_higher),
            Some(std::cmp::Ordering::Less)
        );
    }
}
