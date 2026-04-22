//! Pair constraint validators
//!
//! Validates constraints that compare values of one property to another:
//! - sh:equals - values must be equal
//! - sh:disjoint - values must not overlap
//! - sh:lessThan - values must be less than
//! - sh:lessThanOrEquals - values must be less than or equal to

use crate::constraints::{Constraint, ConstraintViolation};
use fluree_db_core::FlakeValue;
use std::cmp::Ordering;

/// Validate sh:equals constraint
///
/// The set of values for the constrained property must be identical to
/// the set of values for the specified property.
pub fn validate_equals(
    values: &[FlakeValue],
    other_values: &[FlakeValue],
    other_path: &str,
) -> Option<ConstraintViolation> {
    // Convert to sets for comparison
    let values_set: std::collections::HashSet<_> = values.iter().collect();
    let other_set: std::collections::HashSet<_> = other_values.iter().collect();

    if values_set != other_set {
        // Find values that differ
        let missing: Vec<_> = other_set.difference(&values_set).collect();
        let extra: Vec<_> = values_set.difference(&other_set).collect();

        let mut msg_parts = Vec::new();
        if !missing.is_empty() {
            msg_parts.push(format!("missing values from {other_path}: {missing:?}"));
        }
        if !extra.is_empty() {
            msg_parts.push(format!("extra values not in {other_path}: {extra:?}"));
        }

        return Some(ConstraintViolation {
            constraint: Constraint::Equals(fluree_db_core::Sid {
                namespace_code: 0,
                name: other_path.into(),
            }),
            value: None,
            message: format!(
                "Value set does not equal value set for {}: {}",
                other_path,
                msg_parts.join("; ")
            ),
        });
    }

    None
}

/// Validate sh:disjoint constraint
///
/// The set of values for the constrained property must have no overlap
/// with the set of values for the specified property.
pub fn validate_disjoint(
    values: &[FlakeValue],
    other_values: &[FlakeValue],
    other_path: &str,
) -> Option<ConstraintViolation> {
    let values_set: std::collections::HashSet<_> = values.iter().collect();
    let other_set: std::collections::HashSet<_> = other_values.iter().collect();

    let intersection: Vec<_> = values_set.intersection(&other_set).collect();

    if !intersection.is_empty() {
        return Some(ConstraintViolation {
            constraint: Constraint::Disjoint(fluree_db_core::Sid {
                namespace_code: 0,
                name: other_path.into(),
            }),
            value: intersection.first().map(|v| (**v).clone()),
            message: format!(
                "Values must be disjoint from {other_path}, but found common values: {intersection:?}"
            ),
        });
    }

    None
}

/// Validate sh:lessThan constraint
///
/// Each value of the constrained property must be strictly less than
/// each value of the specified property.
pub fn validate_less_than(
    value: &FlakeValue,
    other_values: &[FlakeValue],
    other_path: &str,
) -> Option<ConstraintViolation> {
    for other in other_values {
        if let Some(ord) = compare_values(value, other) {
            if ord != Ordering::Less {
                return Some(ConstraintViolation {
                    constraint: Constraint::LessThan(fluree_db_core::Sid {
                        namespace_code: 0,
                        name: other_path.into(),
                    }),
                    value: Some(value.clone()),
                    message: format!(
                        "Value {value:?} is not less than {other:?} from {other_path}"
                    ),
                });
            }
        } else {
            // Incomparable types
            return Some(ConstraintViolation {
                constraint: Constraint::LessThan(fluree_db_core::Sid {
                    namespace_code: 0,
                    name: other_path.into(),
                }),
                value: Some(value.clone()),
                message: format!(
                    "Cannot compare value {value:?} with {other:?} from {other_path} (incompatible types)"
                ),
            });
        }
    }

    None
}

/// Validate sh:lessThanOrEquals constraint
///
/// Each value of the constrained property must be less than or equal to
/// each value of the specified property.
pub fn validate_less_than_or_equals(
    value: &FlakeValue,
    other_values: &[FlakeValue],
    other_path: &str,
) -> Option<ConstraintViolation> {
    for other in other_values {
        if let Some(ord) = compare_values(value, other) {
            if ord == Ordering::Greater {
                return Some(ConstraintViolation {
                    constraint: Constraint::LessThanOrEquals(fluree_db_core::Sid {
                        namespace_code: 0,
                        name: other_path.into(),
                    }),
                    value: Some(value.clone()),
                    message: format!(
                        "Value {value:?} is not less than or equal to {other:?} from {other_path}"
                    ),
                });
            }
        } else {
            // Incomparable types
            return Some(ConstraintViolation {
                constraint: Constraint::LessThanOrEquals(fluree_db_core::Sid {
                    namespace_code: 0,
                    name: other_path.into(),
                }),
                value: Some(value.clone()),
                message: format!(
                    "Cannot compare value {value:?} with {other:?} from {other_path} (incompatible types)"
                ),
            });
        }
    }

    None
}

/// Compare two FlakeValues, returning None if they are incomparable
fn compare_values(a: &FlakeValue, b: &FlakeValue) -> Option<Ordering> {
    match (a, b) {
        // Numeric comparisons
        (FlakeValue::Long(x), FlakeValue::Long(y)) => Some(x.cmp(y)),
        (FlakeValue::Double(x), FlakeValue::Double(y)) => x.partial_cmp(y),
        (FlakeValue::Long(x), FlakeValue::Double(y)) => (*x as f64).partial_cmp(y),
        (FlakeValue::Double(x), FlakeValue::Long(y)) => x.partial_cmp(&(*y as f64)),
        (FlakeValue::BigInt(x), FlakeValue::BigInt(y)) => Some(x.cmp(y)),
        (FlakeValue::Decimal(x), FlakeValue::Decimal(y)) => x.partial_cmp(y),

        // String comparisons
        (FlakeValue::String(x), FlakeValue::String(y)) => Some(x.cmp(y)),

        // DateTime comparisons
        (FlakeValue::DateTime(x), FlakeValue::DateTime(y)) => Some(x.cmp(y)),

        // Ref comparisons (by SID)
        (FlakeValue::Ref(x), FlakeValue::Ref(y)) => {
            if x.namespace_code != y.namespace_code {
                Some(x.namespace_code.cmp(&y.namespace_code))
            } else {
                Some(x.name.cmp(&y.name))
            }
        }

        // Boolean comparisons (false < true)
        (FlakeValue::Boolean(x), FlakeValue::Boolean(y)) => Some(x.cmp(y)),

        // Incomparable types
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equals_same_values() {
        let values = vec![FlakeValue::Long(1), FlakeValue::Long(2)];
        let other = vec![FlakeValue::Long(2), FlakeValue::Long(1)];
        assert!(validate_equals(&values, &other, "other").is_none());
    }

    #[test]
    fn test_equals_different_values() {
        let values = vec![FlakeValue::Long(1), FlakeValue::Long(2)];
        let other = vec![FlakeValue::Long(2), FlakeValue::Long(3)];
        assert!(validate_equals(&values, &other, "other").is_some());
    }

    #[test]
    fn test_disjoint_no_overlap() {
        let values = vec![FlakeValue::Long(1), FlakeValue::Long(2)];
        let other = vec![FlakeValue::Long(3), FlakeValue::Long(4)];
        assert!(validate_disjoint(&values, &other, "other").is_none());
    }

    #[test]
    fn test_disjoint_with_overlap() {
        let values = vec![FlakeValue::Long(1), FlakeValue::Long(2)];
        let other = vec![FlakeValue::Long(2), FlakeValue::Long(3)];
        assert!(validate_disjoint(&values, &other, "other").is_some());
    }

    #[test]
    fn test_less_than_valid() {
        let value = FlakeValue::Long(5);
        let other = vec![FlakeValue::Long(10), FlakeValue::Long(20)];
        assert!(validate_less_than(&value, &other, "other").is_none());
    }

    #[test]
    fn test_less_than_invalid() {
        let value = FlakeValue::Long(15);
        let other = vec![FlakeValue::Long(10), FlakeValue::Long(20)];
        assert!(validate_less_than(&value, &other, "other").is_some());
    }

    #[test]
    fn test_less_than_or_equals_valid() {
        let value = FlakeValue::Long(10);
        let other = vec![FlakeValue::Long(10), FlakeValue::Long(20)];
        assert!(validate_less_than_or_equals(&value, &other, "other").is_none());
    }

    #[test]
    fn test_less_than_or_equals_invalid() {
        let value = FlakeValue::Long(25);
        let other = vec![FlakeValue::Long(10), FlakeValue::Long(20)];
        assert!(validate_less_than_or_equals(&value, &other, "other").is_some());
    }

    #[test]
    fn test_compare_strings() {
        assert_eq!(
            compare_values(
                &FlakeValue::String("alice".into()),
                &FlakeValue::String("bob".into())
            ),
            Some(Ordering::Less)
        );
    }
}
