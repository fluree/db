//! Cardinality constraint validators (sh:minCount, sh:maxCount)

use super::{Constraint, ConstraintViolation};
use fluree_db_core::FlakeValue;

/// Validate sh:minCount constraint
pub fn validate_min_count(values: &[FlakeValue], min: usize) -> Option<ConstraintViolation> {
    if values.len() < min {
        Some(ConstraintViolation {
            constraint: Constraint::MinCount(min),
            value: None,
            message: format!(
                "Expected at least {} value(s) but found {}",
                min,
                values.len()
            ),
        })
    } else {
        None
    }
}

/// Validate sh:maxCount constraint
pub fn validate_max_count(values: &[FlakeValue], max: usize) -> Option<ConstraintViolation> {
    if values.len() > max {
        Some(ConstraintViolation {
            constraint: Constraint::MaxCount(max),
            value: None,
            message: format!(
                "Expected at most {} value(s) but found {}",
                max,
                values.len()
            ),
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_count_satisfied() {
        let values = vec![
            FlakeValue::String("a".to_string()),
            FlakeValue::String("b".to_string()),
        ];
        assert!(validate_min_count(&values, 2).is_none());
        assert!(validate_min_count(&values, 1).is_none());
    }

    #[test]
    fn test_min_count_violated() {
        let values = vec![FlakeValue::String("a".to_string())];
        let violation = validate_min_count(&values, 2);
        assert!(violation.is_some());
        assert!(violation.unwrap().message.contains("at least 2"));
    }

    #[test]
    fn test_max_count_satisfied() {
        let values = vec![
            FlakeValue::String("a".to_string()),
            FlakeValue::String("b".to_string()),
        ];
        assert!(validate_max_count(&values, 2).is_none());
        assert!(validate_max_count(&values, 3).is_none());
    }

    #[test]
    fn test_max_count_violated() {
        let values = vec![
            FlakeValue::String("a".to_string()),
            FlakeValue::String("b".to_string()),
            FlakeValue::String("c".to_string()),
        ];
        let violation = validate_max_count(&values, 2);
        assert!(violation.is_some());
        assert!(violation.unwrap().message.contains("at most 2"));
    }

    #[test]
    fn test_empty_values() {
        let values: Vec<FlakeValue> = vec![];
        assert!(validate_min_count(&values, 0).is_none());
        assert!(validate_min_count(&values, 1).is_some());
        assert!(validate_max_count(&values, 0).is_none());
    }
}
