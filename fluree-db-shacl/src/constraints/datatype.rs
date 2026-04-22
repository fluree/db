//! Datatype and node kind constraint validators

use super::{Constraint, ConstraintViolation};
use crate::compile::NodeKind;
use fluree_db_core::{FlakeValue, Sid};
use fluree_vocab::namespaces::BLANK_NODE;

/// Validate sh:datatype constraint
///
/// Checks that a value's datatype matches the expected datatype SID.
pub fn validate_datatype(
    value: &FlakeValue,
    actual_dt: &Sid,
    expected_dt: &Sid,
) -> Option<ConstraintViolation> {
    if actual_dt != expected_dt {
        Some(ConstraintViolation {
            constraint: Constraint::Datatype(expected_dt.clone()),
            value: Some(value.clone()),
            message: format!(
                "Expected datatype {} but found {}",
                expected_dt.name, actual_dt.name
            ),
        })
    } else {
        None
    }
}

/// Validate sh:nodeKind constraint
///
/// Checks that a value matches the expected node kind.
pub fn validate_node_kind(
    value: &FlakeValue,
    expected_kind: NodeKind,
) -> Option<ConstraintViolation> {
    let actual_kind = infer_node_kind(value);

    let matches = match expected_kind {
        NodeKind::BlankNode => matches!(actual_kind, Some(NodeKind::BlankNode)),
        NodeKind::IRI => matches!(actual_kind, Some(NodeKind::IRI)),
        NodeKind::Literal => matches!(actual_kind, Some(NodeKind::Literal)),
        NodeKind::BlankNodeOrIRI => {
            matches!(actual_kind, Some(NodeKind::BlankNode | NodeKind::IRI))
        }
        NodeKind::BlankNodeOrLiteral => {
            matches!(actual_kind, Some(NodeKind::BlankNode | NodeKind::Literal))
        }
        NodeKind::IRIOrLiteral => {
            matches!(actual_kind, Some(NodeKind::IRI | NodeKind::Literal))
        }
    };

    if !matches {
        Some(ConstraintViolation {
            constraint: Constraint::NodeKind(expected_kind),
            value: Some(value.clone()),
            message: format!("Expected node kind {expected_kind:?} but found {actual_kind:?}"),
        })
    } else {
        None
    }
}

/// Infer the node kind from a FlakeValue
fn infer_node_kind(value: &FlakeValue) -> Option<NodeKind> {
    match value {
        FlakeValue::Ref(sid) => {
            // Check if it's a blank node (namespace code for blank nodes)
            if sid.namespace_code == BLANK_NODE {
                Some(NodeKind::BlankNode)
            } else {
                Some(NodeKind::IRI)
            }
        }
        FlakeValue::String(_)
        | FlakeValue::Long(_)
        | FlakeValue::Double(_)
        | FlakeValue::Boolean(_)
        | FlakeValue::BigInt(_)
        | FlakeValue::Decimal(_)
        | FlakeValue::DateTime(_)
        | FlakeValue::Date(_)
        | FlakeValue::Time(_)
        | FlakeValue::GYear(_)
        | FlakeValue::GYearMonth(_)
        | FlakeValue::GMonth(_)
        | FlakeValue::GDay(_)
        | FlakeValue::GMonthDay(_)
        | FlakeValue::YearMonthDuration(_)
        | FlakeValue::DayTimeDuration(_)
        | FlakeValue::Duration(_)
        | FlakeValue::Json(_)
        | FlakeValue::GeoPoint(_) => Some(NodeKind::Literal),
        FlakeValue::Vector(_) => Some(NodeKind::Literal), // Treat vectors as literals
        FlakeValue::Null => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_vocab::namespaces::XSD;
    use fluree_vocab::xsd_names;

    #[test]
    fn test_datatype_match() {
        let value = FlakeValue::String("hello".to_string());
        let dt = Sid::new(XSD, xsd_names::STRING);
        assert!(validate_datatype(&value, &dt, &dt).is_none());
    }

    #[test]
    fn test_datatype_mismatch() {
        let value = FlakeValue::String("hello".to_string());
        let actual_dt = Sid::new(XSD, xsd_names::STRING);
        let expected_dt = Sid::new(XSD, xsd_names::INTEGER);
        let violation = validate_datatype(&value, &actual_dt, &expected_dt);
        assert!(violation.is_some());
    }

    #[test]
    fn test_node_kind_iri() {
        let value = FlakeValue::Ref(Sid::new(100, "example"));
        assert!(validate_node_kind(&value, NodeKind::IRI).is_none());
        assert!(validate_node_kind(&value, NodeKind::BlankNodeOrIRI).is_none());
        assert!(validate_node_kind(&value, NodeKind::IRIOrLiteral).is_none());
        assert!(validate_node_kind(&value, NodeKind::Literal).is_some());
    }

    #[test]
    fn test_node_kind_literal() {
        let value = FlakeValue::String("hello".to_string());
        assert!(validate_node_kind(&value, NodeKind::Literal).is_none());
        assert!(validate_node_kind(&value, NodeKind::BlankNodeOrLiteral).is_none());
        assert!(validate_node_kind(&value, NodeKind::IRIOrLiteral).is_none());
        assert!(validate_node_kind(&value, NodeKind::IRI).is_some());
    }

    #[test]
    fn test_node_kind_blank_node() {
        let value = FlakeValue::Ref(Sid::new(BLANK_NODE, "b1"));
        assert!(validate_node_kind(&value, NodeKind::BlankNode).is_none());
        assert!(validate_node_kind(&value, NodeKind::BlankNodeOrIRI).is_none());
        assert!(validate_node_kind(&value, NodeKind::BlankNodeOrLiteral).is_none());
        assert!(validate_node_kind(&value, NodeKind::Literal).is_some());
    }
}
