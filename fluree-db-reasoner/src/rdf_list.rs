//! RDF List Traversal Utility
//!
//! Provides utilities for traversing RDF lists (rdf:first/rdf:rest chains).
//! Used by OWL2-RL rules that reference list structures:
//! - owl:intersectionOf
//! - owl:unionOf
//! - owl:oneOf
//! - owl:propertyChainAxiom
//! - owl:hasKey

use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::namespaces::is_rdf_nil;
use fluree_db_core::range::{RangeMatch, RangeTest};
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{GraphDbRef, Sid};
use fluree_vocab::namespaces::RDF;
use fluree_vocab::predicates::{RDF_FIRST, RDF_REST};

use crate::error::{ReasonerError, Result};
use crate::owl;
use crate::types::{ChainElement, PropertyExpression};

/// Maximum list length to prevent infinite loops on malformed data
const MAX_LIST_LENGTH: usize = 10_000;

/// Collect all elements from an RDF list starting at the given head node.
///
/// Traverses the rdf:first/rdf:rest chain until reaching rdf:nil.
///
/// # Arguments
///
/// * `db` - Bundled database reference (snapshot, graph, overlay, as-of time)
/// * `list_head` - The SID of the list head node (blank node or IRI)
///
/// # Returns
///
/// A vector of SIDs representing the list elements (rdf:first values).
/// Returns an empty vector if the list is empty or malformed.
///
/// # Errors
///
/// Returns an error if:
/// - Database query fails
/// - List exceeds MAX_LIST_LENGTH (malformed/cyclic data)
///
/// # Example
///
/// For an RDF list like:
/// ```turtle
/// _:list1 rdf:first ex:A ;
///         rdf:rest _:list2 .
/// _:list2 rdf:first ex:B ;
///         rdf:rest rdf:nil .
/// ```
///
/// `collect_list_elements(db, &_:list1)` returns `[ex:A, ex:B]`
pub async fn collect_list_elements(db: GraphDbRef<'_>, list_head: &Sid) -> Result<Vec<Sid>> {
    let mut elements = Vec::new();
    let mut current_node = list_head.clone();

    // Create SIDs for rdf:first and rdf:rest
    let rdf_first_sid = Sid::new(RDF, RDF_FIRST);
    let rdf_rest_sid = Sid::new(RDF, RDF_REST);

    loop {
        // Check for rdf:nil terminator
        if is_rdf_nil(&current_node) {
            break;
        }

        // Guard against malformed/cyclic lists
        if elements.len() >= MAX_LIST_LENGTH {
            return Err(ReasonerError::Internal(format!(
                "RDF list exceeds maximum length of {MAX_LIST_LENGTH} (possible cycle or malformed data)"
            )));
        }

        // Query for rdf:first value at current node
        // Using SPOT index: subject = current_node, predicate = rdf:first
        let first_flakes: Vec<Flake> = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch {
                    s: Some(current_node.clone()),
                    p: Some(rdf_first_sid.clone()),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| f.op) // Only assertions, not retractions
            .collect();

        // Extract the first element (should be exactly one)
        if let Some(first_flake) = first_flakes.first() {
            if let FlakeValue::Ref(element_sid) = &first_flake.o {
                elements.push(element_sid.clone());
            }
            // Note: Non-Ref values (literals) are skipped - most OWL list uses expect IRIs/blank nodes
            // For owl:oneOf with literals, callers may need a variant that returns FlakeValue
        }

        // Query for rdf:rest to get next node
        let rest_flakes: Vec<Flake> = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch {
                    s: Some(current_node.clone()),
                    p: Some(rdf_rest_sid.clone()),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| f.op)
            .collect();

        // Move to next node
        if let Some(rest_flake) = rest_flakes.first() {
            if let FlakeValue::Ref(next_node) = &rest_flake.o {
                current_node = next_node.clone();
            } else {
                // rdf:rest must point to another node or rdf:nil
                break;
            }
        } else {
            // No rdf:rest found - malformed list, stop here
            break;
        }
    }

    Ok(elements)
}

/// Collect list elements including literal values.
///
/// Similar to `collect_list_elements` but returns FlakeValue instead of Sid,
/// allowing for lists that contain literal values (useful for owl:oneOf with
/// data values).
///
/// # Arguments
///
/// * `db` - Bundled database reference (snapshot, graph, overlay, as-of time)
/// * `list_head` - The SID of the list head node
///
/// # Returns
///
/// A vector of FlakeValues representing the list elements.
pub async fn collect_list_values(db: GraphDbRef<'_>, list_head: &Sid) -> Result<Vec<FlakeValue>> {
    let mut elements = Vec::new();
    let mut current_node = list_head.clone();

    let rdf_first_sid = Sid::new(RDF, RDF_FIRST);
    let rdf_rest_sid = Sid::new(RDF, RDF_REST);

    loop {
        if is_rdf_nil(&current_node) {
            break;
        }

        if elements.len() >= MAX_LIST_LENGTH {
            return Err(ReasonerError::Internal(format!(
                "RDF list exceeds maximum length of {MAX_LIST_LENGTH} (possible cycle or malformed data)"
            )));
        }

        // Query for rdf:first
        let first_flakes: Vec<Flake> = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch {
                    s: Some(current_node.clone()),
                    p: Some(rdf_first_sid.clone()),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| f.op)
            .collect();

        if let Some(first_flake) = first_flakes.first() {
            elements.push(first_flake.o.clone());
        }

        // Query for rdf:rest
        let rest_flakes: Vec<Flake> = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch {
                    s: Some(current_node.clone()),
                    p: Some(rdf_rest_sid.clone()),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| f.op)
            .collect();

        if let Some(rest_flake) = rest_flakes.first() {
            if let FlakeValue::Ref(next_node) = &rest_flake.o {
                current_node = next_node.clone();
            } else {
                break;
            }
        } else {
            break;
        }
    }

    Ok(elements)
}

/// Collect property chain elements from an RDF list, resolving owl:inverseOf.
///
/// This function traverses an RDF list (rdf:first/rdf:rest chain) and converts
/// each element to a `ChainElement`. Elements can be:
/// - Direct property references: `ex:hasParent` → `ChainElement::direct(ex:hasParent)`
/// - Inverse expressions: `_:bnode owl:inverseOf ex:hasChild` → `ChainElement::inverse(ex:hasChild)`
/// - Nested inverses are normalized: `(P^-1)^-1 = P`
///
/// # Arguments
///
/// * `db` - Bundled database reference (snapshot, graph, overlay, as-of time)
/// * `list_head` - The SID of the list head node
///
/// # Returns
///
/// A vector of `ChainElement` values representing the chain.
///
/// # Example
///
/// For an RDF list like:
/// ```turtle
/// _:list1 rdf:first ex:hasParent ;
///         rdf:rest _:list2 .
/// _:list2 rdf:first [ owl:inverseOf ex:hasChild ] ;
///         rdf:rest rdf:nil .
/// ```
///
/// Returns: `[ChainElement::direct(ex:hasParent), ChainElement::inverse(ex:hasChild)]`
pub async fn collect_chain_elements(
    db: GraphDbRef<'_>,
    list_head: &Sid,
) -> Result<Vec<ChainElement>> {
    let mut elements = Vec::new();
    let mut current_node = list_head.clone();

    let rdf_first_sid = Sid::new(RDF, RDF_FIRST);
    let rdf_rest_sid = Sid::new(RDF, RDF_REST);

    loop {
        if is_rdf_nil(&current_node) {
            break;
        }

        if elements.len() >= MAX_LIST_LENGTH {
            return Err(ReasonerError::Internal(format!(
                "RDF list exceeds maximum length of {MAX_LIST_LENGTH} (possible cycle or malformed data)"
            )));
        }

        // Query for rdf:first
        let first_flakes: Vec<Flake> = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch {
                    s: Some(current_node.clone()),
                    p: Some(rdf_first_sid.clone()),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| f.op)
            .collect();

        if let Some(first_flake) = first_flakes.first() {
            if let FlakeValue::Ref(element_sid) = &first_flake.o {
                // Try to resolve this element - it might be an owl:inverseOf expression
                let chain_element = resolve_chain_element(db, element_sid, 0).await?;
                elements.push(chain_element);
            }
        }

        // Query for rdf:rest
        let rest_flakes: Vec<Flake> = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch {
                    s: Some(current_node.clone()),
                    p: Some(rdf_rest_sid.clone()),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| f.op)
            .collect();

        if let Some(rest_flake) = rest_flakes.first() {
            if let FlakeValue::Ref(next_node) = &rest_flake.o {
                current_node = next_node.clone();
            } else {
                break;
            }
        } else {
            break;
        }
    }

    Ok(elements)
}

/// Resolve a chain element, handling owl:inverseOf expressions.
///
/// If the element is a blank node with owl:inverseOf, recursively resolve
/// the target property and toggle the inverse flag.
///
/// # Double Inverse Normalization
///
/// If we have nested owl:inverseOf expressions, they cancel out:
/// - `P` → `ChainElement { property: P, is_inverse: false }`
/// - `_:b1 owl:inverseOf P` → `ChainElement { property: P, is_inverse: true }`
/// - `_:b2 owl:inverseOf _:b1` where `_:b1 owl:inverseOf P` → `ChainElement { property: P, is_inverse: false }`
///
/// The `depth` parameter tracks recursion to prevent infinite loops on malformed data.
fn resolve_chain_element<'a>(
    db: GraphDbRef<'a>,
    element_sid: &'a Sid,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ChainElement>> + Send + 'a>> {
    Box::pin(async move {
        const MAX_INVERSE_DEPTH: usize = 10;

        if depth >= MAX_INVERSE_DEPTH {
            return Err(ReasonerError::Internal(format!(
                "owl:inverseOf nesting exceeds maximum depth of {MAX_INVERSE_DEPTH}"
            )));
        }

        // Check if this element has owl:inverseOf
        let inverse_of_sid = owl::inverse_of_sid();
        let inverse_flakes: Vec<Flake> = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch {
                    s: Some(element_sid.clone()),
                    p: Some(inverse_of_sid),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| f.op)
            .collect();

        if let Some(inverse_flake) = inverse_flakes.first() {
            if let FlakeValue::Ref(target_sid) = &inverse_flake.o {
                // This is an owl:inverseOf expression - recursively resolve the target
                let inner = resolve_chain_element(db, target_sid, depth + 1).await?;
                // Toggle the inverse flag (double inverse normalization)
                return Ok(inner.with_inverse_toggle());
            }
        }

        // Not an owl:inverseOf expression - direct property reference
        Ok(ChainElement::direct(element_sid.clone()))
    })
}

/// Resolve a property expression from an owl:onProperty value.
///
/// This handles the three forms of property expressions in OWL:
/// 1. Named property: Direct IRI reference
/// 2. Inverse: `{ owl:inverseOf P }` → PropertyExpression::Inverse
/// 3. Chain: `{ owl:propertyChainAxiom (P1 P2 ...) }` → PropertyExpression::Chain
///
/// # Double Inverse Normalization
///
/// Nested inverses are normalized: `(P^-1)^-1 = P`
///
/// # Arguments
///
/// * `db` - Bundled database reference (snapshot, graph, overlay, as-of time)
/// * `property_sid` - The SID from owl:onProperty (may be a named property or blank node)
///
/// # Returns
///
/// A PropertyExpression representing the resolved property.
pub async fn resolve_property_expression(
    db: GraphDbRef<'_>,
    property_sid: &Sid,
) -> Result<PropertyExpression> {
    resolve_property_expression_inner(db, property_sid, 0).await
}

/// Inner implementation with depth tracking for recursion safety.
fn resolve_property_expression_inner<'a>(
    db: GraphDbRef<'a>,
    property_sid: &'a Sid,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<PropertyExpression>> + Send + 'a>> {
    Box::pin(async move {
        const MAX_DEPTH: usize = 10;

        if depth >= MAX_DEPTH {
            return Err(ReasonerError::Internal(format!(
                "Property expression nesting exceeds maximum depth of {MAX_DEPTH}"
            )));
        }

        // Query all properties of this node to check for owl:inverseOf or owl:propertyChainAxiom
        let node_flakes: Vec<Flake> = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch {
                    s: Some(property_sid.clone()),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| f.op)
            .collect();

        // Check for owl:inverseOf
        let inverse_of_sid = owl::inverse_of_sid();
        for flake in &node_flakes {
            if flake.p == inverse_of_sid {
                if let FlakeValue::Ref(target_sid) = &flake.o {
                    // Recursively resolve the target
                    let inner =
                        resolve_property_expression_inner(db, target_sid, depth + 1).await?;
                    // Apply inverse (with double-inverse normalization)
                    return Ok(PropertyExpression::inverse(inner));
                }
            }
        }

        // Check for owl:propertyChainAxiom
        let chain_axiom_sid = owl::property_chain_axiom_sid();
        for flake in &node_flakes {
            if flake.p == chain_axiom_sid {
                if let FlakeValue::Ref(list_head) = &flake.o {
                    // Parse the chain using collect_chain_elements
                    let chain_elements = collect_chain_elements(db, list_head).await?;
                    if chain_elements.len() >= 2 {
                        return Ok(PropertyExpression::chain(chain_elements));
                    }
                    // Invalid chain (less than 2 elements) - fall through to named
                }
            }
        }

        // Not an inverse or chain - treat as named property
        Ok(PropertyExpression::named(property_sid.clone()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::namespaces::{is_rdf_first, is_rdf_rest};
    use fluree_vocab::rdf_names;

    #[test]
    fn test_rdf_nil_detection() {
        let nil_sid = Sid::new(RDF, "nil");
        assert!(is_rdf_nil(&nil_sid));

        let not_nil = Sid::new(RDF, "first");
        assert!(!is_rdf_nil(&not_nil));
    }

    #[test]
    fn test_rdf_first_rest_detection() {
        let first_sid = Sid::new(RDF, rdf_names::FIRST);
        assert!(is_rdf_first(&first_sid));

        let rest_sid = Sid::new(RDF, rdf_names::REST);
        assert!(is_rdf_rest(&rest_sid));
    }
}
