//! RDF lists in OWL axioms.
//!
//! OWL2-RL rules that reference list structures:
//! - owl:intersectionOf
//! - owl:unionOf
//! - owl:oneOf
//! - owl:propertyChainAxiom
//! - owl:hasKey
//!
//! A list is read in either stored shape — compact members or an explicit
//! `rdf:first`/`rdf:rest` chain — by [`fluree_db_core::rdf_list`].

use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::range::{RangeMatch, RangeTest};
use fluree_db_core::rdf_list::read_lists;
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{GraphDbRef, Sid};

use crate::error::{ReasonerError, Result};
use crate::owl;
use crate::types::{ChainElement, PropertyExpression};

/// The lists among `values`, the flakes of one subject and predicate, each
/// as its IRI and blank-node members. Literal members are skipped: the list
/// axioms OWL2-RL reads name classes, properties and individuals.
///
/// For a Turtle `ex:C owl:hasKey ( ex:p ex:q )`, or the same list written as
/// `rdf:first`/`rdf:rest` triples, `values` are `ex:C`'s `owl:hasKey` flakes
/// and the result is `[[ex:p, ex:q]]`.
pub async fn collect_list_elements(db: GraphDbRef<'_>, values: &[Flake]) -> Result<Vec<Vec<Sid>>> {
    Ok(read_lists(db, values)
        .await?
        .lists
        .into_iter()
        .map(|members| {
            members
                .into_iter()
                .filter_map(|m| match m {
                    FlakeValue::Ref(sid) => Some(sid),
                    _ => None,
                })
                .collect()
        })
        .collect())
}

/// The property chains among `values`, the flakes of one subject's
/// `owl:propertyChainAxiom`, resolving `owl:inverseOf` members.
///
/// Members can be:
/// - Direct property references: `ex:hasParent` → `ChainElement::direct(ex:hasParent)`
/// - Inverse expressions: `_:bnode owl:inverseOf ex:hasChild` → `ChainElement::inverse(ex:hasChild)`
/// - Nested inverses are normalized: `(P^-1)^-1 = P`
pub async fn collect_chain_elements(
    db: GraphDbRef<'_>,
    values: &[Flake],
) -> Result<Vec<Vec<ChainElement>>> {
    let mut chains = Vec::new();
    for members in collect_list_elements(db, values).await? {
        let mut chain = Vec::with_capacity(members.len());
        for member in &members {
            chain.push(resolve_chain_element(db, member, 0).await?);
        }
        chains.push(chain);
    }
    Ok(chains)
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
        let chain_flakes: Vec<Flake> = node_flakes
            .into_iter()
            .filter(|f| f.p == chain_axiom_sid)
            .collect();
        for chain_elements in collect_chain_elements(db, &chain_flakes).await? {
            if chain_elements.len() >= 2 {
                return Ok(PropertyExpression::chain(chain_elements));
            }
            // Invalid chain (less than 2 elements) - fall through to named
        }

        // Not an inverse or chain - treat as named property
        Ok(PropertyExpression::named(property_sid.clone()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::namespaces::{is_rdf_first, is_rdf_nil, is_rdf_rest};
    use fluree_vocab::namespaces::RDF;
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
