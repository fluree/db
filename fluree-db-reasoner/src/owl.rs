//! OWL utilities for reasoning operations
//!
//! This module provides utility functions and types for working with OWL
//! vocabulary in reasoning rules, reducing code duplication.

use fluree_db_core::namespaces::is_rdf_type;
use fluree_db_core::{FlakeValue, GraphDbRef, IndexType, RangeMatch, RangeTest, Sid};
use fluree_vocab::namespaces::OWL;
use fluree_vocab::owl_names::*;

/// Get the SID for owl:sameAs
#[inline]
pub fn same_as_sid() -> Sid {
    Sid::new(OWL, SAME_AS)
}

/// Get the SID for owl:Restriction
#[inline]
pub fn restriction_sid() -> Sid {
    Sid::new(OWL, RESTRICTION)
}

/// Get the SID for owl:SymmetricProperty
#[inline]
pub fn symmetric_property_sid() -> Sid {
    Sid::new(OWL, SYMMETRIC_PROPERTY)
}

/// Get the SID for owl:TransitiveProperty
#[inline]
pub fn transitive_property_sid() -> Sid {
    Sid::new(OWL, TRANSITIVE_PROPERTY)
}

/// Get the SID for owl:FunctionalProperty
#[inline]
pub fn functional_property_sid() -> Sid {
    Sid::new(OWL, FUNCTIONAL_PROPERTY)
}

/// Get the SID for owl:InverseFunctionalProperty
#[inline]
pub fn inverse_functional_property_sid() -> Sid {
    Sid::new(OWL, INVERSE_FUNCTIONAL_PROPERTY)
}

/// Get the SID for owl:inverseOf
#[inline]
pub fn inverse_of_sid() -> Sid {
    Sid::new(OWL, INVERSE_OF)
}

/// Get the SID for owl:equivalentClass
#[inline]
pub fn equivalent_class_sid() -> Sid {
    Sid::new(OWL, EQUIVALENT_CLASS)
}

/// Get the SID for owl:equivalentProperty
#[inline]
pub fn equivalent_property_sid() -> Sid {
    Sid::new(OWL, EQUIVALENT_PROPERTY)
}

/// Get the SID for owl:propertyChainAxiom
#[inline]
pub fn property_chain_axiom_sid() -> Sid {
    Sid::new(OWL, PROPERTY_CHAIN_AXIOM)
}

/// Get the SID for owl:hasKey
#[inline]
pub fn has_key_sid() -> Sid {
    Sid::new(OWL, HAS_KEY)
}

/// Get the SID for owl:onProperty
#[inline]
pub fn on_property_sid() -> Sid {
    Sid::new(OWL, ON_PROPERTY)
}

/// Get the SID for owl:hasValue
#[inline]
pub fn has_value_sid() -> Sid {
    Sid::new(OWL, HAS_VALUE)
}

/// Get the SID for owl:someValuesFrom
#[inline]
pub fn some_values_from_sid() -> Sid {
    Sid::new(OWL, SOME_VALUES_FROM)
}

/// Get the SID for owl:allValuesFrom
#[inline]
pub fn all_values_from_sid() -> Sid {
    Sid::new(OWL, ALL_VALUES_FROM)
}

/// Get the SID for owl:maxCardinality
#[inline]
pub fn max_cardinality_sid() -> Sid {
    Sid::new(OWL, MAX_CARDINALITY)
}

/// Get the SID for owl:maxQualifiedCardinality
#[inline]
pub fn max_qualified_cardinality_sid() -> Sid {
    Sid::new(OWL, MAX_QUALIFIED_CARDINALITY)
}

/// Get the SID for owl:onClass
#[inline]
pub fn on_class_sid() -> Sid {
    Sid::new(OWL, ON_CLASS)
}

/// Get the SID for owl:intersectionOf
#[inline]
pub fn intersection_of_sid() -> Sid {
    Sid::new(OWL, INTERSECTION_OF)
}

/// Get the SID for owl:unionOf
#[inline]
pub fn union_of_sid() -> Sid {
    Sid::new(OWL, UNION_OF)
}

/// Get the SID for owl:oneOf
#[inline]
pub fn one_of_sid() -> Sid {
    Sid::new(OWL, ONE_OF)
}

/// Query helper for finding OWL-typed entities
///
/// Common pattern: find all entities that are instances of a specific OWL class
/// (e.g., all owl:SymmetricProperty instances, all owl:Restriction instances)
///
/// Returns flakes of the form: (?entity rdf:type owl:Class)
pub async fn find_owl_typed_entities(
    db: GraphDbRef<'_>,
    owl_class: &str,
) -> crate::Result<Vec<fluree_db_core::flake::Flake>> {
    let owl_class_sid = Sid::new(OWL, owl_class);

    let flakes = db
        .range(
            IndexType::Opst,
            RangeTest::Eq,
            RangeMatch {
                o: Some(FlakeValue::Ref(owl_class_sid)),
                ..Default::default()
            },
        )
        .await?;

    // Filter to only rdf:type assertions
    Ok(flakes
        .into_iter()
        .filter(|f| is_rdf_type(&f.p) && f.op)
        .collect())
}

/// Centralized registry of commonly used OWL SIDs
///
/// This eliminates redundant SID creation across reasoning functions.
/// Create once and reuse throughout a reasoning session.
#[derive(Clone)]
pub struct OwlSidRegistry {
    // Core OWL classes
    pub restriction: Sid,
    pub symmetric_property: Sid,
    pub transitive_property: Sid,
    pub functional_property: Sid,
    pub inverse_functional_property: Sid,

    // OWL properties
    pub inverse_of: Sid,
    pub equivalent_class: Sid,
    pub equivalent_property: Sid,
    pub same_as: Sid,
    pub property_chain_axiom: Sid,
    pub has_key: Sid,

    // Restriction properties
    pub on_property: Sid,
    pub has_value: Sid,
    pub some_values_from: Sid,
    pub all_values_from: Sid,
    pub max_cardinality: Sid,
    pub max_qualified_cardinality: Sid,
    pub on_class: Sid,

    // Class expressions
    pub intersection_of: Sid,
    pub union_of: Sid,
    pub one_of: Sid,
}

impl OwlSidRegistry {
    /// Create a new registry with all commonly used OWL SIDs
    pub fn new() -> Self {
        Self {
            restriction: restriction_sid(),
            symmetric_property: symmetric_property_sid(),
            transitive_property: transitive_property_sid(),
            functional_property: functional_property_sid(),
            inverse_functional_property: inverse_functional_property_sid(),
            inverse_of: inverse_of_sid(),
            equivalent_class: equivalent_class_sid(),
            equivalent_property: equivalent_property_sid(),
            same_as: same_as_sid(),
            property_chain_axiom: property_chain_axiom_sid(),
            has_key: has_key_sid(),
            on_property: on_property_sid(),
            has_value: has_value_sid(),
            some_values_from: some_values_from_sid(),
            all_values_from: all_values_from_sid(),
            max_cardinality: max_cardinality_sid(),
            max_qualified_cardinality: max_qualified_cardinality_sid(),
            on_class: on_class_sid(),
            intersection_of: intersection_of_sid(),
            union_of: union_of_sid(),
            one_of: one_of_sid(),
        }
    }
}

impl Default for OwlSidRegistry {
    fn default() -> Self {
        Self::new()
    }
}
