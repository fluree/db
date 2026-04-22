//! OWL Restriction Types and Parsing
//!
//! This module handles OWL class restrictions used in OWL2-RL reasoning:
//! - owl:hasValue (cls-hv1/2)
//! - owl:someValuesFrom (cls-svf1/2)
//! - owl:allValuesFrom (cls-avf)
//! - owl:maxCardinality / owl:maxQualifiedCardinality (cls-maxc2/maxqc3/4)
//! - owl:intersectionOf (cls-int1/2)
//! - owl:unionOf (cls-uni)
//! - owl:oneOf (cls-oo)
//!
//! Restrictions are anonymous classes defined by property constraints.
//! For example:
//! ```turtle
//! ex:Parent owl:equivalentClass [
//!     a owl:Restriction ;
//!     owl:onProperty ex:hasChild ;
//!     owl:someValuesFrom ex:Person
//! ] .
//! ```

use crate::owl;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::namespaces::is_rdf_type;
use fluree_db_core::range::{RangeMatch, RangeTest};
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{GraphDbRef, Sid};
use fluree_vocab::namespaces::OWL;
use fluree_vocab::owl_names::*;
use hashbrown::HashMap;

use crate::error::Result;
use crate::rdf_list::{collect_list_elements, resolve_property_expression};
use crate::types::PropertyExpression;

/// A value that can appear in a hasValue restriction
///
/// **Restricted form**: Currently only supports Ref values (IRIs/blank nodes).
/// Literal hasValue restrictions are not yet supported to avoid datatype
/// preservation issues. This matches the "restricted form first" approach
/// used for owl:hasKey.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RestrictionValue {
    /// Reference to another resource (IRI or blank node)
    Ref(Sid),
    // Note: Literal support deferred - requires preserving datatype in derived flakes
}

/// Reference to a class, which can be a named class or another restriction
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClassRef {
    /// A named class (IRI)
    Named(Sid),
    /// An anonymous restriction class (blank node ID)
    /// The actual restriction details are looked up separately
    Anonymous(Sid),
}

impl ClassRef {
    /// Get the underlying SID regardless of variant
    pub fn sid(&self) -> &Sid {
        match self {
            ClassRef::Named(sid) => sid,
            ClassRef::Anonymous(sid) => sid,
        }
    }
}

/// Types of OWL restrictions
///
/// Each variant corresponds to a specific OWL2-RL rule pattern.
///
/// Property expressions in restrictions support:
/// - Named properties: `owl:onProperty ex:hasChild`
/// - Inverse properties: `owl:onProperty [ owl:inverseOf ex:hasParent ]`
/// - Property chains: `owl:onProperty [ owl:propertyChainAxiom (ex:hasParent ex:hasSibling) ]`
#[derive(Debug, Clone)]
pub enum RestrictionType {
    /// owl:hasValue restriction (cls-hv1/2)
    /// - Forward: P(x, v) → type(x, C) where C is restriction class
    /// - Backward: type(x, C) → P(x, v)
    HasValue {
        /// The property expression being restricted (can be named, inverse, or chain)
        property: PropertyExpression,
        /// The required value
        value: RestrictionValue,
    },

    /// owl:someValuesFrom restriction (cls-svf1/2)
    /// P(x, y), type(y, D) → type(x, C) where C is restriction class
    SomeValuesFrom {
        /// The property expression being restricted (can be named, inverse, or chain)
        property: PropertyExpression,
        /// The target class that values must be instances of
        target_class: ClassRef,
    },

    /// owl:allValuesFrom restriction (cls-avf)
    /// type(x, C), P(x, y) → type(y, D) where C is restriction class
    AllValuesFrom {
        /// The property expression being restricted (can be named, inverse, or chain)
        property: PropertyExpression,
        /// The class that all values must be instances of
        target_class: ClassRef,
    },

    /// owl:maxCardinality = 1 restriction (cls-maxc2)
    /// P(x, y1), P(x, y2), type(x, C) → sameAs(y1, y2)
    /// Identity-producing rule
    MaxCardinality1 {
        /// The property expression being restricted (can be named, inverse, or chain)
        property: PropertyExpression,
    },

    /// owl:maxQualifiedCardinality = 1 restriction (cls-maxqc3/4)
    /// P(x, y1), P(x, y2), type(x, C), type(y1, D), type(y2, D) → sameAs(y1, y2)
    /// Identity-producing rule
    MaxQualifiedCardinality1 {
        /// The property expression being restricted (can be named, inverse, or chain)
        property: PropertyExpression,
        /// The qualifying class
        on_class: Sid,
    },

    /// owl:intersectionOf restriction (cls-int1/2)
    /// - Forward: type(x, C1) ∧ type(x, C2) ∧ ... → type(x, C)
    /// - Backward: type(x, C) → type(x, C1) ∧ type(x, C2) ∧ ...
    IntersectionOf {
        /// The member classes of the intersection
        members: Vec<ClassRef>,
    },

    /// owl:unionOf restriction (cls-uni)
    /// type(x, Ci) → type(x, C) for any member Ci
    UnionOf {
        /// The member classes of the union
        members: Vec<ClassRef>,
    },

    /// owl:oneOf restriction (cls-oo)
    /// Enumerated class: the class extension is exactly the listed individuals
    /// For each individual i in the list: type(i, C)
    OneOf {
        /// The enumerated individuals
        individuals: Vec<Sid>,
    },
}

/// Parsed restriction data for a restriction class (blank node)
#[derive(Debug, Clone)]
pub struct ParsedRestriction {
    /// The blank node ID representing this restriction class
    pub restriction_id: Sid,
    /// The type of restriction
    pub restriction_type: RestrictionType,
}

/// Collection of all parsed restrictions in the ontology
#[derive(Debug, Default)]
pub struct RestrictionIndex {
    /// Map from restriction blank node ID to parsed restriction
    by_id: HashMap<Sid, ParsedRestriction>,

    /// Index: property → list of HasValue restrictions on that property (named properties)
    has_value_by_property: HashMap<Sid, Vec<Sid>>,

    /// Index: property → list of SomeValuesFrom restrictions on that property (named properties)
    some_values_from_by_property: HashMap<Sid, Vec<Sid>>,

    /// Index: property → list of AllValuesFrom restrictions on that property (named properties)
    all_values_from_by_property: HashMap<Sid, Vec<Sid>>,

    /// Index: property → list of MaxCardinality1 restrictions on that property (named properties)
    max_cardinality_by_property: HashMap<Sid, Vec<Sid>>,

    /// Index: property → list of MaxQualifiedCardinality1 restrictions on that property (named properties)
    max_qualified_cardinality_by_property: HashMap<Sid, Vec<Sid>>,

    /// Index: inverse property → list of HasValue restrictions on that inverse property
    has_value_by_inverse_property: HashMap<Sid, Vec<Sid>>,

    /// Index: inverse property → list of SomeValuesFrom restrictions on that inverse property
    some_values_from_by_inverse_property: HashMap<Sid, Vec<Sid>>,

    /// Index: inverse property → list of AllValuesFrom restrictions on that inverse property
    all_values_from_by_inverse_property: HashMap<Sid, Vec<Sid>>,

    /// List of restrictions with chain property expressions (need special handling)
    chain_property_restrictions: Vec<Sid>,

    /// List of IntersectionOf restriction IDs
    intersection_restrictions: Vec<Sid>,

    /// List of UnionOf restriction IDs
    union_restrictions: Vec<Sid>,

    /// List of OneOf restriction IDs
    one_of_restrictions: Vec<Sid>,

    /// Count of hasValue restrictions skipped due to literal values (diagnostic)
    /// Literal hasValue is not yet supported - only Ref values are processed
    skipped_literal_has_value: usize,
}

impl RestrictionIndex {
    /// Create an empty restriction index
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if the index is empty (no restrictions)
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Get a restriction by its ID
    pub fn get(&self, id: &Sid) -> Option<&ParsedRestriction> {
        self.by_id.get(id)
    }

    /// Get all HasValue restriction IDs for a property
    pub fn has_value_restrictions_for(&self, property: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.has_value_by_property
            .get(property)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all SomeValuesFrom restriction IDs for a property
    pub fn some_values_from_restrictions_for(&self, property: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.some_values_from_by_property
            .get(property)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all AllValuesFrom restriction IDs for a property
    pub fn all_values_from_restrictions_for(&self, property: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.all_values_from_by_property
            .get(property)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all MaxCardinality1 restriction IDs for a property
    pub fn max_cardinality_restrictions_for(&self, property: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.max_cardinality_by_property
            .get(property)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all MaxQualifiedCardinality1 restriction IDs for a property
    pub fn max_qualified_cardinality_restrictions_for(&self, property: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.max_qualified_cardinality_by_property
            .get(property)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all IntersectionOf restriction IDs
    pub fn intersection_restrictions(&self) -> &[Sid] {
        &self.intersection_restrictions
    }

    /// Get all UnionOf restriction IDs
    pub fn union_restrictions(&self) -> &[Sid] {
        &self.union_restrictions
    }

    /// Get all OneOf restriction IDs
    pub fn one_of_restrictions(&self) -> &[Sid] {
        &self.one_of_restrictions
    }

    /// Get all HasValue restriction IDs for an inverse property
    pub fn has_value_restrictions_for_inverse(&self, property: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.has_value_by_inverse_property
            .get(property)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all SomeValuesFrom restriction IDs for an inverse property
    pub fn some_values_from_restrictions_for_inverse(&self, property: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.some_values_from_by_inverse_property
            .get(property)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all AllValuesFrom restriction IDs for an inverse property
    pub fn all_values_from_restrictions_for_inverse(&self, property: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.all_values_from_by_inverse_property
            .get(property)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all restriction IDs with chain property expressions
    pub fn chain_property_restrictions(&self) -> &[Sid] {
        &self.chain_property_restrictions
    }

    /// Get all properties that have any restrictions
    pub fn restricted_properties(&self) -> impl Iterator<Item = &Sid> {
        self.has_value_by_property
            .keys()
            .chain(self.some_values_from_by_property.keys())
            .chain(self.all_values_from_by_property.keys())
            .chain(self.max_cardinality_by_property.keys())
            .chain(self.max_qualified_cardinality_by_property.keys())
    }

    /// Get all inverse properties that have any restrictions
    pub fn restricted_inverse_properties(&self) -> impl Iterator<Item = &Sid> {
        self.has_value_by_inverse_property
            .keys()
            .chain(self.some_values_from_by_inverse_property.keys())
            .chain(self.all_values_from_by_inverse_property.keys())
    }

    /// Check if there are any identity-producing restriction rules
    pub fn has_identity_producing_restrictions(&self) -> bool {
        !self.max_cardinality_by_property.is_empty()
            || !self.max_qualified_cardinality_by_property.is_empty()
    }

    /// Get the count of hasValue restrictions that were skipped due to literal values
    ///
    /// This is a diagnostic counter - literal hasValue is not yet supported.
    /// Only Ref (IRI/blank node) values are processed.
    pub fn skipped_literal_has_value_count(&self) -> usize {
        self.skipped_literal_has_value
    }

    /// Record that a literal hasValue restriction was skipped (internal)
    fn record_skipped_literal_has_value(&mut self) {
        self.skipped_literal_has_value += 1;
    }

    /// Add a parsed restriction to the index (public for testing)
    #[cfg(test)]
    pub fn add_restriction_for_test(&mut self, restriction: ParsedRestriction) {
        self.add_restriction(restriction);
    }

    /// Add a parsed restriction to the index
    fn add_restriction(&mut self, restriction: ParsedRestriction) {
        let id = restriction.restriction_id.clone();

        // Index by property for property-based restrictions
        // PropertyExpression handling:
        // - Named(sid) → index by sid in the main property indices
        // - Inverse(Named(sid)) → index by sid in the inverse property indices
        // - Chain(...) → add to chain_property_restrictions list
        match &restriction.restriction_type {
            RestrictionType::HasValue { property, .. } => {
                match property {
                    PropertyExpression::Named(sid) => {
                        self.has_value_by_property
                            .entry(sid.clone())
                            .or_default()
                            .push(id.clone());
                    }
                    PropertyExpression::Inverse(inner) => {
                        if let PropertyExpression::Named(sid) = inner.as_ref() {
                            self.has_value_by_inverse_property
                                .entry(sid.clone())
                                .or_default()
                                .push(id.clone());
                        } else {
                            // Complex inverse (chain inside inverse) - treat as chain
                            self.chain_property_restrictions.push(id.clone());
                        }
                    }
                    PropertyExpression::Chain(_) => {
                        self.chain_property_restrictions.push(id.clone());
                    }
                }
            }
            RestrictionType::SomeValuesFrom { property, .. } => match property {
                PropertyExpression::Named(sid) => {
                    self.some_values_from_by_property
                        .entry(sid.clone())
                        .or_default()
                        .push(id.clone());
                }
                PropertyExpression::Inverse(inner) => {
                    if let PropertyExpression::Named(sid) = inner.as_ref() {
                        self.some_values_from_by_inverse_property
                            .entry(sid.clone())
                            .or_default()
                            .push(id.clone());
                    } else {
                        self.chain_property_restrictions.push(id.clone());
                    }
                }
                PropertyExpression::Chain(_) => {
                    self.chain_property_restrictions.push(id.clone());
                }
            },
            RestrictionType::AllValuesFrom { property, .. } => match property {
                PropertyExpression::Named(sid) => {
                    self.all_values_from_by_property
                        .entry(sid.clone())
                        .or_default()
                        .push(id.clone());
                }
                PropertyExpression::Inverse(inner) => {
                    if let PropertyExpression::Named(sid) = inner.as_ref() {
                        self.all_values_from_by_inverse_property
                            .entry(sid.clone())
                            .or_default()
                            .push(id.clone());
                    } else {
                        self.chain_property_restrictions.push(id.clone());
                    }
                }
                PropertyExpression::Chain(_) => {
                    self.chain_property_restrictions.push(id.clone());
                }
            },
            RestrictionType::MaxCardinality1 { property } => {
                match property {
                    PropertyExpression::Named(sid) => {
                        self.max_cardinality_by_property
                            .entry(sid.clone())
                            .or_default()
                            .push(id.clone());
                    }
                    PropertyExpression::Inverse(_) | PropertyExpression::Chain(_) => {
                        // Cardinality on inverse/chain is complex - add to chain list
                        self.chain_property_restrictions.push(id.clone());
                    }
                }
            }
            RestrictionType::MaxQualifiedCardinality1 { property, .. } => match property {
                PropertyExpression::Named(sid) => {
                    self.max_qualified_cardinality_by_property
                        .entry(sid.clone())
                        .or_default()
                        .push(id.clone());
                }
                PropertyExpression::Inverse(_) | PropertyExpression::Chain(_) => {
                    self.chain_property_restrictions.push(id.clone());
                }
            },
            RestrictionType::IntersectionOf { .. } => {
                self.intersection_restrictions.push(id.clone());
            }
            RestrictionType::UnionOf { .. } => {
                self.union_restrictions.push(id.clone());
            }
            RestrictionType::OneOf { .. } => {
                self.one_of_restrictions.push(id.clone());
            }
        }

        self.by_id.insert(id, restriction);
    }
}

/// Extract all OWL restrictions from the database
///
/// This function:
/// 1. Finds all owl:Restriction instances
/// 2. For each restriction, extracts:
///    - owl:onProperty
///    - owl:hasValue / owl:someValuesFrom / owl:allValuesFrom / etc.
/// 3. Builds a RestrictionIndex for efficient lookup during rule application
pub async fn extract_restrictions(db: GraphDbRef<'_>) -> Result<RestrictionIndex> {
    let mut index = RestrictionIndex::new();

    // Step 1: Find all owl:Restriction instances
    // Query OPST for ?x rdf:type owl:Restriction
    let owl_restriction_sid = owl::restriction_sid();

    let restriction_flakes: Vec<Flake> = db
        .range(
            IndexType::Opst,
            RangeTest::Eq,
            RangeMatch {
                o: Some(FlakeValue::Ref(owl_restriction_sid)),
                ..Default::default()
            },
        )
        .await?
        .into_iter()
        .filter(|f| is_rdf_type(&f.p) && f.op)
        .collect();

    // Collect restriction blank node IDs
    let restriction_ids: Vec<Sid> = restriction_flakes.iter().map(|f| f.s.clone()).collect();

    // Build a set of all class expression SIDs (restrictions, intersections, unions)
    // This is used to correctly identify nested anonymous class expressions
    let mut class_expression_sids: hashbrown::HashSet<Sid> =
        restriction_ids.iter().cloned().collect();

    // Pre-pass: Also find subjects of owl:intersectionOf and owl:unionOf
    // These don't require owl:Restriction type but are still class expressions
    let intersection_of_sid = owl::intersection_of_sid();
    let union_of_sid = owl::union_of_sid();
    let one_of_sid = owl::one_of_sid();

    // Query for owl:intersectionOf subjects
    let int_flakes: Vec<Flake> = db
        .range(
            IndexType::Psot,
            RangeTest::Eq,
            RangeMatch {
                p: Some(intersection_of_sid.clone()),
                ..Default::default()
            },
        )
        .await?
        .into_iter()
        .filter(|f| f.p.namespace_code == OWL && f.p.name.as_ref() == INTERSECTION_OF && f.op)
        .collect();

    for flake in &int_flakes {
        class_expression_sids.insert(flake.s.clone());
    }

    // Query for owl:unionOf subjects
    let union_flakes: Vec<Flake> = db
        .range(
            IndexType::Psot,
            RangeTest::Eq,
            RangeMatch {
                p: Some(union_of_sid.clone()),
                ..Default::default()
            },
        )
        .await?
        .into_iter()
        .filter(|f| f.p.namespace_code == OWL && f.p.name.as_ref() == UNION_OF && f.op)
        .collect();

    for flake in &union_flakes {
        class_expression_sids.insert(flake.s.clone());
    }

    // Query for owl:oneOf subjects
    let one_of_flakes_pre: Vec<Flake> = db
        .range(
            IndexType::Psot,
            RangeTest::Eq,
            RangeMatch {
                p: Some(one_of_sid.clone()),
                ..Default::default()
            },
        )
        .await?
        .into_iter()
        .filter(|f| f.p.namespace_code == OWL && f.p.name.as_ref() == ONE_OF && f.op)
        .collect();

    for flake in &one_of_flakes_pre {
        class_expression_sids.insert(flake.s.clone());
    }

    // Helper closure to convert SID to ClassRef, checking if it's a known class expression
    let to_class_ref = |sid: Sid| -> ClassRef {
        if class_expression_sids.contains(&sid) {
            ClassRef::Anonymous(sid)
        } else {
            ClassRef::Named(sid)
        }
    };

    // Step 2: For each restriction, extract its components
    // Create SIDs for restriction properties
    let on_property_sid = owl::on_property_sid();
    let has_value_sid = owl::has_value_sid();
    let some_values_from_sid = owl::some_values_from_sid();
    let all_values_from_sid = owl::all_values_from_sid();
    let max_cardinality_sid = owl::max_cardinality_sid();
    let max_qualified_cardinality_sid = owl::max_qualified_cardinality_sid();
    let on_class_sid = owl::on_class_sid();

    for restriction_id in &restriction_ids {
        // Query all properties of this restriction using SPOT index
        let restriction_flakes: Vec<Flake> = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch {
                    s: Some(restriction_id.clone()),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| f.op)
            .collect();

        // Extract components
        let mut on_property_raw: Option<Sid> = None;
        let mut has_value: Option<FlakeValue> = None;
        let mut some_values_from: Option<Sid> = None;
        let mut all_values_from: Option<Sid> = None;
        let mut max_cardinality: Option<u32> = None;
        let mut max_qualified_cardinality: Option<u32> = None;
        let mut on_class: Option<Sid> = None;

        for flake in &restriction_flakes {
            if flake.p == on_property_sid {
                if let FlakeValue::Ref(prop) = &flake.o {
                    on_property_raw = Some(prop.clone());
                }
            } else if flake.p == has_value_sid {
                has_value = Some(flake.o.clone());
            } else if flake.p == some_values_from_sid {
                if let FlakeValue::Ref(cls) = &flake.o {
                    some_values_from = Some(cls.clone());
                }
            } else if flake.p == all_values_from_sid {
                if let FlakeValue::Ref(cls) = &flake.o {
                    all_values_from = Some(cls.clone());
                }
            } else if flake.p == max_cardinality_sid {
                if let Some(n) = extract_cardinality_value(&flake.o) {
                    max_cardinality = Some(n);
                }
            } else if flake.p == max_qualified_cardinality_sid {
                if let Some(n) = extract_cardinality_value(&flake.o) {
                    max_qualified_cardinality = Some(n);
                }
            } else if flake.p == on_class_sid {
                if let FlakeValue::Ref(cls) = &flake.o {
                    on_class = Some(cls.clone());
                }
            }
        }

        // Resolve property expression (handles owl:inverseOf and owl:propertyChainAxiom)
        let property = if let Some(prop_sid) = on_property_raw {
            Some(resolve_property_expression(db, &prop_sid).await?)
        } else {
            None
        };

        // Build restriction based on what was found
        if let Some(property) = property {
            if let Some(value) = has_value {
                // HasValue restriction - only support Ref values (restricted form)
                // Literal hasValue requires preserving datatype in derived flakes
                if let FlakeValue::Ref(r) = value {
                    index.add_restriction(ParsedRestriction {
                        restriction_id: restriction_id.clone(),
                        restriction_type: RestrictionType::HasValue {
                            property,
                            value: RestrictionValue::Ref(r),
                        },
                    });
                } else {
                    // Record skipped literal hasValue for diagnostics
                    index.record_skipped_literal_has_value();
                }
            } else if let Some(target) = some_values_from {
                // SomeValuesFrom restriction - target could be a nested class expression
                index.add_restriction(ParsedRestriction {
                    restriction_id: restriction_id.clone(),
                    restriction_type: RestrictionType::SomeValuesFrom {
                        property,
                        target_class: to_class_ref(target),
                    },
                });
            } else if let Some(target) = all_values_from {
                // AllValuesFrom restriction - target could be a nested class expression
                index.add_restriction(ParsedRestriction {
                    restriction_id: restriction_id.clone(),
                    restriction_type: RestrictionType::AllValuesFrom {
                        property,
                        target_class: to_class_ref(target),
                    },
                });
            } else if let Some(1) = max_cardinality {
                // MaxCardinality = 1 restriction
                index.add_restriction(ParsedRestriction {
                    restriction_id: restriction_id.clone(),
                    restriction_type: RestrictionType::MaxCardinality1 { property },
                });
            } else if let Some(1) = max_qualified_cardinality {
                // MaxQualifiedCardinality = 1 restriction
                if let Some(qual_class) = on_class {
                    index.add_restriction(ParsedRestriction {
                        restriction_id: restriction_id.clone(),
                        restriction_type: RestrictionType::MaxQualifiedCardinality1 {
                            property,
                            on_class: qual_class,
                        },
                    });
                }
            }
            // Note: Cardinality > 1 is not useful for OWL2-RL identity derivation
        }
    }

    // Step 3: Extract intersectionOf, unionOf, oneOf class expressions
    // These don't require owl:Restriction type
    // Note: We reuse int_flakes and union_flakes from the pre-pass above

    for flake in int_flakes {
        if let FlakeValue::Ref(list_head) = &flake.o {
            if let Ok(members) = collect_list_elements(db, list_head).await {
                if !members.is_empty() {
                    // Use to_class_ref to properly identify nested class expressions
                    let class_refs: Vec<ClassRef> =
                        members.into_iter().map(&to_class_ref).collect();
                    index.add_restriction(ParsedRestriction {
                        restriction_id: flake.s.clone(),
                        restriction_type: RestrictionType::IntersectionOf {
                            members: class_refs,
                        },
                    });
                }
            }
        }
    }

    for flake in union_flakes {
        if let FlakeValue::Ref(list_head) = &flake.o {
            if let Ok(members) = collect_list_elements(db, list_head).await {
                if !members.is_empty() {
                    // Use to_class_ref to properly identify nested class expressions
                    let class_refs: Vec<ClassRef> =
                        members.into_iter().map(&to_class_ref).collect();
                    index.add_restriction(ParsedRestriction {
                        restriction_id: flake.s.clone(),
                        restriction_type: RestrictionType::UnionOf {
                            members: class_refs,
                        },
                    });
                }
            }
        }
    }

    // Note: We reuse one_of_flakes_pre from the pre-pass above
    for flake in one_of_flakes_pre {
        if let FlakeValue::Ref(list_head) = &flake.o {
            if let Ok(individuals) = collect_list_elements(db, list_head).await {
                if !individuals.is_empty() {
                    index.add_restriction(ParsedRestriction {
                        restriction_id: flake.s.clone(),
                        restriction_type: RestrictionType::OneOf { individuals },
                    });
                }
            }
        }
    }

    Ok(index)
}

/// Extract a cardinality value from a FlakeValue
///
/// Cardinality values are typically xsd:nonNegativeInteger literals.
fn extract_cardinality_value(value: &FlakeValue) -> Option<u32> {
    match value {
        FlakeValue::Long(n) => {
            if *n >= 0 && *n <= u32::MAX as i64 {
                Some(*n as u32)
            } else {
                None
            }
        }
        // Could also handle string representations, but Long should cover most cases
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_restriction_index_empty() {
        let index = RestrictionIndex::new();
        assert!(index.is_empty());
        assert!(!index.has_identity_producing_restrictions());
    }

    #[test]
    fn test_restriction_value_equality() {
        let sid1 = Sid::new(100, "test1");
        let sid2 = Sid::new(100, "test1");
        let sid3 = Sid::new(100, "test2");

        let rv1 = RestrictionValue::Ref(sid1);
        let rv2 = RestrictionValue::Ref(sid2);
        let rv3 = RestrictionValue::Ref(sid3);

        assert_eq!(rv1, rv2);
        assert_ne!(rv1, rv3);
    }

    #[test]
    fn test_class_ref() {
        let sid = Sid::new(100, "MyClass");
        let class_ref = ClassRef::Named(sid.clone());

        if let ClassRef::Named(s) = class_ref {
            assert_eq!(s, sid);
        } else {
            panic!("Expected Named variant");
        }
    }

    #[test]
    fn test_extract_cardinality_value() {
        assert_eq!(extract_cardinality_value(&FlakeValue::Long(1)), Some(1));
        assert_eq!(extract_cardinality_value(&FlakeValue::Long(0)), Some(0));
        assert_eq!(extract_cardinality_value(&FlakeValue::Long(-1)), None);
        assert_eq!(extract_cardinality_value(&FlakeValue::Long(100)), Some(100));
        assert_eq!(
            extract_cardinality_value(&FlakeValue::String("1".to_string())),
            None
        );
    }
}
