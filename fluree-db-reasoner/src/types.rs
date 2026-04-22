//! Shared types for the reasoner crate.
//!
//! Important: this crate must NOT depend on `fluree-db-query` to avoid cyclic deps.

use fluree_db_core::Sid;

/// Reasoning modes that may affect derived-facts materialization and caching.
///
/// This mirrors `fluree-db-query::rewrite::ReasoningModes` structurally, but is defined
/// locally to keep `fluree-db-reasoner` dependency-free of the query crate.
///
/// # Reasoning Methods
///
/// - `rdfs` - RDFS entailment (subClassOf, subPropertyOf hierarchies)
/// - `owl2ql` - OWL 2 QL profile (query rewriting for simple ontologies)
/// - `owl2rl` - OWL 2 RL profile (forward-chaining materialization)
/// - `owl_datalog` - Extended OWL with Datalog-expressible constructs:
///   - Complex class expressions (intersectionOf, unionOf with restrictions)
///   - Property chains with inverse elements and arbitrary length
///   - Enhanced someValuesFrom/allValuesFrom reasoning
///   - This is a SUPERSET of owl2rl, enabling additional constructs
/// - `datalog` - Custom user-defined datalog rules
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReasoningModes {
    pub rdfs: bool,
    pub owl2ql: bool,
    pub datalog: bool,
    pub owl2rl: bool,
    /// OWL-Datalog: Extended OWL reasoning with complex class expressions.
    /// This is a superset of owl2rl that supports additional constructs
    /// expressible in Datalog: complex intersections, property chains with
    /// inverses, nested restrictions, etc.
    pub owl_datalog: bool,
    pub explicit_none: bool,
}

/// An element in a property chain (owl:propertyChainAxiom).
///
/// Property chains express derived properties as compositions of other properties:
/// `P = P1 o P2 o ... o Pn`
///
/// Each element can be a direct property or its inverse:
/// - Direct: `P(x, y)` matches the property in the forward direction
/// - Inverse: `P^-1(x, y)` means `P(y, x)` - the property traversed backwards
///
/// # Examples
///
/// Chain `hasSibling = hasParent o hasChild^-1`:
/// - P1 = hasParent (direct)
/// - P2 = hasChild (inverse) - meaning `hasChild(y, x)` or equivalently `isChildOf(x, y)`
///
/// Chain `hasUncle = hasParent o hasSibling`:
/// - P1 = hasParent (direct)
/// - P2 = hasSibling (direct)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChainElement {
    /// The property SID
    pub property: Sid,
    /// Whether to traverse this property in reverse direction
    pub is_inverse: bool,
}

impl ChainElement {
    /// Create a direct (non-inverse) chain element
    pub fn direct(property: Sid) -> Self {
        Self {
            property,
            is_inverse: false,
        }
    }

    /// Create an inverse chain element
    pub fn inverse(property: Sid) -> Self {
        Self {
            property,
            is_inverse: true,
        }
    }

    /// Normalize this element by applying double inverse cancellation.
    ///
    /// If we have `owl:inverseOf` wrapping an `owl:inverseOf`, they cancel out.
    /// This is handled during parsing, but this method can apply additional normalization.
    ///
    /// Double inverse: `(P^-1)^-1 = P`
    pub fn with_inverse_toggle(mut self) -> Self {
        self.is_inverse = !self.is_inverse;
        self
    }
}

/// A complete property chain definition.
///
/// Represents: `DerivedProperty = Element1 o Element2 o ... o ElementN`
///
/// For example, `hasUncle = hasParent o hasSibling` where:
/// - derived_property = ex:hasUncle
/// - chain = [ChainElement::direct(ex:hasParent), ChainElement::direct(ex:hasSibling)]
///
/// The chain must have at least 2 elements.
#[derive(Debug, Clone)]
pub struct PropertyChain {
    /// The property being defined by this chain
    pub derived_property: Sid,
    /// The chain elements (minimum 2)
    pub chain: Vec<ChainElement>,
}

impl PropertyChain {
    /// Create a new property chain
    pub fn new(derived_property: Sid, chain: Vec<ChainElement>) -> Self {
        debug_assert!(
            chain.len() >= 2,
            "Property chain must have at least 2 elements"
        );
        Self {
            derived_property,
            chain,
        }
    }

    /// Get the length of the chain
    pub fn len(&self) -> usize {
        self.chain.len()
    }

    /// Check if the chain is empty (should never be true for valid chains)
    pub fn is_empty(&self) -> bool {
        self.chain.is_empty()
    }

    /// Get the first element of the chain
    pub fn first(&self) -> Option<&ChainElement> {
        self.chain.first()
    }

    /// Get the last element of the chain
    pub fn last(&self) -> Option<&ChainElement> {
        self.chain.last()
    }
}

/// A property expression used in owl:onProperty.
///
/// In OWL2, `owl:onProperty` can reference:
/// - A named property (IRI)
/// - An inverse property expression: `owl:inverseOf P`
/// - A property chain expression: `owl:propertyChainAxiom (P1 P2 ...)`
///
/// # Examples
///
/// Named property:
/// ```json
/// "owl:onProperty": {"@id": "ex:manages"}
/// ```
///
/// Inverse property:
/// ```json
/// "owl:onProperty": {"owl:inverseOf": {"@id": "ex:manages"}}
/// ```
///
/// Property chain:
/// ```json
/// "owl:onProperty": {"owl:propertyChainAxiom": {"@list": [...]}}
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyExpression {
    /// A named property reference (IRI)
    Named(Sid),

    /// An inverse property expression: owl:inverseOf P
    /// The inner PropertyExpression allows for nested inverses which normalize:
    /// (P^-1)^-1 = P
    Inverse(Box<PropertyExpression>),

    /// A property chain expression: owl:propertyChainAxiom (P1 P2 ...)
    /// Each element can be direct or inverse.
    Chain(Vec<ChainElement>),
}

impl PropertyExpression {
    /// Create a named property expression
    pub fn named(property: Sid) -> Self {
        PropertyExpression::Named(property)
    }

    /// Create an inverse property expression
    pub fn inverse(inner: PropertyExpression) -> Self {
        // Normalize double inverse: (P^-1)^-1 = P
        match inner {
            PropertyExpression::Inverse(inner_inner) => *inner_inner,
            _ => PropertyExpression::Inverse(Box::new(inner)),
        }
    }

    /// Create a chain property expression
    pub fn chain(elements: Vec<ChainElement>) -> Self {
        PropertyExpression::Chain(elements)
    }

    /// Check if this is a simple named property
    pub fn is_named(&self) -> bool {
        matches!(self, PropertyExpression::Named(_))
    }

    /// Get the named property SID if this is a simple named property
    pub fn as_named(&self) -> Option<&Sid> {
        match self {
            PropertyExpression::Named(sid) => Some(sid),
            _ => None,
        }
    }

    /// Convert to a ChainElement if possible (named or single inverse).
    /// Returns None for chains (which require special handling).
    pub fn to_chain_element(&self) -> Option<ChainElement> {
        match self {
            PropertyExpression::Named(sid) => Some(ChainElement::direct(sid.clone())),
            PropertyExpression::Inverse(inner) => {
                // Only handle simple inverse of named property
                if let PropertyExpression::Named(sid) = inner.as_ref() {
                    Some(ChainElement::inverse(sid.clone()))
                } else {
                    None
                }
            }
            PropertyExpression::Chain(_) => None,
        }
    }
}
