//! OWL2-RL ontology extraction
//!
//! Extends the base Ontology with OWL2-RL specific information:
//! - Symmetric properties
//! - Transitive properties
//! - Inverse properties
//! - Domain and range declarations
//! - sameAs assertions

use crate::owl;
use crate::types::PropertyChain;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::flake::Flake;
use fluree_db_core::namespaces::{
    is_owl_inverse_of, is_owl_same_as, is_rdf_type, is_rdfs_domain, is_rdfs_range,
};
use fluree_db_core::range::{RangeMatch, RangeTest};
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{GraphDbRef, Sid};
use fluree_vocab::namespaces::{OWL, RDFS};
use fluree_vocab::owl_names::*;
use fluree_vocab::predicates::{RDFS_DOMAIN, RDFS_RANGE};
use hashbrown::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::Result;
use crate::rdf_list::{collect_chain_elements, collect_list_elements};

/// OWL2-RL specific ontology information
///
/// This captures the OWL constructs needed for RL materialization rules,
/// beyond what's captured in the base Ontology for QL rewriting.
#[derive(Debug, Clone)]
pub struct OntologyRL {
    inner: Arc<OntologyRLInner>,
}

#[derive(Debug)]
struct OntologyRLInner {
    /// Properties declared as owl:SymmetricProperty
    symmetric_properties: HashSet<Sid>,
    /// Properties declared as owl:TransitiveProperty
    transitive_properties: HashSet<Sid>,
    /// P → [P_inv1, P_inv2, ...] (bidirectional inverse mappings)
    inverse_of: HashMap<Sid, Vec<Sid>>,
    /// P → [C1, C2, ...] rdfs:domain declarations (property -> domain classes)
    domain: HashMap<Sid, Vec<Sid>>,
    /// P → [C1, C2, ...] rdfs:range declarations (property -> range classes)
    range: HashMap<Sid, Vec<Sid>>,
    /// P1 → [P2, ...] super-property relationships (P1 rdfs:subPropertyOf* P2)
    /// Built by inverting SchemaHierarchy.subproperties_of closure
    super_properties: HashMap<Sid, Vec<Sid>>,
    /// C1 → [C2, ...] super-class relationships (C1 rdfs:subClassOf* C2)
    /// Built by inverting SchemaHierarchy.subclasses_of closure
    /// Used for cax-sco: type(x, C1), subClassOf(C1, C2) → type(x, C2)
    super_classes: HashMap<Sid, Vec<Sid>>,
    /// C1 → [C2, ...] equivalent class relationships (bidirectional)
    /// For cax-eqc: type(x, C1), equivalentClass(C1, C2) → type(x, C2)
    equivalent_classes: HashMap<Sid, Vec<Sid>>,
    /// Property chain axioms (prp-spo2)
    /// Each PropertyChain defines: P = P1 o P2 o ... o Pn
    /// meaning P1(u0, u1), P2(u1, u2), ..., Pn(u_{n-1}, u_n) → P(u0, u_n)
    /// Supports arbitrary length chains (≥2) and inverse elements.
    property_chains: Vec<PropertyChain>,
    /// Properties declared as owl:FunctionalProperty (prp-fp)
    /// P(x, y1), P(x, y2) → sameAs(y1, y2)
    functional_properties: HashSet<Sid>,
    /// Properties declared as owl:InverseFunctionalProperty (prp-ifp)
    /// P(x1, y), P(x2, y) → sameAs(x1, x2)
    inverse_functional_properties: HashSet<Sid>,
    /// C → [[P1, P2, ...], ...] owl:hasKey declarations (prp-key)
    /// hasKey(C, [P1..Pn]), type(x,C), P1(x,z1).., type(y,C), P1(y,z1).. → sameAs(x,y)
    /// Each class can have multiple key declarations (rare but valid)
    has_keys: HashMap<Sid, Vec<Vec<Sid>>>,
    /// Schema epoch for cache validation
    epoch: u64,
}

impl OntologyRL {
    /// Create an empty ontology (no OWL assertions)
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(OntologyRLInner {
                symmetric_properties: HashSet::new(),
                transitive_properties: HashSet::new(),
                inverse_of: HashMap::new(),
                domain: HashMap::new(),
                range: HashMap::new(),
                super_properties: HashMap::new(),
                super_classes: HashMap::new(),
                equivalent_classes: HashMap::new(),
                property_chains: Vec::new(),
                functional_properties: HashSet::new(),
                inverse_functional_properties: HashSet::new(),
                has_keys: HashMap::new(),
                epoch: 0,
            }),
        }
    }

    /// Create an ontology with basic OWL properties (symmetric, transitive, inverse).
    ///
    /// For full OWL2-RL support, use [`new_full`](Self::new_full) instead.
    pub fn new(
        symmetric_properties: HashSet<Sid>,
        transitive_properties: HashSet<Sid>,
        inverse_of: HashMap<Sid, Vec<Sid>>,
        epoch: u64,
    ) -> Self {
        Self {
            inner: Arc::new(OntologyRLInner {
                symmetric_properties,
                transitive_properties,
                inverse_of,
                domain: HashMap::new(),
                range: HashMap::new(),
                super_properties: HashMap::new(),
                super_classes: HashMap::new(),
                equivalent_classes: HashMap::new(),
                property_chains: Vec::new(),
                functional_properties: HashSet::new(),
                inverse_functional_properties: HashSet::new(),
                has_keys: HashMap::new(),
                epoch,
            }),
        }
    }

    /// Create an ontology with all OWL2-RL data
    #[allow(clippy::too_many_arguments)]
    pub fn new_full(
        symmetric_properties: HashSet<Sid>,
        transitive_properties: HashSet<Sid>,
        inverse_of: HashMap<Sid, Vec<Sid>>,
        domain: HashMap<Sid, Vec<Sid>>,
        range: HashMap<Sid, Vec<Sid>>,
        super_properties: HashMap<Sid, Vec<Sid>>,
        super_classes: HashMap<Sid, Vec<Sid>>,
        equivalent_classes: HashMap<Sid, Vec<Sid>>,
        property_chains: Vec<PropertyChain>,
        functional_properties: HashSet<Sid>,
        inverse_functional_properties: HashSet<Sid>,
        has_keys: HashMap<Sid, Vec<Vec<Sid>>>,
        epoch: u64,
    ) -> Self {
        Self {
            inner: Arc::new(OntologyRLInner {
                symmetric_properties,
                transitive_properties,
                inverse_of,
                domain,
                range,
                super_properties,
                super_classes,
                equivalent_classes,
                property_chains,
                functional_properties,
                inverse_functional_properties,
                has_keys,
                epoch,
            }),
        }
    }

    /// Extract OWL2-RL ontology from a database with overlay
    ///
    /// Queries for:
    /// - `?p rdf:type owl:SymmetricProperty`
    /// - `?p rdf:type owl:TransitiveProperty`
    /// - `?p owl:inverseOf ?q`
    /// - `?p rdfs:domain ?c`
    /// - `?p rdfs:range ?c`
    pub async fn from_db_with_overlay(db: GraphDbRef<'_>) -> Result<Self> {
        let epoch = db.snapshot.t as u64;
        let mut symmetric_properties = HashSet::new();
        let mut transitive_properties = HashSet::new();
        let mut inverse_of: HashMap<Sid, Vec<Sid>> = HashMap::new();
        let mut domain: HashMap<Sid, Vec<Sid>> = HashMap::new();
        let mut range: HashMap<Sid, Vec<Sid>> = HashMap::new();

        // Create SIDs for owl:SymmetricProperty and owl:TransitiveProperty
        let owl_symmetric_sid = owl::symmetric_property_sid();
        let owl_transitive_sid = owl::transitive_property_sid();

        // Query OPST index for all rdf:type assertions where object is owl:SymmetricProperty
        // This finds all ?p rdf:type owl:SymmetricProperty
        let symmetric_flakes: Vec<Flake> = db
            .range(
                IndexType::Opst,
                RangeTest::Eq,
                RangeMatch {
                    o: Some(FlakeValue::Ref(owl_symmetric_sid)),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| is_rdf_type(&f.p) && f.op)
            .collect();

        for flake in symmetric_flakes {
            symmetric_properties.insert(flake.s.clone());
        }

        // Query for all ?p rdf:type owl:TransitiveProperty
        let transitive_flakes: Vec<Flake> = db
            .range(
                IndexType::Opst,
                RangeTest::Eq,
                RangeMatch {
                    o: Some(FlakeValue::Ref(owl_transitive_sid)),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| is_rdf_type(&f.p) && f.op)
            .collect();

        for flake in transitive_flakes {
            transitive_properties.insert(flake.s.clone());
        }

        // Query PSOT index for all owl:inverseOf assertions
        let owl_inverse_of_sid = owl::inverse_of_sid();

        let inverse_flakes: Vec<Flake> = db
            .range(
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch {
                    p: Some(owl_inverse_of_sid),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| is_owl_inverse_of(&f.p) && f.op)
            .collect();

        for flake in inverse_flakes {
            // P owl:inverseOf Q means P and Q are inverses of each other
            if let FlakeValue::Ref(q) = &flake.o {
                // Record bidirectionally
                inverse_of
                    .entry(flake.s.clone())
                    .or_default()
                    .push(q.clone());
                inverse_of
                    .entry(q.clone())
                    .or_default()
                    .push(flake.s.clone());
            }
        }

        // Query PSOT index for all rdfs:domain assertions
        // This finds all ?p rdfs:domain ?c
        let rdfs_domain_sid = Sid::new(RDFS, RDFS_DOMAIN);

        let domain_flakes: Vec<Flake> = db
            .range(
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch {
                    p: Some(rdfs_domain_sid),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| is_rdfs_domain(&f.p) && f.op)
            .collect();

        for flake in domain_flakes {
            // P rdfs:domain C means any subject of P is of type C
            if let FlakeValue::Ref(c) = &flake.o {
                domain.entry(flake.s.clone()).or_default().push(c.clone());
            }
        }

        // Query PSOT index for all rdfs:range assertions
        // This finds all ?p rdfs:range ?c
        let rdfs_range_sid = Sid::new(RDFS, RDFS_RANGE);

        let range_flakes: Vec<Flake> = db
            .range(
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch {
                    p: Some(rdfs_range_sid),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| is_rdfs_range(&f.p) && f.op)
            .collect();

        for flake in range_flakes {
            // P rdfs:range C means any object of P is of type C (when object is a Ref)
            if let FlakeValue::Ref(c) = &flake.o {
                range.entry(flake.s.clone()).or_default().push(c.clone());
            }
        }

        // Build super_properties by inverting SchemaHierarchy.subproperties_of
        // For prp-spo1: P1(x,y), P1 rdfs:subPropertyOf* P2 -> P2(x,y)
        // subproperties_of(P2) gives us all P1s, so we invert to get P1 -> [P2s]
        let mut super_properties: HashMap<Sid, Vec<Sid>> = HashMap::new();
        if let Some(hierarchy) = db.snapshot.schema_hierarchy() {
            // Get all properties that have subproperties by checking the schema
            if let Some(schema) = &db.snapshot.schema {
                for pred_info in &schema.pred.vals {
                    let p2 = &pred_info.id;
                    // Get all subproperties (descendants) of P2
                    for p1 in hierarchy.subproperties_of(p2) {
                        // P1 is a subproperty of P2, so P2 is a super-property of P1
                        super_properties
                            .entry(p1.clone())
                            .or_default()
                            .push(p2.clone());
                    }
                }
            }
        }

        // Normalize super_properties: sort and dedup to avoid redundant work
        for props in super_properties.values_mut() {
            props.sort();
            props.dedup();
        }

        // Build super_classes by inverting SchemaHierarchy.subclasses_of
        // For cax-sco: type(x, C1), C1 rdfs:subClassOf* C2 -> type(x, C2)
        // subclasses_of(C2) gives us all C1s, so we invert to get C1 -> [C2s]
        // Note: In Fluree, classes are stored in pred.vals along with properties
        let mut super_classes: HashMap<Sid, Vec<Sid>> = HashMap::new();
        if let Some(hierarchy) = db.snapshot.schema_hierarchy() {
            if let Some(schema) = &db.snapshot.schema {
                for pred_info in &schema.pred.vals {
                    let c2 = &pred_info.id;
                    // Get all subclasses (descendants) of C2
                    // For properties without subclasses, this returns empty
                    for c1 in hierarchy.subclasses_of(c2) {
                        // C1 is a subclass of C2, so C2 is a super-class of C1
                        super_classes
                            .entry(c1.clone())
                            .or_default()
                            .push(c2.clone());
                    }
                }
            }
        }

        // Normalize super_classes: sort and dedup to avoid redundant work
        for classes in super_classes.values_mut() {
            classes.sort();
            classes.dedup();
        }

        // Query PSOT index for all owl:equivalentClass assertions
        // C1 owl:equivalentClass C2 means type(x, C1) ↔ type(x, C2) (bidirectional)
        let mut equivalent_classes: HashMap<Sid, Vec<Sid>> = HashMap::new();
        let owl_equivalent_class_sid = owl::equivalent_class_sid();

        let equiv_flakes: Vec<Flake> = db
            .range(
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch {
                    p: Some(owl_equivalent_class_sid),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| f.p.namespace_code == OWL && f.p.name.as_ref() == EQUIVALENT_CLASS && f.op)
            .collect();

        for flake in equiv_flakes {
            // C1 owl:equivalentClass C2 - store bidirectionally
            if let FlakeValue::Ref(c2) = &flake.o {
                let c1 = &flake.s;
                // Record both directions: C1 → C2 and C2 → C1
                equivalent_classes
                    .entry(c1.clone())
                    .or_default()
                    .push(c2.clone());
                equivalent_classes
                    .entry(c2.clone())
                    .or_default()
                    .push(c1.clone());
            }
        }

        // Normalize equivalent_classes: sort and dedup to avoid redundant work
        for classes in equivalent_classes.values_mut() {
            classes.sort();
            classes.dedup();
        }

        // Query PSOT index for all owl:propertyChainAxiom assertions
        // P owl:propertyChainAxiom (P1 P2 ...) means P1(u0,u1), P2(u1,u2), ... → P(u0, un)
        // Supports arbitrary length chains (≥2) and inverse elements via owl:inverseOf
        let mut property_chains: Vec<PropertyChain> = Vec::new();
        let owl_chain_axiom_sid = owl::property_chain_axiom_sid();

        let chain_flakes: Vec<Flake> = db
            .range(
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch {
                    p: Some(owl_chain_axiom_sid.clone()),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| {
                f.p.namespace_code == OWL && f.p.name.as_ref() == PROPERTY_CHAIN_AXIOM && f.op
            })
            .collect();

        for flake in chain_flakes {
            // Subject is the derived property P
            let derived_prop = flake.s.clone();
            // Object is the head of an RDF list containing chain properties (may include owl:inverseOf blanks)
            if let FlakeValue::Ref(list_head) = &flake.o {
                // Traverse the RDF list to get chain elements, resolving owl:inverseOf to ChainElements
                if let Ok(chain_elements) = collect_chain_elements(db, list_head).await {
                    // Only store chains with at least 2 elements
                    if chain_elements.len() >= 2 {
                        property_chains.push(PropertyChain::new(derived_prop, chain_elements));
                    }
                    // Note: Chains with length < 2 are silently ignored (invalid)
                }
            }
        }

        // Query OPST index for owl:FunctionalProperty declarations
        // ?p rdf:type owl:FunctionalProperty
        let mut functional_properties = HashSet::new();
        let owl_functional_sid = owl::functional_property_sid();

        let functional_flakes: Vec<Flake> = db
            .range(
                IndexType::Opst,
                RangeTest::Eq,
                RangeMatch {
                    o: Some(FlakeValue::Ref(owl_functional_sid)),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| is_rdf_type(&f.p) && f.op)
            .collect();

        for flake in functional_flakes {
            functional_properties.insert(flake.s.clone());
        }

        // Query OPST index for owl:InverseFunctionalProperty declarations
        // ?p rdf:type owl:InverseFunctionalProperty
        let mut inverse_functional_properties = HashSet::new();
        let owl_inverse_functional_sid = owl::inverse_functional_property_sid();

        let inv_functional_flakes: Vec<Flake> = db
            .range(
                IndexType::Opst,
                RangeTest::Eq,
                RangeMatch {
                    o: Some(FlakeValue::Ref(owl_inverse_functional_sid)),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| is_rdf_type(&f.p) && f.op)
            .collect();

        for flake in inv_functional_flakes {
            inverse_functional_properties.insert(flake.s.clone());
        }

        // Query PSOT index for all owl:hasKey declarations
        // C owl:hasKey (P1 P2 ...) means instances of C with same key values are sameAs
        let mut has_keys: HashMap<Sid, Vec<Vec<Sid>>> = HashMap::new();
        let owl_has_key_sid = owl::has_key_sid();

        let has_key_flakes: Vec<Flake> = db
            .range(
                IndexType::Psot,
                RangeTest::Eq,
                RangeMatch {
                    p: Some(owl_has_key_sid.clone()),
                    ..Default::default()
                },
            )
            .await?
            .into_iter()
            .filter(|f| f.p.namespace_code == OWL && f.p.name.as_ref() == HAS_KEY && f.op)
            .collect();

        for flake in has_key_flakes {
            // Subject is the class C
            let class = &flake.s;
            // Object is the head of an RDF list containing key properties
            if let FlakeValue::Ref(list_head) = &flake.o {
                // Traverse the RDF list to get key properties
                if let Ok(key_properties) = collect_list_elements(db, list_head).await {
                    // Only store non-empty key lists
                    if !key_properties.is_empty() {
                        has_keys
                            .entry(class.clone())
                            .or_default()
                            .push(key_properties);
                    }
                }
            }
        }

        Ok(Self::new_full(
            symmetric_properties,
            transitive_properties,
            inverse_of,
            domain,
            range,
            super_properties,
            super_classes,
            equivalent_classes,
            property_chains,
            functional_properties,
            inverse_functional_properties,
            has_keys,
            epoch,
        ))
    }

    /// Get symmetric properties
    pub fn symmetric_properties(&self) -> &HashSet<Sid> {
        &self.inner.symmetric_properties
    }

    /// Get transitive properties
    pub fn transitive_properties(&self) -> &HashSet<Sid> {
        &self.inner.transitive_properties
    }

    /// Get inverses of a property
    pub fn inverses_of(&self, p: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.inner
            .inverse_of
            .get(p)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Check if a property is symmetric
    pub fn is_symmetric(&self, p: &Sid) -> bool {
        self.inner.symmetric_properties.contains(p)
    }

    /// Check if a property is transitive
    pub fn is_transitive(&self, p: &Sid) -> bool {
        self.inner.transitive_properties.contains(p)
    }

    /// Get domain classes for a property
    ///
    /// Returns the classes that are the domain of this property.
    /// For `P rdfs:domain C`, using P implies subject is of type C.
    pub fn domain_of(&self, p: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.inner
            .domain
            .get(p)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get range classes for a property
    ///
    /// Returns the classes that are the range of this property.
    /// For `P rdfs:range C`, using P implies object is of type C.
    pub fn range_of(&self, p: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.inner
            .range
            .get(p)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all properties that have domain declarations
    pub fn properties_with_domain(&self) -> impl Iterator<Item = &Sid> {
        self.inner.domain.keys()
    }

    /// Get all properties that have range declarations
    pub fn properties_with_range(&self) -> impl Iterator<Item = &Sid> {
        self.inner.range.keys()
    }

    /// Get super-properties of a property (for prp-spo1)
    ///
    /// Returns the properties P2 such that P1 rdfs:subPropertyOf* P2.
    /// For `P1 rdfs:subPropertyOf P2`, facts P1(x,y) imply P2(x,y).
    pub fn super_properties_of(&self, p: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.inner
            .super_properties
            .get(p)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all properties that have super-properties
    pub fn properties_with_super_properties(&self) -> impl Iterator<Item = &Sid> {
        self.inner.super_properties.keys()
    }

    /// Get super-classes of a class (for cax-sco)
    ///
    /// Returns the classes C2 such that C1 rdfs:subClassOf* C2.
    /// For `C1 rdfs:subClassOf C2`, facts type(x, C1) imply type(x, C2).
    pub fn super_classes_of(&self, c: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.inner
            .super_classes
            .get(c)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all classes that have super-classes
    pub fn classes_with_super_classes(&self) -> impl Iterator<Item = &Sid> {
        self.inner.super_classes.keys()
    }

    /// Get equivalent classes of a class (for cax-eqc)
    ///
    /// Returns the classes C2 such that C1 owl:equivalentClass C2.
    /// For `C1 owl:equivalentClass C2`, facts type(x, C1) imply type(x, C2) and vice versa.
    pub fn equivalent_classes_of(&self, c: &Sid) -> &[Sid] {
        static EMPTY: &[Sid] = &[];
        self.inner
            .equivalent_classes
            .get(c)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all classes that have equivalent classes
    pub fn classes_with_equivalent_classes(&self) -> impl Iterator<Item = &Sid> {
        self.inner.equivalent_classes.keys()
    }

    /// Get all property chains
    ///
    /// Returns all property chain axioms for rule application.
    /// Each chain defines: derived_property = element1 o element2 o ... o elementN
    pub fn property_chains(&self) -> &[PropertyChain] {
        &self.inner.property_chains
    }

    /// Get property chains for a derived property (for prp-spo2)
    ///
    /// Returns all chains that derive the given property.
    pub fn property_chains_for(&self, p: &Sid) -> Vec<&PropertyChain> {
        self.inner
            .property_chains
            .iter()
            .filter(|chain| &chain.derived_property == p)
            .collect()
    }

    /// Get all derived properties that have property chain axioms
    pub fn properties_with_chains(&self) -> impl Iterator<Item = &Sid> {
        self.inner
            .property_chains
            .iter()
            .map(|c| &c.derived_property)
    }

    /// Check if there are any property chains
    pub fn has_property_chains(&self) -> bool {
        !self.inner.property_chains.is_empty()
    }

    /// Get all predicates that appear as chain components
    ///
    /// Used for seeding initial delta - need to load facts with these predicates.
    pub fn chain_component_predicates(&self) -> HashSet<Sid> {
        let mut components = HashSet::new();
        for chain in &self.inner.property_chains {
            for element in &chain.chain {
                components.insert(element.property.clone());
            }
        }
        components
    }

    /// Get functional properties (prp-fp)
    ///
    /// FunctionalProperty(P), P(x, y1), P(x, y2) → sameAs(y1, y2)
    pub fn functional_properties(&self) -> &HashSet<Sid> {
        &self.inner.functional_properties
    }

    /// Check if a property is functional
    pub fn is_functional(&self, p: &Sid) -> bool {
        self.inner.functional_properties.contains(p)
    }

    /// Get inverse-functional properties (prp-ifp)
    ///
    /// InverseFunctionalProperty(P), P(x1, y), P(x2, y) → sameAs(x1, x2)
    pub fn inverse_functional_properties(&self) -> &HashSet<Sid> {
        &self.inner.inverse_functional_properties
    }

    /// Check if a property is inverse-functional
    pub fn is_inverse_functional(&self, p: &Sid) -> bool {
        self.inner.inverse_functional_properties.contains(p)
    }

    /// Get all predicates that have owl:inverseOf declarations (prp-inv)
    ///
    /// These predicates need their facts seeded for the inverse rule to fire.
    pub fn properties_with_inverses(&self) -> impl Iterator<Item = &Sid> {
        self.inner.inverse_of.keys()
    }

    /// Get hasKey declarations for a class (prp-key)
    ///
    /// hasKey(C, [P1..Pn]) means instances of C with matching key values are sameAs.
    /// Returns a list of key property lists (a class can have multiple hasKey declarations).
    pub fn has_keys_of(&self, class: &Sid) -> &[Vec<Sid>] {
        static EMPTY: &[Vec<Sid>] = &[];
        self.inner
            .has_keys
            .get(class)
            .map(std::vec::Vec::as_slice)
            .unwrap_or(EMPTY)
    }

    /// Get all classes that have owl:hasKey declarations
    pub fn classes_with_keys(&self) -> impl Iterator<Item = &Sid> {
        self.inner.has_keys.keys()
    }

    /// Get all key properties across all hasKey declarations
    ///
    /// Used for seeding initial delta - need to load facts with these predicates.
    pub fn all_key_properties(&self) -> HashSet<Sid> {
        let mut props = HashSet::new();
        for key_lists in self.inner.has_keys.values() {
            for key_list in key_lists {
                for prop in key_list {
                    props.insert(prop.clone());
                }
            }
        }
        props
    }

    /// Get ontology epoch
    pub fn epoch(&self) -> u64 {
        self.inner.epoch
    }

    /// Check if ontology is empty (no OWL2-RL assertions)
    pub fn is_empty(&self) -> bool {
        self.inner.symmetric_properties.is_empty()
            && self.inner.transitive_properties.is_empty()
            && self.inner.inverse_of.is_empty()
            && self.inner.domain.is_empty()
            && self.inner.range.is_empty()
            && self.inner.super_properties.is_empty()
            && self.inner.super_classes.is_empty()
            && self.inner.equivalent_classes.is_empty()
            && self.inner.property_chains.is_empty()
            && self.inner.functional_properties.is_empty()
            && self.inner.inverse_functional_properties.is_empty()
            && self.inner.has_keys.is_empty()
    }

    /// Check if ontology has identity-producing rules (fp/ifp/hasKey that generate sameAs)
    pub fn has_identity_rules(&self) -> bool {
        !self.inner.functional_properties.is_empty()
            || !self.inner.inverse_functional_properties.is_empty()
            || !self.inner.has_keys.is_empty()
    }

    /// Check if ontology has only domain/range (no property rules)
    ///
    /// This is useful because domain/range rules don't interact with
    /// symmetric/transitive/inverse rules in the same way.
    pub fn has_property_rules(&self) -> bool {
        !self.inner.symmetric_properties.is_empty()
            || !self.inner.transitive_properties.is_empty()
            || !self.inner.inverse_of.is_empty()
            || !self.inner.super_properties.is_empty()
            || !self.inner.property_chains.is_empty()
            || !self.inner.functional_properties.is_empty()
            || !self.inner.inverse_functional_properties.is_empty()
    }

    /// Check if ontology has domain or range declarations
    pub fn has_domain_range(&self) -> bool {
        !self.inner.domain.is_empty() || !self.inner.range.is_empty()
    }

    /// Check if ontology has class hierarchy rules (subClassOf, equivalentClass)
    pub fn has_class_rules(&self) -> bool {
        !self.inner.super_classes.is_empty() || !self.inner.equivalent_classes.is_empty()
    }
}

/// Load initial owl:sameAs assertions from the database
///
/// Returns pairs of SIDs that are asserted to be the same.
pub async fn load_same_as_assertions(db: GraphDbRef<'_>) -> Result<Vec<(Sid, Sid)>> {
    let owl_same_as_sid = owl::same_as_sid();

    let same_as_flakes: Vec<Flake> = db
        .range(
            IndexType::Psot,
            RangeTest::Eq,
            RangeMatch {
                p: Some(owl_same_as_sid),
                ..Default::default()
            },
        )
        .await?
        .into_iter()
        .filter(|f| is_owl_same_as(&f.p) && f.op)
        .collect();

    let mut pairs = Vec::new();
    for flake in same_as_flakes {
        if let FlakeValue::Ref(o) = &flake.o {
            pairs.push((flake.s.clone(), o.clone()));
        }
    }

    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_ontology() {
        let ont = OntologyRL::empty();
        assert!(ont.is_empty());
        assert_eq!(ont.epoch(), 0);
    }

    #[test]
    fn test_ontology_with_data() {
        let mut symmetric = HashSet::new();
        symmetric.insert(Sid::new(100, "knows"));

        let mut transitive = HashSet::new();
        transitive.insert(Sid::new(100, "ancestorOf"));

        let mut inverse = HashMap::new();
        inverse.insert(Sid::new(100, "hasParent"), vec![Sid::new(100, "hasChild")]);

        let ont = OntologyRL::new(symmetric, transitive, inverse, 42);

        assert!(!ont.is_empty());
        assert_eq!(ont.epoch(), 42);
        assert!(ont.is_symmetric(&Sid::new(100, "knows")));
        assert!(!ont.is_symmetric(&Sid::new(100, "likes")));
        assert!(ont.is_transitive(&Sid::new(100, "ancestorOf")));
        assert_eq!(ont.inverses_of(&Sid::new(100, "hasParent")).len(), 1);
    }
}
