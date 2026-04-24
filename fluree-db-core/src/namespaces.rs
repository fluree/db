//! Database-specific namespace utilities
//!
//! This module contains utility functions for working with namespaces in the context
//! of Fluree databases. For the actual namespace codes and IRI constants, see the
//! `fluree-vocab` crate.
//!
//! The genesis database/root is seeded with a baseline
//! namespace table (`default-namespaces` / `default-namespace-codes`) and then
//! allocates new namespace codes lazily at first use during transactions.
//!
//! Rust should mirror that behavior: seed genesis `Db.namespace_codes` with this
//! baseline so query/transaction code can reliably encode standard IRIs even
//! before any index exists.
use fluree_vocab::namespaces::{
    BLANK_NODE, DID_KEY, EMPTY, FLUREE_COMMIT, FLUREE_DB, FLUREE_URN, JSON_LD, OGC_GEO, OWL, RDF,
    RDFS, SHACL, XSD,
};
use fluree_vocab::predicates::*;
use std::collections::HashMap;

use crate::sid::Sid;

/// Check if a SID is rdf:type
#[inline]
pub fn is_rdf_type(sid: &Sid) -> bool {
    sid.namespace_code == RDF && sid.name.as_ref() == RDF_TYPE
}

/// Check if a SID is rdf:first
#[inline]
pub fn is_rdf_first(sid: &Sid) -> bool {
    sid.namespace_code == RDF && sid.name.as_ref() == RDF_FIRST
}

/// Check if a SID is rdf:rest
#[inline]
pub fn is_rdf_rest(sid: &Sid) -> bool {
    sid.namespace_code == RDF && sid.name.as_ref() == RDF_REST
}

/// Check if a SID is rdf:nil
#[inline]
pub fn is_rdf_nil(sid: &Sid) -> bool {
    sid.namespace_code == RDF && sid.name.as_ref() == RDF_NIL
}

/// Check if a SID is rdfs:subClassOf
#[inline]
pub fn is_rdfs_subclass_of(sid: &Sid) -> bool {
    sid.namespace_code == RDFS && sid.name.as_ref() == RDFS_SUBCLASSOF
}

/// Check if a SID is rdfs:subPropertyOf
#[inline]
pub fn is_rdfs_subproperty_of(sid: &Sid) -> bool {
    sid.namespace_code == RDFS && sid.name.as_ref() == RDFS_SUBPROPERTYOF
}

/// Check if a SID is rdfs:domain
#[inline]
pub fn is_rdfs_domain(sid: &Sid) -> bool {
    sid.namespace_code == RDFS && sid.name.as_ref() == RDFS_DOMAIN
}

/// Check if a SID is rdfs:range
#[inline]
pub fn is_rdfs_range(sid: &Sid) -> bool {
    sid.namespace_code == RDFS && sid.name.as_ref() == RDFS_RANGE
}

/// Check if a SID is owl:inverseOf
#[inline]
pub fn is_owl_inverse_of(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_INVERSEOF
}

/// Check if a SID is owl:equivalentClass
#[inline]
pub fn is_owl_equivalent_class(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_EQUIVALENTCLASS
}

/// Check if a SID is owl:equivalentProperty
#[inline]
pub fn is_owl_equivalent_property(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_EQUIVALENTPROPERTY
}

/// Check if a SID is owl:sameAs
#[inline]
pub fn is_owl_same_as(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_SAMEAS
}

/// Check if a SID is owl:SymmetricProperty
#[inline]
pub fn is_owl_symmetric_property(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_SYMMETRICPROPERTY
}

/// Check if a SID is owl:TransitiveProperty
#[inline]
pub fn is_owl_transitive_property(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_TRANSITIVEPROPERTY
}

/// Check if a SID is owl:FunctionalProperty
#[inline]
pub fn is_owl_functional_property(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_FUNCTIONALPROPERTY
}

/// Check if a SID is owl:InverseFunctionalProperty
#[inline]
pub fn is_owl_inverse_functional_property(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_INVERSEFUNCTIONALPROPERTY
}

/// Check if a SID is owl:imports
#[inline]
pub fn is_owl_imports(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_IMPORTS
}

/// Check if a SID is owl:Ontology (the class)
#[inline]
pub fn is_owl_ontology_class(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_ONTOLOGY
}

/// Check if a SID is owl:Class
#[inline]
pub fn is_owl_class_class(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_CLASS
}

/// Check if a SID is owl:ObjectProperty
#[inline]
pub fn is_owl_object_property_class(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_OBJECTPROPERTY
}

/// Check if a SID is owl:DatatypeProperty
#[inline]
pub fn is_owl_datatype_property_class(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_DATATYPEPROPERTY
}

/// Check if a SID is rdf:Property
#[inline]
pub fn is_rdf_property_class(sid: &Sid) -> bool {
    sid.namespace_code == RDF && sid.name.as_ref() == RDF_PROPERTY
}

/// Check whether `pred` is a schema-describing predicate that may be projected
/// from an imported ontology graph into the schema bundle overlay.
///
/// The set is deliberately narrow: entailment-relevant RDFS/OWL predicates only.
/// Instance-data predicates are excluded so that importing an ontology graph
/// cannot leak its non-schema triples into query results.
#[inline]
pub fn is_schema_predicate(pred: &Sid) -> bool {
    is_rdfs_subclass_of(pred)
        || is_rdfs_subproperty_of(pred)
        || is_rdfs_domain(pred)
        || is_rdfs_range(pred)
        || is_owl_inverse_of(pred)
        || is_owl_equivalent_class(pred)
        || is_owl_equivalent_property(pred)
        || is_owl_same_as(pred)
        || is_owl_imports(pred)
}

/// Check whether `cls` is a schema-describing class — when an `rdf:type <cls>`
/// triple appears in an imported graph it should be projected into the schema
/// bundle. Other `rdf:type` triples (instance typing) are dropped.
#[inline]
pub fn is_schema_class(cls: &Sid) -> bool {
    is_owl_ontology_class(cls)
        || is_owl_class_class(cls)
        || is_owl_object_property_class(cls)
        || is_owl_datatype_property_class(cls)
        || is_owl_symmetric_property(cls)
        || is_owl_transitive_property(cls)
        || is_owl_functional_property(cls)
        || is_owl_inverse_functional_property(cls)
        || is_rdf_property_class(cls)
}

/// Baseline namespace codes (code -> prefix) matching Fluree's reserved codepoints.
pub fn default_namespace_codes() -> HashMap<u16, String> {
    let mut map = HashMap::new();
    map.insert(EMPTY, String::new());
    map.insert(JSON_LD, "@".to_string());
    map.insert(XSD, fluree_vocab::xsd::NS.to_string());
    map.insert(RDF, fluree_vocab::rdf::NS.to_string());
    map.insert(RDFS, fluree_vocab::rdfs::NS.to_string());
    map.insert(SHACL, fluree_vocab::shacl::NS.to_string());
    map.insert(OWL, fluree_vocab::owl::NS.to_string());
    map.insert(FLUREE_DB, fluree_vocab::fluree::DB.to_string());
    map.insert(DID_KEY, "did:key:".to_string());
    map.insert(FLUREE_COMMIT, fluree_vocab::fluree::COMMIT.to_string());
    map.insert(BLANK_NODE, "_:".to_string());
    map.insert(OGC_GEO, fluree_vocab::geo::NS.to_string());
    map.insert(FLUREE_URN, fluree_vocab::fluree::URN.to_string());
    map
}
