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
use fluree_vocab::db as fluree_db_predicates;
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

// ============================================================================
// Edge-annotation system predicates (durable attachment encoding)
// ============================================================================
//
// Helpers for the seven `https://ns.flur.ee/db#reifies*` predicates that
// encode an annotation's reified edge. These are **system-controlled** —
// user transactions must never assert or retract them directly. The
// predicates are emitted only by the internal `@annotation` / `@reifies`
// lowering path. See `EDGE_ANNOTATIONS_IMPL_PLAN.md` M1.

/// True for `f:reifiesGraph` — the named graph of the reified edge.
#[inline]
pub fn is_reifies_graph(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_DB && sid.name.as_ref() == fluree_db_predicates::REIFIES_GRAPH
}

/// True for `f:reifiesSubject`.
#[inline]
pub fn is_reifies_subject(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_DB && sid.name.as_ref() == fluree_db_predicates::REIFIES_SUBJECT
}

/// True for `f:reifiesPredicate`.
#[inline]
pub fn is_reifies_predicate(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_DB && sid.name.as_ref() == fluree_db_predicates::REIFIES_PREDICATE
}

/// True for `f:reifiesObject`.
#[inline]
pub fn is_reifies_object(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_DB && sid.name.as_ref() == fluree_db_predicates::REIFIES_OBJECT
}

/// True for `f:reifiesDatatype`.
#[inline]
pub fn is_reifies_datatype(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_DB && sid.name.as_ref() == fluree_db_predicates::REIFIES_DATATYPE
}

/// True for `f:reifiesLang`.
#[inline]
pub fn is_reifies_lang(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_DB && sid.name.as_ref() == fluree_db_predicates::REIFIES_LANG
}

/// True for `f:reifiesListIndex`.
#[inline]
pub fn is_reifies_list_index(sid: &Sid) -> bool {
    sid.namespace_code == FLUREE_DB && sid.name.as_ref() == fluree_db_predicates::REIFIES_LIST_INDEX
}

/// True if `sid` is **any** of the seven `f:reifies*` predicates.
///
/// This is the canonical reserved-predicate firewall check used by every
/// write surface (parse, lower_sparql_update, turtle ingest, import,
/// raw_txn_upload, flake_sink) and by the read-side system-fact filter.
/// Implemented as a single namespace-code check followed by a name
/// dispatch — costs an integer compare plus one short string compare.
#[inline]
pub fn is_reserved_reifies_predicate(sid: &Sid) -> bool {
    if sid.namespace_code != FLUREE_DB {
        return false;
    }
    matches!(
        sid.name.as_ref(),
        fluree_db_predicates::REIFIES_GRAPH
            | fluree_db_predicates::REIFIES_SUBJECT
            | fluree_db_predicates::REIFIES_PREDICATE
            | fluree_db_predicates::REIFIES_OBJECT
            | fluree_db_predicates::REIFIES_DATATYPE
            | fluree_db_predicates::REIFIES_LANG
            | fluree_db_predicates::REIFIES_LIST_INDEX
    )
}

/// Construct the seven canonical `f:reifies*` predicate SIDs.
///
/// Returned in the order `[Graph, Subject, Predicate, Object, Datatype,
/// Lang, ListIndex]`. Callers that only need a subset should use the
/// individual `is_reifies_*` helpers above; this is for the staging
/// path and tests that need to emit the full bundle.
pub fn reifies_predicate_sids() -> [Sid; 7] {
    [
        Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_GRAPH),
        Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_SUBJECT),
        Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_PREDICATE),
        Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_OBJECT),
        Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_DATATYPE),
        Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_LANG),
        Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_LIST_INDEX),
    ]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reifies_predicate_set_is_complete_and_disjoint() {
        let sids = reifies_predicate_sids();
        // Every canonical SID must pass the firewall check.
        for sid in &sids {
            assert!(
                is_reserved_reifies_predicate(sid),
                "{sid:?} should be in the reserved set"
            );
        }
        // The seven SIDs are pairwise distinct.
        for i in 0..sids.len() {
            for j in (i + 1)..sids.len() {
                assert_ne!(sids[i], sids[j], "duplicate SID at indices {i}/{j}");
            }
        }
        // Non-Fluree-DB SIDs and unrelated FLUREE_DB names are not reserved.
        assert!(!is_reserved_reifies_predicate(&Sid::new(RDF, "type")));
        assert!(!is_reserved_reifies_predicate(&Sid::new(FLUREE_DB, "alias")));
        assert!(!is_reserved_reifies_predicate(&Sid::new(FLUREE_DB, "t")));
        // Defensive: a name that *prefix-matches* "reifies" but is not one
        // of the seven must not slip through.
        assert!(!is_reserved_reifies_predicate(&Sid::new(
            FLUREE_DB,
            "reifies"
        )));
        assert!(!is_reserved_reifies_predicate(&Sid::new(
            FLUREE_DB,
            "reifiesSomethingElse"
        )));
    }

    #[test]
    fn per_predicate_helpers_dispatch_correctly() {
        let g = Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_GRAPH);
        let s = Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_SUBJECT);
        let p = Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_PREDICATE);
        let o = Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_OBJECT);
        let dt = Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_DATATYPE);
        let lang = Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_LANG);
        let li = Sid::new(FLUREE_DB, fluree_db_predicates::REIFIES_LIST_INDEX);

        assert!(is_reifies_graph(&g) && !is_reifies_graph(&s));
        assert!(is_reifies_subject(&s) && !is_reifies_subject(&p));
        assert!(is_reifies_predicate(&p) && !is_reifies_predicate(&o));
        assert!(is_reifies_object(&o) && !is_reifies_object(&dt));
        assert!(is_reifies_datatype(&dt) && !is_reifies_datatype(&lang));
        assert!(is_reifies_lang(&lang) && !is_reifies_lang(&li));
        assert!(is_reifies_list_index(&li) && !is_reifies_list_index(&g));
    }
}
