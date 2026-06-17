//! Schema flake detection helpers
//!
//! Schema flakes must be allowed regardless of policy for query planning and
//! formatting to work correctly.

use fluree_db_core::{
    is_rdf_type, is_rdfs_domain, is_rdfs_range, is_rdfs_subclass_of, is_rdfs_subproperty_of,
    FlakeValue, Sid,
};
use fluree_vocab::namespaces::{OWL, RDF, RDFS};

/// Well-known schema type names
const RDFS_CLASS: &str = "Class";
const OWL_CLASS: &str = "Class";
const RDF_PROPERTY: &str = "Property";
const OWL_OBJECT_PROPERTY: &str = "ObjectProperty";
const OWL_DATATYPE_PROPERTY: &str = "DatatypeProperty";

/// Check if a SID represents rdfs:Class
#[inline]
pub fn is_rdfs_class(sid: &Sid) -> bool {
    sid.namespace_code == RDFS && sid.name.as_ref() == RDFS_CLASS
}

/// Check if a SID represents owl:Class
#[inline]
pub fn is_owl_class(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_CLASS
}

/// Check if a SID represents rdf:Property
#[inline]
pub fn is_rdf_property(sid: &Sid) -> bool {
    // rdf:Property uses RDF namespace
    sid.namespace_code == RDF && sid.name.as_ref() == RDF_PROPERTY
}

/// Check if a SID represents owl:ObjectProperty
#[inline]
pub fn is_owl_object_property(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_OBJECT_PROPERTY
}

/// Check if a SID represents owl:DatatypeProperty
#[inline]
pub fn is_owl_datatype_property(sid: &Sid) -> bool {
    sid.namespace_code == OWL && sid.name.as_ref() == OWL_DATATYPE_PROPERTY
}

/// Check if an object value represents a schema type
fn is_schema_type(object: &FlakeValue) -> bool {
    match object {
        FlakeValue::Ref(sid) => {
            is_rdfs_class(sid)
                || is_owl_class(sid)
                || is_rdf_property(sid)
                || is_owl_object_property(sid)
                || is_owl_datatype_property(sid)
        }
        _ => false,
    }
}

/// Check if a flake is a schema flake that should bypass policy
///
/// Schema flakes must be allowed regardless of policy for query planning
/// and formatting to work correctly. This includes:
/// - `rdfs:subClassOf` assertions
/// - `rdfs:subPropertyOf` assertions
/// - `rdfs:domain` / `rdfs:range` assertions
/// - `rdf:type` assertions where object is a schema class/property type
pub fn is_schema_flake(predicate: &Sid, object: &FlakeValue) -> bool {
    // Schema property predicates
    is_rdfs_subclass_of(predicate)
        || is_rdfs_subproperty_of(predicate)
        || is_rdfs_domain(predicate)
        || is_rdfs_range(predicate)
        // Type assertions for schema classes/properties
        || (is_rdf_type(predicate) && is_schema_type(object))
}

/// Predicate-axis subset of [`is_schema_flake`]: predicates that can make a
/// flake a schema flake irrespective of its object.
///
/// Used by [`PolicySet::covers_predicate`](crate::PolicySet::covers_predicate)
/// so schema predicates are always reported as "covered" — keeping them on the
/// per-flake path where the [`is_schema_flake`] bypass fires, instead of being
/// constant-folded to an empty result under a default-deny policy.
///
/// `rdf:type`'s schema-ness depends on the object (only schema class/property
/// objects qualify), but a query planner sees only the predicate, so `rdf:type`
/// is treated as a schema predicate here. This is the safe over-covering
/// direction: it forgoes the fast-path skip for `rdf:type` scans rather than
/// risk constant-folding a genuine schema type-assertion.
pub fn is_schema_predicate(predicate: &Sid) -> bool {
    is_rdfs_subclass_of(predicate)
        || is_rdfs_subproperty_of(predicate)
        || is_rdfs_domain(predicate)
        || is_rdfs_range(predicate)
        || is_rdf_type(predicate)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    #[test]
    fn test_is_schema_flake_subclass() {
        let rdfs_subclassof = make_sid(RDFS, "subClassOf");
        let some_object = FlakeValue::Ref(make_sid(100, "SomeClass"));

        assert!(is_schema_flake(&rdfs_subclassof, &some_object));
    }

    #[test]
    fn test_is_schema_flake_type_rdfs_class() {
        let rdf_type = make_sid(RDF, "type");
        let rdfs_class = FlakeValue::Ref(make_sid(RDFS, "Class"));

        assert!(is_schema_flake(&rdf_type, &rdfs_class));
    }

    #[test]
    fn test_is_schema_flake_type_owl_object_property() {
        let rdf_type = make_sid(RDF, "type");
        let owl_prop = FlakeValue::Ref(make_sid(OWL, "ObjectProperty"));

        assert!(is_schema_flake(&rdf_type, &owl_prop));
    }

    #[test]
    fn test_not_schema_flake_regular_type() {
        let rdf_type = make_sid(RDF, "type");
        let person = FlakeValue::Ref(make_sid(100, "Person"));

        // Regular type assertion is NOT a schema flake
        assert!(!is_schema_flake(&rdf_type, &person));
    }

    #[test]
    fn test_not_schema_flake_regular_property() {
        let name_prop = make_sid(100, "name");
        let value = FlakeValue::String("Alice".to_string());

        // Regular property assertion is NOT a schema flake
        assert!(!is_schema_flake(&name_prop, &value));
    }

    #[test]
    fn test_is_schema_predicate() {
        // Schema structural predicates (object-independent).
        assert!(is_schema_predicate(&make_sid(RDFS, "subClassOf")));
        assert!(is_schema_predicate(&make_sid(RDFS, "subPropertyOf")));
        assert!(is_schema_predicate(&make_sid(RDFS, "domain")));
        assert!(is_schema_predicate(&make_sid(RDFS, "range")));
        // rdf:type over-covers (schema-ness is object-dependent, but the
        // planner sees only the predicate).
        assert!(is_schema_predicate(&make_sid(RDF, "type")));
        // A regular data property is not a schema predicate.
        assert!(!is_schema_predicate(&make_sid(100, "name")));
    }
}
