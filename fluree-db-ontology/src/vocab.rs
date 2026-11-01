//! The IRIs the input policy reads.

pub const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
pub const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
pub const OWL: &str = "http://www.w3.org/2002/07/owl#";
pub const SKOS: &str = "http://www.w3.org/2004/02/skos/core#";
pub const SH: &str = "http://www.w3.org/ns/shacl#";
pub const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
pub const SCHEMA: &str = "https://schema.org/";
pub const SCHEMA_HTTP: &str = "http://schema.org/";
pub const NIF: &str = "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#";
pub const DOC: &str = "https://ns.flur.ee/doc#";
/// Graph roles of an extraction request: `xt:document`, `xt:ontology`,
/// `xt:instances` (the catalog) and `xt:request`.
pub const XT: &str = "https://ns.flur.ee/extract#";

pub const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
pub const RDF_PROPERTY: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
pub const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
pub const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
pub const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
pub const RDFS_CLASS: &str = "http://www.w3.org/2000/01/rdf-schema#Class";
pub const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
pub const RDFS_COMMENT: &str = "http://www.w3.org/2000/01/rdf-schema#comment";
pub const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
pub const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
pub const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
pub const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
pub const OWL_OBJECT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#ObjectProperty";
pub const OWL_DATATYPE_PROPERTY: &str = "http://www.w3.org/2002/07/owl#DatatypeProperty";
pub const OWL_ONTOLOGY: &str = "http://www.w3.org/2002/07/owl#Ontology";
pub const SKOS_PREF_LABEL: &str = "http://www.w3.org/2004/02/skos/core#prefLabel";
pub const SKOS_ALT_LABEL: &str = "http://www.w3.org/2004/02/skos/core#altLabel";
pub const SKOS_HIDDEN_LABEL: &str = "http://www.w3.org/2004/02/skos/core#hiddenLabel";
pub const SKOS_DEFINITION: &str = "http://www.w3.org/2004/02/skos/core#definition";
pub const SH_NODE_SHAPE: &str = "http://www.w3.org/ns/shacl#NodeShape";
pub const SH_PROPERTY_SHAPE: &str = "http://www.w3.org/ns/shacl#PropertyShape";
pub const NIF_IS_STRING: &str =
    "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#isString";
pub const DOC_SOURCE_DOCUMENT: &str = "https://ns.flur.ee/doc#sourceDocument";
pub const XT_ONTOLOGY: &str = "https://ns.flur.ee/extract#ontology";
pub const XT_INSTANCES: &str = "https://ns.flur.ee/extract#instances";
pub const XT_DOCUMENT: &str = "https://ns.flur.ee/extract#document";
pub const XT_REQUEST: &str = "https://ns.flur.ee/extract#request";
pub const XT_REQUEST_CLASS: &str = "https://ns.flur.ee/extract#Request";
pub const XT_WINDOW: &str = "https://ns.flur.ee/extract#window";

/// Declarations that make a subject part of the ontology rather than the
/// catalog, when a flat graph (Turtle) is split.
pub const SCHEMA_TYPES: &[&str] = &[
    RDFS_CLASS,
    RDF_PROPERTY,
    OWL_CLASS,
    OWL_OBJECT_PROPERTY,
    OWL_DATATYPE_PROPERTY,
    OWL_ONTOLOGY,
];

/// Prefixes used to abbreviate predicates in report keys (`rdfs:domain`), in
/// the order the policy tries them.
pub const REPORT_PREFIXES: &[(&str, &str)] = &[
    ("rdfs:", RDFS),
    ("rdf:", RDF),
    ("owl:", OWL),
    ("skos:", SKOS),
    ("sh:", SH),
];

/// Prefixes always available for writing and for resolving `--seed` names.
pub const WELL_KNOWN_PREFIXES: &[(&str, &str)] = &[
    ("rdf", RDF),
    ("rdfs", RDFS),
    ("owl", OWL),
    ("skos", SKOS),
    ("sh", SH),
    ("xsd", XSD),
];

/// An IRI shortened with [`REPORT_PREFIXES`], or as it is.
pub fn short(iri: &str) -> String {
    REPORT_PREFIXES
        .iter()
        .find_map(|(prefix, ns)| iri.strip_prefix(ns).map(|local| format!("{prefix}{local}")))
        .unwrap_or_else(|| iri.to_string())
}
