//! Tier 3 input: a `graphql:Schema` instance.
//!
//! The presence of one changes the question the builder answers. Tiers 1 and 2
//! describe everything the ledger holds; tier 3 describes what someone chose to
//! publish, so an unlisted class is absent rather than inferred. That is the
//! whole point: a curated schema is an API contract, and a contract that grows a
//! type the moment someone writes an instance is not one.

use std::collections::BTreeMap;

/// How much of a class a curated schema exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Exposure {
    /// A type, plus root query fields.
    Public,
    /// A type, reachable only by following a reference. No root fields — a
    /// caller can read one through its parent but cannot enumerate them.
    Protected,
    /// Not exposed. References to it degrade to `Node`, so the edge is still
    /// visible as an IRI without naming a type the caller cannot query.
    Private,
}

/// A `graphql:Schema` instance.
#[derive(Debug, Clone, Default)]
pub struct CuratedSchema {
    /// `graphql:name` — which schema this is, when a ledger defines several.
    pub name: Option<String>,
    /// Class IRI → exposure. A class absent from this map is not in the schema.
    pub exposure: BTreeMap<String, Exposure>,
    /// Class IRIs marked `graphql:isInterface`: abstract, so they become an
    /// interface implemented by the classes below them.
    pub interfaces: Vec<String>,
    /// Interface class IRI → the class IRIs beneath it in the RDFS hierarchy.
    ///
    /// Filled by the caller, because the hierarchy is data rather than
    /// something a shape declares — and it is stored as descendants, which is
    /// the direction this question needs.
    pub interface_members: BTreeMap<String, Vec<String>>,
    /// `f:graphqlEnableMutations`.
    pub mutations: bool,
    /// `f:graphqlIriBase` — where mutations mint new subjects.
    pub iri_base: Option<String>,
    /// Class IRI → `f:graphqlPluralName`.
    pub plural_names: BTreeMap<String, String>,
    /// Class IRI → `graphql:name`, overriding both the derived name and
    /// `sh:name`.
    pub type_names: BTreeMap<String, String>,
}

impl CuratedSchema {
    /// The exposure of a class, or `Private` when the schema does not list it.
    ///
    /// Absent and private differ in intent but not in effect, and collapsing
    /// them here is what makes "only what is listed" the default.
    pub fn exposure_of(&self, class_iri: &str) -> Exposure {
        self.exposure
            .get(class_iri)
            .copied()
            .unwrap_or(Exposure::Private)
    }

    /// Whether the class becomes a type at all.
    pub fn exposes(&self, class_iri: &str) -> bool {
        self.exposure_of(class_iri) != Exposure::Private
    }

    /// Whether the class gets root query fields.
    pub fn is_queryable(&self, class_iri: &str) -> bool {
        self.exposure_of(class_iri) == Exposure::Public
    }

    pub fn is_interface(&self, class_iri: &str) -> bool {
        self.interfaces.iter().any(|i| i == class_iri)
    }

    /// Whether `class_iri` is beneath the interface `interface_iri`.
    pub fn implements(&self, class_iri: &str, interface_iri: &str) -> bool {
        self.interface_members
            .get(interface_iri)
            .is_some_and(|members| members.iter().any(|m| m == class_iri))
    }

    /// The `graphql:name` declared for a class, if any.
    pub fn type_name_override(&self, class_iri: &str) -> Option<&str> {
        self.type_names.get(class_iri).map(String::as_str)
    }
}
