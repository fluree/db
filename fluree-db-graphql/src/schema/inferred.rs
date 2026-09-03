//! Tier 1: a schema from HEAD statistics alone.
//!
//! Input is what the ledger has actually observed — classes, the properties seen
//! on their instances, the datatypes and target classes of those properties — with
//! SIDs already resolved to IRIs and policy pruning already applied by the caller.
//!
//! The tier is deliberately lossy, in three specific ways:
//!
//! * **Every field is a nullable list.** Statistics can say a property has never
//!   been seen twice on one subject; they cannot say it never will be. Promoting
//!   that observation to `T` instead of `[T]` would break every client the first
//!   time someone writes a second value. Cardinality is what tier 2 is for.
//! * **No interfaces.** An RDF class with subclasses is usually still
//!   instantiable, so it would have to be both an interface and an object type,
//!   and one of the two would need an invented name. A reference whose targets
//!   span several classes becomes a union instead, which needs no such convention.
//!   Interfaces arrive in tier 2/3, where `graphql:isInterface` says which classes
//!   are abstract.
//! * **No reverse fields.** Statistics record a reference from the subject's side;
//!   naming the other direction is guesswork. `sh:inversePath` is the explicit route.

use fluree_db_core::ValueTypeTag;

use crate::naming::Namer;
use crate::schema::model::SchemaModel;

/// The fallback object type for a reference with no known target class.
pub const NODE_TYPE: &str = "Node";

/// One class and the properties observed on its instances.
#[derive(Debug, Clone)]
pub struct ClassObservation {
    pub iri: String,
    /// Instance count. Classes with no instances are skipped.
    pub count: u64,
    pub properties: Vec<PropertyObservation>,
}

/// One property as seen on instances of the owning class.
#[derive(Debug, Clone)]
pub struct PropertyObservation {
    pub iri: String,
    /// Value-type tags observed, `JSON_LD_ID` included where values are references.
    pub datatypes: Vec<ValueTypeTag>,
    /// Whether any value carried a language tag.
    pub has_language_tags: bool,
    /// Target class IRIs for reference values.
    pub ref_classes: Vec<String>,
}

impl PropertyObservation {
    /// Values include references (`@id`), so the property is an edge.
    pub fn is_reference(&self) -> bool {
        self.datatypes.contains(&ValueTypeTag::JSON_LD_ID)
    }

    /// Values include literals, so the property is not purely an edge.
    pub fn has_literals(&self) -> bool {
        self.datatypes
            .iter()
            .any(|t| *t != ValueTypeTag::JSON_LD_ID)
    }
}

/// Build the inferred schema: statistics alone, no shapes.
///
/// `namer` supplies the ledger's `@context` and `@vocab` so IRIs shorten to the
/// names a user of this ledger already writes.
pub fn build(classes: &[ClassObservation], namer: &Namer) -> SchemaModel {
    crate::schema::build::build(classes, &[], namer)
}
