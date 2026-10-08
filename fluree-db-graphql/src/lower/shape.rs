//! How to reshape a JSON-LD query result into a GraphQL response.
//!
//! The lowered query is written with `"@context": {}`, so its result is keyed by
//! full IRIs and carries no aliases. The shape tree records what the document
//! actually asked for — response keys, per-type fragment selections, `__typename`
//! — and the executor walks the two together.
//!
//! Fragments are why this cannot be a flat key map: under a union,
//! `... on Person { name }` and `... on Organization { name }` share the response
//! key `name` but read different predicates, so the selection has to be resolved
//! against each node's own `rdf:type`.

use crate::schema::model::RootKind;

/// Where one response key's value comes from.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldSource {
    /// The subject IRI.
    Id,
    /// The concrete GraphQL type name, from the node's `rdf:type`.
    Typename,
    /// The key the value arrives under: a predicate IRI, or `@reverse:<iri>`
    /// for a field read backwards.
    Property(String),
}

/// One entry in the GraphQL response object.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldShape {
    /// The key this value appears under in the response.
    pub response_key: String,
    pub source: FieldSource,
    /// Render as a JSON array. Scalars and objects alike.
    pub list: bool,
    /// Present for object-valued fields.
    pub child: Option<NodeShape>,
    /// The enum this field's values belong to. Values arrive as the underlying
    /// IRI or literal and have to be rendered as the member's name.
    pub enum_type: Option<String>,
    /// Which language-tagged values to keep, when the caller asked.
    pub language: Option<LanguageSpec>,
}

/// Which of a language-tagged property's values to return.
#[derive(Debug, Clone, PartialEq)]
pub enum LanguageSpec {
    /// Every value, whatever its tag — the explicit form of the default.
    Any,
    /// The first of these languages that has any value; nothing if none does.
    /// Preference order, not a filter — `"en,fr"` means English if there is
    /// English, else French.
    Preferred(Vec<String>),
}

impl LanguageSpec {
    /// Parse the spec string a `lang:` argument carries.
    pub fn parse(spec: &str) -> LanguageSpec {
        let spec = spec.trim();
        if spec == "*" || spec.eq_ignore_ascii_case("all") {
            return LanguageSpec::Any;
        }
        LanguageSpec::Preferred(
            spec.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_ascii_lowercase)
                .collect(),
        )
    }
}

/// The response object for one node.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NodeShape {
    /// The GraphQL type of this position — concrete, an interface, or a union.
    /// Answers `__typename` when the subject carries no usable `rdf:type`.
    pub type_name: String,
    /// Fields selected directly on the node's static type.
    pub common: Vec<FieldShape>,
    /// Fields selected under a fragment, applied only when the node's
    /// `rdf:type` includes the condition's class.
    pub conditional: Vec<ConditionalShape>,
}

/// A fragment's selections, gated on the node's concrete type.
#[derive(Debug, Clone, PartialEq)]
pub struct ConditionalShape {
    /// GraphQL type name the fragment is conditioned on.
    pub type_name: String,
    /// Class IRI to test the node's `rdf:type` against.
    pub class_iri: String,
    pub fields: Vec<FieldShape>,
}

impl NodeShape {
    /// True when resolving this node needs its `rdf:type`, either to pick a
    /// fragment or to answer `__typename`.
    pub fn needs_type(&self) -> bool {
        !self.conditional.is_empty()
            || self
                .common
                .iter()
                .any(|f| f.source == FieldSource::Typename)
    }

    /// Every field that could apply to this node, fragments included.
    pub fn all_fields(&self) -> impl Iterator<Item = &FieldShape> {
        self.common
            .iter()
            .chain(self.conditional.iter().flat_map(|c| c.fields.iter()))
    }
}

/// The result shape for one root field.
#[derive(Debug, Clone, PartialEq)]
pub struct RootShape {
    pub kind: RootKind,
    /// Unused for [`RootKind::Count`].
    pub node: NodeShape,
}
