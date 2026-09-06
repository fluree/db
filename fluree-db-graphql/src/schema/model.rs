//! Language-neutral schema model.
//!
//! All three tiers — inferred (stats), shaped (SHACL), curated (`graphql:Schema`) —
//! produce a [`SchemaModel`]. SDL, introspection, and query lowering read only from
//! this model, so a tier can never express something the other consumers cannot see.

use std::collections::HashMap;

/// Which tier put an element in the model. Surfaced by the `explain` extension so a
/// user can tell an inferred guess from something their shapes actually declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Provenance {
    /// Derived from HEAD statistics alone.
    Inferred,
    /// Derived from a `sh:NodeShape`.
    Shaped,
    /// Declared by a `graphql:Schema` instance.
    Curated,
}

/// GraphQL scalars this mapping can produce.
///
/// Everything past the five built-ins is a custom scalar, registered only when
/// some field references it. Variant order is significant: [`reduce_scalars`]
/// sorts by it, so `Int` must precede `Long` for the widening rule.
///
/// [`reduce_scalars`]: crate::schema::datatype::reduce_scalars
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Scalar {
    String,
    Int,
    /// 64-bit and unbounded integers, which `Int` (32-bit signed) would overflow.
    Long,
    Float,
    /// Arbitrary-precision decimal, carried as its lexical form.
    Decimal,
    Boolean,
    Id,
    DateTime,
    Date,
    Time,
    Json,
}

impl Scalar {
    /// The GraphQL type name.
    pub fn type_name(self) -> &'static str {
        match self {
            Scalar::String => "String",
            Scalar::Int => "Int",
            Scalar::Long => "Long",
            Scalar::Float => "Float",
            Scalar::Decimal => "Decimal",
            Scalar::Boolean => "Boolean",
            Scalar::Id => "ID",
            Scalar::DateTime => "DateTime",
            Scalar::Date => "Date",
            Scalar::Time => "Time",
            Scalar::Json => "JSON",
        }
    }

    /// Custom scalars need an explicit registration; the five built-ins do not.
    pub fn is_custom(self) -> bool {
        !matches!(
            self,
            Scalar::String | Scalar::Int | Scalar::Float | Scalar::Boolean | Scalar::Id
        )
    }

    /// Ordering-comparable scalars get `LT`/`LTE`/`GT`/`GTE` filter operators.
    pub fn is_ordered(self) -> bool {
        !matches!(self, Scalar::Boolean | Scalar::Id | Scalar::Json)
    }

    /// Every scalar, so callers can check the mapping's own names are reserved.
    pub const ALL: &'static [Scalar] = &[
        Scalar::String,
        Scalar::Int,
        Scalar::Long,
        Scalar::Float,
        Scalar::Decimal,
        Scalar::Boolean,
        Scalar::Id,
        Scalar::DateTime,
        Scalar::Date,
        Scalar::Time,
        Scalar::Json,
    ];

    /// Text-like scalars get the regex operators.
    pub fn is_textual(self) -> bool {
        matches!(self, Scalar::String)
    }
}

/// What a field resolves to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    /// The synthetic `id` field: the subject IRI itself, not a predicate.
    Id,
    Scalar(Scalar),
    Enum(String),
    Object(String),
    Interface(String),
    Union(String),
}

impl FieldType {
    /// The GraphQL type name this field refers to.
    pub fn type_name(&self) -> &str {
        match self {
            FieldType::Id => "ID",
            FieldType::Scalar(s) => s.type_name(),
            FieldType::Enum(n)
            | FieldType::Object(n)
            | FieldType::Interface(n)
            | FieldType::Union(n) => n,
        }
    }

    /// Composite fields have a sub-selection; leaf fields do not.
    pub fn is_composite(&self) -> bool {
        matches!(
            self,
            FieldType::Object(_) | FieldType::Interface(_) | FieldType::Union(_)
        )
    }

    /// Abstract positions need the concrete type name at resolve time.
    pub fn is_abstract(&self) -> bool {
        matches!(self, FieldType::Interface(_) | FieldType::Union(_))
    }
}

/// Whether a field reads the predicate forwards (subject→object) or backwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Direction {
    #[default]
    Forward,
    Reverse,
}

/// One field on an object or interface type.
#[derive(Debug, Clone)]
pub struct Field {
    /// GraphQL field name (already collision-resolved).
    pub name: String,
    /// Predicate IRI. Empty for [`FieldType::Id`], which is not backed by a predicate.
    pub iri: String,
    pub direction: Direction,
    pub ty: FieldType,
    /// `[T]` rather than `T`.
    pub list: bool,
    /// Trailing `!`. On a list this marks the list itself non-null.
    pub non_null: bool,
    pub description: Option<String>,
    /// The values carry language tags, so the field accepts a `lang` argument
    /// selecting among them.
    pub language_tagged: bool,
    pub provenance: Provenance,
}

impl Field {
    /// The synthetic `id: ID!` every type carries.
    pub fn id_field(provenance: Provenance) -> Self {
        Field {
            name: "id".to_string(),
            iri: String::new(),
            direction: Direction::Forward,
            ty: FieldType::Id,
            list: false,
            non_null: true,
            description: Some("The subject IRI.".to_string()),
            language_tagged: false,
            provenance,
        }
    }

    /// True for the synthetic identity field.
    pub fn is_id(&self) -> bool {
        matches!(self.ty, FieldType::Id)
    }
}

/// A concrete GraphQL object type, backed by one class IRI.
#[derive(Debug, Clone)]
pub struct ObjectType {
    pub name: String,
    /// Class IRI. Empty for built-ins such as `Node`.
    pub iri: String,
    pub description: Option<String>,
    /// Interface type names this object implements.
    pub implements: Vec<String>,
    pub fields: Vec<Field>,
    pub provenance: Provenance,
}

/// An interface type: a class that has subclasses, or one flagged `graphql:isInterface`.
#[derive(Debug, Clone)]
pub struct InterfaceType {
    pub name: String,
    pub iri: String,
    pub description: Option<String>,
    /// Interfaces this interface itself implements (superclass chain).
    pub implements: Vec<String>,
    pub fields: Vec<Field>,
    pub provenance: Provenance,
}

/// A union of object types, from a property observed pointing at several classes.
#[derive(Debug, Clone)]
pub struct UnionType {
    pub name: String,
    pub description: Option<String>,
    /// Member object type names.
    pub members: Vec<String>,
    pub provenance: Provenance,
}

/// An enum, from `sh:in` over a homogeneous value list.
#[derive(Debug, Clone)]
pub struct EnumType {
    pub name: String,
    pub description: Option<String>,
    /// `(graphql name, underlying value)` — an IRI or a lexical form.
    pub values: Vec<(String, String)>,
    /// Whether the underlying values are IRIs. Decides both how a filter names
    /// them (a `values` pattern rather than a literal comparison) and how a
    /// result is read back.
    pub iri_valued: bool,
    pub provenance: Provenance,
}

impl EnumType {
    /// The GraphQL name for an underlying value.
    pub fn name_for(&self, value: &str) -> Option<&str> {
        self.values
            .iter()
            .find(|(_, v)| v == value)
            .map(|(n, _)| n.as_str())
    }

    /// The underlying value for a GraphQL name.
    pub fn value_for(&self, name: &str) -> Option<&str> {
        self.values
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

/// What a root query field does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootKind {
    /// `person(id: ID!): Person` — one subject by IRI.
    Single,
    /// `persons(where:, orderBy:, limit:, offset:): [Person]`
    List,
    /// `persons_count(where:): Int`
    Count,
}

/// A field on the root `Query` type.
#[derive(Debug, Clone)]
pub struct RootField {
    pub name: String,
    /// Class IRI this root field selects instances of.
    pub class_iri: String,
    /// The object/interface type name it yields.
    pub type_name: String,
    pub kind: RootKind,
    pub description: Option<String>,
    pub provenance: Provenance,
}

/// The complete schema for one ledger (and one `graphql:Schema`, in tier 3).
#[derive(Debug, Clone, Default)]
pub struct SchemaModel {
    pub objects: Vec<ObjectType>,
    pub interfaces: Vec<InterfaceType>,
    pub unions: Vec<UnionType>,
    pub enums: Vec<EnumType>,
    pub query_fields: Vec<RootField>,
    /// Things a builder had to drop or approximate. Surfaced via `explain`, never fatal.
    pub warnings: Vec<String>,
}

impl SchemaModel {
    pub fn object(&self, name: &str) -> Option<&ObjectType> {
        self.objects.iter().find(|o| o.name == name)
    }

    pub fn interface(&self, name: &str) -> Option<&InterfaceType> {
        self.interfaces.iter().find(|i| i.name == name)
    }

    pub fn union(&self, name: &str) -> Option<&UnionType> {
        self.unions.iter().find(|u| u.name == name)
    }

    pub fn enum_type(&self, name: &str) -> Option<&EnumType> {
        self.enums.iter().find(|e| e.name == name)
    }

    /// Fields of an object or interface by type name.
    pub fn fields_of(&self, type_name: &str) -> Option<&[Field]> {
        self.object(type_name)
            .map(|o| o.fields.as_slice())
            .or_else(|| self.interface(type_name).map(|i| i.fields.as_slice()))
    }

    /// The concrete object types a selection on `type_name` can land on. For a
    /// concrete object that is just itself; for an interface or union it is the
    /// members. Lowering uses this to resolve a fragment's type condition.
    pub fn possible_types(&self, type_name: &str) -> Vec<&ObjectType> {
        if let Some(o) = self.object(type_name) {
            return vec![o];
        }
        if let Some(u) = self.union(type_name) {
            return u.members.iter().filter_map(|m| self.object(m)).collect();
        }
        if self.interface(type_name).is_some() {
            return self
                .objects
                .iter()
                .filter(|o| o.implements.iter().any(|i| i == type_name))
                .collect();
        }
        Vec::new()
    }

    /// Class IRI → GraphQL type name, for both objects and interfaces.
    pub fn type_name_by_iri(&self) -> HashMap<&str, &str> {
        let mut map = HashMap::new();
        for o in &self.objects {
            if !o.iri.is_empty() {
                map.insert(o.iri.as_str(), o.name.as_str());
            }
        }
        for i in &self.interfaces {
            if !i.iri.is_empty() {
                map.insert(i.iri.as_str(), i.name.as_str());
            }
        }
        map
    }

    /// Put every collection in a deterministic order so SDL and golden tests are stable.
    pub fn sort(&mut self) {
        self.objects.sort_by(|a, b| a.name.cmp(&b.name));
        self.interfaces.sort_by(|a, b| a.name.cmp(&b.name));
        self.unions.sort_by(|a, b| a.name.cmp(&b.name));
        self.enums.sort_by(|a, b| a.name.cmp(&b.name));
        self.query_fields.sort_by(|a, b| a.name.cmp(&b.name));
        for o in &mut self.objects {
            o.implements.sort();
        }
        for u in &mut self.unions {
            u.members.sort();
        }
    }
}
