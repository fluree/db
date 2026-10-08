//! The schema builder: statistics and shapes into one [`SchemaModel`].
//!
//! Both tiers run in a single pass rather than one overlaying the other, because
//! naming is global. Names are allocated once over the union of classes, so a
//! shaped class and an inferred one cannot both claim `Person` and then have to
//! be reconciled afterwards.
//!
//! Where they overlap, the shape wins: it is what someone wrote down, against a
//! statistic that is only what has happened so far. A shape's own `sh:closed`
//! decides whether observed-but-undeclared properties survive.

use std::collections::BTreeMap;

use crate::naming::{self, NameScope, Namer, RESERVED_FIELD_NAMES, RESERVED_TYPE_NAMES};
use crate::schema::curated::CuratedSchema;
use crate::schema::datatype::{reduce_scalars, scalar_for_datatype_iri, scalar_for_tag};
use crate::schema::inferred::{ClassObservation, PropertyObservation, NODE_TYPE};
use crate::schema::model::{
    Direction, EnumType, Field, FieldType, InterfaceType, ObjectType, Provenance, RootField,
    RootKind, Scalar, SchemaModel, UnionType,
};
use crate::schema::shaped::{AllowedValue, ShapeDescription, ShapedProperty};

/// Build the model from everything the ledger says about itself.
///
/// `shapes` may be empty, which is tier 1.
pub fn build(
    classes: &[ClassObservation],
    shapes: &[ShapeDescription],
    namer: &Namer,
) -> SchemaModel {
    build_curated(classes, shapes, None, namer)
}

/// [`build`], with a `graphql:Schema` deciding what is exposed.
///
/// The curated schema is a filter and an overlay, not a third source of types:
/// it says which of the classes the ledger already describes are published, and
/// how they are named. Everything about a type's *shape* still comes from its
/// SHACL shape and the data.
pub fn build_curated(
    classes: &[ClassObservation],
    shapes: &[ShapeDescription],
    curated: Option<&CuratedSchema>,
    namer: &Namer,
) -> SchemaModel {
    let mut model = SchemaModel::default();

    // A class earns a type by having instances, by being shaped, or both: a
    // shape describes a class the ledger is *meant* to hold, which is worth
    // exposing before the first instance is written.
    let observed: BTreeMap<&str, &ClassObservation> = classes
        .iter()
        .filter(|c| c.count > 0)
        .map(|c| (c.iri.as_str(), c))
        .collect();
    let shaped = merge_shapes(shapes);

    let mut class_iris: Vec<&str> = observed.keys().copied().collect();
    class_iris.extend(shaped.keys().copied());
    // A curated schema can list a class the ledger has neither shaped nor
    // instantiated; that is still a declaration of intent to publish it.
    if let Some(curated) = curated {
        class_iris.extend(curated.exposure.keys().map(String::as_str));
    }
    class_iris.sort_unstable();
    class_iris.dedup();
    // Tier 3 publishes only what it lists. Absent means absent, not inferred.
    if let Some(curated) = curated {
        class_iris.retain(|iri| curated.exposes(iri));
    }

    // Names are allocated in two passes so a name someone *declared* cannot be
    // taken first by one merely derived from an IRI. Only a declaration can
    // lose to another declaration, and then the later one is reported rather
    // than silently resolved into a name its author did not write.
    let mut type_scope = NameScope::new(namer, RESERVED_TYPE_NAMES);
    type_scope.claim("", NODE_TYPE);
    for iri in &class_iris {
        let Some(declared) = curated
            .and_then(|c| c.type_name_override(iri))
            .or_else(|| shaped.get(iri).and_then(|s| s.name.as_deref()))
        else {
            continue;
        };
        let Some(name) = naming::sanitize(declared) else {
            model.warnings.push(format!(
                "`sh:name` \"{declared}\" on the shape for <{iri}> is not a usable GraphQL \
                 name; the name was derived from the IRI instead"
            ));
            continue;
        };
        if !type_scope.claim(iri, &name) {
            model.warnings.push(format!(
                "`sh:name` \"{name}\" on the shape for <{iri}> was already claimed by another \
                 shape; the name was derived from the IRI instead"
            ));
        }
    }
    for iri in &class_iris {
        type_scope.assign(iri);
    }

    let mut unions: BTreeMap<Vec<String>, String> = BTreeMap::new();
    let mut enums: BTreeMap<String, EnumType> = BTreeMap::new();
    let mut needs_node = false;

    let is_interface = |iri: &str| curated.is_some_and(|c| c.is_interface(iri));
    let interface_iris: Vec<&str> = class_iris
        .iter()
        .copied()
        .filter(|iri| is_interface(iri))
        .collect();

    for iri in &class_iris {
        let type_name = type_scope.get(iri).expect("assigned above").to_string();
        let shape = shaped.get(iri).copied();
        let observation = observed.get(iri).copied();

        let mut ctx = FieldCtx {
            type_scope: &type_scope,
            interfaces: &interface_iris,
            owner: &type_name,
            unions: &mut unions,
            enums: &mut enums,
            needs_node: &mut needs_node,
            warnings: &mut model.warnings,
        };
        let fields = build_fields(namer, &mut ctx, shape, observation);

        let provenance = if curated.is_some() {
            Provenance::Curated
        } else if shape.is_some() {
            Provenance::Shaped
        } else {
            Provenance::Inferred
        };
        let description = shape.and_then(|s| s.description.clone());

        if is_interface(iri) {
            // An abstract class contributes an interface, not a type: nothing
            // is an instance of it directly, so a concrete type would only
            // ever be empty.
            model.interfaces.push(InterfaceType {
                name: type_name,
                iri: (*iri).to_string(),
                description,
                implements: Vec::new(),
                fields,
                provenance,
            });
        } else {
            model.objects.push(ObjectType {
                name: type_name,
                iri: (*iri).to_string(),
                description,
                implements: Vec::new(),
                fields,
                provenance,
            });
        }
    }

    // Every exposed class whose shape declares a superclass interface
    // implements it. Membership is by subclass edge, which the shape layer
    // records as the interface's own class IRI on the subject shape.
    if let Some(curated) = curated {
        link_interfaces(&mut model, curated);
    }

    for (members, name) in unions {
        model.unions.push(UnionType {
            name,
            description: None,
            members,
            provenance: Provenance::Inferred,
        });
    }
    model.enums = enums.into_values().collect();

    if needs_node {
        model.objects.push(ObjectType {
            name: NODE_TYPE.to_string(),
            iri: String::new(),
            description: Some(
                "A referenced subject whose type this schema does not expose.".to_string(),
            ),
            implements: Vec::new(),
            fields: vec![Field::id_field(Provenance::Inferred)],
            provenance: Provenance::Inferred,
        });
    }

    model.query_fields = root_fields(&model, curated, namer);
    model.sort();
    model
}

/// Give each object the interfaces it is beneath in the class hierarchy.
///
/// GraphQL requires an implementor to declare every one of the interface's
/// fields, so a class missing one is left out — a schema that will not register
/// at all is a worse outcome than one missing edge.
fn link_interfaces(model: &mut SchemaModel, curated: &CuratedSchema) {
    let interfaces: Vec<(String, String, Vec<String>)> = model
        .interfaces
        .iter()
        .map(|i| {
            (
                i.name.clone(),
                i.iri.clone(),
                i.fields.iter().map(|f| f.name.clone()).collect(),
            )
        })
        .collect();

    for object in &mut model.objects {
        if object.iri.is_empty() {
            continue;
        }
        for (name, iri, required) in &interfaces {
            if !curated.implements(&object.iri, iri) {
                continue;
            }
            if required
                .iter()
                .all(|f| object.fields.iter().any(|of| &of.name == f))
            {
                object.implements.push(name.clone());
            }
        }
    }
}

/// Several shapes can target one class; they are merged into a single view of it.
///
/// The first shape (by IRI order, which the caller fixes) supplies the type's
/// name and description, `sh:closed` holds if *any* of them closes the type, and
/// a property declared twice keeps its first declaration.
fn merge_shapes(shapes: &[ShapeDescription]) -> BTreeMap<&str, &ShapeDescription> {
    let mut out: BTreeMap<&str, &ShapeDescription> = BTreeMap::new();
    for shape in shapes {
        out.entry(shape.class_iri.as_str()).or_insert(shape);
    }
    out
}

/// The mutable state shared while typing one class's fields.
struct FieldCtx<'a> {
    type_scope: &'a NameScope<'a>,
    /// Class IRIs that became interfaces, so a reference to one is typed as the
    /// interface rather than as an object that has no instances.
    interfaces: &'a [&'a str],
    owner: &'a str,
    unions: &'a mut BTreeMap<Vec<String>, String>,
    enums: &'a mut BTreeMap<String, EnumType>,
    needs_node: &'a mut bool,
    warnings: &'a mut Vec<String>,
}

impl FieldCtx<'_> {
    /// The type of a reference to `class_iri`.
    fn reference_to(&mut self, class_iri: &str) -> FieldType {
        match self.type_scope.get(class_iri) {
            Some(name) => {
                if self.interfaces.contains(&class_iri) {
                    FieldType::Interface(name.to_string())
                } else {
                    FieldType::Object(name.to_string())
                }
            }
            None => {
                *self.needs_node = true;
                FieldType::Object(NODE_TYPE.to_string())
            }
        }
    }

    /// As [`Self::reference_to`], for a target already resolved to a type name.
    fn named_reference(&self, type_name: &str) -> FieldType {
        let is_interface = self
            .interfaces
            .iter()
            .any(|iri| self.type_scope.get(iri) == Some(type_name));
        if is_interface {
            FieldType::Interface(type_name.to_string())
        } else {
            FieldType::Object(type_name.to_string())
        }
    }
}

fn build_fields(
    namer: &Namer,
    ctx: &mut FieldCtx<'_>,
    shape: Option<&ShapeDescription>,
    observation: Option<&ClassObservation>,
) -> Vec<Field> {
    let mut fields = vec![Field::id_field(if shape.is_some() {
        Provenance::Shaped
    } else {
        Provenance::Inferred
    })];
    let mut field_scope = NameScope::new(namer, RESERVED_FIELD_NAMES);

    // Shaped properties first, in `sh:order` then IRI order, so the declared
    // ordering survives into the SDL.
    let mut shaped_properties: Vec<&ShapedProperty> = shape
        .map(|s| s.properties.iter().collect())
        .unwrap_or_default();
    shaped_properties.sort_by(|a, b| {
        order_key(a.order)
            .partial_cmp(&order_key(b.order))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.iri.cmp(&b.iri))
            .then_with(|| a.direction.cmp(&b.direction))
    });

    // Declared field names are claimed before any are derived, for the same
    // reason as type names above.
    for property in &shaped_properties {
        claim_declared_field_name(&mut field_scope, property, ctx.warnings, ctx.owner);
    }
    for property in &shaped_properties {
        let name = field_scope.assign(&field_scope_key(property));
        let observed = observation.and_then(|o| find_observation(o, &property.iri));
        fields.push(shaped_field(ctx, property, name, observed));
    }

    // Then whatever the data has that the shape did not mention — unless the
    // shape closed the type, in which case those values are not part of it.
    if !shape.is_some_and(|s| s.closed) {
        let mut extra: Vec<&PropertyObservation> = observation
            .map(|o| {
                o.properties
                    .iter()
                    .filter(|p| p.iri != fluree_vocab::rdf::TYPE)
                    .filter(|p| {
                        shape.is_none_or(|s| s.property(&p.iri, Direction::Forward).is_none())
                    })
                    .collect()
            })
            .unwrap_or_default();
        extra.sort_by(|a, b| a.iri.cmp(&b.iri));

        for property in extra {
            let name = field_scope.assign(&property.iri);
            let ty = inferred_field_type(ctx, property, &name);
            fields.push(Field {
                name,
                iri: property.iri.clone(),
                direction: Direction::Forward,
                ty,
                list: true,
                non_null: false,
                description: None,
                language_tagged: property.has_language_tags,
                provenance: Provenance::Inferred,
            });
        }
    }

    fields
}

/// `sh:order` is optional; unordered properties sort after ordered ones.
fn order_key(order: Option<f64>) -> f64 {
    order.unwrap_or(f64::INFINITY)
}

/// The name-scope key for a property shape.
///
/// A reverse field reads the same predicate backwards, so it cannot share the
/// forward field's name. The `#reverse` suffix both distinguishes the two and
/// gives the derived fallback a readable shape — `employer` forward, and
/// `employer_reverse` back — since the namer takes the key's local part.
fn field_scope_key(property: &ShapedProperty) -> String {
    match property.direction {
        Direction::Forward => property.iri.clone(),
        Direction::Reverse => format!("{}#reverse", property.iri),
    }
}

fn claim_declared_field_name(
    scope: &mut NameScope<'_>,
    property: &ShapedProperty,
    warnings: &mut Vec<String>,
    owner: &str,
) {
    let Some(declared) = property.name.as_deref() else {
        return;
    };
    let Some(name) = naming::sanitize(declared) else {
        warnings.push(format!(
            "`sh:name` \"{declared}\" on {owner}.<{}> is not a usable GraphQL name; the name \
             was derived from the IRI instead",
            property.iri
        ));
        return;
    };
    if !scope.claim(&field_scope_key(property), &name) {
        warnings.push(format!(
            "`sh:name` \"{name}\" on {owner}.<{}> was already claimed; the name was derived \
             from the IRI instead",
            property.iri
        ));
    }
}

fn shaped_field(
    ctx: &mut FieldCtx<'_>,
    property: &ShapedProperty,
    name: String,
    observed: Option<&PropertyObservation>,
) -> Field {
    let ty = shaped_field_type(ctx, property, &name, observed);
    // A shape can say so outright with `sh:datatype rdf:langString`; otherwise
    // the data does.
    let language_tagged = property
        .datatype
        .as_deref()
        .is_some_and(|dt| dt == fluree_vocab::rdf::LANG_STRING)
        || observed.is_some_and(|o| o.has_language_tags);
    Field {
        name,
        iri: property.iri.clone(),
        direction: property.direction,
        ty,
        list: !property.is_single(),
        non_null: property.is_required(),
        description: property.description.clone(),
        language_tagged,
        provenance: Provenance::Shaped,
    }
}

/// A shaped property's type, in declaration order of specificity.
fn shaped_field_type(
    ctx: &mut FieldCtx<'_>,
    property: &ShapedProperty,
    field_name: &str,
    observed: Option<&PropertyObservation>,
) -> FieldType {
    // `sh:in` over a homogeneous, nameable value set is an enum — the one place
    // the schema can state the whole domain rather than describe it.
    if !property.allowed_values.is_empty() {
        if let Some(ty) = enum_field_type(ctx, property, field_name) {
            return ty;
        }
    }
    if let Some(class) = &property.class {
        return ctx.reference_to(class);
    }
    if let Some(datatype) = &property.datatype {
        return FieldType::Scalar(scalar_for_datatype_iri(datatype));
    }
    if property.node_kind_is_iri {
        *ctx.needs_node = true;
        return FieldType::Object(NODE_TYPE.to_string());
    }
    // The shape declared a path but nothing about its values, so fall back to
    // what the data shows — a shape is allowed to be partial.
    match observed {
        Some(observation) => inferred_field_type(ctx, observation, field_name),
        None => FieldType::Scalar(Scalar::String),
    }
}

/// `sh:in` → an enum, when every member yields a usable GraphQL name.
///
/// Returns `None` (and warns) when it does not, so the field falls through to
/// its datatype rather than being dropped.
fn enum_field_type(
    ctx: &mut FieldCtx<'_>,
    property: &ShapedProperty,
    field_name: &str,
) -> Option<FieldType> {
    let name = format!("{}{}Enum", ctx.owner, capitalize(field_name));
    let mut values: Vec<(String, String)> = Vec::with_capacity(property.allowed_values.len());
    // A mix of IRIs and literals has no single way to be written back into a
    // query, so it is not an enum.
    let all_iris = property
        .allowed_values
        .iter()
        .all(|v| matches!(v, AllowedValue::Iri(_)));
    let all_literals = property
        .allowed_values
        .iter()
        .all(|v| matches!(v, AllowedValue::String(_)));
    if !all_iris && !all_literals {
        ctx.warnings.push(format!(
            "`sh:in` on {}.{field_name} mixes IRIs and literals; the field kept its datatype \
             instead",
            ctx.owner
        ));
        return None;
    }

    for value in &property.allowed_values {
        let label = match value {
            AllowedValue::Iri(iri) => iri.rsplit(['#', '/']).next().unwrap_or(iri),
            AllowedValue::String(s) => s.as_str(),
        };
        let Some(label) = naming::sanitize(label) else {
            ctx.warnings.push(format!(
                "`sh:in` on {}.{field_name} holds a value that is not a usable enum name \
                 (\"{}\"); the field kept its datatype instead",
                ctx.owner,
                value.as_str()
            ));
            return None;
        };
        values.push((label, value.as_str().to_string()));
    }
    // Two members that sanitize to the same name would make the enum ambiguous
    // in both directions.
    let mut names: Vec<&String> = values.iter().map(|(n, _)| n).collect();
    names.sort();
    let duplicated = names.windows(2).any(|w| w[0] == w[1]);
    if duplicated {
        ctx.warnings.push(format!(
            "`sh:in` on {}.{field_name} has members whose names collide; the field kept its \
             datatype instead",
            ctx.owner
        ));
        return None;
    }

    let iri_valued = property
        .allowed_values
        .iter()
        .all(|v| matches!(v, AllowedValue::Iri(_)));
    ctx.enums.entry(name.clone()).or_insert_with(|| EnumType {
        name: name.clone(),
        description: None,
        values,
        iri_valued,
        provenance: Provenance::Shaped,
    });
    Some(FieldType::Enum(name))
}

/// Decide a field's type from the datatypes and targets observed for it.
fn inferred_field_type(
    ctx: &mut FieldCtx<'_>,
    property: &PropertyObservation,
    field_name: &str,
) -> FieldType {
    if !property.is_reference() {
        let scalars: Vec<Scalar> = property
            .datatypes
            .iter()
            .copied()
            .map(scalar_for_tag)
            .collect();
        return FieldType::Scalar(reduce_scalars(&scalars));
    }

    if property.has_literals() {
        // No GraphQL type covers both a reference and a literal. `String` at least
        // always resolves — an IRI renders as its own lexical form — where an
        // object type would fail on the literal values.
        ctx.warnings.push(format!(
            "{}.{field_name} ({}) holds both references and literals; \
             typed as String, so referenced subjects render as their IRI",
            ctx.owner, property.iri
        ));
        return FieldType::Scalar(Scalar::String);
    }

    // Only targets that are themselves exposed can be named.
    let mut members: Vec<String> = property
        .ref_classes
        .iter()
        .filter_map(|iri| {
            ctx.type_scope
                .get(iri)
                .map(std::string::ToString::to_string)
        })
        .collect();
    members.sort();
    members.dedup();

    match members.len() {
        0 => {
            *ctx.needs_node = true;
            FieldType::Object(NODE_TYPE.to_string())
        }
        1 => ctx.named_reference(&members.remove(0)),
        _ => {
            let name = ctx
                .unions
                .entry(members.clone())
                .or_insert_with(|| members.join("Or"))
                .clone();
            FieldType::Union(name)
        }
    }
}

fn find_observation<'a>(
    observation: &'a ClassObservation,
    iri: &str,
) -> Option<&'a PropertyObservation> {
    observation.properties.iter().find(|p| p.iri == iri)
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) => c.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// `person` / `persons` / `persons_count` per object type.
fn root_fields(
    model: &SchemaModel,
    curated: Option<&CuratedSchema>,
    namer: &Namer,
) -> Vec<RootField> {
    let mut scope = NameScope::new(namer, &[]);
    let mut out = Vec::new();
    // An interface is queryable too: a root field over it returns whichever
    // implementor each subject turns out to be.
    let roots: Vec<(&str, &str, Provenance)> = model
        .objects
        .iter()
        .map(|o| (o.name.as_str(), o.iri.as_str(), o.provenance))
        .chain(
            model
                .interfaces
                .iter()
                .map(|i| (i.name.as_str(), i.iri.as_str(), i.provenance)),
        )
        .collect();

    for (type_name, class_iri, provenance) in roots {
        // `Node` is a placeholder for subjects of unexposed types; there is
        // nothing coherent to enumerate.
        if class_iri.is_empty() {
            continue;
        }
        // A protected class is reachable through a reference but not listable.
        if curated.is_some_and(|c| !c.is_queryable(class_iri)) {
            continue;
        }
        let o = RootNames {
            name: type_name,
            iri: class_iri,
            provenance,
        };
        let plural_override = curated.and_then(|c| c.plural_names.get(class_iri));
        let singular = claim_root(&mut scope, o.iri, &lower_first(o.name));
        let plural = claim_root(
            &mut scope,
            &format!("{}#plural", o.iri),
            &plural_override
                .cloned()
                .unwrap_or_else(|| naming::pluralize(&lower_first(o.name))),
        );
        let count = claim_root(
            &mut scope,
            &format!("{}#count", o.iri),
            &format!("{plural}_count"),
        );

        out.push(RootField {
            name: singular,
            class_iri: o.iri.to_string(),
            type_name: o.name.to_string(),
            kind: RootKind::Single,
            description: Some(format!("One `{}` by IRI.", o.name)),
            provenance: o.provenance,
        });
        out.push(RootField {
            name: plural,
            class_iri: o.iri.to_string(),
            type_name: o.name.to_string(),
            kind: RootKind::List,
            description: Some(format!("Instances of `{}`.", o.name)),
            provenance: o.provenance,
        });
        out.push(RootField {
            name: count,
            class_iri: o.iri.to_string(),
            type_name: o.name.to_string(),
            kind: RootKind::Count,
            description: Some(format!("How many `{}` match.", o.name)),
            provenance: o.provenance,
        });
    }
    out
}

/// The names one root-field triple is built from.
struct RootNames<'a> {
    name: &'a str,
    iri: &'a str,
    provenance: Provenance,
}

/// Take `preferred` if free, else fall back to the scope's collision rules.
fn claim_root(scope: &mut NameScope<'_>, key: &str, preferred: &str) -> String {
    if scope.claim(key, preferred) {
        preferred.to_string()
    } else {
        scope.assign(key)
    }
}

fn lower_first(name: &str) -> String {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) => c.to_lowercase().chain(chars).collect(),
        None => String::new(),
    }
}
