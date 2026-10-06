//! GraphQL root field → one JSON-LD query.
//!
//! Lowering targets the **JSON-LD query document**, not the resolved query IR.
//! The document is what the engine's own parser validates and encodes, so this
//! crate inherits that work instead of restating it; it is also directly
//! reviewable, which is what makes the `explain` extension worth having. The cost
//! is one JSON parse per request, against a query that is about to touch the index.
//!
//! Every query is written with `"@context": {}` — no compaction. Output keys are
//! then exactly the IRIs the selection asked for, so [`shape`] can match them
//! without reimplementing JSON-LD term selection, and IRIs are compacted for the
//! client afterwards by the caller's [`Namer`](crate::naming::Namer).

pub mod filter;
pub mod reshape;
pub mod shape;

use std::collections::BTreeMap;

use async_graphql::Value as GqlValue;
use serde_json::json;
use serde_json::Value as Json;

use crate::error::{Error, Result};
use crate::naming::Namer;
use crate::schema::model::{Direction, RootField, RootKind, SchemaModel};
use crate::selection::Selection;
use filter::PatternBuilder;
use shape::{ConditionalShape, FieldShape, FieldSource, LanguageSpec, NodeShape, RootShape};

/// The alias a count query projects its total under.
pub const COUNT_ALIAS: &str = "?count";

/// A lowered root field: the query to run, and how to reshape its result.
#[derive(Debug, Clone, PartialEq)]
pub struct Lowered {
    pub query: Json,
    pub shape: RootShape,
}

/// Lower one root field and its selection subtree.
///
/// `namer` expands the compacted IRIs a client sends back (`ex:alice`): the
/// lowered query carries no context, so every IRI in it must be absolute.
pub fn lower(
    model: &SchemaModel,
    root: &RootField,
    selection: &Selection,
    namer: &Namer,
) -> Result<Lowered> {
    let mut builder = PatternBuilder::new(namer);
    let subject = builder.fresh_var();

    let mut reverse_terms = ReverseTerms::default();
    let (node_shape, spec) = if root.kind == RootKind::Count {
        (NodeShape::default(), SpecNode::default())
    } else {
        let mut spec = SpecNode::default();
        let shape = build_level(
            model,
            &root.type_name,
            &selection.children,
            &mut spec,
            &mut reverse_terms,
        )?;
        (shape, spec)
    };
    let context = reverse_terms.to_context();

    let query = match root.kind {
        RootKind::Single => {
            reject_unknown_args(selection, &[], &root.name)?;
            let id = selection.argument("id").ok_or_else(|| {
                Error::Lower(format!("`{}` requires an `id` argument", root.name))
            })?;
            let GqlValue::String(iri) = id else {
                return Err(Error::Lower(format!(
                    "`{}`'s `id` must be an IRI string",
                    root.name
                )));
            };
            // Not an IRI-constant expansion: that returns a bare `{"@id": ...}`
            // stub for a subject that does not exist, and does not check the
            // subject's type. Constraining a variable instead makes both cases
            // produce no row, which is the `null` GraphQL expects.
            json!({
                "@context": context,
                "select": { subject.as_str(): spec.to_json() },
                "where": [
                    { "@id": subject.as_str(), "@type": root.class_iri },
                    ["values", [subject.as_str(), [{ "@id": namer.expand(iri) }]]]
                ],
                "limit": 1,
            })
        }
        RootKind::List => {
            let mut query = json!({
                "@context": context,
                "select": { subject.as_str(): spec.to_json() },
                "where": root_patterns(model, root, &subject, selection, builder)?,
            });
            apply_modifiers(&mut query, model, root, &subject, selection)?;
            query
        }
        RootKind::Count => {
            reject_unknown_args(selection, &["where"], &root.name)?;
            json!({
                "@context": context,
                // Filters bind multi-valued fields, so a plain `count` would
                // charge one subject once per matching value.
                "select": [format!("(as (count-distinct {subject}) {COUNT_ALIAS})")],
                "where": root_patterns(model, root, &subject, selection, builder)?,
            })
        }
    };

    Ok(Lowered {
        query,
        shape: RootShape {
            kind: root.kind,
            node: node_shape,
        },
    })
}

fn root_patterns(
    model: &SchemaModel,
    root: &RootField,
    subject: &str,
    selection: &Selection,
    mut builder: PatternBuilder<'_>,
) -> Result<Json> {
    builder.push(json!({ "@id": subject, "@type": root.class_iri }));
    if let Some(where_arg) = selection.argument("where") {
        if !matches!(where_arg, GqlValue::Null) {
            builder.apply(model, &root.type_name, subject, where_arg)?;
        }
    }
    Ok(Json::Array(builder.into_patterns()))
}

fn apply_modifiers(
    query: &mut Json,
    model: &SchemaModel,
    root: &RootField,
    subject: &str,
    selection: &Selection,
) -> Result<()> {
    reject_unknown_args(
        selection,
        &["where", "limit", "offset", "orderBy"],
        &root.name,
    )?;
    let map = query.as_object_mut().expect("object literal");

    for (name, key) in [("limit", "limit"), ("offset", "offset")] {
        if let Some(value) = selection.argument(name) {
            let GqlValue::Number(n) = value else {
                if matches!(value, GqlValue::Null) {
                    continue;
                }
                return Err(Error::Lower(format!("`{name}` must be an integer")));
            };
            let n = n
                .as_u64()
                .ok_or_else(|| Error::Lower(format!("`{name}` must be a non-negative integer")))?;
            map.insert(key.to_string(), json!(n));
        }
    }

    if let Some(order) = selection.argument("orderBy") {
        if !matches!(order, GqlValue::Null) {
            map.insert(
                "orderBy".to_string(),
                Json::Array(order_keys(model, root, subject, order)?),
            );
        }
    }
    Ok(())
}

/// `orderBy: { field: ASC }` → JSON-LD `orderBy` entries.
///
/// Only single-valued fields are orderable (see `runtime::is_orderable`), so no
/// entry here can multiply the result. In tier 1 that leaves `id` alone, which
/// needs no extra pattern because it *is* the subject variable.
fn order_keys(
    model: &SchemaModel,
    root: &RootField,
    subject: &str,
    order: &GqlValue,
) -> Result<Vec<Json>> {
    let GqlValue::Object(entries) = order else {
        return Err(Error::Lower("`orderBy` must be an object".to_string()));
    };
    let mut out = Vec::new();
    for (field_name, direction) in entries {
        if matches!(direction, GqlValue::Null) {
            continue;
        }
        let field = model
            .fields_of(&root.type_name)
            .and_then(|fs| fs.iter().find(|f| f.name == field_name.as_str()))
            .ok_or_else(|| {
                Error::Lower(format!(
                    "`{}` has no field `{field_name}` to order by",
                    root.type_name
                ))
            })?;
        if !field.is_id() {
            return Err(Error::Lower(format!(
                "ordering by `{}.{field_name}` is not supported yet; \
                 only `id` is orderable in an inferred schema",
                root.type_name
            )));
        }
        let descending = matches!(direction, GqlValue::Enum(d) if d.as_str() == "DESC");
        out.push(if descending {
            json!(["desc", subject])
        } else {
            json!(subject)
        });
    }
    Ok(out)
}

fn reject_unknown_args(selection: &Selection, allowed: &[&str], field: &str) -> Result<()> {
    for (name, _) in &selection.arguments {
        if !allowed.contains(&name.as_str()) && name != "id" {
            return Err(Error::Lower(format!(
                "`{field}` does not accept the argument `{name}`"
            )));
        }
    }
    Ok(())
}

// === Selection set → select spec + response shape ===

/// The JSON-LD selection spec for one level, built so repeated predicates merge.
///
/// Fragments make merging necessary: `... on Person { knows { name } }` and
/// `... on Employee { knows { id } }` both select `knows`, and the query has to
/// ask for the union of their sub-selections. The JSON-LD select tree carries no
/// type conditions, so asking for the superset is both correct and the only
/// option; [`NodeShape`] decides what to surface per node.
#[derive(Debug, Default, Clone, PartialEq)]
struct SpecNode {
    id: bool,
    typed: bool,
    /// Predicate IRI → sub-level, `None` for a leaf.
    props: BTreeMap<String, Option<SpecNode>>,
    /// Predicate IRI → per-value ordering and paging.
    modifiers: BTreeMap<String, Json>,
}

impl SpecNode {
    fn merge_property(&mut self, iri: &str, sub: Option<SpecNode>) {
        match self.props.get_mut(iri) {
            Some(existing) => match (existing.as_mut(), sub) {
                (Some(a), Some(b)) => a.merge(b),
                (None, Some(b)) => *existing = Some(b),
                (_, None) => {}
            },
            None => {
                self.props.insert(iri.to_string(), sub);
            }
        }
    }

    /// Record `friend(limit: 5)`-style modifiers for a predicate.
    ///
    /// Two fragments selecting the same predicate with *different* modifiers
    /// cannot both be honoured — the query asks for one set of values — so the
    /// first wins and the conflict is reported by `build_level`.
    fn set_modifiers(&mut self, iri: &str, modifiers: Option<Json>) {
        if let Some(m) = modifiers {
            self.modifiers.entry(iri.to_string()).or_insert(m);
        }
    }

    fn merge(&mut self, other: SpecNode) {
        self.id |= other.id;
        self.typed |= other.typed;
        for (iri, sub) in other.props {
            self.merge_property(&iri, sub);
        }
        for (iri, m) in other.modifiers {
            self.modifiers.entry(iri).or_insert(m);
        }
    }

    fn to_json(&self) -> Json {
        let mut items = Vec::new();
        if self.id {
            items.push(json!("@id"));
        }
        if self.typed {
            items.push(json!("@type"));
        }
        for (iri, sub) in &self.props {
            let selection = sub.as_ref().map(SpecNode::to_json);
            match (selection, self.modifiers.get(iri)) {
                // A bare predicate name: no sub-selection, nothing to shape.
                (None, None) => items.push(json!(iri)),
                // The object form of a nested selection carries the modifiers
                // alongside the sub-selection.
                (selection, Some(modifiers)) => {
                    let mut spec = modifiers.clone();
                    if let Some(selection) = selection {
                        spec.as_object_mut()
                            .expect("modifier object")
                            .insert("select".to_string(), selection);
                    }
                    items.push(json!({ iri.as_str(): spec }));
                }
                (Some(selection), None) => items.push(json!({ iri.as_str(): selection })),
            }
        }
        Json::Array(items)
    }
}

/// Generated `@context` terms for the reverse edges a query selects.
///
/// Without them a reverse selection comes back under the plain predicate IRI —
/// the same key a forward selection of that predicate uses — so a query reading
/// an edge in both directions would collide. A `{"@reverse": <iri>}` term gives
/// the reverse side a key of its own, and the query carries no other context, so
/// nothing else is compacted.
#[derive(Debug, Default)]
struct ReverseTerms {
    /// Predicate IRI → generated term, in first-seen order.
    terms: Vec<(String, String)>,
}

impl ReverseTerms {
    fn term_for(&mut self, iri: &str) -> String {
        if let Some((_, term)) = self.terms.iter().find(|(known, _)| known == iri) {
            return term.clone();
        }
        // Underscore-prefixed so it cannot collide with a predicate's own name.
        let term = format!("_rev{}", self.terms.len());
        self.terms.push((iri.to_string(), term.clone()));
        term
    }

    fn to_context(&self) -> Json {
        let mut map = serde_json::Map::new();
        for (iri, term) in &self.terms {
            map.insert(term.clone(), json!({ "@reverse": iri }));
        }
        Json::Object(map)
    }
}

/// The `lang:` argument on a language-tagged field.
fn language_spec(
    field: &crate::schema::model::Field,
    child: &Selection,
) -> Result<Option<LanguageSpec>> {
    let Some(value) = child
        .argument("lang")
        .filter(|v| !matches!(v, GqlValue::Null))
    else {
        // A language-tagged literal still has to be unwrapped even when the
        // caller says nothing: the hydration renders it as
        // `{"@value": …, "@language": …}`, and the field is declared `String`.
        // Returning the object would make the schema's own type a lie.
        return Ok(field.language_tagged.then_some(LanguageSpec::Any));
    };
    let GqlValue::String(spec) = value else {
        return Err(Error::Lower(format!(
            "`lang` on `{}` must be a string, got {value}",
            field.name
        )));
    };
    Ok(Some(LanguageSpec::parse(spec)))
}

/// The key a field is selected and returned under.
fn selection_key(field: &crate::schema::model::Field, reverse_terms: &mut ReverseTerms) -> String {
    match field.direction {
        Direction::Forward => field.iri.clone(),
        Direction::Reverse => reverse_terms.term_for(&field.iri),
    }
}

/// `friend(where:, orderBy:, limit:, offset:)` → the JSON-LD nested-selection
/// modifiers, or `None` when the field carried no arguments.
///
/// `where` is refused: filtering a nested level means evaluating a predicate over
/// values the hydration has already materialized, which is a different engine
/// from the one that answers the WHERE clause. Ordering and paging act on the
/// values as they stand, so they need no such thing.
fn nested_arguments(
    model: &SchemaModel,
    owner_type: &str,
    field: &crate::schema::model::Field,
    child: &Selection,
) -> Result<Option<Json>> {
    if child.arguments.is_empty() {
        return Ok(None);
    }
    let label = format!("{owner_type}.{}", child.name);

    let mut spec = serde_json::Map::new();
    for (name, value) in &child.arguments {
        if matches!(value, GqlValue::Null) {
            continue;
        }
        match name.as_str() {
            "where" => {
                return Err(Error::Lower(format!(
                    "`where` on the nested field `{label}` is not supported; \
                     filter at a root field and traverse to it instead"
                )))
            }
            "limit" | "offset" => {
                let GqlValue::Number(n) = value else {
                    return Err(Error::Lower(format!(
                        "`{name}` on `{label}` must be an integer"
                    )));
                };
                let n = n.as_u64().ok_or_else(|| {
                    Error::Lower(format!("`{name}` on `{label}` must be non-negative"))
                })?;
                spec.insert(name.clone(), json!(n));
            }
            "orderBy" => {
                spec.insert(
                    "orderBy".to_string(),
                    Json::Array(nested_order_keys(model, field, &label, value)?),
                );
            }
            // Handled separately: it selects among values rather than shaping
            // a list of subjects.
            "lang" => {}
            other => {
                return Err(Error::Lower(format!(
                    "`{label}` does not accept the argument `{other}`"
                )))
            }
        }
    }
    Ok((!spec.is_empty()).then(|| Json::Object(spec)))
}

fn nested_order_keys(
    model: &SchemaModel,
    field: &crate::schema::model::Field,
    label: &str,
    order: &GqlValue,
) -> Result<Vec<Json>> {
    let GqlValue::Object(entries) = order else {
        return Err(Error::Lower(format!(
            "`orderBy` on `{label}` must be an object"
        )));
    };
    let target = field.ty.type_name();
    let mut out = Vec::new();
    for (name, direction) in entries {
        if matches!(direction, GqlValue::Null) {
            continue;
        }
        let sort_field = model
            .fields_of(target)
            .and_then(|fs| fs.iter().find(|f| f.name == name.as_str()))
            .ok_or_else(|| {
                Error::Lower(format!(
                    "`{target}` has no field `{name}` to order `{label}` by"
                ))
            })?;
        // `id` is the subject IRI, which the hydration renders under `@id`
        // rather than as a predicate.
        let key = if sort_field.is_id() {
            json!("@id")
        } else {
            json!(sort_field.iri)
        };
        let descending = matches!(direction, GqlValue::Enum(d) if d.as_str() == "DESC");
        out.push(if descending {
            json!(["desc", key])
        } else {
            key
        });
    }
    Ok(out)
}

fn build_level(
    model: &SchemaModel,
    type_name: &str,
    children: &[Selection],
    spec: &mut SpecNode,
    reverse_terms: &mut ReverseTerms,
) -> Result<NodeShape> {
    let mut common: Vec<FieldShape> = Vec::new();
    // Keyed by type condition, in first-seen order so the response is stable.
    let mut conditional: Vec<ConditionalShape> = Vec::new();

    for child in children {
        let owner = match &child.type_condition {
            // A condition naming the static type adds nothing.
            None => None,
            Some(cond) if cond == type_name => None,
            Some(cond) => Some(cond.clone()),
        };
        let owner_type = owner.as_deref().unwrap_or(type_name);

        if child.name == "__typename" {
            spec.typed = true;
            push_field(
                &mut common,
                &mut conditional,
                model,
                owner.as_deref(),
                FieldShape {
                    response_key: child.response_key.clone(),
                    source: FieldSource::Typename,
                    list: false,
                    child: None,
                    enum_type: None,
                    language: None,
                },
            )?;
            continue;
        }

        let field = model
            .fields_of(owner_type)
            .and_then(|fs| fs.iter().find(|f| f.name == child.name))
            .ok_or_else(|| Error::Lower(format!("`{owner_type}` has no field `{}`", child.name)))?;

        let nested_modifiers = nested_arguments(model, owner_type, field, child)?;

        let shape = if field.is_id() {
            spec.id = true;
            FieldShape {
                response_key: child.response_key.clone(),
                source: FieldSource::Id,
                list: false,
                child: None,
                enum_type: None,
                language: None,
            }
        } else if field.ty.is_composite() {
            let mut sub = SpecNode::default();
            let sub_shape = build_level(
                model,
                field.ty.type_name(),
                &child.children,
                &mut sub,
                reverse_terms,
            )?;
            if sub_shape.needs_type() {
                sub.typed = true;
            }
            let key = selection_key(field, reverse_terms);
            spec.merge_property(&key, Some(sub));
            spec.set_modifiers(&key, nested_modifiers);
            FieldShape {
                response_key: child.response_key.clone(),
                source: FieldSource::Property(key),
                list: field.list,
                child: Some(sub_shape),
                enum_type: None,
                language: None,
            }
        } else {
            let key = selection_key(field, reverse_terms);
            let language = language_spec(field, child)?;
            // Asking for a language means asking for the tag too, so the leaf
            // becomes a sub-selection of the literal's value and language.
            match &language {
                Some(_) => {
                    let mut literal = SpecNode::default();
                    literal.props.insert("@value".to_string(), None);
                    literal.props.insert("@language".to_string(), None);
                    spec.merge_property(&key, Some(literal));
                }
                None => spec.merge_property(&key, None),
            }
            spec.set_modifiers(&key, nested_modifiers);
            FieldShape {
                response_key: child.response_key.clone(),
                source: FieldSource::Property(key),
                list: field.list,
                child: None,
                enum_type: match &field.ty {
                    crate::schema::model::FieldType::Enum(name) => Some(name.clone()),
                    _ => None,
                },
                language,
            }
        };

        push_field(
            &mut common,
            &mut conditional,
            model,
            owner.as_deref(),
            shape,
        )?;
    }

    if !conditional.is_empty() {
        spec.typed = true;
    }
    Ok(NodeShape {
        type_name: type_name.to_string(),
        common,
        conditional,
    })
}

fn push_field(
    common: &mut Vec<FieldShape>,
    conditional: &mut Vec<ConditionalShape>,
    model: &SchemaModel,
    owner: Option<&str>,
    shape: FieldShape,
) -> Result<()> {
    let Some(type_name) = owner else {
        common.push(shape);
        return Ok(());
    };
    if let Some(entry) = conditional.iter_mut().find(|c| c.type_name == type_name) {
        entry.fields.push(shape);
        return Ok(());
    }
    let class_iri = model
        .object(type_name)
        .map(|o| o.iri.clone())
        .ok_or_else(|| {
            Error::Lower(format!(
                "fragment condition `{type_name}` is not a concrete type in this schema"
            ))
        })?;
    conditional.push(ConditionalShape {
        type_name: type_name.to_string(),
        class_iri,
        fields: vec![shape],
    });
    Ok(())
}
