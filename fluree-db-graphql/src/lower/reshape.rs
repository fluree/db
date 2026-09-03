//! JSON-LD query result → GraphQL response value.
//!
//! The engine returns rows keyed by full IRIs, with JSON-LD's usual collapsing of
//! a single-element array to a bare value. The [`RootShape`] built during lowering
//! says which response key each IRI belongs under, which fragment applies to which
//! node, and which fields are lists — so this pass restores aliases, re-expands the
//! collapsed values, resolves `__typename`, and compacts subject IRIs.

use serde_json::{json, Map, Value as Json};

use crate::error::{Error, Result};
use crate::naming::Namer;
use crate::schema::model::{RootKind, SchemaModel};

use super::shape::{FieldShape, FieldSource, LanguageSpec, NodeShape, RootShape};

/// Reshape the rows of one lowered query into the value for its root field.
pub fn reshape(shape: &RootShape, model: &SchemaModel, namer: &Namer, rows: &Json) -> Result<Json> {
    let rows = rows
        .as_array()
        .ok_or_else(|| Error::Execution("the query result was not an array of rows".to_string()))?;

    Ok(match shape.kind {
        RootKind::Count => {
            // A count query projects one column of one row.
            rows.first()
                .and_then(Json::as_array)
                .and_then(|cols| cols.first())
                .cloned()
                .unwrap_or_else(|| json!(0))
        }
        // No row means the subject does not exist, or is not of this class.
        RootKind::Single => match rows.first() {
            Some(node) => reshape_node(&shape.node, model, namer, node),
            None => Json::Null,
        },
        RootKind::List => Json::Array(
            rows.iter()
                .map(|node| reshape_node(&shape.node, model, namer, node))
                .collect(),
        ),
    })
}

fn reshape_node(shape: &NodeShape, model: &SchemaModel, namer: &Namer, node: &Json) -> Json {
    let mut out = Map::new();
    let concrete = concrete_type(shape, model, node);

    for field in &shape.common {
        insert_field(&mut out, field, model, namer, node, &concrete);
    }
    for cond in &shape.conditional {
        if !has_type(node, &cond.class_iri) {
            continue;
        }
        for field in &cond.fields {
            insert_field(&mut out, field, model, namer, node, &concrete);
        }
    }

    // The runtime dispatches an abstract position on `__typename`, whether or not
    // the document asked for it.
    if shape.needs_type() {
        out.entry("__typename")
            .or_insert_with(|| json!(concrete.clone()));
    }
    Json::Object(out)
}

fn insert_field(
    out: &mut Map<String, Json>,
    field: &FieldShape,
    model: &SchemaModel,
    namer: &Namer,
    node: &Json,
    concrete: &str,
) {
    let value = match &field.source {
        FieldSource::Id => node
            .get("@id")
            .and_then(Json::as_str)
            .map_or(Json::Null, |iri| json!(namer.compact(iri))),
        FieldSource::Typename => json!(concrete),
        FieldSource::Property(iri) => {
            let raw = node.get(iri.as_str());
            match (raw, &field.child) {
                // A GraphQL list field is always an array, even when JSON-LD
                // collapsed it or the subject had no value at all.
                (None, _) if field.list => Json::Array(Vec::new()),
                (None, _) => Json::Null,
                (Some(Json::Null), _) if field.list => Json::Array(Vec::new()),
                (Some(Json::Null), _) => Json::Null,
                (Some(v), None) => match (&field.enum_type, &field.language) {
                    (Some(name), _) => enum_value(model, name, v, field),
                    (None, Some(spec)) => language_value(v, spec, field.list),
                    (None, None) => scalar_value(v, field.list),
                },
                (Some(v), Some(child)) => {
                    let items: Vec<Json> = as_items(v)
                        .iter()
                        .map(|item| reshape_node(child, model, namer, item))
                        .collect();
                    if field.list {
                        Json::Array(items)
                    } else {
                        items.into_iter().next().unwrap_or(Json::Null)
                    }
                }
            }
        }
    };
    out.insert(field.response_key.clone(), value);
}

/// Render an enum-typed field: values arrive as the underlying IRI or literal
/// and have to come back as the member's GraphQL name.
///
/// A value the enum does not list becomes `null` rather than being passed
/// through — an undeclared value is not a member, and emitting it would produce
/// a response the schema says is impossible.
fn enum_value(model: &SchemaModel, enum_name: &str, value: &Json, field: &FieldShape) -> Json {
    let Some(enum_type) = model.enum_type(enum_name) else {
        return Json::Null;
    };
    // A literal member arrives as a string; an IRI member as the expanded node
    // `{"@id": ...}`, since selecting a reference predicate yields a node.
    let render = |v: &Json| {
        let underlying = match v {
            Json::String(s) => Some(s.as_str()),
            Json::Object(map) => map.get("@id").and_then(Json::as_str),
            _ => None,
        };
        underlying
            .and_then(|s| enum_type.name_for(s))
            .map_or(Json::Null, |name| json!(name))
    };
    if field.list {
        Json::Array(
            as_items(value)
                .iter()
                .map(render)
                .filter(|v| !v.is_null())
                .collect(),
        )
    } else {
        as_items(value).first().map_or(Json::Null, render)
    }
}

/// Select among language-tagged values.
///
/// The values arrive as `{"@value": …, "@language": …}` because the lowering
/// asked for the tag. A preference list returns the values of the *first*
/// language that has any — `"en,fr"` means English if there is English, else
/// French, not both — because a caller asking for a label wants one label.
fn language_value(value: &Json, spec: &LanguageSpec, list: bool) -> Json {
    let items = as_items(value);
    let tagged: Vec<(&Json, Option<&str>)> = items
        .iter()
        .map(|item| {
            let literal = item.get("@value").unwrap_or(item);
            let lang = item.get("@language").and_then(Json::as_str);
            (literal, lang)
        })
        .collect();

    let chosen: Vec<Json> = match spec {
        // The field is typed `String`, so the tag cannot come back with the
        // value; `*` is the explicit way to say "every value, whatever its tag",
        // which is also what omitting `lang` does.
        LanguageSpec::Any => tagged
            .iter()
            .map(|(literal, _)| (*literal).clone())
            .collect(),
        LanguageSpec::Preferred(languages) => languages
            .iter()
            .find_map(|wanted| {
                let matches: Vec<Json> = tagged
                    .iter()
                    .filter(|(_, lang)| lang.is_some_and(|l| l.eq_ignore_ascii_case(wanted)))
                    .map(|(literal, _)| (*literal).clone())
                    .collect();
                (!matches.is_empty()).then_some(matches)
            })
            .unwrap_or_default(),
    };

    if list {
        Json::Array(chosen)
    } else {
        chosen.into_iter().next().unwrap_or(Json::Null)
    }
}

fn scalar_value(value: &Json, list: bool) -> Json {
    if list {
        Json::Array(as_items(value).to_vec())
    } else {
        match value {
            Json::Array(items) => items.first().cloned().unwrap_or(Json::Null),
            other => other.clone(),
        }
    }
}

/// JSON-LD renders one value bare and several as an array; both mean "the values".
fn as_items(value: &Json) -> std::borrow::Cow<'_, [Json]> {
    match value {
        Json::Array(items) => std::borrow::Cow::Borrowed(items),
        other => std::borrow::Cow::Owned(vec![other.clone()]),
    }
}

/// The GraphQL type name for this node.
///
/// A subject can carry several `rdf:type`s, only some of which this schema
/// exposes, so the candidates are filtered to the types valid at this position.
/// With nothing usable, the static type stands in — correct for a concrete
/// position, and the only answer available for an unexposed subject.
fn concrete_type(shape: &NodeShape, model: &SchemaModel, node: &Json) -> String {
    let possible = model.possible_types(&shape.type_name);
    if possible.len() == 1 && possible[0].name == shape.type_name {
        return shape.type_name.clone();
    }
    for iri in type_iris(node) {
        if let Some(o) = possible.iter().find(|o| o.iri == iri) {
            return o.name.clone();
        }
    }
    shape.type_name.clone()
}

fn has_type(node: &Json, class_iri: &str) -> bool {
    type_iris(node).contains(&class_iri)
}

fn type_iris(node: &Json) -> Vec<&str> {
    match node.get("@type") {
        Some(Json::String(s)) => vec![s.as_str()],
        Some(Json::Array(items)) => items.iter().filter_map(Json::as_str).collect(),
        _ => Vec::new(),
    }
}
