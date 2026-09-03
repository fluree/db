//! Mutations: GraphQL write fields → JSON-LD transactions.
//!
//! Mutations exist only in tier 3, and only when the `graphql:Schema` turns them
//! on. A schema derived from what a ledger happens to contain should never
//! become a write surface by accident, and the IRI namespace new subjects are
//! minted under has no safe default — a wrong guess writes identifiers that
//! cannot be un-minted.
//!
//! Every verb lowers to an ordinary JSON-LD transaction, so SHACL validation,
//! policy, and commit semantics apply exactly as they do to any other write.
//! Nothing here bypasses them; a rejected write surfaces as a GraphQL error.

use async_graphql::Value as GqlValue;
use serde_json::{json, Map, Value as Json};

use crate::error::{Error, Result};
use crate::naming::Namer;
use crate::schema::model::{Direction, Field, FieldType, ObjectType, SchemaModel};
use crate::selection::Selection;

/// What a mutation root field does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    /// `create_<T>(input:)` — one new subject.
    Create,
    /// `update_<T>(ids:, set:)` — replace the listed properties on each subject.
    Update,
    /// `delete_<T>(ids:)` — retract every fact about each subject.
    Delete,
}

/// A mutation root field.
#[derive(Debug, Clone)]
pub struct MutationField {
    pub name: String,
    pub kind: MutationKind,
    /// The type it operates on.
    pub type_name: String,
    pub class_iri: String,
}

impl MutationField {
    /// The three verbs for one type, in a stable order.
    pub fn for_type(object: &ObjectType) -> Vec<MutationField> {
        [
            (MutationKind::Create, "create"),
            (MutationKind::Update, "update"),
            (MutationKind::Delete, "delete"),
        ]
        .into_iter()
        .map(|(kind, verb)| MutationField {
            name: format!("{verb}_{}", object.name),
            kind,
            type_name: object.name.clone(),
            class_iri: object.iri.clone(),
        })
        .collect()
    }

    /// The GraphQL input type name for this type's writable fields.
    pub fn input_type_name(type_name: &str) -> String {
        format!("{type_name}Input")
    }

    /// The result type for `update_`/`delete_`.
    pub fn result_type_name(type_name: &str) -> String {
        format!("{type_name}MutationResult")
    }
}

/// The variable the update and delete templates bind their subjects to.
///
/// Underscore-prefixed so it cannot collide with anything a user wrote; GraphQL
/// documents never name query variables.
const SUBJECT_VAR: &str = "?_subject";

/// Whether a field can be written.
///
/// Reverse fields are not writable: the fact belongs to the *other* subject, so
/// writing one here would silently edit a node the caller did not name.
/// Interfaces and unions are not writable either — an input has to name one
/// concrete predicate value, and neither says which.
pub fn is_writable(field: &Field) -> bool {
    !field.is_id()
        && field.direction == Direction::Forward
        && !matches!(field.ty, FieldType::Interface(_) | FieldType::Union(_))
}

/// A lowered mutation: the transaction to run, and the subjects it touches.
#[derive(Debug, Clone, PartialEq)]
pub struct Lowered {
    /// The JSON-LD transaction body.
    pub transaction: Json,
    /// How it must be executed.
    pub verb: Verb,
    /// The subject IRIs affected, for reading the result back.
    pub subjects: Vec<String>,
}

/// Which write path a lowered transaction takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verb {
    Insert,
    Upsert,
    Update,
}

/// Lower one mutation root field.
pub fn lower(
    model: &SchemaModel,
    field: &MutationField,
    selection: &Selection,
    namer: &Namer,
    iri_base: Option<&str>,
) -> Result<Lowered> {
    match field.kind {
        MutationKind::Create => lower_create(model, field, selection, namer, iri_base),
        MutationKind::Update => lower_update(model, field, selection, namer),
        MutationKind::Delete => lower_delete(field, selection, namer),
    }
}

fn lower_create(
    model: &SchemaModel,
    field: &MutationField,
    selection: &Selection,
    namer: &Namer,
    iri_base: Option<&str>,
) -> Result<Lowered> {
    let input = selection
        .argument("input")
        .ok_or_else(|| Error::Lower(format!("`{}` requires an `input` argument", field.name)))?;
    let GqlValue::Object(entries) = input else {
        return Err(Error::Lower(format!(
            "`{}`'s `input` must be an object",
            field.name
        )));
    };

    // An explicit `id` names the subject; otherwise one is minted under the
    // schema's `f:graphqlIriBase`, which mutations require precisely because
    // there is no safe default.
    let iri = match entries.get("id") {
        Some(GqlValue::String(id)) => namer.expand(id),
        Some(other) => {
            return Err(Error::Lower(format!(
                "`{}`'s `id` must be an IRI string, got {other}",
                field.name
            )))
        }
        None => {
            let base = iri_base.ok_or_else(|| {
                Error::Lower(format!(
                    "`{}` cannot mint an IRI: the schema declares no \
                     `f:graphqlIriBase`, so pass an explicit `id`",
                    field.name
                ))
            })?;
            format!("{base}{}", new_id())
        }
    };

    let mut node = Map::new();
    node.insert("@id".to_string(), json!(iri));
    node.insert("@type".to_string(), json!(field.class_iri));
    write_properties(
        model,
        &field.type_name,
        entries,
        namer,
        &mut node,
        &field.name,
    )?;

    Ok(Lowered {
        transaction: json!({ "@context": {}, "@graph": [Json::Object(node)] }),
        verb: Verb::Insert,
        subjects: vec![iri],
    })
}

fn lower_update(
    model: &SchemaModel,
    field: &MutationField,
    selection: &Selection,
    namer: &Namer,
) -> Result<Lowered> {
    let subjects = id_list(selection, namer, &field.name)?;
    let set = selection
        .argument("set")
        .ok_or_else(|| Error::Lower(format!("`{}` requires a `set` argument", field.name)))?;
    let GqlValue::Object(entries) = set else {
        return Err(Error::Lower(format!(
            "`{}`'s `set` must be an object",
            field.name
        )));
    };
    if entries.contains_key("id") {
        return Err(Error::Lower(format!(
            "`{}` cannot change `id`: a subject's IRI is its identity, so \
             renaming one is a create and a delete",
            field.name
        )));
    }

    // `where`/`delete`/`insert` rather than an upsert, for two reasons: a
    // property set to `null` has to be retracted with nothing put back, which an
    // upsert cannot express (a node with only `@id` is not a valid one); and
    // this stays a single atomic transaction across every property and subject,
    // where clearing separately would not.
    let mut node = Map::new();
    write_properties(
        model,
        &field.type_name,
        entries,
        namer,
        &mut node,
        &field.name,
    )?;

    // Anchoring the match on the subject's type makes `update_Person` on a
    // Company's IRI a no-op rather than writing Person facts onto it.
    let mut where_clause = vec![
        json!([
            "values",
            [
                SUBJECT_VAR,
                subjects
                    .iter()
                    .map(|iri| json!({ "@id": iri }))
                    .collect::<Vec<_>>()
            ]
        ]),
        json!({ "@id": SUBJECT_VAR, "@type": field.class_iri }),
    ];
    let mut delete_clause = Vec::new();
    let mut insert_clause = Vec::new();

    for (i, (predicate, value)) in node.iter().enumerate() {
        let var = format!("?_old{i}");
        // Optional: a property the subject does not yet have still gets its new
        // value, and its absence must not drop the whole solution.
        where_clause.push(json!([
            "optional",
            { "@id": SUBJECT_VAR, predicate.as_str(): var.as_str() }
        ]));
        delete_clause.push(json!({ "@id": SUBJECT_VAR, predicate.as_str(): var.as_str() }));
        // A null clears: retracted above, and nothing inserted.
        if !value.is_null() && !matches!(value, Json::Array(a) if a.is_empty()) {
            insert_clause.push(json!({ "@id": SUBJECT_VAR, predicate.as_str(): value }));
        }
    }

    let mut transaction = Map::new();
    transaction.insert("@context".to_string(), json!({}));
    transaction.insert("where".to_string(), Json::Array(where_clause));
    transaction.insert("delete".to_string(), Json::Array(delete_clause));
    if !insert_clause.is_empty() {
        transaction.insert("insert".to_string(), Json::Array(insert_clause));
    }

    Ok(Lowered {
        transaction: Json::Object(transaction),
        verb: Verb::Update,
        subjects,
    })
}

fn lower_delete(field: &MutationField, selection: &Selection, namer: &Namer) -> Result<Lowered> {
    let subjects = id_list(selection, namer, &field.name)?;

    // A variable predicate retracts every fact about the subject. The `@type`
    // pattern keeps the delete scoped to the type the field names, so
    // `delete_Person` on a Company's IRI is a no-op rather than a wipe.
    Ok(Lowered {
        transaction: json!({
            "@context": {},
            "where": [
                ["values", [SUBJECT_VAR, subjects.iter().map(|iri| json!({ "@id": iri }))
                    .collect::<Vec<_>>()]],
                { "@id": SUBJECT_VAR, "@type": field.class_iri },
                { "@id": SUBJECT_VAR, "?p": "?o" }
            ],
            "delete": [{ "@id": SUBJECT_VAR, "?p": "?o" }]
        }),
        verb: Verb::Update,
        subjects,
    })
}

/// The `ids` argument, expanded to absolute IRIs.
fn id_list(selection: &Selection, namer: &Namer, field_name: &str) -> Result<Vec<String>> {
    let ids = selection
        .argument("ids")
        .ok_or_else(|| Error::Lower(format!("`{field_name}` requires an `ids` argument")))?;
    let GqlValue::List(items) = ids else {
        return Err(Error::Lower(format!(
            "`{field_name}`'s `ids` must be a list"
        )));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let GqlValue::String(id) = item else {
            return Err(Error::Lower(format!(
                "`{field_name}`'s `ids` must be IRI strings, got {item}"
            )));
        };
        out.push(namer.expand(id));
    }
    if out.is_empty() {
        return Err(Error::Lower(format!(
            "`{field_name}` was given no `ids`; a mutation that affects nothing \
             is more likely a mistake than an intent"
        )));
    }
    Ok(out)
}

/// Render input entries as JSON-LD predicate values on `node`.
fn write_properties(
    model: &SchemaModel,
    type_name: &str,
    entries: &async_graphql::indexmap::IndexMap<async_graphql::Name, GqlValue>,
    namer: &Namer,
    node: &mut Map<String, Json>,
    field_name: &str,
) -> Result<()> {
    for (key, value) in entries {
        if key == "id" {
            continue;
        }
        let field = model
            .fields_of(type_name)
            .and_then(|fs| fs.iter().find(|f| f.name == key.as_str()))
            .filter(|f| is_writable(f))
            .ok_or_else(|| Error::Lower(format!("`{type_name}` has no writable field `{key}`")))?;

        // A null clears the property: `upsert` retracts the listed predicates
        // before asserting, so an empty value list leaves nothing behind.
        let values: Vec<&GqlValue> = match value {
            GqlValue::Null => Vec::new(),
            GqlValue::List(items) => items.iter().collect(),
            single => vec![single],
        };
        if values.len() > 1 && !field.list {
            return Err(Error::Lower(format!(
                "`{type_name}.{key}` is single-valued ({field_name})"
            )));
        }

        let rendered: Vec<Json> = values
            .into_iter()
            .map(|v| render_value(model, field, v, type_name, key.as_str(), namer))
            .collect::<Result<Vec<_>>>()?;
        node.insert(
            field.iri.clone(),
            if field.list {
                Json::Array(rendered)
            } else {
                rendered.into_iter().next().unwrap_or(Json::Null)
            },
        );
    }
    Ok(())
}

fn render_value(
    model: &SchemaModel,
    field: &Field,
    value: &GqlValue,
    type_name: &str,
    key: &str,
    namer: &Namer,
) -> Result<Json> {
    let label = format!("{type_name}.{key}");
    Ok(match (&field.ty, value) {
        // A reference is written as a link, never as a nested node: creating a
        // subject as a side effect of linking to it would make one mutation
        // write two objects the caller only named one of.
        (FieldType::Object(_), GqlValue::String(id)) => json!({ "@id": namer.expand(id) }),
        (FieldType::Enum(name), GqlValue::Enum(member)) => {
            let enum_type = model
                .enum_type(name)
                .ok_or_else(|| Error::Lower(format!("unknown enum `{name}` ({label})")))?;
            let underlying = enum_type.value_for(member.as_str()).ok_or_else(|| {
                Error::Lower(format!("`{member}` is not a member of `{name}` ({label})"))
            })?;
            if enum_type.iri_valued {
                json!({ "@id": underlying })
            } else {
                json!(underlying)
            }
        }
        (_, GqlValue::String(s)) => json!(s),
        (_, GqlValue::Number(n)) => json!(n),
        (_, GqlValue::Boolean(b)) => json!(b),
        (_, other) => {
            return Err(Error::Lower(format!(
                "`{label}` cannot be written from {other}"
            )))
        }
    })
}

/// A fresh local name for a minted subject.
///
/// A process-lifetime counter supplies the uniqueness; hashing it under
/// per-process random seeds keeps the minted IRIs from reading as a visible
/// sequence. Knowing another subject's IRI grants nothing on its own, since
/// reads are governed by policy — so unguessability is a nicety and uniqueness
/// is the property actually relied on.
///
/// **128 bits, not 64.** Each writer in a clustered deployment seeds
/// independently, so uniqueness across the cluster is birthday-bound: at 64
/// bits that is ~2³² mints, and the failure is silent — `create_<T>` lowers to
/// an `Insert`, so a collision merges the new subject's facts onto whatever
/// already holds that IRI rather than erroring. Two independent seeds put the
/// bound at ~2⁶⁴ instead, which is UUIDv4's. A minted IRI is permanent, so this
/// is a width that has to be right the first time.
///
/// Deliberately no clock: `SystemTime::now()` *panics* on
/// `wasm32-unknown-unknown`, and it would compile fine on the way there. That
/// rules out a ULID or a UUIDv7; the seeds come from `RandomState`, which is
/// the same source the previous 64-bit form already relied on.
fn new_id() -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static SEEDS: OnceLock<(RandomState, RandomState)> = OnceLock::new();

    let (high, low) = SEEDS.get_or_init(|| (RandomState::new(), RandomState::new()));
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let half = |seed: &RandomState| {
        let mut hasher = seed.build_hasher();
        hasher.write_u64(n);
        hasher.finish()
    };
    format!("{:016x}{:016x}", half(high), half(low))
}
