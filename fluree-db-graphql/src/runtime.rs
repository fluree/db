//! Turns a [`SchemaModel`] into an executable `async-graphql` dynamic schema.
//!
//! The whole model is rendered as types with resolvers, but only the root fields
//! actually do any work: a root resolver hands its entire selection subtree to a
//! [`RootExecutor`], which compiles it to **one** Fluree query and returns the
//! hydrated JSON. Every nested field is a pass-through that indexes its parent
//! JSON object by response key, so there is no N+1 and no per-field I/O.
//!
//! The executor's JSON must therefore be keyed by *response key* (alias where the
//! document used one) — the executor compiled the selection set, so it is the part
//! that knows the aliases.
//!
//! Nested pass-throughs borrow out of the root's owned JSON rather than cloning it;
//! async-graphql keeps a parent `FieldValue` alive for the whole of its children's
//! resolution, so only leaf values (which must become `async_graphql::Value`) copy.

use std::any::Any;
use std::sync::Arc;

use async_graphql::dynamic::{
    Enum as DynEnum, EnumItem, Field as DynField, FieldFuture, FieldValue, InputObject, InputValue,
    Interface, InterfaceField, Object, ResolverContext, Scalar as DynScalar, Schema, TypeRef,
};
use async_graphql::Value as GqlValue;

use crate::error::{Error, Result};
use crate::limits::Limits;
use crate::mutate::{self, MutationField, MutationKind};
use crate::schema::model::{
    Direction, Field, FieldType, ObjectType, RootField, RootKind, Scalar, SchemaModel,
};
use crate::selection::{Operation, Selection};

/// Key under which a compiled [`Operation`] must be attached to the request.
///
/// The root resolver reads it from the request data; without it a query cannot be
/// lowered, because async-graphql's resolver-facing selection API drops fragment
/// type conditions (see [`crate::selection`]).
pub type OperationData = Arc<Operation>;

/// The executor a request runs against, attached to the request alongside the
/// operation.
///
/// Read from request data rather than captured in the resolvers, so one
/// registered [`Schema`] serves every request against that ledger version.
/// Registering is not cheap — thousands of allocations for a hundred-class
/// schema — and doing it per request dominated the cost of a small query.
pub type ExecutorData = Arc<dyn RootExecutor>;

/// What a root resolver hands to the executor.
pub struct RootRequest {
    /// The root field being resolved.
    pub root: RootField,
    /// Its full selection subtree, variables already substituted.
    pub selection: Selection,
}

/// What a mutation resolver hands to the executor.
pub struct MutationRequest {
    pub field: MutationField,
    pub selection: Selection,
}

/// Compiles a root field's selection subtree to a Fluree query and runs it.
///
/// The returned JSON is keyed by GraphQL response key: an object for
/// [`RootKind::Single`], an array for [`RootKind::List`], a number for
/// [`RootKind::Count`]. At any abstract (interface/union) position the object must
/// carry a `__typename` naming its concrete type.
#[async_trait::async_trait]
pub trait RootExecutor: Send + Sync + 'static {
    async fn resolve(&self, request: RootRequest) -> Result<serde_json::Value>;

    /// Run one mutation. The default refuses, so a schema built without
    /// mutations cannot acquire them by being handed a writing executor.
    async fn mutate(&self, request: MutationRequest) -> Result<serde_json::Value> {
        Err(crate::error::Error::Lower(format!(
            "`{}` is a mutation, and this endpoint is read-only",
            request.field.name
        )))
    }
}

/// Build an executable schema from a model.
///
/// `mutations` are the write fields to expose; empty means a read-only schema,
/// which is every tier below 3 and any tier-3 schema that did not opt in.
///
/// `limits` bound what a document may ask of the schema. async-graphql defaults
/// both depth and complexity to unlimited, and a derived schema is cyclic
/// wherever one class references another, so leaving them off would let the
/// caller choose the recursion depth.
pub fn build_schema(
    model: &SchemaModel,
    mutations: &[MutationField],
    limits: &Limits,
) -> Result<Schema> {
    let mutation_type = (!mutations.is_empty()).then_some("Mutation");
    let mut builder = Schema::build("Query", mutation_type, None);

    for name in custom_scalars_in_use(model) {
        builder = builder.register(DynScalar::new(name));
    }

    for e in &model.enums {
        let mut en = DynEnum::new(&e.name);
        if let Some(d) = &e.description {
            en = en.description(d);
        }
        for (name, _) in &e.values {
            en = en.item(EnumItem::new(name));
        }
        builder = builder.register(en);
    }

    for u in &model.unions {
        let mut un = async_graphql::dynamic::Union::new(&u.name);
        if let Some(d) = &u.description {
            un = un.description(d);
        }
        for m in &u.members {
            un = un.possible_type(m);
        }
        builder = builder.register(un);
    }

    for i in &model.interfaces {
        let mut iface = Interface::new(&i.name);
        if let Some(d) = &i.description {
            iface = iface.description(d);
        }
        for parent in &i.implements {
            iface = iface.implement(parent);
        }
        for f in &i.fields {
            let mut ifield = InterfaceField::new(&f.name, type_ref(f));
            if let Some(d) = &f.description {
                ifield = ifield.description(d);
            }
            for arg in nested_field_arguments(model, f) {
                ifield = ifield.argument(arg);
            }
            iface = iface.field(ifield);
        }
        builder = builder.register(iface);
    }

    for o in &model.objects {
        builder = builder.register(build_object(model, o));
    }

    for input in build_input_objects(model) {
        builder = builder.register(input);
    }
    builder = builder.register(sort_direction_enum());

    let mut query = Object::new("Query");
    for root in &model.query_fields {
        query = query.field(build_root_field(model, root));
    }
    builder = builder.register(query);

    if !mutations.is_empty() {
        for input in build_mutation_inputs(model, mutations) {
            builder = builder.register(input);
        }
        for result in build_mutation_results(model, mutations) {
            builder = builder.register(result);
        }
        let mut mutation = Object::new("Mutation");
        for field in mutations {
            mutation = mutation.field(build_mutation_field(field));
        }
        builder = builder.register(mutation);
    }

    if limits.max_depth != usize::MAX {
        builder = builder.limit_depth(limits.max_depth);
    }
    if limits.max_complexity != usize::MAX {
        builder = builder.limit_complexity(limits.max_complexity);
    }

    builder
        .finish()
        .map_err(|e| Error::Schema(format!("schema registration failed: {e}")))
}

fn build_object(model: &SchemaModel, o: &ObjectType) -> Object {
    let mut obj = Object::new(&o.name);
    if let Some(d) = &o.description {
        obj = obj.description(d);
    }
    for i in &o.implements {
        obj = obj.implement(i);
    }
    for f in &o.fields {
        let spec = f.clone();
        let mut field = DynField::new(&f.name, type_ref(f), move |ctx| resolve_nested(&spec, ctx));
        if let Some(d) = &f.description {
            field = field.description(d);
        }
        for arg in nested_field_arguments(model, f) {
            field = field.argument(arg);
        }
        obj = obj.field(field);
    }
    obj
}

fn build_root_field(model: &SchemaModel, root: &RootField) -> DynField {
    let spec = root.clone();
    let ty = match root.kind {
        RootKind::Single => TypeRef::named(&root.type_name),
        RootKind::List => TypeRef::named_nn_list(&root.type_name),
        RootKind::Count => TypeRef::named_nn(TypeRef::INT),
    };
    let abstract_type = model.object(&root.type_name).is_none() && root.kind != RootKind::Count;

    let mut field = DynField::new(&root.name, ty, move |ctx| {
        let spec = spec.clone();
        let response_key = response_key(&ctx);
        FieldFuture::new(async move {
            let executor = request_executor(&ctx)?;
            let op: &OperationData = ctx.data::<OperationData>().map_err(|_| {
                async_graphql::Error::new(
                    "internal: the compiled operation was not attached to the request",
                )
            })?;
            let selection = op
                .selections
                .iter()
                .find(|s| s.response_key == response_key)
                .cloned()
                .ok_or_else(|| {
                    async_graphql::Error::new(format!(
                        "internal: root field `{response_key}` is missing from the compiled operation"
                    ))
                })?;
            let kind = spec.kind;
            let json = executor
                .resolve(RootRequest {
                    root: spec,
                    selection,
                })
                .await
                .map_err(to_gql_error)?;
            root_field_value(kind, abstract_type, json)
        })
    });

    if let Some(d) = &root.description {
        field = field.description(d);
    }
    for arg in root_field_arguments(model, root) {
        field = field.argument(arg);
    }
    field
}

fn build_mutation_field(field: &MutationField) -> DynField {
    let spec = field.clone();
    let ty = match field.kind {
        MutationKind::Create => TypeRef::named(&field.type_name),
        MutationKind::Update | MutationKind::Delete => {
            TypeRef::named_nn(MutationField::result_type_name(&field.type_name))
        }
    };

    let mut dyn_field = DynField::new(&field.name, ty, move |ctx| {
        let spec = spec.clone();
        let response_key = response_key(&ctx);
        FieldFuture::new(async move {
            let executor = request_executor(&ctx)?;
            let op: &OperationData = ctx.data::<OperationData>().map_err(|_| {
                async_graphql::Error::new(
                    "internal: the compiled operation was not attached to the request",
                )
            })?;
            let selection = op
                .selections
                .iter()
                .find(|s| s.response_key == response_key)
                .cloned()
                .ok_or_else(|| {
                    async_graphql::Error::new(format!(
                        "internal: mutation `{response_key}` is missing from the compiled operation"
                    ))
                })?;
            let kind = spec.kind;
            let json = executor
                .mutate(MutationRequest {
                    field: spec,
                    selection,
                })
                .await
                .map_err(to_gql_error)?;
            match kind {
                // A create can legitimately return nothing to select.
                MutationKind::Create if json.is_null() => Ok(None),
                _ => Ok(Some(FieldValue::owned_any(json))),
            }
        })
    });

    dyn_field = match field.kind {
        MutationKind::Create => dyn_field.argument(InputValue::new(
            "input",
            TypeRef::named_nn(MutationField::input_type_name(&field.type_name)),
        )),
        MutationKind::Update => dyn_field
            .argument(InputValue::new(
                "ids",
                TypeRef::named_nn_list_nn(TypeRef::ID),
            ))
            .argument(InputValue::new(
                "set",
                TypeRef::named_nn(MutationField::input_type_name(&field.type_name)),
            )),
        MutationKind::Delete => dyn_field.argument(InputValue::new(
            "ids",
            TypeRef::named_nn_list_nn(TypeRef::ID),
        )),
    };
    dyn_field.description(match field.kind {
        MutationKind::Create => format!("Create one `{}`.", field.type_name),
        MutationKind::Update => format!(
            "Replace the listed properties on each `{}`. A null clears one.",
            field.type_name
        ),
        MutationKind::Delete => format!("Retract every fact about each `{}`.", field.type_name),
    })
}

/// One input type per mutated type, shared by `create` and `update`.
fn build_mutation_inputs(model: &SchemaModel, mutations: &[MutationField]) -> Vec<InputObject> {
    let mut seen: Vec<&str> = Vec::new();
    let mut out = Vec::new();
    for field in mutations {
        if seen.contains(&field.type_name.as_str()) {
            continue;
        }
        seen.push(&field.type_name);
        let Some(fields) = model.fields_of(&field.type_name) else {
            continue;
        };
        let mut input = InputObject::new(MutationField::input_type_name(&field.type_name))
            .description(format!(
                "Writable properties of `{}`. A reference is written as the target's                  `id`; creating one as a side effect would write an object the caller                  did not name.",
                field.type_name
            ))
            // Optional: absent on create means "mint one".
            .field(InputValue::new("id", TypeRef::named(TypeRef::ID)));
        for f in fields.iter().filter(|f| mutate::is_writable(f)) {
            input = input.field(InputValue::new(&f.name, input_type_ref(f)));
        }
        out.push(input);
    }
    out
}

/// The input type for a writable field: a reference is given by `id`.
///
/// Every input field is nullable regardless of the output's `!`: on `update` a
/// null clears the property, and requiring a value the caller did not intend to
/// change would make partial updates impossible.
fn input_type_ref(field: &Field) -> TypeRef {
    let name = match &field.ty {
        FieldType::Object(_) => TypeRef::ID.to_string(),
        other => other.type_name().to_string(),
    };
    if field.list {
        TypeRef::named_nn_list(name)
    } else {
        TypeRef::named(name)
    }
}

/// `{ affected_count, affected_objects }` for `update` and `delete`.
fn build_mutation_results(model: &SchemaModel, mutations: &[MutationField]) -> Vec<Object> {
    let mut seen: Vec<&str> = Vec::new();
    let mut out = Vec::new();
    for field in mutations {
        if field.kind == MutationKind::Create || seen.contains(&field.type_name.as_str()) {
            continue;
        }
        seen.push(&field.type_name);
        let type_name = field.type_name.clone();
        let _ = model;
        out.push(
            Object::new(MutationField::result_type_name(&type_name))
                .field(DynField::new(
                    "affected_count",
                    TypeRef::named_nn(TypeRef::INT),
                    |ctx| resolve_result_entry(ctx, "affected_count"),
                ))
                .field(DynField::new(
                    "affected_objects",
                    TypeRef::named_nn_list(&type_name),
                    |ctx| resolve_result_entry(ctx, "affected_objects"),
                )),
        );
    }
    out
}

/// Read one entry of a mutation result, which the executor built as JSON.
fn resolve_result_entry<'a>(ctx: ResolverContext<'a>, key: &'static str) -> FieldFuture<'a> {
    FieldFuture::new(async move {
        let parent = ctx
            .parent_value
            .try_downcast_ref::<serde_json::Value>()
            .map_err(|_| async_graphql::Error::new("internal: mutation result is not JSON"))?;
        match parent.get(key) {
            Some(serde_json::Value::Array(items)) => Ok(Some(FieldValue::list(
                items
                    .iter()
                    .map(|v| FieldValue::borrowed_any(v as &(dyn Any + Send + Sync)))
                    .collect::<Vec<_>>(),
            ))),
            Some(v) => Ok(Some(FieldValue::value(
                GqlValue::from_json(v.clone())
                    .map_err(|e| async_graphql::Error::new(e.to_string()))?,
            ))),
            None => Ok(None),
        }
    })
}

/// Shape the executor's JSON for one root field.
fn root_field_value<'a>(
    kind: RootKind,
    abstract_type: bool,
    json: serde_json::Value,
) -> async_graphql::Result<Option<FieldValue<'a>>> {
    match kind {
        RootKind::Count => Ok(Some(FieldValue::value(
            GqlValue::from_json(json).map_err(|e| async_graphql::Error::new(e.to_string()))?,
        ))),
        RootKind::Single => match json {
            serde_json::Value::Null => Ok(None),
            v => Ok(Some(owned_composite(v, abstract_type)?)),
        },
        RootKind::List => {
            let serde_json::Value::Array(items) = json else {
                return Err(async_graphql::Error::new(
                    "internal: list root field did not produce an array",
                ));
            };
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(owned_composite(item, abstract_type)?);
            }
            Ok(Some(FieldValue::list(out)))
        }
    }
}

/// Pass-through resolver: index the parent JSON by response key.
fn resolve_nested<'a>(field: &Field, ctx: ResolverContext<'a>) -> FieldFuture<'a> {
    let key = response_key(&ctx);
    let ty = field.ty.clone();
    FieldFuture::new(async move {
        let parent = ctx
            .parent_value
            .try_downcast_ref::<serde_json::Value>()
            .map_err(|_| {
                async_graphql::Error::new("internal: parent value is not a JSON object")
            })?;
        let Some(value) = parent.get(&key) else {
            return Ok(None);
        };
        if value.is_null() {
            return Ok(None);
        }
        if !ty.is_composite() {
            return Ok(Some(FieldValue::value(
                GqlValue::from_json(value.clone())
                    .map_err(|e| async_graphql::Error::new(e.to_string()))?,
            )));
        }
        let is_abstract = ty.is_abstract();
        match value {
            serde_json::Value::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(borrowed_composite(item, is_abstract)?);
                }
                Ok(Some(FieldValue::list(out)))
            }
            v => Ok(Some(borrowed_composite(v, is_abstract)?)),
        }
    })
}

fn owned_composite<'a>(
    value: serde_json::Value,
    is_abstract: bool,
) -> async_graphql::Result<FieldValue<'a>> {
    let type_name = is_abstract
        .then(|| concrete_type_name(&value))
        .transpose()?;
    let fv = FieldValue::owned_any(value);
    Ok(match type_name {
        Some(t) => fv.with_type(t),
        None => fv,
    })
}

fn borrowed_composite(
    value: &serde_json::Value,
    is_abstract: bool,
) -> async_graphql::Result<FieldValue<'_>> {
    let type_name = is_abstract.then(|| concrete_type_name(value)).transpose()?;
    let fv = FieldValue::borrowed_any(value as &(dyn Any + Send + Sync));
    Ok(match type_name {
        Some(t) => fv.with_type(t),
        None => fv,
    })
}

fn concrete_type_name(value: &serde_json::Value) -> async_graphql::Result<String> {
    value
        .get("__typename")
        .and_then(serde_json::Value::as_str)
        .map(std::string::ToString::to_string)
        .ok_or_else(|| {
            async_graphql::Error::new(
                "internal: a value at an interface/union position carried no `__typename`",
            )
        })
}

/// The executor this request runs against.
fn request_executor(ctx: &ResolverContext<'_>) -> async_graphql::Result<ExecutorData> {
    ctx.data::<ExecutorData>()
        .cloned()
        .map_err(|_| async_graphql::Error::new("internal: no executor was attached to the request"))
}

fn response_key(ctx: &ResolverContext<'_>) -> String {
    let field = ctx.ctx.field();
    field.alias().unwrap_or_else(|| field.name()).to_string()
}

fn to_gql_error(e: Error) -> async_graphql::Error {
    let code = e.code();
    let mut err = async_graphql::Error::new(e.to_string());
    err.extensions
        .get_or_insert_with(Default::default)
        .set("code", code);
    err
}

// === Type references and arguments ===

fn type_ref(field: &Field) -> TypeRef {
    let name = field.ty.type_name().to_string();
    match (field.list, field.non_null) {
        // RDF never produces a null inside a multi-valued field, so list items
        // are always non-null; `non_null` refers to the list itself.
        (true, true) => TypeRef::named_nn_list_nn(name),
        (true, false) => TypeRef::named_nn_list(name),
        (false, true) => TypeRef::named_nn(name),
        (false, false) => TypeRef::named(name),
    }
}

fn root_field_arguments(model: &SchemaModel, root: &RootField) -> Vec<InputValue> {
    match root.kind {
        RootKind::Single => vec![InputValue::new("id", TypeRef::named_nn(TypeRef::ID))],
        RootKind::Count => vec![InputValue::new(
            "where",
            TypeRef::named(filter_input_name(&root.type_name)),
        )],
        RootKind::List => {
            let mut args = vec![
                InputValue::new("where", TypeRef::named(filter_input_name(&root.type_name))),
                InputValue::new("limit", TypeRef::named(TypeRef::INT)),
                InputValue::new("offset", TypeRef::named(TypeRef::INT)),
            ];
            if has_orderable_field(model, &root.type_name) {
                args.push(InputValue::new(
                    "orderBy",
                    TypeRef::named(order_input_name(&root.type_name)),
                ));
            }
            args
        }
    }
}

/// A multi-valued object field takes the same shaping arguments as a root list
/// field, so `friend(limit: 5, orderBy: {...})` works at any depth.
fn nested_field_arguments(model: &SchemaModel, field: &Field) -> Vec<InputValue> {
    // A language-tagged literal takes a `lang` spec rather than the shaping
    // arguments, which only make sense for a list of subjects.
    if !field.ty.is_composite() {
        return if field.language_tagged {
            vec![
                InputValue::new("lang", TypeRef::named(TypeRef::STRING)).description(
                    "Which language to return: a comma-separated preference list — \
                 `\"en,fr\"` yields the English values if there are any, else the \
                 French ones — or `\"*\"` for every value whatever its tag, which is \
                 also what omitting this does. The tag itself is not returned; the \
                 field is a String.",
                ),
            ]
        } else {
            Vec::new()
        };
    }
    if !field.list {
        return Vec::new();
    }
    // The hydration IR carries per-value modifiers on forward properties only;
    // a reverse selection has nowhere to put them. Advertising the arguments
    // and then refusing them would be worse than not offering them.
    if field.direction == Direction::Reverse {
        return Vec::new();
    }
    let target = field.ty.type_name();
    let mut args = vec![
        InputValue::new("where", TypeRef::named(filter_input_name(target))),
        InputValue::new("limit", TypeRef::named(TypeRef::INT)),
        InputValue::new("offset", TypeRef::named(TypeRef::INT)),
    ];
    if has_nested_orderable_field(model, target) {
        args.push(InputValue::new(
            "orderBy",
            TypeRef::named(nested_order_input_name(target)),
        ));
    }
    args
}

// === Input objects ===

fn filter_input_name(type_name: &str) -> String {
    format!("{type_name}Filter")
}

fn order_input_name(type_name: &str) -> String {
    format!("{type_name}Order")
}

fn nested_order_input_name(type_name: &str) -> String {
    format!("{type_name}NestedOrder")
}

fn scalar_filter_name(scalar: Scalar) -> String {
    format!("{}Filter", scalar.type_name())
}

fn enum_filter_name(enum_name: &str) -> String {
    format!("{enum_name}Filter")
}

fn sort_direction_enum() -> DynEnum {
    DynEnum::new("SortDirection")
        .description("Sort order for `orderBy`.")
        .item(EnumItem::new("ASC"))
        .item(EnumItem::new("DESC"))
}

fn has_orderable_field(model: &SchemaModel, type_name: &str) -> bool {
    model
        .fields_of(type_name)
        .is_some_and(|fs| fs.iter().any(is_orderable))
}

fn has_nested_orderable_field(model: &SchemaModel, type_name: &str) -> bool {
    model
        .fields_of(type_name)
        .is_some_and(|fs| fs.iter().any(is_nested_orderable))
}

/// Ordering a **root** list orders the query's solutions, and a multi-valued key
/// would multiply subjects rather than order them — so only single-valued leaf
/// fields qualify. In an inferred schema that leaves `id`.
fn is_orderable(f: &Field) -> bool {
    !f.list && is_leaf(f)
}

/// Ordering a **nested** list sorts values already materialized for one subject,
/// so a multi-valued key is fine: it sorts by each value's first entry and
/// duplicates nothing. This is why the two positions take different inputs.
fn is_nested_orderable(f: &Field) -> bool {
    is_leaf(f)
}

fn is_leaf(f: &Field) -> bool {
    matches!(
        f.ty,
        FieldType::Id | FieldType::Scalar(_) | FieldType::Enum(_)
    )
}

fn build_input_objects(model: &SchemaModel) -> Vec<InputObject> {
    let mut out = Vec::new();

    for scalar in scalars_in_use(model) {
        out.push(scalar_filter_input(scalar));
    }
    for e in &model.enums {
        out.push(
            InputObject::new(enum_filter_name(&e.name))
                .field(InputValue::new("EQ", TypeRef::named(&e.name)))
                .field(InputValue::new("NEQ", TypeRef::named(&e.name)))
                .field(InputValue::new("IN", TypeRef::named_nn_list(&e.name)))
                .field(InputValue::new("NIN", TypeRef::named_nn_list(&e.name)))
                .field(InputValue::new("EXISTS", TypeRef::named(TypeRef::BOOLEAN))),
        );
    }

    // Interfaces and unions are filterable positions too, so every composite type
    // in the model gets a filter input, not just the concrete objects.
    for (name, fields) in filterable_types(model) {
        out.push(type_filter_input(&name, &fields));
        if fields.iter().any(is_orderable) {
            out.push(type_order_input(
                order_input_name(&name),
                &fields,
                is_orderable,
                format!(
                    "Sort keys for a `{name}` list. Only single-valued fields: ordering the \
                     query's solutions by a multi-valued one would repeat subjects."
                ),
            ));
        }
        if fields.iter().any(is_nested_orderable) {
            out.push(type_order_input(
                nested_order_input_name(&name),
                &fields,
                is_nested_orderable,
                format!(
                    "Sort keys for a nested `{name}` list. These sort one subject's values \
                     rather than the query's solutions, so a multi-valued field is allowed \
                     and sorts by its first value."
                ),
            ));
        }
    }
    out
}

fn filterable_types(model: &SchemaModel) -> Vec<(String, Vec<Field>)> {
    let mut out: Vec<(String, Vec<Field>)> = model
        .objects
        .iter()
        .map(|o| (o.name.clone(), o.fields.clone()))
        .chain(
            model
                .interfaces
                .iter()
                .map(|i| (i.name.clone(), i.fields.clone())),
        )
        .collect();
    // A union has no fields of its own; only `id` is filterable across members.
    for u in &model.unions {
        out.push((u.name.clone(), vec![Field::id_field(u.provenance)]));
    }
    out
}

fn type_filter_input(type_name: &str, fields: &[Field]) -> InputObject {
    let name = filter_input_name(type_name);
    let mut input = InputObject::new(&name).description(format!(
        "Filter over `{type_name}`. Sibling entries are combined with AND; a filter on a \
         multi-valued field holds when any value satisfies it."
    ));
    for f in fields {
        let Some(filter_ty) = field_filter_type(f) else {
            continue;
        };
        input = input.field(InputValue::new(&f.name, TypeRef::named(filter_ty)));
    }
    input
        // Meaningful where this filter sits on a reference field: whether the
        // reference has any value at all. At the root there is nothing to test,
        // and lowering says so rather than quietly ignoring it.
        .field(InputValue::new("EXISTS", TypeRef::named(TypeRef::BOOLEAN)))
        .field(InputValue::new("AND", TypeRef::named_nn_list(&name)))
        .field(InputValue::new("OR", TypeRef::named_nn_list(&name)))
        .field(InputValue::new("NOT", TypeRef::named(&name)))
}

/// The input type a field accepts inside a `where`. Object-valued fields recurse
/// into the target type's filter, which is what makes `friend: { name: ... }` work.
fn field_filter_type(f: &Field) -> Option<String> {
    match &f.ty {
        FieldType::Id => Some(scalar_filter_name(Scalar::Id)),
        FieldType::Scalar(s) => Some(scalar_filter_name(*s)),
        FieldType::Enum(n) => Some(enum_filter_name(n)),
        FieldType::Object(n) | FieldType::Interface(n) | FieldType::Union(n) => {
            Some(filter_input_name(n))
        }
    }
}

fn type_order_input(
    name: String,
    fields: &[Field],
    accept: fn(&Field) -> bool,
    description: String,
) -> InputObject {
    let mut input = InputObject::new(name).description(description);
    for f in fields.iter().filter(|f| accept(f)) {
        input = input.field(InputValue::new(&f.name, TypeRef::named("SortDirection")));
    }
    input
}

fn scalar_filter_input(scalar: Scalar) -> InputObject {
    let t = scalar.type_name();
    let mut input = InputObject::new(scalar_filter_name(scalar))
        .field(InputValue::new("EQ", TypeRef::named(t)))
        .field(InputValue::new("NEQ", TypeRef::named(t)))
        .field(InputValue::new("IN", TypeRef::named_nn_list(t)))
        .field(InputValue::new("NIN", TypeRef::named_nn_list(t)))
        .field(InputValue::new("EXISTS", TypeRef::named(TypeRef::BOOLEAN)));
    if scalar.is_ordered() {
        input = input
            .field(InputValue::new("LT", TypeRef::named(t)))
            .field(InputValue::new("LTE", TypeRef::named(t)))
            .field(InputValue::new("GT", TypeRef::named(t)))
            .field(InputValue::new("GTE", TypeRef::named(t)));
    }
    if scalar.is_textual() {
        // RE/NRE are case-sensitive, IRE/NIRE case-insensitive — GraphDB's naming.
        for op in ["RE", "IRE", "NRE", "NIRE"] {
            input = input.field(InputValue::new(op, TypeRef::named(TypeRef::STRING)));
        }
    }
    input
}

// === Scalar inventory ===

fn scalars_in_use(model: &SchemaModel) -> Vec<Scalar> {
    let mut seen: Vec<Scalar> = vec![Scalar::Id];
    let mut note = |s: Scalar| {
        if !seen.contains(&s) {
            seen.push(s);
        }
    };
    for f in all_fields(model) {
        match f.ty {
            FieldType::Id => note(Scalar::Id),
            FieldType::Scalar(s) => note(s),
            _ => {}
        }
    }
    seen.sort();
    seen
}

fn custom_scalars_in_use(model: &SchemaModel) -> Vec<&'static str> {
    scalars_in_use(model)
        .into_iter()
        .filter(|s| s.is_custom())
        .map(Scalar::type_name)
        .collect()
}

fn all_fields(model: &SchemaModel) -> impl Iterator<Item = &Field> {
    model
        .objects
        .iter()
        .flat_map(|o| o.fields.iter())
        .chain(model.interfaces.iter().flat_map(|i| i.fields.iter()))
}
