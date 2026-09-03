//! GraphQL over a ledger.
//!
//! The schema is derived from what the ledger already holds — no registration
//! step, no uploaded SDL. Tier 1 reads HEAD statistics; tiers 2 and 3 layer SHACL
//! shapes and a `graphql:Schema` on top (not yet implemented here).
//!
//! Execution is one Fluree query per root field. A root resolver hands its entire
//! selection subtree to [`LedgerExecutor`], which lowers it to a JSON-LD query,
//! runs it through the ordinary read path, and reshapes the result — so policy,
//! time travel, reasoning and formatting all behave exactly as they do for any
//! other read.
//!
//! Statistics reach this module through [`NoveltyMerge::Reconciled`]: the schema
//! turns "this class has instances" into "this type exists", so a novelty
//! assertion that restates an already-indexed fact must not resurrect a class
//! whose facts were all retracted (#1391).

use std::collections::BTreeMap;
use std::sync::Arc;

use fluree_db_core::{
    is_rdf_type, FlakeValue, IndexStats, LedgerSnapshot, OverlayProvider, Sid, ValueTypeTag,
};
use fluree_db_graphql::async_graphql;
use fluree_db_graphql::error::{Error as GqlError, Result as GqlResult};
use fluree_db_graphql::lower::{self, reshape};
use fluree_db_graphql::mutate::{self, MutationField, MutationKind, Verb};
use fluree_db_graphql::naming::Namer;
use fluree_db_graphql::runtime::{
    build_schema, ExecutorData, MutationRequest, RootExecutor, RootRequest,
};
use fluree_db_graphql::schema::curated::{CuratedSchema, Exposure};
use fluree_db_graphql::schema::inferred::{ClassObservation, PropertyObservation};
use fluree_db_graphql::schema::model::{Provenance, SchemaModel};
use fluree_db_graphql::schema::shaped::{AllowedValue, ShapeDescription, ShapedProperty};
use fluree_db_graphql::selection;
use fluree_db_novelty::{assemble_fast_stats_with, stats_merge_site, Novelty, NoveltyMerge};
use fluree_db_query::policy::QueryPolicyEnforcer;
use serde_json::{json, Value as JsonValue};

use crate::error::ApiError;
use crate::{Fluree, GraphDb, LedgerState, QueryExecutionOptions, Result};

pub use fluree_db_graphql::limits::{Limits, DEFAULT_MAX_COMPLEXITY, DEFAULT_MAX_DEPTH};

/// A GraphQL request in the usual HTTP envelope.
#[derive(Debug, Clone, Default)]
pub struct GraphQlRequest {
    pub query: String,
    pub variables: Option<JsonValue>,
    pub operation_name: Option<String>,
    /// Return `extensions.explain`: the Fluree query or transaction each root
    /// field lowered to, and where its schema came from.
    ///
    /// This reports what *ran*. It is not a dry run — a mutation still writes —
    /// because silently not writing when the caller asked to see the plan would
    /// be the more surprising behaviour.
    pub explain: bool,
    /// What this document is allowed to ask for.
    ///
    /// Set by whoever runs the endpoint, never by the client: unlike the fields
    /// above, nothing in the HTTP envelope maps here. A server reads it from
    /// configuration; an embedder running its own documents can widen it.
    pub limits: Limits,
}

impl GraphQlRequest {
    pub fn new(query: impl Into<String>) -> Self {
        GraphQlRequest {
            query: query.into(),
            variables: None,
            operation_name: None,
            explain: false,
            limits: Limits::default(),
        }
    }

    /// Set the resource bounds this document must respect.
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_variables(mut self, variables: JsonValue) -> Self {
        self.variables = Some(variables);
        self
    }

    pub fn with_operation_name(mut self, name: impl Into<String>) -> Self {
        self.operation_name = Some(name.into());
        self
    }

    pub fn explained(mut self) -> Self {
        self.explain = true;
        self
    }
}

/// A request whose document has been parsed, once.
///
/// A caller has to know whether the operation writes *before* it can execute
/// it — a mutation needs a `LedgerState`, a read needs a policy view — and
/// answering that means parsing. Parsing again to execute would be the third
/// time over the same string: once to classify, once to extract the selection
/// tree, and once more inside async-graphql. So the document is parsed here and
/// handed on, to `selection::extract` and then to the executor by way of
/// `Request::set_parsed_query`.
pub struct PreparedRequest<'a> {
    request: &'a GraphQlRequest,
    doc: async_graphql::parser::types::ExecutableDocument,
    writes: bool,
}

impl<'a> PreparedRequest<'a> {
    /// Parse and classify a request.
    ///
    /// `Err` is a finished GraphQL error envelope rather than an error type: a
    /// document this rejects is refused the way every other GraphQL failure is,
    /// as a `200` with `errors`, so the caller returns it as-is.
    pub fn new(request: &'a GraphQlRequest) -> std::result::Result<Self, JsonValue> {
        use async_graphql::parser::types::OperationType;

        // Before `parse_query`, not after: pest descends the grammar
        // recursively with no limit of its own, so a deeply nested document
        // overflows the stack and aborts the process before async-graphql's own
        // recursion counter is ever consulted. An abort is not catchable.
        if let Err(e) = fluree_db_graphql::limits::guard_nesting(&request.query) {
            return Err(error_envelope(&e.to_string(), e.code()));
        }
        let doc = async_graphql::parser::parse_query(&request.query)
            .map_err(|e| error_envelope(&e.to_string(), "GRAPHQL_PARSE_FAILED"))?;

        // Decided from the document, not the HTTP method: GraphQL sends
        // everything over `POST`, so the method says nothing about intent.
        let writes = doc.operations.iter().any(|(name, op)| {
            op.node.ty == OperationType::Mutation
                && request
                    .operation_name
                    .as_deref()
                    .is_none_or(|wanted| name.is_none_or(|n| n == wanted))
        });
        Ok(Self {
            request,
            doc,
            writes,
        })
    }

    /// Whether this request's operation writes.
    pub fn writes(&self) -> bool {
        self.writes
    }

    /// The request this was prepared from.
    pub fn request(&self) -> &'a GraphQlRequest {
        self.request
    }
}

// =============================================================================
// Schema derivation
// =============================================================================

/// A ledger view's derived schema, plus the namer that produced its names.
///
/// The two travel together because they answer the same question from opposite
/// ends: the namer turns IRIs into GraphQL names when the schema is built, and
/// turns them back when a query's `id` arguments and results cross the boundary.
#[derive(Debug)]
pub struct DerivedSchema {
    pub model: SchemaModel,
    pub namer: Namer,
    /// The `graphql:Schema` this was built against, when the ledger has one.
    /// Mutations read their `iri_base` and enablement from it.
    pub curated: Option<CuratedSchema>,
}

impl DerivedSchema {
    /// The mutation fields this schema exposes.
    ///
    /// Empty unless a `graphql:Schema` turned them on: a schema derived from
    /// whatever a ledger happens to contain must never become a write surface
    /// by accident.
    pub fn mutations(&self) -> Vec<MutationField> {
        if !self.curated.as_ref().is_some_and(|c| c.mutations) {
            return Vec::new();
        }
        self.model
            .objects
            .iter()
            // The `Node` placeholder names no class to write to.
            .filter(|o| !o.iri.is_empty())
            .flat_map(MutationField::for_type)
            .collect()
    }

    /// Where new subjects are minted, if the schema says.
    pub fn iri_base(&self) -> Option<&str> {
        self.curated.as_ref().and_then(|c| c.iri_base.as_deref())
    }
}

/// Build the ledger's GraphQL schema model from HEAD statistics.
///
/// Applies the caller's view policy by pruning: a class or predicate under an
/// unconditional `Deny` never becomes a type or a field, so it is absent from
/// introspection rather than present-but-empty.
pub async fn schema_model(db: &GraphDb) -> SchemaModel {
    // Callers wanting the namer too should use `derive_schema`, which shares one
    // cached derivation; this keeps the simple entry point.
    match Arc::try_unwrap(derive_schema(db).await) {
        Ok(derived) => derived.model,
        Err(shared) => shared.model.clone(),
    }
}

/// The ledger's schema as SDL, read fields only.
pub async fn schema_sdl(db: &GraphDb) -> Result<String> {
    fluree_db_graphql::sdl(&derive_schema(db).await.model).map_err(to_api_error)
}

/// The ledger's schema as SDL, including mutations when the curated schema
/// enables them.
pub async fn schema_sdl_with_mutations(db: &GraphDb) -> Result<String> {
    let derived = derive_schema(db).await;
    fluree_db_graphql::sdl::sdl_with_mutations(&derived.model, &derived.mutations())
        .map_err(to_api_error)
}

/// Derive the view's schema, reusing a cached derivation when one applies.
///
/// Deriving is not cheap: it walks every class's property statistics, and the
/// novelty merge probes the base index once per `(graph, subject, predicate)` the
/// window touches. A GraphiQL session re-derives it on every keystroke's
/// introspection query, so the cache is what makes the endpoint usable.
pub async fn derive_schema(db: &GraphDb) -> Arc<DerivedSchema> {
    let key = cache_key(db);
    if let Some(key) = &key {
        if let Some(hit) = schema_cache().lock().get(key) {
            return Arc::clone(hit);
        }
    }

    let namer = namer_for(db);
    let curated = curated_schema(db).await;
    let derived = Arc::new(DerivedSchema {
        model: fluree_db_graphql::schema::build::build_curated(
            &observations(db),
            &shapes(db).await,
            curated.as_ref(),
            &namer,
        ),
        namer,
        curated,
    });
    if let Some(key) = key {
        schema_cache().lock().put(key, Arc::clone(&derived));
    }
    derived
}

/// Everything the derived schema depends on, reduced to something comparable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SchemaCacheKey {
    ledger_id: String,
    /// The indexed snapshot: its statistics are the schema's base.
    index_t: i64,
    /// The view's as-of time, which bounds the overlay half of the merge.
    as_of_t: i64,
    /// The overlay's globally-unique content stamp, so novelty writes invalidate.
    overlay_version: u64,
    /// The default context decides every name in the schema.
    context: u64,
}

/// The cache key for a view, or `None` when the derivation must not be reused.
///
/// Two cases decline. An overlay with no `content_version` makes no uniqueness
/// guarantee, so a stale entry could outlive the data it described. And a
/// policy-bearing view prunes the schema by identity: reducing an enforcer to a
/// comparable fingerprint is not something this module can do correctly, and
/// getting it wrong would leak one identity's schema to another. Both fall back
/// to deriving afresh, which is what happens today for every request.
fn cache_key(db: &GraphDb) -> Option<SchemaCacheKey> {
    if db.policy_enforcer().is_some_and(|p| !p.is_root()) {
        return None;
    }
    Some(SchemaCacheKey {
        ledger_id: db.snapshot.ledger_id.clone(),
        index_t: db.snapshot.t,
        as_of_t: db.t,
        overlay_version: db.overlay.content_version()?,
        context: hash_context(db.default_context.as_ref()),
    })
}

fn hash_context(context: Option<&JsonValue>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match context {
        // `serde_json` is built with `preserve_order`, so a context's rendering is
        // stable for a given ledger and this hash is a sound identity.
        Some(ctx) => ctx.to_string().hash(&mut hasher),
        None => 0u8.hash(&mut hasher),
    }
    hasher.finish()
}

type SchemaCache = parking_lot::Mutex<lru::LruCache<SchemaCacheKey, Arc<DerivedSchema>>>;

/// Process-wide, and small: entries are only useful while a ledger sits at one
/// `(index t, overlay version)`, and every write past that makes a new key.
fn schema_cache() -> &'static SchemaCache {
    static CACHE: std::sync::OnceLock<SchemaCache> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| {
        parking_lot::Mutex::new(lru::LruCache::new(
            std::num::NonZeroUsize::new(16).expect("nonzero"),
        ))
    })
}

/// The `Namer` for a view: the ledger's default context supplies the prefixes and
/// `@vocab` that shorten IRIs to the names this ledger's users already write.
fn namer_for(db: &GraphDb) -> Namer {
    let mut prefixes = Vec::new();
    let mut vocab = None;
    if let Some(obj) = db.default_context.as_ref().and_then(JsonValue::as_object) {
        for (key, value) in obj {
            let Some(iri) = value.as_str() else { continue };
            if key == "@vocab" {
                vocab = Some(iri.to_string());
            } else if !key.starts_with('@') {
                prefixes.push((key.clone(), iri.to_string()));
            }
        }
    }
    Namer::new(prefixes, vocab)
}

/// HEAD stats merged with novelty, as class observations in plain IRIs.
fn observations(db: &GraphDb) -> Vec<ClassObservation> {
    let snapshot = db.snapshot.as_ref();
    let stats = merged_stats(snapshot, db.overlay.as_ref(), db.t);
    // Root, or an explicitly unrestricted policy, sees the whole schema.
    let policy = db
        .policy_enforcer()
        .map(std::convert::AsRef::as_ref)
        .filter(|p| !p.is_root());

    let mut out = Vec::new();
    for entry in stats.classes.iter().flatten() {
        if entry.count == 0 {
            continue;
        }
        if policy.is_some_and(|p| crate::cypher_procedures::class_denied(p, &entry.class_sid)) {
            continue;
        }
        let Some(class_iri) = user_iri(&entry.class_sid, snapshot) else {
            continue;
        };

        let mut properties = Vec::new();
        for usage in &entry.properties {
            if is_rdf_type(&usage.property_sid) {
                continue;
            }
            if policy
                .is_some_and(|p| crate::cypher_procedures::predicate_denied(p, &usage.property_sid))
            {
                continue;
            }
            let Some(iri) = user_iri(&usage.property_sid, snapshot) else {
                continue;
            };
            let datatypes: Vec<ValueTypeTag> = usage
                .datatypes
                .iter()
                .filter(|&&(_, count)| count > 0)
                .map(|&(tag, _)| ValueTypeTag::from_u8(tag))
                .collect();
            if datatypes.is_empty() {
                continue;
            }
            let ref_classes: Vec<String> = usage
                .ref_classes
                .iter()
                .filter(|rc| rc.count > 0)
                .filter(|rc| {
                    !policy
                        .is_some_and(|p| crate::cypher_procedures::class_denied(p, &rc.class_sid))
                })
                .filter_map(|rc| user_iri(&rc.class_sid, snapshot))
                .collect();
            properties.push(PropertyObservation {
                iri,
                datatypes,
                has_language_tags: !usage.langs.is_empty(),
                ref_classes,
            });
        }

        out.push(ClassObservation {
            iri: class_iri,
            count: entry.count,
            properties,
        });
    }
    out
}

// =============================================================================
// Tier 3: the curated schema
// =============================================================================

/// The ledger's `graphql:Schema`, if it has one.
///
/// Read straight from the graph rather than through the SHACL compiler: these
/// are not shape constraints, and a `graphql:` flake does not move
/// `shacl_epoch`. The derivation cache keys on the overlay's content version,
/// which any write moves, so editing a curated schema invalidates it.
///
/// A ledger with several `graphql:Schema` instances is ambiguous, and guessing
/// which one to serve would be worse than serving none — so it falls back to
/// tier 2 with a warning until a caller names one.
async fn curated_schema(db: &GraphDb) -> Option<CuratedSchema> {
    let snapshot = db.snapshot.as_ref();

    // Nothing to read unless the ledger has seen the vocabulary at all: the
    // namespace is absent from its table, so no flake can mention it.
    snapshot.encode_iri_strict(fluree_vocab::graphql::PUBLIC_SHAPE)?;

    let shapes = compiled_shapes(db).await;
    let mut schemas: BTreeMap<Sid, CuratedSchema> = BTreeMap::new();

    for (predicate_iri, exposure) in [
        (fluree_vocab::graphql::PUBLIC_SHAPE, Exposure::Public),
        (fluree_vocab::graphql::PROTECTED_SHAPE, Exposure::Protected),
        (fluree_vocab::graphql::PRIVATE_SHAPE, Exposure::Private),
    ] {
        let Some(predicate) = snapshot.encode_iri_strict(predicate_iri) else {
            continue;
        };
        for flake in predicate_flakes(db, &predicate).await {
            let FlakeValue::Ref(shape_id) = &flake.o else {
                continue;
            };
            let entry = schemas.entry(flake.s.clone()).or_default();
            for class_iri in shape_target_classes(shapes.as_deref(), shape_id, snapshot) {
                entry.exposure.insert(class_iri, exposure);
            }
        }
    }

    if schemas.is_empty() {
        return None;
    }
    if schemas.len() > 1 {
        tracing::warn!(
            count = schemas.len(),
            "several graphql:Schema instances found; GraphQL fell back to the shaped schema"
        );
        return None;
    }
    let (schema_subject, mut curated) = schemas.into_iter().next().expect("len checked");

    // Scalar settings on the schema node itself.
    for flake in subject_flakes(db, &schema_subject).await {
        let Some(iri) = user_iri_unfiltered(&flake.p, snapshot) else {
            continue;
        };
        match iri.as_str() {
            fluree_vocab::graphql::NAME => {
                if let FlakeValue::String(name) = &flake.o {
                    curated.name = Some(name.clone());
                }
            }
            fluree_vocab::graphql::ENABLE_MUTATIONS => {
                curated.mutations = matches!(flake.o, FlakeValue::Boolean(true));
            }
            fluree_vocab::graphql::IRI_BASE => {
                if let FlakeValue::String(base) = &flake.o {
                    curated.iri_base = Some(base.clone());
                }
            }
            _ => {}
        }
    }

    // Per-shape settings: `graphql:name`, `graphql:isInterface`, plural name.
    for (predicate_iri, kind) in [
        (fluree_vocab::graphql::NAME, ShapeSetting::Name),
        (fluree_vocab::graphql::IS_INTERFACE, ShapeSetting::Interface),
        (fluree_vocab::graphql::PLURAL_NAME, ShapeSetting::PluralName),
    ] {
        let Some(predicate) = snapshot.encode_iri_strict(predicate_iri) else {
            continue;
        };
        for flake in predicate_flakes(db, &predicate).await {
            if flake.s == schema_subject {
                continue; // Already read above, as a schema-level setting.
            }
            for class_iri in shape_target_classes(shapes.as_deref(), &flake.s, snapshot) {
                match (kind, &flake.o) {
                    (ShapeSetting::Name, FlakeValue::String(v)) => {
                        curated.type_names.insert(class_iri, v.clone());
                    }
                    (ShapeSetting::PluralName, FlakeValue::String(v)) => {
                        curated.plural_names.insert(class_iri, v.clone());
                    }
                    (ShapeSetting::Interface, FlakeValue::Boolean(true)) => {
                        curated.interfaces.push(class_iri);
                    }
                    _ => {}
                }
            }
        }
    }
    curated.interfaces.sort();
    curated.interfaces.dedup();

    // The hierarchy stores descendants, which is the direction this needs: for
    // each interface, the classes beneath it are its implementors.
    if let Some(hierarchy) = novelty_aware_hierarchy(db).await {
        for interface_iri in &curated.interfaces {
            let Some(interface_sid) = snapshot.encode_iri_strict(interface_iri) else {
                continue;
            };
            let members: Vec<String> = hierarchy
                .subclasses_of(&interface_sid)
                .iter()
                .filter_map(|sid| user_iri(sid, snapshot))
                .collect();
            curated
                .interface_members
                .insert(interface_iri.clone(), members);
        }
    }

    Some(curated)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShapeSetting {
    Name,
    PluralName,
    Interface,
}

/// The class IRIs a node shape targets.
fn shape_target_classes(
    shapes: Option<&fluree_db_shacl::ShaclCache>,
    shape_id: &Sid,
    snapshot: &LedgerSnapshot,
) -> Vec<String> {
    let Some(shapes) = shapes else {
        return Vec::new();
    };
    shapes
        .all_shapes()
        .iter()
        .filter(|s| &s.id == shape_id)
        .flat_map(|s| s.targets.iter())
        .filter_map(|t| match t {
            fluree_db_shacl::TargetType::Class(sid)
            | fluree_db_shacl::TargetType::ImplicitClass(sid) => user_iri(sid, snapshot),
            _ => None,
        })
        .collect()
}

/// The class hierarchy including subclass edges written since the last index
/// build.
///
/// `LedgerSnapshot::schema_hierarchy` is index-only, so on a ledger whose
/// `rdfs:subClassOf` edges are still in novelty it reports nothing — which
/// would silently drop every interface's implementors.
async fn novelty_aware_hierarchy(db: &GraphDb) -> Option<fluree_db_core::SchemaHierarchy> {
    fluree_db_core::compute_schema_hierarchy_with_overlay(&db.snapshot, db.overlay.as_ref(), db.t)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "class hierarchy unavailable; interfaces will have no implementors");
            None
        })
}

async fn predicate_flakes(db: &GraphDb, predicate: &Sid) -> Vec<fluree_db_core::Flake> {
    db.as_graph_db_ref()
        .range(
            fluree_db_core::IndexType::Psot,
            fluree_db_core::RangeTest::Eq,
            fluree_db_core::RangeMatch::predicate(predicate.clone()),
        )
        .await
        .unwrap_or_default()
}

async fn subject_flakes(db: &GraphDb, subject: &Sid) -> Vec<fluree_db_core::Flake> {
    db.as_graph_db_ref()
        .range(
            fluree_db_core::IndexType::Spot,
            fluree_db_core::RangeTest::Eq,
            fluree_db_core::RangeMatch::subject(subject.clone()),
        )
        .await
        .unwrap_or_default()
}

/// A SID's IRI without the Fluree-vocabulary filter, for reading configuration
/// predicates (which are deliberately in Fluree's own namespace).
fn user_iri_unfiltered(sid: &Sid, snapshot: &LedgerSnapshot) -> Option<String> {
    let prefix = snapshot.namespaces().get(&sid.namespace_code)?;
    Some(format!("{}{}", prefix, sid.name))
}

// =============================================================================
// Tier 2: SHACL shapes
// =============================================================================

/// The ledger's class-targeting node shapes, as the builder's plain-IRI view.
///
/// `None` shapes — an unresolvable path, a policy-denied class, a sequence or
/// alternative path — are dropped rather than approximated: a GraphQL field is a
/// contract, and one derived from a path the engine could not compile would be a
/// promise nothing can keep.
async fn shapes(db: &GraphDb) -> Vec<ShapeDescription> {
    let Some(cache) = compiled_shapes(db).await else {
        return Vec::new();
    };
    let snapshot = db.snapshot.as_ref();
    let policy = db
        .policy_enforcer()
        .map(std::convert::AsRef::as_ref)
        .filter(|p| !p.is_root());

    let mut out = Vec::new();
    for shape in cache.all_shapes() {
        if shape.deactivated {
            continue;
        }
        for target in &shape.targets {
            use fluree_db_shacl::TargetType;
            let class_sid = match target {
                TargetType::Class(sid) | TargetType::ImplicitClass(sid) => sid,
                // Only class targets describe a GraphQL type. A `sh:targetNode`
                // or `sh:targetSubjectsOf` shape constrains a set of subjects
                // that is not a class, so it has no type to become.
                _ => continue,
            };
            if policy.is_some_and(|p| crate::cypher_procedures::class_denied(p, class_sid)) {
                continue;
            }
            let Some(class_iri) = user_iri(class_sid, snapshot) else {
                continue;
            };
            out.push(ShapeDescription {
                class_iri,
                name: shape.name.clone(),
                description: shape.description.clone(),
                closed: shape_is_closed(shape),
                properties: shaped_properties(shape, snapshot, policy),
            });
        }
    }
    // The builder keeps the first shape per class, so order has to be stable.
    out.sort_by(|a, b| a.class_iri.cmp(&b.class_iri));
    out
}

fn shape_is_closed(shape: &fluree_db_shacl::CompiledShape) -> bool {
    shape.structural_constraints.iter().any(|c| {
        matches!(
            c,
            fluree_db_shacl::NodeConstraint::Closed {
                is_closed: true,
                ..
            }
        )
    })
}

fn shaped_properties(
    shape: &fluree_db_shacl::CompiledShape,
    snapshot: &LedgerSnapshot,
    policy: Option<&QueryPolicyEnforcer>,
) -> Vec<ShapedProperty> {
    use fluree_db_graphql::schema::model::Direction;
    use fluree_db_shacl::{Constraint, NodeKind, PropertyPath};

    let mut out = Vec::new();
    for ps in &shape.property_shapes {
        // Only a single predicate, forwards or backwards, is a GraphQL field. A
        // sequence or alternative path names no one predicate to read or write.
        let (predicate, direction) = match &ps.path {
            PropertyPath::Predicate(sid) => (sid, Direction::Forward),
            PropertyPath::Inverse(sid) => (sid, Direction::Reverse),
            _ => continue,
        };
        if policy.is_some_and(|p| crate::cypher_procedures::predicate_denied(p, predicate)) {
            continue;
        }
        let Some(iri) = user_iri(predicate, snapshot) else {
            continue;
        };

        let mut property = ShapedProperty {
            iri,
            direction,
            name: ps.name.clone(),
            description: ps.description.clone(),
            order: ps.order,
            ..Default::default()
        };
        for constraint in &ps.constraints {
            match constraint {
                Constraint::MinCount(n) => property.min_count = Some(*n),
                Constraint::MaxCount(n) => property.max_count = Some(*n),
                Constraint::Datatype(sid) => property.datatype = user_iri(sid, snapshot),
                Constraint::Class(sid) => property.class = user_iri(sid, snapshot),
                Constraint::NodeKind(kind) => {
                    property.node_kind_is_iri = matches!(
                        kind,
                        NodeKind::IRI | NodeKind::BlankNodeOrIRI | NodeKind::BlankNode
                    );
                }
                Constraint::In(values) => {
                    property.allowed_values = values
                        .iter()
                        .filter_map(|v| allowed_value(v, snapshot))
                        .collect();
                    // A member that could not be rendered would silently shrink
                    // the domain, so an incomplete set is no set at all.
                    if property.allowed_values.len() != values.len() {
                        property.allowed_values.clear();
                    }
                }
                _ => {}
            }
        }
        out.push(property);
    }
    out
}

/// One `sh:in` member as the builder sees it.
fn allowed_value(
    value: &fluree_db_core::FlakeValue,
    snapshot: &LedgerSnapshot,
) -> Option<AllowedValue> {
    use fluree_db_core::FlakeValue;
    match value {
        FlakeValue::Ref(sid) => user_iri(sid, snapshot).map(AllowedValue::Iri),
        FlakeValue::String(s) => Some(AllowedValue::String(s.clone())),
        // Numeric and boolean members are a legitimate `sh:in`, but a GraphQL
        // enum's members are names; `42` is not one. The field keeps its
        // datatype instead.
        _ => None,
    }
}

/// The ledger's compiled shapes, from a process-wide cache.
///
/// Compilation reads every `sh:*` flake in the shapes graphs, so it is not
/// something to redo per request. The key adds `shacl_epoch` to the schema
/// cache's: that epoch is exactly "a shape-affecting flake was committed", which
/// is what the write path already keys its own compile cache on.
async fn compiled_shapes(db: &GraphDb) -> Option<Arc<fluree_db_shacl::ShaclCache>> {
    let key = shape_cache_key(db);
    if let Some(key) = &key {
        if let Some(hit) = shape_cache().lock().get(key) {
            return Some(Arc::clone(hit));
        }
    }
    let compiled = compile_shapes(db).await?;
    if let Some(key) = key {
        shape_cache().lock().put(key, Arc::clone(&compiled));
    }
    Some(compiled)
}

/// Compile shapes from the view's shapes graphs.
///
/// This reads the index, which is why the whole derivation path is `async`:
/// blocking a runtime worker on it would risk starving the very executor the
/// reads need to make progress.
async fn compile_shapes(db: &GraphDb) -> Option<Arc<fluree_db_shacl::ShaclCache>> {
    let g_ids = crate::tx::resolve_shapes_source_g_ids(db.ledger_config(), &db.snapshot)
        .unwrap_or_else(|_| vec![0]);
    let views: Vec<GraphDb> = g_ids
        .iter()
        .map(|g_id| db.clone().with_graph_id(*g_id))
        .collect();
    let refs: Vec<fluree_db_core::GraphDbRef<'_>> =
        views.iter().map(GraphDb::as_graph_db_ref).collect();

    // The snapshot's own hierarchy reflects the last index build only, so a
    // subclass edge written since would not expand shape targeting.
    let hierarchy = novelty_aware_hierarchy(db).await;
    let engine = fluree_db_shacl::ShaclEngine::from_dbs_with_hierarchy(
        &refs,
        db.snapshot.ledger_id.clone(),
        hierarchy,
    )
    .await;
    match engine {
        Ok(engine) => Some(engine.shared_cache()),
        Err(e) => {
            // A ledger whose shapes will not compile still has a schema — the
            // inferred one. Refusing to serve GraphQL over it would be a worse
            // answer than serving the tier below.
            tracing::warn!(error = %e, "SHACL shapes did not compile; GraphQL fell back to the inferred schema");
            None
        }
    }
}

type ShapeCache =
    parking_lot::Mutex<lru::LruCache<ShapeCacheKey, Arc<fluree_db_shacl::ShaclCache>>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ShapeCacheKey {
    ledger_id: String,
    index_t: i64,
    shacl_epoch: u64,
    /// The compiled shapes bake in subclass expansion, so a hierarchy change
    /// invalidates them even when no `sh:*` flake moved.
    schema_epoch: u64,
}

fn shape_cache() -> &'static ShapeCache {
    static CACHE: std::sync::OnceLock<ShapeCache> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| {
        parking_lot::Mutex::new(lru::LruCache::new(
            std::num::NonZeroUsize::new(8).expect("nonzero"),
        ))
    })
}

fn shape_cache_key(db: &GraphDb) -> Option<ShapeCacheKey> {
    let novelty = db.novelty_for_stats()?;
    Some(ShapeCacheKey {
        ledger_id: db.snapshot.ledger_id.clone(),
        index_t: db.snapshot.t,
        shacl_epoch: novelty.shacl_epoch,
        schema_epoch: novelty.schema_epoch,
    })
}

/// A SID's IRI, or `None` for Fluree's own vocabulary — commit metadata and
/// edge-annotation reifiers are not part of anyone's data model.
fn user_iri(sid: &Sid, snapshot: &LedgerSnapshot) -> Option<String> {
    let prefix = snapshot.namespaces().get(&sid.namespace_code)?;
    let iri = format!("{}{}", prefix, sid.name);
    (!iri.starts_with("https://ns.flur.ee/")).then_some(iri)
}

/// HEAD-index stats merged with the overlay up to `to_t`.
///
/// The novelty half honours the view's as-of time, so a time-traveled view does
/// not see a class created after it. The indexed half cannot: `IndexStats` is a
/// snapshot of the last index build with no historical form, so a class whose
/// every fact predates that build stays in the schema regardless of `to_t`. The
/// schema is therefore a superset for a time-traveled view — queries against a
/// type that did not yet exist return nothing rather than misreporting.
fn merged_stats(snapshot: &LedgerSnapshot, overlay: &dyn OverlayProvider, to_t: i64) -> IndexStats {
    let indexed = snapshot.stats.clone().unwrap_or_default();
    match overlay.as_any().downcast_ref::<Novelty>() {
        Some(novelty) => assemble_fast_stats_with(
            &indexed,
            snapshot,
            novelty,
            to_t,
            None,
            NoveltyMerge::Reconciled {
                site: stats_merge_site::GRAPHQL_SCHEMA,
            },
        ),
        None => indexed,
    }
}

// =============================================================================
// Execution
// =============================================================================

/// Runs a root field's selection subtree against a ledger view.
struct LedgerExecutor {
    fluree: Fluree,
    db: GraphDb,
    schema: Arc<DerivedSchema>,
    /// The ledger being written, for a mutation request. `None` on the
    /// read-only path, which is what makes a read endpoint structurally
    /// incapable of writing rather than merely disinclined to.
    ///
    /// GraphQL executes root mutation fields serially, so each takes the
    /// ledger, commits, and puts the new state back for the next.
    ledger: Option<parking_lot::Mutex<Option<LedgerState>>>,
    /// What each root field lowered to, collected when `explain` was asked for.
    /// `None` costs nothing on the ordinary path.
    explain: Option<parking_lot::Mutex<Vec<JsonValue>>>,
    /// Cancellation and timeout controls for every query this request runs.
    ///
    /// GraphQL resolves root fields concurrently, so one document can launch
    /// many queries; they share one handle so a timeout or a client disconnect
    /// cancels all of them, not whichever happens to check next.
    options: QueryExecutionOptions,
}

impl LedgerExecutor {
    /// Record one root field's plan, if this request is explaining.
    fn record_plan(&self, field: &str, provenance: Provenance, kind: &str, body: &JsonValue) {
        let Some(log) = self.explain.as_ref() else {
            return;
        };
        log.lock().push(json!({
            "field": field,
            "provenance": provenance_name(provenance),
            kind: body,
        }));
    }
}

fn provenance_name(provenance: Provenance) -> &'static str {
    match provenance {
        Provenance::Inferred => "inferred",
        Provenance::Shaped => "shaped",
        Provenance::Curated => "curated",
    }
}

#[async_trait::async_trait]
impl RootExecutor for LedgerExecutor {
    async fn resolve(&self, request: RootRequest) -> GqlResult<JsonValue> {
        let lowered = lower::lower(
            &self.schema.model,
            &request.root,
            &request.selection,
            &self.schema.namer,
        )?;
        self.record_plan(
            &request.root.name,
            request.root.provenance,
            "query",
            &lowered.query,
        );
        let result = self
            .fluree
            .query_with_options(&self.db, &lowered.query, self.options.clone())
            .await
            .map_err(|e| GqlError::Execution(e.to_string()))?;
        let rows = result
            .to_jsonld_async(self.db.as_graph_db_ref())
            .await
            .map_err(|e| GqlError::Execution(e.to_string()))?;
        reshape::reshape(
            &lowered.shape,
            &self.schema.model,
            &self.schema.namer,
            &rows,
        )
    }

    async fn mutate(&self, request: MutationRequest) -> GqlResult<JsonValue> {
        let slot = self.ledger.as_ref().ok_or_else(|| {
            GqlError::Lower(format!(
                "`{}` is a mutation, and this endpoint is read-only",
                request.field.name
            ))
        })?;

        let lowered = mutate::lower(
            &self.schema.model,
            &request.field,
            &request.selection,
            &self.schema.namer,
            self.schema.iri_base(),
        )?;
        self.record_plan(
            &request.field.name,
            Provenance::Curated,
            "transaction",
            &lowered.transaction,
        );

        let ledger = slot.lock().take().ok_or_else(|| {
            GqlError::Execution(
                "an earlier mutation in this request failed, so there is no ledger state left"
                    .to_string(),
            )
        })?;
        // The write APIs consume the ledger, and a rejected write does not hand
        // it back. `LedgerState` is `Arc`-backed, so keeping a copy costs
        // nothing and means a refused mutation leaves the caller exactly where
        // it started rather than destroying the state a later field needs.
        let unchanged = ledger.clone();
        let result = match lowered.verb {
            Verb::Insert => self.fluree.insert(ledger, &lowered.transaction).await,
            Verb::Upsert => self.fluree.upsert(ledger, &lowered.transaction).await,
            Verb::Update => self.fluree.update(ledger, &lowered.transaction).await,
        };
        // A SHACL violation, a policy denial, or any other rejection arrives
        // here as an ordinary transaction error and becomes a GraphQL error.
        // Nothing on this path can bypass them: it is the same write API any
        // other client uses.
        let result = result.map_err(|e| {
            slot.lock().replace(unchanged);
            GqlError::Execution(e.to_string())
        })?;
        let committed = result.ledger;
        let view = GraphDb::from_ledger_state(&committed)
            .with_default_context(self.db.default_context.clone());
        slot.lock().replace(committed);

        self.read_back(&view, &request, &lowered.subjects).await
    }
}

impl LedgerExecutor {
    /// Read the mutated subjects back through the ordinary query path, so what
    /// a mutation returns is what a query would have returned.
    async fn read_back(
        &self,
        view: &GraphDb,
        request: &MutationRequest,
        subjects: &[String],
    ) -> GqlResult<JsonValue> {
        let field = &request.field;
        // A delete has nothing to read back: the subjects are gone.
        if field.kind == MutationKind::Delete {
            return Ok(json!({
                "affected_count": subjects.len(),
                "affected_objects": []
            }));
        }

        // `create` returns the object itself; `update` returns it nested under
        // `affected_objects`, so the selection to read differs by verb.
        let mut selection = match field.kind {
            MutationKind::Create => request.selection.clone(),
            _ => request
                .selection
                .children
                .iter()
                .find(|c| c.name == "affected_objects")
                .cloned()
                .unwrap_or_else(|| empty_selection("affected_objects")),
        };
        // The mutation's own arguments (`input`, `ids`, `set`) mean nothing to a
        // read; `restrict_to_subjects` below is what scopes it.
        selection.arguments.clear();

        let root = fluree_db_graphql::schema::model::RootField {
            name: field.name.clone(),
            class_iri: field.class_iri.clone(),
            type_name: field.type_name.clone(),
            kind: fluree_db_graphql::schema::model::RootKind::List,
            description: None,
            provenance: fluree_db_graphql::schema::model::Provenance::Curated,
        };
        let mut lowered = lower::lower(&self.schema.model, &root, &selection, &self.schema.namer)?;
        // Scope the read to exactly the subjects this mutation touched.
        restrict_to_subjects(&mut lowered.query, subjects);

        let result = self
            .fluree
            .query_with_options(view, &lowered.query, self.options.clone())
            .await
            .map_err(|e| GqlError::Execution(e.to_string()))?;
        let rows = result
            .to_jsonld_async(view.as_graph_db_ref())
            .await
            .map_err(|e| GqlError::Execution(e.to_string()))?;
        let objects = reshape::reshape(
            &lowered.shape,
            &self.schema.model,
            &self.schema.namer,
            &rows,
        )?;

        Ok(match field.kind {
            MutationKind::Create => objects
                .as_array()
                .and_then(|a| a.first().cloned())
                .unwrap_or(JsonValue::Null),
            _ => {
                let count = objects.as_array().map_or(0, Vec::len);
                json!({ "affected_count": count, "affected_objects": objects })
            }
        })
    }
}

/// A selection that asks for nothing, for a mutation whose caller selected no
/// `affected_objects`.
fn empty_selection(name: &str) -> fluree_db_graphql::selection::Selection {
    fluree_db_graphql::selection::Selection {
        response_key: name.to_string(),
        name: name.to_string(),
        arguments: Vec::new(),
        type_condition: None,
        children: Vec::new(),
    }
}

/// Bind the read-back query's root variable to exactly `subjects`.
///
/// The lowered read is a class-scoped list; a mutation must report on the rows
/// it touched, not on every instance of the type.
fn restrict_to_subjects(query: &mut JsonValue, subjects: &[String]) {
    let Some(patterns) = query.get_mut("where").and_then(JsonValue::as_array_mut) else {
        return;
    };
    let subject_var = patterns
        .first()
        .and_then(|p| p.get("@id"))
        .and_then(JsonValue::as_str)
        .unwrap_or("?_gql0")
        .to_string();
    patterns.push(json!([
        "values",
        [
            subject_var,
            subjects
                .iter()
                .map(|iri| json!({ "@id": iri }))
                .collect::<Vec<_>>()
        ]
    ]));
}

impl Fluree {
    /// Execute a GraphQL request against a ledger view.
    ///
    /// Returns the GraphQL response envelope (`{"data": …}`, plus `"errors"` when
    /// anything failed) rather than a `Result`: a GraphQL error is part of the
    /// response body, not a transport failure. Only errors that prevent a
    /// response at all — an unschematisable ledger — come back as `Err`.
    pub async fn graphql(&self, db: &GraphDb, request: &GraphQlRequest) -> Result<JsonValue> {
        match PreparedRequest::new(request) {
            Ok(prepared) => {
                self.graphql_with_options(db, prepared, QueryExecutionOptions::default())
                    .await
            }
            Err(envelope) => Ok(envelope),
        }
    }

    /// Execute a GraphQL request with explicit execution controls.
    ///
    /// One document can resolve many root fields concurrently, so the options —
    /// and in particular the cancellation handle — are shared across every query
    /// the request runs. Without this a server has no way to bound a GraphQL
    /// read: the cancellation handle it installs for every other read surface
    /// cannot reach the queries this lowers to.
    pub async fn graphql_with_options(
        &self,
        db: &GraphDb,
        prepared: PreparedRequest<'_>,
        options: QueryExecutionOptions,
    ) -> Result<JsonValue> {
        self.run_graphql(db, prepared, None, options).await
    }

    /// Execute a GraphQL request that may write.
    ///
    /// Separate from [`Fluree::graphql`] because writing needs a `LedgerState`,
    /// which a read view does not carry — so a read endpoint is structurally
    /// incapable of mutating rather than merely declining to. Returns the
    /// response envelope alongside the ledger state after any commits; on a
    /// pure query the state comes back untouched.
    ///
    /// `default_context` is explicit rather than looked up, because it decides
    /// both the GraphQL field names and the IRIs a mutation *writes* — so it has
    /// to be the same context the caller's reads used. Pass what
    /// [`Fluree::get_default_context`] returns for the ordinary case.
    ///
    /// Mutations still require the ledger's `graphql:Schema` to enable them.
    pub async fn graphql_transact(
        &self,
        ledger: LedgerState,
        default_context: Option<JsonValue>,
        request: &GraphQlRequest,
    ) -> Result<(JsonValue, LedgerState)> {
        match PreparedRequest::new(request) {
            Ok(prepared) => {
                self.graphql_transact_with_options(
                    ledger,
                    default_context,
                    prepared,
                    QueryExecutionOptions::default(),
                )
                .await
            }
            Err(envelope) => Ok((envelope, ledger)),
        }
    }

    /// [`Fluree::graphql_transact`] with explicit execution controls.
    ///
    /// The options bound the *reads* — the read-back after each commit, and any
    /// query fields in the same document. A write is not cancellable once the
    /// transaction is handed to the write path.
    pub async fn graphql_transact_with_options(
        &self,
        ledger: LedgerState,
        default_context: Option<JsonValue>,
        prepared: PreparedRequest<'_>,
        options: QueryExecutionOptions,
    ) -> Result<(JsonValue, LedgerState)> {
        let db = GraphDb::from_ledger_state(&ledger).with_default_context(default_context);
        let slot = parking_lot::Mutex::new(Some(ledger));
        let envelope = self
            .run_graphql(&db, prepared, Some(&slot), options)
            .await?;
        let ledger = slot.lock().take().ok_or_else(|| {
            ApiError::Internal("the ledger state was consumed by a failed mutation".to_string())
        })?;
        Ok((envelope, ledger))
    }

    async fn run_graphql(
        &self,
        db: &GraphDb,
        prepared: PreparedRequest<'_>,
        ledger: Option<&parking_lot::Mutex<Option<LedgerState>>>,
        options: QueryExecutionOptions,
    ) -> Result<JsonValue> {
        let PreparedRequest { request, doc, .. } = prepared;
        let derived = derive_schema(db).await;
        if derived.model.query_fields.is_empty() {
            return Ok(error_envelope(
                "this ledger has no classes to expose; write some typed data first",
                "EMPTY_SCHEMA",
            ));
        }
        // Only a writing caller gets the write surface, so a read endpoint does
        // not even advertise mutations it could not run.
        let mutations = if ledger.is_some() {
            derived.mutations()
        } else {
            Vec::new()
        };

        let executor = Arc::new(LedgerExecutor {
            fluree: self.clone(),
            db: db.clone(),
            schema: Arc::clone(&derived),
            ledger: ledger.map(|slot| parking_lot::Mutex::new(slot.lock().take())),
            explain: request.explain.then(|| parking_lot::Mutex::new(Vec::new())),
            options,
        });
        // Registering the schema costs thousands of allocations — 2.5 ms for a
        // hundred-class ledger — so it is cached beside the model rather than
        // rebuilt per request. The resolvers read the per-request executor from
        // the request's data, which is what lets one registration be shared.
        let schema = registered_schema(db, &derived, &mutations, &request.limits)?;

        // The selection tree is extracted from the parsed document, not from the
        // resolver context, because async-graphql's resolver-facing selection API
        // discards fragment type conditions. See `fluree_db_graphql::selection`.
        let variables = request
            .variables
            .clone()
            .map_or_else(async_graphql::Variables::default, |v| {
                async_graphql::Variables::from_json(v)
            });
        let operation = match selection::extract(
            &doc,
            request.operation_name.as_deref(),
            &variables,
            &request.limits,
        ) {
            Ok(op) => op,
            Err(e) => return Ok(error_envelope(&e.to_string(), e.code())),
        };

        let mut req = async_graphql::Request::new(&request.query)
            .variables(variables)
            .data(Arc::new(operation))
            .data(Arc::clone(&executor) as ExecutorData);
        if let Some(name) = &request.operation_name {
            req = req.operation_name(name);
        }
        // Hand over the document already parsed. `execute` uses it as-is and
        // validation — the depth and complexity limits included — still runs on
        // it, so this saves the parse without weakening anything.
        req.set_parsed_query(doc);
        let mut response = to_envelope(schema.execute(req).await);
        // Hand the (possibly advanced) ledger back to the caller's slot.
        if let (Some(caller), Some(mine)) = (ledger, executor.ledger.as_ref()) {
            *caller.lock() = mine.lock().take();
        }
        if let Some(log) = executor.explain.as_ref() {
            attach_explain(&mut response, &derived, log.lock().clone());
        }
        Ok(response)
    }

    /// The ledger view's GraphQL schema, as SDL.
    ///
    /// Read-only: mutation fields appear only on the writing entry point, so
    /// the SDL a read endpoint serves matches what it can actually answer. Use
    /// [`schema_sdl_with_mutations`] for the write surface.
    pub async fn graphql_sdl(&self, db: &GraphDb) -> Result<String> {
        schema_sdl(db).await
    }
}

/// async-graphql's response → the wire envelope.
fn to_envelope(response: async_graphql::Response) -> JsonValue {
    let mut envelope = serde_json::Map::new();
    envelope.insert(
        "data".to_string(),
        response.data.into_json().unwrap_or(JsonValue::Null),
    );
    if !response.errors.is_empty() {
        envelope.insert(
            "errors".to_string(),
            JsonValue::Array(
                response
                    .errors
                    .iter()
                    .map(|e| serde_json::to_value(e).unwrap_or(JsonValue::Null))
                    .collect(),
            ),
        );
    }
    JsonValue::Object(envelope)
}

/// The registered executable schema for a view, from a process-wide cache.
///
/// Keyed on the derivation's own key plus whether mutations are registered, so
/// a read request and a write request against the same ledger version do not
/// share one — the read schema deliberately has no `Mutation` type.
fn registered_schema(
    db: &GraphDb,
    derived: &DerivedSchema,
    mutations: &[fluree_db_graphql::mutate::MutationField],
    limits: &Limits,
) -> Result<async_graphql::dynamic::Schema> {
    let key = cache_key(db).map(|schema| RegisteredKey {
        schema,
        mutations: !mutations.is_empty(),
        limits: *limits,
    });
    if let Some(key) = &key {
        if let Some(hit) = registered_cache().lock().get(key) {
            return Ok(hit.clone());
        }
    }
    let schema = build_schema(&derived.model, mutations, limits).map_err(to_api_error)?;
    if let Some(key) = key {
        registered_cache().lock().put(key, schema.clone());
    }
    Ok(schema)
}

/// The limits are part of the key: they are baked into the registered schema,
/// so a request configured with tighter bounds must not be served one built
/// with looser ones.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RegisteredKey {
    schema: SchemaCacheKey,
    mutations: bool,
    limits: Limits,
}

type RegisteredCache =
    parking_lot::Mutex<lru::LruCache<RegisteredKey, async_graphql::dynamic::Schema>>;

fn registered_cache() -> &'static RegisteredCache {
    static CACHE: std::sync::OnceLock<RegisteredCache> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| {
        parking_lot::Mutex::new(lru::LruCache::new(
            std::num::NonZeroUsize::new(16).expect("nonzero"),
        ))
    })
}

/// Attach `extensions.explain` to a response.
///
/// The plans are what actually ran, in execution order — for a query, the
/// JSON-LD it lowered to; for a mutation, the transaction that was committed.
/// A field that errored before lowering simply has no entry, which is itself
/// informative.
fn attach_explain(response: &mut JsonValue, derived: &DerivedSchema, plans: Vec<JsonValue>) {
    let Some(envelope) = response.as_object_mut() else {
        return;
    };
    let explain = json!({
        "tier": tier_name(derived),
        // Approximations the schema had to make. Empty is the good case.
        "warnings": derived.model.warnings,
        "fields": plans,
    });
    envelope
        .entry("extensions")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .expect("extensions object")
        .insert("explain".to_string(), explain);
}

/// Which tier produced this schema.
fn tier_name(derived: &DerivedSchema) -> &'static str {
    if derived.curated.is_some() {
        "curated"
    } else if derived
        .model
        .objects
        .iter()
        .any(|o| o.provenance == Provenance::Shaped)
    {
        "shaped"
    } else {
        "inferred"
    }
}

fn error_envelope(message: &str, code: &str) -> JsonValue {
    json!({
        "data": null,
        "errors": [{ "message": message, "extensions": { "code": code } }]
    })
}

fn to_api_error(e: GqlError) -> ApiError {
    ApiError::Query(fluree_db_query::QueryError::InvalidQuery(e.to_string()))
}
