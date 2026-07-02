//! SHACL validation engine
//!
//! This module provides the core validation logic for checking RDF data
//! against SHACL shapes.

use crate::cache::{ShaclCache, ShaclCacheKey};
use crate::compile::{CompiledShape, PropertyShape, Severity, ShapeCompiler, ShapeId, TargetType};
use crate::constraints::cardinality::{validate_max_count, validate_min_count};
use crate::constraints::datatype::{validate_datatype, validate_node_kind};
use crate::constraints::lang::{validate_language_in, validate_unique_lang};
use crate::constraints::pattern::{validate_max_length, validate_min_length, validate_pattern};
use crate::constraints::value::{
    validate_has_value, validate_in, validate_max_exclusive, validate_max_inclusive,
    validate_min_exclusive, validate_min_inclusive,
};
use crate::constraints::{Constraint, ConstraintViolation, NestedShape, NodeConstraint};
use crate::error::Result;
use fluree_db_core::{
    FlakeValue, GraphDbRef, GraphId, IndexType, LedgerSnapshot, NoOverlay, RangeMatch, RangeTest,
    SchemaHierarchy, Sid,
};
use fluree_vocab::namespaces::{BLANK_NODE, RDF};
use fluree_vocab::rdf_names;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};

/// Per-transaction memo for `sh:class` value-membership verdicts.
///
/// Keyed by `(value node, expected class, focus data graph)`. The engine that
/// drives transaction validation is built fresh per transaction and shared
/// across every focus node, so this map is scoped to exactly one validation
/// pass and never leaks a verdict across transactions or staged states. The
/// focus data graph is part of the key because membership is resolved against
/// the *union* of the focus data graph and the vocabulary graphs — the same
/// value can legitimately resolve differently when referenced from a different
/// data graph.
type ClassMembershipCache = Mutex<HashMap<(Sid, Sid, GraphId), bool>>;

/// `(focus node, shape id)` pairs currently being validated on the call stack.
///
/// Recursive shape references are legal SHACL (`FriendShape → sh:node
/// FriendShape` via a property) and cyclic *data* would otherwise recurse
/// forever. On re-entry of an already-active pair the engine assumes
/// conformance — the standard terminating interpretation, since the spec
/// leaves recursive validation undefined. One set is created per top-level
/// validation entry point; entries are removed on exit, so the set only
/// reflects the live call stack.
type ActiveShapeChecks = Mutex<HashSet<(Sid, ShapeId)>>;

/// Threaded context for resolving `sh:class` value membership: the extra
/// vocabulary graphs to union into the `rdf:type` / `rdfs:subClassOf` lookup
/// (the `f:shapesSource` graph[s]) plus the per-transaction memo. `Copy`
/// because it is just two borrows passed down the validation call tree.
/// A live cross-ledger membership source for `sh:class`: a handle into the
/// model ledger M holding the controlled vocabulary, plus the data ledger's
/// namespace map (code → IRI prefix, including this transaction's staged
/// allocations) needed to translate D-term Sids into M's term space.
#[derive(Clone, Copy)]
pub struct CrossLedgerMembership<'a> {
    /// `GraphDbRef` into M's value-set graph at the resolved `t`.
    pub model_db: GraphDbRef<'a>,
    /// D's namespace codes → IRI prefixes (base + this transaction's staged
    /// allocations). Used to decode a D-term Sid to its full IRI before
    /// re-encoding it against M (whose split mode may differ), because the
    /// staged base snapshot alone can't decode namespaces introduced this txn.
    pub data_ns_map: &'a HashMap<u16, String>,
}

#[derive(Clone, Copy)]
struct ClassMembershipCtx<'a> {
    /// Graphs beyond the focus node's own data graph to consult for membership.
    /// Empty = legacy behaviour (focus data graph for `rdf:type`, schema graph
    /// 0 for `subClassOf`).
    membership_g_ids: &'a [GraphId],
    /// Per-transaction memo shared across all focus nodes in one pass.
    cache: &'a ClassMembershipCache,
    /// Cross-ledger value-set source (model ledger M holding the controlled
    /// vocabulary), when `f:shapesSource` is cross-ledger. Consulted on demand
    /// for `sh:class` membership after the local lookup misses.
    cross_ledger: Option<CrossLedgerMembership<'a>>,
}

/// SHACL validation engine
///
/// When constructed with a `SchemaHierarchy`, the engine properly handles RDFS
/// reasoning for `sh:targetClass`:
/// - A shape targeting `Animal` will also apply to instances of `Dog`
///   (if `Dog rdfs:subClassOf Animal`)
pub struct ShaclEngine {
    /// Cached compiled shapes
    cache: ShaclCache,
    /// Schema hierarchy for RDFS reasoning (optional)
    hierarchy: Option<SchemaHierarchy>,
    /// Extra graphs consulted when resolving `sh:class` value membership —
    /// typically the `f:shapesSource` graph(s), so a shared value-set
    /// vocabulary can live alongside the shapes. The focus node's own data
    /// graph is always consulted in addition to these.
    membership_g_ids: Vec<GraphId>,
    /// Per-transaction memo of resolved `sh:class` membership verdicts.
    class_cache: ClassMembershipCache,
}

impl ShaclEngine {
    /// Create a new engine from a cache (without hierarchy support)
    ///
    /// For full RDFS reasoning support, use `new_with_hierarchy` instead.
    pub fn new(cache: ShaclCache) -> Self {
        Self {
            cache,
            hierarchy: None,
            membership_g_ids: Vec::new(),
            class_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Create a new engine from a cache with hierarchy support
    ///
    /// The hierarchy enables RDFS reasoning for `sh:targetClass`:
    /// shapes targeting a class will also apply to instances of subclasses.
    pub fn new_with_hierarchy(cache: ShaclCache, hierarchy: SchemaHierarchy) -> Self {
        Self {
            cache,
            hierarchy: Some(hierarchy),
            membership_g_ids: Vec::new(),
            class_cache: Mutex::new(HashMap::new()),
        }
    }

    /// Build an engine by compiling shapes from a single-graph database with
    /// optional overlay (convenience over [`Self::from_dbs_with_overlay`]).
    ///
    /// The overlay (typically novelty) allows compiling shapes that were
    /// transacted in previous commits but haven't been indexed yet.
    /// Automatically extracts the schema hierarchy for RDFS reasoning.
    pub async fn from_db_with_overlay(
        db: GraphDbRef<'_>,
        ledger_id: impl Into<String>,
    ) -> Result<Self> {
        Self::from_dbs_with_overlay(std::slice::from_ref(&db), ledger_id).await
    }

    /// Build an engine by compiling shapes from multiple graphs.
    ///
    /// Used when `f:shapesSource` resolves to a non-default graph (or when
    /// shapes are split across several graphs). The engine will hold the
    /// union of all shapes found across the input graphs.
    ///
    /// The schema hierarchy for RDFS reasoning is taken from the first
    /// graph's snapshot (hierarchy is a schema-level property and not
    /// graph-scoped — all `GraphDbRef`s share the same underlying snapshot
    /// in practice).
    pub async fn from_dbs_with_overlay(
        dbs: &[GraphDbRef<'_>],
        ledger_id: impl Into<String>,
    ) -> Result<Self> {
        let shapes = ShapeCompiler::compile_from_dbs(dbs).await?;

        // Cache key pins the latest-seen `t` across all input snapshots.
        // In practice all dbs share one snapshot, but we take the max to be
        // conservative if callers ever pass differently-timed refs.
        let max_t = dbs.iter().map(|d| d.snapshot.t).max().unwrap_or(0);
        let key = ShaclCacheKey::new(ledger_id, max_t as u64);

        // Hierarchy is schema-level — pick the first db's snapshot.
        let hierarchy = dbs.first().and_then(|d| d.snapshot.schema_hierarchy());
        let cache = ShaclCache::new(key, shapes, hierarchy.as_ref());

        Ok(Self {
            cache,
            hierarchy,
            membership_g_ids: Vec::new(),
            class_cache: Mutex::new(HashMap::new()),
        })
    }

    /// Set the additional graphs consulted when resolving `sh:class` value
    /// membership (typically the `f:shapesSource` graph ids). The focus node's
    /// own data graph is always consulted in addition to these, so a shared
    /// value-set vocabulary can live in the shapes graph while the referencing
    /// data lives in a different graph.
    pub fn with_membership_graphs(mut self, g_ids: Vec<GraphId>) -> Self {
        self.membership_g_ids = g_ids;
        self
    }

    /// Build an engine by compiling shapes from a database (no overlay)
    ///
    /// This is a convenience method for when there is no novelty to consider,
    /// such as when loading from a fully indexed database.
    ///
    /// Automatically extracts the schema hierarchy for RDFS reasoning.
    pub async fn from_db(
        snapshot: &LedgerSnapshot,
        g_id: GraphId,
        ledger_id: impl Into<String>,
    ) -> Result<Self> {
        let db = GraphDbRef::new(snapshot, g_id, &NoOverlay, snapshot.t);
        Self::from_db_with_overlay(db, ledger_id).await
    }

    /// Validate a focus node against all applicable shapes.
    ///
    /// Target-type discovery:
    /// - `sh:targetNode` / `sh:targetClass`: resolved from the cache against
    ///   `focus_node` and `node_types`.
    /// - `sh:targetSubjectsOf(p)` / `sh:targetObjectsOf(p)`: checked against
    ///   the **post-transaction view** via `db.range()`. A shape applies iff
    ///   the focus actually participates in the predicate in post-state.
    ///
    /// The post-state check is necessary because predicate-target
    /// applicability cannot be determined from staged flakes alone:
    /// - A base-state edge may make the shape apply even though nothing
    ///   about that predicate was staged (e.g., alice already has `ex:ssn`
    ///   and this txn only retracts `ex:name`).
    /// - A retraction can remove the only edge that connected the focus to
    ///   the predicate, so the shape should no longer apply.
    ///
    /// `db.range()` returns only assertions (retractions are suppressed by
    /// the overlay/snapshot composition), so the existence check is exactly
    /// the post-state answer.
    pub async fn validate_node(
        &self,
        db: GraphDbRef<'_>,
        focus_node: &Sid,
        node_types: &[Sid],
        cross_ledger: Option<CrossLedgerMembership<'_>>,
    ) -> Result<ValidationReport> {
        let mut results = Vec::new();

        // Find shapes that apply to this node
        let mut applicable_shapes: Vec<&CompiledShape> = Vec::new();

        // By explicit target node
        applicable_shapes.extend(self.cache.shapes_for_node(focus_node));

        // By class targeting
        for class in node_types {
            applicable_shapes.extend(self.cache.shapes_for_class(class));
        }

        // By `sh:targetSubjectsOf(p)`: focus must currently have `p` as
        // outbound predicate (SPOT existence check). Only predicates that
        // are actually used as `SubjectsOf` targets are probed, so this is
        // bounded by the shape-set size, not the data size.
        for predicate in self.cache.by_target_subjects_of.keys() {
            let flakes = db
                .range(
                    IndexType::Spot,
                    RangeTest::Eq,
                    RangeMatch::subject_predicate(focus_node.clone(), predicate.clone()),
                )
                .await?;
            if !flakes.is_empty() {
                applicable_shapes.extend(self.cache.shapes_for_subjects_of(predicate));
            }
        }

        // By `sh:targetObjectsOf(p)`: focus must currently appear as the
        // object of `p` (OPST existence check). Same bounded-cost argument.
        for predicate in self.cache.by_target_objects_of.keys() {
            let flakes = db
                .range(
                    IndexType::Opst,
                    RangeTest::Eq,
                    RangeMatch::predicate_object(
                        predicate.clone(),
                        FlakeValue::Ref(focus_node.clone()),
                    ),
                )
                .await?;
            if !flakes.is_empty() {
                applicable_shapes.extend(self.cache.shapes_for_objects_of(predicate));
            }
        }

        // Remove duplicates
        let mut seen = HashSet::new();
        applicable_shapes.retain(|s| seen.insert(&s.id));

        // Collect all shapes for logical constraint resolution
        let all_shapes: Vec<&CompiledShape> = self.cache.all_shapes().iter().collect();

        // Validate against each shape. `class_ctx` carries the `f:shapesSource`
        // vocabulary graphs (for cross-graph `sh:class` value-sets) and the
        // per-transaction membership memo down to `validate_class_constraint`.
        let class_ctx = ClassMembershipCtx {
            membership_g_ids: &self.membership_g_ids,
            cache: &self.class_cache,
            cross_ledger,
        };
        let active = ActiveShapeChecks::default();
        for shape in applicable_shapes {
            if shape.deactivated {
                continue;
            }

            let shape_results =
                validate_shape(db, focus_node, shape, &all_shapes, Some(class_ctx), &active)
                    .await?;
            results.extend(shape_results);
        }

        let conforms = results.iter().all(|r| r.severity != Severity::Violation);

        Ok(ValidationReport { conforms, results })
    }

    /// Validate a focus node without an overlay
    pub async fn validate_node_no_overlay(
        &self,
        snapshot: &LedgerSnapshot,
        g_id: GraphId,
        focus_node: &Sid,
        node_types: &[Sid],
    ) -> Result<ValidationReport> {
        let db = GraphDbRef::new(snapshot, g_id, &NoOverlay, snapshot.t);
        self.validate_node(db, focus_node, node_types, None).await
    }

    /// Validate all focus nodes targeted by shapes
    pub async fn validate_all(&self, db: GraphDbRef<'_>) -> Result<ValidationReport> {
        let mut all_results = Vec::new();

        // Collect all shapes for logical constraint resolution
        let all_shapes: Vec<&CompiledShape> = self.cache.all_shapes().iter().collect();

        let class_ctx = ClassMembershipCtx {
            membership_g_ids: &self.membership_g_ids,
            cache: &self.class_cache,
            // Full-db validation (`validate_all`) has no cross-ledger model
            // context; `sh:class` uses the local lookup only.
            cross_ledger: None,
        };
        for shape in self.cache.all_shapes() {
            if shape.deactivated {
                continue;
            }

            // Get focus nodes for this shape (with hierarchy expansion)
            let focus_nodes = get_focus_nodes(db, shape, self.hierarchy.as_ref()).await?;

            for focus_node in focus_nodes {
                let active = ActiveShapeChecks::default();
                let results = validate_shape(
                    db,
                    &focus_node,
                    shape,
                    &all_shapes,
                    Some(class_ctx),
                    &active,
                )
                .await?;
                all_results.extend(results);
            }
        }

        let conforms = all_results
            .iter()
            .all(|r| r.severity != Severity::Violation);

        Ok(ValidationReport {
            conforms,
            results: all_results,
        })
    }

    /// Validate all focus nodes without an overlay
    pub async fn validate_all_no_overlay(
        &self,
        snapshot: &LedgerSnapshot,
        g_id: GraphId,
    ) -> Result<ValidationReport> {
        let db = GraphDbRef::new(snapshot, g_id, &NoOverlay, snapshot.t);
        self.validate_all(db).await
    }

    /// Get the underlying cache
    pub fn cache(&self) -> &ShaclCache {
        &self.cache
    }

    // ========================================================================
    // Optimization: Early exit when no shapes
    // ========================================================================
    // Pattern: `(if (empty? shapes) :valid ...)`
    // This elides all validation work when no SHACL shapes are defined.

    /// Check if there are any shapes to validate against
    ///
    /// Use this for early exit: if no shapes exist, validation is a no-op.
    /// This follows the SHACL implementation optimization.
    #[inline]
    pub fn has_shapes(&self) -> bool {
        !self.cache.is_empty()
    }

    /// Check if there are no shapes (validation will be a no-op)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Get the number of shapes
    #[inline]
    pub fn shape_count(&self) -> usize {
        self.cache.len()
    }

    /// Validate only the subjects that were modified in a transaction
    ///
    /// This is the primary entry point for transaction-time validation.
    /// It validates only the subjects present in `modified_subjects` against
    /// applicable shapes, returning early if no shapes exist.
    ///
    /// # Arguments
    /// * `snapshot` - The database snapshot to validate against
    /// * `overlay` - Overlay containing staged changes (so validation sees new data)
    /// * `modified_subjects` - Set of subject SIDs that were modified in the transaction
    ///
    /// # Returns
    /// * `ValidationReport` - conforming if no violations, or containing all violations
    pub async fn validate_staged(
        &self,
        db: GraphDbRef<'_>,
        modified_subjects: &HashSet<Sid>,
    ) -> Result<ValidationReport> {
        // Early exit: no shapes means automatic conformance
        // This is the key optimization: (if (empty? shapes) :valid ...)
        if self.cache.is_empty() {
            return Ok(ValidationReport::conforming());
        }

        // Early exit: no modified subjects means nothing to validate
        if modified_subjects.is_empty() {
            return Ok(ValidationReport::conforming());
        }

        let mut all_results = Vec::new();

        // For each modified subject, find its types and validate
        let rdf_type = Sid::new(RDF, rdf_names::TYPE);

        for subject in modified_subjects {
            // Get the types of this subject (through the overlay so we see staged data)
            let type_flakes = db
                .range(
                    IndexType::Spot,
                    RangeTest::Eq,
                    RangeMatch::subject_predicate(subject.clone(), rdf_type.clone()),
                )
                .await?;

            let node_types: Vec<Sid> = type_flakes
                .iter()
                .filter_map(|f| {
                    if let FlakeValue::Ref(t) = &f.o {
                        Some(t.clone())
                    } else {
                        None
                    }
                })
                .collect();

            // Validate this node against applicable shapes
            let report = self.validate_node(db, subject, &node_types, None).await?;
            all_results.extend(report.results);
        }

        let conforms = all_results
            .iter()
            .all(|r| r.severity != Severity::Violation);

        Ok(ValidationReport {
            conforms,
            results: all_results,
        })
    }

    /// Validate staged changes, returning an error if validation fails
    ///
    /// This is a convenience wrapper around `validate_staged` that converts
    /// validation failures into errors, suitable for use in transaction staging.
    pub async fn validate_staged_or_error(
        &self,
        db: GraphDbRef<'_>,
        modified_subjects: &HashSet<Sid>,
    ) -> Result<()> {
        let report = self.validate_staged(db, modified_subjects).await?;

        if report.conforms {
            Ok(())
        } else {
            // Build detailed error messages (limit to first 10 to avoid huge errors)
            let details: Vec<String> = report
                .results
                .iter()
                .filter(|r| r.severity == Severity::Violation)
                .take(10)
                .map(|r| {
                    if let Some(ref path) = r.result_path {
                        format!(
                            "Node {}: property {}: {}",
                            r.focus_node.name, path.name, r.message
                        )
                    } else {
                        format!("Node {}: {}", r.focus_node.name, r.message)
                    }
                })
                .collect();

            Err(crate::error::ShaclError::ValidationFailed {
                violation_count: report.violation_count(),
                warning_count: report.warning_count(),
                details,
            })
        }
    }
}

/// Get focus nodes for a shape based on its targeting declarations
///
/// When a hierarchy is provided, `TargetType::Class` targets are expanded
/// to include instances of all subclasses. For example, a shape targeting
/// `Animal` will also match instances of `Dog` (if `Dog rdfs:subClassOf Animal`).
async fn get_focus_nodes(
    db: GraphDbRef<'_>,
    shape: &CompiledShape,
    hierarchy: Option<&SchemaHierarchy>,
) -> Result<Vec<Sid>> {
    let mut focus_nodes = Vec::new();

    for target in &shape.targets {
        match target {
            TargetType::Class(class) | TargetType::ImplicitClass(class) => {
                // Build list of classes to query: target class + all subclasses
                let mut classes_to_query = vec![class.clone()];
                if let Some(h) = hierarchy {
                    classes_to_query.extend(h.subclasses_of(class).iter().cloned());
                }

                // Find all instances of each class
                let rdf_type = Sid::new(RDF, rdf_names::TYPE);
                for cls in classes_to_query {
                    let flakes = db
                        .range(
                            IndexType::Psot,
                            RangeTest::Eq,
                            RangeMatch::predicate_object(rdf_type.clone(), FlakeValue::Ref(cls)),
                        )
                        .await?;

                    for flake in flakes {
                        focus_nodes.push(flake.s.clone());
                    }
                }
            }
            TargetType::Node(nodes) => {
                focus_nodes.extend(nodes.iter().cloned());
            }
            TargetType::SubjectsOf(predicate) => {
                // Find all subjects that have this predicate
                let flakes = db
                    .range(
                        IndexType::Psot,
                        RangeTest::Eq,
                        RangeMatch::predicate(predicate.clone()),
                    )
                    .await?;

                for flake in flakes {
                    focus_nodes.push(flake.s.clone());
                }
            }
            TargetType::ObjectsOf(predicate) => {
                // Find all objects of triples with this predicate
                let flakes = db
                    .range(
                        IndexType::Psot,
                        RangeTest::Eq,
                        RangeMatch::predicate(predicate.clone()),
                    )
                    .await?;

                for flake in flakes {
                    if let FlakeValue::Ref(obj) = &flake.o {
                        focus_nodes.push(obj.clone());
                    }
                }
            }
        }
    }

    // Remove duplicates
    let mut seen = HashSet::new();
    focus_nodes.retain(|n| seen.insert(n.clone()));

    Ok(focus_nodes)
}

/// Validate a focus node against a single shape
///
/// Note: This function uses `Box::pin` for recursive calls to avoid infinitely-sized futures.
fn validate_shape<'a>(
    db: GraphDbRef<'a>,
    focus_node: &'a Sid,
    shape: &'a CompiledShape,
    all_shapes: &'a [&'a CompiledShape],
    class_ctx: Option<ClassMembershipCtx<'a>>,
    active: &'a ActiveShapeChecks,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ValidationResult>>> + Send + 'a>>
{
    Box::pin(async move {
        // A deactivated shape is ignored entirely — including when reached via
        // sh:node / logical shape references, not just from target selection.
        if shape.deactivated {
            return Ok(Vec::new());
        }

        // Recursion guard: a (focus, shape) pair already on the call stack
        // (via sh:node / logical shape references over cyclic data) is assumed
        // conforming so validation terminates.
        let guard_key = (focus_node.clone(), shape.id.clone());
        if !active.lock().insert(guard_key.clone()) {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();

        // Validate property shapes
        for prop_shape in &shape.property_shapes {
            let prop_results = validate_property_shape(
                db, focus_node, prop_shape, shape, all_shapes, class_ctx, active,
            )
            .await?;
            results.extend(prop_results);
        }

        // Value constraints declared directly on the node shape apply to the
        // focus node itself (a node shape's value nodes = the focus node).
        if !shape.node_constraints.is_empty() {
            let node_results =
                validate_node_value_constraints(db, focus_node, shape, class_ctx).await?;
            results.extend(node_results);
        }

        // Validate structural constraints (closed, logical)
        for constraint in &shape.structural_constraints {
            let constraint_results = validate_structural_constraint(
                db, focus_node, constraint, shape, all_shapes, class_ctx, active,
            )
            .await?;
            results.extend(constraint_results);
        }

        active.lock().remove(&guard_key);
        Ok(results)
    })
}

/// Validate value constraints declared directly on a node shape (no `sh:path`)
/// against the focus node itself. Per spec, a node shape's value-node set is
/// exactly the focus node, so per-value constraints (`sh:in`, `sh:hasValue`,
/// `sh:nodeKind`, `sh:class`, ranges, …) evaluate over `[focus]`.
async fn validate_node_value_constraints<'a>(
    db: GraphDbRef<'a>,
    focus_node: &Sid,
    shape: &'a CompiledShape,
    class_ctx: Option<ClassMembershipCtx<'a>>,
) -> Result<Vec<ValidationResult>> {
    let mut results = Vec::new();
    let values = [FlakeValue::Ref(focus_node.clone())];
    let datatypes = [fluree_db_core::id_datatype_sid()];

    let push = |violation: ConstraintViolation, results: &mut Vec<ValidationResult>| {
        results.push(ValidationResult {
            focus_node: focus_node.clone(),
            result_path: None,
            source_shape: shape.id.clone(),
            source_constraint: None,
            severity: shape.severity,
            message: shape.message.clone().unwrap_or(violation.message),
            value: violation.value,
            graph_id: None,
        });
    };

    for constraint in &shape.node_constraints {
        match constraint {
            Constraint::Equals(target_prop)
            | Constraint::Disjoint(target_prop)
            | Constraint::LessThan(target_prop)
            | Constraint::LessThanOrEquals(target_prop) => {
                let target_flakes = db
                    .range(
                        IndexType::Spot,
                        RangeTest::Eq,
                        RangeMatch::subject_predicate(focus_node.clone(), target_prop.clone()),
                    )
                    .await?;
                let target_values: Vec<FlakeValue> =
                    target_flakes.iter().map(|f| f.o.clone()).collect();
                for violation in
                    validate_pair_constraint(constraint, &values, &target_values, &target_prop.name)
                {
                    push(violation, &mut results);
                }
            }
            Constraint::Class(expected_class) => {
                for violation in
                    validate_class_constraint(db, &values, expected_class, class_ctx).await?
                {
                    push(violation, &mut results);
                }
            }
            _ => {
                for violation in validate_constraint(constraint, &values, &datatypes, &[None])? {
                    push(violation, &mut results);
                }
            }
        }
    }

    Ok(results)
}

/// Validate a structural (node-level) constraint
///
/// Note: This function uses `Box::pin` for recursive calls to avoid infinitely-sized futures.
fn validate_structural_constraint<'a>(
    db: GraphDbRef<'a>,
    focus_node: &'a Sid,
    constraint: &'a crate::constraints::NodeConstraint,
    parent_shape: &'a CompiledShape,
    all_shapes: &'a [&'a CompiledShape],
    class_ctx: Option<ClassMembershipCtx<'a>>,
    active: &'a ActiveShapeChecks,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ValidationResult>>> + Send + 'a>>
{
    Box::pin(async move {
        use crate::compile::Severity;

        let mut results = Vec::new();

        match constraint {
            NodeConstraint::Closed {
                is_closed,
                ignored_properties,
            } => {
                if *is_closed {
                    // Get all properties used by the focus node
                    let node_flakes = db
                        .range(
                            IndexType::Spot,
                            RangeTest::Eq,
                            RangeMatch::subject(focus_node.clone()),
                        )
                        .await?;

                    // Collect declared properties from the shape's property shapes
                    // Only single-predicate property shapes declare a property
                    // for closed-shape purposes; complex paths have no single
                    // predicate to exempt.
                    let declared_properties: std::collections::HashSet<&Sid> = parent_shape
                        .property_shapes
                        .iter()
                        .filter_map(|ps| ps.path.as_predicate())
                        .collect();

                    // Per SHACL spec section 4.8.1, rdf:type is implicitly ignored
                    let rdf_type_sid = Sid::new(RDF, rdf_names::TYPE);
                    let mut effective_ignored = ignored_properties.clone();
                    effective_ignored.insert(rdf_type_sid);

                    // Check each property on the node
                    for flake in node_flakes {
                        let prop = &flake.p;
                        if !declared_properties.contains(prop) && !effective_ignored.contains(prop)
                        {
                            results.push(ValidationResult {
                                focus_node: focus_node.clone(),
                                result_path: Some(prop.clone()),
                                source_shape: parent_shape.id.clone(),
                                source_constraint: None,
                                severity: parent_shape.severity,
                                message: parent_shape.message.clone().unwrap_or_else(|| {
                                    format!("Property {} not allowed by closed shape", prop.name)
                                }),
                                value: Some(flake.o.clone()),
                                graph_id: None,
                            });
                        }
                    }
                }
            }

            NodeConstraint::Node(nested_shape) => {
                // sh:node - the focus node must conform to the referenced shape
                let nested_results = validate_nested_shape(
                    db,
                    focus_node,
                    nested_shape.as_ref(),
                    parent_shape,
                    all_shapes,
                    class_ctx,
                    active,
                )
                .await?;
                let has_violations = nested_results
                    .iter()
                    .any(|r| r.severity == Severity::Violation);
                if has_violations {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: None,
                        source_shape: parent_shape.id.clone(),
                        source_constraint: None,
                        severity: parent_shape.severity,
                        message: parent_shape.message.clone().unwrap_or_else(|| {
                            format!(
                                "Node does not conform to shape {} (sh:node)",
                                nested_shape.id.name
                            )
                        }),
                        value: None,
                        graph_id: None,
                    });
                }
            }

            NodeConstraint::Not(nested_shape) => {
                // sh:not - the nested shape must NOT match
                let nested_results = validate_nested_shape(
                    db,
                    focus_node,
                    nested_shape.as_ref(),
                    parent_shape,
                    all_shapes,
                    class_ctx,
                    active,
                )
                .await?;
                // If the nested shape has NO violations, that's a violation of sh:not.
                // An "unresolved shape" violation from validate_nested_shape counts as
                // a violation (the shape didn't match), so sh:not is satisfied.
                let has_violations = nested_results
                    .iter()
                    .any(|r| r.severity == Severity::Violation);
                if !has_violations {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: None,
                        source_shape: parent_shape.id.clone(),
                        source_constraint: None,
                        severity: parent_shape.severity,
                        message: parent_shape.message.clone().unwrap_or_else(|| {
                            format!(
                                "Node conforms to shape {} which is not allowed (sh:not)",
                                nested_shape.id.name
                            )
                        }),
                        value: None,
                        graph_id: None,
                    });
                }
            }

            NodeConstraint::And(nested_shapes) => {
                // sh:and - ALL nested shapes must match (no violations)
                for nested in nested_shapes {
                    let nested_results = validate_nested_shape(
                        db,
                        focus_node,
                        nested.as_ref(),
                        parent_shape,
                        all_shapes,
                        class_ctx,
                        active,
                    )
                    .await?;
                    // Include violations from the nested shape
                    for r in nested_results {
                        if r.severity == Severity::Violation {
                            results.push(ValidationResult {
                                focus_node: focus_node.clone(),
                                result_path: r.result_path,
                                source_shape: parent_shape.id.clone(),
                                source_constraint: None,
                                severity: parent_shape.severity,
                                message: parent_shape.message.clone().unwrap_or_else(|| {
                                    format!("sh:and constraint - {}", r.message)
                                }),
                                value: r.value,
                                graph_id: None,
                            });
                        }
                    }
                }
            }

            NodeConstraint::Or(nested_shapes) => {
                // sh:or - at least ONE nested shape must match (have no violations)
                let mut any_conforms = false;
                let mut all_messages = Vec::new();

                for nested in nested_shapes {
                    let nested_results = validate_nested_shape(
                        db,
                        focus_node,
                        nested.as_ref(),
                        parent_shape,
                        all_shapes,
                        class_ctx,
                        active,
                    )
                    .await?;
                    let has_violations = nested_results
                        .iter()
                        .any(|r| r.severity == Severity::Violation);
                    if !has_violations {
                        any_conforms = true;
                        break;
                    }
                    // Collect messages for reporting if none match
                    for r in nested_results {
                        if r.severity == Severity::Violation {
                            all_messages.push(format!("{}: {}", nested.id.name, r.message));
                        }
                    }
                }

                if !any_conforms && !nested_shapes.is_empty() {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: None,
                        source_shape: parent_shape.id.clone(),
                        source_constraint: None,
                        severity: parent_shape.severity,
                        message: parent_shape.message.clone().unwrap_or_else(|| {
                            format!(
                                "Node does not conform to any shape in sh:or. Violations: {}",
                                all_messages.join("; ")
                            )
                        }),
                        value: None,
                        graph_id: None,
                    });
                }
            }

            NodeConstraint::Xone(nested_shapes) => {
                // sh:xone - exactly ONE nested shape must match
                let mut conforming_count = 0;
                let mut conforming_shapes = Vec::new();

                for nested in nested_shapes {
                    let nested_results = validate_nested_shape(
                        db,
                        focus_node,
                        nested.as_ref(),
                        parent_shape,
                        all_shapes,
                        class_ctx,
                        active,
                    )
                    .await?;
                    let has_violations = nested_results
                        .iter()
                        .any(|r| r.severity == Severity::Violation);
                    if !has_violations {
                        conforming_count += 1;
                        conforming_shapes.push(nested.id.name.clone());
                    }
                }

                if conforming_count == 0 {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: None,
                        source_shape: parent_shape.id.clone(),
                        source_constraint: None,
                        severity: parent_shape.severity,
                        message: parent_shape.message.clone().unwrap_or_else(|| {
                            "Node does not conform to any shape in sh:xone".to_string()
                        }),
                        value: None,
                        graph_id: None,
                    });
                } else if conforming_count > 1 {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: None,
                        source_shape: parent_shape.id.clone(),
                        source_constraint: None,
                        severity: parent_shape.severity,
                        message: parent_shape.message.clone().unwrap_or_else(|| {
                            format!(
                                "Node conforms to {} shapes in sh:xone (must be exactly 1): {}",
                                conforming_count,
                                conforming_shapes.join(", ")
                            )
                        }),
                        value: None,
                        graph_id: None,
                    });
                }
            }
        }

        Ok(results)
    })
}

/// Validate a focus node against a nested shape (inline shape from sh:and/or/xone)
///
/// Unlike `validate_shape` which validates against a `CompiledShape`, this validates
/// directly against the constraints embedded in a `NestedShape`.
fn validate_nested_shape<'a>(
    db: GraphDbRef<'a>,
    focus_node: &'a Sid,
    nested: &'a NestedShape,
    parent_shape: &'a CompiledShape,
    all_shapes: &'a [&'a CompiledShape],
    class_ctx: Option<ClassMembershipCtx<'a>>,
    active: &'a ActiveShapeChecks,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ValidationResult>>> + Send + 'a>>
{
    Box::pin(async move {
        // If the NestedShape has no inline constraints, try to find the referenced shape
        // in all_shapes (for top-level shapes referenced by ID in sh:and/or/xone)
        if nested.property_constraints.is_empty()
            && nested.node_constraints.is_empty()
            && nested.value_constraints.is_empty()
        {
            if let Some(ref_shape) = all_shapes.iter().find(|s| s.id == nested.id) {
                return validate_shape(db, focus_node, ref_shape, all_shapes, class_ctx, active)
                    .await;
            }
            // Shape not found and no inline constraints — treat as unresolved.
            // Return a violation to prevent sh:or from being trivially true.
            return Ok(vec![ValidationResult {
                focus_node: focus_node.clone(),
                result_path: None,
                source_shape: parent_shape.id.clone(),
                source_constraint: Some(nested.id.clone()),
                severity: Severity::Violation,
                message: format!("Referenced shape {} could not be resolved", nested.id.name),
                value: None,
                graph_id: None,
            }]);
        }

        let mut results = Vec::new();

        // Validate property constraints
        for (path, constraints) in &nested.property_constraints {
            // A path that never compiled surfaces as a violation on this member.
            if let Some(reason) = path.unresolvable_reason() {
                results.push(ValidationResult {
                    focus_node: focus_node.clone(),
                    result_path: None,
                    source_shape: parent_shape.id.clone(),
                    source_constraint: Some(nested.id.clone()),
                    severity: Severity::Violation,
                    message: format!("Unsupported sh:path expression: {reason}"),
                    value: None,
                    graph_id: None,
                });
                continue;
            }

            // Value nodes reached by the member's path. Simple predicate → SPOT
            // scan; complex path → evaluate the AST (same as top-level shapes).
            let (values, datatypes, langs): (Vec<FlakeValue>, Vec<Sid>, Vec<Option<String>>) =
                if let Some(pred) = path.as_predicate() {
                    let flakes = db
                        .range(
                            IndexType::Spot,
                            RangeTest::Eq,
                            RangeMatch::subject_predicate(focus_node.clone(), pred.clone()),
                        )
                        .await?;
                    (
                        flakes.iter().map(|f| f.o.clone()).collect(),
                        flakes.iter().map(|f| f.dt.clone()).collect(),
                        flakes
                            .iter()
                            .map(|f| f.m.as_ref().and_then(|m| m.lang.clone()))
                            .collect(),
                    )
                } else {
                    crate::path::split_path_values(
                        crate::path::eval_path(db, focus_node, path).await?,
                    )
                };

            let result_path = path.as_predicate().cloned();
            let path_label = path
                .as_predicate()
                .map(|p| p.name.to_string())
                .unwrap_or_else(|| "path".to_string());

            // Validate each constraint
            for constraint in constraints {
                // Handle pair constraints separately since they need snapshot access
                match constraint {
                    Constraint::Equals(target_prop) => {
                        // Get values for the target property
                        let target_flakes = db
                            .range(
                                IndexType::Spot,
                                RangeTest::Eq,
                                RangeMatch::subject_predicate(
                                    focus_node.clone(),
                                    target_prop.clone(),
                                ),
                            )
                            .await?;
                        let target_values: std::collections::HashSet<_> =
                            target_flakes.iter().map(|f| &f.o).collect();
                        let source_values: std::collections::HashSet<_> = values.iter().collect();

                        if source_values != target_values {
                            results.push(ValidationResult {
                                focus_node: focus_node.clone(),
                                result_path: result_path.clone(),
                                source_shape: parent_shape.id.clone(),
                                source_constraint: Some(nested.id.clone()),
                                severity: Severity::Violation,
                                message: format!(
                                    "Value set for {} does not equal value set for {}",
                                    path_label, target_prop.name
                                ),
                                value: None,
                                graph_id: None,
                            });
                        }
                    }
                    Constraint::Class(expected_class) => {
                        let violations =
                            validate_class_constraint(db, &values, expected_class, class_ctx)
                                .await?;
                        for violation in violations {
                            results.push(ValidationResult {
                                focus_node: focus_node.clone(),
                                result_path: result_path.clone(),
                                source_shape: parent_shape.id.clone(),
                                source_constraint: Some(nested.id.clone()),
                                severity: Severity::Violation,
                                message: violation.message,
                                value: violation.value,
                                graph_id: None,
                            });
                        }
                    }
                    Constraint::QualifiedValueShape {
                        shape,
                        min_count,
                        max_count,
                    } => {
                        let mut conforming = 0usize;
                        for (i, value) in values.iter().enumerate() {
                            let conforms = check_value_against_nested_shape(
                                db,
                                value,
                                datatypes.get(i),
                                langs.get(i).and_then(|l| l.as_deref()),
                                shape,
                                parent_shape,
                                all_shapes,
                                class_ctx,
                                active,
                            )
                            .await?;
                            if conforms {
                                conforming += 1;
                            }
                        }
                        let below = min_count.map(|min| conforming < min).unwrap_or(false);
                        let above = max_count.map(|max| conforming > max).unwrap_or(false);
                        if below || above {
                            results.push(ValidationResult {
                                focus_node: focus_node.clone(),
                                result_path: result_path.clone(),
                                source_shape: parent_shape.id.clone(),
                                source_constraint: Some(nested.id.clone()),
                                severity: Severity::Violation,
                                message: format!(
                                    "Found {} value(s) conforming to shape {} (expected {}..{})",
                                    conforming,
                                    shape.id.name,
                                    min_count.map_or_else(|| "0".into(), |n| n.to_string()),
                                    max_count.map_or_else(|| "*".into(), |n| n.to_string()),
                                ),
                                value: None,
                                graph_id: None,
                            });
                        }
                    }
                    Constraint::Pattern(..)
                    | Constraint::MinLength(_)
                    | Constraint::MaxLength(_)
                        if has_iri_ref(&values) =>
                    {
                        let effective = stringify_iri_values(db, &values);
                        let violations =
                            validate_constraint(constraint, &effective, &datatypes, &langs)?;
                        for violation in violations {
                            results.push(ValidationResult {
                                focus_node: focus_node.clone(),
                                result_path: result_path.clone(),
                                source_shape: parent_shape.id.clone(),
                                source_constraint: Some(nested.id.clone()),
                                severity: Severity::Violation,
                                message: violation.message,
                                value: violation.value,
                                graph_id: None,
                            });
                        }
                    }
                    _ => {
                        let violations =
                            validate_constraint(constraint, &values, &datatypes, &langs)?;
                        for violation in violations {
                            results.push(ValidationResult {
                                focus_node: focus_node.clone(),
                                result_path: result_path.clone(),
                                source_shape: parent_shape.id.clone(),
                                source_constraint: Some(nested.id.clone()),
                                severity: Severity::Violation,
                                message: violation.message,
                                value: violation.value,
                                graph_id: None,
                            });
                        }
                    }
                }
            }
        }

        // Validate nested node constraints recursively
        for node_constraint in &nested.node_constraints {
            let nested_results = validate_structural_constraint(
                db,
                focus_node,
                node_constraint,
                parent_shape,
                all_shapes,
                class_ctx,
                active,
            )
            .await?;
            results.extend(nested_results);
        }

        Ok(results)
    })
}

/// Validate a focus node against a property shape
#[allow(clippy::too_many_arguments)]
async fn validate_property_shape<'a>(
    db: GraphDbRef<'a>,
    focus_node: &Sid,
    prop_shape: &PropertyShape,
    parent_shape: &'a CompiledShape,
    all_shapes: &'a [&'a CompiledShape],
    class_ctx: Option<ClassMembershipCtx<'a>>,
    active: &'a ActiveShapeChecks,
) -> Result<Vec<ValidationResult>> {
    let mut results = Vec::new();

    // A path that never compiled surfaces here (only for focus nodes this shape
    // actually targets) rather than as a ledger-wide compile failure.
    if let Some(reason) = prop_shape.path.unresolvable_reason() {
        results.push(ValidationResult {
            focus_node: focus_node.clone(),
            result_path: None,
            source_shape: parent_shape.id.clone(),
            source_constraint: Some(prop_shape.id.clone()),
            severity: prop_shape.severity,
            message: format!("Unsupported sh:path expression: {reason}"),
            value: None,
            graph_id: None,
        });
        return Ok(results);
    }

    // Get all value nodes reached by this property shape's path on the focus node.
    // Simple single-predicate paths take the plain SPOT scan; complex paths
    // (inverse/sequence/alternative/transitive) evaluate the path AST. The
    // language column feeds sh:uniqueLang / sh:languageIn.
    let (values, datatypes, langs): (Vec<FlakeValue>, Vec<Sid>, Vec<Option<String>>) =
        if let Some(pred) = prop_shape.path.as_predicate() {
            let flakes = db
                .range(
                    IndexType::Spot,
                    RangeTest::Eq,
                    RangeMatch::subject_predicate(focus_node.clone(), pred.clone()),
                )
                .await?;
            (
                flakes.iter().map(|f| f.o.clone()).collect(),
                flakes.iter().map(|f| f.dt.clone()).collect(),
                flakes
                    .iter()
                    .map(|f| f.m.as_ref().and_then(|m| m.lang.clone()))
                    .collect(),
            )
        } else {
            crate::path::split_path_values(
                crate::path::eval_path(db, focus_node, &prop_shape.path).await?,
            )
        };

    // Validate each constraint
    for constraint in &prop_shape.constraints {
        // Constraints that need DB access (pair constraints, sh:class) are
        // handled here; the rest delegate to the pure-values helper below.
        match constraint {
            Constraint::Equals(target_prop)
            | Constraint::Disjoint(target_prop)
            | Constraint::LessThan(target_prop)
            | Constraint::LessThanOrEquals(target_prop) => {
                let target_flakes = db
                    .range(
                        IndexType::Spot,
                        RangeTest::Eq,
                        RangeMatch::subject_predicate(focus_node.clone(), target_prop.clone()),
                    )
                    .await?;
                let target_values: Vec<FlakeValue> =
                    target_flakes.iter().map(|f| f.o.clone()).collect();

                let violations = validate_pair_constraint(
                    constraint,
                    &values,
                    &target_values,
                    &target_prop.name,
                );
                for violation in violations {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: prop_shape.path.as_predicate().cloned(),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: prop_shape.message.clone().unwrap_or(violation.message),
                        value: violation.value,
                        graph_id: None,
                    });
                }
            }
            Constraint::Class(expected_class) => {
                let class_violations =
                    validate_class_constraint(db, &values, expected_class, class_ctx).await?;
                for violation in class_violations {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: prop_shape.path.as_predicate().cloned(),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: prop_shape.message.clone().unwrap_or(violation.message),
                        value: violation.value,
                        graph_id: None,
                    });
                }
            }
            Constraint::QualifiedValueShape {
                shape,
                min_count,
                max_count,
            } => {
                let mut conforming = 0usize;
                for (i, value) in values.iter().enumerate() {
                    let conforms = check_value_against_nested_shape(
                        db,
                        value,
                        datatypes.get(i),
                        langs.get(i).and_then(|l| l.as_deref()),
                        shape,
                        parent_shape,
                        all_shapes,
                        class_ctx,
                        active,
                    )
                    .await?;
                    if conforms {
                        conforming += 1;
                    }
                }

                let mut qualified_messages: Vec<String> = Vec::new();
                if let Some(min) = min_count {
                    if conforming < *min {
                        qualified_messages.push(format!(
                            "Expected at least {} value(s) conforming to shape {} but found {}",
                            min, shape.id.name, conforming
                        ));
                    }
                }
                if let Some(max) = max_count {
                    if conforming > *max {
                        qualified_messages.push(format!(
                            "Expected at most {} value(s) conforming to shape {} but found {}",
                            max, shape.id.name, conforming
                        ));
                    }
                }
                for message in qualified_messages {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: prop_shape.path.as_predicate().cloned(),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: prop_shape.message.clone().unwrap_or(message),
                        value: None,
                        graph_id: None,
                    });
                }
            }
            Constraint::Pattern(..) | Constraint::MinLength(_) | Constraint::MaxLength(_)
                if has_iri_ref(&values) =>
            {
                // String facets apply to STR(iri) — decode IRI refs first.
                let effective = stringify_iri_values(db, &values);
                let violations = validate_constraint(constraint, &effective, &datatypes, &langs)?;
                for violation in violations {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: prop_shape.path.as_predicate().cloned(),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: prop_shape.message.clone().unwrap_or(violation.message),
                        value: violation.value,
                        graph_id: None,
                    });
                }
            }
            _ => {
                // Handle other constraints
                let violations = validate_constraint(constraint, &values, &datatypes, &langs)?;

                for violation in violations {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: prop_shape.path.as_predicate().cloned(),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: prop_shape.message.clone().unwrap_or(violation.message),
                        value: violation.value,
                        graph_id: None,
                    });
                }
            }
        }
    }

    // Validate per-value structural constraints (e.g. sh:or on a property shape).
    // Each value of the property is checked individually against the nested shapes.
    for structural in &prop_shape.value_structural_constraints {
        let structural_results = validate_property_value_structural_constraint(
            db,
            focus_node,
            &values,
            &datatypes,
            &langs,
            structural,
            prop_shape,
            parent_shape,
            all_shapes,
            class_ctx,
            active,
        )
        .await?;
        results.extend(structural_results);
    }

    Ok(results)
}

/// Validate a structural constraint (sh:or/sh:and/sh:xone/sh:not) per-value
/// on a property shape.
///
/// Unlike `validate_structural_constraint` which evaluates against the focus node,
/// this evaluates against each individual value of the property.
#[allow(clippy::too_many_arguments)]
async fn validate_property_value_structural_constraint<'a>(
    db: GraphDbRef<'a>,
    focus_node: &Sid,
    values: &[FlakeValue],
    datatypes: &[Sid],
    langs: &[Option<String>],
    constraint: &'a NodeConstraint,
    prop_shape: &PropertyShape,
    parent_shape: &'a CompiledShape,
    all_shapes: &'a [&'a CompiledShape],
    class_ctx: Option<ClassMembershipCtx<'a>>,
    active: &'a ActiveShapeChecks,
) -> Result<Vec<ValidationResult>> {
    let mut results = Vec::new();

    match constraint {
        NodeConstraint::Or(nested_shapes) => {
            // For each value, at least one nested shape must accept it
            for (i, value) in values.iter().enumerate() {
                let dt = datatypes.get(i);
                let mut any_conforms = false;
                let mut all_messages = Vec::new();

                for nested in nested_shapes {
                    let conforms = check_value_against_nested_shape(
                        db,
                        value,
                        dt,
                        langs.get(i).and_then(|l| l.as_deref()),
                        nested,
                        parent_shape,
                        all_shapes,
                        class_ctx,
                        active,
                    )
                    .await?;
                    if conforms {
                        any_conforms = true;
                        break;
                    }
                    all_messages.push(nested.id.name.to_string());
                }

                if !any_conforms && !nested_shapes.is_empty() {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: prop_shape.path.as_predicate().cloned(),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: prop_shape.message.clone().unwrap_or_else(|| {
                            format!(
                                "Value {:?} does not conform to any shape in sh:or (tried: {})",
                                value,
                                all_messages.join(", ")
                            )
                        }),
                        value: Some(value.clone()),
                        graph_id: None,
                    });
                }
            }
        }

        NodeConstraint::And(nested_shapes) => {
            // For each value, ALL nested shapes must accept it
            for (i, value) in values.iter().enumerate() {
                let dt = datatypes.get(i);
                for nested in nested_shapes {
                    let conforms = check_value_against_nested_shape(
                        db,
                        value,
                        dt,
                        langs.get(i).and_then(|l| l.as_deref()),
                        nested,
                        parent_shape,
                        all_shapes,
                        class_ctx,
                        active,
                    )
                    .await?;
                    if !conforms {
                        results.push(ValidationResult {
                            focus_node: focus_node.clone(),
                            result_path: prop_shape.path.as_predicate().cloned(),
                            source_shape: parent_shape.id.clone(),
                            source_constraint: Some(prop_shape.id.clone()),
                            severity: prop_shape.severity,
                            message: prop_shape.message.clone().unwrap_or_else(|| {
                                format!(
                                    "Value {:?} does not conform to shape {} (sh:and)",
                                    value, nested.id.name
                                )
                            }),
                            value: Some(value.clone()),
                            graph_id: None,
                        });
                    }
                }
            }
        }

        NodeConstraint::Xone(nested_shapes) => {
            // For each value, exactly ONE nested shape must accept it
            for (i, value) in values.iter().enumerate() {
                let dt = datatypes.get(i);
                let mut conforming_count = 0;

                for nested in nested_shapes {
                    let conforms = check_value_against_nested_shape(
                        db,
                        value,
                        dt,
                        langs.get(i).and_then(|l| l.as_deref()),
                        nested,
                        parent_shape,
                        all_shapes,
                        class_ctx,
                        active,
                    )
                    .await?;
                    if conforms {
                        conforming_count += 1;
                    }
                }

                if conforming_count == 0 {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: prop_shape.path.as_predicate().cloned(),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: prop_shape.message.clone().unwrap_or_else(|| {
                            format!("Value {value:?} does not conform to any shape in sh:xone")
                        }),
                        value: Some(value.clone()),
                        graph_id: None,
                    });
                } else if conforming_count > 1 {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: prop_shape.path.as_predicate().cloned(),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: prop_shape.message.clone().unwrap_or_else(|| {
                            format!(
                                "Value {value:?} conforms to {conforming_count} shapes in sh:xone (must be exactly 1)"
                            )
                        }),
                        value: Some(value.clone()),
                        graph_id: None,
                    });
                }
            }
        }

        NodeConstraint::Node(nested) => {
            // sh:node - each value must conform to the referenced shape
            for (i, value) in values.iter().enumerate() {
                let dt = datatypes.get(i);
                let conforms = check_value_against_nested_shape(
                    db,
                    value,
                    dt,
                    langs.get(i).and_then(|l| l.as_deref()),
                    nested,
                    parent_shape,
                    all_shapes,
                    class_ctx,
                    active,
                )
                .await?;
                if !conforms {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: prop_shape.path.as_predicate().cloned(),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: prop_shape.message.clone().unwrap_or_else(|| {
                            format!(
                                "Value {:?} does not conform to shape {} (sh:node)",
                                value, nested.id.name
                            )
                        }),
                        value: Some(value.clone()),
                        graph_id: None,
                    });
                }
            }
        }

        NodeConstraint::Not(nested) => {
            // For each value, the nested shape must NOT accept it
            for (i, value) in values.iter().enumerate() {
                let dt = datatypes.get(i);
                let conforms = check_value_against_nested_shape(
                    db,
                    value,
                    dt,
                    langs.get(i).and_then(|l| l.as_deref()),
                    nested,
                    parent_shape,
                    all_shapes,
                    class_ctx,
                    active,
                )
                .await?;
                if conforms {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: prop_shape.path.as_predicate().cloned(),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: prop_shape.message.clone().unwrap_or_else(|| {
                            format!(
                                "Value {:?} conforms to shape {} which is not allowed (sh:not)",
                                value, nested.id.name
                            )
                        }),
                        value: Some(value.clone()),
                        graph_id: None,
                    });
                }
            }
        }

        NodeConstraint::Closed { .. } => {
            // Closed constraint at property level is not meaningful — skip
        }
    }

    Ok(results)
}

/// Check whether a single property value conforms to a nested shape.
///
/// For nested shapes with `value_constraints` (anonymous shapes like
/// `[sh:datatype xsd:string]`), validates the constraints directly against
/// the value and datatype. For IRI/blank-node values (`FlakeValue::Ref`),
/// delegates to `validate_nested_shape` which can look up the value as a
/// focus node in the database.
#[allow(clippy::too_many_arguments)]
async fn check_value_against_nested_shape<'a>(
    db: GraphDbRef<'a>,
    value: &FlakeValue,
    datatype: Option<&Sid>,
    lang: Option<&str>,
    nested: &'a NestedShape,
    parent_shape: &'a CompiledShape,
    all_shapes: &'a [&'a CompiledShape],
    class_ctx: Option<ClassMembershipCtx<'a>>,
    active: &'a ActiveShapeChecks,
) -> Result<bool> {
    // If the nested shape has value-level constraints (e.g. sh:datatype without sh:path),
    // check them directly against the value/datatype.
    if !nested.value_constraints.is_empty() {
        // sh:class on an anonymous value shape needs db access for the
        // rdf:type lookup; the pure constraint-set path below skips it.
        for constraint in &nested.value_constraints {
            if let Constraint::Class(expected_class) = constraint {
                let violations = validate_class_constraint(
                    db,
                    std::slice::from_ref(value),
                    expected_class,
                    class_ctx,
                )
                .await?;
                if !violations.is_empty() {
                    return Ok(false);
                }
            }
        }
        let dt_arr: [Sid; 1];
        let dt_slice: &[Sid] = match datatype {
            Some(dt) => {
                dt_arr = [dt.clone()];
                &dt_arr
            }
            None => &[],
        };
        let lang_arr = [lang.map(str::to_string)];
        let violations = validate_constraint_set(
            &nested.value_constraints,
            std::slice::from_ref(value),
            dt_slice,
            &lang_arr,
        )?;
        return Ok(violations.is_empty());
    }

    // For IRI/blank-node values, evaluate the nested shape against the value as a focus node
    if let FlakeValue::Ref(sid) = value {
        let nested_results =
            validate_nested_shape(db, sid, nested, parent_shape, all_shapes, class_ctx, active)
                .await?;
        let has_violations = nested_results
            .iter()
            .any(|r| r.severity == Severity::Violation);
        return Ok(!has_violations);
    }

    // Literal value with no value_constraints — can't evaluate meaningfully.
    // Treat as non-conforming (the nested shape presumably expects something specific).
    Ok(false)
}

/// Replace IRI refs with their full-IRI string form for the string facets
/// (`sh:pattern` / `sh:minLength` / `sh:maxLength`), per SPARQL `STR()`.
/// Blank nodes stay refs (string facets fail on them, per spec); an IRI whose
/// namespace can't be decoded (e.g. allocated in this very transaction) also
/// stays a ref and fails closed.
fn stringify_iri_values(db: GraphDbRef<'_>, values: &[FlakeValue]) -> Vec<FlakeValue> {
    values
        .iter()
        .map(|v| match v {
            FlakeValue::Ref(sid) if sid.namespace_code != BLANK_NODE => db
                .snapshot
                .decode_sid(sid)
                .map(FlakeValue::String)
                .unwrap_or_else(|| v.clone()),
            _ => v.clone(),
        })
        .collect()
}

/// Whether any value is a non-blank IRI ref (candidate for
/// [`stringify_iri_values`]).
fn has_iri_ref(values: &[FlakeValue]) -> bool {
    values
        .iter()
        .any(|v| matches!(v, FlakeValue::Ref(sid) if sid.namespace_code != BLANK_NODE))
}

/// Apply multiple constraints to a set of values and collect all violations.
fn validate_constraint_set(
    constraints: &[Constraint],
    values: &[FlakeValue],
    datatypes: &[Sid],
    langs: &[Option<String>],
) -> Result<Vec<ConstraintViolation>> {
    let mut all_violations = Vec::new();
    for constraint in constraints {
        let violations = validate_constraint(constraint, values, datatypes, langs)?;
        all_violations.extend(violations);
    }
    Ok(all_violations)
}

/// Validate a constraint against a set of values
fn validate_constraint(
    constraint: &Constraint,
    values: &[FlakeValue],
    datatypes: &[Sid],
    langs: &[Option<String>],
) -> Result<Vec<ConstraintViolation>> {
    let mut violations = Vec::new();

    match constraint {
        // Cardinality constraints apply to the value set
        Constraint::MinCount(min) => {
            if let Some(v) = validate_min_count(values, *min) {
                violations.push(v);
            }
        }
        Constraint::MaxCount(max) => {
            if let Some(v) = validate_max_count(values, *max) {
                violations.push(v);
            }
        }

        // Value constraints apply to the value set
        Constraint::HasValue(expected) => {
            if let Some(v) = validate_has_value(values, expected) {
                violations.push(v);
            }
        }

        // Per-value constraints
        Constraint::Datatype(expected_dt) => {
            for (i, value) in values.iter().enumerate() {
                if let Some(actual_dt) = datatypes.get(i) {
                    if let Some(v) = validate_datatype(value, actual_dt, expected_dt) {
                        violations.push(v);
                    }
                }
            }
        }
        Constraint::NodeKind(kind) => {
            for value in values {
                if let Some(v) = validate_node_kind(value, *kind) {
                    violations.push(v);
                }
            }
        }
        Constraint::Class(_class) => {
            // `sh:class` requires DB access to check `rdf:type` of each value.
            // Handled in `validate_property_shape` (this function is the
            // pure-values path without a snapshot).
        }
        Constraint::MinInclusive(min) => {
            for value in values {
                if let Some(v) = validate_min_inclusive(value, min) {
                    violations.push(v);
                }
            }
        }
        Constraint::MaxInclusive(max) => {
            for value in values {
                if let Some(v) = validate_max_inclusive(value, max) {
                    violations.push(v);
                }
            }
        }
        Constraint::MinExclusive(min) => {
            for value in values {
                if let Some(v) = validate_min_exclusive(value, min) {
                    violations.push(v);
                }
            }
        }
        Constraint::MaxExclusive(max) => {
            for value in values {
                if let Some(v) = validate_max_exclusive(value, max) {
                    violations.push(v);
                }
            }
        }
        Constraint::Pattern(pattern, flags) => {
            for value in values {
                if let Some(v) = validate_pattern(value, pattern, flags.as_deref())? {
                    violations.push(v);
                }
            }
        }
        Constraint::MinLength(min) => {
            for value in values {
                if let Some(v) = validate_min_length(value, *min) {
                    violations.push(v);
                }
            }
        }
        Constraint::MaxLength(max) => {
            for value in values {
                if let Some(v) = validate_max_length(value, *max) {
                    violations.push(v);
                }
            }
        }
        Constraint::In(allowed) => {
            for value in values {
                if let Some(v) = validate_in(value, allowed) {
                    violations.push(v);
                }
            }
        }

        // Pair constraints need access to another property's values, so they
        // can't be evaluated from a plain `(values, datatypes)` pair.
        // Handled in `validate_property_shape` via `validate_pair_constraint`.
        Constraint::Equals(_)
        | Constraint::Disjoint(_)
        | Constraint::LessThan(_)
        | Constraint::LessThanOrEquals(_) => {}

        // Language constraints (tags come from flake metadata via `langs`)
        Constraint::UniqueLang(unique) => {
            if *unique {
                violations.extend(validate_unique_lang(values, langs));
            }
        }
        Constraint::LanguageIn(allowed) => {
            for (i, value) in values.iter().enumerate() {
                let lang = langs.get(i).and_then(|l| l.as_deref());
                if let Some(v) = validate_language_in(value, lang, allowed) {
                    violations.push(v);
                }
            }
        }

        // Qualified value shape needs db access for nested-shape conformance
        // counting — handled in `validate_property_shape` (this function is
        // the pure-values path without a snapshot).
        Constraint::QualifiedValueShape { .. } => {}
    }

    Ok(violations)
}

/// Validate a pair constraint (`sh:disjoint`, `sh:lessThan`, `sh:lessThanOrEquals`,
/// or `sh:equals`) given already-loaded values from both properties.
///
/// Returns every violation produced by the underlying per-value helpers so the
/// caller can decorate each with focus-node / source-shape metadata. For the
/// set-level constraints (`equals`, `disjoint`) at most one violation is ever
/// produced; for the pairwise constraints (`lessThan*`) up to one violation
/// per source value is produced.
fn validate_pair_constraint(
    constraint: &Constraint,
    values: &[FlakeValue],
    other_values: &[FlakeValue],
    other_path: &str,
) -> Vec<ConstraintViolation> {
    use crate::constraints::pair::{
        validate_disjoint, validate_equals, validate_less_than, validate_less_than_or_equals,
    };

    let mut out = Vec::new();
    match constraint {
        Constraint::Equals(_) => {
            if let Some(v) = validate_equals(values, other_values, other_path) {
                out.push(v);
            }
        }
        Constraint::Disjoint(_) => {
            if let Some(v) = validate_disjoint(values, other_values, other_path) {
                out.push(v);
            }
        }
        Constraint::LessThan(_) => {
            for value in values {
                if let Some(v) = validate_less_than(value, other_values, other_path) {
                    out.push(v);
                }
            }
        }
        Constraint::LessThanOrEquals(_) => {
            for value in values {
                if let Some(v) = validate_less_than_or_equals(value, other_values, other_path) {
                    out.push(v);
                }
            }
        }
        // Caller is responsible for only passing pair-constraint variants.
        _ => {}
    }
    out
}

/// Validate `sh:class` for a set of property values.
///
/// For each value (which must be a `Ref` — a literal can never be an instance
/// of a class), resolve whether it is (transitively) an instance of
/// `expected_class`. Membership is looked up across the **union** of the focus
/// node's own data graph and any `f:shapesSource` vocabulary graphs threaded in
/// via `class_ctx`, so a shared value-set (e.g. a list of US states defined in
/// the shapes graph) is discoverable even when the referencing records live in
/// a different graph. Resolution uses, in order:
/// 1. Direct / indexed-hierarchy match against the `SchemaHierarchy`.
/// 2. Live `rdfs:subClassOf` walk (schema graph 0 unioned with the vocabulary
///    graphs) for novelty-added or vocabulary-local relations.
///
/// When `class_ctx` is present its per-transaction memo collapses repeated
/// `(value, class, focus graph)` checks to a single lookup. A value with no
/// conforming `rdf:type` is a violation.
async fn validate_class_constraint(
    db: GraphDbRef<'_>,
    values: &[FlakeValue],
    expected_class: &Sid,
    class_ctx: Option<ClassMembershipCtx<'_>>,
) -> Result<Vec<ConstraintViolation>> {
    let mut out = Vec::new();
    if values.is_empty() {
        return Ok(out);
    }

    // Extra vocabulary graphs (f:shapesSource) unioned into the membership
    // lookup. Empty when no context is threaded (e.g. `sh:class` reached via a
    // referenced shape), which preserves the historical data-graph-only lookup.
    let membership_g_ids: &[GraphId] = class_ctx.map(|c| c.membership_g_ids).unwrap_or(&[]);
    // Cross-ledger value-set source, if any.
    let cross_ledger: Option<CrossLedgerMembership<'_>> = class_ctx.and_then(|c| c.cross_ledger);

    // Fast-path acceptable set: expected_class + its descendants per the
    // indexed-schema hierarchy. Misses novelty-added subclass relations;
    // we fall through to `is_subclass_of` (a db walk) for those.
    let hierarchy = db.snapshot.schema_hierarchy();
    let mut hierarchy_accepted: HashSet<Sid> = HashSet::new();
    hierarchy_accepted.insert(expected_class.clone());
    if let Some(h) = &hierarchy {
        for sub in h.subclasses_of(expected_class) {
            hierarchy_accepted.insert(sub.clone());
        }
    }

    for value in values {
        let value_ref = match value {
            FlakeValue::Ref(r) => r,
            other => {
                out.push(ConstraintViolation {
                    constraint: Constraint::Class(expected_class.clone()),
                    value: Some(other.clone()),
                    message: format!(
                        "Value {:?} is a literal and cannot be an instance of class {}",
                        other, expected_class.name
                    ),
                });
                continue;
            }
        };

        // Per-transaction memo keyed on (value, class, focus data graph). The
        // guard is dropped before any `.await` so the validation future stays
        // `Send`. Cache hits also skip the range scan (and its fuel charge), so
        // per-transaction fuel depends on intra-transaction value repetition.
        let cache_key = (value_ref.clone(), expected_class.clone(), db.g_id);
        let cached: Option<bool> = class_ctx.and_then(|c| {
            let guard = c.cache.lock();
            guard.get(&cache_key).copied()
        });

        let conforms = match cached {
            Some(hit) => hit,
            None => {
                let computed = value_conforms_to_class(
                    db,
                    membership_g_ids,
                    cross_ledger,
                    value_ref,
                    &hierarchy_accepted,
                    expected_class,
                )
                .await?;
                if let Some(c) = class_ctx {
                    c.cache.lock().insert(cache_key, computed);
                }
                computed
            }
        };

        if !conforms {
            out.push(ConstraintViolation {
                constraint: Constraint::Class(expected_class.clone()),
                value: Some(value.clone()),
                message: format!(
                    "Value {} is not an instance of class {}",
                    value_ref.name, expected_class.name
                ),
            });
        }
    }

    Ok(out)
}

/// Resolve whether `value_ref` is (transitively) an instance of
/// `expected_class`, consulting the focus node's data graph unioned with
/// `membership_g_ids` (the `f:shapesSource` vocabulary graph[s]). When a
/// cross-ledger model handle is present, it is consulted on demand only after
/// the local lookup misses (so locally-typed values never touch the model
/// ledger).
async fn value_conforms_to_class(
    db: GraphDbRef<'_>,
    membership_g_ids: &[GraphId],
    cross_ledger: Option<CrossLedgerMembership<'_>>,
    value_ref: &Sid,
    hierarchy_accepted: &HashSet<Sid>,
    expected_class: &Sid,
) -> Result<bool> {
    let rdf_type = Sid::new(RDF, rdf_names::TYPE);

    // Graphs to consult for the value's `rdf:type`: the focus data graph plus
    // the configured vocabulary graphs, de-duplicated.
    let mut lookup_g_ids: Vec<GraphId> = vec![db.g_id];
    for &g in membership_g_ids {
        if !lookup_g_ids.contains(&g) {
            lookup_g_ids.push(g);
        }
    }

    let mut value_types: Vec<Sid> = Vec::new();
    for &g in &lookup_g_ids {
        let gdb = rescope_to_graph(db, g);
        let type_flakes = gdb
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject_predicate(value_ref.clone(), rdf_type.clone()),
            )
            .await?;
        for f in &type_flakes {
            if let FlakeValue::Ref(t) = &f.o {
                if !value_types.contains(t) {
                    value_types.push(t.clone());
                }
            }
        }
    }

    // Fast path: any indexed-hierarchy match.
    if value_types.iter().any(|t| hierarchy_accepted.contains(t)) {
        return Ok(true);
    }

    // Slow path: walk the live `rdfs:subClassOf` graph (schema graph 0 unioned
    // with the vocabulary graphs) for novelty-added / vocabulary-local relations.
    for t in &value_types {
        if is_subclass_of(db, membership_g_ids, t, expected_class).await? {
            return Ok(true);
        }
    }

    // Cross-ledger fallback: the controlled vocabulary lives in a model ledger.
    // Only reached when the value isn't typed locally.
    if let Some(cl) = cross_ledger {
        return value_conforms_cross_ledger(cl, value_ref, expected_class).await;
    }

    Ok(false)
}

/// Decode a data-ledger Sid to its IRI using an explicit namespace-code map.
///
/// Mirrors `LedgerSnapshot::decode_sid` but reads from a supplied map so that
/// namespaces this transaction *staged* (absent from the base snapshot) still
/// decode. `EMPTY` / `OVERFLOW` codes carry the full IRI as the name.
fn decode_sid_with_ns_map(ns_map: &HashMap<u16, String>, sid: &Sid) -> Option<String> {
    use fluree_vocab::namespaces::{EMPTY, OVERFLOW};
    if sid.namespace_code == EMPTY || sid.namespace_code == OVERFLOW {
        return Some(sid.name.to_string());
    }
    ns_map
        .get(&sid.namespace_code)
        .map(|prefix| format!("{}{}", prefix, sid.name))
}

/// Resolve `sh:class` membership against a cross-ledger model ledger `M`.
///
/// `value_ref` / `expected_class` are Sids in the data ledger D's term space,
/// so they are decoded to IRIs against D's (staged) namespace map and
/// re-encoded against M — which re-splits with its own mode, so differing
/// namespace-split modes between the ledgers are handled correctly. Well-known
/// vocab predicates (`rdf:type`, `rdfs:subClassOf`) share global namespace
/// codes across ledgers, so only the user IRIs need translation. If M has never
/// seen the value or class IRI, it cannot be a member there.
async fn value_conforms_cross_ledger(
    cl: CrossLedgerMembership<'_>,
    value_ref: &Sid,
    expected_class: &Sid,
) -> Result<bool> {
    let m_db = cl.model_db;
    // D term -> IRI (via D's staged ns map) -> M term. A missing decode/encode
    // means the value/class is simply not known to M -> not a member there.
    let (Some(value_iri), Some(class_iri)) = (
        decode_sid_with_ns_map(cl.data_ns_map, value_ref),
        decode_sid_with_ns_map(cl.data_ns_map, expected_class),
    ) else {
        return Ok(false);
    };
    let (Some(m_value), Some(m_class)) = (
        m_db.snapshot.encode_iri_strict(&value_iri),
        m_db.snapshot.encode_iri_strict(&class_iri),
    ) else {
        return Ok(false);
    };

    let rdf_type = Sid::new(RDF, rdf_names::TYPE);
    let type_flakes = m_db
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(m_value, rdf_type),
        )
        .await?;
    let m_types: Vec<Sid> = type_flakes
        .iter()
        .filter_map(|f| match &f.o {
            FlakeValue::Ref(t) => Some(t.clone()),
            _ => None,
        })
        .collect();

    if m_types.contains(&m_class) {
        return Ok(true);
    }

    // Subclass reasoning within M: walk `subClassOf` over M's value-set graph
    // unioned with M's schema graph (g_id=0).
    for t in &m_types {
        if is_subclass_of(m_db, &[m_db.g_id], t, &m_class).await? {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Rescope a `GraphDbRef` to a specific graph while preserving every other
/// field — tracker, runtime_small_dicts, eager, overlay, snapshot, and t.
///
/// **Do not replace this with `GraphDbRef::new(..)`.** That constructor resets
/// `tracker` (and `runtime_small_dicts`, `eager`) to their defaults, silently
/// disabling fuel accounting on any walk a tracked validation is running. The
/// copy-and-mutate-`g_id` pattern leans on `GraphDbRef: Copy` to carry every
/// field through unchanged.
fn rescope_to_graph(db: GraphDbRef<'_>, g_id: GraphId) -> GraphDbRef<'_> {
    let mut scoped = db;
    scoped.g_id = g_id;
    scoped
}

/// BFS upward from `start` over `rdfs:subClassOf`, returning true if `target`
/// is reachable.
///
/// The walk consults the **schema graph** (`g_id = 0`) unioned with any
/// `membership_g_ids` (the `f:shapesSource` vocabulary graph[s]). Rationale:
/// `rdfs:subClassOf` is schema-level data — the indexed `SchemaHierarchy` is
/// built from the default graph, and this fallback must match that semantic,
/// while a value-set vocabulary configured via `f:shapesSource` may define a
/// small class hierarchy in its own graph that must also be honoured.
///
/// Uses `db.range()` via rescoped `GraphDbRef`s so novelty-added subclass
/// relations are visible — the indexed `SchemaHierarchy` can lag behind.
///
/// Returns `Ok(true)` immediately when `start == target` (every class is a
/// subclass of itself for the purposes of `sh:class`). Cycle-guarded via a
/// `visited` set, since `rdfs:subClassOf` graphs in user data can be malformed.
async fn is_subclass_of(
    db: GraphDbRef<'_>,
    membership_g_ids: &[GraphId],
    start: &Sid,
    target: &Sid,
) -> Result<bool> {
    use std::collections::VecDeque;

    if start == target {
        return Ok(true);
    }

    // Graphs holding subClassOf edges: the schema graph plus the vocabulary
    // graphs, de-duplicated. Rescoping preserves the caller's tracker — see
    // `rescope_to_graph` for why `GraphDbRef::new(..)` must NOT be used.
    let mut walk_g_ids: Vec<GraphId> = vec![0];
    for &g in membership_g_ids {
        if !walk_g_ids.contains(&g) {
            walk_g_ids.push(g);
        }
    }

    let sub_class_of = Sid::new(fluree_vocab::namespaces::RDFS, "subClassOf");
    let mut visited: HashSet<Sid> = HashSet::new();
    visited.insert(start.clone());
    let mut queue: VecDeque<Sid> = VecDeque::new();
    queue.push_back(start.clone());

    while let Some(current) = queue.pop_front() {
        for &g in &walk_g_ids {
            let scoped = rescope_to_graph(db, g);
            let flakes = scoped
                .range(
                    IndexType::Spot,
                    RangeTest::Eq,
                    RangeMatch::subject_predicate(current.clone(), sub_class_of.clone()),
                )
                .await?;
            for f in flakes {
                if let FlakeValue::Ref(parent) = &f.o {
                    if parent == target {
                        return Ok(true);
                    }
                    if visited.insert(parent.clone()) {
                        queue.push_back(parent.clone());
                    }
                }
            }
        }
    }
    Ok(false)
}

/// SHACL validation report
#[derive(Debug, Clone)]
pub struct ValidationReport {
    /// Whether all shapes conform (no Violation-level results)
    pub conforms: bool,
    /// Individual validation results
    pub results: Vec<ValidationResult>,
}

impl ValidationReport {
    /// Create an empty conforming report
    pub fn conforming() -> Self {
        Self {
            conforms: true,
            results: Vec::new(),
        }
    }

    /// Count violations (Severity::Violation results)
    pub fn violation_count(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.severity == Severity::Violation)
            .count()
    }

    /// Count warnings (Severity::Warning results)
    pub fn warning_count(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.severity == Severity::Warning)
            .count()
    }
}

/// Individual validation result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// The focus node that was validated
    pub focus_node: Sid,
    /// The property path (if property constraint)
    pub result_path: Option<Sid>,
    /// The shape that produced this result
    pub source_shape: Sid,
    /// The constraint component that produced this result
    pub source_constraint: Option<Sid>,
    /// Severity level
    pub severity: Severity,
    /// Human-readable message
    pub message: String,
    /// The value that caused the violation (if applicable)
    pub value: Option<FlakeValue>,
    /// The graph where the focus node was being validated. Populated by the
    /// staged-validation path (`validate_staged_nodes`) so that callers can
    /// apply per-graph SHACL policy (e.g. warn vs reject, enable/disable).
    /// `None` for non-staged paths (e.g. `validate_all`).
    pub graph_id: Option<GraphId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ShaclCacheKey;
    use fluree_db_core::GraphDbRef;

    /// Regression: `rescope_to_graph` — used by the `sh:class` value-membership
    /// and fallback subclass walks — must preserve the caller's tracker (and
    /// other per-validation context). A naive rebuild via `GraphDbRef::new(..)`
    /// would silently drop `tracker`, disabling fuel accounting on tracked
    /// validations. This pins the invariant.
    #[test]
    fn rescope_to_graph_preserves_tracker_and_other_fields() {
        use fluree_db_core::tracking::TrackingOptions;
        use fluree_db_core::{LedgerSnapshot, NoOverlay, Tracker};

        let snapshot = LedgerSnapshot::genesis("test:schema-rescope");
        let tracker = Tracker::new(TrackingOptions {
            track_time: false,
            track_fuel: true,
            track_policy: false,
            max_fuel: Some(1000),
        });
        assert!(tracker.is_enabled(), "tracker must be enabled for the test");

        let db = GraphDbRef::new(&snapshot, 7, &NoOverlay, snapshot.t)
            .with_tracker(&tracker)
            .eager();
        assert_eq!(db.g_id, 7, "precondition: caller is in a non-default graph");
        assert!(
            db.tracker.is_some(),
            "precondition: caller's db has tracker attached"
        );
        assert!(db.eager, "precondition: caller's db is eager");

        let schema_db = super::rescope_to_graph(db, 0);

        assert_eq!(schema_db.g_id, 0, "schema walk must run in default graph");
        assert!(
            schema_db.tracker.is_some(),
            "tracker must survive rescope — otherwise fuel accounting is lost on \
             the fallback subClassOf walk"
        );
        assert!(schema_db.eager, "eager flag must survive rescope");
        assert_eq!(schema_db.t, db.t, "as-of time must be preserved");
        assert!(
            std::ptr::eq(schema_db.overlay, db.overlay),
            "overlay reference must be preserved"
        );
    }

    #[test]
    fn test_engine_no_shapes_optimization() {
        // Create an empty cache (no shapes)
        let key = ShaclCacheKey::new("test", 1);
        let cache = ShaclCache::new(key, vec![], None);
        let engine = ShaclEngine::new(cache);

        // Engine should report no shapes
        assert!(!engine.has_shapes());
        assert!(engine.is_empty());
        assert_eq!(engine.shape_count(), 0);
    }

    #[test]
    fn test_engine_with_shapes() {
        use crate::compile::{CompiledShape, TargetType};
        use fluree_db_core::SidInterner;

        let interner = SidInterner::new();
        let shape = CompiledShape {
            id: interner.intern(100, "TestShape"),
            targets: vec![TargetType::Node(vec![interner.intern(100, "ex:alice")])],
            property_shapes: vec![],
            node_constraints: vec![],
            structural_constraints: vec![],
            severity: Severity::Violation,
            name: None,
            message: None,
            deactivated: false,
        };

        let key = ShaclCacheKey::new("test", 1);
        let cache = ShaclCache::new(key, vec![shape], None);
        let engine = ShaclEngine::new(cache);

        // Engine should report having shapes
        assert!(engine.has_shapes());
        assert!(!engine.is_empty());
        assert_eq!(engine.shape_count(), 1);
    }

    #[tokio::test]
    async fn test_validate_staged_empty_shapes_returns_conforming() {
        // This is the key optimization test:
        // When there are no shapes, validate_staged should return immediately
        // without doing any database work.

        use fluree_db_core::LedgerSnapshot;

        let snapshot = LedgerSnapshot::genesis("test:main");

        // Empty cache (no shapes)
        let key = ShaclCacheKey::new("test", 1);
        let shacl_cache = ShaclCache::new(key, vec![], None);
        let engine = ShaclEngine::new(shacl_cache);

        // Even with subjects to validate, should return conforming immediately
        let mut modified_subjects = HashSet::new();
        modified_subjects.insert(Sid::new(100, "ex:alice"));
        modified_subjects.insert(Sid::new(100, "ex:bob"));

        let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
        let report = engine
            .validate_staged(db, &modified_subjects)
            .await
            .expect("validation should succeed");

        // Should conform (no shapes = nothing to violate)
        assert!(report.conforms);
        assert_eq!(report.results.len(), 0);
    }

    #[tokio::test]
    async fn test_validate_staged_empty_subjects_returns_conforming() {
        use fluree_db_core::LedgerSnapshot;

        let snapshot = LedgerSnapshot::genesis("test:main");

        // Even with shapes, if no subjects modified, should return conforming
        use crate::compile::{CompiledShape, TargetType};

        let shape = CompiledShape {
            id: Sid::new(100, "TestShape"),
            targets: vec![TargetType::Class(Sid::new(100, "ex:Person"))],
            property_shapes: vec![],
            node_constraints: vec![],
            structural_constraints: vec![],
            severity: Severity::Violation,
            name: None,
            message: None,
            deactivated: false,
        };

        let key = ShaclCacheKey::new("test", 1);
        let shacl_cache = ShaclCache::new(key, vec![shape], None);
        let engine = ShaclEngine::new(shacl_cache);

        // Empty subject set
        let modified_subjects = HashSet::new();

        let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
        let report = engine
            .validate_staged(db, &modified_subjects)
            .await
            .expect("validation should succeed");

        // Should conform (no subjects = nothing to validate)
        assert!(report.conforms);
        assert_eq!(report.results.len(), 0);
    }

    #[test]
    fn test_validation_report_conforming() {
        let report = ValidationReport::conforming();
        assert!(report.conforms);
        assert_eq!(report.results.len(), 0);
        assert_eq!(report.violation_count(), 0);
        assert_eq!(report.warning_count(), 0);
    }
}
