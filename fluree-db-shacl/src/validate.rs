//! SHACL validation engine
//!
//! This module provides the core validation logic for checking RDF data
//! against SHACL shapes.

use crate::cache::{ShaclCache, ShaclCacheKey};
use crate::compile::{CompiledShape, PropertyShape, Severity, ShapeCompiler, TargetType};
use crate::constraints::cardinality::{validate_max_count, validate_min_count};
use crate::constraints::datatype::{validate_datatype, validate_node_kind};
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
use fluree_vocab::namespaces::RDF;
use fluree_vocab::rdf_names;
use std::collections::HashSet;

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
}

impl ShaclEngine {
    /// Create a new engine from a cache (without hierarchy support)
    ///
    /// For full RDFS reasoning support, use `new_with_hierarchy` instead.
    pub fn new(cache: ShaclCache) -> Self {
        Self {
            cache,
            hierarchy: None,
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

        Ok(Self { cache, hierarchy })
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

        // Validate against each shape
        for shape in applicable_shapes {
            if shape.deactivated {
                continue;
            }

            let shape_results = validate_shape(db, focus_node, shape, &all_shapes).await?;
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
        self.validate_node(db, focus_node, node_types).await
    }

    /// Validate all focus nodes targeted by shapes
    pub async fn validate_all(&self, db: GraphDbRef<'_>) -> Result<ValidationReport> {
        let mut all_results = Vec::new();

        // Collect all shapes for logical constraint resolution
        let all_shapes: Vec<&CompiledShape> = self.cache.all_shapes().iter().collect();

        for shape in self.cache.all_shapes() {
            if shape.deactivated {
                continue;
            }

            // Get focus nodes for this shape (with hierarchy expansion)
            let focus_nodes = get_focus_nodes(db, shape, self.hierarchy.as_ref()).await?;

            for focus_node in focus_nodes {
                let results = validate_shape(db, &focus_node, shape, &all_shapes).await?;
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
            let report = self.validate_node(db, subject, &node_types).await?;
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
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ValidationResult>>> + Send + 'a>>
{
    Box::pin(async move {
        let mut results = Vec::new();

        // Validate property shapes
        for prop_shape in &shape.property_shapes {
            let prop_results =
                validate_property_shape(db, focus_node, prop_shape, shape, all_shapes).await?;
            results.extend(prop_results);
        }

        // Validate structural constraints (closed, logical)
        for constraint in &shape.structural_constraints {
            let constraint_results =
                validate_structural_constraint(db, focus_node, constraint, shape, all_shapes)
                    .await?;
            results.extend(constraint_results);
        }

        Ok(results)
    })
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
                    let declared_properties: std::collections::HashSet<&Sid> = parent_shape
                        .property_shapes
                        .iter()
                        .map(|ps| &ps.path)
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
                                severity: Severity::Violation,
                                message: format!(
                                    "Property {} not allowed by closed shape",
                                    prop.name
                                ),
                                value: Some(flake.o.clone()),
                                graph_id: None,
                            });
                        }
                    }
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
                        severity: Severity::Violation,
                        message: format!(
                            "Node conforms to shape {} which is not allowed (sh:not)",
                            nested_shape.id.name
                        ),
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
                                severity: Severity::Violation,
                                message: format!("sh:and constraint - {}", r.message),
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
                        severity: Severity::Violation,
                        message: format!(
                            "Node does not conform to any shape in sh:or. Violations: {}",
                            all_messages.join("; ")
                        ),
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
                        severity: Severity::Violation,
                        message: "Node does not conform to any shape in sh:xone".to_string(),
                        value: None,
                        graph_id: None,
                    });
                } else if conforming_count > 1 {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: None,
                        source_shape: parent_shape.id.clone(),
                        source_constraint: None,
                        severity: Severity::Violation,
                        message: format!(
                            "Node conforms to {} shapes in sh:xone (must be exactly 1): {}",
                            conforming_count,
                            conforming_shapes.join(", ")
                        ),
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
                return validate_shape(db, focus_node, ref_shape, all_shapes).await;
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
            // Get all values for this property on the focus node
            let flakes = db
                .range(
                    IndexType::Spot,
                    RangeTest::Eq,
                    RangeMatch::subject_predicate(focus_node.clone(), path.clone()),
                )
                .await?;

            let values: Vec<FlakeValue> = flakes.iter().map(|f| f.o.clone()).collect();
            let datatypes: Vec<Sid> = flakes.iter().map(|f| f.dt.clone()).collect();

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
                                result_path: Some(path.clone()),
                                source_shape: parent_shape.id.clone(),
                                source_constraint: Some(nested.id.clone()),
                                severity: Severity::Violation,
                                message: format!(
                                    "Value set for {} does not equal value set for {}",
                                    path.name, target_prop.name
                                ),
                                value: None,
                                graph_id: None,
                            });
                        }
                    }
                    _ => {
                        let violations = validate_constraint(constraint, &values, &datatypes)?;
                        for violation in violations {
                            results.push(ValidationResult {
                                focus_node: focus_node.clone(),
                                result_path: Some(path.clone()),
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
            )
            .await?;
            results.extend(nested_results);
        }

        Ok(results)
    })
}

/// Validate a focus node against a property shape
async fn validate_property_shape<'a>(
    db: GraphDbRef<'a>,
    focus_node: &Sid,
    prop_shape: &PropertyShape,
    parent_shape: &'a CompiledShape,
    all_shapes: &'a [&'a CompiledShape],
) -> Result<Vec<ValidationResult>> {
    let mut results = Vec::new();

    // Get all values for this property on the focus node
    let flakes = db
        .range(
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject_predicate(focus_node.clone(), prop_shape.path.clone()),
        )
        .await?;

    let values: Vec<FlakeValue> = flakes.iter().map(|f| f.o.clone()).collect();
    let datatypes: Vec<Sid> = flakes.iter().map(|f| f.dt.clone()).collect();

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
                        result_path: Some(prop_shape.path.clone()),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: violation.message,
                        value: violation.value,
                        graph_id: None,
                    });
                }
            }
            Constraint::Class(expected_class) => {
                let class_violations =
                    validate_class_constraint(db, &values, expected_class).await?;
                for violation in class_violations {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: Some(prop_shape.path.clone()),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: violation.message,
                        value: violation.value,
                        graph_id: None,
                    });
                }
            }
            _ => {
                // Handle other constraints
                let violations = validate_constraint(constraint, &values, &datatypes)?;

                for violation in violations {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: Some(prop_shape.path.clone()),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: violation.message,
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
            structural,
            prop_shape,
            parent_shape,
            all_shapes,
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
    constraint: &'a NodeConstraint,
    prop_shape: &PropertyShape,
    parent_shape: &'a CompiledShape,
    all_shapes: &'a [&'a CompiledShape],
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
                        nested,
                        parent_shape,
                        all_shapes,
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
                        result_path: Some(prop_shape.path.clone()),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: format!(
                            "Value {:?} does not conform to any shape in sh:or (tried: {})",
                            value,
                            all_messages.join(", ")
                        ),
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
                        nested,
                        parent_shape,
                        all_shapes,
                    )
                    .await?;
                    if !conforms {
                        results.push(ValidationResult {
                            focus_node: focus_node.clone(),
                            result_path: Some(prop_shape.path.clone()),
                            source_shape: parent_shape.id.clone(),
                            source_constraint: Some(prop_shape.id.clone()),
                            severity: prop_shape.severity,
                            message: format!(
                                "Value {:?} does not conform to shape {} (sh:and)",
                                value, nested.id.name
                            ),
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
                        nested,
                        parent_shape,
                        all_shapes,
                    )
                    .await?;
                    if conforms {
                        conforming_count += 1;
                    }
                }

                if conforming_count == 0 {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: Some(prop_shape.path.clone()),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: format!(
                            "Value {value:?} does not conform to any shape in sh:xone"
                        ),
                        value: Some(value.clone()),
                        graph_id: None,
                    });
                } else if conforming_count > 1 {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: Some(prop_shape.path.clone()),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: format!(
                            "Value {value:?} conforms to {conforming_count} shapes in sh:xone (must be exactly 1)"
                        ),
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
                    nested,
                    parent_shape,
                    all_shapes,
                )
                .await?;
                if conforms {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: Some(prop_shape.path.clone()),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: format!(
                            "Value {:?} conforms to shape {} which is not allowed (sh:not)",
                            value, nested.id.name
                        ),
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
async fn check_value_against_nested_shape<'a>(
    db: GraphDbRef<'a>,
    value: &FlakeValue,
    datatype: Option<&Sid>,
    nested: &'a NestedShape,
    parent_shape: &'a CompiledShape,
    all_shapes: &'a [&'a CompiledShape],
) -> Result<bool> {
    // If the nested shape has value-level constraints (e.g. sh:datatype without sh:path),
    // check them directly against the value/datatype.
    if !nested.value_constraints.is_empty() {
        let dt_arr: [Sid; 1];
        let dt_slice: &[Sid] = match datatype {
            Some(dt) => {
                dt_arr = [dt.clone()];
                &dt_arr
            }
            None => &[],
        };
        let violations = validate_constraint_set(
            &nested.value_constraints,
            std::slice::from_ref(value),
            dt_slice,
        )?;
        return Ok(violations.is_empty());
    }

    // For IRI/blank-node values, evaluate the nested shape against the value as a focus node
    if let FlakeValue::Ref(sid) = value {
        let nested_results =
            validate_nested_shape(db, sid, nested, parent_shape, all_shapes).await?;
        let has_violations = nested_results
            .iter()
            .any(|r| r.severity == Severity::Violation);
        return Ok(!has_violations);
    }

    // Literal value with no value_constraints — can't evaluate meaningfully.
    // Treat as non-conforming (the nested shape presumably expects something specific).
    Ok(false)
}

/// Apply multiple constraints to a set of values and collect all violations.
fn validate_constraint_set(
    constraints: &[Constraint],
    values: &[FlakeValue],
    datatypes: &[Sid],
) -> Result<Vec<ConstraintViolation>> {
    let mut all_violations = Vec::new();
    for constraint in constraints {
        let violations = validate_constraint(constraint, values, datatypes)?;
        all_violations.extend(violations);
    }
    Ok(all_violations)
}

/// Validate a constraint against a set of values
fn validate_constraint(
    constraint: &Constraint,
    values: &[FlakeValue],
    datatypes: &[Sid],
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

        // Language constraints
        // Note: Language tags are stored in the flake's datatype field (rdf:langString)
        // with the language as a separate attribute. Full validation requires access to
        // language metadata which is not available in this simplified validation path.
        Constraint::UniqueLang(_unique) => {
            // TODO: Implement when language metadata is available
            // Requires checking the language tag from flake metadata, not FlakeValue
        }
        Constraint::LanguageIn(_allowed_langs) => {
            // TODO: Implement when language metadata is available
            // Requires checking the language tag from flake metadata, not FlakeValue
        }

        // Qualified value shape - requires nested validation
        Constraint::QualifiedValueShape { .. } => {
            // TODO: Implement qualified value shape validation
            // This requires recursive shape validation
        }
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
/// of a class), look up `rdf:type` flakes and check conformance via:
/// 1. Direct match: value's type == `expected_class`
/// 2. Indexed-schema hierarchy: type is a descendant of `expected_class` per
///    the `SchemaHierarchy` cached on the snapshot (fast, but only reflects
///    already-indexed subclass relations)
/// 3. Live subclass walk: BFS upward over `rdfs:subClassOf` via `db.range()`,
///    which sees novelty-added relations that aren't yet in the hierarchy
///
/// A value with no conforming `rdf:type` is a violation.
async fn validate_class_constraint(
    db: GraphDbRef<'_>,
    values: &[FlakeValue],
    expected_class: &Sid,
) -> Result<Vec<ConstraintViolation>> {
    let mut out = Vec::new();
    if values.is_empty() {
        return Ok(out);
    }

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

    let rdf_type = Sid::new(RDF, rdf_names::TYPE);
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

        let type_flakes = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject_predicate(value_ref.clone(), rdf_type.clone()),
            )
            .await?;

        let value_types: Vec<Sid> = type_flakes
            .iter()
            .filter_map(|f| match &f.o {
                FlakeValue::Ref(t) => Some(t.clone()),
                _ => None,
            })
            .collect();

        // Fast path: any indexed-hierarchy match.
        let mut conforms = value_types.iter().any(|t| hierarchy_accepted.contains(t));

        // Slow path: walk the live `rdfs:subClassOf` graph (covers novelty-added
        // subclass relations that haven't made it into the hierarchy yet).
        if !conforms {
            for t in &value_types {
                if is_subclass_of(db, t, expected_class).await? {
                    conforms = true;
                    break;
                }
            }
        }

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

/// Rescope a `GraphDbRef` to the default (schema) graph while preserving
/// every other field — tracker, runtime_small_dicts, eager, overlay,
/// snapshot, and t.
///
/// **Do not replace this with `GraphDbRef::new(db.snapshot, 0, db.overlay, db.t)`.**
/// That constructor resets `tracker` (and `runtime_small_dicts`, `eager`) to
/// their defaults, which silently disables fuel accounting on any schema
/// walks a tracked validation is running. The copy-and-mutate-`g_id` pattern
/// below leans on `GraphDbRef: Copy` to carry every field through unchanged.
fn rescope_to_schema_graph(db: GraphDbRef<'_>) -> GraphDbRef<'_> {
    let mut schema_db = db;
    schema_db.g_id = 0;
    schema_db
}

/// BFS upward from `start` over `rdfs:subClassOf`, returning true if `target`
/// is reachable.
///
/// The walk is scoped to the **default graph** (`g_id = 0`), not the caller's
/// graph. Rationale: `rdfs:subClassOf` is schema-level data — the indexed
/// `SchemaHierarchy` is built exclusively from the default graph, and this
/// fallback walk must match that semantic. Otherwise a subject being validated
/// in graph `G` would not see a subclass edge asserted in the schema graph.
///
/// Uses `db.range()` via a rebuilt `GraphDbRef` so novelty-added subclass
/// relations are visible — the indexed `SchemaHierarchy` can lag behind.
///
/// Returns `Ok(true)` immediately when `start == target` (every class is a
/// subclass of itself for the purposes of `sh:class`). Cycle-guarded via a
/// `visited` set, since `rdfs:subClassOf` graphs in user data can be malformed.
async fn is_subclass_of(db: GraphDbRef<'_>, start: &Sid, target: &Sid) -> Result<bool> {
    use std::collections::VecDeque;

    if start == target {
        return Ok(true);
    }

    // Schema relations live in g_id=0. Use `rescope_to_schema_graph` so the
    // caller's tracker and other per-validation context survive — see the
    // function's docstring for why `GraphDbRef::new(..)` must NOT be used.
    let schema_db = rescope_to_schema_graph(db);

    let sub_class_of = Sid::new(fluree_vocab::namespaces::RDFS, "subClassOf");
    let mut visited: HashSet<Sid> = HashSet::new();
    visited.insert(start.clone());
    let mut queue: VecDeque<Sid> = VecDeque::new();
    queue.push_back(start.clone());

    while let Some(current) = queue.pop_front() {
        let flakes = schema_db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject_predicate(current, sub_class_of.clone()),
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

    /// Regression: `rescope_to_schema_graph` — used by the `sh:class` fallback
    /// subclass walk — must preserve the caller's tracker (and other
    /// per-validation context). A naive rebuild via `GraphDbRef::new(..)`
    /// would silently drop `tracker`, disabling fuel accounting on tracked
    /// validations. This pins the invariant.
    #[test]
    fn rescope_to_schema_graph_preserves_tracker_and_other_fields() {
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

        let schema_db = super::rescope_to_schema_graph(db);

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
