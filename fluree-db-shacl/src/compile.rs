//! Shape compilation from SHACL flakes
//!
//! This module compiles SHACL shape definitions from database flakes into
//! efficient `CompiledShape` structures that can be used for validation.

use crate::constraints::{Constraint, NestedShape, NodeConstraint};
use crate::error::Result;
use crate::path::{resolve_sh_path, PropertyPath};
use crate::predicates;
use fluree_db_core::{Flake, FlakeValue, GraphDbRef, IndexType, RangeMatch, RangeTest, Sid};
use fluree_vocab::namespaces::{RDF, SHACL};
use fluree_vocab::rdf_names;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Unique identifier for a shape
pub type ShapeId = Sid;

/// How a shape selects its target focus nodes
#[derive(Debug, Clone, PartialEq)]
pub enum TargetType {
    /// sh:targetClass - all instances of the class
    Class(Sid),
    /// sh:targetNode - specific node(s)
    Node(Vec<Sid>),
    /// sh:targetSubjectsOf - subjects of triples with this predicate
    SubjectsOf(Sid),
    /// sh:targetObjectsOf - objects of triples with this predicate
    ObjectsOf(Sid),
    /// Implicit class targeting (shape is also a class)
    ImplicitClass(Sid),
}

/// Severity level for constraint violations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Severity {
    #[default]
    Violation,
    Warning,
    Info,
}

/// A compiled property shape
#[derive(Debug, Clone)]
pub struct PropertyShape {
    /// The shape ID (blank node or IRI)
    pub id: ShapeId,
    /// The compiled `sh:path` expression (a single predicate or a path AST).
    pub path: PropertyPath,
    /// Constraints on this property
    pub constraints: Vec<Constraint>,
    /// Per-value structural constraints (sh:or/sh:and/sh:xone/sh:not on a property shape).
    /// Unlike `CompiledShape::structural_constraints` which apply to the focus node,
    /// these are evaluated against each value of the property individually.
    pub value_structural_constraints: Vec<NodeConstraint>,
    /// Severity level for violations
    pub severity: Severity,
    /// Human-readable name
    pub name: Option<String>,
    /// Human-readable message for violations
    pub message: Option<String>,
}

/// A compiled node shape
#[derive(Debug, Clone)]
pub struct CompiledShape {
    /// The shape ID
    pub id: ShapeId,
    /// How this shape targets focus nodes
    pub targets: Vec<TargetType>,
    /// Property shapes (constraints on specific properties)
    pub property_shapes: Vec<PropertyShape>,
    /// Constraints directly on the node (not property-specific)
    pub node_constraints: Vec<Constraint>,
    /// Node-level structural constraints (sh:closed, logical constraints)
    pub structural_constraints: Vec<NodeConstraint>,
    /// Severity level for violations
    pub severity: Severity,
    /// Human-readable name
    pub name: Option<String>,
    /// Human-readable message for violations
    pub message: Option<String>,
    /// Whether this shape is deactivated (sh:deactivated true)
    pub deactivated: bool,
}

impl CompiledShape {
    /// Check if this shape has any targets
    pub fn has_targets(&self) -> bool {
        !self.targets.is_empty()
    }

    /// Check if this shape targets a specific class
    pub fn targets_class(&self, class: &Sid) -> bool {
        self.targets.iter().any(|t| match t {
            TargetType::Class(c) | TargetType::ImplicitClass(c) => c == class,
            _ => false,
        })
    }
}

/// Builder for compiling shapes from database flakes
pub struct ShapeCompiler {
    /// Collected shape data by shape ID
    shapes: HashMap<ShapeId, ShapeData>,
    /// Collected property shape data by property shape ID
    property_shapes: HashMap<ShapeId, PropertyShapeData>,
}

/// Intermediate representation during compilation
#[derive(Default)]
struct ShapeData {
    targets: Vec<TargetType>,
    property_shape_ids: Vec<ShapeId>,
    node_constraints: Vec<Constraint>,
    /// sh:closed
    is_closed: Option<bool>,
    /// sh:ignoredProperties (list of property SIDs)
    ignored_properties: HashSet<Sid>,
    /// sh:node - references to shapes the node (or each property value, when
    /// this entry backs a property shape) must conform to
    node_shapes: Vec<Sid>,
    /// sh:not - reference to a shape that must NOT match
    not_shape: Option<Sid>,
    /// sh:and - reference to RDF list head (expanded during list processing)
    and_list: Option<Sid>,
    /// sh:and - expanded shape references
    and_shapes: Vec<Sid>,
    /// sh:or - reference to RDF list head (expanded during list processing)
    or_list: Option<Sid>,
    /// sh:or - expanded shape references
    or_shapes: Vec<Sid>,
    /// sh:xone - reference to RDF list head (expanded during list processing)
    xone_list: Option<Sid>,
    /// sh:xone - expanded shape references
    xone_shapes: Vec<Sid>,
    severity: Severity,
    name: Option<String>,
    message: Option<String>,
    deactivated: bool,
}

/// Intermediate representation for property shapes
#[derive(Default)]
struct PropertyShapeData {
    /// Raw `sh:path` object (a predicate IRI or a path-expression blank node).
    path: Option<Sid>,
    /// `path` compiled into a [`PropertyPath`] AST (filled by `resolve_paths`).
    resolved_path: Option<PropertyPath>,
    constraints: Vec<Constraint>,
    severity: Severity,
    name: Option<String>,
    message: Option<String>,
    /// sh:flags for pattern constraint (combined during finalize)
    pattern_flags: Option<String>,
    /// Temporary storage for pattern string (combined with flags in finalize)
    pattern_string: Option<String>,
    /// sh:in list values (accumulated from RDF list traversal)
    in_values: Vec<FlakeValue>,
    /// sh:languageIn values (String tags from JSON-LD @list flattening, or a
    /// single Ref to a Turtle RDF-list head expanded in expand_rdf_lists)
    language_in_values: Vec<FlakeValue>,
    /// sh:deactivated — a deactivated property shape is skipped entirely
    deactivated: bool,
    /// sh:qualifiedValueShape — reference to the shape conforming values are
    /// counted against (combined with the counts in finalize)
    qualified_shape: Option<Sid>,
    /// sh:qualifiedMinCount
    qualified_min: Option<usize>,
    /// sh:qualifiedMaxCount
    qualified_max: Option<usize>,
    /// sh:qualifiedValueShapesDisjoint
    qualified_disjoint: bool,
}

impl ShapeCompiler {
    /// Create a new shape compiler
    pub fn new() -> Self {
        Self {
            shapes: HashMap::new(),
            property_shapes: HashMap::new(),
        }
    }

    /// Compile shapes from a single graph (convenience over
    /// [`Self::compile_from_dbs`]).
    ///
    /// Queries both the indexed database and any novelty overlay attached to
    /// `db` — important because shapes may be defined in the same transaction
    /// as the data they validate.
    pub async fn compile_from_db(db: GraphDbRef<'_>) -> Result<Vec<CompiledShape>> {
        Self::compile_from_dbs(std::slice::from_ref(&db)).await
    }

    /// Compile shapes from multiple graphs into a single shape set.
    ///
    /// Used when `f:shapesSource` resolves to a non-default graph (or when
    /// the operator wants to split schema across multiple graphs and merge
    /// them at validation time). Each `GraphDbRef` is scanned for SHACL
    /// predicates; results are accumulated into one `ShapeCompiler` so that
    /// cross-graph shape references (e.g. `sh:and` of a shape defined in
    /// another graph) and RDF list expansion still resolve correctly.
    ///
    /// Each `GraphDbRef` carries its own snapshot + overlay, so novelty
    /// visibility is preserved per input graph.
    pub async fn compile_from_dbs(dbs: &[GraphDbRef<'_>]) -> Result<Vec<CompiledShape>> {
        let mut compiler = Self::new();

        // Query for all SHACL predicates to find shapes
        // We look for subjects that have SHACL predicates
        let shacl_predicates = [
            // Targeting
            predicates::TARGET_CLASS,
            predicates::TARGET_NODE,
            predicates::TARGET_SUBJECTS_OF,
            predicates::TARGET_OBJECTS_OF,
            // Property shape
            predicates::PROPERTY,
            predicates::PATH,
            // Cardinality
            predicates::MIN_COUNT,
            predicates::MAX_COUNT,
            // Value type
            predicates::DATATYPE,
            predicates::NODE_KIND,
            predicates::CLASS,
            // Value range
            predicates::MIN_INCLUSIVE,
            predicates::MAX_INCLUSIVE,
            predicates::MIN_EXCLUSIVE,
            predicates::MAX_EXCLUSIVE,
            // String
            predicates::PATTERN,
            predicates::FLAGS,
            predicates::MIN_LENGTH,
            predicates::MAX_LENGTH,
            // Value
            predicates::HAS_VALUE,
            predicates::IN,
            // Pair constraints
            predicates::EQUALS,
            predicates::DISJOINT,
            predicates::LESS_THAN,
            predicates::LESS_THAN_OR_EQUALS,
            // Closed shape
            predicates::CLOSED,
            predicates::IGNORED_PROPERTIES,
            // Language
            predicates::UNIQUE_LANG,
            predicates::LANGUAGE_IN,
            // Shape-based constraints
            predicates::NODE,
            predicates::QUALIFIED_VALUE_SHAPE,
            predicates::QUALIFIED_MIN_COUNT,
            predicates::QUALIFIED_MAX_COUNT,
            predicates::QUALIFIED_VALUE_SHAPES_DISJOINT,
            // Logical constraints
            predicates::NOT,
            predicates::AND,
            predicates::OR,
            predicates::XONE,
            // Metadata
            predicates::DEACTIVATED,
            predicates::SEVERITY,
            predicates::MESSAGE,
            predicates::NAME,
        ];

        // Query each input graph for all SHACL predicates, accumulating into
        // one compiler so cross-graph sh:and/or/xone/sh:in references resolve.
        let mut class_typed: HashSet<Sid> = HashSet::new();
        for db in dbs {
            for pred_name in &shacl_predicates {
                let pred = Sid::new(SHACL, pred_name);
                let flakes = db
                    .range(IndexType::Psot, RangeTest::Eq, RangeMatch::predicate(pred))
                    .await?;

                for flake in flakes {
                    compiler.process_flake(&flake)?;
                }
            }

            // Collect subjects typed as a class — a shape that is also a class
            // implicitly targets its own instances (SHACL "implicit class
            // targets"). Bound-object scans, so cost scales with the number of
            // declared classes, not the data.
            let rdf_type = Sid::new(RDF, rdf_names::TYPE);
            for class_class in [
                Sid::new(fluree_vocab::namespaces::RDFS, "Class"),
                Sid::new(fluree_vocab::namespaces::OWL, "Class"),
            ] {
                let flakes = db
                    .range(
                        IndexType::Opst,
                        RangeTest::Eq,
                        RangeMatch::predicate_object(
                            rdf_type.clone(),
                            FlakeValue::Ref(class_class),
                        ),
                    )
                    .await?;
                class_typed.extend(flakes.iter().map(|f| f.s.clone()));
            }

            // Expand rdf:first/rdf:rest lists referenced by sh:in / sh:and /
            // sh:or / sh:xone / sh:ignoredProperties. Run after each graph so
            // that lists whose head lives in this graph can resolve — a list
            // spanning multiple graphs will still resolve on a later pass
            // because `expand_rdf_lists` walks transitively via `db.range`.
            compiler.expand_rdf_lists(*db).await?;

            // Resolve each property shape's `sh:path` into a path AST. Runs per
            // graph so a path whose blank-node structure lives in this graph can
            // resolve; a plain-predicate path resolves trivially on any graph.
            compiler.resolve_paths(*db).await?;
        }

        compiler.apply_implicit_class_targets(&class_typed);
        compiler.finalize()
    }

    /// Add an implicit-class target to every compiled shape that is also
    /// declared a class (`rdfs:Class` / `owl:Class`): per SHACL, such a shape
    /// targets all instances of itself.
    fn apply_implicit_class_targets(&mut self, class_typed: &HashSet<Sid>) {
        for (id, data) in &mut self.shapes {
            if class_typed.contains(id)
                && !data
                    .targets
                    .iter()
                    .any(|t| matches!(t, TargetType::ImplicitClass(c) if c == id))
            {
                data.targets.push(TargetType::ImplicitClass(id.clone()));
            }
        }
    }

    /// Resolve raw `sh:path` objects into [`PropertyPath`] ASTs.
    ///
    /// A plain predicate IRI resolves to [`PropertyPath::Predicate`]; a blank-node
    /// path expression is walked into the full AST. A blank node that carries no
    /// recognizable path structure in the current graph is left unresolved so a
    /// later graph pass can complete it (and, failing that, `finalize` reports it
    /// rather than silently treating the blank node as a predicate).
    async fn resolve_paths(&mut self, db: GraphDbRef<'_>) -> Result<()> {
        let pending: Vec<Sid> = self
            .property_shapes
            .iter()
            .filter(|(_, ps)| ps.resolved_path.is_none() && ps.path.is_some())
            .map(|(id, _)| id.clone())
            .collect();

        for ps_id in pending {
            let resolved = match resolve_sh_path(db, &ps_id).await {
                Ok(Some(path)) => path,
                // No usable sh:path in this graph — leave for a later pass.
                Ok(None) => continue,
                // Unsupported form (e.g. inverse of a composite path). Record the
                // reason as an `Unresolvable` path — surfaced as a violation when
                // the shape fires, not as a ledger-wide compile failure. The error
                // only fires once the structure is present, so it's graph-correct.
                Err(err) => {
                    if let Some(ps) = self.property_shapes.get_mut(&ps_id) {
                        ps.resolved_path = Some(PropertyPath::Unresolvable(err.to_string()));
                    }
                    continue;
                }
            };
            // A path still referencing a blank node anywhere in its AST wasn't
            // fully resolved here (its structure lives in a graph not yet
            // scanned) — leave it for a later pass.
            if !resolved.references_blank_node() {
                if let Some(ps) = self.property_shapes.get_mut(&ps_id) {
                    ps.resolved_path = Some(resolved);
                }
            }
        }
        Ok(())
    }

    /// Expand RDF lists that were referenced by sh:in, sh:and, sh:or, sh:xone
    async fn expand_rdf_lists(&mut self, db: GraphDbRef<'_>) -> Result<()> {
        let rdf_first = Sid::new(RDF, rdf_names::FIRST);
        let rdf_rest = Sid::new(RDF, rdf_names::REST);
        let rdf_nil = Sid::new(RDF, rdf_names::NIL);

        // Expand sh:in list references in in_values
        // If in_values contains a single Ref, it might be an RDF list head that needs expansion
        let mut in_list_expansions: Vec<(Sid, Sid)> = Vec::new(); // (property_shape_id, list_head)

        let mut lang_list_expansions: Vec<(Sid, Sid)> = Vec::new();
        for (ps_id, ps_data) in &self.property_shapes {
            // Check if in_values has a single Ref value (potential RDF list head)
            if ps_data.in_values.len() == 1 {
                if let FlakeValue::Ref(list_head) = &ps_data.in_values[0] {
                    in_list_expansions.push((ps_id.clone(), list_head.clone()));
                }
            }
            // Same Turtle encoding for sh:languageIn.
            if ps_data.language_in_values.len() == 1 {
                if let FlakeValue::Ref(list_head) = &ps_data.language_in_values[0] {
                    lang_list_expansions.push((ps_id.clone(), list_head.clone()));
                }
            }
        }

        // Expand RDF list references
        for (ps_id, list_head) in in_list_expansions {
            let values = traverse_rdf_list(db, &list_head, &rdf_first, &rdf_rest, &rdf_nil).await?;
            if !values.is_empty() {
                if let Some(ps_data) = self.property_shapes.get_mut(&ps_id) {
                    // Replace the single Ref with the expanded values
                    ps_data.in_values = values;
                }
            }
        }
        for (ps_id, list_head) in lang_list_expansions {
            let values = traverse_rdf_list(db, &list_head, &rdf_first, &rdf_rest, &rdf_nil).await?;
            if !values.is_empty() {
                if let Some(ps_data) = self.property_shapes.get_mut(&ps_id) {
                    ps_data.language_in_values = values;
                }
            }
        }

        // Expand sh:ignoredProperties RDF-list heads (Turtle encoding). JSON-LD
        // @list flattens to one flake per member, so members arrive directly;
        // a Turtle list arrives as a single blank-node head that must be
        // walked, otherwise the head itself would be treated as the ignored
        // property and the real members would be rejected by sh:closed.
        let ignored_candidates: Vec<(Sid, Sid)> = self
            .shapes
            .iter()
            .flat_map(|(shape_id, sd)| {
                sd.ignored_properties
                    .iter()
                    .map(|p| (shape_id.clone(), p.clone()))
            })
            .collect();
        for (shape_id, head) in ignored_candidates {
            let values = traverse_rdf_list(db, &head, &rdf_first, &rdf_rest, &rdf_nil).await?;
            if values.is_empty() {
                // Not a list head in this graph — a plain property IRI.
                continue;
            }
            if let Some(sd) = self.shapes.get_mut(&shape_id) {
                sd.ignored_properties.remove(&head);
                for v in values {
                    if let FlakeValue::Ref(p) = v {
                        sd.ignored_properties.insert(p);
                    }
                }
            }
        }

        // Collect logical constraint list heads
        let mut and_lists: Vec<(Sid, Sid)> = Vec::new();
        let mut or_lists: Vec<(Sid, Sid)> = Vec::new();
        let mut xone_lists: Vec<(Sid, Sid)> = Vec::new();

        for (shape_id, shape_data) in &self.shapes {
            if let Some(list_head) = &shape_data.and_list {
                and_lists.push((shape_id.clone(), list_head.clone()));
            }
            if let Some(list_head) = &shape_data.or_list {
                or_lists.push((shape_id.clone(), list_head.clone()));
            }
            if let Some(list_head) = &shape_data.xone_list {
                xone_lists.push((shape_id.clone(), list_head.clone()));
            }
        }

        // Expand sh:and lists
        for (shape_id, list_head) in and_lists {
            let values = traverse_rdf_list(db, &list_head, &rdf_first, &rdf_rest, &rdf_nil).await?;
            let shape_refs: Vec<Sid> = values
                .into_iter()
                .filter_map(|v| {
                    if let FlakeValue::Ref(sid) = v {
                        Some(sid)
                    } else {
                        None
                    }
                })
                .collect();
            if let Some(shape_data) = self.shapes.get_mut(&shape_id) {
                shape_data.and_shapes = shape_refs;
            }
        }

        // Expand sh:or lists
        for (shape_id, list_head) in or_lists {
            let values = traverse_rdf_list(db, &list_head, &rdf_first, &rdf_rest, &rdf_nil).await?;
            let shape_refs: Vec<Sid> = values
                .into_iter()
                .filter_map(|v| {
                    if let FlakeValue::Ref(sid) = v {
                        Some(sid)
                    } else {
                        None
                    }
                })
                .collect();
            if let Some(shape_data) = self.shapes.get_mut(&shape_id) {
                shape_data.or_shapes = shape_refs;
            }
        }

        // Expand sh:xone lists
        for (shape_id, list_head) in xone_lists {
            let values = traverse_rdf_list(db, &list_head, &rdf_first, &rdf_rest, &rdf_nil).await?;
            let shape_refs: Vec<Sid> = values
                .into_iter()
                .filter_map(|v| {
                    if let FlakeValue::Ref(sid) = v {
                        Some(sid)
                    } else {
                        None
                    }
                })
                .collect();
            if let Some(shape_data) = self.shapes.get_mut(&shape_id) {
                shape_data.xone_shapes = shape_refs;
            }
        }

        Ok(())
    }

    /// Process a single SHACL flake
    fn process_flake(&mut self, flake: &Flake) -> Result<()> {
        let pred_name = flake.p.name.as_ref();

        // Determine if this is a shape or property shape based on what predicates it has
        match pred_name {
            // Target predicates indicate a node shape
            name if name == predicates::TARGET_CLASS => {
                if let FlakeValue::Ref(class) = &flake.o {
                    self.get_or_create_shape(&flake.s)
                        .targets
                        .push(TargetType::Class(class.clone()));
                }
            }
            name if name == predicates::TARGET_NODE => {
                if let FlakeValue::Ref(node) = &flake.o {
                    let shape = self.get_or_create_shape(&flake.s);
                    // Find or create Node target
                    let mut found = false;
                    for target in &mut shape.targets {
                        if let TargetType::Node(nodes) = target {
                            nodes.push(node.clone());
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        shape.targets.push(TargetType::Node(vec![node.clone()]));
                    }
                }
            }
            name if name == predicates::TARGET_SUBJECTS_OF => {
                if let FlakeValue::Ref(prop) = &flake.o {
                    self.get_or_create_shape(&flake.s)
                        .targets
                        .push(TargetType::SubjectsOf(prop.clone()));
                }
            }
            name if name == predicates::TARGET_OBJECTS_OF => {
                if let FlakeValue::Ref(prop) = &flake.o {
                    self.get_or_create_shape(&flake.s)
                        .targets
                        .push(TargetType::ObjectsOf(prop.clone()));
                }
            }

            // Property reference from node shape to property shape
            name if name == predicates::PROPERTY => {
                if let FlakeValue::Ref(prop_shape_id) = &flake.o {
                    self.get_or_create_shape(&flake.s)
                        .property_shape_ids
                        .push(prop_shape_id.clone());
                    // Ensure property shape exists
                    self.get_or_create_property_shape(prop_shape_id);
                }
            }

            // Path predicate indicates a property shape
            name if name == predicates::PATH => {
                if let FlakeValue::Ref(path) = &flake.o {
                    self.get_or_create_property_shape(&flake.s).path = Some(path.clone());
                }
            }

            // Cardinality constraints
            name if name == predicates::MIN_COUNT => {
                if let FlakeValue::Long(n) = &flake.o {
                    self.add_property_constraint(&flake.s, Constraint::MinCount(*n as usize));
                }
            }
            name if name == predicates::MAX_COUNT => {
                if let FlakeValue::Long(n) = &flake.o {
                    self.add_property_constraint(&flake.s, Constraint::MaxCount(*n as usize));
                }
            }

            // Value type constraints
            name if name == predicates::DATATYPE => {
                if let FlakeValue::Ref(dt) = &flake.o {
                    self.add_property_constraint(&flake.s, Constraint::Datatype(dt.clone()));
                }
            }
            name if name == predicates::NODE_KIND => {
                if let FlakeValue::Ref(kind) = &flake.o {
                    if let Some(node_kind) = parse_node_kind(kind) {
                        self.add_property_constraint(&flake.s, Constraint::NodeKind(node_kind));
                    }
                }
            }
            name if name == predicates::CLASS => {
                if let FlakeValue::Ref(class) = &flake.o {
                    self.add_property_constraint(&flake.s, Constraint::Class(class.clone()));
                }
            }

            // Value range constraints
            name if name == predicates::MIN_INCLUSIVE => {
                self.add_property_constraint(&flake.s, Constraint::MinInclusive(flake.o.clone()));
            }
            name if name == predicates::MAX_INCLUSIVE => {
                self.add_property_constraint(&flake.s, Constraint::MaxInclusive(flake.o.clone()));
            }
            name if name == predicates::MIN_EXCLUSIVE => {
                self.add_property_constraint(&flake.s, Constraint::MinExclusive(flake.o.clone()));
            }
            name if name == predicates::MAX_EXCLUSIVE => {
                self.add_property_constraint(&flake.s, Constraint::MaxExclusive(flake.o.clone()));
            }

            // String constraints
            name if name == predicates::PATTERN => {
                if let FlakeValue::String(pattern) = &flake.o {
                    // Store pattern, will be combined with flags in finalize
                    self.get_or_create_property_shape(&flake.s).pattern_string =
                        Some(pattern.clone());
                }
            }
            name if name == predicates::FLAGS => {
                if let FlakeValue::String(flags) = &flake.o {
                    self.get_or_create_property_shape(&flake.s).pattern_flags = Some(flags.clone());
                }
            }
            name if name == predicates::MIN_LENGTH => {
                if let FlakeValue::Long(n) = &flake.o {
                    self.add_property_constraint(&flake.s, Constraint::MinLength(*n as usize));
                }
            }
            name if name == predicates::MAX_LENGTH => {
                if let FlakeValue::Long(n) = &flake.o {
                    self.add_property_constraint(&flake.s, Constraint::MaxLength(*n as usize));
                }
            }

            // Value constraints
            name if name == predicates::HAS_VALUE => {
                self.add_property_constraint(&flake.s, Constraint::HasValue(flake.o.clone()));
            }
            // sh:in - accumulate values directly into in_values
            // Values can come from expanded @list or individual flakes
            name if name == predicates::IN => {
                self.get_or_create_property_shape(&flake.s)
                    .in_values
                    .push(flake.o.clone());
            }

            // Pair constraints
            name if name == predicates::EQUALS => {
                if let FlakeValue::Ref(prop) = &flake.o {
                    self.add_property_constraint(&flake.s, Constraint::Equals(prop.clone()));
                }
            }
            name if name == predicates::DISJOINT => {
                if let FlakeValue::Ref(prop) = &flake.o {
                    self.add_property_constraint(&flake.s, Constraint::Disjoint(prop.clone()));
                }
            }
            name if name == predicates::LESS_THAN => {
                if let FlakeValue::Ref(prop) = &flake.o {
                    self.add_property_constraint(&flake.s, Constraint::LessThan(prop.clone()));
                }
            }
            name if name == predicates::LESS_THAN_OR_EQUALS => {
                if let FlakeValue::Ref(prop) = &flake.o {
                    self.add_property_constraint(
                        &flake.s,
                        Constraint::LessThanOrEquals(prop.clone()),
                    );
                }
            }

            // Closed shape constraints (node-level)
            name if name == predicates::CLOSED => {
                if let FlakeValue::Boolean(closed) = &flake.o {
                    self.get_or_create_shape(&flake.s).is_closed = Some(*closed);
                }
            }
            name if name == predicates::IGNORED_PROPERTIES => {
                // This points to an RDF list - will be expanded similarly to sh:in
                if let FlakeValue::Ref(prop) = &flake.o {
                    self.get_or_create_shape(&flake.s)
                        .ignored_properties
                        .insert(prop.clone());
                }
            }

            // Language constraints
            name if name == predicates::UNIQUE_LANG => {
                if let FlakeValue::Boolean(v) = &flake.o {
                    self.add_property_constraint(&flake.s, Constraint::UniqueLang(*v));
                }
            }
            name if name == predicates::LANGUAGE_IN => {
                self.get_or_create_property_shape(&flake.s)
                    .language_in_values
                    .push(flake.o.clone());
            }

            // Shape-based constraints
            name if name == predicates::NODE => {
                if let FlakeValue::Ref(shape_ref) = &flake.o {
                    self.get_or_create_shape(&flake.s)
                        .node_shapes
                        .push(shape_ref.clone());
                }
            }
            name if name == predicates::QUALIFIED_VALUE_SHAPE => {
                if let FlakeValue::Ref(shape_ref) = &flake.o {
                    self.get_or_create_property_shape(&flake.s).qualified_shape =
                        Some(shape_ref.clone());
                }
            }
            name if name == predicates::QUALIFIED_MIN_COUNT => {
                if let FlakeValue::Long(n) = &flake.o {
                    self.get_or_create_property_shape(&flake.s).qualified_min = Some(*n as usize);
                }
            }
            name if name == predicates::QUALIFIED_MAX_COUNT => {
                if let FlakeValue::Long(n) = &flake.o {
                    self.get_or_create_property_shape(&flake.s).qualified_max = Some(*n as usize);
                }
            }
            name if name == predicates::QUALIFIED_VALUE_SHAPES_DISJOINT => {
                if let FlakeValue::Boolean(v) = &flake.o {
                    self.get_or_create_property_shape(&flake.s)
                        .qualified_disjoint = *v;
                }
            }

            // Logical constraints (node-level)
            name if name == predicates::NOT => {
                if let FlakeValue::Ref(shape_ref) = &flake.o {
                    self.get_or_create_shape(&flake.s).not_shape = Some(shape_ref.clone());
                }
            }
            name if name == predicates::AND => {
                // Accumulate shape references directly (JSON-LD @list expands to individual flakes)
                if let FlakeValue::Ref(shape_ref) = &flake.o {
                    self.get_or_create_shape(&flake.s)
                        .and_shapes
                        .push(shape_ref.clone());
                }
            }
            name if name == predicates::OR => {
                // Accumulate shape references directly (JSON-LD @list expands to individual flakes)
                if let FlakeValue::Ref(shape_ref) = &flake.o {
                    self.get_or_create_shape(&flake.s)
                        .or_shapes
                        .push(shape_ref.clone());
                }
            }
            name if name == predicates::XONE => {
                // Accumulate shape references directly (JSON-LD @list expands to individual flakes)
                if let FlakeValue::Ref(shape_ref) = &flake.o {
                    self.get_or_create_shape(&flake.s)
                        .xone_shapes
                        .push(shape_ref.clone());
                }
            }

            // Metadata
            name if name == predicates::DEACTIVATED => {
                if let FlakeValue::Boolean(v) = &flake.o {
                    // The subject may be a node shape, a property shape, or
                    // both maps may hold an entry for it — deactivate wherever
                    // it appears so the shape is ignored entirely.
                    if let Some(ps) = self.property_shapes.get_mut(&flake.s) {
                        ps.deactivated = *v;
                    }
                    self.get_or_create_shape(&flake.s).deactivated = *v;
                }
            }
            name if name == predicates::SEVERITY => {
                if let FlakeValue::Ref(sev) = &flake.o {
                    let severity = parse_severity(sev);
                    // Try as property shape first, then node shape
                    if let Some(ps) = self.property_shapes.get_mut(&flake.s) {
                        ps.severity = severity;
                    } else if let Some(ns) = self.shapes.get_mut(&flake.s) {
                        ns.severity = severity;
                    }
                }
            }
            name if name == predicates::MESSAGE => {
                if let FlakeValue::String(msg) = &flake.o {
                    if let Some(ps) = self.property_shapes.get_mut(&flake.s) {
                        ps.message = Some(msg.clone());
                    } else if let Some(ns) = self.shapes.get_mut(&flake.s) {
                        ns.message = Some(msg.clone());
                    }
                }
            }
            name if name == predicates::NAME => {
                if let FlakeValue::String(n) = &flake.o {
                    if let Some(ps) = self.property_shapes.get_mut(&flake.s) {
                        ps.name = Some(n.clone());
                    } else if let Some(ns) = self.shapes.get_mut(&flake.s) {
                        ns.name = Some(n.clone());
                    }
                }
            }

            _ => {}
        }

        Ok(())
    }

    fn get_or_create_shape(&mut self, id: &Sid) -> &mut ShapeData {
        self.shapes.entry(id.clone()).or_default()
    }

    fn get_or_create_property_shape(&mut self, id: &Sid) -> &mut PropertyShapeData {
        self.property_shapes.entry(id.clone()).or_default()
    }

    fn add_property_constraint(&mut self, id: &Sid, constraint: Constraint) {
        self.get_or_create_property_shape(id)
            .constraints
            .push(constraint);
    }

    /// Finalize compilation and produce CompiledShape instances
    fn finalize(self) -> Result<Vec<CompiledShape>> {
        // Destructure so both maps remain accessible throughout finalization.
        let Self {
            shapes,
            property_shapes: ps_map,
        } = self;

        let mut compiled = Vec::new();

        for (id, data) in &shapes {
            // Resolve property shapes
            let mut prop_shapes = Vec::new();
            for ps_id in &data.property_shape_ids {
                if let Some(ps_data) = ps_map.get(ps_id) {
                    if ps_data.deactivated {
                        continue;
                    }
                    if ps_data.path.is_some() {
                        // `sh:path` present. If it never resolved to an AST it
                        // becomes an `Unresolvable` path, surfaced as a violation
                        // only when this shape fires — not a compile error that
                        // would wedge every transaction on the ledger.
                        let path = resolved_path_of(ps_data);
                        let mut constraints = build_constraints_from_ps_data(ps_data);

                        // sh:qualifiedValueShape needs the shape map to inline
                        // the qualified shape, so it's attached here rather
                        // than in build_constraints_from_ps_data.
                        if let Some(q) = qualified_constraint(ps_data, &ps_map, &mut HashSet::new())
                        {
                            constraints.push(q);
                        }

                        // Check if this property shape's subject also has structural
                        // constraints (e.g. sh:or on a property shape). If so, build
                        // per-value structural constraints from its ShapeData entry.
                        let value_structural_constraints = shapes
                            .get(ps_id)
                            .map(|sd| build_logical_constraints(sd, &ps_map))
                            .unwrap_or_default();

                        prop_shapes.push(PropertyShape {
                            id: ps_id.clone(),
                            path,
                            constraints,
                            value_structural_constraints,
                            severity: ps_data.severity,
                            name: ps_data.name.clone(),
                            message: ps_data.message.clone(),
                        });
                    }
                }
            }

            // Sibling disjointness: a disjoint qualified constraint consults
            // the qualified shapes declared by the OTHER property shapes of
            // this node shape.
            let all_qualified: Vec<(usize, Arc<NestedShape>)> = prop_shapes
                .iter()
                .enumerate()
                .flat_map(|(i, ps)| {
                    ps.constraints
                        .iter()
                        .filter_map(move |constraint| match constraint {
                            Constraint::QualifiedValueShape { shape, .. } => {
                                Some((i, Arc::clone(shape)))
                            }
                            _ => None,
                        })
                })
                .collect();
            if all_qualified.len() > 1 {
                for (i, ps) in prop_shapes.iter_mut().enumerate() {
                    for constraint in &mut ps.constraints {
                        if let Constraint::QualifiedValueShape {
                            disjoint: true,
                            sibling_shapes,
                            ..
                        } = constraint
                        {
                            *sibling_shapes = all_qualified
                                .iter()
                                .filter(|(j, _)| *j != i)
                                .map(|(_, s)| Arc::clone(s))
                                .collect();
                        }
                    }
                }
            }

            // Build structural constraints (closed + logical)
            let mut structural_constraints = Vec::new();

            // Add closed constraint if sh:closed is true
            if data.is_closed == Some(true) {
                structural_constraints.push(NodeConstraint::Closed {
                    is_closed: true,
                    ignored_properties: data.ignored_properties.clone(),
                });
            }

            // Add logical constraints (sh:not, sh:and, sh:or, sh:xone)
            structural_constraints.extend(build_logical_constraints(data, &ps_map));

            // Value constraints declared directly on the node shape (no
            // sh:path) accumulate in a path-less PropertyShapeData entry keyed
            // by the shape's own Sid; per spec they apply to the focus node
            // itself. Metadata (sh:message / sh:name) that landed on that entry
            // also belongs to the node shape.
            let mut node_constraints = data.node_constraints.clone();
            let mut message = data.message.clone();
            let mut name = data.name.clone();
            let mut severity = data.severity;
            if let Some(own_ps) = ps_map.get(id) {
                if own_ps.path.is_none() {
                    node_constraints.extend(build_constraints_from_ps_data(own_ps));
                    message = message.or_else(|| own_ps.message.clone());
                    name = name.or_else(|| own_ps.name.clone());
                    // sh:severity routes to the path-less entry too (the
                    // metadata arms prefer the property-shape map).
                    if severity == Severity::Violation {
                        severity = own_ps.severity;
                    }
                }
            }

            compiled.push(CompiledShape {
                id: id.clone(),
                targets: data.targets.clone(),
                property_shapes: prop_shapes,
                node_constraints,
                structural_constraints,
                severity,
                name,
                message,
                deactivated: data.deactivated,
            });
        }

        Ok(compiled)
    }
}

/// Build the final constraint list from a `PropertyShapeData`, combining
/// pattern + flags and expanding sh:in values.
fn build_constraints_from_ps_data(ps_data: &PropertyShapeData) -> Vec<Constraint> {
    let mut constraints = Vec::new();

    for constraint in &ps_data.constraints {
        match constraint {
            // Skip In constraints — will be replaced with expanded values below
            Constraint::In(_) => {}
            other => constraints.push(other.clone()),
        }
    }

    // Add LanguageIn with all accumulated tags (one constraint, not one per tag)
    if !ps_data.language_in_values.is_empty() {
        let langs: Vec<String> = ps_data
            .language_in_values
            .iter()
            .filter_map(|v| match v {
                FlakeValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect();
        if !langs.is_empty() {
            constraints.push(Constraint::LanguageIn(langs));
        }
    }

    // Add Pattern constraint with flags if present
    if let Some(pattern) = &ps_data.pattern_string {
        constraints.push(Constraint::Pattern(
            pattern.clone(),
            ps_data.pattern_flags.clone(),
        ));
    }

    // Add In constraint with expanded values if present
    if !ps_data.in_values.is_empty() {
        constraints.push(Constraint::In(ps_data.in_values.clone()));
    } else {
        // Keep original In constraint if no expansion happened
        for constraint in &ps_data.constraints {
            if let Constraint::In(values) = constraint {
                // Only keep if it's not an RDF list reference
                if values.len() != 1 || !matches!(values.first(), Some(FlakeValue::Ref(_))) {
                    constraints.push(constraint.clone());
                }
            }
        }
    }

    constraints
}

/// The compiled path for a property shape, or an `Unresolvable` placeholder
/// when `sh:path` was present but never resolved. Keeping this off the error
/// path means one broken shape can't fail every transaction on the ledger — the
/// failure is scoped to focus nodes the owning shape actually targets.
fn resolved_path_of(ps_data: &PropertyShapeData) -> PropertyPath {
    ps_data.resolved_path.clone().unwrap_or_else(|| {
        PropertyPath::Unresolvable("unsupported or unresolvable sh:path expression".to_string())
    })
}

/// Build a `NestedShape` for a member of sh:or/sh:and/sh:xone/sh:not/sh:node
/// or a qualified value shape, inlining value-level or property constraints
/// from `PropertyShapeData` when the member is an anonymous shape.
fn build_nested_shape(sid: &ShapeId, ps_map: &HashMap<ShapeId, PropertyShapeData>) -> NestedShape {
    build_nested_shape_inner(sid, ps_map, &mut HashSet::new())
}

/// Recursive worker for [`build_nested_shape`]. `seen` holds the shape ids on
/// the current inlining stack: a qualified-shape reference cycle between
/// anonymous property shapes would otherwise inline forever. On re-entry the
/// member is left bare, deferring to named-ref resolution at validation time
/// (where the runtime recursion guard applies).
fn build_nested_shape_inner(
    sid: &ShapeId,
    ps_map: &HashMap<ShapeId, PropertyShapeData>,
    seen: &mut HashSet<ShapeId>,
) -> NestedShape {
    let bare = || NestedShape {
        id: sid.clone(),
        property_constraints: Vec::new(),
        node_constraints: Vec::new(),
        value_constraints: Vec::new(),
        message: None,
    };
    if !seen.insert(sid.clone()) {
        return bare();
    }

    let nested = if let Some(ps_data) = ps_map.get(sid) {
        if ps_data.path.is_none() {
            // Anonymous shape with constraints but no sh:path — these are
            // value-level constraints (e.g. sh:datatype on the value node).
            let value_constraints = build_constraints_from_ps_data(ps_data);
            NestedShape {
                id: sid.clone(),
                property_constraints: Vec::new(),
                node_constraints: Vec::new(),
                value_constraints,
                message: ps_data.message.clone(),
            }
        } else {
            // Has sh:path — inline as a property constraint on the nested
            // shape, carrying the compiled path AST (so complex paths on a
            // nested member are evaluated, not scanned as a bare blank-node
            // predicate).
            let mut constraints = build_constraints_from_ps_data(ps_data);
            if let Some(q) = qualified_constraint(ps_data, ps_map, seen) {
                constraints.push(q);
            }
            NestedShape {
                id: sid.clone(),
                property_constraints: vec![(resolved_path_of(ps_data), constraints)],
                node_constraints: Vec::new(),
                value_constraints: Vec::new(),
                message: ps_data.message.clone(),
            }
        }
    } else {
        // Named shape reference — constraints resolve at validation time.
        bare()
    };

    seen.remove(sid);
    nested
}

/// The `sh:qualifiedValueShape` constraint for a property shape, if declared.
fn qualified_constraint(
    ps_data: &PropertyShapeData,
    ps_map: &HashMap<ShapeId, PropertyShapeData>,
    seen: &mut HashSet<ShapeId>,
) -> Option<Constraint> {
    ps_data
        .qualified_shape
        .as_ref()
        .map(|q_ref| Constraint::QualifiedValueShape {
            shape: Arc::new(build_nested_shape_inner(q_ref, ps_map, seen)),
            min_count: ps_data.qualified_min,
            max_count: ps_data.qualified_max,
            disjoint: ps_data.qualified_disjoint,
            sibling_shapes: Vec::new(),
        })
}

/// Build shape-based and logical `NodeConstraint`s (sh:node, sh:not, sh:and,
/// sh:or, sh:xone) from a `ShapeData`, using `build_nested_shape` to inline
/// anonymous member constraints.
fn build_logical_constraints(
    data: &ShapeData,
    ps_map: &HashMap<ShapeId, PropertyShapeData>,
) -> Vec<NodeConstraint> {
    let mut constraints = Vec::new();

    for shape_ref in &data.node_shapes {
        constraints.push(NodeConstraint::Node(Arc::new(build_nested_shape(
            shape_ref, ps_map,
        ))));
    }

    if let Some(ref shape_ref) = data.not_shape {
        constraints.push(NodeConstraint::Not(Arc::new(build_nested_shape(
            shape_ref, ps_map,
        ))));
    }

    if !data.and_shapes.is_empty() {
        let nested = data
            .and_shapes
            .iter()
            .map(|sid| Arc::new(build_nested_shape(sid, ps_map)))
            .collect();
        constraints.push(NodeConstraint::And(nested));
    }

    if !data.or_shapes.is_empty() {
        let nested = data
            .or_shapes
            .iter()
            .map(|sid| Arc::new(build_nested_shape(sid, ps_map)))
            .collect();
        constraints.push(NodeConstraint::Or(nested));
    }

    if !data.xone_shapes.is_empty() {
        let nested = data
            .xone_shapes
            .iter()
            .map(|sid| Arc::new(build_nested_shape(sid, ps_map)))
            .collect();
        constraints.push(NodeConstraint::Xone(nested));
    }

    constraints
}

/// Traverse an RDF list and collect all values
async fn traverse_rdf_list(
    db: GraphDbRef<'_>,
    list_head: &Sid,
    rdf_first: &Sid,
    rdf_rest: &Sid,
    rdf_nil: &Sid,
) -> Result<Vec<FlakeValue>> {
    let mut values = Vec::new();
    let mut current = list_head.clone();

    // Limit iterations to prevent infinite loops
    const MAX_LIST_LENGTH: usize = 10000;

    for _ in 0..MAX_LIST_LENGTH {
        // Check if we've reached rdf:nil
        if current == *rdf_nil {
            break;
        }

        // Get rdf:first value
        let first_flakes = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject_predicate(current.clone(), rdf_first.clone()),
            )
            .await?;

        if let Some(first_flake) = first_flakes.first() {
            values.push(first_flake.o.clone());
        }

        // Get rdf:rest to continue traversal
        let rest_flakes = db
            .range(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject_predicate(current.clone(), rdf_rest.clone()),
            )
            .await?;

        if let Some(rest_flake) = rest_flakes.first() {
            if let FlakeValue::Ref(next) = &rest_flake.o {
                current = next.clone();
            } else {
                break;
            }
        } else {
            break;
        }
    }

    Ok(values)
}

/// Parse sh:nodeKind value to NodeKind enum
fn parse_node_kind(sid: &Sid) -> Option<NodeKind> {
    if sid.namespace_code != SHACL {
        return None;
    }
    match sid.name.as_ref() {
        predicates::BLANK_NODE => Some(NodeKind::BlankNode),
        predicates::IRI => Some(NodeKind::IRI),
        predicates::LITERAL => Some(NodeKind::Literal),
        predicates::BLANK_NODE_OR_IRI => Some(NodeKind::BlankNodeOrIRI),
        predicates::BLANK_NODE_OR_LITERAL => Some(NodeKind::BlankNodeOrLiteral),
        predicates::IRI_OR_LITERAL => Some(NodeKind::IRIOrLiteral),
        _ => None,
    }
}

/// Parse sh:severity value
fn parse_severity(sid: &Sid) -> Severity {
    if sid.namespace_code != SHACL {
        return Severity::Violation;
    }
    match sid.name.as_ref() {
        predicates::WARNING => Severity::Warning,
        predicates::INFO => Severity::Info,
        _ => Severity::Violation,
    }
}

/// SHACL node kind values
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    BlankNode,
    IRI,
    Literal,
    BlankNodeOrIRI,
    BlankNodeOrLiteral,
    IRIOrLiteral,
}

impl Default for ShapeCompiler {
    fn default() -> Self {
        Self::new()
    }
}
