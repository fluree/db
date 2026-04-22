//! Shape compilation from SHACL flakes
//!
//! This module compiles SHACL shape definitions from database flakes into
//! efficient `CompiledShape` structures that can be used for validation.

use crate::constraints::{Constraint, NestedShape, NodeConstraint};
use crate::error::Result;
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
    /// The property path (simplified: just a predicate for now)
    pub path: Sid,
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
    path: Option<Sid>,
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
            // Logical constraints
            predicates::NOT,
            predicates::AND,
            predicates::OR,
            predicates::XONE,
            // Metadata
            predicates::SEVERITY,
            predicates::MESSAGE,
            predicates::NAME,
        ];

        // Query each input graph for all SHACL predicates, accumulating into
        // one compiler so cross-graph sh:and/or/xone/sh:in references resolve.
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

            // Expand rdf:first/rdf:rest lists referenced by sh:in / sh:and /
            // sh:or / sh:xone / sh:ignoredProperties. Run after each graph so
            // that lists whose head lives in this graph can resolve — a list
            // spanning multiple graphs will still resolve on a later pass
            // because `expand_rdf_lists` walks transitively via `db.range`.
            compiler.expand_rdf_lists(*db).await?;
        }

        compiler.finalize()
    }

    /// Expand RDF lists that were referenced by sh:in, sh:and, sh:or, sh:xone
    async fn expand_rdf_lists(&mut self, db: GraphDbRef<'_>) -> Result<()> {
        let rdf_first = Sid::new(RDF, rdf_names::FIRST);
        let rdf_rest = Sid::new(RDF, rdf_names::REST);
        let rdf_nil = Sid::new(RDF, rdf_names::NIL);

        // Expand sh:in list references in in_values
        // If in_values contains a single Ref, it might be an RDF list head that needs expansion
        let mut in_list_expansions: Vec<(Sid, Sid)> = Vec::new(); // (property_shape_id, list_head)

        for (ps_id, ps_data) in &self.property_shapes {
            // Check if in_values has a single Ref value (potential RDF list head)
            if ps_data.in_values.len() == 1 {
                if let FlakeValue::Ref(list_head) = &ps_data.in_values[0] {
                    in_list_expansions.push((ps_id.clone(), list_head.clone()));
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
                // Points to an RDF list of language tags - simplified for now
                if let FlakeValue::String(lang) = &flake.o {
                    self.add_property_constraint(
                        &flake.s,
                        Constraint::LanguageIn(vec![lang.clone()]),
                    );
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
                    if let Some(path) = &ps_data.path {
                        let constraints = build_constraints_from_ps_data(ps_data);

                        // Check if this property shape's subject also has structural
                        // constraints (e.g. sh:or on a property shape). If so, build
                        // per-value structural constraints from its ShapeData entry.
                        let value_structural_constraints = shapes
                            .get(ps_id)
                            .map(|sd| build_logical_constraints(sd, &ps_map))
                            .unwrap_or_default();

                        prop_shapes.push(PropertyShape {
                            id: ps_id.clone(),
                            path: path.clone(),
                            constraints,
                            value_structural_constraints,
                            severity: ps_data.severity,
                            name: ps_data.name.clone(),
                            message: ps_data.message.clone(),
                        });
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

            compiled.push(CompiledShape {
                id: id.clone(),
                targets: data.targets.clone(),
                property_shapes: prop_shapes,
                node_constraints: data.node_constraints.clone(),
                structural_constraints,
                severity: data.severity,
                name: data.name.clone(),
                message: data.message.clone(),
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

/// Build a `NestedShape` for a member of sh:or/sh:and/sh:xone/sh:not,
/// inlining value-level or property constraints from `PropertyShapeData`
/// when the member is an anonymous shape.
fn build_nested_shape(sid: &ShapeId, ps_map: &HashMap<ShapeId, PropertyShapeData>) -> NestedShape {
    if let Some(ps_data) = ps_map.get(sid) {
        if ps_data.path.is_none() {
            // Anonymous shape with constraints but no sh:path — these are
            // value-level constraints (e.g. sh:datatype on the value node).
            let value_constraints = build_constraints_from_ps_data(ps_data);
            return NestedShape {
                id: sid.clone(),
                property_constraints: Vec::new(),
                node_constraints: Vec::new(),
                value_constraints,
            };
        }
        // Has sh:path — inline as a property constraint on the nested shape
        let constraints = build_constraints_from_ps_data(ps_data);
        return NestedShape {
            id: sid.clone(),
            property_constraints: vec![(ps_data.path.clone().unwrap(), constraints)],
            node_constraints: Vec::new(),
            value_constraints: Vec::new(),
        };
    }
    // Named shape reference — constraints will be resolved at validation time
    NestedShape {
        id: sid.clone(),
        property_constraints: Vec::new(),
        node_constraints: Vec::new(),
        value_constraints: Vec::new(),
    }
}

/// Build logical `NodeConstraint`s (sh:not, sh:and, sh:or, sh:xone) from a
/// `ShapeData`, using `build_nested_shape` to inline anonymous member constraints.
fn build_logical_constraints(
    data: &ShapeData,
    ps_map: &HashMap<ShapeId, PropertyShapeData>,
) -> Vec<NodeConstraint> {
    let mut constraints = Vec::new();

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
