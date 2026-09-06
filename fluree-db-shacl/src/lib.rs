//! SHACL validation engine for Fluree DB
//!
//! This crate provides SHACL (Shapes Constraint Language) validation for RDF data
//! in Fluree databases. It supports validation of node shapes and property shapes
//! against a focus node set.
//!
//! # Overview
//!
//! SHACL validation works by:
//! 1. Compiling shape definitions from database flakes into `CompiledShape` structures
//! 2. Determining target nodes for each shape (via `sh:targetClass`, `sh:targetNode`, etc.)
//! 3. Validating each focus node against applicable shape constraints
//! 4. Producing a `ValidationReport` with conformance status and any violations
//!
//! # Supported Constraints
//!
//! Currently supported constraint types:
//! - Cardinality: `sh:minCount`, `sh:maxCount`
//! - Value type: `sh:datatype`, `sh:nodeKind`, `sh:class` (with RDFS subclass reasoning)
//! - Value range: `sh:minInclusive`, `sh:maxInclusive`, `sh:minExclusive`, `sh:maxExclusive`
//! - String: `sh:pattern`, `sh:minLength`, `sh:maxLength` — literals match on
//!   their lexical form and IRIs on the full decoded IRI (per SPARQL `STR()`).
//!   An IRI whose namespace was allocated in the same transaction can't be
//!   decoded against the base snapshot and fails closed; blank nodes fail per
//!   spec.
//! - Value: `sh:hasValue`, `sh:in`
//! - Closed: `sh:closed`, `sh:ignoredProperties`
//! - Pair: `sh:equals`, `sh:disjoint`, `sh:lessThan`, `sh:lessThanOrEquals`
//! - Language: `sh:uniqueLang`, `sh:languageIn` (basic language-range matching
//!   per SPARQL `langMatches`; tags come from flake metadata)
//! - Logical: `sh:not`, `sh:and`, `sh:or`, `sh:xone`
//! - Shape-based: `sh:node` — on a property shape each value node must conform
//!   to the referenced node shape; on a node shape the focus node itself must.
//!   Recursive shape references (e.g. `FriendShape → knows → sh:node
//!   FriendShape` over cyclic data) terminate: a `(focus, shape)` pair already
//!   being validated on the call stack is assumed conforming.
//! - Qualified: `sh:qualifiedValueShape` with `sh:qualifiedMinCount` /
//!   `sh:qualifiedMaxCount` — counts the values conforming to the qualified
//!   shape, including on property shapes used as logical-constraint members.
//!   `sh:qualifiedValueShapesDisjoint` excludes values that conform to a
//!   sibling qualified shape (top-level property shapes)
//! - Node-shape value constraints: per-value constraints declared directly on
//!   a node shape (no `sh:path`) apply to the focus node itself
//! - `sh:deactivated` — a deactivated shape is ignored entirely, including
//!   when referenced via `sh:node` or logical constraints
//! - Implicit class targets: a shape that is also an `rdfs:Class` /
//!   `owl:Class` targets its own instances
//! - Messages: `sh:message` on a property shape (or on the node shape for
//!   `sh:closed` and node-level logical constraints) replaces the generated
//!   violation message
//!
//! # Property Paths (`sh:path`)
//!
//! Besides a single predicate IRI, `sh:path` supports property path expressions
//! (compiled by [`path::resolve_sh_path`] into a [`PropertyPath`] AST and evaluated
//! by [`path::eval_path`]): `sh:inversePath` (over any path — the inverse of a
//! composite rewrites into the AST, e.g. `^(p1/p2)` becomes `^p2/^p1`),
//! sequence paths (RDF lists), `sh:alternativePath`, `sh:zeroOrMorePath`,
//! `sh:oneOrMorePath`, and `sh:zeroOrOnePath` — including nesting of these.
//! Malformed paths (a literal step, multiple un-listed values for an operator)
//! compile to [`PropertyPath::Unresolvable`] and surface as a violation when
//! the owning shape fires on a focus node — scoped to that shape's targets
//! rather than failing every transaction on the ledger.
//!
//! # Target Selection
//!
//! All five SHACL target types select focus nodes:
//! - `sh:targetNode`, `sh:targetClass`, `sh:targetClass` (implicit) — resolved
//!   from the cache's hashmap indexes built at [`ShaclCache::new`] time.
//! - `sh:targetSubjectsOf(p)`, `sh:targetObjectsOf(p)` — resolved inside
//!   [`validate::ShaclEngine::validate_node`] by a bounded post-state
//!   existence check against `db` (SPOT for subjects-of, OPST for
//!   objects-of). This is correct for both base-state edges and the
//!   retraction/cross-graph cases that a staged-flakes-only hint pass
//!   cannot cover. Cost scales with the number of predicate-targeted
//!   shapes in the cache, not with data size.
//!
//! # SPARQL-based Constraints (`sh:sparql`)
//!
//! `sh:sparql` on a node or property shape attaches a `sh:SPARQLConstraint`:
//! its `sh:select` query runs once per focus node with `$this` pre-bound
//! (and `$PATH`, on property shapes with a plain predicate path); every
//! solution row is a violation. `?value` / `?path` / `?message` bindings
//! populate the matching report fields (`sh:value` defaults to the focus
//! node), and `sh:message` templates substitute `{?var}` / `{$var}` from the
//! solution. `sh:prefixes` declarations resolve through `owl:imports`. The
//! spec's pre-binding restrictions are enforced at shape-compile time; a
//! query that breaks them (or fails to parse) raises a validation *failure*
//! — scoped to the shapes that use it — when the shape fires. See
//! [`sparql`] for the full semantics.
//!
//! # Annotation Properties
//!
//! `sh:name`, `sh:description`, `sh:order` and `sh:defaultValue` are compiled
//! onto [`CompiledShape`] / [`PropertyShape`] but constrain nothing —
//! validation never reads them. They exist for consumers that render or
//! generate from shapes (the GraphQL schema derivation in `fluree-db-graphql`
//! takes its field names, docs and field order from them).
//!
//! `sh:defaultValue` is deliberately **never materialized**: a default is a
//! statement about presentation, not about what the graph holds, and asserting
//! the triple would make `sh:minCount 1` self-satisfying.
//!
//! # Not Yet Supported
//!
//! - SPARQL-based constraint *components* (`sh:ConstraintComponent`,
//!   `sh:validator`, `sh:parameter`) — declarations are ignored.
//! - `$shapesGraph` / `$currentShape` in `sh:sparql` queries (optional per
//!   spec; queries using them raise a failure).
//! - `sh:sparql` against a literal `sh:targetNode` focus (a literal has no
//!   graph presence to pre-bind; the constraint is skipped for such targets).
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_shacl::{ShaclEngine, ValidationReport};
//!
//! // Build SHACL engine from database shapes
//! let engine = ShaclEngine::from_db(&db).await?;
//!
//! // Validate a staged transaction view
//! let report = engine.validate(&view).await?;
//!
//! if !report.conforms {
//!     for violation in &report.results {
//!         println!("Violation: {:?}", violation);
//!     }
//! }
//! ```

pub mod cache;
pub mod compile;
pub mod constraints;
pub mod error;
pub mod path;
pub mod report_text;
pub mod sparql;
pub mod validate;

pub use cache::{ShaclCache, ShaclCacheKey};
pub use compile::NodeKind;
pub use compile::{CompiledShape, LiteralTarget, PropertyShape, Severity, ShapeId, TargetType};
pub use constraints::{Constraint, NodeConstraint};
pub use error::{Result, ShaclError};
pub use path::PropertyPath;
pub use report_text::{format_violations, unresolved_sid, violations_of};
pub use sparql::SparqlConstraint;
pub use validate::{
    CrossLedgerMembership, FocusNode, ShaclEngine, ValidationReport, ValidationResult,
};

/// SHACL namespace code (re-exported from fluree-vocab)
pub use fluree_vocab::namespaces::SHACL;

/// SHACL vocabulary full IRIs (re-exported from fluree-vocab)
pub use fluree_vocab::shacl;

/// Well-known SHACL predicate local names (re-exported from fluree-vocab)
///
/// These are the local name portions of SHACL predicates, used for SID construction.
/// For full IRIs, use the `shacl` module instead.
pub use fluree_vocab::shacl_names as predicates;
