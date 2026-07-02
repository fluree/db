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
//! - String: `sh:pattern`, `sh:minLength`, `sh:maxLength`
//! - Value: `sh:hasValue`, `sh:in`
//! - Closed: `sh:closed`, `sh:ignoredProperties`
//! - Pair: `sh:equals`, `sh:disjoint`, `sh:lessThan`, `sh:lessThanOrEquals`
//! - Logical: `sh:not`, `sh:and`, `sh:or`, `sh:xone`
//! - Shape-based: `sh:node` — on a property shape each value node must conform
//!   to the referenced node shape; on a node shape the focus node itself must.
//!   Recursive shape references (e.g. `FriendShape → knows → sh:node
//!   FriendShape` over cyclic data) terminate: a `(focus, shape)` pair already
//!   being validated on the call stack is assumed conforming.
//! - Qualified: `sh:qualifiedValueShape` with `sh:qualifiedMinCount` /
//!   `sh:qualifiedMaxCount` — counts the values conforming to the qualified
//!   shape, including on property shapes used as logical-constraint members
//!   (`sh:qualifiedValueShapesDisjoint` is not supported)
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
//! by [`path::eval_path`]): `sh:inversePath` (over a single predicate), sequence
//! paths (RDF lists), `sh:alternativePath`, `sh:zeroOrMorePath`,
//! `sh:oneOrMorePath`, and `sh:zeroOrOnePath` — including nesting of these.
//! The one unsupported form, the inverse of a composite path (`^(p1/p2)`),
//! compiles to [`PropertyPath::Unresolvable`] and surfaces as a violation when
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
//! # Not Yet Supported
//!
//! The following constraints are parsed/compiled but are **not enforced** at
//! validation time. Shapes using these will load without error but their
//! constraints will silently pass. Plan to fix under the SHACL compliance
//! effort tracked in the repo.
//!
//! - `sh:uniqueLang`, `sh:languageIn` — require access to language-tag metadata
//!   on flakes, which is not yet threaded through the validation path.
//! - `sh:qualifiedValueShapesDisjoint` — sibling-shape disjointness for
//!   qualified value shapes (the counting form of `sh:qualifiedValueShape` is
//!   supported).
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
pub mod validate;

pub use cache::{ShaclCache, ShaclCacheKey};
pub use compile::{CompiledShape, PropertyShape, Severity, ShapeId, TargetType};
pub use constraints::Constraint;
pub use error::{Result, ShaclError};
pub use path::PropertyPath;
pub use validate::{CrossLedgerMembership, ShaclEngine, ValidationReport, ValidationResult};

/// SHACL namespace code (re-exported from fluree-vocab)
pub use fluree_vocab::namespaces::SHACL;

/// SHACL vocabulary full IRIs (re-exported from fluree-vocab)
pub use fluree_vocab::shacl;

/// Well-known SHACL predicate local names (re-exported from fluree-vocab)
///
/// These are the local name portions of SHACL predicates, used for SID construction.
/// For full IRIs, use the `shacl` module instead.
pub use fluree_vocab::shacl_names as predicates;
