//! GraphQL over a Fluree ledger.
//!
//! A ledger's GraphQL schema is derived from what the ledger already contains,
//! in three tiers, each a strict superset of the one below:
//!
//! 1. **Inferred** — HEAD statistics alone. Every class becomes a type, every
//!    observed property a nullable list field. No configuration, read-only.
//! 2. **Shaped** — a `sh:NodeShape` with `sh:targetClass` contributes cardinality,
//!    datatypes, enums, reverse fields and documentation. Shapes and statistics
//!    build one model together ([`schema::build`]) rather than one overlaying the
//!    other, because naming is global; a shape wins where they overlap, and
//!    `sh:closed` decides whether observed-but-undeclared properties survive.
//! 3. **Curated** — a `graphql:Schema` instance (the `datashapes.org/graphql#`
//!    vocabulary, shared with TopBraid EDG and GraphDB) picks which shapes are
//!    exposed and is the only tier that can enable mutations.
//!
//! All three produce a [`SchemaModel`]; [`runtime`] renders that into an
//! executable async-graphql schema whose root resolvers compile a whole selection
//! set into a single Fluree query ([`lower`]), then reshape its result
//! ([`lower::reshape`]). [`schema::bootstrap`] runs the mapping backwards, emitting
//! SHACL from a derived schema as a starting point for tier 2.
//!
//! See `docs/query/graphql.md` for the reference and `docs/cli/graphql.md`
//! for the full SHACL and curation mapping tables.

pub mod error;
pub mod limits;
pub mod lower;
pub mod mutate;
pub mod naming;
pub mod runtime;
pub mod schema;
pub mod sdl;
pub mod selection;

/// The GraphQL runtime, re-exported so downstream crates share this crate's
/// pinned version rather than depending on `async-graphql` themselves.
pub use async_graphql;

pub use error::{Error, Result};
pub use schema::model::SchemaModel;
pub use sdl::sdl;
