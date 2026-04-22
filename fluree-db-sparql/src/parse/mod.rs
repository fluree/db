//! SPARQL Parser.
//!
//! This module parses tokenized SPARQL into the typed AST.
//! The parser consumes tokens (not raw `&str`) for better maintainability.
//!
//! ## Usage
//!
//! ```
//! use fluree_db_sparql::parse::parse_sparql;
//!
//! let output = parse_sparql("SELECT ?name WHERE { ?s <http://example.org/name> ?name }");
//! if !output.has_errors() {
//!     let ast = output.ast.unwrap();
//!     // Use the AST...
//! }
//! ```
//!
//! ## Phases
//!
//! - Phase 2: Basic query parsing (SELECT, triple patterns, modifiers) ✓
//! - Phase 3: Graph patterns (OPTIONAL, UNION, FILTER, BIND, VALUES, MINUS) ✓
//! - Phase 4: Expressions (arithmetic, comparison, functions, aggregates) ✓
//! - Phase 5: Property paths ✓
//! - Phase 6: Other query forms (CONSTRUCT, ASK, DESCRIBE) ✓
//! - Phase 7: SPARQL Update ✓
//! - Phase 8: Validation (ground-only, property paths, Update restrictions) ✓

pub mod expr;
pub mod path;
mod query;
mod stream;

pub use expr::parse_expression;
pub use path::parse_property_path;
pub use query::parse_sparql;
pub use stream::TokenStream;
