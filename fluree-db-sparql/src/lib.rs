//! # Fluree DB SPARQL Parser
//!
//! A SPARQL 1.1 front-end for Fluree DB with:
//! - Fast compiled parsing (no runtime BNF interpretation)
//! - LLM-friendly diagnostics with precise source spans
//! - Direct lowering to query algebra (optional, via `lowering` feature)
//! - Capability-driven validation for Fluree-specific restrictions
//!
//! ## Features
//!
//! - `lowering` (default): Enable lowering to `fluree-db-query` algebra.
//!   Disable for smaller Lambda/WASM builds when only parse/validate is needed.
//!
//! ## Architecture
//!
//! The parser operates in three phases:
//!
//! 1. **Parse**: SPARQL string → `SparqlAst` with source spans (no DB access)
//! 2. **Validate**: Check Fluree restrictions, emit `Vec<Diagnostic>`
//! 3. **Lower**: Convert to query algebra using `IriEncoder` (requires `lowering` feature)
//!
//! ## Quick Start
//!
//! ```
//! use fluree_db_sparql::{parse_sparql, validate, Capabilities};
//!
//! let sparql = "SELECT ?name WHERE { ?s <http://example.org/name> ?name }";
//! let output = parse_sparql(sparql);
//! assert!(!output.has_errors());
//!
//! let ast = output.ast.unwrap();
//! let diagnostics = validate(&ast, &Capabilities::default());
//! assert!(diagnostics.iter().all(|d| !d.is_error()));
//! ```

pub mod ast;
pub mod diag;
pub mod lex;
pub mod parse;
pub mod span;
pub mod validate;

#[cfg(feature = "lowering")]
pub mod lower;

// Re-exports
pub use ast::{Prologue, QueryBody, SparqlAst, UpdateOperation};
pub use diag::{DiagCode, Diagnostic, ParseOutput, Severity};
pub use parse::parse_sparql;
pub use span::SourceSpan;
pub use validate::{validate, Capabilities};

#[cfg(feature = "lowering")]
pub use lower::{lower_sparql, lower_sparql_with_source, LowerError};
