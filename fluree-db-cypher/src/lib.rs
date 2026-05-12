//! # Fluree DB Cypher Parser
//!
//! openCypher 9 parser with LLM-friendly diagnostics, lowering into the
//! shared `fluree-db-query` IR. The same execution engine that powers
//! SPARQL and JSON-LD queries handles Cypher reads.
//!
//! See [GQL_CYPHER_SUPPORT.md](../../GQL_CYPHER_SUPPORT.md) for the design
//! and frozen contract.
//!
//! ## Features
//!
//! - `lowering` (default): enable lowering to `fluree-db-query` algebra.
//!   Disable for parser-only WASM/Lambda builds.
//!
//! ## Quick start
//!
//! ```
//! use fluree_db_cypher::parse_cypher;
//!
//! let cypher = "MATCH (n:Person) RETURN n";
//! let output = parse_cypher(cypher);
//! assert!(!output.has_errors());
//! ```

pub mod ast;
pub mod diag;
pub mod lex;
pub mod parse;
pub mod span;
pub mod validate;

#[cfg(feature = "lowering")]
pub mod lower;

pub use ast::CypherAst;
pub use diag::{DiagCode, Diagnostic, ParseOutput, Severity};
pub use parse::parse_cypher;
pub use span::SourceSpan;
pub use validate::{validate, Capabilities};

#[cfg(feature = "lowering")]
pub use lower::{lower_cypher, LowerError};
