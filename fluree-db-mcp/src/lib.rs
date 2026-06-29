//! Unified Fluree MCP service.
//!
//! One configurable MCP surface that exposes a selectable set of **toolsets**
//! ([`Toolset`]) over a single server, rather than one server per feature. The
//! CLI serves it over stdio (`fluree mcp serve --toolsets …`); the same service
//! can later back the server's HTTP `/mcp` for transport parity.
//!
//! Today's toolsets:
//! - `memory` — the developer-memory store (`fluree-db-memory`)
//! - `docs` — the embedded, version-pinned documentation (`fluree-db-docs`)
//!
//! `database` (the server's `sparql_query` / `get_data_model`) is reserved on
//! the [`Toolset`] surface but not yet reachable over stdio — it depends on a
//! running server's state.

mod docs_tools;
mod memory_tools;
mod service;
mod toolset;

pub use service::{serve_stdio, FlureeMcpService};
pub use toolset::Toolset;
