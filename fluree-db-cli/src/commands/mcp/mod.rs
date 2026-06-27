//! `fluree mcp` — the unified MCP surface.
//!
//! One `fluree` MCP server exposes a selectable set of toolsets (`memory`,
//! `docs`) over a single stdio transport:
//! - `serve` (in [`serve`]) runs the server; spawned by the IDE.
//! - `init` writes the IDE's MCP config so it spawns `serve` on demand.
//! - `status` reports which toolsets are installed per detected IDE.

mod ide;
pub mod serve;

use crate::error::{CliError, CliResult};
use fluree_db_mcp::Toolset;

/// Parse a `--toolsets` value into a validated, canonical selection.
pub fn parse_toolsets(value: &str) -> CliResult<Vec<Toolset>> {
    Toolset::parse_selection(value).map_err(CliError::Usage)
}

/// `fluree mcp init` — register the Fluree MCP server (with the selected
/// toolsets) in an IDE's config. Needs no `.fluree/` directory: the server
/// lazy-inits its store, and the docs toolset is stateless.
pub fn init(ide: Option<&str>, toolsets: &str) -> CliResult<()> {
    let toolsets = parse_toolsets(toolsets)?;
    ide::run_install(ide, &toolsets)
}

/// `fluree mcp status` — show, per detected IDE, whether the `fluree` server is
/// installed and which toolsets it exposes.
pub fn status() -> CliResult<()> {
    ide::run_status()
}

/// Back-compat: `fluree memory mcp-install` / `fluree memory init` installed the
/// memory server only. Routes to the unified installer with the `memory`
/// toolset and prints a one-line deprecation pointer.
pub fn memory_alias_install(ide: Option<&str>) -> CliResult<()> {
    eprintln!("note: this command is deprecated — use `fluree mcp init --toolsets memory`.");
    ide::run_install(ide, &[Toolset::Memory])
}
