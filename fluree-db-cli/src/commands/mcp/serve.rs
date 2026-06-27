use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_mcp::{FlureeMcpService, Toolset};
use fluree_db_memory::MemoryStore;
use std::path::Path;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer as _;

/// Run the unified MCP server over `transport`, exposing `toolsets`.
///
/// `dirs` is `Some` only when the `memory` toolset is enabled (the caller
/// resolves it); the docs toolset is stateless and needs no project directory.
pub async fn run(transport: &str, toolsets: &[Toolset], dirs: Option<&FlureeDir>) -> CliResult<()> {
    match transport {
        "stdio" => run_stdio(toolsets, dirs).await,
        other => Err(CliError::Usage(format!(
            "unsupported MCP transport '{other}'; valid: stdio"
        ))),
    }
}

/// Set up file-based tracing for the MCP server.
///
/// MCP uses stdio for JSON-RPC, so we can't write to stdout/stderr. When the
/// memory toolset is active we log to `.fluree-memory/.local/mcp.log`; a
/// docs-only server has no project dir, so logging is simply skipped.
fn init_mcp_tracing(memory_dir: Option<&Path>) {
    let Some(dir) = memory_dir else { return };

    let log_path = dir.join(".local").join("mcp.log");
    if let Some(parent) = log_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    // Truncate on each server start (keeps the log fresh and bounded)
    let log_file = match std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&log_path)
    {
        Ok(f) => f,
        Err(_) => return, // silently skip if we can't write
    };

    let filter = EnvFilter::new("debug");

    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_target(true)
            .with_writer(std::sync::Mutex::new(log_file))
            .with_filter(filter),
    );

    let _ = tracing::subscriber::set_global_default(subscriber);
}

/// Launch the MCP server on stdio (stdin/stdout).
///
/// This is the primary transport for IDE integration. The IDE spawns
/// `fluree mcp serve --transport stdio --toolsets …` and communicates via
/// JSON-RPC over stdin/stdout.
async fn run_stdio(toolsets: &[Toolset], dirs: Option<&FlureeDir>) -> CliResult<()> {
    // Build the memory store only when the memory toolset is enabled. The store
    // lazy-initializes its directories on first tool call, so `serve` never
    // requires `init` to have run first.
    let memory = if toolsets.contains(&Toolset::Memory) {
        let dirs = dirs
            .ok_or_else(|| CliError::Config("memory toolset requires a Fluree directory".into()))?;

        // Determine memory_dir: .fluree-memory/ at the project root (same logic
        // as the CLI). Always enabled in unified mode — MemoryStore creates the
        // directory structure on init.
        let memory_dir = if dirs.is_unified() {
            let project_root = dirs.data_dir().parent().unwrap_or(dirs.data_dir());
            Some(project_root.join(".fluree-memory"))
        } else {
            None
        };

        // Set up file-based logging (stdout/stderr are reserved for JSON-RPC).
        init_mcp_tracing(memory_dir.as_deref());

        // Process-private, in-memory ledger rebuilt from the `.ttl` files on
        // startup. Many MCP servers can run at once (one per IDE/agent session)
        // over the same `.fluree-memory` files, so the ledger cache must not be
        // shared on disk — sharing it let one process's rebuild delete commits
        // another was reading. See f49b7edbf.
        let fluree = context::build_memory_fluree();
        Some(MemoryStore::new_ephemeral_ledger(fluree, memory_dir))
    } else {
        None
    };

    tracing::info!(toolsets = ?toolsets, "MCP server starting");

    let service = FlureeMcpService::new(toolsets, memory);
    fluree_db_mcp::serve_stdio(service)
        .await
        .map_err(CliError::Config)?;

    tracing::info!("MCP server shutting down");
    Ok(())
}
