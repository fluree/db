use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_memory::{MemoryStore, MemoryToolService};
use rmcp::ServiceExt;
use std::path::Path;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer as _;

/// Run the MCP server with the given transport.
pub async fn run(transport: &str, dirs: &FlureeDir) -> CliResult<()> {
    match transport {
        "stdio" => run_stdio(dirs).await,
        other => Err(CliError::Usage(format!(
            "unsupported MCP transport '{other}'; valid: stdio"
        ))),
    }
}

/// Set up file-based tracing for the MCP server.
///
/// MCP uses stdio for JSON-RPC, so we can't write to stdout/stderr.
/// Writes to `.fluree-memory/.local/mcp.log` instead.
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
/// `fluree mcp serve --transport stdio` and communicates via JSON-RPC
/// over stdin/stdout.
async fn run_stdio(dirs: &FlureeDir) -> CliResult<()> {
    let fluree = context::build_fluree(dirs)?;

    // Determine memory_dir: .fluree-memory/ at the project root (same logic as CLI).
    // Always enable in unified mode — MemoryStore creates the directory structure on init.
    let memory_dir = if dirs.is_unified() {
        let project_root = dirs.data_dir().parent().unwrap_or(dirs.data_dir());
        Some(project_root.join(".fluree-memory"))
    } else {
        None
    };

    // Set up file-based logging (stdout/stderr are reserved for JSON-RPC)
    init_mcp_tracing(memory_dir.as_deref());

    tracing::info!("MCP server starting");

    let store = MemoryStore::new(fluree, memory_dir);
    let service = MemoryToolService::new(store);

    let transport = rmcp::transport::io::stdio();

    let server = service
        .serve(transport)
        .await
        .map_err(|e| CliError::Config(format!("failed to start MCP server: {e}")))?;

    tracing::info!("MCP server ready, waiting for client");

    // Block until the client disconnects
    server
        .waiting()
        .await
        .map_err(|e| CliError::Config(format!("MCP server error: {e}")))?;

    tracing::info!("MCP server shutting down");

    Ok(())
}
