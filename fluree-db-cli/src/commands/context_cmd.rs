//! `fluree context get/set` — manage the default JSON-LD context for a ledger.

use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use std::path::PathBuf;

/// `fluree context get [ledger]`
pub async fn get(explicit_ledger: Option<&str>, dirs: &FlureeDir) -> CliResult<()> {
    let alias = context::resolve_ledger(explicit_ledger, dirs)?;
    let fluree = context::build_fluree(dirs)?;
    let ledger_id = context::to_ledger_id(&alias);

    match fluree.get_default_context(&ledger_id).await? {
        Some(ctx) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&ctx).unwrap_or_else(|_| ctx.to_string())
            );
        }
        None => {
            // Print null to stdout for scripting consistency (matches HTTP API),
            // plus a human-readable hint on stderr.
            println!("null");
            eprintln!("No default context set for '{alias}'.");
        }
    }

    Ok(())
}

/// `fluree context set [ledger] -e '...' | -f file.json`
pub async fn set(
    explicit_ledger: Option<&str>,
    expr: Option<&str>,
    file: Option<&PathBuf>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let alias = context::resolve_ledger(explicit_ledger, dirs)?;
    let fluree = context::build_fluree(dirs)?;
    let ledger_id = context::to_ledger_id(&alias);

    // Read context from expr, file, or stdin
    let json_str = if let Some(e) = expr {
        e.to_string()
    } else if let Some(path) = file {
        std::fs::read_to_string(path).map_err(|e| {
            CliError::Usage(format!("failed to read file '{}': {}", path.display(), e))
        })?
    } else {
        // Try stdin
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .map_err(|e| CliError::Usage(format!("failed to read from stdin: {e}")))?;
        if buf.trim().is_empty() {
            return Err(CliError::Usage(
                "no context provided. Use -e '...' or -f file.json, or pipe JSON to stdin."
                    .to_string(),
            ));
        }
        buf
    };

    let context: serde_json::Value = serde_json::from_str(&json_str)
        .map_err(|e| CliError::Usage(format!("invalid JSON: {e}")))?;

    // Accept either { "@context": {...} } wrapper or bare object
    let ctx_value = if let Some(inner) = context.get("@context") {
        inner.clone()
    } else {
        context
    };

    if !ctx_value.is_object() {
        return Err(CliError::Usage(
            "context must be a JSON object mapping prefixes to IRIs".to_string(),
        ));
    }

    match fluree.set_default_context(&ledger_id, &ctx_value).await? {
        fluree_db_api::SetContextResult::Updated => {
            eprintln!("Default context updated for '{alias}'.");
        }
        fluree_db_api::SetContextResult::Conflict => {
            return Err(CliError::Api(fluree_db_api::ApiError::internal(
                "concurrent update conflict after retries — please retry.",
            )));
        }
    }

    Ok(())
}
