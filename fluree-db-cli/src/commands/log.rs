use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use futures::StreamExt;

pub async fn run(
    ledger: Option<&str>,
    oneline: bool,
    count: Option<usize>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    // Check for tracked ledger — log requires local commit chain access
    let store = crate::config::TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let alias = context::resolve_ledger(ledger, dirs)?;
    if store.get_tracked(&alias).is_some()
        || store.get_tracked(&context::to_ledger_id(&alias)).is_some()
    {
        return Err(CliError::Usage(
            "commit log is not available for tracked ledgers (no local commit chain).\n  \
             Use `fluree track status` to check remote state instead."
                .to_string(),
        ));
    }

    let fluree = context::build_fluree(dirs)?;
    let ledger_id = context::to_ledger_id(&alias);

    // Look up ledger record from nameservice
    let record = fluree
        .nameservice()
        .lookup(&ledger_id)
        .await?
        .ok_or_else(|| CliError::NotFound(format!("ledger '{alias}' not found")))?;

    let commit_head_id = record
        .commit_head_id
        .ok_or_else(|| CliError::NotFound(format!("ledger '{alias}' has no commits")))?;

    // Walk commit chain by CID
    let store = fluree.content_store(&ledger_id);
    let stream: std::pin::Pin<
        Box<dyn futures::Stream<Item = fluree_db_core::Result<fluree_db_core::Commit>>>,
    > = Box::pin(fluree_db_core::trace_commits_by_id(
        store,
        commit_head_id,
        0,
    ));
    let mut stream = std::pin::pin!(stream);
    let mut shown = 0usize;
    let limit = count.unwrap_or(usize::MAX);

    while let Some(result) = stream.next().await {
        if shown >= limit {
            break;
        }

        let commit = result?;

        let commit_id_str = commit
            .id
            .as_ref()
            .map(std::string::ToString::to_string)
            .unwrap_or_default();
        let short_hash = abbreviate_hash(&commit_id_str);

        if oneline {
            // Note: commit messages are not currently persisted in the commit
            // format, so we show the timestamp instead.
            let time_str = commit.time.as_deref().unwrap_or("");
            println!("t={:<4}  {}  {}", commit.t, short_hash, time_str);
        } else {
            println!("commit {short_hash}");
            if let Some(ref time) = commit.time {
                println!("Date:    {time}");
            }
            println!("t:       {}", commit.t);
            println!("Flakes:  {}", commit.flakes.len());
            println!();
        }

        shown += 1;
    }

    if shown == 0 {
        println!("No commits found for ledger '{alias}'");
    }

    Ok(())
}

/// Extract a short commit hash from a content address for display.
///
/// Handles formats like:
/// - `fluree:file://ledger/main/commit/<hash>.fcv2`
/// - `fluree:commit:sha256:<hex>`
/// - Plain hex strings
fn abbreviate_hash(address: &str) -> String {
    // Try to extract hash from path-style addresses (e.g., .../commit/<hash>.fcv2)
    if let Some(pos) = address.rfind("/commit/") {
        let after = &address[pos + 8..];
        let hash = after
            .strip_suffix(".fcv2")
            .or_else(|| after.strip_suffix(".json"))
            .unwrap_or(after);
        if hash.len() >= 7 {
            return hash[..7].to_string();
        }
        return hash.to_string();
    }

    // Try sha256: prefix style
    if let Some(pos) = address.find("sha256:") {
        let hex = &address[pos + 7..];
        if hex.len() >= 7 {
            return hex[..7].to_string();
        }
        return hex.to_string();
    }

    // Fallback: last path segment or first 7 chars
    if let Some(last) = address.rsplit('/').next() {
        let clean = last
            .strip_suffix(".fcv2")
            .or_else(|| last.strip_suffix(".json"))
            .unwrap_or(last);
        if clean.len() >= 7 {
            return clean[..7].to_string();
        }
        return clean.to_string();
    }

    if address.len() >= 7 {
        address[..7].to_string()
    } else {
        address.to_string()
    }
}
