use crate::context;
use crate::error::{CliError, CliResult};
use crate::remote_client::RemoteLedgerClient;
use fluree_db_api::server_defaults::FlureeDir;
use futures::StreamExt;

pub async fn run(
    ledger: Option<&str>,
    oneline: bool,
    count: Option<usize>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = run_remote(&alias, oneline, count, &client).await;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return result;
    }

    if !direct {
        if let Some(client) = context::try_server_route_client(dirs) {
            let alias = context::resolve_ledger(ledger, dirs)?;
            let result = run_remote(&alias, oneline, count, &client).await;
            context::persist_refreshed_tokens(&client, context::LOCAL_SERVER_REMOTE, dirs).await;
            return result;
        }
    }

    run_local(ledger, oneline, count, dirs).await
}

async fn run_remote(
    alias: &str,
    oneline: bool,
    count: Option<usize>,
    client: &RemoteLedgerClient,
) -> CliResult<()> {
    let response = client
        .commit_log(alias, count)
        .await
        .map_err(|e| CliError::Remote(format!("failed to fetch log for '{alias}': {e}")))?;

    let commits = response
        .get("commits")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CliError::Remote("unexpected log response: missing commits array".into()))?;

    if commits.is_empty() {
        println!("No commits found for ledger '{alias}'");
        return Ok(());
    }

    for commit in commits {
        let t = commit
            .get("t")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let commit_id = commit
            .get("commit_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let time = commit.get("time").and_then(|v| v.as_str()).unwrap_or("");
        let asserts = commit
            .get("asserts")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let retracts = commit
            .get("retracts")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let flake_count = commit
            .get("flake_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(asserts + retracts);
        let short = abbreviate_hash(commit_id);

        if oneline {
            println!("t={t:<4}  {short}  {time}");
        } else {
            println!("commit {short}");
            if !time.is_empty() {
                println!("Date:    {time}");
            }
            println!("t:       {t}");
            println!("Flakes:  {flake_count}");
            println!();
        }
    }

    if let Some(true) = response
        .get("truncated")
        .and_then(serde_json::Value::as_bool)
    {
        let total = response
            .get("count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        eprintln!(
            "(showing {} of {} commits — pass -n to widen)",
            commits.len(),
            total
        );
    }

    Ok(())
}

async fn run_local(
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
             Use `fluree track status` to check remote state instead, or pass `--remote <name>`."
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
        .clone()
        .ok_or_else(|| CliError::NotFound(format!("ledger '{alias}' has no commits")))?;

    // Walk commit chain by CID. Use a branch-aware store so the walk can
    // cross fork points — pre-fork commits live under the source branch's
    // namespace, not the current branch's.
    let store = fluree_db_nameservice::branched_content_store_for_record(
        fluree.backend(),
        fluree.nameservice(),
        &record,
    )
    .await
    .map_err(|e| CliError::Config(format!("failed to build branched store: {e}")))?;
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
