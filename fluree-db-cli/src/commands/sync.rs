//! Sync commands: fetch, pull, push, clone (named-remote and origin-based)

use crate::config::{resolve_storage_path, TomlSyncConfigStore};
use crate::context;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_core::commit::codec::{read_commit_envelope, verify_commit_blob};
use fluree_db_core::pack::{PackRequest, LARGE_TRANSFER_THRESHOLD};
use fluree_db_core::storage::ContentAddressedWrite;
use fluree_db_core::ContentKind;
use fluree_db_core::ContentStore;
use fluree_db_nameservice::{
    ConfigLookup, ConfigPayload, ConfigPublisher, ConfigValue, FileTrackingStore, LedgerConfig,
    RefKind, RefLookup, RefPublisher, RemoteName, RemoteTrackingStore,
};
use fluree_db_nameservice_sync::{
    ingest_pack_stream, ingest_pack_stream_with_header, peek_pack_header, FetchResult,
    HttpRemoteClient, MultiOriginFetcher, RemoteEndpoint, SyncConfigStore, SyncDriver,
};
use futures::StreamExt;
use std::sync::Arc;

fn token_has_storage_permissions(token: &str) -> Option<bool> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;

    let storage_all = claims
        .get("fluree.storage.all")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let storage_ledgers_len = claims
        .get("fluree.storage.ledgers")
        .and_then(|v| v.as_array())
        .map(std::vec::Vec::len)
        .unwrap_or(0);

    Some(storage_all || storage_ledgers_len > 0)
}

fn replication_permission_error(remote: &str) -> CliError {
    CliError::Config(format!(
        "this operation replicates ledger refs (pull/push/fetch) and requires a root token with `fluree.storage.*` permissions.\n  {} if you only have query access, use `fluree track` (or `--remote`) and run queries/transactions remotely.\n  {} fluree auth login --remote {}",
        "hint:".cyan().bold(),
        "hint:".cyan().bold(),
        remote
    ))
}

/// Format bytes as a human-readable size (e.g., "1.2 GiB", "342 MiB").
fn format_human_bytes(bytes: u64) -> String {
    const GIB: f64 = 1_073_741_824.0;
    const MIB: f64 = 1_048_576.0;
    const KIB: f64 = 1_024.0;

    let b = bytes as f64;
    if b >= GIB {
        format!("{:.1} GiB", b / GIB)
    } else if b >= MIB {
        format!("{:.0} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.0} KiB", b / KIB)
    } else {
        format!("{bytes} bytes")
    }
}

/// Prompt the user to confirm a large transfer. Returns `true` if confirmed.
fn confirm_large_transfer(estimated_bytes: u64) -> bool {
    use std::io::{self, BufRead, Write};
    eprint!(
        "  Estimated transfer size: ~{}. This may take several minutes. Continue? [Y/n] ",
        format_human_bytes(estimated_bytes)
    );
    io::stderr().flush().ok();

    let stdin = io::stdin();
    let mut line = String::new();
    if stdin.lock().read_line(&mut line).is_err() {
        return true; // Non-interactive: proceed
    }
    let trimmed = line.trim().to_lowercase();
    trimmed.is_empty() || trimmed == "y" || trimmed == "yes"
}

fn map_sync_auth_error(remote: &str, err: &str) -> Option<CliError> {
    // `fluree-db-nameservice-sync` reports remote failures as strings; match the common
    // permission-related server errors and provide a clearer CLI message.
    if err.contains("401")
        || err.contains("403")
        || err.contains("Bearer token required")
        || err.contains("Untrusted issuer")
        || err.contains("Token lacks storage proxy permissions")
        || err.contains("Storage proxy not enabled")
    {
        Some(replication_permission_error(remote))
    } else {
        None
    }
}

/// Build a SyncDriver with all configured remotes
async fn build_sync_driver(dirs: &FlureeDir) -> CliResult<(SyncDriver, Arc<TomlSyncConfigStore>)> {
    let fluree = context::build_fluree(dirs)?;
    let config_store = Arc::new(TomlSyncConfigStore::new(dirs.config_dir().to_path_buf()));

    // Get the nameservice as RefPublisher
    let local: Arc<dyn RefPublisher> = fluree
        .nameservice_mode()
        .publisher_arc()
        .ok_or_else(|| CliError::Config("sync requires a read-write nameservice".into()))?;

    // Create a FileTrackingStore using the same storage path
    let storage = resolve_storage_path(dirs);
    let tracking: Arc<dyn RemoteTrackingStore> = Arc::new(FileTrackingStore::new(&storage));

    let mut driver = SyncDriver::new(
        local,
        tracking,
        config_store.clone() as Arc<dyn SyncConfigStore>,
    );

    // Add HTTP clients for all configured remotes
    let remotes = config_store
        .list_remotes()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    for remote in remotes {
        match &remote.endpoint {
            RemoteEndpoint::Http { base_url } => {
                let client = Arc::new(HttpRemoteClient::new(
                    base_url.clone(),
                    remote.auth.token.clone(),
                ));
                driver.add_client(&remote.name, client);
            }
            RemoteEndpoint::Sse { .. } | RemoteEndpoint::Storage { .. } => {
                // Skip non-HTTP remotes for now
                eprintln!(
                    "{} skipping non-HTTP remote '{}'",
                    "warning:".yellow().bold(),
                    remote.name.as_str()
                );
            }
        }
    }

    Ok((driver, config_store))
}

/// Fetch refs from a remote (like git fetch)
pub async fn run_fetch(remote: &str, dirs: &FlureeDir) -> CliResult<()> {
    // Proactively fail with a clear message for query-only tokens.
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let remote_cfg = store
        .get_remote(&RemoteName::new(remote))
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{remote}' not found")))?;
    if let Some(tok) = &remote_cfg.auth.token {
        if let Some(false) = token_has_storage_permissions(tok) {
            return Err(replication_permission_error(remote));
        }
    }

    let (driver, _config) = build_sync_driver(dirs).await?;
    let remote_name = RemoteName::new(remote);

    println!("Fetching from '{}'...", remote.cyan());

    let result = driver.fetch_remote(&remote_name).await.map_err(|e| {
        let msg = e.to_string();
        map_sync_auth_error(remote, &msg)
            .unwrap_or_else(|| CliError::Config(format!("fetch failed: {msg}")))
    })?;

    print_fetch_result(&result);
    Ok(())
}

fn print_fetch_result(result: &FetchResult) {
    if result.updated.is_empty() && result.unchanged.is_empty() {
        println!("No ledgers found on remote.");
        return;
    }

    if !result.updated.is_empty() {
        println!("{}", "Updated:".green().bold());
        for (ledger_id, tracking) in &result.updated {
            let t = tracking.commit_ref.as_ref().map(|r| r.t).unwrap_or(0);
            println!("  {ledger_id} -> t={t}");
        }
    }

    if !result.unchanged.is_empty() {
        println!(
            "{} {} ledger(s) unchanged",
            "Already up to date:".dimmed(),
            result.unchanged.len()
        );
    }
}

/// Pull commits from upstream and apply them to the local ledger.
///
/// Downloads commit blobs page-by-page (newest→oldest) until we reach local
/// history, then applies them oldest→newest via `import_commits_incremental`.
///
/// Falls back to origin-based pull (CID chain walk via LedgerConfig) when
/// no upstream remote is configured.
pub async fn run_pull(ledger: Option<&str>, no_indexes: bool, dirs: &FlureeDir) -> CliResult<()> {
    let ledger_id = context::resolve_ledger(ledger, dirs)?;
    let ledger_id = context::to_ledger_id(&ledger_id);

    let config_store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let upstream = config_store
        .get_upstream(&ledger_id)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    let Some(upstream) = upstream else {
        // No named upstream — try origin-based pull via LedgerConfig.
        return run_pull_via_origins(&ledger_id, no_indexes, dirs).await;
    };

    // Resolve remote config → build client.
    let remote_cfg = config_store
        .get_remote(&upstream.remote)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", upstream.remote)))?;

    // Proactively fail with a clear message for query-only tokens.
    if let Some(tok) = &remote_cfg.auth.token {
        if let Some(false) = token_has_storage_permissions(tok) {
            return Err(replication_permission_error(upstream.remote.as_str()));
        }
    }

    let base_url = match &remote_cfg.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{}' is not an HTTP remote",
                upstream.remote.as_str()
            )));
        }
    };

    let client = context::build_client_from_auth(&base_url, &remote_cfg.auth);

    // Use the remote ledger ID for all remote API calls (may differ from local ledger_id).
    // Note: UpstreamConfig field is named `remote_alias` (pre-existing; should be `remote_id`).
    let remote_ledger_id = &upstream.remote_alias;

    // Resolve remote head.
    let info = client
        .ledger_info(remote_ledger_id, None)
        .await
        .map_err(|e| CliError::Config(format!("pull failed (remote ledger info): {e}")))?;
    let remote_t = info
        .get("t")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| CliError::Config("remote ledger-info response missing 't'".into()))?;

    // Resolve local head.
    let fluree = context::build_fluree(dirs)?;
    let storage = fluree
        .backend()
        .admin_storage_cloned()
        .ok_or_else(|| CliError::Config("sync requires managed storage backend".into()))?;
    let local_ref = fluree
        .nameservice_mode()
        .get_ref(&ledger_id, RefKind::CommitHead)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("local ledger '{ledger_id}' not found")))?;

    if remote_t <= local_ref.t {
        println!("{} '{}' is already up to date", "✓".green(), ledger_id);
        context::persist_refreshed_tokens(&client, upstream.remote.as_str(), dirs).await;
        return Ok(());
    }

    println!(
        "Pulling '{}' from '{}' (local t={}, remote t={})...",
        ledger_id.cyan(),
        upstream.remote.as_str().cyan(),
        local_ref.t,
        remote_t
    );

    // Try pack protocol first (needs NsRecord for head CID).
    match client.fetch_ns_record(remote_ledger_id).await {
        Ok(Some(remote_ns)) => {
            if let Some(ref remote_head_cid) = remote_ns.commit_head_id {
                let have: Vec<fluree_db_core::ContentId> =
                    local_ref.id.clone().into_iter().collect();

                // Build pack request — include indexes by default.
                let pack_request = if !no_indexes {
                    if let Some(ref remote_index_id) = remote_ns.index_head_id {
                        let local_index_id = fluree
                            .nameservice_mode()
                            .get_ref(&ledger_id, RefKind::IndexHead)
                            .await
                            .ok()
                            .flatten()
                            .and_then(|r| r.id);
                        PackRequest::with_indexes(
                            vec![remote_head_cid.clone()],
                            have,
                            remote_index_id.clone(),
                            local_index_id,
                        )
                    } else {
                        PackRequest::commits(vec![remote_head_cid.clone()], have)
                    }
                } else {
                    PackRequest::commits(vec![remote_head_cid.clone()], have)
                };

                match client
                    .fetch_pack_response(remote_ledger_id, &pack_request)
                    .await
                {
                    Ok(Some(response)) => {
                        // Peek the header to check estimated transfer size.
                        let mut body_stream = response.bytes_stream();
                        match peek_pack_header(&mut body_stream).await {
                            Ok((header, buf_tail)) => {
                                // Check if transfer is large and indexes are included.
                                let wants_indexes = pack_request.include_indexes;
                                let is_large =
                                    header.estimated_total_bytes > LARGE_TRANSFER_THRESHOLD;

                                let ingest_result = if wants_indexes && is_large {
                                    if confirm_large_transfer(header.estimated_total_bytes) {
                                        // User confirmed — proceed with indexes.
                                        ingest_pack_stream_with_header(
                                            &header,
                                            buf_tail,
                                            &mut body_stream,
                                            &storage,
                                            &ledger_id,
                                        )
                                        .await
                                    } else {
                                        // User declined — drop stream, re-request commits only.
                                        drop(body_stream);
                                        eprintln!(
                                            "  Skipping index transfer, pulling commits only..."
                                        );
                                        let have: Vec<fluree_db_core::ContentId> =
                                            local_ref.id.clone().into_iter().collect();
                                        let commits_only = PackRequest::commits(
                                            vec![remote_head_cid.clone()],
                                            have,
                                        );
                                        match client
                                            .fetch_pack_response(
                                                remote_ledger_id,
                                                &commits_only,
                                            )
                                            .await
                                        {
                                            Ok(Some(resp2)) => {
                                                ingest_pack_stream(
                                                    resp2,
                                                    &storage,
                                                    &ledger_id,
                                                )
                                                .await
                                            }
                                            Ok(None) => Err(
                                                fluree_db_nameservice_sync::SyncError::PackNotSupported,
                                            ),
                                            Err(e) => Err(
                                                fluree_db_nameservice_sync::SyncError::Remote(
                                                    e.to_string(),
                                                ),
                                            ),
                                        }
                                    }
                                } else {
                                    // Small transfer or no indexes — proceed directly.
                                    ingest_pack_stream_with_header(
                                        &header,
                                        buf_tail,
                                        &mut body_stream,
                                        &storage,
                                        &ledger_id,
                                    )
                                    .await
                                };

                                match ingest_result {
                                    Ok(result) => {
                                        let count = result.commits_stored;
                                        let idx_count = result.index_artifacts_stored;
                                        let handle = fluree
                                            .ledger_cached(&ledger_id)
                                            .await
                                            .map_err(|e| {
                                                CliError::Config(format!(
                                                    "failed to load ledger: {e}"
                                                ))
                                            })?;
                                        fluree
                                            .set_commit_head(
                                                &handle,
                                                remote_head_cid,
                                                remote_ns.commit_t,
                                            )
                                            .await
                                            .map_err(|e| {
                                                CliError::Config(format!(
                                                    "pull failed (set head): {e}"
                                                ))
                                            })?;

                                        if idx_count > 0 {
                                            if let Some(ref remote_index_id) =
                                                remote_ns.index_head_id
                                            {
                                                fluree
                                                    .set_index_head(
                                                        &handle,
                                                        remote_index_id,
                                                        remote_ns.index_t,
                                                    )
                                                    .await
                                                    .map_err(|e| {
                                                        CliError::Config(format!(
                                                            "pull failed (set index head): {e}"
                                                        ))
                                                    })?;
                                            }
                                        }

                                        if idx_count > 0 {
                                            println!(
                                                "{} '{}' pulled {} commit(s) + {} index artifact(s) via pack (new head t={})",
                                                "✓".green(),
                                                ledger_id,
                                                count,
                                                idx_count,
                                                remote_ns.commit_t
                                            );
                                        } else {
                                            println!(
                                                "{} '{}' pulled {} commit(s) via pack (new head t={})",
                                                "✓".green(),
                                                ledger_id,
                                                count,
                                                remote_ns.commit_t
                                            );
                                        }
                                        context::persist_refreshed_tokens(
                                            &client,
                                            upstream.remote.as_str(),
                                            dirs,
                                        )
                                        .await;
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "  {} pack import failed: {e}, falling back to paginated export",
                                            "warning:".yellow().bold()
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "  {} pack header read failed: {e}, falling back to paginated export",
                                    "warning:".yellow().bold()
                                );
                            }
                        }
                    }
                    Ok(None) => {} // server doesn't support pack — fall through
                    Err(e) => {
                        eprintln!(
                            "  {} pack request failed: {e}, falling back to paginated export",
                            "warning:".yellow().bold()
                        );
                    }
                }
            }
        }
        Ok(None) => {} // ledger not found via proxy (unexpected); fall back to export
        Err(e) => {
            eprintln!(
                "  {} pack preflight failed: {e}, falling back to paginated export",
                "warning:".yellow().bold()
            );
        }
    }

    // Fetch pages (newest→oldest) until we reach local history.
    let mut all_commits: Vec<fluree_db_api::Base64Bytes> = Vec::new();
    let mut all_blobs: std::collections::HashMap<String, fluree_db_api::Base64Bytes> =
        std::collections::HashMap::new();
    let mut cursor: Option<String> = None;

    loop {
        let page = client
            .fetch_commits(remote_ledger_id, cursor.as_deref(), 100)
            .await
            .map_err(|e| {
                let msg = e.to_string();
                map_sync_auth_error(upstream.remote.as_str(), &msg).unwrap_or_else(|| {
                    CliError::Config(format!("pull failed (fetch commits): {msg}"))
                })
            })?;

        for commit in &page.commits {
            all_commits.push(commit.clone());
        }
        for (addr, blob) in &page.blobs {
            all_blobs
                .entry(addr.clone())
                .or_insert_with(|| blob.clone());
        }

        // If this page reached our local history, stop fetching.
        if page.oldest_t <= local_ref.t + 1 {
            break;
        }
        match page.next_cursor_id {
            Some(cid) => cursor = Some(cid.to_string()),
            None => break, // Reached genesis.
        }
    }

    // Filter to only commits with t > local_t, then reverse to oldest→newest.
    use fluree_db_core::commit::codec::format::{CommitHeader, HEADER_LEN};
    let mut to_import: Vec<fluree_db_api::Base64Bytes> = Vec::new();
    for commit in &all_commits {
        if commit.0.len() < HEADER_LEN {
            continue;
        }
        let header = CommitHeader::read_from(&commit.0)
            .map_err(|e| CliError::Config(format!("invalid commit in pull response: {e}")))?;
        if header.t > local_ref.t {
            to_import.push(commit.clone());
        }
    }
    to_import.reverse(); // oldest→newest

    if to_import.is_empty() {
        println!("{} '{}' is already up to date", "✓".green(), ledger_id);
        context::persist_refreshed_tokens(&client, upstream.remote.as_str(), dirs).await;
        return Ok(());
    }

    let count = to_import.len();

    // Import incrementally (validates chain, ancestry, writes blobs, advances head, updates novelty).
    let handle = fluree
        .ledger_cached(&ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to load ledger: {e}")))?;

    let result = fluree
        .import_commits_incremental(&handle, to_import, all_blobs)
        .await
        .map_err(|e| CliError::Config(format!("pull failed (import): {e}")))?;

    println!(
        "{} '{}' pulled {} commit(s) (new head t={})",
        "✓".green(),
        ledger_id,
        count,
        result.head_t
    );

    // Persist refreshed token if auto-refresh happened.
    context::persist_refreshed_tokens(&client, upstream.remote.as_str(), dirs).await;
    Ok(())
}

/// Push a ledger to its upstream remote
pub async fn run_push(ledger: Option<&str>, dirs: &FlureeDir) -> CliResult<()> {
    let ledger_id = context::resolve_ledger(ledger, dirs)?;
    let ledger_id = context::to_ledger_id(&ledger_id);

    let config_store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let upstream = config_store
        .get_upstream(&ledger_id)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;
    let Some(upstream) = upstream else {
        return Err(CliError::Config(format!(
            "no upstream configured for '{}'\n  {} fluree upstream set {} <remote>",
            ledger_id,
            "hint:".cyan().bold(),
            ledger_id
        )));
    };

    println!(
        "Pushing '{}' to '{}'...",
        ledger_id.cyan(),
        upstream.remote.as_str()
    );

    // Resolve remote config (HTTP only for commit-push).
    let remote_cfg = config_store
        .get_remote(&upstream.remote)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", upstream.remote)))?;

    let base_url = match &remote_cfg.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{}' is not an HTTP remote",
                upstream.remote.as_str()
            )));
        }
    };

    // Build remote ledger client (wire refresh if configured).
    let client = context::build_client_from_auth(&base_url, &remote_cfg.auth);

    // Use the remote ledger ID for all remote API calls (may differ from local ledger_id).
    let remote_ledger_id = &upstream.remote_alias;

    // Resolve remote head (t + commit CID).
    let info = client
        .ledger_info(remote_ledger_id, None)
        .await
        .map_err(|e| CliError::Config(format!("push failed (remote ledger info): {e}")))?;
    let remote_t = info
        .get("t")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| CliError::Config("remote ledger-info response missing 't'".into()))?;
    let remote_commit_id: Option<fluree_db_core::ContentId> = info
        .get("commitId")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok());

    // Resolve local head.
    let fluree = context::build_fluree(dirs)?;
    let local_ref = fluree
        .nameservice_mode()
        .get_ref(&ledger_id, RefKind::CommitHead)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("local ledger '{ledger_id}' not found")))?;

    if local_ref.t < remote_t {
        return Err(CliError::Config(format!(
            "push rejected; remote is ahead (local t={}, remote t={}). Pull first.",
            local_ref.t, remote_t
        )));
    }

    // Collect commits to push (oldest -> newest), ensuring the remote head is in our history.
    let local_head_cid = local_ref.id.clone().ok_or_else(|| {
        CliError::Config(format!(
            "local ledger '{ledger_id}' has no commit head; nothing to push"
        ))
    })?;

    // Use ContentStore for CID-based chain walking (storage-agnostic).
    let content_store = fluree.content_store(&ledger_id);

    let mut to_push_cids: Vec<fluree_db_core::ContentId> = Vec::new();

    // trace_commit_envelopes_by_id yields commits where t > stop_at_t (exclusive
    // of stop_at_t), so when local_t == remote_t the stream yields nothing —
    // that means there is nothing to push and we should short-circuit.
    let mut found_base = local_ref.t == remote_t || (remote_t == 0 && remote_commit_id.is_none());

    if !found_base {
        let stream = fluree_db_core::trace_commit_envelopes_by_id(
            content_store.clone(),
            local_head_cid.clone(),
            remote_t,
        );
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            let (cid, env) = item.map_err(|e| CliError::Config(e.to_string()))?;
            if env.t > remote_t {
                to_push_cids.push(cid);
            }
        }

        // Verify chain continuity: the oldest local commit we want to push
        // should have a previous_id that matches the remote's commitId.
        // If the remote didn't provide commitId, we trust t-based matching.
        if let Some(remote_cid) = remote_commit_id.as_ref() {
            if let Some(oldest_cid) = to_push_cids.last() {
                let oldest_env =
                    fluree_db_core::load_commit_envelope_by_id(&content_store, oldest_cid)
                        .await
                        .map_err(|e| CliError::Config(e.to_string()))?;
                if !oldest_env.parent_ids().any(|id| id == remote_cid) {
                    return Err(CliError::Config(format!(
                        "cannot push: histories diverged at t={remote_t} \
                         (remote head != local history). Pull first."
                    )));
                }
            }
        }

        found_base = !to_push_cids.is_empty();
    }

    if !found_base {
        return Err(CliError::Config(
            "cannot push: remote head not found in local history. Pull first.".into(),
        ));
    }

    if to_push_cids.is_empty() {
        println!("{} '{}' is already up to date", "✓".green(), ledger_id);
        context::persist_refreshed_tokens(&client, upstream.remote.as_str(), dirs).await;
        return Ok(());
    }

    to_push_cids.reverse(); // oldest -> newest

    // Build request: commit bytes + any referenced txn blobs.
    let mut commits = Vec::with_capacity(to_push_cids.len());
    let mut blobs: std::collections::HashMap<String, fluree_db_api::Base64Bytes> =
        std::collections::HashMap::new();

    for cid in &to_push_cids {
        use fluree_db_core::ContentStore;
        let bytes = content_store
            .get(cid)
            .await
            .map_err(|e| CliError::Config(format!("failed to read local commit {cid}: {e}")))?;
        let commit = fluree_db_core::commit::codec::read_commit(&bytes)
            .map_err(|e| CliError::Config(format!("failed to decode local commit {cid}: {e}")))?;
        commits.push(fluree_db_api::Base64Bytes(bytes));

        if let Some(txn_cid) = &commit.txn {
            let txn_key = txn_cid.to_string();
            if let std::collections::hash_map::Entry::Vacant(e) = blobs.entry(txn_key.clone()) {
                let txn_bytes = content_store.get(txn_cid).await.map_err(|e| {
                    CliError::Config(format!(
                        "commit references txn blob '{txn_key}' but it is not readable locally: {e}"
                    ))
                })?;
                e.insert(fluree_db_api::Base64Bytes(txn_bytes));
            }
        }
    }

    let req = fluree_db_api::PushCommitsRequest { commits, blobs };
    let resp = client
        .push_commits(remote_ledger_id, &req)
        .await
        .map_err(|e| CliError::Config(format!("push failed: {e}")))?;

    println!(
        "{} '{}' pushed {} commit(s) (new head t={})",
        "✓".green(),
        ledger_id,
        resp.accepted,
        resp.head.t
    );

    // Persist refreshed token if auto-refresh happened.
    context::persist_refreshed_tokens(&client, upstream.remote.as_str(), dirs).await;
    Ok(())
}

/// Publish a local ledger to a remote server.
///
/// Creates the ledger on the remote if it doesn't exist, pushes all local
/// commits, and configures upstream tracking for subsequent push/pull.
///
/// Usage: `fluree publish <remote> [ledger] [--remote-name <name>]`
pub async fn run_publish(
    remote_name: &str,
    explicit_ledger: Option<&str>,
    remote_ledger_name: Option<&str>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let ledger_id = context::resolve_ledger(explicit_ledger, dirs)?;
    let ledger_id = context::to_ledger_id(&ledger_id);
    let remote_ledger_id = remote_ledger_name
        .map(context::to_ledger_id)
        .unwrap_or_else(|| ledger_id.clone());

    // Resolve remote config.
    let config_store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let remote_cfg = config_store
        .get_remote(&RemoteName::new(remote_name))
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{remote_name}' not found")))?;

    let base_url = match &remote_cfg.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{remote_name}' is not an HTTP remote"
            )));
        }
    };

    let client = context::build_client_from_auth(&base_url, &remote_cfg.auth);

    println!(
        "Publishing '{}' to '{}' (remote ledger: '{}')...",
        ledger_id.cyan(),
        remote_name.cyan(),
        remote_ledger_id.cyan(),
    );

    // Check if remote ledger already exists.
    let remote_exists = client
        .ledger_exists(&remote_ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to check remote ledger existence: {e}")))?;

    if remote_exists {
        // If it exists, check remote head — if remote has commits we can't
        // blindly push everything, fall back to normal push behavior.
        let info = client
            .ledger_info(&remote_ledger_id, None)
            .await
            .map_err(|e| CliError::Config(format!("failed to get remote ledger info: {e}")))?;
        let remote_t = info
            .get("t")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);

        if remote_t > 0 {
            return Err(CliError::Config(format!(
                "remote ledger '{remote_ledger_id}' already has data (t={remote_t}). \
                 Use `fluree push` instead, or choose a different remote ledger name with --remote-name."
            )));
        }
        eprintln!("  Remote ledger exists (empty, t=0) — pushing commits...");
    } else {
        // Create the ledger on the remote.
        client
            .create_ledger(&remote_ledger_id)
            .await
            .map_err(|e| CliError::Config(format!("failed to create remote ledger: {e}")))?;
        eprintln!(
            "  {} Created remote ledger '{}'",
            "✓".green(),
            remote_ledger_id
        );
    }

    // Resolve local head.
    let fluree = context::build_fluree(dirs)?;
    let local_ref = fluree
        .nameservice_mode()
        .get_ref(&ledger_id, RefKind::CommitHead)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("local ledger '{ledger_id}' not found")))?;

    let local_head_cid = local_ref.id.clone().ok_or_else(|| {
        CliError::Config(format!(
            "local ledger '{ledger_id}' has no commits; nothing to publish"
        ))
    })?;

    // Walk the full commit chain (oldest → newest).
    let content_store = fluree.content_store(&ledger_id);

    let mut to_push_cids: Vec<fluree_db_core::ContentId> = Vec::new();
    {
        let stream = fluree_db_novelty::trace_commit_envelopes_by_id(
            content_store.clone(),
            local_head_cid.clone(),
            0, // walk all the way to genesis
        );
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            let (cid, _env) = item.map_err(|e| CliError::Config(e.to_string()))?;
            to_push_cids.push(cid);
        }
    }

    if to_push_cids.is_empty() {
        println!("{} '{}' has no commits to publish", "✓".green(), ledger_id);
        return Ok(());
    }

    to_push_cids.reverse(); // oldest → newest

    // Build push request: commit bytes + txn blobs.
    let mut commits = Vec::with_capacity(to_push_cids.len());
    let mut blobs: std::collections::HashMap<String, fluree_db_api::Base64Bytes> =
        std::collections::HashMap::new();

    for cid in &to_push_cids {
        use fluree_db_core::ContentStore;
        let bytes = content_store
            .get(cid)
            .await
            .map_err(|e| CliError::Config(format!("failed to read local commit {cid}: {e}")))?;
        let commit = fluree_db_core::commit::codec::read_commit(&bytes)
            .map_err(|e| CliError::Config(format!("failed to decode local commit {cid}: {e}")))?;
        commits.push(fluree_db_api::Base64Bytes(bytes));

        if let Some(txn_cid) = &commit.txn {
            let txn_key = txn_cid.to_string();
            if let std::collections::hash_map::Entry::Vacant(e) = blobs.entry(txn_key.clone()) {
                let txn_bytes = content_store.get(txn_cid).await.map_err(|e| {
                    CliError::Config(format!(
                        "commit references txn blob '{txn_key}' but it is not readable locally: {e}"
                    ))
                })?;
                e.insert(fluree_db_api::Base64Bytes(txn_bytes));
            }
        }
    }

    eprint!("  Pushing {} commit(s)...\r", commits.len());

    let req = fluree_db_api::PushCommitsRequest { commits, blobs };
    let resp = client
        .push_commits(&remote_ledger_id, &req)
        .await
        .map_err(|e| CliError::Config(format!("push failed: {e}")))?;

    // Configure upstream tracking.
    use fluree_db_nameservice_sync::UpstreamConfig;
    config_store
        .set_upstream(&UpstreamConfig {
            local_alias: ledger_id.clone(),
            remote: RemoteName::new(remote_name),
            remote_alias: remote_ledger_id.clone(),
            auto_pull: false,
        })
        .await
        .map_err(|e| CliError::Config(format!("failed to set upstream: {e}")))?;

    println!(
        "{} Published '{}' to '{}' ({} commit(s), remote head t={})",
        "✓".green(),
        ledger_id,
        remote_name,
        resp.accepted,
        resp.head.t,
    );
    println!(
        "  {} upstream set to '{}/{}'",
        "→".cyan(),
        remote_name,
        remote_ledger_id,
    );

    // Persist refreshed token if auto-refresh happened.
    context::persist_refreshed_tokens(&client, remote_name, dirs).await;
    Ok(())
}

/// Clone a ledger from a remote server.
///
/// Downloads all commits via paginated export (bulk import), sets the commit
/// head, and configures upstream tracking.
///
/// Usage: `fluree clone <remote> <ledger> [--alias <local-name>]`
pub async fn run_clone(
    remote_name: &str,
    ledger: &str,
    alias: Option<&str>,
    no_indexes: bool,
    no_txns: bool,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let ledger_id = context::to_ledger_id(ledger);
    let local_id = alias.map_or_else(|| ledger_id.clone(), context::to_ledger_id);

    // Clone aliasing is supported: commits use CID-based references (not
    // storage addresses), so the local ledger ID can differ from the remote.

    // Resolve remote config.
    let config_store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let remote_cfg = config_store
        .get_remote(&RemoteName::new(remote_name))
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{remote_name}' not found")))?;

    if let Some(tok) = &remote_cfg.auth.token {
        if let Some(false) = token_has_storage_permissions(tok) {
            return Err(replication_permission_error(remote_name));
        }
    }

    let base_url = match &remote_cfg.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{remote_name}' is not an HTTP remote"
            )));
        }
    };

    let client = context::build_client_from_auth(&base_url, &remote_cfg.auth);

    // Verify the remote ledger exists (ledger_info returns 404 if not).
    let info = client
        .ledger_info(&ledger_id, None)
        .await
        .map_err(|e| CliError::Config(format!("clone failed (remote ledger info): {e}")))?;

    // t is informational — some servers may not include it in their response.
    let remote_t = info.get("t").and_then(serde_json::Value::as_i64);

    if remote_t == Some(0) {
        return Err(CliError::Config(format!(
            "remote ledger '{ledger_id}' is empty (t=0); nothing to clone"
        )));
    }

    match remote_t {
        Some(t) => println!(
            "Cloning '{}' from '{}' (remote t={})...",
            local_id.cyan(),
            remote_name.cyan(),
            t
        ),
        None => println!(
            "Cloning '{}' from '{}'...",
            local_id.cyan(),
            remote_name.cyan(),
        ),
    }

    // Create the local ledger.
    let fluree = context::build_fluree(dirs)?;
    let storage = fluree
        .backend()
        .admin_storage_cloned()
        .ok_or_else(|| CliError::Config("sync requires managed storage backend".into()))?;
    fluree
        .create_ledger(&local_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to create local ledger: {e}")))?;

    // Fetch all commit objects from the remote.
    let mut head_commit_id: Option<fluree_db_core::ContentId> = None;
    let mut head_t: i64 = 0;
    let mut total_commits: usize = 0;

    let handle = fluree
        .ledger_cached(&local_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to load ledger: {e}")))?;

    // Try pack protocol first (needs NsRecord for head CID).
    let mut used_pack = false;
    let mut remote_ns_record: Option<fluree_db_nameservice::NsRecord> = None;
    let mut index_artifacts_stored: usize = 0;
    match client.fetch_ns_record(&ledger_id).await {
        Ok(Some(ns)) => {
            if let Some(ref hcid) = ns.commit_head_id {
                // Build pack request — include indexes by default for clone.
                let mut pack_request = if !no_indexes {
                    if let Some(ref remote_index_id) = ns.index_head_id {
                        PackRequest::with_indexes(
                            vec![hcid.clone()],
                            vec![],
                            remote_index_id.clone(),
                            None, // Clone has no local index
                        )
                    } else {
                        PackRequest::commits(vec![hcid.clone()], vec![])
                    }
                } else {
                    PackRequest::commits(vec![hcid.clone()], vec![])
                };
                if no_txns {
                    pack_request.include_txns = false;
                }

                match client.fetch_pack_response(&ledger_id, &pack_request).await {
                    Ok(Some(response)) => {
                        // Peek header for size estimation.
                        let mut body_stream = response.bytes_stream();
                        match peek_pack_header(&mut body_stream).await {
                            Ok((header, buf_tail)) => {
                                let wants_indexes = pack_request.include_indexes;
                                let is_large =
                                    header.estimated_total_bytes > LARGE_TRANSFER_THRESHOLD;

                                let ingest_result = if wants_indexes && is_large {
                                    if confirm_large_transfer(header.estimated_total_bytes) {
                                        ingest_pack_stream_with_header(
                                            &header,
                                            buf_tail,
                                            &mut body_stream,
                                            &storage,
                                            &local_id,
                                        )
                                        .await
                                    } else {
                                        drop(body_stream);
                                        eprintln!(
                                            "  Skipping index transfer, cloning commits only..."
                                        );
                                        let commits_only = if no_txns {
                                            PackRequest::commits_no_txns(vec![hcid.clone()], vec![])
                                        } else {
                                            PackRequest::commits(vec![hcid.clone()], vec![])
                                        };
                                        match client
                                            .fetch_pack_response(&ledger_id, &commits_only)
                                            .await
                                        {
                                            Ok(Some(resp2)) => {
                                                ingest_pack_stream(
                                                    resp2,
                                                    &storage,
                                                    &local_id,
                                                )
                                                .await
                                            }
                                            Ok(None) => Err(
                                                fluree_db_nameservice_sync::SyncError::PackNotSupported,
                                            ),
                                            Err(e) => Err(
                                                fluree_db_nameservice_sync::SyncError::Remote(
                                                    e.to_string(),
                                                ),
                                            ),
                                        }
                                    }
                                } else {
                                    ingest_pack_stream_with_header(
                                        &header,
                                        buf_tail,
                                        &mut body_stream,
                                        &storage,
                                        &local_id,
                                    )
                                    .await
                                };

                                match ingest_result {
                                    Ok(result) => {
                                        total_commits = result.commits_stored;
                                        index_artifacts_stored = result.index_artifacts_stored;
                                        head_commit_id = Some(hcid.clone());
                                        head_t = ns.commit_t;
                                        used_pack = true;
                                        let objects = result.commits_stored
                                            + result.txn_blobs_stored
                                            + result.index_artifacts_stored;
                                        eprint!("  fetched {objects} object(s) via pack\r");
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "  {} pack import failed: {e}, falling back to paginated export",
                                            "warning:".yellow().bold()
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "  {} pack header read failed: {e}, falling back to paginated export",
                                    "warning:".yellow().bold()
                                );
                            }
                        }
                    }
                    Ok(None) => {} // server doesn't support pack — fall through
                    Err(e) => {
                        eprintln!(
                            "  {} pack request failed: {e}, falling back to paginated export",
                            "warning:".yellow().bold()
                        );
                    }
                }
            }
            remote_ns_record = Some(ns);
        }
        Ok(None) => {} // ledger not found via proxy; fall back
        Err(e) => {
            eprintln!(
                "  {} pack preflight failed: {e}, falling back to paginated export",
                "warning:".yellow().bold()
            );
        }
    }

    if !used_pack {
        let mut cursor: Option<String> = None;
        loop {
            let page = client
                .fetch_commits(&ledger_id, cursor.as_deref(), 500)
                .await
                .map_err(|e| {
                    let msg = e.to_string();
                    map_sync_auth_error(remote_name, &msg).unwrap_or_else(|| {
                        CliError::Config(format!("clone failed (fetch commits): {msg}"))
                    })
                })?;

            if head_commit_id.is_none() {
                head_commit_id = Some(page.head_commit_id.clone());
                head_t = page.head_t;
            }

            fluree
                .import_commits_bulk(&handle, &page)
                .await
                .map_err(|e| CliError::Config(format!("clone failed (import): {e}")))?;

            total_commits += page.count;
            eprint!("  fetched {total_commits} commits...\r");

            match page.next_cursor_id {
                Some(cid) => cursor = Some(cid.to_string()),
                None => break,
            }
        }
    }

    eprintln!(); // Clear progress line.

    // Set the commit head.
    let head_cid = head_commit_id
        .ok_or_else(|| CliError::Config("clone failed: no commits fetched from remote".into()))?;
    fluree
        .set_commit_head(&handle, &head_cid, head_t)
        .await
        .map_err(|e| CliError::Config(format!("clone failed (set head): {e}")))?;

    // Set index head if index artifacts were transferred.
    if index_artifacts_stored > 0 {
        if let Some(ref ns) = remote_ns_record {
            if let Some(ref remote_index_id) = ns.index_head_id {
                fluree
                    .set_index_head(&handle, remote_index_id, ns.index_t)
                    .await
                    .map_err(|e| CliError::Config(format!("clone failed (set index head): {e}")))?;
            }
        }
    }

    // Configure upstream tracking.
    // Note: UpstreamConfig fields use `_alias` naming (pre-existing; should be `_id`).
    use fluree_db_nameservice_sync::UpstreamConfig;
    config_store
        .set_upstream(&UpstreamConfig {
            local_alias: local_id.clone(),
            remote: RemoteName::new(remote_name),
            remote_alias: ledger_id.clone(),
            auto_pull: false,
        })
        .await
        .map_err(|e| CliError::Config(format!("failed to set upstream: {e}")))?;

    if index_artifacts_stored > 0 {
        println!(
            "{} Cloned '{}' ({} commits + {} index artifacts, head t={})",
            "✓".green(),
            local_id,
            total_commits,
            index_artifacts_stored,
            head_t
        );
    } else {
        println!(
            "{} Cloned '{}' ({} commits, head t={})",
            "✓".green(),
            local_id,
            total_commits,
            head_t
        );
    }
    println!(
        "  {} upstream set to '{}/{}'",
        "→".cyan(),
        remote_name,
        ledger_id
    );

    // Persist refreshed token if auto-refresh happened.
    context::persist_refreshed_tokens(&client, remote_name, dirs).await;
    Ok(())
}

/// Clone a ledger from an origin URI using CID-based chain walking.
///
/// Downloads all commits by following the parent chain from the head
/// commit backwards, fetching each commit + txn blob via MultiOriginFetcher.
///
/// Usage: `fluree clone --origin http://localhost:8090 mydb:main`
pub async fn run_clone_origin(
    origin_uri: &str,
    token: Option<&str>,
    ledger: &str,
    alias: Option<&str>,
    no_indexes: bool,
    no_txns: bool,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let ledger_id = context::to_ledger_id(ledger);
    let local_id = alias.map_or_else(|| ledger_id.clone(), context::to_ledger_id);

    // 1. Build bootstrap fetcher from the single origin URI.
    let mut fetcher = MultiOriginFetcher::from_bootstrap(origin_uri, token.map(String::from));

    // 2. Fetch NsRecord from the remote to discover the head commit.
    let ns_record = fetcher
        .fetch_ns_record(&ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("clone failed (fetch ns record): {e}")))?
        .ok_or_else(|| CliError::NotFound(format!("ledger '{ledger_id}' not found on origin")))?;

    let head_t = ns_record.commit_t;

    // 3. Optionally upgrade fetcher via LedgerConfig (if advertised in NsRecord).
    let mut config_bytes_for_storage: Option<(fluree_db_core::ContentId, Vec<u8>)> = None;
    if let Some(config_cid) = &ns_record.config_id {
        match fetcher.fetch(config_cid, &ledger_id).await {
            Ok(config_bytes) => {
                if !config_cid.verify(&config_bytes) {
                    return Err(CliError::Config(
                        "LedgerConfig integrity verification failed".into(),
                    ));
                }
                let config: LedgerConfig = serde_json::from_slice(&config_bytes)
                    .map_err(|e| CliError::Config(format!("invalid LedgerConfig: {e}")))?;
                fetcher = MultiOriginFetcher::from_config(&config, token.map(String::from));
                config_bytes_for_storage = Some((config_cid.clone(), config_bytes));
            }
            Err(e) => {
                // Non-fatal: fall back to bootstrap fetcher.
                eprintln!(
                    "  {} could not fetch LedgerConfig: {e}",
                    "warning:".yellow().bold()
                );
            }
        }
    }

    // 4. Create the local ledger.
    let fluree = context::build_fluree(dirs)?;
    let storage = fluree
        .backend()
        .admin_storage_cloned()
        .ok_or_else(|| CliError::Config("sync requires managed storage backend".into()))?;
    fluree
        .create_ledger(&local_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to create local ledger: {e}")))?;

    // Handle empty remote ledger: local ledger created but nothing to fetch.
    let head_cid = match &ns_record.commit_head_id {
        Some(cid) => cid.clone(),
        None => {
            println!(
                "{} Created '{}' (remote is empty, nothing to fetch)",
                "✓".green(),
                local_id,
            );
            return Ok(());
        }
    };

    println!(
        "Cloning '{}' from '{}' (remote t={})...",
        local_id.cyan(),
        origin_uri.cyan(),
        head_t
    );

    // 5. Fetch commit chain — try pack protocol first (single round-trip).
    let content_store = fluree.content_store(&local_id);
    let mut commits_fetched = 0usize;
    let mut index_artifacts_fetched = 0usize;

    // Build pack request — include indexes by default for clone.
    let mut pack_request = if !no_indexes {
        if let Some(ref remote_index_id) = ns_record.index_head_id {
            PackRequest::with_indexes(
                vec![head_cid.clone()],
                vec![],
                remote_index_id.clone(),
                None, // Clone has no local index
            )
        } else {
            PackRequest::commits(vec![head_cid.clone()], vec![])
        }
    } else {
        PackRequest::commits(vec![head_cid.clone()], vec![])
    };
    if no_txns {
        pack_request.include_txns = false;
    }
    let used_pack = match fetcher.fetch_pack_response(&ledger_id, &pack_request).await {
        Ok(Some(response)) => {
            let mut body_stream = response.bytes_stream();
            match peek_pack_header(&mut body_stream).await {
                Ok((header, buf_tail)) => {
                    let wants_indexes = pack_request.include_indexes;
                    let is_large = header.estimated_total_bytes > LARGE_TRANSFER_THRESHOLD;

                    let ingest_result = if wants_indexes && is_large {
                        if confirm_large_transfer(header.estimated_total_bytes) {
                            ingest_pack_stream_with_header(
                                &header,
                                buf_tail,
                                &mut body_stream,
                                &storage,
                                &local_id,
                            )
                            .await
                        } else {
                            drop(body_stream);
                            eprintln!("  Skipping index transfer, cloning commits only...");
                            let commits_only = if no_txns {
                                PackRequest::commits_no_txns(vec![head_cid.clone()], vec![])
                            } else {
                                PackRequest::commits(vec![head_cid.clone()], vec![])
                            };
                            match fetcher.fetch_pack_response(&ledger_id, &commits_only).await {
                                Ok(Some(resp2)) => {
                                    ingest_pack_stream(resp2, &storage, &local_id).await
                                }
                                Ok(None) => {
                                    Err(fluree_db_nameservice_sync::SyncError::PackNotSupported)
                                }
                                Err(e) => Err(e),
                            }
                        }
                    } else {
                        ingest_pack_stream_with_header(
                            &header,
                            buf_tail,
                            &mut body_stream,
                            &storage,
                            &local_id,
                        )
                        .await
                    };

                    match ingest_result {
                        Ok(result) => {
                            commits_fetched = result.commits_stored;
                            index_artifacts_fetched = result.index_artifacts_stored;
                            let objects = result.commits_stored
                                + result.txn_blobs_stored
                                + result.index_artifacts_stored;
                            eprint!("  fetched {objects} object(s) via pack\r");
                            true
                        }
                        Err(e) => {
                            eprintln!(
                                "  {} pack import failed: {e}, falling back to object fetch",
                                "warning:".yellow().bold()
                            );
                            false
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "  {} pack header read failed: {e}, falling back to object fetch",
                        "warning:".yellow().bold()
                    );
                    false
                }
            }
        }
        Ok(None) => false,
        Err(e) => {
            eprintln!(
                "  {} pack not available: {e}, falling back to object fetch",
                "warning:".yellow().bold()
            );
            false
        }
    };

    if !used_pack {
        // Walk the commit chain backwards from head, fetching + storing blobs.
        //
        // If a commit already exists locally (e.g., from a previously interrupted
        // clone), we skip fetching but still read the local bytes to continue
        // chain traversal — this ensures all ancestors and txn blobs are present.
        let mut frontier = vec![head_cid.clone()];
        let mut visited_commits = std::collections::HashSet::new();

        while let Some(cid) = frontier.pop() {
            if !visited_commits.insert(cid.clone()) {
                continue;
            }
            let commit_bytes = if content_store.has(&cid).await.unwrap_or(false) {
                // Already have this commit — read local bytes for chain traversal.
                content_store.get(&cid).await.map_err(|e| {
                    CliError::Config(format!("clone failed (read local commit): {e}"))
                })?
            } else {
                // Fetch commit blob from origin.
                let bytes = fetcher
                    .fetch(&cid, &ledger_id)
                    .await
                    .map_err(|e| CliError::Config(format!("clone failed (fetch commit): {e}")))?;

                // Verify integrity + derive CID from the commit blob.
                let derived_id = verify_commit_blob(&bytes).map_err(|e| {
                    CliError::Config(format!("clone failed (commit integrity): {e}"))
                })?;
                if derived_id != cid {
                    return Err(CliError::Config(format!(
                        "clone failed: commit CID mismatch (expected {cid}, got {derived_id})"
                    )));
                }

                // Store commit blob locally.
                storage
                    .content_write_bytes(ContentKind::Commit, &local_id, &bytes)
                    .await
                    .map_err(|e| CliError::Config(format!("clone failed (store commit): {e}")))?;

                commits_fetched += 1;
                eprint!("  fetched {commits_fetched} commit(s)...\r");

                bytes
            };

            // Parse envelope-only (no flake decode) for parent + txn CID.
            let envelope = read_commit_envelope(&commit_bytes)
                .map_err(|e| CliError::Config(format!("clone failed (read envelope): {e}")))?;

            // Fetch + store txn blob (if present and not explicitly skipped).
            if !no_txns {
                if let Some(txn_cid) = &envelope.txn {
                    if !content_store.has(txn_cid).await.unwrap_or(false) {
                        let txn_bytes = fetcher.fetch(txn_cid, &ledger_id).await.map_err(|e| {
                            CliError::Config(format!("clone failed (fetch txn blob): {e}"))
                        })?;
                        // Txn blobs use full-bytes SHA-256, so put_with_id is safe.
                        content_store
                            .put_with_id(txn_cid, &txn_bytes)
                            .await
                            .map_err(|e| {
                                CliError::Config(format!("clone failed (store txn blob): {e}"))
                            })?;
                    }
                }
            }

            // Follow all parents backwards.
            for parent_id in envelope.parent_ids() {
                frontier.push(parent_id.clone());
            }
        }
    }

    eprintln!(); // Clear progress line.

    // 6. Set the commit head on the local ledger.
    let handle = fluree
        .ledger_cached(&local_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to load ledger: {e}")))?;

    fluree
        .set_commit_head(&handle, &head_cid, head_t)
        .await
        .map_err(|e| CliError::Config(format!("clone failed (set head): {e}")))?;

    // Set index head if index artifacts were transferred.
    if index_artifacts_fetched > 0 {
        if let Some(ref remote_index_id) = ns_record.index_head_id {
            fluree
                .set_index_head(&handle, remote_index_id, ns_record.index_t)
                .await
                .map_err(|e| CliError::Config(format!("clone failed (set index head): {e}")))?;
        }
    }

    // 7. Store LedgerConfig blob + update config_id on local NsRecord (if fetched).
    if let Some((config_cid, config_bytes)) = config_bytes_for_storage {
        use fluree_db_nameservice::ConfigCasResult;

        // Store the LedgerConfig blob in CAS.
        content_store
            .put_with_id(&config_cid, &config_bytes)
            .await
            .map_err(|e| CliError::Config(format!("clone failed (store LedgerConfig): {e}")))?;

        // Update the config_id on the local NsRecord via ConfigPublisher.
        let current = fluree
            .nameservice_mode()
            .get_config(&local_id)
            .await
            .map_err(|e| CliError::Config(format!("clone failed (get config): {e}")))?;
        let existing_payload = current
            .as_ref()
            .and_then(|c| c.payload.clone())
            .unwrap_or_default();
        let new_config = ConfigValue::new(
            current.as_ref().map_or(1, |c| c.v + 1),
            Some(ConfigPayload {
                config_id: Some(config_cid),
                default_context: existing_payload.default_context,
                extra: existing_payload.extra,
            }),
        );
        match fluree
            .nameservice_mode()
            .push_config(&local_id, current.as_ref(), &new_config)
            .await
            .map_err(|e| CliError::Config(format!("clone failed (push config): {e}")))?
        {
            ConfigCasResult::Updated => {}
            ConfigCasResult::Conflict { .. } => {
                // During clone this is unexpected (we just created the ledger), but
                // handle it gracefully — the config blob is already in CAS, so a
                // retry via `fluree config set-origins` will work.
                eprintln!(
                    "  {} config was modified concurrently; LedgerConfig blob stored but config_id not set",
                    "warning:".yellow().bold()
                );
            }
        }
    }

    if index_artifacts_fetched > 0 {
        println!(
            "{} Cloned '{}' ({} commit(s) + {} index artifact(s), head t={})",
            "✓".green(),
            local_id,
            commits_fetched,
            index_artifacts_fetched,
            head_t
        );
    } else {
        println!(
            "{} Cloned '{}' ({} commit(s), head t={})",
            "✓".green(),
            local_id,
            commits_fetched,
            head_t
        );
    }

    Ok(())
}

/// Pull via origins (CID chain walk) when no named upstream is configured.
///
/// Loads the local LedgerConfig from CAS, builds a MultiOriginFetcher,
/// fetches the remote NsRecord, then walks the commit chain from the remote
/// head to the local head, storing blobs along the way.
async fn run_pull_via_origins(
    ledger_id: &str,
    no_indexes: bool,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let fluree = context::build_fluree(dirs)?;
    let storage = fluree
        .backend()
        .admin_storage_cloned()
        .ok_or_else(|| CliError::Config("sync requires managed storage backend".into()))?;

    let ns_record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await?
        .ok_or_else(|| CliError::NotFound(format!("local ledger '{ledger_id}' not found")))?;

    let config_id = ns_record.config_id.ok_or_else(|| {
        CliError::Config(format!(
            "no upstream and no LedgerConfig for '{}'; cannot pull\n  {} set an upstream: fluree upstream set {} <remote>\n  {} or set origins: fluree config set-origins {} --file origins.json",
            ledger_id,
            "hint:".cyan().bold(),
            ledger_id,
            "hint:".cyan().bold(),
            ledger_id,
        ))
    })?;

    // Load LedgerConfig from local CAS.
    let content_store = fluree.content_store(ledger_id);
    let config_bytes = content_store.get(&config_id).await.map_err(|e| {
        CliError::Config(format!("failed to load LedgerConfig from local CAS: {e}"))
    })?;
    let config: LedgerConfig = serde_json::from_slice(&config_bytes)
        .map_err(|e| CliError::Config(format!("invalid LedgerConfig: {e}")))?;

    // Build fetcher from config (no token for now; future: credential store).
    let fetcher = MultiOriginFetcher::from_config(&config, None);
    if fetcher.is_empty() {
        return Err(CliError::Config(
            "LedgerConfig has no satisfiable origins".into(),
        ));
    }

    // Fetch remote NsRecord to discover current head.
    let remote_ns = fetcher
        .fetch_ns_record(ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("pull failed (fetch ns record): {e}")))?
        .ok_or_else(|| {
            CliError::NotFound(format!(
                "ledger '{ledger_id}' not found on any configured origin"
            ))
        })?;

    let remote_head_cid = remote_ns.commit_head_id.ok_or_else(|| {
        CliError::Config(format!("remote ledger '{ledger_id}' is empty (no commits)"))
    })?;
    let remote_t = remote_ns.commit_t;

    // Resolve local head.
    let local_ref = fluree
        .nameservice_mode()
        .get_ref(ledger_id, RefKind::CommitHead)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("local ledger '{ledger_id}' not found")))?;

    if remote_t <= local_ref.t {
        println!("{} '{}' is already up to date", "✓".green(), ledger_id);
        return Ok(());
    }

    println!(
        "Pulling '{}' via origins (local t={}, remote t={})...",
        ledger_id.cyan(),
        local_ref.t,
        remote_t
    );

    // Try pack protocol first (single round-trip).
    let have: Vec<fluree_db_core::ContentId> = local_ref.id.clone().into_iter().collect();
    let pack_request = if !no_indexes {
        if let Some(ref remote_index_id) = remote_ns.index_head_id {
            let local_index_id = fluree
                .nameservice_mode()
                .get_ref(ledger_id, RefKind::IndexHead)
                .await
                .ok()
                .flatten()
                .and_then(|r| r.id);
            PackRequest::with_indexes(
                vec![remote_head_cid.clone()],
                have,
                remote_index_id.clone(),
                local_index_id,
            )
        } else {
            PackRequest::commits(vec![remote_head_cid.clone()], have)
        }
    } else {
        PackRequest::commits(vec![remote_head_cid.clone()], have)
    };
    match fetcher.fetch_pack_response(ledger_id, &pack_request).await {
        Ok(Some(response)) => {
            let mut body_stream = response.bytes_stream();
            match peek_pack_header(&mut body_stream).await {
                Ok((header, buf_tail)) => {
                    let wants_indexes = pack_request.include_indexes;
                    let is_large = header.estimated_total_bytes > LARGE_TRANSFER_THRESHOLD;

                    let ingest_result = if wants_indexes && is_large {
                        if confirm_large_transfer(header.estimated_total_bytes) {
                            ingest_pack_stream_with_header(
                                &header,
                                buf_tail,
                                &mut body_stream,
                                &storage,
                                ledger_id,
                            )
                            .await
                        } else {
                            drop(body_stream);
                            eprintln!("  Skipping index transfer, pulling commits only...");
                            let have: Vec<fluree_db_core::ContentId> =
                                local_ref.id.clone().into_iter().collect();
                            let commits_only =
                                PackRequest::commits(vec![remote_head_cid.clone()], have);
                            match fetcher.fetch_pack_response(ledger_id, &commits_only).await {
                                Ok(Some(resp2)) => {
                                    ingest_pack_stream(resp2, &storage, ledger_id).await
                                }
                                Ok(None) => {
                                    Err(fluree_db_nameservice_sync::SyncError::PackNotSupported)
                                }
                                Err(e) => Err(e),
                            }
                        }
                    } else {
                        ingest_pack_stream_with_header(
                            &header,
                            buf_tail,
                            &mut body_stream,
                            &storage,
                            ledger_id,
                        )
                        .await
                    };

                    match ingest_result {
                        Ok(result) => {
                            let count = result.commits_stored;
                            let idx_count = result.index_artifacts_stored;
                            let handle = fluree.ledger_cached(ledger_id).await.map_err(|e| {
                                CliError::Config(format!("failed to load ledger: {e}"))
                            })?;
                            fluree
                                .set_commit_head(&handle, &remote_head_cid, remote_t)
                                .await
                                .map_err(|e| {
                                    CliError::Config(format!("pull failed (set head): {e}"))
                                })?;

                            if idx_count > 0 {
                                if let Some(ref remote_index_id) = remote_ns.index_head_id {
                                    fluree
                                        .set_index_head(&handle, remote_index_id, remote_ns.index_t)
                                        .await
                                        .map_err(|e| {
                                            CliError::Config(format!(
                                                "pull failed (set index head): {e}"
                                            ))
                                        })?;
                                }
                            }

                            if idx_count > 0 {
                                println!(
                                    "{} '{}' pulled {} commit(s) + {} index artifact(s) via pack (new head t={})",
                                    "✓".green(),
                                    ledger_id,
                                    count,
                                    idx_count,
                                    remote_t
                                );
                            } else {
                                println!(
                                    "{} '{}' pulled {} commit(s) via pack (new head t={})",
                                    "✓".green(),
                                    ledger_id,
                                    count,
                                    remote_t
                                );
                            }
                            return Ok(());
                        }
                        Err(e) => {
                            eprintln!(
                                "  {} pack import failed: {e}, falling back to CID walk",
                                "warning:".yellow().bold()
                            );
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "  {} pack header read failed: {e}, falling back to CID walk",
                        "warning:".yellow().bold()
                    );
                }
            }
        }
        Ok(None) => {} // server doesn't support pack — fall through
        Err(e) => {
            eprintln!(
                "  {} pack not available: {e}, falling back to CID walk",
                "warning:".yellow().bold()
            );
        }
    }

    // Walk chain from remote head toward local head.
    struct FetchedCommit {
        bytes: Vec<u8>,
        txn: Option<fluree_db_core::ContentId>,
    }
    let mut fetched: Vec<FetchedCommit> = Vec::new();
    let mut frontier = vec![remote_head_cid.clone()];
    let mut visited_pull = std::collections::HashSet::new();

    while let Some(cid) = frontier.pop() {
        if !visited_pull.insert(cid.clone()) {
            continue;
        }
        // Primary stop: hit local head CID — its ancestry is complete.
        if local_ref.id.as_ref() == Some(&cid) {
            continue;
        }

        // If commit already exists locally (e.g., from an interrupted pull),
        // read local bytes to continue chain traversal but skip re-fetching.
        if content_store.has(&cid).await.unwrap_or(false) {
            let local_bytes = content_store
                .get(&cid)
                .await
                .map_err(|e| CliError::Config(format!("pull failed (read local commit): {e}")))?;
            let envelope = read_commit_envelope(&local_bytes)
                .map_err(|e| CliError::Config(format!("pull failed (read envelope): {e}")))?;
            if envelope.t <= local_ref.t {
                continue;
            }
            for parent_id in envelope.parent_ids() {
                frontier.push(parent_id.clone());
            }
            continue;
        }

        let commit_bytes = fetcher
            .fetch(&cid, ledger_id)
            .await
            .map_err(|e| CliError::Config(format!("pull failed (fetch commit): {e}")))?;

        // Verify integrity.
        let derived_id = verify_commit_blob(&commit_bytes)
            .map_err(|e| CliError::Config(format!("pull failed (commit integrity): {e}")))?;
        if derived_id != cid {
            return Err(CliError::Config(format!(
                "pull failed: commit CID mismatch (expected {cid}, got {derived_id})"
            )));
        }

        let envelope = read_commit_envelope(&commit_bytes)
            .map_err(|e| CliError::Config(format!("pull failed (read envelope): {e}")))?;

        // Secondary stop: t-based early exit (reached or passed local history).
        if envelope.t <= local_ref.t {
            continue;
        }

        for parent_id in envelope.parent_ids() {
            frontier.push(parent_id.clone());
        }
        fetched.push(FetchedCommit {
            bytes: commit_bytes,
            txn: envelope.txn,
        });
    }

    if fetched.is_empty() {
        println!("{} '{}' is already up to date", "✓".green(), ledger_id);
        return Ok(());
    }

    // Reverse to oldest→newest, store blobs.
    fetched.reverse();
    for fc in &fetched {
        storage
            .content_write_bytes(ContentKind::Commit, ledger_id, &fc.bytes)
            .await
            .map_err(|e| CliError::Config(format!("pull failed (store commit): {e}")))?;

        if let Some(txn_cid) = &fc.txn {
            if !content_store.has(txn_cid).await.unwrap_or(false) {
                let txn_bytes = fetcher
                    .fetch(txn_cid, ledger_id)
                    .await
                    .map_err(|e| CliError::Config(format!("pull failed (fetch txn blob): {e}")))?;
                content_store
                    .put_with_id(txn_cid, &txn_bytes)
                    .await
                    .map_err(|e| CliError::Config(format!("pull failed (store txn blob): {e}")))?;
            }
        }
    }

    let count = fetched.len();

    // Set new commit head.
    let handle = fluree
        .ledger_cached(ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to load ledger: {e}")))?;
    fluree
        .set_commit_head(&handle, &remote_head_cid, remote_t)
        .await
        .map_err(|e| CliError::Config(format!("pull failed (set head): {e}")))?;

    println!(
        "{} '{}' pulled {} commit(s) (new head t={})",
        "✓".green(),
        ledger_id,
        count,
        remote_t
    );

    Ok(())
}
