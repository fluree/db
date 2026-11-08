//! Sync commands: fetch, pull, push, clone (named-remote and origin-based)

use crate::config::{resolve_storage_path, TomlSyncConfigStore};
use crate::context;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_core::commit::codec::{commit_blob_matches, read_commit_envelope};
use fluree_db_core::pack::{PackRequest, LARGE_TRANSFER_THRESHOLD};
use fluree_db_core::storage::ContentAddressedWrite;
use fluree_db_core::ContentKind;
use fluree_db_core::ContentStore;
use fluree_db_nameservice::{
    ConfigLookup, ConfigPayload, ConfigValue, FileTrackingStore, LedgerConfig, RefKind, RefLookup,
    RemoteName, RemoteTrackingStore,
};
use fluree_db_nameservice_sync::{
    ingest_pack_stream_into, ingest_pack_stream_with_header_into, peek_pack_header, FetchResult,
    HttpRemoteClient, MultiOriginFetcher, PackSink, RemoteEndpoint, SyncConfigStore, SyncDriver,
};
use std::sync::Arc;

/// Takes a pack's erasure frames and journal as they arrive.
struct Reception<'a> {
    fluree: &'a fluree_db_api::Fluree,
    taken: fluree_db_api::retention::erasure::PackReception,
}

impl<'a> Reception<'a> {
    fn new(fluree: &'a fluree_db_api::Fluree) -> Self {
        Reception {
            fluree,
            taken: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl PackSink for Reception<'_> {
    async fn frame(
        &mut self,
        frame: fluree_db_core::pack::PackFrame,
    ) -> std::result::Result<(), String> {
        self.fluree
            .receive_pack_frame(&mut self.taken, frame)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Where an origin walk reads objects: an origin's object requests.
#[async_trait::async_trait]
trait ObjectSource: Sync {
    async fn object(
        &self,
        cid: &fluree_db_core::ContentId,
        ledger_id: &str,
    ) -> fluree_db_nameservice_sync::Result<Vec<u8>>;
}

#[async_trait::async_trait]
impl ObjectSource for MultiOriginFetcher {
    async fn object(
        &self,
        cid: &fluree_db_core::ContentId,
        ledger_id: &str,
    ) -> fluree_db_nameservice_sync::Result<Vec<u8>> {
        self.fetch(cid, ledger_id).await
    }
}

/// Walk `ledger_id`'s history from `head` object by object, as `fluree
/// clone --origin` does without a pack, storing it as `local_id`'s: each
/// commit it does not hold, and its transaction document unless
/// `no_txns`. An erased commit's answer goes to `reception`. Returns the
/// commits fetched.
async fn walk_origin(
    fluree: &fluree_db_api::Fluree,
    source: &dyn ObjectSource,
    (ledger_id, local_id): (&str, &str),
    head: &fluree_db_core::ContentId,
    (storage, namespace): (
        &Arc<dyn fluree_db_core::storage::Storage>,
        &fluree_db_core::StorageNamespace,
    ),
    no_txns: bool,
    reception: &mut OriginReception<'_>,
) -> CliResult<usize> {
    // If a commit already exists locally (e.g., from a previously interrupted
    // clone), we skip fetching but still read the local bytes to continue
    // chain traversal — this ensures all ancestors and txn blobs are present.
    let content_store = fluree.content_store(namespace);
    let mut commits_fetched = 0usize;
    let mut frontier = vec![head.clone()];
    let mut visited_commits = std::collections::HashSet::new();

    while let Some(cid) = frontier.pop() {
        if !visited_commits.insert(cid.clone()) {
            continue;
        }
        // A read of an erased commit returns its replacement here whether
        // or not this ledger holds it; one it does not is fetched, for the
        // erasure that makes it a holder.
        let held = content_store.has(&cid).await.unwrap_or(false)
            && fluree
                .ledger_holds_commit(local_id, &cid)
                .await
                .map_err(|e| CliError::Config(format!("clone failed (read local commit): {e}")))?;
        let commit_bytes = if held {
            // Already have this commit — read local bytes for chain traversal.
            content_store
                .get(&cid)
                .await
                .map_err(|e| CliError::Config(format!("clone failed (read local commit): {e}")))?
                .into_vec()
        } else {
            // Fetch commit blob from origin.
            let bytes = match source.object(&cid, ledger_id).await {
                // It travels as its erasure; nothing is stored for it.
                Err(fluree_db_nameservice_sync::SyncError::Erased { frames, .. }) => {
                    let replacement = reception.take_erased(&cid, &frames).await?;
                    let envelope = read_commit_envelope(&replacement).map_err(|e| {
                        CliError::Config(format!("clone failed (read erased commit {cid}): {e}"))
                    })?;
                    frontier.extend(envelope.parent_ids().cloned());
                    continue;
                }
                fetched => fetched
                    .map_err(|e| CliError::Config(format!("clone failed (fetch commit): {e}")))?,
            };

            if !commit_blob_matches(&cid, &bytes) {
                return Err(CliError::Config(format!(
                    "clone failed: commit {cid} does not match its bytes"
                )));
            }

            // Store commit blob locally, under its CID's own address.
            storage
                .content_write_bytes_with_hash(
                    ContentKind::Commit,
                    namespace,
                    &cid.address_digest(),
                    &bytes,
                )
                .await
                .map_err(|e| CliError::Config(format!("clone failed (store commit): {e}")))?;

            commits_fetched += 1;
            eprint!("  fetched {commits_fetched} commit(s)...\r");

            bytes
        };

        // Parse envelope-only (no flake decode) for parent + txn CID.
        let envelope = read_commit_envelope(&commit_bytes)
            .map_err(|e| CliError::Config(format!("clone failed (read envelope): {e}")))?;

        // Fetch + store txn blob (if present and not explicitly skipped). An
        // erased commit's is not served.
        let erased = fluree_db_core::commit::codec::replacement::is_replacement(&commit_bytes);
        if !no_txns && !erased {
            if let Some(txn_cid) = &envelope.txn {
                if !content_store.has(txn_cid).await.unwrap_or(false) {
                    // A txn blob the origin does not HAVE is a provenance
                    // gap on the source, not a reason to abandon the
                    // clone — the commit itself carries the flakes. Only
                    // a genuine all-origins 404 is tolerated: an auth
                    // failure, a 5xx, a network blip, or an integrity
                    // failure still aborts, so a transient fault can't
                    // masquerade as a provenance gap.
                    match source.object(txn_cid, ledger_id).await {
                        Ok(txn_bytes) => {
                            // Txn blobs use full-bytes SHA-256, so put_with_id is safe.
                            content_store
                                .put_with_id(txn_cid, &txn_bytes)
                                .await
                                .map_err(|e| {
                                    CliError::Config(format!("clone failed (store txn blob): {e}"))
                                })?;
                        }
                        Err(fluree_db_nameservice_sync::SyncError::NotFound(_)) => {
                            eprintln!(
                                "  warning: commit t={} references txn blob {txn_cid} which no origin has; continuing without it",
                                envelope.t
                            );
                        }
                        Err(e) => {
                            return Err(CliError::Config(format!(
                                "clone failed (fetch txn blob {txn_cid}): {e}"
                            )));
                        }
                    }
                }
            }
        }

        // Follow all parents backwards.
        for parent_id in envelope.parent_ids() {
            frontier.push(parent_id.clone());
        }
    }
    Ok(commits_fetched)
}

/// Ready `ledger_id` for the head a pack from `remote` brought: its
/// erasures, which only an erasure authority's are, and its journal.
async fn accept_pack(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    reception: Reception<'_>,
    head: &fluree_db_core::ContentId,
    remote: &fluree_db_nameservice_sync::RemoteConfig,
) -> CliResult<fluree_db_api::retention::erasure::AcceptedPack> {
    fluree
        .accept_pack(ledger_id, reception.taken, head, remote.erasure_authority)
        .await
        .map_err(|e| match e.status_code() {
            403 => CliError::Config(format!(
                "{e}: to accept them, designate '{name}' an erasure authority with \
                 `fluree remote erasure-authority {name} true`",
                name = remote.name.as_str()
            )),
            _ => CliError::Config(format!("the pack's erasures were not accepted: {e}")),
        })
}

/// Takes what an origin's transfer brings besides objects: its journal,
/// and its erasures when the transfer designates the origin an erasure
/// authority (`--erasure-authority`; no remote setting covers an origin).
/// Otherwise it refuses them, and remembers that it did: that history must
/// not then be fetched object by object, past what it erases.
struct OriginReception<'a> {
    fluree: &'a fluree_db_api::Fluree,
    taken: fluree_db_api::retention::erasure::PackReception,
    authority: bool,
    refused: bool,
}

impl<'a> OriginReception<'a> {
    fn new(fluree: &'a fluree_db_api::Fluree, authority: bool) -> Self {
        OriginReception {
            fluree,
            taken: Default::default(),
            authority,
            refused: false,
        }
    }

    /// Take an object request's answer for an erased commit: its frames.
    /// Returns the commit's replacement, whose envelope the walk follows.
    async fn take_erased(
        &mut self,
        cid: &fluree_db_core::ContentId,
        frames: &[u8],
    ) -> CliResult<Vec<u8>> {
        use fluree_db_core::pack::{
            decode_frame, ErasureArtifactKind, PackFrame, DEFAULT_MAX_PAYLOAD,
        };
        let mut replacement = None;
        let mut pos = 0;
        while pos < frames.len() {
            let (frame, used) = decode_frame(&frames[pos..], DEFAULT_MAX_PAYLOAD)
                .map_err(|e| CliError::Config(format!("the erasure of commit {cid}: {e}")))?;
            pos += used;
            if let PackFrame::ErasureArtifact {
                kind: ErasureArtifactKind::Replacement,
                payload,
            } = &frame
            {
                replacement = Some(payload.clone());
            }
            self.frame(frame).await.map_err(CliError::Config)?;
        }
        replacement.ok_or_else(|| {
            CliError::Config(format!(
                "the erasure of commit {cid} carries no replacement"
            ))
        })
    }

    /// Ready `ledger_id` for `head`, before it moves there: take the
    /// erasures, and merge the origin's journal into its own.
    async fn accept(
        &mut self,
        ledger_id: &str,
        head: &fluree_db_core::ContentId,
    ) -> CliResult<usize> {
        self.fluree
            .accept_pack(
                ledger_id,
                std::mem::take(&mut self.taken),
                head,
                self.authority,
            )
            .await
            .map(|accepted| accepted.erased_commits)
            .map_err(|e| CliError::Config(format!("the origin's transfer was not accepted: {e}")))
    }
}

#[async_trait::async_trait]
impl PackSink for OriginReception<'_> {
    async fn frame(
        &mut self,
        frame: fluree_db_core::pack::PackFrame,
    ) -> std::result::Result<(), String> {
        if self.authority || matches!(frame, fluree_db_core::pack::PackFrame::Manifest(_)) {
            return self
                .fluree
                .receive_pack_frame(&mut self.taken, frame)
                .await
                .map_err(|e| e.to_string());
        }
        self.refused = true;
        Err(
            "the origin's history holds permanently erased commits; take them with \
             `--erasure-authority`, which designates the origin an erasure authority for this \
             transfer, or clone or pull from a remote designated one"
                .to_string(),
        )
    }
}

/// What `remote`'s history gained, without a commit, since this copy has
/// all of its commits: erasures, and its journal. A pack of no commits,
/// whose head is the remote's, which must be in this copy's history.
/// Returns the erased commits it brought.
async fn pull_without_commits(
    fluree: &fluree_db_api::Fluree,
    client: &crate::remote_client::RemoteLedgerClient,
    remote_ledger_id: &str,
    ledger_id: &str,
    local_head: &fluree_db_nameservice::RefValue,
    remote: &fluree_db_nameservice_sync::RemoteConfig,
    (storage, namespace): (
        &Arc<dyn fluree_db_core::storage::Storage>,
        &fluree_db_core::StorageNamespace,
    ),
) -> CliResult<usize> {
    let failed = |e: String| CliError::Config(format!("pull failed (journal and erasures): {e}"));
    let Some(head) = client
        .fetch_ns_record(remote_ledger_id)
        .await
        .map_err(|e| failed(e.to_string()))?
        .and_then(|ns| ns.commit_head_id)
    else {
        return Ok(0);
    };
    let held_here = local_head.id.as_ref() == Some(&head)
        || fluree
            .ledger_holds_commit(ledger_id, &head)
            .await
            .unwrap_or(false);
    if !held_here {
        return Ok(0);
    }
    let held = fluree
        .held_erasure_digests()
        .await
        .map_err(|e| failed(e.to_string()))?;
    let request = PackRequest::commits(vec![head.clone()], vec![head.clone()]).with_erasures(held);
    // A server without packs has nothing to add.
    let Some(response) = client
        .fetch_pack_response(remote_ledger_id, &request)
        .await
        .map_err(|e| failed(e.to_string()))?
    else {
        return Ok(0);
    };
    let mut reception = Reception::new(fluree);
    ingest_pack_stream_into(response, storage, namespace, Some(&mut reception))
        .await
        .map_err(|e| failed(e.to_string()))?;
    Ok(accept_pack(fluree, ledger_id, reception, &head, remote)
        .await?
        .erased_commits)
}

/// Finish what an interrupted clone or pull left of its erasures.
async fn resume_erasures(fluree: &fluree_db_api::Fluree) {
    if let Err(e) = fluree.resume_received_erasures().await {
        eprintln!(
            "  {} an earlier transfer's erasures were not completed: {e}",
            "warning:".yellow().bold()
        );
    }
}

/// Passes one pull runs on its erasures' files before leaving the rest to
/// the next.
const PURGE_PASSES: usize = 8;

/// What deleting a ledger's erased values from this copy's storage came to.
#[derive(Debug, PartialEq, Eq)]
enum Purge {
    /// No erasure of the ledger had files left to delete.
    Nothing,
    /// This many erasures' files are deleted.
    Done(usize),
    /// Some are still there, for this reason.
    Pending(String),
}

impl Purge {
    fn print(&self) {
        match self {
            Purge::Nothing => {}
            Purge::Done(requests) => println!(
                "{} {requests} erasure(s) purged: the erased values are deleted from this copy's \
                 storage",
                "✓".green()
            ),
            Purge::Pending(why) => eprintln!(
                "  {} erased values are still in this copy's storage ({why}); the next \
                 `fluree pull` carries on",
                "warning:".yellow().bold()
            ),
        }
    }
}

/// Delete from this copy's storage what `ledger_id`'s erasures still leave
/// there: the originals of the erased commits, the indexes built from them,
/// and their transactions. A server first waits for readers of the older
/// indexes to finish; this process holds the storage and reads none of
/// them, so it does not.
async fn purge_received(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> Purge {
    let mut requests = std::collections::BTreeSet::new();
    for _ in 0..PURGE_PASSES {
        let report = match fluree
            .run_purge_pass(ledger_id, std::time::Duration::ZERO)
            .await
        {
            Ok(report) => report,
            Err(e) => return Purge::Pending(e.to_string()),
        };
        requests.extend(report.requests);
        if let Some(why) = report.blocked {
            return Purge::Pending(why);
        }
        if !report.waiting {
            return match requests.len() {
                0 => Purge::Nothing,
                requests => Purge::Done(requests),
            };
        }
    }
    Purge::Pending(format!("not finished in {PURGE_PASSES} passes"))
}

/// A received index this copy did not take: its history holds a commit
/// erased here, which the sender's index may hold the original of.
fn index_not_set_note() {
    eprintln!(
        "  {} the received index was not used: this history holds a commit erased \
         here, so the ledger indexes its replacements itself",
        "note:".cyan().bold()
    );
}

/// `count` erased commits, as a summary's suffix.
fn erased_note(count: usize) -> String {
    match count {
        0 => String::new(),
        n => format!(", {n} erased"),
    }
}

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

/// Whether a remote error string indicates an auth/permission failure
/// (expired or insufficient token). `fluree-db-nameservice-sync` reports
/// remote failures as strings, so we match on the common server messages.
///
/// The HTTP status checks match the canonical reason phrase (e.g.
/// `401 Unauthorized`) rather than the bare code: the sync client always
/// renders statuses via reqwest's `StatusCode` Display, and the error
/// strings interpolate the request URL and response body, so a bare `401`
/// could false-match a port or ledger name that merely contains those digits.
fn is_replication_auth_error(err: &str) -> bool {
    err.contains("401 Unauthorized")
        || err.contains("403 Forbidden")
        || err.contains("Bearer token required")
        || err.contains("Untrusted issuer")
        || err.contains("Token lacks storage proxy permissions")
        || err.contains("Storage proxy not enabled")
}

fn map_sync_auth_error(remote: &str, err: &str) -> Option<CliError> {
    if is_replication_auth_error(err) {
        Some(replication_permission_error(remote))
    } else {
        None
    }
}

/// Where the local ledger `ledger_id` stores its artifacts.
async fn local_namespace(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> CliResult<fluree_db_core::StorageNamespace> {
    fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .map(|record| record.storage_namespace())
        .ok_or_else(|| CliError::NotFound(format!("local ledger '{ledger_id}' not found")))
}

/// Keep garbage collection out of `ledger_id` while a transfer stores
/// commits and transactions before setting a head naming them.
async fn hold_garbage(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> CliResult<Option<fluree_db_core::RootHold>> {
    fluree
        .hold_garbage(ledger_id)
        .await
        .map_err(|e| CliError::Config(e.to_string()))
}

/// Build a SyncDriver with all configured remotes
async fn build_sync_driver(dirs: &FlureeDir) -> CliResult<(SyncDriver, Arc<TomlSyncConfigStore>)> {
    let fluree = context::build_fluree(dirs).await?;
    let config_store = Arc::new(TomlSyncConfigStore::new(dirs.config_dir().to_path_buf()));

    let local: Arc<dyn fluree_db_nameservice::NameServicePublisher> = fluree
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
            let t = tracking.commit_head.as_ref().map(|r| r.t).unwrap_or(0);
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
pub async fn run_pull(
    ledger: Option<&str>,
    no_indexes: bool,
    erasure_authority: bool,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let ledger_id = context::resolve_ledger(ledger, dirs)?;
    let ledger_id = context::to_ledger_id(&ledger_id)?;
    let fluree = pull(ledger_id.clone(), no_indexes, erasure_authority, dirs).await?;
    purge_received(&fluree, &ledger_id).await.print();
    Ok(())
}

/// Pull `ledger_id` from its upstream, or else its origins. Returns the
/// instance it pulled into, its hold on the ledger's garbage released.
async fn pull(
    ledger_id: fluree_db_core::LedgerId,
    no_indexes: bool,
    erasure_authority: bool,
    dirs: &FlureeDir,
) -> CliResult<fluree_db_api::Fluree> {
    let config_store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let upstream = config_store
        .get_upstream(&ledger_id)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    let Some(upstream) = upstream else {
        // No named upstream — try origin-based pull via LedgerConfig.
        return run_pull_via_origins(&ledger_id, no_indexes, erasure_authority, dirs).await;
    };
    if erasure_authority {
        return Err(CliError::Usage(format!(
            "--erasure-authority applies to pulls from origins; designate the remote '{}' \
             with `fluree remote erasure-authority {} true`",
            upstream.remote.as_str(),
            upstream.remote.as_str()
        )));
    }

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
    let fluree = context::build_fluree(dirs).await?;
    resume_erasures(&fluree).await;
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
    let namespace = local_namespace(&fluree, &ledger_id).await?;
    let _garbage_hold = hold_garbage(&fluree, &ledger_id).await?;

    if remote_t <= local_ref.t {
        let erased = pull_without_commits(
            &fluree,
            &client,
            remote_ledger_id.as_str(),
            ledger_id.as_str(),
            &local_ref,
            &remote_cfg,
            (&storage, &namespace),
        )
        .await?;
        println!(
            "{} '{}' is already up to date{}",
            "✓".green(),
            ledger_id,
            erased_note(erased)
        );
        context::persist_refreshed_tokens(&client, upstream.remote.as_str(), dirs).await;
        return Ok(fluree);
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
                let held = fluree
                    .held_erasure_digests()
                    .await
                    .map_err(|e| CliError::Config(e.to_string()))?;
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
                }
                .with_erasures(held.clone());
                let mut reception = Reception::new(&fluree);

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
                                        ingest_pack_stream_with_header_into(
                                            &header,
                                            buf_tail,
                                            &mut body_stream,
                                            &storage,
                                            &namespace,
                                            Some(&mut reception),
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
                                        )
                                        .with_erasures(held.clone());
                                        match client
                                            .fetch_pack_response(
                                                remote_ledger_id,
                                                &commits_only,
                                            )
                                            .await
                                        {
                                            Ok(Some(resp2)) => {
                                                ingest_pack_stream_into(
                                                    resp2,
                                                    &storage,
                                                    &namespace,
                                                    Some(&mut reception),
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
                                    ingest_pack_stream_with_header_into(
                                        &header,
                                        buf_tail,
                                        &mut body_stream,
                                        &storage,
                                        &namespace,
                                        Some(&mut reception),
                                    )
                                    .await
                                };

                                match ingest_result {
                                    Ok(result) => {
                                        let count = result.commits_stored;
                                        let idx_count = result.index_artifacts_stored;
                                        // Before the head moves: what it erases
                                        // of history held here goes first.
                                        let accepted = accept_pack(
                                            &fluree,
                                            ledger_id.as_str(),
                                            reception,
                                            remote_head_cid,
                                            &remote_cfg,
                                        )
                                        .await?;
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
                                                if !fluree
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
                                                    })?
                                                {
                                                    index_not_set_note();
                                                }
                                            }
                                        }

                                        if idx_count > 0 {
                                            println!(
                                                "{} '{}' pulled {} commit(s) + {} index artifact(s) via pack{} (new head t={})",
                                                "✓".green(),
                                                ledger_id,
                                                count,
                                                idx_count,
                                                erased_note(accepted.erased_commits),
                                                remote_ns.commit_t
                                            );
                                        } else {
                                            println!(
                                                "{} '{}' pulled {} commit(s) via pack{} (new head t={})",
                                                "✓".green(),
                                                ledger_id,
                                                count,
                                                erased_note(accepted.erased_commits),
                                                remote_ns.commit_t
                                            );
                                        }
                                        context::persist_refreshed_tokens(
                                            &client,
                                            upstream.remote.as_str(),
                                            dirs,
                                        )
                                        .await;
                                        return Ok(fluree);
                                    }
                                    Err(e) => {
                                        let msg = e.to_string();
                                        if is_replication_auth_error(&msg) {
                                            // Token expired mid-transfer: falling back to a
                                            // paginated export would just 401 again. Fail fast
                                            // with the re-login guidance.
                                            return Err(replication_permission_error(
                                                upstream.remote.as_str(),
                                            ));
                                        }
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

    // Fetch pages (newest→oldest) until we reach local history. The request
    // asks for the first-parent line and the commits its merges brought in.
    // A server predating that mode answers with every parent in one list,
    // which imports only when the history has no merge.
    let mut pages: Vec<fluree_db_api::ExportCommitsResponse> = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let page = client
            .fetch_commits(
                remote_ledger_id,
                cursor.as_deref(),
                local_ref.id.as_ref(),
                100,
            )
            .await
            .map_err(|e| {
                let msg = e.to_string();
                map_sync_auth_error(upstream.remote.as_str(), &msg).unwrap_or_else(|| {
                    CliError::Config(format!("pull failed (fetch commits): {msg}"))
                })
            })?;

        // A `lineage` export stops at our head by itself. The default export
        // has to be cut off once a page reaches our history.
        let reached_local = !page.lineage && page.oldest_t <= local_ref.t + 1;
        let next = page.next_cursor_id.clone();
        pages.push(page);
        if reached_local {
            break;
        }
        match next {
            Some(cid) => cursor = Some(cid.to_string()),
            None => break, // Reached our head, or genesis.
        }
    }

    let (to_import, merged_commits) = pull_import_set(&pages, local_ref.t)?;
    let mut all_blobs = std::collections::HashMap::new();
    for page in pages {
        for (addr, blob) in page.blobs {
            all_blobs.entry(addr).or_insert(blob);
        }
    }

    if to_import.is_empty() {
        println!("{} '{}' is already up to date", "✓".green(), ledger_id);
        context::persist_refreshed_tokens(&client, upstream.remote.as_str(), dirs).await;
        return Ok(fluree);
    }

    let count = to_import.len();

    // Import incrementally (validates chain, ancestry, writes blobs, advances head, updates novelty).
    let (commit_ids, commits) = to_import.into_iter().unzip();
    let (merged_commit_ids, merged_commits) = merged_commits.into_iter().unzip();
    let result = fluree
        .import_commits_incremental(
            &ledger_id,
            fluree_db_api::PushCommitsRequest {
                commits,
                blobs: all_blobs,
                missing_blobs: Vec::new(),
                merged_commits,
                commit_ids,
                merged_commit_ids,
                ..Default::default()
            },
        )
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
    Ok(fluree)
}

/// The commits a paginated pull imports: the line oldest first, and the
/// merged-in commits parents before children.
///
/// `pages` are in the order they were fetched, newest first. A `lineage`
/// export holds only new commits. The default export also holds commits at
/// or below `local_t`, which are dropped.
fn pull_import_set(
    pages: &[fluree_db_api::ExportCommitsResponse],
    local_t: i64,
) -> CliResult<(PulledCommits, PulledCommits)> {
    use fluree_db_core::commit::codec::format::{CommitHeader, HEADER_LEN};

    let lineage = pages.iter().all(|page| page.lineage);
    let mut to_import = Vec::new();
    let mut merged_pages = Vec::with_capacity(pages.len());
    for page in pages {
        let (line_ids, merged_ids) = page
            .ids()
            .map_err(|e| CliError::Config(format!("invalid pull response: {e}")))?;
        for (id, commit) in line_ids.into_iter().zip(&page.commits) {
            if commit.0.len() < HEADER_LEN {
                continue;
            }
            let header = CommitHeader::read_from(&commit.0)
                .map_err(|e| CliError::Config(format!("invalid commit in pull response: {e}")))?;
            if lineage || header.t > local_t {
                to_import.push((id, commit.clone()));
            }
        }
        merged_pages.push(
            merged_ids
                .into_iter()
                .zip(page.merged_commits.iter().cloned()),
        );
    }
    to_import.reverse(); // oldest→newest

    // Older pages first, so parents stay ahead of children. A commit that
    // merges on two pages brought in arrives twice and is kept once.
    let mut seen = std::collections::HashSet::new();
    let merged = merged_pages
        .into_iter()
        .rev()
        .flatten()
        .filter(|(id, _)| seen.insert(id.clone()))
        .collect();
    Ok((to_import, merged))
}

/// Pulled commits with their CIDs, in import order.
type PulledCommits = Vec<(fluree_db_core::ContentId, fluree_db_api::Base64Bytes)>;

/// The remote head a push plans from, or `None` when the remote has no
/// commits.
///
/// A remote that reports a head `t` without a commit id is matched to the
/// local line's commit at that `t`.
async fn remote_base(
    store: &dyn ContentStore,
    local_head: &fluree_db_core::ContentId,
    remote_t: i64,
    remote_commit_id: Option<fluree_db_core::ContentId>,
) -> CliResult<Option<fluree_db_core::ContentId>> {
    if remote_commit_id.is_some() || remote_t == 0 {
        return Ok(remote_commit_id);
    }
    let line = fluree_db_core::collect_first_parent_cids(store, local_head, remote_t - 1)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;
    line.into_iter()
        .find(|(t, _)| *t == remote_t)
        .map(|(_, id)| Some(id))
        .ok_or_else(|| {
            CliError::Config(
                "cannot push: remote head not found in local history. Pull first.".into(),
            )
        })
}

/// Refuse a push whose history contains a merge when the remote does not
/// accept the commits that merge brought in.
async fn ensure_merges_supported(
    client: &crate::remote_client::RemoteLedgerClient,
    plan: &fluree_db_core::CommitTransferPlan,
) -> CliResult<()> {
    if plan.merged.is_empty() || client.supports_push_merges().await {
        return Ok(());
    }
    Err(CliError::Config(format!(
        "cannot push: this history contains a merge, which brings in {} commit(s) \
         from other branches. The remote does not accept those commits. Upgrade the \
         remote server, then push again.",
        plan.merged.len()
    )))
}

/// Read a plan's commits, and the txn blobs they reference, into a push
/// request.
async fn build_push_request(
    store: &dyn ContentStore,
    plan: &fluree_db_core::CommitTransferPlan,
) -> CliResult<fluree_db_api::PushCommitsRequest> {
    let mut blobs = std::collections::HashMap::new();
    let mut missing_blobs = Vec::new();
    let commits = read_push_commits(store, &plan.lineage, &mut blobs, &mut missing_blobs).await?;
    let merged_commits =
        read_push_commits(store, &plan.merged, &mut blobs, &mut missing_blobs).await?;
    Ok(fluree_db_api::PushCommitsRequest {
        commits,
        blobs,
        missing_blobs,
        merged_commits,
        commit_ids: plan.lineage.clone(),
        merged_commit_ids: plan.merged.clone(),
        ..Default::default()
    })
}

/// Read commit bytes in order, collecting each commit's txn blob into
/// `blobs`.
///
/// A txn blob missing locally is declared in `missing_blobs` rather than
/// left for the receiver to infer from an absent key.
async fn read_push_commits(
    store: &dyn ContentStore,
    cids: &[fluree_db_core::ContentId],
    blobs: &mut std::collections::HashMap<String, fluree_db_api::Base64Bytes>,
    missing_blobs: &mut Vec<String>,
) -> CliResult<Vec<fluree_db_api::Base64Bytes>> {
    let mut commits = Vec::with_capacity(cids.len());
    for cid in cids {
        let bytes = store
            .get(cid)
            .await
            .map_err(|e| CliError::Config(format!("failed to read local commit {cid}: {e}")))?
            .into_vec();
        let commit = fluree_db_core::commit::codec::read_commit(&bytes)
            .map_err(|e| CliError::Config(format!("failed to decode local commit {cid}: {e}")))?;
        // An erased commit's transaction document holds what was erased.
        let erased = fluree_db_core::commit::codec::replacement::is_replacement(&bytes);
        commits.push(fluree_db_api::Base64Bytes(bytes));

        if let Some(txn_cid) = commit.txn.as_ref().filter(|_| !erased) {
            let txn_key = txn_cid.to_string();
            if let std::collections::hash_map::Entry::Vacant(e) = blobs.entry(txn_key.clone()) {
                match store.get(txn_cid).await {
                    Ok(txn_bytes) => {
                        e.insert(fluree_db_api::Base64Bytes(txn_bytes.into_vec()));
                    }
                    Err(fluree_db_core::Error::NotFound(_)) => {
                        eprintln!(
                            "  warning: commit t={} references txn blob '{txn_key}' which is missing locally; pushing without it",
                            commit.t
                        );
                        missing_blobs.push(txn_key);
                    }
                    Err(e) => {
                        return Err(CliError::Config(format!(
                            "commit references txn blob '{txn_key}' but it is not readable locally: {e}"
                        )));
                    }
                }
            }
        }
    }
    Ok(commits)
}

const ERASURES_UNSUPPORTED: &str = "cannot push: this history holds permanently erased \
     commits, which the remote does not take. It must be a single server on file storage, of a \
     release that takes them.";

/// The frames a push of erasure chains alone carries: a push body is at
/// most 50 MB, and base64 adds a third.
const PUSH_ERASURE_BYTES: usize = 32 * 1024 * 1024;

/// Send a push to the remote, ready for erased history and retention (see
/// `Fluree::ready_push`); `base` is the remote's head.
async fn send_push(
    fluree: &fluree_db_api::Fluree,
    client: &crate::remote_client::RemoteLedgerClient,
    ids: (&str, &str),
    mut req: fluree_db_api::PushCommitsRequest,
    base: Option<&fluree_db_core::ContentId>,
) -> CliResult<fluree_db_api::PushCommitsResponse> {
    let erased = fluree
        .ready_push(ids.0, &mut req, base)
        .await
        .map_err(|e| CliError::Config(format!("push failed: {e}")))?;
    if erased && !client.supports_push_erasures().await {
        return Err(CliError::Config(ERASURES_UNSUPPORTED.into()));
    }
    warn_if_removals_stay(client, &req).await;
    send_readied(fluree, client, ids, req, base).await
}

/// Warn when `req` carries removals the remote does not take (a cluster,
/// which keeps its own journal, or a release before removals): it goes on
/// serving the history they remove here.
async fn warn_if_removals_stay(
    client: &crate::remote_client::RemoteLedgerClient,
    req: &fluree_db_api::PushCommitsRequest,
) {
    if req.carries_removals() && !client.supports_push_retention().await {
        eprintln!(
            "  {} the remote does not take this ledger's removal journal from a push: a \
             removal made only here is not made there, and it goes on serving that \
             history. Removals made there, or pulled from there, are unaffected.",
            "warning:".yellow().bold()
        );
    }
}

/// What `ledger_id`'s history up to `head`, which the remote has, gained
/// without a commit: erasures, and the removal journal. Pushes them, with
/// no commits, when the remote takes such a push. Returns whether it did.
async fn push_without_commits(
    fluree: &fluree_db_api::Fluree,
    client: &crate::remote_client::RemoteLedgerClient,
    ids: (&str, &str),
    head: &fluree_db_core::ContentId,
) -> CliResult<bool> {
    let mut req = fluree_db_api::PushCommitsRequest::default();
    let erased = fluree
        .ready_push(ids.0, &mut req, Some(head))
        .await
        .map_err(|e| CliError::Config(format!("push failed: {e}")))?;
    if erased && !client.supports_push_erasures().await {
        return Err(CliError::Config(ERASURES_UNSUPPORTED.into()));
    }
    if !erased && (req.retention.is_none() || !client.supports_push_retention().await) {
        warn_if_removals_stay(client, &req).await;
        return Ok(false);
    }
    send_readied(fluree, client, ids, req, Some(head)).await?;
    Ok(true)
}

/// Send `req`, ready. When the remote asks for the erasures of commits it
/// already has, push them first, in pushes of no commits a body holds,
/// then `req` again.
async fn send_readied(
    fluree: &fluree_db_api::Fluree,
    client: &crate::remote_client::RemoteLedgerClient,
    (ledger_id, remote_ledger_id): (&str, &str),
    req: fluree_db_api::PushCommitsRequest,
    base: Option<&fluree_db_core::ContentId>,
) -> CliResult<fluree_db_api::PushCommitsResponse> {
    match client.push_commits(remote_ledger_id, &req).await {
        Err(crate::remote_client::RemoteLedgerError::Conflict(msg)) => {
            let (Some(needed), Some(base)) = (fluree_db_api::erasures_needed(&msg), base) else {
                return Err(push_error(
                    &req,
                    crate::remote_client::RemoteLedgerError::Conflict(msg),
                ));
            };
            let mut pushes = fluree
                .erasure_pushes(ledger_id, base, &needed, PUSH_ERASURE_BYTES)
                .await
                .map_err(|e| CliError::Config(format!("push failed: {e}")))?;
            while let Some(erasures) = pushes
                .next()
                .await
                .map_err(|e| CliError::Config(format!("push failed: {e}")))?
            {
                client
                    .push_commits(remote_ledger_id, &erasures)
                    .await
                    .map_err(|e| push_error(&erasures, e))?;
            }
            client
                .push_commits(remote_ledger_id, &req)
                .await
                .map_err(|e| push_error(&req, e))
        }
        result => result.map_err(|e| push_error(&req, e)),
    }
}

fn push_error(
    req: &fluree_db_api::PushCommitsRequest,
    e: crate::remote_client::RemoteLedgerError,
) -> CliError {
    match e {
        // The remote advertised `push-merges`, so a 404 there most likely
        // came from a node that has not been upgraded yet.
        crate::remote_client::RemoteLedgerError::NotFound(msg)
            if !req.merged_commits.is_empty() =>
        {
            CliError::Config(format!(
                "push failed: the remote answered 404 for a push containing a merge \
                 ({msg}). It advertises support for such pushes. A node serving the \
                 request may still run an earlier release."
            ))
        }
        e => CliError::Config(format!("push failed: {e}")),
    }
}

/// Push a ledger to its upstream remote
pub async fn run_push(ledger: Option<&str>, dirs: &FlureeDir) -> CliResult<()> {
    let ledger_id = context::resolve_ledger(ledger, dirs)?;
    let ledger_id = context::to_ledger_id(&ledger_id)?;

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
    let fluree = context::build_fluree(dirs).await?;
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

    let local_head_cid = local_ref.id.clone().ok_or_else(|| {
        CliError::Config(format!(
            "local ledger '{ledger_id}' has no commit head; nothing to push"
        ))
    })?;

    // Use a branch-aware ContentStore for CID-based chain walking. Without
    // the branch fallback, walking past a fork point on a branched ledger
    // 404s on every pre-fork ancestor.
    let content_store = fluree
        .branched_content_store(&ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to build branched store: {e}")))?;

    let base = remote_base(
        content_store.as_ref(),
        &local_head_cid,
        remote_t,
        remote_commit_id,
    )
    .await?;
    let plan = fluree_db_core::plan_commit_transfer(
        content_store.as_ref(),
        &local_head_cid,
        base.as_ref(),
    )
    .await
    .map_err(|e| CliError::Config(e.to_string()))?
    .ok_or_else(|| {
        CliError::Config(format!(
            "cannot push: histories diverged at t={remote_t} \
             (the remote head is not on this branch's line of commits). Pull first."
        ))
    })?;

    if plan.lineage.is_empty() {
        let sent = push_without_commits(
            &fluree,
            &client,
            (ledger_id.as_str(), remote_ledger_id.as_str()),
            &local_head_cid,
        )
        .await?;
        let note = if sent {
            " (sent its erasures and removal journal)"
        } else {
            ""
        };
        println!(
            "{} '{}' is already up to date{note}",
            "✓".green(),
            ledger_id
        );
        context::persist_refreshed_tokens(&client, upstream.remote.as_str(), dirs).await;
        return Ok(());
    }

    ensure_merges_supported(&client, &plan).await?;
    let req = build_push_request(content_store.as_ref(), &plan).await?;
    let resp = send_push(
        &fluree,
        &client,
        (ledger_id.as_str(), remote_ledger_id.as_str()),
        req,
        base.as_ref(),
    )
    .await?;

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
    let ledger_id = context::to_ledger_id(&ledger_id)?;
    let remote_ledger_id = remote_ledger_name
        .map(context::to_ledger_id)
        .transpose()?
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

    // Resolve local head.
    let fluree = context::build_fluree(dirs).await?;
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

    // Walking to genesis on a branched ledger always crosses the fork point,
    // so we need the branch-aware store to resolve pre-fork ancestors.
    let content_store = fluree
        .branched_content_store(&ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to build branched store: {e}")))?;

    // Planned before the remote ledger is created, so a remote that cannot
    // take this history is refused before anything is written to it.
    let plan = fluree_db_core::plan_commit_transfer(content_store.as_ref(), &local_head_cid, None)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .expect("every line reaches genesis when there is no base");
    ensure_merges_supported(&client, &plan).await?;
    let mut req = build_push_request(content_store.as_ref(), &plan).await?;
    let erased = fluree
        .ready_push(&ledger_id, &mut req, None)
        .await
        .map_err(|e| CliError::Config(format!("publish failed: {e}")))?;
    if erased && !client.supports_push_erasures().await {
        return Err(CliError::Config(ERASURES_UNSUPPORTED.into()));
    }
    warn_if_removals_stay(&client, &req).await;

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

    eprint!("  Pushing {} commit(s)...\r", req.commits.len());
    let resp = send_push(
        &fluree,
        &client,
        (ledger_id.as_str(), remote_ledger_id.as_str()),
        req,
        None,
    )
    .await?;

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
    let ledger_id = context::to_ledger_id(ledger)?;
    let local_id = alias.map_or_else(|| Ok(ledger_id.clone()), context::to_ledger_id)?;

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
    let fluree = context::build_fluree(dirs).await?;
    resume_erasures(&fluree).await;
    let storage = fluree
        .backend()
        .admin_storage_cloned()
        .ok_or_else(|| CliError::Config("sync requires managed storage backend".into()))?;
    fluree
        .create_ledger(&local_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to create local ledger: {e}")))?;
    let namespace = local_namespace(&fluree, &local_id).await?;
    let _garbage_hold = hold_garbage(&fluree, &local_id).await?;

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
    let mut erased_commits: usize = 0;
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
                let held = fluree
                    .held_erasure_digests()
                    .await
                    .map_err(|e| CliError::Config(e.to_string()))?;
                pack_request = pack_request.with_erasures(held.clone());
                let mut reception = Reception::new(&fluree);

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
                                        ingest_pack_stream_with_header_into(
                                            &header,
                                            buf_tail,
                                            &mut body_stream,
                                            &storage,
                                            &namespace,
                                            Some(&mut reception),
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
                                        }
                                        .with_erasures(held.clone());
                                        match client
                                            .fetch_pack_response(&ledger_id, &commits_only)
                                            .await
                                        {
                                            Ok(Some(resp2)) => {
                                                ingest_pack_stream_into(
                                                    resp2,
                                                    &storage,
                                                    &namespace,
                                                    Some(&mut reception),
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
                                    ingest_pack_stream_with_header_into(
                                        &header,
                                        buf_tail,
                                        &mut body_stream,
                                        &storage,
                                        &namespace,
                                        Some(&mut reception),
                                    )
                                    .await
                                };

                                match ingest_result {
                                    Ok(result) => {
                                        // Before the head is set: its erasures
                                        // read for the history they erase.
                                        erased_commits = accept_pack(
                                            &fluree,
                                            local_id.as_str(),
                                            reception,
                                            hcid,
                                            &remote_cfg,
                                        )
                                        .await?
                                        .erased_commits;
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
                                        let msg = e.to_string();
                                        if is_replication_auth_error(&msg) {
                                            // Token expired mid-transfer: falling back to a
                                            // paginated export would just 401 again. Fail fast
                                            // with the re-login guidance.
                                            return Err(replication_permission_error(remote_name));
                                        }
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
                .fetch_commits(&ledger_id, cursor.as_deref(), None, 500)
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
                if !fluree
                    .set_index_head(&handle, remote_index_id, ns.index_t)
                    .await
                    .map_err(|e| CliError::Config(format!("clone failed (set index head): {e}")))?
                {
                    index_not_set_note();
                }
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
            "{} Cloned '{}' ({} commits + {} index artifacts{}, head t={})",
            "✓".green(),
            local_id,
            total_commits,
            index_artifacts_stored,
            erased_note(erased_commits),
            head_t
        );
    } else {
        println!(
            "{} Cloned '{}' ({} commits{}, head t={})",
            "✓".green(),
            local_id,
            total_commits,
            erased_note(erased_commits),
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

/// What an origin clone transfers, and whether it designates the origin an
/// erasure authority.
#[derive(Clone, Copy, Debug, Default)]
pub struct OriginCloneOptions {
    pub no_indexes: bool,
    pub no_txns: bool,
    pub erasure_authority: bool,
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
    options: OriginCloneOptions,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let OriginCloneOptions {
        no_indexes,
        no_txns,
        erasure_authority,
    } = options;
    let ledger_id = context::to_ledger_id(ledger)?;
    let local_id = alias.map_or_else(|| Ok(ledger_id.clone()), context::to_ledger_id)?;

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
    let fluree = context::build_fluree(dirs).await?;
    let storage = fluree
        .backend()
        .admin_storage_cloned()
        .ok_or_else(|| CliError::Config("sync requires managed storage backend".into()))?;
    fluree
        .create_ledger(&local_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to create local ledger: {e}")))?;
    let namespace = local_namespace(&fluree, &local_id).await?;
    let _garbage_hold = hold_garbage(&fluree, &local_id).await?;

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
    let content_store = fluree.content_store(&namespace);
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
    let held = fluree
        .held_erasure_digests()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;
    let pack_request = pack_request.with_erasures(held.clone());
    let mut reception = OriginReception::new(&fluree, erasure_authority);
    let mut erased_commits = 0;
    let used_pack = match fetcher.fetch_pack_response(&ledger_id, &pack_request).await {
        Ok(Some(response)) => {
            let mut body_stream = response.bytes_stream();
            match peek_pack_header(&mut body_stream).await {
                Ok((header, buf_tail)) => {
                    let wants_indexes = pack_request.include_indexes;
                    let is_large = header.estimated_total_bytes > LARGE_TRANSFER_THRESHOLD;

                    let ingest_result = if wants_indexes && is_large {
                        if confirm_large_transfer(header.estimated_total_bytes) {
                            ingest_pack_stream_with_header_into(
                                &header,
                                buf_tail,
                                &mut body_stream,
                                &storage,
                                &namespace,
                                Some(&mut reception),
                            )
                            .await
                        } else {
                            drop(body_stream);
                            eprintln!("  Skipping index transfer, cloning commits only...");
                            let commits_only = if no_txns {
                                PackRequest::commits_no_txns(vec![head_cid.clone()], vec![])
                            } else {
                                PackRequest::commits(vec![head_cid.clone()], vec![])
                            }
                            .with_erasures(held.clone());
                            match fetcher.fetch_pack_response(&ledger_id, &commits_only).await {
                                Ok(Some(resp2)) => {
                                    ingest_pack_stream_into(
                                        resp2,
                                        &storage,
                                        &namespace,
                                        Some(&mut reception),
                                    )
                                    .await
                                }
                                Ok(None) => {
                                    Err(fluree_db_nameservice_sync::SyncError::PackNotSupported)
                                }
                                Err(e) => Err(e),
                            }
                        }
                    } else {
                        ingest_pack_stream_with_header_into(
                            &header,
                            buf_tail,
                            &mut body_stream,
                            &storage,
                            &namespace,
                            Some(&mut reception),
                        )
                        .await
                    };

                    match ingest_result {
                        Ok(result) => {
                            erased_commits = reception.accept(local_id.as_str(), &head_cid).await?;
                            commits_fetched = result.commits_stored;
                            index_artifacts_fetched = result.index_artifacts_stored;
                            let objects = result.commits_stored
                                + result.txn_blobs_stored
                                + result.index_artifacts_stored;
                            eprint!("  fetched {objects} object(s) via pack\r");
                            true
                        }
                        Err(e) => {
                            if reception.refused {
                                return Err(CliError::Config(e.to_string()));
                            }
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
        commits_fetched += walk_origin(
            &fluree,
            &fetcher,
            (ledger_id.as_str(), local_id.as_str()),
            &head_cid,
            (&storage, &namespace),
            no_txns,
            &mut reception,
        )
        .await?;
        // Before the head is set: its erasures read for the history they
        // erase, and the journal arrives with them.
        erased_commits = reception.accept(local_id.as_str(), &head_cid).await?;
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
            if !fluree
                .set_index_head(&handle, remote_index_id, ns_record.index_t)
                .await
                .map_err(|e| CliError::Config(format!("clone failed (set index head): {e}")))?
            {
                index_not_set_note();
            }
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
        let fence = fluree
            .nameservice()
            .lookup(&local_id)
            .await
            .map_err(|e| CliError::Config(format!("clone failed (lookup): {e}")))?
            .and_then(|record| record.fence);
        match fluree
            .publisher()
            .map_err(|e| CliError::Config(e.to_string()))?
            .push_config_fenced(&local_id, fence, current.as_ref(), &new_config)
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
            "{} Cloned '{}' ({} commit(s) + {} index artifact(s){}, head t={})",
            "✓".green(),
            local_id,
            commits_fetched,
            index_artifacts_fetched,
            erased_note(erased_commits),
            head_t
        );
    } else {
        println!(
            "{} Cloned '{}' ({} commit(s){}, head t={})",
            "✓".green(),
            local_id,
            commits_fetched,
            erased_note(erased_commits),
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
    erasure_authority: bool,
    dirs: &FlureeDir,
) -> CliResult<fluree_db_api::Fluree> {
    let fluree = context::build_fluree(dirs).await?;
    let storage = fluree
        .backend()
        .admin_storage_cloned()
        .ok_or_else(|| CliError::Config("sync requires managed storage backend".into()))?;

    let ns_record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await?
        .ok_or_else(|| CliError::NotFound(format!("local ledger '{ledger_id}' not found")))?;
    let namespace = ns_record.storage_namespace();
    let _garbage_hold = hold_garbage(&fluree, ledger_id).await?;

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
    let content_store = fluree.content_store(&namespace);
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
        // Its journal may have changed without a commit: a pack of none.
        let held_here = local_ref.id.as_ref() == Some(&remote_head_cid)
            || fluree
                .ledger_holds_commit(ledger_id, &remote_head_cid)
                .await
                .unwrap_or(false);
        let held = fluree
            .held_erasure_digests()
            .await
            .map_err(|e| CliError::Config(e.to_string()))?;
        let request =
            PackRequest::commits(vec![remote_head_cid.clone()], vec![remote_head_cid.clone()])
                .with_erasures(held);
        let response = match held_here {
            true => fetcher
                .fetch_pack_response(ledger_id, &request)
                .await
                .map_err(|e| CliError::Config(format!("pull failed (journal since): {e}")))?,
            false => None,
        };
        if let Some(response) = response {
            let mut reception = OriginReception::new(&fluree, erasure_authority);
            ingest_pack_stream_into(response, &storage, &namespace, Some(&mut reception))
                .await
                .map_err(|e| CliError::Config(format!("pull failed (journal since): {e}")))?;
            reception.accept(ledger_id, &remote_head_cid).await?;
        }
        println!("{} '{}' is already up to date", "✓".green(), ledger_id);
        return Ok(fluree);
    }

    println!(
        "Pulling '{}' via origins (local t={}, remote t={})...",
        ledger_id.cyan(),
        local_ref.t,
        remote_t
    );

    // Try pack protocol first (single round-trip).
    let held = fluree
        .held_erasure_digests()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;
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
    }
    .with_erasures(held.clone());
    let mut reception = OriginReception::new(&fluree, erasure_authority);
    match fetcher.fetch_pack_response(ledger_id, &pack_request).await {
        Ok(Some(response)) => {
            let mut body_stream = response.bytes_stream();
            match peek_pack_header(&mut body_stream).await {
                Ok((header, buf_tail)) => {
                    let wants_indexes = pack_request.include_indexes;
                    let is_large = header.estimated_total_bytes > LARGE_TRANSFER_THRESHOLD;

                    let ingest_result = if wants_indexes && is_large {
                        if confirm_large_transfer(header.estimated_total_bytes) {
                            ingest_pack_stream_with_header_into(
                                &header,
                                buf_tail,
                                &mut body_stream,
                                &storage,
                                &namespace,
                                Some(&mut reception),
                            )
                            .await
                        } else {
                            drop(body_stream);
                            eprintln!("  Skipping index transfer, pulling commits only...");
                            let have: Vec<fluree_db_core::ContentId> =
                                local_ref.id.clone().into_iter().collect();
                            let commits_only =
                                PackRequest::commits(vec![remote_head_cid.clone()], have)
                                    .with_erasures(held.clone());
                            match fetcher.fetch_pack_response(ledger_id, &commits_only).await {
                                Ok(Some(resp2)) => {
                                    ingest_pack_stream_into(
                                        resp2,
                                        &storage,
                                        &namespace,
                                        Some(&mut reception),
                                    )
                                    .await
                                }
                                Ok(None) => {
                                    Err(fluree_db_nameservice_sync::SyncError::PackNotSupported)
                                }
                                Err(e) => Err(e),
                            }
                        }
                    } else {
                        ingest_pack_stream_with_header_into(
                            &header,
                            buf_tail,
                            &mut body_stream,
                            &storage,
                            &namespace,
                            Some(&mut reception),
                        )
                        .await
                    };

                    match ingest_result {
                        Ok(result) => {
                            reception.accept(ledger_id, &remote_head_cid).await?;
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
                                    if !fluree
                                        .set_index_head(&handle, remote_index_id, remote_ns.index_t)
                                        .await
                                        .map_err(|e| {
                                            CliError::Config(format!(
                                                "pull failed (set index head): {e}"
                                            ))
                                        })?
                                    {
                                        index_not_set_note();
                                    }
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
                            return Ok(fluree);
                        }
                        Err(e) => {
                            if reception.refused {
                                return Err(CliError::Config(e.to_string()));
                            }
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
        id: fluree_db_core::ContentId,
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
        // An erased commit reads here whether or not this ledger holds it.
        let held = content_store.has(&cid).await.unwrap_or(false)
            && fluree
                .ledger_holds_commit(ledger_id, &cid)
                .await
                .map_err(|e| CliError::Config(format!("pull failed (read local commit): {e}")))?;
        if held {
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

        if !commit_blob_matches(&cid, &commit_bytes) {
            return Err(CliError::Config(format!(
                "pull failed: commit {cid} does not match its bytes"
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
            id: cid.clone(),
            bytes: commit_bytes,
            txn: envelope.txn,
        });
    }

    if fetched.is_empty() {
        println!("{} '{}' is already up to date", "✓".green(), ledger_id);
        return Ok(fluree);
    }

    // Reverse to oldest→newest, store blobs.
    fetched.reverse();
    for fc in &fetched {
        storage
            .content_write_bytes_with_hash(
                ContentKind::Commit,
                &namespace,
                &fc.id.address_digest(),
                &fc.bytes,
            )
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

    Ok(fluree)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_api::{Base64Bytes, ExportCommitsResponse};
    use fluree_db_core::{Commit, ContentId, ContentKind};

    /// Commit bytes at `t`, distinct per `tag`.
    fn commit(t: i64, tag: &str) -> Base64Bytes {
        let parent = ContentId::new(ContentKind::Commit, tag.as_bytes());
        let commit = Commit::new(t, Vec::new()).with_parent(parent);
        let written = fluree_db_core::commit::codec::write_commit(&commit, false, None).unwrap();
        Base64Bytes(written.bytes)
    }

    fn bytes_of(commits: PulledCommits) -> Vec<Base64Bytes> {
        commits.into_iter().map(|(_, bytes)| bytes).collect()
    }

    fn page(
        commits: Vec<Base64Bytes>,
        merged_commits: Vec<Base64Bytes>,
        lineage: bool,
    ) -> ExportCommitsResponse {
        ExportCommitsResponse {
            ledger: "mydb:main".to_string(),
            head_commit_id: ContentId::new(ContentKind::Commit, b"head"),
            head_t: 0,
            count: commits.len(),
            commits,
            blobs: Default::default(),
            missing_blobs: Vec::new(),
            newest_t: 0,
            oldest_t: 0,
            next_cursor_id: None,
            effective_limit: 100,
            lineage,
            merged_commits,
            commit_ids: Vec::new(),
            merged_commit_ids: Vec::new(),
        }
    }

    #[test]
    fn pull_import_set_orders_lineage_pages_oldest_first() {
        let (c2, c3, c4) = (commit(2, "c2"), commit(3, "c3"), commit(4, "c4"));
        let (a, b) = (commit(7, "a"), commit(8, "b"));
        // Fetched newest first. `b` was brought in by merges on both pages.
        let pages = [
            page(vec![c4.clone(), c3.clone()], vec![b.clone()], true),
            page(vec![c2.clone()], vec![a.clone(), b.clone()], true),
        ];

        let (to_import, merged) = pull_import_set(&pages, 1).unwrap();
        assert_eq!(bytes_of(to_import), [c2, c3, c4]);
        // The older page's commits come first, and `b` is kept once.
        assert_eq!(bytes_of(merged), [a, b]);
    }

    #[test]
    fn pull_import_set_drops_known_commits_from_the_default_export() {
        let (c1, c2, c3) = (commit(1, "c1"), commit(2, "c2"), commit(3, "c3"));
        let pages = [page(vec![c3.clone(), c2.clone(), c1], Vec::new(), false)];

        let (to_import, merged) = pull_import_set(&pages, 1).unwrap();
        assert_eq!(bytes_of(to_import), [c2, c3]);
        assert!(merged.is_empty());
    }

    /// An origin's pack carries its journal, which the copy takes before its
    /// head moves: history the origin removed is refused there too. An
    /// erasure frame is refused, and remembered as refused.
    #[tokio::test]
    async fn an_origin_reception_takes_the_journal_and_refuses_erasures() {
        use fluree_db_api::retention::{FactSelector, RemovalRequest};
        use fluree_db_api::{ApiError, FlureeBuilder};
        use fluree_db_core::pack::{decode_frame, read_stream_preamble, PackFrame};
        use serde_json::json;

        let ctx = json!({"ex": "http://example.org/"});
        let there = tempfile::tempdir().unwrap();
        let origin = FlureeBuilder::file(there.path().to_string_lossy().to_string())
            .without_indexing()
            .build()
            .unwrap();
        origin.create_ledger("t/origin").await.unwrap();
        let id = "t/origin:main";
        let t1 = origin
            .graph(id)
            .transact()
            .insert(&json!({"@context": ctx, "@id": "ex:alice", "ex:email": "a@old"}))
            .commit()
            .await
            .unwrap()
            .receipt
            .t;
        origin
            .graph(id)
            .transact()
            .upsert(&json!({"@context": ctx, "@id": "ex:alice", "ex:email": "a@new"}))
            .commit()
            .await
            .unwrap();
        let old = RemovalRequest {
            matching: vec![FactSelector {
                subject: "http://example.org/alice".into(),
                predicate: "http://example.org/email".into(),
                graph: None,
                object: Some(json!("a@old")),
            }],
            ..Default::default()
        };
        origin.remove_occurrences(id, old).await.unwrap();
        let record = origin.nameservice().lookup(id).await.unwrap().unwrap();
        let head = record.commit_head_id.clone().unwrap();
        let handle = origin.ledger_cached(id).await.unwrap();
        let request = PackRequest::commits(vec![head.clone()], vec![]).with_erasures(Vec::new());
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        fluree_db_api::pack::stream_pack(&origin, &handle, &request, tx).await;
        let mut pack = Vec::new();
        while let Some(chunk) = rx.recv().await {
            pack.extend(chunk.unwrap());
        }

        let here = tempfile::tempdir().unwrap();
        let copy = FlureeBuilder::file(here.path().to_string_lossy().to_string())
            .without_indexing()
            .without_ledger_caching()
            .build()
            .unwrap();
        copy.create_ledger("t/origin-copy").await.unwrap();
        let copy_id = "t/origin-copy:main";
        let namespace = local_namespace(&copy, copy_id).await.unwrap();
        let storage = copy.backend().admin_storage_cloned().unwrap();
        let mut reception = OriginReception::new(&copy, false);
        let mut pos = read_stream_preamble(&pack).unwrap();
        loop {
            let (frame, used) =
                decode_frame(&pack[pos..], fluree_db_core::pack::DEFAULT_MAX_PAYLOAD).unwrap();
            pos += used;
            match frame {
                PackFrame::End => break,
                PackFrame::Header(_) => {}
                PackFrame::Data { cid, payload } => {
                    fluree_db_nameservice_sync::ingest_pack_frame(
                        &cid, &payload, &storage, &namespace,
                    )
                    .await
                    .unwrap();
                }
                frame => reception.frame(frame).await.unwrap(),
            }
        }
        reception.accept(copy_id, &head).await.unwrap();
        let handle = copy.ledger_cached(copy_id).await.unwrap();
        copy.set_commit_head(&handle, &head, record.commit_t)
            .await
            .unwrap();
        let past = copy
            .query_connection(&json!({
                "@context": ctx,
                "from": format!("{copy_id}@t:{t1}"),
                "select": "?o",
                "where": {"@id": "ex:alice", "ex:email": "?o"}
            }))
            .await;
        assert!(
            matches!(past, Err(ApiError::HistoryUnavailable(_))),
            "{past:?}"
        );

        assert!(reception
            .frame(PackFrame::Erasure(json!({})))
            .await
            .is_err());
        assert!(reception.refused);
    }

    /// An origin answering object requests as a server does: an erased
    /// commit with its erasure, which it records answering.
    struct TestOrigin<'a> {
        fluree: &'a fluree_db_api::Fluree,
        ledger_id: &'a str,
        erased: std::sync::Mutex<Vec<ContentId>>,
    }

    #[async_trait::async_trait]
    impl ObjectSource for TestOrigin<'_> {
        async fn object(
            &self,
            cid: &ContentId,
            _ledger_id: &str,
        ) -> fluree_db_nameservice_sync::Result<Vec<u8>> {
            use fluree_db_api::retention::erasure::ErasedObject;
            use fluree_db_nameservice_sync::SyncError;
            match self
                .fluree
                .erased_object(self.ledger_id, cid)
                .await
                .unwrap()
            {
                Some(ErasedObject::Chain(frames)) => {
                    self.erased.lock().unwrap().push(cid.clone());
                    Err(SyncError::Erased {
                        cid: cid.to_string(),
                        frames,
                    })
                }
                Some(ErasedObject::Withheld) => Err(SyncError::NotFound(cid.to_string())),
                None => {
                    let namespace = local_namespace(self.fluree, self.ledger_id).await.unwrap();
                    match self.fluree.content_store(&namespace).get(cid).await {
                        Ok(bytes) => Ok(bytes.into_vec()),
                        Err(_) => Err(SyncError::NotFound(cid.to_string())),
                    }
                }
            }
        }
    }

    /// `walk_origin` from `source` into `local_id`, taking erasures with
    /// `authority` or not.
    async fn walk_into<'a>(
        copy: &'a fluree_db_api::Fluree,
        source: &TestOrigin<'_>,
        storage: &Arc<dyn fluree_db_core::storage::Storage>,
        head: &ContentId,
        local_id: &str,
        authority: bool,
    ) -> (CliResult<usize>, OriginReception<'a>) {
        let mut reception = OriginReception::new(copy, authority);
        let namespace = local_namespace(copy, local_id).await.unwrap();
        let walked = walk_origin(
            copy,
            source,
            (source.ledger_id, local_id),
            head,
            (storage, &namespace),
            false,
            &mut reception,
        )
        .await;
        (walked, reception)
    }

    /// An origin's erased commit, walked object by object as `fluree clone
    /// --origin` walks it, is refused unless the clone designates the origin
    /// an erasure authority; designated, the clone reads as the origin does.
    /// A second clone on the same storage, which reads the erased commit
    /// already, fetches its erasure all the same: that makes it a holder,
    /// which later erasures reach, and brings the journal.
    #[tokio::test]
    async fn an_origin_walk_takes_erased_commits_only_from_an_authority() {
        use fluree_db_api::retention::erasure::{ErasureConfig, ErasureRequest, Verifier};
        use fluree_db_api::retention::FactSelector;
        use fluree_db_api::{FlureeBuilder, ReindexOptions};
        use serde_json::json;

        let ctx = json!({"ex": "http://example.org/"});
        let there = tempfile::tempdir().unwrap();
        let origin = FlureeBuilder::file(there.path().to_string_lossy().to_string())
            .without_indexing()
            .with_erasure(ErasureConfig {
                prover: Arc::new(fluree_db_erasure::testing::NativeProver::default()),
                verifier: Arc::new(Verifier::accepting_dev_receipts()),
                retry_ms: 10,
            })
            .build()
            .unwrap();
        origin.create_ledger("t/walk").await.unwrap();
        let id = "t/walk:main";
        for doc in [
            json!({"@context": ctx, "@graph": [
                {"@id": "ex:alice", "ex:email": "a@secret"},
                {"@id": "ex:bob", "ex:email": "b@kept"}
            ]}),
            json!({"@context": ctx, "@id": "ex:carol", "ex:email": "c@later"}),
        ] {
            origin
                .graph(id)
                .transact()
                .insert(&doc)
                .commit()
                .await
                .unwrap();
        }
        let request = ErasureRequest {
            matching: vec![FactSelector {
                subject: "http://example.org/alice".into(),
                predicate: "http://example.org/email".into(),
                graph: None,
                object: Some(json!("a@secret")),
            }],
            ..Default::default()
        };
        origin.erase_occurrences(id, request, None).await.unwrap();
        origin.reindex(id, ReindexOptions::default()).await.unwrap();
        assert_eq!(origin.run_erasure_pass(id).await.unwrap().published, 1);
        let record = origin.nameservice().lookup(id).await.unwrap().unwrap();
        let head = record.commit_head_id.clone().unwrap();
        let source = TestOrigin {
            fluree: &origin,
            ledger_id: id,
            erased: Default::default(),
        };

        let here = tempfile::tempdir().unwrap();
        let copy = FlureeBuilder::file(here.path().to_string_lossy().to_string())
            .without_indexing()
            .without_ledger_caching()
            .with_erasure_verifier(Arc::new(Verifier::accepting_dev_receipts()))
            .build()
            .unwrap();
        let storage = copy.backend().admin_storage_cloned().unwrap();
        for local in ["t/walk-refused", "t/walk-copy", "t/walk-again"] {
            copy.create_ledger(local).await.unwrap();
        }
        let walk = |local_id: &'static str, authority: bool| {
            walk_into(&copy, &source, &storage, &head, local_id, authority)
        };
        let (walked, refusing) = walk("t/walk-refused:main", false).await;
        assert!(walked.is_err());
        assert!(refusing.refused);
        source.erased.lock().unwrap().clear();

        let (walked, mut reception) = walk("t/walk-copy:main", true).await;
        walked.unwrap();
        // The assertion, and the removal's retraction.
        let erased = std::mem::take(&mut *source.erased.lock().unwrap());
        assert_eq!(erased.len(), 2);
        let copy_id = "t/walk-copy:main";
        assert_eq!(reception.accept(copy_id, &head).await.unwrap(), 2);
        let handle = copy.ledger_cached(copy_id).await.unwrap();
        copy.set_commit_head(&handle, &head, record.commit_t)
            .await
            .unwrap();
        let emails = copy
            .query_connection(&json!({
                "@context": ctx,
                "from": copy_id,
                "select": "?o",
                "where": {"@id": "?s", "ex:email": "?o"}
            }))
            .await
            .unwrap();
        let ledger = copy.ledger(copy_id).await.unwrap();
        let mut emails: Vec<String> = emails
            .to_jsonld(&ledger.snapshot)
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        emails.sort();
        assert_eq!(emails, ["b@kept", "c@later"]);

        // Every folder of this storage reads the erased commits now.
        let again = "t/walk-again:main";
        let namespace = local_namespace(&copy, again).await.unwrap();
        for cid in &erased {
            assert!(copy.content_store(&namespace).has(cid).await.unwrap());
            assert!(!copy.ledger_holds_commit(again, cid).await.unwrap());
        }
        let (walked, mut reception) = walk(again, true).await;
        walked.unwrap();
        assert_eq!(*source.erased.lock().unwrap(), erased);
        assert_eq!(reception.accept(again, &head).await.unwrap(), 2);
        for cid in &erased {
            assert!(copy.ledger_holds_commit(again, cid).await.unwrap());
        }
        let status = copy.retention_status(again).await.unwrap();
        assert!(
            status
                .registration
                .is_some_and(|r| !r.journal.removals.is_empty()),
            "the journal arrives with the erasures"
        );
    }

    /// The files under `root` holding `needle`.
    fn files_holding(root: &std::path::Path, needle: &str) -> Vec<std::path::PathBuf> {
        let mut found = Vec::new();
        let mut next = vec![root.to_path_buf()];
        while let Some(dir) = next.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    next.push(path);
                } else if std::fs::read(&path)
                    .unwrap()
                    .windows(needle.len())
                    .any(|w| w == needle.as_bytes())
                {
                    found.push(path);
                }
            }
        }
        found
    }

    /// A copy holding a commit an erasure it received erases deletes the
    /// original from its storage before the pull returns, without waiting as
    /// a server does for readers of the older indexes; a second pull finds
    /// nothing left.
    #[tokio::test]
    async fn a_pull_deletes_what_a_received_erasure_erases() {
        use fluree_db_api::retention::erasure::{
            ErasureConfig, ErasureRequest, ErasureStage, Verifier,
        };
        use fluree_db_api::retention::FactSelector;
        use fluree_db_api::{FlureeBuilder, ReindexOptions};
        use serde_json::json;

        let there = tempfile::tempdir().unwrap();
        let origin = FlureeBuilder::file(there.path().to_string_lossy().to_string())
            .without_indexing()
            .with_erasure(ErasureConfig {
                prover: Arc::new(fluree_db_erasure::testing::NativeProver::default()),
                verifier: Arc::new(Verifier::accepting_dev_receipts()),
                retry_ms: 10,
            })
            .build()
            .unwrap();
        origin.create_ledger("t/purge").await.unwrap();
        let id = "t/purge:main";
        origin
            .graph(id)
            .transact()
            .insert(
                &json!({"@context": {"ex": "http://example.org/"}, "@graph": [
                    {"@id": "ex:alice", "ex:email": "a@secret"},
                    {"@id": "ex:bob", "ex:email": "b@kept"}
                ]}),
            )
            .commit()
            .await
            .unwrap();
        let mut before = Vec::new();
        origin.archive_ledger(id, false, &mut before).await.unwrap();
        let request = ErasureRequest {
            matching: vec![FactSelector {
                subject: "http://example.org/alice".into(),
                predicate: "http://example.org/email".into(),
                graph: None,
                object: Some(json!("a@secret")),
            }],
            ..Default::default()
        };
        origin.erase_occurrences(id, request, None).await.unwrap();
        origin.reindex(id, ReindexOptions::default()).await.unwrap();
        assert_eq!(origin.run_erasure_pass(id).await.unwrap().published, 1);
        let mut after = Vec::new();
        origin.archive_ledger(id, true, &mut after).await.unwrap();

        let here = tempfile::tempdir().unwrap();
        let copy = FlureeBuilder::file(here.path().to_string_lossy().to_string())
            .without_indexing()
            .without_ledger_caching()
            .with_erasure_verifier(Arc::new(Verifier::accepting_dev_receipts()))
            .build()
            .unwrap();
        let held = copy
            .restore_ledger("t/purge-copy", &mut before.as_slice())
            .await
            .unwrap()
            .ledger_id;
        copy.reindex(&held, ReindexOptions::default())
            .await
            .unwrap();
        copy.restore_ledger("t/purge-again", &mut after.as_slice())
            .await
            .unwrap();
        assert!(!files_holding(here.path(), "a@secret").is_empty());

        let purge = purge_received(&copy, &held).await;
        assert_eq!(purge, Purge::Done(1));
        assert_eq!(
            files_holding(here.path(), "a@secret"),
            Vec::<std::path::PathBuf>::new()
        );
        let jobs = copy
            .retention_status(&held)
            .await
            .unwrap()
            .registration
            .unwrap()
            .erasures;
        assert_eq!(jobs[0].stage, ErasureStage::Purged);
        assert_eq!(purge_received(&copy, &held).await, Purge::Nothing);
    }
}
