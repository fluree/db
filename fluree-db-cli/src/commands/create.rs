use crate::config;
use crate::context;
use crate::detect;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Import tuning options passed from global + create-specific CLI flags.
pub struct ImportOpts {
    pub memory_budget_mb: usize,
    pub parallelism: usize,
    pub chunk_size_mb: usize,
    pub leaflet_rows: usize,
    pub leaflets_per_leaf: usize,
}

/// `fluree create <ledger> --remote <name>` — create an empty ledger on the
/// remote server. Only the empty-create case is supported; bulk imports
/// (`--from`, `--memory`) require local data ingestion and are dispatched
/// before this is reached. Active-ledger pointer is **not** touched —
/// remote storage is separate from local.
pub async fn run_remote(ledger: &str, remote_name: &str, dirs: &FlureeDir) -> CliResult<()> {
    let client = context::build_remote_client(remote_name, dirs).await?;
    let ledger_id = context::to_ledger_id(ledger);
    let response = client.create_ledger(&ledger_id).await.map_err(|e| {
        CliError::Remote(format!(
            "failed to create '{ledger}' on remote '{remote_name}': {e}"
        ))
    })?;
    context::persist_refreshed_tokens(&client, remote_name, dirs).await;

    let resolved = response
        .get("ledger")
        .and_then(|v| v.as_str())
        .unwrap_or(&ledger_id);
    println!("Created ledger '{resolved}' on remote '{remote_name}'");
    Ok(())
}

/// `fluree create <ledger> --remote <name> --from <archive>.flpack` — restore a
/// ledger onto a remote server from a local `.flpack` archive. The remote
/// creates the ledger under `ledger` (which may differ from the archived
/// ledger's name). No local state is written — remote storage is separate.
///
/// Picks the upload path from the server's advertised capabilities: a direct
/// streamed `POST /import` by default, or the negotiated presigned-upload flow
/// when the server is size-capped and the archive exceeds its direct limit.
pub async fn run_remote_flpack_import(
    ledger: &str,
    remote_name: &str,
    path: &Path,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let meta = std::fs::metadata(path)
        .map_err(|e| CliError::Input(format!("failed to stat {}: {e}", path.display())))?;
    let size = meta.len();

    let client = context::build_remote_client(remote_name, dirs).await?;
    let ledger_id = context::to_ledger_id(ledger);

    let cap = client.fetch_import_capability().await;
    let result = if cap.needs_negotiated_upload(size) {
        run_remote_flpack_negotiated(&client, ledger, &ledger_id, remote_name, path, size).await?
    } else {
        run_remote_flpack_direct(&client, ledger, &ledger_id, remote_name, path, size).await?
    };
    context::persist_refreshed_tokens(&client, remote_name, dirs).await;

    print_remote_import_summary(remote_name, &ledger_id, &result);
    Ok(())
}

/// Direct path: stream the archive straight to `POST /import/<ledger>`.
async fn run_remote_flpack_direct(
    client: &crate::remote_client::RemoteLedgerClient,
    ledger: &str,
    ledger_id: &str,
    remote_name: &str,
    path: &Path,
    size: u64,
) -> CliResult<serde_json::Value> {
    use colored::Colorize;
    eprintln!(
        "Importing '{}' to remote '{}' from {} ({})...",
        ledger.cyan(),
        remote_name,
        path.display(),
        format_human_bytes(size),
    );
    client.import_ledger(ledger_id, path).await.map_err(|e| {
        CliError::Remote(format!(
            "failed to import '{ledger}' to remote '{remote_name}': {e}"
        ))
    })
}

/// Negotiated path: mint an upload slot, PUT the archive out-of-band, notify
/// `complete`, then poll status to a terminal state.
async fn run_remote_flpack_negotiated(
    client: &crate::remote_client::RemoteLedgerClient,
    ledger: &str,
    ledger_id: &str,
    remote_name: &str,
    path: &Path,
    size: u64,
) -> CliResult<serde_json::Value> {
    use colored::Colorize;
    use std::collections::HashMap;

    eprintln!(
        "Uploading '{}' to remote '{}' via presigned upload ({})...",
        ledger.cyan(),
        remote_name,
        format_human_bytes(size),
    );

    // 1. Mint an upload slot.
    let mint = client
        .mint_import_upload(ledger_id, Some(size))
        .await
        .map_err(|e| CliError::Remote(format!("failed to start upload to '{remote_name}': {e}")))?;
    let import_id = mint["import_id"]
        .as_str()
        .ok_or_else(|| CliError::Remote("mint response missing import_id".to_string()))?
        .to_string();
    let upload = &mint["upload"];
    let url = upload["url"]
        .as_str()
        .ok_or_else(|| CliError::Remote("mint response missing upload.url".to_string()))?;
    let headers: HashMap<String, String> = upload["headers"]
        .as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    // 2. Upload the archive directly to the minted URL.
    client
        .put_upload_file(url, &headers, path)
        .await
        .map_err(|e| CliError::Remote(format!("failed to upload archive: {e}")))?;

    // 3. Notify the server to begin the restore.
    client
        .complete_import_upload(&import_id)
        .await
        .map_err(|e| CliError::Remote(format!("failed to start remote import: {e}")))?;

    eprintln!("Upload complete; restoring on '{remote_name}'...");

    // 4. Poll status to a terminal state.
    poll_remote_import(client, &import_id).await
}

/// Poll `import-upload/{id}` until the import reaches a terminal state.
async fn poll_remote_import(
    client: &crate::remote_client::RemoteLedgerClient,
    import_id: &str,
) -> CliResult<serde_json::Value> {
    use std::time::{Duration, Instant};

    let deadline = Instant::now() + Duration::from_secs(1800);
    let mut backoff = Duration::from_millis(500);
    loop {
        let status = client
            .import_upload_status(import_id)
            .await
            .map_err(|e| CliError::Remote(format!("failed to poll import status: {e}")))?;
        match status["status"].as_str() {
            Some("succeeded") => return Ok(status["result"].clone()),
            Some("failed") => {
                let err = status["error"].as_str().unwrap_or("unknown error");
                return Err(CliError::Remote(format!("remote import failed: {err}")));
            }
            Some("running" | "awaiting-upload") => {}
            other => {
                return Err(CliError::Remote(format!(
                    "unexpected remote import status: {other:?}"
                )));
            }
        }
        if Instant::now() >= deadline {
            return Err(CliError::Remote(
                "timed out waiting for remote import to complete".to_string(),
            ));
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(3));
    }
}

/// Print the shared success summary for a remote import (both paths).
fn print_remote_import_summary(remote_name: &str, ledger_id: &str, result: &serde_json::Value) {
    use colored::Colorize;
    let resolved = result["ledger_id"].as_str().unwrap_or(ledger_id);
    let commits = result["commits"].as_u64().unwrap_or(0);
    let txn_blobs = result["txn_blobs"].as_u64().unwrap_or(0);
    let index_artifacts = result["index_artifacts"].as_u64().unwrap_or(0);
    println!(
        "{} Imported '{}' to remote '{}' — {} commits, {} txn blobs, {} index artifacts",
        "✓".green(),
        resolved,
        remote_name,
        commits,
        txn_blobs,
        index_artifacts,
    );
}

pub async fn run(
    ledger: &str,
    from: Option<&Path>,
    dirs: &FlureeDir,
    verbose: bool,
    quiet: bool,
    import_opts: &ImportOpts,
) -> CliResult<()> {
    // Refuse if this alias is already tracked (mutual exclusion)
    let store = config::TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    if store.get_tracked(ledger).is_some() {
        return Err(CliError::Usage(format!(
            "alias '{ledger}' is already used by a tracked ledger.\n  \
             Run `fluree track remove {ledger}` first, or choose a different name."
        )));
    }

    let fluree = context::build_fluree(dirs)?;

    match from {
        Some(path) if is_flpack_path(path) => {
            run_flpack_import(&fluree, ledger, path, dirs).await?;
        }
        Some(path) if path.is_dir() => {
            // Validate directory format (catches mixed formats & empty dirs).
            fluree_db_api::scan_directory_format(path)?;
            run_bulk_import(
                &fluree,
                ledger,
                path,
                dirs.data_dir(),
                verbose,
                quiet,
                import_opts,
            )
            .await?;
        }
        Some(path) if is_import_path(path)? => {
            // Bulk import: Turtle or JSON-LD file (any size).
            // The import pipeline handles both small (single-chunk) and large
            // (auto-split) files via resolve_chunk_source.
            run_bulk_import(
                &fluree,
                ledger,
                path,
                dirs.data_dir(),
                verbose,
                quiet,
                import_opts,
            )
            .await?;
        }
        Some(path) => {
            // Non-Turtle single file: detect format.
            let content = std::fs::read_to_string(path)
                .map_err(|e| CliError::Input(format!("failed to read {}: {e}", path.display())))?;
            let format = detect::detect_data_format(Some(path), &content, None)?;

            match format {
                detect::DataFormat::Turtle => {
                    // Safety redirect: if a .ttl file reaches this branch
                    // (e.g., due to path/extension edge cases), always route
                    // through the import pipeline to avoid novelty limits.
                    run_bulk_import(
                        &fluree,
                        ledger,
                        path,
                        dirs.data_dir(),
                        verbose,
                        quiet,
                        import_opts,
                    )
                    .await?;
                }
                detect::DataFormat::JsonLd => {
                    // JSON-LD: create ledger + transact
                    fluree.create_ledger(ledger).await?;

                    let json: serde_json::Value = serde_json::from_str(&content)?;
                    let result = fluree
                        .graph(ledger)
                        .transact()
                        .insert(&json)
                        .commit()
                        .await?;

                    config::write_active_ledger(dirs.data_dir(), ledger)?;
                    println!(
                        "Created ledger '{}' ({} flakes, t={})",
                        ledger, result.receipt.flake_count, result.receipt.t
                    );
                }
            }
        }
        None => {
            // Create empty ledger
            fluree.create_ledger(ledger).await?;
            config::write_active_ledger(dirs.data_dir(), ledger)?;
            println!("Created ledger '{ledger}'");
        }
    }

    Ok(())
}

/// Run the bulk import pipeline for a Turtle file or directory.
///
/// Prints effective import settings (memory budget, parallelism, chunk size,
/// run budget) to stderr so the user can cancel if the values look excessive.
/// Shows a live progress bar unless `quiet` is set.
async fn run_bulk_import(
    fluree: &fluree_db_api::Fluree,
    ledger: &str,
    path: &Path,
    fluree_dir: &Path,
    verbose: bool,
    quiet: bool,
    import_opts: &ImportOpts,
) -> CliResult<()> {
    use colored::Colorize;
    use fluree_db_api::ImportPhase;
    use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

    if verbose {
        println!("Importing from: {}", path.display());
    }

    let ledger_owned = ledger.to_string();

    let mut builder = fluree.create(ledger).import(path);
    if import_opts.parallelism > 0 {
        builder = builder.parallelism(import_opts.parallelism);
    }
    if import_opts.memory_budget_mb > 0 {
        builder = builder.memory_budget_mb(import_opts.memory_budget_mb);
    }
    if import_opts.chunk_size_mb > 0 {
        builder = builder.chunk_size_mb(import_opts.chunk_size_mb);
    }
    if import_opts.leaflet_rows != 25_000 {
        builder = builder.leaflet_rows(import_opts.leaflet_rows);
    }
    if import_opts.leaflets_per_leaf != 10 {
        builder = builder.leaflets_per_leaf(import_opts.leaflets_per_leaf);
    }
    let settings = builder.effective_import_settings();
    let mem_auto = import_opts.memory_budget_mb == 0;
    let par_auto = import_opts.parallelism == 0;
    if !quiet {
        eprintln!(
            "Import settings: memory budget {} MB{}, parallelism {}{}, chunk size {} MB",
            settings.memory_budget_mb,
            if mem_auto { " (auto)" } else { "" },
            settings.parallelism,
            if par_auto { " (auto)" } else { "" },
            settings.chunk_size_mb,
        );
        if mem_auto || par_auto {
            eprintln!("  Override with --memory-budget-mb and --parallelism");
        }
    }

    // ------------------------------------------------------------------------
    // Crash breadcrumb for customer support (survives SIGSEGV/OOM-kill).
    //
    // The CLI defaults to "no logs" for UX; when the process dies hard (e.g.
    // SIGSEGV under memory pressure), there is no Rust panic message.
    //
    // We write a small JSON "breadcrumb" file periodically during import so
    // users can attach it to bug reports. This is intentionally minimal and
    // low-frequency (<= 1 write/sec).
    // ------------------------------------------------------------------------
    let breadcrumb_path: Option<std::path::PathBuf> = {
        let crash_dir = fluree_dir.join("crash");
        if std::fs::create_dir_all(&crash_dir).is_ok() {
            let pid = std::process::id();
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let name = format!(
                "import_{}_{}_{}.json",
                sanitize_for_filename(ledger),
                ts,
                pid
            );
            let p = crash_dir.join(name);
            // Initial record (best-effort).
            let started_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let init = serde_json::json!({
                "kind": "bulk_import",
                "ledger": ledger_owned,
                "pid": pid,
                "started_epoch_ms": started_ms,
                "source_path": path.display().to_string(),
                "settings": {
                    "memory_budget_mb": settings.memory_budget_mb,
                    "parallelism": settings.parallelism,
                    "chunk_size_mb": settings.chunk_size_mb,
                    "max_inflight_chunks": settings.max_inflight_chunks,
                    "leaflet_rows": import_opts.leaflet_rows,
                    "leaflets_per_leaf": import_opts.leaflets_per_leaf
                },
                "status": "running",
                "last_phase": "starting"
            });
            let _ = std::fs::write(&p, serde_json::to_vec_pretty(&init).unwrap_or_default());
            Some(p)
        } else {
            None
        }
    };
    let breadcrumb_last_write: std::sync::Arc<std::sync::Mutex<std::time::Instant>> =
        std::sync::Arc::new(std::sync::Mutex::new(std::time::Instant::now()));

    // Two progress bars shown simultaneously: Committing and Indexing.
    // The active phase advances while the other stays at 0% or 100%.
    let multi = if quiet {
        MultiProgress::with_draw_target(ProgressDrawTarget::hidden())
    } else {
        MultiProgress::new()
    };

    let style =
        ProgressStyle::with_template("{prefix:12} {spinner:.dim} [{bar:25}] {percent:>3}%  {msg}")
            .unwrap()
            .tick_strings(&["|", "/", "-", "\\", " "])
            .progress_chars("=>-");

    let scan_bar = multi.add(ProgressBar::new(100));
    scan_bar.set_style(style.clone());
    scan_bar.set_prefix(format!("{}", "Reading".green().bold()));
    scan_bar.enable_steady_tick(std::time::Duration::from_millis(120));

    let commit_bar = multi.add(ProgressBar::new(100));
    commit_bar.set_style(style.clone());
    commit_bar.set_prefix(format!("{}", "Committing".green().bold()));
    commit_bar.enable_steady_tick(std::time::Duration::from_millis(120));

    let index_bar = multi.add(ProgressBar::new(100));
    index_bar.set_style(style);
    index_bar.set_prefix(format!("{}", "Indexing".green().bold()));
    index_bar.enable_steady_tick(std::time::Duration::from_millis(120));

    let sb = scan_bar.clone();
    let cb = commit_bar.clone();
    let ib = index_bar.clone();
    // Track when the commit phase actually starts (first Committing event),
    // so M flakes/s reflects commit throughput, not reading/parsing time.
    let commit_start: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    // Whether we've received any Scanning event (streaming mode). When true,
    // the scan bar manages its own lifecycle — the Committing handler must not
    // clear it prematurely.
    let is_streaming_scan = std::sync::atomic::AtomicBool::new(false);
    let breadcrumb_path_for_cb = breadcrumb_path.clone();
    let breadcrumb_last_write_for_cb = std::sync::Arc::clone(&breadcrumb_last_write);
    let breadcrumb_ledger_for_cb = ledger_owned.clone();
    builder = builder.on_progress(move |phase| {
        // Best-effort: update crash breadcrumb at most once per second.
        // Avoid heavy work in the callback when the progress bars are active.
        if let Some(ref p) = breadcrumb_path_for_cb {
            if let Ok(mut last) = breadcrumb_last_write_for_cb.lock() {
                if last.elapsed() >= Duration::from_secs(1) {
                    *last = std::time::Instant::now();
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let phase_str = format!("{phase:?}");
                    let doc = serde_json::json!({
                        "kind": "bulk_import",
                        "ledger": breadcrumb_ledger_for_cb,
                        "pid": std::process::id(),
                        "updated_epoch_ms": now_ms,
                        "status": "running",
                        "last_phase": phase_str
                    });
                    let _ = std::fs::write(p, serde_json::to_vec_pretty(&doc).unwrap_or_default());
                }
            }
        }
        // Continue with normal progress handling below.
        match phase {
            ImportPhase::Parsing {
                chunk,
                total,
                chunk_bytes,
            } => {
                let mb = chunk_bytes as f64 / (1024.0 * 1024.0);
                cb.set_length(total as u64);
                cb.set_position(chunk.saturating_sub(1) as u64);
                cb.set_message(format!("Parsing chunk {chunk} ({mb:.0} MB)..."));
            }
            ImportPhase::Scanning {
                bytes_read,
                total_bytes,
            } => {
                is_streaming_scan.store(true, std::sync::atomic::Ordering::Relaxed);
                let gb_read = bytes_read as f64 / (1024.0 * 1024.0 * 1024.0);
                if total_bytes > 0 {
                    // Known total — normal percentage bar.
                    sb.set_length(total_bytes);
                    sb.set_position(bytes_read);
                    let gb_total = total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                    sb.set_message(format!("{gb_read:.1} / {gb_total:.1} GB"));
                    if bytes_read >= total_bytes {
                        sb.finish_with_message(format!("{gb_total:.1} GB"));
                    }
                } else {
                    // Unknown total (compressed source — uncompressed size
                    // not known ahead of time). Show running bytes only; the
                    // bar's length is left at a sentinel so it never renders
                    // as complete, and the Committing handler will clear it
                    // when reading is genuinely done.
                    sb.set_length(u64::MAX);
                    sb.set_position(bytes_read);
                    sb.set_message(format!("{gb_read:.2} GB read"));
                }
            }
            ImportPhase::Committing {
                chunk,
                total,
                cumulative_flakes,
                ..
            } => {
                // For non-streaming imports (small files, directories), scanning
                // never happens — hide the scan bar on the first Committing event.
                // For streaming imports, the scan bar finishes itself via
                // finish_with_message() when reading completes; don't kill it here
                // while the reader thread is still active.
                if !sb.is_finished()
                    && !is_streaming_scan.load(std::sync::atomic::Ordering::Relaxed)
                {
                    sb.finish_and_clear();
                }
                let t0 = *commit_start.get_or_init(std::time::Instant::now);
                if total > 0 {
                    // Estimated totals can undercount (e.g. compressed ndjson
                    // sources) — never let the length fall below the position,
                    // which would render the bar overfull.
                    cb.set_length((total as u64).max(chunk as u64));
                } else {
                    // Unknown total: sentinel length so the bar never renders
                    // as complete (mirrors the Scanning handler).
                    cb.set_length(u64::MAX);
                }
                cb.set_position(chunk as u64);
                let secs = t0.elapsed().as_secs_f64();
                let rate = if secs > 0.0 {
                    cumulative_flakes as f64 / secs / 1_000_000.0
                } else {
                    0.0
                };
                cb.set_message(format!("{rate:.2} M flakes/s"));
            }
            ImportPhase::PreparingIndex { stage } => {
                cb.finish();
                // Unknown-total streaming scan (compressed input) leaves the
                // scan bar without a natural finish event from the reader.
                // By PreparingIndex, parsing is fully done so reading must be
                // too — lock the bar at its last reported byte count.
                if !sb.is_finished() {
                    sb.finish();
                }
                // Show activity immediately (avoid "Indexing 0%" during merge/remap).
                ib.set_length(100);
                ib.set_position(1);
                ib.set_message(stage.to_string());
            }
            ImportPhase::Indexing {
                stage,
                processed_flakes,
                total_flakes,
                stage_elapsed_secs,
            } => {
                cb.finish();
                ib.set_length(total_flakes);
                // Start at 1% minimum so the bar shows activity immediately
                let pos = if processed_flakes == 0 && total_flakes > 0 {
                    total_flakes / 100
                } else {
                    processed_flakes
                };
                ib.set_position(pos);
                let rate = if stage_elapsed_secs > 0.0 {
                    processed_flakes as f64 / 1_000_000.0 / stage_elapsed_secs
                } else {
                    0.0
                };
                ib.set_message(format!("{stage} {rate:.2} M flakes/s"));
            }
            ImportPhase::Done => {
                ib.finish();
                // Mark breadcrumb as complete (best-effort).
                if let Some(ref p) = breadcrumb_path_for_cb {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let doc = serde_json::json!({
                        "kind": "bulk_import",
                        "ledger": breadcrumb_ledger_for_cb,
                        "pid": std::process::id(),
                        "updated_epoch_ms": now_ms,
                        "status": "done"
                    });
                    let _ = std::fs::write(p, serde_json::to_vec_pretty(&doc).unwrap_or_default());
                }
            }
        }
    });

    let start = std::time::Instant::now();
    let result = match builder.execute().await {
        Ok(r) => r,
        Err(e) => {
            // Persist failure marker for customer bug reports (best-effort).
            if let Some(ref p) = breadcrumb_path {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let doc = serde_json::json!({
                    "kind": "bulk_import",
                    "ledger": ledger_owned,
                    "pid": std::process::id(),
                    "updated_epoch_ms": now_ms,
                    "status": "error",
                    "error": e.to_string()
                });
                let _ = std::fs::write(p, serde_json::to_vec_pretty(&doc).unwrap_or_default());
            }
            return Err(e.into());
        }
    };
    let elapsed = start.elapsed();

    config::write_active_ledger(fluree_dir, ledger)?;

    let secs = elapsed.as_secs_f64();
    let total_m = result.flake_count as f64 / 1_000_000.0;
    let mflakes_per_sec = total_m / secs;
    println!(
        "\n\nAbout ledger '{}':\nImported {:.1}M flakes in {:.2}s ({:.2} M flakes/s) across {} commits (t={})",
        ledger, total_m, secs, mflakes_per_sec, result.t, result.t
    );

    if let Some(ref summary) = result.summary {
        if !summary.top_classes.is_empty() {
            println!("\n  Top classes:");
            for (iri, count) in &summary.top_classes {
                println!("    {:>12}  {}", format_with_commas(*count), iri);
            }
        }
        if !summary.top_properties.is_empty() {
            println!("\n  Top properties:");
            for (iri, count) in &summary.top_properties {
                println!("    {:>12}  {}", format_with_commas(*count), iri);
            }
        }
        if !summary.top_connections.is_empty() {
            println!("\n  Top connections:");
            for (src, prop, tgt, count) in &summary.top_connections {
                println!(
                    "    {:>12}  {} -> {} -> {}",
                    format_with_commas(*count),
                    src,
                    prop,
                    tgt
                );
            }
        }
        println!();
    }

    // Success: remove the crash breadcrumb so the presence of files in
    // `<data_dir>/crash/` continues to be a strong signal of *failed/crashed*
    // runs that need investigation.
    if let Some(p) = breadcrumb_path {
        let _ = std::fs::remove_file(p);
    }

    Ok(())
}

// ============================================================================
// Native ledger import (.flpack)
// ============================================================================

/// Whether this path looks like a `.flpack` ledger archive.
pub(crate) fn is_flpack_path(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("flpack"))
}

/// Import a native ledger pack file (`.flpack`) into the local store.
///
/// Streams the archive into `Fluree::restore_ledger`, which creates the ledger,
/// ingests all CAS objects (verifying each), and finalizes the commit/index
/// heads from the embedded nameservice manifest — rolling back on any failure.
async fn run_flpack_import(
    fluree: &fluree_db_api::Fluree,
    ledger: &str,
    path: &Path,
    dirs: &FlureeDir,
) -> CliResult<()> {
    use colored::Colorize;

    let file_size = std::fs::metadata(path)
        .map_err(|e| CliError::Input(format!("failed to stat {}: {e}", path.display())))?
        .len();

    eprintln!(
        "Importing ledger '{}' from {} ({})...",
        ledger.cyan(),
        path.display(),
        format_human_bytes(file_size),
    );

    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| CliError::Input(format!("failed to open {}: {e}", path.display())))?;

    let result = fluree
        .restore_ledger(ledger, &mut file)
        .await
        .map_err(|e| CliError::Config(format!("failed to restore ledger from .flpack: {e}")))?;

    config::write_active_ledger(dirs.data_dir(), ledger)?;

    let objects = result.commits + result.txn_blobs + result.index_artifacts;
    println!(
        "{} Imported '{}' — {} commits, {} txn blobs, {} index artifacts ({} objects)",
        "✓".green(),
        result.ledger_id,
        result.commits,
        result.txn_blobs,
        result.index_artifacts,
        objects,
    );

    Ok(())
}

/// Format bytes as a human-readable size.
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

// ============================================================================
// Import path detection (single files only)
// ============================================================================

/// Whether this single-file path should use the import pipeline.
///
/// - `.ttl` files (case-insensitive) → import (auto-splits large files)
/// - `.jsonld` files (case-insensitive) → import (bypasses novelty)
/// - `.ttl.gz` → error with helpful message
/// - Everything else (e.g. `.json`) → detect-based transact path
///
/// Note: directories are handled separately in `run()` via `fluree_db_api::scan_directory_format()`.
fn is_import_path(path: &Path) -> CliResult<bool> {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let name_lower = name.to_ascii_lowercase();

    // Bulk import handles every supported RDF format. Each may carry an outer
    // `.gz` or `.zst` suffix — the bulk pipeline decompresses transparently.
    // `.bz2` is rejected explicitly because the import pipeline doesn't link
    // a bzip2 decoder.
    if name_lower.ends_with(".bz2") {
        return Err(CliError::Input(format!(
            "bzip2-compressed files are not supported; use .gz or .zst, or \
             decompress first: {}",
            path.display()
        )));
    }
    // Single source of truth with the import pipeline's own discovery rules —
    // a format accepted here is guaranteed to resolve there.
    Ok(fluree_db_api::is_bulk_import_file(path))
}

/// Replace non-alphanumeric characters with underscores for safe filenames.
fn sanitize_for_filename(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

// ============================================================================
// Memory history import (--memory)
// ============================================================================

struct GitCommit {
    sha: String,
    timestamp: String,
    message: String,
}

/// Resolve the repo root for a memory import.
///
/// If `path` is `"."`, finds the git root from the current directory.
/// Otherwise uses the path as-is. Validates that `.fluree-memory/repo.ttl` exists.
fn resolve_memory_repo(path: &Path) -> CliResult<std::path::PathBuf> {
    let repo_root = if path == Path::new(".") {
        let output = std::process::Command::new("git")
            .args(["rev-parse", "--show-toplevel"])
            .output()
            .map_err(|e| CliError::Config(format!("failed to run git: {e}")))?;
        if !output.status.success() {
            return Err(CliError::Usage(
                "not in a git repository; pass an explicit path to --memory".into(),
            ));
        }
        std::path::PathBuf::from(String::from_utf8_lossy(&output.stdout).trim())
    } else {
        path.to_path_buf()
    };

    let repo_ttl = repo_root.join(".fluree-memory/repo.ttl");
    if !repo_ttl.exists() {
        return Err(CliError::Usage(format!(
            "no memory store found at {}",
            repo_ttl.display()
        )));
    }
    Ok(repo_root)
}

/// Get git commits that touched the memory TTL files, oldest first.
fn git_memory_commits(repo_root: &Path, include_user: bool) -> CliResult<Vec<GitCommit>> {
    let mut args = vec![
        "log",
        "--reverse",
        "--format=%H\t%aI\t%s",
        "--diff-filter=AMDR",
        "--",
        ".fluree-memory/repo.ttl",
    ];
    if include_user {
        args.push(".fluree-memory/.local/user.ttl");
    }

    let output = std::process::Command::new("git")
        .current_dir(repo_root)
        .args(&args)
        .output()
        .map_err(|e| CliError::Config(format!("failed to run git log: {e}")))?;

    if !output.status.success() {
        return Err(CliError::Config(
            "git log failed — is this a git repository?".into(),
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let commits: Vec<GitCommit> = stdout
        .lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.splitn(3, '\t').collect();
            if parts.len() == 3 {
                Some(GitCommit {
                    sha: parts[0].to_string(),
                    timestamp: parts[1].to_string(),
                    message: parts[2].to_string(),
                })
            } else {
                None
            }
        })
        .collect();

    Ok(commits)
}

/// Get file content at a specific git commit.
fn git_show(repo_root: &Path, sha: &str, file: &str) -> CliResult<String> {
    let output = std::process::Command::new("git")
        .current_dir(repo_root)
        .args(["show", &format!("{sha}:{file}")])
        .output()
        .map_err(|e| CliError::Config(format!("failed to run git show: {e}")))?;

    if !output.status.success() {
        return Ok(String::new());
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Import memory history from git into a Fluree ledger with time-travel.
///
/// Each git commit that touched `.fluree-memory/repo.ttl` (and optionally
/// `.local/user.ttl`) becomes a Fluree transaction. The retract-all + insert
/// pattern at each commit gives a clean snapshot at every `t`.
pub async fn run_memory_import(
    ledger: &str,
    memory_path: &Path,
    no_user: bool,
    dirs: &FlureeDir,
    quiet: bool,
) -> CliResult<()> {
    let repo_root = resolve_memory_repo(memory_path)?;
    let include_user = !no_user;
    let commits = git_memory_commits(&repo_root, include_user)?;

    let fluree = context::build_fluree(dirs)?;

    // Create ledger + transact memory schema
    fluree
        .create_ledger(ledger)
        .await
        .map_err(|e| CliError::Config(format!("failed to create ledger: {e}")))?;

    let schema = fluree_db_memory::schema::memory_schema_jsonld();
    fluree
        .graph(ledger)
        .transact()
        .insert(&schema)
        .commit()
        .await
        .map_err(|e| CliError::Config(format!("failed to transact schema: {e}")))?;

    if commits.is_empty() {
        // No git history — import current file state as a single transaction
        let repo_ttl = std::fs::read_to_string(repo_root.join(".fluree-memory/repo.ttl"))
            .map_err(|e| CliError::Input(format!("failed to read repo.ttl: {e}")))?;

        if let Some(data) = fluree_db_memory::turtle_io::parse_and_inject_fulltext(&repo_ttl)
            .map_err(|e| CliError::Input(format!("failed to parse repo.ttl: {e}")))?
        {
            fluree
                .graph(ledger)
                .transact()
                .insert(&data)
                .commit()
                .await?;
        }

        config::write_active_ledger(dirs.data_dir(), ledger)?;
        println!("Created ledger '{ledger}' from current memory state (no git history found)");
        return Ok(());
    }

    if !quiet {
        eprintln!(
            "Importing {} commits into ledger '{}'...",
            commits.len(),
            ledger
        );
    }

    let mut last_t = 1u64; // t=1 is the schema transaction
    for (i, commit) in commits.iter().enumerate() {
        // Extract TTL content at this commit
        let repo_ttl = git_show(&repo_root, &commit.sha, ".fluree-memory/repo.ttl")?;
        let user_ttl = if include_user {
            git_show(&repo_root, &commit.sha, ".fluree-memory/.local/user.ttl")?
        } else {
            String::new()
        };

        // Parse both files into JSON-LD
        let repo_data = if repo_ttl.is_empty() {
            None
        } else {
            fluree_db_memory::turtle_io::parse_and_inject_fulltext(&repo_ttl).map_err(|e| {
                CliError::Input(format!(
                    "failed to parse repo.ttl at {}: {e}",
                    &commit.sha[..8]
                ))
            })?
        };

        let user_data = if user_ttl.is_empty() {
            None
        } else {
            fluree_db_memory::turtle_io::parse_and_inject_fulltext(&user_ttl).map_err(|e| {
                CliError::Input(format!(
                    "failed to parse user.ttl at {}: {e}",
                    &commit.sha[..8]
                ))
            })?
        };

        // Merge the @graph arrays from both files into one insert payload
        let insert_nodes = merge_jsonld_graphs(repo_data, user_data);

        // Build commit metadata from the git commit. f:message is a user
        // claim — supply it via the txn-meta sidecar (works for update-shape
        // transactions which have no @graph envelope).
        let commit_opts =
            fluree_db_api::CommitOpts::default().with_timestamp(commit.timestamp.clone());

        // Single transaction: retract all existing memory triples + insert new state.
        // The WHERE pivots on mem:content to target only memory instances (not schema).
        // On the first commit the WHERE matches nothing, so DELETE is a no-op.
        let mut txn = serde_json::json!({
            "@context": {
                "mem": "https://ns.flur.ee/memory#",
                "f": "https://ns.flur.ee/db#"
            },
            "where": [
                { "@id": "?s", "mem:content": "?c" },
                { "@id": "?s", "?p": "?o" }
            ],
            "delete": { "@id": "?s", "?p": "?o" },
            "txn-meta": {
                "f:message": format!("git:{} {}", &commit.sha[..8], commit.message)
            }
        });

        if let Some(nodes) = &insert_nodes {
            txn.as_object_mut()
                .unwrap()
                .insert("insert".to_string(), nodes.clone());
        }

        let result = fluree
            .graph(ledger)
            .transact()
            .update(&txn)
            .commit_opts(commit_opts)
            .commit()
            .await?;

        last_t = result.receipt.t.max(0) as u64;

        if !quiet {
            eprintln!(
                "  [{}/{}] t={} {} — {}",
                i + 1,
                commits.len(),
                last_t,
                &commit.sha[..8],
                commit.message,
            );
        }
    }

    config::write_active_ledger(dirs.data_dir(), ledger)?;

    println!(
        "Created ledger '{}' with {} commits (t=1..{})",
        ledger,
        commits.len(),
        last_t,
    );
    println!(
        "  Earliest: {} — {}",
        &commits[0].sha[..8],
        commits[0].message
    );
    println!(
        "  Latest:   {} — {}",
        &commits.last().unwrap().sha[..8],
        commits.last().unwrap().message,
    );
    println!();
    println!("Query with time travel:");
    println!(
        "  fluree query {ledger} 'PREFIX mem: <https://ns.flur.ee/memory#> SELECT ?id ?content WHERE {{ ?id a mem:Fact ; mem:content ?content }} LIMIT 5'"
    );
    println!(
        "  fluree query {ledger} --at-t 2 'PREFIX mem: <https://ns.flur.ee/memory#> SELECT ...'   # state at first commit"
    );

    Ok(())
}

/// Merge @graph arrays from repo.ttl and user.ttl JSON-LD into a single array.
fn merge_jsonld_graphs(
    repo: Option<serde_json::Value>,
    user: Option<serde_json::Value>,
) -> Option<serde_json::Value> {
    let mut nodes = Vec::new();

    for data in [repo, user].into_iter().flatten() {
        if let Some(graph) = data.get("@graph").and_then(|g| g.as_array()) {
            nodes.extend(graph.iter().cloned());
        } else if data.is_object() {
            // Single node (no @graph wrapper)
            nodes.push(data);
        }
    }

    if nodes.is_empty() {
        None
    } else {
        Some(serde_json::Value::Array(nodes))
    }
}

/// Format a u64 with comma-separated thousands (e.g. 543_174_590 → "543,174,590").
fn format_with_commas(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(ch);
    }
    result
}

#[cfg(test)]
mod is_import_path_tests {
    use super::is_import_path;
    use std::path::Path;

    #[test]
    fn ndjson_jsonl_are_import_paths() {
        for name in [
            "data.jsonl",
            "data.ndjson",
            "data.jsonl.gz",
            "data.ndjson.zst",
        ] {
            assert!(
                is_import_path(Path::new(name)).unwrap(),
                "{name} should route to bulk import"
            );
        }
    }

    #[test]
    fn rdf_formats_remain_import_paths() {
        for name in ["x.ttl", "x.nt", "x.nq", "x.trig", "x.jsonld", "x.ttl.gz"] {
            assert!(is_import_path(Path::new(name)).unwrap(), "{name}");
        }
    }

    #[test]
    fn non_bulk_inputs_are_not_import_paths() {
        // `.json` deliberately routes to the detect/transact path, not import.
        assert!(!is_import_path(Path::new("x.json")).unwrap());
        assert!(!is_import_path(Path::new("x.csv")).unwrap());
    }
}
