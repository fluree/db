use crate::cli::GraphAction;
use crate::commands::create::{self, ImportOpts, ImportTarget};
use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use crate::remote_client::RemoteLedgerClient;
use comfy_table::{ContentArrangement, Table};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_core::graph_registry::{
    config_graph_iri, txn_meta_graph_iri, CONFIG_GRAPH_ID, DEFAULT_GRAPH_ID, TXN_META_GRAPH_ID,
};

/// Global flags a graph subcommand reads.
pub struct GlobalFlags {
    pub verbose: bool,
    pub quiet: bool,
    /// The global `--memory-budget-mb` and `--parallelism`, for an import
    /// that leaves its own at 0.
    pub memory_budget_mb: usize,
    pub parallelism: usize,
}

pub async fn run(action: GraphAction, dirs: &FlureeDir, globals: GlobalFlags) -> CliResult<()> {
    match action {
        GraphAction::List {
            ledger,
            remote,
            json,
            include_system,
        } => {
            run_list(
                ledger.as_deref(),
                dirs,
                remote.as_deref(),
                json,
                include_system,
            )
            .await
        }
        GraphAction::Drop {
            iri,
            ledger,
            remote,
        } => run_drop(&iri, ledger.as_deref(), dirs, remote.as_deref()).await,
        GraphAction::Import {
            iri,
            from,
            ledger,
            remote,
            import,
        } => {
            let opts = import.opts(globals.memory_budget_mb, globals.parallelism);
            run_import(
                &iri,
                &from,
                ledger.as_deref(),
                dirs,
                remote.as_deref(),
                &globals,
                &opts,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_import(
    iri: &str,
    from: &std::path::Path,
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    globals: &GlobalFlags,
    opts: &ImportOpts,
) -> CliResult<()> {
    if iri.is_empty() {
        return Err(CliError::Usage(
            "graph IRI must not be empty (an import fills a new named graph)".to_string(),
        ));
    }
    if !from.exists() {
        return Err(CliError::Input(format!(
            "{} does not exist",
            from.display()
        )));
    }

    if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        let ledger_id = context::to_ledger_id(&alias)?;
        import_on_server(&client, &ledger_id, remote_name, iri, from, opts).await?;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return Ok(());
    }

    let mode = context::resolve_ledger_mode(ledger, dirs).await?;
    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            import_on_server(&client, &remote_alias, &remote_name, iri, from, opts).await?;
            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;
            Ok(())
        }
        LedgerMode::Local { fluree, alias } => {
            create::run_graph_import(
                &fluree,
                &alias,
                iri,
                from,
                dirs,
                globals.verbose,
                globals.quiet,
                opts,
            )
            .await
        }
    }
}

/// Run the import on the server behind `client`, uploading `from` to it.
async fn import_on_server(
    client: &RemoteLedgerClient,
    ledger_id: &str,
    server_label: &str,
    iri: &str,
    from: &std::path::Path,
    opts: &ImportOpts,
) -> CliResult<()> {
    use colored::Colorize;

    let cap = client.fetch_import_capability().await;
    if !cap.supports_graph_source_upload() {
        return Err(CliError::Usage(format!(
            "'{server_label}' does not offer server-side import into a graph \
             (source-upload-graph); stop the server and run `fluree graph import` \
             locally, or upgrade it"
        )));
    }
    if !from.is_file() {
        return Err(CliError::Usage(format!(
            "an import on a server takes a single file ({} is a directory); \
             concatenate it, or stop the server and import locally",
            from.display()
        )));
    }
    let result = create::upload_source_import(
        client,
        &cap,
        ledger_id,
        server_label,
        from,
        opts,
        ImportTarget::Graph(iri),
    )
    .await?;

    let resolved = result["ledger_id"].as_str().unwrap_or(ledger_id);
    let base_t = result["graph"]["base_t"].as_i64().unwrap_or(0);
    let t = result["t"].as_i64().unwrap_or(0);
    let flakes = result["flake_count"].as_u64().unwrap_or(0);
    println!(
        "{} Imported {} flakes into graph <{}> of '{}' on '{}' (t={}..{})",
        "✓".green(),
        flakes,
        iri,
        resolved,
        server_label,
        base_t + 1,
        t,
    );
    if result["has_annotations"].as_bool() == Some(true) {
        eprintln!(
            "{} The graph holds statement annotations, so the ledger's annotation arena \
             was set aside; quoted-triple queries read by scan until the ledger is reindexed.",
            "info:".cyan().bold()
        );
    }
    Ok(())
}

async fn run_list(
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    json: bool,
    include_system: bool,
) -> CliResult<()> {
    let payload = if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        let info = client.ledger_info(&alias, None).await?;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        info
    } else {
        let mode = {
            let mode = context::resolve_ledger_mode(ledger, dirs).await?;
            mode
        };
        match mode {
            LedgerMode::Tracked {
                client,
                remote_alias,
                remote_name,
                ..
            } => {
                let info = client.ledger_info(&remote_alias, None).await?;
                context::persist_refreshed_tokens(&client, &remote_name, dirs).await;
                info
            }
            LedgerMode::Local { fluree, alias } => {
                let ledger_id = context::to_ledger_id(&alias)?;
                fluree.ledger_info(&ledger_id).execute().await?
            }
        }
    };

    let ledger_id = payload
        .get("ledger")
        .and_then(|v| v.get("alias"))
        .and_then(serde_json::Value::as_str)
        .or_else(|| payload.get("alias").and_then(serde_json::Value::as_str));

    let graphs = extract_named_graphs(&payload).ok_or_else(|| {
        CliError::Usage(
            "info response is missing `named-graphs`; the targeted server may be too old"
                .to_string(),
        )
    })?;

    let resolved_ledger = ledger_id.unwrap_or("");
    let txn_meta = if resolved_ledger.is_empty() {
        String::new()
    } else {
        txn_meta_graph_iri(resolved_ledger)
    };
    let config = if resolved_ledger.is_empty() {
        String::new()
    } else {
        config_graph_iri(resolved_ledger)
    };

    let mut parsed: Vec<(&serde_json::Value, Option<fluree_db_core::GraphId>)> =
        Vec::with_capacity(graphs.len());
    for entry in graphs {
        let g_id = match entry.get("g-id").and_then(serde_json::Value::as_u64) {
            Some(n) => Some(fluree_db_core::GraphId::try_from(n).map_err(|_| {
                CliError::Usage(format!(
                    "info response contains out-of-range g-id {n}; GraphId is u32"
                ))
            })?),
            None => None,
        };
        parsed.push((entry, g_id));
    }

    let entries: Vec<(&serde_json::Value, Option<fluree_db_core::GraphId>)> = parsed
        .into_iter()
        .filter(|(entry, g_id)| {
            if include_system {
                return true;
            }
            let g_id = g_id.unwrap_or(fluree_db_core::GraphId::MAX);
            let iri = entry
                .get("iri")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            !is_system_graph(g_id, iri, &txn_meta, &config)
        })
        .collect();

    if json {
        let arr = serde_json::Value::Array(entries.iter().map(|(e, _)| (*e).clone()).collect());
        println!(
            "{}",
            serde_json::to_string_pretty(&arr).unwrap_or_else(|_| arr.to_string()),
        );
        return Ok(());
    }

    if entries.is_empty() {
        if include_system {
            println!("No graphs registered.");
        } else {
            println!(
                "No user named graphs registered. (Pass --include-system to see default, txn-meta, config.)",
            );
        }
        return Ok(());
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec!["IRI", "Kind", "g-id", "Flakes", "Size", "Placement"]);
    for (entry, g_id) in entries {
        let iri = entry
            .get("iri")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        let g_id_label = g_id
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let kind = match g_id {
            Some(DEFAULT_GRAPH_ID) => "default",
            Some(TXN_META_GRAPH_ID) => "system:txn-meta",
            Some(CONFIG_GRAPH_ID) => "system:config",
            Some(_) => "user",
            None => "?",
        };
        let flakes = entry
            .get("flakes")
            .and_then(serde_json::Value::as_u64)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let size = entry
            .get("size")
            .and_then(serde_json::Value::as_u64)
            .map(format_bytes)
            .unwrap_or_else(|| "-".to_string());
        let placement = entry
            .get("placement")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-");
        table.add_row(vec![
            iri.to_string(),
            kind.to_string(),
            g_id_label,
            flakes,
            size,
            placement.to_string(),
        ]);
    }
    println!("{table}");
    Ok(())
}

fn extract_named_graphs(payload: &serde_json::Value) -> Option<&Vec<serde_json::Value>> {
    payload
        .get("ledger")
        .and_then(|v| v.get("named-graphs"))
        .and_then(serde_json::Value::as_array)
        .or_else(|| {
            payload
                .get("named-graphs")
                .and_then(serde_json::Value::as_array)
        })
}

fn is_system_graph(g_id: fluree_db_core::GraphId, iri: &str, txn_meta: &str, config: &str) -> bool {
    if matches!(g_id, DEFAULT_GRAPH_ID | TXN_META_GRAPH_ID | CONFIG_GRAPH_ID) {
        return true;
    }
    if !txn_meta.is_empty() && iri == txn_meta {
        return true;
    }
    if !config.is_empty() && iri == config {
        return true;
    }
    iri == "urn:default"
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1_024;
    const MB: u64 = KB * 1_024;
    const GB: u64 = MB * 1_024;
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

async fn run_drop(
    iri: &str,
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
) -> CliResult<()> {
    if iri.is_empty() {
        return Err(CliError::Usage(
            "graph IRI must not be empty (the default graph cannot be dropped)".to_string(),
        ));
    }

    if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        let client = context::build_remote_client(remote_name, dirs).await?;
        let response = client.drop_named_graph(&alias, iri).await?;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        print_remote_response(&response);
        return Ok(());
    }

    let mode = {
        let mode = context::resolve_ledger_mode(ledger, dirs).await?;
        mode
    };

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            let response = client.drop_named_graph(&remote_alias, iri).await?;
            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;
            print_remote_response(&response);
        }
        LedgerMode::Local { fluree, alias } => {
            let report = fluree.drop_named_graph(&alias, iri).await?;
            print_local_report(&report);
        }
    }

    Ok(())
}

fn print_local_report(report: &fluree_db_api::DropNamedGraphReport) {
    println!(
        "Dropped graph <{}> from '{}' (t={}).",
        report.graph_iri, report.ledger_id, report.t,
    );
}

fn print_remote_response(value: &serde_json::Value) {
    let graph_iri = value
        .get("graph_iri")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<unknown>");
    let ledger_id = value
        .get("ledger_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<unknown>");
    let committed = value
        .get("committed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let t = value
        .get("t")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    if committed {
        println!("Dropped graph <{graph_iri}> from '{ledger_id}' (t={t}).");
    } else {
        // Only a server whose drop retracts the graph's facts reports this.
        println!(
            "Graph <{graph_iri}> in '{ledger_id}' was already empty — no commit produced (t={t}).",
        );
    }
}
