use crate::cli::IcebergMapArgs;
use crate::context;
use crate::error::{CliError, CliResult};
use comfy_table::{ContentArrangement, Table};
use fluree_db_api::server_defaults::FlureeDir;

// =============================================================================
// fluree iceberg map
// =============================================================================

pub async fn run_iceberg_map(
    args: IcebergMapArgs,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    if let Some(remote_name) = args.remote.as_deref() {
        let client = crate::context::build_remote_client(remote_name, dirs).await?;
        let result = run_iceberg_map_remote(&client, &args).await.map_err(|e| {
            CliError::Remote(format!(
                "failed to map Iceberg graph source on '{remote_name}': {e}"
            ))
        });
        crate::context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return result;
    }

    // Try server routing (unless --direct)
    if !direct {
        if let Some(client) = crate::context::try_server_route_client(dirs) {
            return run_iceberg_map_remote(&client, &args)
                .await
                .map_err(|e| CliError::Remote(format!("failed to map Iceberg graph source: {e}")));
        }
    }

    // Local execution
    run_iceberg_map_local(args, dirs).await
}

// =============================================================================
// fluree iceberg list
// =============================================================================

pub async fn run_iceberg_list(
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        let client = context::build_remote_client(remote_name, dirs).await?;
        let result = client.list_ledgers().await.map_err(|e| {
            CliError::Remote(format!(
                "failed to list Iceberg graph sources on '{remote_name}': {e}"
            ))
        })?;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return print_iceberg_list_remote(&result, Some(remote_name));
    }

    if !direct {
        if let Some(client) = context::try_server_route_client(dirs) {
            let result = client.list_ledgers().await.map_err(|e| {
                CliError::Remote(format!("failed to list Iceberg graph sources: {e}"))
            })?;
            return print_iceberg_list_remote(&result, None);
        }
    }

    let fluree = context::build_fluree(dirs)?;
    let gs_records = fluree.nameservice().all_graph_source_records().await?;
    let mut entries: Vec<_> = gs_records
        .into_iter()
        .filter(|gs| !gs.retracted && is_iceberg_family_source_type(&gs.source_type))
        .collect();
    entries.sort_by(|a, b| a.graph_source_id.cmp(&b.graph_source_id));

    if entries.is_empty() {
        println!("No Iceberg graph sources found.");
        return Ok(());
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec!["NAME", "BRANCH", "TYPE", "T"]);

    for gs in entries {
        let t_str = if gs.index_t > 0 {
            gs.index_t.to_string()
        } else {
            "-".to_string()
        };
        table.add_row(vec![
            gs.name,
            gs.branch,
            format_source_type(&gs.source_type),
            t_str,
        ]);
    }

    println!("{table}");
    Ok(())
}

// =============================================================================
// fluree iceberg info
// =============================================================================

pub async fn run_iceberg_info(
    name: &str,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if let Some(remote_name) = remote_flag {
        let client = context::build_remote_client(remote_name, dirs).await?;
        let info = client.ledger_info(name, None).await.map_err(|e| {
            CliError::Remote(format!(
                "failed to load Iceberg graph source info from '{remote_name}': {e}"
            ))
        })?;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return print_iceberg_info_remote(name, &info);
    }

    if !direct {
        if let Some(client) = context::try_server_route_client(dirs) {
            let info = client.ledger_info(name, None).await.map_err(|e| {
                CliError::Remote(format!("failed to load Iceberg graph source info: {e}"))
            })?;
            return print_iceberg_info_remote(name, &info);
        }
    }

    let fluree = context::build_fluree(dirs)?;
    let gs_id = context::to_ledger_id(name);
    let gs = fluree
        .nameservice()
        .lookup_graph_source(&gs_id)
        .await?
        .ok_or_else(|| {
            CliError::NotFound(format!("'{name}' not found as an Iceberg graph source"))
        })?;

    if !is_iceberg_family_source_type(&gs.source_type) {
        return Err(CliError::NotFound(format!(
            "'{name}' is not an Iceberg graph source"
        )));
    }

    print_graph_source_info(&gs);
    Ok(())
}

// =============================================================================
// fluree iceberg drop
// =============================================================================

pub async fn run_iceberg_drop(
    name: &str,
    force: bool,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    if !force {
        return Err(CliError::Usage(format!(
            "use --force to confirm deletion of '{name}'"
        )));
    }

    if let Some(remote_name) = remote_flag {
        let client = context::build_remote_client(remote_name, dirs).await?;
        let info = client.ledger_info(name, None).await.map_err(|e| {
            CliError::Remote(format!(
                "failed to validate Iceberg graph source on '{remote_name}': {e}"
            ))
        })?;
        ensure_remote_iceberg_info(name, &info)?;

        let response = client.drop_resource(name, true).await.map_err(|e| {
            CliError::Remote(format!(
                "failed to drop Iceberg graph source on '{remote_name}': {e}"
            ))
        })?;
        context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return print_remote_drop_response(&response);
    }

    if !direct {
        if let Some(client) = context::try_server_route_client(dirs) {
            let info = client.ledger_info(name, None).await.map_err(|e| {
                CliError::Remote(format!("failed to validate Iceberg graph source: {e}"))
            })?;
            ensure_remote_iceberg_info(name, &info)?;

            let response = client.drop_resource(name, true).await.map_err(|e| {
                CliError::Remote(format!("failed to drop Iceberg graph source: {e}"))
            })?;
            return print_remote_drop_response(&response);
        }
    }

    let fluree = context::build_fluree(dirs)?;
    let gs_id = context::to_ledger_id(name);
    let gs = fluree
        .nameservice()
        .lookup_graph_source(&gs_id)
        .await?
        .ok_or_else(|| {
            CliError::NotFound(format!("'{name}' not found as an Iceberg graph source"))
        })?;

    if !is_iceberg_family_source_type(&gs.source_type) {
        return Err(CliError::NotFound(format!(
            "'{name}' is not an Iceberg graph source"
        )));
    }

    let gs_report = fluree
        .drop_graph_source(name, None, fluree_db_api::DropMode::Hard)
        .await?;

    match gs_report.status {
        fluree_db_api::admin::DropStatus::Dropped => {
            println!(
                "Dropped Iceberg graph source '{}:{}'",
                gs_report.name, gs_report.branch
            );
            for w in &gs_report.warnings {
                eprintln!("  warning: {w}");
            }
        }
        fluree_db_api::admin::DropStatus::AlreadyRetracted => {
            println!(
                "Iceberg graph source '{}:{}' was already dropped",
                gs_report.name, gs_report.branch
            );
        }
        fluree_db_api::admin::DropStatus::NotFound => {
            return Err(CliError::NotFound(format!(
                "'{name}' not found as an Iceberg graph source"
            )));
        }
    }

    Ok(())
}

async fn run_iceberg_map_remote(
    client: &crate::remote_client::RemoteLedgerClient,
    args: &IcebergMapArgs,
) -> CliResult<()> {
    let body = args_to_json(args)?;
    let result = client.iceberg_map(&body).await?;

    // Print response
    let gs_id = result
        .get("graph_source_id")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");
    let table = result
        .get("table_identifier")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let catalog = result
        .get("catalog_uri")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let connection = result
        .get("connection_tested")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    if let Some(mapping) = result.get("mapping_source").and_then(|v| v.as_str()) {
        println!("Mapped Iceberg table as R2RML graph source '{gs_id}'");
        println!("  Table:       {table}");
        println!("  Catalog:     {catalog}");
        println!("  R2RML:       {mapping}");
        if let Some(count) = result
            .get("triples_map_count")
            .and_then(serde_json::Value::as_u64)
        {
            println!("  TriplesMaps: {count}");
        }
        println!(
            "  Connection:  {}",
            if connection { "verified" } else { "not tested" }
        );
        let validated = result
            .get("mapping_validated")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        println!(
            "  Mapping:     {}",
            if validated {
                "validated"
            } else {
                "not validated (check mapping source)"
            }
        );
    } else {
        println!("Mapped Iceberg table as graph source '{gs_id}'");
        println!("  Table:       {table}");
        println!("  Catalog:     {catalog}");
        println!(
            "  Connection:  {}",
            if connection {
                "verified"
            } else {
                "not tested (direct mode or catalog unreachable)"
            }
        );
    }

    Ok(())
}

/// Convert CLI args to a JSON body for the server endpoint.
fn args_to_json(args: &IcebergMapArgs) -> CliResult<serde_json::Value> {
    let mut body = serde_json::json!({
        "name": args.name,
        "mode": args.mode,
    });
    let obj = body.as_object_mut().unwrap();

    if let Some(ref v) = args.catalog_uri {
        obj.insert("catalog_uri".into(), v.clone().into());
    }
    if let Some(ref v) = args.table {
        obj.insert("table".into(), v.clone().into());
    }
    if let Some(ref v) = args.table_location {
        obj.insert("table_location".into(), v.clone().into());
    }
    if let Some(ref v) = args.r2rml {
        // Read file content and send it (not the path)
        let content = std::fs::read_to_string(v).map_err(|e| {
            CliError::Input(format!("failed to read R2RML file {}: {e}", v.display()))
        })?;
        obj.insert("r2rml".into(), content.into());
    }
    if let Some(ref v) = args.r2rml_type {
        obj.insert("r2rml_type".into(), v.clone().into());
    }
    if let Some(ref v) = args.branch {
        obj.insert("branch".into(), v.clone().into());
    }
    if let Some(ref v) = args.auth_bearer {
        obj.insert("auth_bearer".into(), v.clone().into());
    }
    if let Some(ref v) = args.oauth2_token_url {
        obj.insert("oauth2_token_url".into(), v.clone().into());
    }
    if let Some(ref v) = args.oauth2_client_id {
        obj.insert("oauth2_client_id".into(), v.clone().into());
    }
    if let Some(ref v) = args.oauth2_client_secret {
        obj.insert("oauth2_client_secret".into(), v.clone().into());
    }
    if let Some(ref v) = args.warehouse {
        obj.insert("warehouse".into(), v.clone().into());
    }
    if args.no_vended_credentials {
        obj.insert("no_vended_credentials".into(), true.into());
    }
    if let Some(ref v) = args.s3_region {
        obj.insert("s3_region".into(), v.clone().into());
    }
    if let Some(ref v) = args.s3_endpoint {
        obj.insert("s3_endpoint".into(), v.clone().into());
    }
    if args.s3_path_style {
        obj.insert("s3_path_style".into(), true.into());
    }

    Ok(body)
}

fn print_iceberg_list_remote(
    result: &serde_json::Value,
    remote_label: Option<&str>,
) -> CliResult<()> {
    let entries = result.as_array().ok_or_else(|| {
        CliError::Remote("unexpected response format: expected JSON array".into())
    })?;

    let filtered: Vec<&serde_json::Value> = entries
        .iter()
        .filter(|entry| {
            entry
                .get("type")
                .and_then(|v| v.as_str())
                .is_some_and(is_iceberg_family_type_str)
        })
        .collect();

    if filtered.is_empty() {
        match remote_label {
            Some(name) => println!("No Iceberg graph sources on remote '{name}'."),
            None => println!("No Iceberg graph sources found."),
        }
        return Ok(());
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec!["NAME", "BRANCH", "TYPE", "T"]);

    for entry in filtered {
        let name = entry
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("(unknown)");
        let branch = entry
            .get("branch")
            .and_then(|v| v.as_str())
            .unwrap_or("main");
        let entry_type = entry.get("type").and_then(|v| v.as_str()).unwrap_or("?");
        let t = entry
            .get("t")
            .and_then(serde_json::Value::as_i64)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        table.add_row(vec![
            name.to_string(),
            branch.to_string(),
            entry_type.to_string(),
            t,
        ]);
    }

    println!("{table}");
    Ok(())
}

fn print_iceberg_info_remote(name: &str, info: &serde_json::Value) -> CliResult<()> {
    ensure_remote_iceberg_info(name, info)?;
    print_remote_graph_source_info(info);
    Ok(())
}

fn ensure_remote_iceberg_info(name: &str, info: &serde_json::Value) -> CliResult<()> {
    let info_type = info.get("type").and_then(|v| v.as_str()).ok_or_else(|| {
        CliError::NotFound(format!("'{name}' not found as an Iceberg graph source"))
    })?;

    if !is_iceberg_family_type_str(info_type) || info.get("graph_source_id").is_none() {
        return Err(CliError::NotFound(format!(
            "'{name}' is not an Iceberg graph source"
        )));
    }

    Ok(())
}

fn print_remote_drop_response(response: &serde_json::Value) -> CliResult<()> {
    let status = response
        .get("status")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CliError::Remote("unexpected drop response: missing status".into()))?;
    let ledger_id = response
        .get("ledger_id")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");

    match status {
        "dropped" => println!("Dropped Iceberg graph source '{ledger_id}'"),
        "already_retracted" => {
            println!("Iceberg graph source '{ledger_id}' was already dropped");
        }
        "not_found" => {
            return Err(CliError::NotFound(format!(
                "'{ledger_id}' not found as an Iceberg graph source"
            )))
        }
        other => {
            return Err(CliError::Remote(format!(
                "unexpected drop status '{other}'"
            )))
        }
    }

    if let Some(warnings) = response.get("warnings").and_then(|v| v.as_array()) {
        for warning in warnings.iter().filter_map(|v| v.as_str()) {
            eprintln!("  warning: {warning}");
        }
    }

    Ok(())
}

fn print_remote_graph_source_info(info: &serde_json::Value) {
    let name = info.get("name").and_then(|v| v.as_str()).unwrap_or("?");
    let branch = info.get("branch").and_then(|v| v.as_str()).unwrap_or("?");
    let gs_type = info.get("type").and_then(|v| v.as_str()).unwrap_or("?");
    let gs_id = info
        .get("graph_source_id")
        .and_then(|v| v.as_str())
        .unwrap_or("?");

    println!("Name:           {name}");
    println!("Branch:         {branch}");
    println!("Type:           {gs_type}");
    println!("ID:             {gs_id}");

    if let Some(t) = info.get("index_t").and_then(serde_json::Value::as_i64) {
        println!("Index t:        {t}");
    }
    if let Some(id) = info.get("index_id").and_then(|v| v.as_str()) {
        println!("Index ID:       {id}");
    }
    if let Some(deps) = info.get("dependencies").and_then(|v| v.as_array()) {
        let dep_strs: Vec<&str> = deps.iter().filter_map(|v| v.as_str()).collect();
        if !dep_strs.is_empty() {
            println!("Dependencies:   {}", dep_strs.join(", "));
        }
    }
    if let Some(config) = info.get("config") {
        println!();
        println!("Configuration:");
        println!(
            "{}",
            serde_json::to_string_pretty(config).unwrap_or_default()
        );
    }
}

fn print_graph_source_info(gs: &fluree_db_nameservice::GraphSourceRecord) {
    println!("Name:           {}", gs.name);
    println!("Branch:         {}", gs.branch);
    println!("Type:           {}", format_source_type(&gs.source_type));
    println!("ID:             {}", gs.graph_source_id);
    println!("Retracted:      {}", gs.retracted);
    println!("Index t:        {}", gs.index_t);
    println!(
        "Index ID:       {}",
        gs.index_id
            .as_ref()
            .map(std::string::ToString::to_string)
            .as_deref()
            .unwrap_or("(none)")
    );

    if !gs.dependencies.is_empty() {
        println!("Dependencies:   {}", gs.dependencies.join(", "));
    }

    if !gs.config.is_empty() && gs.config != "{}" {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&gs.config) {
            println!();
            println!("Configuration:");
            println!(
                "{}",
                serde_json::to_string_pretty(&parsed).unwrap_or_else(|_| gs.config.clone())
            );
        }
    }
}

// =============================================================================
// Local execution (feature-gated)
// =============================================================================

#[cfg(feature = "iceberg")]
async fn run_iceberg_map_local(args: IcebergMapArgs, dirs: &FlureeDir) -> CliResult<()> {
    let fluree = crate::context::build_fluree(dirs)?;
    let iceberg_config = build_iceberg_config(&args)?;

    if let Some(ref r2rml_path) = args.r2rml {
        // Read mapping file content
        let mapping_content = std::fs::read_to_string(r2rml_path).map_err(|e| {
            crate::error::CliError::Input(format!(
                "Failed to read R2RML mapping file '{}': {}",
                r2rml_path.display(),
                e
            ))
        })?;

        let config = fluree_db_api::R2rmlCreateConfig {
            iceberg: iceberg_config,
            mapping: fluree_db_api::R2rmlMappingInput::Content(mapping_content),
            mapping_media_type: args.r2rml_type.clone(),
        };

        let result = fluree.create_r2rml_graph_source(config).await?;

        println!(
            "Mapped Iceberg table as R2RML graph source '{}'",
            result.graph_source_id
        );
        println!("  Table:       {}", result.table_identifier);
        println!("  Catalog:     {}", result.catalog_uri);
        println!("  R2RML:       {}", result.mapping_source);
        println!("  TriplesMaps: {}", result.triples_map_count);
        println!(
            "  Connection:  {}",
            if result.connection_tested {
                "verified"
            } else {
                "not tested"
            }
        );
        println!(
            "  Mapping:     {}",
            if result.mapping_validated {
                "validated"
            } else {
                "not validated (check mapping source)"
            }
        );
    } else {
        let result = fluree.create_iceberg_graph_source(iceberg_config).await?;

        println!(
            "Mapped Iceberg table as graph source '{}'",
            result.graph_source_id
        );
        println!("  Table:       {}", result.table_identifier);
        println!("  Catalog:     {}", result.catalog_uri);
        println!(
            "  Connection:  {}",
            if result.connection_tested {
                "verified"
            } else {
                "not tested (direct mode or catalog unreachable)"
            }
        );
    }

    Ok(())
}

#[cfg(not(feature = "iceberg"))]
async fn run_iceberg_map_local(_args: IcebergMapArgs, _dirs: &FlureeDir) -> CliResult<()> {
    Err(CliError::Usage(
        "Iceberg support not compiled. Rebuild with `--features iceberg`.".into(),
    ))
}

// =============================================================================
// Helpers
// =============================================================================

#[cfg(feature = "iceberg")]
fn build_iceberg_config(args: &IcebergMapArgs) -> CliResult<fluree_db_api::IcebergCreateConfig> {
    let mode = args.mode.to_lowercase();
    let mut config = match mode.as_str() {
        "rest" => {
            let catalog_uri = args
                .catalog_uri
                .as_ref()
                .ok_or_else(|| CliError::Usage("--catalog-uri is required for rest mode".into()))?;
            let table = args.table.as_deref().unwrap_or_default();
            if table.is_empty() && args.r2rml.is_none() {
                return Err(CliError::Usage(
                    "--table is required for rest mode (or use --r2rml to define tables via mapping)"
                        .into(),
                ));
            }
            let table = if table.is_empty() {
                "default.default"
            } else {
                table
            };
            fluree_db_api::IcebergCreateConfig::new(&args.name, catalog_uri, table)
        }
        "direct" => {
            let location = args.table_location.as_ref().ok_or_else(|| {
                CliError::Usage("--table-location is required for direct mode".into())
            })?;
            fluree_db_api::IcebergCreateConfig::new_direct(&args.name, location)
        }
        other => {
            return Err(CliError::Usage(format!(
                "unknown catalog mode '{other}'. Use 'rest' or 'direct'."
            )));
        }
    };

    if let Some(ref branch) = args.branch {
        config = config.with_branch(branch);
    }
    if let Some(ref token) = args.auth_bearer {
        config = config.with_auth_bearer(token);
    }
    if let (Some(ref url), Some(ref id), Some(ref secret)) = (
        &args.oauth2_token_url,
        &args.oauth2_client_id,
        &args.oauth2_client_secret,
    ) {
        config = config.with_auth_oauth2(url, id, secret);
    }
    if let Some(ref wh) = args.warehouse {
        config = config.with_warehouse(wh);
    }
    if args.no_vended_credentials {
        config = config.with_vended_credentials(false);
    }
    if let Some(ref region) = args.s3_region {
        config = config.with_s3_region(region);
    }
    if let Some(ref endpoint) = args.s3_endpoint {
        config = config.with_s3_endpoint(endpoint);
    }
    if args.s3_path_style {
        config = config.with_s3_path_style(true);
    }

    Ok(config)
}

fn is_iceberg_family_source_type(st: &fluree_db_nameservice::GraphSourceType) -> bool {
    matches!(
        st,
        fluree_db_nameservice::GraphSourceType::Iceberg
            | fluree_db_nameservice::GraphSourceType::R2rml
    )
}

fn is_iceberg_family_type_str(s: &str) -> bool {
    matches!(s, "Iceberg" | "R2RML")
}

fn format_source_type(st: &fluree_db_nameservice::GraphSourceType) -> String {
    match st {
        fluree_db_nameservice::GraphSourceType::Bm25 => "BM25".to_string(),
        fluree_db_nameservice::GraphSourceType::Vector => "Vector".to_string(),
        fluree_db_nameservice::GraphSourceType::Geo => "Geo".to_string(),
        fluree_db_nameservice::GraphSourceType::R2rml => "R2RML".to_string(),
        fluree_db_nameservice::GraphSourceType::Iceberg => "Iceberg".to_string(),
        fluree_db_nameservice::GraphSourceType::Unknown(s) => format!("Unknown({s})"),
    }
}
