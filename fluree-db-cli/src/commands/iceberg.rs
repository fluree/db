use crate::cli::IcebergMapArgs;
use crate::context;
use crate::error::{CliError, CliResult};
use crate::graph_source_display::{print_local_graph_source, print_remote_graph_source};
use comfy_table::{ContentArrangement, Table};
use fluree_db_api::ledger_info::graph_source_type_label;
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

    let fluree = context::build_fluree(dirs).await?;
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
            graph_source_type_label(&gs.source_type),
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

    let fluree = context::build_fluree(dirs).await?;
    let gs_id = context::to_ledger_id(name)?;
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

    print_local_graph_source(&gs);
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

    let fluree = context::build_fluree(dirs).await?;
    let gs_id = context::to_ledger_id(name)?;
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
        if let Some(table_count) = result
            .get("table_count")
            .and_then(serde_json::Value::as_u64)
        {
            let table_names: Vec<String> = result
                .get("table_names")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            println!(
                "  Tables:      {}",
                format_table_summary(table_count as usize, &table_names)
            );
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
                "not tested (tested for REST catalogs only, or catalog unreachable)"
            }
        );
    }

    print_model_warnings(&model_warnings_of(&result));
    Ok(())
}

/// Infer an R2RML mapping media type from a file extension.
///
/// Delegates to the shared [`fluree_db_r2rml::loader::MappingFormat`] resolver so
/// the CLI and the server share one ext→format table and cannot drift (issue
/// #1397): `.jsonld`/`.json` → `application/ld+json`, any other extension →
/// `text/turtle` (the resolver's default), case-insensitively. Returns `None`
/// only when the path has no extension at all, leaving the server to apply the
/// same default. An explicit `--r2rml-type` still overrides this at the call site.
pub(crate) fn infer_mapping_media_type(path: &std::path::Path) -> Option<String> {
    use fluree_db_r2rml::loader::MappingFormat;
    // No extension means no signal to infer from — defer to the server default.
    path.extension()?;
    let source = path.to_str()?;
    Some(
        MappingFormat::resolve(None, source)
            .media_type()
            .to_string(),
    )
}

/// Convert CLI args to a JSON body for the server endpoint.
/// Print registration warnings about the `--model` reference (one per line).
pub(crate) fn print_model_warnings(warnings: &[String]) {
    for w in warnings {
        eprintln!("warning: {w}");
    }
}

/// The `model_warnings` of a server map response, if any.
pub(crate) fn model_warnings_of(result: &serde_json::Value) -> Vec<String> {
    result
        .get("model_warnings")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|w| w.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// The `iceberg/map` body for a remote server: the source's connection as the
/// structured `catalog` / `io` its stored config holds (built by the same
/// conversion a local map uses, so remote and local store the same record),
/// and the mapping's text rather than its path.
#[cfg(feature = "iceberg")]
pub fn args_to_json(args: &IcebergMapArgs) -> CliResult<serde_json::Value> {
    let stored = build_iceberg_config(args)?.to_iceberg_gs_config();
    let mut body = serde_json::json!({
        "name": args.name,
        "catalog": stored.catalog,
        "io": stored.io,
    });
    let obj = body.as_object_mut().unwrap();

    if let Some(ref v) = args.table {
        obj.insert("table".into(), v.clone().into());
    }
    if let Some(ref v) = args.r2rml {
        // Read file content and send it (not the path)
        let content = std::fs::read_to_string(v).map_err(|e| {
            CliError::Input(format!("failed to read R2RML file {}: {e}", v.display()))
        })?;
        obj.insert("r2rml".into(), content.into());
    }
    // Explicit `--r2rml-type` wins; otherwise infer from the mapping file
    // extension so the server records a concrete, self-describing media type
    // (issue #1397).
    let r2rml_type = args
        .r2rml_type
        .clone()
        .or_else(|| args.r2rml.as_deref().and_then(infer_mapping_media_type));
    if let Some(v) = r2rml_type {
        obj.insert("r2rml_type".into(), v.into());
    }
    if let Some(ref v) = args.branch {
        obj.insert("branch".into(), v.clone().into());
    }
    if let Some(ref v) = args.model {
        obj.insert("model".into(), v.clone().into());
    }
    if let Some(v) = args.default_allow {
        obj.insert("default_allow".into(), v.into());
    }

    Ok(body)
}

#[cfg(not(feature = "iceberg"))]
pub fn args_to_json(_args: &IcebergMapArgs) -> CliResult<serde_json::Value> {
    Err(CliError::Usage(
        "Iceberg support not compiled; the request a server takes is built by it. Rebuild \
         with `--features iceberg`."
            .into(),
    ))
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
    print_remote_graph_source(info);
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

// =============================================================================
// Local execution (feature-gated)
// =============================================================================

#[cfg(feature = "iceberg")]
async fn run_iceberg_map_local(args: IcebergMapArgs, dirs: &FlureeDir) -> CliResult<()> {
    let fluree = crate::context::build_fluree(dirs).await?;
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
            // Explicit `--r2rml-type` wins; otherwise infer from the file
            // extension (issue #1397).
            mapping_media_type: args
                .r2rml_type
                .clone()
                .or_else(|| infer_mapping_media_type(r2rml_path)),
        };

        let result = fluree.create_r2rml_graph_source(config).await?;

        println!(
            "Mapped Iceberg table as R2RML graph source '{}'",
            result.graph_source_id
        );
        println!("  Table:       {}", result.table_identifier);
        print_model_warnings(&result.model_warnings);
        println!("  Catalog:     {}", result.catalog_uri);
        println!("  R2RML:       {}", result.mapping_source);
        println!("  TriplesMaps: {}", result.triples_map_count);
        println!(
            "  Tables:      {}",
            format_table_summary(result.table_count, &result.table_names)
        );
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
        print_model_warnings(&result.model_warnings);
        println!("  Catalog:     {}", result.catalog_uri);
        println!(
            "  Connection:  {}",
            if result.connection_tested {
                "verified"
            } else {
                "not tested (tested for REST catalogs only, or catalog unreachable)"
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

/// The connection `iceberg map`'s flags describe, through the codec every
/// surface uses ([`fluree_db_api::IcebergConnectionFields`]). A REST catalog
/// with no storage flag reads with the identity its warehouse implies (see
/// [`fluree_db_api::default_storage_auth`]): `catalog` for `bq://`, `vended`
/// for `bl://`. A Google catalog identity with no
/// `--auth-google-quota-project` bills to the project a `bq://projects/<P>/…`
/// or `bl://projects/<P>/…` warehouse names
/// ([`fluree_db_api::default_google_quota_project`]): BigLake refuses a
/// request that names none, whatever the identity.
#[cfg(feature = "iceberg")]
pub(crate) fn connection_fields(
    args: &IcebergMapArgs,
) -> CliResult<fluree_db_api::IcebergConnectionFields> {
    let mut auth = args.auth.fields(Some(&args.google))?;
    if !auth.google.is_empty() && auth.google_quota_project.is_none() {
        auth.google_quota_project =
            fluree_db_api::default_google_quota_project(args.warehouse.as_deref());
    }
    let default_storage_auth = if args.storage.is_given() || args.no_vended_credentials {
        None
    } else {
        fluree_db_api::default_storage_auth(args.warehouse.as_deref())
    };
    if default_storage_auth == Some(fluree_db_api::StorageAuth::Catalog) && auth.google.is_empty() {
        return Err(CliError::Usage(
            "a bq:// warehouse (BigLake federation) never vends: its files are read with the \
             catalog's own Google identity (--storage-identity catalog, its default). \
             Authenticate the catalog with --auth-google-metadata, --auth-google-adc, \
             --auth-google-service-account-env or --auth-google-wif-config, or choose another \
             --storage-identity"
                .to_string(),
        ));
    }
    Ok(fluree_db_api::IcebergConnectionFields {
        mode: args.mode.clone(),
        catalog_uri: args.catalog_uri.clone(),
        table_location: args.table_location.clone(),
        region: args.region.clone(),
        catalog_id: args.catalog_id.clone(),
        table_bucket_arn: args.table_bucket_arn.clone(),
        auth,
        warehouse: args.warehouse.clone(),
        no_vended_credentials: args.no_vended_credentials,
        s3_region: args.s3_region.clone(),
        s3_endpoint: args.s3_endpoint.clone(),
        s3_path_style: args.s3_path_style,
        storage_auth: args.storage.fields()?,
        default_storage_auth,
    })
}

/// The create config `iceberg map`'s flags describe; a local map registers
/// it and a remote one sends its stored connection.
#[cfg(feature = "iceberg")]
pub fn build_iceberg_config(
    args: &IcebergMapArgs,
) -> CliResult<fluree_db_api::IcebergCreateConfig> {
    use super::source_auth::usage;
    // The CLI names variables of its own process; it has no secret resolver.
    let rules = fluree_db_api::SecretRules::local(false);
    let connection = connection_fields(args)?
        .into_connection(&rules)
        .map_err(usage)?;
    let mut config = fluree_db_api::IcebergCreateConfig::from_connection_for(
        &args.name,
        connection,
        args.table.as_deref(),
        args.r2rml.is_some(),
    )
    .map_err(|e| usage(e.into()))?;

    if let Some(ref branch) = args.branch {
        config = config.with_branch(branch);
    }
    if let Some(ref model) = args.model {
        config = config.with_model(model);
    }
    if let Some(allow) = args.default_allow {
        config = config.with_default_allow(allow);
    }

    Ok(config)
}

/// Format the `Tables:` summary as `N (name1, name2, …)`, or just `N` when no
/// table names are available (e.g. an unvalidated address-based mapping).
pub(crate) fn format_table_summary(count: usize, names: &[String]) -> String {
    if names.is_empty() {
        count.to_string()
    } else {
        format!("{count} ({})", names.join(", "))
    }
}

/// Mapped (R2RML-backed) graph sources: Iceberg, R2RML and SQL. `fluree sql`
/// and `fluree iceberg` share list/info/drop over this family.
fn is_iceberg_family_source_type(st: &fluree_db_nameservice::GraphSourceType) -> bool {
    matches!(
        st,
        fluree_db_nameservice::GraphSourceType::Iceberg
            | fluree_db_nameservice::GraphSourceType::R2rml
            | fluree_db_nameservice::GraphSourceType::Sql
            | fluree_db_nameservice::GraphSourceType::Delta
    )
}

fn is_iceberg_family_type_str(s: &str) -> bool {
    matches!(s, "Iceberg" | "R2RML" | "SQL" | "Delta")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `fluree iceberg map gs <args>`, as clap parses it.
    fn parse(args: &[&str]) -> IcebergMapArgs {
        use clap::Parser;
        let mut argv = vec!["fluree", "iceberg", "map", "gs"];
        argv.extend_from_slice(args);
        match crate::cli::Cli::try_parse_from(argv).unwrap().command {
            crate::cli::Commands::Iceberg {
                action: crate::cli::IcebergAction::Map(args),
            } => *args,
            _ => unreachable!(),
        }
    }

    fn rest(extra: &[&str]) -> IcebergMapArgs {
        let mut args = vec![
            "--catalog-uri",
            "https://catalog.example.com",
            "--table",
            "ns.tbl",
        ];
        args.extend_from_slice(extra);
        parse(&args)
    }

    #[cfg(feature = "iceberg")]
    fn stored(args: &IcebergMapArgs) -> serde_json::Value {
        serde_json::to_value(build_iceberg_config(args).unwrap().to_iceberg_gs_config()).unwrap()
    }

    #[cfg(feature = "iceberg")]
    fn refusal(args: &IcebergMapArgs) -> String {
        build_iceberg_config(args).unwrap_err().to_string()
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn args_to_json_carries_model_ledger() {
        let body = args_to_json(&rest(&[])).unwrap();
        assert!(body.get("model").is_none());
        assert!(body.get("default_allow").is_none());
        let body = args_to_json(&rest(&[
            "--model",
            "governance:main",
            "--default-allow",
            "true",
        ]))
        .unwrap();
        assert_eq!(body["model"], "governance:main");
        assert_eq!(body["default_allow"], true);
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn a_remote_body_is_the_structured_connection_a_local_map_stores() {
        let args = rest(&[
            "--oauth2-token-url",
            "https://catalog.example.com/v1/oauth/tokens",
            "--oauth2-client-secret-env",
            "CATALOG_SECRET",
            "--oauth2-scope",
            "session:role:ICEBERG_READER",
            "--oauth2-audience",
            "polaris",
            "--warehouse",
            "wh",
        ]);
        let body = args_to_json(&args).unwrap();
        let stored = stored(&args);
        assert_eq!(body["catalog"], stored["catalog"]);
        assert_eq!(body["io"], stored["io"]);
        let auth = &body["catalog"]["auth"];
        assert_eq!(auth["scope"], "session:role:ICEBERG_READER");
        assert_eq!(auth["audience"], "polaris");
        // Omitting the client id sends an empty one (Snowflake Horizon).
        assert_eq!(auth["client_id"], "");
        // A secret named by variable reaches the server as the name.
        assert_eq!(auth["client_secret"]["env_var"], "CATALOG_SECRET");
        // No flat field survives into the structured body.
        assert!(body.get("oauth2_scope").is_none() && body.get("catalog_uri").is_none());
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn a_secret_named_by_variable_is_stored_as_the_name() {
        let stored = stored(&rest(&["--auth-bearer-env", "CATALOG_TOKEN"]));
        assert_eq!(stored["catalog"]["auth"]["type"], "bearer");
        assert_eq!(
            stored["catalog"]["auth"]["token"]["env_var"],
            "CATALOG_TOKEN"
        );
    }

    #[test]
    fn a_secret_is_given_one_way_or_the_other() {
        use clap::Parser;
        let parse = |extra: &[&str]| {
            let mut argv = vec![
                "fluree",
                "iceberg",
                "map",
                "gs",
                "--catalog-uri",
                "https://c",
            ];
            argv.extend_from_slice(extra);
            crate::cli::Cli::try_parse_from(argv)
        };
        assert!(parse(&["--auth-bearer-env", "T"]).is_ok());
        assert!(parse(&["--auth-bearer", "t", "--auth-bearer-env", "T"]).is_err());
        assert!(parse(&[
            "--oauth2-client-secret",
            "s",
            "--oauth2-client-secret-env",
            "S"
        ])
        .is_err());
        // The legacy vending switch and the storage identity are one choice.
        assert!(parse(&["--no-vended-credentials", "--storage-identity", "ambient"]).is_err());
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn half_given_or_doubly_given_auth_is_refused_by_flag() {
        // A token URL alone used to register a source with no auth.
        let e = refusal(&rest(&[
            "--oauth2-token-url",
            "https://catalog.example.com/v1/oauth/tokens",
        ]));
        assert!(
            e.contains("--oauth2-token-url needs --oauth2-client-secret"),
            "{e}"
        );
        let e = refusal(&rest(&["--auth-bearer", "t", "--auth-google-metadata"]));
        assert!(
            e.contains("--auth-google-metadata and --auth-bearer are two ways"),
            "{e}"
        );
        let e = refusal(&rest(&["--auth-google-metadata", "--auth-google-adc"]));
        assert!(e.contains("two ways"), "{e}");
        let e = refusal(&rest(&["--auth-google-quota-project", "lake"]));
        assert!(
            e.contains("--auth-google-quota-project needs --auth-google-metadata"),
            "{e}"
        );
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn each_google_flag_stores_its_identity() {
        let auth = |extra: &[&str]| stored(&rest(extra))["catalog"]["auth"].clone();
        assert_eq!(
            auth(&["--auth-google-metadata"]),
            serde_json::json!({"type": "google_metadata", "scopes": null, "metadata_url": null})
        );
        assert_eq!(
            auth(&["--auth-google-adc", "--auth-google-quota-project", "lake"]),
            serde_json::json!({"type": "google", "credentials": {"source": "application_default"},
                "quota_project": "lake"})
        );
        let sa = auth(&[
            "--auth-google-service-account-env",
            "SA_KEY",
            "--auth-google-scopes",
            "https://www.googleapis.com/auth/bigquery",
        ]);
        assert_eq!(sa["credentials"]["source"], "service_account");
        assert_eq!(sa["credentials"]["key"]["env_var"], "SA_KEY");
        assert_eq!(sa["scopes"], "https://www.googleapis.com/auth/bigquery");

        let dir = tempfile::tempdir().unwrap();
        let wif = dir.path().join("wif.json");
        std::fs::write(
            &wif,
            r#"{"type": "external_account", "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/aws"}"#,
        )
        .unwrap();
        let wif_auth = auth(&["--auth-google-wif-config", wif.to_str().unwrap()]);
        assert_eq!(
            wif_auth["credentials"]["config"]["type"],
            "external_account"
        );
        std::fs::write(&wif, "[]").unwrap();
        let e = refusal(&rest(&["--auth-google-wif-config", wif.to_str().unwrap()]));
        assert!(e.contains("does not hold a JSON object"), "{e}");
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn a_biglake_warehouse_defaults_its_storage_identity() {
        let federation = stored(&rest(&[
            "--warehouse",
            "bq://projects/p/locations/us/catalogs/c",
            "--auth-google-metadata",
        ]));
        assert_eq!(federation["io"]["storage_auth"]["type"], "catalog");
        assert_eq!(federation["io"]["vended_credentials"], false);
        let runtime = stored(&rest(&["--warehouse", "bl://projects/p/catalogs/c"]));
        assert_eq!(runtime["io"]["storage_auth"]["type"], "vended");
        // Any other warehouse keeps what vended_credentials always meant.
        assert!(stored(&rest(&["--warehouse", "wh"]))["io"]
            .get("storage_auth")
            .is_none());
        // A choice the caller made wins.
        let ambient = stored(&rest(&[
            "--warehouse",
            "bq://projects/p/locations/us/catalogs/c",
            "--storage-identity",
            "ambient",
        ]));
        assert_eq!(ambient["io"]["storage_auth"]["type"], "ambient");
        // Federation without a Google identity cannot read its files.
        let e = refusal(&rest(&[
            "--warehouse",
            "bq://projects/p/locations/us/catalogs/c",
            "--auth-bearer",
            "t",
        ]));
        assert!(e.contains("never vends"), "{e}");
    }

    /// BigLake refuses a request that names no quota project, whatever the
    /// identity: a Google catalog identity with none bills to the project its
    /// `bq://` or `bl://` warehouse names. The default is stored, so a remote
    /// map sends it; the server infers none.
    #[cfg(feature = "iceberg")]
    #[test]
    fn a_biglake_warehouse_defaults_the_catalog_quota_project() {
        let federation = rest(&[
            "--warehouse",
            "bq://projects/lake-project/locations/us-central1",
            "--auth-google-metadata",
        ]);
        let auth = &stored(&federation)["catalog"]["auth"];
        assert_eq!(auth["type"], "google", "{auth}");
        assert_eq!(auth["credentials"]["source"], "metadata", "{auth}");
        assert_eq!(auth["quota_project"], "lake-project", "{auth}");
        assert_eq!(
            args_to_json(&federation).unwrap()["catalog"]["auth"]["quota_project"],
            "lake-project"
        );
        let runtime = stored(&rest(&[
            "--warehouse",
            "bl://projects/123456789012/catalogs/c",
            "--auth-google-adc",
        ]));
        assert_eq!(runtime["catalog"]["auth"]["quota_project"], "123456789012");
        // A project the caller names wins.
        let named = stored(&rest(&[
            "--warehouse",
            "bq://projects/lake-project/locations/us-central1",
            "--auth-google-metadata",
            "--auth-google-quota-project",
            "billing-project",
        ]));
        assert_eq!(named["catalog"]["auth"]["quota_project"], "billing-project");
        // Another warehouse names no project, so none is defaulted, and an
        // identity that is not Google's takes none.
        let plain = stored(&rest(&["--warehouse", "wh", "--auth-google-metadata"]));
        assert!(
            plain["catalog"]["auth"].get("quota_project").is_none(),
            "{}",
            plain["catalog"]["auth"]
        );
        let bearer = stored(&rest(&[
            "--warehouse",
            "bq://projects/lake-project/locations/us-central1",
            "--auth-bearer",
            "t",
            "--storage-identity",
            "ambient",
        ]));
        assert!(bearer["catalog"]["auth"].get("quota_project").is_none());
        // Storage reads never inherit it.
        assert!(stored(&federation)["io"]["storage_auth"]
            .get("quota_project")
            .is_none());
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn a_direct_source_reads_gcs_with_its_own_google_identity() {
        let stored = stored(&parse(&[
            "--mode",
            "direct",
            "--table-location",
            "s3://bucket/warehouse/ns/t",
            "--storage-google-service-account-env",
            "SA_KEY",
            "--storage-google-quota-project",
            "lake",
        ]));
        let storage = &stored["io"]["storage_auth"];
        assert_eq!(storage["type"], "google");
        assert_eq!(storage["credentials"]["key"]["env_var"], "SA_KEY");
        assert_eq!(storage["quota_project"], "lake");
        let e = refusal(&parse(&[
            "--mode",
            "direct",
            "--table-location",
            "s3://bucket/warehouse/ns/t",
            "--storage-identity",
            "google",
        ]));
        assert!(
            e.contains("--storage-identity needs --storage-google-metadata"),
            "{e}"
        );
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn rest_only_flags_are_refused_by_other_modes() {
        let glue = |extra: &[&str]| {
            let mut args = vec![
                "--mode",
                "glue",
                "--region",
                "us-east-1",
                "--r2rml",
                "mapping.ttl",
            ];
            args.extend_from_slice(extra);
            build_iceberg_config(&parse(&args))
        };
        assert!(glue(&[]).is_ok());

        for flag in [
            "--auth-bearer",
            "--auth-bearer-env",
            "--oauth2-token-url",
            "--oauth2-client-id",
            "--oauth2-client-secret",
            "--oauth2-client-secret-env",
            "--oauth2-scope",
            "--oauth2-audience",
            "--auth-google-service-account-env",
            "--auth-google-scopes",
            "--warehouse",
        ] {
            let err = glue(&[flag, "x"]).unwrap_err().to_string();
            assert!(
                err.contains(&format!("{flag} applies to rest mode only")),
                "{flag}: {err}"
            );
        }
        let err = glue(&["--auth-google-metadata"]).unwrap_err().to_string();
        assert!(
            err.contains("--auth-google-metadata applies to rest mode only"),
            "{err}"
        );
    }

    #[test]
    fn infer_mapping_media_type_maps_known_extensions() {
        use std::path::Path;
        assert_eq!(
            infer_mapping_media_type(Path::new("mapping.ttl")).as_deref(),
            Some("text/turtle")
        );
        assert_eq!(
            infer_mapping_media_type(Path::new("mapping.turtle")).as_deref(),
            Some("text/turtle")
        );
        assert_eq!(
            infer_mapping_media_type(Path::new("mapping.jsonld")).as_deref(),
            Some("application/ld+json")
        );
        assert_eq!(
            infer_mapping_media_type(Path::new("mapping.json")).as_deref(),
            Some("application/ld+json")
        );
        // Extension matching is case-insensitive.
        assert_eq!(
            infer_mapping_media_type(Path::new("MAPPING.TTL")).as_deref(),
            Some("text/turtle")
        );
        // An unrecognized extension defers to the shared resolver's default
        // (Turtle) — the same decision the server makes, so the two never drift.
        assert_eq!(
            infer_mapping_media_type(Path::new("mapping.rdf")).as_deref(),
            Some("text/turtle")
        );
        // No extension at all -> None, so the caller leaves the type unset and the
        // server applies the same default.
        assert_eq!(infer_mapping_media_type(Path::new("mapping")), None);
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn args_to_json_infers_r2rml_type_from_ttl_extension() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("airlines.ttl");
        std::fs::write(&path, "@prefix rr: <http://www.w3.org/ns/r2rml#> .").unwrap();

        // No explicit --r2rml-type: inferred from the `.ttl` extension.
        let body = args_to_json(&rest(&["--r2rml", path.to_str().unwrap()])).unwrap();
        assert_eq!(body["r2rml_type"], "text/turtle");
    }

    #[cfg(feature = "iceberg")]
    #[test]
    fn args_to_json_explicit_r2rml_type_overrides_extension() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("airlines.ttl");
        std::fs::write(&path, "@prefix rr: <http://www.w3.org/ns/r2rml#> .").unwrap();

        // Explicit flag wins over the conflicting `.ttl` extension.
        let body = args_to_json(&rest(&[
            "--r2rml",
            path.to_str().unwrap(),
            "--r2rml-type",
            "application/ld+json",
        ]))
        .unwrap();
        assert_eq!(body["r2rml_type"], "application/ld+json");
    }
}
