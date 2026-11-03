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
            e.context(format!(
                "failed to map Iceberg graph source on '{remote_name}'"
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
                .map_err(|e| e.context("failed to map Iceberg graph source"));
        }
    }

    // Local execution
    if StoredRefs::from_args(&args)?.is_some() {
        return Err(CliError::Usage(
            "--stored-catalog and --stored-mapping name a catalog connection or mapping that a \
             remote stores, and a local map has no such store. Map on the remote with \
             --remote <NAME>, or give the connection options and --r2rml <FILE> here"
                .into(),
        ));
    }
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
    let refs = StoredRefs::from_args(args)?;
    let body = match &refs {
        Some(refs) => refs.body(args)?,
        None => inline_body(args)?,
    };
    let result = client
        .iceberg_map(&body)
        .await
        .map_err(|e| map_refusal(e, refs.is_some()))?;

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
    let mapping = result
        .get("mapping_source")
        .and_then(|v| v.as_str())
        .unwrap_or("-");

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
        remote_connection_summary(&result, &args.mode)
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

    print_model_warnings(&model_warnings_of(&result));
    Ok(())
}

/// The `Connection:` line of a `map` report: whether every mapped table loaded
/// the way a query will load it, through the catalog or, in Direct mode, at the
/// table's location, within the registration check's time budget, and if not,
/// why. No outcome fails `map`.
fn connection_summary(check: &fluree_db_api::ConnectionCheck, mode: &str) -> String {
    check.summary(mode.eq_ignore_ascii_case("direct"))
}

/// [`connection_summary`] for a server's map response. A server that predates
/// `connection_check` sends only `connection_tested`, which cannot say why.
fn remote_connection_summary(result: &serde_json::Value, mode: &str) -> String {
    if let Some(check) = result
        .get("connection_check")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
    {
        return connection_summary(&check, mode);
    }
    let tested = result
        .get("connection_tested")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if tested {
        "verified".to_string()
    } else {
        "not verified: the server did not say why".to_string()
    }
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

/// The `iceberg/map` body for a remote server: by reference when the flags
/// name a stored catalog connection or mapping ([`StoredRefs`]), otherwise the
/// connection and mapping given inline.
pub fn args_to_json(args: &IcebergMapArgs) -> CliResult<serde_json::Value> {
    match StoredRefs::from_args(args)? {
        Some(refs) => refs.body(args),
        None => inline_body(args),
    }
}

/// An inline `iceberg/map` body: the source's connection as the structured
/// `catalog` / `io` its stored config holds (built by the same conversion a
/// local map uses, so remote and local store the same record), and the
/// mapping's text rather than its path.
#[cfg(feature = "iceberg")]
fn inline_body(args: &IcebergMapArgs) -> CliResult<serde_json::Value> {
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
    // The mapping's text, not its path.
    let (text, media_type) = read_mapping(inline_mapping(args)?, args.r2rml_type.as_deref())?;
    obj.insert("r2rml".into(), text.into());
    if let Some(v) = media_type {
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
fn inline_body(_args: &IcebergMapArgs) -> CliResult<serde_json::Value> {
    Err(CliError::Usage(
        "Iceberg support not compiled; the request a server takes is built by it. Rebuild \
         with `--features iceberg`."
            .into(),
    ))
}

/// A mapping file's text, sent instead of its path, and its media type: an
/// explicit `--r2rml-type` wins, otherwise the file extension's, so the
/// server records a concrete, self-describing media type (issue #1397).
fn read_mapping(
    path: &std::path::Path,
    r2rml_type: Option<&str>,
) -> CliResult<(String, Option<String>)> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        CliError::Input(format!("failed to read R2RML file {}: {e}", path.display()))
    })?;
    let media_type = r2rml_type
        .map(str::to_string)
        .or_else(|| infer_mapping_media_type(path));
    Ok((text, media_type))
}

/// The mapping file of a map that names its connection inline. Parsing
/// requires `--r2rml` unless `--stored-mapping` is given, and a stored mapping
/// never reaches an inline map, so this refuses only args built some other
/// way: a graph source needs a mapping, and an inline map has no other.
#[cfg(feature = "iceberg")]
fn inline_mapping(args: &IcebergMapArgs) -> CliResult<&std::path::Path> {
    args.r2rml.as_deref().ok_or_else(|| {
        CliError::Usage(
            "iceberg map needs a mapping: --r2rml <FILE>, or --stored-mapping <ID> on a remote \
             that stores mappings"
                .into(),
        )
    })
}

// =============================================================================
// fluree iceberg map, by reference
// =============================================================================

/// A map request that names what a remote already stores instead of restating
/// it: a catalog connection (`--stored-catalog`), a mapping
/// (`--stored-mapping`), or both. Parsing has already refused every inline
/// connection option beside a reference, and `--r2rml` beside a stored
/// mapping; these variants are the combinations left. Neither carries a
/// connection, so a by-reference body cannot carry a credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoredRefs {
    /// A stored connection, read with the mapping file given here and the
    /// table the caller names. A connection alone maps nothing, so the
    /// mapping is required, and the remote requires the table beside it.
    Catalog {
        catalog: String,
        mapping: std::path::PathBuf,
        table: String,
    },
    /// A stored mapping, which implies its connection and (unless `--table`
    /// picks another) its first table. A connection named too must be the
    /// mapping's own; the remote checks.
    Mapping {
        mapping: String,
        catalog: Option<String>,
    },
}

/// Every key a by-reference `iceberg/map` body may carry. A key outside it is
/// refused before any request, naming its option, rather than dropped or
/// sent; the remote refuses unknown keys beside a reference anyway. `r2rml` /
/// `r2rml_type` only ever travel with `stored_catalog_id`, since
/// [`StoredRefs::Mapping`] holds no mapping text.
pub(crate) const BY_REFERENCE_KEYS: &[&str] = &[
    "name",
    "table",
    "branch",
    "model",
    "default_allow",
    "stored_catalog_id",
    "stored_mapping_id",
    "r2rml",
    "r2rml_type",
];

impl StoredRefs {
    /// The references `args` names, or `None` for an inline map.
    pub(crate) fn from_args(args: &IcebergMapArgs) -> CliResult<Option<Self>> {
        match (&args.stored_catalog, &args.stored_mapping, &args.r2rml) {
            (None, None, _) => Ok(None),
            (catalog, Some(mapping), None) => Ok(Some(Self::Mapping {
                mapping: mapping.clone(),
                catalog: catalog.clone(),
            })),
            // clap refuses the pair; refuse it here too rather than pick one.
            (_, Some(_), Some(_)) => Err(CliError::Usage(
                "--stored-mapping and --r2rml are two mappings; give one".into(),
            )),
            (Some(catalog), None, Some(path)) => match &args.table {
                Some(table) => Ok(Some(Self::Catalog {
                    catalog: catalog.clone(),
                    mapping: path.clone(),
                    table: table.clone(),
                })),
                None => Err(CliError::Usage(
                    "--stored-catalog with --r2rml needs --table <namespace.table>: a stored \
                     connection is mapped one named table at a time"
                        .into(),
                )),
            },
            (Some(_), None, None) => Err(CliError::Usage(
                "--stored-catalog needs a mapping: --r2rml <FILE>, or --stored-mapping <ID> for \
                 one the remote stores. A stored connection alone does not say how its tables \
                 map to RDF"
                    .into(),
            )),
        }
    }

    /// The `iceberg/map` body: the references, the mapping text for a stored
    /// connection, and the source options. Never a connection.
    pub(crate) fn body(&self, args: &IcebergMapArgs) -> CliResult<serde_json::Value> {
        use serde_json::Value;
        let some = |v: &Option<String>| v.clone().map(Value::from);
        let mut fields: Vec<(&str, &str, Option<Value>)> = vec![
            ("name", "<NAME>", Some(args.name.clone().into())),
            ("branch", "--branch", some(&args.branch)),
            ("model", "--model", some(&args.model)),
            (
                "default_allow",
                "--default-allow",
                args.default_allow.map(Value::from),
            ),
        ];
        match self {
            Self::Catalog {
                catalog,
                mapping,
                table,
            } => {
                let (text, media_type) = read_mapping(mapping, args.r2rml_type.as_deref())?;
                fields.push(("table", "--table", Some(table.clone().into())));
                fields.push((
                    "stored_catalog_id",
                    "--stored-catalog",
                    Some(catalog.clone().into()),
                ));
                fields.push(("r2rml", "--r2rml", Some(text.into())));
                fields.push(("r2rml_type", "--r2rml-type", media_type.map(Value::from)));
            }
            Self::Mapping { mapping, catalog } => {
                // Optional: the mapping's first table unless one is named.
                fields.push(("table", "--table", some(&args.table)));
                fields.push((
                    "stored_mapping_id",
                    "--stored-mapping",
                    Some(mapping.clone().into()),
                ));
                fields.push(("stored_catalog_id", "--stored-catalog", some(catalog)));
            }
        }

        let mut body = serde_json::Map::new();
        for (key, option, value) in fields {
            let Some(value) = value else { continue };
            if !BY_REFERENCE_KEYS.contains(&key) {
                return Err(CliError::Usage(format!(
                    "{option} cannot be given with --stored-catalog or --stored-mapping"
                )));
            }
            body.insert(key.to_string(), value);
        }
        Ok(Value::Object(body))
    }
}

/// What a by-reference request appends to the remote's refusal of it.
pub(crate) const STORED_REFS_HINT: &str =
    "this remote may not support stored catalog connections or mappings";

/// A failed map request as a CLI error. A by-reference request the remote
/// refused as malformed or naming nothing it has (400, 404, 422) keeps the
/// remote's message and adds [`STORED_REFS_HINT`]: a server without stored
/// references refuses the keys, or the missing connection they replace. An
/// auth failure or a conflict says what it says.
fn map_refusal(e: crate::remote_client::RemoteLedgerError, by_reference: bool) -> CliError {
    use crate::remote_client::RemoteLedgerError as E;
    let refused = matches!(e, E::BadRequest(_) | E::NotFound(_) | E::ValidationError(_));
    if by_reference && refused {
        CliError::Remote(format!("{e}\n  hint: {STORED_REFS_HINT}"))
    } else {
        e.into()
    }
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

    let (mapping_content, mapping_media_type) =
        read_mapping(inline_mapping(&args)?, args.r2rml_type.as_deref())?;

    let config = fluree_db_api::R2rmlCreateConfig {
        iceberg: iceberg_config,
        mapping: fluree_db_api::R2rmlMappingInput::Content(mapping_content),
        mapping_media_type,
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
        connection_summary(&result.connection_check, &args.mode)
    );
    println!(
        "  Mapping:     {}",
        if result.mapping_validated {
            "validated"
        } else {
            "not validated (check mapping source)"
        }
    );

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
        // A mapping is required; a test that does not name one gets a file.
        if !args.contains(&"--r2rml") && !args.contains(&"--stored-mapping") {
            argv.extend(["--r2rml", test_mapping()]);
        }
        match crate::cli::Cli::try_parse_from(argv).unwrap().command {
            crate::cli::Commands::Iceberg {
                action: crate::cli::IcebergAction::Map(args),
            } => *args,
            _ => unreachable!(),
        }
    }

    /// A mapping file for tests that do not name one of their own.
    fn test_mapping() -> &'static str {
        static PATH: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        PATH.get_or_init(|| {
            let path = std::env::temp_dir().join(format!(
                "fluree-cli-iceberg-test-mapping-{}.ttl",
                std::process::id()
            ));
            std::fs::write(&path, "@prefix rr: <http://www.w3.org/ns/r2rml#> .").unwrap();
            path.to_string_lossy().into_owned()
        })
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

    /// The `Connection:` line says why a source is not verified, and a slow
    /// check reads as slow, not as a failure.
    #[test]
    fn connection_line_names_each_outcome() {
        use fluree_db_api::ConnectionCheck;
        let rest = |check: ConnectionCheck| connection_summary(&check, "rest");
        let direct = |check: ConnectionCheck| connection_summary(&check, "DIRECT");

        assert_eq!(rest(ConnectionCheck::Verified), "verified");
        assert_eq!(direct(ConnectionCheck::Verified), "verified");

        let timed_out = ConnectionCheck::TimedOut {
            loaded: 2,
            total: 5,
            budget_secs: 10,
        };
        assert_eq!(
            rest(timed_out.clone()),
            "not verified: the check ran past 10 s (2 of 5 tables loaded); \
             the tables may only be slow"
        );
        assert_eq!(rest(timed_out.clone()), direct(timed_out));

        let missing = ConnectionCheck::TableFailed {
            table: "sales.nope".to_string(),
            error: "Table not found: sales.nope".to_string(),
        };
        assert_eq!(
            rest(missing.clone()),
            "not verified: table 'sales.nope': it did not load through the catalog; \
             queries that read it fail until it loads (Table not found: sales.nope)"
        );
        assert_eq!(
            direct(missing),
            "not verified: table 'sales.nope': its metadata could not be read at its \
             location; queries that read it fail until it loads (Table not found: sales.nope)"
        );

        let unreachable = ConnectionCheck::CatalogUnreachable {
            table: "sales.orders".to_string(),
            error: "Connection error: connection refused".to_string(),
        };
        assert_eq!(
            rest(unreachable),
            "not verified: table 'sales.orders': the catalog did not answer; queries that \
             read it fail until it loads (Connection error: connection refused)"
        );

        let client = ConnectionCheck::ClientFailed {
            error: "no secret resolver".to_string(),
        };
        assert_eq!(
            rest(client),
            "not verified: the catalog client could not be built, so no table was loaded \
             (no secret resolver)"
        );
        assert_eq!(
            rest(ConnectionCheck::NoTables),
            "not verified: the mapping could not be read, so no table was loaded"
        );
    }

    /// A server's `connection_check` reaches the line; one that sends only
    /// `connection_tested` (an older server) still gets an answer.
    #[test]
    fn remote_connection_line_reads_the_servers_outcome() {
        let timed_out = serde_json::json!({
            "connection_tested": false,
            "connection_check": {"outcome": "timed_out", "loaded": 0, "total": 3, "budget_secs": 10},
        });
        assert_eq!(
            remote_connection_summary(&timed_out, "glue"),
            "not verified: the check ran past 10 s (0 of 3 tables loaded); \
             the tables may only be slow"
        );
        let verified = serde_json::json!({
            "connection_tested": true,
            "connection_check": {"outcome": "verified"},
        });
        assert_eq!(remote_connection_summary(&verified, "rest"), "verified");
        let older = serde_json::json!({"connection_tested": false});
        assert_eq!(
            remote_connection_summary(&older, "rest"),
            "not verified: the server did not say why"
        );
        assert_eq!(
            remote_connection_summary(&serde_json::json!({"connection_tested": true}), "rest"),
            "verified"
        );
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
                "--r2rml",
                "m.ttl",
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
        std::fs::write(&wif, WIF_CONFIG).unwrap();
        let wif_auth = auth(&["--auth-google-wif-config", wif.to_str().unwrap()]);
        assert_eq!(
            wif_auth["credentials"]["config"]["type"],
            "external_account"
        );
        std::fs::write(&wif, "[]").unwrap();
        let e = refusal(&rest(&["--auth-google-wif-config", wif.to_str().unwrap()]));
        assert!(e.contains("does not hold a JSON object"), "{e}");
    }

    /// What `gcloud iam workload-identity-pools create-cred-config … --aws`
    /// writes.
    #[cfg(feature = "iceberg")]
    const WIF_CONFIG: &str = r#"{
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/aws",
        "subject_token_type": "urn:ietf:params:aws:token-type:aws4_request",
        "token_url": "https://sts.googleapis.com/v1/token",
        "credential_source": {
            "environment_id": "aws1",
            "region_url": "http://169.254.169.254/latest/meta-data/placement/availability-zone",
            "url": "http://169.254.169.254/latest/meta-data/iam/security-credentials",
            "regional_cred_verification_url": "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15"
        }
    }"#;

    #[cfg(feature = "iceberg")]
    #[test]
    fn a_wif_config_flag_refuses_a_key_file_before_any_request_is_built() {
        // The remote body carries the file as read; a service-account key
        // there would travel to the server before the server refused it.
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("wif.json");
        let path = file.to_str().unwrap();
        for (text, refused) in [
            (
                r#"{"type": "service_account", "private_key": "KEY-MATERIAL"}"#,
                "google-service-account-env instead",
            ),
            (r#"{"audience": "a"}"#, "must be `external_account`"),
            // An external account the server would refuse, refused here.
            (
                r#"{"type": "external_account", "audience": "a"}"#,
                "subject_token_type",
            ),
        ] {
            std::fs::write(&file, text).unwrap();
            for args in [
                rest(&["--auth-google-wif-config", path]),
                parse(&[
                    "--mode",
                    "direct",
                    "--table-location",
                    "gs://b/w/ns/t",
                    "--storage-google-wif-config",
                    path,
                ]),
            ] {
                let e = args_to_json(&args).unwrap_err().to_string();
                assert!(e.contains(refused), "{text}: {e}");
                assert!(!e.contains("KEY-MATERIAL"), "{e}");
            }
        }
        std::fs::write(&file, WIF_CONFIG).unwrap();
        assert!(args_to_json(&rest(&["--auth-google-wif-config", path])).is_ok());
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

    // -------------------------------------------------------------------------
    // By reference: --stored-catalog / --stored-mapping
    // -------------------------------------------------------------------------

    /// `fluree iceberg map gs <args>` as clap parses it, or clap's refusal.
    fn try_parse(args: &[&str]) -> Result<IcebergMapArgs, clap::Error> {
        use clap::Parser;
        let mut argv = vec!["fluree", "iceberg", "map", "gs"];
        argv.extend_from_slice(args);
        crate::cli::Cli::try_parse_from(argv).map(|cli| match cli.command {
            crate::cli::Commands::Iceberg {
                action: crate::cli::IcebergAction::Map(args),
            } => *args,
            _ => unreachable!(),
        })
    }

    /// A mapping file on disk, and its path.
    fn mapping_file() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("orders.ttl");
        std::fs::write(&path, "@prefix rr: <http://www.w3.org/ns/r2rml#> .").unwrap();
        let path = path.to_str().unwrap().to_string();
        (dir, path)
    }

    /// Every key of a by-reference body is one [`BY_REFERENCE_KEYS`] allows,
    /// and none describes a connection. An older server reads a body with no
    /// `catalog_uri` as a rest-mode map missing its catalog and refuses it, so
    /// a body that never carries one can never register a source there.
    fn assert_by_reference(body: &serde_json::Value) {
        let obj = body.as_object().expect("an object");
        for key in obj.keys() {
            assert!(
                BY_REFERENCE_KEYS.contains(&key.as_str()),
                "{key} in a by-reference body: {body}"
            );
        }
        for key in ["mode", "catalog_uri", "catalog", "io"] {
            assert!(obj.get(key).is_none(), "{key} in {body}");
        }
    }

    #[test]
    fn a_stored_catalog_is_sent_with_the_mapping_text() {
        let (_dir, path) = mapping_file();
        let body = args_to_json(&parse(&[
            "--stored-catalog",
            "cat-1",
            "--r2rml",
            &path,
            "--table",
            "sales.orders",
            "--branch",
            "dev",
            "--model",
            "governance:main",
            "--default-allow",
            "false",
        ]))
        .unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "name": "gs",
                "stored_catalog_id": "cat-1",
                "r2rml": "@prefix rr: <http://www.w3.org/ns/r2rml#> .",
                "r2rml_type": "text/turtle",
                "table": "sales.orders",
                "branch": "dev",
                "model": "governance:main",
                "default_allow": false,
            })
        );
        assert_by_reference(&body);
    }

    #[test]
    fn a_stored_mapping_alone_is_sent_as_its_id() {
        let body = args_to_json(&parse(&["--stored-mapping", "map-1"])).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"name": "gs", "stored_mapping_id": "map-1"})
        );
        // A table picks one of the mapping's tables; the remote checks it.
        let body = args_to_json(&parse(&[
            "--stored-mapping",
            "map-1",
            "--table",
            "sales.orders",
        ]))
        .unwrap();
        assert_eq!(body["table"], "sales.orders");
        assert_by_reference(&body);
    }

    #[test]
    fn both_references_are_sent_together() {
        let body = args_to_json(&parse(&[
            "--stored-catalog",
            "cat-1",
            "--stored-mapping",
            "map-1",
        ]))
        .unwrap();
        assert_eq!(
            body,
            serde_json::json!({
                "name": "gs",
                "stored_catalog_id": "cat-1",
                "stored_mapping_id": "map-1",
            })
        );
        assert_by_reference(&body);
    }

    /// Every map needs a mapping, and parsing names both ways to give one. A
    /// stored connection does not stand in for it.
    #[test]
    fn a_map_without_a_mapping_is_refused_by_parsing() {
        for argv in [
            &["--catalog-uri", "https://c", "--table", "sales.orders"][..],
            &["--stored-catalog", "cat-1"][..],
            &["--stored-catalog", "cat-1", "--table", "sales.orders"][..],
        ] {
            let e = try_parse(argv).expect_err("refused");
            assert_eq!(
                e.kind(),
                clap::error::ErrorKind::MissingRequiredArgument,
                "{argv:?}: {e}"
            );
            let e = e.to_string();
            assert!(e.contains("--r2rml <R2RML>"), "{e}");
            assert!(e.contains("--stored-mapping <ID>"), "{e}");
        }
    }

    /// Args built without parsing are refused too: a stored connection
    /// alone, or an inline connection with no mapping file.
    #[cfg(feature = "iceberg")]
    #[test]
    fn a_map_without_a_mapping_is_refused_when_built_directly() {
        let mut args = parse(&["--stored-catalog", "cat-1", "--table", "sales.orders"]);
        args.r2rml = None;
        let e = args_to_json(&args).unwrap_err().to_string();
        assert!(e.contains("--stored-catalog needs a mapping"), "{e}");

        let mut args = rest(&[]);
        args.r2rml = None;
        let e = args_to_json(&args).unwrap_err().to_string();
        assert!(e.contains("iceberg map needs a mapping"), "{e}");
    }

    #[test]
    fn a_stored_catalog_with_a_mapping_file_needs_a_table() {
        let (_dir, path) = mapping_file();
        let e = args_to_json(&parse(&["--stored-catalog", "cat-1", "--r2rml", &path]))
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("--stored-catalog with --r2rml needs --table"),
            "{e}"
        );
    }

    #[test]
    fn a_blank_stored_id_is_refused_by_parsing() {
        for flag in ["--stored-catalog", "--stored-mapping"] {
            for id in ["", "  "] {
                let e = try_parse(&[flag, id]).expect_err("refused");
                assert_eq!(e.kind(), clap::error::ErrorKind::ValueValidation, "{e}");
            }
        }
    }

    /// The options a reference leaves the caller. Every other `iceberg map`
    /// option describes the connection, so it must conflict with both
    /// references: a new connection option that does not is caught here
    /// rather than sent beside a reference.
    const REFERENCE_COMPATIBLE: &[&str] = &[
        "name",
        "remote",
        "table",
        "branch",
        "model",
        "default_allow",
        "r2rml",
        "r2rml_type",
        "stored_catalog",
        "stored_mapping",
    ];

    /// The argv that gives `arg` once, with a value it accepts.
    fn give(arg: &clap::Arg) -> Vec<String> {
        let long = format!("--{}", arg.get_long().expect("a long option"));
        if !arg.get_action().takes_values() {
            return vec![long];
        }
        let value = arg
            .get_possible_values()
            .first()
            .map(|v| v.get_name().to_string())
            .unwrap_or_else(|| "x".to_string());
        vec![long, value]
    }

    #[test]
    fn every_connection_option_conflicts_with_a_reference() {
        use clap::CommandFactory;
        let cli = crate::cli::Cli::command();
        let map = cli
            .find_subcommand("iceberg")
            .and_then(|c| c.find_subcommand("map"))
            .expect("iceberg map");
        let connection: Vec<&clap::Arg> = map
            .get_arguments()
            .filter(|a| !a.is_global_set() && !a.is_positional())
            .filter(|a| !REFERENCE_COMPATIBLE.contains(&a.get_id().as_str()))
            .collect();
        // The sweep reaches the flattened auth and storage groups.
        for id in ["mode", "catalog_uri", "auth_bearer", "auth_google_adc"] {
            assert!(connection.iter().any(|a| a.get_id() == id), "{id}");
        }
        assert!(connection
            .iter()
            .any(|a| a.get_id() == "storage_google_metadata"));
        for arg in connection {
            for reference in [["--stored-catalog", "c"], ["--stored-mapping", "m"]] {
                let mut argv: Vec<String> = reference.iter().map(ToString::to_string).collect();
                argv.extend(give(arg));
                let argv: Vec<&str> = argv.iter().map(String::as_str).collect();
                let e = try_parse(&argv)
                    .err()
                    .unwrap_or_else(|| panic!("{} accepted beside {}", arg.get_id(), reference[0]));
                assert_eq!(
                    e.kind(),
                    clap::error::ErrorKind::ArgumentConflict,
                    "{argv:?}: {e}"
                );
            }
        }
    }

    #[test]
    fn a_stored_mapping_refuses_an_inline_mapping() {
        let (_dir, path) = mapping_file();
        for extra in [["--r2rml", path.as_str()], ["--r2rml-type", "text/turtle"]] {
            let mut argv = vec!["--stored-mapping", "m"];
            argv.extend_from_slice(&extra);
            let e = try_parse(&argv).expect_err("refused");
            assert_eq!(e.kind(), clap::error::ErrorKind::ArgumentConflict, "{e}");
        }
        // A stored connection takes one.
        assert!(try_parse(&["--stored-catalog", "c", "--r2rml", &path]).is_ok());
    }

    #[test]
    fn a_refused_reference_carries_the_hint() {
        use crate::remote_client::RemoteLedgerError as E;
        let refused = || E::BadRequest("stored_catalog_id is not a field of this request".into());
        let e = map_refusal(refused(), true).to_string();
        assert!(e.contains("stored_catalog_id is not a field"), "{e}");
        assert!(e.contains(STORED_REFS_HINT), "{e}");
        for e in [
            E::NotFound("no route".into()),
            E::ValidationError("v".into()),
        ] {
            assert!(map_refusal(e, true).to_string().contains(STORED_REFS_HINT));
        }
        // An inline map, an auth failure and a conflict say only what they say.
        assert!(!map_refusal(refused(), false)
            .to_string()
            .contains(STORED_REFS_HINT));
        for e in [E::Unauthorized, E::Conflict("exists".into())] {
            assert!(!map_refusal(e, true).to_string().contains(STORED_REFS_HINT));
        }
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
