//! `fluree delta` — register a Delta Lake graph source, and the read-only
//! commands that lead up to it: `browse`, `preview`, `verify`, `generate` and
//! `validate`.
//!
//! `fluree delta list|info|drop` share the mapped-source implementations in
//! [`super::iceberg`].

use crate::cli::{
    DeltaBrowseArgs, DeltaBrowseDepth, DeltaGenerateArgs, DeltaMapArgs, DeltaS3Args,
    DeltaSourceArgs, DeltaTableArgs, DeltaUnityArgs, DeltaValidateArgs,
};
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub async fn run_delta_map(args: DeltaMapArgs, dirs: &FlureeDir, direct: bool) -> CliResult<()> {
    if let Some(remote_name) = args.remote.as_deref() {
        let client = crate::context::build_remote_client(remote_name, dirs).await?;
        let result = run_delta_map_remote(&client, &args).await.map_err(|e| {
            CliError::Remote(format!(
                "failed to map Delta graph source on '{remote_name}': {e}"
            ))
        });
        crate::context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return result;
    }

    if !direct {
        if let Some(client) = crate::context::try_server_route_client(dirs) {
            return run_delta_map_remote(&client, &args)
                .await
                .map_err(|e| CliError::Remote(format!("failed to map Delta graph source: {e}")));
        }
    }

    run_delta_map_local(args, dirs).await
}

fn read_mapping(source: &DeltaSourceArgs) -> CliResult<String> {
    std::fs::read_to_string(&source.r2rml).map_err(|e| {
        CliError::Input(format!(
            "Failed to read R2RML mapping file '{}': {e}",
            source.r2rml.display()
        ))
    })
}

fn mapping_media_type(source: &DeltaSourceArgs) -> Option<String> {
    source
        .r2rml_type
        .clone()
        .or_else(|| super::iceberg::infer_mapping_media_type(&source.r2rml))
}

fn table_pairs(source: &DeltaSourceArgs) -> CliResult<BTreeMap<String, String>> {
    source
        .table
        .iter()
        .map(|kv| pair("--table", "NAME=LOCATION", kv))
        .collect()
}

/// `KEY=VALUE`, both non-empty.
fn pair(flag: &str, shape: &str, given: &str) -> CliResult<(String, String)> {
    given
        .split_once('=')
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .filter(|(k, v)| !k.is_empty() && !v.is_empty())
        .ok_or_else(|| CliError::Usage(format!("{flag} expects {shape}, got '{given}'")))
}

fn require_location(source: &DeltaSourceArgs) -> CliResult<()> {
    if source.root.is_none() && source.table.is_empty() && source.unity.unity_uri.is_none() {
        return Err(CliError::Usage(
            "give --root, --unity-uri, and/or one or more --table NAME=LOCATION".to_string(),
        ));
    }
    Ok(())
}

fn insert_set(body: &mut Value, fields: &[(&str, &Option<String>)]) {
    let obj = body.as_object_mut().expect("a JSON object");
    for (key, value) in fields {
        if let Some(v) = value {
            obj.insert((*key).into(), v.clone().into());
        }
    }
}

/// The connection under the field names every Delta endpoint gives it.
fn unity_json(unity: &DeltaUnityArgs, body: &mut Value) {
    insert_set(
        body,
        &[
            ("unity_uri", &unity.unity_uri),
            ("unity_catalog", &unity.unity_catalog),
            ("unity_schema", &unity.unity_schema),
            ("auth_bearer", &unity.auth_bearer),
            ("auth_bearer_env", &unity.auth_bearer_env),
            ("oauth2_client_id", &unity.oauth2_client_id),
            ("oauth2_client_secret", &unity.oauth2_client_secret),
            ("oauth2_client_secret_env", &unity.oauth2_client_secret_env),
            ("oauth2_token_url", &unity.oauth2_token_url),
            ("oauth2_scope", &unity.oauth2_scope),
        ],
    );
}

fn s3_json(s3: &DeltaS3Args, body: &mut Value) {
    insert_set(
        body,
        &[
            ("s3_region", &s3.s3_region),
            ("s3_endpoint", &s3.s3_endpoint),
        ],
    );
    if s3.s3_path_style {
        body["s3_path_style"] = true.into();
    }
}

/// What `delta/map` and `delta/r2rml/validate` both take.
fn source_json(source: &DeltaSourceArgs) -> CliResult<Value> {
    require_location(source)?;
    let mut body = json!({ "r2rml": read_mapping(source)? });
    insert_set(
        &mut body,
        &[
            ("r2rml_type", &mapping_media_type(source)),
            ("root", &source.root),
            ("azure_tenant_id", &source.azure_tenant_id),
            ("azure_client_id", &source.azure_client_id),
            ("azure_client_secret", &source.azure_client_secret),
            ("azure_client_secret_env", &source.azure_client_secret_env),
        ],
    );
    s3_json(&source.s3, &mut body);
    unity_json(&source.unity, &mut body);
    let tables = table_pairs(source)?;
    if !tables.is_empty() {
        body["tables"] = serde_json::to_value(tables).unwrap();
    }
    Ok(body)
}

fn args_to_json(args: &DeltaMapArgs) -> CliResult<Value> {
    let mut body = source_json(&args.source)?;
    body["name"] = args.name.clone().into();
    insert_set(
        &mut body,
        &[("branch", &args.branch), ("model", &args.model)],
    );
    if let Some(v) = args.default_allow {
        body["default_allow"] = v.into();
    }
    Ok(body)
}

async fn run_delta_map_remote(
    client: &crate::remote_client::RemoteLedgerClient,
    args: &DeltaMapArgs,
) -> CliResult<()> {
    let body = args_to_json(args)?;
    let result = client.delta_map(&body).await?;
    let text = |k: &str| {
        result
            .get(k)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_string()
    };
    let versions: BTreeMap<String, u64> = result
        .get("table_versions")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    print_created(
        &text("graph_source_id"),
        &text("mapping_source"),
        result
            .get("triples_map_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as usize,
        &strings(&result, "table_names"),
        &versions,
        &strings(&result, "table_warnings"),
        result
            .get("mapping_validated")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    );
    super::iceberg::print_model_warnings(&super::iceberg::model_warnings_of(&result));
    Ok(())
}

fn strings(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|t| t.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(feature = "delta")]
fn unity_config(unity: &DeltaUnityArgs) -> CliResult<Option<fluree_db_api::DeltaUnityConfig>> {
    let secret = super::iceberg::secret_value;
    Ok(fluree_db_api::DeltaUnityFields {
        uri: unity.unity_uri.clone(),
        catalog: unity.unity_catalog.clone(),
        schema: unity.unity_schema.clone(),
        bearer: secret(&unity.auth_bearer, &unity.auth_bearer_env),
        oauth2_client_id: unity.oauth2_client_id.clone(),
        oauth2_client_secret: secret(&unity.oauth2_client_secret, &unity.oauth2_client_secret_env),
        oauth2_token_url: unity.oauth2_token_url.clone(),
        oauth2_scope: unity.oauth2_scope.clone(),
    }
    .into_config()?)
}

#[cfg(feature = "delta")]
fn create_config(
    name: &str,
    source: &DeltaSourceArgs,
) -> CliResult<fluree_db_api::DeltaCreateConfig> {
    require_location(source)?;
    let azure = fluree_db_api::DeltaAzureFields {
        tenant_id: source.azure_tenant_id.clone(),
        client_id: source.azure_client_id.clone(),
        client_secret: source.azure_client_secret.clone(),
        client_secret_env: source.azure_client_secret_env.clone(),
    }
    .into_auth()?;
    Ok(fluree_db_api::DeltaCreateConfig {
        unity: unity_config(&source.unity)?,
        name: name.to_string(),
        branch: None,
        root: source.root.clone(),
        tables: table_pairs(source)?,
        io: fluree_db_api::DeltaIoConfig {
            s3_region: source.s3.s3_region.clone(),
            s3_endpoint: source.s3.s3_endpoint.clone(),
            s3_path_style: source.s3.s3_path_style,
            azure,
        },
        mapping: fluree_db_api::R2rmlMappingInput::Content(read_mapping(source)?),
        mapping_media_type: mapping_media_type(source),
        model: None,
        default_allow: None,
    })
}

#[cfg(feature = "delta")]
async fn run_delta_map_local(args: DeltaMapArgs, dirs: &FlureeDir) -> CliResult<()> {
    let fluree = crate::context::build_fluree(dirs)?;
    let config = fluree_db_api::DeltaCreateConfig {
        branch: args.branch.clone(),
        model: args.model.clone(),
        default_allow: args.default_allow,
        ..create_config(&args.name, &args.source)?
    };
    let result = fluree.create_delta_graph_source(config).await?;
    print_created(
        &result.graph_source_id,
        &result.mapping_source,
        result.triples_map_count,
        &result.table_names,
        &result.table_versions,
        &result.table_warnings,
        result.mapping_validated,
    );
    super::iceberg::print_model_warnings(&result.model_warnings);
    Ok(())
}

#[cfg(not(feature = "delta"))]
const NOT_COMPILED: &str = "Delta graph source support not compiled. Rebuild with `--features \
                            delta`, or run against a server that has it (`--remote`).";

#[cfg(not(feature = "delta"))]
async fn run_delta_map_local(_args: DeltaMapArgs, _dirs: &FlureeDir) -> CliResult<()> {
    Err(CliError::Usage(NOT_COMPILED.into()))
}

// =============================================================================
// Read-only: the catalog, and a mapping before it is registered
// =============================================================================

/// What a read-only command asks, in each of the two places it can be asked.
enum Ask<'a> {
    Browse(&'a DeltaBrowseArgs),
    Preview(&'a DeltaTableArgs),
    Verify(&'a DeltaTableArgs),
    Generate(&'a DeltaGenerateArgs),
    Validate(&'a DeltaValidateArgs),
}

impl Ask<'_> {
    fn route(&self) -> &'static str {
        match self {
            Self::Browse(_) => "catalog/browse",
            Self::Preview(_) => "catalog/preview",
            Self::Verify(_) => "catalog/verify",
            Self::Generate(_) => "r2rml/generate",
            Self::Validate(_) => "r2rml/validate",
        }
    }

    fn remote(&self) -> Option<&str> {
        match self {
            Self::Browse(a) => a.remote.as_deref(),
            Self::Preview(a) | Self::Verify(a) => a.remote.as_deref(),
            Self::Generate(a) => a.remote.as_deref(),
            Self::Validate(a) => a.remote.as_deref(),
        }
    }

    /// The catalog, for the commands that are about one.
    fn unity(&self) -> Option<&DeltaUnityArgs> {
        match self {
            Self::Browse(a) => Some(&a.unity),
            Self::Preview(a) | Self::Verify(a) => Some(&a.unity),
            Self::Generate(a) => Some(&a.unity),
            Self::Validate(_) => None,
        }
    }

    fn body(&self) -> CliResult<Value> {
        if self.unity().is_some_and(|u| u.unity_uri.is_none()) {
            return Err(CliError::Usage("--unity-uri is required".to_string()));
        }
        let mut body = match self {
            Self::Validate(a) => return source_json(&a.source),
            Self::Browse(a) => json!({
                "depth": match a.depth {
                    DeltaBrowseDepth::Schemas => "schemas",
                    DeltaBrowseDepth::Tables => "tables",
                },
            }),
            Self::Preview(a) => json!({ "table": a.table }),
            Self::Verify(a) => {
                let mut body = json!({ "table": a.table });
                s3_json(&a.s3, &mut body);
                body
            }
            Self::Generate(a) => json!({
                "tables": a.tables,
                "base_namespace": a.base_namespace,
                "per_table_overrides": overrides(a)?,
                "options": {
                    "emit_fk_joins": !a.no_joins,
                    "subject_strategy": if a.strict_subjects { "identifier" } else { "auto" },
                },
            }),
        };
        if let Some(unity) = self.unity() {
            unity_json(unity, &mut body);
        }
        Ok(body)
    }

    #[cfg(feature = "delta")]
    async fn local(&self, dirs: &FlureeDir) -> CliResult<Value> {
        let fluree = crate::context::build_fluree(dirs)?;
        let unity = match self.unity() {
            Some(unity) => unity_config(unity)?,
            None => None,
        };
        let unity = || unity.clone().expect("checked by `body`");
        let answer = match self {
            Self::Browse(a) => {
                let depth = match a.depth {
                    DeltaBrowseDepth::Schemas => fluree_db_api::DeltaBrowseDepth::Schemas,
                    DeltaBrowseDepth::Tables => fluree_db_api::DeltaBrowseDepth::Tables,
                };
                serde_json::to_value(fluree.browse_delta_unity(&unity(), depth).await?)
            }
            Self::Preview(a) => {
                serde_json::to_value(fluree.preview_delta_unity_table(&unity(), &a.table).await?)
            }
            Self::Verify(a) => {
                let io = fluree_db_api::DeltaIoConfig {
                    s3_region: a.s3.s3_region.clone(),
                    s3_endpoint: a.s3.s3_endpoint.clone(),
                    s3_path_style: a.s3.s3_path_style,
                    azure: None,
                };
                serde_json::to_value(
                    fluree
                        .verify_delta_unity_table(&unity(), &io, &a.table)
                        .await?,
                )
            }
            Self::Generate(a) => {
                let request: GenerateBody = serde_json::from_value(self.body()?)
                    .map_err(|e| CliError::Usage(e.to_string()))?;
                let req = fluree_db_api::GenerateDeltaR2rmlRequest {
                    unity: unity(),
                    tables: a.tables.clone(),
                    base_namespace: a.base_namespace.clone(),
                    per_table_overrides: request
                        .per_table_overrides
                        .into_iter()
                        .map(|o| {
                            let table_override = fluree_db_api::TableOverride {
                                primary_key: o.subject_key,
                                class_name: o.class_name,
                                subject_strategy: None,
                            };
                            (o.table, table_override)
                        })
                        .collect(),
                    options: request.options,
                };
                serde_json::to_value(fluree.generate_delta_r2rml(req).await?)
            }
            Self::Validate(a) => serde_json::to_value(
                fluree
                    .validate_delta_r2rml(&create_config("validate", &a.source)?)
                    .await?,
            ),
        };
        answer.map_err(|e| CliError::Config(e.to_string()))
    }

    #[cfg(not(feature = "delta"))]
    async fn local(&self, _dirs: &FlureeDir) -> CliResult<Value> {
        Err(CliError::Usage(NOT_COMPILED.into()))
    }

    /// On the named remote, else the local server when there is one, else here.
    async fn answer(&self, dirs: &FlureeDir, direct: bool) -> CliResult<Value> {
        let body = self.body()?;
        let failed = |e| CliError::Remote(format!("delta {}: {e}", self.route()));
        if let Some(remote_name) = self.remote() {
            let client = crate::context::build_remote_client(remote_name, dirs).await?;
            let result = client.delta_read(self.route(), &body).await.map_err(failed);
            crate::context::persist_refreshed_tokens(&client, remote_name, dirs).await;
            return result;
        }
        if !direct {
            if let Some(client) = crate::context::try_server_route_client(dirs) {
                return client.delta_read(self.route(), &body).await.map_err(failed);
            }
        }
        self.local(dirs).await
    }
}

#[cfg(feature = "delta")]
#[derive(serde::Deserialize)]
struct GenerateBody {
    per_table_overrides: Vec<OverrideBody>,
    options: fluree_db_api::GenerateOptions,
}

#[cfg(feature = "delta")]
#[derive(serde::Deserialize)]
struct OverrideBody {
    table: String,
    subject_key: Option<Vec<String>>,
    class_name: Option<String>,
}

/// `--subject-key` and `--class-name`, one entry per table named.
fn overrides(args: &DeltaGenerateArgs) -> CliResult<Vec<Value>> {
    let mut by_table: BTreeMap<String, Value> = BTreeMap::new();
    let given = args
        .subject_key
        .iter()
        .map(|g| ("--subject-key", "TABLE=COLUMN[,COLUMN]", g))
        .chain(
            args.class_name
                .iter()
                .map(|g| ("--class-name", "TABLE=NAME", g)),
        );
    for (flag, shape, given) in given {
        let (table, value) = pair(flag, shape, given)?;
        if !args.tables.contains(&table) {
            return Err(CliError::Usage(format!(
                "{flag} names '{table}', which is not among the tables"
            )));
        }
        let entry = by_table
            .entry(table.clone())
            .or_insert_with(|| json!({ "table": table }));
        if flag == "--subject-key" {
            let columns: Vec<&str> = value.split(',').map(str::trim).collect();
            entry["subject_key"] = json!(columns);
        } else {
            entry["class_name"] = value.into();
        }
    }
    Ok(by_table.into_values().collect())
}

fn print_json(value: &Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("")
}

pub async fn run_delta_browse(
    args: DeltaBrowseArgs,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let listing = Ask::Browse(&args).answer(dirs, direct).await?;
    if args.json {
        print_json(&listing);
    } else {
        print!("{}", listing_text(&listing));
    }
    Ok(())
}

fn listing_text(listing: &Value) -> String {
    let mut out = String::new();
    for (title, key) in [("Catalogs", "catalogs"), ("Schemas", "schemas")] {
        let names = strings(listing, key);
        if !names.is_empty() {
            out.push_str(&format!("{title}:\n"));
            for name in names {
                out.push_str(&format!("  {name}\n"));
            }
        }
    }
    let tables = listing["tables"].as_array().cloned().unwrap_or_default();
    if !tables.is_empty() {
        out.push_str("Tables:\n");
    }
    for table in &tables {
        out.push_str(&format!("  {}", text(table, "full_name")));
        let unreadable = text(table, "unreadable");
        if !unreadable.is_empty() {
            out.push_str(&format!("  (not readable: {unreadable})"));
        } else if !text(table, "access_rule").is_empty() {
            out.push_str(&format!(
                "  (has a {}; Unity may not issue credentials for it)",
                text(table, "access_rule")
            ));
        }
        out.push('\n');
    }
    if out.is_empty() {
        out.push_str("Nothing listed.\n");
    }
    out
}

pub async fn run_delta_preview(
    args: DeltaTableArgs,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let table = Ask::Preview(&args).answer(dirs, direct).await?;
    if args.json {
        print_json(&table);
    } else {
        print!("{}", preview_text(&table));
    }
    Ok(())
}

fn preview_text(table: &Value) -> String {
    let mut out = format!(
        "{}  [{} {}]\n",
        text(table, "full_name"),
        text(table, "kind"),
        text(table, "format")
    );
    for (label, key) in [
        ("Location", "location"),
        ("Comment", "comment"),
        ("Access rule", "access_rule"),
        ("Not readable", "unreadable"),
    ] {
        if !text(table, key).is_empty() {
            out.push_str(&format!("  {label}: {}\n", text(table, key)));
        }
    }
    let columns = table["columns"].as_array().cloned().unwrap_or_default();
    let width = |key: &str| {
        columns
            .iter()
            .map(|c| text(c, key).len())
            .max()
            .unwrap_or(0)
    };
    let (name_w, type_w) = (width("name"), width("type_text"));
    out.push_str("  Columns:\n");
    for column in &columns {
        let mut notes = Vec::new();
        if column["nullable"] == false {
            notes.push("not null".to_string());
        }
        if column["mappable"] == false {
            notes.push("cannot be mapped".to_string());
        }
        if column["masked"] == true {
            notes.push("masked".to_string());
        }
        if !text(column, "comment").is_empty() {
            notes.push(format!("-- {}", text(column, "comment")));
        }
        out.push_str(&format!(
            "    {:name_w$}  {:type_w$}  {:12}  {}\n",
            text(column, "name"),
            text(column, "type_text"),
            text(column, "xsd_type"),
            notes.join(", ")
        ));
    }
    let key = strings(table, "primary_key");
    if !key.is_empty() {
        out.push_str(&format!("  Primary key: {}\n", key.join(", ")));
    }
    for fk in table["foreign_keys"].as_array().into_iter().flatten() {
        out.push_str(&format!(
            "  Foreign key: {} -> {} ({})\n",
            strings(fk, "columns").join(", "),
            text(fk, "parent_table"),
            strings(fk, "parent_columns").join(", ")
        ));
    }
    out
}

pub async fn run_delta_verify(
    args: DeltaTableArgs,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let access = Ask::Verify(&args).answer(dirs, direct).await?;
    if args.json {
        print_json(&access);
    } else if access["readable"] == true {
        println!("{} is readable", text(&access, "full_name"));
        println!("  Location:   {}", text(&access, "location"));
        println!("  Version:    {}", access["version"]);
        println!("  Data files: {}", access["data_file_count"]);
        if let Some(probed) = access["probed_data_file"].as_str() {
            println!("  Read check: {probed}");
        }
    } else {
        println!("{} is not readable", text(&access, "full_name"));
        println!("  {}", text(&access, "error"));
    }
    if access["readable"] == true {
        Ok(())
    } else {
        Err(CliError::ExitCode(1))
    }
}

/// Diagnostics on standard error; true if any is an error.
fn report_diagnostics(answer: &Value) -> bool {
    let mut any_error = false;
    for d in answer["diagnostics"].as_array().into_iter().flatten() {
        let severity = text(d, "severity");
        any_error |= severity == "error";
        let place = match (text(d, "table"), text(d, "column")) {
            ("", _) => String::new(),
            (table, "") => format!(" {table}:"),
            (table, column) => format!(" {table}.{column}:"),
        };
        eprintln!("{severity}:{place} {}", text(d, "message"));
    }
    any_error
}

pub async fn run_delta_generate(
    args: DeltaGenerateArgs,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let answer = Ask::Generate(&args).answer(dirs, direct).await?;
    if args.json {
        print_json(&answer);
        return Ok(());
    }
    report_diagnostics(&answer);
    match &args.output {
        Some(path) => {
            std::fs::write(path, text(&answer, "turtle")).map_err(|e| {
                CliError::Input(format!("Failed to write '{}': {e}", path.display()))
            })?;
            eprintln!(
                "Wrote a mapping of {} table(s) to {}",
                strings(&answer, "tables").len(),
                path.display()
            );
        }
        None => print!("{}", text(&answer, "turtle")),
    }
    Ok(())
}

pub async fn run_delta_validate(
    args: DeltaValidateArgs,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    let answer = Ask::Validate(&args).answer(dirs, direct).await?;
    if args.json {
        print_json(&answer);
    }
    let failed = if args.json {
        answer["diagnostics"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|d| text(d, "severity") == "error")
    } else {
        let failed = report_diagnostics(&answer);
        println!(
            "{} TriplesMap(s) over {}",
            answer["triples_map_count"],
            strings(&answer, "table_names").join(", ")
        );
        failed
    };
    if failed || answer["compiled_ok"] == false {
        return Err(CliError::ExitCode(1));
    }
    if !args.json {
        println!("The mapping is sound.");
    }
    Ok(())
}

fn print_created(
    graph_source_id: &str,
    mapping_source: &str,
    triples_map_count: usize,
    table_names: &[String],
    table_versions: &BTreeMap<String, u64>,
    table_warnings: &[String],
    mapping_validated: bool,
) {
    println!("Mapped Delta tables as graph source '{graph_source_id}'");
    println!("  R2RML:       {mapping_source}");
    println!("  TriplesMaps: {triples_map_count}");
    println!("  Tables:      {}", table_names.len());
    for name in table_names {
        match table_versions.get(name) {
            Some(version) => println!("    {name} (version {version})"),
            None => println!("    {name} (not readable yet)"),
        }
    }
    if !mapping_validated {
        println!("  Warning:     the mapping could not be read, so it was not validated");
    }
    for w in table_warnings {
        println!("  Warning:     {w}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn map_args(extra: &[&str]) -> Result<DeltaMapArgs, clap::Error> {
        let mapping = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
        let mut argv = vec!["fluree", "delta", "map", "sales", "--r2rml", mapping];
        argv.extend_from_slice(extra);
        match crate::cli::Cli::try_parse_from(argv)?.command {
            crate::cli::Commands::Delta {
                action: crate::cli::DeltaAction::Map(args),
            } => Ok(*args),
            _ => unreachable!("parsed as delta map"),
        }
    }

    #[test]
    fn unity_options_reach_a_remote_server_under_the_routes_field_names() {
        let args = map_args(&[
            "--unity-uri",
            "https://workspace.example.com",
            "--unity-catalog",
            "main",
            "--oauth2-client-id",
            "app",
            "--oauth2-client-secret-env",
            "SP_SECRET",
        ])
        .unwrap();
        let body = args_to_json(&args).unwrap();
        assert_eq!(body["unity_uri"], "https://workspace.example.com");
        assert_eq!(body["unity_catalog"], "main");
        assert_eq!(body["oauth2_client_id"], "app");
        assert_eq!(body["oauth2_client_secret_env"], "SP_SECRET");
        assert!(body.get("root").is_none());
    }

    #[test]
    fn unity_excludes_root_and_its_options_need_it() {
        let unity = ["--unity-uri", "https://workspace.example.com"];
        assert!(map_args(&[unity[0], unity[1], "--auth-bearer-env", "T"]).is_ok());
        assert!(map_args(&[unity[0], unity[1], "--root", "s3://lake/Tables"]).is_err());
        // With --root present clap waives `requires`; the API's all-or-nothing
        // check on the Unity fields is what refuses that combination.
        assert!(map_args(&["--table", "t=s3://lake/t", "--unity-catalog", "main"]).is_err());
        assert!(map_args(&["--table", "t=s3://lake/t", "--auth-bearer-env", "T"]).is_err());
        assert!(map_args(&[
            unity[0],
            unity[1],
            "--auth-bearer",
            "t",
            "--auth-bearer-env",
            "T"
        ])
        .is_err());
        // A table by path may sit beside the catalog.
        assert!(map_args(&[
            unity[0],
            unity[1],
            "--auth-bearer-env",
            "T",
            "--table",
            "raw=s3://lake/raw"
        ])
        .is_ok());
    }

    fn delta(argv: &[&str]) -> Result<crate::cli::DeltaAction, clap::Error> {
        let mut full = vec!["fluree", "delta"];
        full.extend_from_slice(argv);
        match crate::cli::Cli::try_parse_from(full)?.command {
            crate::cli::Commands::Delta { action } => Ok(action),
            _ => unreachable!("parsed as delta"),
        }
    }

    const UNITY: [&str; 4] = [
        "--unity-uri",
        "https://workspace.example.com",
        "--auth-bearer-env",
        "T",
    ];

    fn with_unity<'a>(argv: &[&'a str]) -> Vec<&'a str> {
        let mut full = argv.to_vec();
        full.extend_from_slice(&UNITY);
        full
    }

    #[test]
    fn each_read_only_command_asks_its_own_route_with_the_connection() {
        let mapping = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
        let cases: [(Vec<&str>, &str, Value); 5] = [
            (
                with_unity(&["browse", "--unity-catalog", "main", "--depth", "schemas"]),
                "catalog/browse",
                json!({"depth": "schemas", "unity_catalog": "main"}),
            ),
            (
                with_unity(&["preview", "sales.orders"]),
                "catalog/preview",
                json!({"table": "sales.orders"}),
            ),
            (
                with_unity(&["verify", "sales.orders", "--s3-region", "us-east-1"]),
                "catalog/verify",
                json!({"table": "sales.orders", "s3_region": "us-east-1"}),
            ),
            (
                with_unity(&[
                    "generate",
                    "a.b.c",
                    "--base-namespace",
                    "https://example.org/",
                ]),
                "r2rml/generate",
                json!({"tables": ["a.b.c"], "base_namespace": "https://example.org/"}),
            ),
            (
                vec!["validate", "--root", "s3://lake/Tables", "--r2rml", mapping],
                "r2rml/validate",
                json!({"root": "s3://lake/Tables"}),
            ),
        ];
        for (argv, route, expected) in cases {
            let action = delta(&argv).unwrap_or_else(|e| panic!("{argv:?}: {e}"));
            let ask = match &action {
                crate::cli::DeltaAction::Browse(a) => Ask::Browse(a),
                crate::cli::DeltaAction::Preview(a) => Ask::Preview(a),
                crate::cli::DeltaAction::Verify(a) => Ask::Verify(a),
                crate::cli::DeltaAction::Generate(a) => Ask::Generate(a),
                crate::cli::DeltaAction::Validate(a) => Ask::Validate(a),
                _ => unreachable!(),
            };
            assert_eq!(ask.route(), route);
            let body = ask.body().unwrap();
            for (key, value) in expected.as_object().unwrap() {
                assert_eq!(&body[key], value, "{route}: {key}");
            }
            if route == "r2rml/validate" {
                assert!(body["r2rml"].as_str().unwrap().contains("[package]"));
                assert!(body.get("name").is_none());
            } else {
                assert_eq!(body["unity_uri"], "https://workspace.example.com");
                assert_eq!(body["auth_bearer_env"], "T");
            }
        }
    }

    #[test]
    fn a_catalog_command_needs_its_catalog() {
        for argv in [vec!["browse"], vec!["preview", "t"], vec!["verify", "t"]] {
            let action = delta(&argv).unwrap();
            let ask = match &action {
                crate::cli::DeltaAction::Browse(a) => Ask::Browse(a),
                crate::cli::DeltaAction::Preview(a) => Ask::Preview(a),
                crate::cli::DeltaAction::Verify(a) => Ask::Verify(a),
                _ => unreachable!(),
            };
            let error = ask.body().unwrap_err().to_string();
            assert!(error.contains("--unity-uri is required"), "{error}");
        }
        assert!(delta(&["generate", "--base-namespace", "https://example.org/"]).is_err());
        assert!(delta(&with_unity(&["generate", "a.b.c"])).is_err());
    }

    fn generate_body(extra: &[&str]) -> CliResult<Value> {
        let mut argv = with_unity(&[
            "generate",
            "orders",
            "main.sales.customers",
            "--base-namespace",
            "https://example.org/",
        ]);
        argv.extend_from_slice(extra);
        match delta(&argv).unwrap() {
            crate::cli::DeltaAction::Generate(a) => Ask::Generate(&a).body(),
            _ => unreachable!(),
        }
    }

    #[test]
    fn generate_options_reach_the_request_the_server_reads() {
        let plain = generate_body(&[]).unwrap();
        assert_eq!(plain["per_table_overrides"], json!([]));
        assert_eq!(
            plain["options"],
            json!({"emit_fk_joins": true, "subject_strategy": "auto"})
        );

        let tuned = generate_body(&[
            "--subject-key",
            "orders=order_id, line",
            "--class-name",
            "orders=Purchase",
            "--class-name",
            "main.sales.customers=Buyer",
            "--no-joins",
            "--strict-subjects",
        ])
        .unwrap();
        assert_eq!(
            tuned["per_table_overrides"],
            json!([
                {"table": "main.sales.customers", "class_name": "Buyer"},
                {"table": "orders", "subject_key": ["order_id", "line"], "class_name": "Purchase"},
            ])
        );
        assert_eq!(
            tuned["options"],
            json!({"emit_fk_joins": false, "subject_strategy": "identifier"})
        );

        let stray = generate_body(&["--class-name", "returns=Return"]).unwrap_err();
        assert!(stray.to_string().contains("'returns'"), "{stray}");
        assert!(generate_body(&["--subject-key", "orders"]).is_err());
    }

    #[test]
    fn a_listing_and_a_preview_say_what_matters_about_a_table() {
        let listing = listing_text(&json!({
            "catalogs": [], "schemas": ["main.sales"],
            "tables": [
                {"full_name": "main.sales.orders", "kind": "MANAGED", "format": "DELTA"},
                {"full_name": "main.sales.recent", "kind": "VIEW",
                 "unreadable": "is a VIEW, not a Delta table"},
                {"full_name": "main.sales.eu", "access_rule": "row filter"},
            ],
        }));
        assert!(listing.contains("Schemas:\n  main.sales\n"), "{listing}");
        assert!(listing.contains("  main.sales.orders\n"), "{listing}");
        assert!(
            listing.contains("main.sales.recent  (not readable: is a VIEW"),
            "{listing}"
        );
        assert!(
            listing.contains("main.sales.eu  (has a row filter;"),
            "{listing}"
        );
        assert!(!listing.contains("Catalogs"));
        assert_eq!(listing_text(&json!({})), "Nothing listed.\n");

        let preview = preview_text(&json!({
            "full_name": "main.sales.orders", "kind": "MANAGED", "format": "DELTA",
            "location": "s3://bucket/t", "access_rule": "column mask",
            "columns": [
                {"name": "order_id", "type_text": "bigint", "xsd_type": "xsd:integer",
                 "nullable": false, "mappable": true, "masked": false},
                {"name": "extras", "type_text": "variant", "xsd_type": null,
                 "nullable": true, "mappable": false, "masked": true, "comment": "raw"},
            ],
            "primary_key": ["order_id"],
            "foreign_keys": [{"name": "fk", "columns": ["buyer"],
                              "parent_table": "main.sales.customers", "parent_columns": ["id"]}],
        }));
        assert!(
            preview.contains("order_id  bigint   xsd:integer   not null"),
            "{preview}"
        );
        assert!(
            preview.contains("cannot be mapped, masked, -- raw"),
            "{preview}"
        );
        assert!(preview.contains("Access rule: column mask"), "{preview}");
        assert!(preview.contains("Primary key: order_id"), "{preview}");
        assert!(
            preview.contains("Foreign key: buyer -> main.sales.customers (id)"),
            "{preview}"
        );
    }
}
