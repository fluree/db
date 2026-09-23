//! `fluree delta map` — register a Delta Lake graph source.
//!
//! `fluree delta list|info|drop` share the mapped-source implementations in
//! [`super::iceberg`].

use crate::cli::DeltaMapArgs;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
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

fn read_mapping(args: &DeltaMapArgs) -> CliResult<String> {
    std::fs::read_to_string(&args.r2rml).map_err(|e| {
        CliError::Input(format!(
            "Failed to read R2RML mapping file '{}': {e}",
            args.r2rml.display()
        ))
    })
}

fn mapping_media_type(args: &DeltaMapArgs) -> Option<String> {
    args.r2rml_type
        .clone()
        .or_else(|| super::iceberg::infer_mapping_media_type(&args.r2rml))
}

fn table_pairs(args: &DeltaMapArgs) -> CliResult<BTreeMap<String, String>> {
    args.table
        .iter()
        .map(|kv| {
            kv.split_once('=')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .filter(|(k, v)| !k.is_empty() && !v.is_empty())
                .ok_or_else(|| {
                    CliError::Usage(format!("--table expects NAME=LOCATION, got '{kv}'"))
                })
        })
        .collect()
}

fn require_location(args: &DeltaMapArgs) -> CliResult<()> {
    if args.root.is_none() && args.table.is_empty() {
        return Err(CliError::Usage(
            "give --root and/or one or more --table NAME=LOCATION".to_string(),
        ));
    }
    Ok(())
}

fn args_to_json(args: &DeltaMapArgs) -> CliResult<serde_json::Value> {
    require_location(args)?;
    let mut body = serde_json::json!({
        "name": args.name,
        "r2rml": read_mapping(args)?,
    });
    let obj = body.as_object_mut().unwrap();
    if let Some(v) = mapping_media_type(args) {
        obj.insert("r2rml_type".into(), v.into());
    }
    for (key, value) in [
        ("root", &args.root),
        ("branch", &args.branch),
        ("s3_region", &args.s3_region),
        ("s3_endpoint", &args.s3_endpoint),
        ("azure_tenant_id", &args.azure_tenant_id),
        ("azure_client_id", &args.azure_client_id),
        ("azure_client_secret", &args.azure_client_secret),
        ("azure_client_secret_env", &args.azure_client_secret_env),
        ("model", &args.model),
    ] {
        if let Some(v) = value {
            obj.insert(key.into(), v.clone().into());
        }
    }
    let tables = table_pairs(args)?;
    if !tables.is_empty() {
        obj.insert("tables".into(), serde_json::to_value(tables).unwrap());
    }
    if args.s3_path_style {
        obj.insert("s3_path_style".into(), true.into());
    }
    if let Some(v) = args.default_allow {
        obj.insert("default_allow".into(), v.into());
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
    let strings = |k: &str| -> Vec<String> {
        result
            .get(k)
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|t| t.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
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
        &strings("table_names"),
        &versions,
        &strings("table_warnings"),
        result
            .get("mapping_validated")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    );
    super::iceberg::print_model_warnings(&super::iceberg::model_warnings_of(&result));
    Ok(())
}

#[cfg(feature = "delta")]
async fn run_delta_map_local(args: DeltaMapArgs, dirs: &FlureeDir) -> CliResult<()> {
    require_location(&args)?;
    let fluree = crate::context::build_fluree(dirs)?;
    let azure = fluree_db_api::DeltaAzureFields {
        tenant_id: args.azure_tenant_id.clone(),
        client_id: args.azure_client_id.clone(),
        client_secret: args.azure_client_secret.clone(),
        client_secret_env: args.azure_client_secret_env.clone(),
    }
    .into_auth()?;
    let config = fluree_db_api::DeltaCreateConfig {
        name: args.name.clone(),
        branch: args.branch.clone(),
        root: args.root.clone(),
        tables: table_pairs(&args)?,
        io: fluree_db_api::DeltaIoConfig {
            s3_region: args.s3_region.clone(),
            s3_endpoint: args.s3_endpoint.clone(),
            s3_path_style: args.s3_path_style,
            azure,
        },
        mapping: fluree_db_api::R2rmlMappingInput::Content(read_mapping(&args)?),
        mapping_media_type: mapping_media_type(&args),
        model: args.model.clone(),
        default_allow: args.default_allow,
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
async fn run_delta_map_local(_args: DeltaMapArgs, _dirs: &FlureeDir) -> CliResult<()> {
    Err(CliError::Usage(
        "Delta graph source support not compiled. Rebuild with `--features delta`, or map \
         against a server that has it (`--remote`)."
            .into(),
    ))
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
