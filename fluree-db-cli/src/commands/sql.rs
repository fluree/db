//! `fluree sql map` — register a SQL graph source.
//!
//! `fluree sql list|info|drop` share the mapped-source implementations in
//! [`super::iceberg`].

use crate::cli::SqlMapArgs;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

pub async fn run_sql_map(args: SqlMapArgs, dirs: &FlureeDir, direct: bool) -> CliResult<()> {
    if let Some(remote_name) = args.remote.as_deref() {
        let client = crate::context::build_remote_client(remote_name, dirs).await?;
        let result = run_sql_map_remote(&client, &args).await.map_err(|e| {
            CliError::Remote(format!(
                "failed to map SQL graph source on '{remote_name}': {e}"
            ))
        });
        crate::context::persist_refreshed_tokens(&client, remote_name, dirs).await;
        return result;
    }

    if !direct {
        if let Some(client) = crate::context::try_server_route_client(dirs) {
            return run_sql_map_remote(&client, &args)
                .await
                .map_err(|e| CliError::Remote(format!("failed to map SQL graph source: {e}")));
        }
    }

    run_sql_map_local(args, dirs).await
}

fn read_mapping(args: &SqlMapArgs) -> CliResult<String> {
    std::fs::read_to_string(&args.r2rml).map_err(|e| {
        CliError::Input(format!(
            "Failed to read R2RML mapping file '{}': {e}",
            args.r2rml.display()
        ))
    })
}

fn mapping_media_type(args: &SqlMapArgs) -> Option<String> {
    args.r2rml_type
        .clone()
        .or_else(|| super::iceberg::infer_mapping_media_type(&args.r2rml))
}

fn session_pairs(args: &SqlMapArgs) -> CliResult<std::collections::BTreeMap<String, String>> {
    args.session
        .iter()
        .map(|kv| {
            kv.split_once('=')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .filter(|(k, _)| !k.is_empty())
                .ok_or_else(|| CliError::Usage(format!("--session expects KEY=VALUE, got '{kv}'")))
        })
        .collect()
}

fn args_to_json(args: &SqlMapArgs) -> CliResult<serde_json::Value> {
    let mut body = serde_json::json!({
        "name": args.name,
        "endpoint": args.endpoint,
        "r2rml": read_mapping(args)?,
    });
    let obj = body.as_object_mut().unwrap();
    if let Some(v) = mapping_media_type(args) {
        obj.insert("r2rml_type".into(), v.into());
    }
    for (key, value) in [
        ("branch", &args.branch),
        ("dialect", &args.dialect),
        ("protocol", &args.protocol),
        ("catalog", &args.catalog),
        ("schema", &args.schema),
        ("user", &args.user),
        ("auth_bearer", &args.auth_bearer),
        ("oauth2_token_url", &args.oauth2_token_url),
        ("oauth2_client_id", &args.oauth2_client_id),
        ("oauth2_client_secret", &args.oauth2_client_secret),
        ("oauth2_scope", &args.oauth2_scope),
        ("oauth2_audience", &args.oauth2_audience),
        ("model", &args.model),
    ] {
        if let Some(v) = value {
            obj.insert(key.into(), v.clone().into());
        }
    }
    let session = session_pairs(args)?;
    if !session.is_empty() {
        obj.insert("session".into(), serde_json::to_value(session).unwrap());
    }
    Ok(body)
}

async fn run_sql_map_remote(
    client: &crate::remote_client::RemoteLedgerClient,
    args: &SqlMapArgs,
) -> CliResult<()> {
    let body = args_to_json(args)?;
    let result = client.sql_map(&body).await?;
    let get = |k: &str| {
        result
            .get(k)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_string()
    };
    let n = |k: &str| {
        result
            .get(k)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    let flag = |k: &str| {
        result
            .get(k)
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    };
    let tables: Vec<String> = result
        .get("table_names")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|t| t.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    print_created(
        &get("graph_source_id"),
        &get("endpoint"),
        &get("mapping_source"),
        n("triples_map_count") as usize,
        n("table_count") as usize,
        &tables,
        flag("connection_tested"),
        flag("mapping_validated"),
    );
    Ok(())
}

#[cfg(feature = "sql")]
async fn run_sql_map_local(args: SqlMapArgs, dirs: &FlureeDir) -> CliResult<()> {
    use fluree_db_api::{SqlAuthConfig, SqlConfigValue, SqlDialect, WireProtocol};

    let fluree = crate::context::build_fluree(dirs)?;
    let mapping = read_mapping(&args)?;
    let mut config = fluree_db_api::SqlCreateConfig::new(&args.name, &args.endpoint, mapping);
    config.branch = args.branch.clone();
    config.mapping_media_type = mapping_media_type(&args);
    config.catalog = args.catalog.clone();
    config.schema = args.schema.clone();
    config.user = args.user.clone();
    config.session = session_pairs(&args)?;
    config.model = args.model.clone();
    if let Some(d) = &args.dialect {
        config.dialect = match d.to_lowercase().as_str() {
            "trino" => SqlDialect::Trino,
            "postgres" | "postgresql" => SqlDialect::Postgres,
            "mysql" => SqlDialect::Mysql,
            "sqlite" => SqlDialect::Sqlite,
            other => {
                return Err(CliError::Usage(format!(
                    "unknown --dialect '{other}' (trino, postgres, mysql, sqlite)"
                )))
            }
        };
    }
    if let Some(p) = &args.protocol {
        config.protocol = match p.to_lowercase().as_str() {
            "trino" => WireProtocol::Trino,
            "presto" => WireProtocol::Presto,
            other => {
                return Err(CliError::Usage(format!(
                    "unknown --protocol '{other}' (trino, presto)"
                )))
            }
        };
    }
    if let (Some(url), Some(secret)) = (&args.oauth2_token_url, &args.oauth2_client_secret) {
        config.auth = SqlAuthConfig::OAuth2ClientCredentials {
            token_url: url.clone(),
            client_id: SqlConfigValue::Literal(args.oauth2_client_id.clone().unwrap_or_default()),
            client_secret: SqlConfigValue::Literal(secret.clone()),
            scope: args.oauth2_scope.clone(),
            audience: args.oauth2_audience.clone(),
        };
    } else if let Some(token) = &args.auth_bearer {
        config.auth = SqlAuthConfig::Bearer {
            token: SqlConfigValue::Literal(token.clone()),
        };
    }

    let result = fluree.create_sql_graph_source(config).await?;
    print_created(
        &result.graph_source_id,
        &result.endpoint,
        &result.mapping_source,
        result.triples_map_count,
        result.table_count,
        &result.table_names,
        result.connection_tested,
        result.mapping_validated,
    );
    Ok(())
}

#[cfg(not(feature = "sql"))]
async fn run_sql_map_local(_args: SqlMapArgs, _dirs: &FlureeDir) -> CliResult<()> {
    Err(CliError::Usage(
        "SQL graph source support not compiled. Rebuild with `--features sql`.".into(),
    ))
}

#[allow(clippy::too_many_arguments)]
fn print_created(
    graph_source_id: &str,
    endpoint: &str,
    mapping_source: &str,
    triples_map_count: usize,
    table_count: usize,
    table_names: &[String],
    connection_tested: bool,
    mapping_validated: bool,
) {
    println!("Mapped SQL endpoint as graph source '{graph_source_id}'");
    println!("  Endpoint:    {endpoint}");
    println!("  R2RML:       {mapping_source}");
    println!("  TriplesMaps: {triples_map_count}");
    println!(
        "  Tables:      {}",
        super::iceberg::format_table_summary(table_count, table_names)
    );
    println!(
        "  Connection:  {}",
        if connection_tested {
            "verified"
        } else {
            "not tested (endpoint unreachable or credentials rejected)"
        }
    );
    println!(
        "  Mapping:     {}",
        if mapping_validated {
            "validated"
        } else {
            "not validated (check mapping source)"
        }
    );
}
