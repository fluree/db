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

/// The endpoint auth the flags describe, through the codec every surface uses:
/// one mode or none, in the CLI's spelling when refused.
#[cfg(feature = "iceberg")]
fn endpoint_auth(args: &SqlMapArgs) -> CliResult<fluree_db_api::IcebergAuthConfig> {
    use super::source_auth::usage;
    let auth = args.auth.fields(Some(&args.google))?;
    // The CLI names variables of its own process; it has no secret resolver.
    auth.check(&fluree_db_api::SecretRules::local(false))
        .map_err(usage)?;
    auth.into_auth(fluree_db_api::AuthProfile::Catalog)
        .map_err(usage)
}

/// The `sql/map` body for a remote server.
///
/// A bearer token or OAuth2 client credentials go as the flat fields, which
/// every server version reads. A server older than the structured `auth`
/// does not refuse a field it does not know: given `auth`, it would register
/// the source with no auth and say nothing. Only what has no flat field (a
/// Google identity) goes as the structured `auth`, which such a server still
/// drops. The server converts the flat fields with the codec
/// [`endpoint_auth`] uses, so both store the same record.
#[cfg(feature = "iceberg")]
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
    if let Some(v) = args.default_allow {
        obj.insert("default_allow".into(), v.into());
    }
    if args.allow_duplicate_subjects {
        obj.insert("allow_duplicate_subjects".into(), true.into());
    }
    use fluree_db_api::IcebergAuthConfig;
    match endpoint_auth(args)? {
        IcebergAuthConfig::None => {}
        IcebergAuthConfig::Bearer { .. } | IcebergAuthConfig::OAuth2ClientCredentials { .. } => {
            let flags = &args.auth;
            for (key, value) in [
                ("auth_bearer", &flags.auth_bearer),
                ("auth_bearer_env", &flags.auth_bearer_env),
                ("oauth2_token_url", &flags.oauth2_token_url),
                ("oauth2_client_id", &flags.oauth2_client_id),
                ("oauth2_client_secret", &flags.oauth2_client_secret),
                ("oauth2_client_secret_env", &flags.oauth2_client_secret_env),
                ("oauth2_scope", &flags.oauth2_scope),
                ("oauth2_audience", &flags.oauth2_audience),
            ] {
                if let Some(v) = value {
                    body[key] = v.clone().into();
                }
            }
        }
        auth => {
            body["auth"] =
                serde_json::to_value(auth).map_err(|e| CliError::Input(e.to_string()))?;
        }
    }
    Ok(body)
}

#[cfg(not(feature = "iceberg"))]
fn args_to_json(_args: &SqlMapArgs) -> CliResult<serde_json::Value> {
    Err(CliError::Usage(
        "SQL graph source support not compiled; the request a server takes is built by it. \
         Rebuild with `--features sql`."
            .into(),
    ))
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
    super::iceberg::print_model_warnings(&super::iceberg::model_warnings_of(&result));
    print_mapping_warnings(&string_list_of(&result, "mapping_warnings"));
    Ok(())
}

fn string_list_of(v: &serde_json::Value, key: &str) -> Vec<String> {
    v.get(key)
        .and_then(|w| w.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn print_mapping_warnings(warnings: &[String]) {
    for w in warnings {
        println!("  Warning:     {w}");
    }
}

/// `fluree sql check NAME`: re-probe subject key uniqueness (local only).
pub async fn run_sql_check(name: &str, dirs: &FlureeDir) -> CliResult<()> {
    run_sql_check_local(name, dirs).await
}

#[cfg(feature = "sql")]
async fn run_sql_check_local(name: &str, dirs: &FlureeDir) -> CliResult<()> {
    let fluree = crate::context::build_fluree(dirs).await?;
    let id = if name.contains(':') {
        name.to_string()
    } else {
        format!("{name}:main")
    };
    let result = fluree.check_sql_graph_source(&id).await?;
    println!("Checked SQL graph source '{}'", result.graph_source_id);
    if result.duplicate_subject_tables.is_empty() {
        println!("  Subject keys: unique in every probed table");
    } else {
        println!(
            "  Subject keys: NOT unique in {} ({})",
            result.duplicate_subject_tables.join(", "),
            if result.allow_duplicate_subjects {
                "accepted: registered with --allow-duplicate-subjects"
            } else {
                "the pushdown lane refuses statements over these tables"
            }
        );
    }
    print_mapping_warnings(&result.mapping_warnings);
    Ok(())
}

#[cfg(not(feature = "sql"))]
async fn run_sql_check_local(_name: &str, _dirs: &FlureeDir) -> CliResult<()> {
    Err(CliError::Usage(
        "this build has no SQL graph source support (feature `sql`)".to_string(),
    ))
}

#[cfg(feature = "sql")]
async fn run_sql_map_local(args: SqlMapArgs, dirs: &FlureeDir) -> CliResult<()> {
    use fluree_db_api::{SqlDialect, WireProtocol};

    let fluree = crate::context::build_fluree(dirs).await?;
    let mapping = read_mapping(&args)?;
    let mut config = fluree_db_api::SqlCreateConfig::new(&args.name, &args.endpoint, mapping);
    config.branch = args.branch.clone();
    config.mapping_media_type = mapping_media_type(&args);
    config.catalog = args.catalog.clone();
    config.schema = args.schema.clone();
    config.user = args.user.clone();
    config.session = session_pairs(&args)?;
    config.model = args.model.clone();
    config.default_allow = args.default_allow;
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
    config.auth = endpoint_auth(&args)?;

    config.allow_duplicate_subjects = args.allow_duplicate_subjects;
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
    super::iceberg::print_model_warnings(&result.model_warnings);
    print_mapping_warnings(&result.mapping_warnings);
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

#[cfg(all(test, feature = "iceberg"))]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> SqlMapArgs {
        use clap::Parser;
        let mut argv = vec![
            "fluree",
            "sql",
            "map",
            "orders",
            "--endpoint",
            "https://trino.example.com",
            "--r2rml",
            "m.ttl",
        ];
        argv.extend_from_slice(args);
        match crate::cli::Cli::try_parse_from(argv).unwrap().command {
            crate::cli::Commands::Sql {
                action: crate::cli::SqlAction::Map(args),
            } => *args,
            _ => unreachable!(),
        }
    }

    #[test]
    fn sql_secrets_can_name_a_variable_and_google_reaches_the_endpoint() {
        let auth = serde_json::to_value(
            endpoint_auth(&parse(&["--auth-bearer-env", "TRINO_TOKEN"])).unwrap(),
        )
        .unwrap();
        assert_eq!(auth["token"]["env_var"], "TRINO_TOKEN");
        let google = endpoint_auth(&parse(&["--auth-google-adc"])).unwrap();
        assert!(matches!(
            google,
            fluree_db_api::IcebergAuthConfig::Google { .. }
        ));
    }

    /// The `sql/map` body for `flags`, and the auth a server stores from it.
    fn remote_body(flags: &[&str]) -> (serde_json::Value, fluree_db_api::IcebergAuthConfig) {
        let mapping = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(mapping.path(), "").unwrap();
        let mut args = parse(flags);
        args.r2rml = mapping.path().to_path_buf();
        let body = args_to_json(&args).unwrap();
        let mut fields = body.as_object().unwrap().clone();
        for key in [
            "name",
            "endpoint",
            "r2rml",
            "r2rml_type",
            "branch",
            "dialect",
            "protocol",
            "catalog",
            "schema",
            "user",
            "model",
            "session",
            "default_allow",
            "allow_duplicate_subjects",
        ] {
            fields.remove(key);
        }
        let stored = fluree_db_api::sql_auth_from_json(
            fields,
            &fluree_db_api::SecretRules::listed("TRINO_TOKEN,TRINO_SECRET,SA_KEY", false),
        )
        .unwrap();
        assert_eq!(stored, endpoint_auth(&args).unwrap(), "{flags:?}");
        (body, stored)
    }

    #[test]
    fn a_remote_sql_map_sends_what_has_a_flat_form_as_the_flat_fields() {
        // A server older than the structured `auth` ignores it and registers
        // the source with no auth; the flat fields are what it reads.
        let oauth2 = [
            "--oauth2-token-url",
            "https://t.example.com/token",
            "--oauth2-client-id",
            "c",
            "--oauth2-scope",
            "s",
            "--oauth2-audience",
            "a",
        ];
        for (flags, flat) in [
            (vec!["--auth-bearer", "t"], vec![("auth_bearer", "t")]),
            (
                vec!["--auth-bearer-env", "TRINO_TOKEN"],
                vec![("auth_bearer_env", "TRINO_TOKEN")],
            ),
            (
                [&oauth2[..], &["--oauth2-client-secret", "x"]].concat(),
                vec![
                    ("oauth2_token_url", "https://t.example.com/token"),
                    ("oauth2_client_id", "c"),
                    ("oauth2_client_secret", "x"),
                    ("oauth2_scope", "s"),
                    ("oauth2_audience", "a"),
                ],
            ),
            (
                [&oauth2[..], &["--oauth2-client-secret-env", "TRINO_SECRET"]].concat(),
                vec![
                    ("oauth2_token_url", "https://t.example.com/token"),
                    ("oauth2_client_id", "c"),
                    ("oauth2_client_secret_env", "TRINO_SECRET"),
                    ("oauth2_scope", "s"),
                    ("oauth2_audience", "a"),
                ],
            ),
        ] {
            let (body, _) = remote_body(&flags);
            assert!(body.get("auth").is_none(), "{flags:?}: {body}");
            for (key, value) in flat {
                assert_eq!(body[key], value, "{flags:?}: {body}");
            }
        }

        // No auth: neither form.
        let (body, stored) = remote_body(&[]);
        assert_eq!(stored, fluree_db_api::IcebergAuthConfig::None);
        assert!(body.get("auth").is_none() && body.get("auth_bearer").is_none());

        // A Google identity has no flat field: the structured `auth`.
        for flags in [
            vec!["--auth-google-adc"],
            vec!["--auth-google-service-account-env", "SA_KEY"],
        ] {
            let (body, stored) = remote_body(&flags);
            assert!(matches!(
                stored,
                fluree_db_api::IcebergAuthConfig::Google { .. }
            ));
            assert_eq!(body["auth"]["type"], "google", "{flags:?}: {body}");
        }
    }

    #[test]
    fn sql_auth_given_two_ways_or_half_given_is_refused() {
        // OAuth2 used to win over a bearer silently, and a token URL alone
        // to store no auth.
        let e = endpoint_auth(&parse(&[
            "--auth-bearer",
            "t",
            "--oauth2-token-url",
            "https://t.example.com",
            "--oauth2-client-secret",
            "s",
        ]))
        .unwrap_err()
        .to_string();
        assert!(e.contains("two ways"), "{e}");
        let e = endpoint_auth(&parse(&["--oauth2-token-url", "https://t.example.com"]))
            .unwrap_err()
            .to_string();
        assert!(e.contains("--oauth2-token-url needs"), "{e}");
    }
}
