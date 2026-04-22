use crate::cli::PolicyArgs;
use crate::commands::insert::{
    build_policy_ctx, print_txn_result, resolve_positional_args, warn_novelty_if_needed,
};
use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use crate::input;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::CommitOpts;
use std::path::Path;

/// Format detected for the update body.
///
/// `update` accepts JSON-LD (with where/delete/insert) and SPARQL UPDATE.
/// Turtle is not valid here—use `insert` or `upsert` for Turtle data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdateFormat {
    JsonLd,
    SparqlUpdate,
}

/// Detect whether the input is JSON-LD or SPARQL UPDATE.
fn detect_update_format(
    path: Option<&Path>,
    content: &str,
    explicit: Option<&str>,
) -> CliResult<UpdateFormat> {
    // Explicit flag
    if let Some(fmt) = explicit {
        return match fmt.to_lowercase().as_str() {
            "jsonld" | "json-ld" | "json" => Ok(UpdateFormat::JsonLd),
            "sparql" | "sparql-update" => Ok(UpdateFormat::SparqlUpdate),
            other => Err(CliError::Usage(format!(
                "unknown update format '{other}'\n  {} valid formats: jsonld, sparql",
                colored::Colorize::bold(colored::Colorize::cyan("help:"))
            ))),
        };
    }

    // File extension
    if let Some(p) = path {
        if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
            match ext.to_lowercase().as_str() {
                "json" | "jsonld" => return Ok(UpdateFormat::JsonLd),
                "rq" | "ru" | "sparql" => return Ok(UpdateFormat::SparqlUpdate),
                _ => {}
            }
        }
    }

    // Content sniffing
    sniff_update_format(content)
}

fn sniff_update_format(content: &str) -> CliResult<UpdateFormat> {
    // Try JSON parse first
    if serde_json::from_str::<serde_json::Value>(content).is_ok() {
        return Ok(UpdateFormat::JsonLd);
    }

    // Check for SPARQL UPDATE keywords
    let upper = content.trim().to_uppercase();
    if upper.starts_with("INSERT")
        || upper.starts_with("DELETE")
        || upper.starts_with("PREFIX")
        || upper.starts_with("BASE")
    {
        return Ok(UpdateFormat::SparqlUpdate);
    }

    Err(CliError::Usage(format!(
        "could not detect update format\n  {} use --format jsonld or --format sparql to specify",
        colored::Colorize::bold(colored::Colorize::cyan("help:"))
    )))
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    args: &[String],
    expr: Option<&str>,
    file_flag: Option<&Path>,
    format_flag: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
    policy: &PolicyArgs,
) -> CliResult<()> {
    let (explicit_ledger, positional_inline, positional_file) = resolve_positional_args(args)?;

    // Resolve input: -e > positional inline > -f > positional file > stdin
    let source = input::resolve_input(
        expr,
        positional_inline,
        file_flag,
        positional_file.as_deref(),
    )?;
    let content = input::read_input(&source)?;

    // For format detection, prefer the -f path, then positional file
    let detect_path = file_flag.or(positional_file.as_deref());
    let txn_format = detect_update_format(detect_path, &content, format_flag)?;

    // Resolve ledger mode: --remote flag, local, tracked, or auto-route to local server
    let mode = if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(explicit_ledger, dirs)?;
        context::build_remote_mode(remote_name, &alias, dirs).await?
    } else {
        let mode = context::resolve_ledger_mode(explicit_ledger, dirs).await?;
        if direct {
            mode
        } else {
            context::try_server_route(mode, dirs)
        }
    };

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            remote_name,
            ..
        } => {
            let client = client.with_policy(policy.clone());
            let result = match txn_format {
                UpdateFormat::SparqlUpdate => client.update_sparql(&remote_alias, &content).await?,
                UpdateFormat::JsonLd => {
                    let json: serde_json::Value = serde_json::from_str(&content)?;
                    client.update_jsonld(&remote_alias, &json).await?
                }
            };

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            print_txn_result(&result);
        }
        LedgerMode::Local { fluree, alias } => match txn_format {
            UpdateFormat::SparqlUpdate => {
                // SPARQL UPDATE requires the server's parsing/lowering pipeline
                // which needs access to the ledger's namespace registry. This is
                // handled automatically when routing through the HTTP server.
                return Err(CliError::Usage(
                    "SPARQL UPDATE is not supported in direct local mode.\n  \
                     Start a server with `fluree server start` and retry (the CLI \
                     auto-routes through a running server), or use --remote to target \
                     a remote server.\n  \
                     Alternatively, use JSON-LD format with where/delete/insert keys."
                        .into(),
                ));
            }
            UpdateFormat::JsonLd => {
                let json: serde_json::Value = serde_json::from_str(&content)?;
                let policy_ctx = build_policy_ctx(&fluree, &alias, policy).await?;
                let graph = fluree.graph(&alias);
                let mut b = graph
                    .transact()
                    .update(&json)
                    .commit_opts(CommitOpts::default());
                if let Some(ctx) = policy_ctx {
                    b = b.policy(ctx);
                }
                let result = b.commit().await?;

                println!(
                    "Committed t={}, {} flakes",
                    result.receipt.t, result.receipt.flake_count
                );
                warn_novelty_if_needed(&result.indexing);
            }
        },
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn detect_explicit_jsonld() {
        let fmt = detect_update_format(None, "", Some("jsonld")).unwrap();
        assert_eq!(fmt, UpdateFormat::JsonLd);

        let fmt = detect_update_format(None, "", Some("json-ld")).unwrap();
        assert_eq!(fmt, UpdateFormat::JsonLd);

        let fmt = detect_update_format(None, "", Some("json")).unwrap();
        assert_eq!(fmt, UpdateFormat::JsonLd);
    }

    #[test]
    fn detect_explicit_sparql() {
        let fmt = detect_update_format(None, "", Some("sparql")).unwrap();
        assert_eq!(fmt, UpdateFormat::SparqlUpdate);

        let fmt = detect_update_format(None, "", Some("sparql-update")).unwrap();
        assert_eq!(fmt, UpdateFormat::SparqlUpdate);
    }

    #[test]
    fn detect_explicit_unknown_errors() {
        let result = detect_update_format(None, "", Some("turtle"));
        assert!(result.is_err());
    }

    #[test]
    fn detect_by_file_extension() {
        let fmt = detect_update_format(Some(Path::new("update.json")), "anything", None).unwrap();
        assert_eq!(fmt, UpdateFormat::JsonLd);

        let fmt = detect_update_format(Some(Path::new("update.jsonld")), "anything", None).unwrap();
        assert_eq!(fmt, UpdateFormat::JsonLd);

        let fmt = detect_update_format(Some(Path::new("update.ru")), "anything", None).unwrap();
        assert_eq!(fmt, UpdateFormat::SparqlUpdate);

        let fmt = detect_update_format(Some(Path::new("update.rq")), "anything", None).unwrap();
        assert_eq!(fmt, UpdateFormat::SparqlUpdate);

        let fmt = detect_update_format(Some(Path::new("update.sparql")), "anything", None).unwrap();
        assert_eq!(fmt, UpdateFormat::SparqlUpdate);
    }

    #[test]
    fn sniff_json_ld_body() {
        let content = r#"{"where": [], "delete": [], "insert": []}"#;
        let fmt = detect_update_format(None, content, None).unwrap();
        assert_eq!(fmt, UpdateFormat::JsonLd);
    }

    #[test]
    fn sniff_sparql_insert_data() {
        let content = "INSERT DATA { <http://example.org/x> <http://example.org/val> \"hello\" }";
        let fmt = detect_update_format(None, content, None).unwrap();
        assert_eq!(fmt, UpdateFormat::SparqlUpdate);
    }

    #[test]
    fn sniff_sparql_delete_where() {
        let content =
            "DELETE { ?s <http://example.org/val> ?o } WHERE { ?s <http://example.org/val> ?o }";
        let fmt = detect_update_format(None, content, None).unwrap();
        assert_eq!(fmt, UpdateFormat::SparqlUpdate);
    }

    #[test]
    fn sniff_sparql_with_prefix() {
        let content = "PREFIX ex: <http://example.org/>\nINSERT DATA { ex:x ex:val \"hello\" }";
        let fmt = detect_update_format(None, content, None).unwrap();
        assert_eq!(fmt, UpdateFormat::SparqlUpdate);
    }

    #[test]
    fn sniff_unrecognized_errors() {
        let result = detect_update_format(None, "not json and not sparql", None);
        assert!(result.is_err());
    }

    #[test]
    fn explicit_overrides_extension() {
        // Even with .ru extension, explicit "jsonld" wins
        let fmt = detect_update_format(Some(Path::new("update.ru")), "{}", Some("jsonld")).unwrap();
        assert_eq!(fmt, UpdateFormat::JsonLd);
    }
}
