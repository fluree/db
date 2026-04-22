//! Fluree CLI library.
//!
//! This crate provides the types, command handlers, and utilities that
//! power the `fluree` CLI binary. Library consumers can import these
//! directly to build alternative interfaces (web UI, TUI, etc.) on top
//! of the same command logic.

pub mod cli;
pub mod commands;
pub mod config;
pub mod context;
pub mod detect;
pub mod error;
pub mod input;
pub mod output;
pub mod remote_client;

use cli::{Cli, Commands};
use fluree_db_api::server_defaults::{generate_config_template_for, ConfigFormat, FlureeDir};

/// Dispatch a parsed [`Cli`] to the appropriate command handler.
///
/// This is the main entry point for executing CLI commands. The binary
/// calls this after parsing args and initializing tracing. Library
/// consumers can construct a [`Cli`] programmatically and call this
/// directly, or call individual command handlers from [`commands`].
pub async fn run(cli: Cli) -> error::CliResult<()> {
    // Set the global remote HTTP timeout from CLI args before dispatching.
    context::set_remote_timeout(std::time::Duration::from_secs(cli.timeout));

    let config_path = cli.config.as_deref();
    let direct = cli.direct;

    match cli.command {
        Commands::Init { global, format } => {
            let config_format = match format {
                cli::InitFormat::Toml => ConfigFormat::Toml,
                cli::InitFormat::Jsonld => ConfigFormat::JsonLd,
            };
            commands::init::run(global, config_format)
        }

        Commands::Create {
            ledger,
            from,
            memory,
            no_user,
            chunk_size_mb,
            memory_budget_mb,
            parallelism,
            leaflet_rows,
            leaflets_per_leaf,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;

            if from.is_some() && memory.is_some() {
                return Err(error::CliError::Usage(
                    "--from and --memory are mutually exclusive".into(),
                ));
            }

            if let Some(memory_path) = memory {
                return commands::create::run_memory_import(
                    &ledger,
                    &memory_path,
                    no_user,
                    &fluree_dir,
                    cli.quiet,
                )
                .await;
            }

            // Create-specific flags take precedence; fall back to global flags.
            let import_opts = commands::create::ImportOpts {
                memory_budget_mb: if memory_budget_mb > 0 {
                    memory_budget_mb
                } else {
                    cli.memory_budget_mb
                },
                parallelism: if parallelism > 0 {
                    parallelism
                } else {
                    cli.parallelism
                },
                chunk_size_mb,
                leaflet_rows,
                leaflets_per_leaf,
            };
            commands::create::run(
                &ledger,
                from.as_deref(),
                &fluree_dir,
                cli.verbose,
                cli.quiet,
                &import_opts,
            )
            .await
        }

        Commands::Use { ledger } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::use_cmd::run(&ledger, &fluree_dir).await
        }

        Commands::List { remote } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::list::run(&fluree_dir, remote.as_deref(), direct).await
        }

        Commands::Info {
            ledger,
            remote,
            graph,
        } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::info::run(
                ledger.as_deref(),
                &fluree_dir,
                remote.as_deref(),
                direct,
                graph.as_deref(),
            )
            .await
        }

        Commands::Branch { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::branch::run(action, &fluree_dir, direct).await
        }

        Commands::Drop { name, force } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::drop::run(&name, force, &fluree_dir).await
        }

        Commands::Insert {
            args,
            expr,
            file,
            format,
            remote,
            policy,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::insert::run(
                &args,
                expr.as_deref(),
                file.as_deref(),
                format.as_deref(),
                &fluree_dir,
                remote.as_deref(),
                direct,
                &policy,
            )
            .await
        }

        Commands::Update {
            args,
            expr,
            file,
            format,
            remote,
            policy,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::update::run(
                &args,
                expr.as_deref(),
                file.as_deref(),
                format.as_deref(),
                &fluree_dir,
                remote.as_deref(),
                direct,
                &policy,
            )
            .await
        }

        Commands::Upsert {
            args,
            expr,
            file,
            format,
            remote,
            policy,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::upsert::run(
                &args,
                expr.as_deref(),
                file.as_deref(),
                format.as_deref(),
                &fluree_dir,
                remote.as_deref(),
                direct,
                &policy,
            )
            .await
        }

        Commands::Query {
            args,
            expr,
            file,
            format,
            normalize_arrays,
            bench,
            explain,
            sparql,
            jsonld,
            at,
            remote,
            policy,
        } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::query::run(
                &args,
                expr.as_deref(),
                file.as_deref(),
                &format,
                normalize_arrays,
                bench,
                explain,
                sparql,
                jsonld,
                at.as_deref(),
                &fluree_dir,
                remote.as_deref(),
                direct,
                &policy,
            )
            .await
        }

        Commands::History {
            entity,
            ledger,
            from,
            to,
            predicate,
            format,
        } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::history::run(
                &entity,
                ledger.as_deref(),
                &from,
                &to,
                predicate.as_deref(),
                &format,
                &fluree_dir,
            )
            .await
        }

        Commands::Context { action } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            match action {
                cli::ContextAction::Get { ledger } => {
                    commands::context_cmd::get(ledger.as_deref(), &fluree_dir).await
                }
                cli::ContextAction::Set { ledger, expr, file } => {
                    commands::context_cmd::set(
                        ledger.as_deref(),
                        expr.as_deref(),
                        file.as_ref(),
                        &fluree_dir,
                    )
                    .await
                }
            }
        }

        Commands::Export {
            ledger,
            format,
            all_graphs,
            graph,
            context,
            context_file,
            at,
        } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::export::run(
                ledger.as_deref(),
                &format,
                all_graphs,
                graph.as_deref(),
                context.as_deref(),
                context_file.as_deref(),
                at.as_deref(),
                &fluree_dir,
            )
            .await
        }

        Commands::Log {
            ledger,
            oneline,
            count,
        } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::log::run(ledger.as_deref(), oneline, count, &fluree_dir).await
        }

        Commands::Show {
            commit,
            ledger,
            remote,
        } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::show::run(
                &commit,
                ledger.as_deref(),
                &fluree_dir,
                remote.as_deref(),
                cli.direct,
            )
            .await
        }

        Commands::Config { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            match action {
                cli::ConfigAction::SetOrigins { ledger, file } => {
                    commands::config_cmd::run_set_origins(&ledger, &file, &fluree_dir).await
                }
                other => commands::config_cmd::run(other, &fluree_dir),
            }
        }

        Commands::Prefix { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::prefix::run(action, &fluree_dir)
        }

        Commands::Completions { shell } => {
            commands::completions::run(shell);
            Ok(())
        }

        Commands::Token { action } => commands::token::run(action),

        Commands::Remote { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::remote::run(action, &fluree_dir).await
        }

        Commands::Auth { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::auth::run(action, &fluree_dir).await
        }

        Commands::Upstream { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::upstream::run(action, &fluree_dir).await
        }

        Commands::Fetch { remote } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::sync::run_fetch(&remote, &fluree_dir).await
        }

        Commands::Pull { ledger, no_indexes } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::sync::run_pull(ledger.as_deref(), no_indexes, &fluree_dir).await
        }

        Commands::Push { ledger } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::sync::run_push(ledger.as_deref(), &fluree_dir).await
        }

        Commands::Publish {
            remote,
            ledger,
            remote_name,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::sync::run_publish(
                &remote,
                ledger.as_deref(),
                remote_name.as_deref(),
                &fluree_dir,
            )
            .await
        }

        Commands::Clone {
            args,
            origin,
            token,
            alias,
            no_indexes,
            no_txns,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            if let Some(origin_uri) = origin {
                // --origin mode: args = [ledger]
                if args.len() != 1 {
                    return Err(error::CliError::Usage(
                        "with --origin, provide exactly one positional arg: <ledger>".into(),
                    ));
                }
                commands::sync::run_clone_origin(
                    &origin_uri,
                    token.as_deref(),
                    &args[0],
                    alias.as_deref(),
                    no_indexes,
                    no_txns,
                    &fluree_dir,
                )
                .await
            } else {
                // Named-remote mode: args = [remote, ledger]
                if args.len() != 2 {
                    return Err(error::CliError::Usage(
                        "usage: fluree clone <remote> <ledger>  or  fluree clone --origin <uri> <ledger>".into(),
                    ));
                }
                commands::sync::run_clone(
                    &args[0],
                    &args[1],
                    alias.as_deref(),
                    no_indexes,
                    no_txns,
                    &fluree_dir,
                )
                .await
            }
        }

        Commands::Track { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::track::run(action, &fluree_dir).await
        }

        Commands::Index { ledger } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::index::run_index(ledger.as_deref(), &fluree_dir).await
        }

        Commands::Reindex { ledger } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::index::run_reindex(ledger.as_deref(), &fluree_dir).await
        }

        #[cfg(feature = "server")]
        Commands::Server { action } => commands::server::run(action, config_path).await,

        #[cfg(not(feature = "server"))]
        Commands::Server { .. } => Err(error::CliError::Server(
            "server support not compiled. Rebuild with `--features server`.".into(),
        )),

        Commands::Memory { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::memory::run(action, &fluree_dir).await
        }

        Commands::Iceberg { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            match action {
                cli::IcebergAction::Map(args) => {
                    commands::iceberg::run_iceberg_map(*args, &fluree_dir, direct).await
                }
                cli::IcebergAction::List { remote } => {
                    commands::iceberg::run_iceberg_list(&fluree_dir, remote.as_deref(), direct)
                        .await
                }
                cli::IcebergAction::Info { name, remote } => {
                    commands::iceberg::run_iceberg_info(
                        &name,
                        &fluree_dir,
                        remote.as_deref(),
                        direct,
                    )
                    .await
                }
                cli::IcebergAction::Drop {
                    name,
                    force,
                    remote,
                } => {
                    commands::iceberg::run_iceberg_drop(
                        &name,
                        force,
                        &fluree_dir,
                        remote.as_deref(),
                        direct,
                    )
                    .await
                }
            }
        }

        Commands::Mcp { action } => {
            // IDEs may spawn `fluree mcp serve` from a cwd that is not inside a
            // project with a local `.fluree/` directory. In that case, fall back
            // to global directories (creating them if needed) so the MCP server
            // can still start and expose tools.
            //
            // IMPORTANT: This must not print to stdout/stderr (stdio transport
            // uses stdout for JSON-RPC).
            let fluree_dir = if let Some(p) = config_path {
                config::require_fluree_dir(Some(p))?
            } else if let Some(local) = config::find_fluree_dir() {
                local
            } else {
                let global = FlureeDir::global().ok_or_else(|| {
                    error::CliError::Config("cannot determine global directories".into())
                })?;

                // For split global dirs, use an absolute data-dir storage path
                // so the server finds the right directory regardless of cwd.
                let storage_override = if !global.is_unified() {
                    let path = global.data_dir().join("storage");
                    let path_str = path.to_str().ok_or_else(|| {
                        error::CliError::Config(format!(
                            "data directory path is not valid UTF-8: {}",
                            path.display()
                        ))
                    })?;
                    Some(path_str.to_owned())
                } else {
                    None
                };

                let template =
                    generate_config_template_for(ConfigFormat::Toml, storage_override.as_deref());

                // Create minimal directory structure if missing.
                config::init_fluree_dir(&global, &template, ConfigFormat::Toml.filename())?;
                global
            };
            match action {
                cli::McpAction::Serve { transport } => {
                    commands::mcp_serve::run(&transport, &fluree_dir).await
                }
            }
        }
    }
}
