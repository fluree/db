use crate::cli::PolicyArgs;
use crate::commands::insert::resolve_positional_args;
use crate::context::{self, LedgerMode};
use crate::detect;
use crate::error::{CliError, CliResult};
use crate::input;
use crate::output::{self, OutputFormatKind};
use fluree_db_api::server_defaults::FlureeDir;
use std::path::Path;
use std::time::Instant;

/// Parse a `--at` value into a `TimeSpec`.
///
/// Accepts:
/// - Integer → `TimeSpec::AtT(n)`
/// - ISO-8601 datetime string (contains `-` and `:`) → `TimeSpec::AtTime(s)`
/// - Otherwise → `TimeSpec::AtCommit(s)` (commit CID prefix)
pub fn parse_time_spec(at: &str) -> fluree_db_api::TimeSpec {
    if let Ok(t) = at.parse::<i64>() {
        fluree_db_api::TimeSpec::at_t(t)
    } else if at.contains('-') && at.contains(':') {
        // Looks like ISO-8601 timestamp (e.g., "2024-01-15T10:30:00Z")
        fluree_db_api::TimeSpec::at_time(at.to_string())
    } else {
        // Treat as commit CID prefix
        fluree_db_api::TimeSpec::at_commit(at.to_string())
    }
}

/// Format a Duration for human display.
///
/// - >= 1s   → "1.23s"
/// - >= 1ms  → "5.2ms"
/// - < 1ms   → "523μs"
fn format_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs_f64();
    if secs >= 1.0 {
        format!("{secs:.2}s")
    } else if secs >= 0.001 {
        format!("{:.1}ms", secs * 1_000.0)
    } else {
        format!("{}μs", d.as_micros())
    }
}

/// Format a usize with comma-separated thousands.
fn format_count(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(ch);
    }
    result
}

fn time_spec_to_suffix(spec: &fluree_db_api::TimeSpec) -> String {
    match spec {
        fluree_db_api::TimeSpec::Latest => "@t:latest".to_string(),
        fluree_db_api::TimeSpec::AtT(t) => format!("@t:{t}"),
        fluree_db_api::TimeSpec::AtTime(iso) => format!("@iso:{iso}"),
        fluree_db_api::TimeSpec::AtCommit(prefix) => format!("@commit:{prefix}"),
    }
}

fn attach_time_suffix_preserving_fragment(ledger: &str, suffix: &str) -> String {
    match ledger.split_once('#') {
        Some((base, frag)) => format!("{base}{suffix}#{frag}"),
        None => format!("{ledger}{suffix}"),
    }
}

fn inject_sparql_from_before_where(sparql: &str, from_iri: &str) -> Option<String> {
    // Minimal injection strategy for CLI ergonomics:
    // - Works for the common `SELECT ... WHERE { ... }` shape.
    // - If the query already contains FROM/FROM NAMED, caller should not inject.
    let lower = sparql.to_ascii_lowercase();
    let where_idx = lower.find(" where ")?; // require standard spacing
    let insert = format!(" FROM <{from_iri}>");
    let mut out = String::with_capacity(sparql.len() + insert.len());
    out.push_str(&sparql[..where_idx]);
    out.push_str(&insert);
    out.push_str(&sparql[where_idx..]);
    Some(out)
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    args: &[String],
    expr: Option<&str>,
    file_flag: Option<&Path>,
    format_str: &str,
    normalize_arrays: bool,
    bench: bool,
    explain: bool,
    sparql_flag: bool,
    fql_flag: bool,
    at: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
    policy: &PolicyArgs,
) -> CliResult<()> {
    const BENCH_ROWS: usize = 5;
    const DEFAULT_TABLE_PREVIEW_ROWS: usize = 200;
    let limit = if bench { Some(BENCH_ROWS) } else { None };
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
    let query_format = detect::detect_query_format(detect_path, &content, sparql_flag, fql_flag)?;

    // Parse output format
    let output_format = match format_str.to_lowercase().as_str() {
        "json" => OutputFormatKind::Json,
        "typed-json" | "typed_json" | "typedjson" => OutputFormatKind::TypedJson,
        "table" => OutputFormatKind::Table,
        "csv" => OutputFormatKind::Csv,
        "tsv" => OutputFormatKind::Tsv,
        other => {
            return Err(CliError::Usage(format!(
                "unknown output format '{other}'; valid formats: json, typed-json, table, csv, tsv"
            )));
        }
    };

    if explain {
        if bench {
            return Err(CliError::Usage(
                "--bench is not compatible with --explain".to_string(),
            ));
        }
        if normalize_arrays {
            return Err(CliError::Usage(
                "--normalize-arrays is not applicable to --explain".to_string(),
            ));
        }
        if !matches!(
            output_format,
            OutputFormatKind::Json | OutputFormatKind::TypedJson
        ) {
            return Err(CliError::Usage(
                "--explain output is JSON only; use --format json".to_string(),
            ));
        }
    }

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
            // Attach policy flags to the remote client so headers + body opts
            // ride through on every request (see RemoteLedgerClient::with_policy).
            let client = client.with_policy(policy.clone());
            // Delimited fast path: for SPARQL + TSV/CSV, request the format directly
            // from the server and stream the raw bytes to stdout (no JSON round-trip).
            if matches!(output_format, OutputFormatKind::Tsv | OutputFormatKind::Csv) {
                if query_format != detect::QueryFormat::Sparql {
                    return Err(CliError::Usage(
                        "--format tsv/csv is only supported for SPARQL queries on remote ledgers"
                            .to_string(),
                    ));
                }
                let accept = if output_format == OutputFormatKind::Tsv {
                    "text/tab-separated-values"
                } else {
                    "text/csv"
                };
                let timer = Instant::now();
                let bytes = match at {
                    Some(at_str) => {
                        if fluree_db_api::sparql_dataset_ledger_ids(&content)
                            .map(|v| !v.is_empty())
                            .unwrap_or(false)
                        {
                            return Err(CliError::Usage(
                                "SPARQL query already contains FROM/FROM NAMED; \
                                 for remote time travel, encode time travel in the FROM IRI \
                                 (e.g., FROM <ledger@t:1>) instead of using --at"
                                    .to_string(),
                            ));
                        }
                        let spec = parse_time_spec(at_str);
                        let suffix = time_spec_to_suffix(&spec);
                        let from_iri =
                            attach_time_suffix_preserving_fragment(&remote_alias, &suffix);
                        let injected =
                            inject_sparql_from_before_where(&content, &from_iri).ok_or_else(
                                || {
                                    CliError::Usage(
                                        "unable to inject SPARQL FROM clause for remote time travel; \
                                         please write the query as `SELECT ... WHERE { ... }` or include an explicit FROM"
                                            .to_string(),
                                    )
                                },
                            )?;
                        client
                            .query_connection_sparql_accept_bytes(&injected, accept)
                            .await?
                    }
                    None => {
                        client
                            .query_sparql_accept_bytes(&remote_alias, &content, accept)
                            .await?
                    }
                };
                let elapsed = timer.elapsed();

                context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

                use std::io::Write;
                std::io::stdout().write_all(&bytes)?;
                eprintln!("({output_format}, {})", format_duration(elapsed));
                return Ok(());
            }

            // Execute query via remote HTTP
            let timer = Instant::now();
            let result = match (query_format, at, explain) {
                (detect::QueryFormat::Sparql, Some(at_str), true) => {
                    // Remote `--at --explain` over SPARQL: inject the time
                    // suffix as a FROM and POST to the ledger-scoped explain
                    // endpoint. Same shape as the non-explain SPARQL `--at`
                    // case below — the server's `/explain/{ledger}` accepts
                    // same-ledger FROM with time travel (see the
                    // explain-time-travel fix). Queries with their own
                    // FROM/FROM NAMED must encode time travel there.
                    if fluree_db_api::sparql_dataset_ledger_ids(&content)
                        .map(|v| !v.is_empty())
                        .unwrap_or(false)
                    {
                        return Err(CliError::Usage(
                            "SPARQL query already contains FROM/FROM NAMED; \
                             for remote time travel, encode time travel in the FROM IRI \
                             (e.g., FROM <ledger@t:1>) instead of using --at"
                                .to_string(),
                        ));
                    }
                    let spec = parse_time_spec(at_str);
                    let suffix = time_spec_to_suffix(&spec);
                    let from_iri = attach_time_suffix_preserving_fragment(&remote_alias, &suffix);
                    let injected = inject_sparql_from_before_where(&content, &from_iri).ok_or_else(
                        || {
                            CliError::Usage(
                                "unable to inject SPARQL FROM clause for remote time travel; \
                                 please write the query as `SELECT ... WHERE { ... }` or include an explicit FROM"
                                    .to_string(),
                            )
                        },
                    )?;
                    client.explain_sparql(&remote_alias, &injected).await?
                }
                (detect::QueryFormat::JsonLd, Some(at_str), true) => {
                    // Remote `--at --explain` over JSON-LD: inject the
                    // time-suffixed `from` into the body and POST to the
                    // ledger-scoped explain endpoint. Path drives auth,
                    // body's `from` drives snapshot selection.
                    let spec = parse_time_spec(at_str);
                    let suffix = time_spec_to_suffix(&spec);
                    let from_id = attach_time_suffix_preserving_fragment(&remote_alias, &suffix);
                    let mut json_query: serde_json::Value = serde_json::from_str(&content)?;
                    if let Some(obj) = json_query.as_object_mut() {
                        obj.insert("from".to_string(), serde_json::Value::String(from_id));
                    } else {
                        return Err(CliError::Input(
                            "JSON-LD query must be a JSON object".to_string(),
                        ));
                    }
                    client.explain_jsonld(&remote_alias, &json_query).await?
                }
                (detect::QueryFormat::Sparql, None, true) => {
                    client.explain_sparql(&remote_alias, &content).await?
                }
                (detect::QueryFormat::JsonLd, None, true) => {
                    let json_query: serde_json::Value = serde_json::from_str(&content)?;
                    client.explain_jsonld(&remote_alias, &json_query).await?
                }
                (detect::QueryFormat::Sparql, Some(at_str), false) => {
                    // Remote time-travel via ledger-scoped SPARQL: path drives
                    // auth (`can_read("mydb:main")` matches scoped tokens),
                    // injected FROM carries the @t:N suffix for snapshot
                    // resolution. We inject a single FROM before WHERE for
                    // the common SELECT shape; queries with their own
                    // FROM/FROM NAMED must encode time travel there.
                    if fluree_db_api::sparql_dataset_ledger_ids(&content)
                        .map(|v| !v.is_empty())
                        .unwrap_or(false)
                    {
                        return Err(CliError::Usage(
                            "SPARQL query already contains FROM/FROM NAMED; \
                             for remote time travel, encode time travel in the FROM IRI \
                             (e.g., FROM <ledger@t:1>) instead of using --at"
                                .to_string(),
                        ));
                    }
                    let spec = parse_time_spec(at_str);
                    let suffix = time_spec_to_suffix(&spec);
                    let from_iri = attach_time_suffix_preserving_fragment(&remote_alias, &suffix);
                    let injected = inject_sparql_from_before_where(&content, &from_iri).ok_or_else(
                        || {
                            CliError::Usage(
                                "unable to inject SPARQL FROM clause for remote time travel; \
                                 please write the query as `SELECT ... WHERE { ... }` or include an explicit FROM"
                                    .to_string(),
                            )
                        },
                    )?;
                    client.query_sparql(&remote_alias, &injected).await?
                }
                (detect::QueryFormat::JsonLd, Some(at_str), false) => {
                    // Remote time-travel via ledger-scoped JSON-LD: path
                    // drives auth, body's `from` carries the @t:N suffix.
                    let spec = parse_time_spec(at_str);
                    let suffix = time_spec_to_suffix(&spec);
                    let from_id = attach_time_suffix_preserving_fragment(&remote_alias, &suffix);
                    let mut json_query: serde_json::Value = serde_json::from_str(&content)?;
                    if let Some(obj) = json_query.as_object_mut() {
                        obj.insert("from".to_string(), serde_json::Value::String(from_id));
                    } else {
                        return Err(CliError::Input(
                            "JSON-LD query must be a JSON object".to_string(),
                        ));
                    }
                    client.query_jsonld(&remote_alias, &json_query).await?
                }
                (detect::QueryFormat::Sparql, None, false) => {
                    client.query_sparql(&remote_alias, &content).await?
                }
                (detect::QueryFormat::JsonLd, None, false) => {
                    let json_query: serde_json::Value = serde_json::from_str(&content)?;
                    client.query_jsonld(&remote_alias, &json_query).await?
                }
            };
            let elapsed = timer.elapsed();

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            if explain {
                println!("{}", serde_json::to_string_pretty(&result)?);
                eprintln!("(explain, {})", format_duration(elapsed));
                return Ok(());
            }

            // Safety: rendering a `table` for millions of rows will effectively hang the CLI.
            // For table output, show a preview unless the result set is small (or --bench is used).
            let effective_limit = if !bench
                && output_format == OutputFormatKind::Table
                && query_format == detect::QueryFormat::Sparql
                && limit.is_none()
            {
                let total = result
                    .pointer("/results/bindings")
                    .and_then(|v| v.as_array())
                    .map(std::vec::Vec::len)
                    .unwrap_or(0);
                if total > DEFAULT_TABLE_PREVIEW_ROWS {
                    Some(DEFAULT_TABLE_PREVIEW_ROWS)
                } else {
                    None
                }
            } else {
                limit
            };

            let output =
                output::format_result(&result, output_format, query_format, effective_limit)?;
            println!("{}", output.text);
            print_footer(output.total_rows, effective_limit, elapsed);
        }
        LedgerMode::Local { fluree, alias } => {
            // Load a single view (optionally time-traveled) and execute against it.
            // This avoids the redundant `fluree.ledger()` load (and duplicate BinaryIndexStore load)
            // that previously occurred before the lazy graph query loaded its own view.
            let view = match at {
                Some(at_str) => {
                    let spec = parse_time_spec(at_str);
                    fluree.db_at_with_default_context(&alias, spec).await?
                }
                None => fluree.db_with_default_context(&alias).await?,
            };

            let view = if policy.is_set() {
                let opts = policy.to_options().map_err(CliError::Usage)?;
                fluree.wrap_policy(view, &opts, None).await?
            } else {
                view
            };

            if explain {
                let timer = Instant::now();
                let resp = match query_format {
                    detect::QueryFormat::Sparql => {
                        fluree.explain_sparql(&view, content.as_str()).await?
                    }
                    detect::QueryFormat::JsonLd => {
                        let json_query: serde_json::Value = serde_json::from_str(&content)?;
                        fluree.explain(&view, &json_query).await?
                    }
                };
                let elapsed = timer.elapsed();
                println!("{}", serde_json::to_string_pretty(&resp)?);
                eprintln!("(explain, {})", format_duration(elapsed));
                return Ok(());
            }

            // Benchmark mode should measure query execution only (not view loading or result formatting).
            // For JSON-LD queries, we also exclude CLI-side JSON parsing from the timed region.
            let (result, elapsed) = if bench {
                let timer = Instant::now();
                let result = match query_format {
                    detect::QueryFormat::Sparql => fluree.query(&view, content.as_str()).await?,
                    detect::QueryFormat::JsonLd => {
                        let json_query: serde_json::Value = serde_json::from_str(&content)?;
                        fluree.query(&view, &json_query).await?
                    }
                };
                (result, timer.elapsed())
            } else {
                // Default behavior: include view load + query + formatting in the reported time.
                let timer = Instant::now();
                let result = match query_format {
                    detect::QueryFormat::Sparql => fluree.query(&view, content.as_str()).await?,
                    detect::QueryFormat::JsonLd => {
                        let json_query: serde_json::Value = serde_json::from_str(&content)?;
                        fluree.query(&view, &json_query).await?
                    }
                };
                (result, timer.elapsed())
            };

            if bench {
                // Benchmark output should be representative but cheap: show a table preview
                // without materializing full SPARQL JSON / full-result formatting.
                match query_format {
                    detect::QueryFormat::Sparql => {
                        // CONSTRUCT/DESCRIBE produce graph results (not tabular SPARQL JSON).
                        // For bench mode, avoid expensive graph materialization and just report size.
                        if result.output.construct_template().is_some() {
                            println!(
                                "(graph result: {} triples)",
                                format_count(result.row_count())
                            );
                            print_footer(result.row_count(), None, elapsed);
                            return Ok(());
                        }

                        if let Some(output) = output::format_sparql_table_from_result(
                            &result,
                            &view.snapshot,
                            Some(BENCH_ROWS),
                        )? {
                            println!("{}", output.text);
                            print_footer(output.total_rows, Some(BENCH_ROWS), elapsed);
                        } else {
                            // Rare fallback: GROUP BY produces grouped bindings requiring
                            // disaggregation, so fall back to the existing JSON-based formatter.
                            let formatted_json = result.to_sparql_json(&view.snapshot)?;
                            let output = output::format_result(
                                &formatted_json,
                                OutputFormatKind::Table,
                                query_format,
                                Some(BENCH_ROWS),
                            )?;
                            println!("{}", output.text);
                            print_footer(output.total_rows, Some(BENCH_ROWS), elapsed);
                        }
                    }
                    detect::QueryFormat::JsonLd => {
                        // JSON-LD can be nested; keep bench output in the lightweight TSV form.
                        let (text, total_rows) =
                            result.to_tsv_limited(&view.snapshot, BENCH_ROWS)?;
                        print!("{text}");
                        print_footer(total_rows, Some(BENCH_ROWS), elapsed);
                    }
                }
            } else if matches!(output_format, OutputFormatKind::Tsv | OutputFormatKind::Csv) {
                // Delimited fast path: write bytes directly to stdout (no JSON intermediate).
                let total_rows = result.row_count();
                let fmt_timer = Instant::now();
                let bytes = if output_format == OutputFormatKind::Tsv {
                    result.to_tsv_bytes(&view.snapshot)?
                } else {
                    result.to_csv_bytes(&view.snapshot)?
                };
                let fmt_elapsed = fmt_timer.elapsed();
                use std::io::Write;
                std::io::stdout().write_all(&bytes)?;
                eprintln!(
                    "({} rows, query: {}, {output_format}: {})",
                    format_count(total_rows),
                    format_duration(elapsed),
                    format_duration(fmt_elapsed),
                );
            } else {
                // JSON-LD queries can produce nested expansion results; always render as JSON.
                let display_format = match output_format {
                    OutputFormatKind::TypedJson => OutputFormatKind::TypedJson,
                    _ if query_format == detect::QueryFormat::JsonLd => OutputFormatKind::Json,
                    _ => output_format,
                };

                // Graph results (SPARQL CONSTRUCT/DESCRIBE) don't have a meaningful table view.
                let display_format = if query_format == detect::QueryFormat::Sparql
                    && result.output.construct_template().is_some()
                    && display_format == OutputFormatKind::Table
                {
                    OutputFormatKind::Json
                } else {
                    display_format
                };

                // Safety: rendering a `table` for millions of rows will effectively hang the CLI.
                // For table output, show a preview unless the result set is small.
                let effective_limit = if display_format == OutputFormatKind::Table
                    && query_format == detect::QueryFormat::Sparql
                    && limit.is_none()
                {
                    let total = result.row_count();
                    if total > DEFAULT_TABLE_PREVIEW_ROWS {
                        Some(DEFAULT_TABLE_PREVIEW_ROWS)
                    } else {
                        None
                    }
                } else {
                    limit
                };

                // Fast path: SPARQL table output (avoid materializing full SPARQL JSON).
                if query_format == detect::QueryFormat::Sparql
                    && display_format == OutputFormatKind::Table
                {
                    let render_timer = Instant::now();
                    if let Some(output) = output::format_sparql_table_from_result(
                        &result,
                        &view.snapshot,
                        effective_limit,
                    )? {
                        let render_elapsed = render_timer.elapsed();
                        println!("{}", output.text);
                        if let Some(n) = effective_limit {
                            eprintln!(
                                "(first {} of {} rows, query: {}, render: {})",
                                format_count(n),
                                format_count(output.total_rows),
                                format_duration(elapsed),
                                format_duration(render_elapsed),
                            );
                        } else {
                            eprintln!(
                                "({} rows, query: {}, render: {})",
                                format_count(output.total_rows),
                                format_duration(elapsed),
                                format_duration(render_elapsed),
                            );
                        }
                        return Ok(());
                    }
                }

                // Full formatting path
                let render_timer = Instant::now();
                let formatted_json = if query_format == detect::QueryFormat::Sparql
                    && result.output.construct_template().is_some()
                {
                    // CONSTRUCT/DESCRIBE graph results: render as JSON-LD.
                    result.to_construct(&view.snapshot)?
                } else if display_format == OutputFormatKind::TypedJson {
                    let mut config = fluree_db_api::FormatterConfig::typed_json();
                    if normalize_arrays {
                        config = config.with_normalize_arrays();
                    }
                    result.format_async(view.as_graph_db_ref(), &config).await?
                } else if normalize_arrays && query_format == detect::QueryFormat::JsonLd {
                    let config = fluree_db_api::FormatterConfig::jsonld().with_normalize_arrays();
                    result.format_async(view.as_graph_db_ref(), &config).await?
                } else {
                    match query_format {
                        detect::QueryFormat::Sparql => result.to_sparql_json(&view.snapshot)?,
                        detect::QueryFormat::JsonLd => {
                            result.to_jsonld_async(view.as_graph_db_ref()).await?
                        }
                    }
                };
                let output = output::format_result(
                    &formatted_json,
                    display_format,
                    query_format,
                    effective_limit,
                )?;
                let render_elapsed = render_timer.elapsed();
                println!("{}", output.text);
                eprintln!(
                    "({} rows, query: {}, render: {})",
                    format_count(output.total_rows),
                    format_duration(elapsed),
                    format_duration(render_elapsed),
                );
            }
        }
    }

    Ok(())
}

/// Print the timing/row-count footer line to stderr.
fn print_footer(total_rows: usize, limit: Option<usize>, elapsed: std::time::Duration) {
    let time_str = format_duration(elapsed);
    match limit {
        Some(n) if n < total_rows => {
            eprintln!(
                "(first {} of {} rows, {})",
                format_count(n),
                format_count(total_rows),
                time_str
            );
        }
        _ => {
            eprintln!("({} rows, {})", format_count(total_rows), time_str);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{attach_time_suffix_preserving_fragment, inject_sparql_from_before_where};

    #[test]
    fn attach_time_suffix_preserves_fragment() {
        assert_eq!(
            attach_time_suffix_preserving_fragment("myledger:main#txn-meta", "@t:1"),
            "myledger:main@t:1#txn-meta"
        );
        assert_eq!(
            attach_time_suffix_preserving_fragment("myledger:main", "@t:1"),
            "myledger:main@t:1"
        );
    }

    #[test]
    fn inject_sparql_from_before_where_inserts_once() {
        let q = "SELECT * WHERE { ?s ?p ?o }";
        let out = inject_sparql_from_before_where(q, "myledger:main@t:1").unwrap();
        assert_eq!(out, "SELECT * FROM <myledger:main@t:1> WHERE { ?s ?p ?o }");
    }
}
