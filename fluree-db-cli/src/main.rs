use clap::Parser;
use fluree_db_cli::cli::{Cli, Commands};
use fluree_db_cli::error::exit_with_error;
#[cfg(feature = "otel")]
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::EnvFilter;
#[cfg(feature = "otel")]
use tracing_subscriber::Layer as _;

fn init_tracing(cli: &Cli) {
    // CLI tracing policy:
    //   --quiet  → always "off" (no logs, no matter what)
    //   --verbose → "info" level for fluree crates (useful diagnostics)
    //   default  → "off" (clean terminal, progress bars only)
    //   RUST_LOG → honoured only when neither --verbose nor --quiet is set,
    //              so developers can still get fine-grained control.
    let filter = if cli.quiet {
        EnvFilter::new("off")
    } else if cli.verbose {
        // --verbose: honour RUST_LOG if set, otherwise show info for fluree crates.
        EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())
    } else {
        // Default: suppress all logs. RUST_LOG is intentionally ignored so that
        // developer env vars don't leak log lines into the user-facing CLI output
        // (which uses progress bars on stderr). Use --verbose to see logs.
        EnvFilter::new("off")
    };

    let ansi = !(cli.no_color || std::env::var_os("NO_COLOR").is_some());

    // When the `otel` feature is enabled and OTEL env vars are set, use a
    // dual-layer subscriber (same architecture as fluree-db-server):
    //   - Console layer: controlled by --quiet/--verbose/RUST_LOG
    //   - OTEL layer: hardcoded Targets filter for fluree_* crates at DEBUG
    #[cfg(feature = "otel")]
    {
        let otel_service = std::env::var("OTEL_SERVICE_NAME").ok();
        let otel_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();

        if otel_service.is_some() && otel_endpoint.is_some() {
            let otel_targets = Targets::new()
                .with_target("fluree_db_cli", tracing::Level::DEBUG)
                .with_target("fluree_db_api", tracing::Level::DEBUG)
                .with_target("fluree_db_query", tracing::Level::DEBUG)
                .with_target("fluree_db_transact", tracing::Level::DEBUG)
                .with_target("fluree_db_indexer", tracing::Level::DEBUG)
                .with_target("fluree_db_ledger", tracing::Level::DEBUG)
                .with_target("fluree_db_connection", tracing::Level::DEBUG)
                .with_target("fluree_db_nameservice", tracing::Level::DEBUG)
                .with_target("fluree_db_core", tracing::Level::DEBUG);

            let otel_layer = init_otel_layer(
                otel_service.as_deref().unwrap(),
                otel_endpoint.as_deref().unwrap(),
            );

            let subscriber = tracing_subscriber::registry()
                .with(otel_layer.with_filter(otel_targets))
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_ansi(ansi)
                        .with_target(true)
                        .with_writer(std::io::stderr)
                        .compact()
                        .with_filter(filter),
                );

            let _ = tracing::dispatcher::set_global_default(tracing::Dispatch::new(subscriber));
            return;
        }
    }

    let subscriber = tracing_subscriber::registry().with(filter).with(
        tracing_subscriber::fmt::layer()
            .with_ansi(ansi)
            .with_target(true)
            .with_writer(std::io::stderr)
            .compact(),
    );

    let _ = tracing::dispatcher::set_global_default(tracing::Dispatch::new(subscriber));
}

/// Initialize the OTEL tracing layer (same configuration as fluree-db-server).
///
/// SYNC: This function mirrors `fluree-db-server/src/telemetry.rs::init_otel_layer`.
/// If you change the exporter, sampler, batch processor, or Targets filter here,
/// apply the same change there. Both must stay in lock-step.
/// See CLAUDE.md § "Tracing & OTEL Spans" for the maintenance protocol.
#[cfg(feature = "otel")]
static OTEL_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

#[cfg(feature = "otel")]
fn init_otel_layer(
    service_name: &str,
    endpoint: &str,
) -> impl tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync {
    use opentelemetry::{global, KeyValue};
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::runtime;
    use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use opentelemetry_sdk::Resource;
    use tracing_opentelemetry::OpenTelemetryLayer;

    let protocol = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        .unwrap_or_else(|_| "grpc".to_string())
        .to_lowercase();

    let exporter = match protocol.as_str() {
        "http/protobuf" | "http" => opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .expect("failed to build OTLP HTTP span exporter"),
        _ => opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .expect("failed to build OTLP gRPC span exporter"),
    };

    let sampler = match std::env::var("OTEL_TRACES_SAMPLER")
        .unwrap_or_else(|_| "always_on".to_string())
        .to_lowercase()
        .as_str()
    {
        "always_off" => Sampler::AlwaysOff,
        "traceidratio" => {
            let ratio = std::env::var("OTEL_TRACES_SAMPLER_ARG")
                .unwrap_or_else(|_| "1.0".to_string())
                .parse::<f64>()
                .unwrap_or(1.0);
            Sampler::TraceIdRatioBased(ratio)
        }
        "parentbased_always_on" => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
        "parentbased_always_off" => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
        _ => Sampler::AlwaysOn,
    };

    // Match server's queue size to handle span volume from large imports.
    let batch = BatchSpanProcessor::builder(exporter, runtime::Tokio)
        .with_batch_config(
            opentelemetry_sdk::trace::BatchConfigBuilder::default()
                .with_max_queue_size(1_000_000)
                .build(),
        )
        .build();

    let resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new("service.name", service_name.to_string()),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    let tracer_provider = SdkTracerProvider::builder()
        .with_span_processor(batch)
        .with_sampler(sampler)
        .with_resource(resource)
        .build();

    let _ = OTEL_PROVIDER.set(tracer_provider.clone());
    global::set_tracer_provider(tracer_provider);

    // Register W3C TraceContext propagator (kept in sync with fluree-db-server).
    //
    // SYNC: fluree-db-server/src/telemetry.rs::init_otel_layer registers the
    // same propagator. See CLAUDE.md § "Tracing & OTEL Spans".
    global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());

    OpenTelemetryLayer::new(global::tracer("fluree-cli"))
}

/// Flush and shutdown the OTEL tracer provider.
///
/// No-op when the `otel` feature is disabled or OTEL was not initialized.
async fn shutdown_tracer() {
    #[cfg(feature = "otel")]
    {
        if let Some(provider) = OTEL_PROVIDER.get() {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Disable color when --no-color flag or NO_COLOR env var is set.
    // We intentionally do NOT disable on "stdout is not a TTY" because errors
    // go to stderr — piping stdout (e.g., `fluree query ... | jq`) should not
    // strip color from error messages that appear on the terminal's stderr.
    if cli.no_color || std::env::var_os("NO_COLOR").is_some() {
        colored::control::set_override(false);
    }

    // Skip CLI tracing for:
    // - Server subcommands (Run, Child) that own the global tracing subscriber
    // - MCP serve (stdio transport uses stdout; any tracing to stderr could
    //   interfere with the JSON-RPC protocol)
    let skip_tracing = matches!(cli.command, Commands::Mcp { .. });

    #[cfg(feature = "server")]
    let skip_tracing = skip_tracing
        || matches!(
            cli.command,
            Commands::Server {
                action: fluree_db_cli::cli::ServerAction::Run { .. }
                    | fluree_db_cli::cli::ServerAction::Child { .. }
            }
        );

    if !skip_tracing {
        init_tracing(&cli);
    }

    let result = fluree_db_cli::run(cli).await;
    shutdown_tracer().await;

    if let Err(e) = result {
        exit_with_error(e);
    }
}
