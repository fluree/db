//! Telemetry module for logging and tracing setup
//!
//! Provides unified logging configuration, optional OTEL tracing, and graceful shutdown.
//! Follows the logging plan for performance-conscious observability.

use crate::config::ServerConfig;
use std::borrow::Cow;
use std::env;
#[cfg(feature = "otel")]
use tracing_subscriber::filter::Targets;
#[cfg(feature = "otel")]
use tracing_subscriber::Layer;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

/// Telemetry configuration
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    /// Primary log filter (RUST_LOG env var)
    pub log_filter: String,
    /// Fallback log level if RUST_LOG not set
    pub default_level: String,
    /// Request ID header name (default: "x-request-id")
    pub request_id_header: String,
    /// Log format ("human" or "json")
    pub log_format: LogFormat,
    /// Sensitive data handling
    pub sensitive_data: SensitiveDataHandling,
    /// Query text logging mode
    pub query_text_logging: QueryTextLogging,
    /// OTEL service name (if OTEL enabled)
    pub otel_service_name: Option<String>,
    /// OTEL endpoint (if OTEL enabled)
    pub otel_endpoint: Option<String>,
}

impl TelemetryConfig {
    /// Check if OTEL tracing should be enabled
    ///
    /// Requires both service name and endpoint to be set
    pub fn is_otel_enabled(&self) -> bool {
        self.otel_service_name.is_some() && self.otel_endpoint.is_some()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogFormat {
    Human,
    Json,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SensitiveDataHandling {
    /// Show all data
    Off,
    /// Mask sensitive fields (default)
    Mask,
    /// Hash sensitive data
    Hash,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryTextLogging {
    /// Don't log query text (default)
    Off,
    /// Log full query text at debug level
    Full,
    /// Log SHA256 hash of query at info level, full text at trace
    Hash,
}

impl TelemetryConfig {
    /// Create telemetry config with server config for CLI log level support
    pub fn with_server_config(server_config: &ServerConfig) -> Self {
        let rust_log = env::var("RUST_LOG").unwrap_or_default();
        let default_level = if rust_log.is_empty() {
            // Fallback to LOG_LEVEL env var, then server config, then "info"
            env::var("LOG_LEVEL").unwrap_or_else(|_| server_config.log_level.clone())
        } else {
            server_config.log_level.clone() // Not used when RUST_LOG is set, but store for consistency
        };

        Self::from_env_with_defaults(default_level)
    }
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        let rust_log = env::var("RUST_LOG").unwrap_or_default();
        let default_level = if rust_log.is_empty() {
            // Fallback to LOG_LEVEL when RUST_LOG is unset
            env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string())
        } else {
            "info".to_string() // Not used when RUST_LOG is set
        };

        Self::from_env_with_defaults(default_level)
    }
}

impl TelemetryConfig {
    fn from_env_with_defaults(default_level: String) -> Self {
        Self {
            log_filter: env::var("RUST_LOG").unwrap_or_default(),
            default_level,
            request_id_header: env::var("LOG_REQUEST_ID_HEADER")
                .unwrap_or_else(|_| "x-request-id".to_string()),
            log_format: match env::var("LOG_FORMAT")
                .unwrap_or_default()
                .to_lowercase()
                .as_str()
            {
                "json" => LogFormat::Json,
                _ => LogFormat::Human,
            },
            sensitive_data: match env::var("LOG_SENSITIVE_DATA")
                .unwrap_or_else(|_| "mask".to_string())
                .to_lowercase()
                .as_str()
            {
                "off" => SensitiveDataHandling::Off,
                "hash" => SensitiveDataHandling::Hash,
                _ => SensitiveDataHandling::Mask,
            },
            query_text_logging: match env::var("LOG_QUERY_TEXT")
                .unwrap_or_else(|_| "0".to_string())
                .to_lowercase()
                .as_str()
            {
                "1" | "true" | "full" => QueryTextLogging::Full,
                "hash" => QueryTextLogging::Hash,
                _ => QueryTextLogging::Off,
            },
            otel_service_name: env::var("OTEL_SERVICE_NAME").ok(),
            otel_endpoint: env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok(),
        }
    }
}

/// Initialize logging and tracing
///
/// Sets up the global tracing subscriber with:
/// - EnvFilter for level filtering
/// - Optional JSON formatting for CloudWatch
/// - Optional OTEL tracing export
///
/// Safe to call multiple times - will only initialize once.
pub fn init_logging(config: &TelemetryConfig) {
    // Check if a global subscriber is already set (e.g., from tests)
    if tracing::dispatcher::has_been_set() {
        tracing::debug!("tracing subscriber already initialized, skipping");
        return;
    }

    let filter = if config.log_filter.is_empty() {
        EnvFilter::new(&config.default_level)
    } else {
        EnvFilter::new(&config.log_filter)
    };

    // When OTEL is enabled, use a dual-layer architecture:
    // - Console layer: uses RUST_LOG (EnvFilter) — operator controls all crates, all levels
    // - OTEL layer: uses a hardcoded Targets filter — only fluree_* crates at DEBUG
    //
    // This prevents third-party crate spans (hyper, tonic, h2, tower-http) from flooding
    // the OTEL BatchSpanProcessor queue when RUST_LOG=debug is set for investigation.
    #[cfg(feature = "otel")]
    {
        if config.is_otel_enabled() {
            // Hardcoded allowlist: only fluree_* crates, capped at DEBUG (never TRACE).
            // TRACE-level spans (per-leaf-node cursors etc.) generate thousands per query
            // and would overwhelm the batch exporter.
            let otel_targets = Targets::new()
                .with_target("fluree_db_server", tracing::Level::DEBUG)
                .with_target("fluree_db_api", tracing::Level::DEBUG)
                .with_target("fluree_db_query", tracing::Level::DEBUG)
                .with_target("fluree_db_transact", tracing::Level::DEBUG)
                .with_target("fluree_db_indexer", tracing::Level::DEBUG)
                .with_target("fluree_db_ledger", tracing::Level::DEBUG)
                .with_target("fluree_db_connection", tracing::Level::DEBUG)
                .with_target("fluree_db_nameservice", tracing::Level::DEBUG)
                .with_target("fluree_db_core", tracing::Level::DEBUG);

            let subscriber = tracing_subscriber::registry()
                .with(init_otel_layer(config).with_filter(otel_targets))
                .with(
                    tracing_subscriber::fmt::layer()
                        .compact()
                        .with_filter(filter),
                );

            let _ = tracing::dispatcher::set_global_default(tracing::Dispatch::new(subscriber));
            return;
        }
    }

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().compact());

    let _ = tracing::dispatcher::set_global_default(tracing::Dispatch::new(subscriber));
}

/// Initialize OTEL tracing layer
///
/// Only call this if OTEL environment variables are set.
/// Returns a tracing layer that exports spans via OTLP.
///
/// SYNC: This function mirrors `fluree-db-cli/src/main.rs::init_otel_layer`.
/// If you change the exporter, sampler, batch processor, or Targets filter here,
/// apply the same change there. Both must stay in lock-step.
/// See CLAUDE.md § "Tracing & OTEL Spans" for the maintenance protocol.
#[cfg(feature = "otel")]
static OTEL_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

#[cfg(feature = "otel")]
fn init_otel_layer(
    config: &TelemetryConfig,
) -> impl Layer<tracing_subscriber::Registry> + Send + Sync {
    use opentelemetry::{global, KeyValue};
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::runtime;
    use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use opentelemetry_sdk::Resource;
    use tracing_opentelemetry::OpenTelemetryLayer;

    // Determine protocol (default: grpc)
    let protocol = env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        .unwrap_or_else(|_| "grpc".to_string())
        .to_lowercase();

    // Configure OTLP span exporter based on protocol
    let exporter = match protocol.as_str() {
        "http/protobuf" | "http" => opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(config.otel_endpoint.as_ref().unwrap())
            .build()
            .expect("failed to build OTLP HTTP span exporter"),
        _ => opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(config.otel_endpoint.as_ref().unwrap())
            .build()
            .expect("failed to build OTLP gRPC span exporter"),
    };

    // Configure sampler based on environment
    let sampler = match env::var("OTEL_TRACES_SAMPLER")
        .unwrap_or_else(|_| "always_on".to_string())
        .to_lowercase()
        .as_str()
    {
        "always_off" => Sampler::AlwaysOff,
        "traceidratio" => {
            let ratio = env::var("OTEL_TRACES_SAMPLER_ARG")
                .unwrap_or_else(|_| "1.0".to_string())
                .parse::<f64>()
                .unwrap_or(1.0);
            Sampler::TraceIdRatioBased(ratio)
        }
        "parentbased_always_on" => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
        "parentbased_always_off" => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
        _ => Sampler::AlwaysOn, // default
    };

    // Increase max queue size from default (~2048) to handle span volume from
    // large queries at debug level without dropping parent spans.
    let batch = BatchSpanProcessor::builder(exporter, runtime::Tokio)
        .with_batch_config(
            opentelemetry_sdk::trace::BatchConfigBuilder::default()
                .with_max_queue_size(1_000_000)
                .build(),
        )
        .build();

    let resource = Resource::builder_empty()
        .with_attributes(vec![
            KeyValue::new(
                "service.name",
                config.otel_service_name.as_ref().unwrap().clone(),
            ),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ])
        .build();

    // Configure tracer provider
    let tracer_provider = SdkTracerProvider::builder()
        .with_span_processor(batch)
        .with_sampler(sampler)
        .with_resource(resource)
        .build();

    // Set global tracer provider
    let _ = OTEL_PROVIDER.set(tracer_provider.clone());
    global::set_tracer_provider(tracer_provider);

    // Create tracing layer
    OpenTelemetryLayer::new(global::tracer("fluree-db"))
}

/// Shutdown telemetry gracefully
///
/// Call this before application exit to ensure all spans are exported.
/// This is a no-op if OTEL is not enabled.
pub async fn shutdown_tracer() {
    #[cfg(feature = "otel")]
    {
        if let Some(provider) = OTEL_PROVIDER.get() {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
        }
    }
    // No-op when OTEL feature is disabled
}

/// Extract request ID from headers
///
/// Checks multiple common header names in priority order:
/// 1. Configured header name
/// 2. x-amzn-trace-id (AWS Lambda)
/// 3. x-trace-id (generic)
///
/// Returns None if no request ID found.
pub fn extract_request_id(
    headers: &axum::http::HeaderMap,
    config: &TelemetryConfig,
) -> Option<String> {
    // Check configured header first
    if let Some(value) = headers.get(&config.request_id_header) {
        if let Ok(id) = value.to_str() {
            return Some(id.to_string());
        }
    }

    // Fallback to AWS trace ID
    if let Some(value) = headers.get("x-amzn-trace-id") {
        if let Ok(id) = value.to_str() {
            return Some(id.to_string());
        }
    }

    // Generic trace ID fallback
    if let Some(value) = headers.get("x-trace-id") {
        if let Ok(id) = value.to_str() {
            return Some(id.to_string());
        }
    }

    None
}

/// Extract trace ID from headers or generate one
///
/// Looks for OTEL trace ID headers, falls back to request ID.
/// Returns None if no trace context available.
pub fn extract_trace_id(headers: &axum::http::HeaderMap) -> Option<String> {
    // Check for OTEL traceparent header
    if let Some(traceparent) = headers.get("traceparent") {
        if let Ok(tp) = traceparent.to_str() {
            // Parse traceparent format: version-trace_id-span_id-flags
            if let Some(trace_id) = tp.split('-').nth(1) {
                return Some(trace_id.to_string());
            }
        }
    }

    // Check for x-trace-id
    if let Some(trace_id) = headers.get("x-trace-id") {
        if let Ok(id) = trace_id.to_str() {
            return Some(id.to_string());
        }
    }

    None
}

/// Create a request span with correlation context and dynamic OTEL naming.
///
/// This is the main entry point for creating spans at request boundaries.
/// Includes all correlation fields for CloudWatch filtering.
///
/// When `input_format` is provided, the OTEL span name becomes
/// `"operation:format"` (e.g. `"query:sparql"`, `"transact:turtle"`),
/// producing descriptive names in Jaeger/Tempo instead of generic "request".
pub fn create_request_span(
    operation: &str,
    request_id: Option<&str>,
    trace_id: Option<&str>,
    ledger_id: Option<&str>,
    tenant_id: Option<&str>,
    input_format: Option<&str>,
) -> tracing::Span {
    let otel_name: Cow<'_, str> = match input_format {
        Some(fmt) => format!("{operation}:{fmt}").into(),
        None => operation.into(),
    };
    // error_code: intentionally Empty on success (OTEL convention — omit, don't record "ok").
    // Only recorded on error paths via set_span_error_code().
    tracing::info_span!(
        "request",
        otel.name = %otel_name,
        operation = operation,
        request_id = request_id,
        trace_id = trace_id,
        ledger_id = ledger_id,
        tenant_id = tenant_id,
        error_code = tracing::field::Empty,
        query_hash = tracing::field::Empty,
    )
}

/// Helper to set error code on a span
pub fn set_span_error_code(span: &tracing::Span, error_code: &str) {
    span.record("error_code", error_code);
}

/// Utility to mask sensitive data in logs
///
/// Applies the configured sensitive data handling policy.
pub fn mask_sensitive_data(data: &str, handling: &SensitiveDataHandling) -> String {
    match handling {
        SensitiveDataHandling::Off => data.to_string(),
        SensitiveDataHandling::Mask => {
            // Simple masking: replace with asterisks but keep length
            "*".repeat(data.len().min(20))
        }
        SensitiveDataHandling::Hash => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            data.hash(&mut hasher);
            format!("{:x}", hasher.finish())
        }
    }
}

/// Handle query text logging according to configuration
///
/// Returns (info_level_text, debug_level_text, trace_level_text) where:
/// - info_level_text: text to log at info level (or None)
/// - debug_level_text: text to log at debug level (or None)
/// - trace_level_text: text to log at trace level (or None)
pub fn handle_query_text_logging(
    query_text: &str,
    config: &TelemetryConfig,
) -> (Option<String>, Option<String>, Option<String>) {
    match config.query_text_logging {
        QueryTextLogging::Off => (None, None, None),
        QueryTextLogging::Full => (
            None,
            Some(query_text.to_string()),
            Some(query_text.to_string()),
        ),
        QueryTextLogging::Hash => {
            // Log fast non-crypto hash at info level, full text at trace
            use ahash::AHasher;
            use std::hash::Hasher;
            let mut hasher = AHasher::default();
            hasher.write(query_text.as_bytes());
            let hash = format!("ahash64:{:x}", hasher.finish());
            (Some(hash), None, Some(query_text.to_string()))
        }
    }
}

/// Check if query text logging is enabled
pub fn should_log_query_text(config: &TelemetryConfig) -> bool {
    !matches!(config.query_text_logging, QueryTextLogging::Off)
}

/// Log query text at appropriate levels based on configuration
pub fn log_query_text(query_text: &str, config: &TelemetryConfig, span: &tracing::Span) {
    let (info_text, debug_text, trace_text) = handle_query_text_logging(query_text, config);

    if let Some(hash) = info_text {
        span.record("query_hash", hash.as_str());
        tracing::info!(query_hash = %hash, "query logged");
    }

    if let Some(full_text) = debug_text {
        // Apply sensitive data masking if configured
        let logged_text = mask_sensitive_data(&full_text, &config.sensitive_data);
        tracing::debug!(query_text = %logged_text, "full query text");
    }

    // Only log at trace level if query text logging is enabled
    if let Some(raw_text) = trace_text {
        if tracing::enabled!(tracing::Level::TRACE) {
            let logged_text = mask_sensitive_data(&raw_text, &config.sensitive_data);
            tracing::trace!(query_text = %logged_text, "raw query text");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    #[test]
    fn test_extract_request_id() {
        let config = TelemetryConfig::default();
        let mut headers = HeaderMap::new();

        // Test configured header
        headers.insert("x-request-id", "test-123".parse().unwrap());
        assert_eq!(
            extract_request_id(&headers, &config),
            Some("test-123".to_string())
        );

        // Test AWS header fallback
        let mut headers = HeaderMap::new();
        headers.insert("x-amzn-trace-id", "aws-456".parse().unwrap());
        assert_eq!(
            extract_request_id(&headers, &config),
            Some("aws-456".to_string())
        );
    }

    #[test]
    fn test_extract_trace_id() {
        let mut headers = HeaderMap::new();

        // Test traceparent header
        headers.insert(
            "traceparent",
            "00-12345678901234567890123456789012-1234567890123456-01"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            extract_trace_id(&headers),
            Some("12345678901234567890123456789012".to_string())
        );
    }

    #[test]
    fn test_mask_sensitive_data() {
        let data = "secret-password";

        assert_eq!(
            mask_sensitive_data(data, &SensitiveDataHandling::Off),
            "secret-password"
        );
        assert_eq!(
            mask_sensitive_data(data, &SensitiveDataHandling::Mask),
            "***************"
        );
        // Hash should be deterministic and different from input
        let hashed = mask_sensitive_data(data, &SensitiveDataHandling::Hash);
        assert_ne!(hashed, "secret-password");
        assert_eq!(
            mask_sensitive_data(data, &SensitiveDataHandling::Hash),
            hashed
        ); // deterministic
    }
}
