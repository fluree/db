//! Exercise the production OTLP HTTP setup and flush against a local collector.
//! This target runs alone because logging and the provider are process-global.

use fluree_db_server::telemetry::{init_logging, shutdown_tracer, TelemetryConfig};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn http_export_delivers_spans_on_shutdown() {
    // Set exporter configuration before creating the runtime or collector threads.
    for key in [
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        "OTEL_EXPORTER_OTLP_COMPRESSION",
        "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
    ] {
        std::env::remove_var(key);
    }
    std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf");
    std::env::set_var("OTEL_TRACES_SAMPLER", "always_on");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let collector = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/traces"))
            .and(header("content-type", "application/x-protobuf"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&collector)
            .await;

        init_logging(&TelemetryConfig {
            log_filter: "off".into(),
            otel_service_name: Some("fluree-http-export-test".into()),
            otel_endpoint: Some(format!("{}/v1/traces", collector.uri())),
            ..TelemetryConfig::default()
        });
        tracing::info_span!(target: "fluree_db_server", "http_export_regression").in_scope(|| {});

        shutdown_tracer().await;
        let requests = collector.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1, "shutdown must flush the recorded span");
        // Protobuf length-delimited strings must contain both the resource and
        // span names; a successful empty request is not evidence of an export.
        let body = &requests[0].body;
        for expected in ["fluree-http-export-test", "http_export_regression"] {
            assert!(
                body.windows(expected.len())
                    .any(|bytes| bytes == expected.as_bytes()),
                "export is missing {expected}"
            );
        }
    });
}
