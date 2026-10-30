//! Delta graph source endpoints: `delta/map`, and the read-only `delta/catalog/*`
//! and `delta/r2rml/*` that lead up to it.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::FlureeHeaders;
use crate::state::AppState;
use crate::telemetry::{create_request_span, extract_request_id, extract_trace_id};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::Instrument;

use super::ledger::forward_write_request;

/// Request body for `POST /v1/fluree/delta/map`
#[derive(Deserialize)]
pub struct DeltaMapRequest {
    /// Graph source name
    pub name: String,
    /// Directory the mapping's `rr:tableName`s resolve beneath
    /// (`dbo.orders` → `<root>/dbo/orders`)
    pub root: Option<String>,
    /// Explicit table name → table location
    #[serde(default)]
    pub tables: BTreeMap<String, String>,
    /// R2RML mapping content (Turtle by default)
    pub r2rml: String,
    /// R2RML mapping media type
    pub r2rml_type: Option<String>,
    /// Branch name
    pub branch: Option<String>,
    /// Model ledger (`name:branch`) whose default graph supplies the source's
    /// view policies and class/property hierarchy.
    pub model: Option<String>,
    /// `default-allow` for governed requests that match no policy.
    pub default_allow: Option<bool>,
    /// Materialization: the column that marks a row as deleted, as for
    /// `/iceberg/map`. Omit to merge rows into their subjects; a change feed's
    /// deletes retract either way.
    pub delete_column: Option<String>,
    /// Values of `delete_column` that mark a row as deleted; a `null` element
    /// matches a NULL value. Required when `delete_column` is set.
    #[serde(default)]
    pub delete_values: Vec<Option<String>>,
    /// Materialization: the column that orders one key's rows within a version.
    pub order_by: Option<String>,
    /// The catalog and storage options; see [`ConnectionFields`].
    #[serde(flatten)]
    pub connection: ConnectionFields,
}

/// The rest of a Delta request: its Unity Catalog connection and storage
/// options, given either as the structured `unity` / `io` objects (exactly the
/// stored config's shape) or as the flat fields (`unity_uri`, `unity_catalog`,
/// `unity_schema`, `auth_bearer[_env]`, `oauth2_*`, `s3_*`, `azure_*`). An
/// `_env` field names a server environment variable listed in
/// `FLUREE_GRAPH_SOURCE_SECRET_ENV_VARS`. Read by
/// [`fluree_db_api::delta_connection_from_json`].
pub type ConnectionFields = serde_json::Map<String, serde_json::Value>;

/// The connection of a request that names its tables' storage
/// (`reads_storage`) or only its catalog, through the one codec and then the
/// SSRF guard on the URLs it names.
fn connection(
    fields: ConnectionFields,
    reads_storage: bool,
    rules: &fluree_db_api::SecretRules,
) -> Result<fluree_db_api::DeltaConnection> {
    let conn = fluree_db_api::delta_connection_from_json(fields, reads_storage, rules)
        .map_err(super::iceberg::input_error)?;
    let unity = conn.unity.as_ref();
    super::iceberg_ssrf::guard_connection_urls(
        unity.map(|u| u.uri.as_str()),
        unity.and_then(|u| super::iceberg_ssrf::token_url(&u.auth)),
        conn.io.s3_endpoint.as_deref(),
    )?;
    Ok(conn)
}

/// Every field a Delta route takes beside its connection: `delta/map`'s
/// (which `delta/r2rml/validate` takes too), and those of the catalog routes
/// (browse's `depth`, preview's and verify's `table`, generate's
/// `base_namespace`, `per_table_overrides` and `options`; its `tables` is
/// map's name). Each route ignores the ones it does not take itself, rather
/// than refusing them, so a client can send one payload to browse, preview,
/// verify, generate and map. A route reads its own fields before this
/// applies; a key no Delta route knows is still refused.
const ROUTE_FIELDS: &[&str] = &[
    // delta/map
    "name",
    "root",
    "tables",
    "r2rml",
    "r2rml_type",
    "branch",
    "model",
    "default_allow",
    "delete_column",
    "delete_values",
    "order_by",
    // catalog/browse
    "depth",
    // catalog/preview, catalog/verify
    "table",
    // r2rml/generate (`tables` above)
    "base_namespace",
    "per_table_overrides",
    "options",
];

/// The storage options: what reading a table's data files takes. Browse and
/// generate read Unity's record of a table only, never its files, so they
/// ignore these. The routes that read files (map, validate, verify) use
/// them, and preview and verify refuse an Azure principal (see
/// [`unity_table_connection`]).
const STORAGE_FIELDS: &[&str] = &[
    "io",
    "s3_region",
    "s3_endpoint",
    "s3_path_style",
    "azure_tenant_id",
    "azure_client_id",
    "azure_client_secret",
    "azure_client_secret_env",
];

/// A request's connection fields, without the other routes' own fields
/// (see [`ROUTE_FIELDS`]) and, when the route reads no data files
/// (`reads_storage` false), without the storage options.
fn connection_fields(mut fields: ConnectionFields, reads_storage: bool) -> ConnectionFields {
    for key in ROUTE_FIELDS {
        fields.remove(*key);
    }
    if !reads_storage {
        for key in STORAGE_FIELDS {
            fields.remove(*key);
        }
    }
    fields
}

/// A catalog route's Unity Catalog connection, which it cannot do without.
fn required_unity(conn: fluree_db_api::DeltaConnection) -> Result<fluree_db_api::DeltaUnityConfig> {
    conn.unity.ok_or_else(|| {
        super::iceberg::input_error(fluree_db_api::GraphSourceInputError::incomplete(
            "unity_uri",
            &["unity_uri", "unity"],
        ))
    })
}

/// Response for `POST /v1/fluree/delta/map`
#[derive(Serialize)]
pub struct DeltaMapResponse {
    pub graph_source_id: String,
    pub mapping_source: String,
    pub triples_map_count: usize,
    pub table_count: usize,
    pub table_names: Vec<String>,
    pub mapping_validated: bool,
    /// Current Delta version of each mapped table that opened at registration.
    pub table_versions: BTreeMap<String, u64>,
    /// Mapped tables that could not be read at registration, and why.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub table_warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_warnings: Vec<String>,
}

/// Map Delta tables as a graph source
///
/// POST /v1/fluree/delta/map
pub async fn delta_map(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }
    delta_map_local(state, request).await.into_response()
}

async fn delta_map_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let headers = FlureeHeaders::from_headers(request.headers())?;
    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: DeltaMapRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let span = create_request_span(
        "delta:map",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.name),
        None,
        None,
    );
    async move {
        tracing::info!(status = "start", name = %req.name, "delta map requested");

        let rules = fluree_db_api::SecretRules::server(&state.fluree);
        let result = state
            .fluree
            .create_delta_graph_source(build_delta_config(req, &rules)?)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            graph_source_id = %result.graph_source_id,
            "delta graph source mapped"
        );
        Ok((
            StatusCode::CREATED,
            Json(DeltaMapResponse {
                graph_source_id: result.graph_source_id,
                mapping_source: result.mapping_source,
                triples_map_count: result.triples_map_count,
                table_count: result.table_names.len(),
                table_names: result.table_names,
                mapping_validated: result.mapping_validated,
                table_versions: result.table_versions,
                table_warnings: result.table_warnings,
                model_warnings: result.model_warnings,
            }),
        ))
    }
    .instrument(span)
    .await
}

fn build_delta_config(
    req: DeltaMapRequest,
    rules: &fluree_db_api::SecretRules,
) -> Result<fluree_db_api::DeltaCreateConfig> {
    let conn = connection(connection_fields(req.connection, true), true, rules)?;
    let delete = req
        .delete_column
        .map(|column| {
            let convention = fluree_db_api::DeleteConvention {
                column,
                deleted_values: req.delete_values,
            };
            convention
                .validate()
                .map_err(|e| ServerError::bad_request(format!("invalid delete convention: {e}")))
                .map(|()| convention)
        })
        .transpose()?;
    Ok(fluree_db_api::DeltaCreateConfig {
        unity: conn.unity,
        name: req.name,
        branch: req.branch,
        root: req.root,
        tables: req.tables,
        io: conn.io,
        mapping: fluree_db_api::R2rmlMappingInput::Content(req.r2rml),
        mapping_media_type: req.r2rml_type,
        model: req.model,
        default_allow: req.default_allow,
        delete,
        order_by: req.order_by,
    })
}

// =============================================================================
// Read-only: the catalog, and a mapping before it is registered
// =============================================================================

async fn parse_body<T: serde::de::DeserializeOwned>(request: Request) -> Result<T> {
    let bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))
}

fn span_for(operation: &str, state: &AppState, request: &Request) -> Result<tracing::Span> {
    let headers = FlureeHeaders::from_headers(request.headers())?;
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    Ok(create_request_span(
        operation,
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
        None,
    ))
}

/// Request body for `POST /v1/fluree/delta/catalog/browse`
#[derive(Deserialize)]
pub struct DeltaBrowseRequest {
    /// `schemas` or `tables` (the default): how far a listing of one catalog
    /// reaches. With no `unity_catalog` the catalogs are listed; with a
    /// `unity_schema` too, that schema's tables.
    #[serde(default)]
    pub depth: fluree_db_api::DeltaBrowseDepth,
    /// The Unity Catalog connection (no storage options).
    #[serde(flatten)]
    pub connection: ConnectionFields,
}

/// Request body for `POST /v1/fluree/delta/catalog/preview` and `…/verify`
#[derive(Deserialize)]
pub struct DeltaTableRequest {
    /// Completed from `unity_catalog` and `unity_schema` when short
    pub table: String,
    /// The Unity Catalog connection, and S3 options for the table Unity
    /// places (its credentials are Unity's, so no Azure principal).
    #[serde(flatten)]
    pub connection: ConnectionFields,
}

#[derive(Deserialize)]
pub struct DeltaTableOverride {
    /// As the table is named in `tables`
    pub table: String,
    /// Columns of the subject, in place of the declared or chosen key
    #[serde(default)]
    pub subject_key: Option<Vec<String>>,
    #[serde(default)]
    pub class_name: Option<String>,
    #[serde(default)]
    pub subject_strategy: Option<fluree_db_api::SubjectStrategy>,
}

/// Request body for `POST /v1/fluree/delta/r2rml/generate`
#[derive(Deserialize)]
pub struct DeltaGenerateRequest {
    /// The Unity Catalog connection (no storage options).
    #[serde(flatten)]
    pub connection: ConnectionFields,
    /// In output order
    pub tables: Vec<String>,
    /// The base every generated IRI derives from
    pub base_namespace: String,
    #[serde(default)]
    pub per_table_overrides: Vec<DeltaTableOverride>,
    #[serde(default)]
    pub options: fluree_db_api::GenerateOptions,
}

/// A Unity-placed table's connection (preview, verify): Unity issues its
/// credentials, so an Azure principal would read nothing, and is refused
/// rather than ignored: the caller would believe the table was read as it.
fn unity_table_connection(
    fields: ConnectionFields,
    rules: &fluree_db_api::SecretRules,
) -> Result<fluree_db_api::DeltaConnection> {
    let conn = connection(connection_fields(fields, true), true, rules)?;
    if conn.io.azure.is_some() {
        return Err(super::iceberg::input_error(
            fluree_db_api::GraphSourceInputError::invalid(
                "azure",
                "Unity Catalog issues a Unity table's storage credentials; an Azure service \
                 principal applies to delta/map and delta/r2rml/validate",
            ),
        ));
    }
    Ok(conn)
}

/// A catalog-only route's connection (browse, generate), which reads no data
/// files: no storage options.
fn unity_catalog_connection(
    fields: ConnectionFields,
    rules: &fluree_db_api::SecretRules,
) -> Result<fluree_db_api::DeltaConnection> {
    connection(connection_fields(fields, false), false, rules)
}

/// POST /v1/fluree/delta/catalog/browse
pub async fn delta_catalog_browse(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    async {
        let span = span_for("delta:catalog:browse", &state, &request)?;
        let req: DeltaBrowseRequest = parse_body(request).await?;
        async {
            let rules = fluree_db_api::SecretRules::server(&state.fluree);
            let unity = required_unity(unity_catalog_connection(req.connection, &rules)?)?;
            let listing = state
                .fluree
                .browse_delta_unity(&unity, req.depth)
                .await
                .map_err(ServerError::Api)?;
            Ok::<_, ServerError>((StatusCode::OK, Json(listing)))
        }
        .instrument(span)
        .await
    }
    .await
    .into_response()
}

/// POST /v1/fluree/delta/catalog/preview
pub async fn delta_catalog_preview(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    async {
        let span = span_for("delta:catalog:preview", &state, &request)?;
        let req: DeltaTableRequest = parse_body(request).await?;
        async {
            let rules = fluree_db_api::SecretRules::server(&state.fluree);
            let unity = required_unity(unity_table_connection(req.connection, &rules)?)?;
            let preview = state
                .fluree
                .preview_delta_unity_table(&unity, &req.table)
                .await
                .map_err(ServerError::Api)?;
            Ok::<_, ServerError>((StatusCode::OK, Json(preview)))
        }
        .instrument(span)
        .await
    }
    .await
    .into_response()
}

/// POST /v1/fluree/delta/catalog/verify
pub async fn delta_catalog_verify(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    async {
        let span = span_for("delta:catalog:verify", &state, &request)?;
        let req: DeltaTableRequest = parse_body(request).await?;
        async {
            let rules = fluree_db_api::SecretRules::server(&state.fluree);
            let conn = unity_table_connection(req.connection, &rules)?;
            let io = conn.io.clone();
            let unity = required_unity(conn)?;
            let access = state
                .fluree
                .verify_delta_unity_table(&unity, &io, &req.table)
                .await
                .map_err(ServerError::Api)?;
            tracing::info!(table = %access.full_name, readable = access.readable, "delta table verified");
            Ok::<_, ServerError>((StatusCode::OK, Json(access)))
        }
        .instrument(span)
        .await
    }
    .await
    .into_response()
}

fn generate_request(
    req: DeltaGenerateRequest,
    rules: &fluree_db_api::SecretRules,
) -> Result<fluree_db_api::GenerateDeltaR2rmlRequest> {
    if req.tables.is_empty() {
        return Err(ServerError::bad_request(
            "at least one table is required for generate",
        ));
    }
    Ok(fluree_db_api::GenerateDeltaR2rmlRequest {
        unity: required_unity(unity_catalog_connection(req.connection, rules)?)?,
        tables: req.tables,
        base_namespace: req.base_namespace,
        per_table_overrides: req
            .per_table_overrides
            .into_iter()
            .map(|o| {
                let table_override = fluree_db_api::TableOverride {
                    primary_key: o.subject_key,
                    class_name: o.class_name,
                    subject_strategy: o.subject_strategy,
                };
                (o.table, table_override)
            })
            .collect(),
        options: req.options,
    })
}

/// POST /v1/fluree/delta/r2rml/generate
pub async fn delta_r2rml_generate(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    async {
        let span = span_for("delta:r2rml:generate", &state, &request)?;
        let req: DeltaGenerateRequest = parse_body(request).await?;
        async {
            let api_req =
                generate_request(req, &fluree_db_api::SecretRules::server(&state.fluree))?;
            let response = state
                .fluree
                .generate_delta_r2rml(api_req)
                .await
                .map_err(ServerError::Api)?;
            tracing::info!(
                tables = response.tables.len(),
                diagnostics = response.diagnostics.len(),
                "delta r2rml generated"
            );
            Ok::<_, ServerError>((StatusCode::OK, Json(response)))
        }
        .instrument(span)
        .await
    }
    .await
    .into_response()
}

/// A `delta/map` body, whose `name` validation has no use for.
fn validate_request(mut body: serde_json::Value) -> Result<DeltaMapRequest> {
    if let Some(fields) = body.as_object_mut() {
        fields.entry("name").or_insert_with(|| "validate".into());
    }
    serde_json::from_value(body).map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))
}

/// Check a mapping against the tables a `delta/map` of the same body would
/// read. Registers nothing.
///
/// POST /v1/fluree/delta/r2rml/validate
pub async fn delta_r2rml_validate(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    async {
        let span = span_for("delta:r2rml:validate", &state, &request)?;
        let req = validate_request(parse_body(request).await?)?;
        async {
            let rules = fluree_db_api::SecretRules::server(&state.fluree);
            let response = state
                .fluree
                .validate_delta_r2rml(&build_delta_config(req, &rules)?)
                .await
                .map_err(ServerError::Api)?;
            Ok::<_, ServerError>((StatusCode::OK, Json(response)))
        }
        .instrument(span)
        .await
    }
    .await
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(extra: serde_json::Value) -> DeltaMapRequest {
        let mut body = serde_json::json!({"name": "sales", "r2rml": ""});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(body).unwrap()
    }

    fn rules(listed: &str) -> fluree_db_api::SecretRules {
        fluree_db_api::SecretRules::listed(listed, false)
    }

    fn stored(config: &fluree_db_api::DeltaCreateConfig) -> serde_json::Value {
        serde_json::to_value(config.to_gs_config("m.ttl")).unwrap()
    }

    #[test]
    fn a_unity_request_stores_its_catalog_and_a_listed_variables_name() {
        let req = request(serde_json::json!({
            "unity_uri": "https://workspace.example.com",
            "unity_catalog": "main",
            "oauth2_client_id": "app",
            "oauth2_client_secret_env": "SP_SECRET",
        }));
        let config = build_delta_config(req, &rules("SP_SECRET")).unwrap();
        config.validate().unwrap();
        let unity = &stored(&config)["unity"];
        assert_eq!(unity["uri"], "https://workspace.example.com");
        assert_eq!(unity["catalog"], "main");
        assert_eq!(unity["auth"]["client_secret"]["env_var"], "SP_SECRET");
        assert_eq!(
            unity["auth"]["token_url"],
            "https://workspace.example.com/oidc/v1/token"
        );
    }

    #[test]
    fn a_unity_secret_may_name_only_a_listed_variable() {
        for (field, extra) in [
            ("auth_bearer_env", serde_json::json!({})),
            (
                "oauth2_client_secret_env",
                serde_json::json!({"oauth2_client_id": "app"}),
            ),
        ] {
            let mut body = serde_json::json!({
                "unity_uri": "https://workspace.example.com",
                field: "AWS_SECRET_ACCESS_KEY",
            });
            body.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let req = request(body);
            let err = build_delta_config(req, &rules("SP_SECRET"))
                .err()
                .unwrap_or_else(|| panic!("{field} accepted"));
            assert!(
                err.to_string().contains(fluree_db_api::SECRET_ENV_VARS_ENV),
                "{err}"
            );
        }
    }

    #[test]
    fn materialize_options_are_stored_with_the_source() {
        let config = build_delta_config(
            request(serde_json::json!({
                "root": "s3://lake/Tables",
                "delete_column": "op",
                "delete_values": ["D", null],
                "order_by": "updated_at",
            })),
            &rules(""),
        )
        .unwrap();
        let stored = stored(&config);
        assert_eq!(stored["delete"]["column"], "op");
        assert_eq!(
            stored["delete"]["deleted_values"],
            serde_json::json!(["D", null])
        );
        assert_eq!(stored["order_by"], "updated_at");

        let bare = build_delta_config(
            request(serde_json::json!({"root": "s3://lake/Tables", "delete_column": "op"})),
            &rules(""),
        );
        assert!(
            bare.err()
                .is_some_and(|e| e.to_string().contains("delete convention")),
            "a delete column with no values is refused"
        );
    }

    #[test]
    fn a_source_by_path_is_built_as_before() {
        let config = build_delta_config(
            request(serde_json::json!({"root": "s3://lake/Tables"})),
            &rules(""),
        )
        .unwrap();
        assert!(config.unity.is_none());
        assert!(stored(&config).get("unity").is_none());
    }

    fn generate(extra: serde_json::Value) -> Result<fluree_db_api::GenerateDeltaR2rmlRequest> {
        let mut body = serde_json::json!({
            "unity_uri": "https://workspace.example.com",
            "unity_catalog": "main",
            "auth_bearer_env": "UNITY_TOKEN",
            "tables": ["sales.orders", "sales.customers"],
            "base_namespace": "https://example.org/",
        });
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        generate_request(serde_json::from_value(body).unwrap(), &rules("UNITY_TOKEN"))
    }

    #[test]
    fn a_generate_request_carries_its_tables_overrides_and_options() {
        let req = generate(serde_json::json!({
            "per_table_overrides": [
                {"table": "sales.orders", "subject_key": ["order_id", "line"], "class_name": "Purchase"},
                {"table": "sales.customers", "subject_strategy": "identifier"},
            ],
            "options": {"emit_fk_joins": false},
        }))
        .unwrap();
        assert_eq!(req.tables, ["sales.orders", "sales.customers"]);
        assert_eq!(req.unity.catalog.as_deref(), Some("main"));
        let orders = &req.per_table_overrides["sales.orders"];
        assert_eq!(
            orders.primary_key.as_deref(),
            Some(&["order_id".to_string(), "line".to_string()][..])
        );
        assert_eq!(orders.class_name.as_deref(), Some("Purchase"));
        assert_eq!(
            req.per_table_overrides["sales.customers"].subject_strategy,
            Some(fluree_db_api::SubjectStrategy::Identifier)
        );
        assert!(!req.options.emit_fk_joins);
        // An option left out keeps its default.
        assert!(req.options.keep_fk_keys_as_literals);
    }

    #[test]
    fn a_catalog_endpoint_needs_a_catalog_and_a_listed_variable() {
        assert!(generate(serde_json::json!({})).is_ok());
        let stray = generate(serde_json::json!({"unity_uri": null}));
        assert!(stray
            .err()
            .unwrap()
            .to_string()
            .contains("unity_catalog needs unity_uri"));
        let none = generate(serde_json::json!({
            "unity_uri": null, "unity_catalog": null, "auth_bearer_env": null,
        }));
        assert!(none.err().unwrap().to_string().contains("unity_uri"));
        let unlisted = generate(serde_json::json!({"auth_bearer_env": "AWS_SECRET_ACCESS_KEY"}));
        assert!(unlisted
            .err()
            .unwrap()
            .to_string()
            .contains(fluree_db_api::SECRET_ENV_VARS_ENV));
    }

    #[test]
    fn every_route_ignores_the_other_routes_fields() {
        // One payload, carrying every route's own fields, sent to browse,
        // preview, verify, generate, validate and map.
        let unity = serde_json::json!({"unity_uri": "https://workspace.example.com",
            "unity_catalog": "main", "auth_bearer_env": "UNITY_TOKEN"});
        let mut body = unity.clone();
        for (key, value) in serde_json::json!({
            // map, validate
            "name": "sales", "root": "s3://lake/t", "r2rml": "", "r2rml_type": "text/turtle",
            "branch": "main", "model": "m:main", "default_allow": true,
            "delete_column": "op", "delete_values": ["d"], "order_by": "ts",
            // browse
            "depth": "schemas",
            // preview, verify
            "table": "sales.orders",
            // generate (its `tables` is set below: map's is an object)
            "base_namespace": "https://example.org/", "per_table_overrides": [], "options": {},
            // storage
            "s3_region": "us-east-2", "s3_endpoint": "https://s3.example.com",
            "s3_path_style": true,
        })
        .as_object()
        .unwrap()
        {
            body[key] = value.clone();
        }
        let rules = rules("UNITY_TOKEN,SP_SECRET");
        let json = |v| serde_json::to_value(v).unwrap();
        let bare = json(
            fluree_db_api::delta_connection_from_json(
                unity.as_object().unwrap().clone(),
                false,
                &rules,
            )
            .unwrap()
            .unity,
        );
        let generate = |body: &serde_json::Value| {
            let mut generate = body.clone();
            generate["tables"] = serde_json::json!(["sales.orders"]);
            generate_request(serde_json::from_value(generate).unwrap(), &rules)
        };

        let browse: DeltaBrowseRequest = serde_json::from_value(body.clone()).unwrap();
        let conn = unity_catalog_connection(browse.connection, &rules).unwrap();
        assert_eq!(json(conn.unity), bare);
        assert_eq!(json(Some(generate(&body).unwrap().unity)), bare);
        let table: DeltaTableRequest = serde_json::from_value(body.clone()).unwrap();
        let conn = unity_table_connection(table.connection, &rules).unwrap();
        assert_eq!(json(conn.unity), bare);
        assert_eq!(conn.io.s3_region.as_deref(), Some("us-east-2"));
        let map = build_delta_config(serde_json::from_value(body.clone()).unwrap(), &rules);
        assert!(map.is_ok(), "{:?}", map.err());
        assert!(validate_request(body.clone()).is_ok());

        // A key no Delta route knows is still refused, on every route.
        let mut unknown = body.clone();
        unknown["unity_schemaa"] = "s".into();
        let refused = |e: ServerError| assert!(e.to_string().contains("unity_schemaa"), "{e}");
        let browse: DeltaBrowseRequest = serde_json::from_value(unknown.clone()).unwrap();
        refused(unity_catalog_connection(browse.connection, &rules).unwrap_err());
        let table: DeltaTableRequest = serde_json::from_value(unknown.clone()).unwrap();
        refused(unity_table_connection(table.connection, &rules).unwrap_err());
        refused(generate(&unknown).unwrap_err());
        refused(build_delta_config(serde_json::from_value(unknown).unwrap(), &rules).unwrap_err());
    }

    #[test]
    fn an_azure_principal_is_ignored_only_where_no_data_file_is_read() {
        let unity = serde_json::json!({"unity_uri": "https://workspace.example.com",
            "unity_catalog": "main", "auth_bearer_env": "UNITY_TOKEN"});
        let azure = |mut body: serde_json::Value| {
            for (key, value) in serde_json::json!({"azure_tenant_id": "t",
                "azure_client_id": "c", "azure_client_secret_env": "SP_SECRET"})
            .as_object()
            .unwrap()
            {
                body[key] = value.clone();
            }
            body
        };
        let structured = serde_json::json!({
            "unity": {"uri": "https://workspace.example.com", "catalog": "main",
                "auth": {"type": "bearer", "token": {"env_var": "UNITY_TOKEN"}}},
            "io": {"s3_region": "us-east-2", "azure": {"type": "client_secret",
                "tenant_id": "t", "client_id": "c", "client_secret": {"env_var": "SP_SECRET"}}},
        });
        let rules = rules("UNITY_TOKEN,SP_SECRET");
        for body in [azure(unity), structured] {
            // Browse and generate read Unity's record only: ignored.
            let browse: DeltaBrowseRequest = serde_json::from_value(body.clone()).unwrap();
            let conn = unity_catalog_connection(browse.connection, &rules);
            assert!(conn.is_ok(), "{body}: {:?}", conn.err());
            let mut generate = body.clone();
            generate["tables"] = serde_json::json!(["sales.orders"]);
            generate["base_namespace"] = "https://example.org/".into();
            let generate = generate_request(serde_json::from_value(generate).unwrap(), &rules);
            assert!(generate.is_ok(), "{body}: {:?}", generate.err());
            // Preview and verify: Unity issues the table's credentials, so the
            // principal would not be the identity read with. Refused.
            let mut table = body.clone();
            table["table"] = "sales.orders".into();
            let table: DeltaTableRequest = serde_json::from_value(table).unwrap();
            let e = unity_table_connection(table.connection, &rules).unwrap_err();
            assert!(
                matches!(&e, ServerError::Api(fluree_db_api::ApiError::GraphSourceInput(i))
                    if i.field == "azure"),
                "{body}: {e}"
            );
        }
    }

    #[test]
    fn a_validate_body_is_a_map_body_whose_name_is_optional() {
        let unnamed = validate_request(serde_json::json!({"root": "s3://lake/t", "r2rml": "x"}));
        assert_eq!(unnamed.unwrap().name, "validate");
        let named = validate_request(
            serde_json::json!({"name": "sales", "root": "s3://lake/t", "r2rml": "x"}),
        );
        assert_eq!(named.unwrap().name, "sales");
        assert!(validate_request(serde_json::json!({"root": "s3://lake/t"})).is_err());
    }
}
