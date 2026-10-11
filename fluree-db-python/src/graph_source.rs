//! Graph sources: Iceberg, Delta and SQL tables mapped to RDF by R2RML and
//! queried in place, registered from plain specs the Python layer builds.

use crate::convert::to_json;
use crate::error::{api_error, invalid_request};
use crate::runtime::block_on;
use fluree_db_api::{
    CatalogMode, CatalogModeArgs, DeltaAzureFields, DeltaCreateConfig, DeltaUnityFields, DropMode,
    Fluree, IcebergCreateConfig, R2rmlCreateConfig, R2rmlMappingInput, SqlAuthConfig,
    SqlConfigValue, SqlCreateConfig,
};
use fluree_db_nameservice::GraphSourceType;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;

/// What every graph source spec carries.
#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct Common {
    name: String,
    branch: Option<String>,
    /// R2RML, as Turtle (or JSON-LD, by `mapping_type`).
    mapping: String,
    mapping_type: Option<String>,
    model: Option<String>,
    default_allow: Option<bool>,
}

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct IcebergSpec {
    table_location: Option<String>,
    catalog_uri: Option<String>,
    glue: Option<GlueSpec>,
    s3_tables: Option<S3TablesSpec>,
    table: Option<String>,
    warehouse: Option<String>,
    auth: Option<Py<PyAny>>,
    /// `None` = REST's default (on); only REST can vend.
    vended_credentials: Option<bool>,
    s3_region: Option<String>,
    s3_endpoint: Option<String>,
    s3_path_style: bool,
    order_by: Option<String>,
}

/// An AWS Glue Data Catalog (`fluree.Glue`).
#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct GlueSpec {
    region: Option<String>,
    catalog_id: Option<String>,
}

/// An AWS S3 Tables table bucket (`fluree.S3Tables`).
#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct S3TablesSpec {
    table_bucket_arn: String,
    region: Option<String>,
}

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct DeltaSpec {
    root: Option<String>,
    tables: BTreeMap<String, String>,
    unity: Option<UnitySpec>,
    s3_region: Option<String>,
    s3_endpoint: Option<String>,
    s3_path_style: bool,
    azure: Option<AzureSpec>,
}

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct UnitySpec {
    uri: String,
    catalog: Option<String>,
    schema: Option<String>,
    bearer: Option<Py<PyAny>>,
    client_id: Option<String>,
    client_secret: Option<Py<PyAny>>,
    token_url: Option<String>,
    scope: Option<String>,
}

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct AzureSpec {
    tenant_id: String,
    client_id: String,
    client_secret: Option<String>,
    client_secret_env: Option<String>,
}

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct SqlSpec {
    endpoint: String,
    dialect: String,
    protocol: String,
    catalog: Option<String>,
    schema: Option<String>,
    user: Option<String>,
    auth: Option<Py<PyAny>>,
    session: BTreeMap<String, String>,
    allow_duplicate_subjects: bool,
}

/// A serde value from its JSON form, as the Python layer spells it.
fn from_py<T: DeserializeOwned>(py: Python<'_>, value: &Py<PyAny>, what: &str) -> PyResult<T> {
    serde_json::from_value(to_json(value.bind(py))?)
        .map_err(|e| invalid_request(format!("invalid {what}: {e}")))
}

fn auth(py: Python<'_>, auth: Option<&Py<PyAny>>) -> PyResult<SqlAuthConfig> {
    auth.map_or(Ok(SqlAuthConfig::None), |a| from_py(py, a, "auth"))
}

/// The registered source, and whatever the registration noticed but did not
/// refuse: a table it could not read, an endpoint it could not reach.
fn registered<'py>(
    py: Python<'py>,
    id: &str,
    tables: &[String],
    warnings: Vec<String>,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("id", id)?;
    out.set_item("tables", tables)?;
    out.set_item("warnings", warnings)?;
    Ok(out)
}

pub(crate) fn map_iceberg<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    common: Common,
    spec: IcebergSpec,
) -> PyResult<Bound<'py, PyDict>> {
    // Exactly one place the tables are found; which one names the catalog mode.
    let mode = match (
        &spec.table_location,
        &spec.catalog_uri,
        &spec.glue,
        &spec.s3_tables,
    ) {
        (Some(_), None, None, None) => "direct",
        (None, Some(_), None, None) => "rest",
        (None, None, Some(_), None) => "glue",
        (None, None, None, Some(_)) => "s3tables",
        _ => {
            return Err(invalid_request(
                "give exactly one of a table_location (direct), a catalog_uri (REST catalog), \
                 glue (AWS Glue Data Catalog) or s3_tables (AWS S3 Tables)",
            ))
        }
    };
    // Settings only a REST catalog uses: refused by any other mode rather than
    // silently ignored. Turning vending off agrees with every mode, so only an
    // explicit request for it counts.
    let rest_only: Vec<&'static str> = [
        ("warehouse", spec.warehouse.is_some()),
        ("auth", spec.auth.is_some()),
        ("vended_credentials", spec.vended_credentials == Some(true)),
    ]
    .into_iter()
    .filter_map(|(name, given)| given.then_some(name))
    .collect();
    // The same parse the CLI and server use: the mapping names the tables, so a
    // catalog mode needs no `table` of its own.
    let mode_args = CatalogModeArgs {
        mode,
        catalog_uri: spec.catalog_uri.as_deref(),
        table_location: spec.table_location.as_deref(),
        region: spec
            .glue
            .as_ref()
            .and_then(|g| g.region.as_deref())
            .or_else(|| spec.s3_tables.as_ref().and_then(|t| t.region.as_deref())),
        catalog_id: spec.glue.as_ref().and_then(|g| g.catalog_id.as_deref()),
        table_bucket_arn: spec.s3_tables.as_ref().map(|t| t.table_bucket_arn.as_str()),
        rest_only: &rest_only,
    };
    let iceberg =
        IcebergCreateConfig::from_mode(&common.name, mode_args, spec.table.as_deref(), true)
            .map_err(|e| invalid_request(e.message(str::to_string)))?;
    let mut config = R2rmlCreateConfig {
        iceberg,
        mapping: R2rmlMappingInput::Content(common.mapping),
        mapping_media_type: None,
    };
    let rest = spec.catalog_uri.is_some();
    if let CatalogMode::Rest(catalog) = &mut config.iceberg.connection.catalog_mode {
        catalog.warehouse = spec.warehouse;
        catalog.auth = auth(py, spec.auth.as_ref())?;
        config.iceberg.connection.io.vended_credentials = spec.vended_credentials.unwrap_or(true);
    }
    let io = &mut config.iceberg.connection.io;
    io.s3_region = spec.s3_region;
    io.s3_endpoint = spec.s3_endpoint;
    io.s3_path_style = spec.s3_path_style;
    config.iceberg.branch = common.branch;
    config.iceberg.order_by = spec.order_by;
    config.iceberg.model = common.model;
    config.iceberg.default_allow = common.default_allow;
    config.mapping_media_type = common.mapping_type;
    let created = block_on(py, fluree.create_r2rml_graph_source(config))?.map_err(api_error)?;
    let mut warnings = created.model_warnings;
    if rest && !created.connection_tested {
        warnings.push(format!(
            "could not reach the catalog at {}; the source is registered, and queries will \
             fail until it can be reached",
            created.catalog_uri
        ));
    }
    registered(py, &created.graph_source_id, &created.table_names, warnings)
}

pub(crate) fn map_delta<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    common: Common,
    spec: DeltaSpec,
) -> PyResult<Bound<'py, PyDict>> {
    let unity = spec
        .unity
        .map(|unity| -> PyResult<_> {
            let secret = |v: Option<&Py<PyAny>>| -> PyResult<Option<SqlConfigValue>> {
                v.map(|v| from_py(py, v, "secret")).transpose()
            };
            DeltaUnityFields {
                uri: Some(unity.uri),
                catalog: unity.catalog,
                schema: unity.schema,
                bearer: secret(unity.bearer.as_ref())?,
                oauth2_client_id: unity.client_id,
                oauth2_client_secret: secret(unity.client_secret.as_ref())?,
                oauth2_token_url: unity.token_url,
                oauth2_scope: unity.scope,
            }
            .into_config()
            .map_err(api_error)
        })
        .transpose()?
        .flatten();
    let azure = spec
        .azure
        .map(|azure| {
            DeltaAzureFields {
                tenant_id: Some(azure.tenant_id),
                client_id: Some(azure.client_id),
                client_secret: azure.client_secret,
                client_secret_env: azure.client_secret_env,
            }
            .into_auth()
            .map_err(api_error)
        })
        .transpose()?
        .flatten();
    let mut config = DeltaCreateConfig::new(&common.name, String::new(), common.mapping);
    config.root = spec.root;
    config.tables = spec.tables;
    config.unity = unity;
    config.io.s3_region = spec.s3_region;
    config.io.s3_endpoint = spec.s3_endpoint;
    config.io.s3_path_style = spec.s3_path_style;
    config.io.azure = azure;
    config.branch = common.branch;
    config.mapping_media_type = common.mapping_type;
    config.model = common.model;
    config.default_allow = common.default_allow;
    let created = block_on(py, fluree.create_delta_graph_source(config))?.map_err(api_error)?;
    let mut warnings = created.table_warnings;
    warnings.extend(created.model_warnings);
    registered(py, &created.graph_source_id, &created.table_names, warnings)
}

pub(crate) fn map_sql<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    common: Common,
    spec: SqlSpec,
) -> PyResult<Bound<'py, PyDict>> {
    let mut config = SqlCreateConfig::new(&common.name, &spec.endpoint, common.mapping);
    config.dialect = serde_json::from_value(serde_json::json!(spec.dialect))
        .map_err(|_| invalid_request(format!("unknown SQL dialect {:?}", spec.dialect)))?;
    config.protocol = serde_json::from_value(serde_json::json!(spec.protocol))
        .map_err(|_| invalid_request(format!("unknown wire protocol {:?}", spec.protocol)))?;
    config.catalog = spec.catalog;
    config.schema = spec.schema;
    config.user = spec.user;
    config.auth = auth(py, spec.auth.as_ref())?;
    config.session = spec.session;
    config.allow_duplicate_subjects = spec.allow_duplicate_subjects;
    config.branch = common.branch;
    config.mapping_media_type = common.mapping_type;
    config.model = common.model;
    config.default_allow = common.default_allow;
    let created = block_on(py, fluree.create_sql_graph_source(config))?.map_err(api_error)?;
    let mut warnings = created.mapping_warnings;
    warnings.extend(created.model_warnings);
    if !created.connection_tested {
        warnings.push(format!(
            "could not reach {}; the source is registered, and queries will fail until it \
             can be reached",
            created.endpoint
        ));
    }
    registered(py, &created.graph_source_id, &created.table_names, warnings)
}

fn kind(source_type: &GraphSourceType) -> String {
    match source_type {
        GraphSourceType::R2rml | GraphSourceType::Iceberg => "iceberg".into(),
        GraphSourceType::Sql => "sql".into(),
        GraphSourceType::Delta => "delta".into(),
        GraphSourceType::Bm25 => "bm25".into(),
        GraphSourceType::Vector => "vector".into(),
        GraphSourceType::Geo => "geo".into(),
        GraphSourceType::Unknown(other) => other.clone(),
    }
}

/// Every graph source not dropped: `{"id", "name", "branch", "kind"}`.
pub(crate) fn list<'py>(py: Python<'py>, fluree: &Fluree) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let records = block_on(py, fluree.nameservice().all_graph_source_records())?
        .map_err(|e| api_error(e.into()))?;
    let mut sources: Vec<_> = records.into_iter().filter(|r| !r.retracted).collect();
    sources.sort_by(|a, b| a.graph_source_id.as_str().cmp(b.graph_source_id.as_str()));
    sources
        .iter()
        .map(|record| {
            let out = PyDict::new(py);
            out.set_item("id", record.graph_source_id.as_str())?;
            out.set_item("name", &record.name)?;
            out.set_item("branch", &record.branch)?;
            out.set_item("kind", kind(&record.source_type))?;
            Ok(out)
        })
        .collect()
}

pub(crate) fn drop(py: Python<'_>, fluree: &Fluree, name: &str, branch: &str) -> PyResult<()> {
    block_on(
        py,
        fluree.drop_graph_source(name, Some(branch), DropMode::Hard),
    )?
    .map(|_| ())
    .map_err(api_error)
}

/// Bring an Iceberg source's rows into `into` as ledger data, reading only
/// what the source added since the last pass when it can.
pub(crate) fn materialize<'py>(
    py: Python<'py>,
    fluree: &Fluree,
    source: &str,
    into: &str,
    full: bool,
) -> PyResult<Bound<'py, PyDict>> {
    let result = block_on(
        py,
        fluree.materialize_r2rml_graph_source(source, into, full),
    )?
    .map_err(api_error)?;
    let out = PyDict::new(py);
    out.set_item("snapshot", result.to_snapshot_id)?;
    out.set_item("incremental", result.incremental)?;
    out.set_item("committed", result.committed)?;
    out.set_item("rows_read", result.rows_read)?;
    out.set_item("subjects_upserted", result.subjects_upserted)?;
    out.set_item("subjects_retracted", result.subjects_retracted)?;
    Ok(out)
}
