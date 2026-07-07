//! Env-gated live integration tests: the AWS Glue onboarding/introspection
//! surface (catalog browse + table preview) via the public API — the paths that
//! used to hard-reject Glue. SKIP unless `FLUREE_GLUE_IT_DATABASE` is set (no
//! live dependency in CI). Requires ambient AWS credentials.
//!
//! Run (matches the sandbox harness):
//! ```
//! AWS_PROFILE=aj-sandbox AWS_REGION=us-east-1 \
//!   FLUREE_GLUE_IT_DATABASE=enterprise_dw FLUREE_GLUE_IT_TABLE=dim_store \
//!   cargo test -p fluree-db-api --features iceberg --test it_glue_live -- --nocapture
//! ```
#![cfg(feature = "iceberg")]

use fluree_db_api::{
    browse_iceberg_catalog, preview_iceberg_table, BrowseDepth, IcebergConnectionConfig, StatsTier,
    TableIdentifier,
};

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.is_empty())
}
fn region() -> Option<String> {
    env("FLUREE_GLUE_IT_REGION").or_else(|| env("AWS_REGION"))
}
fn conn() -> IcebergConnectionConfig {
    IcebergConnectionConfig::glue(region(), env("FLUREE_GLUE_IT_CATALOG_ID"))
}

#[tokio::test]
async fn glue_browse_lists_database_and_tables() {
    let Some(db) = env("FLUREE_GLUE_IT_DATABASE") else {
        eprintln!("skip glue_browse: set FLUREE_GLUE_IT_DATABASE");
        return;
    };
    let browse = browse_iceberg_catalog(conn(), BrowseDepth::Tables)
        .await
        .expect("browse should succeed for a Glue catalog");
    assert!(
        browse.namespaces.iter().any(|n| n == &db),
        "namespaces {:?} should include {db}",
        browse.namespaces
    );
    assert!(
        browse.tables.iter().any(|t| t.namespace == db),
        "expected at least one table in namespace {db}"
    );
    eprintln!(
        "glue browse OK: {} namespaces, {} tables",
        browse.namespaces.len(),
        browse.tables.len()
    );
}

#[tokio::test]
async fn glue_preview_returns_schema() {
    let Some(namespace) = env("FLUREE_GLUE_IT_DATABASE") else {
        eprintln!("skip glue_preview: set FLUREE_GLUE_IT_DATABASE + FLUREE_GLUE_IT_TABLE");
        return;
    };
    let name =
        env("FLUREE_GLUE_IT_TABLE").expect("FLUREE_GLUE_IT_TABLE required when DATABASE set");
    let preview = preview_iceberg_table(
        conn(),
        TableIdentifier { namespace, name },
        StatsTier::Schema,
    )
    .await
    .expect("preview should succeed for a Glue table (via the S3 metadata fallback)");
    assert!(
        !preview.schema.columns.is_empty(),
        "preview schema should list columns"
    );
    assert!(
        preview.schema.format_version >= 1,
        "expected an Iceberg format version"
    );
    eprintln!(
        "glue preview OK: {} columns, format v{}",
        preview.schema.columns.len(),
        preview.schema.format_version
    );
}
