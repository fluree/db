//! Live check: the AWS Glue onboarding/introspection surface (catalog browse +
//! table preview) via the public API — the paths that used to hard-reject Glue.
//!
//! Run:
//! ```
//! AWS_PROFILE=aj-sandbox AWS_REGION=us-east-1 \
//!   cargo run --example glue_onboarding -p fluree-db-api --features iceberg
//! ```

use fluree_db_api::{
    browse_iceberg_catalog, preview_iceberg_table, BrowseDepth, IcebergConnectionConfig, StatsTier,
    TableIdentifier,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let conn = IcebergConnectionConfig::glue(Some(region), None);

    println!("=== browse_iceberg_catalog (Glue, depth=Tables) ===");
    let browse = browse_iceberg_catalog(conn.clone(), BrowseDepth::Tables).await?;
    println!("namespaces: {:?}", browse.namespaces);
    println!("tables ({}):", browse.tables.len());
    for t in browse.tables.iter().take(12) {
        println!("  {}.{}", t.namespace, t.name);
    }

    println!("\n=== preview_iceberg_table (Glue) enterprise_dw.dim_store ===");
    let table = TableIdentifier {
        namespace: "enterprise_dw".into(),
        name: "dim_store".into(),
    };
    let preview = preview_iceberg_table(conn, table, StatsTier::Schema).await?;
    println!("{preview:#?}");

    println!("\nONBOARDING OK");
    Ok(())
}
