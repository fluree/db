//! Reads Databricks tables through a live Unity Catalog. Ignored by default:
//!
//! ```text
//! UNITY_URI=https://<workspace> UNITY_TOKEN=… \
//! UNITY_TABLES=main.sales.orders=1000,main.sales.items=5000 \
//!   cargo test -p fluree-db-delta --test unity_live -- --ignored --nocapture
//! ```
//!
//! Each entry is `catalog.schema.table=<row count>`. The process needs no
//! storage credentials, and must have none for the result to mean anything.

use fluree_db_delta::{DeltaIoConfig, DeltaTable, UnityConfig, VersionSelector};
use fluree_db_iceberg::auth::AuthConfig;
use fluree_db_iceberg::ConfigValue;
use fluree_db_tabular::ColumnBatch;
use futures::StreamExt;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a Databricks workspace"]
async fn tables_are_read_by_name_with_credentials_unity_issues() {
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("set {name}"));
    let unity = UnityConfig {
        uri: var("UNITY_URI"),
        auth: AuthConfig::Bearer {
            token: ConfigValue::literal(var("UNITY_TOKEN")),
        },
        catalog: None,
        schema: None,
    };
    let io = DeltaIoConfig {
        s3_region: std::env::var("UNITY_S3_REGION").ok(),
        ..Default::default()
    };

    for entry in var("UNITY_TABLES").split(',') {
        let (name, expected) = entry.split_once('=').expect("name=rows");
        let table = DeltaTable::open_in_unity(name, &unity, name, &io)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let snapshot = table.snapshot(VersionSelector::Latest).await.unwrap();
        let rows: usize = snapshot
            .scan(&["id".to_string()], &[])
            .unwrap()
            .map(|b| b.expect("batch"))
            .collect::<Vec<ColumnBatch>>()
            .await
            .iter()
            .map(|b| b.num_rows)
            .sum();
        println!("{name}: version {}, {rows} rows", snapshot.version());
        assert_eq!(rows, expected.parse::<usize>().unwrap(), "{name}");
    }
}

/// `UNITY_CATALOG` and `UNITY_SCHEMA` scope the listing; `UNITY_DESCRIBE` names
/// tables to describe, comma-separated.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a Databricks workspace"]
async fn a_catalog_is_listed_and_its_tables_described() {
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("set {name}"));
    let unity = UnityConfig {
        uri: var("UNITY_URI"),
        auth: AuthConfig::Bearer {
            token: ConfigValue::literal(var("UNITY_TOKEN")),
        },
        catalog: std::env::var("UNITY_CATALOG").ok(),
        schema: std::env::var("UNITY_SCHEMA").ok(),
    };
    let listing = fluree_db_delta::browse_unity(&unity, fluree_db_delta::BrowseDepth::Tables)
        .await
        .unwrap();
    println!(
        "catalogs: {:?}\nschemas: {:?}",
        listing.catalogs, listing.schemas
    );
    for t in &listing.tables {
        println!(
            "  {} [{} {:?}] rule={:?} unreadable={:?}",
            t.full_name, t.kind, t.format, t.access_rule, t.unreadable
        );
    }
    for name in std::env::var("UNITY_DESCRIBE")
        .unwrap_or_default()
        .split(',')
    {
        if name.is_empty() {
            continue;
        }
        let table = fluree_db_delta::describe_unity_table(&unity, name)
            .await
            .unwrap();
        println!(
            "{} pk={:?} fks={:?} rule={:?}",
            table.full_name, table.primary_key, table.foreign_keys, table.access_rule
        );
        for c in &table.columns {
            println!(
                "  {} {} -> {:?} nullable={} masked={} {:?}",
                c.name, c.type_text, c.field_type, c.nullable, c.masked, c.comment
            );
        }
    }
}
