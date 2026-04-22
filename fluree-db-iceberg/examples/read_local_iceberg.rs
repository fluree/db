//! Example: Read local Iceberg table (no REST catalog needed).
//!
//! This demonstrates reading from a local Iceberg warehouse
//! by directly loading metadata and Parquet files.
//!
//! Run with:
//! ```
//! cargo run --example read_local_iceberg -p fluree-db-iceberg
//! ```

use fluree_db_iceberg::io::parquet::ParquetReader;
use fluree_db_iceberg::io::storage::MemoryStorage;
use fluree_db_iceberg::io::IcebergStorage;
use fluree_db_iceberg::metadata::TableMetadata;
use fluree_db_iceberg::scan::{ScanConfig, ScanPlanner};
use std::fs;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Path to local Iceberg warehouse
    let warehouse_base = std::env::var("ICEBERG_WAREHOUSE")
        .unwrap_or_else(|_| "../db/dev-resources/openflights/warehouse".to_string());

    let table_path = Path::new(&warehouse_base).join("openflights/airlines");
    let metadata_path = table_path.join("metadata/v2.metadata.json");

    println!("=== Reading Local Iceberg Table ===");
    println!("Table path: {table_path:?}");
    println!("Metadata path: {metadata_path:?}");

    // Load metadata
    let metadata_bytes = fs::read(&metadata_path)?;
    let metadata = TableMetadata::from_json(&metadata_bytes)?;

    println!("\n=== Table Metadata ===");
    println!("Format version: {}", metadata.format_version);
    println!(
        "Table UUID: {}",
        metadata.table_uuid.as_deref().unwrap_or("N/A")
    );
    println!("Location: {}", metadata.location);

    if let Some(schema) = metadata.current_schema() {
        println!("\n=== Schema (ID: {}) ===", schema.schema_id);
        for field in &schema.fields {
            println!(
                "  {} (id={}): {:?} {}",
                field.name,
                field.id,
                field.type_string().unwrap_or("complex"),
                if field.required {
                    "NOT NULL"
                } else {
                    "NULLABLE"
                }
            );
        }
    }

    if let Some(snapshot) = metadata.current_snapshot() {
        println!("\n=== Current Snapshot ===");
        println!("Snapshot ID: {}", snapshot.snapshot_id);
        println!("Timestamp: {}", snapshot.timestamp_ms);

        let manifest_list_uri = snapshot
            .manifest_list
            .as_ref()
            .ok_or("Snapshot has no manifest list (v1 format not supported)")?;
        println!("Manifest list: {manifest_list_uri}");

        // Load all files into memory storage for the scan
        let mut storage = MemoryStorage::new();

        // Load manifest list
        let manifest_list_path = manifest_list_uri
            .strip_prefix(&metadata.location)
            .unwrap_or(manifest_list_uri)
            .trim_start_matches('/');
        let manifest_list_file = table_path.join(manifest_list_path);
        println!("\nLoading manifest list: {manifest_list_file:?}");
        let manifest_list_bytes = fs::read(&manifest_list_file)?;
        storage.add_file(manifest_list_uri.clone(), manifest_list_bytes);

        // Parse manifest list to find manifest files
        let manifest_list_data = storage.read(manifest_list_uri).await?;
        let manifest_list = fluree_db_iceberg::manifest::parse_manifest_list(&manifest_list_data)?;

        println!("Found {} manifests", manifest_list.len());

        // Load each manifest and its data files
        for entry in &manifest_list {
            let manifest_path = entry
                .manifest_path
                .strip_prefix(&metadata.location)
                .unwrap_or(&entry.manifest_path)
                .trim_start_matches('/');
            let manifest_file = table_path.join(manifest_path);
            println!("  Loading manifest: {manifest_file:?}");
            let manifest_bytes = fs::read(&manifest_file)?;
            storage.add_file(entry.manifest_path.clone(), manifest_bytes);

            // Parse manifest to find data files
            let manifest_data = storage.read(&entry.manifest_path).await?;
            let manifest = fluree_db_iceberg::manifest::parse_manifest(&manifest_data)?;

            for data_entry in &manifest {
                let data_path = data_entry
                    .data_file
                    .file_path
                    .strip_prefix(&metadata.location)
                    .unwrap_or(&data_entry.data_file.file_path)
                    .trim_start_matches('/');
                let data_file = table_path.join(data_path);
                println!(
                    "    Loading data file: {:?} ({} rows)",
                    data_file, data_entry.data_file.record_count
                );
                let data_bytes = fs::read(&data_file)?;
                storage.add_file(data_entry.data_file.file_path.clone(), data_bytes);
            }
        }

        // Create scan plan
        println!("\n=== Scan Planning ===");
        let scan_config = ScanConfig::new();
        let planner = ScanPlanner::new(&storage, &metadata, scan_config);
        let plan = planner.plan_scan().await?;

        println!("Files selected: {}", plan.files_selected);
        println!("Files pruned: {}", plan.files_pruned);
        println!("Estimated rows: {}", plan.estimated_row_count);

        // Read data
        println!("\n=== Reading Data ===");
        let reader = ParquetReader::new(&storage);
        let mut total_rows = 0;

        for task in &plan.tasks {
            println!("Reading file: {}", task.data_file.file_path);
            let batches = reader.read_task(task).await?;

            for batch in &batches {
                total_rows += batch.num_rows;

                // Print first few rows of first batch
                if total_rows <= batch.num_rows && batch.num_rows > 0 {
                    println!("\nSample data (first 5 rows):");
                    let col_names: Vec<_> = batch.schema.fields.iter().map(|f| &f.name).collect();
                    println!("Columns: {col_names:?}");

                    for row_idx in 0..batch.num_rows.min(5) {
                        let mut row_values = Vec::new();
                        for col in &batch.columns {
                            let val = match col {
                                fluree_db_iceberg::io::batch::Column::Int32(v) => {
                                    v.get(row_idx).map(|x| format!("{x:?}")).unwrap_or_default()
                                }
                                fluree_db_iceberg::io::batch::Column::Int64(v) => {
                                    v.get(row_idx).map(|x| format!("{x:?}")).unwrap_or_default()
                                }
                                fluree_db_iceberg::io::batch::Column::String(v) => {
                                    v.get(row_idx).map(|x| format!("{x:?}")).unwrap_or_default()
                                }
                                fluree_db_iceberg::io::batch::Column::Boolean(v) => {
                                    v.get(row_idx).map(|x| format!("{x:?}")).unwrap_or_default()
                                }
                                _ => "...".to_string(),
                            };
                            row_values.push(val);
                        }
                        println!("  Row {row_idx}: {row_values:?}");
                    }
                }
            }
        }

        println!("\n=== Summary ===");
        println!("Total rows read: {total_rows}");
    } else {
        println!("No current snapshot found");
    }

    Ok(())
}
