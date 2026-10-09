//! AWS Glue and S3 Tables catalog modes, end to end, against moto: a local mock
//! of S3, the Glue Data Catalog and S3 Tables. No AWS account and no live
//! dependency.
//!
//! The tables are a committed fixture (`tests/fixtures/iceberg/glue/`) written by
//! pyiceberg's `GlueCatalog` the way Glue-integrated writers write them: the
//! current metadata file is recorded only in the Glue table's `metadata_location`
//! parameter, and there is no `metadata/version-hint.text`. This test replays the
//! fixture's S3 objects and Glue registrations into moto, then reads the tables
//! through the public API exactly as a deployment would: the Glue client finds
//! moto through the standard AWS endpoint override (`AWS_ENDPOINT_URL_GLUE`), and
//! the S3 reads through the source's own `s3_endpoint` (no S3 override is set, so
//! a mode that ignored `s3_endpoint` would fail here).
//!
//! What it pins:
//! - a table resolves through Glue `GetTable` (50 rows);
//! - Glue's pointer wins over newer-looking files: with three metadata files the
//!   current one is read (200), and an uncommitted higher-numbered `00099-…`
//!   metadata file next to the committed one is never read (100, not 0);
//! - one source joins two Glue tables named by its mapping (200);
//! - a table missing from Glue and a non-Iceberg (Hive) Glue table fail clearly;
//! - materialize scans (full, incremental from a snapshot, pinned to one) and a
//!   time-pinned query read the snapshots Glue's metadata names;
//! - browse lists the Glue database and only its Iceberg tables; preview reads
//!   the schema from the metadata file (Glue returns no inline metadata);
//! - S3 Tables: a table resolved through `GetTableMetadataLocation` (its
//!   metadata file copied into the table's own moto-managed warehouse bucket), a
//!   missing table classified as not-found on the wire, and browse.
//!
//! It runs when `FLUREE_GLUE_LOCAL_ENDPOINT` names a moto server (CI: the `test`
//! job's `moto` service; locally: `scripts/glue-local/up.sh`, or any
//! `moto_server`), and skips otherwise; `glue_moto_is_configured_in_ci` keeps a
//! skip from reading as a pass in CI. Regenerate the fixture with
//! `scripts/glue-local/write_fixture.sh`.
//!
//! ```text
//! FLUREE_GLUE_LOCAL_ENDPOINT=http://127.0.0.1:5000 \
//!   cargo test -p fluree-db-api --features glue-moto --test it_iceberg_glue_moto
//! ```
#![cfg(feature = "glue-moto")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use fluree_db_api::{
    browse_iceberg_catalog, preview_iceberg_table, BrowseDepth, FlureeBuilder, FlureeR2rmlProvider,
    IcebergConnectionConfig, IcebergCreateConfig, R2rmlCreateConfig, R2rmlMappingInput, StatsTier,
    TableIdentifier,
};
use futures::TryStreamExt;

const REGION: &str = "us-east-1";

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/iceberg/glue")
}

fn endpoint() -> Option<String> {
    std::env::var("FLUREE_GLUE_LOCAL_ENDPOINT")
        .ok()
        .filter(|v| !v.trim().is_empty())
}

/// The fixture's Glue registrations (`glue-tables.json`).
struct Fixture {
    bucket: String,
    database: String,
    /// `(name, table_type, location, parameters)` of every Glue table to register.
    tables: Vec<(String, String, String, HashMap<String, String>)>,
}

fn load_fixture() -> Fixture {
    let raw = std::fs::read_to_string(fixture_dir().join("glue-tables.json"))
        .expect("read glue-tables.json");
    let json: serde_json::Value = serde_json::from_str(&raw).expect("parse glue-tables.json");
    let text = |v: &serde_json::Value, k: &str| v[k].as_str().expect(k).to_string();
    let tables = json["tables"]
        .as_array()
        .into_iter()
        .chain(json["non_iceberg_tables"].as_array())
        .flatten()
        .map(|t| {
            let parameters = t["parameters"]
                .as_object()
                .expect("parameters")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().expect("string parameter").to_string()))
                .collect();
            (
                text(t, "name"),
                text(t, "table_type"),
                text(t, "location"),
                parameters,
            )
        })
        .collect();
    Fixture {
        bucket: text(&json, "bucket"),
        database: text(&json, "database"),
        tables,
    }
}

/// The parent of a fixture table's current snapshot (the state before its last
/// append), read from the fixture's own copy of the metadata Glue points at.
fn parent_snapshot(fixture: &Fixture, table: &str) -> i64 {
    let (_, _, _, parameters) = fixture
        .tables
        .iter()
        .find(|(name, ..)| name == table)
        .expect("fixture table");
    let location = &parameters["metadata_location"];
    let key = location
        .strip_prefix(&format!("s3://{}/", fixture.bucket))
        .expect("metadata in the fixture bucket");
    let json = std::fs::read_to_string(fixture_dir().join("objects").join(key))
        .expect("read fixture metadata");
    let metadata =
        fluree_db_iceberg::metadata::TableMetadata::from_json_str(&json).expect("parse metadata");
    let current = metadata.current_snapshot().expect("current snapshot");
    current.parent_snapshot_id.expect("two appends")
}

fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read fixture dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            files_under(&path, out);
        } else {
            out.push(path);
        }
    }
}

/// Replay the fixture into moto: the bucket and its objects, then the Glue
/// database and tables. Idempotent, so a moto that already holds them is fine.
async fn seed(endpoint: &str, fixture: &Fixture) {
    use aws_sdk_glue::error::ProvideErrorMetadata;

    let credentials = aws_sdk_s3::config::Credentials::new("test", "test", None, None, "moto");
    let s3 = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(REGION))
            .credentials_provider(credentials.clone())
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build(),
    );
    let glue = aws_sdk_glue::Client::from_conf(
        aws_sdk_glue::Config::builder()
            .behavior_version(aws_sdk_glue::config::BehaviorVersion::latest())
            .region(aws_sdk_glue::config::Region::new(REGION))
            .credentials_provider(credentials)
            .endpoint_url(endpoint)
            .build(),
    );

    // moto may still be starting (a CI service container): wait for it.
    let mut ready = false;
    for _ in 0..60 {
        if s3.list_buckets().send().await.is_ok() {
            ready = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(ready, "moto did not answer at {endpoint}");

    if let Err(e) = s3.create_bucket().bucket(&fixture.bucket).send().await {
        let code = e.code();
        assert!(
            matches!(
                code,
                Some("BucketAlreadyOwnedByYou" | "BucketAlreadyExists")
            ),
            "create bucket: {e:?}"
        );
    }
    let objects = fixture_dir().join("objects");
    let mut files = Vec::new();
    files_under(&objects, &mut files);
    for file in files {
        let key = file
            .strip_prefix(&objects)
            .expect("under objects/")
            .to_string_lossy()
            .replace('\\', "/");
        s3.put_object()
            .bucket(&fixture.bucket)
            .key(&key)
            .body(std::fs::read(&file).expect("read fixture object").into())
            .send()
            .await
            .unwrap_or_else(|e| panic!("put {key}: {e:?}"));
    }

    let database = aws_sdk_glue::types::DatabaseInput::builder()
        .name(&fixture.database)
        .build()
        .expect("database input");
    if let Err(e) = glue.create_database().database_input(database).send().await {
        assert_eq!(
            e.code(),
            Some("AlreadyExistsException"),
            "create database: {e:?}"
        );
    }
    for (name, table_type, location, parameters) in &fixture.tables {
        let table = aws_sdk_glue::types::TableInput::builder()
            .name(name)
            .table_type(table_type)
            .set_parameters(Some(parameters.clone()))
            .storage_descriptor(
                aws_sdk_glue::types::StorageDescriptor::builder()
                    .location(location)
                    .build(),
            )
            .build()
            .expect("table input");
        if let Err(e) = glue
            .create_table()
            .database_name(&fixture.database)
            .table_input(table)
            .send()
            .await
        {
            assert_eq!(
                e.code(),
                Some("AlreadyExistsException"),
                "create {name}: {e:?}"
            );
        }
    }
}

/// An S3 Tables table bucket in moto holding `customers` from the fixture, and
/// its ARN. S3 Tables keeps a table's files in a managed warehouse bucket of its
/// own and only accepts a metadata location inside it, so the fixture's current
/// metadata file is copied there (its manifests are absolute paths into the
/// already-seeded fixture bucket). Idempotent, like `seed`.
async fn seed_s3_tables(endpoint: &str, fixture: &Fixture) -> String {
    use aws_sdk_s3tables::error::ProvideErrorMetadata;

    let credentials = aws_sdk_s3::config::Credentials::new("test", "test", None, None, "moto");
    let tables = aws_sdk_s3tables::Client::from_conf(
        aws_sdk_s3tables::Config::builder()
            .behavior_version(aws_sdk_s3tables::config::BehaviorVersion::latest())
            .region(aws_sdk_s3tables::config::Region::new(REGION))
            .credentials_provider(credentials.clone())
            .endpoint_url(endpoint)
            .build(),
    );
    let s3 = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(REGION))
            .credentials_provider(credentials)
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build(),
    );

    let bucket_name = "fluree-s3tables-it";
    let arn = match tables.create_table_bucket().name(bucket_name).send().await {
        Ok(out) => out.arn().to_string(),
        Err(e) => {
            assert_eq!(
                e.code(),
                Some("ConflictException"),
                "create table bucket: {e:?}"
            );
            let listed = tables
                .list_table_buckets()
                .prefix(bucket_name)
                .send()
                .await
                .expect("list table buckets");
            listed.table_buckets()[0].arn().to_string()
        }
    };
    let namespace = fixture.database.as_str();
    if let Err(e) = tables
        .create_namespace()
        .table_bucket_arn(&arn)
        .namespace(namespace)
        .send()
        .await
    {
        assert_eq!(
            e.code(),
            Some("ConflictException"),
            "create namespace: {e:?}"
        );
    }
    if let Err(e) = tables
        .create_table()
        .table_bucket_arn(&arn)
        .namespace(namespace)
        .name("customers")
        .format(aws_sdk_s3tables::types::OpenTableFormat::Iceberg)
        .send()
        .await
    {
        assert_eq!(e.code(), Some("ConflictException"), "create table: {e:?}");
    }
    // The same call the engine resolves a table with; it also returns the version
    // token and warehouse location an update needs.
    let table = tables
        .get_table_metadata_location()
        .table_bucket_arn(&arn)
        .namespace(namespace)
        .name("customers")
        .send()
        .await
        .expect("get table metadata location");

    // The fixture's current customers metadata, copied into the table's warehouse.
    let (_, _, _, parameters) = fixture
        .tables
        .iter()
        .find(|(name, ..)| name == "customers")
        .expect("fixture customers");
    let source = &parameters["metadata_location"];
    let key = source
        .strip_prefix(&format!("s3://{}/", fixture.bucket))
        .expect("metadata in the fixture bucket");
    let file = key.rsplit('/').next().expect("metadata file name");
    let warehouse = table.warehouse_location().trim_end_matches('/');
    let warehouse_bucket = warehouse.strip_prefix("s3://").expect("s3 warehouse");
    s3.put_object()
        .bucket(warehouse_bucket)
        .key(format!("metadata/{file}"))
        .body(
            std::fs::read(fixture_dir().join("objects").join(key))
                .expect("read fixture metadata")
                .into(),
        )
        .send()
        .await
        .expect("put metadata into the warehouse");
    let updated = tables
        .update_table_metadata_location()
        .table_bucket_arn(&arn)
        .namespace(namespace)
        .name("customers")
        .version_token(table.version_token())
        .metadata_location(format!("{warehouse}/metadata/{file}"))
        .send()
        .await;
    match updated {
        Ok(_) => {}
        // moto 5.2.3 applies the update but answers with `namespace` as a string
        // where the AWS model has a list, so the SDK cannot parse the 200. The
        // engine's own GetTableMetadataLocation read below proves it took.
        Err(e) if e.raw_response().is_some_and(|r| r.status().is_success()) => {}
        Err(e) => panic!("point the table at its metadata: {e:?}"),
    }
    arn
}

/// The process's AWS SDK environment for the Fluree side: moto's dummy
/// credentials, and Glue (only Glue) routed to moto. Set once, before anything
/// builds a client; this binary's other test only reads unrelated variables.
fn point_aws_sdk_at(endpoint: &str) {
    let missing = std::env::temp_dir().join("fluree-glue-moto-no-aws-config");
    std::env::set_var("AWS_ENDPOINT_URL_GLUE", endpoint);
    std::env::set_var("AWS_ENDPOINT_URL_S3TABLES", endpoint);
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", REGION);
    // Keep a developer's real profile out of the run.
    std::env::set_var("AWS_CONFIG_FILE", &missing);
    std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &missing);
    for var in [
        "AWS_PROFILE",
        "AWS_SESSION_TOKEN",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
    ] {
        std::env::remove_var(var);
    }
}

/// The glue-mode connection every source here uses: the catalog through Glue,
/// the data through the source's own S3 endpoint.
fn glue_connection(endpoint: &str) -> IcebergConnectionConfig {
    IcebergConnectionConfig::glue(Some(REGION.to_string()), None)
        .with_s3_endpoint(endpoint)
        .with_s3_path_style(true)
        .with_s3_region(REGION)
}

/// The S3 Tables connection: the catalog through S3 Tables (its region from the
/// ARN), the data through the source's own S3 endpoint.
fn s3_tables_connection(endpoint: &str, table_bucket_arn: &str) -> IcebergConnectionConfig {
    IcebergConnectionConfig::s3_tables(None, table_bucket_arn)
        .with_s3_endpoint(endpoint)
        .with_s3_path_style(true)
        .with_s3_region(REGION)
}

fn mapping(database: &str, body: &str) -> String {
    format!(
        "@prefix rr: <http://www.w3.org/ns/r2rml#> .\n\
         @prefix ex: <http://example.org/> .\n\
         {}",
        body.replace("{db}", database)
    )
}

const CUSTOMERS: &str = r#"
<http://example.org/mapping#Customers> a rr:TriplesMap ;
  rr:logicalTable [ rr:tableName "{db}.customers" ] ;
  rr:subjectMap [ rr:template "http://example.org/customer/{customer_id}" ; rr:class ex:Customer ] ;
  rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
"#;

fn orders_map(table: &str) -> String {
    format!(
        r#"
<http://example.org/mapping#Orders> a rr:TriplesMap ;
  rr:logicalTable [ rr:tableName "{{db}}.{table}" ] ;
  rr:subjectMap [ rr:template "http://example.org/order/{{order_id}}" ; rr:class ex:Order ] ;
  rr:predicateObjectMap [ rr:predicate ex:customer ;
    rr:objectMap [ rr:template "http://example.org/customer/{{customer_id}}" ; rr:termType rr:IRI ] ] .
"#
    )
}

const OTHER: &str = r#"
<http://example.org/mapping#Rows> a rr:TriplesMap ;
  rr:logicalTable [ rr:tableName "{db}.{table}" ] ;
  rr:subjectMap [ rr:template "http://example.org/row/{id}" ; rr:class ex:Row ] .
"#;

/// Register a glue-mode source over `mapping` and count the subjects `where`
/// matches (or the error that says why it could not).
async fn rows(
    fluree: &fluree_db_api::Fluree,
    connection: IcebergConnectionConfig,
    name: &str,
    mapping: String,
    select: &str,
    pattern: serde_json::Value,
) -> Result<usize, String> {
    // Mapping-driven: each rr:tableName names a table in the catalog.
    let config = R2rmlCreateConfig {
        iceberg: IcebergCreateConfig::from_connection(
            name,
            connection,
            fluree_db_iceberg::config::MAPPING_DEFINED_TABLE,
        ),
        mapping: R2rmlMappingInput::Content(mapping),
        mapping_media_type: Some("text/turtle".to_string()),
    };
    fluree
        .create_r2rml_graph_source(config)
        .await
        .map_err(|e| format!("create: {e}"))?;
    let query = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": format!("{name}:main"),
        "select": [select],
        "where": pattern,
    });
    let result = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .map_err(|e| e.to_string())?;
    Ok(result.as_array().map_or(0, Vec::len))
}

#[tokio::test]
async fn glue_catalog_mode_reads_tables_resolved_through_glue() {
    let Some(endpoint) = endpoint() else {
        eprintln!("SKIPPED: set FLUREE_GLUE_LOCAL_ENDPOINT to a moto server");
        return;
    };
    let fixture = load_fixture();
    seed(&endpoint, &fixture).await;
    let table_bucket_arn = seed_s3_tables(&endpoint, &fixture).await;
    point_aws_sdk_at(&endpoint);
    // Fluree's Iceberg disk caches (catalog pointers, metadata, Parquet) live under
    // the temp dir and outlive the process. Without a fresh one, a re-run (or a
    // nextest retry) answers from the previous run's cache and never asks Glue,
    // which is how a stale-pointer bug passed the count checks once.
    let caches = tempfile::tempdir().expect("cache dir");
    std::env::set_var("TMPDIR", caches.path());
    let db = fixture.database.as_str();
    let fluree = FlureeBuilder::memory().build_memory();
    let ty = |class: &str| serde_json::json!({"@id": "?s", "@type": class});

    let mut failures = Vec::new();
    let mut expect = |check: &str, got: Result<usize, String>, want: Result<usize, &str>| {
        let ok = match (&got, want) {
            (Ok(n), Ok(w)) => *n == w,
            (Err(e), Err(needle)) => e.contains(needle),
            _ => false,
        };
        eprintln!("{} {check}: {got:?}", if ok { "pass" } else { "FAIL" });
        if !ok {
            failures.push(format!("{check}: expected {want:?}, got {got:?}"));
        }
    };

    expect(
        "one table resolved through Glue GetTable",
        rows(
            &fluree,
            glue_connection(&endpoint),
            "customers",
            mapping(db, CUSTOMERS),
            "?s",
            ty("ex:Customer"),
        )
        .await,
        Ok(50),
    );
    expect(
        "three metadata files: Glue's current one is read",
        rows(
            &fluree,
            glue_connection(&endpoint),
            "orders",
            mapping(db, &orders_map("orders")),
            "?s",
            ty("ex:Order"),
        )
        .await,
        Ok(200),
    );
    expect(
        "the Glue pointer wins over an uncommitted 00099 metadata file",
        rows(
            &fluree,
            glue_connection(&endpoint),
            "orphan",
            mapping(db, &orders_map("orders_orphan")),
            "?s",
            ty("ex:Order"),
        )
        .await,
        Ok(100),
    );
    expect(
        "one source joins two Glue tables named by its mapping",
        rows(
            &fluree,
            glue_connection(&endpoint),
            "joined",
            mapping(db, &format!("{CUSTOMERS}{}", orders_map("orders"))),
            "?o",
            serde_json::json!([
                {"@id": "?o", "@type": "ex:Order", "ex:customer": "?c"},
                {"@id": "?c", "@type": "ex:Customer"}
            ]),
        )
        .await,
        Ok(200),
    );
    // Materialize / incremental scans and a time-pinned query over Glue: `orders`
    // is two appends of 100 rows, so a dropped pin or window reads all 200.
    let first = parent_snapshot(&fixture, "orders");
    let provider = FlureeR2rmlProvider::new(&fluree);
    let orders_table = format!("{db}.orders");
    let scanned = |from: Option<i64>, to: Option<i64>| {
        let provider = &provider;
        let orders_table = &orders_table;
        async move {
            let scan = provider
                .scan_for_materialize_stream("orders:main", orders_table, &[], from, to, None)
                .await
                .map_err(|e| e.to_string())?;
            let batches: Vec<_> = scan.stream.try_collect().await.map_err(|e| e.to_string())?;
            Ok::<usize, String>(batches.iter().map(|b| b.num_rows).sum())
        }
    };
    expect(
        "materialize: a full scan reads the current snapshot",
        scanned(None, None).await,
        Ok(200),
    );
    expect(
        "materialize: an incremental scan reads only the last append",
        scanned(Some(first), None).await,
        Ok(100),
    );
    expect(
        "materialize: a scan pinned to the first snapshot reads its rows",
        scanned(None, Some(first)).await,
        Ok(100),
    );
    let pinned = serde_json::json!({
        "@context": {"ex": "http://example.org/"},
        "from": format!("orders:main@snapshot:{first}"),
        "select": ["?s"],
        "where": {"@id": "?s", "@type": "ex:Order"},
    });
    expect(
        "a query pinned to the first snapshot",
        fluree
            .query_from()
            .jsonld(&pinned)
            .execute_formatted()
            .await
            .map(|rows| rows.as_array().map_or(0, Vec::len))
            .map_err(|e| e.to_string()),
        Ok(100),
    );
    expect(
        "a table missing from Glue fails clearly",
        rows(
            &fluree,
            glue_connection(&endpoint),
            "missing",
            mapping(db, &OTHER.replace("{table}", "no_such_table")),
            "?s",
            ty("ex:Row"),
        )
        .await,
        // Only the Glue client words it this way: the error came from GetTable.
        Err("Glue table glue_it.no_such_table not found"),
    );
    expect(
        "a non-Iceberg Glue table fails clearly",
        rows(
            &fluree,
            glue_connection(&endpoint),
            "hive",
            mapping(db, &OTHER.replace("{table}", "hive_table")),
            "?s",
            ty("ex:Row"),
        )
        .await,
        Err("is not an Iceberg table"),
    );

    // S3 Tables: the same reads through `GetTableMetadataLocation`.
    let s3t = || s3_tables_connection(&endpoint, &table_bucket_arn);
    expect(
        "s3tables: one table resolved via GetTableMetadataLocation",
        rows(
            &fluree,
            s3t(),
            "s3t-customers",
            mapping(db, CUSTOMERS),
            "?s",
            ty("ex:Customer"),
        )
        .await,
        Ok(50),
    );
    expect(
        "s3tables: a table missing from the bucket fails clearly",
        rows(
            &fluree,
            s3t(),
            "s3t-missing",
            mapping(db, &OTHER.replace("{table}", "no_such_table")),
            "?s",
            ty("ex:Row"),
        )
        .await,
        // Only the S3 Tables client words it this way.
        Err("S3 Tables table glue_it.no_such_table not found"),
    );

    // `/info` for a Glue source: the catalog object and the per-table row count
    // (read from the metadata file Glue points at; Glue returns none inline).
    match fluree.ledger_info("customers:main").execute().await {
        Ok(info) => {
            let source = &info["source"];
            let count = source["table-row-counts"][format!("{db}.customers")].as_i64();
            let kind = source["catalog"]["type"].as_str();
            // The mapping's table only: never the mapping-defined placeholder.
            let tables = source["tables"].clone();
            let ok = count == Some(50)
                && kind == Some("glue")
                && tables == serde_json::json!([format!("{db}.customers")]);
            eprintln!(
                "{} info: catalog={kind:?} customers={count:?}",
                if ok { "pass" } else { "FAIL" }
            );
            if !ok {
                failures.push(format!("info: {source}"));
            }
        }
        Err(e) => failures.push(format!("info: {e}")),
    }

    // Onboarding: browse lists the database and only its Iceberg tables.
    match browse_iceberg_catalog(glue_connection(&endpoint), BrowseDepth::Tables).await {
        Ok(browse) => {
            let tables: Vec<String> = browse
                .tables
                .iter()
                .filter(|t| t.namespace == db)
                .map(|t| t.name.clone())
                .collect();
            let ok = browse.namespaces.iter().any(|n| n == db)
                && ["customers", "orders", "orders_orphan"]
                    .iter()
                    .all(|t| tables.iter().any(|n| n == t))
                && !tables.iter().any(|n| n == "hive_table");
            eprintln!("{} browse: {tables:?}", if ok { "pass" } else { "FAIL" });
            if !ok {
                failures.push(format!("browse: {:?} / {tables:?}", browse.namespaces));
            }
        }
        Err(e) => failures.push(format!("browse: {e}")),
    }

    // S3 Tables browse: the namespace and its table.
    match browse_iceberg_catalog(s3t(), BrowseDepth::Tables).await {
        Ok(browse) => {
            let ok = browse.namespaces.iter().any(|n| n == db)
                && browse
                    .tables
                    .iter()
                    .any(|t| t.namespace == db && t.name == "customers");
            eprintln!(
                "{} s3tables browse: {:?}",
                if ok { "pass" } else { "FAIL" },
                browse.tables
            );
            if !ok {
                failures.push(format!("s3tables browse: {:?}", browse.tables));
            }
        }
        Err(e) => failures.push(format!("s3tables browse: {e}")),
    }

    // Preview reads the schema from the metadata file Glue points at.
    let table = TableIdentifier {
        namespace: db.to_string(),
        name: "customers".to_string(),
    };
    match preview_iceberg_table(glue_connection(&endpoint), table, StatsTier::Stats).await {
        Ok(preview) => {
            let columns: Vec<&str> = preview
                .schema
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect();
            let ok = columns == ["customer_id", "name", "country", "birth_date"]
                && preview.schema.row_count == Some(50);
            eprintln!(
                "{} preview: {columns:?} rows={:?}",
                if ok { "pass" } else { "FAIL" },
                preview.schema.row_count
            );
            if !ok {
                failures.push(format!(
                    "preview: {columns:?} rows={:?}",
                    preview.schema.row_count
                ));
            }
        }
        Err(e) => failures.push(format!("preview: {e}")),
    }

    assert!(
        failures.is_empty(),
        "glue-mode checks failed:\n{}",
        failures.join("\n")
    );
}

/// A skipped run is not a passing one: the CI job that runs this binary must
/// supply moto. Gated on the marker the job sets beside the endpoint (as
/// `live_bridge_backends_are_configured_in_ci` is), not on `CI`, so a job that
/// never meant to run it does not fail.
#[test]
fn glue_moto_is_configured_in_ci() {
    if std::env::var("FLUREE_GLUE_MOTO").is_err() {
        eprintln!("SKIPPED: not a job that provides moto");
        return;
    }
    assert!(
        endpoint().is_some(),
        "FLUREE_GLUE_LOCAL_ENDPOINT must be set where FLUREE_GLUE_MOTO is, or the Glue \
         checks pass by doing nothing"
    );
}
