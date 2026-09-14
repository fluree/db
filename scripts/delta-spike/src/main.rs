//! Correctness probe, not the production graph-source adapter or a benchmark.
use std::{collections::BTreeMap, error::Error, fs, sync::Arc};

use delta_kernel::arrow::json::writer::{JsonArray, WriterBuilder};
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::expressions::Expression;
use delta_kernel::object_store::aws::AmazonS3Builder;
use delta_kernel::{scan::Scan, Snapshot};
use delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use delta_kernel_default_engine::storage::store_from_url_opts;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use serde::Deserialize;
use serde_json::{json, Value};
use url::Url;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
type Engine = DefaultEngine<TokioBackgroundExecutor>;

#[derive(Deserialize)]
struct Manifest {
    tables: BTreeMap<String, Table>,
}
#[derive(Deserialize)]
struct Table {
    key: String,
    versions: Vec<Version>,
    #[serde(default)]
    missing_data_versions: Vec<MissingData>,
}
#[derive(Deserialize)]
struct MissingData {
    version: u64,
    file: String,
}
#[derive(Deserialize)]
struct Version {
    version: u64,
    columns: Vec<String>,
    rows: Vec<Value>,
    filter: Option<Filter>,
}
#[derive(Deserialize)]
struct Filter {
    column: String,
    minimum: i64,
    columns: Vec<String>,
}

fn rows(scan: Scan, engine: Arc<Engine>, columns: &[String]) -> Result<Vec<Value>> {
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(Vec::new());
    // Kernel execute supplies logical rows, including partition values and
    // deletion masks. Do not substitute a raw scan of the directory's Parquet.
    for result in scan.execute(engine)? {
        let batch = result.try_into_record_batch()?;
        let names: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        if names != columns {
            return Err(format!("unexpected columns: {names:?}").into());
        }
        writer.write(&batch)?;
    }
    writer.finish()?;
    Ok(serde_json::from_slice(&writer.into_inner())?)
}

fn assert_rows(mut actual: Vec<Value>, mut expected: Vec<Value>, key: &str) -> Result<()> {
    actual.sort_by_key(|r| r[key].as_i64());
    expected.sort_by_key(|r| r[key].as_i64());
    if actual != expected {
        return Err(format!(
            "rows differ: actual={actual}, expected={expected}",
            actual = json!(actual),
            expected = json!(expected)
        )
        .into());
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: fluree-delta-spike <manifest.json> <local-root-or-s3-uri>".into());
    }
    let manifest: Manifest = serde_json::from_slice(&fs::read(&args[0])?)?;
    let base = if args[1].starts_with("s3://") {
        Url::parse(&format!("{}/", args[1].trim_end_matches('/')))?
    } else {
        Url::from_directory_path(fs::canonicalize(&args[1])?).map_err(|_| "invalid local root")?
    };
    let mut snapshots = 0;
    let mut scans = 0;
    for (name, expected) in manifest.tables {
        let url = base.join(&format!("{name}/"))?;
        let store = if url.scheme() == "s3" {
            Arc::new(
                AmazonS3Builder::from_env()
                    .with_url(url.to_string())
                    .build()?,
            ) as _
        } else {
            store_from_url_opts(&url, std::iter::empty::<(&str, &str)>())?
        };
        let engine = Arc::new(DefaultEngineBuilder::new(store).build());
        let latest = expected.versions.last().ok_or("empty version oracle")?;
        let current = Snapshot::builder_for(url.clone()).build(engine.as_ref())?;
        if current.version() != latest.version {
            return Err("latest version mismatch".into());
        }
        assert_rows(
            rows(
                current.clone().scan_builder().build()?,
                engine.clone(),
                &latest.columns,
            )?,
            latest.rows.clone(),
            &expected.key,
        )?;
        scans += 1;
        for version in expected.versions {
            let snapshot = Snapshot::builder_for(url.clone())
                .at_version(version.version)
                .build(engine.as_ref())?;
            assert_rows(
                rows(
                    snapshot.clone().scan_builder().build()?,
                    engine.clone(),
                    &version.columns,
                )?,
                version.rows.clone(),
                &expected.key,
            )?;
            scans += 1;
            let filter = version.filter.or_else(|| {
                (name == "fact_order").then(|| Filter {
                    column: "amount".to_owned(),
                    minimum: 300,
                    columns: vec!["order_id".to_owned(), "amount".to_owned()],
                })
            });
            if let Some(filter) = filter {
                let columns = filter.columns;
                let schema =
                    Arc::new(snapshot.schema().project_as_struct(
                        &columns.iter().map(String::as_str).collect::<Vec<_>>(),
                    )?);
                let scan = snapshot
                    .clone()
                    .scan_builder()
                    .with_schema(schema)
                    .with_predicate(Arc::new(
                        Expression::column([filter.column.as_str()])
                            .ge(Expression::literal(filter.minimum)),
                    ))
                    .build()?;
                let mut actual = rows(scan, engine.clone(), &columns)?;
                // Pushed predicates only prune. Apply an exact residual before
                // comparison; nullable amounts must not pass the predicate.
                let candidates = actual.len();
                actual.retain(|r| {
                    r[&filter.column]
                        .as_i64()
                        .is_some_and(|n| n >= filter.minimum)
                });
                let wanted = version
                    .rows
                    .iter()
                    .filter(|r| {
                        r[&filter.column]
                            .as_i64()
                            .is_some_and(|n| n >= filter.minimum)
                    })
                    .map(|r| {
                        Value::Object(columns.iter().map(|c| (c.clone(), r[c].clone())).collect())
                    })
                    .collect();
                assert_rows(actual, wanted, &expected.key)?;
                let empty = snapshot
                    .scan_builder()
                    .with_predicate(Arc::new(
                        Expression::column([expected.key.as_str()]).lt(Expression::literal(0_i64)),
                    ))
                    .build()?;
                let mut actual = rows(empty, engine.clone(), &version.columns)?;
                actual.retain(|r| r[&expected.key].as_i64().is_some_and(|n| n < 0));
                assert_rows(actual, vec![], &expected.key)?;
                scans += 2;
                println!(
                    "{}",
                    json!({"table":name,"version":version.version,"projected_candidates":candidates})
                );
            }
            snapshots += 1;
        }
        for missing in expected.missing_data_versions {
            // Log replay must succeed; the failure must come from reading the
            // missing historical data, not an unrelated unsupported feature.
            let snapshot = Snapshot::builder_for(url.clone())
                .at_version(missing.version)
                .build(engine.as_ref())?;
            let columns = snapshot
                .schema()
                .fields()
                .map(|f| f.name().to_owned())
                .collect::<Vec<_>>();
            let error = rows(snapshot.scan_builder().build()?, engine.clone(), &columns)
                .expect_err("missing historical data unexpectedly resolved");
            let message = error.to_string();
            if !message.contains(&missing.file) || !is_not_found(error.as_ref()) {
                return Err(format!("expected missing data error, got: {error:?}").into());
            }
            println!(
                "{}",
                json!({"table":name,"version":missing.version,"expected_failure":"missing_data"})
            );
        }
        // A future/nonexistent version must not silently resolve to current.
        if Snapshot::builder_for(url)
            .at_version(current.version() + 1)
            .build(engine.as_ref())
            .is_ok()
        {
            return Err("nonexistent version unexpectedly resolved".into());
        }
    }
    println!(
        "{}",
        json!({"reader":"delta_kernel 0.28.0","snapshots":snapshots,"scans":scans,"status":"passed"})
    );
    Ok(())
}

fn is_not_found(error: &(dyn Error + 'static)) -> bool {
    if let Some(error) = error.downcast_ref::<delta_kernel::Error>() {
        match error {
            delta_kernel::Error::FileNotFound(_) => return true,
            delta_kernel::Error::IOError(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return true
            }
            _ => {}
        }
    }
    if let Some(delta_kernel::object_store::Error::NotFound { .. }) =
        error.downcast_ref::<delta_kernel::object_store::Error>()
    {
        return true;
    }
    error.source().is_some_and(is_not_found)
}
