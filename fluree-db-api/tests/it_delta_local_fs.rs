//! Delta graph sources end to end, against the committed fixtures under
//! `fluree-db-delta/tests/fixtures` (see that crate's `tests/reader.rs` for
//! their provenance). Expected values are restated from the generators' inputs.
//!
//! Run with:
//!   cargo test -p fluree-db-api --features delta --test it_delta_local_fs

#![cfg(all(feature = "delta", feature = "native"))]

use fluree_db_api::{
    ApiError, DeltaCreateConfig, Fluree, FlureeBuilder, FlureeR2rmlProvider, TimeSpec,
};
use fluree_db_query::error::QueryError;
use fluree_db_query::r2rml::{ScanCmpOp, ScanFilter, ScanValue};
use futures::TryStreamExt;
use serde_json::{json, Value};

/// Commit times Delta Spark recorded in the `in_commit_time` fixture's log:
/// v0 = {1}, v1 = {1, 2}, v2 = {2}.
const ICT: [i64; 3] = [1_789_829_202_524, 1_789_829_202_991, 1_789_829_203_546];

const MAPPING: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    <http://example.org/mapping#Item>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "in_commit_time" ] ;
        rr:subjectMap [ rr:template "http://example.org/item/{id}" ; rr:class ex:Item ] ;
        rr:predicateObjectMap [ rr:predicate ex:amount ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ] ] .

    <http://example.org/mapping#Reading>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "deletion_vectors" ] ;
        rr:subjectMap [ rr:template "http://example.org/reading/{id}" ; rr:class ex:Reading ] ;
        rr:predicateObjectMap [ rr:predicate ex:value ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ] ] .

    <http://example.org/mapping#Store>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "dim_store" ] ;
        rr:subjectMap [ rr:template "http://example.org/store/{store_id}" ; rr:class ex:Store ] ;
        rr:predicateObjectMap [ rr:predicate ex:storeName ; rr:objectMap [ rr:column "name" ] ] .

    <http://example.org/mapping#Order>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "fact_order" ] ;
        rr:subjectMap [ rr:template "http://example.org/order/{order_id}" ; rr:class ex:Order ] ;
        rr:predicateObjectMap [ rr:predicate ex:total ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ] ] ;
        rr:predicateObjectMap [
            rr:predicate ex:store ;
            rr:objectMap [
                rr:parentTriplesMap <http://example.org/mapping#Store> ;
                rr:joinCondition [ rr:child "store_id" ; rr:parent "store_id" ]
            ]
        ] .

    <http://example.org/mapping#Mapped>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "column_mapping" ] ;
        rr:subjectMap [ rr:template "http://example.org/mapped/{id}" ; rr:class ex:Mapped ] ;
        rr:predicateObjectMap [ rr:predicate ex:mappedAmount ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ] ] .
"#;

fn fixtures() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../fluree-db-delta/tests/fixtures")
}

/// Every test must install the same allowlist: the guard reads it once. The
/// temp directory holds staged copies of fixtures whose history a test plays
/// out commit by commit.
fn allow_fixture_roots() {
    let root = fixtures().canonicalize().expect("fixtures dir");
    let temp = std::env::temp_dir().canonicalize().expect("temp dir");
    std::env::set_var(
        "FLUREE_ICEBERG_LOCAL_ROOTS",
        format!("{}:{}", root.display(), temp.display()),
    );
}

async fn source(name: &str) -> (Fluree, String) {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = fixtures().canonicalize().expect("fixtures dir");
    let mut config = DeltaCreateConfig::new(name, root.to_str().unwrap(), MAPPING);
    config.mapping_media_type = Some("text/turtle".to_string());
    let created = fluree
        .create_delta_graph_source(config)
        .await
        .expect("create delta graph source");
    assert!(created.mapping_validated);
    assert_eq!(created.triples_map_count, 5);
    assert_eq!(
        created.table_warnings,
        Vec::<String>::new(),
        "every mapped fixture table opens at registration"
    );
    assert_eq!(created.table_versions["in_commit_time"], 2);
    assert_eq!(created.table_versions["fact_order"], 4);
    (fluree, format!("{name}:main"))
}

fn iso(ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
        .expect("valid ms")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn sorted_ints(rows: &Value) -> Vec<i64> {
    let mut out: Vec<i64> = rows
        .as_array()
        .expect("array result")
        .iter()
        .map(|r| {
            r.as_i64()
                .or_else(|| r[0].as_i64())
                .unwrap_or_else(|| panic!("integer row, got {r} in {rows}"))
        })
        .collect();
    out.sort_unstable();
    out
}

#[tokio::test]
async fn delta_tables_query_through_r2rml() {
    let (fluree, alias) = source("delta-basic").await;

    // Deletion vectors: 4096 rows written, 9 deleted in place.
    let count = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["(count ?s)"],
        "where": {"@id": "?s", "@type": "ex:Reading"},
    });
    let n = fluree
        .graph(&alias)
        .query()
        .jsonld(&count)
        .execute_formatted()
        .await
        .expect("count readings");
    assert_eq!(sorted_ints(&n), [4087], "got: {n}");

    // A deleted row is gone; its neighbour is not.
    for (id, expected) in [(1024, vec![]), (1025, vec![10250])] {
        let one = json!({
            "@context": {"ex": "http://example.org/"},
            "select": ["?v"],
            "where": {"@id": format!("ex:reading/{id}"), "ex:value": "?v"},
        });
        let rows = fluree
            .graph(&alias)
            .query()
            .jsonld(&one)
            .execute_formatted()
            .await
            .expect("reading lookup");
        assert_eq!(sorted_ints(&rows), expected, "reading {id}: {rows}");
    }

    // A join across two Delta tables, SPARQL twin included. Orders 2..7 remain
    // at the latest version; order 3 has no amount.
    let join = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?total"],
        "where": {"@id": "?o", "ex:total": "?total", "ex:store": {"ex:storeName": "East shop"}},
    });
    let rows = fluree
        .graph(&alias)
        .query()
        .jsonld(&join)
        .execute_formatted()
        .await
        .expect("join");
    assert_eq!(sorted_ints(&rows), [600, 700], "got: {rows}");
    let sparql = fluree
        .query_from()
        .sparql(&format!(
            "PREFIX ex: <http://example.org/> SELECT ?total FROM <{alias}> WHERE {{ \
             ?o ex:total ?total ; ex:store ?s . ?s ex:storeName \"East shop\" }}"
        ))
        .execute_formatted()
        .await
        .expect("sparql join");
    let mut totals: Vec<String> = sparql["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| b["total"]["value"].as_str().expect("value").to_string())
        .collect();
    totals.sort();
    assert_eq!(totals, ["600", "700"], "got: {sparql}");

    // No `t`: a Delta source has no Fluree transaction to page against.
    let raw = fluree
        .graph(&alias)
        .query()
        .jsonld(&join)
        .execute()
        .await
        .expect("raw");
    assert_eq!(raw.t, None);
}

#[tokio::test]
async fn pinned_query_on_a_delta_source_reads_that_version() {
    let (fluree, alias) = source("delta-pinned").await;

    let amounts = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?amount"],
        "where": {"@id": "?s", "@type": "ex:Item", "ex:amount": "?amount"},
    });
    let count = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["(count ?s)"],
        "where": {"@id": "?s", "@type": "ex:Item"},
    });
    let sparql_amounts = |from: &str| {
        format!(
            "PREFIX ex: <http://example.org/> SELECT ?amount FROM <{from}> \
             WHERE {{ ?s a ex:Item ; ex:amount ?amount }}"
        )
    };
    let sparql_values = |v: &Value| -> Vec<String> {
        let mut out: Vec<String> = v["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .iter()
            .map(|b| b["amount"]["value"].as_str().expect("value").to_string())
            .collect();
        out.sort();
        out
    };

    let latest = fluree
        .graph(&alias)
        .query()
        .jsonld(&amounts)
        .execute_formatted()
        .await
        .expect("latest");
    assert_eq!(sorted_ints(&latest), [30000], "got: {latest}");

    // (suffix, spec, amounts at that version)
    let honored: Vec<(String, TimeSpec, Vec<i64>)> = vec![
        ("snapshot:0".into(), TimeSpec::AtSnapshot(0), vec![100]),
        (
            "snapshot:1".into(),
            TimeSpec::AtSnapshot(1),
            vec![100, 30000],
        ),
        ("snapshot:2".into(), TimeSpec::AtSnapshot(2), vec![30000]),
        (
            format!("time:{}", iso(ICT[0])),
            TimeSpec::AtTime(iso(ICT[0])),
            vec![100],
        ),
        (
            format!("time:{}", iso(ICT[2] - 1)),
            TimeSpec::AtTime(iso(ICT[2] - 1)),
            vec![100, 30000],
        ),
        (
            format!("iso:{}", iso(ICT[1])),
            TimeSpec::AtTime(iso(ICT[1])),
            vec![100, 30000],
        ),
        (
            format!("recorded:{}", iso(ICT[1])),
            TimeSpec::AtRecorded(iso(ICT[1])),
            vec![100, 30000],
        ),
        // After the latest commit: the latest state.
        (
            format!("time:{}", iso(ICT[2] + 86_400_000)),
            TimeSpec::AtTime(iso(ICT[2] + 86_400_000)),
            vec![30000],
        ),
    ];
    for (suffix, spec, expected) in &honored {
        let from = format!("{alias}@{suffix}");
        let mut q = amounts.clone();
        q["from"] = Value::String(from.clone());
        let rows = fluree
            .query_from()
            .jsonld(&q)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("from @{suffix}: {e}"));
        assert_eq!(&sorted_ints(&rows), expected, "from @{suffix}: {rows}");

        let mut q = count.clone();
        q["from"] = Value::String(from.clone());
        let n = fluree
            .query_from()
            .jsonld(&q)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("count from @{suffix}: {e}"));
        assert_eq!(
            sorted_ints(&n),
            [expected.len() as i64],
            "count from @{suffix}: {n}"
        );

        let rows = fluree
            .graph_at(&alias, spec.clone())
            .query()
            .jsonld(&amounts)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("graph_at({spec:?}): {e}"));
        assert_eq!(&sorted_ints(&rows), expected, "graph_at({spec:?}): {rows}");

        let rows = fluree
            .query_from()
            .sparql(&sparql_amounts(&from))
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("SPARQL FROM <{from}>: {e}"));
        let want: Vec<String> = expected.iter().map(ToString::to_string).collect();
        assert_eq!(sparql_values(&rows), want, "SPARQL FROM <{from}>: {rows}");
    }

    let expect_refused = |result: Result<Value, ApiError>, route: &str, needle: &str| {
        let err = match result {
            Ok(rows) => panic!("{route}: pinned query returned {rows} instead of an error"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains(needle), "{route}: unexpected error: {err}");
    };
    let refused: Vec<(String, TimeSpec, &str)> = vec![
        (
            format!("time:{}", iso(ICT[0] - 1)),
            TimeSpec::AtTime(iso(ICT[0] - 1)),
            "no snapshot of table 'in_commit_time' at or before",
        ),
        (
            "snapshot:3".into(),
            TimeSpec::AtSnapshot(3),
            "snapshot 3 not found",
        ),
        (
            "t:1".into(),
            TimeSpec::AtT(1),
            "Graph sources have no transaction numbers or commit hashes",
        ),
    ];
    for (suffix, spec, needle) in &refused {
        let from = format!("{alias}@{suffix}");
        let mut q = amounts.clone();
        q["from"] = Value::String(from.clone());
        let result = fluree.query_from().jsonld(&q).execute_formatted().await;
        expect_refused(result, &format!("from @{suffix}"), needle);

        let mut q = count.clone();
        q["from"] = Value::String(from.clone());
        let result = fluree.query_from().jsonld(&q).execute_formatted().await;
        expect_refused(result, &format!("count from @{suffix}"), needle);

        let result = fluree
            .graph_at(&alias, spec.clone())
            .query()
            .jsonld(&amounts)
            .execute_formatted()
            .await;
        expect_refused(result, &format!("graph_at({spec:?})"), needle);

        let result = fluree
            .query_from()
            .sparql(&sparql_amounts(&from))
            .execute_formatted()
            .await;
        expect_refused(result, &format!("SPARQL FROM <{from}>"), needle);
    }

    // The oldest retained commit is named, so the caller can correct the pin.
    let mut q = amounts.clone();
    q["from"] = Value::String(format!("{alias}@time:{}", iso(ICT[0] - 1)));
    let err = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect_err("before the first commit")
        .to_string();
    assert!(err.contains(&iso(ICT[0])), "{err}");

    // One source at two states in one query is refused.
    let two_states = json!({
        "@context": {"ex": "http://example.org/"},
        "from": [format!("{alias}@snapshot:0"), alias],
        "select": ["?amount"],
        "where": {"@id": "?s", "ex:amount": "?amount"},
    });
    let result = fluree
        .query_from()
        .jsonld(&two_states)
        .execute_formatted()
        .await;
    expect_refused(result, "pinned + latest", "two different states");
}

/// The mapping is always the current one, so a pin to a version whose schema
/// lacks a mapped column is an error naming the column, and a version that has
/// the name back under a new identity reads the new column's values.
#[tokio::test]
async fn a_pinned_version_resolves_columns_against_its_own_schema() {
    let (fluree, alias) = source("delta-mapped").await;
    let q = |from: String| {
        json!({
            "@context": {"ex": "http://example.org/"},
            "from": from,
            "select": ["?amount"],
            "where": {"@id": "?s", "ex:mappedAmount": "?amount"},
        })
    };
    let v0 = fluree
        .query_from()
        .jsonld(&q(format!("{alias}@snapshot:0")))
        .execute_formatted()
        .await
        .expect("v0 has `amount`");
    assert_eq!(sorted_ints(&v0), [100, 30000], "got: {v0}");

    // v1 renamed it away.
    let err = fluree
        .query_from()
        .jsonld(&q(format!("{alias}@snapshot:1")))
        .execute_formatted()
        .await
        .expect_err("v1 has no `amount`")
        .to_string();
    assert!(err.contains("no column 'amount' at version 1"), "{err}");

    // v3 re-added `amount` as a new, all-null column over the same Parquet file.
    let v3 = fluree
        .query_from()
        .jsonld(&q(alias.clone()))
        .execute_formatted()
        .await
        .expect("latest");
    assert_eq!(sorted_ints(&v3), Vec::<i64>::new(), "got: {v3}");
}

/// A Delta table is pinned and read for materialization as an Iceberg table
/// is: its latest version is the pin, an earlier version reads as it was, and a
/// version it never had is refused. One provider reads several versions of one
/// table here, so no read may answer with a version an earlier call resolved.
#[tokio::test]
async fn a_delta_table_is_pinned_and_read_whole() {
    let (fluree, alias) = source("delta-pinned-read").await;
    let provider = FlureeR2rmlProvider::new(&fluree);
    let table = "in_commit_time";

    let latest = provider
        .current_snapshot_id(&alias, table)
        .await
        .expect("latest version");
    assert_eq!(latest, Some(2));

    // (pin, rows at that version): v0 = {1}, v1 = {1, 2}, v2 = {2}.
    for (pin, rows) in [(Some(0), 1), (Some(1), 2), (Some(2), 1), (None, 1)] {
        let scan = provider
            .scan_for_materialize_stream(&alias, table, &[], None, pin, None)
            .await
            .unwrap_or_else(|e| panic!("read at {pin:?}: {e}"));
        assert_eq!(scan.to_snapshot_id, Some(pin.unwrap_or(2)), "pin {pin:?}");
        assert!(!scan.incremental);
        assert_eq!(scan.to_sequence, None);
        let batches: Vec<_> = scan.stream.try_collect().await.expect("stream");
        let read: usize = batches.iter().map(|b| b.num_rows).sum();
        assert_eq!(read, rows, "rows at pin {pin:?}");
    }

    // A window start is no reason to read less: the version is read whole.
    let scan = provider
        .scan_for_materialize_stream(&alias, table, &[], Some(0), Some(1), Some(1))
        .await
        .expect("read with a window start");
    let batches: Vec<_> = scan.stream.try_collect().await.expect("stream");
    assert_eq!(batches.iter().map(|b| b.num_rows).sum::<usize>(), 2);

    match provider
        .scan_for_materialize_stream(&alias, table, &[], None, Some(9), None)
        .await
    {
        Ok(_) => panic!("a version the table never had must fail, not fall forward"),
        Err(QueryError::SnapshotNotFound { snapshot_id: 9, .. }) => {}
        Err(other) => panic!("expected SnapshotNotFound, got: {other}"),
    }
}

const FEED_MAPPING: &str = r#"
    @prefix rr: <http://www.w3.org/ns/r2rml#> .
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

    <http://example.org/mapping#Row>
        a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "TABLE" ] ;
        rr:subjectMap [ rr:template "http://example.org/row/{id}" ; rr:class ex:Row ] ;
        rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:amount ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ] ] .
"#;

const STATE_LEDGER: &str = "fluree_materialize_state:main";

/// A copy of fixture `table` under a temp root whose commits after `through`
/// are held back until [`Staged::release`].
struct Staged {
    root: tempfile::TempDir,
    held: Vec<(std::path::PathBuf, std::path::PathBuf)>,
}

impl Staged {
    fn new(table: &str, through: u64) -> Self {
        allow_fixture_roots();
        let root = tempfile::TempDir::new().expect("staging root");
        let copy = root.path().join(table);
        copy_dir(&fixtures().join(table), &copy);
        let log = copy.join("_delta_log");
        let hold = root.path().join("held");
        std::fs::create_dir_all(&hold).expect("held dir");
        let mut held = Vec::new();
        for entry in std::fs::read_dir(&log).expect("log dir") {
            let path = entry.expect("entry").path();
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            let Some(version) = name.get(..20).and_then(|v| v.parse::<u64>().ok()) else {
                continue;
            };
            if version > through {
                let parked = hold.join(&name);
                std::fs::rename(&path, &parked).expect("hold back commit");
                held.push((parked, path));
            }
        }
        Self { root, held }
    }

    fn root(&self) -> String {
        self.root
            .path()
            .canonicalize()
            .expect("staging root")
            .display()
            .to_string()
    }

    fn release(&mut self) {
        for (parked, path) in self.held.drain(..) {
            std::fs::rename(&parked, &path).expect("release commit");
        }
    }
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).expect("create dir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy file");
        }
    }
}

async fn feed_source(
    fluree: &Fluree,
    name: &str,
    table: &str,
    root: &str,
    latest_by_key: bool,
) -> String {
    let mapping = FEED_MAPPING.replace("TABLE", table);
    let mut config = DeltaCreateConfig::new(name, root, mapping);
    config.mapping_media_type = Some("text/turtle".to_string());
    if latest_by_key {
        config = config.with_order_by("amount");
    }
    fluree
        .create_delta_graph_source(config)
        .await
        .expect("create delta graph source");
    format!("{name}:main")
}

/// `(id, name, amount)` of every `ex:Row` in `target`, sorted.
async fn rows_in(fluree: &Fluree, target: &str) -> Vec<(i64, String, i64)> {
    let q = json!({
        "select": ["?s", "?name", "?amount"],
        "where": {
            "@id": "?s",
            "@type": "http://example.org/Row",
            "http://example.org/name": "?name",
            "http://example.org/amount": "?amount"
        },
    });
    let out = fluree
        .graph(target)
        .query()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("query target");
    let mut rows: Vec<(i64, String, i64)> = out
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| {
            let id = r[0]
                .as_str()
                .expect("iri")
                .trim_start_matches("http://example.org/row/");
            (
                id.parse().expect("id"),
                r[1].as_str().expect("name").to_string(),
                r[2].as_i64().expect("amount"),
            )
        })
        .collect();
    rows.sort();
    rows
}

fn expected(rows: &[(i64, &str, i64)]) -> Vec<(i64, String, i64)> {
    rows.iter()
        .map(|(i, n, a)| (*i, (*n).to_string(), *a))
        .collect()
}

/// Materialize until the watermark reaches `head`, as the tracking worker's
/// polls would; returns whether each pass read incrementally.
async fn poll_to(fluree: &Fluree, gs: &str, target: &str, table: &str, head: i64) -> Vec<bool> {
    let mut passes = Vec::new();
    for _ in 0..10 {
        let result = fluree
            .materialize_r2rml_graph_source(gs, target, false)
            .await
            .expect("materialize");
        passes.push(result.incremental);
        let watermark = fluree
            .materialize_watermark(STATE_LEDGER, gs, target, table)
            .await
            .expect("watermark");
        if watermark == Some(head) {
            return passes;
        }
    }
    panic!("the watermark never reached version {head}");
}

/// A Delta source materializes into a native ledger and follows its table
/// through the change data feed: a first pass reads version 2 whole, and once
/// the table has moved on, later passes read only what changed — an update,
/// a delete, a MERGE and a deleted key inserted again — and leave the target
/// equal to the table at version 5. Additive and latest-by-key alike.
#[tokio::test]
async fn a_delta_source_materializes_and_follows_its_change_feed() {
    for latest_by_key in [false, true] {
        let mut staged = Staged::new("change_feed", 2);
        let fluree = FlureeBuilder::memory().build_memory();
        let gs = feed_source(
            &fluree,
            "feed",
            "change_feed",
            &staged.root(),
            latest_by_key,
        )
        .await;
        let target = "feed_native:main";

        let first = poll_to(&fluree, &gs, target, "change_feed", 2).await;
        assert_eq!(first, [false], "a first pass reads the version whole");
        assert_eq!(
            rows_in(&fluree, target).await,
            expected(&[(1, "a", 100), (2, "b", 201), (3, "c", 301), (5, "e", 500)])
        );

        staged.release();
        let later = poll_to(&fluree, &gs, target, "change_feed", 5).await;
        assert!(later.iter().all(|incremental| *incremental), "{later:?}");
        assert_eq!(
            rows_in(&fluree, target).await,
            expected(&[
                (2, "b", 201),
                (3, "c", 301),
                (4, "d2", 401),
                (5, "e2", 501),
                (6, "f", 600),
                (7, "g", 700),
            ]),
            "latest_by_key={latest_by_key}"
        );

        // Nothing new: the next poll writes nothing.
        let idle = fluree
            .materialize_r2rml_graph_source(&gs, target, false)
            .await
            .expect("idle poll");
        assert!(!idle.committed, "an unchanged table commits nothing");
    }
}

/// A window the change feed cannot serve — here it starts before the table
/// turned its feed on — is read whole instead, and the source keeps going.
#[tokio::test]
async fn a_window_the_feed_cannot_serve_is_read_whole() {
    let mut staged = Staged::new("change_feed_late", 0);
    let fluree = FlureeBuilder::memory().build_memory();
    let gs = feed_source(&fluree, "late", "change_feed_late", &staged.root(), false).await;
    let target = "late_native:main";
    poll_to(&fluree, &gs, target, "change_feed_late", 0).await;

    staged.release();
    let passes = poll_to(&fluree, &gs, target, "change_feed_late", 3).await;
    assert_eq!(passes, [false], "the window is read whole");
    assert_eq!(
        rows_in(&fluree, target).await,
        expected(&[(1, "a", 100), (2, "b", 201)])
    );
}

/// Rows a materialize stream yields, and where it says it stopped.
async fn drain(
    stream: fluree_db_query::r2rml::ColumnBatchStream,
    stopped_at: Option<std::sync::Arc<std::sync::OnceLock<u64>>>,
) -> (usize, Option<u64>) {
    let batches: Vec<_> = stream.try_collect().await.expect("rows");
    let read = batches.iter().map(|b| b.num_rows).sum();
    (read, stopped_at.and_then(|cut| cut.get().copied()))
}

/// The per-pass row budget reaches the feed: a window larger than it stops at
/// a commit boundary and says where, the watermark lands there, and passes
/// drain the backlog in bounded steps. A pinned read is never cut.
///
/// Sets `FLUREE_MATERIALIZE_MAX_ROWS_PER_PASS` for the process. The other
/// tests here tolerate it: a whole read is never cut, and they poll until the
/// watermark reaches the head.
#[tokio::test]
async fn a_change_feed_backlog_drains_in_bounded_passes() {
    std::env::set_var("FLUREE_MATERIALIZE_MAX_ROWS_PER_PASS", "4");
    let mut staged = Staged::new("change_feed", 0);
    let fluree = FlureeBuilder::memory().build_memory();
    let gs = feed_source(&fluree, "budget", "change_feed", &staged.root(), false).await;
    let target = "budget_native:main";
    poll_to(&fluree, &gs, target, "change_feed", 0).await;
    staged.release();

    let provider = FlureeR2rmlProvider::new(&fluree);
    let scan = provider
        .scan_for_materialize_stream(&gs, "change_feed", &[], Some(0), None, None)
        .await
        .expect("change window");
    assert!(scan.change_feed && scan.incremental);
    assert_eq!(scan.to_snapshot_id, Some(5));
    assert_eq!(
        drain(scan.stream, scan.stopped_at).await,
        (4, Some(1)),
        "version 1's four changes, then stop"
    );
    let pinned = provider
        .scan_for_materialize_stream(&gs, "change_feed", &[], Some(0), Some(5), None)
        .await
        .expect("pinned change window");
    assert_eq!(
        drain(pinned.stream, pinned.stopped_at).await,
        (11, None),
        "a pinned read is never cut"
    );

    let passes = poll_to(&fluree, &gs, target, "change_feed", 5).await;
    std::env::remove_var("FLUREE_MATERIALIZE_MAX_ROWS_PER_PASS");
    assert!(
        passes.len() > 1,
        "the backlog takes several passes: {passes:?}"
    );
    assert_eq!(
        rows_in(&fluree, target).await,
        expected(&[
            (2, "b", 201),
            (3, "c", 301),
            (4, "d2", 401),
            (5, "e2", 501),
            (6, "f", 600),
            (7, "g", 700),
        ])
    );
}

/// A version is read back by key: an `id` set narrows the read to those rows,
/// with each key value read as the column's own type, so a key the version no
/// longer holds comes back with nothing.
#[tokio::test]
async fn a_version_is_read_back_by_key() {
    let staged = Staged::new("change_feed", 5);
    let fluree = FlureeBuilder::memory().build_memory();
    let gs = feed_source(&fluree, "readback", "change_feed", &staged.root(), true).await;
    let provider = FlureeR2rmlProvider::new(&fluree);
    let keys = ScanFilter {
        column: "id".to_string(),
        op: ScanCmpOp::In,
        value: ScanValue::Set(
            ["1", "4", "6"]
                .map(|id| ScanValue::TemplateKey(id.to_string()))
                .to_vec(),
        ),
    };
    for (version, ids) in [(2, vec![1]), (5, vec![4, 6])] {
        let batches: Vec<_> = provider
            .rows_at_version(&gs, "change_feed", version, std::slice::from_ref(&keys))
            .await
            .expect("read back")
            .try_collect()
            .await
            .expect("stream");
        let mut read: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let column = b.column_by_name("id").expect("id column");
                (0..b.num_rows).map(|row| column.get_i64(row).expect("id"))
            })
            .collect();
        read.sort();
        assert_eq!(read, ids, "version {version}");
    }
}

/// Registration checks each table for the columns its maps project: a column
/// the table lacks is reported then, and the source still registers.
#[tokio::test]
async fn registration_reports_a_mapped_column_the_table_lacks() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = fixtures().canonicalize().expect("fixtures dir");
    let mapping = r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix ex: <http://example.org/> .
        <http://example.org/mapping#Store>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "dim_store" ] ;
            rr:subjectMap [ rr:template "http://example.org/store/{store_id}" ] ;
            rr:predicateObjectMap [ rr:predicate ex:city ; rr:objectMap [ rr:column "city" ] ] .
        <http://example.org/mapping#Absent>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "not_there" ] ;
            rr:subjectMap [ rr:template "http://example.org/absent/{id}" ] .
    "#;
    let mut config = DeltaCreateConfig::new("delta-warn", root.to_str().unwrap(), mapping);
    config.mapping_media_type = Some("text/turtle".to_string());
    let created = fluree
        .create_delta_graph_source(config)
        .await
        .expect("registers despite warnings");
    assert!(created.table_versions.is_empty(), "{created:?}");
    let warnings = created.table_warnings.join("\n");
    assert!(warnings.contains("no column 'city'"), "{warnings}");
    assert!(warnings.contains("table 'not_there'"), "{warnings}");

    // A name that cannot be placed at all is an error, not a warning.
    let mut unplaced = DeltaCreateConfig::new("delta-unplaced", root.to_str().unwrap(), mapping);
    unplaced.mapping_media_type = Some("text/turtle".to_string());
    unplaced.root = None;
    unplaced.tables.insert(
        "dim_store".to_string(),
        format!("{}/dim_store", root.display()),
    );
    let err = fluree
        .create_delta_graph_source(unplaced)
        .await
        .expect_err("`not_there` has no location")
        .to_string();
    assert!(err.contains("'not_there'"), "{err}");
}

/// Storage the instance's host-identity policy refuses is a registration
/// error: the configuration decides it and no later query could read the
/// table. The error is the typed 403 a query would report, naming what was
/// refused, and nothing is published. (A table that fails to probe for any
/// other reason still registers with a warning; see above.)
#[tokio::test]
async fn a_path_source_the_host_identity_policy_refuses_is_not_registered() {
    let mapping = r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix ex: <http://example.org/> .
        <http://example.org/mapping#Store>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "dim_store" ] ;
            rr:subjectMap [ rr:template "http://example.org/store/{store_id}" ] ;
            rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] .
    "#;
    let source = |name: &str, root: &str| {
        let mut config = DeltaCreateConfig::new(name, root, mapping);
        config.mapping_media_type = Some("text/turtle".to_string());
        config.io.s3_region = Some("us-east-1".to_string());
        config
    };
    for (policy, name, named) in [
        (
            fluree_db_api::HostIdentityPolicy::deny_all(),
            "delta-no-aws",
            "aws",
        ),
        (
            fluree_db_api::HostIdentityPolicy::parse(None, Some("lake")).unwrap(),
            "delta-off-allowlist",
            "stack-internal",
        ),
    ] {
        let fluree = FlureeBuilder::memory()
            .build_memory()
            .with_host_identity_policy(policy);
        let err = fluree
            .create_delta_graph_source(source(name, "s3://stack-internal/wh"))
            .await
            .expect_err("the policy refuses the table's storage");
        assert_eq!(err.status_code(), 403, "{err}");
        assert_eq!(
            err.error_type(),
            fluree_vocab::errors::HOST_IDENTITY_REFUSED,
            "{err}"
        );
        assert!(err.to_string().contains(named), "{err}");
        assert!(
            fluree
                .nameservice()
                .lookup_graph_source(&format!("{name}:main"))
                .await
                .unwrap()
                .is_none(),
            "{name} was published"
        );
    }
}

/// A literal service-principal secret is stored with the source; the info view
/// every client can read must not return it.
#[tokio::test]
async fn graph_source_info_redacts_a_stored_azure_secret() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = fixtures().canonicalize().expect("fixtures dir");
    let mut config = DeltaCreateConfig::new("delta-secret", root.to_str().unwrap(), MAPPING);
    config.mapping_media_type = Some("text/turtle".to_string());
    config.io.azure = Some(fluree_db_api::DeltaAzureAuth::ClientSecret {
        tenant_id: "tenant-visible".to_string(),
        client_id: "client-visible".to_string(),
        client_secret: fluree_db_api::DeltaConfigValue::literal("hunter2-do-not-leak"),
    });
    fluree
        .create_delta_graph_source(config)
        .await
        .expect("create");
    let record = fluree
        .nameservice()
        .lookup_graph_source("delta-secret:main")
        .await
        .expect("lookup")
        .expect("record");
    assert!(
        record.config.contains("hunter2-do-not-leak"),
        "precondition: the literal is what gets stored"
    );
    let info = fluree_db_api::ledger_info::build_graph_source_info(&fluree, &record)
        .await
        .expect("info")
        .to_string();
    assert!(!info.contains("hunter2-do-not-leak"), "{info}");
    assert!(info.contains("client-visible"), "{info}");
}

/// View policy on a Delta source: request policies through the shared R2RML
/// gate, and a registered model ledger supplying rules and default-allow —
/// the record's `model` / `default_allow` are read by Delta-specific code.
#[tokio::test]
async fn view_policies_govern_a_delta_source() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = fixtures().canonicalize().expect("fixtures dir");
    let context = json!({ "ex": "http://example.org/", "f": "https://ns.flur.ee/db#" });

    // `dim_store` has three stores; `fact_order` six orders with five totals.
    let names = |from: &str, opts: Value| {
        let mut q = json!({
            "@context": context.clone(),
            "from": from,
            "select": ["?name"],
            "where": { "@id": "?s", "ex:storeName": "?name" },
        });
        if !opts.is_null() {
            q["opts"] = opts;
        }
        q
    };
    let totals = |from: &str, opts: Value| {
        json!({
            "@context": context.clone(),
            "from": from,
            "opts": opts,
            "select": ["?total"],
            "where": { "@id": "?o", "ex:total": "?total" },
        })
    };
    let count = |fluree: &Fluree, q: Value| {
        let fluree = fluree.clone();
        async move {
            fluree
                .query_from()
                .jsonld(&q)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{q}: {e}"))
                .as_array()
                .map_or(0, Vec::len)
        }
    };

    // --- Request policies on an ungoverned source.
    let mut open = DeltaCreateConfig::new("delta-open", root.to_str().unwrap(), MAPPING);
    open.mapping_media_type = Some("text/turtle".to_string());
    fluree
        .create_delta_graph_source(open)
        .await
        .expect("open source");
    let open = "delta-open:main";
    let hide_names = json!([{
        "@id": "ex:hideNames", "@type": "f:AccessPolicy", "f:action": "f:view",
        "f:onProperty": [{ "@id": "http://example.org/storeName" }], "f:allow": false
    }]);
    let deny_names = json!({ "policy": hide_names, "default-allow": true });
    assert_eq!(count(&fluree, names(open, Value::Null)).await, 3);
    assert_eq!(count(&fluree, names(open, deny_names.clone())).await, 0);
    assert_eq!(
        count(&fluree, totals(open, deny_names)).await,
        5,
        "the deny targets one property only"
    );
    assert_eq!(
        count(
            &fluree,
            names(open, json!({ "policy": [], "default-allow": false }))
        )
        .await,
        0,
        "no matching rule and default-allow false"
    );

    // --- A model ledger: baseline rule hides totals, a reader class grants them.
    const MODEL: &str = "delta-governance:main";
    let ledger = fluree.create_ledger(MODEL).await.expect("model ledger");
    fluree
        .insert(
            ledger,
            &json!({
                "@context": context.clone(),
                "@graph": [
                    { "@id": "ex:hideTotals", "@type": "f:AccessPolicy", "f:action": { "@id": "f:view" },
                      "f:onProperty": [{ "@id": "ex:total" }], "f:allow": false },
                    { "@id": "ex:readTotals", "@type": "ex:ReaderPolicy", "f:action": { "@id": "f:view" },
                      "f:onProperty": [{ "@id": "ex:total" }], "f:allow": true },
                    { "@id": "http://example.org/users/reader", "f:policyClass": { "@id": "ex:ReaderPolicy" } }
                ]
            }),
        )
        .await
        .expect("seed model ledger");
    let mut governed = DeltaCreateConfig::new("delta-governed", root.to_str().unwrap(), MAPPING);
    governed.mapping_media_type = Some("text/turtle".to_string());
    governed.model = Some(MODEL.to_string());
    governed.default_allow = Some(true);
    fluree
        .create_delta_graph_source(governed)
        .await
        .expect("governed source");
    let governed = "delta-governed:main";

    // No request options at all: the registered model and default-allow apply.
    assert_eq!(
        count(&fluree, names(governed, Value::Null)).await,
        3,
        "registered default-allow keeps unrestricted properties readable"
    );
    let mut bare_totals = totals(governed, Value::Null);
    bare_totals.as_object_mut().unwrap().remove("opts");
    assert_eq!(
        count(&fluree, bare_totals).await,
        0,
        "the model's baseline rule hides totals"
    );
    let reader = json!({ "identity": "http://example.org/users/reader", "default-allow": false });
    assert_eq!(count(&fluree, totals(governed, reader)).await, 5);
}

/// A version whose log replays but whose data was vacuumed resolves, then fails
/// at scan with a message that says so — not a raw storage error, and never the
/// current rows.
#[tokio::test]
async fn a_vacuumed_version_is_an_explicit_error() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = fixtures().canonicalize().expect("fixtures dir");
    let mapping = r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix ex: <http://example.org/> .
        <http://example.org/mapping#Lost>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "history_loss" ] ;
            rr:subjectMap [ rr:template "http://example.org/lost/{id}" ] ;
            rr:predicateObjectMap [ rr:predicate ex:amount ; rr:objectMap [ rr:column "amount" ] ] .
    "#;
    let mut config = DeltaCreateConfig::new("delta-lost", root.to_str().unwrap(), mapping);
    config.mapping_media_type = Some("text/turtle".to_string());
    fluree
        .create_delta_graph_source(config)
        .await
        .expect("create");
    let q = |from: &str| {
        json!({
            "@context": {"ex": "http://example.org/"},
            "from": from,
            "select": ["?a"],
            "where": {"@id": "?s", "ex:amount": "?a"},
        })
    };
    let latest = fluree
        .query_from()
        .jsonld(&q("delta-lost:main"))
        .execute_formatted()
        .await
        .expect("latest reads");
    assert_eq!(latest.as_array().map(Vec::len), Some(1), "{latest}");

    let err = fluree
        .query_from()
        .jsonld(&q("delta-lost:main@snapshot:0"))
        .execute_formatted()
        .await
        .expect_err("v0's only data file is gone")
        .to_string();
    assert!(
        err.contains("version 0 of table 'history_loss' can no longer be read"),
        "{err}"
    );
}

/// Scan filters are advisory: a Delta scan returns every row, so each query
/// shape must apply its own constraints. COUNT over a constant-object pattern
/// and over a FILTER must agree with the row query.
#[tokio::test]
async fn aggregates_apply_constraints_the_scan_does_not() {
    let (fluree, alias) = source("delta-agg").await;
    let run = |sparql: String| {
        let fluree = fluree.clone();
        async move {
            let v = fluree
                .query_from()
                .sparql(&sparql)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{sparql}: {e}"));
            v["results"]["bindings"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        }
    };
    let ex = "http://example.org/";
    // fact_order at latest: totals 250, 400, 500, 600, 700 (order 3 has none).
    let rows = run(format!(
        "SELECT ?o FROM <{alias}> WHERE {{ ?o <{ex}total> 600 }}"
    ))
    .await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    for (label, pattern, expected) in [
        ("constant object", format!("?o <{ex}total> 600"), "1"),
        (
            "filter",
            format!("?o <{ex}total> ?t FILTER(?t >= 500)"),
            "3",
        ),
        (
            "class + constant",
            format!("?o a <{ex}Order> ; <{ex}total> 600"),
            "1",
        ),
    ] {
        let n = run(format!(
            "SELECT (COUNT(?o) AS ?n) FROM <{alias}> WHERE {{ {pattern} }}"
        ))
        .await;
        assert_eq!(
            n[0]["n"]["value"].as_str(),
            Some(expected),
            "COUNT with {label}: {n:?}"
        );
    }
}

/// A mapped boolean column compares equal to a boolean literal. The `flags`
/// fixture (delta-rs) holds ids 1..6 with shipped = T, T, F, T, null, F.
#[tokio::test]
async fn a_mapped_boolean_compares_equal_to_a_boolean_literal() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let root = fixtures().canonicalize().expect("fixtures dir");
    let mapping = r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix ex: <http://example.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        <http://example.org/mapping#Flag>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "flags" ] ;
            rr:subjectMap [ rr:template "http://example.org/flag/{id}" ] ;
            rr:predicateObjectMap [
                rr:predicate ex:shipped ;
                rr:objectMap [ rr:column "shipped" ; rr:datatype xsd:boolean ]
            ] .
    "#;
    let mut config = DeltaCreateConfig::new("delta-flags", root.to_str().unwrap(), mapping);
    config.mapping_media_type = Some("text/turtle".to_string());
    fluree
        .create_delta_graph_source(config)
        .await
        .expect("create");

    let ids = |filter: &'static str| {
        let fluree = fluree.clone();
        async move {
            let sparql = format!(
                "SELECT ?s FROM <delta-flags:main> WHERE {{ \
                 ?s <http://example.org/shipped> ?b FILTER({filter}) }}"
            );
            let v = fluree
                .query_from()
                .sparql(&sparql)
                .execute_formatted()
                .await
                .unwrap_or_else(|e| panic!("{sparql}: {e}"));
            let mut out: Vec<String> = v["results"]["bindings"]
                .as_array()
                .expect("bindings")
                .iter()
                .map(|b| {
                    let iri = b["s"]["value"].as_str().expect("iri");
                    iri.rsplit('/').next().unwrap().to_string()
                })
                .collect();
            out.sort();
            out
        }
    };
    assert_eq!(ids("?b = false").await, ["3", "6"]);
    assert_eq!(ids("?b = true").await, ["1", "2", "4"]);
    assert_eq!(ids("?b != true").await, ["3", "6"]);
    assert_eq!(ids("!?b").await, ["3", "6"]);

    // The JSON-LD surface shares the binding, so the same comparison holds.
    let jsonld = json!({
        "@context": {"ex": "http://example.org/"},
        "from": "delta-flags:main",
        "select": ["?s"],
        "where": [{"@id": "?s", "ex:shipped": "?b"}, ["filter", "(= ?b false)"]],
    });
    let rows = fluree
        .query_from()
        .jsonld(&jsonld)
        .execute_formatted()
        .await
        .expect("jsonld filter");
    assert_eq!(rows.as_array().map(Vec::len), Some(2), "{rows}");
}

/// A table that needs a reader feature Kernel does not implement is flagged
/// when it is mapped, not first discovered by a query, and never read.
#[tokio::test]
async fn registration_warns_of_an_unsupported_reader_protocol() {
    allow_fixture_roots();
    let fluree = FlureeBuilder::memory().build_memory();
    let staged =
        tempfile::TempDir::new_in(fixtures()).expect("staging dir under the fixtures root");
    let table = staged.path().join("flags");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    for entry in std::fs::read_dir(fixtures().join("flags")).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), table.join(entry.file_name())).unwrap();
        }
    }
    let log = "_delta_log/00000000000000000000.json";
    let rewritten: Vec<String> = std::fs::read_to_string(fixtures().join("flags").join(log))
        .unwrap()
        .lines()
        .map(|line| {
            let mut action: Value = serde_json::from_str(line).unwrap();
            if action.get("protocol").is_some() {
                action["protocol"] = json!({
                    "minReaderVersion": 3,
                    "minWriterVersion": 7,
                    "readerFeatures": ["flureeUnknownFeature"],
                    "writerFeatures": ["flureeUnknownFeature"],
                });
            }
            action.to_string()
        })
        .collect();
    std::fs::write(table.join(log), rewritten.join("\n")).unwrap();

    let mapping = r#"
        @prefix rr: <http://www.w3.org/ns/r2rml#> .
        @prefix ex: <http://example.org/> .
        <http://example.org/mapping#Flag>
            a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "flags" ] ;
            rr:subjectMap [ rr:template "http://example.org/flag/{id}" ] ;
            rr:predicateObjectMap [ rr:predicate ex:shipped ; rr:objectMap [ rr:column "shipped" ] ] .
    "#;
    let root = staged.path().canonicalize().unwrap();
    let mut config = DeltaCreateConfig::new("delta-unsupported", root.to_str().unwrap(), mapping);
    config.mapping_media_type = Some("text/turtle".to_string());
    let created = fluree
        .create_delta_graph_source(config)
        .await
        .expect("registers despite warnings");
    assert!(created.table_versions.is_empty(), "{created:?}");
    let warnings = created.table_warnings.join("\n");
    assert!(
        warnings.contains("table 'flags'") && warnings.contains("flureeUnknownFeature"),
        "{warnings}"
    );

    let err = fluree
        .query_from()
        .jsonld(&json!({
            "@context": {"ex": "http://example.org/"},
            "from": "delta-unsupported:main",
            "select": ["?s"],
            "where": {"@id": "?s", "ex:shipped": "?v"},
        }))
        .execute_formatted()
        .await
        .expect_err("an unsupported table is never read");
    assert!(err.to_string().contains("flureeUnknownFeature"), "{err}");
}
