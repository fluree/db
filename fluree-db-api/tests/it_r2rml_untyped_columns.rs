//! Every query lane over an R2RML source answers an un-annotated column the way
//! the generic pipeline does.
//!
//! A `rr:column` object map with no `rr:datatype` materializes an `xsd:string`
//! literal whose lexical form is the materializer's rendering of the physical
//! value (`10`, `-0`, `0.050000000`, `2024-01-01T00:00:00Z`, hex bytes). So
//! grouping, ordering, `MIN`/`MAX` and `COUNT(DISTINCT)` over it go by that
//! string: "10" sorts before "9", and -0.0 and 0.0 are different strings. A
//! fast lane that reads the physical column must reach the same answer, or
//! decline.
//!
//! Two sources carry one column of every physical type: the committed local
//! Iceberg table `silver.cells` (the fused aggregate, and the scan's own
//! file-level pushdowns) and a fake Trino endpoint (the SQL block and
//! aggregate lanes). Every case runs on SPARQL and on its JSON-LD twin, with
//! fast paths on and off, and pins the routing stamp: `MustFire` where the
//! lane serves the shape, `MustNotFire` where it must decline, each paired on
//! the same source with a shape that fires. The answers the generic lane gives
//! are pinned too, for the shapes whose answer differs from the physical
//! order: two lanes agreeing is no evidence of either being right, and the
//! scan's pushdowns run with fast paths off as well.
//!
//! Own binary: it flips the process-wide fast-path switch, sets
//! `FLUREE_ICEBERG_LOCAL_ROOTS`, and asserts the absence of routing stamps.

#![cfg(all(feature = "sql", feature = "native"))]

#[path = "support/fake_sql.rs"]
mod fake_sql;
#[path = "support/span_capture.rs"]
mod span_capture;

use fake_sql::{FakeSql, Table};
use fluree_db_api::{
    set_fast_paths_disabled, Fluree, FlureeBuilder, R2rmlCreateConfig, SqlCreateConfig,
};
use serde_json::{json, Value};
use tokio::sync::Mutex;

/// Both tests flip the process-wide fast-path switch.
static KILL_SWITCH: Mutex<()> = Mutex::const_new(());

/// The physical columns of `silver.cells` / `lab.cells`, each mapped
/// un-annotated to `ex:<name>`.
const COLUMNS: &[&str] = &[
    "i32", "i64", "f32", "f64", "dec", "flag", "day", "ts", "tstz", "raw", "label",
];

const FUSED: &str = "fused_r2rml_aggregate";
const SQL_AGG: &str = "sql_aggregate_pushdown";
const SQL_BLOCK: &str = "sql_block_pushdown";

/// Annotated maps over the same tables: `xsd:integer` over the long column and
/// over `ntext`, the string column holding the int column's values as text.
const TYPED_POMS: &str = "\
    rr:predicateObjectMap [ rr:predicate ex:i64N ; rr:objectMap [ rr:column \"i64\" ; rr:datatype xsd:integer ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:ntextN ; rr:objectMap [ rr:column \"ntext\" ; rr:datatype xsd:integer ] ] ;
";

/// The SQL source only: a `timestamp(9)` column, and language-tagged maps
/// over the double and long columns.
const SQL_POMS: &str = "\
    rr:predicateObjectMap [ rr:predicate ex:ts9 ; rr:objectMap [ rr:column \"ts9\" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:f64en ; rr:objectMap [ rr:column \"f64\" ; rr:language \"en\" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:i64en ; rr:objectMap [ rr:column \"i64\" ; rr:language \"en\" ] ] ;
";

/// `Cell` maps every column un-annotated (plus `extra`); `Twin` maps the same
/// table again under its own subjects, reached from a cell by `ex:twin`, so a
/// grouping key can come from the far side of a join.
fn cells_mapping(table: &str, extra: &str) -> String {
    let mut out = String::from(
        "@prefix rr: <http://www.w3.org/ns/r2rml#> .\n@prefix ex: <http://example.org/> .\n\
         @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n",
    );
    let poms = |prefix: &str| -> String {
        std::iter::once("grp")
            .chain(COLUMNS.iter().copied())
            .map(|c| {
                format!(
                    "    rr:predicateObjectMap [ rr:predicate ex:{prefix}{c} ; rr:objectMap [ rr:column \"{c}\" ] ] ;\n"
                )
            })
            .collect()
    };
    out.push_str(&format!(
        "<http://example.org/mapping#Cell> a rr:TriplesMap ;\n\
         \x20   rr:logicalTable [ rr:tableName \"{table}\" ] ;\n\
         \x20   rr:subjectMap [ rr:template \"http://example.org/cell/{{id}}\" ; rr:class ex:Cell ] ;\n\
         \x20   rr:predicateObjectMap [ rr:predicate ex:id ; rr:objectMap [ rr:column \"id\" ] ] ;\n\
         {}{extra}\
         \x20   rr:predicateObjectMap [ rr:predicate ex:twin ; rr:objectMap [\n\
         \x20       rr:parentTriplesMap <http://example.org/mapping#Twin> ;\n\
         \x20       rr:joinCondition [ rr:child \"id\" ; rr:parent \"id\" ] ] ] .\n\
         <http://example.org/mapping#Twin> a rr:TriplesMap ;\n\
         \x20   rr:logicalTable [ rr:tableName \"{table}\" ] ;\n\
         \x20   rr:subjectMap [ rr:template \"http://example.org/twin/{{id}}\" ; rr:class ex:Twin ] ;\n\
         {}\
         \x20   rr:predicateObjectMap [ rr:predicate ex:tid ; rr:objectMap [ rr:column \"id\" ] ] .\n",
        poms(""),
        poms("t_"),
    ));
    out
}

/// `lab.refs`: values to join the cells to by literal value.
const REFS_MAPPING: &str = r#"
    <http://example.org/mapping#Ref> a rr:TriplesMap ;
        rr:logicalTable [ rr:tableName "lab.refs" ] ;
        rr:subjectMap [ rr:template "http://example.org/ref/{id}" ] ;
        rr:predicateObjectMap [ rr:predicate ex:refN ; rr:objectMap [ rr:column "n" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:refF ; rr:objectMap [ rr:column "f" ] ] ;
        rr:predicateObjectMap [ rr:predicate ex:refD ; rr:objectMap [ rr:column "d" ] ] .
"#;

/// `lab.cells` on the fake endpoint: the Iceberg fixture's rows, except that
/// row 6 holds 2.5 where the fixture holds NaN (the fake orders a NaN equal to
/// every number, which no database does).
fn sql_tables() -> FakeSql {
    let cols: &[(&str, &str)] = &[
        ("id", "bigint"),
        ("grp", "varchar"),
        ("i32", "integer"),
        ("i64", "bigint"),
        ("f32", "real"),
        ("f64", "double"),
        ("dec", "decimal(38,9)"),
        ("flag", "boolean"),
        ("day", "date"),
        ("ts", "timestamp(6)"),
        ("tstz", "timestamp(6) with time zone"),
        ("raw", "varbinary"),
        ("label", "varchar"),
        ("ntext", "varchar"),
        ("ts9", "timestamp(9)"),
    ];
    let row = |id: i64,
               grp: &str,
               i: Value,
               l: Value,
               f: Value,
               d: Value,
               dec: Value,
               flag: Value,
               day: Value,
               ts: Value,
               raw: Value,
               label: &str| {
        let tstz = match &ts {
            Value::String(s) => json!(format!("{s} UTC")),
            other => other.clone(),
        };
        let ntext = match &i {
            Value::Number(n) => json!(n.to_string()),
            _ => Value::Null,
        };
        // Two instants a nanosecond apart, which decode to one microsecond.
        let ts9 = match id {
            1..=4 => json!("2024-01-01 00:00:00.000000001"),
            5..=7 => json!("2024-01-01 00:00:00.000000002"),
            _ => Value::Null,
        };
        vec![
            json!(id),
            json!(grp),
            i,
            l,
            f,
            d,
            dec,
            flag,
            day,
            ts,
            tstz,
            raw,
            json!(label),
            ntext,
            ts9,
        ]
    };
    let n = Value::Null;
    let rows = vec![
        row(
            1,
            "x",
            json!(2),
            json!(2),
            json!(1.5),
            json!(1.5),
            json!("0.050000000"),
            json!(true),
            json!("2024-01-09"),
            json!("2024-01-01 00:00:00.000000"),
            json!("AAE="),
            "b",
        ),
        row(
            2,
            "y",
            json!(9),
            json!(9),
            json!(0.1),
            json!(0.1),
            json!("1.500000000"),
            json!(false),
            json!("2024-01-10"),
            json!("2024-01-01 00:00:00.000001"),
            json!("/w=="),
            "a",
        ),
        row(
            3,
            "x",
            json!(-3),
            json!(-3),
            json!(-0.0),
            json!(-0.0),
            json!("-0.500000000"),
            json!(true),
            json!("1969-12-31"),
            json!("1969-12-31 23:59:59.999999"),
            json!(""),
            "c",
        ),
        row(
            4,
            "y",
            json!(9),
            json!(9),
            json!(0.0),
            json!(0.0),
            json!("10.000000000"),
            json!(true),
            json!("2024-01-09"),
            json!("2024-01-01 00:00:00.000000"),
            json!("AAE="),
            "b",
        ),
        row(
            5,
            "x",
            json!(10),
            json!(10),
            json!(10.0),
            json!(10.0),
            json!("0.050000000"),
            json!(false),
            json!("2000-02-29"),
            json!("2000-02-29 12:30:00.000000"),
            json!("Cg=="),
            "B",
        ),
        row(
            6,
            "y",
            json!(100),
            json!(9_007_199_254_740_993_i64),
            json!(2.5),
            json!(2.5),
            json!("2.000000000"),
            json!(true),
            json!("1970-01-01"),
            json!("1970-01-01 00:00:00.000000"),
            json!("/w=="),
            "\u{e9}",
        ),
        row(
            7,
            "x",
            json!(10),
            json!(10),
            json!(1.5),
            json!(1e21),
            json!("-0.500000000"),
            json!(false),
            json!("2024-01-10"),
            json!("2024-01-01 00:00:00.500000"),
            json!("AAE="),
            "a",
        ),
        row(
            8,
            "y",
            n.clone(),
            n.clone(),
            n.clone(),
            n.clone(),
            n.clone(),
            n.clone(),
            n.clone(),
            n.clone(),
            n.clone(),
            "z",
        ),
    ];
    FakeSql::new()
        .table(Table::new("lab.cells", cols, rows))
        .table(Table::new(
            "lab.refs",
            &[
                ("id", "bigint"),
                ("n", "bigint"),
                ("f", "double"),
                ("d", "decimal(38,2)"),
            ],
            vec![
                vec![json!(1), json!(10), json!(-0.0), json!("10.00")],
                vec![json!(2), json!(9), json!(0.0), json!("0.05")],
                vec![json!(3), json!(2), json!(1.5), json!("-0.50")],
            ],
        ))
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Routing {
    MustFire,
    MustNotFire,
}

struct Case {
    name: String,
    sparql: String,
    /// The JSON-LD twin, run with the same `from`.
    jsonld: Value,
    /// Output variables, for rendering JSON-LD rows like SPARQL ones.
    vars: Vec<&'static str>,
    /// Whether row order is part of the answer.
    ordered: bool,
    /// The routing stamps the fast run must (not) carry. A case with none
    /// reaches no fast lane, and pins its answer instead.
    routes: Vec<(&'static str, Routing)>,
    /// The generic pipeline's answer, where pinning it is the point.
    expect: Option<Vec<String>>,
    /// The data files the scan reads, on either surface and with fast paths
    /// on or off, where whether a pushdown pruned is the point.
    files_read: Option<usize>,
}

impl Case {
    fn new(name: String, sparql: String, jsonld: Value, vars: &[&'static str]) -> Self {
        Self {
            name,
            sparql,
            jsonld,
            vars: vars.to_vec(),
            ordered: false,
            routes: Vec::new(),
            expect: None,
            files_read: None,
        }
    }

    fn ordered(mut self) -> Self {
        self.ordered = true;
        self
    }

    fn route(mut self, site: &'static str, routing: Routing) -> Self {
        self.routes.push((site, routing));
        self
    }

    fn expect(mut self, rows: &[&str]) -> Self {
        self.expect = Some(rows.iter().map(ToString::to_string).collect());
        self
    }

    fn files_read(mut self, n: usize) -> Self {
        self.files_read = Some(n);
        self
    }
}

const PREFIX: &str = "PREFIX ex: <http://example.org/>\n";

/// SPARQL JSON results as `var=value` rows (variables alphabetical), sorted
/// unless the order is part of the answer.
fn sparql_rows(v: &Value, ordered: bool) -> Vec<String> {
    let mut vars: Vec<String> = v["head"]["vars"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    vars.sort();
    let mut out: Vec<String> = v["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("not SPARQL JSON: {v}"))
        .iter()
        .map(|b| {
            vars.iter()
                .map(|var| format!("{var}={}", b[var]["value"].as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    if !ordered {
        out.sort();
    }
    out
}

/// JSON-LD select rows rendered like [`sparql_rows`].
fn jsonld_rows(v: &Value, vars: &[&str], ordered: bool) -> Vec<String> {
    let mut order: Vec<(usize, &str)> = vars.iter().copied().enumerate().collect();
    order.sort_by(|a, b| a.1.cmp(b.1));
    let cell = |c: &Value| match c {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Object(o) => match o.get("@value") {
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => o
                .get("@id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        },
        other => other.to_string(),
    };
    let mut out: Vec<String> = v
        .as_array()
        .unwrap_or_else(|| panic!("not JSON-LD select results: {v}"))
        .iter()
        .map(|row| {
            // A one-variable select answers bare values, not one-element rows.
            let row = match row {
                Value::Array(cells) => cells.clone(),
                single => vec![single.clone()],
            };
            order
                .iter()
                .map(|(i, var)| format!("{var}={}", cell(&row[*i])))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    if !ordered {
        out.sort();
    }
    out
}

/// One run's rows, the routing sites that proceeded, and the data files read.
struct Run {
    rows: Vec<String>,
    sites: Vec<String>,
    files: usize,
}

async fn run(
    fluree: &Fluree,
    store: &span_capture::SpanStore,
    c: &Case,
    from: &str,
    jsonld: bool,
    fast: bool,
) -> Run {
    set_fast_paths_disabled(!fast);
    let events = store.find_events("fast-path outcome").len();
    let reads = store.find_spans("iceberg.parquet_read").len();
    let rows = if jsonld {
        let mut q = c.jsonld.clone();
        q["@context"] = json!({"ex": "http://example.org/"});
        q["from"] = json!(from);
        fluree
            .query_from()
            .jsonld(&q)
            .execute_formatted()
            .await
            .map(|v| jsonld_rows(&v, &c.vars, c.ordered))
    } else {
        fluree
            .query_from()
            .sparql(&format!("{PREFIX}{}", c.sparql))
            .execute_formatted()
            .await
            .map(|v| sparql_rows(&v, c.ordered))
    }
    .unwrap_or_else(|e| vec![format!("ERROR {e}")]);
    set_fast_paths_disabled(false);
    Run {
        rows,
        sites: store.find_events("fast-path outcome")[events..]
            .iter()
            .filter(|e| e.fields.get("outcome").map(String::as_str) == Some("proceed"))
            .filter_map(|e| e.fields.get("site").cloned())
            .collect(),
        files: store.find_spans("iceberg.parquet_read").len() - reads,
    }
}

/// Run every case on both surfaces, fast and generic; collect every
/// disagreement.
async fn check(fluree: &Fluree, cases: &[Case], from: &str) -> Vec<String> {
    let (store, _guard) = span_capture::init_test_tracing();
    let mut failures = Vec::new();
    for c in cases {
        let mut problems = Vec::new();
        let mut answers: Vec<(&str, Vec<String>)> = Vec::new();
        for (surface, jsonld) in [("sparql", false), ("json-ld", true)] {
            let fast = run(fluree, &store, c, from, jsonld, true).await;
            let generic = run(fluree, &store, c, from, jsonld, false).await;
            if fast.rows != generic.rows {
                problems.push(format!(
                    "{surface}: fast {:?} != generic {:?}",
                    fast.rows, generic.rows
                ));
            }
            for (site, routing) in &c.routes {
                let fired = fast.sites.iter().any(|s| s == site);
                match (routing, fired) {
                    (Routing::MustFire, false) => problems.push(format!(
                        "{surface}: `{site}` must fire [proceeded: {:?}]",
                        fast.sites
                    )),
                    (Routing::MustNotFire, true) => {
                        problems.push(format!("{surface}: `{site}` must not fire"));
                    }
                    _ => {}
                }
            }
            if let Some(n) = c.files_read {
                for (lane, r) in [("fast", &fast), ("generic", &generic)] {
                    if r.files != n {
                        problems.push(format!("{surface} {lane}: read {} files, not {n}", r.files));
                    }
                }
            }
            answers.push((surface, generic.rows));
        }
        if answers[0].1 != answers[1].1 {
            problems.push(format!(
                "json-ld {:?} != sparql {:?}",
                answers[1].1, answers[0].1
            ));
        }
        match &c.expect {
            Some(expect) if &answers[0].1 != expect => {
                problems.push(format!("answer {:?} != expected {expect:?}", answers[0].1));
            }
            None if c.routes.is_empty() => {
                problems.push("a case no lane serves must pin its answer".to_string());
            }
            _ => {}
        }
        if !problems.is_empty() {
            failures.push(format!("{}:\n  {}", c.name, problems.join("\n  ")));
        }
    }
    // A `MustNotFire` only means something beside a shape on the same source
    // that reaches the lane.
    for c in cases {
        for (site, routing) in &c.routes {
            let paired = cases
                .iter()
                .any(|o| o.routes.contains(&(site, Routing::MustFire)));
            if *routing == Routing::MustNotFire && !paired {
                failures.push(format!(
                    "{}: no `MustFire` case on this source pairs the `{site}` decline",
                    c.name
                ));
            }
        }
    }
    failures
}

// ---------------------------------------------------------------------------
// Shapes, one per column
// ---------------------------------------------------------------------------

/// `GROUP BY` an un-annotated column: one group per rendered string.
fn group_case(src: &str, c: &str) -> Case {
    Case::new(
        format!("{c}: GROUP BY with COUNT"),
        format!("SELECT ?v (COUNT(?s) AS ?n) FROM <{src}> WHERE {{ ?s ex:{c} ?v }} GROUP BY ?v"),
        json!({
            "select": ["?v", "(as (count ?s) ?n)"],
            "where": {"@id": "?s", format!("ex:{c}"): "?v"},
            "groupBy": ["?v"],
        }),
        &["v", "n"],
    )
}

/// A composite key (a string column beside the un-annotated one) and an
/// aggregate over a third column.
fn composite_case(src: &str, c: &str) -> Case {
    Case::new(
        format!("{c}: composite GROUP BY with COUNT of another column"),
        format!(
            "SELECT ?g ?v (COUNT(?i) AS ?n) FROM <{src}> \
             WHERE {{ ?s ex:grp ?g ; ex:{c} ?v ; ex:id ?i }} GROUP BY ?g ?v"
        ),
        json!({
            "select": ["?g", "?v", "(as (count ?i) ?n)"],
            "where": {"@id": "?s", "ex:grp": "?g", format!("ex:{c}"): "?v", "ex:id": "?i"},
            "groupBy": ["?g", "?v"],
        }),
        &["g", "v", "n"],
    )
}

/// The key read from the far side of a join (the fused join path's dim map).
fn dim_group_case(src: &str, c: &str) -> Case {
    Case::new(
        format!("{c}: GROUP BY a key across a join"),
        format!(
            "SELECT ?v (COUNT(?s) AS ?n) FROM <{src}> \
             WHERE {{ ?s ex:twin ?t . ?t ex:t_{c} ?v }} GROUP BY ?v"
        ),
        json!({
            "select": ["?v", "(as (count ?s) ?n)"],
            "where": [{"@id": "?s", "ex:twin": "?t"}, {"@id": "?t", format!("ex:t_{c}"): "?v"}],
            "groupBy": ["?v"],
        }),
        &["v", "n"],
    )
}

fn direction(desc: bool) -> (&'static str, Value) {
    if desc {
        ("DESC(?v)", json!("(desc ?v)"))
    } else {
        ("?v", json!("?v"))
    }
}

/// The grouped rows ordered by the key, top two: a top-k over the groups.
fn group_order_case(src: &str, c: &str, desc: bool) -> Case {
    let (sparql_ord, jsonld_ord) = direction(desc);
    Case::new(
        format!("{c}: GROUP BY ORDER BY {sparql_ord} LIMIT 2"),
        format!(
            "SELECT ?v (COUNT(?s) AS ?n) FROM <{src}> WHERE {{ ?s ex:{c} ?v }} \
             GROUP BY ?v ORDER BY {sparql_ord} LIMIT 2"
        ),
        json!({
            "select": ["?v", "(as (count ?s) ?n)"],
            "where": {"@id": "?s", format!("ex:{c}"): "?v"},
            "groupBy": ["?v"],
            "orderBy": [jsonld_ord],
            "limit": 2,
        }),
        &["v", "n"],
    )
    .ordered()
}

fn minmax_case(src: &str, c: &str) -> Case {
    Case::new(
        format!("{c}: MIN and MAX"),
        format!("SELECT (MIN(?v) AS ?lo) (MAX(?v) AS ?hi) FROM <{src}> WHERE {{ ?s ex:{c} ?v }}"),
        json!({
            "select": ["(as (min ?v) ?lo)", "(as (max ?v) ?hi)"],
            "where": {"@id": "?s", format!("ex:{c}"): "?v"},
        }),
        &["lo", "hi"],
    )
}

fn grouped_minmax_case(src: &str, c: &str) -> Case {
    Case::new(
        format!("{c}: MIN and MAX per group"),
        format!(
            "SELECT ?g (MIN(?v) AS ?lo) (MAX(?v) AS ?hi) FROM <{src}> \
             WHERE {{ ?s ex:grp ?g ; ex:{c} ?v }} GROUP BY ?g"
        ),
        json!({
            "select": ["?g", "(as (min ?v) ?lo)", "(as (max ?v) ?hi)"],
            "where": {"@id": "?s", "ex:grp": "?g", format!("ex:{c}"): "?v"},
            "groupBy": ["?g"],
        }),
        &["g", "lo", "hi"],
    )
}

fn count_distinct_case(src: &str, c: &str) -> Case {
    Case::new(
        format!("{c}: COUNT(DISTINCT)"),
        format!("SELECT (COUNT(DISTINCT ?v) AS ?n) FROM <{src}> WHERE {{ ?s ex:{c} ?v }}"),
        json!({
            "select": ["(as (count-distinct ?v) ?n)"],
            "where": {"@id": "?s", format!("ex:{c}"): "?v"},
        }),
        &["n"],
    )
}

fn distinct_case(src: &str, c: &str) -> Case {
    Case::new(
        format!("{c}: SELECT DISTINCT"),
        format!("SELECT DISTINCT ?v FROM <{src}> WHERE {{ ?s ex:{c} ?v }}"),
        json!({
            "selectDistinct": ["?v"],
            "where": {"@id": "?s", format!("ex:{c}"): "?v"},
        }),
        &["v"],
    )
}

/// `ORDER BY` the column with a `LIMIT`: the rows a top-k keeps.
fn order_case(src: &str, c: &str, desc: bool, k: usize) -> Case {
    let (sparql_ord, jsonld_ord) = direction(desc);
    Case::new(
        format!("{c}: ORDER BY {sparql_ord} LIMIT {k}"),
        format!("SELECT ?v FROM <{src}> WHERE {{ ?s ex:{c} ?v }} ORDER BY {sparql_ord} LIMIT {k}"),
        json!({
            "select": ["?v"],
            "where": {"@id": "?s", format!("ex:{c}"): "?v"},
            "orderBy": [jsonld_ord],
            "limit": k,
        }),
        &["v"],
    )
    .ordered()
}

/// `FILTER(?v <op> <lit>)`, `lit` written as SPARQL (`"5"`, `10`) and as a
/// JSON-LD filter argument.
fn filter_case(src: &str, c: &str, op: &str, lit: &str) -> Case {
    Case::new(
        format!("{c}: FILTER(?v {op} {lit})"),
        format!("SELECT ?s ?v FROM <{src}> WHERE {{ ?s ex:{c} ?v FILTER(?v {op} {lit}) }}"),
        json!({
            "select": ["?s", "?v"],
            "where": [
                {"@id": "?s", format!("ex:{c}"): "?v"},
                ["filter", format!("({op} ?v {lit})")]
            ],
        }),
        &["s", "v"],
    )
}

/// A constant string object: `?s ex:<c> "<lit>"`.
fn constant_case(src: &str, c: &str, lit: &str) -> Case {
    Case::new(
        format!("{c}: constant object \"{lit}\""),
        format!("SELECT ?s FROM <{src}> WHERE {{ ?s ex:{c} \"{lit}\" }}"),
        json!({
            "select": ["?s"],
            "where": {"@id": "?s", format!("ex:{c}"): lit},
        }),
        &["s"],
    )
}

fn cell(ids: &[u32]) -> Vec<String> {
    ids.iter()
        .map(|i| format!("s=http://example.org/cell/{i}"))
        .collect()
}

fn cell_v(rows: &[(u32, &str)]) -> Vec<String> {
    rows.iter()
        .map(|(i, v)| format!("s=http://example.org/cell/{i} v={v}"))
        .collect()
}

fn refs(rows: &[(u32, u32)]) -> Vec<String> {
    rows.iter()
        .map(|(r, s)| format!("r=http://example.org/ref/{r} s=http://example.org/cell/{s}"))
        .collect()
}

fn strs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

// ---------------------------------------------------------------------------
// Iceberg: the fused aggregate and the scan's pushdowns
// ---------------------------------------------------------------------------

fn iceberg_cases() -> Vec<Case> {
    use Routing::{MustFire, MustNotFire};
    let src = "cells:main";
    let mut cases = Vec::new();
    for c in COLUMNS {
        cases.push(group_case(src, c).route(FUSED, MustFire));
        cases.push(composite_case(src, c).route(FUSED, MustFire));
        cases.push(dim_group_case(src, c).route(FUSED, MustFire));
        cases.push(group_order_case(src, c, false).route(FUSED, MustFire));
        cases.push(group_order_case(src, c, true).route(FUSED, MustFire));
        // The fold declines a MIN/MAX over a string and a DISTINCT aggregate.
        cases.push(minmax_case(src, c).route(FUSED, MustNotFire));
        cases.push(grouped_minmax_case(src, c).route(FUSED, MustNotFire));
        cases.push(count_distinct_case(src, c).route(FUSED, MustNotFire));
    }
    // The generic answers these lanes must reach, where the string differs
    // from the value: "10" before "9", -0 apart from 0, "NaN" a string.
    let pinned = [
        (
            "i32: GROUP BY with COUNT",
            vec!["n=1 v=-3", "n=1 v=100", "n=1 v=2", "n=2 v=10", "n=2 v=9"],
        ),
        (
            "i32: GROUP BY ORDER BY ?v LIMIT 2",
            vec!["n=1 v=-3", "n=2 v=10"],
        ),
        (
            "i32: GROUP BY ORDER BY DESC(?v) LIMIT 2",
            vec!["n=2 v=9", "n=1 v=2"],
        ),
        ("i32: MIN and MAX", vec!["hi=9 lo=-3"]),
        (
            "i32: MIN and MAX per group",
            vec!["g=x hi=2 lo=-3", "g=y hi=9 lo=100"],
        ),
        (
            "f64: GROUP BY with COUNT",
            vec![
                "n=1 v=-0",
                "n=1 v=0",
                "n=1 v=0.1",
                "n=1 v=1.5",
                "n=1 v=10",
                "n=1 v=1000000000000000000000",
                "n=1 v=NaN",
            ],
        ),
        ("f64: COUNT(DISTINCT)", vec!["n=7"]),
        (
            "dec: GROUP BY with COUNT",
            vec![
                "n=1 v=1.500000000",
                "n=1 v=10.000000000",
                "n=1 v=2.000000000",
                "n=2 v=-0.500000000",
                "n=2 v=0.050000000",
            ],
        ),
        (
            "ts: GROUP BY ORDER BY DESC(?v) LIMIT 2",
            vec![
                "n=2 v=2024-01-01T00:00:00Z",
                "n=1 v=2024-01-01T00:00:00.500000Z",
            ],
        ),
        (
            "raw: GROUP BY with COUNT",
            vec!["n=1 v=", "n=1 v=0a", "n=2 v=ff", "n=3 v=0001"],
        ),
    ];
    for (name, rows) in pinned {
        let c = cases.iter_mut().find(|c| c.name == name).expect(name);
        c.expect = Some(rows.iter().map(ToString::to_string).collect());
    }

    // The scan's file-level pushdowns, which run with fast paths off as well.
    // The second data file holds only two- and three-digit integers, so a
    // prune by the column's numeric bounds loses rows the string comparison
    // keeps; a string column, and an equality with an integer's own rendering,
    // still prune.
    cases.push(
        order_case(src, "i32", true, 1)
            .expect(&["v=9"])
            .files_read(2),
    );
    cases.push(
        order_case(src, "label", true, 1)
            .expect(&["v=\u{e9}"])
            .files_read(1),
    );
    cases.push(
        filter_case(src, "i32", "<", "\"5\"")
            .expect(&strs(&cell_v(&[
                (1, "2"),
                (3, "-3"),
                (5, "10"),
                (6, "100"),
                (7, "10"),
            ])))
            .files_read(2),
    );
    cases.push(
        filter_case(src, "i32", ">", "\"50\"")
            .expect(&strs(&cell_v(&[(2, "9"), (4, "9")])))
            .files_read(2),
    );
    cases.push(
        filter_case(src, "label", ">", "\"y\"")
            .expect(&strs(&cell_v(&[(6, "\u{e9}"), (8, "z")])))
            .files_read(1),
    );
    // A string never equals an integer, so `!=` keeps every row.
    cases.push(filter_case(src, "i32", "!=", "10").expect(&strs(&cell_v(&[
        (1, "2"),
        (2, "9"),
        (3, "-3"),
        (4, "9"),
        (5, "10"),
        (6, "100"),
        (7, "10"),
    ]))));
    let mut in_case = filter_case(src, "i32", "IN", "(\"100\", \"10\")");
    in_case.jsonld["where"][1] = json!(["filter", "(in ?v [\"100\" \"10\"])"]);
    cases.push(
        in_case
            .expect(&strs(&cell_v(&[(5, "10"), (6, "100"), (7, "10")])))
            .files_read(1),
    );
    cases.push(
        constant_case(src, "i32", "100")
            .expect(&strs(&cell(&[6])))
            .files_read(1),
    );
    cases.push(
        constant_case(src, "day", "1969-12-31")
            .expect(&strs(&cell(&[3])))
            .files_read(1),
    );
    // A numeric datatype over a string column: the terms order by number and
    // the file bounds by string ("9" above "100"), so the top-k must not prune
    // by them. Over a long column it still does.
    cases.push(
        order_case(src, "ntextN", true, 1)
            .expect(&["v=100"])
            .files_read(2),
    );
    cases.push(
        order_case(src, "i64N", true, 1)
            .expect(&["v=9007199254740993"])
            .files_read(1),
    );
    // Equality with a cell's own rendering prunes by the cell's value, for
    // every type with an exact round trip. NaN equals nothing by value and -0
    // equals 0, so those two do not push.
    for (c, lit, ids, files) in [
        ("dec", "10.000000000", &[4][..], 1),
        ("ts", "1969-12-31T23:59:59.999999Z", &[3], 1),
        ("tstz", "1969-12-31T23:59:59.999999Z", &[3], 1),
        ("f64", "1000000000000000000000", &[7], 1),
        ("f32", "2.5", &[6], 1),
        ("flag", "false", &[2, 5, 7], 2),
        ("f64", "NaN", &[6], 2),
        ("f64", "-0", &[3], 2),
    ] {
        cases.push(
            constant_case(src, c, lit)
                .expect(&strs(&cell(ids)))
                .files_read(files),
        );
    }
    cases
}

#[tokio::test(flavor = "current_thread")]
async fn untyped_columns_agree_on_every_iceberg_lane() {
    let _lock = KILL_SWITCH.lock().await;
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "unset FLUREE_DISABLE_QUERY_FAST_PATHS: this pins nothing otherwise"
    );
    let fixtures = format!("{}/tests/fixtures/iceberg", env!("CARGO_MANIFEST_DIR"));
    // SAFETY: set before any storage or scan is built; nothing else in this
    // binary reads the variable.
    std::env::set_var("FLUREE_ICEBERG_LOCAL_ROOTS", &fixtures);
    let location = format!("file://{fixtures}/silver/cells");
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_r2rml_graph_source(
            R2rmlCreateConfig::new_direct(
                "cells",
                &location,
                cells_mapping("silver.cells", TYPED_POMS),
            )
            .with_mapping_media_type("text/turtle"),
        )
        .await
        .expect("create the cells graph source");
    let failures = check(&fluree, &iceberg_cases(), "cells:main").await;
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}

// ---------------------------------------------------------------------------
// SQL: the block and aggregate lanes
// ---------------------------------------------------------------------------

/// The columns whose rendering the database's `=` matches: the aggregate lane
/// may group them in the statement. A float renders -0.0 and 0.0 apart where
/// the database groups them. (The `timestamp(6)` columns qualify; the
/// `timestamp(9)` one, mapped separately, does not.)
const SQL_EQ_EXACT: &[&str] = &[
    "i32", "i64", "dec", "flag", "day", "ts", "tstz", "raw", "label",
];

/// What each `lab.cells` row (ids 1 to 8) renders as, per column.
fn sql_rendered(c: &str) -> [Option<&'static str>; 8] {
    let ts = [
        Some("2024-01-01T00:00:00Z"),
        Some("2024-01-01T00:00:00.000001Z"),
        Some("1969-12-31T23:59:59.999999Z"),
        Some("2024-01-01T00:00:00Z"),
        Some("2000-02-29T12:30:00Z"),
        Some("1970-01-01T00:00:00Z"),
        Some("2024-01-01T00:00:00.500000Z"),
        None,
    ];
    match c {
        "i32" => [
            Some("2"),
            Some("9"),
            Some("-3"),
            Some("9"),
            Some("10"),
            Some("100"),
            Some("10"),
            None,
        ],
        "i64" => [
            Some("2"),
            Some("9"),
            Some("-3"),
            Some("9"),
            Some("10"),
            Some("9007199254740993"),
            Some("10"),
            None,
        ],
        "f32" => [
            Some("1.5"),
            Some("0.1"),
            Some("-0"),
            Some("0"),
            Some("10"),
            Some("2.5"),
            Some("1.5"),
            None,
        ],
        "f64" => [
            Some("1.5"),
            Some("0.1"),
            Some("-0"),
            Some("0"),
            Some("10"),
            Some("2.5"),
            Some("1000000000000000000000"),
            None,
        ],
        "dec" => [
            Some("0.050000000"),
            Some("1.500000000"),
            Some("-0.500000000"),
            Some("10.000000000"),
            Some("0.050000000"),
            Some("2.000000000"),
            Some("-0.500000000"),
            None,
        ],
        "flag" => [
            Some("true"),
            Some("false"),
            Some("true"),
            Some("true"),
            Some("false"),
            Some("true"),
            Some("false"),
            None,
        ],
        "day" => [
            Some("2024-01-09"),
            Some("2024-01-10"),
            Some("1969-12-31"),
            Some("2024-01-09"),
            Some("2000-02-29"),
            Some("1970-01-01"),
            Some("2024-01-10"),
            None,
        ],
        "ts" | "tstz" => ts,
        "raw" => [
            Some("0001"),
            Some("ff"),
            Some(""),
            Some("0001"),
            Some("0a"),
            Some("ff"),
            Some("0001"),
            None,
        ],
        "label" => [
            Some("b"),
            Some("a"),
            Some("c"),
            Some("b"),
            Some("B"),
            Some("\u{e9}"),
            Some("a"),
            Some("z"),
        ],
        other => panic!("no renderings for {other}"),
    }
}

/// The `(id, rendering)` rows of column `c` that `keep` holds for.
fn sql_rows_where(c: &str, keep: impl Fn(&str) -> bool) -> Vec<String> {
    let rows: Vec<(u32, &str)> = sql_rendered(c)
        .iter()
        .zip(1..)
        .filter_map(|(v, id)| v.filter(|v| keep(v)).map(|v| (id, v)))
        .collect();
    cell_v(&rows)
}

fn sql_cases() -> Vec<Case> {
    use Routing::{MustFire, MustNotFire};
    let src = "cells-sql:main";
    let mut cases = Vec::new();
    for c in COLUMNS {
        // Grouping goes to the statement where the database groups as the
        // strings do; otherwise the fused fold takes it, from the rows.
        let grouped = |case: Case| {
            if SQL_EQ_EXACT.contains(c) {
                case.route(SQL_AGG, MustFire)
            } else {
                case.route(SQL_AGG, MustNotFire).route(FUSED, MustFire)
            }
        };
        cases.push(grouped(group_case(src, c)));
        cases.push(grouped(composite_case(src, c)));
        cases.push(grouped(group_order_case(src, c, false)));
        cases.push(grouped(group_order_case(src, c, true)));
        // The database orders a string column by code point (Trino); every
        // other column it orders by value, where the term orders by string.
        let extreme = if *c == "label" { MustFire } else { MustNotFire };
        cases.push(minmax_case(src, c).route(SQL_AGG, extreme));
        cases.push(grouped_minmax_case(src, c).route(SQL_AGG, extreme));
        let distinct = if SQL_EQ_EXACT.contains(c) {
            MustFire
        } else {
            MustNotFire
        };
        cases.push(count_distinct_case(src, c).route(SQL_AGG, distinct));
        // The block lane fetches the rows either way; what it may not push is
        // an ORDER BY … LIMIT, a DISTINCT, or a comparison the database makes
        // differently. The answers show whether it did.
        cases.push(distinct_case(src, c).route(SQL_BLOCK, MustFire));
        cases.push(order_case(src, c, false, 2).route(SQL_BLOCK, MustFire));
        cases.push(order_case(src, c, true, 1).route(SQL_BLOCK, MustFire));
        // A string compares as a string (by code point), and never equals 10.
        cases.push(
            filter_case(src, c, "<", "\"5\"")
                .route(SQL_BLOCK, MustFire)
                .expect(&strs(&sql_rows_where(c, |v| v < "5"))),
        );
        cases.push(
            filter_case(src, c, "!=", "10")
                .route(SQL_BLOCK, MustFire)
                .expect(&strs(&sql_rows_where(c, |_| true))),
        );
    }
    // A `timestamp(6)` groups in the statement (above); a `timestamp(9)` keeps
    // instants a nanosecond apart that decode, and render, as one.
    cases.push(
        group_case(src, "ts9")
            .route(SQL_AGG, MustNotFire)
            .route(FUSED, MustFire)
            .expect(&["n=7 v=2024-01-01T00:00:00Z"]),
    );
    cases.push(
        count_distinct_case(src, "ts9")
            .route(SQL_AGG, MustNotFire)
            .expect(&["n=1"]),
    );
    // Language-tagged: a double's "-0"@en and "0"@en stay apart, so the
    // statement may not group them; a long's rendering it may.
    cases.push(
        group_case(src, "f64en")
            .route(SQL_AGG, MustNotFire)
            .expect(&[
                "n=1 v=-0",
                "n=1 v=0",
                "n=1 v=0.1",
                "n=1 v=1.5",
                "n=1 v=10",
                "n=1 v=1000000000000000000000",
                "n=1 v=2.5",
            ]),
    );
    cases.push(group_case(src, "i64en").route(SQL_AGG, MustFire).expect(&[
        "n=1 v=-3",
        "n=1 v=2",
        "n=1 v=9007199254740993",
        "n=2 v=10",
        "n=2 v=9",
    ]));
    // A numeric datatype over a text column: the database orders the text,
    // the engine the numbers. Over a bigint the extremes still go to the
    // statement. (The fake orders numeric-looking text as numbers, so only the
    // routing stamp tells the two apart here; `orders_as` is unit-tested.)
    cases.push(
        minmax_case(src, "ntextN")
            .route(SQL_AGG, MustNotFire)
            .expect(&["hi=100 lo=-3"]),
    );
    cases.push(
        minmax_case(src, "i64N")
            .route(SQL_AGG, MustFire)
            .expect(&["hi=9007199254740993 lo=-3"]),
    );
    cases.push(
        order_case(src, "ntextN", true, 1)
            .route(SQL_BLOCK, MustFire)
            .expect(&["v=100"]),
    );
    let pinned = [
        ("i32: MIN and MAX", vec!["hi=9 lo=-3"]),
        (
            "i32: GROUP BY ORDER BY DESC(?v) LIMIT 2",
            vec!["n=2 v=9", "n=1 v=2"],
        ),
        ("i32: ORDER BY DESC(?v) LIMIT 1", vec!["v=9"]),
        ("i64: ORDER BY ?v LIMIT 2", vec!["v=-3", "v=10"]),
        (
            "f64: GROUP BY with COUNT",
            vec![
                "n=1 v=-0",
                "n=1 v=0",
                "n=1 v=0.1",
                "n=1 v=1.5",
                "n=1 v=10",
                "n=1 v=1000000000000000000000",
                "n=1 v=2.5",
            ],
        ),
        ("f64: COUNT(DISTINCT)", vec!["n=7"]),
        ("dec: MIN and MAX", vec!["hi=2.000000000 lo=-0.500000000"]),
        (
            "ts: ORDER BY DESC(?v) LIMIT 1",
            vec!["v=2024-01-01T00:00:00Z"],
        ),
    ];
    for (name, rows) in pinned {
        let c = cases.iter_mut().find(|c| c.name == name).expect(name);
        c.expect = Some(rows.iter().map(ToString::to_string).collect());
    }
    // A join on a literal value: the database's `=` on the two columns must be
    // the strings' equality. Integers of one type are; -0.0 = 0.0 and
    // 10 = 10.00 are not.
    for (a, b, routing, expect) in [
        (
            "i64",
            "refN",
            MustFire,
            refs(&[(1, 5), (1, 7), (2, 2), (2, 4), (3, 1)]),
        ),
        ("f64", "refF", MustNotFire, refs(&[(1, 3), (2, 4), (3, 1)])),
        ("dec", "refD", MustNotFire, vec![]),
        ("i64", "refD", MustNotFire, vec![]),
    ] {
        cases.push(
            Case::new(
                format!("join by value: {a} = {b}"),
                format!("SELECT ?s ?r FROM <{src}> WHERE {{ ?s ex:{a} ?v . ?r ex:{b} ?v }}"),
                json!({
                    "select": ["?s", "?r"],
                    "where": [
                        {"@id": "?s", format!("ex:{a}"): "?v"},
                        {"@id": "?r", format!("ex:{b}"): "?v"}
                    ],
                }),
                &["s", "r"],
            )
            .route(SQL_BLOCK, routing)
            .expect(&strs(&expect)),
        );
    }
    cases
}

#[tokio::test(flavor = "current_thread")]
async fn untyped_columns_agree_on_every_sql_lane() {
    let _lock = KILL_SWITCH.lock().await;
    assert!(
        std::env::var_os("FLUREE_DISABLE_QUERY_FAST_PATHS").is_none(),
        "unset FLUREE_DISABLE_QUERY_FAST_PATHS: this pins nothing otherwise"
    );
    let server = sql_tables().mount().await;
    let fluree = FlureeBuilder::memory().build_memory();
    let mapping = format!(
        "{}{REFS_MAPPING}",
        cells_mapping("lab.cells", &format!("{TYPED_POMS}{SQL_POMS}"))
    );
    fluree
        .create_sql_graph_source(SqlCreateConfig::new("cells-sql", server.uri(), mapping))
        .await
        .expect("create the cells SQL graph source");
    let failures = check(&fluree, &sql_cases(), "cells-sql:main").await;
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}
