//! Repeatable operator timings on an explicitly indexed people graph.
//!
//! `cargo run -p fluree-db-api --profile dev-fast --example query_operator_probe -- 50000 5`
//! Arguments: people, measured repetitions. Set `PROBE_PLANS=1` to print plans.
//! `PROBE_ONLY=optional_not_exists` selects a case. `PROBE_VERIFY=1` checks the
//! OPTIONAL case against seeded evaluation and aggregate counts against raw
//! query rows, outside the timed region.
//! Cases ending in `_drain` cannot reach their LIMIT on the default fixture.
//! `PROBE_DRAIN_BASELINE=1` removes that LIMIT to compare throughput planning.
//! `PROBE_GROUP_BASELINE=1` uses COUNT(?fof) for grouped COUNT(*) cases: ?fof is
//! always bound, so results agree while consumption stays on the row path.
//! `FLUREE_HASH_JOIN=1` forces eligible hash joins; inspect `PROBE_PLANS` as well.
//! Measures query execution (without response serialization), with one warmup.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::time::Instant;

use fluree_db_api::{FlureeBuilder, IndexConfig, QueryInput, ReindexOptions};
use rand::{rngs::StdRng, Rng, SeedableRng};

const QUERIES: &[(&str, &str)] = &[
    ("filter_rows", "SELECT ?p ?name WHERE { ?p a ex:Person ; ex:name ?name ; ex:age ?age FILTER(?age > 60) }"),
    ("two_hop_aggregate", "SELECT ?city (COUNT(DISTINCT ?fof) AS ?n) WHERE { ?p ex:livesIn ?city ; ex:knows ?f . ?f ex:knows ?fof FILTER(?fof != ?p) } GROUP BY ?city ORDER BY DESC(?n) LIMIT 10"),
    ("optional_not_exists", "SELECT ?p ?org WHERE { ?p a ex:Person OPTIONAL { ?p ex:worksFor ?org } FILTER NOT EXISTS { ?p ex:knows ?x . ?x ex:worksFor ?org } }"),
    ("type_filter_count", "SELECT (COUNT(*) AS ?n) WHERE { ?p a ex:Person ; ex:age ?age FILTER(?age > 60) }"),
    ("chain_filter_count", "SELECT (COUNT(*) AS ?n) WHERE { ?p ex:livesIn ?city ; ex:knows ?f . ?f ex:knows ?fof FILTER(?fof != ?p) }"),
    ("group_count", "SELECT ?city (COUNT(*) AS ?n) WHERE { ?p ex:livesIn ?city ; ex:knows ?f . ?f ex:knows ?fof } GROUP BY ?city"),
    ("group_filter_count", "SELECT ?city (COUNT(*) AS ?n) WHERE { ?p ex:livesIn ?city ; ex:knows ?f . ?f ex:knows ?fof FILTER(?fof != ?p) } GROUP BY ?city"),
    ("group_person_count", "SELECT ?p (COUNT(*) AS ?n) WHERE { ?p ex:livesIn ?city ; ex:knows ?f . ?f ex:knows ?fof } GROUP BY ?p"),
    ("group_count_distinct", "SELECT ?city (COUNT(DISTINCT ?fof) AS ?n) WHERE { ?p ex:livesIn ?city ; ex:knows ?f . ?f ex:knows ?fof } GROUP BY ?city"),
    ("not_exists_count", "SELECT (COUNT(*) AS ?n) WHERE { ?p a ex:Person FILTER NOT EXISTS { ?p ex:worksFor ?org } }"),
    ("minus_count", "SELECT (COUNT(*) AS ?n) WHERE { ?p a ex:Person MINUS { ?p ex:worksFor ?org } }"),
    ("chain_distinct_limit", "SELECT DISTINCT ?p ?fof WHERE { ?p ex:knows ?f . ?f ex:knows ?fof } LIMIT 1"),
    ("chain_limit", "SELECT ?p ?fof WHERE { ?p ex:knows ?f . ?f ex:knows ?fof } LIMIT 1"),
    ("chain_distinct_drain", "SELECT DISTINCT ?city WHERE { ?p ex:knows ?f . ?f ex:livesIn ?city } LIMIT 1000"),
    ("long_chain_distinct_drain", "SELECT DISTINCT ?city WHERE { ?p ex:knows ?f . ?f ex:knows ?fof . ?fof ex:livesIn ?city } LIMIT 1000"),
    ("chain_sparse_drain", "SELECT ?p ?fof WHERE { ?p ex:knows ?f . ?f ex:knows ?fof FILTER(CONTAINS(LCASE(CONCAT(STR(?p), STR(?fof))), \"/absent/\")) } LIMIT 100"),
    ("long_chain_sparse_drain", "SELECT ?p ?end WHERE { ?p ex:knows ?f . ?f ex:knows ?fof . ?fof ex:knows ?end FILTER(CONTAINS(LCASE(CONCAT(STR(?p), STR(?end))), \"/absent/\")) } LIMIT 100"),
    ("star_limit", "SELECT ?p ?n WHERE { ?p a ex:Person ; ex:name ?n ; ex:age ?a } LIMIT 10"),
    ("star_sparse_limit", "SELECT ?p ?n WHERE { ?p a ex:Person ; ex:name ?n ; ex:age ?a FILTER(STRENDS(?n, \"999\")) } LIMIT 10"),
    ("construct_transform", "CONSTRUCT { ?org ex:employs ?p . ?p ex:label ?name } WHERE { ?p ex:worksFor ?org ; ex:name ?name }"),
];

fn sorted_rows(value: &serde_json::Value) -> Vec<serde_json::Value> {
    let mut rows = value.as_array().expect("row array").clone();
    rows.sort_by_key(ToString::to_string);
    rows
}

fn replace_once(query: &str, from: &str, to: &str) -> String {
    assert_eq!(
        query.matches(from).count(),
        1,
        "reference rewrite must match once: {from}"
    );
    query.replacen(from, to, 1)
}

fn people_graph(people: usize) -> String {
    let mut rng = StdRng::seed_from_u64(42);
    let mut ttl = String::from("@prefix ex: <http://example.org/> .\n");
    let cities = (people / 100).max(1);
    let orgs = (people / 20).max(1);
    for i in 0..cities {
        writeln!(ttl, "ex:city{i} a ex:City .").unwrap();
    }
    for i in 0..orgs {
        writeln!(ttl, "ex:org{i} a ex:Org ; ex:name \"Org {i}\" .").unwrap();
    }
    for i in 0..people {
        let age = rng.gen_range(18..=90);
        let city = rng.gen_range(0..cities);
        write!(ttl, "ex:person{i} a ex:Person ; ex:name \"Person {i}\" ; ex:age {age} ; ex:livesIn ex:city{city}").unwrap();
        if rng.gen_bool(0.8) {
            write!(ttl, " ; ex:worksFor ex:org{}", rng.gen_range(0..orgs)).unwrap();
        }
        for _ in 0..3 {
            write!(ttl, " ; ex:knows ex:person{}", rng.gen_range(0..people)).unwrap();
        }
        ttl.push_str(" .\n");
    }
    ttl
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let people = args.get(1).map_or(50_000, |s| s.parse().expect("people"));
    let reps = args.get(2).map_or(5, |s| s.parse().expect("repetitions"));
    assert!(people > 0 && reps > 0);
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .create_ledger("operator-probe")
        .await
        .expect("create");
    fluree
        .insert_turtle_with_opts(
            ledger,
            &people_graph(people),
            Default::default(),
            Default::default(),
            &IndexConfig {
                reindex_min_bytes: 5_000_000_000,
                reindex_max_bytes: 5_000_000_000,
            },
            None,
        )
        .await
        .expect("insert");
    fluree
        .reindex("operator-probe:main", ReindexOptions::default())
        .await
        .expect("reindex");
    let view = fluree.db("operator-probe:main").await.expect("view");
    println!("people={people}, repetitions={reps}, indexed, execution only");
    for (name, query) in QUERIES {
        if std::env::var("PROBE_ONLY").is_ok_and(|selected| selected != *name) {
            continue;
        }
        let query = format!("PREFIX ex: <http://example.org/>\n{query}");
        let query = if std::env::var_os("PROBE_GROUP_BASELINE").is_some() {
            assert!(
                matches!(
                    *name,
                    "group_count" | "group_filter_count" | "group_person_count"
                ),
                "PROBE_GROUP_BASELINE requires a grouped COUNT(*) case"
            );
            replace_once(&query, "COUNT(*)", "COUNT(?fof)")
        } else {
            query
        };
        let full_drain = name.ends_with("_drain");
        let drain_baseline = std::env::var_os("PROBE_DRAIN_BASELINE").is_some();
        if drain_baseline {
            assert!(
                full_drain,
                "PROBE_DRAIN_BASELINE requires PROBE_ONLY selecting a _drain case"
            );
        }
        let query = if drain_baseline {
            query
                .rsplit_once(" LIMIT ")
                .expect("drain limit")
                .0
                .to_string()
        } else {
            query
        };
        if std::env::var_os("PROBE_PLANS").is_some() {
            let plan = fluree.explain_sparql(&view, &query).await.expect("explain");
            println!("{name}: {plan}");
        }
        let mut times = Vec::with_capacity(reps);
        let mut expected = None;
        for iteration in 0..=reps {
            let start = Instant::now();
            let result = fluree
                .query(&view, QueryInput::Sparql(&query))
                .await
                .expect(name);
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            let json = result.to_jsonld(&view.snapshot).expect("format");
            if let Some(expected) = &expected {
                assert_eq!(&json, expected, "unstable results for {name}");
            } else {
                expected = Some(json);
            }
            if iteration > 0 {
                times.push(elapsed);
            }
        }
        times.sort_by(f64::total_cmp);
        let median = times[(reps - 1) / 2].midpoint(times[reps / 2]);
        println!("{name:24} {median:10.3} ms");
        if matches!(*name, "star_limit" | "star_sparse_limit")
            && std::env::var_os("PROBE_VERIFY").is_some()
        {
            let full_query = replace_once(&query, " LIMIT 10", "");
            let full = fluree
                .query(&view, QueryInput::Sparql(&full_query))
                .await
                .expect("full star");
            let full = full.to_jsonld(&view.snapshot).expect("format full star");
            let prefix: Vec<_> = full
                .as_array()
                .expect("star rows")
                .iter()
                .take(10)
                .cloned()
                .collect();
            assert_eq!(expected.as_ref().unwrap(), &serde_json::json!(prefix));
            println!("{name}: prefix agrees with the full drain");
        }
        if full_drain && std::env::var_os("PROBE_VERIFY").is_some() {
            let limit = if name.contains("distinct") { 1000 } else { 100 };
            let actual = sorted_rows(expected.as_ref().expect("warmup result"));
            assert!(
                actual.len() < limit,
                "fixture must exhaust before reaching LIMIT"
            );
            let full_query = query
                .rsplit_once(" LIMIT ")
                .map_or(query.as_str(), |(q, _)| q);
            let full = fluree
                .query(&view, QueryInput::Sparql(full_query))
                .await
                .expect("full drain");
            assert_eq!(
                actual,
                sorted_rows(&full.to_jsonld(&view.snapshot).expect("format full drain"))
            );
            println!(
                "{name}: {} rows agree with the unlimited query",
                actual.len()
            );
        }
        if matches!(*name, "chain_filter_count" | "minus_count")
            && std::env::var_os("PROBE_VERIFY").is_some()
        {
            let projection = if *name == "minus_count" {
                "SELECT ?p"
            } else {
                "SELECT ?p ?city ?f ?fof"
            };
            let raw_query = replace_once(&query, "SELECT (COUNT(*) AS ?n)", projection);
            let raw = fluree
                .query(&view, QueryInput::Sparql(&raw_query))
                .await
                .expect("raw count rows");
            let rows: usize = raw.batches.iter().map(fluree_db_api::Batch::len).sum();
            assert_eq!(
                expected.as_ref().unwrap(),
                &serde_json::json!([[rows]]),
                "join count differs from raw rows"
            );
            println!("{name}: {rows} rows agree with ordinary row execution");
            if *name == "minus_count" {
                // The mandatory triple binds ?p, so this fixture's MINUS and
                // NOT EXISTS have identical semantics and independent matching.
                let control = replace_once(&query, "MINUS", "FILTER NOT EXISTS");
                let result = fluree
                    .query(&view, QueryInput::Sparql(&control))
                    .await
                    .expect("existence control");
                assert_eq!(
                    expected.as_ref().unwrap(),
                    &result.to_jsonld(&view.snapshot).unwrap()
                );
                println!("{name}: count agrees with the existence control");
            }
        }
        if matches!(
            *name,
            "two_hop_aggregate"
                | "group_count_distinct"
                | "group_count"
                | "group_filter_count"
                | "group_person_count"
        ) && std::env::var_os("PROBE_VERIFY").is_some()
        {
            // Fold the ordinary bag of joined rows independently of the
            // aggregate planner. Verify every group, before ORDER BY/LIMIT.
            let full_query = query.split(" ORDER BY ").next().unwrap();
            let (_, suffix) = full_query.split_once(" WHERE ").unwrap();
            let (body, _) = suffix.rsplit_once(" GROUP BY ").unwrap();
            let key = if *name == "group_person_count" {
                "?p"
            } else {
                "?city"
            };
            let raw_query =
                format!("PREFIX ex: <http://example.org/> SELECT {key} ?fof WHERE {body}");
            let raw = fluree
                .query(&view, QueryInput::Sparql(&raw_query))
                .await
                .expect("raw rows");
            let raw = raw.to_jsonld(&view.snapshot).expect("format raw rows");
            let mut groups = BTreeMap::new();
            let distinct = matches!(*name, "two_hop_aggregate" | "group_count_distinct");
            for row in raw.as_array().expect("row array") {
                let (_, count, values) = groups
                    .entry(row[0].to_string())
                    .or_insert_with(|| (row[0].clone(), 0usize, BTreeSet::new()));
                *count += 1;
                if distinct {
                    values.insert(row[1].to_string());
                }
            }
            let mut control_rows: Vec<_> = groups
                .into_values()
                .map(|(city, count, values)| {
                    serde_json::json!([city, if distinct { values.len() } else { count }])
                })
                .collect();
            let result = fluree
                .query(&view, QueryInput::Sparql(full_query))
                .await
                .expect("all groups");
            let actual_rows =
                sorted_rows(&result.to_jsonld(&view.snapshot).expect("format groups"));
            control_rows.sort_by_key(ToString::to_string);
            assert_eq!(
                actual_rows, control_rows,
                "group counts differ from raw rows"
            );
            println!(
                "{name}: {} groups agree with counts from raw rows",
                actual_rows.len()
            );
        }
        if *name == "optional_not_exists" && std::env::var_os("PROBE_VERIFY").is_some() {
            let control = replace_once(&query, "FILTER NOT EXISTS", "FILTER (false || NOT EXISTS");
            let control = replace_once(
                &control,
                "?x ex:worksFor ?org } }",
                "?x ex:worksFor ?org }) }",
            );
            let result = fluree
                .query(&view, QueryInput::Sparql(&control))
                .await
                .expect("seeded control");
            let control_rows =
                sorted_rows(&result.to_jsonld(&view.snapshot).expect("format control"));
            let actual_rows = sorted_rows(&expected.expect("warmup result"));
            assert_eq!(
                actual_rows, control_rows,
                "partial-key lookup differs from seeded evaluation"
            );
            println!(
                "{name}: {} rows agree with seeded evaluation",
                actual_rows.len()
            );
        }
    }
}
