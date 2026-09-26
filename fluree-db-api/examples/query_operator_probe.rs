//! Repeatable operator timings on an explicitly indexed people graph.
//!
//! `cargo run -p fluree-db-api --profile dev-fast --example query_operator_probe -- 50000 5`
//! Arguments: people, measured repetitions. Set `PROBE_PLANS=1` to print plans.
//! `PROBE_ONLY=optional_not_exists` selects a case. `PROBE_VERIFY=1` checks the
//! OPTIONAL case against seeded evaluation and the distinct aggregates against
//! counts computed from raw query rows, outside the timed region.
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
    ("group_count_distinct", "SELECT ?city (COUNT(DISTINCT ?fof) AS ?n) WHERE { ?p ex:livesIn ?city ; ex:knows ?f . ?f ex:knows ?fof } GROUP BY ?city"),
    ("not_exists_count", "SELECT (COUNT(*) AS ?n) WHERE { ?p a ex:Person FILTER NOT EXISTS { ?p ex:worksFor ?org } }"),
    ("minus_count", "SELECT (COUNT(*) AS ?n) WHERE { ?p a ex:Person MINUS { ?p ex:worksFor ?org } }"),
    ("chain_distinct_limit", "SELECT DISTINCT ?p ?fof WHERE { ?p ex:knows ?f . ?f ex:knows ?fof } LIMIT 1"),
    ("chain_limit", "SELECT ?p ?fof WHERE { ?p ex:knows ?f . ?f ex:knows ?fof } LIMIT 1"),
    ("star_limit", "SELECT ?p ?n WHERE { ?p a ex:Person ; ex:name ?n ; ex:age ?a } LIMIT 10"),
    ("construct_transform", "CONSTRUCT { ?org ex:employs ?p . ?p ex:label ?name } WHERE { ?p ex:worksFor ?org ; ex:name ?name }"),
];

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
        if matches!(*name, "two_hop_aggregate" | "group_count_distinct")
            && std::env::var_os("PROBE_VERIFY").is_some()
        {
            // Fold the ordinary bag of joined rows independently of the
            // aggregate planner. Verify every group, before ORDER BY/LIMIT.
            let full_query = query.split(" ORDER BY ").next().unwrap();
            let (_, suffix) = full_query.split_once(" WHERE ").unwrap();
            let (body, _) = suffix.rsplit_once(" GROUP BY ").unwrap();
            let raw_query =
                format!("PREFIX ex: <http://example.org/> SELECT ?city ?fof WHERE {body}");
            let raw = fluree
                .query(&view, QueryInput::Sparql(&raw_query))
                .await
                .expect("raw rows");
            let raw = raw.to_jsonld(&view.snapshot).expect("format raw rows");
            let mut groups = BTreeMap::new();
            for row in raw.as_array().expect("row array") {
                let (_, values) = groups
                    .entry(row[0].to_string())
                    .or_insert_with(|| (row[0].clone(), BTreeSet::new()));
                values.insert(row[1].to_string());
            }
            let mut control_rows: Vec<_> = groups
                .into_values()
                .map(|(city, values)| serde_json::json!([city, values.len()]))
                .collect();
            let result = fluree
                .query(&view, QueryInput::Sparql(full_query))
                .await
                .expect("all groups");
            let mut actual_rows = result
                .to_jsonld(&view.snapshot)
                .expect("format groups")
                .as_array()
                .expect("group rows")
                .clone();
            control_rows.sort_by_key(ToString::to_string);
            actual_rows.sort_by_key(ToString::to_string);
            assert_eq!(
                actual_rows, control_rows,
                "distinct counts differ from raw rows"
            );
            println!(
                "{name}: {} groups agree with counts from raw rows",
                actual_rows.len()
            );
        }
        if *name == "optional_not_exists" && std::env::var_os("PROBE_VERIFY").is_some() {
            let control = query
                .replace("FILTER NOT EXISTS", "FILTER (false || NOT EXISTS")
                .replace("?x ex:worksFor ?org } }", "?x ex:worksFor ?org }) }");
            let result = fluree
                .query(&view, QueryInput::Sparql(&control))
                .await
                .expect("seeded control");
            let mut control_rows = result
                .to_jsonld(&view.snapshot)
                .expect("format control")
                .as_array()
                .expect("control rows")
                .clone();
            let mut actual_rows = expected
                .expect("warmup result")
                .as_array()
                .expect("result rows")
                .clone();
            control_rows.sort_by_key(ToString::to_string);
            actual_rows.sort_by_key(ToString::to_string);
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
