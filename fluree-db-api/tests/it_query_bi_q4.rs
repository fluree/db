//! BSBM BI Q4: grouped average prices with and without each product feature.
//! Keep the two grouped subqueries, cast, projected ratio, ORDER BY and LIMIT
//! from the benchmark: testing only the feature IDs misses an unbound ratio.
use crate::support::{assert_index_defaults, genesis_ledger, rebuild_and_publish_index};
use fluree_db_api::{Fluree, FlureeBuilder};
use serde_json::{json, Value};

const WITH: &str = r"SELECT ?feature (AVG(xsd:float(xsd:string(?price))) AS ?withFeaturePrice) WHERE {
    ?product a ex:Type ; ex:feature ?feature .
    ?offer ex:product ?product ; ex:price ?price .
} GROUP BY ?feature";
const WITHOUT: &str = r"SELECT ?feature (AVG(xsd:float(xsd:string(?price))) AS ?withoutFeaturePrice) WHERE {
    { SELECT DISTINCT ?feature WHERE { ?p a ex:Type ; ex:feature ?feature . } }
    ?product a ex:Type .
    ?offer ex:product ?product ; ex:price ?price .
    FILTER NOT EXISTS { ?product ex:feature ?feature }
} GROUP BY ?feature";

fn query(without_only: bool) -> String {
    let body = if without_only {
        format!("SELECT ?feature ?withoutFeaturePrice FROM <q4:main> WHERE {{ {{ {WITHOUT} }} }} ORDER BY ?feature")
    } else {
        format!("SELECT ?feature (?withFeaturePrice/?withoutFeaturePrice AS ?priceRatio) FROM <q4:main> WHERE {{ {{ {WITH} }} {{ {WITHOUT} }} }} ORDER BY DESC(?withFeaturePrice/?withoutFeaturePrice) ?feature LIMIT 10")
    };
    format!(
        "PREFIX ex: <http://example.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> {body}"
    )
}

async fn seed(fluree: &Fluree, n: usize, features: usize) {
    seed_with_offers(fluree, n, features, 3).await;
}

async fn seed_with_offers(fluree: &Fluree, n: usize, features: usize, offers: usize) {
    let ledger = genesis_ledger(fluree, "q4:main");
    let mut graph = Vec::new();
    for i in 0..n {
        graph.push(json!({"@id": format!("ex:p{i}"), "@type": "ex:Type", "ex:feature": [
            {"@id":format!("ex:f{}", i % features)}, {"@id":format!("ex:f{}", (i+1) % features)}, {"@id":"ex:common"}
        ]}));
        for j in 0..offers {
            graph.push(json!({"@id": format!("ex:o{i}-{j}"), "ex:product": {"@id":format!("ex:p{i}")}, "ex:price": (i%100 + j + 1)}));
        }
    }
    // A feature with no offers must survive the WITHOUT-only subquery;
    // a feature on every priced product has an empty complement and must not.
    graph
        .push(json!({"@id":"ex:offerless", "@type":"ex:Type", "ex:feature":{"@id":"ex:noOffers"}}));
    // Multiple price values and an invalid cast exercise bag multiplicity and
    // aggregate inputs that evaluate to UNBOUND. A missing price joins no rows.
    graph.push(
        json!({"@id":"ex:multi", "ex:product":{"@id":"ex:p0"}, "ex:price":[17,19,"invalid"]}),
    );
    graph.push(json!({"@id":"ex:missing", "ex:product":{"@id":"ex:p1"}}));
    fluree
        .insert(
            ledger,
            &json!({"@context":{"ex":"http://example.org/"},"@graph":graph}),
        )
        .await
        .unwrap();
}

fn expected(n: usize, features: usize, without_only: bool) -> Vec<(String, f64)> {
    expected_with_offers(n, features, 3, without_only)
}

fn expected_with_offers(
    n: usize,
    features: usize,
    offers: usize,
    without_only: bool,
) -> Vec<(String, f64)> {
    let mut rows = Vec::new();
    for f in 0..=features {
        let mut with = Vec::new();
        let mut without = Vec::new();
        for i in 0..n {
            let target = if f < features && (i % features == f || (i + 1) % features == f) {
                &mut with
            } else {
                &mut without
            };
            target.extend((0..offers).map(|j| (i % 100 + j + 1) as f64));
            if i == 0 {
                target.extend([17.0, 19.0]);
            }
        }
        if without.is_empty() || (!without_only && with.is_empty()) {
            continue;
        }
        let avg_without = without.iter().sum::<f64>() / without.len() as f64;
        let value = if without_only {
            avg_without
        } else {
            (with.iter().sum::<f64>() / with.len() as f64) / avg_without
        };
        let feature = if f == features {
            "noOffers".to_owned()
        } else {
            format!("f{f}")
        };
        rows.push((format!("http://example.org/{feature}"), value));
    }
    if without_only {
        rows.sort_by(|a, b| a.0.cmp(&b.0));
    } else {
        rows.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        rows.truncate(10);
    }
    rows
}

fn assert_rows(result: &Value, expected: &[(String, f64)], value_var: &str) {
    let rows = result["results"]["bindings"].as_array().unwrap();
    assert_eq!(rows.len(), expected.len(), "{result}");
    for (row, (feature, value)) in rows.iter().zip(expected) {
        assert_eq!(row["feature"]["value"], *feature);
        let actual: f64 = row[value_var]["value"]
            .as_str()
            .expect("aggregate/projection must be bound")
            .parse()
            .unwrap();
        assert!(
            (actual - value).abs() <= value.abs().max(1.0) * 1e-12,
            "{feature}: {actual} != {value}"
        );
    }
}

#[tokio::test]
async fn bi_q4_complement_preserves_prices_and_empty_groups() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, 120, 12).await;
    for indexed in [false, true] {
        if indexed {
            rebuild_and_publish_index(&fluree, "q4:main").await;
        }
        for without_only in [false, true] {
            let q = query(without_only);
            // GROUP BY without aggregates enumerates the same distinct feature
            // universe but deliberately does not match the complement rewrite.
            let control = q.replace(
                "SELECT DISTINCT ?feature WHERE { ?p a ex:Type ; ex:feature ?feature . }",
                "SELECT ?feature WHERE { ?p a ex:Type ; ex:feature ?feature . } GROUP BY ?feature",
            );
            let expected = expected(120, 12, without_only);
            // Also cover an arithmetic aggregate input, which uses a
            // separate lowering path from a bare cast.
            let legacy_optional = q.replace(
                "xsd:float(xsd:string(?price))",
                "(xsd:float(xsd:string(?price)) + 0)",
            );
            let reversed = q.replace(
                &format!("{{ {WITH} }} {{ {WITHOUT} }}"),
                &format!("{{ {WITHOUT} }} {{ {WITH} }}"),
            );
            let ledger = fluree.ledger("q4:main").await.unwrap();
            let db = crate::support::graphdb_from_ledger(&ledger);
            for sparql in [q, control, legacy_optional, reversed] {
                let result = fluree
                    .query_from()
                    .sparql(&sparql)
                    .track_all()
                    .execute_tracked()
                    .await
                    .unwrap();
                assert_eq!(result.status, 200);
                assert_rows(
                    &result.result,
                    &expected,
                    if without_only {
                        "withoutFeaturePrice"
                    } else {
                        "priceRatio"
                    },
                );
                let view_query = sparql.replace(" FROM <q4:main>", "");
                let view_result = db
                    .query(&fluree)
                    .sparql(&view_query)
                    .track_all()
                    .execute_tracked()
                    .await
                    .unwrap();
                assert_eq!(view_result.status, 200);
                assert_rows(
                    &view_result.result,
                    &expected,
                    if without_only {
                        "withoutFeaturePrice"
                    } else {
                        "priceRatio"
                    },
                );
            }
        }
    }
}

#[tokio::test]
async fn bi_q4_shared_aggregate_bounds_work() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    seed(&fluree, 1_000, 100).await;
    rebuild_and_publish_index(&fluree, "q4:main").await;
    let ledger = fluree.ledger("q4:main").await.unwrap();
    let db = crate::support::graphdb_from_ledger(&ledger);
    let q = query(false).replace(" FROM <q4:main>", "");
    // Adding zero preserves these numeric inputs but deliberately bypasses
    // sharing, retaining the previous correlated aggregate plan.
    let control = q.replace(
        "xsd:float(xsd:string(?price))",
        "(xsd:float(xsd:string(?price)) + 0)",
    );
    let mut fuel = Vec::new();
    for sparql in [&q, &control] {
        let result = db
            .query(&fluree)
            .sparql(sparql)
            .track_all()
            .execute_tracked()
            .await
            .unwrap();
        assert_eq!(result.status, 200);
        assert_rows(&result.result, &expected(1_000, 100, false), "priceRatio");
        fuel.push(result.fuel.expect("tracked query fuel"));
    }
    // A work-count guard, not a wall-clock assertion. The observed reduction
    // is approximately 9x; allow ample headroom for unrelated accounting changes.
    assert!(
        fuel[0] * 4.0 < fuel[1],
        "shared and correlated fuel: {fuel:?}"
    );
}

/// Local scaling probe, excluded from normal tests. Dataset creation and index
/// construction are outside the measured interval; every result is checked.
/// Set Q4_VIEW=1 for direct graph execution; Q4_PRODUCTS, Q4_FEATURES and
/// Q4_OFFERS control the fixture. FLUREE_DISABLE_AGG_COMPLEMENT_SHARING=1
/// selects the previous aggregate plan for A/B comparisons.
#[tokio::test]
#[ignore = "local Q4 scaling probe"]
async fn bi_q4_local_scaling() {
    assert_index_defaults();
    let n = std::env::var("Q4_PRODUCTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let features = std::env::var("Q4_FEATURES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let offers = std::env::var("Q4_OFFERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    assert!(features >= 2 && n >= features && offers > 0);
    let fluree = FlureeBuilder::memory().build_memory();
    seed_with_offers(&fluree, n, features, offers).await;
    rebuild_and_publish_index(&fluree, "q4:main").await;
    let use_view = std::env::var_os("Q4_VIEW").is_some();
    let ledger = fluree.ledger("q4:main").await.unwrap();
    let db = crate::support::graphdb_from_ledger(&ledger);
    let q = if use_view {
        query(false).replace(" FROM <q4:main>", "")
    } else {
        query(false)
    };
    let expected = expected_with_offers(n, features, offers, false);
    let mut times = Vec::new();
    let mut last_fuel = None;
    for iteration in 0..6 {
        let start = std::time::Instant::now();
        let result = if use_view {
            db.query(&fluree)
                .sparql(&q)
                .track_all()
                .execute_tracked()
                .await
                .unwrap()
        } else {
            fluree
                .query_from()
                .sparql(&q)
                .track_all()
                .execute_tracked()
                .await
                .unwrap()
        };
        let elapsed = start.elapsed();
        assert_eq!(result.status, 200);
        assert_rows(&result.result, &expected, "priceRatio");
        if iteration > 0 {
            times.push(elapsed);
        }
        last_fuel = result.fuel;
    }
    times.sort();
    eprintln!(
        "Q4 products={n} features={features} offers={offers} view={use_view}: median={:?}, fuel={last_fuel:?}",
        times[times.len() / 2]
    );
}
