//! Scope boundaries should preserve query semantics without retaining the whole
//! offer-level intermediate in BSBM BI Q8. The scale probe uses the same query.
use crate::support::{assert_index_defaults, genesis_ledger, rebuild_and_publish_index};
use fluree_db_api::{Fluree, FlureeBuilder};
use serde_json::{json, Value};

const LEDGER: &str = "q8-stream:main";
const OFFERS: usize = 24;

fn query(flat: bool) -> String {
    let offers =
        "?product a ex:Type . ?offer ex:product ?product ; ex:vendor ?vendor ; ex:price ?price .";
    let avg = format!("{{ SELECT ?product (AVG(xsd:float(xsd:string(?price))) AS ?avgPrice) WHERE {{ {offers} }} GROUP BY ?product }}");
    let group = format!("{offers} {avg}");
    let group = if flat {
        group
    } else {
        format!("{{ {group} }} .")
    };
    format!(
        r"PREFIX ex: <http://example.org/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?vendor (xsd:float(?belowAvg)/?offerCount AS ?cheapExpensiveRatio)
        FROM <{LEDGER}> WHERE {{
            {{ SELECT ?vendor (COUNT(?offer) AS ?belowAvg) WHERE {{
                {group} FILTER(xsd:float(xsd:string(?price)) < ?avgPrice)
            }} GROUP BY ?vendor }}
            {{ SELECT ?vendor (COUNT(?offer) AS ?offerCount) WHERE {{
                ?product a ex:Type . ?offer ex:product ?product ; ex:vendor ?vendor .
            }} GROUP BY ?vendor }}
        }} ORDER BY DESC(xsd:float(?belowAvg)/?offerCount) ?vendor LIMIT 10"
    )
}

async fn seed(n: usize) -> Fluree {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, LEDGER);
    let mut graph = Vec::new();
    for p in 0..n {
        graph.push(json!({"@id":format!("ex:p{p}"), "@type":"ex:Type"}));
        for price in 0..OFFERS {
            graph.push(json!({"@id":format!("ex:o{p}-{price}"),
                "ex:product":{"@id":format!("ex:p{p}")},
                "ex:vendor":{"@id":format!("ex:v{}", price % 4)},
                "ex:price":price * price}));
        }
    }
    // A different class must not enter either the numerator or denominator.
    graph.push(json!({"@id":"ex:other", "@type":"ex:OtherType"}));
    graph.push(
        json!({"@id":"ex:other-offer", "ex:product":{"@id":"ex:other"},
        "ex:vendor":{"@id":"ex:v0"}, "ex:price":0}),
    );
    fluree
        .insert(
            ledger,
            &json!({"@context":{"ex":"http://example.org/"}, "@graph":graph}),
        )
        .await
        .unwrap();
    fluree
}

async fn execute(fluree: &Fluree, flat: bool) -> Value {
    let response = fluree
        .query_from()
        .sparql(&query(flat))
        .track_all()
        .execute_tracked()
        .await
        .unwrap();
    assert_eq!(response.status, 200, "{response:?}");
    let rows = response.result["results"]["bindings"].clone();
    let values = rows.as_array().unwrap();
    assert_eq!(values.len(), 4);
    // Squares 0²..23² average 180 1/6: prices 0²..13² are below
    // average. Vendors 0/1 supply four of their six offers below it;
    // vendors 2/3 supply three. This oracle is independent of query execution.
    for (i, row) in values.iter().enumerate() {
        assert_eq!(row["vendor"]["value"], format!("http://example.org/v{i}"));
        let ratio: f64 = row["cheapExpensiveRatio"]["value"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let expected = if i < 2 { 2.0 / 3.0 } else { 0.5 };
        assert!((ratio - expected).abs() < 0.000_001, "{row}");
    }
    rows
}

#[tokio::test]
async fn nested_group_q8_matches_flat_group_and_ratio_oracle() {
    let fluree = seed(64).await;
    for indexed in [false, true] {
        if indexed {
            rebuild_and_publish_index(&fluree, LEDGER).await;
        }
        assert_eq!(execute(&fluree, false).await, execute(&fluree, true).await);
    }
}

#[tokio::test]
#[ignore = "local scaling probe: Q8_PRODUCTS controls fixture size"]
async fn q8_scope_memory_probe() {
    let n = std::env::var("Q8_PRODUCTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let fluree = seed(n).await;
    rebuild_and_publish_index(&fluree, LEDGER).await;
    let flat = std::env::var_os("Q8_FLAT").is_some();
    for run in 0..3 {
        eprintln!("Q8_QUERY_BEGIN run={run}");
        let start = std::time::Instant::now();
        execute(&fluree, flat).await;
        eprintln!(
            "Q8 products={n} offers={} flat={flat} run={run} elapsed={:?}",
            n * OFFERS,
            start.elapsed()
        );
    }
}
