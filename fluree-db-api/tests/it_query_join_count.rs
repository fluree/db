//! Count-only joins must preserve the bag of filtered matches without emitting it.

use crate::support::{genesis_ledger, span_capture};
use fluree_db_api::{FlureeBuilder, QueryInput, ReindexOptions};
use serde_json::json;

#[tokio::test]
async fn join_count_preserves_filters_multiplicity_and_visible_facts() {
    const PEOPLE: usize = 1200;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "join-count:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let mut graph = vec![
        json!({"@id": "ex:a", "ex:knows": [{"@id": "ex:a"}, {"@id": "ex:x"}, {"@id": "ex:y"}]}),
        json!({"@id": "ex:b", "ex:knows": {"@id": "ex:x"}}),
    ];
    for i in 0..PEOPLE {
        graph.push(json!({"@id": format!("ex:p{i}"),
            "ex:livesIn": [{"@id": "ex:city1"}, {"@id": "ex:city2"}],
            "ex:knows": [{"@id": "ex:a"}, {"@id": "ex:b"}]
        }));
    }
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"}, "@graph": graph
            }),
        )
        .await
        .unwrap()
        .ledger;
    let before_update = ledger.t();
    let path = "?p ex:livesIn ?city ; ex:knows ?f . ?f ex:knows ?fof";
    let filter = "FILTER(?fof != ?p && ?fof != ?f)";
    let count_query = |body: &str| {
        format!("PREFIX ex: <http://example.org/> SELECT (COUNT(*) AS ?n) WHERE {{ {body} }}")
    };
    let query = count_query(&format!("{path} {filter}"));

    for indexed in [false, true] {
        if indexed {
            fluree
                .reindex(ledger_id, ReindexOptions::default())
                .await
                .unwrap();
        }
        let view = fluree.db(ledger_id).await.unwrap();
        let (spans, guard) = span_capture::init_test_tracing();
        let result = fluree
            .query(&view, QueryInput::Sparql(&query))
            .await
            .unwrap();
        drop(guard);
        assert_eq!(
            result.to_jsonld(&view.snapshot).unwrap(),
            json!([[PEOPLE * 6]])
        );
        let drains = spans.find_events("nested-loop count drain complete");
        assert_eq!(
            drains.len(),
            1,
            "indexed={indexed}: count-only join must run"
        );
        let counted: usize = drains[0].fields["counted_rows"].parse().unwrap();
        let materialized: usize = drains[0].fields["materialized_rows"].parse().unwrap();
        assert_eq!(counted + materialized, PEOPLE * 6);
        if indexed {
            assert_eq!(counted, PEOPLE * 6);
            assert_eq!(materialized, 0, "indexed count must avoid output batches");
        }

        for (body, expected) in [
            (format!("{path} {filter}"), PEOPLE * 6),
            (path.to_string(), PEOPLE * 8),
            (format!("{path} {filter} FILTER(?fof != ex:y)"), PEOPLE * 4),
            (format!("{path} FILTER(?fof = ex:x)"), PEOPLE * 4),
            (
                format!("{path} BIND(?fof AS ?copy) FILTER(?copy != ?p && ?copy != ?f)"),
                PEOPLE * 6,
            ),
            (format!("{path} FILTER(?fof / 0 > 1)"), 0),
            (
                format!("VALUES ?p {{ ex:p0 ex:p0 UNDEF }} {path} {filter}"),
                (PEOPLE + 2) * 6,
            ),
        ] {
            let query = count_query(&body);
            let count = fluree
                .query(&view, QueryInput::Sparql(&query))
                .await
                .unwrap();
            assert_eq!(
                count.to_jsonld(&view.snapshot).unwrap(),
                json!([[expected]]),
                "{query}"
            );
            let rows = query.replace("SELECT (COUNT(*) AS ?n)", "SELECT ?p ?city ?f ?fof");
            let result = fluree
                .query(&view, QueryInput::Sparql(&rows))
                .await
                .unwrap();
            assert_eq!(
                result
                    .batches
                    .iter()
                    .map(fluree_db_api::Batch::len)
                    .sum::<usize>(),
                expected,
                "{rows}"
            );
        }
    }

    fluree.update(ledger, &json!({
        "@context": {"ex": "http://example.org/"},
        "delete": {"@id": "ex:a", "ex:knows": {"@id": "ex:y"}},
        "insert": [
            {"@id": "ex:b", "ex:knows": [{"@id": "ex:z"}, {"@id": "ex:w"}]},
            {"@id": "ex:new", "ex:livesIn": {"@id": "ex:city1"}, "ex:knows": {"@id": "ex:newFriend"}},
            {"@id": "ex:newFriend", "ex:knows": {"@id": "ex:x"}}
        ]
    })).await.unwrap();
    for indexed in [false, true] {
        if indexed {
            fluree
                .reindex(ledger_id, ReindexOptions::default())
                .await
                .unwrap();
        }
        let view = fluree.db(ledger_id).await.unwrap();
        let result = fluree
            .query(&view, QueryInput::Sparql(&query))
            .await
            .unwrap();
        assert_eq!(
            result.to_jsonld(&view.snapshot).unwrap(),
            json!([[PEOPLE * 8 + 1]])
        );
    }
    let historical = fluree.db_at_t(ledger_id, before_update).await.unwrap();
    let result = fluree
        .query(&historical, QueryInput::Sparql(&query))
        .await
        .unwrap();
    assert_eq!(
        result.to_jsonld(&historical.snapshot).unwrap(),
        json!([[PEOPLE * 6]])
    );
}
