//! Count-only joins must preserve the bag of filtered matches without emitting it.

use crate::support::{genesis_ledger, span_capture};
use fluree_db_api::{FlureeBuilder, QueryInput, ReindexOptions};
use serde_json::json;

fn grouped_counts(rows: &serde_json::Value, columns: &[usize]) -> serde_json::Value {
    let mut groups = std::collections::BTreeMap::new();
    for row in rows.as_array().unwrap() {
        let key: Vec<_> = columns.iter().map(|&col| row[col].clone()).collect();
        let (_, count) = groups.entry(json!(key).to_string()).or_insert((key, 0));
        *count += 1;
    }
    json!(groups
        .into_values()
        .map(|(mut key, count)| {
            key.push(json!(count));
            key
        })
        .collect::<Vec<_>>())
}

fn sorted_rows(rows: serde_json::Value) -> serde_json::Value {
    let mut rows = rows.as_array().unwrap().clone();
    rows.sort_by_key(ToString::to_string);
    json!(rows)
}

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
        let grouped_query = format!("PREFIX ex: <http://example.org/> SELECT ?city (COUNT(*) AS ?n) WHERE {{ {path} {filter} }} GROUP BY ?city");
        let grouped = fluree
            .query(&view, QueryInput::Sparql(&grouped_query))
            .await
            .unwrap();
        drop(guard);
        let grouped = grouped.to_jsonld(&view.snapshot).unwrap();
        assert_eq!(grouped.as_array().unwrap().len(), 2);
        assert!(grouped
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row[1] == json!(PEOPLE * 3)));
        let grouped_drains = spans.find_events("nested-loop grouped count drain complete");
        assert_eq!(grouped_drains.len(), 1, "indexed={indexed}");
        if indexed {
            assert_eq!(
                grouped_drains[0].fields["counted_rows"],
                (PEOPLE * 6).to_string()
            );
            assert_eq!(grouped_drains[0].fields["materialized_rows"], "0");
        }
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
            let raw = result.to_jsonld(&view.snapshot).unwrap();
            for (group_vars, columns) in [
                ("?city", vec![1]),
                ("?city ?f", vec![1, 2]),
                ("?fof", vec![3]),
            ] {
                // Includes a right-side key, which must decline the drain and
                // use ordinary grouping, plus a composite driving-side key.
                let grouped_query = format!("PREFIX ex: <http://example.org/> SELECT {group_vars} (COUNT(*) AS ?n) WHERE {{ {body} }} GROUP BY {group_vars}");
                let grouped = fluree
                    .query(&view, QueryInput::Sparql(&grouped_query))
                    .await
                    .unwrap();
                assert_eq!(
                    sorted_rows(grouped.to_jsonld(&view.snapshot).unwrap()),
                    sorted_rows(grouped_counts(&raw, &columns)),
                    "indexed={indexed}: {grouped_query}"
                );
            }
        }

        for (body, group_vars, columns) in [
            (
                format!("VALUES ?tag {{ UNDEF 1 1 2 }} {path}"),
                "?city ?tag",
                vec![0, 1],
            ),
            // A shared subject left unbound by VALUES must be filled by the
            // scan; grouping its original UNDEF would silently miscount.
            (
                "VALUES ?f { ex:a UNDEF } ?f ex:knows ?fof".into(),
                "?f",
                vec![0],
            ),
            (
                format!("{{ SELECT DISTINCT ?city ?fof WHERE {{ {path} }} }}"),
                "?city",
                vec![0],
            ),
            (
                format!("{{ SELECT ?city ?fof WHERE {{ {path} }} LIMIT 17 }}"),
                "?city",
                vec![0],
            ),
        ] {
            let raw_query =
                format!("PREFIX ex: <http://example.org/> SELECT {group_vars} WHERE {{ {body} }}");
            let raw = fluree
                .query(&view, QueryInput::Sparql(&raw_query))
                .await
                .unwrap();
            let expected = sorted_rows(grouped_counts(
                &raw.to_jsonld(&view.snapshot).unwrap(),
                &columns,
            ));
            // Multiple COUNT(*) outputs may share the grouped drain. A mixed
            // aggregate (COUNT(?fof)) must retain row consumption.
            for other_count in ["COUNT(*)", "COUNT(?fof)"] {
                let grouped_query = format!("PREFIX ex: <http://example.org/> SELECT {group_vars} (COUNT(*) AS ?n) ({other_count} AS ?m) WHERE {{ {body} }} GROUP BY {group_vars}");
                let grouped = fluree
                    .query(&view, QueryInput::Sparql(&grouped_query))
                    .await
                    .unwrap();
                let mut grouped = grouped.to_jsonld(&view.snapshot).unwrap();
                for row in grouped.as_array_mut().unwrap() {
                    let row = row.as_array_mut().unwrap();
                    let second_count = row.pop().unwrap();
                    assert_eq!(row.last().unwrap(), &second_count, "{grouped_query}");
                }
                assert_eq!(
                    sorted_rows(grouped),
                    expected,
                    "indexed={indexed}: {grouped_query}"
                );
            }
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
        let grouped_query = format!("PREFIX ex: <http://example.org/> SELECT ?city (COUNT(*) AS ?n) WHERE {{ {path} {filter} }} GROUP BY ?city");
        let grouped = fluree
            .query(&view, QueryInput::Sparql(&grouped_query))
            .await
            .unwrap();
        let raw_query =
            format!("PREFIX ex: <http://example.org/> SELECT ?city WHERE {{ {path} {filter} }}");
        let raw = fluree
            .query(&view, QueryInput::Sparql(&raw_query))
            .await
            .unwrap();
        assert_eq!(
            sorted_rows(grouped.to_jsonld(&view.snapshot).unwrap()),
            sorted_rows(grouped_counts(
                &raw.to_jsonld(&view.snapshot).unwrap(),
                &[0]
            ))
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
    let grouped_query = format!("PREFIX ex: <http://example.org/> SELECT ?city (COUNT(*) AS ?n) WHERE {{ {path} {filter} }} GROUP BY ?city");
    let grouped = fluree
        .query(&historical, QueryInput::Sparql(&grouped_query))
        .await
        .unwrap();
    let grouped = grouped.to_jsonld(&historical.snapshot).unwrap();
    assert_eq!(grouped.as_array().unwrap().len(), 2);
    assert!(grouped
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row[1] == json!(PEOPLE * 3)));
}
