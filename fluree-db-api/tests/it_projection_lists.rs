//! Subject projections preserve list boundaries, including context compaction.

use crate::support;
use fluree_db_api::{FlureeBuilder, FormatterConfig};
use serde_json::{json, Value};

const EX: &str = "http://example.org/";

#[tokio::test]
async fn projection_lists_preserve_boundaries_and_round_trip() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/projection-lists/shapes:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    fluree
        .insert_turtle(
            ledger,
            &format!(
                "@prefix ex: <{EX}> .
         @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
         ex:s ex:many (3 1 3), (2); ex:single (9); ex:empty ();
              ex:mixed (4 5), 4; ex:plain 6, 7;
              ex:refs (ex:b ex:a ex:b); ex:words (\"bonjour\"@fr \"hello\"@en);
              rdf:type (ex:A ex:B), ex:Plain .
         ex:a ex:name \"A\" . ex:b ex:name \"B\" ."
            ),
        )
        .await
        .unwrap();
    for lane in ["novelty", "indexed"] {
        if lane == "indexed" {
            support::rebuild_and_publish_index(&fluree, id).await;
        }
        let query = json!({"@context": {"ex": EX}, "selectOne": {"ex:s": ["*"]}});
        let got = fluree
            .graph(id)
            .query()
            .jsonld(&query)
            .execute_formatted()
            .await
            .unwrap();
        assert_eq!(
            got["ex:many"],
            json!([{"@list": [3, 1, 3]}, {"@list": [2]}]),
            "{lane}"
        );
        assert_eq!(got["ex:single"], json!({"@list": [9]}), "{lane}");
        assert_eq!(got["ex:empty"], json!({"@list": []}), "{lane}");
        assert_eq!(got["ex:mixed"], json!([{"@list": [4, 5]}, 4]), "{lane}");
        assert_eq!(got["ex:plain"], json!([6, 7]), "{lane}");
        assert_eq!(
            got["ex:refs"],
            json!({"@list": [{"@id": "ex:b"}, {"@id": "ex:a"}, {"@id": "ex:b"}]}),
            "{lane}"
        );
        assert_eq!(
            got["ex:words"],
            json!({"@list": [{"@value": "bonjour", "@language": "fr"}, {"@value": "hello", "@language": "en"}]}),
            "{lane}"
        );
        assert_eq!(
            got["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"],
            json!([
                {"@list": [{"@id": "ex:A"}, {"@id": "ex:B"}]}, {"@id": "ex:Plain"}
            ]),
            "{lane}: list-valued rdf:type must use ordinary reference objects"
        );
        assert!(got.get("@type").is_none());

        let copy_id = format!("it/projection-lists/{lane}-copy:main");
        let copy = fluree.create_ledger(&copy_id).await.unwrap();
        let mut doc = got.clone();
        doc["@context"] = json!({"ex": EX});
        fluree.insert(copy, &doc).await.unwrap();
        assert_eq!(
            fluree
                .graph(&copy_id)
                .query()
                .jsonld(&query)
                .execute_formatted()
                .await
                .unwrap(),
            got,
            "{lane}: reinsertion"
        );

        let nested = fluree
            .graph(id)
            .query()
            .jsonld(&json!({
                "@context": {"ex": EX},
                "selectOne": {"ex:s": [{"ex:refs": ["@id", "ex:name"]}]}
            }))
            .execute_formatted()
            .await
            .unwrap();
        assert_eq!(
            nested["ex:refs"]["@list"],
            json!([
                {"@id": "ex:b", "ex:name": "B"}, {"@id": "ex:a", "ex:name": "A"}, {"@id": "ex:b", "ex:name": "B"}
            ]),
            "{lane}: nested member expansion"
        );

        let normalized = fluree
            .graph(id)
            .query()
            .jsonld(&query)
            .format(FormatterConfig::jsonld().with_normalize_arrays())
            .execute_formatted()
            .await
            .unwrap();
        assert_eq!(normalized["ex:single"], json!([{"@list": [9]}]), "{lane}");
        assert_eq!(normalized["ex:empty"], json!([{"@list": []}]), "{lane}");

        let flat = fluree
            .graph(id)
            .query()
            .jsonld(&json!({
                "@context": {"ex": EX}, "select": "?v",
                "where": {"@id": "ex:s", "ex:many": "?v"}, "orderBy": "?v"
            }))
            .execute_formatted()
            .await
            .unwrap();
        assert_eq!(
            flat,
            json!([1, 2, 3, 3]),
            "{lane}: flat queries still return members"
        );
    }
}

#[tokio::test]
async fn projection_list_containers_compact_only_one_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/projection-lists/containers:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    fluree
        .insert_turtle(
            ledger,
            &format!(
                "@prefix ex: <{EX}> . ex:s ex:single (1); ex:empty (); ex:many (1 2), (3);
         ex:mixed (4), 5; ex:oneSet (6) ."
            ),
        )
        .await
        .unwrap();
    let context = json!({"ex": EX,
        "single": {"@id": "ex:single", "@container": "@list"},
        "ex:empty": {"@container": "@list"},
        "many": {"@id": "ex:many", "@container": "@list"},
        "mixed": {"@id": "ex:mixed", "@container": "@list"},
        "ex:oneSet": {"@container": "@set"}
    });
    let query = json!({"@context": context, "selectOne": {"ex:s": ["*"]}});
    let mut got = fluree
        .graph(id)
        .query()
        .jsonld(&query)
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(got["single"], json!([1]));
    assert_eq!(got["ex:empty"], json!([]));
    assert_eq!(
        got[format!("{EX}many")],
        json!([{"@list": [1, 2]}, {"@list": [3]}])
    );
    assert_eq!(got[format!("{EX}mixed")], json!([{"@list": [4]}, 5]));
    assert_eq!(got["ex:oneSet"], json!([{"@list": [6]}]));
    assert!(got.get("many").is_none() && got.get("mixed").is_none());
    let copy_id = "it/projection-lists/containers-copy:main";
    let copy = fluree.create_ledger(copy_id).await.unwrap();
    got["@context"] = context;
    fluree.insert(copy, &got).await.unwrap();
    got.as_object_mut().unwrap().remove("@context");
    assert_eq!(
        fluree
            .graph(copy_id)
            .query()
            .jsonld(&query)
            .execute_formatted()
            .await
            .unwrap(),
        got
    );
}

#[tokio::test]
async fn projection_lists_in_a_dataset_stay_separate_even_with_identical_members() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ids = ["it/projection-lists/a:main", "it/projection-lists/b:main"];
    for id in ids {
        let ledger = fluree.create_ledger(id).await.unwrap();
        fluree
            .insert_turtle(
                ledger,
                &format!("@prefix ex: <{EX}> . ex:s ex:items (1 2); ex:name \"S\" ."),
            )
            .await
            .unwrap();
    }
    for context in [
        json!({"ex": EX}),
        json!({"ex": EX, "ex:items": {"@container": "@list"}}),
    ] {
        let got = fluree
            .query_from()
            .jsonld(&json!({
                "@context": context, "from": ids, "selectOne": {"ex:s": ["*"]}
            }))
            .execute_formatted()
            .await
            .unwrap();
        let items: &Value = got
            .get("ex:items")
            .or_else(|| got.get(format!("{EX}items")))
            .unwrap();
        assert_eq!(
            items,
            &json!([{"@list": [1, 2]}, {"@list": [1, 2]}]),
            "{got}"
        );
        assert_eq!(
            got["ex:name"], "S",
            "ordinary RDF set union still deduplicates"
        );
    }
    let repeated = fluree
        .query_from()
        .jsonld(&json!({
            "@context": {"ex": EX}, "from": [ids[0], ids[0]],
            "selectOne": {"ex:s": ["*"]}
        }))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(
        repeated["ex:items"],
        json!({"@list": [1, 2]}),
        "naming the same graph twice must not duplicate its list"
    );
}

#[tokio::test]
async fn projection_modifiers_apply_to_each_lists_members() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/projection-lists/modifiers:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    fluree
        .insert_turtle(
            ledger,
            &format!("@prefix ex: <{EX}> . ex:s ex:items (3 1 2), (6 4 5) ."),
        )
        .await
        .unwrap();
    let got = fluree
        .graph(id)
        .query()
        .jsonld(&json!({
            "@context": {"ex": EX}, "selectOne": {"ex:s": [
                {"ex:items": {"orderBy": ["@value"], "offset": 1, "limit": 1}}
            ]}
        }))
        .execute_formatted()
        .await
        .unwrap();
    assert_eq!(got["ex:items"], json!([{"@list": [2]}, {"@list": [5]}]));
}
