//! A DELETE naming a few values of a wide slot reads just those values.
//!
//! The resolver reads a slot by value when the index's statistics say the
//! slot is wide enough for that to pay, and every intent naming it is a
//! plain string or a reference. Each case here deletes one value that way,
//! and the same value beside four absent ones, more than the slot's bound,
//! so the slot is read whole; both must leave the same facts. On an indexed
//! ledger, and on one whose novelty retracts, adds and restores values over
//! the index.

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder, LedgerState};
use fluree_db_core::comparator::IndexType;
use fluree_db_core::{range_with_overlay, FlakeValue, RangeMatch, RangeOptions, RangeTest};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/ns/";

fn ctx() -> JsonValue {
    json!({"ex": EX})
}

#[derive(Clone, Copy, Debug)]
enum Lane {
    Indexed,
    NoveltyOverIndex,
}

/// `ex:s ex:tag`: forty plain strings, a list holding `t1` twice and `t5`,
/// `t2` with a tag beside the plain `t2`, an IRI and a blank node. On the
/// novelty lane, commits after the index retract `t7`, add `t50`, and
/// retract and restore `t9`, and others add facts of other subjects.
async fn ledger(fluree: &Fluree, id: &str, lane: Lane) -> LedgerState {
    let mut tags: Vec<JsonValue> = (0..40).map(|i| json!(format!("t{i}"))).collect();
    tags.push(json!({"@value": "t2", "@language": "en"}));
    tags.push(json!({"@id": "ex:r"}));
    tags.push(json!({"@id": "_:b1", "ex:n": "x"}));
    let mut ledger = fluree.create_ledger(id).await.expect("create");
    for txn in [
        json!({"@context": ctx(), "@id": "ex:s", "ex:tag": tags}),
        json!({"@context": ctx(), "@id": "ex:s", "ex:tag": {"@list": ["t1", "t5", "t1"]}}),
    ] {
        ledger = fluree.insert(ledger, &txn).await.expect("seed").ledger;
    }
    drop(ledger);
    support::rebuild_and_publish_index(fluree, id).await;
    let mut ledger = fluree.ledger(id).await.expect("reload");
    assert!(ledger.snapshot.range_provider.is_some(), "indexed");
    if let Lane::NoveltyOverIndex = lane {
        for (op, value) in [
            ("delete", "t7"),
            ("insert", "t50"),
            ("delete", "t9"),
            ("insert", "t9"),
        ] {
            let mut txn = json!({"@context": ctx()});
            txn[op] = json!({"@id": "ex:s", "ex:tag": value});
            ledger = fluree.update(ledger, &txn).await.expect("novelty").ledger;
        }
        // Enough other novelty that a slot is read by a seek into novelty
        // rather than one walk over it.
        let filler: Vec<JsonValue> = (0..200)
            .map(|i| json!({"@id": format!("ex:f{i}"), "ex:q": "x"}))
            .collect();
        let txn = json!({"@context": ctx(), "@graph": filler});
        ledger = fluree.insert(ledger, &txn).await.expect("filler").ledger;
    }
    ledger
}

/// The stored `ex:s ex:tag` facts, as `value[@lang][#position]`, sorted;
/// the blank node as `<blank node>`.
async fn tags(ledger: &LedgerState) -> Vec<String> {
    let flakes = range_with_overlay(
        &ledger.snapshot,
        0,
        ledger.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject_predicate(
            ledger.snapshot.encode_iri(&format!("{EX}s")).expect("s"),
            ledger
                .snapshot
                .encode_iri(&format!("{EX}tag"))
                .expect("tag"),
        ),
        RangeOptions::new().with_to_t(ledger.t()),
    )
    .await
    .expect("range");
    let mut out: Vec<String> = flakes
        .iter()
        .filter(|f| f.op)
        .map(|f| {
            let o = match &f.o {
                FlakeValue::String(s) => s.clone(),
                // An insert mints a blank node's id per transaction, so the
                // two ledgers compared hold one node under two ids.
                FlakeValue::Ref(sid)
                    if sid.namespace_code == fluree_vocab::namespaces::BLANK_NODE =>
                {
                    "<blank node>".to_string()
                }
                FlakeValue::Ref(sid) => format!("<{}>", sid.name),
                other => format!("{other:?}"),
            };
            let lang = f.m.as_ref().and_then(|m| m.lang.as_deref());
            let i = f.m.as_ref().and_then(|m| m.i);
            format!(
                "{o}{}{}",
                lang.map(|l| format!("@{l}")).unwrap_or_default(),
                i.map(|i| format!("#{i}")).unwrap_or_default()
            )
        })
        .collect();
    out.sort();
    out
}

/// The blank node's stable id, `_:fdb-…`.
async fn blank_node_id(ledger: &LedgerState) -> String {
    let flakes = range_with_overlay(
        &ledger.snapshot,
        0,
        ledger.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject_predicate(
            ledger.snapshot.encode_iri(&format!("{EX}s")).expect("s"),
            ledger
                .snapshot
                .encode_iri(&format!("{EX}tag"))
                .expect("tag"),
        ),
        RangeOptions::new().with_to_t(ledger.t()),
    )
    .await
    .expect("range");
    flakes
        .iter()
        .find_map(|f| match &f.o {
            FlakeValue::Ref(sid) if sid.namespace_code == fluree_vocab::namespaces::BLANK_NODE => {
                Some(format!("_:{}", sid.name))
            }
            _ => None,
        })
        .expect("a blank-node tag")
}

/// One delete per case, each of a different value, applied in turn to two
/// identical ledgers: on one each value is deleted alone (read by value),
/// on the other beside four absent values (the slot is read). After every
/// delete the two must have retracted the same facts and hold the same.
async fn deletes_alike(lane: Lane, cases: &[(&str, JsonValue)]) -> Vec<String> {
    let fluree = FlureeBuilder::memory().build_memory();
    let mut ledgers = [
        ledger(&fluree, "it/delete-point-a:main", lane).await,
        ledger(&fluree, "it/delete-point-b:main", lane).await,
    ];
    let mut seen = Vec::new();
    for (what, value) in cases {
        let mut outcomes = Vec::new();
        for (n, padding) in [0, 4].into_iter().enumerate() {
            let ledger = ledgers[n].clone();
            let before = tags(&ledger).await;
            let value = match value.as_str() {
                Some("BLANK") => json!({"@id": blank_node_id(&ledger).await}),
                _ => value.clone(),
            };
            let mut values = vec![value];
            values.extend((0..padding).map(|i| json!(format!("absent{i}"))));
            let txn = json!({"@context": ctx(), "delete": {"@id": "ex:s", "ex:tag": values}});
            let r = fluree.update(ledger, &txn).await.expect("delete");
            let after = tags(&r.ledger).await;
            let removed: Vec<_> = before
                .iter()
                .filter(|t| !after.contains(t))
                .cloned()
                .collect();
            outcomes.push((r.receipt.retract_count, removed, after));
            ledgers[n] = r.ledger;
        }
        assert_eq!(outcomes[0], outcomes[1], "{lane:?} {what}");
        seen.push(format!("{lane:?} {what}: removed {:?}", outcomes[0].1));
    }
    seen
}

#[tokio::test]
async fn deleting_one_value_of_a_wide_slot_removes_what_reading_the_slot_does() {
    let cases = [
        ("a plain value", json!("t3")),
        ("a value also in a list", json!("t1")),
        ("a list entry", json!("t5")),
        ("the untagged twin of a tagged value", json!("t2")),
        ("a tagged value", json!({"@value": "t2", "@language": "en"})),
        ("an IRI", json!({"@id": "ex:r"})),
        ("a blank node", json!("BLANK")),
        ("an absent value", json!("absent")),
        ("a value retracted in novelty", json!("t7")),
        ("a value added in novelty", json!("t50")),
        ("a value restored in novelty", json!("t9")),
    ];
    let mut seen = deletes_alike(Lane::Indexed, &cases).await;
    seen.extend(deletes_alike(Lane::NoveltyOverIndex, &cases).await);
    // The answers themselves, so the comparison cannot pass on two empty
    // results.
    let expect = |line: &str| {
        assert!(seen.iter().any(|s| s == line), "{line}\n{seen:#?}");
    };
    for lane in ["Indexed", "NoveltyOverIndex"] {
        expect(&format!(r#"{lane} a plain value: removed ["t3"]"#));
        expect(&format!(
            r#"{lane} a value also in a list: removed ["t1", "t1#0", "t1#2"]"#
        ));
        // `t5` is one of the plain values too, which goes with its entry.
        expect(&format!(r#"{lane} a list entry: removed ["t5", "t5#1"]"#));
        expect(&format!(
            r#"{lane} the untagged twin of a tagged value: removed ["t2"]"#
        ));
        expect(&format!(r#"{lane} a tagged value: removed ["t2@en"]"#));
        expect(&format!("{lane} an absent value: removed []"));
        expect(&format!(r#"{lane} an IRI: removed ["<r>"]"#));
        expect(&format!(r#"{lane} a blank node: removed ["<blank node>"]"#));
    }
    expect("NoveltyOverIndex a value retracted in novelty: removed []");
    expect(r#"NoveltyOverIndex a value added in novelty: removed ["t50"]"#);
    expect(r#"NoveltyOverIndex a value restored in novelty: removed ["t9"]"#);
}
