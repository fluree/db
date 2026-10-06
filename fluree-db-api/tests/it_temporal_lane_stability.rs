//! Temporal values answer the same thing before and after a reindex.
//!
//! Sibling of `it_timezone_accessors`, which pins the property for `TZ()` and
//! `TIMEZONE()`. This file pins it for everything else that reads a temporal
//! value: arithmetic, comparison and equality, the calendar accessors, and the
//! rendered form itself.
//!
//! Fluree does not support timezone offsets. Every value is canonicalized the
//! moment it is parsed (`fluree_db_core::temporal`): a `dateTime` becomes a UTC
//! instant (the offset is *applied*), and a `date` or `time` drops its
//! designator (the offset is *discarded*, not applied — the index has only ever
//! kept the calendar day / wall clock). That gives one representation per
//! value, which is what makes the two lanes agree.
//!
//! Before this, the parsed value kept its offset and source lexical while it sat
//! in novelty and lost both once indexed, so each of the expressions below
//! answered one thing before a background reindex and another after. The
//! ones that were caught, with the answers each lane used to give:
//!
//! | expression                                  | novelty                   | indexed                     |
//! |---------------------------------------------|---------------------------|-----------------------------|
//! | `"17:00:00-06:00" - "08:00:00+09:00"`       | `PT0S`                    | `PT9H`                      |
//! | `"2026-01-01+05:00" - "2026-01-01"`         | `-PT5H`                   | `PT0S`                      |
//! | `"2026-01-01-05:00" + PT20H`                | `2026-01-02`              | `2026-01-01`                |
//! | `"2026-01-01+05:00" = "2026-01-01"`         | `false`                   | `true`                      |
//! | `HOURS("2010-12-21T15:38:02-08:00")`        | `15`                      | `23`                        |
//! | `STR("2010-12-21T15:38:02-08:00")`          | as written                | `…T23:38:02.000000Z`        |
//! | `STR("2010-12-21T15:38:02.5Z")` (no offset) | `…02.5Z`                  | `…02.500000Z`               |
//!
//! Same query, unchanged data, different answer with no write behind it.
//!
//! Every case runs on **both lanes and the two are asserted equal**, which is
//! the property this file exists to defend — separately from the value, so a
//! regression reports which of the two problems it is. A single-lane test would
//! pass against exactly the bug this pins.

#![cfg(feature = "native")]

use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, FormatterConfig, IndexConfig, TxnOpts};
use serde_json::Value;

const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";

const DATA: &str = r#"
@prefix ex: <http://example.org/ns/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

# The XPath F&O worked example for op:subtract-times, whose two operands
# normalize to the same UTC time-of-day on different UTC dates.
ex:a ex:tEvening "17:00:00-06:00"^^xsd:time .
ex:a ex:tMorning "08:00:00+09:00"^^xsd:time .
ex:a ex:tNaive   "17:00:00"^^xsd:time .

ex:a ex:dOffNeg "2026-01-01-05:00"^^xsd:date .
ex:a ex:dOffPos "2026-01-01+05:00"^^xsd:date .
ex:a ex:dNaive  "2026-01-01"^^xsd:date .

# Same instant, one carrying an offset and one already normalized; and one
# with fractional seconds and no offset at all.
ex:a ex:dtOff  "2010-12-21T15:38:02-08:00"^^xsd:dateTime .
ex:a ex:dtZ    "2010-12-21T23:38:02Z"^^xsd:dateTime .
ex:a ex:dtFrac "2010-12-21T15:38:02.5Z"^^xsd:dateTime .
"#;

const BINDINGS: &str = "ex:a ex:tEvening ?tEvening ; ex:tMorning ?tMorning ; \
                        ex:tNaive ?tNaive ; ex:dOffNeg ?dOffNeg ; \
                        ex:dOffPos ?dOffPos ; ex:dNaive ?dNaive ; \
                        ex:dtOff ?dtOff ; ex:dtZ ?dtZ ; ex:dtFrac ?dtFrac .";

struct Case {
    /// What the expression exercises, quoted in any failure.
    what: &'static str,
    expr: &'static str,
    expected: &'static str,
}

fn cases() -> Vec<Case> {
    vec![
        // --- rendering: the canonical form, whatever was written -------------
        Case {
            what: "dateTime renders as its UTC instant",
            expr: "STR(?dtOff)",
            expected: "2010-12-21T23:38:02Z",
        },
        Case {
            what: "dateTime fraction keeps no trailing zeros",
            expr: "STR(?dtFrac)",
            expected: "2010-12-21T15:38:02.5Z",
        },
        Case {
            what: "date renders without its designator",
            expr: "STR(?dOffPos)",
            expected: "2026-01-01",
        },
        Case {
            what: "time renders without its designator, offset not applied",
            expr: "STR(?tEvening)",
            expected: "17:00:00",
        },
        // --- equality: one value per canonical form --------------------------
        Case {
            what: "dateTime equality across spellings of one instant",
            expr: "?dtOff = ?dtZ",
            expected: "true",
        },
        Case {
            what: "date equality, offset vs naive",
            expr: "?dOffPos = ?dNaive",
            expected: "true",
        },
        Case {
            what: "time equality, offset vs naive wall clock",
            expr: "?tEvening = ?tNaive",
            expected: "true",
        },
        // --- accessors: UTC components -----------------------------------------
        Case {
            what: "HOURS reads the UTC hour",
            expr: "HOURS(?dtOff)",
            expected: "23",
        },
        Case {
            what: "DAY of a date is its calendar day",
            expr: "DAY(?dOffPos)",
            expected: "1",
        },
        // --- xsd:time arithmetic: wall clocks, offsets discarded ------------
        //
        // XPath's own worked example answers P1D here, by anchoring both
        // operands to a common reference date and subtracting full instants.
        // We answer PT9H: 17:00 − 08:00. Deviation, deliberate — the offsets
        // that would justify P1D are not kept.
        Case {
            what: "time - time, day-crossing offsets (XPath: P1D)",
            expr: "?tEvening - ?tMorning",
            expected: "PT9H",
        },
        Case {
            what: "time - time, offset vs its own naive wall clock",
            expr: "?tEvening - ?tNaive",
            expected: "PT0S",
        },
        Case {
            what: "time + dayTimeDuration wraps within the day",
            expr: "?tEvening + \"PT8H\"^^xsd:dayTimeDuration",
            expected: "01:00:00",
        },
        // --- xsd:date arithmetic: calendar dates, offsets discarded ---------
        Case {
            what: "date - date, offset vs naive same calendar date",
            expr: "?dOffPos - ?dNaive",
            expected: "PT0S",
        },
        Case {
            what: "date - date, opposing offsets on the same calendar date",
            expr: "?dOffPos - ?dOffNeg",
            expected: "PT0S",
        },
        // XPath keeps the date's own timezone and also answers 2026-01-01 here,
        // so on this input the lane-stable answer is the conformant one too.
        Case {
            what: "date + sub-day duration on an offset date (XPath: 2026-01-01)",
            expr: "?dOffNeg + \"PT20H\"^^xsd:dayTimeDuration",
            expected: "2026-01-01",
        },
        Case {
            what: "date + duration crossing a day boundary",
            expr: "?dOffNeg + \"PT25H\"^^xsd:dayTimeDuration",
            expected: "2026-01-02",
        },
        // --- xsd:dateTime arithmetic: the instant, so XPath exactly ---------
        Case {
            what: "dateTime - dateTime, offset vs same instant in Z",
            expr: "?dtOff - ?dtZ",
            expected: "PT0S",
        },
        Case {
            what: "dateTime + dayTimeDuration on an offset value",
            expr: "?dtOff + \"PT1H\"^^xsd:dayTimeDuration",
            expected: "2010-12-22T00:38:02Z",
        },
    ]
}

async fn setup(reindex: bool) -> (tempfile::TempDir, Fluree, String) {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(path).build().expect("build Fluree");
    let alias = "temporallanes:main".to_string();
    let ledger = fluree.create_ledger(&alias).await.expect("create_ledger");
    let index_config = IndexConfig {
        reindex_min_bytes: 5_000_000_000,
        reindex_max_bytes: 5_000_000_000,
    };
    let _ = fluree
        .insert_turtle_with_opts(
            ledger,
            DATA,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
            None,
        )
        .await
        .expect("insert");
    if reindex {
        fluree
            .reindex(&alias, ReindexOptions::default())
            .await
            .expect("reindex");
    }
    (dir, fluree, alias)
}

/// The single `?v` cell rendered as a string, or `None` when it came back
/// unbound. Booleans and numbers render as they would in a comparison.
async fn eval(fluree: &Fluree, alias: &str, expr: &str) -> Option<String> {
    let q = format!("{PREFIX}SELECT ?v WHERE {{ {BINDINGS} BIND({expr} AS ?v) }}");
    let snapshot = fluree.graph(alias).load().await.expect("load");
    let rows: Value = snapshot
        .query()
        .sparql(&q)
        .format(FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("query {expr}: {e}"));

    let arr = rows
        .as_array()
        .unwrap_or_else(|| panic!("rows for {expr}: {rows}"));
    assert_eq!(arr.len(), 1, "expected one row for {expr}, got {rows}");
    let cell = match &arr[0] {
        Value::Array(cols) => cols.first().cloned().unwrap_or(Value::Null),
        other => other.clone(),
    };
    match cell {
        Value::Null => None,
        Value::String(s) => Some(s),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Object(ref o) => Some(
            o.get("@value")
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("no @value for {expr}: {cell}"))
                .to_string(),
        ),
        other => panic!("unexpected cell for {expr}: {other}"),
    }
}

#[tokio::test]
async fn temporal_values_agree_across_lanes() {
    let mut failures: Vec<String> = Vec::new();

    let (_d1, novelty, a1) = setup(false).await;
    let (_d2, indexed, a2) = setup(true).await;

    for case in cases() {
        let got_novelty = eval(&novelty, &a1, case.expr).await;
        let got_indexed = eval(&indexed, &a2, case.expr).await;
        let want = Some(case.expected.to_string());

        // The property this file exists for, asserted in its own right: a
        // reindex must not change the answer.
        if got_novelty != got_indexed {
            failures.push(format!(
                "{} — `{}`: lanes disagree — novelty {got_novelty:?}, indexed {got_indexed:?}",
                case.what, case.expr
            ));
        }
        for (lane, got) in [("novelty", &got_novelty), ("indexed", &got_indexed)] {
            if *got != want {
                failures.push(format!(
                    "[{lane}] {} — `{}`: expected {want:?}, got {got:?}",
                    case.what, case.expr
                ));
            }
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
