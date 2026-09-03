//! `TZ()` and `TIMEZONE()` answer the same thing before and after a reindex.
//!
//! Fluree does not support timezone offsets: a `dateTime` is a UTC instant and
//! a `date`/`time` carries no designator at all, from the moment the lexical is
//! parsed (see `fluree_db_core::temporal`). So there is no offset for these
//! accessors to report, on either lane.
//!
//! They used to read one. The parsed value kept its source offset while it sat
//! in novelty and lost it once indexed, so the same query over unchanged data
//! returned `"-08:00"` and then `"Z"` once a background reindex moved the value
//! — a result that changes with no write behind it, which a caller can neither
//! predict nor control. This file is the pin against that ever coming back.
//!
//! So `TZ` is `"Z"` and `TIMEZONE` is `"PT0S"` for every temporal value, and
//! both are unbound for a non-temporal argument (a type error — SPARQL 1.1
//! types both over `xsd:dateTime`).
//!
//! **This is a deliberate deviation from SPARQL 1.1 §17.4.5.8-9**, which
//! expects the source offset. The two W3C tests that pin that behaviour
//! (`functions/tz-01`, `functions/timezone-01`) are registered as not-supported
//! in `testsuite-sparql/tests/registers/mod.rs`, where the trade-off is
//! written up. Worth knowing: those tests only ever passed because that harness
//! loads into a fresh in-memory ledger and never reindexes — on an indexed
//! ledger they already failed 3 of 4 rows apiece.
//!
//! Every case runs on **both lanes** and the two are asserted equal, which is
//! the property this file exists to defend. A single-lane test would have
//! passed against the original bug on whichever lane it happened to pick.

#![cfg(feature = "native")]

use fluree_db_api::admin::ReindexOptions;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, FormatterConfig, IndexConfig, TxnOpts};
use serde_json::Value;

const PREFIX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";

const DATA: &str = r#"
@prefix ex: <http://example.org/ns/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:a ex:dtOffset  "2010-12-21T15:38:02-08:00"^^xsd:dateTime .
ex:a ex:dtZulu    "2010-06-21T11:28:01Z"^^xsd:dateTime .
ex:a ex:dtNaive   "2011-02-01T01:02:03"^^xsd:dateTime .
ex:a ex:dateOff   "2026-01-01+05:00"^^xsd:date .
ex:a ex:dateNaive "2026-01-01"^^xsd:date .
ex:a ex:timeOff   "09:30:00+05:30"^^xsd:time .
ex:a ex:gyearOff  "2005Z"^^xsd:gYear .
ex:a ex:plain     "not a date" .
"#;

struct Case {
    expr: &'static str,
    /// `None` = the expression must answer unbound.
    expected: Option<&'static str>,
}

fn cases() -> Vec<Case> {
    vec![
        // Every temporal value is a UTC instant by the time it is stored, no
        // matter which lexical form it arrived in or which lane serves it.
        Case {
            expr: "TZ(?dtOffset)",
            expected: Some("Z"),
        },
        Case {
            expr: "TZ(?dtZulu)",
            expected: Some("Z"),
        },
        Case {
            expr: "TZ(?dtNaive)",
            expected: Some("Z"),
        },
        Case {
            expr: "TZ(?dateOff)",
            expected: Some("Z"),
        },
        Case {
            expr: "TZ(?dateNaive)",
            expected: Some("Z"),
        },
        Case {
            expr: "TZ(?timeOff)",
            expected: Some("Z"),
        },
        Case {
            expr: "TZ(?gyearOff)",
            expected: Some("Z"),
        },
        // Not a temporal value: a type error, so unbound. The old code answered
        // "" here — the spec's own encoding of "this value has no timezone",
        // about a value that is not a temporal at all.
        Case {
            expr: "TZ(?plain)",
            expected: None,
        },
        Case {
            expr: "TIMEZONE(?dtOffset)",
            expected: Some("PT0S"),
        },
        Case {
            expr: "TIMEZONE(?dtZulu)",
            expected: Some("PT0S"),
        },
        Case {
            expr: "TIMEZONE(?dtNaive)",
            expected: Some("PT0S"),
        },
        Case {
            expr: "TIMEZONE(?dateOff)",
            expected: Some("PT0S"),
        },
        Case {
            expr: "TIMEZONE(?dateNaive)",
            expected: Some("PT0S"),
        },
        Case {
            expr: "TIMEZONE(?timeOff)",
            expected: Some("PT0S"),
        },
        Case {
            expr: "TIMEZONE(?plain)",
            expected: None,
        },
    ]
}

const BINDINGS: &str = "ex:a ex:dtOffset ?dtOffset ; ex:dtZulu ?dtZulu ; \
                        ex:dtNaive ?dtNaive ; ex:dateOff ?dateOff ; \
                        ex:dateNaive ?dateNaive ; ex:timeOff ?timeOff ; \
                        ex:gyearOff ?gyearOff ; ex:plain ?plain .";

async fn setup(reindex: bool) -> (tempfile::TempDir, Fluree, String) {
    let dir = tempfile::tempdir().expect("tmpdir");
    let path = dir.path().to_string_lossy().to_string();
    let fluree = FlureeBuilder::file(path).build().expect("build Fluree");
    let alias = "tzaccessors:main".to_string();
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

/// The single `?v` cell, or `None` when it came back unbound.
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
async fn timezone_accessors_agree_across_lanes() {
    let mut failures: Vec<String> = Vec::new();

    let (_d1, novelty, a1) = setup(false).await;
    let (_d2, indexed, a2) = setup(true).await;

    for case in cases() {
        let got_novelty = eval(&novelty, &a1, case.expr).await;
        let got_indexed = eval(&indexed, &a2, case.expr).await;
        let want = case.expected.map(str::to_string);

        // The property this file exists for: a reindex must not change the
        // answer. Asserted separately from the value so a future regression
        // reports *which* of the two problems it is.
        if got_novelty != got_indexed {
            failures.push(format!(
                "{}: lanes disagree — novelty {got_novelty:?}, indexed {got_indexed:?}",
                case.expr
            ));
        }
        for (lane, got) in [("novelty", &got_novelty), ("indexed", &got_indexed)] {
            if *got != want {
                failures.push(format!(
                    "[{lane}] {}: expected {want:?}, got {got:?}",
                    case.expr
                ));
            }
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
