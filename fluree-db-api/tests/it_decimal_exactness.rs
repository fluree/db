//! Decimal exactness integration tests
//!
//! xsd:decimal values must never round-trip through f64 anywhere between
//! ingestion and output: query constants, SPARQL UPDATE templates, and
//! stored values all carry exact BigDecimal representations.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use support::{
    assert_index_defaults, genesis_ledger, start_background_indexer_local, trigger_index_and_wait,
    MemoryFluree,
};

async fn run_sparql_update(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    sparql: &str,
) -> fluree_db_api::TransactResult {
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    assert!(
        !parsed.has_errors(),
        "SPARQL parse errors: {:?}",
        parsed.diagnostics
    );
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &ast,
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower SPARQL UPDATE to Txn IR");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect("stage SPARQL UPDATE")
}

/// Extract the literal values of a single-variable SPARQL JSON result.
fn binding_values(sparql_json: &JsonValue, var: &str) -> Vec<String> {
    sparql_json["results"]["bindings"]
        .as_array()
        .expect("bindings array")
        .iter()
        .map(|b| {
            b[var]["value"]
                .as_str()
                .expect("binding value string")
                .to_string()
        })
        .collect()
}

fn memory_fluree() -> MemoryFluree {
    assert_index_defaults();
    FlureeBuilder::memory().build_memory()
}

#[tokio::test]
async fn sparql_insert_data_decimal_roundtrip_is_exact() {
    let fluree = memory_fluree();
    let ledger = genesis_ledger(&fluree, "decimal/insert:main");

    // 19.99 has no exact f64 representation; an f64 round-trip surfaces as
    // 19.989999999999998... in exact-decimal output.
    let result = run_sparql_update(
        &fluree,
        ledger,
        r"
        PREFIX ex: <http://example.org/>
        INSERT DATA { ex:item ex:price 19.99 . }
        ",
    )
    .await;
    let ledger = result.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?price WHERE { ex:item ex:price ?price . }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "price"), vec!["19.99"]);
}

#[tokio::test]
async fn sparql_insert_data_high_precision_decimal_survives() {
    let fluree = memory_fluree();
    let ledger = genesis_ledger(&fluree, "decimal/precision:main");

    // More significant digits than f64 can hold (~17).
    let lexical = "1234567890123456789.0123456789";
    let result = run_sparql_update(
        &fluree,
        ledger,
        &format!(
            r#"
            PREFIX ex: <http://example.org/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            INSERT DATA {{ ex:big ex:amount "{lexical}"^^xsd:decimal . }}
            "#
        ),
    )
    .await;
    let ledger = result.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?amount WHERE { ex:big ex:amount ?amount . }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "amount"), vec![lexical]);
}

#[tokio::test]
async fn small_decimal_renders_in_plain_form_not_exponent() {
    // bigdecimal 0.4's Display switches to E-notation past a magnitude
    // threshold (0.0000001 -> "1E-7"), which is invalid xsd:decimal lexical
    // form. Output paths must emit the plain form. Exposed by exact storage:
    // these values used to be f64 doubles, now they're exact decimals.
    let fluree = memory_fluree();
    let ledger = genesis_ledger(&fluree, "decimal/plain-form:main");

    let lexical = "0.0000001";
    let result = run_sparql_update(
        &fluree,
        ledger,
        &format!(
            r#"
            PREFIX ex: <http://example.org/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            INSERT DATA {{ ex:tiny ex:amount "{lexical}"^^xsd:decimal . }}
            "#
        ),
    )
    .await;
    let ledger = result.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?amount WHERE { ex:tiny ex:amount ?amount . }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");

    // SPARQL JSON output.
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "amount"), vec![lexical]);

    // STR() must also yield the plain lexical, not E-notation.
    let str_query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ex:tiny ex:amount ?a . BIND(STR(?a) AS ?s) }
    ";
    let result = support::query_sparql(&fluree, &ledger, str_query)
        .await
        .expect("str query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "s"), vec![lexical]);
}

#[tokio::test]
async fn sparql_decimal_constant_matches_stored_decimal() {
    let fluree = memory_fluree();
    let ledger = genesis_ledger(&fluree, "decimal/constant:main");

    let result = run_sparql_update(
        &fluree,
        ledger,
        r"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
            ex:a ex:price 19.99 .
            ex:b ex:price 20.00 .
        }
        ",
    )
    .await;
    let ledger = result.ledger;

    // Constant in object position must exactly match the stored decimal.
    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s ex:price 19.99 . }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "s"), vec!["ex:a"]);

    // FILTER equality with a decimal constant.
    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s ex:price ?p . FILTER(?p = 20.00) }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "s"), vec!["ex:b"]);
}

#[tokio::test]
async fn jsonld_number_decimal_matches_sparql_constant_across_paths() {
    // The same decimal written as a JSON number via JSON-LD and referenced
    // as a SPARQL constant must be ONE value — not two encodings.
    let fluree = memory_fluree();
    let ledger = genesis_ledger(&fluree, "decimal/crosspath:main");

    let insert = serde_json::json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@id": "ex:item",
        "ex:price": {"@value": 19.99, "@type": "xsd:decimal"}
    });
    let ledger = fluree.insert(ledger, &insert).await.expect("insert").ledger;

    // SPARQL constant matches the JSON-LD-ingested value.
    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s ex:price 19.99 . }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "s"), vec!["ex:item"]);

    // SPARQL DELETE DATA retracts the JSON-LD-ingested fact.
    let result = run_sparql_update(
        &fluree,
        ledger,
        r"
        PREFIX ex: <http://example.org/>
        DELETE DATA { ex:item ex:price 19.99 . }
        ",
    )
    .await;
    let ledger = result.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?price WHERE { ex:item ex:price ?price . }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(
        binding_values(&sparql_json, "price"),
        Vec::<String>::new(),
        "SPARQL DELETE DATA must retract the JSON-LD-ingested decimal"
    );
}

#[tokio::test]
async fn trig_graph_block_decimal_matches_default_graph_decimal() {
    // A bare decimal literal inside a GRAPH block must parse exactly as
    // xsd:decimal — the same as in the default graph — not via f64 as
    // xsd:double.
    let fluree = memory_fluree();
    let ledger = genesis_ledger(&fluree, "decimal/trig:main");

    let trig = r"
        @prefix ex: <http://example.org/> .

        ex:default ex:price 19.99 .

        GRAPH <http://example.org/g> {
            ex:named ex:price 19.99 .
        }
    ";
    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(trig)
        .execute()
        .await
        .expect("upsert trig");
    let ledger = result.ledger;

    // Default graph value is exact.
    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?price WHERE { ex:default ex:price ?price . }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query default");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "price"), vec!["19.99"]);

    // Named-graph value is exact and the same value.
    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?price WHERE { GRAPH <http://example.org/g> { ex:named ex:price ?price . } }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query named graph");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "price"), vec!["19.99"]);
}

#[tokio::test]
async fn integer_beyond_i64_round_trips_exactly() {
    // xsd:integer is unbounded: a literal past i64 must promote to BigInt
    // end to end (it previously lexed to 0).
    let fluree = memory_fluree();
    let ledger = genesis_ledger(&fluree, "decimal/bigint:main");

    let big = "123456789012345678901234567890";
    let turtle = format!(
        r"
        @prefix ex: <http://example.org/> .
        ex:item ex:serial {big} .
        "
    );
    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(&turtle)
        .execute()
        .await
        .expect("upsert turtle");
    let ledger = result.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?serial WHERE { ex:item ex:serial ?serial . }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "serial"), vec![big]);

    // The same literal as a SPARQL constant matches the stored value.
    let query = format!(
        r"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE {{ ?s ex:serial {big} . }}
        "
    );
    let result = support::query_sparql(&fluree, &ledger, &query)
        .await
        .expect("query constant");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(binding_values(&sparql_json, "s"), vec!["ex:item"]);

    // Typed lexical form via SPARQL UPDATE round-trips and is queryable by
    // the typed constant (both previously degraded through i64 paths).
    let result = run_sparql_update(
        &fluree,
        ledger,
        &format!(
            r#"
            PREFIX ex: <http://example.org/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            INSERT DATA {{ ex:typed ex:serial "{big}"^^xsd:integer . }}
            "#
        ),
    )
    .await;
    let ledger = result.ledger;

    let query = format!(
        r#"
        PREFIX ex: <http://example.org/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?s WHERE {{ ?s ex:serial "{big}"^^xsd:integer . }}
        "#
    );
    let result = support::query_sparql(&fluree, &ledger, &query)
        .await
        .expect("typed constant query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let mut subjects = binding_values(&sparql_json, "s");
    subjects.sort();
    assert_eq!(
        subjects,
        vec!["ex:item", "ex:typed"],
        "typed xsd:integer constant must match both bare- and typed-ingested values"
    );

    // VALUES with a typed constant: the binding must carry the declared
    // datatype (it was labeled xsd:string, which made the join always fail).
    let query = format!(
        r#"
        PREFIX ex: <http://example.org/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?s WHERE {{
            VALUES ?serial {{ "{big}"^^xsd:integer }}
            ?s ex:serial ?serial .
        }}
        "#
    );
    let result = support::query_sparql(&fluree, &ledger, &query)
        .await
        .expect("typed VALUES query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let mut subjects = binding_values(&sparql_json, "s");
    subjects.sort();
    assert_eq!(
        subjects,
        vec!["ex:item", "ex:typed"],
        "typed VALUES constant must join against stored values"
    );
}

#[tokio::test]
async fn sum_avg_over_indexed_decimals_is_exact() {
    // Indexed decimals are arena-backed (NUM_BIG encoded). SUM/AVG must
    // decode and accumulate them exactly — they previously contributed
    // nothing to non-streaming aggregates.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "decimal/agg-indexed:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            let result = run_sparql_update(
                &fluree,
                ledger,
                r"
                PREFIX ex: <http://example.org/>
                INSERT DATA {
                    ex:a ex:amount 19.99 .
                    ex:b ex:amount 0.01 .
                    ex:c ex:amount 10.00 .
                }
                ",
            )
            .await;

            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            let query = r"
                PREFIX ex: <http://example.org/>
                SELECT (SUM(?amount) AS ?total) (AVG(?amount) AS ?mean)
                WHERE { ?s ex:amount ?amount . }
            ";
            let result = support::query_sparql(&fluree, &ledger, query)
                .await
                .expect("aggregate query");
            let sparql_json = result
                .to_sparql_json(&ledger.snapshot)
                .expect("to_sparql_json");

            // BigDecimal addition preserves the max input scale ("30.00").
            // Compare numerically: any f64 contamination or dropped rows
            // would surface as a different value entirely (e.g. "0" when
            // arena-backed rows are skipped).
            fn as_decimal(s: &str) -> num_bigdecimal::BigDecimal {
                s.parse().expect("decimal result")
            }
            let totals = binding_values(&sparql_json, "total");
            assert_eq!(
                as_decimal(&totals[0]),
                as_decimal("30"),
                "SUM over indexed decimals must be exact (19.99 + 0.01 + 10.00), got {totals:?}"
            );
            let means = binding_values(&sparql_json, "mean");
            assert_eq!(
                as_decimal(&means[0]),
                as_decimal("10"),
                "AVG over indexed decimals must be exact, got {means:?}"
            );
        })
        .await;
}

#[tokio::test]
async fn count_with_numeric_filter_over_decimal_rows_is_correct() {
    // The numeric-compare COUNT fast path can't compare arena-keyed decimals
    // by encoded key; it must defer to the general pipeline rather than
    // count decimal rows as non-matches.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "decimal/count-filter:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            // Index a mix of integer and decimal rows.
            let result = run_sparql_update(
                &fluree,
                ledger,
                r"
                PREFIX ex: <http://example.org/>
                INSERT DATA {
                    ex:a ex:amount 5 .
                    ex:b ex:amount 15 .
                    ex:c ex:amount 10.50 .
                    ex:d ex:amount 20.25 .
                }
                ",
            )
            .await;
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Add decimal novelty on top of the indexed base (overlay lane).
            let result = run_sparql_update(
                &fluree,
                ledger,
                r"
                PREFIX ex: <http://example.org/>
                INSERT DATA { ex:e ex:amount 30.75 . }
                ",
            )
            .await;
            let ledger = result.ledger;

            // Matches: 15, 10.50, 20.25, 30.75 (> 10) — integers and decimals,
            // base and novelty.
            let query = r"
                PREFIX ex: <http://example.org/>
                SELECT (COUNT(?s) AS ?n)
                WHERE { ?s ex:amount ?o . FILTER(?o > 10) }
            ";
            let result = support::query_sparql(&fluree, &ledger, query)
                .await
                .expect("count query");
            let sparql_json = result
                .to_sparql_json(&ledger.snapshot)
                .expect("to_sparql_json");
            assert_eq!(
                binding_values(&sparql_json, "n"),
                vec!["4"],
                "COUNT must include decimal rows matching the numeric filter"
            );

            // Decimal threshold over mixed rows: 15, 10.50 excluded? (> 10.6):
            // matches 15, 20.25, 30.75.
            let query = r"
                PREFIX ex: <http://example.org/>
                SELECT (COUNT(?s) AS ?n)
                WHERE { ?s ex:amount ?o . FILTER(?o > 10.6) }
            ";
            let result = support::query_sparql(&fluree, &ledger, query)
                .await
                .expect("count query decimal threshold");
            let sparql_json = result
                .to_sparql_json(&ledger.snapshot)
                .expect("to_sparql_json");
            assert_eq!(
                binding_values(&sparql_json, "n"),
                vec!["3"],
                "decimal threshold must compare numerically against all rows"
            );
        })
        .await;
}

#[tokio::test]
async fn scale_variant_decimal_retracts_indexed_fact() {
    // 1.50 and 1.5 are one xsd:decimal value. A retract written with a
    // different scale than the indexed assert must still hit the same fact
    // (arena keys normalize, so there is exactly one persisted encoding).
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "decimal/scale-retract:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            let result = run_sparql_update(
                &fluree,
                ledger,
                r#"
                PREFIX ex: <http://example.org/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                INSERT DATA { ex:item ex:price "1.50"^^xsd:decimal . }
                "#,
            )
            .await;
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Constant with a different scale matches the indexed fact.
            let query = r"
                PREFIX ex: <http://example.org/>
                SELECT ?s WHERE { ?s ex:price 1.5 . }
            ";
            let result = support::query_sparql(&fluree, &ledger, query)
                .await
                .expect("query");
            let sparql_json = result
                .to_sparql_json(&ledger.snapshot)
                .expect("to_sparql_json");
            assert_eq!(
                binding_values(&sparql_json, "s"),
                vec!["ex:item"],
                "1.5 constant must match indexed 1.50"
            );

            // Retract with the other scale form removes the fact.
            let result = run_sparql_update(
                &fluree,
                ledger,
                r"
                PREFIX ex: <http://example.org/>
                DELETE DATA { ex:item ex:price 1.5 . }
                ",
            )
            .await;
            let ledger = result.ledger;

            let query = r"
                PREFIX ex: <http://example.org/>
                SELECT ?price WHERE { ex:item ex:price ?price . }
            ";
            let result = support::query_sparql(&fluree, &ledger, query)
                .await
                .expect("query");
            let sparql_json = result
                .to_sparql_json(&ledger.snapshot)
                .expect("to_sparql_json");
            assert_eq!(
                binding_values(&sparql_json, "price"),
                Vec::<String>::new(),
                "retract written as 1.5 must remove the fact indexed as 1.50"
            );
        })
        .await;
}

#[tokio::test]
async fn group_by_and_distinct_unify_decimals_across_index_and_novelty() {
    // The same decimal value served encoded from the index (arena handle)
    // and decoded from novelty (raw flake) must land in ONE group and ONE
    // distinct row.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "decimal/groupkey:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            let result = run_sparql_update(
                &fluree,
                ledger,
                r"
                PREFIX ex: <http://example.org/>
                INSERT DATA {
                    ex:a ex:price 19.99 .
                    ex:b ex:price 5.25 .
                }
                ",
            )
            .await;
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            // Same value again via novelty (decoded lane) plus a scale variant.
            let result = run_sparql_update(
                &fluree,
                ledger,
                r#"
                PREFIX ex: <http://example.org/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                INSERT DATA {
                    ex:c ex:price 19.99 .
                    ex:d ex:price "5.250"^^xsd:decimal .
                }
                "#,
            )
            .await;
            let ledger = result.ledger;

            // DISTINCT: two values total (19.99, 5.25), each in two
            // representations.
            let query = r"
                PREFIX ex: <http://example.org/>
                SELECT DISTINCT ?price WHERE { ?s ex:price ?price . }
            ";
            let result = support::query_sparql(&fluree, &ledger, query)
                .await
                .expect("distinct query");
            let sparql_json = result
                .to_sparql_json(&ledger.snapshot)
                .expect("to_sparql_json");
            let mut prices = binding_values(&sparql_json, "price");
            prices.sort();
            assert_eq!(
                prices.len(),
                2,
                "index- and novelty-served copies of one value must dedup: {prices:?}"
            );

            // GROUP BY: each value groups its two subjects together.
            let query = r"
                PREFIX ex: <http://example.org/>
                SELECT ?price (COUNT(?s) AS ?n)
                WHERE { ?s ex:price ?price . }
                GROUP BY ?price
            ";
            let result = support::query_sparql(&fluree, &ledger, query)
                .await
                .expect("group query");
            let sparql_json = result
                .to_sparql_json(&ledger.snapshot)
                .expect("to_sparql_json");
            let counts = binding_values(&sparql_json, "n");
            assert_eq!(
                counts,
                vec!["2", "2"],
                "each decimal value must form one group of two subjects"
            );
        })
        .await;
}

#[tokio::test]
async fn named_graph_decimal_decodes_against_its_own_arena() {
    // NumBig arenas are per (graph, predicate): the default graph's 19.99
    // and the named graph's 42.42 both occupy handle 0 of their own arena.
    // A value projected out of a GRAPH scope must decode against ITS graph's
    // arena — decoding against the outer graph would return 19.99 for the
    // named-graph row.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "decimal/graph-arena:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger = genesis_ledger(&fluree, ledger_id);

            let trig = r"
                @prefix ex: <http://example.org/> .

                ex:default ex:price 19.99 .

                GRAPH <http://example.org/g> {
                    ex:named ex:price 42.42 .
                }
            ";
            let result = fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("upsert trig");
            trigger_index_and_wait(&handle, ledger_id, result.receipt.t).await;
            let ledger = fluree.ledger(ledger_id).await.expect("load ledger");

            let query = r"
                PREFIX ex: <http://example.org/>
                SELECT ?price WHERE { GRAPH <http://example.org/g> { ex:named ex:price ?price } }
            ";
            let result = support::query_sparql(&fluree, &ledger, query)
                .await
                .expect("graph query");
            let sparql_json = result
                .to_sparql_json(&ledger.snapshot)
                .expect("to_sparql_json");
            assert_eq!(
                binding_values(&sparql_json, "price"),
                vec!["42.42"],
                "named-graph decimal must decode against its own arena, not the default graph's"
            );
        })
        .await;
}

#[tokio::test]
async fn sparql_delete_data_decimal_retracts_exactly() {
    let fluree = memory_fluree();
    let ledger = genesis_ledger(&fluree, "decimal/delete:main");

    let result = run_sparql_update(
        &fluree,
        ledger,
        r"
        PREFIX ex: <http://example.org/>
        INSERT DATA { ex:item ex:price 19.99 . }
        ",
    )
    .await;

    // DELETE DATA with the same lexical must hit the same stored fact —
    // an f64 round-trip on either side breaks retract identity.
    let result = run_sparql_update(
        &fluree,
        result.ledger,
        r"
        PREFIX ex: <http://example.org/>
        DELETE DATA { ex:item ex:price 19.99 . }
        ",
    )
    .await;
    let ledger = result.ledger;

    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?price WHERE { ex:item ex:price ?price . }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(
        binding_values(&sparql_json, "price"),
        Vec::<String>::new(),
        "deleted decimal fact must not survive"
    );
}

// =============================================================================
// Inline xsd:decimal encoding (v3 root format)
// =============================================================================

/// Run a full rebuild, publish the new index, and return the decoded index root
/// so tests can assert the on-disk decimal-encoding format.
async fn full_rebuild_publish_decode_root(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_binary_index::format::index_root::IndexRoot {
    use fluree_db_core::storage::ContentStore;
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("nameservice lookup")
        .expect("ledger record");
    let result = fluree_db_indexer::rebuild_index_from_commits(
        fluree.content_store(ledger_id),
        ledger_id,
        &record,
        fluree_db_indexer::IndexerConfig::default(),
    )
    .await
    .expect("full rebuild");
    let root_bytes = fluree
        .content_store(ledger_id)
        .get(&result.root_id)
        .await
        .expect("fetch root bytes");
    fluree
        .publisher()
        .expect("read-write nameservice")
        .publish_index(ledger_id, result.index_t, &result.root_id)
        .await
        .expect("publish index");
    fluree_db_binary_index::format::index_root::IndexRoot::decode(&root_bytes).expect("decode root")
}

#[tokio::test]
async fn full_reindex_writes_inline_decimal_v3_format_and_roundtrips() {
    // A full rebuild adopts the inline-decimal format: the root is v3
    // (InlineWhenFits), small exact decimals encode inline, and a value too
    // large to fit inline falls back to the arena — all round-trip exactly.
    let fluree = memory_fluree();
    let ledger_id = "decimal/inline-format:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let result = run_sparql_update(
        &fluree,
        ledger,
        r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
            ex:a ex:amount 19.99 .
            ex:b ex:amount 0.0000001 .
            ex:c ex:amount "1234567890123456789.5"^^<http://www.w3.org/2001/XMLSchema#decimal> .
        }
        "#,
    )
    .await;
    let _ = result;

    let root = full_rebuild_publish_decode_root(&fluree, ledger_id).await;
    assert_eq!(
        root.decimal_encoding(),
        fluree_db_core::DecimalEncoding::InlineWhenFits,
        "a full rebuild must write the inline-decimal (v3) format"
    );

    let ledger = fluree
        .ledger(ledger_id)
        .await
        .expect("load reindexed ledger");
    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s ?amount WHERE { ?s ex:amount ?amount . }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");

    let mut amounts = binding_values(&sparql_json, "amount");
    amounts.sort();
    // Two inline-eligible decimals + one arena-overflow decimal, all exact and
    // in plain (non-exponent) form.
    assert_eq!(
        amounts,
        vec![
            "0.0000001".to_string(),
            "1234567890123456789.5".to_string(),
            "19.99".to_string(),
        ],
        "inline + arena decimals must round-trip exactly after reindex"
    );
}

#[tokio::test]
async fn inline_decimal_equality_constant_matches_after_reindex() {
    // A decimal equality constant must encode the same way as the stored inline
    // row so the bound-object lookup hits it (issue #1328 narrowing).
    let fluree = memory_fluree();
    let ledger_id = "decimal/inline-eq:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    run_sparql_update(
        &fluree,
        ledger,
        r"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
            ex:a ex:price 19.99 .
            ex:b ex:price 20.00 .
        }
        ",
    )
    .await;

    let root = full_rebuild_publish_decode_root(&fluree, ledger_id).await;
    assert_eq!(
        root.decimal_encoding(),
        fluree_db_core::DecimalEncoding::InlineWhenFits
    );

    let ledger = fluree
        .ledger(ledger_id)
        .await
        .expect("load reindexed ledger");
    let query = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE { ?s ex:price 19.99 . }
    ";
    let result = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("query");
    let sparql_json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    assert_eq!(
        binding_values(&sparql_json, "s"),
        vec!["ex:a".to_string()],
        "decimal equality constant must match the stored inline decimal"
    );
}

/// Canonicalize a SPARQL bindings array for differential comparison: any literal
/// whose value parses as a `BigDecimal` is rewritten to its normalized form.
/// Indexing canonicalizes decimal scale (`10.50` -> `10.5`) for both the arena
/// and inline encodings, so a novelty-vs-indexed comparison must compare by
/// numeric value, not lexical form. Datatype and structure are preserved and
/// still compared exactly.
fn canon_decimal_bindings(bindings: &JsonValue) -> JsonValue {
    let mut bindings = bindings.clone();
    if let Some(rows) = bindings.as_array_mut() {
        for row in rows {
            if let Some(obj) = row.as_object_mut() {
                for (_var, cell) in obj.iter_mut() {
                    if let Some(v) = cell.get("value").and_then(|v| v.as_str()) {
                        if let Ok(bd) = v.parse::<num_bigdecimal::BigDecimal>() {
                            cell["value"] = JsonValue::String(bd.normalized().to_plain_string());
                        }
                    }
                }
            }
        }
    }
    bindings
}

#[tokio::test]
async fn inline_decimal_results_match_novelty_differential() {
    // Differential: the same query must return identical results whether the
    // decimals are unindexed (novelty, canonical FlakeValue::Decimal) or indexed
    // under the inline (v3) format. Proves inline encoding is observably
    // identical to the canonical representation across SELECT / ORDER BY / FILTER
    // / aggregation.
    let fluree = memory_fluree();
    let ledger_id = "decimal/inline-differential:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let result = run_sparql_update(
        &fluree,
        ledger,
        r"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
            ex:a ex:amount 19.99 .
            ex:b ex:amount 0.01 .
            ex:c ex:amount 10.50 .
            ex:d ex:amount 100.00 .
        }
        ",
    )
    .await;
    let novelty_ledger = result.ledger;

    let queries = [
        // Plain projection + ORDER BY on the decimal value.
        r"PREFIX ex: <http://example.org/>
          SELECT ?amount WHERE { ?s ex:amount ?amount . } ORDER BY ?amount",
        // FILTER comparison against a decimal threshold.
        r"PREFIX ex: <http://example.org/>
          SELECT ?amount WHERE { ?s ex:amount ?amount . FILTER(?amount > 10.0) } ORDER BY ?amount",
        // Aggregation (SUM/AVG) + COUNT.
        r"PREFIX ex: <http://example.org/>
          SELECT (SUM(?amount) AS ?total) (COUNT(?amount) AS ?n) WHERE { ?s ex:amount ?amount . }",
    ];

    // Results from the unindexed (novelty) state.
    let mut novelty_results = Vec::new();
    for q in &queries {
        let r = support::query_sparql(&fluree, &novelty_ledger, q)
            .await
            .expect("novelty query");
        novelty_results.push(
            r.to_sparql_json(&novelty_ledger.snapshot)
                .expect("to_sparql_json")["results"]["bindings"]
                .clone(),
        );
    }

    // Reindex into the inline (v3) format.
    let root = full_rebuild_publish_decode_root(&fluree, ledger_id).await;
    assert_eq!(
        root.decimal_encoding(),
        fluree_db_core::DecimalEncoding::InlineWhenFits
    );
    let indexed_ledger = fluree
        .ledger(ledger_id)
        .await
        .expect("load reindexed ledger");

    for (q, novelty_bindings) in queries.iter().zip(novelty_results) {
        let r = support::query_sparql(&fluree, &indexed_ledger, q)
            .await
            .expect("indexed query");
        let indexed_bindings = r
            .to_sparql_json(&indexed_ledger.snapshot)
            .expect("to_sparql_json")["results"]["bindings"]
            .clone();
        assert_eq!(
            canon_decimal_bindings(&indexed_bindings),
            canon_decimal_bindings(&novelty_bindings),
            "inline-indexed results must match novelty results (by value + datatype) for query:\n{q}"
        );
    }
}

#[tokio::test]
async fn inline_decimal_order_by_and_range_are_numeric_after_reindex() {
    // Order-preserving inline decimal keys: ORDER BY and range filters on a
    // decimal predicate must use NUMERIC order, not scale-broken key order.
    // 0.05 vs 0.5 (different scales) and negatives are the cases the old
    // equality-keyed layout got wrong.
    let fluree = memory_fluree();
    let ledger_id = "decimal/inline-order:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    run_sparql_update(
        &fluree,
        ledger,
        r"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
            ex:a ex:v 0.5 .
            ex:b ex:v 0.05 .
            ex:c ex:v -1 .
            ex:d ex:v 2 .
            ex:e ex:v 19.99 .
            ex:f ex:v -0.01 .
            ex:g ex:v 1000.5 .
        }
        ",
    )
    .await;

    let root = full_rebuild_publish_decode_root(&fluree, ledger_id).await;
    assert_eq!(
        root.decimal_encoding(),
        fluree_db_core::DecimalEncoding::InlineWhenFits
    );
    let ledger = fluree
        .ledger(ledger_id)
        .await
        .expect("load reindexed ledger");

    // 1. Plain ORDER BY ascending — full numeric order across signs and scales.
    let asc = support::query_sparql(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?v WHERE { ?s ex:v ?v } ORDER BY ?v",
    )
    .await
    .expect("order by asc");
    let asc_json = asc.to_sparql_json(&ledger.snapshot).expect("json");
    assert_eq!(
        binding_values(&asc_json, "v"),
        vec!["-1", "-0.01", "0.05", "0.5", "2", "19.99", "1000.5"],
        "ORDER BY must be numeric (0.05 < 0.5, negatives first)"
    );

    // 2. ORDER BY DESC LIMIT — exercises the reverse-POST top-k fast path.
    let desc = support::query_sparql(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?v WHERE { ?s ex:v ?v } ORDER BY DESC(?v) LIMIT 3",
    )
    .await
    .expect("order by desc limit");
    let desc_json = desc.to_sparql_json(&ledger.snapshot).expect("json");
    assert_eq!(
        binding_values(&desc_json, "v"),
        vec!["1000.5", "19.99", "2"],
        "ORDER BY DESC LIMIT must return the numerically largest values"
    );

    // 3. SELECT with a range FILTER — numeric subset.
    let filtered = support::query_sparql(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?v WHERE { ?s ex:v ?v FILTER(?v > 0.1) } ORDER BY ?v",
    )
    .await
    .expect("range filter");
    let filtered_json = filtered.to_sparql_json(&ledger.snapshot).expect("json");
    assert_eq!(
        binding_values(&filtered_json, "v"),
        vec!["0.5", "2", "19.99", "1000.5"],
        "FILTER(?v > 0.1) must exclude 0.05 and the negatives"
    );

    // 4. COUNT with a range FILTER — exercises the numeric-compare COUNT fast path.
    let counted = support::query_sparql(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT (COUNT(?s) AS ?n) WHERE { ?s ex:v ?v FILTER(?v > 0.1) }",
    )
    .await
    .expect("count filter");
    let counted_json = counted.to_sparql_json(&ledger.snapshot).expect("json");
    assert_eq!(
        binding_values(&counted_json, "n"),
        vec!["4"],
        "COUNT over a decimal range filter must match the four values > 0.1"
    );

    // 5. COUNT with an integer threshold against decimal rows (cross-form).
    let counted_int = support::query_sparql(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT (COUNT(?s) AS ?n) WHERE { ?s ex:v ?v FILTER(?v >= 2) }",
    )
    .await
    .expect("count int threshold");
    let counted_int_json = counted_int.to_sparql_json(&ledger.snapshot).expect("json");
    assert_eq!(
        binding_values(&counted_int_json, "n"),
        vec!["3"],
        "FILTER(?v >= 2) over decimals must count 2, 19.99, 1000.5"
    );
}
