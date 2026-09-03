//! The resource bounds every other read surface already has.
//!
//! A GraphQL document decides its own shape: a derived schema is cyclic
//! wherever one class references another, so nesting depth is the caller's
//! choice, and root fields resolve concurrently so aliases multiply whatever
//! one field costs. Two ceilings apply — cancellation (shared by every query a
//! request runs) and the document limits.

use crate::support::{genesis_ledger, MemoryFluree};
use fluree_db_api::graphql::{GraphQlRequest, Limits};
use fluree_db_api::{FlureeBuilder, GraphDb, LedgerState, QueryExecutionOptions};
use fluree_db_core::{QueryCancellation, QueryCancellationReason};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/";

fn context() -> JsonValue {
    json!({ "ex": EX })
}

/// Alice knows Bob and Bob knows Alice, so `knows` is a cycle the schema
/// exposes and a document can descend without limit.
async fn seed(fluree: &MemoryFluree, ledger_id: &str) -> LedgerState {
    let ledger = genesis_ledger(fluree, ledger_id);
    fluree
        .insert(
            ledger,
            &json!({
                "@context": context(),
                "@graph": [
                    {
                        "@id": "ex:alice",
                        "@type": "ex:Person",
                        "ex:name": "Alice",
                        "ex:knows": [{ "@id": "ex:bob" }]
                    },
                    {
                        "@id": "ex:bob",
                        "@type": "ex:Person",
                        "ex:name": "Bob",
                        "ex:knows": [{ "@id": "ex:alice" }]
                    }
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger
}

fn view(ledger: &LedgerState) -> GraphDb {
    GraphDb::from_ledger_state(ledger).with_default_context(Some(context()))
}

/// `{ persons { knows { knows { … name } } } }` nested `levels` deep.
fn nested(levels: usize) -> String {
    let mut q = String::from("name");
    for _ in 0..levels {
        q = format!("knows {{ {q} }}");
    }
    format!("{{ persons {{ {q} }} }}")
}

fn errors(response: &JsonValue) -> String {
    response["errors"]
        .as_array()
        .map(|e| {
            e.iter()
                .filter_map(|v| v["message"].as_str())
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default()
}

// ── Cancellation reaches the lowered query (#1) ──────────────────────────────

/// The blocking gap: `Fluree::graphql` ran every lowered query with
/// `QueryExecutionOptions::default()`, so the cancellation handle a server
/// installs for `/v1/fluree/query` could not reach a GraphQL read. An
/// already-cancelled handle is the cheapest proof that it now does — if the
/// options were dropped on the floor, this query would simply succeed.
#[tokio::test]
async fn a_cancelled_handle_stops_a_graphql_read() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-cancel-read:main").await;

    let cancellation = QueryCancellation::new();
    cancellation.cancel_with(QueryCancellationReason::Timeout);
    let options = QueryExecutionOptions::new().with_cancellation(cancellation);

    let response = fluree
        .graphql_with_options(
            &view(&ledger),
            &GraphQlRequest::new("{ persons { id name } }"),
            options,
        )
        .await
        .expect("graphql request");

    assert!(
        response.get("errors").is_some(),
        "a cancelled request must not return data: {response}"
    );
}

/// The same handle has to cover *all* of a document's root fields, not the
/// first one: async-graphql resolves them concurrently, which is what turns one
/// document into N queries.
#[tokio::test]
async fn cancellation_covers_every_aliased_root_field() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-cancel-aliases:main").await;

    let cancellation = QueryCancellation::new();
    cancellation.cancel_with(QueryCancellationReason::ClientDisconnected);
    let options = QueryExecutionOptions::new().with_cancellation(cancellation);

    let document = "{ a: persons { id } b: persons { id } c: persons { id } }";
    let response = fluree
        .graphql_with_options(&view(&ledger), &GraphQlRequest::new(document), options)
        .await
        .expect("graphql request");

    for alias in ["a", "b", "c"] {
        assert!(
            response["data"][alias].as_array().is_none_or(Vec::is_empty),
            "`{alias}` produced rows under a cancelled handle: {response}"
        );
    }
    assert!(
        response.get("errors").is_some(),
        "a cancelled request must not report success: {response}"
    );
}

/// An uncancelled handle must not change the answer: the plumbing is a ceiling,
/// not a filter.
#[tokio::test]
async fn an_uncancelled_handle_answers_normally() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-cancel-clean:main").await;

    let options = QueryExecutionOptions::new().with_cancellation(QueryCancellation::new());
    let response = fluree
        .graphql_with_options(
            &view(&ledger),
            &GraphQlRequest::new("{ persons { id name } }"),
            options,
        )
        .await
        .expect("graphql request");

    assert!(response.get("errors").is_none(), "{response}");
    assert_eq!(response["data"]["persons"].as_array().unwrap().len(), 2);
}

// ── Document limits (#2) ─────────────────────────────────────────────────────

#[tokio::test]
async fn a_document_deeper_than_the_limit_is_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-depth:main").await;

    let limits = Limits {
        max_depth: 4,
        ..Limits::default()
    };
    let response = fluree
        .graphql(
            &view(&ledger),
            &GraphQlRequest::new(nested(10)).with_limits(limits),
        )
        .await
        .expect("graphql request");

    let message = errors(&response);
    assert!(
        message.contains("nests deeper"),
        "expected a depth refusal, got: {response}"
    );
}

/// Depth counts fields, the way async-graphql counts them: the root field is
/// level 1 and a leaf is a level of its own, so `nested(2)` — `persons`, two
/// `knows`, `name` — sits exactly on a limit of 4. Counting differently here
/// than the schema does would refuse documents the schema would accept.
#[tokio::test]
async fn a_document_at_the_limit_is_accepted() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-depth-boundary:main").await;

    let limits = Limits {
        max_depth: 4,
        ..Limits::default()
    };
    let response = fluree
        .graphql(
            &view(&ledger),
            &GraphQlRequest::new(nested(2)).with_limits(limits),
        )
        .await
        .expect("graphql request");

    assert!(
        response.get("errors").is_none(),
        "a document exactly at the limit must be accepted: {response}"
    );

    // One level further is refused, so the limit is the boundary and not a
    // number the walk happens to stay under.
    let over = fluree
        .graphql(
            &view(&ledger),
            &GraphQlRequest::new(nested(3)).with_limits(limits),
        )
        .await
        .expect("graphql request");
    assert!(
        over.get("errors").is_some(),
        "one level past the limit must be refused: {over}"
    );
}

/// An inline fragment or a spread is flattened into the level that holds it, so
/// counting it as a level would refuse a document that selects nothing deeper
/// than a permitted one.
#[tokio::test]
async fn fragments_do_not_count_as_nesting() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-depth-fragments:main").await;

    let document = "{ persons { ... on Person { ...F } } } fragment F on Person { id name }";
    let limits = Limits {
        max_depth: 2,
        ..Limits::default()
    };
    let response = fluree
        .graphql(
            &view(&ledger),
            &GraphQlRequest::new(document).with_limits(limits),
        )
        .await
        .expect("graphql request");

    assert!(
        response.get("errors").is_none(),
        "fragments were counted as depth: {response}"
    );
}

#[tokio::test]
async fn an_alias_fan_out_past_the_complexity_limit_is_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-complexity:main").await;

    let document = format!(
        "{{ {} }}",
        (0..200)
            .map(|i| format!("a{i}: persons {{ id name }}"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    let limits = Limits {
        max_complexity: 50,
        ..Limits::default()
    };
    let response = fluree
        .graphql(
            &view(&ledger),
            &GraphQlRequest::new(document).with_limits(limits),
        )
        .await
        .expect("graphql request");

    assert!(
        response.get("errors").is_some(),
        "a 200-alias document must not run under a 50-field budget: {response}"
    );
    assert!(
        response["data"].get("a0").is_none(),
        "the document must be refused before any field resolves: {response}"
    );
}

/// The limits are baked into the registered schema, which is cached — so they
/// have to be part of the cache key. Otherwise the first request through would
/// decide the ceiling for every later one.
///
/// Complexity is the knob that proves it: depth is refused during extraction,
/// before the schema is even consulted, so only a complexity limit can show
/// that the *cached schema* carries the right bound.
#[tokio::test]
async fn a_tighter_limit_is_not_served_a_looser_cached_schema() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-limit-cache:main").await;
    let db = view(&ledger);
    let document = "{ persons { id name knows { id name } } }";

    // Populate the cache with a permissive schema first: the ordering is the point.
    let loose = fluree
        .graphql(&db, &GraphQlRequest::new(document))
        .await
        .expect("graphql request");
    assert!(loose.get("errors").is_none(), "{loose}");

    let tight = fluree
        .graphql(
            &db,
            &GraphQlRequest::new(document).with_limits(Limits {
                max_complexity: 3,
                ..Limits::default()
            }),
        )
        .await
        .expect("graphql request");
    assert!(
        tight.get("errors").is_some(),
        "the tighter request was served the cached permissive schema: {tight}"
    );

    // And back the other way: the tight derivation must not have replaced the
    // entry the permissive request reads.
    let loose_again = fluree
        .graphql(&db, &GraphQlRequest::new(document))
        .await
        .expect("graphql request");
    assert!(
        loose_again.get("errors").is_none(),
        "a tight request's schema was cached over the permissive one: {loose_again}"
    );
}

/// The stack guard, which is the one limit that cannot run after parsing.
///
/// `async_graphql_parser` counts recursion while *building the AST*, but pest
/// has already descended the grammar with no limit of its own by then — so a
/// document a few hundred KB long overflows the stack and aborts the process.
/// An abort is not catchable, so this test passing at all is the assertion: the
/// document has to be refused before the parser sees it.
#[tokio::test]
async fn a_document_deep_enough_to_overflow_the_parser_is_refused_unparsed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-parser-stack:main").await;

    // 10x past what was measured to abort the process when parsed.
    let document = format!(
        "{}name{}",
        "{ persons ".repeat(100_000),
        "}".repeat(100_000)
    );
    let response = fluree
        .graphql(&view(&ledger), &GraphQlRequest::new(document.clone()))
        .await
        .expect("graphql request");

    assert!(
        errors(&response).contains("nests more than"),
        "expected a pre-parse refusal, got: {response}"
    );

    // The route asks `is_mutation` first, which parses too — it must not be the
    // hole the guard leaves open.
    assert!(
        !fluree_db_api::graphql::is_mutation(&GraphQlRequest::new(document)),
        "is_mutation parsed a document the guard refuses"
    );
}

/// Defaults are what an endpoint gets when nobody configures anything, so they
/// have to be real numbers rather than "unlimited" spelled differently.
#[tokio::test]
async fn the_default_limits_refuse_a_pathological_document() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "gql-defaults:main").await;

    let response = fluree
        .graphql(&view(&ledger), &GraphQlRequest::new(nested(64)))
        .await
        .expect("graphql request");

    assert!(
        response.get("errors").is_some(),
        "the default limits let a 64-deep document through: {response}"
    );
}
