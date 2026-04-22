//! Conditional update integration tests.
//!
//! Tests "dependent on current database state" update patterns:
//! - Atomic increment/decrement
//! - Compare-and-swap (optimistic concurrency)
//! - State machine transitions
//! - Guarded updates (threshold / precondition)
//! - Atomic transfers (double-entry)
//! - Insert-if-not-exists (conditional create)
//! - Capped accumulator (increment with ceiling)
//! - Cascading / dependent updates (graph traversal)
//! - Batch conditional updates (multi-entity)

mod support;

use fluree_db_api::{FlureeBuilder, LedgerState, Novelty};
use fluree_db_core::LedgerSnapshot;
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({
        "id": "@id",
        "type": "@type",
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// Helper: build a memory-backed Fluree and seed it with initial data.
async fn seed(ledger_id: &str, seed_data: JsonValue) -> (fluree_db_api::Fluree, LedgerState) {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx(),
                "insert": { "@graph": seed_data }
            }),
        )
        .await
        .expect("seed data");

    (fluree, seeded.ledger)
}

/// Helper: query a single entity by @id and return all properties.
async fn query_entity(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    entity_id: &str,
) -> JsonValue {
    let q = json!({
        "@context": ctx(),
        "selectOne": { entity_id: ["*"] }
    });
    support::query_jsonld(fluree, ledger, &q)
        .await
        .expect("query entity")
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async")
}

// ============================================================================
// 1. Atomic Increment / Decrement
// ============================================================================

/// Atomic increment: read current value, add 1, write back — all in one transaction.
#[tokio::test]
async fn atomic_increment_counter() {
    let (fluree, ledger) = seed(
        "it/conditional:atomic-increment",
        json!([{ "id": "ex:counter", "ex:count": 10 }]),
    )
    .await;

    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "ex:counter", "ex:count": "?old" },
                    ["bind", "?new", "(+ ?old 1)"]
                ],
                "delete": { "id": "ex:counter", "ex:count": "?old" },
                "insert": { "id": "ex:counter", "ex:count": "?new" }
            }),
        )
        .await
        .expect("atomic increment");

    let result = query_entity(&fluree, &updated.ledger, "ex:counter").await;
    assert_eq!(result.get("ex:count"), Some(&json!(11)));
}

#[tokio::test]
async fn update_supports_subquery_aggregate_max_plus_one() {
    let (fluree, ledger) = seed(
        "it/conditional:max-plus-one",
        json!([
            { "id": "ex:item1", "ex:seq": 1 },
            { "id": "ex:item2", "ex:seq": 2 }
        ]),
    )
    .await;

    // Compute MAX(ex:seq) in a subquery, then bind next = max + 1 and insert it.
    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    ["query", {
                        "@context": ctx(),
                        "select": ["(as (max ?n) ?m)"],
                        "where": [{ "id": "?s", "ex:seq": "?n" }]
                    }],
                    ["bind", "?next", "(+ ?m 1)"]
                ],
                "insert": { "id": "ex:counter", "ex:next": "?next" }
            }),
        )
        .await
        .expect("max+1 update");

    let result = query_entity(&fluree, &updated.ledger, "ex:counter").await;
    assert_eq!(result.get("ex:next"), Some(&json!(3)));
}

/// Atomic decrement: subtract 1 from current value.
#[tokio::test]
async fn atomic_decrement_counter() {
    let (fluree, ledger) = seed(
        "it/conditional:atomic-decrement",
        json!([{ "id": "ex:counter", "ex:count": 10 }]),
    )
    .await;

    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "ex:counter", "ex:count": "?old" },
                    ["bind", "?new", "(- ?old 1)"]
                ],
                "delete": { "id": "ex:counter", "ex:count": "?old" },
                "insert": { "id": "ex:counter", "ex:count": "?new" }
            }),
        )
        .await
        .expect("atomic decrement");

    let result = query_entity(&fluree, &updated.ledger, "ex:counter").await;
    assert_eq!(result.get("ex:count"), Some(&json!(9)));
}

/// Increment by a custom amount (e.g., add 5 loyalty points).
#[tokio::test]
async fn atomic_increment_by_n() {
    let (fluree, ledger) = seed(
        "it/conditional:increment-by-n",
        json!([{ "id": "ex:user1", "ex:points": 100 }]),
    )
    .await;

    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "ex:user1", "ex:points": "?old" },
                    ["bind", "?new", "(+ ?old 50)"]
                ],
                "delete": { "id": "ex:user1", "ex:points": "?old" },
                "insert": { "id": "ex:user1", "ex:points": "?new" }
            }),
        )
        .await
        .expect("increment by N");

    let result = query_entity(&fluree, &updated.ledger, "ex:user1").await;
    assert_eq!(result.get("ex:points"), Some(&json!(150)));
}

// ============================================================================
// 2. Compare-and-Swap (Optimistic Concurrency)
// ============================================================================

/// Compare-and-swap: update succeeds only when the current value matches expected.
/// This is the foundation of optimistic concurrency control.
#[tokio::test]
async fn compare_and_swap_success() {
    let (fluree, ledger) = seed(
        "it/conditional:cas-success",
        json!([{ "id": "ex:item", "ex:version": 1, "ex:price": 19.99 }]),
    )
    .await;

    // Client read version=1, now wants to update price. Pins version=1 in WHERE.
    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": { "id": "ex:item", "ex:version": 1, "ex:price": "?oldPrice" },
                "delete": { "id": "ex:item", "ex:version": 1, "ex:price": "?oldPrice" },
                "insert": { "id": "ex:item", "ex:version": 2, "ex:price": 24.99 }
            }),
        )
        .await
        .expect("CAS success");

    let result = query_entity(&fluree, &updated.ledger, "ex:item").await;
    assert_eq!(result.get("ex:version"), Some(&json!(2)));
    assert_eq!(result.get("ex:price"), Some(&json!(24.99)));
}

/// Compare-and-swap: update is a no-op when version doesn't match (stale read).
#[tokio::test]
async fn compare_and_swap_stale_version_is_noop() {
    let (fluree, ledger) = seed(
        "it/conditional:cas-stale",
        json!([{ "id": "ex:item", "ex:version": 3, "ex:price": 19.99 }]),
    )
    .await;
    let t_before = ledger.t();

    // Client has stale version=1, but current is 3. WHERE won't match → no-op.
    //
    // Pattern: use a WHERE variable in the INSERT subject so that when WHERE
    // returns 0 rows, the variable is unbound and the INSERT produces 0 flakes.
    // All-literal INSERTs fire unconditionally (the "delete-if-exists, always
    // insert" pattern), so CAS must use a variable to be conditional.
    let result = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": { "id": "?s", "ex:version": 1, "ex:price": "?oldPrice" },
                "delete": { "id": "?s", "ex:version": 1, "ex:price": "?oldPrice" },
                "insert": { "id": "?s", "ex:version": 2, "ex:price": 24.99 }
            }),
        )
        .await
        .expect("CAS stale should succeed as no-op");

    // t should not change — no data was modified.
    assert_eq!(result.ledger.t(), t_before, "t should not bump on no-op");

    let item = query_entity(&fluree, &result.ledger, "ex:item").await;
    assert_eq!(item.get("ex:version"), Some(&json!(3)), "version unchanged");
    assert_eq!(item.get("ex:price"), Some(&json!(19.99)), "price unchanged");
}

// ============================================================================
// 3. State Machine Transitions
// ============================================================================

/// State machine: only transition from a valid source state.
#[tokio::test]
async fn state_machine_valid_transition() {
    let (fluree, ledger) = seed(
        "it/conditional:state-valid",
        json!([{ "id": "ex:order1", "ex:status": "pending", "ex:item": "Widget" }]),
    )
    .await;

    // pending → approved (valid)
    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": { "id": "ex:order1", "ex:status": "pending" },
                "delete": { "id": "ex:order1", "ex:status": "pending" },
                "insert": { "id": "ex:order1", "ex:status": "approved" }
            }),
        )
        .await
        .expect("valid state transition");

    let result = query_entity(&fluree, &updated.ledger, "ex:order1").await;
    assert_eq!(result.get("ex:status"), Some(&json!("approved")));
}

/// State machine: invalid transition is a no-op (order is "pending", not "shipped").
#[tokio::test]
async fn state_machine_invalid_transition_is_noop() {
    let (fluree, ledger) = seed(
        "it/conditional:state-invalid",
        json!([{ "id": "ex:order1", "ex:status": "pending", "ex:item": "Widget" }]),
    )
    .await;
    let t_before = ledger.t();

    // Try shipped → delivered, but current state is "pending" — no match → no-op.
    // Use variable subject so INSERT is conditional on WHERE matching.
    let result = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": { "id": "?s", "ex:status": "shipped" },
                "delete": { "id": "?s", "ex:status": "shipped" },
                "insert": { "id": "?s", "ex:status": "delivered" }
            }),
        )
        .await
        .expect("invalid transition should be no-op");

    assert_eq!(result.ledger.t(), t_before);
    let order = query_entity(&fluree, &result.ledger, "ex:order1").await;
    assert_eq!(order.get("ex:status"), Some(&json!("pending")));
}

/// State machine: multi-step transition chain.
#[tokio::test]
async fn state_machine_chain() {
    let (fluree, ledger) = seed(
        "it/conditional:state-chain",
        json!([{ "id": "ex:order1", "ex:status": "pending" }]),
    )
    .await;

    // pending → approved
    let step1 = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": { "id": "ex:order1", "ex:status": "pending" },
                "delete": { "id": "ex:order1", "ex:status": "pending" },
                "insert": { "id": "ex:order1", "ex:status": "approved" }
            }),
        )
        .await
        .expect("step 1");

    // approved → shipped
    let step2 = fluree
        .update(
            step1.ledger,
            &json!({
                "@context": ctx(),
                "where": { "id": "ex:order1", "ex:status": "approved" },
                "delete": { "id": "ex:order1", "ex:status": "approved" },
                "insert": { "id": "ex:order1", "ex:status": "shipped" }
            }),
        )
        .await
        .expect("step 2");

    // shipped → delivered
    let step3 = fluree
        .update(
            step2.ledger,
            &json!({
                "@context": ctx(),
                "where": { "id": "ex:order1", "ex:status": "shipped" },
                "delete": { "id": "ex:order1", "ex:status": "shipped" },
                "insert": { "id": "ex:order1", "ex:status": "delivered" }
            }),
        )
        .await
        .expect("step 3");

    let result = query_entity(&fluree, &step3.ledger, "ex:order1").await;
    assert_eq!(result.get("ex:status"), Some(&json!("delivered")));
}

// ============================================================================
// 4. Guarded Update (Threshold / Precondition)
// ============================================================================

/// Guarded update: only deduct if balance >= amount (prevents overdraft).
#[tokio::test]
async fn guarded_deduction_sufficient_balance() {
    let (fluree, ledger) = seed(
        "it/conditional:guard-sufficient",
        json!([{ "id": "ex:account", "ex:balance": 500 }]),
    )
    .await;

    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "ex:account", "ex:balance": "?bal" },
                    ["filter", "(>= ?bal 100)"],
                    ["bind", "?newBal", "(- ?bal 100)"]
                ],
                "delete": { "id": "ex:account", "ex:balance": "?bal" },
                "insert": { "id": "ex:account", "ex:balance": "?newBal" }
            }),
        )
        .await
        .expect("guarded deduction");

    let result = query_entity(&fluree, &updated.ledger, "ex:account").await;
    assert_eq!(result.get("ex:balance"), Some(&json!(400)));
}

/// Guarded update: deduction blocked when balance too low — no-op.
#[tokio::test]
async fn guarded_deduction_insufficient_balance_is_noop() {
    let (fluree, ledger) = seed(
        "it/conditional:guard-insufficient",
        json!([{ "id": "ex:account", "ex:balance": 50 }]),
    )
    .await;
    let t_before = ledger.t();

    let result = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "ex:account", "ex:balance": "?bal" },
                    ["filter", "(>= ?bal 100)"],
                    ["bind", "?newBal", "(- ?bal 100)"]
                ],
                "delete": { "id": "ex:account", "ex:balance": "?bal" },
                "insert": { "id": "ex:account", "ex:balance": "?newBal" }
            }),
        )
        .await
        .expect("guarded deduction no-op");

    assert_eq!(result.ledger.t(), t_before, "t should not bump");
    let acct = query_entity(&fluree, &result.ledger, "ex:account").await;
    assert_eq!(
        acct.get("ex:balance"),
        Some(&json!(50)),
        "balance unchanged"
    );
}

// ============================================================================
// 5. Atomic Transfer (Double-Entry)
// ============================================================================

/// Atomic transfer: move value from one entity to another in a single transaction.
#[tokio::test]
async fn atomic_transfer_between_accounts() {
    let (fluree, ledger) = seed(
        "it/conditional:transfer",
        json!([
            { "id": "ex:alice-acct", "ex:balance": 1000 },
            { "id": "ex:bob-acct",   "ex:balance": 200  }
        ]),
    )
    .await;

    // Transfer 150 from Alice to Bob, guarded by sufficient balance.
    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "ex:alice-acct", "ex:balance": "?aliceBal" },
                    { "id": "ex:bob-acct",   "ex:balance": "?bobBal"   },
                    ["filter", "(>= ?aliceBal 150)"],
                    ["bind", "?newAlice", "(- ?aliceBal 150)",
                             "?newBob",   "(+ ?bobBal 150)"]
                ],
                "delete": [
                    { "id": "ex:alice-acct", "ex:balance": "?aliceBal" },
                    { "id": "ex:bob-acct",   "ex:balance": "?bobBal"   }
                ],
                "insert": [
                    { "id": "ex:alice-acct", "ex:balance": "?newAlice" },
                    { "id": "ex:bob-acct",   "ex:balance": "?newBob"   }
                ]
            }),
        )
        .await
        .expect("atomic transfer");

    let alice = query_entity(&fluree, &updated.ledger, "ex:alice-acct").await;
    let bob = query_entity(&fluree, &updated.ledger, "ex:bob-acct").await;
    assert_eq!(alice.get("ex:balance"), Some(&json!(850)));
    assert_eq!(bob.get("ex:balance"), Some(&json!(350)));
}

/// Atomic transfer: blocked when sender has insufficient funds.
#[tokio::test]
async fn atomic_transfer_blocked_insufficient_funds() {
    let (fluree, ledger) = seed(
        "it/conditional:transfer-blocked",
        json!([
            { "id": "ex:alice-acct", "ex:balance": 30 },
            { "id": "ex:bob-acct",   "ex:balance": 200 }
        ]),
    )
    .await;
    let t_before = ledger.t();

    let result = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "ex:alice-acct", "ex:balance": "?aliceBal" },
                    { "id": "ex:bob-acct",   "ex:balance": "?bobBal"   },
                    ["filter", "(>= ?aliceBal 150)"],
                    ["bind", "?newAlice", "(- ?aliceBal 150)",
                             "?newBob",   "(+ ?bobBal 150)"]
                ],
                "delete": [
                    { "id": "ex:alice-acct", "ex:balance": "?aliceBal" },
                    { "id": "ex:bob-acct",   "ex:balance": "?bobBal"   }
                ],
                "insert": [
                    { "id": "ex:alice-acct", "ex:balance": "?newAlice" },
                    { "id": "ex:bob-acct",   "ex:balance": "?newBob"   }
                ]
            }),
        )
        .await
        .expect("transfer blocked");

    assert_eq!(result.ledger.t(), t_before);
}

// ============================================================================
// 6. Insert-If-Not-Exists (Conditional Create)
// ============================================================================

/// Insert-if-not-exists using OPTIONAL + FILTER(!bound(...)).
/// Creates entity only if it doesn't already exist.
#[tokio::test]
async fn insert_if_not_exists_creates_new() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = LedgerSnapshot::genesis("it/conditional:insert-if-not-exists-new");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    // Seed with just ex:alice — ex:bob does NOT exist.
    let seeded = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx(),
                "insert": { "id": "ex:alice", "schema:name": "Alice" }
            }),
        )
        .await
        .expect("seed");

    // Insert ex:bob only if not exists.
    let updated = fluree
        .update(
            seeded.ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    ["optional", { "id": "ex:bob", "schema:name": "?existing" }],
                    ["filter", "(not (bound ?existing))"]
                ],
                "insert": { "id": "ex:bob", "schema:name": "Bob", "schema:age": 25 }
            }),
        )
        .await
        .expect("insert-if-not-exists");

    let bob = query_entity(&fluree, &updated.ledger, "ex:bob").await;
    assert_eq!(bob.get("schema:name"), Some(&json!("Bob")));
    assert_eq!(bob.get("schema:age"), Some(&json!(25)));
}

/// Insert-if-not-exists: no-op when entity already exists.
#[tokio::test]
async fn insert_if_not_exists_noop_when_exists() {
    let (fluree, ledger) = seed(
        "it/conditional:insert-if-not-exists-exists",
        json!([{ "id": "ex:alice", "schema:name": "Alice", "schema:age": 30 }]),
    )
    .await;
    let t_before = ledger.t();

    // Try to insert ex:alice again — should be a no-op because she exists.
    // Use BIND to create variables for the insert values so the INSERT is
    // conditional on WHERE (FILTER) passing. All-literal INSERTs fire
    // unconditionally, so conditional creates must use WHERE variables.
    let result = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    ["optional", { "id": "ex:alice", "schema:name": "?existing" }],
                    ["filter", "(not (bound ?existing))"],
                    ["bind", "?newName", "\"Alice 2\"", "?newAge", "99"]
                ],
                "insert": { "id": "ex:alice", "schema:name": "?newName", "schema:age": "?newAge" }
            }),
        )
        .await
        .expect("insert-if-not-exists noop");

    assert_eq!(result.ledger.t(), t_before, "t should not bump");
    let alice = query_entity(&fluree, &result.ledger, "ex:alice").await;
    assert_eq!(
        alice.get("schema:name"),
        Some(&json!("Alice")),
        "name unchanged"
    );
}

// ============================================================================
// 7. Capped Accumulator (Increment with Ceiling)
// ============================================================================

/// Capped accumulator: increment points but cap at 1000.
#[tokio::test]
async fn capped_accumulator_below_cap() {
    let (fluree, ledger) = seed(
        "it/conditional:cap-below",
        json!([{ "id": "ex:user1", "ex:points": 900 }]),
    )
    .await;

    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "ex:user1", "ex:points": "?pts" },
                    ["filter", "(< ?pts 1000)"],
                    ["bind", "?new", "(if (> (+ ?pts 150) 1000) 1000 (+ ?pts 150))"]
                ],
                "delete": { "id": "ex:user1", "ex:points": "?pts" },
                "insert": { "id": "ex:user1", "ex:points": "?new" }
            }),
        )
        .await
        .expect("capped increment");

    let result = query_entity(&fluree, &updated.ledger, "ex:user1").await;
    // 900 + 150 = 1050 > 1000, so capped at 1000
    assert_eq!(result.get("ex:points"), Some(&json!(1000)));
}

/// Capped accumulator: no-op when already at cap.
#[tokio::test]
async fn capped_accumulator_already_at_cap_is_noop() {
    let (fluree, ledger) = seed(
        "it/conditional:cap-at-max",
        json!([{ "id": "ex:user1", "ex:points": 1000 }]),
    )
    .await;
    let t_before = ledger.t();

    let result = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "ex:user1", "ex:points": "?pts" },
                    ["filter", "(< ?pts 1000)"],
                    ["bind", "?new", "(if (> (+ ?pts 150) 1000) 1000 (+ ?pts 150))"]
                ],
                "delete": { "id": "ex:user1", "ex:points": "?pts" },
                "insert": { "id": "ex:user1", "ex:points": "?new" }
            }),
        )
        .await
        .expect("capped at max");

    assert_eq!(result.ledger.t(), t_before, "already at cap = no-op");
}

// ============================================================================
// 8. Cascading / Dependent Update (Graph Traversal)
// ============================================================================

/// Cascading update: update a derived field based on a related entity's value.
/// Reads from ex:order → ex:customer, accumulates lifetime spend.
#[tokio::test]
async fn cascading_update_lifetime_spend() {
    let (fluree, ledger) = seed(
        "it/conditional:cascade",
        json!([
            { "id": "ex:alice", "ex:lifetimeSpend": 500 },
            { "id": "ex:order1", "ex:customer": { "id": "ex:alice" }, "ex:total": 75 }
        ]),
    )
    .await;

    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "ex:order1", "ex:customer": "?cust", "ex:total": "?orderTotal" },
                    { "id": "?cust", "ex:lifetimeSpend": "?ls" },
                    ["bind", "?newLs", "(+ ?ls ?orderTotal)"]
                ],
                "delete": { "id": "?cust", "ex:lifetimeSpend": "?ls" },
                "insert": { "id": "?cust", "ex:lifetimeSpend": "?newLs" }
            }),
        )
        .await
        .expect("cascading update");

    let alice = query_entity(&fluree, &updated.ledger, "ex:alice").await;
    assert_eq!(alice.get("ex:lifetimeSpend"), Some(&json!(575)));
}

// ============================================================================
// 9. Batch Conditional Update (Multi-Entity)
// ============================================================================

/// Batch update: give every user in a department a raise.
/// Uses string department name for straightforward WHERE matching.
#[tokio::test]
async fn batch_conditional_salary_raise() {
    let (fluree, ledger) = seed(
        "it/conditional:batch-raise",
        json!([
            { "id": "ex:emp1", "ex:dept": "engineering", "ex:salary": 100 },
            { "id": "ex:emp2", "ex:dept": "engineering", "ex:salary": 200 },
            { "id": "ex:emp3", "ex:dept": "sales",       "ex:salary": 150 }
        ]),
    )
    .await;

    // Give all engineering employees a raise of salary/10 (10%).
    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "?emp", "ex:dept": "engineering", "ex:salary": "?sal" },
                    ["bind", "?newSal", "(+ ?sal (/ ?sal 10))"]
                ],
                "delete": { "id": "?emp", "ex:salary": "?sal" },
                "insert": { "id": "?emp", "ex:salary": "?newSal" }
            }),
        )
        .await
        .expect("batch raise");

    let emp1 = query_entity(&fluree, &updated.ledger, "ex:emp1").await;
    let emp2 = query_entity(&fluree, &updated.ledger, "ex:emp2").await;
    let emp3 = query_entity(&fluree, &updated.ledger, "ex:emp3").await;

    // Engineering employees get 10% raise: 100→110, 200→220
    assert_eq!(emp1.get("ex:salary"), Some(&json!(110)));
    assert_eq!(emp2.get("ex:salary"), Some(&json!(220)));
    // Sales employee unchanged
    assert_eq!(emp3.get("ex:salary"), Some(&json!(150)));
}

/// Batch update: apply status change to all entities matching a condition.
#[tokio::test]
async fn batch_conditional_status_change() {
    let (fluree, ledger) = seed(
        "it/conditional:batch-status",
        json!([
            { "id": "ex:task1", "ex:status": "pending", "ex:priority": "high" },
            { "id": "ex:task2", "ex:status": "pending", "ex:priority": "low"  },
            { "id": "ex:task3", "ex:status": "done",    "ex:priority": "high" }
        ]),
    )
    .await;

    // Approve all pending tasks (regardless of priority).
    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": { "id": "?task", "ex:status": "pending" },
                "delete": { "id": "?task", "ex:status": "pending" },
                "insert": { "id": "?task", "ex:status": "approved" }
            }),
        )
        .await
        .expect("batch status change");

    let t1 = query_entity(&fluree, &updated.ledger, "ex:task1").await;
    let t2 = query_entity(&fluree, &updated.ledger, "ex:task2").await;
    let t3 = query_entity(&fluree, &updated.ledger, "ex:task3").await;

    assert_eq!(t1.get("ex:status"), Some(&json!("approved")));
    assert_eq!(t2.get("ex:status"), Some(&json!("approved")));
    // task3 was "done", not "pending" — unchanged.
    assert_eq!(t3.get("ex:status"), Some(&json!("done")));
}

// ============================================================================
// 10. Computed Value with Preserved History
// ============================================================================

/// Update a value and simultaneously record the old value for auditing.
#[tokio::test]
async fn update_with_audit_trail() {
    let (fluree, ledger) = seed(
        "it/conditional:audit-trail",
        json!([{ "id": "ex:product", "ex:price": 100 }]),
    )
    .await;

    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "ex:product", "ex:price": "?oldPrice" },
                    ["bind", "?newPrice", "(- ?oldPrice 10)"]
                ],
                "delete": { "id": "ex:product", "ex:price": "?oldPrice" },
                "insert": {
                    "id": "ex:product",
                    "ex:price": "?newPrice",
                    "ex:previousPrice": "?oldPrice"
                }
            }),
        )
        .await
        .expect("update with audit");

    let product = query_entity(&fluree, &updated.ledger, "ex:product").await;
    assert_eq!(product.get("ex:price"), Some(&json!(90)));
    assert_eq!(product.get("ex:previousPrice"), Some(&json!(100)));
}

// ============================================================================
// 11. Percentage-Based Update
// ============================================================================

/// Apply a percentage increase using multiplication in BIND.
#[tokio::test]
async fn percentage_based_update() {
    let (fluree, ledger) = seed(
        "it/conditional:percentage",
        json!([{ "id": "ex:stock", "ex:quantity": 200, "ex:reorderLevel": 50 }]),
    )
    .await;

    // Double the reorder level
    let updated = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [
                    { "id": "ex:stock", "ex:reorderLevel": "?level" },
                    ["bind", "?newLevel", "(* ?level 2)"]
                ],
                "delete": { "id": "ex:stock", "ex:reorderLevel": "?level" },
                "insert": { "id": "ex:stock", "ex:reorderLevel": "?newLevel" }
            }),
        )
        .await
        .expect("percentage update");

    let stock = query_entity(&fluree, &updated.ledger, "ex:stock").await;
    assert_eq!(stock.get("ex:reorderLevel"), Some(&json!(100)));
}
