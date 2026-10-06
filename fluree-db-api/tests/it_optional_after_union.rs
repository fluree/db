//! `OPTIONAL` over a shared variable the required row left UNBOUND (SPARQL 1.1
//! §18.2.4 `LeftJoin`).
//!
//! ```sparql
//! SELECT ?s ?f WHERE {
//!   ?s schema:name ?name .
//!   { { ?s ex:friend ?f } UNION { ?s schema:age ?age } }
//!   OPTIONAL { ?s ex:friend ?f }
//! }
//! ```
//!
//! The UNION emits ten solutions: six that bind `?f` (branch one) and four that
//! leave it unbound (branch two). A solution with `?f` unbound IS compatible
//! with one that binds it, so the left join must EXTEND those four rows rather
//! than pass them through — thirteen rows, not ten.
//!
//! The merge that fixes this asks only whether the required row's column is
//! `Binding::Unbound`, never how it got that way, so the same tests cover the
//! other way a variable arrives unbound: a `VALUES` row with an `UNDEF` cell,
//! which lowers to exactly that binding and has no `UNION` anywhere.
//!
//! The property that cannot be satisfied by accident is the second assertion in
//! each test: deleting the `OPTIONAL` clause outright must change the answer.
//! Before the fix the two queries returned byte-identical rows, which is the
//! sharpest possible statement of a left join contributing nothing.
//!
//! Every lane is covered on purpose, because they reach the OPTIONAL through
//! different code and failed differently: the novelty lane's per-row
//! substituted scan plus result cache, the indexed lane's batched subject
//! probe, `PlanTreeOptionalBuilder`'s correlation-key partition for a
//! multi-pattern inner, and `GroupedPatternOptionalBuilder`'s per-subject star
//! probe for a chain of single-triple OPTIONALs. SPARQL and JSON-LD share the
//! IR, so the surfaces are pinned in pairs.
//!
//! The same left join over a variable an earlier `OPTIONAL` failed to bind
//! (#1734) is covered in the last section: SPARQL and JSON-LD now leave that
//! variable `Unbound` rather than `Binding::Poisoned`, so it reaches exactly the
//! merge exercised here.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{
    FlureeBuilder, IndexConfig, LedgerManagerConfig, LedgerState, QueryInput, QueryResult,
};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::{json, Value};
use support::{
    genesis_ledger_for_fluree, graphdb_from_ledger, normalize_rows, start_background_indexer_local,
    trigger_index_and_wait_outcome, wait_for_index_application,
};

/// `seed_values_dataset` from `it_query_values.rs`, verbatim — the fixture the
/// issue's row counts were measured on.
fn seed_data() -> Value {
    json!({
        "@context": {
            "schema": "http://schema.org/",
            "ex": "http://example.com/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {"@id":"ex:brian","schema:name":"Brian","schema:email":"brian@example.org","schema:age":50,"ex:favNums":7},
            {"@id":"ex:alice","schema:name":"Alice","schema:email":"alice@example.org","schema:age":50,"ex:favNums":[42,76,9],"ex:friend":[{"@id":"ex:brian"}]},
            {"@id":"ex:cam","schema:name":"Cam","schema:email":"cam@example.org","schema:age":34,"ex:favNums":[5,10],"ex:friend":[{"@id":"ex:alice"},{"@id":"ex:brian"}]},
            {"@id":"ex:liam","schema:name":"Liam","schema:email":"liam@example.org","schema:age":13,"ex:favNums":[42,11],"ex:friend":[{"@id":"ex:alice"},{"@id":"ex:brian"},{"@id":"ex:cam"}]},
            {"@id":"ex:nikola",
             "schema:name":"Nikola",
             "ex:greeting":[{"@value":"Здраво","@language":"sb"},{"@value":"Hello","@language":"en"}],
             "ex:birthday":{"@value":"2000-01-01","@type":"xsd:datetime"},
             "ex:cool":true}
        ]
    })
}

const PREFIXES: &str = "PREFIX ex: <http://example.com/>\n\
     PREFIX schema: <http://schema.org/>\n";

const UNION_THEN_OPTIONAL: &str = "SELECT ?s ?f WHERE { \
     ?s schema:name ?name . \
     { { ?s ex:friend ?f } UNION { ?s schema:age ?age } } \
     OPTIONAL { ?s ex:friend ?f } }";

/// The same query with the `OPTIONAL` clause deleted.
const UNION_ONLY: &str = "SELECT ?s ?f WHERE { \
     ?s schema:name ?name . \
     { { ?s ex:friend ?f } UNION { ?s schema:age ?age } } }";

fn query_context() -> Value {
    json!({
        "schema": "http://schema.org/",
        "ex": "http://example.com/"
    })
}

fn jsonld_union_then_optional() -> Value {
    json!({
        "@context": query_context(),
        "select": ["?s", "?f"],
        "where": [
            {"@id": "?s", "schema:name": "?name"},
            ["union",
                {"@id": "?s", "ex:friend": "?f"},
                {"@id": "?s", "schema:age": "?age"}
            ],
            ["optional", {"@id": "?s", "ex:friend": "?f"}]
        ]
    })
}

fn jsonld_union_only() -> Value {
    json!({
        "@context": query_context(),
        "select": ["?s", "?f"],
        "where": [
            {"@id": "?s", "schema:name": "?name"},
            ["union",
                {"@id": "?s", "ex:friend": "?f"},
                {"@id": "?s", "schema:age": "?age"}
            ]
        ]
    })
}

/// The ten UNION solutions: six bind `?f`, four leave it unbound. `ex:nikola`
/// has a name but neither `ex:friend` nor `schema:age`, so neither branch
/// admits it.
fn expected_union_only() -> Vec<Value> {
    normalize_rows(&json!([
        ["ex:alice", "ex:brian"],
        ["ex:cam", "ex:alice"],
        ["ex:cam", "ex:brian"],
        ["ex:liam", "ex:alice"],
        ["ex:liam", "ex:brian"],
        ["ex:liam", "ex:cam"],
        ["ex:brian", null],
        ["ex:alice", null],
        ["ex:cam", null],
        ["ex:liam", null]
    ]))
}

/// §18.2.4 over those ten. The six `?f`-bound rows join to their own friend
/// triple and come back unchanged. Of the four unbound rows, `ex:brian` has no
/// friends and passes through unbound, while `ex:alice`/`ex:cam`/`ex:liam` are
/// extended by their 1/2/3 friends — 6 + 1 + 1 + 2 + 3 = 13.
fn expected_union_then_optional() -> Vec<Value> {
    normalize_rows(&json!([
        ["ex:alice", "ex:brian"],
        ["ex:cam", "ex:alice"],
        ["ex:cam", "ex:brian"],
        ["ex:liam", "ex:alice"],
        ["ex:liam", "ex:brian"],
        ["ex:liam", "ex:cam"],
        ["ex:brian", null],
        ["ex:alice", "ex:brian"],
        ["ex:cam", "ex:alice"],
        ["ex:cam", "ex:brian"],
        ["ex:liam", "ex:alice"],
        ["ex:liam", "ex:brian"],
        ["ex:liam", "ex:cam"]
    ]))
}

fn rows(result: &QueryResult, snapshot: &fluree_db_core::LedgerSnapshot) -> Vec<Value> {
    normalize_rows(&result.to_jsonld(snapshot).expect("to_jsonld"))
}

/// Assert both the exact row multiset and the delete-the-clause property.
fn assert_left_join_contributes(lane: &str, with_optional: &[Value], without_optional: &[Value]) {
    assert_eq!(
        without_optional,
        expected_union_only(),
        "{lane}: the UNION alone must still be the ten solutions the left join starts from"
    );
    assert_ne!(
        with_optional, without_optional,
        "{lane}: deleting the OPTIONAL must change the answer — identical rows mean the left \
         join contributed nothing"
    );
    assert_eq!(
        with_optional,
        expected_union_then_optional(),
        "{lane}: OPTIONAL must extend the four rows the UNION left ?f unbound on"
    );
}

async fn seed_novelty(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> LedgerState {
    let ledger = genesis_ledger_for_fluree(fluree, ledger_id);
    fluree
        .insert(ledger, &seed_data())
        .await
        .expect("seed insert")
        .ledger
}

#[tokio::test]
async fn sparql_optional_extends_union_unbound_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:sparql").await;
    let db = graphdb_from_ledger(&ledger);

    let with_optional = fluree
        .query(
            &db,
            QueryInput::Sparql(&format!("{PREFIXES}{UNION_THEN_OPTIONAL}")),
        )
        .await
        .expect("query with OPTIONAL");
    let without_optional = fluree
        .query(&db, QueryInput::Sparql(&format!("{PREFIXES}{UNION_ONLY}")))
        .await
        .expect("query without OPTIONAL");

    assert_left_join_contributes(
        "sparql/novelty",
        &rows(&with_optional, &ledger.snapshot),
        &rows(&without_optional, &ledger.snapshot),
    );
}

#[tokio::test]
async fn jsonld_optional_extends_union_unbound_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:jsonld").await;

    let with_optional = support::query_jsonld(&fluree, &ledger, &jsonld_union_then_optional())
        .await
        .expect("query with OPTIONAL");
    let without_optional = support::query_jsonld(&fluree, &ledger, &jsonld_union_only())
        .await
        .expect("query without OPTIONAL");

    assert_left_join_contributes(
        "json-ld/novelty",
        &rows(&with_optional, &ledger.snapshot),
        &rows(&without_optional, &ledger.snapshot),
    );
}

/// The merge is not specific to the object position. Here the UNION's second
/// branch binds neither `?s` nor `?f`, so the single solution it contributes is
/// compatible with every `ex:friend` triple and the left join extends it into
/// all six — the six `?f`-bound rows plus six more, twelve in all.
#[tokio::test]
async fn optional_extends_a_row_with_no_correlation_bound_at_all() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:free").await;
    let db = graphdb_from_ledger(&ledger);

    let query = "SELECT ?s ?f WHERE { \
         { { ?s ex:friend ?f } UNION { ex:nikola schema:name ?nn } } \
         OPTIONAL { ?s ex:friend ?f } }";

    let result = fluree
        .query(&db, QueryInput::Sparql(&format!("{PREFIXES}{query}")))
        .await
        .expect("query");

    assert_eq!(
        rows(&result, &ledger.snapshot),
        normalize_rows(&json!([
            ["ex:alice", "ex:brian"],
            ["ex:cam", "ex:alice"],
            ["ex:cam", "ex:brian"],
            ["ex:liam", "ex:alice"],
            ["ex:liam", "ex:brian"],
            ["ex:liam", "ex:cam"],
            ["ex:alice", "ex:brian"],
            ["ex:cam", "ex:alice"],
            ["ex:cam", "ex:brian"],
            ["ex:liam", "ex:alice"],
            ["ex:liam", "ex:brian"],
            ["ex:liam", "ex:cam"]
        ]))
    );
}

/// A two-pattern OPTIONAL routes to `PlanTreeOptionalBuilder` instead of the
/// single-triple builder, and its batched lane partitions inner results by a
/// correlation key an unbound variable has no value for. Every friend here has
/// a `schema:name`, so the second pattern removes no solutions and the answer
/// is the same thirteen rows.
#[tokio::test]
async fn multi_pattern_optional_extends_union_unbound_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:multi").await;
    let db = graphdb_from_ledger(&ledger);

    let multi_pattern = "SELECT ?s ?f WHERE { \
         ?s schema:name ?name . \
         { { ?s ex:friend ?f } UNION { ?s schema:age ?age } } \
         OPTIONAL { ?s ex:friend ?f . ?f schema:name ?fname } }";

    let with_optional = fluree
        .query(
            &db,
            QueryInput::Sparql(&format!("{PREFIXES}{multi_pattern}")),
        )
        .await
        .expect("query with multi-pattern OPTIONAL");
    let without_optional = fluree
        .query(&db, QueryInput::Sparql(&format!("{PREFIXES}{UNION_ONLY}")))
        .await
        .expect("query without OPTIONAL");

    assert_left_join_contributes(
        "sparql/multi-pattern",
        &rows(&with_optional, &ledger.snapshot),
        &rows(&without_optional, &ledger.snapshot),
    );
}

/// The indexed lane reaches the OPTIONAL through `build_batch`'s subject probe
/// rather than the per-row substituted scan, so it needs its own coverage —
/// a novelty-only pin would have gone green on a half fix.
#[tokio::test]
async fn indexed_optional_extends_union_unbound_rows() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "optional-after-union:indexed";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let ledger = fluree
                .insert_with_opts(
                    ledger,
                    &seed_data(),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;

            trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            wait_for_index_application(&fluree, ledger_id, ledger.t()).await;
            let view = fluree.db(ledger_id).await.expect("indexed view");
            // Lane guard. Post-fix this shape declines the batched probe, so
            // without this a lost indexer race would silently downgrade the
            // whole test to a second novelty-lane assertion — which passes on
            // the unfixed engine too.
            assert!(
                view.binary_store().is_some(),
                "the indexed lane needs a binary store on the view; without one this test is a \
                 duplicate of the novelty-lane one"
            );

            let with_optional = fluree
                .query(
                    &view,
                    QueryInput::Sparql(&format!("{PREFIXES}{UNION_THEN_OPTIONAL}")),
                )
                .await
                .expect("query with OPTIONAL");
            let without_optional = fluree
                .query(
                    &view,
                    QueryInput::Sparql(&format!("{PREFIXES}{UNION_ONLY}")),
                )
                .await
                .expect("query without OPTIONAL");

            assert_left_join_contributes(
                "sparql/indexed",
                &rows(&with_optional, &view.snapshot),
                &rows(&without_optional, &view.snapshot),
            );
        })
        .await;
}

// ---------------------------------------------------------------------------
// `VALUES … UNDEF`: the same unbound column, reached without a UNION.
// ---------------------------------------------------------------------------

/// The five named subjects, with `?f` supplied as `UNDEF` on every row.
const VALUES_UNDEF: &str = "VALUES (?s ?f) { \
     (ex:alice UNDEF) (ex:cam UNDEF) (ex:liam UNDEF) \
     (ex:brian UNDEF) (ex:nikola UNDEF) } ";

/// The same left join with no `VALUES` at all — the answer the `UNDEF` column
/// must not change.
const OPTIONAL_ONLY: &str = "SELECT ?s ?f WHERE { \
     ?s schema:name ?name . \
     OPTIONAL { ?s ex:friend ?f } }";

/// `ex:alice` has one friend, `ex:cam` two, `ex:liam` three; `ex:brian` and
/// `ex:nikola` have none and pass through unbound. 1 + 2 + 3 + 1 + 1 = 8.
fn expected_friends_left_join() -> Vec<Value> {
    normalize_rows(&json!([
        ["ex:alice", "ex:brian"],
        ["ex:cam", "ex:alice"],
        ["ex:cam", "ex:brian"],
        ["ex:liam", "ex:alice"],
        ["ex:liam", "ex:brian"],
        ["ex:liam", "ex:cam"],
        ["ex:brian", null],
        ["ex:nikola", null]
    ]))
}

fn jsonld_optional_only() -> Value {
    json!({
        "@context": query_context(),
        "select": ["?s", "?f"],
        "where": [
            {"@id": "?s", "schema:name": "?name"},
            ["optional", {"@id": "?s", "ex:friend": "?f"}]
        ]
    })
}

fn jsonld_values_undef_then_optional() -> Value {
    json!({
        "@context": query_context(),
        "select": ["?s", "?f"],
        "where": [
            {"@id": "?s", "schema:name": "?name"},
            ["values", [["?s", "?f"], [
                [{"@type": "@id", "@value": "ex:alice"}, null],
                [{"@type": "@id", "@value": "ex:cam"}, null],
                [{"@type": "@id", "@value": "ex:liam"}, null],
                [{"@type": "@id", "@value": "ex:brian"}, null],
                [{"@type": "@id", "@value": "ex:nikola"}, null]
            ]]],
            ["optional", {"@id": "?s", "ex:friend": "?f"}]
        ]
    })
}

/// Assert the exact row multiset, plus the property that the `UNDEF` column
/// changed nothing: an `UNDEF` cell is a variable the row leaves unbound, so
/// the left join must produce the same solutions as if the `VALUES` were not
/// there at all. Before the fix this returned eight rows with `?f` null on
/// every one — the friends were found and then thrown away.
fn assert_undef_column_is_transparent(lane: &str, with_values: &[Value], without_values: &[Value]) {
    assert_eq!(
        without_values,
        expected_friends_left_join(),
        "{lane}: the plain left join must be the eight solutions the UNDEF form is measured \
         against"
    );
    assert_eq!(
        with_values, without_values,
        "{lane}: an UNDEF cell leaves ?f unbound, so OPTIONAL must extend it exactly as it does \
         when no VALUES clause is present"
    );
    assert_eq!(
        with_values,
        expected_friends_left_join(),
        "{lane}: OPTIONAL must extend the rows VALUES left ?f unbound on"
    );
}

#[tokio::test]
async fn sparql_optional_extends_values_undef_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:values-undef").await;
    let db = graphdb_from_ledger(&ledger);

    let with_undef = format!(
        "SELECT ?s ?f WHERE {{ ?s schema:name ?name . {VALUES_UNDEF} \
         OPTIONAL {{ ?s ex:friend ?f }} }}"
    );

    let with_values = fluree
        .query(&db, QueryInput::Sparql(&format!("{PREFIXES}{with_undef}")))
        .await
        .expect("query with VALUES … UNDEF");
    let without_values = fluree
        .query(
            &db,
            QueryInput::Sparql(&format!("{PREFIXES}{OPTIONAL_ONLY}")),
        )
        .await
        .expect("query without VALUES");

    assert_undef_column_is_transparent(
        "sparql/values-undef",
        &rows(&with_values, &ledger.snapshot),
        &rows(&without_values, &ledger.snapshot),
    );
}

#[tokio::test]
async fn jsonld_optional_extends_values_undef_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:values-undef-jsonld").await;

    let with_values = support::query_jsonld(&fluree, &ledger, &jsonld_values_undef_then_optional())
        .await
        .expect("query with VALUES … UNDEF");
    let without_values = support::query_jsonld(&fluree, &ledger, &jsonld_optional_only())
        .await
        .expect("query without VALUES");

    assert_undef_column_is_transparent(
        "json-ld/values-undef",
        &rows(&with_values, &ledger.snapshot),
        &rows(&without_values, &ledger.snapshot),
    );
}

/// The `UNDEF` form on `PlanTreeOptionalBuilder`'s lane. Every friend has a
/// `schema:name`, so the second inner pattern removes nothing and the answer is
/// the same eight rows — but at the merge-base this returned five, all null:
/// the correlation-key partition lost the multiplicity as well as the binding.
#[tokio::test]
async fn multi_pattern_optional_extends_values_undef_rows() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:values-undef-multi").await;
    let db = graphdb_from_ledger(&ledger);

    let multi_pattern = format!(
        "SELECT ?s ?f WHERE {{ ?s schema:name ?name . {VALUES_UNDEF} \
         OPTIONAL {{ ?s ex:friend ?f . ?f schema:name ?fname }} }}"
    );

    let with_values = fluree
        .query(
            &db,
            QueryInput::Sparql(&format!("{PREFIXES}{multi_pattern}")),
        )
        .await
        .expect("query with multi-pattern OPTIONAL");
    let without_values = fluree
        .query(
            &db,
            QueryInput::Sparql(&format!("{PREFIXES}{OPTIONAL_ONLY}")),
        )
        .await
        .expect("query without VALUES");

    assert_undef_column_is_transparent(
        "sparql/values-undef/multi-pattern",
        &rows(&with_values, &ledger.snapshot),
        &rows(&without_values, &ledger.snapshot),
    );
}

/// A correlation variable the OPTIONAL can only READ, left unbound by the
/// UNION's second branch.
///
/// `?age` reaches `PlanTreeOptionalBuilder`'s correlation set through the
/// FILTER, but no inner pattern can bind it, so an unbound `?age` makes the
/// filter an error under correlated and independent evaluation alike and the
/// row's answer is the padded one either way. This pins that answer, so the
/// scoping that keeps these rows on the batched hash-join lane cannot quietly
/// change it: four `?age`-bound rows join or don't on the filter, and the five
/// `?age`-unbound rows pass through — ten in all.
#[tokio::test]
async fn optional_reading_an_unbound_filter_operand_pads_the_row() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:filter-operand").await;
    let db = graphdb_from_ledger(&ledger);

    let query = "SELECT ?s ?age ?f WHERE { \
         { { ?s schema:name ?n . ?s schema:age ?age } UNION { ?s schema:name ?n } } \
         OPTIONAL { ?s ex:friend ?f . FILTER(?age > 20) } }";

    let result = fluree
        .query(&db, QueryInput::Sparql(&format!("{PREFIXES}{query}")))
        .await
        .expect("query");

    assert_eq!(
        rows(&result, &ledger.snapshot),
        normalize_rows(&json!([
            // ?age bound: alice and cam clear the filter, brian has no friends,
            // liam is 13.
            ["ex:alice", 50, "ex:brian"],
            ["ex:brian", 50, null],
            ["ex:cam", 34, "ex:alice"],
            ["ex:cam", 34, "ex:brian"],
            ["ex:liam", 13, null],
            // ?age unbound: `?age > 20` is an error, so no friend joins.
            ["ex:alice", null, null],
            ["ex:brian", null, null],
            ["ex:cam", null, null],
            ["ex:liam", null, null],
            ["ex:nikola", null, null]
        ]))
    );
}

// ---------------------------------------------------------------------------
// The grouped lane: a chain of single-triple OPTIONALs on one subject.
// ---------------------------------------------------------------------------

/// Two chained single-triple `OPTIONAL`s on the same subject route to
/// `GroupedPatternOptionalBuilder`
/// (`where_plan.rs::collect_grouped_single_triple_optionals`), a fourth lane
/// with its own merge. Its object variables are structurally optional-only, but
/// its SUBJECT only has to be PRESENT in the required schema — the UNION's
/// second branch leaves it unbound on one row.
///
/// The failure mode this pins is the nastier one: not a solution missing a
/// binding, but a solution reporting an email and an age for a subject it
/// declines to name.
#[tokio::test]
async fn grouped_optional_chain_binds_the_subject_the_union_left_unbound() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-union:grouped").await;
    let db = graphdb_from_ledger(&ledger);

    let query = "SELECT ?s ?e ?a WHERE { \
         { { ?s schema:name ?n } UNION { ex:nikola schema:name ?nn } } \
         OPTIONAL { ?s schema:email ?e } \
         OPTIONAL { ?s schema:age ?a } }";

    let result = fluree
        .query(&db, QueryInput::Sparql(&format!("{PREFIXES}{query}")))
        .await
        .expect("query");
    let actual = rows(&result, &ledger.snapshot);

    assert!(
        actual
            .iter()
            .all(|row| !row.as_array().expect("row array")[0].is_null()),
        "no solution may report an email/age for a subject it leaves unbound; got {actual:#?}"
    );
    assert_eq!(
        actual,
        normalize_rows(&json!([
            ["ex:alice", "alice@example.org", 50],
            ["ex:brian", "brian@example.org", 50],
            ["ex:cam", "cam@example.org", 34],
            ["ex:liam", "liam@example.org", 13],
            ["ex:nikola", null, null],
            ["ex:alice", "alice@example.org", 50],
            ["ex:brian", "brian@example.org", 50],
            ["ex:cam", "cam@example.org", 34],
            ["ex:liam", "liam@example.org", 13]
        ]))
    );
}

// ---------------------------------------------------------------------------
// #1734: a second OPTIONAL on a variable an earlier OPTIONAL failed to bind.
// ---------------------------------------------------------------------------
//
// An OPTIONAL that matches nothing leaves its variables unbound (§18.2.4), and
// an unbound variable is compatible with any later binding of it. The engine
// used to record `Binding::Poisoned` instead — Cypher's null, which blocks every
// later match — so the second OPTIONAL lost both its bindings and its fan-out.
// Each test reaches the unbound column through a different emitter or the
// second OPTIONAL through a different lane.

const GREETING_THEN_FRIEND: &str = "SELECT ?s ?f WHERE { \
     ?s schema:name ?name . \
     OPTIONAL { ?s ex:greeting ?f } \
     OPTIONAL { ?s ex:friend ?f } }";

/// The same query with the second `OPTIONAL` deleted.
const GREETING_ONLY: &str = "SELECT ?s ?f WHERE { \
     ?s schema:name ?name . \
     OPTIONAL { ?s ex:greeting ?f } }";

fn greeting(value: &str, lang: &str) -> Value {
    json!({"@value": value, "@language": lang})
}

/// Nikola's two greetings, then every friend of a subject with no greeting:
/// alice 1, cam 2, liam 3, and brian — who has neither — genuinely unbound.
fn expected_greeting_then_friend() -> Vec<Value> {
    normalize_rows(&json!([
        ["ex:nikola", greeting("Здраво", "sb")],
        ["ex:nikola", greeting("Hello", "en")],
        ["ex:alice", "ex:brian"],
        ["ex:cam", "ex:alice"],
        ["ex:cam", "ex:brian"],
        ["ex:liam", "ex:alice"],
        ["ex:liam", "ex:brian"],
        ["ex:liam", "ex:cam"],
        ["ex:brian", null]
    ]))
}

fn expected_greeting_only() -> Vec<Value> {
    normalize_rows(&json!([
        ["ex:nikola", greeting("Здраво", "sb")],
        ["ex:nikola", greeting("Hello", "en")],
        ["ex:alice", null],
        ["ex:brian", null],
        ["ex:cam", null],
        ["ex:liam", null]
    ]))
}

async fn sparql(
    fluree: &fluree_db_api::Fluree,
    db: &fluree_db_api::GraphDb,
    snapshot: &fluree_db_core::LedgerSnapshot,
    query: &str,
) -> Vec<Value> {
    let result = fluree
        .query(db, QueryInput::Sparql(&format!("{PREFIXES}{query}")))
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\n{query}"));
    rows(&result, snapshot)
}

/// Exact rows, plus the delete-the-clause property: before the fix the second
/// OPTIONAL contributed nothing and both queries returned the same six rows.
fn assert_second_optional_contributes(lane: &str, with_second: &[Value], first_only: &[Value]) {
    assert_eq!(
        first_only,
        expected_greeting_only(),
        "{lane}: the first OPTIONAL alone must be the six rows the second extends"
    );
    assert_ne!(
        with_second, first_only,
        "{lane}: deleting the second OPTIONAL must change the answer"
    );
    assert_eq!(
        with_second,
        expected_greeting_then_friend(),
        "{lane}: the second OPTIONAL must bind ?f where the first left it unbound"
    );
}

#[tokio::test]
async fn sparql_second_optional_binds_var_first_left_unbound() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-optional:sparql").await;
    let db = graphdb_from_ledger(&ledger);

    assert_second_optional_contributes(
        "sparql/novelty",
        &sparql(&fluree, &db, &ledger.snapshot, GREETING_THEN_FRIEND).await,
        &sparql(&fluree, &db, &ledger.snapshot, GREETING_ONLY).await,
    );
}

#[tokio::test]
async fn jsonld_second_optional_binds_var_first_left_unbound() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-optional:jsonld").await;
    let query = |second: bool| {
        let mut wh = vec![
            json!({"@id": "?s", "schema:name": "?name"}),
            json!(["optional", {"@id": "?s", "ex:greeting": "?f"}]),
        ];
        if second {
            wh.push(json!(["optional", {"@id": "?s", "ex:friend": "?f"}]));
        }
        json!({"@context": query_context(), "select": ["?s", "?f"], "where": wh})
    };

    let with_second = support::query_jsonld(&fluree, &ledger, &query(true))
        .await
        .expect("query with second OPTIONAL");
    let first_only = support::query_jsonld(&fluree, &ledger, &query(false))
        .await
        .expect("query with first OPTIONAL only");

    assert_second_optional_contributes(
        "json-ld/novelty",
        &rows(&with_second, &ledger.snapshot),
        &rows(&first_only, &ledger.snapshot),
    );
}

/// A two-pattern second OPTIONAL routes to `PlanTreeOptionalBuilder`, whose
/// batched lane partitions by a correlation key the unbound `?f` has no value
/// for. Every friend has a name, so the answer is unchanged.
#[tokio::test]
async fn multi_pattern_second_optional_binds_var_first_left_unbound() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-optional:multi").await;
    let db = graphdb_from_ledger(&ledger);

    let query = "SELECT ?s ?f WHERE { \
         ?s schema:name ?name . \
         OPTIONAL { ?s ex:greeting ?f } \
         OPTIONAL { ?s ex:friend ?f . ?f schema:name ?fname } }";

    assert_second_optional_contributes(
        "sparql/multi-pattern",
        &sparql(&fluree, &db, &ledger.snapshot, query).await,
        &sparql(&fluree, &db, &ledger.snapshot, GREETING_ONLY).await,
    );
}

/// Greeting and email are distinct-variable single-triple OPTIONALs on one
/// subject, so they group (`GroupedPatternOptionalBuilder`); the friend
/// OPTIONAL re-uses `?f` and stays outside the group. The grouped builder's
/// own no-match binding is written on its batched lane, covered in
/// `indexed_second_optional_binds_var_first_left_unbound`.
const GROUPED_THEN_FRIEND: &str = "SELECT ?s ?f WHERE { \
     ?s schema:name ?name . \
     OPTIONAL { ?s ex:greeting ?f } \
     OPTIONAL { ?s schema:email ?e } \
     OPTIONAL { ?s ex:friend ?f } }";

#[tokio::test]
async fn grouped_optional_leaves_var_unbound_for_later_optional() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-optional:grouped").await;
    let db = graphdb_from_ledger(&ledger);

    assert_eq!(
        sparql(&fluree, &db, &ledger.snapshot, GROUPED_THEN_FRIEND).await,
        expected_greeting_then_friend()
    );
}

/// `PropertyJoinOperator` absorbs a trailing single-triple OPTIONAL into a
/// required star (three wide, anchored by the range filter) and writes the
/// no-match binding itself. Nikola has no email or age, so the star drops him.
const STAR_THEN_FRIEND: &str = "SELECT ?s ?f WHERE { \
     ?s schema:name ?name . \
     ?s schema:email ?email . \
     ?s schema:age ?age . \
     FILTER(?age > 10) \
     OPTIONAL { ?s ex:greeting ?f } \
     OPTIONAL { ?s ex:friend ?f } }";

fn expected_star_then_friend() -> Vec<Value> {
    normalize_rows(&json!([
        ["ex:alice", "ex:brian"],
        ["ex:cam", "ex:alice"],
        ["ex:cam", "ex:brian"],
        ["ex:liam", "ex:alice"],
        ["ex:liam", "ex:brian"],
        ["ex:liam", "ex:cam"],
        ["ex:brian", null]
    ]))
}

#[tokio::test]
async fn property_join_optional_leaves_var_unbound_for_later_optional() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-optional:property-join").await;
    let db = graphdb_from_ledger(&ledger);

    assert_eq!(
        sparql(&fluree, &db, &ledger.snapshot, STAR_THEN_FRIEND).await,
        expected_star_then_friend()
    );
}

/// The unbound variable in SUBJECT position of the second OPTIONAL. Brian and
/// Nikola have no friend, so `?f` is unbound and every `?f ex:friend ?ff`
/// triple is compatible with their row — each extends into all six.
#[tokio::test]
async fn second_optional_binds_unbound_var_in_subject_position() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-optional:subject").await;
    let db = graphdb_from_ledger(&ledger);

    let query = "SELECT ?s ?f ?ff WHERE { \
         ?s schema:name ?name . \
         OPTIONAL { ?s ex:friend ?f } \
         OPTIONAL { ?f ex:friend ?ff } }";

    let friend_edges = [
        ("ex:alice", "ex:brian"),
        ("ex:cam", "ex:alice"),
        ("ex:cam", "ex:brian"),
        ("ex:liam", "ex:alice"),
        ("ex:liam", "ex:brian"),
        ("ex:liam", "ex:cam"),
    ];
    let mut expected = vec![
        // ?f bound: extended by ?f's own friends, or padded.
        json!(["ex:alice", "ex:brian", null]),
        json!(["ex:cam", "ex:alice", "ex:brian"]),
        json!(["ex:cam", "ex:brian", null]),
        json!(["ex:liam", "ex:alice", "ex:brian"]),
        json!(["ex:liam", "ex:brian", null]),
        json!(["ex:liam", "ex:cam", "ex:alice"]),
        json!(["ex:liam", "ex:cam", "ex:brian"]),
    ];
    for s in ["ex:brian", "ex:nikola"] {
        for (f, ff) in friend_edges {
            expected.push(json!([s, f, ff]));
        }
    }

    assert_eq!(
        sparql(&fluree, &db, &ledger.snapshot, query).await,
        normalize_rows(&Value::Array(expected))
    );
}

/// `COUNT(*)` over the same shape, so a count fast path cannot answer it from
/// the old six-row plan.
#[tokio::test]
async fn count_star_counts_second_optional_fan_out() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-optional:count").await;
    let db = graphdb_from_ledger(&ledger);

    let query = "SELECT (COUNT(*) AS ?n) WHERE { \
         ?s schema:name ?name . \
         OPTIONAL { ?s ex:greeting ?f } \
         OPTIONAL { ?s ex:friend ?f } }";

    assert_eq!(
        sparql(&fluree, &db, &ledger.snapshot, query).await,
        normalize_rows(&json!([[9]]))
    );
}

/// The indexed lane reaches the second OPTIONAL through `build_batch`'s
/// subject probe rather than the per-row substituted scan.
#[tokio::test]
async fn indexed_second_optional_binds_var_first_left_unbound() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "optional-after-optional:indexed";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);
            let ledger = fluree
                .insert_with_opts(
                    ledger,
                    &seed_data(),
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("seed insert")
                .ledger;

            trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            wait_for_index_application(&fluree, ledger_id, ledger.t()).await;
            let view = fluree.db(ledger_id).await.expect("indexed view");
            assert!(
                view.binary_store().is_some(),
                "the indexed lane needs a binary store on the view; without one this test is a \
                 duplicate of the novelty-lane one"
            );

            assert_second_optional_contributes(
                "sparql/indexed",
                &sparql(&fluree, &view, &view.snapshot, GREETING_THEN_FRIEND).await,
                &sparql(&fluree, &view, &view.snapshot, GREETING_ONLY).await,
            );
            // The grouped builder's and the property join's own no-match
            // bindings are written only on their batched (indexed) lanes; on
            // novelty both fall back to the per-row OPTIONAL.
            assert_eq!(
                sparql(&fluree, &view, &view.snapshot, GROUPED_THEN_FRIEND).await,
                expected_greeting_then_friend(),
                "grouped/indexed"
            );
            assert_eq!(
                sparql(&fluree, &view, &view.snapshot, STAR_THEN_FRIEND).await,
                expected_star_then_friend(),
                "property-join/indexed"
            );
        })
        .await;
}

// ============================================================================
// Update WHERE
// ============================================================================
//
// The update WHERE plans separately from reads, so its surface opts into the
// same null semantics on the lowered `Txn`. Here a wrong mode doesn't show on a
// screen — it writes a different set of flakes.

const TAG_GREETING_THEN_FRIEND: &str = "INSERT { ?s ex:tagged ?f } WHERE { \
     ?s schema:name ?name . \
     OPTIONAL { ?s ex:greeting ?f } \
     OPTIONAL { ?s ex:friend ?f } }";

/// The read-side answer minus brian, whose genuinely unbound `?f` writes nothing.
fn expected_tagged() -> Vec<Value> {
    expected_greeting_then_friend()
        .into_iter()
        .filter(|row| !row[1].is_null())
        .collect()
}

async fn tagged_rows(fluree: &fluree_db_api::Fluree, ledger: &LedgerState) -> Vec<Value> {
    sparql(
        fluree,
        &graphdb_from_ledger(ledger),
        &ledger.snapshot,
        "SELECT ?s ?f WHERE { ?s ex:tagged ?f }",
    )
    .await
}

#[tokio::test]
async fn sparql_update_second_optional_binds_var_first_left_unbound() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "optional-after-optional:sparql-update";
    seed_novelty(&fluree, ledger_id).await;

    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&format!("{PREFIXES}{TAG_GREETING_THEN_FRIEND}"))
        .commit()
        .await
        .expect("sparql update");

    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    assert_eq!(tagged_rows(&fluree, &ledger).await, expected_tagged());
}

#[tokio::test]
async fn jsonld_update_second_optional_binds_var_first_left_unbound() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_novelty(&fluree, "optional-after-optional:jsonld-update").await;

    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": query_context(),
                "where": [
                    {"@id": "?s", "schema:name": "?name"},
                    ["optional", {"@id": "?s", "ex:greeting": "?f"}],
                    ["optional", {"@id": "?s", "ex:friend": "?f"}]
                ],
                "insert": {"@id": "?s", "ex:tagged": "?f"}
            }),
        )
        .await
        .expect("json-ld update")
        .ledger;

    assert_eq!(tagged_rows(&fluree, &ledger).await, expected_tagged());
}

// ---------------------------------------------------------------------------
// #1924 / #1925: the planner must not reorder a left join across a pattern it
// shares a not-yet-certain variable with.
// ---------------------------------------------------------------------------
//
// Inner joins commute; a left join does not. `LeftJoin(A, O) ⋈ P` equals
// `LeftJoin(A ⋈ P, O)` only when every variable `P` shares with `O` is already
// certainly bound by `A`. Each test below is a shape where it is not.

const ORDERING_TTL: &str = r#"@prefix ex: <http://example.com/> .
ex:alice ex:knows ex:bob .
ex:bob   ex:worksFor ex:acme .
ex:carol ex:knows ex:bob ; ex:worksFor ex:globex .
ex:dave  ex:knows ex:erin .
ex:a ex:name "A" ; ex:fr ex:b .
ex:b ex:name "B" .
ex:k ex:g "k" .
"#;

async fn seed_ordering(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> LedgerState {
    let ledger = genesis_ledger_for_fluree(fluree, ledger_id);
    fluree
        .insert_turtle(ledger, ORDERING_TTL)
        .await
        .expect("seed turtle")
        .ledger
}

const ORDERING_PREFIX: &str = "PREFIX ex: <http://example.com/>\n";

async fn ordering_rows(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    query: &str,
) -> Vec<Value> {
    let db = graphdb_from_ledger(ledger);
    let result = fluree
        .query(
            &db,
            QueryInput::Sparql(&format!("{ORDERING_PREFIX}{query}")),
        )
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\n{query}"));
    rows(&result, &ledger.snapshot)
}

fn ordering_context() -> Value {
    json!({"ex": "http://example.com/"})
}

/// The triple after the OPTIONAL binds `?org`, which only the OPTIONAL binds
/// before it. Hoisted, it seeded `?org = acme` for carol, the OPTIONAL then
/// failed on `carol worksFor acme` and kept the row: a sixth, fabricated
/// `(carol, acme, bob)`. Alice and dave (`?org` unbound) join both employers.
const TRIPLE_AFTER_OPTIONAL: &str = "SELECT ?p ?org ?y WHERE { \
     ?p ex:knows ?f . \
     OPTIONAL { ?p ex:worksFor ?org } \
     ?y ex:worksFor ?org }";

fn expected_triple_after_optional() -> Vec<Value> {
    normalize_rows(&json!([
        ["ex:alice", "ex:acme", "ex:bob"],
        ["ex:alice", "ex:globex", "ex:carol"],
        ["ex:carol", "ex:globex", "ex:carol"],
        ["ex:dave", "ex:acme", "ex:bob"],
        ["ex:dave", "ex:globex", "ex:carol"]
    ]))
}

#[tokio::test]
async fn sparql_triple_after_optional_is_not_hoisted_above_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_ordering(&fluree, "optional-ordering:hoist").await;

    assert_eq!(
        ordering_rows(&fluree, &ledger, TRIPLE_AFTER_OPTIONAL).await,
        expected_triple_after_optional()
    );
}

#[tokio::test]
async fn jsonld_triple_after_optional_is_not_hoisted_above_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_ordering(&fluree, "optional-ordering:hoist-jsonld").await;

    let query = json!({
        "@context": ordering_context(),
        "select": ["?p", "?org", "?y"],
        "where": [
            {"@id": "?p", "ex:knows": "?f"},
            ["optional", {"@id": "?p", "ex:worksFor": "?org"}],
            {"@id": "?y", "ex:worksFor": "?org"}
        ]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");

    assert_eq!(
        rows(&result, &ledger.snapshot),
        expected_triple_after_optional()
    );
}

/// `{ ex:k ex:g ?f }` shares no variable with the required pattern, so it was
/// ineligible for placement until the later OPTIONAL had bound `?f` — and ran
/// second. Written order binds `?f = "k"` everywhere first; the friend OPTIONAL
/// then fails for `a` (`a ex:fr "k"` is absent) and keeps `"k"`.
const UNCORRELATED_THEN_CORRELATED: &str = "SELECT ?s ?f WHERE { \
     ?s ex:name ?n . \
     OPTIONAL { ex:k ex:g ?f } \
     OPTIONAL { ?s ex:fr ?f } }";

fn expected_uncorrelated_then_correlated() -> Vec<Value> {
    normalize_rows(&json!([["ex:a", "k"], ["ex:b", "k"]]))
}

#[tokio::test]
async fn sparql_uncorrelated_optional_runs_before_a_later_optional() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_ordering(&fluree, "optional-ordering:uncorrelated").await;

    assert_eq!(
        ordering_rows(&fluree, &ledger, UNCORRELATED_THEN_CORRELATED).await,
        expected_uncorrelated_then_correlated()
    );
}

#[tokio::test]
async fn jsonld_uncorrelated_optional_runs_before_a_later_optional() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_ordering(&fluree, "optional-ordering:uncorrelated-jsonld").await;

    let query = json!({
        "@context": ordering_context(),
        "select": ["?s", "?f"],
        "where": [
            {"@id": "?s", "ex:name": "?n"},
            ["optional", {"@id": "ex:k", "ex:g": "?f"}],
            ["optional", {"@id": "?s", "ex:fr": "?f"}]
        ]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");

    assert_eq!(
        rows(&result, &ledger.snapshot),
        expected_uncorrelated_then_correlated()
    );
}

/// An OPTIONAL written first is `LeftJoin({}, O)`: `O`'s own solutions when it
/// has any. The triple then joins those, so only `a` — the one subject with an
/// `ex:fr` — survives. Hoisting the triple above the OPTIONAL instead made it a
/// plain left join and let `b` through with `?x` unbound.
#[tokio::test]
async fn leading_optional_is_joined_not_left_joined() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_ordering(&fluree, "optional-ordering:leading").await;

    assert_eq!(
        ordering_rows(
            &fluree,
            &ledger,
            "SELECT ?s ?x ?n WHERE { OPTIONAL { ?s ex:fr ?x } ?s ex:name ?n }",
        )
        .await,
        normalize_rows(&json!([["ex:a", "ex:b", "A"]]))
    );
}

/// A triple held behind an uncorrelated OPTIONAL. The OPTIONAL is never
/// "eligible" (it shares nothing with what is bound), so the planner falls back
/// to force-placing a pattern — which must be the OPTIONAL, not the blocked
/// triple. Written order binds `?f = "k"` on every row, and nothing has
/// `ex:fr "k"`, so the answer is empty; running the triple first bound
/// `?f = ex:b` and the OPTIONAL then kept those rows.
#[tokio::test]
async fn forced_placement_respects_left_join_order() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_ordering(&fluree, "optional-ordering:forced").await;

    assert_eq!(
        ordering_rows(
            &fluree,
            &ledger,
            "SELECT ?s ?f ?y WHERE { ?s ex:name ?n . \
             OPTIONAL { ex:k ex:g ?f } ?y ex:fr ?f }",
        )
        .await,
        Vec::<Value>::new()
    );
}

/// An OPTIONAL reading the output of an uncorrelated sub-SELECT written after
/// it. The OPTIONAL waits for the sub-SELECT as its pipeline consumer, while
/// the left-join barrier holds the sub-SELECT behind the OPTIONAL. The planner
/// must still emit both: dropping the sub-SELECT returned `(ex:b, "B")` too.
/// Written order binds every named `?s`, then the sub-SELECT keeps `ex:a`.
const OPTIONAL_BEFORE_SUBSELECT: &str = "SELECT ?s ?nm WHERE { \
     OPTIONAL { ?s ex:name ?nm } \
     { SELECT ?s WHERE { ?s ex:fr ?z } } }";

#[tokio::test]
async fn sparql_optional_before_subselect_keeps_the_subselect() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_ordering(&fluree, "optional-ordering:subselect").await;

    assert_eq!(
        ordering_rows(&fluree, &ledger, OPTIONAL_BEFORE_SUBSELECT).await,
        normalize_rows(&json!([["ex:a", "A"]]))
    );
}

#[tokio::test]
async fn jsonld_optional_before_subselect_keeps_the_subselect() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_ordering(&fluree, "optional-ordering:subselect-jsonld").await;

    let query = json!({
        "@context": ordering_context(),
        "select": ["?s", "?nm"],
        "where": [
            ["optional", {"@id": "?s", "ex:name": "?nm"}],
            ["query", {
                "@context": ordering_context(),
                "select": ["?s"],
                "where": [{"@id": "?s", "ex:fr": "?z"}]
            }]
        ]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");

    assert_eq!(
        rows(&result, &ledger.snapshot),
        normalize_rows(&json!([["ex:a", "A"]]))
    );
}

/// `COUNT(*)` over both shapes, so a count fast path cannot answer from the
/// reordered plan.
#[tokio::test]
async fn count_star_respects_left_join_order() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_ordering(&fluree, "optional-ordering:count").await;

    for (query, expected) in [
        (
            "SELECT (COUNT(*) AS ?n) WHERE { ?p ex:knows ?f . \
             OPTIONAL { ?p ex:worksFor ?org } ?y ex:worksFor ?org }",
            5,
        ),
        (
            "SELECT (COUNT(*) AS ?c) WHERE { ?s ex:name ?n . \
             OPTIONAL { ex:k ex:g ?f } OPTIONAL { ?s ex:fr ?f } FILTER(?f = \"k\") }",
            2,
        ),
    ] {
        assert_eq!(
            ordering_rows(&fluree, &ledger, query).await,
            normalize_rows(&json!([[expected]])),
            "{query}"
        );
    }
}
