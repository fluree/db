//! #1721: a delete that removes nothing must not change query results.
//!
//! The planner merges novelty into the aggregate per-datatype breakdown as a
//! blind ±1 delta log. A retraction of a fact the ledger never held still
//! charges its `-1`, and when that drives a predicate's last literal tag to zero
//! the predicate reads as all-ref — which licenses `filter_fold` to rewrite an
//! unrelated `FILTER(?x = ?y)` into an equijoin that does not mean the same
//! thing. These pin the observable consequence: three shapes of no-op delete,
//! none of which touches the data, none of which may touch the answer.
//!
//! The mechanism itself is pinned closer to the source, and without depending on
//! any particular looseness in the join, by
//! `fluree_db_novelty::runtime_stats::tests::spurious_retraction_keeps_a_literal_datatype_observed`.

use crate::support;
use crate::support::{assert_index_defaults, genesis_ledger, normalize_rows};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// One predicate carrying a ref (so the surviving breakdown stays non-empty and
/// all-`JSON_LD_ID` once the literal tags are zeroed) and two literals that
/// share a lexical form but not a datatype. `"abc"^^xsd:string` and
/// `"abc"^^ex:custom` are different terms and are not value-equal under SPARQL
/// `=`, so every correct answer to the query below is an identity pair.
fn seed() -> serde_json::Value {
    json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            {"@id":"ex:s1","ex:p": "abc"},
            {"@id":"ex:s2","ex:p": {"@value":"abc","@type":"ex:custom"}},
            {"@id":"ex:s3","ex:p": {"@id":"ex:n1"}}
        ]
    })
}

/// A delete of two facts that are not in the ledger, one per literal datatype.
fn no_op_delete() -> serde_json::Value {
    json!({
        "@context": { "ex": "http://example.org/ns/" },
        "delete": [
            {"@id":"ex:s1","ex:p": "nothing-here"},
            {"@id":"ex:s2","ex:p": {"@value":"nothing-here","@type":"ex:custom"}}
        ]
    })
}

const Q: &str = r"
    PREFIX ex: <http://example.org/ns/>
    SELECT ?a ?b WHERE {
      ?a ex:p ?x .
      ?b ex:p ?y .
      FILTER(?x = ?y)
    }
";

fn identity_rows() -> Vec<serde_json::Value> {
    normalize_rows(&json!([
        ["ex:s1", "ex:s1"],
        ["ex:s2", "ex:s2"],
        ["ex:s3", "ex:s3"]
    ]))
}

async fn rows(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    query: &str,
) -> Vec<serde_json::Value> {
    normalize_rows(
        &support::query_sparql(fluree, ledger, query)
            .await
            .expect("query")
            .to_jsonld(&ledger.snapshot)
            .expect("jsonld"),
    )
}

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

#[tokio::test]
async fn issue_1721_json_ld_no_op_delete_keeps_filter_equality_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "hazard1721-jsonld:main");
    let ledger = fluree
        .insert(ledger0, &seed())
        .await
        .expect("insert")
        .ledger;

    let before = rows(&fluree, &ledger, Q).await;
    assert_eq!(
        before,
        identity_rows(),
        "baseline: `=` must equate only a term with itself"
    );

    let ledger2 = fluree
        .update(ledger, &no_op_delete())
        .await
        .expect("delete")
        .ledger;

    // The data is provably untouched across the delete.
    let data = rows(
        &fluree,
        &ledger2,
        "PREFIX ex: <http://example.org/ns/> SELECT ?s ?o WHERE { ?s ex:p ?o }",
    )
    .await;
    assert_eq!(data.len(), 3, "the delete removed something: {data:?}");

    assert_eq!(
        rows(&fluree, &ledger2, Q).await,
        before,
        "a delete that removed nothing changed FILTER(?x = ?y) results"
    );
}

/// The same hazard through the SPARQL Update surface: `DELETE DATA` of a triple
/// that is not in the graph is a no-op by SPARQL 1.1 Update.
#[tokio::test]
async fn issue_1721_sparql_delete_data_no_op_keeps_filter_equality_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "hazard1721-sparql:main");
    let ledger = fluree
        .insert(ledger0, &seed())
        .await
        .expect("insert")
        .ledger;

    let before = rows(&fluree, &ledger, Q).await;
    assert_eq!(before, identity_rows(), "baseline");

    let update = r#"
        PREFIX ex: <http://example.org/ns/>
        DELETE DATA {
          ex:s1 ex:p "nothing-here" .
          ex:s2 ex:p "nothing-here"^^<http://example.org/ns/custom> .
        }
    "#;
    let ledger2 = run_sparql_update(&fluree, ledger, update).await.ledger;

    assert_eq!(
        rows(&fluree, &ledger2, Q).await,
        before,
        "DELETE DATA of absent triples changed FILTER(?x = ?y) results"
    );
}

/// No bogus data needed at all: deleting facts that *did* exist, twice. The
/// second delete is a no-op against the data but charges a second `-1`. This is
/// the operationally likely shape — an at-least-once delivery retry, a replayed
/// migration, an idempotent-by-intent client.
#[tokio::test]
async fn issue_1721_replayed_delete_keeps_filter_equality_results() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "hazard1721-replay:main");
    let insert = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            {"@id":"ex:s1","ex:p": "abc"},
            {"@id":"ex:s2","ex:p": {"@value":"abc","@type":"ex:custom"}},
            {"@id":"ex:s3","ex:p": {"@id":"ex:n1"}},
            // Literals that WILL be deleted (once, legitimately) below.
            {"@id":"ex:sx","ex:p": "gone"},
            {"@id":"ex:sy","ex:p": {"@value":"gone","@type":"ex:custom"}}
        ]
    });
    let ledger = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    let del = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "delete": [
            {"@id":"ex:sx","ex:p": "gone"},
            {"@id":"ex:sy","ex:p": {"@value":"gone","@type":"ex:custom"}}
        ]
    });
    let ledger1 = fluree.update(ledger, &del).await.expect("delete 1").ledger;
    let before = rows(&fluree, &ledger1, Q).await;
    assert_eq!(before, identity_rows(), "baseline after the real delete");

    let ledger2 = fluree.update(ledger1, &del).await.expect("delete 2").ledger;
    assert_eq!(
        rows(&fluree, &ledger2, Q).await,
        before,
        "replaying an already-applied delete changed FILTER(?x = ?y) results"
    );
}

/// The fold has to keep working: an all-ref predicate still licenses it after a
/// real index publish, with a literal-valued sibling for contrast.
///
/// Scope, because it is narrower than it looks. `observed_datatypes` is not on
/// the wire — every decoder re-derives it from the breakdown — so a round trip
/// can only ever observe the decoder it happens to route through, and this one
/// routes through `fluree-db-core`'s (the memory backend's), not
/// `fluree-db-binary-index`'s. It cannot see the index-build aggregate at all,
/// since encoding discards whatever that produced. Each producer is therefore
/// pinned where it lives:
/// `fluree_db_binary_index::format::stats_wire::tests::every_stats_decoder_rederives_the_observed_datatype_tags`
/// covers all three decoders, and
/// `fluree_db_indexer::stats::tests::aggregate_carries_the_observed_datatype_tags_across_graphs`
/// covers the build-side aggregate that both the index pipelines and the import
/// now share. That matters more than usual here: the field is fail-closed, so a
/// producer that stopped filling it would cost the optimization silently
/// instead of failing anything.
#[tokio::test]
async fn ref_only_survives_a_published_index_round_trip() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "hazard1721-refonly:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let insert = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [
            {"@id":"ex:a","ex:knows": {"@id":"ex:b"}, "ex:name": "A"},
            {"@id":"ex:b","ex:knows": {"@id":"ex:a"}, "ex:name": "B"}
        ]
    });
    let _ = fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert")
        .ledger;

    support::build_and_publish_index(&fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.expect("reload after index");

    let stats = ledger
        .snapshot
        .stats
        .clone()
        .expect("a published index carries stats");
    let view = fluree_db_core::StatsView::from_db_stats_with_namespaces(
        &stats,
        ledger.snapshot.namespaces(),
    );
    let ns = "http://example.org/ns/";
    assert_eq!(
        view.is_property_ref_only_by_iri(&format!("{ns}knows")),
        Some(true),
        "an all-ref predicate lost its ref-only licence across the index round trip"
    );
    assert_eq!(
        view.is_property_ref_only_by_iri(&format!("{ns}name")),
        Some(false)
    );
}

/// Blast radius: the same hazard against a *published base index* rather than
/// novelty-synthesized stats. Here the literal datatype tags come from the
/// index, and the spurious `-1`s have to cancel them out of the merged
/// breakdown before the fold is licensed.
///
/// An unrelated triple is staged after the index publish so that both
/// measurements are taken with novelty non-empty. The encoded-object read lane
/// answers this query differently from the overlay lane (a datatype-flattening
/// defect of its own, unrelated to the fold), and comparing across that lane
/// switch would measure that instead of the drift.
#[tokio::test]
async fn issue_1721_no_op_delete_does_not_drift_a_published_index() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "hazard1721-indexed:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let _ = fluree
        .insert(ledger0, &seed())
        .await
        .expect("insert")
        .ledger;

    support::build_and_publish_index(&fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.expect("reload after index");
    let unrelated = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [{"@id":"ex:s9","ex:other": "x"}]
    });
    let ledger = fluree
        .insert(ledger, &unrelated)
        .await
        .expect("unrelated insert")
        .ledger;

    let before = rows(&fluree, &ledger, Q).await;
    assert_eq!(before, identity_rows(), "baseline over the published index");

    let ledger2 = fluree
        .update(ledger, &no_op_delete())
        .await
        .expect("delete")
        .ledger;

    assert_eq!(
        rows(&fluree, &ledger2, Q).await,
        before,
        "a no-op delete drifted the stats read off a published index"
    );
}

/// The other way the tag set can describe a graph that is not the one being
/// queried, and this one needs no spurious retraction at all.
///
/// A published index is current state as of the publish, and a read at
/// `to_t <= indexed_t` is served from it verbatim. So delete every literal
/// under a mixed predicate, publish, and the index's tag set is honestly
/// all-ref — for `t = 2`. A query at `t = 1` still sees those literals, and
/// without a guard it would read that same all-ref set as its licence and fold
/// a `FILTER(?x = ?y)` that equates `"abc"` with `"abc"^^ex:custom`.
///
/// The guard is the *historical* tag set the index now persists: monotone
/// across publishes, covering every `t` at or above
/// `IndexStats::historical_since_t`, and substituted for the current-state
/// set on any read below the index `t`. For `ex:p` — whose history carries
/// the literals — that set correctly withholds the fold at `t = 1`, which
/// the folded-vs-unfolded comparison below observes end to end. For a
/// never-literal predicate it correctly KEEPS the fold at historical `t`s
/// (`fluree_db_query::stats_cache` pins the licence around the boundary);
/// this test pins the wire underneath that: a real publish writes the
/// boundary and the historical sets, and a real reload reads them back.
///
/// Nothing about the ledger is unusual here: the delete is a real one, the
/// stats are honest, and the fold is simply reading a fact about the wrong `t`.
#[tokio::test]
async fn issue_1721_time_travel_below_the_index_t_does_not_license_the_fold() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "hazard1721-timetravel:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let ledger1 = fluree
        .insert(ledger0, &seed())
        .await
        .expect("insert")
        .ledger;

    // A legitimate delete of both literals: after this the predicate really is
    // all-ref, and the next index publish will say so.
    let delete_the_literals = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "delete": [
            {"@id":"ex:s1","ex:p": "abc"},
            {"@id":"ex:s2","ex:p": {"@value":"abc","@type":"ex:custom"}}
        ]
    });
    let _ = fluree
        .update(ledger1, &delete_the_literals)
        .await
        .expect("delete the literals")
        .ledger;
    support::build_and_publish_index(&fluree, ledger_id).await;

    // The wire, end to end: a full build walks the whole commit chain, so the
    // published root claims historical coverage from genesis, remembers the
    // deleted literals' tags in ex:p's historical set, and still re-derives
    // the current-state set as honestly all-ref. This is what makes the
    // historical read below sound — and what keeps the fold alive at
    // historical `t`s for predicates that never carried a literal, instead of
    // clearing every set below the index `t` wholesale.
    let ledger = fluree.ledger(ledger_id).await.expect("reload after index");
    let stats = ledger
        .snapshot
        .stats
        .as_ref()
        .expect("a published index carries stats");
    assert_eq!(
        stats.historical_since_t,
        Some(0),
        "a full build must claim historical coverage from genesis"
    );
    let ref_tag = fluree_db_core::ValueTypeTag::JSON_LD_ID.as_u8();
    let p_entry = stats
        .properties
        .as_ref()
        .expect("aggregate properties")
        .iter()
        .find(|e| e.sid.1 == "p")
        .expect("ex:p entry");
    assert_eq!(
        p_entry.observed_datatypes,
        vec![ref_tag],
        "current state as of the publish is honestly all-ref"
    );
    assert!(
        p_entry.historical_datatypes.iter().any(|&t| t != ref_tag),
        "the historical set must remember the deleted literals' tags: {:?}",
        p_entry.historical_datatypes
    );

    // Folded vs unfolded at the same `t`, rather than against a written-out
    // expectation: projecting both compared variables makes `find_foldable`
    // bail, so the second query is the shipping engine's own pre-rewrite
    // answer. The historical read lane has a datatype-flattening defect of its
    // own (#1729) that also moves this answer; comparing the two shapes inside
    // one lane measures the fold and nothing else.
    let foldable = format!(
        r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?a ?b
        FROM <{ledger_id}@t:1>
        WHERE {{ ?a ex:p ?x . ?b ex:p ?y . FILTER(?x = ?y) }}
    "
    );
    let unfolded = format!(
        r"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?a ?b ?x ?y
        FROM <{ledger_id}@t:1>
        WHERE {{ ?a ex:p ?x . ?b ex:p ?y . FILTER(?x = ?y) }}
    "
    );

    assert_eq!(
        subject_pairs(&fluree, &foldable).await,
        subject_pairs(&fluree, &unfolded).await,
        "a read below the published index t folded on the index's current-state tag set"
    );
}

/// Monotone ACROSS publishes, not just within one: the second publish here is
/// an incremental build over a base that already carries the historical tail,
/// and its current state is honestly all-ref — the literals were deleted
/// before it. A per-publish re-derivation would reset the set and lose the
/// deleted literals' tags; the incremental path instead unions the base's
/// persisted set with its window, so the tags survive and the boundary
/// carries forward. This is the property that makes the historical set sound
/// for every `t` back to the boundary, not just since the latest publish.
#[tokio::test]
async fn issue_1721_historical_tags_survive_an_incremental_publish() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "hazard1721-monotone:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let ledger1 = fluree
        .insert(ledger0, &seed())
        .await
        .expect("insert")
        .ledger;

    // First publish: mixed history is current state, tags in both sets.
    support::build_and_publish_index(&fluree, ledger_id).await;
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    drop(ledger1);

    // Delete every literal, then publish AGAIN — this one routes through the
    // incremental pipeline (an index head exists and the gap is one commit).
    let delete_the_literals = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "delete": [
            {"@id":"ex:s1","ex:p": "abc"},
            {"@id":"ex:s2","ex:p": {"@value":"abc","@type":"ex:custom"}}
        ]
    });
    let _ = fluree
        .update(ledger, &delete_the_literals)
        .await
        .expect("delete the literals")
        .ledger;
    support::build_and_publish_index(&fluree, ledger_id).await;

    // Third publish, and this is the one that isolates the union: its novelty
    // window is an unrelated triple with no trace of the literals — the
    // deleted tags can only reach its output through the base root's
    // persisted historical sets. (The second publish's window still contained
    // the delete retractions, whose records the walk observes on its own.)
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let unrelated = json!({
        "@context": { "ex": "http://example.org/ns/" },
        "@graph": [{"@id":"ex:s9","ex:other": "x"}]
    });
    let _ = fluree
        .insert(ledger, &unrelated)
        .await
        .expect("unrelated insert")
        .ledger;
    support::build_and_publish_index(&fluree, ledger_id).await;

    let ledger = fluree
        .ledger(ledger_id)
        .await
        .expect("reload after reindex");
    let stats = ledger
        .snapshot
        .stats
        .as_ref()
        .expect("a published index carries stats");
    assert_eq!(
        stats.historical_since_t,
        Some(0),
        "the incremental publish must carry the base's boundary forward"
    );
    let ref_tag = fluree_db_core::ValueTypeTag::JSON_LD_ID.as_u8();
    let p_entry = stats
        .properties
        .as_ref()
        .expect("aggregate properties")
        .iter()
        .find(|e| e.sid.1 == "p")
        .expect("ex:p entry");
    assert_eq!(
        p_entry.observed_datatypes,
        vec![ref_tag],
        "current state after the delete is honestly all-ref"
    );
    assert!(
        p_entry.historical_datatypes.iter().any(|&t| t != ref_tag),
        "an incremental publish reset the historical set — the deleted \
         literals' tags are gone: {:?}",
        p_entry.historical_datatypes
    );
}

/// The `(?a, ?b)` prefix of every row, sorted — so a query that projects the
/// compared variables to defeat the fold is still comparable to one that does
/// not.
async fn subject_pairs(fluree: &fluree_db_api::Fluree, sparql: &str) -> Vec<serde_json::Value> {
    let jsonld = fluree
        .query_from()
        .sparql(sparql)
        .format(fluree_db_api::FormatterConfig::jsonld())
        .execute_formatted()
        .await
        .expect("historical query");
    let pairs: Vec<serde_json::Value> = jsonld
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| {
            let cells = row.as_array().expect("row cells");
            json!([cells[0].clone(), cells[1].clone()])
        })
        .collect();
    normalize_rows(&json!(pairs))
}
