//! Tests for projection-predicate gating in hydration / graph-crawl.
//!
//! When a JSON-LD query's `select` lists explicit forward predicates (rather
//! than `*`), the range provider receives a `predicate_filter` allow-list via
//! [`RangeOptions::predicate_filter`] and drops non-listed rows BEFORE the
//! per-row subject resolve, object decode, and dict-touch fuel charge.
//!
//! These tests exercise the boundary cases:
//!
//! 1. **Fuel scales with projection field count.** A subject with N
//!    dict-backed forward predicates costs less tracked fuel under a narrow
//!    Explicit projection than under a full-Explicit projection of the same
//!    subject — the delta corresponds to saved dict touches (10 µf each) +
//!    saved per-row charges (1 µf each) on the dropped predicates.
//! 2. **Wildcard projections preserve the legacy decode-everything path.**
//!    `select: ["*"]` does not opt into filtering and emits every predicate.
//! 3. **Overlay parity.** A novelty assert / retract on an unselected
//!    predicate doesn't leak into the result, and one on a selected
//!    predicate is honored.

#![cfg(feature = "native")]

use fluree_db_api::{FlureeBuilder, ReindexOptions};
use serde_json::{json, Value};

fn ctx() -> Value {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// Seed a subject with 8 dict-backed (string) predicates.
///
/// All eight values are strings — each one is a `StringDict`-backed object,
/// so each row decode in the range provider triggers a `DICT_TOUCH_MICRO_FUEL`
/// (10 µf = 0.010 fuel) charge under the legacy "decode every row" path.
async fn seed_eight_string_predicates(path: &str, ledger_id: &str) {
    let fluree = FlureeBuilder::file(path).build().expect("build");
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");
    let tx = json!({
        "@context": ctx(),
        "@id": "ex:subj-1",
        "@type": "ex:Thing",
        "ex:p1": "value-1",
        "ex:p2": "value-2",
        "ex:p3": "value-3",
        "ex:p4": "value-4",
        "ex:p5": "value-5",
        "ex:p6": "value-6",
        "ex:p7": "value-7",
        "ex:p8": "value-8",
    });
    fluree.insert(ledger0, &tx).await.expect("insert");

    // Reindex so the eight predicates land in the persisted dict and the
    // SPOT scan path (not novelty-only) handles the subsequent crawls.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
}

fn explicit_query(ledger: &str, picks: &[&str]) -> Value {
    let mut list: Vec<Value> = picks
        .iter()
        .map(|p| Value::String((*p).to_string()))
        .collect();
    list.insert(0, Value::String("@id".to_string()));
    json!({
        "@context": ctx(),
        "from": ledger,
        "select": { "ex:subj-1": list },
    })
}

fn wildcard_query(ledger: &str) -> Value {
    json!({
        "@context": ctx(),
        "from": ledger,
        "select": { "ex:subj-1": ["*"] },
    })
}

async fn tracked_fuel(fluree: &fluree_db_api::Fluree, query: &Value) -> f64 {
    let resp = fluree
        .query_from()
        .jsonld(query)
        .track_all()
        .execute_tracked()
        .await
        .expect("execute_tracked");
    assert_eq!(resp.status, 200, "query failed: {resp:?}");
    resp.fuel.expect("fuel is tracked")
}

/// Narrow projections consume strictly less tracked fuel than wide ones on
/// the same subject — proves the dict-touch gate fires on dropped predicates.
#[tokio::test]
async fn narrow_projection_costs_less_fuel_than_wide() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();
    let ledger_id = "hyd/scaling:main";
    seed_eight_string_predicates(path, ledger_id).await;
    let fluree = FlureeBuilder::file(path).build().expect("reopen");

    // Wide: project all 8 predicates explicitly. The range provider hands us
    // all 8 string flakes; each pays a dict touch.
    let wide = explicit_query(
        ledger_id,
        &[
            "ex:p1", "ex:p2", "ex:p3", "ex:p4", "ex:p5", "ex:p6", "ex:p7", "ex:p8",
        ],
    );
    let wide_fuel = tracked_fuel(&fluree, &wide).await;

    // Narrow: project only 2 of the 8. The range provider drops the other 6
    // before decode — 6 dict touches + 6 per-row charges saved.
    let narrow = explicit_query(ledger_id, &["ex:p1", "ex:p2"]);
    let narrow_fuel = tracked_fuel(&fluree, &narrow).await;

    // Expected saving on 6 dropped string-valued predicates:
    //   6 × DICT_TOUCH (0.010)  = 0.060
    // + 6 × PER_ROW    (0.001)  = 0.006
    //                  total    = 0.066 fuel
    // We assert at least 0.040 fuel saved to leave headroom for unrelated
    // micro-charges shifting between runs (object-decode bookkeeping, etc.).
    let saved = wide_fuel - narrow_fuel;
    assert!(
        saved >= 0.040,
        "narrow projection should save at least 0.040 fuel vs wide (saw {saved:.3}: wide={wide_fuel:.3}, narrow={narrow_fuel:.3})",
    );
}

/// Wildcard projections never opt into the predicate filter, so they cost
/// the same as a full Explicit projection over the same subject.
#[tokio::test]
async fn wildcard_projection_matches_full_explicit_projection() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();
    let ledger_id = "hyd/wildcard-eq:main";
    seed_eight_string_predicates(path, ledger_id).await;
    let fluree = FlureeBuilder::file(path).build().expect("reopen");

    let wild = wildcard_query(ledger_id);
    let wild_fuel = tracked_fuel(&fluree, &wild).await;

    // Full Explicit list (all 8 user predicates + @type the schema emitted)
    // — the predicate set is the same set the wildcard wants, so fuel must
    // be within a tiny epsilon.
    let full = explicit_query(
        ledger_id,
        &[
            "ex:p1", "ex:p2", "ex:p3", "ex:p4", "ex:p5", "ex:p6", "ex:p7", "ex:p8",
        ],
    );
    let full_fuel = tracked_fuel(&fluree, &full).await;

    // @type isn't in the Explicit list, so the Explicit fuel is actually
    // slightly LOWER (one fewer dict touch for the @type flake). Just sanity-
    // check they're in the same ballpark — within 0.030 fuel.
    let delta = (wild_fuel - full_fuel).abs();
    assert!(
        delta < 0.030,
        "wildcard and full-explicit fuel should be within 0.030 (wild={wild_fuel:.3}, full={full_fuel:.3}, delta={delta:.3})",
    );
}

/// K=1 single-predicate fast path: a projection naming exactly one forward
/// predicate routes through `SPOT(s,p,*)` (via `RangeMatch::subject_predicate`)
/// rather than `SPOT(s,*,*)` + predicate_filter. Correctness contract: the
/// JSON output must include the requested predicate and `@id`, and nothing
/// else.
///
/// Fuel-wise, K=1 should be ≤ a K=2 narrow projection on the same subject —
/// one fewer survivor row, one fewer dict touch.
#[tokio::test]
async fn single_predicate_projection_uses_sp_pair_path() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();
    let ledger_id = "hyd/single-pred:main";
    seed_eight_string_predicates(path, ledger_id).await;
    let fluree = FlureeBuilder::file(path).build().expect("reopen");

    // K=1: just one forward predicate.
    let single = explicit_query(ledger_id, &["ex:p3"]);
    let resp = fluree
        .query_from()
        .jsonld(&single)
        .execute_formatted()
        .await
        .expect("execute_formatted");
    let row = resp.as_array().expect("rows").first().expect("row");
    let obj = row.as_object().expect("obj");

    assert!(obj.contains_key("@id"));
    assert!(obj.contains_key("ex:p3"), "row: {row:?}");
    for unselected in [
        "ex:p1", "ex:p2", "ex:p4", "ex:p5", "ex:p6", "ex:p7", "ex:p8",
    ] {
        assert!(
            !obj.contains_key(unselected),
            "K=1 projection should not emit `{unselected}` — row: {row:?}",
        );
    }

    // Fuel sanity: K=1 should not cost MORE than K=2 on the same subject.
    // (Strict-less is harder to assert without flake; same-or-less is safe.)
    let k1_fuel = tracked_fuel(&fluree, &single).await;
    let k2_fuel = tracked_fuel(&fluree, &explicit_query(ledger_id, &["ex:p3", "ex:p4"])).await;
    assert!(
        k1_fuel <= k2_fuel + 0.001,
        "K=1 fuel ({k1_fuel:.3}) should be ≤ K=2 fuel ({k2_fuel:.3}) on the same subject",
    );
}

/// Per-key bytes also drop: the formatter only emits keys for predicates the
/// projection asked for. (Same as today — this is a smoke test that we
/// haven't broken the actual JSON output shape.)
#[tokio::test]
async fn narrow_projection_emits_only_selected_keys() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();
    let ledger_id = "hyd/narrow-keys:main";
    seed_eight_string_predicates(path, ledger_id).await;
    let fluree = FlureeBuilder::file(path).build().expect("reopen");

    let narrow = explicit_query(ledger_id, &["ex:p1", "ex:p3"]);
    let resp = fluree
        .query_from()
        .jsonld(&narrow)
        .execute_formatted()
        .await
        .expect("execute_formatted");

    // Single-column projection emits a bare object per row.
    let row = resp
        .as_array()
        .expect("rows array")
        .first()
        .expect("at least one row");
    let obj = row.as_object().expect("object row");
    assert!(
        obj.contains_key("@id"),
        "row keys: {:?}",
        obj.keys().collect::<Vec<_>>()
    );
    assert!(obj.contains_key("ex:p1"));
    assert!(obj.contains_key("ex:p3"));
    for unselected in ["ex:p2", "ex:p4", "ex:p5", "ex:p6", "ex:p7", "ex:p8"] {
        assert!(
            !obj.contains_key(unselected),
            "narrow projection should not emit `{unselected}` — row: {row:?}",
        );
    }
}

/// Overlay retract on a SELECTED predicate is honored: the retracted value
/// is dropped from the result. Catches a regression where the overlay filter
/// might accidentally drop retracts before they cancel base assertions.
#[tokio::test]
async fn overlay_retract_on_selected_predicate_is_honored() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();
    let ledger_id = "hyd/overlay-selected-retract:main";
    seed_eight_string_predicates(path, ledger_id).await;
    let fluree = FlureeBuilder::file(path).build().expect("reopen");
    let ledger = fluree.ledger(ledger_id).await.expect("load");

    // Retract ex:p1 — this lands in novelty (no reindex after).
    let retract = json!({
        "@context": ctx(),
        "delete": {
            "@id": "ex:subj-1",
            "ex:p1": "value-1",
        }
    });
    fluree.update(ledger, &retract).await.expect("retract");

    // Project ex:p1 explicitly. The overlay retract must remove it from the
    // result even though the persisted base scan still has the assertion.
    let q = explicit_query(ledger_id, &["ex:p1", "ex:p2"]);
    let resp = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("execute_formatted");
    let row = resp.as_array().expect("rows").first().expect("row");
    let obj = row.as_object().expect("obj");
    assert!(
        !obj.contains_key("ex:p1"),
        "selected-predicate retract should remove the key — row: {row:?}",
    );
    assert!(
        obj.contains_key("ex:p2"),
        "non-retracted selected predicate should remain — row: {row:?}",
    );
}

/// Overlay retract on an UNSELECTED predicate is invisible — the discarded
/// retract should never surface as either an absent key (it wasn't selected)
/// or a smuggled-in row (overlay filter must not promote non-selected ops).
#[tokio::test]
async fn overlay_retract_on_unselected_predicate_does_not_leak() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();
    let ledger_id = "hyd/overlay-unselected-retract:main";
    seed_eight_string_predicates(path, ledger_id).await;
    let fluree = FlureeBuilder::file(path).build().expect("reopen");
    let ledger = fluree.ledger(ledger_id).await.expect("load");

    // Retract ex:p7 (not in the projection below).
    let retract = json!({
        "@context": ctx(),
        "delete": {
            "@id": "ex:subj-1",
            "ex:p7": "value-7",
        }
    });
    fluree.update(ledger, &retract).await.expect("retract");

    // Project only ex:p1, ex:p2 — the ex:p7 retract isn't relevant.
    let q = explicit_query(ledger_id, &["ex:p1", "ex:p2"]);
    let resp = fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("execute_formatted");
    let row = resp.as_array().expect("rows").first().expect("row");
    let obj = row.as_object().expect("obj");
    assert!(obj.contains_key("ex:p1"));
    assert!(obj.contains_key("ex:p2"));
    assert!(
        !obj.contains_key("ex:p7"),
        "unselected predicate must not appear"
    );
}
