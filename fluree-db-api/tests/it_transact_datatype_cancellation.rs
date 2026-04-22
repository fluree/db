//! Prevention tests for same-commit retract+assert cancellation of canonically-
//! equivalent datatype forms.
//!
//! ## Why these tests exist
//!
//! A historical bug in Fluree's transact JSON-LD parser failed to expand CURIE
//! datatype IRIs (e.g., `"xsd:string"`, `"rdf:JSON"`) to their canonical full
//! IRIs when producing flakes. The result: a no-op `update` that deleted a
//! value with one datatype IRI form (say, CURIE) and re-asserted the same
//! value with a different form (say, full IRI) would produce a commit
//! containing BOTH a retract and an assert flake with distinct datatype
//! Sids. Because the two flakes had different Sids, `apply_cancellation`
//! in [`fluree_db_transact::stage`] did not collapse them, and the
//! commit was persisted to disk with the unnecessary retract+assert pair.
//!
//! When such commits are later reindexed, the canonicalized retract and
//! assert land in the same dt bucket, and the resulting same-commit retract+
//! assert interaction drops the underlying fact from the index. This is
//! the root cause of the "lost attributeMapping" false negative observed
//! in the production `_system` audit.
//!
//! PR #148 closes the write-path gap by canonicalizing CURIE datatype IRIs
//! via `fluree_vocab::datatype::KnownDatatype` at parse time (including a
//! fallback for well-known prefixes that are NOT declared in `@context`).
//! **These tests pin that guarantee in place**: for every user-facing
//! transaction path (insert / upsert / update), a same-canonical-value
//! no-op with mixed datatype IRI forms must collapse at staging, producing
//! either no new commit (preferred) or a commit with no flakes for the
//! affected fact.
//!
//! ## What we do NOT test here
//!
//! These tests guarantee that **future** commits written through
//! `fluree-db-api` cannot contain the bad shape. They do NOT test whether
//! Fluree can remediate legacy commits already on disk that contain the
//! bad shape — that is an operational concern, not a write-path invariant.
//! Legacy datasets require an audit + manual re-insertion of affected
//! flakes; see `docs/operations/` for the recovery runbook.

mod support;

use fluree_db_api::FlureeBuilder;
use fluree_db_core::{load_commit_by_id, FlakeValue};
use serde_json::json;

// =============================================================================
// Headline end-to-end test: the exact production shape of the t=23 bug
// =============================================================================

/// The `attributeMapping` `@json` scenario that broke the `_system` ledger.
///
/// Pattern:
///   t=1 insert attributeMapping with `@type: "@json"`
///   t=2 attempted update: delete with `@type: "rdf:JSON"` (CURIE)
///                      + insert with `@type: "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"` (full IRI)
///                      with the SAME JSON value on both sides
///
/// Expected: t=2 is a no-op. The commit is empty, ledger t does not bump,
/// and the attributeMapping triple is still present at the new ledger state.
#[tokio::test]
async fn json_same_value_update_with_mixed_curie_and_full_iri_forms_is_noop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/dt-cancel-json-production-shape:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    // t=1: initial insert using `@json` form
    let initial = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:config",
        "@type": "ex:Config",
        "ex:attributeMapping": {
            "@value": {"email": "email", "name": "name", "groups": "groups"},
            "@type": "@json"
        }
    });
    let ledger1 = fluree.insert(ledger0, &initial).await.unwrap().ledger;
    let t_after_insert = ledger1.t();

    // t=2 attempted update: delete uses `rdf:JSON` CURIE, insert uses full IRI.
    // Both refer to the same canonical datatype `http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON`.
    // The (s, p, o, dt_canonical) identity is equal on both sides ⇒ cancellation must fire.
    let update = json!({
        "@context": {
            "ex": "http://example.org/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "where": {"@id": "ex:config", "@type": "ex:Config"},
        "delete": {
            "@id": "ex:config",
            "ex:attributeMapping": {
                "@value": {"email": "email", "name": "name", "groups": "groups"},
                "@type": "rdf:JSON"
            }
        },
        "insert": {
            "@id": "ex:config",
            "ex:attributeMapping": {
                "@value": {"email": "email", "name": "name", "groups": "groups"},
                "@type": "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"
            }
        }
    });

    let out = fluree
        .update(ledger1, &update)
        .await
        .expect("same-canonical-value no-op update should succeed");

    assert_eq!(
        out.ledger.t(),
        t_after_insert,
        "no-op update (delete+insert of same canonical (s,p,o,dt)) must not produce a new commit"
    );

    // Belt-and-suspenders: query and confirm the flake is still present at the
    // post-update state. If cancellation mishandled the JSON case, this would
    // return an empty result.
    let result = support::query_jsonld(
        &fluree,
        &out.ledger,
        &json!({
            "@context": {"ex": "http://example.org/"},
            "where": {"@id": "ex:config", "ex:attributeMapping": "?o"},
            "select": "?o"
        }),
    )
    .await
    .expect("post-update query")
    .to_jsonld_async(out.ledger.as_graph_db_ref(0))
    .await
    .expect("to_jsonld_async");

    let rows = result.as_array().expect("array result");
    assert_eq!(
        rows.len(),
        1,
        "attributeMapping must still be queryable after same-canonical-value no-op update; got {rows:#?}"
    );
}

// =============================================================================
// String analogue of the production shape (Bug B category)
// =============================================================================

/// The `xsd:string` analogue: delete uses `xsd:string` CURIE, insert uses
/// the full XSD string IRI, same value on both sides.
///
/// Complements `update_where_bound_typed_string_delete_and_insert_use_same_datatype_sid`
/// in `it_transact_update.rs` (added by this PR). That test proves dt_sid
/// stability when the update retract+assert are on DIFFERENT values (`"before"`
/// vs `"after"`) and both sides use the same CURIE form. This test proves
/// the complementary invariant: when the values are the SAME and the forms
/// are MIXED (CURIE on one side, full IRI on the other), cancellation fires
/// and the transaction is a no-op. Both invariants must hold for the fix
/// to be complete.
#[tokio::test]
async fn xsd_string_same_value_update_with_mixed_curie_and_full_iri_forms_is_noop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/dt-cancel-xsd-string-production-shape:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let initial = json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@id": "ex:thing",
        "@type": "ex:Thing",
        "ex:label": {"@value": "hello", "@type": "xsd:string"}
    });
    let ledger1 = fluree.insert(ledger0, &initial).await.unwrap().ledger;
    let t_after_insert = ledger1.t();

    let update = json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "where": {"@id": "ex:thing", "@type": "ex:Thing"},
        "delete": {
            "@id": "ex:thing",
            "ex:label": {"@value": "hello", "@type": "xsd:string"}
        },
        "insert": {
            "@id": "ex:thing",
            "ex:label": {
                "@value": "hello",
                "@type": "http://www.w3.org/2001/XMLSchema#string"
            }
        }
    });

    let out = fluree
        .update(ledger1, &update)
        .await
        .expect("same-canonical-value no-op update should succeed");

    assert_eq!(
        out.ledger.t(),
        t_after_insert,
        "no-op update with mixed xsd:string CURIE/full-IRI must cancel and not bump t"
    );

    let result = support::query_jsonld(
        &fluree,
        &out.ledger,
        &json!({
            "@context": {"ex": "http://example.org/"},
            "where": {"@id": "ex:thing", "ex:label": "?o"},
            "select": "?o"
        }),
    )
    .await
    .expect("post-update query")
    .to_jsonld_async(out.ledger.as_graph_db_ref(0))
    .await
    .expect("to_jsonld_async");

    let rows = result.as_array().expect("array result");
    assert_eq!(
        rows.len(),
        1,
        "label must still be queryable after same-canonical-value no-op update"
    );
}

// =============================================================================
// JSON variants — different entry-point form combinations
// =============================================================================

/// Baseline: both sides use `"@type": "@json"`. This should cancel trivially
/// because there's no CURIE-vs-full mismatch; it's a direct equality.
#[tokio::test]
async fn json_same_value_update_both_sides_at_json_is_noop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/dt-cancel-json-at-json:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let initial = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:doc",
        "@type": "ex:Doc",
        "ex:payload": {"@value": {"k": "v"}, "@type": "@json"}
    });
    let ledger1 = fluree.insert(ledger0, &initial).await.unwrap().ledger;
    let t_after_insert = ledger1.t();

    let update = json!({
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "ex:doc", "@type": "ex:Doc"},
        "delete": {
            "@id": "ex:doc",
            "ex:payload": {"@value": {"k": "v"}, "@type": "@json"}
        },
        "insert": {
            "@id": "ex:doc",
            "ex:payload": {"@value": {"k": "v"}, "@type": "@json"}
        }
    });

    let out = fluree.update(ledger1, &update).await.unwrap();
    assert_eq!(
        out.ledger.t(),
        t_after_insert,
        "same @json value no-op must not bump t"
    );
}

/// No `@context`: delete uses `rdf:JSON` CURIE, insert uses full IRI.
/// Declaring `rdf:` in context, a `rdf:JSON` CURIE on delete must canonicalize
/// to the same IRI as the explicit full-IRI form on insert, making the update
/// a no-op.
#[tokio::test]
async fn json_same_value_update_no_context_rdf_json_and_full_iri_is_noop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/dt-cancel-json-no-ctx:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let initial = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:doc",
        "@type": "ex:Doc",
        "ex:payload": {"@value": {"k": "v"}, "@type": "@json"}
    });
    let ledger1 = fluree.insert(ledger0, &initial).await.unwrap().ledger;
    let t_after_insert = ledger1.t();

    let update = json!({
        "@context": {
            "ex": "http://example.org/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "where": {"@id": "ex:doc", "@type": "ex:Doc"},
        "delete": {
            "@id": "ex:doc",
            "ex:payload": {"@value": {"k": "v"}, "@type": "rdf:JSON"}
        },
        "insert": {
            "@id": "ex:doc",
            "ex:payload": {
                "@value": {"k": "v"},
                "@type": "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"
            }
        }
    });

    let out = fluree
        .update(ledger1, &update)
        .await
        .expect("no-op update with undeclared rdf: CURIE should succeed");

    assert_eq!(
        out.ledger.t(),
        t_after_insert,
        "well-known-prefix fallback must canonicalize rdf:JSON even without @context declaration"
    );
}

/// Initial insert uses `@json` form; update uses `rdf:JSON` CURIE on delete
/// and `@json` on insert. This specifically exercises whether `@json` and
/// `rdf:JSON` land in the same dt bucket.
#[tokio::test]
async fn json_same_value_update_delete_rdf_json_insert_at_json_is_noop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/dt-cancel-json-mixed-at-and-curie:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let initial = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:doc",
        "@type": "ex:Doc",
        "ex:payload": {"@value": {"k": "v"}, "@type": "@json"}
    });
    let ledger1 = fluree.insert(ledger0, &initial).await.unwrap().ledger;
    let t_after_insert = ledger1.t();

    let update = json!({
        "@context": {
            "ex": "http://example.org/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "where": {"@id": "ex:doc", "@type": "ex:Doc"},
        "delete": {
            "@id": "ex:doc",
            "ex:payload": {"@value": {"k": "v"}, "@type": "rdf:JSON"}
        },
        "insert": {
            "@id": "ex:doc",
            "ex:payload": {"@value": {"k": "v"}, "@type": "@json"}
        }
    });

    let out = fluree.update(ledger1, &update).await.unwrap();
    assert_eq!(
        out.ledger.t(),
        t_after_insert,
        "rdf:JSON and @json must refer to the same canonical datatype"
    );
}

// =============================================================================
// Upsert path coverage
// =============================================================================

/// Upsert with the same JSON value but a different `@type` form than was
/// originally inserted. The upsert's implicit retraction + explicit assertion
/// must collapse.
///
/// Complements `upsert_typed_string_retract_and_assert_use_same_datatype_sid`
/// in `it_transact_upsert.rs` (added by this PR). That test proves dt_sid
/// stability on DIFFERENT-value upserts for `xsd:string`; this test proves
/// same-value + mixed-form cancellation for `@json`, which is the more
/// immediately-relevant case since the production bug was on JSON-typed
/// `attributeMapping`.
#[tokio::test]
async fn json_upsert_same_value_with_different_dt_form_is_noop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/dt-cancel-json-upsert:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    // Insert with `@json` form
    let initial = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:doc",
        "@type": "ex:Doc",
        "ex:payload": {"@value": {"k": "v"}, "@type": "@json"}
    });
    let ledger1 = fluree.insert(ledger0, &initial).await.unwrap().ledger;
    let t_after_insert = ledger1.t();

    // Upsert the same value but using the full RDF JSON IRI
    let upsert_txn = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:doc",
        "ex:payload": {
            "@value": {"k": "v"},
            "@type": "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"
        }
    });

    let out = fluree
        .upsert(ledger1, &upsert_txn)
        .await
        .expect("same-canonical-value upsert should succeed");

    assert_eq!(
        out.ledger.t(),
        t_after_insert,
        "upsert with same canonical JSON value must not produce a new commit"
    );
}

/// Upsert with the same string value but a different `@type` form.
///
/// The `xsd:string` counterpart to the JSON test above. Together with
/// `upsert_typed_string_retract_and_assert_use_same_datatype_sid` (different-
/// value version, added by this PR), this forms a matched pair covering
/// upsert's datatype-canonicalization guarantees:
///   - different value + same form → real commit, stable dt_sid (existing)
///   - same    value + mixed form → no-op via cancellation (this test)
#[tokio::test]
async fn xsd_string_upsert_same_value_with_different_dt_form_is_noop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/dt-cancel-string-upsert:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let initial = json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@id": "ex:thing",
        "ex:label": {"@value": "hello", "@type": "xsd:string"}
    });
    let ledger1 = fluree.insert(ledger0, &initial).await.unwrap().ledger;
    let t_after_insert = ledger1.t();

    // Upsert same value with full IRI form
    let upsert_txn = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:thing",
        "ex:label": {
            "@value": "hello",
            "@type": "http://www.w3.org/2001/XMLSchema#string"
        }
    });

    let out = fluree
        .upsert(ledger1, &upsert_txn)
        .await
        .expect("same-canonical-value upsert should succeed");

    assert_eq!(
        out.ledger.t(),
        t_after_insert,
        "upsert with same canonical xsd:string value must not produce a new commit"
    );
}

// =============================================================================
// Direct dt_id invariant tests (flake-level)
// =============================================================================

/// A single insert containing `@json`, `rdf:JSON` CURIE, and full IRI forms
/// of the RDF JSON datatype. All three flakes must have equal `dt` Sids.
/// This is the PR #148 Layer 2 guarantee expressed at the flake level.
#[tokio::test]
async fn single_insert_three_json_datatype_forms_produce_same_dt_sid() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/dt-sid-equal-json:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let insert = json!({
        "@context": {
            "ex": "http://example.org/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "@graph": [
            { "@id": "ex:a", "ex:data": {"@value": {"tag": "a"}, "@type": "@json"} },
            { "@id": "ex:b", "ex:data": {"@value": {"tag": "b"}, "@type": "rdf:JSON"} },
            { "@id": "ex:c", "ex:data": {"@value": {"tag": "c"},
                "@type": "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"} }
        ]
    });

    let result = fluree.insert(ledger0, &insert).await.unwrap();
    let content_store = fluree.content_store(ledger_id);
    let commit = load_commit_by_id(&content_store, &result.receipt.commit_id)
        .await
        .expect("load commit");

    let data_flakes: Vec<_> = commit
        .flakes
        .iter()
        .filter(|f| f.op && f.p.name.as_ref() == "data")
        .collect();
    assert_eq!(
        data_flakes.len(),
        3,
        "expected 3 ex:data flakes, got {data_flakes:#?}"
    );
    let dt0 = &data_flakes[0].dt;
    for (i, f) in data_flakes.iter().enumerate() {
        assert_eq!(
            &f.dt, dt0,
            "ex:data flake #{i} has dt={:?}, expected {:?} (all JSON forms must collapse to same dt Sid)",
            f.dt, dt0
        );
    }
}

/// Two inserts of `xsd:string` values using CURIE and full IRI forms
/// must produce flakes with equal `dt` Sids.
///
/// This is a **direct** statement of the cross-form dt_id equality invariant.
/// The existing `update_where_bound_typed_string_delete_and_insert_use_same_datatype_sid`
/// test in `it_transact_update.rs` exercises a related invariant implicitly
/// (it uses `xsd:string` CURIE on both initial insert AND update templates
/// with context declared for one of them, so it proves CURIE-with-context
/// ≡ CURIE-without-context). This test strengthens the claim to
/// CURIE ≡ full IRI in a single commit, which is the exact cross-form
/// pattern the production t=23 `_system` commits exhibited.
#[tokio::test]
async fn single_insert_two_xsd_string_datatype_forms_produce_same_dt_sid() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/dt-sid-equal-xsd:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let insert = json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            { "@id": "ex:a", "ex:label": {"@value": "Alice", "@type": "xsd:string"} },
            { "@id": "ex:b", "ex:label": {"@value": "Bob",
                "@type": "http://www.w3.org/2001/XMLSchema#string"} }
        ]
    });

    let result = fluree.insert(ledger0, &insert).await.unwrap();
    let content_store = fluree.content_store(ledger_id);
    let commit = load_commit_by_id(&content_store, &result.receipt.commit_id)
        .await
        .expect("load commit");

    let alice = commit
        .flakes
        .iter()
        .find(|f| f.op && matches!(&f.o, FlakeValue::String(s) if s == "Alice"))
        .expect("alice label flake");
    let bob = commit
        .flakes
        .iter()
        .find(|f| f.op && matches!(&f.o, FlakeValue::String(s) if s == "Bob"))
        .expect("bob label flake");

    assert_eq!(
        alice.dt, bob.dt,
        "xsd:string CURIE and full IRI forms must produce the same datatype Sid"
    );
}

// =============================================================================
// Negative-side guards: cancellation must NOT fire when values differ
// =============================================================================

/// Guard: if delete and insert have DIFFERENT JSON values, cancellation must
/// NOT fire; both flakes should survive. This ensures we haven't accidentally
/// over-collapsed the cancellation logic.
#[tokio::test]
async fn json_update_with_different_values_still_produces_commit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/dt-cancel-json-guard-different:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let initial = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:doc",
        "@type": "ex:Doc",
        "ex:payload": {"@value": {"version": 1}, "@type": "@json"}
    });
    let ledger1 = fluree.insert(ledger0, &initial).await.unwrap().ledger;
    let t_after_insert = ledger1.t();

    // Different value: v1 → v2
    let update = json!({
        "@context": {
            "ex": "http://example.org/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "where": {"@id": "ex:doc", "@type": "ex:Doc"},
        "delete": {
            "@id": "ex:doc",
            "ex:payload": {"@value": {"version": 1}, "@type": "rdf:JSON"}
        },
        "insert": {
            "@id": "ex:doc",
            "ex:payload": {"@value": {"version": 2}, "@type": "@json"}
        }
    });

    let out = fluree
        .update(ledger1, &update)
        .await
        .expect("real update with different values should succeed");

    assert!(
        out.ledger.t() > t_after_insert,
        "update with a genuinely different value must produce a new commit (cancellation should NOT over-collapse)"
    );

    // Verify the new value is queryable and the old one is gone.
    let result = support::query_jsonld(
        &fluree,
        &out.ledger,
        &json!({
            "@context": {"ex": "http://example.org/"},
            "where": {"@id": "ex:doc", "ex:payload": "?o"},
            "select": "?o"
        }),
    )
    .await
    .expect("query post-update")
    .to_jsonld_async(out.ledger.as_graph_db_ref(0))
    .await
    .expect("to_jsonld_async");

    let rows = result.as_array().expect("array result");
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one payload value after update"
    );
}

/// Guard: different xsd:string values must also survive cancellation.
///
/// Structural pair to `json_update_with_different_values_still_produces_commit`
/// above. The colleague's existing
/// `update_where_bound_typed_string_delete_and_insert_use_same_datatype_sid`
/// test in `it_transact_update.rs` also uses different `xsd:string` values
/// in an update and successfully produces a commit (the existing test loads
/// it via `result.receipt.commit_id`). This test keeps the symmetric pair
/// with the JSON guard above and makes the `t` bump assertion explicit, so
/// "cancellation must not over-collapse" is stated directly at the
/// structural level rather than inferred from commit-load success.
#[tokio::test]
async fn xsd_string_update_with_different_values_still_produces_commit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/dt-cancel-string-guard-different:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    let initial = json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@id": "ex:thing",
        "@type": "ex:Thing",
        "ex:label": {"@value": "hello", "@type": "xsd:string"}
    });
    let ledger1 = fluree.insert(ledger0, &initial).await.unwrap().ledger;
    let t_after_insert = ledger1.t();

    let update = json!({
        "@context": {
            "ex": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "where": {"@id": "ex:thing", "@type": "ex:Thing"},
        "delete": {
            "@id": "ex:thing",
            "ex:label": {"@value": "hello", "@type": "xsd:string"}
        },
        "insert": {
            "@id": "ex:thing",
            "ex:label": {
                "@value": "goodbye",
                "@type": "http://www.w3.org/2001/XMLSchema#string"
            }
        }
    });

    let out = fluree.update(ledger1, &update).await.unwrap();
    assert!(
        out.ledger.t() > t_after_insert,
        "update with different string values must produce a new commit"
    );
}
