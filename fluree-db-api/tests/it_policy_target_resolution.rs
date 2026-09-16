//! Inline-policy targeting resolution: a target that cannot resolve must not
//! silently turn the rule into something else.
//!
//! Three ways a class-targeted restriction was measured to stop restricting on
//! `main`, all under `default-allow: false`, all leaking rows a broad grant
//! beside them then returned:
//!
//! 1. a broken `rdfs:subClassOf` edge shrinks the `for_classes` closure that
//!    `build_policy_set` pre-expands;
//! 2. a target IRI written as a prefixed name resolves to the EMPTY namespace,
//!    which can never equal a real subject's class SID, so the rule is inert;
//! 3. a policy key spelled through a non-canonical prefix is not recognised at
//!    all, so targeting vanishes and the rule becomes an *untargeted default*
//!    applying to every flake — which widens a narrow grant to everything.
//!
//! **2 and 3 are fixed here**; they now fail the request at parse time.
//! Inertness is harmless for a grant and a data leak for a restriction, and the
//! parser cannot tell which it is holding, so it refuses to guess in either
//! direction. Failing the request also means no `PolicySet` is ever built, so
//! no downstream consumer — the per-flake evaluator, `PolicySet::covers_predicate`,
//! or the raw-row cursor fast path that skips filtering entirely — can act on a
//! collapsed target.
//!
//! **1 is deliberately not changed.** With the edge broken, the subject
//! genuinely is not an instance of the policy's class: the target resolves, the
//! rule applies to exactly the subjects the data says are in scope, and nothing
//! at build time distinguishes that from a restriction deliberately scoped to a
//! class with no instances yet. It is a data defect (a dangling `subClassOf`
//! object) with a security consequence, not a targeting collapse. What the
//! engine owes there is visibility, not a guess — and the guess has a specific
//! cost, pinned by `broken_subclass_edge_on_a_grant_must_keep_failing_closed`.
//!
//! A fourth event must not be confused with any of these: an empty policy-class
//! *selection* (`opts.policy_class == Some([])`) is deliberate and keeps reading
//! unrestricted. See `empty_policy_class_selection_still_reads_unrestricted`.

use crate::support;
use crate::support::{assert_index_defaults, genesis_ledger, normalize_rows};
use fluree_db_api::{policy_builder, FlureeBuilder, GovernanceOptions};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/ns/";

/// Seed a documents ledger whose `ex:TopSecret` parent IRI is `top_secret_parent`.
///
/// `ex:doc2` is multi-typed `ex:Document, ex:TopSecret`: its membership in the
/// *grant* class is asserted, its membership in the *restriction* class is
/// entailed through the subclass edge under test.
async fn seed_documents(
    fluree: &support::MemoryFluree,
    ledger_id: &str,
    top_secret_parent: &str,
) -> support::MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    // Schema first (separate transaction), then data — matching the shape the
    // rest of the policy suite uses for hierarchy tests.
    let ledger1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex": EX, "rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
                "@graph": [
                    {"@id": "ex:Confidential", "rdfs:subClassOf": {"@id": "ex:Document"}},
                    {"@id": "ex:TopSecret", "rdfs:subClassOf": {"@id": top_secret_parent}}
                ]
            }),
        )
        .await
        .expect("schema txn");

    fluree
        .insert(
            ledger1.ledger,
            &json!({
                "@context": {"ex": EX},
                "@graph": [
                    {"@id": "ex:doc1", "@type": "ex:Document", "ex:title": "public"},
                    {"@id": "ex:doc2", "@type": ["ex:Document", "ex:TopSecret"], "ex:title": "SECRET"}
                ]
            }),
        )
        .await
        .expect("data txn")
        .ledger
}

/// `SELECT ?t WHERE { ?s ex:title ?t }` under the supplied inline policy.
fn titles_query(ledger_id: &str, policy: JsonValue) -> JsonValue {
    json!({
        "@context": {"ex": EX, "fluree": "https://ns.flur.ee/db#"},
        "from": ledger_id,
        "opts": {"policy": policy, "default-allow": false},
        "select": "?t",
        "where": {"@id": "?s", "ex:title": "?t"}
    })
}

/// `f:onClass ex:Document` → allow; everything else is per-test.
fn allow_documents() -> JsonValue {
    json!({
        "@id": "http://example.org/ns/allow-documents",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:onClass": [{"@id": "http://example.org/ns/Document"}],
        "f:allow": true
    })
}

async fn titles_visible(
    fluree: &support::MemoryFluree,
    ledger_id: &str,
    policy: JsonValue,
) -> Vec<JsonValue> {
    let result = fluree
        .query_connection(&titles_query(ledger_id, policy))
        .await
        .expect("query_connection");
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    normalize_rows(&jsonld)
}

// ===========================================================================
// Route A — a broken `rdfs:subClassOf` edge
// ===========================================================================

/// Control for routes B and C: with the subclass edge **intact**, the
/// class-targeted deny reaches the subclass instance and hides it.
///
/// This is the half of route A that the engine owes. The other half — a
/// *broken* edge, where `ex:doc2` genuinely is not an `ex:Confidential` and the
/// row comes back — is a data defect, not a targeting collapse: the policy
/// resolves perfectly and applies to exactly the subjects the data says are in
/// scope. Nothing at policy-build time can distinguish it from a restriction
/// deliberately scoped to a class with no instances yet, and the *mirror* shape
/// (a class-targeted grant on the same broken edge) correctly denies, so a fix
/// that made an empty closure match everything would invert a working
/// fail-closed path into a universal deny.
#[tokio::test]
async fn route_a_intact_subclass_edge_lets_the_deny_reach_the_subclass_instance() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/target-a-intact:main";
    seed_documents(&fluree, ledger_id, "ex:Confidential").await;

    let policy = json!([
        allow_documents(),
        {
            "@id": "http://example.org/ns/deny-confidential",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:onClass": [{"@id": "http://example.org/ns/Confidential"}],
            "f:allow": false
        }
    ]);

    assert_eq!(
        titles_visible(&fluree, ledger_id, policy).await,
        normalize_rows(&json!(["public"])),
        "with ex:TopSecret ⊑ ex:Confidential asserted, the f:onClass deny must \
         hide the entailed instance"
    );
}

// ===========================================================================
// Route B — an inline target written as a prefixed name
// ===========================================================================

/// A prefixed target name is rejected rather than silently encoded into the
/// EMPTY namespace, where it can never equal a real class SID.
///
/// `resolve_iri_to_sid` used `encode_iri()`, which never fails: an
/// unregistered prefix falls back to `Sid(EMPTY, "<the whole string>")`. That
/// SID is well-formed and disjoint from every SID the ledger actually holds, so
/// the restriction parsed cleanly, targeted nothing, and the broad grant beside
/// it returned `SECRET`. The request `@context` does not reach policy targets
/// even though it does reach the `where` clause in the same request, so the
/// author has no way to see the difference.
#[tokio::test]
async fn route_b_prefixed_target_iri_is_rejected_not_silently_inert() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/target-b:main";
    seed_documents(&fluree, ledger_id, "ex:Confidential").await;

    let policy = json!([
        allow_documents(),
        {
            "@id": "http://example.org/ns/deny-confidential",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            // Declared in the request's own @context and used successfully in
            // the same request's `where` clause — still not expanded here.
            "f:onClass": [{"@id": "ex:Confidential"}],
            "f:allow": false
        }
    ]);

    let err = fluree
        .query_connection(&titles_query(ledger_id, policy))
        .await
        .expect_err("a policy target that cannot resolve must fail the request");
    let msg = err.to_string();
    assert!(
        msg.contains("ex:Confidential") && msg.contains("f:onClass"),
        "error must name the offending key and IRI, got: {msg}"
    );
}

/// The same rule spelled with an absolute IRI still works — the fix rejects
/// unresolvable targets, it does not reject class targeting.
#[tokio::test]
async fn route_b_absolute_target_iri_still_restricts() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/target-b-absolute:main";
    seed_documents(&fluree, ledger_id, "ex:Confidential").await;

    let policy = json!([
        allow_documents(),
        {
            "@id": "http://example.org/ns/deny-confidential",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:onClass": [{"@id": "http://example.org/ns/Confidential"}],
            "f:allow": false
        }
    ]);

    assert_eq!(
        titles_visible(&fluree, ledger_id, policy).await,
        normalize_rows(&json!(["public"])),
        "absolute-IRI targeting must be unaffected by the resolution guard"
    );
}

/// A target IRI in a namespace the ledger has simply never seen is also
/// rejected. It is provably inert — no flake in the ledger can carry a SID in
/// an unregistered namespace — so accepting it would reproduce route B with a
/// different spelling.
#[tokio::test]
async fn route_b_unknown_namespace_target_is_rejected() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/target-b-unknown-ns:main";
    seed_documents(&fluree, ledger_id, "ex:Confidential").await;

    let policy = json!([{
        "@id": "http://example.org/ns/deny-secret",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:onProperty": [{"@id": "http://never-seen.example/ns/title"}],
        "f:allow": false
    }]);

    let err = fluree
        .query_connection(&titles_query(ledger_id, policy))
        .await
        .expect_err("an unregistered-namespace target must fail the request");
    assert!(
        err.to_string().contains("f:onProperty"),
        "error must name the offending key, got: {err}"
    );
}

// ===========================================================================
// Route C — a policy key spelled through a non-canonical prefix
// ===========================================================================

/// The sharpest of the three: an unrecognised key took the policy's targeting
/// with it, converting "allow only instances of X" into an **untargeted default
/// policy applying to every flake**.
///
/// `fluree:onClass` names the identical IRI as `f:onClass` and the prefix is
/// declared in the request `@context`, but policy keys are matched as literal
/// strings, so `had_on_class` was never set, `TargetMode::Default` was chosen,
/// and the rule landed in `PolicySet::defaults` — the bucket
/// `policy_entries_for_flake` adds to *every* candidate list. No warning fired
/// on any channel.
#[tokio::test]
async fn route_c_aliased_policy_key_is_rejected_not_read_as_untargeted() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/target-c:main";
    seed_documents(&fluree, ledger_id, "ex:Confidential").await;

    // Intent: allow only instances of a class nothing is an instance of, i.e.
    // allow nothing. Read as untargeted, it allowed everything.
    let policy = json!([{
        "@id": "http://example.org/ns/allow-nonexistent",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "fluree:onClass": [{"@id": "http://example.org/ns/Nonexistent"}],
        "f:allow": true
    }]);

    let err = fluree
        .query_connection(&titles_query(ledger_id, policy))
        .await
        .expect_err("an aliased policy key must fail the request");
    let msg = err.to_string();
    assert!(
        msg.contains("fluree:onClass") && msg.contains("f:onClass"),
        "error must name the offending key and the canonical spelling, got: {msg}"
    );
}

/// A misspelled fluree-namespace term drops targeting the same way and is
/// rejected the same way. `f:onClas` is not a known term, so before the guard
/// it was silently dropped and the rule became an untargeted default.
#[tokio::test]
async fn route_c_misspelled_fluree_term_is_rejected() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/target-c-typo:main";
    seed_documents(&fluree, ledger_id, "ex:Confidential").await;

    let policy = json!([{
        "@id": "http://example.org/ns/allow-nonexistent",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:onClas": [{"@id": "http://example.org/ns/Nonexistent"}],
        "f:allow": true
    }]);

    let err = fluree
        .query_connection(&titles_query(ledger_id, policy))
        .await
        .expect_err("an unknown fluree-namespace policy term must fail the request");
    assert!(
        err.to_string().contains("f:onClas"),
        "error must name the offending key, got: {err}"
    );
}

/// The guard must not fire on non-policy vocabulary. A policy node may carry
/// descriptive terms from other namespaces; only the fluree-db namespace and
/// the recognised policy term names are governed.
#[tokio::test]
async fn foreign_vocabulary_on_a_policy_node_is_still_accepted() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/target-c-foreign:main";
    seed_documents(&fluree, ledger_id, "ex:Confidential").await;

    let policy = json!([{
        "@id": "http://example.org/ns/allow-documents",
        "@type": "f:AccessPolicy",
        "rdfs:label": "Everyone may read documents",
        "http://purl.org/dc/terms/creator": "ops",
        "f:action": "f:view",
        "f:onClass": [{"@id": "http://example.org/ns/Document"}],
        "f:allow": true
    }]);

    assert_eq!(
        titles_visible(&fluree, ledger_id, policy).await,
        normalize_rows(&json!(["SECRET", "public"])),
        "descriptive vocabulary on a policy node must not be rejected"
    );
}

// ===========================================================================
// Route C × PR #1855 — does the collapsed rule reach the `no_rules` guard?
// ===========================================================================

/// Definitive answer to whether the route-C collapse interacts with #1855's
/// `is_root` change, checked on the *pre-fix* shape so the answer survives the
/// fix.
///
/// #1855 makes root require an explicit `default_allow: true` once any policy
/// input exists, guarded by
/// `no_rules = view_set.restrictions.is_empty() && modify_set.restrictions.is_empty()`.
/// A collapsed route-C rule is still pushed into `view_set.restrictions` by
/// `build_policy_set` before the `target_mode` match runs, so `no_rules` is
/// false and the guard is never reached. The two changes cannot see each other.
///
/// This is asserted against a rule with **no targeting written at all**, which
/// is the state route C produced and which remains legal: an untargeted default
/// policy is a real, supported shape.
#[tokio::test]
async fn untargeted_rule_lands_in_restrictions_so_1855_no_rules_guard_is_unreachable() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/target-c-isroot:main";
    let ledger = seed_documents(&fluree, ledger_id, "ex:Confidential").await;

    let opts = GovernanceOptions {
        policy: Some(json!([{
            "@id": "http://example.org/ns/untargeted",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:allow": true
        }])),
        default_allow: Some(false),
        ..Default::default()
    };

    let ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
        &[0],
    )
    .await
    .expect("build policy context");

    let wrapper = ctx.wrapper();
    assert!(
        !wrapper.is_root(),
        "an untargeted rule must not collapse the context to root"
    );
    assert_eq!(
        wrapper.view().restrictions.len(),
        1,
        "the rule must be present in view_set.restrictions — this is what makes \
         #1855's `no_rules` guard unreachable for this shape"
    );
    assert_eq!(
        wrapper.view().defaults.len(),
        1,
        "an untargeted rule lands in the defaults bucket, which \
         policy_entries_for_flake adds to every candidate list"
    );
}

// ===========================================================================
// The other "resolved to nothing" — set SELECTION, which must stay root
// ===========================================================================

/// `opts.policy_class == Some([])` is a *deliberate* empty selection and must
/// keep reading unrestricted. This is a different event from a rule's targeting
/// collapsing, and the guard added for the latter must not reach it.
///
/// It is the only spelling a caller has for "no stored rules" on a
/// governance-bound ledger — the hosted router relies on it for privileged and
/// internal reads, because since fail-closed defaults a request with no policy
/// inputs at all is governed by the configured policies rather than root. The
/// two events sit at different layers (set selection vs. a rule's targeting),
/// and the fix lives entirely inside inline-policy parsing, which an empty
/// class list never enters: `opts.policy` is `None`, so `parse_inline_policy`
/// is not called.
#[tokio::test]
async fn empty_policy_class_selection_still_reads_unrestricted() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/empty-class-selection:main";
    let ledger = seed_documents(&fluree, ledger_id, "ex:Confidential").await;

    // A stored rule exists and would deny everything if it were selected.
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@graph": [{
                    "@id": "http://example.org/ns/denyAll",
                    "@type": [
                        "https://ns.flur.ee/db#AccessPolicy",
                        "http://example.org/ns/Restrictive"
                    ],
                    "https://ns.flur.ee/db#action": {"@id": "https://ns.flur.ee/db#view"},
                    "https://ns.flur.ee/db#allow": false
                }]
            }),
        )
        .await
        .expect("stored policy txn")
        .ledger;

    let _ = ledger;

    let query_with_classes = |classes: JsonValue| {
        json!({
            "@context": {"ex": EX},
            "from": ledger_id,
            "opts": {"policy-class": classes, "default-allow": true},
            "select": "?t",
            "where": {"@id": "?s", "ex:title": "?t"}
        })
    };

    async fn rows(fluree: &support::MemoryFluree, ledger_id: &str, q: JsonValue) -> Vec<JsonValue> {
        let result = fluree.query_connection(&q).await.expect("query_connection");
        let led = fluree.ledger(ledger_id).await.expect("ledger");
        let jsonld = result.to_jsonld(&led.snapshot).expect("to_jsonld");
        normalize_rows(&jsonld)
    }

    // Control: naming the class selects the stored deny and hides everything.
    assert_eq!(
        rows(
            &fluree,
            ledger_id,
            query_with_classes(json!(["http://example.org/ns/Restrictive"]))
        )
        .await,
        normalize_rows(&json!([])),
        "naming the policy class must select the stored deny"
    );

    // The case that must not regress.
    assert_eq!(
        rows(&fluree, ledger_id, query_with_classes(json!([]))).await,
        normalize_rows(&json!(["SECRET", "public"])),
        "`policy-class: [] + default-allow: true` must read unrestricted — this \
         is the hosted router's only spelling for a privileged read on a \
         governance-bound ledger"
    );
}

/// Guard on the direction a fix must not invert.
///
/// A class-targeted **grant** whose subclass edge is broken reaches no
/// subclass instance and therefore grants nothing. That is fail-closed and
/// correct, and it is the shape most people test, which is why the mirror
/// asymmetry survived review. Any future repair for the broken-edge case that
/// works by making an empty or shrunken class closure match *everything* turns
/// this row into a universal grant; this test fails first if that happens.
#[tokio::test]
async fn broken_subclass_edge_on_a_grant_must_keep_failing_closed() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/target-a-mirror:main";
    // One typo'd parent IRI: ex:TopSecret no longer reaches ex:Confidential.
    seed_documents(&fluree, ledger_id, "ex:Confidentail").await;

    let policy = json!([{
        "@id": "http://example.org/ns/allow-confidential",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:onClass": [{"@id": "http://example.org/ns/Confidential"}],
        "f:allow": true
    }]);
    assert_eq!(
        titles_visible(&fluree, ledger_id, policy).await,
        normalize_rows(&json!([])),
        "a grant that cannot reach its subclass instances must grant nothing"
    );
}
