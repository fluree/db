//! `f:modify` half of "a policy whose targeting resolves to nothing".
//!
//! `it_policy_target_resolution.rs` pins the read side. These pin the write
//! side, which has the same hole through the same code: the policy builder
//! parses targeting once and hands the finished `PolicyContext` to both
//! `build_policy_set` calls, and `fluree-db-transact` never rebuilds it. One
//! guard therefore has to cover both — and did not, so each of these was a
//! live universal **write** grant.
//!
//! Each case keeps the probe form it was found in: a CONTROL arm that must
//! deny (without it, "the write was refused" proves nothing) and a DEFECT arm
//! that was measured letting the write through.
//!
//! Two related fail-opens are deliberately **not** covered here, because this
//! change does not fix them and a test that pins a leak is worse than no test:
//!
//! * a broken `rdfs:subClassOf` edge making an `f:modify` deny inert — the
//!   subject genuinely is not an instance of the policy's class, so this is a
//!   data defect, not a targeting collapse (see the module docs on
//!   `it_policy_target_resolution.rs`);
//! * a class-targeted policy going inert on a property written after the last
//!   index, via the class→property stats projection in
//!   `fluree-db-policy/src/index.rs`. That one never touches the target set at
//!   all: `for_classes` is fully populated and the restriction simply is not
//!   projected into `by_property`. Confirmed reachable when
//!   `novelty_for_stats` is `None` (`fluree-db-api/src/block_fetch.rs`).

#![cfg(feature = "native")]

use crate::support;
use crate::support::{assert_index_defaults, genesis_ledger};
use fluree_db_api::policy_builder;
use fluree_db_api::{
    CommitOpts, FlureeBuilder, GovernanceOptions, IndexConfig, PolicyContext,
    TrackedTransactionInput, TxnOpts, TxnType,
};
use serde_json::{json, Value};

const EX: &str = "http://example.org/";

fn schema_and_data() -> Value {
    json!({
        "@context": {
            "ex": EX,
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "@graph": [
            {"@id": "ex:Document", "@type": "rdfs:Class"},
            {"@id": "ex:Confidential", "@type": "rdfs:Class",
             "rdfs:subClassOf": {"@id": "ex:Document"}},
            {"@id": "ex:TopSecret", "@type": "rdfs:Class",
             "rdfs:subClassOf": {"@id": "ex:Confidential"}},
            {"@id": "ex:doc1", "@type": "ex:Document", "ex:title": "public"},
            {"@id": "ex:doc2", "@type": ["ex:Document", "ex:TopSecret"],
             "ex:title": "SECRET"}
        ]
    })
}

async fn seeded(ledger_name: &str) -> (support::MemoryFluree, support::MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, ledger_name);
    let ledger = fluree
        .insert(ledger0, &schema_and_data())
        .await
        .expect("seed")
        .ledger;
    (fluree, ledger)
}

/// Build the policy context, surfacing the builder's own error rather than
/// unwrapping it — refusing a collapsed target *is* the behavior under test.
async fn try_build(
    ledger: &support::MemoryLedger,
    opts: &GovernanceOptions,
) -> std::result::Result<PolicyContext, String> {
    policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        opts,
        &[0],
    )
    .await
    .map_err(|e| e.to_string())
}

/// Attempt `ex:doc2 ex:title "TAMPERED"` under `ctx`. True = the write landed.
async fn write_allowed(
    fluree: &support::MemoryFluree,
    ledger: support::MemoryLedger,
    ctx: &PolicyContext,
) -> bool {
    let write = json!({
        "@context": {"ex": EX},
        "insert": {"@id": "ex:doc2", "ex:title": "TAMPERED"}
    });
    let input = TrackedTransactionInput::new(TxnType::Insert, &write, TxnOpts::default(), ctx);
    fluree
        .transact_tracked_with_policy(
            ledger,
            input,
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 100_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
        .is_ok()
}

/// A narrow `f:modify` grant: "allow modify ONLY on instances of
/// `ex:Nonexistent`" under `default-allow: false` — i.e. allow nothing.
/// `on_class_key` / `on_class_value` carry the spelling under test.
fn narrow_modify_allow(on_class_key: &str, on_class_value: Value) -> GovernanceOptions {
    let mut node = serde_json::Map::new();
    node.insert("@id".into(), json!("http://example.org/narrowAllow"));
    node.insert(
        "@type".into(),
        json!(["https://ns.flur.ee/db#AccessPolicy"]),
    );
    node.insert(on_class_key.to_string(), on_class_value);
    node.insert(
        "https://ns.flur.ee/db#action".into(),
        json!([{"@id": "https://ns.flur.ee/db#modify"}]),
    );
    node.insert("https://ns.flur.ee/db#allow".into(), json!(true));

    GovernanceOptions {
        policy: Some(json!([Value::Object(node)])),
        default_allow: Some(false),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Q2-B: an aliased policy KEY dropped targeting and widened the grant.
// ---------------------------------------------------------------------------

/// `fluree:onClass` names the same IRI as `f:onClass` and the prefix is
/// declared in the request `@context`, but policy keys are matched literally.
/// The key was not recognised, `TargetMode::Default` was chosen, the rule
/// landed in the default bucket — and "allow modify only on `ex:Nonexistent`"
/// became a universal write grant.
#[tokio::test]
async fn q2b_aliased_modify_policy_key_does_not_widen_the_grant() {
    assert_index_defaults();

    // CONTROL: the canonical absolute key. A narrow allow denies the write.
    let (fluree, ledger) = seeded("probe_q2b_canon").await;
    let ctx = try_build(
        &ledger,
        &narrow_modify_allow(
            "https://ns.flur.ee/db#onClass",
            json!([{"@id": "http://example.org/Nonexistent"}]),
        ),
    )
    .await
    .expect("canonical key must build");
    assert!(
        !write_allowed(&fluree, ledger, &ctx).await,
        "CONTROL FAILED — a correctly-keyed narrow modify allow did not deny \
         the write, so the aliased-key arm below proves nothing"
    );

    // DEFECT ARM: same IRI, different prefix.
    let (_fluree, ledger) = seeded("probe_q2b_alias").await;
    let err = try_build(
        &ledger,
        &narrow_modify_allow(
            "fluree:onClass",
            json!([{"@id": "http://example.org/Nonexistent"}]),
        ),
    )
    .await
    .expect_err("an aliased policy key must fail the request");
    assert!(
        err.contains("fluree:onClass") && err.contains("f:onClass"),
        "error must name the offending key and the canonical spelling: {err}"
    );
}

// ---------------------------------------------------------------------------
// Q2-C: a prefixed target IRI made an f:modify deny inert.
// ---------------------------------------------------------------------------

fn deny_modify_on(target: &str) -> GovernanceOptions {
    GovernanceOptions {
        policy: Some(json!([{
            "@id": "http://example.org/denyModify",
            "@type": ["https://ns.flur.ee/db#AccessPolicy"],
            "https://ns.flur.ee/db#onClass": [{"@id": target}],
            "https://ns.flur.ee/db#action": [{"@id": "https://ns.flur.ee/db#modify"}],
            "https://ns.flur.ee/db#allow": false
        }])),
        default_allow: Some(true),
        ..Default::default()
    }
}

/// `ex:Confidential` encodes to a SID in the EMPTY namespace that no flake can
/// carry, so the deny targeted nothing and `default-allow: true` let the write
/// through. The absolute-IRI control shows the same deny working.
#[tokio::test]
async fn q2c_prefixed_modify_target_does_not_go_silently_inert() {
    assert_index_defaults();

    // CONTROL: absolute IRI — the deny fires and the write is refused.
    let (fluree, ledger) = seeded("probe_q2c_abs").await;
    let ctx = try_build(&ledger, &deny_modify_on("http://example.org/Confidential"))
        .await
        .expect("absolute target must build");
    assert!(
        !write_allowed(&fluree, ledger, &ctx).await,
        "CONTROL FAILED — an absolute-IRI f:modify deny did not refuse the \
         write, so the prefixed arm below proves nothing"
    );

    // DEFECT ARM: the same class as a prefixed name.
    let (_fluree, ledger) = seeded("probe_q2c_prefixed").await;
    let err = try_build(&ledger, &deny_modify_on("ex:Confidential"))
        .await
        .expect_err("a prefixed target must fail the request");
    assert!(
        err.contains("ex:Confidential") && err.contains("f:onClass"),
        "error must name the offending key and IRI: {err}"
    );
}

// ---------------------------------------------------------------------------
// Q2-D: a malformed f:onClass value dropped targeting and widened the grant.
// ---------------------------------------------------------------------------

/// Two spellings that yield no IRI — an empty list, and a `@value` literal
/// where a node reference belongs. Both left the rule untargeted, so a narrow
/// grant applied to every flake.
///
/// The pre-fix code logged *"f:onClass specified but no IRIs could be resolved
/// - policy will not match any class"* immediately before doing the opposite.
#[tokio::test]
async fn q2d_malformed_onclass_value_does_not_widen_the_grant() {
    assert_index_defaults();

    // CONTROL: a well-formed node reference denies the write.
    let (fluree, ledger) = seeded("probe_q2d_ctl").await;
    let ctx = try_build(
        &ledger,
        &narrow_modify_allow(
            "https://ns.flur.ee/db#onClass",
            json!([{"@id": "http://example.org/Nonexistent"}]),
        ),
    )
    .await
    .expect("well-formed target must build");
    assert!(
        !write_allowed(&fluree, ledger, &ctx).await,
        "CONTROL FAILED — a well-formed narrow modify allow did not deny the \
         write, so the malformed arms below prove nothing"
    );

    for (name, value) in [
        ("probe_q2d_empty", json!([])),
        (
            "probe_q2d_value",
            json!([{"@value": "http://example.org/Nonexistent"}]),
        ),
    ] {
        let (_fluree, ledger) = seeded(name).await;
        let err = try_build(
            &ledger,
            &narrow_modify_allow("https://ns.flur.ee/db#onClass", value.clone()),
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("{value} must fail the request, but it built"));
        assert!(
            err.contains("f:onClass"),
            "error must name the offending key for {value}: {err}"
        );
    }
}

// ---------------------------------------------------------------------------
// Q2-E: a STORED policy whose f:onClass is a literal widened the grant.
// ---------------------------------------------------------------------------

async fn stored_narrow_modify_allow(
    ledger_name: &str,
    on_class_value: Value,
) -> (
    support::MemoryFluree,
    support::MemoryLedger,
    GovernanceOptions,
) {
    let (fluree, ledger) = seeded(ledger_name).await;
    let stored = json!({
        "@context": {"ex": EX, "f": "https://ns.flur.ee/db#"},
        "@graph": [{
            "@id": "ex:narrowModifyAllow",
            "@type": ["f:AccessPolicy", "ex:WriterPolicy"],
            "f:onClass": on_class_value,
            "f:action": [{"@id": "f:modify"}],
            "f:allow": true
        }]
    });
    let ledger = fluree
        .insert(ledger, &stored)
        .await
        .expect("stored policy txn")
        .ledger;
    let opts = GovernanceOptions {
        policy_class: Some(vec!["http://example.org/WriterPolicy".to_string()]),
        default_allow: Some(false),
        ..Default::default()
    };
    (fluree, ledger, opts)
}

/// The stored loader only ever took a target from a `Binding` that carries a
/// Sid, so a literal object was dropped with no flag and no log — the stored
/// path has no `had_on_*` tracking at all. "Allow modify only on
/// `ex:Nonexistent`" became a universal write grant, silently on every channel.
#[tokio::test]
async fn q2e_stored_literal_onclass_does_not_widen_the_grant() {
    assert_index_defaults();

    // CONTROL: a proper node reference denies the write.
    let (fluree, ledger, opts) = stored_narrow_modify_allow(
        "probe_q2e_ctl",
        json!([{"@id": "http://example.org/Nonexistent"}]),
    )
    .await;
    let ctx = try_build(&ledger, &opts)
        .await
        .expect("well-formed stored target must build");
    assert!(
        !write_allowed(&fluree, ledger, &ctx).await,
        "CONTROL FAILED — a well-formed stored narrow modify allow did not deny \
         the write, so the literal arm below proves nothing"
    );

    // DEFECT ARM: the same IRI stored as a plain string literal.
    let (_fluree, ledger, opts) =
        stored_narrow_modify_allow("probe_q2e_lit", json!("http://example.org/Nonexistent")).await;
    let err = try_build(&ledger, &opts)
        .await
        .expect_err("a literal stored target must fail the request");
    assert!(
        err.contains("f:onClass") && err.contains("node reference"),
        "error must say the target is not a node reference: {err}"
    );
}
