//! A commit blob may not smuggle commit provenance (issue #1846).
//!
//! Genuine commit records are *derived* from the commit envelope on every load
//! (`generate_commit_flakes`, with `g: None`) and only then routed to the
//! txn-meta graph (`stamp_graph_on_commit_flakes`). User-supplied transaction
//! metadata rides the envelope's separate `txn_meta` field. Nothing legitimate
//! puts a txn-meta-graph flake into the blob's flake stream, so one that
//! arrives there claiming commit identity is forged by construction.
//!
//! It matters because this replay routes purely by graph Sid, with none of the
//! index builder's graph filtering — which is what makes a forged record *live*
//! on a replica that has not indexed yet, exactly where `resolve_commit_prefix`
//! reads.
//!
//! The assertions name the forged subject directly rather than counting, so a
//! change in how much provenance a commit generates cannot make them vacuous.

use fluree_db_core::{
    config_graph_iri, txn_meta_graph_iri, Commit, ContentId, ContentKind, Flake, FlakeValue,
    IndexType, LedgerSnapshot, Sid, CONFIG_GRAPH_ID, TXN_META_GRAPH_ID,
};
use fluree_db_ledger::LedgerState;
use fluree_db_novelty::Novelty;
use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_DB, XSD};

const LEDGER: &str = "forged:main";
/// A plausible commit-record subject: 64 hex characters in the commit namespace.
const FORGED_HEX: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

fn string_flake(g: Option<Sid>, s: Sid, p: Sid, v: &str) -> Flake {
    let dt = Sid::new(XSD, "string");
    match g {
        Some(g) => Flake::new_in_graph(g, s, p, FlakeValue::String(v.into()), dt, 1, true, None),
        None => Flake::new(s, p, FlakeValue::String(v.into()), dt, 1, true, None),
    }
}

/// True if any flake in the overlay has this subject, in any graph.
fn has_subject(state: &LedgerState, s: &Sid) -> bool {
    state
        .novelty
        .iter_flakes(IndexType::Spot)
        .any(|f| &f.s == s)
}

fn commit_carrying(flakes: Vec<Flake>) -> Commit {
    Commit {
        // A real id, so the load path also generates genuine provenance —
        // the forgery has to be distinguished from it, not from an empty graph.
        id: Some(ContentId::new(ContentKind::Commit, b"forged-test-commit")),
        t: 1,
        time: None,
        flakes,
        parents: Vec::new(),
        txn: None,
        namespace_delta: std::collections::HashMap::new(),
        txn_signature: None,
        commit_signatures: Vec::new(),
        txn_meta: Vec::new(),
        graph_delta: std::collections::HashMap::new(),
        ns_split_mode: None,
    }
}

fn fresh_state() -> (LedgerState, Sid, Sid) {
    let snapshot = LedgerSnapshot::genesis(LEDGER);
    let txn_meta_sid = snapshot
        .encode_iri(&txn_meta_graph_iri(LEDGER))
        .expect("txn-meta graph IRI must encode");
    let config_sid = snapshot
        .encode_iri(&config_graph_iri(LEDGER))
        .expect("config graph IRI must encode");
    (
        LedgerState::new(snapshot, Novelty::new(0)),
        txn_meta_sid,
        config_sid,
    )
}

/// The forgery is dropped, and the rest of the commit is untouched.
#[test]
fn a_commit_blob_cannot_smuggle_commit_provenance_into_the_txn_meta_graph() {
    let (mut state, txn_meta_sid, _) = fresh_state();

    let forged = string_flake(
        Some(txn_meta_sid),
        Sid::new(FLUREE_COMMIT, FORGED_HEX),
        Sid::new(FLUREE_DB, "address"),
        "bafyforged",
    );
    let ordinary = string_flake(
        None,
        Sid::new(0, "http://example.org/alice"),
        Sid::new(0, "http://example.org/name"),
        "Alice",
    );

    state
        .apply_single_commit(commit_carrying(vec![forged, ordinary]), LEDGER)
        .expect("apply");

    assert!(
        !has_subject(&state, &Sid::new(FLUREE_COMMIT, FORGED_HEX)),
        "a blob-carried commit record must not reach the overlay at all — the \
         txn-meta graph is what `resolve_commit_prefix` scans on an unindexed \
         replica, with no index filter in the way"
    );
    assert!(
        has_subject(&state, &Sid::new(0, "http://example.org/alice")),
        "the commit's ordinary data must still be applied — the drop must be \
         surgical, not a rejection of the whole commit"
    );
    assert!(
        state.novelty.segment_count(TXN_META_GRAPH_ID) > 0,
        "genuine provenance, derived from the envelope, must still populate \
         txn-meta — otherwise this test would pass by breaking the feature"
    );
}

/// The config graph is the deliberate opposite case and must be untouched.
///
/// `stage()` permits ordinary transactions to write ledger configuration, so
/// config flakes legitimately ride the blob body and novelty tracks a config
/// watermark off exactly that. A drop generalised from txn-meta to "reserved
/// graphs" would silently break ledger configuration; this test is what fails
/// if someone makes that generalisation.
#[test]
fn config_graph_flakes_in_a_commit_blob_are_left_alone() {
    let (mut state, _, config_sid) = fresh_state();

    let config = string_flake(
        Some(config_sid),
        Sid::new(0, "urn:config:main"),
        Sid::new(FLUREE_DB, "indexing"),
        "on",
    );

    state
        .apply_single_commit(commit_carrying(vec![config]), LEDGER)
        .expect("apply");

    assert!(
        has_subject(&state, &Sid::new(0, "urn:config:main")),
        "config writes ride the commit blob legitimately and must survive"
    );
    assert!(
        state.novelty.segment_count(CONFIG_GRAPH_ID) > 0,
        "and must still route to the config graph, which the config watermark \
         and `resolve_config_sid`'s cheap guard both read"
    );
}

/// The drop keys off the *conjunction*, not either half on its own.
#[test]
fn only_the_conjunction_is_dropped_not_either_half_alone() {
    // Half one: a commit-namespace subject in an ordinary graph is just data.
    {
        let (mut state, _, _) = fresh_state();
        let in_default = string_flake(
            None,
            Sid::new(FLUREE_COMMIT, FORGED_HEX),
            Sid::new(FLUREE_DB, "address"),
            "not-a-forgery",
        );
        state
            .apply_single_commit(commit_carrying(vec![in_default]), LEDGER)
            .expect("apply");
        // `g: None` + FLUREE_COMMIT is the *generated* shape, so it is stamped
        // into txn-meta rather than dropped — that is the stamp's whole job.
        assert!(
            has_subject(&state, &Sid::new(FLUREE_COMMIT, FORGED_HEX)),
            "an unrouted commit-namespace flake is stamped, not dropped"
        );
    }

    // Half two: a non-commit subject already routed to txn-meta is not a
    // commit record and cannot steer the commit resolvers, so it is kept.
    {
        let (mut state, txn_meta_sid, _) = fresh_state();
        let non_commit = string_flake(
            Some(txn_meta_sid),
            Sid::new(0, "http://example.org/note"),
            Sid::new(0, "http://example.org/text"),
            "hello",
        );
        state
            .apply_single_commit(commit_carrying(vec![non_commit]), LEDGER)
            .expect("apply");
        assert!(
            has_subject(&state, &Sid::new(0, "http://example.org/note")),
            "a non-commit subject in txn-meta is not a provenance forgery"
        );
    }
}
