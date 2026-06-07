//! End-to-end test: arena builder → CAS → reader, merged with a real
//! `AttachmentNovelty` overlay.
//!
//! Exercises the full M2b read path against a populated arena and an
//! overlay holding post-arena events. The `(Sid, t, op)` shape returned
//! by `AttachmentNovelty::collect_forward_events` flows directly into
//! `AnnotationArenaReader::current_annotations_merged` with no
//! intermediate plumbing — this test pins that contract.

use fluree_db_binary_index::annotation_arena::{
    build_arenas_from_flakes, build_forward_branch, build_reverse_branch, AnnotationArenaReader,
    DEFAULT_TARGET_ROWS_PER_LEAF,
};
use fluree_db_core::storage::{ContentStore, MemoryContentStore};
use fluree_db_core::{edge::EdgeKey, AnnotationIndexRoot, ContentKind, Flake, FlakeValue, Sid};
use fluree_db_novelty::AttachmentNovelty;
use fluree_vocab::db as db_predicates;

fn ann_sid(name: &str) -> Sid {
    Sid::new(20, name)
}
fn ref_sid(name: &str) -> Sid {
    Sid::new(11, name)
}
fn id_dt() -> Sid {
    fluree_db_core::id_datatype_sid()
}
fn p(suffix: &str) -> Sid {
    Sid::new(fluree_vocab::namespaces::FLUREE_DB, suffix)
}

fn make_bundle(ann: &str, s: &str, predicate: &str, o: &str, t: i64, op: bool) -> Vec<Flake> {
    let a = ann_sid(ann);
    vec![
        Flake::new(
            a.clone(),
            p(db_predicates::REIFIES_SUBJECT),
            FlakeValue::Ref(ref_sid(s)),
            id_dt(),
            t,
            op,
            None,
        ),
        Flake::new(
            a.clone(),
            p(db_predicates::REIFIES_PREDICATE),
            FlakeValue::Ref(ref_sid(predicate)),
            id_dt(),
            t,
            op,
            None,
        ),
        Flake::new(
            a,
            p(db_predicates::REIFIES_OBJECT),
            FlakeValue::Ref(ref_sid(o)),
            id_dt(),
            t,
            op,
            None,
        ),
    ]
}

async fn build_and_store(flakes: &[Flake], store: &MemoryContentStore) -> AnnotationIndexRoot {
    let out = build_arenas_from_flakes(flakes, DEFAULT_TARGET_ROWS_PER_LEAF);

    let mut fwd_pairs = Vec::new();
    for (summary, blob) in out.forward_leaves {
        let cid = store
            .put(ContentKind::AnnotationForwardLeaf, &blob)
            .await
            .unwrap();
        fwd_pairs.push((summary, cid));
    }
    let fwd_branch_bytes = build_forward_branch(&fwd_pairs);
    let fwd_branch_cid = store
        .put(ContentKind::AnnotationForwardBranch, &fwd_branch_bytes)
        .await
        .unwrap();

    let mut rev_pairs = Vec::new();
    for (summary, blob) in out.reverse_leaves {
        let cid = store
            .put(ContentKind::AnnotationReverseLeaf, &blob)
            .await
            .unwrap();
        rev_pairs.push((summary, cid));
    }
    let rev_branch_bytes = build_reverse_branch(&rev_pairs);
    let rev_branch_cid = store
        .put(ContentKind::AnnotationReverseBranch, &rev_branch_bytes)
        .await
        .unwrap();

    AnnotationIndexRoot {
        version: 1,
        max_t: out.max_t,
        forward_branch_cid: fwd_branch_cid,
        reverse_branch_cid: rev_branch_cid,
        stats: out.stats,
    }
}

#[tokio::test]
async fn arena_assert_then_novelty_retract_resolves_correctly() {
    // Arena holds the historical attachment; novelty holds a later
    // retract. Merged read returns no live annotation; indexed-only
    // read still sees the assertion.
    let edge_flakes = make_bundle("ann_a", "alice", "worksFor", "acme", 5, true);
    let store = MemoryContentStore::new();
    let root = build_and_store(&edge_flakes, &store).await;
    let reader = AnnotationArenaReader::new(&root, &store);

    let edge = EdgeKey {
        g: None,
        s: ref_sid("alice"),
        p: ref_sid("worksFor"),
        o: FlakeValue::Ref(ref_sid("acme")),
        dt: id_dt(),
        lang: None,
        list_i: None,
    };

    // Indexed-only: ann_a still appears live.
    let indexed = reader.current_annotations_for(&edge, 100).await.unwrap();
    assert_eq!(indexed, vec![ann_sid("ann_a")]);

    // Build an overlay with the matching retract bundle.
    let mut overlay = AttachmentNovelty::new();
    let retract_bundle = edge.to_reifies_facts(&ann_sid("ann_a"), 10, false);
    overlay.observe_flakes(&retract_bundle).unwrap();

    let novelty_events = overlay.collect_forward_events(&edge);
    assert_eq!(novelty_events.len(), 1, "overlay holds one retract event");

    // Merged read: the arena assert + novelty retract resolves to no
    // live annotation.
    let merged = reader
        .current_annotations_merged(&edge, &novelty_events, 100)
        .await
        .unwrap();
    assert!(merged.is_empty(), "novelty retract overrides arena assert");

    // As-of t=8 — novelty retract not yet visible — ann_a still live.
    let merged_t8 = reader
        .current_annotations_merged(&edge, &novelty_events, 8)
        .await
        .unwrap();
    assert_eq!(merged_t8, vec![ann_sid("ann_a")]);
}

#[tokio::test]
async fn novelty_only_attachments_visible_against_empty_arena() {
    // No arena rows; the only attachment lives in novelty. Merged read
    // sees it.
    let store = MemoryContentStore::new();
    let root = build_and_store(&[], &store).await;
    let reader = AnnotationArenaReader::new(&root, &store);

    let edge = EdgeKey {
        g: None,
        s: ref_sid("alice"),
        p: ref_sid("worksFor"),
        o: FlakeValue::Ref(ref_sid("acme")),
        dt: id_dt(),
        lang: None,
        list_i: None,
    };

    let mut overlay = AttachmentNovelty::new();
    let bundle = edge.to_reifies_facts(&ann_sid("ann_new"), 10, true);
    overlay.observe_flakes(&bundle).unwrap();

    let events = overlay.collect_forward_events(&edge);
    let merged = reader
        .current_annotations_merged(&edge, &events, 100)
        .await
        .unwrap();
    assert_eq!(merged, vec![ann_sid("ann_new")]);
}

#[tokio::test]
async fn reverse_arena_plus_novelty_retarget() {
    // ann_x reifies edge_a in arena. Novelty retracts that and asserts
    // that ann_x now reifies edge_b. Merged reverse lookup returns
    // edge_b only.
    let edge_a_flakes = make_bundle("ann_x", "alice", "worksFor", "acme", 1, true);
    let store = MemoryContentStore::new();
    let root = build_and_store(&edge_a_flakes, &store).await;
    let reader = AnnotationArenaReader::new(&root, &store);

    let edge_a = EdgeKey {
        g: None,
        s: ref_sid("alice"),
        p: ref_sid("worksFor"),
        o: FlakeValue::Ref(ref_sid("acme")),
        dt: id_dt(),
        lang: None,
        list_i: None,
    };
    let edge_b = EdgeKey {
        g: None,
        s: ref_sid("bob"),
        p: ref_sid("worksFor"),
        o: FlakeValue::Ref(ref_sid("acme")),
        dt: id_dt(),
        lang: None,
        list_i: None,
    };

    let mut overlay = AttachmentNovelty::new();
    overlay
        .observe_flakes(&edge_a.to_reifies_facts(&ann_sid("ann_x"), 5, false))
        .unwrap();
    overlay
        .observe_flakes(&edge_b.to_reifies_facts(&ann_sid("ann_x"), 6, true))
        .unwrap();

    let novelty_events = overlay.collect_reverse_events(&ann_sid("ann_x"));
    assert_eq!(
        novelty_events.len(),
        2,
        "overlay surfaces both retract and re-assert events"
    );

    let merged = reader
        .current_targets_merged(&ann_sid("ann_x"), &novelty_events, 100)
        .await
        .unwrap();
    assert_eq!(merged, vec![edge_b]);
}
