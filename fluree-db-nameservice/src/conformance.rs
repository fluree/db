//! Lifecycle conformance cases every nameservice backend must pass.
//!
//! Each case takes a fresh, empty backend. Invoke
//! [`lifecycle_conformance_tests!`](crate::lifecycle_conformance_tests) in a
//! backend's test module to run them all against it.

use crate::binding::{BindingState, DroppedState, Fence, FenceOutcome};
use crate::lifecycle::{self, BranchDrop, LifecycleStore};
use crate::{NameServiceError, NsRecord};
use fluree_db_core::{LedgerId, LedgerName, StorageRoot};

fn id(s: &str) -> LedgerId {
    LedgerId::parse(s).unwrap()
}

fn name(s: &str) -> LedgerName {
    LedgerName::parse(s).unwrap()
}

fn assert_exists(err: NameServiceError) {
    assert!(
        matches!(err, NameServiceError::LedgerAlreadyExists(_)),
        "expected LedgerAlreadyExists, got {err}"
    );
}

/// A create claims the name under a fresh instance root, and a second create
/// of the name is refused.
pub async fn create_claims_the_name<S: LifecycleStore>(store: &S) {
    let record = lifecycle::create_ledger(store, &id("mydb:main"))
        .await
        .unwrap();
    let binding = store.get_binding("mydb").await.unwrap().unwrap().value;
    assert_eq!(binding.state, BindingState::Active);
    assert_eq!(binding.root_branch, "main");
    assert_eq!(
        record.storage_root.as_ref().and_then(StorageRoot::instance),
        Some(binding.instance.clone())
    );
    assert_eq!(record.fence, binding.fence_of("main"));
    assert_eq!(
        store.raw_record("mydb:main").await.unwrap().unwrap().fence,
        record.fence
    );

    assert_exists(
        lifecycle::create_ledger(store, &id("mydb:main"))
            .await
            .unwrap_err(),
    );
    assert_exists(
        lifecycle::create_ledger(store, &id("mydb:other"))
            .await
            .unwrap_err(),
    );
}

/// A soft drop frees the name at once; the next create gets a new instance,
/// root and fence, and the dropped ledger waits in the registry.
pub async fn drop_frees_the_name<S: LifecycleStore>(store: &S) {
    let first = lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let dropped = lifecycle::drop_ledger(store, &name("mydb"), false)
        .await
        .unwrap()
        .expect("dropped");
    assert!(!dropped.hard);
    assert!(store.get_binding("mydb").await.unwrap().is_none());
    assert!(store.raw_record("mydb:main").await.unwrap().is_none());

    let entry = store
        .get_dropped(&dropped.instance)
        .await
        .unwrap()
        .unwrap()
        .value;
    assert_eq!(entry.state, DroppedState::Dropped);
    assert_eq!(entry.name, "mydb");
    assert_eq!(Some(&entry.root), first.storage_root.as_ref());
    assert_eq!(entry.branches.len(), 1);
    assert!(entry.branches[0].frozen);

    let second = lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    assert_ne!(second.storage_root, first.storage_root);
    assert_ne!(second.fence, first.fence);

    assert!(lifecycle::drop_ledger(store, &name("absent"), false)
        .await
        .unwrap()
        .is_none());
}

/// Restore brings a dropped ledger back under its name and root, with a fresh
/// fence on every branch, and forgets the registry entry.
pub async fn restore_reinstates_under_fresh_fences<S: LifecycleStore>(store: &S) {
    let created = lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
        .await
        .unwrap();
    let dropped = lifecycle::drop_ledger(store, &name("mydb"), false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dropped.entry.branches.len(), 2);

    let restored = lifecycle::restore_dropped(store, &dropped.instance)
        .await
        .unwrap();
    assert_eq!(restored.len(), 2);
    let binding = store.get_binding("mydb").await.unwrap().unwrap().value;
    assert_eq!(binding.state, BindingState::Active);
    assert_eq!(binding.instance, dropped.instance);
    assert_eq!(Some(&binding.root), created.storage_root.as_ref());
    for record in &restored {
        assert!(!record.frozen);
        assert_eq!(record.fence, binding.fence_of(&record.branch));
    }
    assert_ne!(binding.fence_of("main"), created.fence);
    assert!(store
        .get_dropped(&dropped.instance)
        .await
        .unwrap()
        .is_none());
}

/// Restoring into a name another ledger now holds is refused, and the dropped
/// ledger stays restorable.
pub async fn restore_into_a_taken_name_is_refused<S: LifecycleStore>(store: &S) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let dropped = lifecycle::drop_ledger(store, &name("mydb"), false)
        .await
        .unwrap()
        .unwrap();
    let replacement = lifecycle::create_ledger(store, &id("mydb")).await.unwrap();

    assert_exists(
        lifecycle::restore_dropped(store, &dropped.instance)
            .await
            .unwrap_err(),
    );
    let entry = store
        .get_dropped(&dropped.instance)
        .await
        .unwrap()
        .unwrap()
        .value;
    assert_eq!(entry.state, DroppedState::Dropped);
    assert_eq!(
        store.raw_record("mydb:main").await.unwrap().unwrap().fence,
        replacement.fence,
        "the replacement ledger is untouched"
    );
}

/// A hard drop leaves a purging entry that restore refuses and
/// `finish_purge` removes.
pub async fn hard_drop_purges<S: LifecycleStore>(store: &S) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let dropped = lifecycle::drop_ledger(store, &name("mydb"), true)
        .await
        .unwrap()
        .unwrap();
    assert!(dropped.hard);
    assert_eq!(dropped.entry.state, DroppedState::Purging);
    assert!(lifecycle::restore_dropped(store, &dropped.instance)
        .await
        .is_err());
    lifecycle::finish_purge(store, &dropped.instance)
        .await
        .unwrap();
    assert!(store
        .get_dropped(&dropped.instance)
        .await
        .unwrap()
        .is_none());
}

/// A soft-dropped ledger can be purged later, and a purge under way blocks
/// restore.
pub async fn soft_dropped_ledger_can_be_purged<S: LifecycleStore>(store: &S) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let dropped = lifecycle::drop_ledger(store, &name("mydb"), false)
        .await
        .unwrap()
        .unwrap();
    let entry = lifecycle::begin_purge(store, &dropped.instance)
        .await
        .unwrap();
    assert_eq!(entry.state, DroppedState::Purging);
    assert!(lifecycle::restore_dropped(store, &dropped.instance)
        .await
        .is_err());
    lifecycle::begin_purge(store, &dropped.instance)
        .await
        .expect("a purge under way resumes");
    lifecycle::finish_purge(store, &dropped.instance)
        .await
        .unwrap();
}

/// Branches are listed in the binding with their own fence, and count
/// against their parent.
pub async fn create_branch_lists_and_counts<S: LifecycleStore>(store: &S) {
    let main = lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let dev = lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
        .await
        .unwrap();
    let binding = store.get_binding("mydb").await.unwrap().unwrap().value;
    assert_eq!(dev.fence, binding.fence_of("dev"));
    assert_ne!(dev.fence, main.fence);
    assert_eq!(dev.source_branch.as_deref(), Some("main"));
    assert_eq!(dev.storage_root, main.storage_root);
    assert_eq!(
        store
            .raw_record("mydb:main")
            .await
            .unwrap()
            .unwrap()
            .branches,
        1
    );

    assert_exists(
        lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
            .await
            .unwrap_err(),
    );
    assert!(matches!(
        lifecycle::create_branch(store, &name("mydb"), "x", "absent", None)
            .await
            .unwrap_err(),
        NameServiceError::NotFound(_)
    ));
}

/// A branch listed in the binding with no record is a create that did not
/// finish, and creating the branch again takes it over.
pub async fn unfinished_branch_create_is_taken_over<S: LifecycleStore>(store: &S) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let dev = lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
        .await
        .unwrap();
    // Lose the record, as a crash before the insert would have.
    store
        .delete_record("mydb:dev", dev.fence.unwrap())
        .await
        .unwrap();

    let again = lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
        .await
        .unwrap();
    assert_ne!(again.fence, dev.fence);
}

/// A record at the key with a fence no binding lists is garbage from a
/// crashed or late writer, and does not block the key.
pub async fn garbage_record_does_not_block_create<S: LifecycleStore>(store: &S) {
    let mut garbage = NsRecord::new(id("mydb:main"));
    garbage.fence = Some(Fence::generate());
    assert!(store.insert_record(&garbage).await.unwrap().is_none());

    let created = lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    assert_eq!(
        store.raw_record("mydb:main").await.unwrap().unwrap().fence,
        created.fence
    );
}

/// A record with no fence predates fencing: it is never deleted to make
/// room.
pub async fn unfenced_record_is_never_deleted<S: LifecycleStore>(store: &S) {
    let legacy = NsRecord::new(id("mydb:main"));
    assert!(store.insert_record(&legacy).await.unwrap().is_none());
    assert_exists(
        lifecycle::create_ledger(store, &id("mydb"))
            .await
            .unwrap_err(),
    );
    assert!(store.raw_record("mydb:main").await.unwrap().is_some());
}

/// A drop interrupted after it claimed the binding resumes to completion.
pub async fn interrupted_drop_resumes<S: LifecycleStore>(store: &S) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let binding = store.get_binding("mydb").await.unwrap().unwrap();
    let dropping = crate::NameBinding {
        state: BindingState::Dropping { hard: false },
        ..binding.value.clone()
    };
    store
        .cas_binding("mydb", Some(binding.version), Some(&dropping))
        .await
        .unwrap();
    assert_exists(
        lifecycle::create_ledger(store, &id("mydb"))
            .await
            .unwrap_err(),
    );

    // Resumed as the soft drop it started as, whatever the retry asks for.
    let dropped = lifecycle::drop_ledger(store, &name("mydb"), true)
        .await
        .unwrap()
        .unwrap();
    assert!(!dropped.hard);
    assert!(store.get_binding("mydb").await.unwrap().is_none());
    assert_eq!(dropped.entry.branches.len(), 1);
}

/// A pending create is invisible and can be abandoned, freeing the name.
pub async fn abandoned_create_frees_the_name<S: LifecycleStore>(store: &S) {
    let pending = lifecycle::begin_create(store, &id("mydb")).await.unwrap();
    assert_eq!(
        store
            .get_binding("mydb")
            .await
            .unwrap()
            .unwrap()
            .value
            .state,
        BindingState::Creating
    );
    assert_exists(
        lifecycle::create_ledger(store, &id("mydb"))
            .await
            .unwrap_err(),
    );
    lifecycle::abandon(store, &pending).await.unwrap();
    assert!(store.get_binding("mydb").await.unwrap().is_none());
    assert!(store.raw_record("mydb:main").await.unwrap().is_none());
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
}

/// Writes conditional on a fence refuse any other fence.
pub async fn fenced_record_writes_check_the_fence<S: LifecycleStore>(store: &S) {
    let created = lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let fence = created.fence.unwrap();
    let stale = Fence::generate();
    for outcome in [
        store.freeze_record("mydb:main", stale).await.unwrap(),
        store.delete_record("mydb:main", stale).await.unwrap(),
        store.adjust_children("mydb:main", stale, 1).await.unwrap(),
    ] {
        assert_eq!(outcome, FenceOutcome::Mismatch);
    }
    assert_eq!(
        store.freeze_record("mydb:absent", fence).await.unwrap(),
        FenceOutcome::Missing
    );
    assert_eq!(
        store.delete_record("mydb:main", fence).await.unwrap(),
        FenceOutcome::Applied
    );
}

/// A compare-and-swap from a read taken before a drop cannot match the
/// binding of the ledger that reused the name, even though the first
/// binding was deleted in between.
pub async fn stale_binding_version_never_matches_a_reused_name<S: LifecycleStore>(store: &S) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let stale = store.get_binding("mydb").await.unwrap().unwrap();
    lifecycle::drop_ledger(store, &name("mydb"), false)
        .await
        .unwrap();
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();

    let mut overwrite = stale.value.clone();
    overwrite.branches.clear();
    let outcome = store
        .cas_binding("mydb", Some(stale.version), Some(&overwrite))
        .await
        .unwrap();
    assert!(
        matches!(outcome, crate::RegistryCas::Conflict { .. }),
        "a stale version matched the new binding: {outcome:?}"
    );
}

/// Record listings see neither bindings, registry entries, nor the records a
/// drop deleted.
pub async fn listings_skip_registry_state<S: LifecycleStore + crate::NameServiceLookup>(store: &S) {
    lifecycle::create_ledger(store, &id("gone")).await.unwrap();
    lifecycle::drop_ledger(store, &name("gone"), false)
        .await
        .unwrap();
    lifecycle::create_ledger(store, &id("kept")).await.unwrap();
    lifecycle::create_branch(store, &name("kept"), "dev", "main", None)
        .await
        .unwrap();

    let mut all: Vec<String> = store
        .all_records()
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.ledger_id.to_string())
        .collect();
    all.sort();
    assert_eq!(all, vec!["kept:dev", "kept:main"]);
    assert!(store.list_branches("gone").await.unwrap().is_empty());
    assert_eq!(store.list_branches("kept").await.unwrap().len(), 2);
    assert!(store.lookup("gone:main").await.unwrap().is_none());
}

/// Dropping a leaf branch hides it at once but keeps its name taken until
/// its storage is gone; then the name is free and the parent's count is back.
pub async fn leaf_branch_drop_frees_the_branch<S: LifecycleStore + crate::NameServiceLookup>(
    store: &S,
) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let dev = lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
        .await
        .unwrap();
    assert!(matches!(
        lifecycle::begin_drop_branch(store, &name("mydb"), "main")
            .await
            .unwrap_err(),
        NameServiceError::InvalidId(_)
    ));

    let BranchDrop::Purge(record) = lifecycle::begin_drop_branch(store, &name("mydb"), "dev")
        .await
        .unwrap()
    else {
        panic!("a leaf branch is purged at once");
    };
    assert_eq!(record.fence, dev.fence);
    assert_eq!(record.storage_root, dev.storage_root);
    assert!(store.raw_record("mydb:dev").await.unwrap().unwrap().frozen);
    assert!(store.lookup("mydb:dev").await.unwrap().unwrap().retracted);
    assert_eq!(store.list_branches("mydb").await.unwrap().len(), 1);
    assert_exists(
        lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
            .await
            .unwrap_err(),
    );

    let parent = lifecycle::finish_drop_branch(store, &name("mydb"), &record)
        .await
        .unwrap();
    assert!(parent.is_none(), "main is not dropped");
    assert!(store.raw_record("mydb:dev").await.unwrap().is_none());
    assert!(store.lookup("mydb:dev").await.unwrap().is_none());
    let main = store.raw_record("mydb:main").await.unwrap().unwrap();
    assert_eq!(main.branches, 0);
    // Finishing again is a no-op, and does not count the child off twice.
    lifecycle::finish_drop_branch(store, &name("mydb"), &record)
        .await
        .unwrap();
    assert_eq!(
        store
            .raw_record("mydb:main")
            .await
            .unwrap()
            .unwrap()
            .branches,
        0
    );

    let again = lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
        .await
        .unwrap();
    assert_ne!(again.fence, dev.fence);
}

/// A branch with children is frozen but kept, since their history reaches
/// into its data; dropping its last child hands it back for purging.
pub async fn branch_with_children_drop_is_deferred<S: LifecycleStore + crate::NameServiceLookup>(
    store: &S,
) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
        .await
        .unwrap();
    lifecycle::create_branch(store, &name("mydb"), "fx", "dev", None)
        .await
        .unwrap();

    let first = lifecycle::begin_drop_branch(store, &name("mydb"), "dev")
        .await
        .unwrap();
    assert!(matches!(first, BranchDrop::Deferred { already: false }));
    let second = lifecycle::begin_drop_branch(store, &name("mydb"), "dev")
        .await
        .unwrap();
    assert!(matches!(second, BranchDrop::Deferred { already: true }));
    assert!(store.lookup("mydb:dev").await.unwrap().unwrap().retracted);
    assert!(matches!(
        lifecycle::create_branch(store, &name("mydb"), "fy", "dev", None)
            .await
            .unwrap_err(),
        NameServiceError::NotFound(_)
    ));

    let BranchDrop::Purge(fx) = lifecycle::begin_drop_branch(store, &name("mydb"), "fx")
        .await
        .unwrap()
    else {
        panic!("fx is a leaf");
    };
    let dev = lifecycle::finish_drop_branch(store, &name("mydb"), &fx)
        .await
        .unwrap()
        .expect("dev's last child is gone");
    assert_eq!(dev.branch, "dev");
    assert_eq!(dev.storage_root, fx.storage_root);
    assert!(lifecycle::finish_drop_branch(store, &name("mydb"), &dev)
        .await
        .unwrap()
        .is_none());

    let binding = store.get_binding("mydb").await.unwrap().unwrap().value;
    let listed: Vec<&str> = binding.branches.iter().map(|b| b.branch.as_str()).collect();
    assert_eq!(listed, vec!["main"]);
    assert_eq!(
        store
            .raw_record("mydb:main")
            .await
            .unwrap()
            .unwrap()
            .branches,
        0
    );
}

/// A frozen record refuses a new child, so a branch being dropped cannot
/// gain one after its drop read the count, but still counts children off.
pub async fn frozen_record_refuses_new_children<S: LifecycleStore>(store: &S) {
    let created = lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let fence = created.fence.unwrap();
    store.adjust_children("mydb:main", fence, 1).await.unwrap();
    store.freeze_record("mydb:main", fence).await.unwrap();
    assert_eq!(
        store.adjust_children("mydb:main", fence, 1).await.unwrap(),
        FenceOutcome::Frozen
    );
    assert_eq!(
        store.adjust_children("mydb:main", fence, -1).await.unwrap(),
        FenceOutcome::Applied
    );
    assert_eq!(
        store
            .raw_record("mydb:main")
            .await
            .unwrap()
            .unwrap()
            .branches,
        0
    );
}

/// A branch dropped before its ledger comes back dropped when the ledger is
/// restored, rather than live over data its drop may have deleted.
pub async fn restore_keeps_dropped_branches_dropped<
    S: LifecycleStore + crate::NameServiceLookup,
>(
    store: &S,
) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
        .await
        .unwrap();
    lifecycle::create_branch(store, &name("mydb"), "fx", "dev", None)
        .await
        .unwrap();
    lifecycle::begin_drop_branch(store, &name("mydb"), "dev")
        .await
        .unwrap();
    let dropped = lifecycle::drop_ledger(store, &name("mydb"), false)
        .await
        .unwrap()
        .unwrap();

    lifecycle::restore_dropped(store, &dropped.instance)
        .await
        .unwrap();
    let binding = store.get_binding("mydb").await.unwrap().unwrap().value;
    assert!(binding.listing("dev").unwrap().dropped);
    assert!(!binding.listing("fx").unwrap().dropped);
    assert!(store.raw_record("mydb:dev").await.unwrap().unwrap().frozen);
    assert!(store.lookup("mydb:dev").await.unwrap().unwrap().retracted);
    let fx = store.lookup("mydb:fx").await.unwrap().unwrap();
    assert!(!fx.retracted && !fx.frozen);
}

/// A record mirrored from its origin binds the name to the origin's instance
/// and root; a second branch joins the binding; a record from a newer
/// instance replaces it, and the old copies stop resolving.
pub async fn mirror_binds_the_origin_instance<S: LifecycleStore + crate::NameServiceLookup>(
    store: &S,
) {
    let origin = crate::memory::MemoryNameService::new();
    let main = lifecycle::create_ledger(&origin, &id("mydb"))
        .await
        .unwrap();
    let dev = lifecycle::create_branch(&origin, &name("mydb"), "dev", "main", None)
        .await
        .unwrap();

    let mut sent = main.clone();
    sent.fence = None;
    lifecycle::mirror_record(store, &sent).await.unwrap();
    lifecycle::mirror_record(store, &sent).await.unwrap();
    lifecycle::mirror_record(store, &dev).await.unwrap();

    let copy = store.lookup("mydb:main").await.unwrap().unwrap();
    assert_eq!(copy.storage_root, main.storage_root);
    let binding = store.get_binding("mydb").await.unwrap().unwrap().value;
    assert_eq!(Some(&binding.root), main.storage_root.as_ref());
    assert_eq!(binding.root_branch, "main");
    assert_eq!(binding.fence_of("dev"), dev.fence);
    assert_eq!(store.list_branches("mydb").await.unwrap().len(), 2);

    lifecycle::drop_ledger(&origin, &name("mydb"), true)
        .await
        .unwrap();
    let replacement = lifecycle::create_ledger(&origin, &id("mydb"))
        .await
        .unwrap();
    lifecycle::mirror_record(store, &replacement).await.unwrap();
    let copy = store.lookup("mydb:main").await.unwrap().unwrap();
    assert_eq!(copy.storage_root, replacement.storage_root);
    assert!(store.lookup("mydb:dev").await.unwrap().is_none());

    // The old ledger's drop, heard late, leaves the new copy alone.
    lifecycle::unmirror_record(store, &id("mydb"), main.instance().as_ref())
        .await
        .unwrap();
    assert!(store.lookup("mydb:main").await.unwrap().is_some());
    lifecycle::unmirror_record(store, &id("mydb"), replacement.instance().as_ref())
        .await
        .unwrap();
    assert!(store.lookup("mydb:main").await.unwrap().is_none());

    let mut legacy = NsRecord::new(id("old:main"));
    legacy.storage_root = None;
    assert!(matches!(
        lifecycle::mirror_record(store, &legacy).await.unwrap_err(),
        NameServiceError::InvalidId(_)
    ));
}

fn assert_fenced<T: std::fmt::Debug>(result: crate::Result<T>, write: &str) {
    match result {
        Err(NameServiceError::Fenced(_)) => {}
        other => panic!("{write}: expected a fence refusal, got {other:?}"),
    }
}

/// [`assert_fenced`], also taking `NotFound` when `missing`.
fn assert_refused<T: std::fmt::Debug>(result: crate::Result<T>, write: &str, missing: bool) {
    match result {
        Err(NameServiceError::NotFound(_)) if missing => {}
        other => assert_fenced(other, write),
    }
}

fn cid(kind: fluree_db_core::ContentKind, label: &str) -> fluree_db_core::ContentId {
    fluree_db_core::ContentId::new(kind, label.as_bytes())
}

/// Advance `id`'s commit head to `t` presenting `fence`, by compare-and-set
/// from its current head. (`publish_commit` is not general on every backend:
/// the replicated one publishes only through its commit queue.)
async fn advance_commit<S: crate::NameServicePublisher>(
    store: &S,
    id: &str,
    fence: Option<Fence>,
    t: i64,
) {
    let current = store.get_ref(id, crate::RefKind::CommitHead).await.unwrap();
    let next = crate::RefValue {
        id: Some(cid(
            fluree_db_core::ContentKind::Commit,
            &format!("{id}@{t}"),
        )),
        t,
    };
    assert_eq!(
        store
            .compare_and_set_ref_fenced(
                id,
                fence,
                crate::RefKind::CommitHead,
                current.as_ref(),
                &next
            )
            .await
            .unwrap(),
        crate::CasResult::Updated
    );
}

/// Try every kind of branch write against `id` presenting `fence`, and
/// require each to be refused.
async fn assert_every_write_refused<S: crate::NameServicePublisher>(
    store: &S,
    id: &str,
    fence: Option<Fence>,
) {
    assert_writes_refused(store, id, fence, false).await;
}

/// [`assert_every_write_refused`], also taking `NotFound` when `missing`:
/// a write to a record that does not exist may say so instead.
async fn assert_writes_refused<S: crate::NameServicePublisher>(
    store: &S,
    id: &str,
    fence: Option<Fence>,
    missing: bool,
) {
    use fluree_db_core::ContentKind::{Commit, IndexRoot};
    let commit = cid(Commit, "stale");
    let index = cid(IndexRoot, "stale");
    let head = |kind| async move { store.get_ref(id, kind).await.unwrap() };
    let next = |t| crate::RefValue {
        id: Some(commit.clone()),
        t,
    };
    assert_refused(
        store.publish_commit_fenced(id, fence, 99, &commit).await,
        "publish_commit",
        missing,
    );
    let current = head(crate::RefKind::CommitHead).await;
    assert_refused(
        store
            .compare_and_set_ref_fenced(
                id,
                fence,
                crate::RefKind::CommitHead,
                current.as_ref(),
                &next(99),
            )
            .await,
        "compare_and_set_ref(commit)",
        missing,
    );
    assert_refused(
        store
            .fast_forward_commit_fenced(id, fence, &next(99), 3)
            .await,
        "fast_forward_commit",
        missing,
    );
    assert_refused(
        store.publish_index_fenced(id, fence, 99, &index).await,
        "publish_index",
        missing,
    );
    assert_refused(
        store
            .publish_index_allow_equal_fenced(id, fence, 99, &index)
            .await,
        "publish_index_allow_equal",
        missing,
    );
    let current = head(crate::RefKind::IndexHead).await;
    assert_refused(
        store
            .compare_and_set_ref_fenced(
                id,
                fence,
                crate::RefKind::IndexHead,
                current.as_ref(),
                &crate::RefValue {
                    id: Some(index.clone()),
                    t: 99,
                },
            )
            .await,
        "compare_and_set_ref(index)",
        missing,
    );
    assert_refused(
        store
            .reset_head_fenced(
                id,
                fence,
                crate::NsRecordSnapshot {
                    commit_head_id: None,
                    commit_t: 0,
                    index_head_id: None,
                    index_t: 0,
                },
            )
            .await,
        "reset_head",
        missing,
    );
    let status = store.get_status(id).await.unwrap();
    let next_status = crate::StatusValue::new(
        status.as_ref().map_or(1, |s| s.v + 1),
        crate::StatusPayload::new("ready"),
    );
    assert_refused(
        store
            .push_status_fenced(id, fence, status.as_ref(), &next_status)
            .await,
        "push_status",
        missing,
    );
    let config = store.get_config(id).await.unwrap();
    let next_config = crate::ConfigValue::new(config.as_ref().map_or(1, |c| c.v + 1), None);
    assert_refused(
        store
            .push_config_fenced(id, fence, config.as_ref(), &next_config)
            .await,
        "push_config",
        missing,
    );
}

/// Every write to a branch record presents the fence its writer loaded: a
/// stale fence, or none, is refused; the branch's own fence is admitted.
pub async fn writes_present_the_branch_fence<S: crate::NameServicePublisher>(store: &S) {
    use fluree_db_core::ContentKind::{Commit, IndexRoot};
    let created = lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let fence = created.fence;
    assert_every_write_refused(store, "mydb:main", Some(Fence::generate())).await;
    assert_every_write_refused(store, "mydb:main", None).await;

    advance_commit(store, "mydb:main", fence, 1).await;
    let head = store
        .get_ref("mydb:main", crate::RefKind::CommitHead)
        .await
        .unwrap();
    assert_eq!(head.as_ref().map(|h| h.t), Some(1));
    let c2 = crate::RefValue {
        id: Some(cid(Commit, "c2")),
        t: 2,
    };
    assert_eq!(
        store
            .compare_and_set_ref_fenced(
                "mydb:main",
                fence,
                crate::RefKind::CommitHead,
                head.as_ref(),
                &c2
            )
            .await
            .unwrap(),
        crate::CasResult::Updated
    );
    store
        .publish_index_fenced("mydb:main", fence, 2, &cid(IndexRoot, "i2"))
        .await
        .unwrap();
    store
        .publish_index_allow_equal_fenced("mydb:main", fence, 2, &cid(IndexRoot, "i2b"))
        .await
        .unwrap();
    let record = store.lookup("mydb:main").await.unwrap().unwrap();
    assert_eq!((record.commit_t, record.index_t), (2, 2));
    assert_eq!(record.index_head_id, Some(cid(IndexRoot, "i2b")));

    let config = store.get_config("mydb:main").await.unwrap();
    let next = crate::ConfigValue::new(config.as_ref().map_or(1, |c| c.v + 1), None);
    assert_eq!(
        store
            .push_config_fenced("mydb:main", fence, config.as_ref(), &next)
            .await
            .unwrap(),
        crate::ConfigCasResult::Updated
    );
    store
        .reset_head_fenced(
            "mydb:main",
            fence,
            crate::NsRecordSnapshot::from_record(&record),
        )
        .await
        .unwrap();
}

/// A frozen branch takes no write, even under its own fence.
pub async fn frozen_branch_refuses_writes<S: crate::NameServicePublisher>(store: &S) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let dev = lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
        .await
        .unwrap();
    lifecycle::create_branch(store, &name("mydb"), "fx", "dev", None)
        .await
        .unwrap();
    lifecycle::begin_drop_branch(store, &name("mydb"), "dev")
        .await
        .unwrap();
    assert_every_write_refused(store, "mydb:dev", dev.fence).await;
}

/// A writer that loaded a ledger before it was dropped cannot publish into
/// it, nor into the ledger that reuses its name, nor into it once restored.
pub async fn stale_writers_are_refused_across_drop_and_restore<S: crate::NameServicePublisher>(
    store: &S,
) {
    use fluree_db_core::ContentKind::Commit;
    let old = lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let dropped = lifecycle::drop_ledger(store, &name("mydb"), false)
        .await
        .unwrap()
        .unwrap();
    assert_fenced(
        store
            .publish_commit_fenced("mydb:main", old.fence, 5, &cid(Commit, "late"))
            .await,
        "publish after drop",
    );
    assert!(
        store.raw_record("mydb:main").await.unwrap().is_none(),
        "a refused publish must not recreate the record"
    );

    let replacement = lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    assert_every_write_refused(store, "mydb:main", old.fence).await;
    let record = store.lookup("mydb:main").await.unwrap().unwrap();
    assert_eq!(record.commit_t, 0, "the stale writer left no trace");

    lifecycle::drop_ledger(store, &name("mydb"), true)
        .await
        .unwrap();
    lifecycle::restore_dropped(store, &dropped.instance)
        .await
        .unwrap();
    assert_every_write_refused(store, "mydb:main", old.fence).await;
    assert_every_write_refused(store, "mydb:main", replacement.fence).await;
    let restored = store.get_binding("mydb").await.unwrap().unwrap().value;
    advance_commit(store, "mydb:main", restored.fence_of("main"), 1).await;
}

/// A record no binding lists — an unfenced one, as a binary from before
/// name bindings left it — reads as absent and takes no write.
pub async fn unbound_record_is_invisible_and_takes_no_writes<S: crate::NameServicePublisher>(
    store: &S,
) {
    assert!(store
        .insert_record(&NsRecord::new(id("old:main")))
        .await
        .unwrap()
        .is_none());
    assert!(store.lookup("old:main").await.unwrap().is_none());
    assert!(store.all_records().await.unwrap().is_empty());
    assert_every_write_refused(store, "old:main", None).await;
    assert_every_write_refused(store, "old:main", Some(Fence::generate())).await;
}

/// A write to a record that does not exist is refused, whatever it
/// presents, and leaves no record behind: publication never creates one.
pub async fn publication_never_creates_a_record<S: crate::NameServicePublisher>(store: &S) {
    assert_writes_refused(store, "nobody:main", None, true).await;
    assert_writes_refused(store, "nobody:main", Some(Fence::generate()), true).await;
    assert!(store.raw_record("nobody:main").await.unwrap().is_none());
    assert!(store.lookup("nobody:main").await.unwrap().is_none());
}

/// A branch record as a binary from before name bindings left it: unfenced.
fn legacy_record(ledger_id: &str, source: Option<&str>, retracted: bool) -> NsRecord {
    NsRecord {
        source_branch: source.map(str::to_string),
        retracted,
        ..NsRecord::new(id(ledger_id))
    }
}

/// The migration binds a ledger from before name bindings to its name,
/// rooted where its data already is, and fences its records: from then on
/// only a writer holding the fence can publish. A retracted branch stays
/// listed as dropped. Running it again changes nothing.
pub async fn migration_binds_legacy_ledgers<S: crate::NameServicePublisher>(store: &S) {
    let main = NsRecord {
        commit_head_id: Some(cid(fluree_db_core::ContentKind::Commit, "legacy")),
        commit_t: 1,
        ..legacy_record("mydb:main", None, false)
    };
    for record in [
        main,
        legacy_record("mydb:dev", Some("main"), false),
        legacy_record("mydb:old", Some("main"), true),
    ] {
        assert!(store.insert_record(&record).await.unwrap().is_none());
    }

    let report = lifecycle::migrate_legacy(store).await.unwrap();
    assert_eq!(report.bound, vec!["mydb".to_string()]);
    assert!(report.dropped.is_empty());

    let binding = store.get_binding("mydb").await.unwrap().unwrap().value;
    assert_eq!(binding.state, BindingState::Active);
    assert_eq!(binding.instance, lifecycle::legacy_instance("mydb"));
    assert_eq!(binding.root, StorageRoot::legacy(&name("mydb")));
    assert_eq!(binding.root_branch, "main");
    assert_eq!(binding.branches.len(), 3);
    assert!(binding.listing("old").unwrap().dropped);

    let main = store.lookup("mydb:main").await.unwrap().expect("main");
    assert_eq!(main.fence, binding.fence_of("main"));
    assert!(main.fence.is_some());
    assert_eq!(main.commit_t, 1);
    assert_eq!(
        store.lookup("mydb:dev").await.unwrap().expect("dev").fence,
        binding.fence_of("dev")
    );
    assert!(store
        .lookup("mydb:old")
        .await
        .unwrap()
        .is_none_or(|r| r.retracted));

    assert_every_write_refused(store, "mydb:main", None).await;
    advance_commit(store, "mydb:main", main.fence, 2).await;

    assert!(lifecycle::migrate_legacy(store).await.unwrap().is_empty());
    assert_eq!(
        store.get_binding("mydb").await.unwrap().unwrap().value,
        binding
    );
}

/// A ledger whose branches were all retracted was soft-dropped: the
/// migration moves it to the registry, frees its name, and it restores to
/// its old root.
pub async fn migration_registers_soft_dropped_ledgers<S: LifecycleStore>(store: &S) {
    for record in [
        legacy_record("gone:main", None, true),
        legacy_record("gone:dev", Some("main"), true),
    ] {
        assert!(store.insert_record(&record).await.unwrap().is_none());
    }

    let report = lifecycle::migrate_legacy(store).await.unwrap();
    assert_eq!(report.dropped, vec!["gone".to_string()]);
    assert!(report.bound.is_empty());
    assert!(store.get_binding("gone").await.unwrap().is_none());
    assert!(store.raw_record("gone:main").await.unwrap().is_none());
    assert!(store.raw_record("gone:dev").await.unwrap().is_none());

    let instance = lifecycle::legacy_instance("gone");
    let entry = store.get_dropped(&instance).await.unwrap().unwrap().value;
    assert_eq!(entry.state, DroppedState::Dropped);
    assert_eq!(entry.name, "gone");
    assert_eq!(entry.root, StorageRoot::legacy(&name("gone")));
    assert_eq!(entry.branches.len(), 2);
    assert!(lifecycle::migrate_legacy(store).await.unwrap().is_empty());

    let restored = lifecycle::restore_dropped(store, &instance).await.unwrap();
    assert_eq!(restored.len(), 2);
    let binding = store.get_binding("gone").await.unwrap().unwrap().value;
    assert_eq!(binding.root, StorageRoot::legacy(&name("gone")));
    assert!(binding.branches.iter().all(|b| !b.dropped));
}

/// A migration interrupted after fencing some of a ledger's records, before
/// binding it, lists them all when it resumes.
pub async fn interrupted_migration_resumes<S: crate::NameServicePublisher>(store: &S) {
    for record in [
        legacy_record("mydb:main", None, false),
        legacy_record("mydb:dev", Some("main"), false),
    ] {
        assert!(store.insert_record(&record).await.unwrap().is_none());
    }
    lifecycle::migrate_legacy(store).await.unwrap();
    let fence = store.raw_record("mydb:dev").await.unwrap().unwrap().fence;
    // Roll back to the crash: the binding not yet written.
    let binding = store.get_binding("mydb").await.unwrap().unwrap();
    store
        .cas_binding("mydb", Some(binding.version), None)
        .await
        .unwrap();
    assert!(store
        .insert_record(&legacy_record("mydb:feature", Some("main"), false))
        .await
        .unwrap()
        .is_none());

    let report = lifecycle::migrate_legacy(store).await.unwrap();
    assert_eq!(report.bound, vec!["mydb".to_string()]);
    let binding = store.get_binding("mydb").await.unwrap().unwrap().value;
    assert_eq!(binding.branches.len(), 3);
    assert_eq!(binding.fence_of("dev"), fence);
    assert!(store.lookup("mydb:feature").await.unwrap().is_some());
}

/// Run every case in turn, each against a fresh backend from `make`: for
/// backends too costly to set up once per test. Keep in step with
/// [`lifecycle_conformance_tests!`](crate::lifecycle_conformance_tests).
/// A retracted graph source's index head can be cleared, so one created
/// again under its name publishes from `t` 0; a live one's is left alone.
pub async fn retracted_graph_source_index_resets<S: crate::NameServicePublisher>(store: &S) {
    use crate::{GraphSourceLookup, GraphSourceType};
    use fluree_db_core::{ContentId, ContentKind};

    let index = |n: &str| ContentId::new(ContentKind::IndexRoot, n.as_bytes());
    let deps = ["docs:main".to_string()];
    async fn head<S: GraphSourceLookup>(store: &S) -> (bool, Option<ContentId>, i64) {
        let record = store
            .lookup_graph_source("search:main")
            .await
            .unwrap()
            .expect("graph source");
        (record.retracted, record.index_id, record.index_t)
    }
    store
        .publish_graph_source("search", "main", GraphSourceType::Bm25, "{}", &deps)
        .await
        .unwrap();
    store
        .publish_graph_source_index("search", "main", &index("a"), 5)
        .await
        .unwrap();
    store
        .reset_graph_source_index("search", "main")
        .await
        .unwrap();
    assert_eq!(head(store).await, (false, Some(index("a")), 5), "live");

    store.retract_graph_source("search", "main").await.unwrap();
    store
        .reset_graph_source_index("search", "main")
        .await
        .unwrap();
    store
        .publish_graph_source("search", "main", GraphSourceType::Bm25, "{}", &deps)
        .await
        .unwrap();
    store
        .publish_graph_source_index("search", "main", &index("b"), 1)
        .await
        .unwrap();
    assert_eq!(head(store).await, (false, Some(index("b")), 1), "recreated");

    store
        .reset_graph_source_index("nothing", "main")
        .await
        .unwrap();
}

/// The resume-only entry points finish an operation already under way and
/// nothing else: never a drop, restore or branch drop of their own.
pub async fn resuming_starts_nothing<S: LifecycleStore>(store: &S) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let dev = lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
        .await
        .unwrap();

    assert!(lifecycle::resume_drop_ledger(store, &name("mydb"))
        .await
        .unwrap()
        .is_none());
    let dev_fence = dev.fence.expect("fenced");
    for fence in [dev_fence, Fence::generate()] {
        assert!(
            lifecycle::resume_drop_branch(store, &name("mydb"), "dev", fence)
                .await
                .unwrap()
                .is_none(),
            "dev is not dropping"
        );
    }
    let binding = store.get_binding("mydb").await.unwrap().unwrap().value;
    assert_eq!(binding.state, BindingState::Active);
    assert!(!binding.listing("dev").unwrap().dropped);

    // A branch dropping under another fence is not the drop being resumed.
    lifecycle::begin_drop_branch(store, &name("mydb"), "dev")
        .await
        .unwrap();
    assert!(
        lifecycle::resume_drop_branch(store, &name("mydb"), "dev", Fence::generate())
            .await
            .unwrap()
            .is_none()
    );

    let dropped = lifecycle::drop_ledger(store, &name("mydb"), false)
        .await
        .unwrap()
        .unwrap();
    assert!(lifecycle::resume_restore(store, &dropped.instance)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .get_dropped(&dropped.instance)
            .await
            .unwrap()
            .unwrap()
            .value
            .state,
        DroppedState::Dropped
    );
    assert!(store.get_binding("mydb").await.unwrap().is_none());
}

/// A create that stops is rolled back only at the version it was seen at: a
/// renewed claim is left alone, and a rolled-back creator can neither renew
/// nor activate. The name is free again afterwards.
pub async fn stopped_create_is_rolled_back_unless_renewed<S: LifecycleStore>(store: &S) {
    let mut pending = lifecycle::begin_create(store, &id("mydb")).await.unwrap();
    let seen = store.get_binding("mydb").await.unwrap().unwrap().version;

    assert!(lifecycle::renew_claim(store, &mut pending).await.unwrap());
    assert!(
        lifecycle::rollback_create(store, "mydb", seen)
            .await
            .unwrap()
            .is_none(),
        "renewed since it was seen"
    );
    assert!(store.raw_record("mydb:main").await.unwrap().is_some());

    let seen = store.get_binding("mydb").await.unwrap().unwrap().version;
    let rolled_back = lifecycle::rollback_create(store, "mydb", seen)
        .await
        .unwrap()
        .expect("rolled back");
    assert_eq!(rolled_back.instance, pending.instance);
    assert!(store.get_binding("mydb").await.unwrap().is_none());
    assert!(store.raw_record("mydb:main").await.unwrap().is_none());

    assert!(!lifecycle::renew_claim(store, &mut pending).await.unwrap());
    assert!(lifecycle::activate(store, &pending).await.is_err());
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();

    // An active ledger is not a create to roll back.
    let active = store.get_binding("mydb").await.unwrap().unwrap().version;
    assert!(lifecycle::rollback_create(store, "mydb", active)
        .await
        .unwrap()
        .is_none());
    assert!(store.get_binding("mydb").await.unwrap().is_some());
}

/// A creator rolled back while paused, resumed after another ledger took the
/// name, finds that ledger's record at its key and deletes nothing.
pub async fn a_rolled_back_creator_deletes_nothing<S: LifecycleStore + crate::NameServiceLookup>(
    store: &S,
) {
    let stale = lifecycle::begin_create(store, &id("mydb")).await.unwrap();
    let seen = store.get_binding("mydb").await.unwrap().unwrap().version;
    lifecycle::rollback_create(store, "mydb", seen)
        .await
        .unwrap()
        .expect("rolled back");
    let live = lifecycle::create_ledger(store, &id("mydb")).await.unwrap();

    let resumed = NsRecord {
        storage_root: None,
        ..stale.record.clone()
    };
    assert!(matches!(
        lifecycle::insert_fenced(store, &resumed).await.unwrap_err(),
        NameServiceError::Conflict(_)
    ));
    let record = store
        .lookup("mydb:main")
        .await
        .unwrap()
        .expect("still live");
    assert_eq!(record.fence, live.fence);
}

/// A restore that made its ledger visible and stopped before forgetting its
/// entry leaves that entry restoring. Dropping the ledger replaces the entry
/// rather than adopt it, so the stale restore cannot be resumed.
pub async fn drop_replaces_a_finished_restores_entry<S: LifecycleStore>(store: &S) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let dropped = lifecycle::drop_ledger(store, &name("mydb"), false)
        .await
        .unwrap()
        .unwrap();
    let stale = store
        .get_dropped(&dropped.instance)
        .await
        .unwrap()
        .unwrap()
        .value;
    lifecycle::restore_dropped(store, &dropped.instance)
        .await
        .unwrap();
    let fences = stale
        .branches
        .iter()
        .map(|r| crate::BranchFence::new(&r.branch, Fence::generate()))
        .collect();
    let leftover = crate::DroppedLedger {
        state: DroppedState::Restoring { fences },
        ..stale
    };
    store
        .cas_dropped(&dropped.instance, None, Some(&leftover))
        .await
        .unwrap();

    lifecycle::drop_ledger(store, &name("mydb"), false)
        .await
        .unwrap()
        .expect("dropped");
    let entry = store
        .get_dropped(&dropped.instance)
        .await
        .unwrap()
        .unwrap()
        .value;
    assert_eq!(entry.state, DroppedState::Dropped);
    assert!(lifecycle::resume_restore(store, &dropped.instance)
        .await
        .unwrap()
        .is_none());
    assert!(
        store.get_binding("mydb").await.unwrap().is_none(),
        "stays dropped"
    );
}

/// A create under way cannot be dropped: it has nothing a drop could keep.
/// Rolling it back is what frees the name.
pub async fn a_create_under_way_is_not_dropped<S: LifecycleStore>(store: &S) {
    lifecycle::begin_create(store, &id("mydb")).await.unwrap();
    assert!(matches!(
        lifecycle::drop_ledger(store, &name("mydb"), true)
            .await
            .unwrap_err(),
        NameServiceError::Conflict(_)
    ));
    assert_eq!(
        store
            .get_binding("mydb")
            .await
            .unwrap()
            .unwrap()
            .value
            .state,
        BindingState::Creating
    );
}

/// A dropped branch has no heads, status or config to read.
pub async fn a_dropped_branch_reads_as_absent<S: crate::NameServicePublisher>(store: &S) {
    use crate::RefKind;
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    lifecycle::drop_ledger(store, &name("mydb"), false)
        .await
        .unwrap();
    assert!(store.heads("mydb:main").await.unwrap().is_none());
    for kind in [RefKind::CommitHead, RefKind::IndexHead] {
        assert!(store.get_ref("mydb:main", kind).await.unwrap().is_none());
    }
    assert!(store.get_status("mydb:main").await.unwrap().is_none());
    assert!(store.get_config("mydb:main").await.unwrap().is_none());
}

/// Fields a later version wrote into a binding survive this version's
/// rewrites of it.
pub async fn unknown_binding_fields_survive_rewrites<S: LifecycleStore>(store: &S) {
    lifecycle::create_ledger(store, &id("mydb")).await.unwrap();
    let current = store.get_binding("mydb").await.unwrap().unwrap();
    let mut later = current.value.clone();
    later
        .extra
        .insert("future".to_string(), serde_json::json!({"x": 1}));
    later.branches[0]
        .extra
        .insert("future".to_string(), serde_json::json!(2));
    store
        .cas_binding("mydb", Some(current.version), Some(&later))
        .await
        .unwrap();

    lifecycle::create_branch(store, &name("mydb"), "dev", "main", None)
        .await
        .unwrap();
    let binding = store.get_binding("mydb").await.unwrap().unwrap().value;
    assert_eq!(
        binding.extra.get("future"),
        Some(&serde_json::json!({"x": 1}))
    );
    assert_eq!(
        binding.listing("main").unwrap().extra.get("future"),
        Some(&serde_json::json!(2))
    );
}

pub async fn run_all<S, F, Fut>(mut make: F)
where
    S: crate::NameServicePublisher,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = S>,
{
    create_claims_the_name(&make().await).await;
    drop_frees_the_name(&make().await).await;
    restore_reinstates_under_fresh_fences(&make().await).await;
    restore_into_a_taken_name_is_refused(&make().await).await;
    hard_drop_purges(&make().await).await;
    soft_dropped_ledger_can_be_purged(&make().await).await;
    create_branch_lists_and_counts(&make().await).await;
    unfinished_branch_create_is_taken_over(&make().await).await;
    garbage_record_does_not_block_create(&make().await).await;
    unfenced_record_is_never_deleted(&make().await).await;
    interrupted_drop_resumes(&make().await).await;
    abandoned_create_frees_the_name(&make().await).await;
    fenced_record_writes_check_the_fence(&make().await).await;
    stale_binding_version_never_matches_a_reused_name(&make().await).await;
    listings_skip_registry_state(&make().await).await;
    leaf_branch_drop_frees_the_branch(&make().await).await;
    branch_with_children_drop_is_deferred(&make().await).await;
    frozen_record_refuses_new_children(&make().await).await;
    restore_keeps_dropped_branches_dropped(&make().await).await;
    mirror_binds_the_origin_instance(&make().await).await;
    writes_present_the_branch_fence(&make().await).await;
    frozen_branch_refuses_writes(&make().await).await;
    stale_writers_are_refused_across_drop_and_restore(&make().await).await;
    unbound_record_is_invisible_and_takes_no_writes(&make().await).await;
    publication_never_creates_a_record(&make().await).await;
    migration_binds_legacy_ledgers(&make().await).await;
    migration_registers_soft_dropped_ledgers(&make().await).await;
    interrupted_migration_resumes(&make().await).await;
    retracted_graph_source_index_resets(&make().await).await;
    resuming_starts_nothing(&make().await).await;
    stopped_create_is_rolled_back_unless_renewed(&make().await).await;
    a_rolled_back_creator_deletes_nothing(&make().await).await;
    drop_replaces_a_finished_restores_entry(&make().await).await;
    a_create_under_way_is_not_dropped(&make().await).await;
    a_dropped_branch_reads_as_absent(&make().await).await;
    unknown_binding_fields_survive_rewrites(&make().await).await;
}

/// Expand to one `#[tokio::test]` per conformance case, each against a fresh
/// backend from `$make`: an async expression evaluating to `(backend, guard)`,
/// where `guard` keeps whatever the backend needs (a temporary directory)
/// alive for the test.
#[macro_export]
macro_rules! lifecycle_conformance_tests {
    ($make:expr) => {
        $crate::lifecycle_conformance_tests!(@cases $make;
            create_claims_the_name,
            drop_frees_the_name,
            restore_reinstates_under_fresh_fences,
            restore_into_a_taken_name_is_refused,
            hard_drop_purges,
            soft_dropped_ledger_can_be_purged,
            create_branch_lists_and_counts,
            unfinished_branch_create_is_taken_over,
            garbage_record_does_not_block_create,
            unfenced_record_is_never_deleted,
            interrupted_drop_resumes,
            abandoned_create_frees_the_name,
            fenced_record_writes_check_the_fence,
            stale_binding_version_never_matches_a_reused_name,
            listings_skip_registry_state,
            leaf_branch_drop_frees_the_branch,
            branch_with_children_drop_is_deferred,
            frozen_record_refuses_new_children,
            restore_keeps_dropped_branches_dropped,
            mirror_binds_the_origin_instance,
            writes_present_the_branch_fence,
            frozen_branch_refuses_writes,
            stale_writers_are_refused_across_drop_and_restore,
            unbound_record_is_invisible_and_takes_no_writes,
            publication_never_creates_a_record,
            migration_binds_legacy_ledgers,
            migration_registers_soft_dropped_ledgers,
            interrupted_migration_resumes,
            retracted_graph_source_index_resets,
            resuming_starts_nothing,
            stopped_create_is_rolled_back_unless_renewed,
            a_rolled_back_creator_deletes_nothing,
            drop_replaces_a_finished_restores_entry,
            a_create_under_way_is_not_dropped,
            a_dropped_branch_reads_as_absent,
            unknown_binding_fields_survive_rewrites,
        );
    };
    (@cases $make:expr; $($case:ident),* $(,)?) => {
        $(
            #[tokio::test]
            async fn $case() {
                let (store, _guard) = $make;
                $crate::conformance::$case(&store).await;
            }
        )*
    };
}
