//! Lifecycle conformance cases every nameservice backend must pass.
//!
//! Each case takes a fresh, empty backend. Invoke
//! [`lifecycle_conformance_tests!`](crate::lifecycle_conformance_tests) in a
//! backend's test module to run them all against it.

use crate::binding::{BindingState, DroppedState, Fence, FenceOutcome};
use crate::lifecycle::{self, LifecycleStore};
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

/// Run every case in turn, each against a fresh backend from `make`: for
/// backends too costly to set up once per test. Keep in step with
/// [`lifecycle_conformance_tests!`](crate::lifecycle_conformance_tests).
pub async fn run_all<S, F, Fut>(mut make: F)
where
    S: LifecycleStore + crate::NameServiceLookup,
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
