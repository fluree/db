//! A write staged against a snapshot the ledger has since moved past is
//! re-based over the commits in between when none of them touched its
//! subjects, and staged again under the lock when one did.
//!
//! Under steady contention the ledger has always moved by the time a stage
//! reaches the lock, so without the re-base every write staged twice and
//! the lock hold was the whole request. With it, the hold is the commit.

#![cfg(all(feature = "native", unix))]

use fluree_db_api::{Fluree, FlureeBuilder, GraphDb, LedgerHandle};
use fluree_db_core::IndexType;

const LEDGER: &str = "it/optimistic-rebase:main";
const PREFIX: &str = "PREFIX ex: <http://example.org/> ";

async fn open() -> (tempfile::TempDir, Fluree, LedgerHandle) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("build");
    fluree.create_ledger(LEDGER).await.expect("create");
    let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
    (dir, fluree, handle)
}

async fn update(fluree: &Fluree, handle: &LedgerHandle, sparql: &str) -> i64 {
    let sparql = format!("{PREFIX}{sparql}");
    fluree
        .stage(handle)
        .sparql_update(&sparql)
        .execute()
        .await
        .expect("sparql update")
        .receipt
        .t
}

/// Run `sparql` on its own task; it parks at the handle's stage gate if one
/// is set.
fn spawn_update(
    fluree: &Fluree,
    handle: &LedgerHandle,
    sparql: &'static str,
) -> tokio::task::JoinHandle<i64> {
    let fluree = fluree.clone();
    let handle = handle.clone();
    tokio::spawn(async move { update(&fluree, &handle, sparql).await })
}

async fn count(fluree: &Fluree, handle: &LedgerHandle, sparql: &str) -> usize {
    let state = handle.snapshot().await.to_ledger_state();
    let result = fluree
        .query(
            &GraphDb::from_ledger_state(&state),
            &format!("{PREFIX}{sparql}"),
        )
        .await
        .expect("query");
    let json = result.to_sparql_json(&state.snapshot).expect("sparql json");
    json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stage_behind_a_disjoint_commit_is_rebased_not_restaged() {
    let (_dir, fluree, handle) = open().await;
    let seed_t = update(&fluree, &handle, "INSERT DATA { ex:seed ex:p 1 }").await;

    let (parked, release) = handle.gate_next_optimistic_stage_for_test();
    let behind = spawn_update(&fluree, &handle, "INSERT DATA { ex:b ex:p 2 }");
    parked.await.expect("the write parks after staging");

    let ahead_t = update(&fluree, &handle, "INSERT DATA { ex:a ex:p 3 }").await;
    assert_eq!(ahead_t, seed_t + 1);
    release.send(true).unwrap();
    let behind_t = behind.await.expect("task");

    assert_eq!(
        behind_t,
        ahead_t + 1,
        "the parked write commits on top of the one that overtook it"
    );
    let stats = handle.write_path_stats();
    assert_eq!(
        (stats.rebased, stats.restaged),
        (1, 0),
        "a disjoint write is re-based, not staged again: {stats:?}"
    );
    assert_eq!(stats.direct, 2, "the seed and the overtaking write");

    assert_eq!(
        count(&fluree, &handle, "SELECT ?s WHERE { ?s ex:p ?o }").await,
        3
    );
    let view = handle.snapshot().await;
    let stamped: Vec<i64> = view
        .novelty
        .iter_flakes(IndexType::Post)
        .filter(|f| &*f.s.name == "b")
        .map(|f| f.t)
        .collect();
    assert!(
        !stamped.is_empty() && stamped.iter().all(|&t| t == behind_t),
        "re-based flakes carry the t they committed at, got {stamped:?}"
    );
    fluree.disconnect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stage_behind_a_commit_on_its_subject_is_restaged() {
    let (_dir, fluree, handle) = open().await;
    update(&fluree, &handle, "INSERT DATA { ex:x ex:p 1 }").await;

    // Staged while ex:x has one value; a re-base would retract only that
    // one and leave the value the overtaking write added.
    let (parked, release) = handle.gate_next_optimistic_stage_for_test();
    let behind = spawn_update(&fluree, &handle, "DELETE WHERE { ex:x ?p ?o }");
    parked.await.expect("the write parks after staging");

    update(&fluree, &handle, "INSERT DATA { ex:x ex:p 2 }").await;
    release.send(true).unwrap();
    behind.await.expect("task");

    let stats = handle.write_path_stats();
    assert_eq!(
        (stats.rebased, stats.restaged),
        (0, 1),
        "a write whose subject moved is staged again under the lock: {stats:?}"
    );
    assert_eq!(
        count(&fluree, &handle, "SELECT ?o WHERE { ex:x ex:p ?o }").await,
        0,
        "the delete saw the value committed after its snapshot"
    );
    fluree.disconnect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stage_whose_namespace_code_was_taken_is_rebased_onto_a_fresh_code() {
    let (_dir, fluree, handle) = open().await;
    update(&fluree, &handle, "INSERT DATA { ex:seed ex:p 1 }").await;

    // Both writes allocate the next free namespace code for a namespace the
    // ledger has not seen; the subjects are disjoint (different local
    // names, so the colliding code alone does not make them the same Sid),
    // the codes collide. The re-base must move the parked write's subject
    // to a fresh code, or it lands under the other write's namespace.
    let (parked, release) = handle.gate_next_optimistic_stage_for_test();
    let behind = spawn_update(
        &fluree,
        &handle,
        "INSERT DATA { <http://b.example/sb> ex:p 1 }",
    );
    parked.await.expect("the write parks after staging");

    update(
        &fluree,
        &handle,
        "INSERT DATA { <http://a.example/sa> ex:p 1 }",
    )
    .await;
    release.send(true).unwrap();
    behind.await.expect("task");

    let stats = handle.write_path_stats();
    assert_eq!(
        (stats.rebased, stats.restaged),
        (1, 0),
        "a taken namespace code is re-allocated, not a reason to stage again: {stats:?}"
    );
    for (present, absent) in [
        ("<http://a.example/sa>", "<http://b.example/sa>"),
        ("<http://b.example/sb>", "<http://a.example/sb>"),
    ] {
        assert_eq!(
            count(
                &fluree,
                &handle,
                &format!("SELECT ?o WHERE {{ {present} ex:p ?o }}")
            )
            .await,
            1,
            "{present} keeps its own namespace"
        );
        assert_eq!(
            count(
                &fluree,
                &handle,
                &format!("SELECT ?o WHERE {{ {absent} ex:p ?o }}")
            )
            .await,
            0,
            "{absent} must not exist: a Sid was left on the taken code"
        );
    }
    let view = handle.snapshot().await;
    let (a, b) = (
        view.snapshot
            .namespace_reverse()
            .get("http://a.example/")
            .copied(),
        view.snapshot
            .namespace_reverse()
            .get("http://b.example/")
            .copied(),
    );
    assert!(
        a.is_some() && b.is_some() && a != b,
        "both namespaces got codes: {a:?} {b:?}"
    );
    fluree.disconnect().await;
}

/// A stage names its subjects under the namespace codes it allocated; the
/// commits in between name theirs under the codes the ledger gave the same
/// prefixes. Once a commit has taken the stage's code, the same IRI carries
/// different codes on the two sides, and an overlap compared code for code
/// reads as disjoint: an upsert re-bases over a write to its own subject and
/// both values survive. The overlap has to be found in the ledger's codes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stage_on_a_subject_written_under_a_moved_code_is_restaged() {
    let (_dir, fluree, handle) = open().await;
    update(&fluree, &handle, "INSERT DATA { ex:seed ex:p 0 }").await;

    let (parked, release) = handle.gate_next_optimistic_stage_for_test();
    let behind = {
        let fluree = fluree.clone();
        let handle = handle.clone();
        tokio::spawn(async move {
            fluree
                .stage(&handle)
                .upsert(&serde_json::json!({
                    "@id": "http://b.example/s",
                    "http://example.org/p": 1
                }))
                .execute()
                .await
                .expect("upsert")
                .receipt
                .t
        })
    };
    parked.await.expect("the write parks after staging");

    // Takes the code the parked stage allocated for its prefix ...
    update(
        &fluree,
        &handle,
        "INSERT DATA { <http://a.example/a> ex:p 9 }",
    )
    .await;
    // ... so this write to the parked stage's own subject gets the next one.
    update(
        &fluree,
        &handle,
        "INSERT DATA { <http://b.example/s> ex:p 2 }",
    )
    .await;
    release.send(true).unwrap();
    behind.await.expect("task");

    let stats = handle.write_path_stats();
    assert_eq!(
        (stats.rebased, stats.restaged),
        (0, 1),
        "a write to the stage's own subject is a conflict under any code: {stats:?}"
    );
    assert_eq!(
        count(
            &fluree,
            &handle,
            "SELECT ?v WHERE { <http://b.example/s> ex:p ?v }"
        )
        .await,
        1,
        "an upsert replaces the value written ahead of it"
    );
    fluree.disconnect().await;
}

/// A stage staged while the ledger had no shapes is bounded to its subjects,
/// and a commit that adds a shape touches only the shape's own subjects, so
/// the stage would re-base over it without ever being validated. A commit
/// that changes what validation depends on is a barrier instead: what was
/// staged before it stages again, and meets the shape.
#[cfg(feature = "shacl")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stage_behind_a_commit_that_adds_a_shape_is_validated_against_it() {
    let (_dir, fluree, handle) = open().await;
    update(&fluree, &handle, "INSERT DATA { ex:seed ex:p 0 }").await;

    let (parked, release) = handle.gate_next_optimistic_stage_for_test();
    let behind = {
        let fluree = fluree.clone();
        let handle = handle.clone();
        tokio::spawn(async move {
            fluree
                .stage(&handle)
                .sparql_update(&format!("{PREFIX}INSERT DATA {{ ex:bob a ex:Person }}"))
                .execute()
                .await
                .map(|r| r.receipt.t)
        })
    };
    parked.await.expect("the write parks after staging");

    update(
        &fluree,
        &handle,
        "PREFIX sh: <http://www.w3.org/ns/shacl#> \
         INSERT DATA { \
           ex:PersonShape a sh:NodeShape ; sh:targetClass ex:Person ; sh:property ex:NameShape . \
           ex:NameShape sh:path ex:name ; sh:minCount 1 . \
         }",
    )
    .await;
    release.send(true).unwrap();

    let result = behind.await.expect("task");
    let err = result.expect_err("a Person without a name violates the shape committed ahead of it");
    assert!(
        format!("{err:?}").contains("MinCount"),
        "rejected for the right reason: {err:?}"
    );
    assert_eq!(
        handle.write_path_stats().rebased,
        0,
        "nothing re-bases across a commit that changes the shapes"
    );
    assert_eq!(
        count(&fluree, &handle, "SELECT ?s WHERE { ?s a ex:Person }").await,
        0,
        "the unvalidated insert must not have landed"
    );
    fluree.disconnect().await;
}

/// Many SPARQL writers on one ledger all land, and most of them by re-base:
/// under contention the state has moved by the time a stage takes the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sparql_writes_under_contention_land_by_rebase() {
    const CLIENTS: usize = 16;
    const PER_CLIENT: usize = 20;

    let (_dir, fluree, handle) = open().await;
    let tasks: Vec<_> = (0..CLIENTS)
        .map(|c| {
            let fluree = fluree.clone();
            let handle = handle.clone();
            tokio::spawn(async move {
                let mut failures = Vec::new();
                for i in 0..PER_CLIENT {
                    let n = c * PER_CLIENT + i;
                    let sparql =
                        format!("{PREFIX}INSERT DATA {{ ex:person-{n} ex:name \"Person {n}\" }}");
                    if let Err(e) = fluree.stage(&handle).sparql_update(&sparql).execute().await {
                        failures.push(e.to_string());
                    }
                }
                failures
            })
        })
        .collect();
    let mut failures = Vec::new();
    for task in tasks {
        failures.extend(task.await.expect("client task"));
    }
    assert!(
        failures.is_empty(),
        "{} of {} commits failed; first: {}",
        failures.len(),
        CLIENTS * PER_CLIENT,
        failures[0]
    );
    assert_eq!(
        handle.snapshot().await.t as usize,
        CLIENTS * PER_CLIENT,
        "every commit advanced the head once"
    );
    assert_eq!(
        count(&fluree, &handle, "SELECT ?s WHERE { ?s ex:name ?n }").await,
        CLIENTS * PER_CLIENT
    );
    let stats = handle.write_path_stats();
    assert_eq!(
        stats.direct + stats.rebased + stats.restaged,
        (CLIENTS * PER_CLIENT) as u64
    );
    assert!(
        stats.rebased > 0,
        "contended disjoint writes should re-base: {stats:?}"
    );
    fluree.disconnect().await;
}
