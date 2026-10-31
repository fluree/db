//! A retraction and an assertion of one fact at one `t` read as one answer
//! on every path: the assertion holds (`fluree_db_core::same_t`).
//!
//! Commits written by earlier versions can hold such a pair: a restating
//! write of a subtyped big integer (the index keeps no subtype, so the
//! retraction names the stored term and the assertion the written one) or of
//! a `dateTime` past the microsecond (the commit keeps six digits, so the two
//! terms reload identical). The transactor no longer builds one, so these
//! tests stage the pair as a pushed commit, which a target takes verbatim,
//! as it takes an old ledger's commits. Each case then requires the value
//! present, once, on every path: live on the instance that took the push,
//! reloaded, at `t - 1` and `t`, in the ledger's stats, after an incremental
//! build, after a full rebuild, and on a ledger never indexed.
//!
//! The targets are file-backed with `without_indexing()`, so every index
//! build is the one the test asks for.

use crate::support;
use fluree_db_api::{
    Base64Bytes, CommitOpts, CommitRef, ConflictStrategy, ExportCommitsRequest, Fluree,
    FlureeBuilder, FormatterConfig, GovernanceOptions, IndexConfig, NamespaceRegistry,
    PushCommitsRequest, ReindexOptions, StagedLedger, TimeSpec,
};
use fluree_db_core::commit::codec::{read_commit, write_commit};
use fluree_db_core::{Flake, Sid};
use serde_json::Value;

const PFX: &str =
    "PREFIX ex: <http://example.org/>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
const EX_P: &str = "http://example.org/p";
const G: &str = "http://example.org/g";
const BIG: &str = "123456789012345678901234567890";
const DT: &str = "2026-10-19T12:34:56.123456Z";

#[derive(Clone, Copy, Debug)]
enum Kind {
    /// Written `^^xsd:nonNegativeInteger`; the pair retracts it as
    /// `xsd:integer` (the term the index decodes) and asserts it as written.
    BigInt,
    /// A `dateTime` written past the microsecond reloads as six digits: the
    /// pair's two terms are identical.
    DateTime,
}

impl Kind {
    fn literal(self) -> String {
        match self {
            Kind::BigInt => format!("\"{BIG}\"^^xsd:nonNegativeInteger"),
            Kind::DateTime => format!("\"{DT}\"^^xsd:dateTime"),
        }
    }

    fn lexical(self) -> &'static str {
        match self {
            Kind::BigInt => BIG,
            Kind::DateTime => DT,
        }
    }

    /// The pair, from the retraction a `DELETE DATA` of the value wrote, in
    /// the order the commit stores it.
    fn pair(self, deleted: &Flake, order: Order) -> Vec<Flake> {
        let mut retraction = deleted.clone();
        let mut assertion = deleted.clone();
        assertion.op = true;
        if let Kind::BigInt = self {
            retraction.dt = Sid::xsd_integer();
        }
        order.arrange(retraction, assertion)
    }
}

/// The order a commit stores a pair's two flakes in. Nothing should depend
/// on it.
#[derive(Clone, Copy, Debug)]
enum Order {
    RetractFirst,
    AssertFirst,
}

impl Order {
    fn arrange(self, retraction: Flake, assertion: Flake) -> Vec<Flake> {
        match self {
            Order::RetractFirst => vec![retraction, assertion],
            Order::AssertFirst => vec![assertion, retraction],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Path {
    /// The value is in the target's index when the pair arrives.
    Indexed,
    /// The value is in novelty only, in a graph the index holds.
    NoveltyOnly,
    /// The value is in novelty only, in a graph the index holds nothing for.
    FreshGraph,
    /// The target has no index; its first build is a full one.
    NeverIndexed,
}

fn open(path: &str) -> Fluree {
    FlureeBuilder::file(path.to_string())
        .without_indexing()
        .build()
        .expect("open")
}

fn pattern(graph: Option<&str>) -> String {
    match graph {
        Some(g) => format!("GRAPH <{g}> {{ ex:s ex:p ?o }}"),
        None => "ex:s ex:p ?o".to_string(),
    }
}

fn insert(graph: Option<&str>, triples: &str) -> String {
    match graph {
        Some(g) => format!("{PFX}INSERT DATA {{ GRAPH <{g}> {{ {triples} }} }}"),
        None => format!("{PFX}INSERT DATA {{ {triples} }}"),
    }
}

/// The source's commits, oldest first, with the last (a `DELETE DATA` of the
/// value) rewritten to hold the pair.
async fn source_commits(
    kind: Kind,
    path: Path,
    graph: Option<&str>,
    order: Order,
) -> Vec<Base64Bytes> {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/same-t-src:main";
    fluree.create_ledger(id).await.expect("create source");
    let value = format!("ex:s ex:p {} .", kind.literal());
    let filler = "ex:filler ex:k \"keep\" .";
    let mut writes = vec![];
    match path {
        Path::Indexed | Path::NeverIndexed => {
            writes.push(insert(None, filler));
            writes.push(insert(graph, &format!("{value} {filler}")));
        }
        Path::NoveltyOnly => {
            writes.push(insert(None, filler));
            writes.push(insert(graph, filler));
            writes.push(insert(graph, &value));
        }
        Path::FreshGraph => {
            writes.push(insert(None, filler));
            writes.push(insert(graph, &value));
        }
    }
    let delete = match graph {
        Some(g) => format!("{PFX}DELETE DATA {{ GRAPH <{g}> {{ {value} }} }}"),
        None => format!("{PFX}DELETE DATA {{ {value} }}"),
    };
    writes.push(delete);
    for w in &writes {
        fluree
            .graph(id)
            .transact()
            .sparql_update(w)
            .commit()
            .await
            .unwrap_or_else(|e| panic!("source write {w}: {e}"));
    }
    let handle = fluree.ledger_cached(id).await.expect("source handle");
    let export = fluree
        .export_commit_range(
            &handle,
            &ExportCommitsRequest {
                cursor: None,
                cursor_id: None,
                limit: Some(20),
                ..Default::default()
            },
        )
        .await
        .expect("export");
    let mut commits = export.commits;
    commits.reverse();
    assert_eq!(commits.len(), writes.len());
    let last = commits.pop().expect("the delete");
    let mut commit = read_commit(&last.0).expect("decode");
    let deleted = commit
        .flakes
        .iter()
        .find(|f| !f.op && f.p.name_str() == "p")
        .cloned()
        .expect("the delete's retraction");
    commit.flakes = kind.pair(&deleted, order);
    commit.id = None;
    commits.push(Base64Bytes(
        write_commit(&commit, true, None).expect("encode").bytes,
    ));
    commits
}

async fn push(fluree: &Fluree, id: &str, commit: Base64Bytes) {
    let config = IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 2_000_000_000,
    };
    let request = PushCommitsRequest {
        commits: vec![commit],
        blobs: Default::default(),
        missing_blobs: Vec::new(),
        merged_commits: Vec::new(),
    };
    let accepted = fluree
        .push_commits(id, request, &GovernanceOptions::default(), &config)
        .await
        .expect("push")
        .accepted;
    assert_eq!(accepted, 1);
}

/// Lexical values of `ex:s ex:p` in `graph`, at `t` when given.
async fn values(fluree: &Fluree, id: &str, graph: Option<&str>, t: Option<i64>) -> Vec<String> {
    let q = format!("{PFX}SELECT ?o WHERE {{ {} }}", pattern(graph));
    let view = match t {
        Some(t) => fluree.graph_at(id, TimeSpec::AtT(t)),
        None => fluree.graph(id),
    };
    let json = view
        .query()
        .sparql(&q)
        .format(FormatterConfig::sparql_json())
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("query at {t:?}: {e}"));
    let mut v: Vec<String> = json["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("bindings: {json}"))
        .iter()
        .map(|b| b["o"]["value"].as_str().unwrap_or("?").to_string())
        .collect();
    v.sort();
    v
}

/// `ex:p`'s count in the ledger's stats, when it reports one.
async fn stats_count(fluree: &Fluree, id: &str) -> Option<u64> {
    let info = fluree.ledger_info(id).execute().await.expect("ledger_info");
    let properties = info.get("stats")?.get("properties")?.clone();
    let named = |v: &Value| v.as_str().is_some_and(|s| s == EX_P || s == "ex:p");
    match properties {
        Value::Array(entries) => entries.iter().find_map(|e| {
            ["@id", "id", "property", "iri"]
                .iter()
                .any(|k| e.get(*k).is_some_and(named))
                .then(|| e.get("count").and_then(Value::as_u64))?
        }),
        Value::Object(map) => map
            .iter()
            .find(|(k, _)| k.as_str() == EX_P || k.as_str() == "ex:p")
            .and_then(|(_, v)| v.as_u64().or_else(|| v.get("count")?.as_u64())),
        _ => None,
    }
}

/// Every read of one case, labelled; each should hold the value once.
async fn reads(
    fluree: &Fluree,
    id: &str,
    graph: Option<&str>,
    t: i64,
    stage: &str,
    out: &mut Vec<(String, Vec<String>)>,
) {
    out.push((
        format!("{stage}: now"),
        values(fluree, id, graph, None).await,
    ));
    out.push((
        format!("{stage}: at t-1"),
        values(fluree, id, graph, Some(t - 1)).await,
    ));
    out.push((
        format!("{stage}: at t"),
        values(fluree, id, graph, Some(t)).await,
    ));
    if graph.is_none() {
        let n = stats_count(fluree, id).await;
        out.push((
            format!("{stage}: stats count"),
            vec![n.map_or_else(|| "missing".to_string(), |n| n.to_string())],
        ));
    }
}

async fn run_case(kind: Kind, path: Path, graph: Option<&str>, order: Order) -> Vec<String> {
    let commits = source_commits(kind, path, graph, order).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let dir_path = dir.path().to_string_lossy().to_string();
    let id = "it/same-t-tgt:main";
    let t_pair = commits.len() as i64;
    let mut observed: Vec<(String, Vec<String>)> = Vec::new();
    {
        let fluree = open(&dir_path);
        fluree.create_ledger(id).await.expect("create target");
        let index_after = match path {
            Path::Indexed => t_pair - 1,
            Path::NoveltyOnly => t_pair - 2,
            Path::FreshGraph => 1,
            Path::NeverIndexed => 0,
        };
        for (n, commit) in commits.into_iter().enumerate() {
            push(&fluree, id, commit).await;
            if n as i64 + 1 == index_after {
                fluree
                    .reindex(id, ReindexOptions::default())
                    .await
                    .expect("base index");
                fluree.disconnect_ledger(id).await;
            }
        }
        // The instance that took the push: novelty applied commit by commit.
        observed.push(("live".into(), values(&fluree, id, graph, None).await));
        fluree.disconnect_ledger(id).await;
    }
    // Reloaded: novelty replayed from the commits.
    reads(
        &open(&dir_path),
        id,
        graph,
        t_pair,
        "reloaded",
        &mut observed,
    )
    .await;
    let fluree = open(&dir_path);
    if path == Path::NeverIndexed {
        support::build_and_publish_index(&fluree, id).await;
        fluree.disconnect_ledger(id).await;
        drop(fluree);
        reads(
            &open(&dir_path),
            id,
            graph,
            t_pair,
            "first build",
            &mut observed,
        )
        .await;
    } else {
        support::build_incrementally_and_publish_index(&fluree, id).await;
        fluree.disconnect_ledger(id).await;
        drop(fluree);
        reads(
            &open(&dir_path),
            id,
            graph,
            t_pair,
            "incremental",
            &mut observed,
        )
        .await;
    }
    let fluree = open(&dir_path);
    fluree
        .reindex(id, ReindexOptions::default())
        .await
        .expect("full rebuild");
    fluree.disconnect_ledger(id).await;
    drop(fluree);
    reads(
        &open(&dir_path),
        id,
        graph,
        t_pair,
        "full rebuild",
        &mut observed,
    )
    .await;

    let want = vec![kind.lexical().to_string()];
    observed
        .into_iter()
        .filter(|(label, v)| {
            if label.ends_with("stats count") {
                v != &vec!["1".to_string()]
            } else {
                v != &want
            }
        })
        .map(|(label, v)| format!("{kind:?} {path:?} {order:?} graph={graph:?} {label}: {v:?}"))
        .collect()
}

/// A same-`t` pair of one fact reads present, once, on every path, for a
/// subtyped big integer and a `dateTime`, in the default and a named graph,
/// with the pair stored in either order. One test per kind and order, so
/// they run side by side.
async fn every_path(kind: Kind, order: Order) {
    let mut failures = Vec::new();
    for graph in [None, Some(G)] {
        for path in [
            Path::Indexed,
            Path::NoveltyOnly,
            Path::FreshGraph,
            Path::NeverIndexed,
        ] {
            if path == Path::FreshGraph && graph.is_none() {
                continue;
            }
            failures.extend(run_case(kind, path, graph, order).await);
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

#[tokio::test]
async fn a_same_t_pair_reads_present_on_every_path() {
    every_path(Kind::BigInt, Order::RetractFirst).await;
}

#[tokio::test]
async fn a_same_t_pair_stored_assert_first_reads_present_on_every_path() {
    every_path(Kind::BigInt, Order::AssertFirst).await;
}

#[tokio::test]
async fn a_same_t_datetime_pair_reads_present_on_every_path() {
    every_path(Kind::DateTime, Order::RetractFirst).await;
}

#[tokio::test]
async fn a_same_t_datetime_pair_stored_assert_first_reads_present_on_every_path() {
    every_path(Kind::DateTime, Order::AssertFirst).await;
}

/// A commit built here may not hold a retraction and an assertion of one
/// fact at one `t`: `build_commit` refuses one with a typed internal error
/// naming the fact. Staging never builds one, so the view is built by hand.
#[tokio::test]
async fn a_local_commit_holding_a_same_t_pair_is_refused() {
    for kind in [Kind::BigInt, Kind::DateTime] {
        let fluree = FlureeBuilder::memory().build_memory();
        let id = "it/same-t-local:main";
        fluree.create_ledger(id).await.expect("create");
        fluree
            .graph(id)
            .transact()
            .sparql_update(&insert(None, &format!("ex:s ex:p {} .", kind.literal())))
            .commit()
            .await
            .expect("insert");
        let ledger = fluree.ledger(id).await.expect("ledger");
        let stored = support::query_sparql(
            &fluree,
            &ledger,
            &format!("{PFX}SELECT ?o WHERE {{ ex:s ex:p ?o }}"),
        )
        .await
        .expect("read");
        assert_eq!(
            stored
                .batches
                .iter()
                .map(fluree_db_api::Batch::len)
                .sum::<usize>(),
            1
        );
        let held = ledger
            .novelty
            .iter_flakes(fluree_db_core::IndexType::Spot)
            .find(|f| f.op && f.p.name_str() == "p")
            .cloned()
            .expect("the stored fact");
        let mut deleted = held.clone();
        deleted.op = false;
        let reverse_graph = ledger
            .snapshot
            .build_reverse_graph()
            .expect("reverse graph");
        let view = StagedLedger::new(
            ledger,
            kind.pair(&deleted, Order::RetractFirst),
            &reverse_graph,
        )
        .expect("stage");
        let ns = NamespaceRegistry::from_db(view.db());
        let err = fluree
            .commit_staged(
                view,
                ns,
                &IndexConfig {
                    reindex_min_bytes: 1_000_000_000,
                    reindex_max_bytes: 2_000_000_000,
                },
                CommitOpts::default(),
            )
            .await
            .expect_err("a same-t pair must be refused");
        let message = err.to_string();
        assert_eq!(err.status_code(), 500, "{kind:?}: {message}");
        assert!(
            message.contains("retract and assert one fact at one t")
                && message.contains("http://example.org/s")
                && message.contains("http://example.org/p"),
            "{kind:?}: {message}"
        );
    }
}

/// The refusal guards only commits built here: a pushed commit holding a
/// pair (as an older ledger's may) is taken verbatim, so old ledgers still
/// replicate.
#[tokio::test]
async fn a_pushed_commit_holding_a_same_t_pair_is_taken() {
    for kind in [Kind::BigInt, Kind::DateTime] {
        let commits = source_commits(kind, Path::NeverIndexed, None, Order::RetractFirst).await;
        let fluree = FlureeBuilder::memory().build_memory();
        let id = "it/same-t-push:main";
        fluree.create_ledger(id).await.expect("create target");
        let n = commits.len();
        for commit in commits {
            push(&fluree, id, commit).await;
        }
        let record = fluree
            .nameservice()
            .lookup(id)
            .await
            .expect("lookup")
            .expect("record");
        assert_eq!(record.commit_t, n as i64, "{kind:?}");
        let head = fluree_db_core::load_commit_by_id(
            fluree.content_store(&record.storage_namespace()).as_ref(),
            record.commit_head_id.as_ref().expect("head"),
        )
        .await
        .expect("head commit");
        let mut ops: Vec<bool> = head
            .flakes
            .iter()
            .filter(|f| f.p.name_str() == "p")
            .map(|f| f.op)
            .collect();
        ops.sort();
        assert_eq!(ops, vec![false, true], "{kind:?}: the target took the pair");
    }
}

/// Rebasing a branch whose commit holds a pair (written before the staging
/// fix) replays the pair as its assertion: the rebased commit is accepted,
/// holds no pair, and the value reads present.
#[tokio::test]
async fn a_rebase_replays_a_same_t_pair_as_its_assertion() {
    // The branch's commits come from a source ledger with a pair, pushed
    // onto a branch of the target.
    let commits = source_commits(Kind::BigInt, Path::NeverIndexed, None, Order::RetractFirst).await;
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it/same-t-rebase";
    let main = "it/same-t-rebase:main";
    fluree.create_ledger(main).await.expect("create");
    let mut commits = commits.into_iter();
    // main: the filler and the value; dev branches there, then takes the pair.
    for commit in commits.by_ref().take(2) {
        push(&fluree, main, commit).await;
    }
    fluree
        .create_branch(name, "dev", Some("main"), None)
        .await
        .expect("branch");
    let dev = "it/same-t-rebase:dev";
    push(&fluree, dev, commits.next().expect("the pair")).await;
    // main moves on, so the rebase replays dev's commit.
    fluree
        .graph(main)
        .transact()
        .sparql_update(&insert(None, "ex:other ex:k \"o\" ."))
        .commit()
        .await
        .expect("main moves");
    fluree
        .rebase_branch(name, "dev", fluree_db_api::ConflictStrategy::default())
        .await
        .expect("rebase");
    let ledger = fluree.ledger(dev).await.expect("dev");
    let at_head: Vec<bool> = ledger
        .novelty
        .iter_flakes(fluree_db_core::IndexType::Spot)
        .filter(|f| f.t == ledger.t() && f.p.name_str() == "p")
        .map(|f| f.op)
        .collect();
    assert!(
        !at_head.contains(&false),
        "the replayed commit holds no retraction of the value: {at_head:?}"
    );
    assert_eq!(
        values(&fluree, dev, None, None).await,
        vec![BIG.to_string()]
    );
}

/// A source ledger's commits, oldest first.
async fn exported(fluree: &Fluree, id: &str) -> Vec<Base64Bytes> {
    let handle = fluree.ledger_cached(id).await.expect("source handle");
    let mut commits = fluree
        .export_commit_range(
            &handle,
            &ExportCommitsRequest {
                cursor: None,
                cursor_id: None,
                limit: Some(20),
                ..Default::default()
            },
        )
        .await
        .expect("export")
        .commits;
    commits.reverse();
    commits
}

/// Two commits: a filler, then one holding an assertion and a retraction of
/// `ex:s ex:p <literal>` at one `t`, over a value absent before it. That is
/// what a merge or revert written before the net-change fold left when its
/// range inserted the value and then deleted it. Push refuses such a commit
/// (its retraction names nothing the target holds); a pull takes it.
async fn absent_pair_commits(literal: &str, order: Order) -> Vec<Base64Bytes> {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/same-t-absent-src:main";
    fluree.create_ledger(id).await.expect("create source");
    for w in [
        insert(None, "ex:filler ex:k \"keep\" ."),
        insert(None, &format!("ex:s ex:p {literal} .")),
    ] {
        fluree
            .graph(id)
            .transact()
            .sparql_update(&w)
            .commit()
            .await
            .expect("source write");
    }
    let mut commits = exported(&fluree, id).await;
    assert_eq!(commits.len(), 2);
    let mut commit = read_commit(&commits[1].0).expect("decode");
    let asserted = commit
        .flakes
        .iter()
        .find(|f| f.op && f.p.name_str() == "p")
        .cloned()
        .expect("the value's assertion");
    let mut retracted = asserted.clone();
    retracted.op = false;
    commit.flakes = order.arrange(retracted, asserted);
    commit.id = None;
    commits[1] = Base64Bytes(write_commit(&commit, true, None).expect("encode").bytes);
    commits
}

/// Pull `commits` into a new ledger `id` at `path`; the head's commit id.
async fn pulled(path: &str, id: &str, commits: Vec<Base64Bytes>) -> fluree_db_core::ContentId {
    let fluree = open(path);
    fluree.create_ledger(id).await.expect("create target");
    let head = fluree
        .import_commits_incremental(id, commits, Vec::new(), Default::default())
        .await
        .expect("pull")
        .head_commit_id;
    fluree.disconnect_ledger(id).await;
    drop(fluree);
    head
}

async fn full_rebuild(path: &str, id: &str) {
    let fluree = open(path);
    fluree
        .reindex(id, ReindexOptions::default())
        .await
        .expect("full rebuild");
    fluree.disconnect_ledger(id).await;
}

async fn revert(path: &str, id: &str, commit: fluree_db_core::ContentId) {
    let fluree = open(path);
    let name = id.split(':').next().expect("ledger name");
    fluree
        .revert_commits(
            name,
            "main",
            vec![CommitRef::Exact(commit)],
            ConflictStrategy::TakeSource,
        )
        .await
        .expect("revert");
    fluree.disconnect_ledger(id).await;
}

/// A pair over a value absent before it reads present and counts once,
/// after a full rebuild too; reverting the commit that holds it restores the
/// state before it, absent. Over a value present before it, the same revert
/// keeps the value. For a string and a big integer, the pair stored in
/// either order, reverted from novelty and from an index.
#[tokio::test]
async fn a_pair_over_an_absent_value_counts_once_and_reverts_to_absent() {
    let big = format!("\"{BIG}\"^^xsd:nonNegativeInteger");
    let mut failures = Vec::new();
    for (literal, lexical) in [("\"v\"", "v"), (big.as_str(), BIG)] {
        for order in [Order::RetractFirst, Order::AssertFirst] {
            for indexed in [false, true] {
                let case = format!("{lexical} {order:?} indexed={indexed}");
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().to_string_lossy().to_string();
                let id = "it/same-t-absent:main";
                let head = pulled(&path, id, absent_pair_commits(literal, order).await).await;
                let got = values(&open(&path), id, None, None).await;
                if got != vec![lexical.to_string()] {
                    failures.push(format!("{case}: pulled: {got:?}"));
                }
                if indexed {
                    full_rebuild(&path, id).await;
                    let fluree = open(&path);
                    let got = values(&fluree, id, None, None).await;
                    let n = stats_count(&fluree, id).await;
                    if got != vec![lexical.to_string()] || n != Some(1) {
                        failures.push(format!("{case}: full rebuild: {got:?}, stats {n:?}"));
                    }
                }
                revert(&path, id, head).await;
                let got = values(&open(&path), id, None, None).await;
                if !got.is_empty() {
                    failures.push(format!("{case}: reverted: {got:?}"));
                }
                full_rebuild(&path, id).await;
                let fluree = open(&path);
                let got = values(&fluree, id, None, None).await;
                let n = stats_count(&fluree, id).await;
                if !got.is_empty() || n.is_some_and(|n| n != 0) {
                    failures.push(format!("{case}: reverted, rebuilt: {got:?}, stats {n:?}"));
                }
            }
        }
    }
    // Over a value present before it: the revert keeps it.
    for order in [Order::RetractFirst, Order::AssertFirst] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_string_lossy().to_string();
        let id = "it/same-t-present:main";
        let commits = source_commits(Kind::BigInt, Path::NeverIndexed, None, order).await;
        let head = pulled(&path, id, commits).await;
        revert(&path, id, head).await;
        let got = values(&open(&path), id, None, None).await;
        if got != vec![BIG.to_string()] {
            failures.push(format!("present before, {order:?}: reverted: {got:?}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// `DELETE DATA` of `literal` on the ledger at `path`: the commit's id.
async fn delete_value(path: &str, id: &str, literal: &str) -> fluree_db_core::ContentId {
    let fluree = open(path);
    let commit_id = fluree
        .graph(id)
        .transact()
        .sparql_update(&format!("{PFX}DELETE DATA {{ ex:s ex:p {literal} . }}"))
        .commit()
        .await
        .expect("delete")
        .receipt
        .commit_id;
    fluree.disconnect_ledger(id).await;
    drop(fluree);
    commit_id
}

/// Revert `commits` together on the ledger at `path`.
async fn revert_range(path: &str, id: &str, commits: Vec<fluree_db_core::ContentId>) {
    let fluree = open(path);
    let name = id.split(':').next().expect("ledger name");
    fluree
        .revert_commits(
            name,
            "main",
            commits.into_iter().map(CommitRef::Exact).collect(),
            ConflictStrategy::TakeSource,
        )
        .await
        .expect("revert");
    fluree.disconnect_ledger(id).await;
}

/// Reverting a pair's commit together with a later one that deleted the
/// value restores what held before the pair, whatever the head holds now:
/// the value, when it was there before (a big integer and a `dateTime`,
/// reverted from novelty and from an index), and nothing, when it wasn't.
/// The pair stored in either order.
#[tokio::test]
async fn a_range_revert_from_a_pair_restores_what_held_before_it() {
    let mut failures = Vec::new();
    for order in [Order::RetractFirst, Order::AssertFirst] {
        for kind in [Kind::BigInt, Kind::DateTime] {
            for indexed in [false, true] {
                let case = format!("present before, {kind:?} {order:?} indexed={indexed}");
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().to_string_lossy().to_string();
                let id = "it/same-t-range:main";
                let commits = source_commits(kind, Path::NeverIndexed, None, order).await;
                let pair = pulled(&path, id, commits).await;
                let delete = delete_value(&path, id, &kind.literal()).await;
                if indexed {
                    full_rebuild(&path, id).await;
                }
                revert_range(&path, id, vec![pair, delete]).await;
                let got = values(&open(&path), id, None, None).await;
                if got != vec![kind.lexical().to_string()] {
                    failures.push(format!("{case}: {got:?}"));
                }
            }
        }
        let big = format!("\"{BIG}\"^^xsd:nonNegativeInteger");
        for literal in ["\"v\"", big.as_str()] {
            let case = format!("absent before, {literal} {order:?}");
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().to_string_lossy().to_string();
            let id = "it/same-t-range-absent:main";
            let pair = pulled(&path, id, absent_pair_commits(literal, order).await).await;
            let delete = delete_value(&path, id, literal).await;
            revert_range(&path, id, vec![pair, delete]).await;
            let got = values(&open(&path), id, None, None).await;
            if !got.is_empty() {
                failures.push(format!("{case}: {got:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A `DELETE DATA` of an indexed big integer, while novelty holds a pair of
/// it (`-x^^xsd:integer`, `+x^^xsd:nonNegativeInteger`), deletes it: the
/// read it names the stored fact from merges the index row and the two ops
/// by index key, and the pair holds the assertion. Under either subtype,
/// with the pair stored in either order.
#[tokio::test]
async fn a_delete_of_an_indexed_big_integer_names_a_pair_novelty_holds() {
    let mut failures = Vec::new();
    for order in [Order::RetractFirst, Order::AssertFirst] {
        for subtype in ["nonNegativeInteger", "integer"] {
            let case = format!("{order:?} ^^xsd:{subtype}");
            let commits = source_commits(Kind::BigInt, Path::Indexed, None, order).await;
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().to_string_lossy().to_string();
            let id = "it/same-t-delete:main";
            {
                let fluree = open(&path);
                fluree.create_ledger(id).await.expect("create target");
                let n = commits.len();
                for (i, commit) in commits.into_iter().enumerate() {
                    if i + 1 == n {
                        fluree
                            .reindex(id, ReindexOptions::default())
                            .await
                            .expect("index the value");
                    }
                    push(&fluree, id, commit).await;
                }
                fluree.disconnect_ledger(id).await;
            }
            let fluree = open(&path);
            let receipt = fluree
                .graph(id)
                .transact()
                .sparql_update(&format!(
                    "{PFX}DELETE DATA {{ ex:s ex:p \"{BIG}\"^^xsd:{subtype} }}"
                ))
                .commit()
                .await
                .expect("delete")
                .receipt;
            if receipt.assert_count != 0 || receipt.retract_count == 0 {
                failures.push(format!(
                    "{case}: committed {}+/{}-",
                    receipt.assert_count, receipt.retract_count
                ));
            }
            fluree.disconnect_ledger(id).await;
            drop(fluree);
            let got = values(&open(&path), id, None, None).await;
            if !got.is_empty() {
                failures.push(format!("{case}: after the delete: {got:?}"));
            }
            full_rebuild(&path, id).await;
            let got = values(&open(&path), id, None, None).await;
            if !got.is_empty() {
                failures.push(format!("{case}: after a full rebuild: {got:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A Cypher `SET` of a property to the value it already holds commits
/// nothing, so it can't leave a same-`t` pair for a revert to undo. If it
/// ever commits, that commit has to revert to the value it restated.
#[tokio::test]
async fn a_restating_cypher_set_commits_nothing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/same-t-cypher:main";
    fluree.create_ledger(id).await.expect("create");
    fluree
        .graph(id)
        .transact()
        .sparql_update(&insert(None, "ex:a a ex:Node ; ex:name \"x\" ."))
        .commit()
        .await
        .expect("seed");
    let ledger = fluree.ledger(id).await.expect("ledger");
    let seeded_t = ledger.t();
    let set = fluree
        .transact_cypher(
            ledger,
            "MATCH (n:`http://example.org/Node`) SET n.`http://example.org/name` = 'x'",
        )
        .await
        .expect("cypher SET");
    let receipt = set.receipt;
    assert_eq!(
        (receipt.flake_count, receipt.t),
        (0, seeded_t),
        "a restating SET committed {}+/{}-",
        receipt.assert_count,
        receipt.retract_count
    );
    assert_eq!(fluree.ledger(id).await.expect("ledger").t(), seeded_t);
}
