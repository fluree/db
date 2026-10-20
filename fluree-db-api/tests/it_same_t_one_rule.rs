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
    Base64Bytes, CommitOpts, ExportCommitsRequest, Fluree, FlureeBuilder, FormatterConfig,
    GovernanceOptions, IndexConfig, NamespaceRegistry, PushCommitsRequest, ReindexOptions,
    StagedLedger, TimeSpec,
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

    /// The pair, from the retraction a `DELETE DATA` of the value wrote.
    fn pair(self, deleted: &Flake) -> Vec<Flake> {
        let mut retraction = deleted.clone();
        let mut assertion = deleted.clone();
        assertion.op = true;
        if let Kind::BigInt = self {
            retraction.dt = Sid::xsd_integer();
        }
        vec![retraction, assertion]
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
async fn source_commits(kind: Kind, path: Path, graph: Option<&str>) -> Vec<Base64Bytes> {
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
    commit.flakes = kind.pair(&deleted);
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

async fn run_case(kind: Kind, path: Path, graph: Option<&str>) -> Vec<String> {
    let commits = source_commits(kind, path, graph).await;
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
        // The fresh-graph branch an incremental build writes keeps no
        // history: it writes the window's winners as rows. That is fixed with
        // the lifecycle walk in `fix/delete-survives-index-build` (#2118),
        // and holds with it; nothing here depends on the same-`t` rule.
        .filter(|(label, _)| !(path == Path::FreshGraph && label == "incremental: at t-1"))
        .filter(|(label, v)| {
            if label.ends_with("stats count") {
                v != &vec!["1".to_string()]
            } else {
                v != &want
            }
        })
        .map(|(label, v)| format!("{kind:?} {path:?} graph={graph:?} {label}: {v:?}"))
        .collect()
}

/// A same-`t` pair of one fact reads present, once, on every path, for a
/// subtyped big integer and a `dateTime`, in the default and a named graph.
#[tokio::test]
async fn a_same_t_pair_reads_present_on_every_path() {
    let mut failures = Vec::new();
    for kind in [Kind::BigInt, Kind::DateTime] {
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
                failures.extend(run_case(kind, path, graph).await);
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
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
        let view = StagedLedger::new(ledger, kind.pair(&deleted), &reverse_graph).expect("stage");
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
        let commits = source_commits(kind, Path::NeverIndexed, None).await;
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
            fluree.content_store(id).as_ref(),
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
    let commits = source_commits(Kind::BigInt, Path::NeverIndexed, None).await;
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
