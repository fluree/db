//! Numbers the index keeps in a NumBig arena (integers beyond `i64`, decimals
//! that don't fit inline) keep their declared XSD datatype (#2027): two
//! subtypes of one value are two terms, in novelty and in the index alike.
//! Index statistics count them as that datatype (#1321), a `BIND` forwards
//! the term (#2058), and the arena is stored in chunks (#2417).
//!
//! A `main` (v4) root types arena values by their value; a v5 binary serves
//! one as v4 did until the ledger's first v5 build.

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions};
use fluree_db_binary_index::format::index_root::{is_current_format, IndexRoot};
use fluree_db_indexer::IndexerConfig;

const PFX: &str = "PREFIX ex: <http://example.org/>\n\
                   PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n\
                   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n";
const EX: &str = "http://example.org/";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
/// `u64::MAX`, above `i64::MAX`.
const MAX: &str = "18446744073709551615";

fn file(path: &str) -> Fluree {
    FlureeBuilder::file(path.to_string())
        .without_indexing()
        .without_storage_lock()
        .build()
        .expect("file-backed Fluree")
}

async fn update(fluree: &Fluree, ledger_id: &str, sparql: &str) {
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(&format!("{PFX}{sparql}"))
        .commit()
        .await
        .expect("update");
}

/// Rows of `q`, each as its sorted `var=value` columns, a literal's value
/// followed by `^^` and its datatype's local name; rows sorted.
async fn rows(fluree: &Fluree, ledger_id: &str, q: &str) -> Vec<String> {
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let json = support::query_sparql(fluree, &ledger, &format!("{PFX}{q}"))
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"))
        .to_sparql_json(&ledger.snapshot)
        .expect("SPARQL JSON");
    let mut out: Vec<String> = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| {
            let mut cols: Vec<String> = b
                .as_object()
                .expect("binding")
                .iter()
                .map(|(var, v)| {
                    let value = v["value"].as_str().unwrap_or_default();
                    let value = value.strip_prefix(EX).unwrap_or(value);
                    match v.get("datatype").and_then(|d| d.as_str()) {
                        Some(dt) => format!("{var}={value}^^{}", dt.trim_start_matches(XSD)),
                        None => format!("{var}={value}"),
                    }
                })
                .collect();
            cols.sort();
            cols.join(" ")
        })
        .collect();
    out.sort();
    out
}

fn strings(rows: &[&str]) -> Vec<String> {
    rows.iter().map(|r| (*r).to_string()).collect()
}

async fn rebuild_with(fluree: &Fluree, ledger_id: &str, config: &IndexerConfig) {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .unwrap();
    let result = fluree_db_indexer::rebuild_index_from_commits(
        fluree.branched_content_store(ledger_id).await.unwrap(),
        ledger_id,
        &record,
        config.clone(),
    )
    .await
    .expect("full rebuild");
    support::publish_built_index(fluree, ledger_id, result.index_t, &result.root_id).await;
}

async fn incremental_with(fluree: &Fluree, ledger_id: &str, config: &IndexerConfig) {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .unwrap();
    let result = fluree_db_indexer::build_incremental_index_for_record(
        fluree.branched_content_store(ledger_id).await.unwrap(),
        &record,
        config.clone(),
    )
    .await
    .expect("incremental build");
    support::publish_built_index(fluree, ledger_id, result.index_t, &result.root_id).await;
}

/// The bytes of `ledger_id`'s published index root.
async fn root_bytes(fluree: &Fluree, ledger_id: &str) -> Vec<u8> {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .unwrap();
    fluree
        .content_store(&record.storage_namespace())
        .get(record.index_head_id.as_ref().expect("indexed"))
        .await
        .unwrap()
        .to_vec()
}

/// The datatype buckets of every `ex:` property of the published index's
/// stats that holds an `UNKNOWN` (255) one. (A triple term still counts as
/// `UNKNOWN`, so `rdf:reifies` is left out.)
async fn unknown_datatype_buckets(fluree: &Fluree, ledger_id: &str) -> Vec<String> {
    let root = IndexRoot::decode(&root_bytes(fluree, ledger_id).await).unwrap();
    let iri = |p_id: u32| {
        let (ns, name) = &root.predicate_sids[p_id as usize];
        let prefix = root.namespace_codes.get(ns).map_or("", String::as_str);
        format!("{prefix}{name}")
    };
    let stats = root.stats.clone().expect("stats");
    let mut out = Vec::new();
    for graph in stats.graphs.iter().flatten() {
        for prop in &graph.properties {
            let iri = iri(prop.p_id);
            if iri.starts_with(EX) && prop.datatypes.iter().any(|&(tag, n)| tag == 255 && n > 0) {
                out.push(format!("g{} {iri}: {:?}", graph.g_id, prop.datatypes));
            }
        }
    }
    out
}

// ── One value under several subtypes ─────────────────────────────────

const SEED: &str = r#"INSERT DATA {
  ex:s ex:n "18446744073709551615"^^xsd:unsignedLong ,
            "18446744073709551615"^^xsd:nonNegativeInteger ;
       ex:m "18446744073709551615"^^xsd:unsignedLong .
  ex:t ex:n "18446744073709551615"^^xsd:integer .
  ex:k ex:n "18446744073709551615"^^xsd:positiveInteger .
  ex:i ex:n "9223372036854775807"^^xsd:unsignedLong .
  ex:j ex:n "9223372036854775808"^^xsd:unsignedLong .
  ex:d ex:n "123456789012345678.5"^^xsd:decimal , 1.5 .
  ex:w ex:x "18446744073709551616"^^xsd:nonNegativeInteger , 5 .
  ex:claim rdf:reifies <<( ex:s ex:n "18446744073709551615"^^xsd:unsignedLong )>> .
  GRAPH ex:g {
    ex:u ex:n "18446744073709551615"^^xsd:unsignedLong ,
              "18446744073709551615"^^xsd:integer .
  }
}"#;

/// Deleting one subtype leaves the other; a `BIND` of a stored value names
/// it under its own subtype, so the `DELETE` retracts it (#2058).
const CHANGES: &str = r#"DELETE DATA { ex:s ex:n "18446744073709551615"^^xsd:nonNegativeInteger } ;
INSERT DATA { ex:t ex:m "18446744073709551615"^^xsd:integer } ;
DELETE { ex:k ex:n ?v } WHERE { ex:k ex:n ?o BIND(?o AS ?v) }"#;

/// A probe matching a value under one datatype. A numeric constant in an
/// expression compares by value, across numeric datatypes, so the probe
/// filters on `DATATYPE`; `check` also names each term with a typed constant
/// in the triple pattern.
fn typed(value: &str, datatype: &str) -> String {
    format!(
        "SELECT ?s WHERE {{ ?s ex:n ?o FILTER(DATATYPE(?o) = xsd:{datatype} && ?o = {value}) }}"
    )
}

/// Each probe and its answer once `SEED` and `CHANGES` are written; `None`
/// where the answer is whatever the unindexed ledger answers. The joins are
/// such probes: a bound numeric object is matched by value, across numeric
/// datatypes, with or without an index.
fn probes() -> Vec<(String, Option<Vec<String>>)> {
    let fixed = |q: &str, rows: &[&str]| (q.to_string(), Some(strings(rows)));
    vec![
        fixed(
            "SELECT ?s ?o WHERE { ?s ex:n ?o }",
            &[
                "o=1.5^^decimal s=d",
                "o=123456789012345678.5^^decimal s=d",
                "o=18446744073709551615^^integer s=t",
                "o=18446744073709551615^^unsignedLong s=s",
                "o=9223372036854775807^^unsignedLong s=i",
                "o=9223372036854775808^^unsignedLong s=j",
            ],
        ),
        fixed(
            "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ex:n ?o }",
            &["c=6^^integer"],
        ),
        fixed(
            "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { GRAPH ex:g { ?s ?p ?o } }",
            &["c=2^^integer"],
        ),
        (
            "SELECT ?dt (COUNT(*) AS ?c) WHERE { ?s ex:n ?o BIND(DATATYPE(?o) AS ?dt) } GROUP BY ?dt"
                .to_string(),
            Some(vec![
                format!("c=1^^integer dt={XSD}integer"),
                format!("c=2^^integer dt={XSD}decimal"),
                format!("c=3^^integer dt={XSD}unsignedLong"),
            ]),
        ),
        (typed(MAX, "unsignedLong"), Some(strings(&["s=s"]))),
        (typed(MAX, "nonNegativeInteger"), Some(Vec::new())),
        (typed(MAX, "integer"), Some(strings(&["s=t"]))),
        (typed("9223372036854775807", "unsignedLong"), Some(strings(&["s=i"]))),
        (typed("9223372036854775808", "unsignedLong"), Some(strings(&["s=j"]))),
        (typed("123456789012345678.5", "decimal"), Some(strings(&["s=d"]))),
        fixed(
            "SELECT ?s WHERE { ?s ex:n 18446744073709551615 }",
            &["s=s", "s=t"],
        ),
        // A bare double equal to an integer beyond `i64` (2^64): an integer
        // datatype's statistics reach its NumBig o_type (#1321).
        fixed(
            "SELECT ?s WHERE { ?s ex:x 1.8446744073709551616E19 }",
            &["s=w"],
        ),
        ("SELECT ?a ?b WHERE { ?a ex:n ?v . ?b ex:m ?v }".to_string(), None),
        (
            "SELECT ?a ?b WHERE { ?a ex:n ?v GRAPH ex:g { ?b ex:n ?v } }".to_string(),
            None,
        ),
        (
            "SELECT ?o ?dt WHERE { ex:claim rdf:reifies ?t BIND(OBJECT(?t) AS ?o) BIND(DATATYPE(?o) AS ?dt) }"
                .to_string(),
            Some(vec![format!(
                "dt={XSD}unsignedLong o=18446744073709551615^^unsignedLong"
            )]),
        ),
    ]
}

/// The subjects a JSON-LD value object matches under `ex:n`: the twin of a
/// typed constant in a SPARQL triple pattern.
async fn jsonld_subjects(
    fluree: &Fluree,
    ledger_id: &str,
    value: &str,
    datatype: &str,
) -> Vec<String> {
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let q = serde_json::json!({
        "@context": {"ex": EX, "xsd": XSD},
        "select": ["?s"],
        "where": {"@id": "?s", "ex:n": {"@value": value, "@type": format!("xsd:{datatype}")}}
    });
    let out = support::query_jsonld_formatted(fluree, &ledger, &q)
        .await
        .expect("JSON-LD query");
    let mut subjects: Vec<String> = out
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| {
            let s = row.as_array().map_or(row, |r| &r[0]);
            s.as_str()
                .unwrap_or_default()
                .trim_start_matches("ex:")
                .to_string()
        })
        .collect();
    subjects.sort();
    subjects
}

async fn check(
    fluree: &Fluree,
    ledger_id: &str,
    stage: &str,
    baseline: &[Vec<String>],
    failures: &mut Vec<String>,
) -> Vec<Vec<String>> {
    for (value, datatype, want) in [
        (MAX, "unsignedLong", vec!["s"]),
        (MAX, "nonNegativeInteger", vec![]),
        (MAX, "integer", vec!["t"]),
        ("9223372036854775807", "unsignedLong", vec!["i"]),
        ("9223372036854775807", "integer", vec![]),
        ("9223372036854775808", "unsignedLong", vec!["j"]),
    ] {
        let got = jsonld_subjects(fluree, ledger_id, value, datatype).await;
        if got != want {
            failures.push(format!(
                "{stage}: JSON-LD {value}^^{datatype}\n  got  {got:?}\n  want {want:?}"
            ));
        }
        let q = format!(r#"SELECT ?s WHERE {{ ?s ex:n "{value}"^^xsd:{datatype} }}"#);
        let got = rows(fluree, ledger_id, &q).await;
        let want: Vec<String> = want.iter().map(|s| format!("s={s}")).collect();
        if got != want {
            failures.push(format!("{stage}: {q}\n  got  {got:?}\n  want {want:?}"));
        }
    }
    let mut answers = Vec::new();
    for (n, (q, want)) in probes().into_iter().enumerate() {
        let got = rows(fluree, ledger_id, &q).await;
        let want = want.or_else(|| baseline.get(n).cloned());
        if let Some(want) = want.filter(|want| *want != got) {
            failures.push(format!("{stage}: {q}\n  got  {got:?}\n  want {want:?}"));
        }
        answers.push(got);
    }
    answers
}

/// Every probe answers alike with no index, over a full build with the
/// changes in novelty, after a reload, after an incremental build and after
/// a full rebuild. Arenas are split into small chunks throughout.
#[tokio::test]
async fn big_numbers_answer_alike_before_and_after_every_build() {
    const ID: &str = "it/numbig-declared:main";
    let config = IndexerConfig::default().with_numbig_chunk_bytes(64);
    let mut failures = Vec::new();

    let unindexed = tempfile::TempDir::new().unwrap();
    let path = unindexed.path().to_string_lossy().to_string();
    let fluree = file(&path);
    fluree.create_ledger(ID).await.unwrap();
    update(&fluree, ID, SEED).await;
    update(&fluree, ID, CHANGES).await;
    let baseline = check(&fluree, ID, "unindexed", &[], &mut failures).await;

    let indexed = tempfile::TempDir::new().unwrap();
    let path = indexed.path().to_string_lossy().to_string();
    let fluree = file(&path);
    fluree.create_ledger(ID).await.unwrap();
    update(&fluree, ID, SEED).await;
    rebuild_with(&fluree, ID, &config).await;
    let fluree = file(&path);
    update(&fluree, ID, CHANGES).await;
    check(
        &fluree,
        ID,
        "changes over a full build",
        &baseline,
        &mut failures,
    )
    .await;
    check(&file(&path), ID, "reloaded", &baseline, &mut failures).await;

    incremental_with(&file(&path), ID, &config).await;
    let fluree = file(&path);
    check(
        &fluree,
        ID,
        "after an incremental build",
        &baseline,
        &mut failures,
    )
    .await;
    for bucket in unknown_datatype_buckets(&fluree, ID).await {
        failures.push(format!(
            "after an incremental build: UNKNOWN datatype {bucket}"
        ));
    }

    rebuild_with(&fluree, ID, &config).await;
    let fluree = file(&path);
    check(
        &fluree,
        ID,
        "after a full rebuild",
        &baseline,
        &mut failures,
    )
    .await;
    for bucket in unknown_datatype_buckets(&fluree, ID).await {
        failures.push(format!("after a full rebuild: UNKNOWN datatype {bucket}"));
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// An identical re-upsert of an indexed big integer under a subtype keeps it,
/// and an upsert of another value replaces it.
#[tokio::test]
async fn an_upsert_keeps_or_replaces_an_indexed_big_integer_subtype() {
    const ID: &str = "it/numbig-upsert:main";
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    fluree.create_ledger(ID).await.unwrap();
    let doc = |value: &str| {
        serde_json::json!({
            "@context": {"ex": EX, "xsd": XSD},
            "@id": "ex:s",
            "ex:n": {"@value": value, "@type": "xsd:unsignedLong"}
        })
    };
    let upsert = |value: &'static str| {
        let fluree = file(&path);
        async move {
            let ledger = fluree.ledger(ID).await.unwrap();
            fluree.upsert(ledger, &doc(value)).await.expect("upsert");
        }
    };
    upsert(MAX).await;
    rebuild_with(&fluree, ID, &IndexerConfig::default()).await;
    upsert(MAX).await;
    let q = "SELECT ?o WHERE { ex:s ex:n ?o }";
    let max = strings(&[&format!("o={MAX}^^unsignedLong")]);
    assert_eq!(rows(&file(&path), ID, q).await, max, "identical re-upsert");
    rebuild_with(&file(&path), ID, &IndexerConfig::default()).await;
    assert_eq!(rows(&file(&path), ID, q).await, max, "rebuilt");

    upsert("18446744073709551614").await;
    let replaced = strings(&["o=18446744073709551614^^unsignedLong"]);
    assert_eq!(rows(&file(&path), ID, q).await, replaced, "replaced");
    rebuild_with(&file(&path), ID, &IndexerConfig::default()).await;
    assert_eq!(
        rows(&file(&path), ID, q).await,
        replaced,
        "replaced, rebuilt"
    );
}

// ── Chunks ───────────────────────────────────────────────────────────

/// The chunks of the default graph's one NumBig arena.
async fn arena_chunks(
    fluree: &Fluree,
    ledger_id: &str,
) -> Vec<fluree_db_binary_index::NumBigChunkRef> {
    let root = IndexRoot::decode(&root_bytes(fluree, ledger_id).await).unwrap();
    let graph = root
        .graph_arenas
        .iter()
        .find(|ga| ga.g_id == 0)
        .expect("default graph arenas");
    let [arena] = graph.numbig.as_slice() else {
        panic!("one NumBig arena: {:?}", graph.numbig);
    };
    arena.chunks.clone()
}

fn amounts(range: std::ops::Range<u32>) -> String {
    let values: Vec<String> = range
        .map(|i| format!("\"1234567890123456789.{i:03}\"^^xsd:decimal"))
        .collect();
    format!("INSERT DATA {{ ex:ledger ex:amt {} }}", values.join(" , "))
}

/// A NumBig arena is stored in byte-bounded chunks of consecutive handles. An
/// incremental build leaves every chunk but the last as it was (which chunks
/// it rewrites is `NumBigArenaRef::closed_chunks`'s unit test), and a full
/// rebuild splits the same values the same way. The arena reads back whole
/// after each build, a reload, an archive round trip and a garbage
/// collection.
#[tokio::test]
async fn an_arena_is_stored_in_chunks_that_incremental_builds_keep() {
    const ID: &str = "it/numbig-chunks:main";
    let config = IndexerConfig::default().with_numbig_chunk_bytes(200);
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    fluree.create_ledger(ID).await.unwrap();
    update(&fluree, ID, &amounts(0..30)).await;
    rebuild_with(&fluree, ID, &config).await;

    let count = "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ex:ledger ex:amt ?o }";
    let first = r#"SELECT ?s WHERE { ?s ex:amt "1234567890123456789.000"^^xsd:decimal }"#;
    let last = |i: u32| {
        format!(r#"SELECT ?s WHERE {{ ?s ex:amt "1234567890123456789.{i:03}"^^xsd:decimal }}"#)
    };
    let found = strings(&["s=ledger"]);

    let before = arena_chunks(&file(&path), ID).await;
    assert!(before.len() >= 3, "{before:?}");
    assert_eq!(before.iter().map(|c| c.entries).sum::<u32>(), 30);
    assert_eq!(
        rows(&file(&path), ID, count).await,
        strings(&["c=30^^integer"])
    );

    update(&file(&path), ID, &amounts(30..40)).await;
    incremental_with(&file(&path), ID, &config).await;
    let after = arena_chunks(&file(&path), ID).await;
    assert_eq!(after.iter().map(|c| c.entries).sum::<u32>(), 40);
    let closed = &before[..before.len() - 1];
    assert_eq!(
        &after[..closed.len()],
        closed,
        "closed chunks are unchanged"
    );
    assert!(after.len() > before.len(), "{after:?}");

    let fluree = file(&path);
    assert_eq!(rows(&fluree, ID, count).await, strings(&["c=40^^integer"]));
    assert_eq!(rows(&fluree, ID, first).await, found);
    assert_eq!(rows(&fluree, ID, &last(39)).await, found);

    // A full rebuild splits the same values the same way.
    rebuild_with(&fluree, ID, &config).await;
    let fluree = file(&path);
    assert_eq!(arena_chunks(&fluree, ID).await.len(), after.len());
    assert_eq!(rows(&fluree, ID, count).await, strings(&["c=40^^integer"]));

    fluree
        .collect_garbage(std::time::Duration::ZERO)
        .await
        .expect("garbage collection");
    let fluree = file(&path);
    assert_eq!(rows(&fluree, ID, count).await, strings(&["c=40^^integer"]));
    assert_eq!(rows(&fluree, ID, &last(39)).await, found);

    let mut archive = Vec::new();
    fluree
        .archive_ledger(ID, true, &mut archive)
        .await
        .expect("archive");
    let copy = fluree
        .restore_ledger("it/numbig-chunks-copy", &mut archive.as_slice())
        .await
        .expect("restore")
        .ledger_id;
    assert_eq!(
        rows(&fluree, &copy, count).await,
        strings(&["c=40^^integer"])
    );
    assert_eq!(rows(&fluree, &copy, &last(39)).await, found);
}

// ── A v4 index ───────────────────────────────────────────────────────

const V4_FIXTURE: &str = "tests/fixtures/numbig-subtypes-4.2.8/storage";

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// A writable copy of the v4 fixture.
fn v4_store() -> tempfile::TempDir {
    let dir = tempfile::TempDir::new().unwrap();
    copy_dir(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(V4_FIXTURE),
        dir.path(),
    );
    dir
}

/// A v4 index with no commit since it was built answers as v4.2.8 did (see
/// the fixture's README): a big integer reads back as `xsd:integer`, its two
/// subtypes are one fact, and a typed `xsd:unsignedLong` finds nothing. The
/// ledger's first v5 build, a full rebuild from commits, keeps each subtype.
#[tokio::test]
async fn a_v4_index_answers_as_v4_until_a_v5_build_keeps_the_subtypes() {
    const ID: &str = "v4/numbig:main";
    let dir = v4_store();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    assert!(
        !is_current_format(&root_bytes(&fluree, ID).await),
        "a v4 root"
    );

    let all = "SELECT ?s ?o WHERE { ?s ex:n ?o }";
    let typed = typed(MAX, "unsignedLong");
    let bare = "SELECT ?s WHERE { ?s ex:n 18446744073709551615 }";
    let distinct = "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ?p ?o }";
    assert_eq!(
        rows(&fluree, ID, all).await,
        strings(&[
            "o=1.5^^decimal s=d",
            "o=123456789012345678.5^^decimal s=d",
            "o=18446744073709551615^^integer s=s",
            "o=18446744073709551615^^integer s=t",
            "o=9223372036854775807^^unsignedLong s=i",
        ])
    );
    assert_eq!(rows(&fluree, ID, &typed).await, Vec::<String>::new());
    assert_eq!(rows(&fluree, ID, bare).await, strings(&["s=s", "s=t"]));
    assert_eq!(
        rows(&fluree, ID, distinct).await,
        strings(&["c=4^^integer"])
    );

    fluree
        .reindex(ID, ReindexOptions::default())
        .await
        .expect("first v5 build");
    let fluree = file(&path);
    assert!(
        is_current_format(&root_bytes(&fluree, ID).await),
        "a v5 root"
    );
    assert_eq!(
        rows(&fluree, ID, all).await,
        strings(&[
            "o=1.5^^decimal s=d",
            "o=123456789012345678.5^^decimal s=d",
            "o=18446744073709551615^^integer s=t",
            "o=18446744073709551615^^nonNegativeInteger s=s",
            "o=18446744073709551615^^unsignedLong s=s",
            "o=9223372036854775807^^unsignedLong s=i",
        ])
    );
    assert_eq!(rows(&fluree, ID, &typed).await, strings(&["s=s"]));
    assert_eq!(
        rows(&fluree, ID, distinct).await,
        strings(&["c=6^^integer"])
    );
    assert!(unknown_datatype_buckets(&fluree, ID).await.is_empty());
}

/// A v4 index serves novelty written over it by a v5 binary as v4 did: the
/// subtypes of a big integer are one fact, so deleting the value under one
/// of them deletes it.
#[tokio::test]
async fn novelty_over_a_v4_index_merges_big_integer_subtypes() {
    const ID: &str = "v4/numbig:main";
    let dir = v4_store();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    update(
        &fluree,
        ID,
        r#"DELETE DATA { ex:s ex:n "18446744073709551615"^^xsd:unsignedLong }"#,
    )
    .await;
    let fluree = file(&path);
    assert!(
        !is_current_format(&root_bytes(&fluree, ID).await),
        "a v4 root"
    );
    assert_eq!(
        rows(&fluree, ID, "SELECT ?o WHERE { ex:s ex:n ?o }").await,
        Vec::<String>::new()
    );
}

/// A datatype first written after the base index keeps its type through an
/// incremental build: the build used to resolve it against the base root's
/// datatype list alone, so an `xsd:int` read back as a custom datatype (and
/// failed to decode), and two new subtypes of one big integer became one
/// fact.
#[tokio::test]
async fn a_datatype_first_written_after_the_base_index_keeps_its_type() {
    const ID: &str = "it/numbig-new-datatype:main";
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    fluree.create_ledger(ID).await.unwrap();
    update(&fluree, ID, r#"INSERT DATA { ex:s ex:p "1"^^xsd:integer }"#).await;
    rebuild_with(&fluree, ID, &IndexerConfig::default()).await;
    update(
        &file(&path),
        ID,
        r#"INSERT DATA { ex:s ex:p "5"^^xsd:int , "7"^^xsd:positiveInteger ,
             "18446744073709551615"^^xsd:unsignedLong ,
             "18446744073709551615"^^xsd:nonNegativeInteger }"#,
    )
    .await;
    let q = "SELECT ?o WHERE { ex:s ex:p ?o }";
    let want = strings(&[
        "o=18446744073709551615^^nonNegativeInteger",
        "o=18446744073709551615^^unsignedLong",
        "o=1^^integer",
        "o=5^^int",
        "o=7^^positiveInteger",
    ]);
    assert_eq!(rows(&file(&path), ID, q).await, want, "novelty");
    incremental_with(&file(&path), ID, &IndexerConfig::default()).await;
    assert_eq!(rows(&file(&path), ID, q).await, want, "incremental");
    rebuild_with(&file(&path), ID, &IndexerConfig::default()).await;
    assert_eq!(rows(&file(&path), ID, q).await, want, "rebuilt");
}

// ── Commits staged by value ──────────────────────────────────────────

/// Rows of `q` through the ledger's cached state, at `t` when given: the
/// state a running instance answers from, novelty applied commit by commit.
async fn cached_rows(fluree: &Fluree, ledger_id: &str, q: &str, t: Option<i64>) -> Vec<String> {
    let view = match t {
        Some(t) => fluree.graph_at(ledger_id, fluree_db_api::TimeSpec::AtT(t)),
        None => fluree.graph(ledger_id),
    };
    let json = view
        .query()
        .sparql(&format!("{PFX}{q}"))
        .format(fluree_db_api::FormatterConfig::sparql_json())
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{q} at {t:?}: {e}"));
    let mut out: Vec<String> = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| {
            let v = &b["o"];
            let value = v["value"].as_str().unwrap_or_default();
            match v.get("datatype").and_then(|d| d.as_str()) {
                Some(dt) => format!("o={value}^^{}", dt.trim_start_matches(XSD)),
                None => format!("o={value}"),
            }
        })
        .collect();
    out.sort();
    out
}

/// The commit at `ledger_id`'s head.
async fn head_commit(fluree: &Fluree, ledger_id: &str) -> fluree_db_core::Commit {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .unwrap()
        .unwrap();
    fluree_db_core::load_commit_by_id(
        fluree.content_store(&record.storage_namespace()).as_ref(),
        record.commit_head_id.as_ref().expect("a head commit"),
    )
    .await
    .expect("head commit")
}

/// The v4 fixture's replacement of a big `xsd:unsignedLong` by `5` stays a
/// replacement through the ledger's first v5 build. Its commit retracts the
/// value as `xsd:integer`, the type v4 read it back with, and is staged by
/// value, so the retraction stands for the stored subtype.
#[tokio::test]
async fn a_v4_replacement_stays_replaced_through_its_first_v5_build() {
    const ID: &str = "v4/numbig-changed:main";
    let dir = v4_store();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    let q = "SELECT ?o WHERE { ex:a ex:n ?o }";
    let head = head_commit(&fluree, ID).await;
    assert_eq!(head.num_big_typing, fluree_db_core::NumBigTyping::ByValue);
    assert!(head.retracts_big_integers_by_value());
    assert_eq!(rows(&fluree, ID, q).await, strings(&["o=5^^integer"]));
    assert_eq!(commit_retracts(&fluree, ID, head.t).await, vec![1]);

    fluree
        .reindex(ID, ReindexOptions::default())
        .await
        .expect("first v5 build");
    let fluree = file(&path);
    assert!(is_current_format(&root_bytes(&fluree, ID).await));
    assert_eq!(rows(&fluree, ID, q).await, strings(&["o=5^^integer"]));
    assert_eq!(
        cached_rows(&fluree, ID, q, Some(head.t - 1)).await,
        strings(&["o=18446744073709551615^^unsignedLong"]),
        "before the replacement, the value as written"
    );
    // The commit retracted one fact: the rebuild's retraction of the
    // subtype it stood for is the index's, not the commit's.
    assert_eq!(commit_retracts(&fluree, ID, head.t).await, vec![1]);
}

/// The `db:retracts` the commit at `t` reports in the ledger's txn-meta graph.
async fn commit_retracts(fluree: &Fluree, ledger_id: &str, t: i64) -> Vec<i64> {
    let q = serde_json::json!({
        "from": format!("{ledger_id}#txn-meta"),
        "select": ["?r"],
        "where": {
            "@id": "?c",
            "https://ns.flur.ee/db#t": t,
            "https://ns.flur.ee/db#retracts": "?r"
        }
    });
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let out = fluree
        .query_connection(&q)
        .await
        .expect("txn-meta query")
        .to_jsonld(&ledger.snapshot)
        .expect("JSON-LD");
    out.as_array()
        .expect("rows")
        .iter()
        .map(|row| {
            let v = row.as_array().map_or(row, |r| &r[0]);
            v.as_i64()
                .or_else(|| v.get("@value").and_then(serde_json::Value::as_i64))
                .unwrap_or_else(|| panic!("db:retracts: {v}"))
        })
        .collect()
}

/// A delete this binary writes over a v4 index is staged by value: that
/// index holds the value's subtypes as one fact, so deleting it under one
/// deletes them all, and the commit says so. The ledger's first v5 build
/// keeps the delete, for both subtypes, and leaves another subject's term
/// of the value alone.
#[tokio::test]
async fn a_delete_over_a_v4_index_stays_deleted_through_its_first_v5_build() {
    const ID: &str = "v4/numbig:main";
    let dir = v4_store();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    update(
        &fluree,
        ID,
        r#"DELETE DATA { ex:s ex:n "18446744073709551615"^^xsd:unsignedLong }"#,
    )
    .await;
    let head = head_commit(&fluree, ID).await;
    assert_eq!(head.num_big_typing, fluree_db_core::NumBigTyping::ByValue);
    let s = "SELECT ?o WHERE { ex:s ex:n ?o }";
    let t = "SELECT ?o WHERE { ex:t ex:n ?o }";
    assert_eq!(rows(&file(&path), ID, s).await, Vec::<String>::new());

    fluree
        .reindex(ID, ReindexOptions::default())
        .await
        .expect("first v5 build");
    let fluree = file(&path);
    assert_eq!(rows(&fluree, ID, s).await, Vec::<String>::new(), "rebuilt");
    assert_eq!(
        rows(&fluree, ID, t).await,
        strings(&["o=18446744073709551615^^integer"])
    );
    assert_eq!(
        cached_rows(&fluree, ID, s, Some(head.t - 1)).await,
        strings(&[
            "o=18446744073709551615^^nonNegativeInteger",
            "o=18446744073709551615^^unsignedLong",
        ]),
        "before the delete, both subtypes"
    );
    // A write over the v5 index names one term.
    update(&fluree, ID, "INSERT DATA { ex:u ex:n 1 }").await;
    assert_eq!(
        head_commit(&fluree, ID).await.num_big_typing,
        fluree_db_core::NumBigTyping::Declared
    );
}

/// Over a v4 index, novelty keeps a value that index doesn't hold under each
/// subtype. Deleting it under one of them still deletes every one, as v4.2.8
/// did, so the ledger's first v5 build, which reads the commit by value,
/// answers as the v4 index did.
#[tokio::test]
async fn a_delete_over_a_v4_index_of_a_value_written_since_deletes_every_subtype() {
    const ID: &str = "v4/numbig:main";
    const V: &str = "18446744073709551613";
    let dir = v4_store();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    update(
        &fluree,
        ID,
        &format!(r#"INSERT DATA {{ ex:y ex:n "{V}"^^xsd:unsignedLong , "{V}"^^xsd:integer }}"#),
    )
    .await;
    update(
        &fluree,
        ID,
        &format!(r#"DELETE DATA {{ ex:y ex:n "{V}"^^xsd:unsignedLong }}"#),
    )
    .await;
    let head = head_commit(&fluree, ID).await;
    assert_eq!(head.num_big_typing, fluree_db_core::NumBigTyping::ByValue);
    assert_eq!(commit_retracts(&fluree, ID, head.t).await, [2]);
    let y = "SELECT ?o WHERE { ex:y ex:n ?o }";
    assert_eq!(
        rows(&file(&path), ID, y).await,
        Vec::<String>::new(),
        "over the v4 index"
    );

    fluree
        .reindex(ID, ReindexOptions::default())
        .await
        .expect("first v5 build");
    assert_eq!(
        rows(&file(&path), ID, y).await,
        Vec::<String>::new(),
        "rebuilt"
    );
}

/// The v4 fixture's commits, oldest first: a big `xsd:unsignedLong`, then
/// its replacement by `5`, staged by value.
async fn v4_replacement_commits() -> Vec<fluree_db_api::Base64Bytes> {
    let dir = v4_store();
    let fluree = file(&dir.path().to_string_lossy());
    let handle = fluree
        .ledger_cached("v4/numbig-changed:main")
        .await
        .expect("fixture");
    let mut commits = fluree
        .export_commit_range(
            &handle,
            &fluree_db_api::ExportCommitsRequest {
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
    assert_eq!(commits.len(), 2);
    commits
}

async fn push(fluree: &Fluree, ledger_id: &str, commit: fluree_db_api::Base64Bytes) {
    let request = fluree_db_api::PushCommitsRequest {
        commits: vec![commit],
        ..Default::default()
    };
    let accepted = fluree
        .push_commits(
            ledger_id,
            request,
            &fluree_db_api::GovernanceOptions::default(),
            &fluree_db_api::IndexConfig {
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 2_000_000_000,
            },
        )
        .await
        .expect("push")
        .accepted;
    assert_eq!(accepted, 1);
}

/// `writes`, committed on `fluree`'s `ledger_id`, as pushed bytes oldest
/// first, the last rewritten as v4.2.8 writes a delete of a big integer: by
/// value, naming its first retraction of one under `xsd:integer`, the type
/// that release's index read it back with.
async fn as_v4_delete(
    fluree: &Fluree,
    ledger_id: &str,
    writes: &[String],
) -> Vec<fluree_db_api::Base64Bytes> {
    for w in writes {
        update(fluree, ledger_id, w).await;
    }
    let handle = fluree.ledger_cached(ledger_id).await.expect("source");
    let mut commits = fluree
        .export_commit_range(
            &handle,
            &fluree_db_api::ExportCommitsRequest {
                cursor: None,
                cursor_id: None,
                limit: Some(writes.len()),
                ..Default::default()
            },
        )
        .await
        .expect("export")
        .commits;
    commits.reverse();
    let last = commits.pop().expect("the delete");
    let mut commit = fluree_db_core::commit::codec::read_commit(&last.0).expect("decode");
    let mut deleted = commit
        .flakes
        .iter()
        .find(|f| !f.op && f.o.is_big_integer())
        .cloned()
        .expect("a retraction of a big integer");
    deleted.dt = fluree_db_core::Sid::new(fluree_vocab::namespaces::XSD, "integer");
    commit.flakes = vec![deleted];
    commit.num_big_typing = fluree_db_core::NumBigTyping::ByValue;
    commit.id = None;
    commits.push(fluree_db_api::Base64Bytes(
        fluree_db_core::commit::codec::write_commit(&commit, true, None)
            .expect("encode")
            .bytes,
    ));
    commits
}

/// A v4 client's delete of a big integer, pushed into a ledger whose v4
/// index predates the value: novelty holds the value under each subtype it
/// was written with, as separate terms, and the delete, which names it as
/// `xsd:integer`, deletes it under every one of them, whether or not
/// `xsd:integer` is among them. It does so live, reloaded and after the
/// ledger's first v5 build alike.
#[tokio::test]
async fn a_v4_delete_pushed_over_a_v4_index_deletes_every_subtype_written_since() {
    const ID: &str = "v4/numbig:main";
    const V: &str = "18446744073709551613";
    for subtypes in [&["unsignedLong", "integer"][..], &["unsignedLong"][..]] {
        let terms: Vec<String> = subtypes
            .iter()
            .map(|dt| format!(r#""{V}"^^xsd:{dt}"#))
            .collect();
        let writes = [
            format!("INSERT DATA {{ ex:y ex:n {} }}", terms.join(" , ")),
            format!("DELETE DATA {{ ex:y ex:n {} }}", terms[0]),
        ];
        let source = v4_store();
        let commits = as_v4_delete(&file(&source.path().to_string_lossy()), ID, &writes).await;
        let dir = v4_store();
        let path = dir.path().to_string_lossy().to_string();
        let fluree = file(&path);
        for commit in commits {
            push(&fluree, ID, commit).await;
        }
        let y = "SELECT ?o WHERE { ex:y ex:n ?o }";
        let none = Vec::<String>::new();
        assert_eq!(
            cached_rows(&fluree, ID, y, None).await,
            none,
            "{subtypes:?} live"
        );
        assert_eq!(
            rows(&file(&path), ID, y).await,
            none,
            "{subtypes:?} reloaded"
        );
        file(&path)
            .reindex(ID, ReindexOptions::default())
            .await
            .expect("first v5 build");
        assert_eq!(
            rows(&file(&path), ID, y).await,
            none,
            "{subtypes:?} rebuilt"
        );
    }
}

/// A reifier's attachment as v4.2.8 commits held it, slot by slot, its
/// object a big `xsd:unsignedLong`; then a v4 client's delete of the object
/// slot, which names the value as `xsd:integer`. Pushed into a ledger whose
/// index holds the attachment, the delete removes the reifier's
/// `rdf:reifies` link: live, reloaded and after a full rebuild.
#[tokio::test]
async fn a_v4_delete_of_a_reified_big_integer_removes_its_link_on_every_path() {
    use fluree_db_core::namespaces::{
        reifies_object_sid, reifies_predicate_sid, reifies_subject_sid,
    };
    use fluree_db_core::{Flake, FlakeValue, NumBigTyping};
    const ID: &str = "it/numbig-link:main";

    // Two commits with this ledger's envelopes, rewritten as v4 wrote them.
    let triple = format!(r#"ex:claim rdf:reifies <<( ex:s ex:n "{MAX}"^^xsd:unsignedLong )>>"#);
    let source_dir = tempfile::TempDir::new().unwrap();
    let source = file(&source_dir.path().to_string_lossy());
    source.create_ledger(ID).await.unwrap();
    update(&source, ID, &format!("INSERT DATA {{ {triple} }}")).await;
    update(&source, ID, &format!("DELETE DATA {{ {triple} }}")).await;
    let handle = source.ledger_cached(ID).await.expect("source");
    let mut exported = source
        .export_commit_range(&handle, &fluree_db_api::ExportCommitsRequest::default())
        .await
        .expect("export")
        .commits;
    exported.reverse();
    let mut commits: Vec<fluree_db_api::Base64Bytes> = Vec::new();
    let mut parent: Option<fluree_db_core::ContentId> = None;
    for bytes in &exported {
        let mut commit = fluree_db_core::commit::codec::read_commit(&bytes.0).unwrap();
        let link = commit.flakes[0].clone();
        let FlakeValue::TripleTerm(term) = &link.o else {
            panic!("a link: {link:?}");
        };
        let slot = |p: &fluree_db_core::Sid, o: FlakeValue, dt| {
            Flake::new(link.s.clone(), p.clone(), o, dt, link.t, link.op, None)
        };
        let object = slot(reifies_object_sid(), term.o.clone(), term.dt.clone());
        commit.flakes = if link.op {
            let id = fluree_db_core::edge::id_datatype_sid();
            vec![
                slot(
                    reifies_subject_sid(),
                    FlakeValue::Ref(term.s.clone()),
                    id.clone(),
                ),
                slot(reifies_predicate_sid(), FlakeValue::Ref(term.p.clone()), id),
                object,
            ]
        } else {
            let integer = fluree_db_core::Sid::new(fluree_vocab::namespaces::XSD, "integer");
            vec![Flake {
                dt: integer,
                ..object
            }]
        };
        commit.num_big_typing = NumBigTyping::ByValue;
        commit.id = None;
        if let Some(parent) = parent.take() {
            commit.parents = vec![parent];
        }
        let written = fluree_db_core::commit::codec::write_commit(&commit, true, None)
            .unwrap()
            .bytes;
        parent = Some(fluree_db_core::ContentId::new(
            fluree_db_core::ContentKind::Commit,
            &written,
        ));
        commits.push(fluree_db_api::Base64Bytes(written));
    }

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    fluree.create_ledger(ID).await.unwrap();
    let mut commits = commits.into_iter();
    push(&fluree, ID, commits.next().unwrap()).await;
    fluree
        .reindex(ID, ReindexOptions::default())
        .await
        .expect("an index holding the attachment");
    let fluree = file(&path);
    let link = "SELECT ?t WHERE { ex:claim rdf:reifies ?t }";
    assert_eq!(rows(&fluree, ID, link).await.len(), 1, "linked");
    push(&fluree, ID, commits.next().unwrap()).await;

    let none = Vec::<String>::new();
    assert_eq!(cached_rows(&fluree, ID, link, None).await, none, "live");
    assert_eq!(rows(&file(&path), ID, link).await, none, "reloaded");
    file(&path)
        .reindex(ID, ReindexOptions::default())
        .await
        .expect("full rebuild");
    assert_eq!(rows(&file(&path), ID, link).await, none, "rebuilt");
}

/// A v4 commit pushed into a ledger whose index keeps subtypes retracts the
/// stored subtype it does not name, on every path: live, reloaded, on
/// another instance catching up, at its `t` once it is history, and through
/// the next build, which is a full one (an incremental build refuses a
/// window that retracts by value). Before it, the value reads as written.
#[tokio::test]
async fn a_v4_commit_pushed_into_a_v5_ledger_retracts_the_stored_subtype() {
    const ID: &str = "it/numbig-pushed-v4:main";
    let mut commits = v4_replacement_commits().await.into_iter();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    fluree.create_ledger(ID).await.unwrap();
    push(&fluree, ID, commits.next().unwrap()).await;
    fluree
        .reindex(ID, ReindexOptions::default())
        .await
        .expect("a v5 index of the value");
    let q = "SELECT ?o WHERE { ex:a ex:n ?o }";
    let written = strings(&["o=18446744073709551615^^unsignedLong"]);
    let replaced = strings(&["o=5^^integer"]);
    assert_eq!(cached_rows(&fluree, ID, q, None).await, written);

    // Another instance on the same storage holds the ledger as it was.
    let peer = file(&path);
    assert_eq!(cached_rows(&peer, ID, q, None).await, written);

    push(&fluree, ID, commits.next().unwrap()).await;
    assert_eq!(cached_rows(&fluree, ID, q, None).await, replaced, "live");
    assert_eq!(rows(&fluree, ID, q).await, replaced, "reloaded");
    peer.refresh(ID, fluree_db_api::RefreshOpts::default())
        .await
        .expect("catch up");
    assert_eq!(cached_rows(&peer, ID, q, None).await, replaced, "caught up");

    update(&fluree, ID, "INSERT DATA { ex:b ex:n 1 }").await;
    let fluree = file(&path);
    assert_eq!(cached_rows(&fluree, ID, q, Some(2)).await, replaced, "at t");
    assert_eq!(
        cached_rows(&fluree, ID, q, Some(1)).await,
        written,
        "before"
    );

    let record = fluree.nameservice().lookup(ID).await.unwrap().unwrap();
    let incremental = fluree_db_indexer::build_incremental_index_for_record(
        fluree.branched_content_store(ID).await.unwrap(),
        &record,
        IndexerConfig::default(),
    )
    .await;
    assert!(
        matches!(
            incremental,
            Err(fluree_db_indexer::IndexerError::IncrementalAbort(_))
        ),
        "an incremental build refuses the window"
    );
    support::build_and_publish_index(&fluree, ID).await;
    let fluree = file(&path);
    assert_eq!(rows(&fluree, ID, q).await, replaced, "next build");
    assert_eq!(cached_rows(&fluree, ID, q, Some(1)).await, written);
}

/// A delete written over a v4 index, still in novelty when the ledger's
/// first v5 index is installed under it, retracts every subtype the new
/// index keeps: the instance holding the ledger works out what the commit
/// retracts again when the index changes how it types big integers.
#[tokio::test]
async fn a_v5_index_installed_under_a_v4_delete_keeps_it_deleted() {
    const ID: &str = "v4/numbig:main";
    let dir = v4_store();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    let s = "SELECT ?o WHERE { ex:s ex:n ?o }";
    let before = fluree.nameservice().lookup(ID).await.unwrap().unwrap();
    update(
        &fluree,
        ID,
        r#"DELETE DATA { ex:s ex:n "18446744073709551615"^^xsd:unsignedLong }"#,
    )
    .await;
    assert_eq!(
        cached_rows(&fluree, ID, s, None).await,
        Vec::<String>::new()
    );

    // A v5 index of the commits before the delete, as a build begun before
    // it would publish.
    let result = fluree_db_indexer::rebuild_index_from_commits(
        fluree.branched_content_store(ID).await.unwrap(),
        ID,
        &before,
        IndexerConfig::default(),
    )
    .await
    .expect("a v5 build up to the delete");
    assert!(result.index_t < head_commit(&fluree, ID).await.t);
    support::publish_built_index(&fluree, ID, result.index_t, &result.root_id).await;
    fluree
        .refresh(ID, fluree_db_api::RefreshOpts::default())
        .await
        .expect("install the v5 index");
    assert!(is_current_format(&root_bytes(&fluree, ID).await));
    assert_eq!(
        cached_rows(&fluree, ID, s, None).await,
        Vec::<String>::new(),
        "the installed index"
    );
    assert_eq!(
        rows(&file(&path), ID, s).await,
        Vec::<String>::new(),
        "reloaded"
    );
}

/// Whether `result` is the refusal of a branch operation over a line whose
/// index, an earlier release's, cannot say which subtype a commit deleted.
fn refused_until_reindexed<T: std::fmt::Debug>(result: fluree_db_api::Result<T>) -> bool {
    matches!(
        &result,
        Err(fluree_db_api::ApiError::Http { status: 409, message }) if message.contains("reindex")
    )
}

async fn revert_head(fluree: &Fluree, ledger_id: &str) -> fluree_db_api::Result<()> {
    let head = head_commit(fluree, ledger_id).await.id.expect("head id");
    let (name, branch) = ledger_id.split_once(':').expect("name:branch");
    fluree
        .revert_commits(
            name,
            branch,
            vec![fluree_db_api::CommitRef::Exact(head)],
            fluree_db_api::ConflictStrategy::TakeSource,
        )
        .await
        .map(|_| ())
}

/// Reverting the v4 fixture's replacement of a big `xsd:unsignedLong` by
/// `5` brings the `xsd:unsignedLong` back: the commit retracted it by value,
/// as `xsd:integer`, and undoing it undoes what it deleted. Over the v4
/// index, which keeps no subtype, the revert is refused until a reindex.
#[tokio::test]
async fn reverting_a_v4_replacement_restores_the_subtype_it_replaced() {
    const ID: &str = "v4/numbig-changed:main";
    let dir = v4_store();
    let path = dir.path().to_string_lossy().to_string();
    let q = "SELECT ?o WHERE { ex:a ex:n ?o }";
    let written = strings(&["o=18446744073709551615^^unsignedLong"]);
    assert!(refused_until_reindexed(revert_head(&file(&path), ID).await));
    file(&path)
        .reindex(ID, ReindexOptions::default())
        .await
        .expect("first v5 build");
    revert_head(&file(&path), ID).await.expect("revert");
    assert_eq!(rows(&file(&path), ID, q).await, written, "reverted");
    rebuild_with(&file(&path), ID, &IndexerConfig::default()).await;
    assert_eq!(rows(&file(&path), ID, q).await, written, "rebuilt");
}

/// Reverting a delete written over a v4 index restores every subtype the
/// value was held under, as one fact there and two terms since.
#[tokio::test]
async fn reverting_a_v4_delete_restores_every_subtype() {
    const ID: &str = "v4/numbig:main";
    let dir = v4_store();
    let path = dir.path().to_string_lossy().to_string();
    let q = "SELECT ?o WHERE { ex:s ex:n ?o }";
    update(&file(&path), ID, "DELETE WHERE { ex:s ex:n ?o }").await;
    assert_eq!(rows(&file(&path), ID, q).await, Vec::<String>::new());
    file(&path)
        .reindex(ID, ReindexOptions::default())
        .await
        .expect("first v5 build");
    revert_head(&file(&path), ID).await.expect("revert");
    let both = strings(&[
        "o=18446744073709551615^^nonNegativeInteger",
        "o=18446744073709551615^^unsignedLong",
    ]);
    assert_eq!(rows(&file(&path), ID, q).await, both, "reverted");
    rebuild_with(&file(&path), ID, &IndexerConfig::default()).await;
    assert_eq!(rows(&file(&path), ID, q).await, both, "rebuilt");
}

/// A preview of merging a branch into one with no commits nets the
/// branch's whole history, reading a commit staged by value as the target,
/// which adopts the commits, will: the v4 fixture's replacement of a big
/// `xsd:unsignedLong` by `5` nets to `5` and the retraction of the
/// `xsd:unsignedLong` (the newest op on each fact), not to the
/// `xsd:unsignedLong` held and an `xsd:integer` retracted.
#[tokio::test]
async fn a_merge_preview_into_an_empty_branch_reads_a_v4_replacement_by_value() {
    const NAME: &str = "it/numbig-preview";
    const MAIN: &str = "it/numbig-preview:main";
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    fluree.create_ledger(MAIN).await.unwrap();
    for commit in v4_replacement_commits().await {
        push(&fluree, MAIN, commit).await;
    }
    fluree
        .create_branch(NAME, "dev", None, None)
        .await
        .expect("branch");
    // `main` back to no commits, so `dev` holds the whole history.
    let main = fluree
        .nameservice()
        .lookup(MAIN)
        .await
        .unwrap()
        .expect("main");
    fluree
        .branch_admin()
        .unwrap()
        .reset_head_fenced(
            MAIN,
            fluree_db_nameservice::WriteBasis::fenced(main.fence),
            fluree_db_nameservice::NsRecordSnapshot {
                commit_head_id: None,
                commit_t: 0,
                index_head_id: None,
                index_t: 0,
            },
        )
        .await
        .unwrap();

    let preview = file(&path)
        .merge_preview_with(
            NAME,
            "dev",
            Some("main"),
            fluree_db_api::MergePreviewOpts {
                include_changes: true,
                ..Default::default()
            },
        )
        .await
        .expect("preview");
    assert!(preview.ancestor.is_none(), "no commit on main");
    let changes = preview.changes.expect("changes");
    let net: Vec<String> = changes
        .entries
        .iter()
        .flat_map(|e| e.asserts.iter().chain(&e.retracts))
        .map(|f| {
            let dt = f.dt.rsplit('#').next().unwrap_or(&f.dt);
            format!("{}{dt}", if f.op { "+" } else { "-" })
        })
        .collect();
    assert_eq!(net, ["+integer", "-unsignedLong"]);
}

/// Merging a branch whose delete was written over a v4 index deletes every
/// subtype of the value on the target: the merge folds what the commit
/// deleted on the branch, not the `xsd:integer` it names, which the target
/// does not hold. While the branch's own index is the v4 one, which cannot
/// say what it deleted, the merge is refused.
#[tokio::test]
async fn merging_a_v4_delete_deletes_every_subtype() {
    const NAME: &str = "v4/numbig";
    const MAIN: &str = "v4/numbig:main";
    const DEV: &str = "v4/numbig:dev";
    let dir = v4_store();
    let path = dir.path().to_string_lossy().to_string();
    let fluree = file(&path);
    fluree
        .create_branch(NAME, "dev", Some("main"), None)
        .await
        .expect("branch");
    update(&fluree, DEV, "DELETE WHERE { ex:s ex:n ?o }").await;
    assert!(head_commit(&fluree, DEV)
        .await
        .retracts_big_integers_by_value());
    // The target moves on, so the merge folds the branch's commits.
    update(&fluree, MAIN, "INSERT DATA { ex:u ex:n 1 }").await;
    let merge = |fluree: Fluree| async move {
        fluree
            .merge_branch(
                NAME,
                "dev",
                Some("main"),
                fluree_db_api::ConflictStrategy::default(),
            )
            .await
            .map(|_| ())
    };
    assert!(refused_until_reindexed(merge(file(&path)).await));

    for id in [DEV, MAIN] {
        file(&path)
            .reindex(id, ReindexOptions::default())
            .await
            .expect("a v5 build");
    }
    merge(file(&path)).await.expect("merge");
    let s = "SELECT ?o WHERE { ex:s ex:n ?o }";
    assert_eq!(
        rows(&file(&path), MAIN, s).await,
        Vec::<String>::new(),
        "merged"
    );
    assert_eq!(
        rows(&file(&path), MAIN, "SELECT ?o WHERE { ex:u ex:n ?o }").await,
        strings(&["o=1^^integer"])
    );
    rebuild_with(&file(&path), MAIN, &IndexerConfig::default()).await;
    assert_eq!(
        rows(&file(&path), MAIN, s).await,
        Vec::<String>::new(),
        "rebuilt"
    );
}
