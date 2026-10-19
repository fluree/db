//! Instruction and allocation counts for the lanes that read several graphs
//! of one ledger, per query shape and scope, for an A/B between two builds.
//!
//! `cargo build -p fluree-db-api --release --example multigraph_lanes_ab`,
//! once at the base and once at the head, then run the two binaries in turn
//! (base, head, base, head) and compare. Use the release profile for parity
//! claims: the dev-fast profile splits each crate into 16 codegen units, and an
//! unrelated change can move an inlining decision across one and shift a
//! scenario by a few percent.
//!
//! - `MG_AB_ONLY=mg_cyclic,mgd_` (or arguments) selects scenarios whose name
//!   contains any of the comma-separated substrings.
//! - `AB_REPS` sets the measured repetitions (default 5); each prints its
//!   median, min and max per query.
//! - `AB_DIGEST=1` prints each scenario's row count and a digest of its sorted
//!   answer instead, to check that two builds agree.
//!
//! Scenarios are `<scope>_<shape>`. Shapes: `group` (A, partitioned GROUP BY),
//! `cyclic` (B, triangle), `exists` (C, EXISTS in an expression), `count_obj`
//! (D, bound-object count), `count_pred` (E, per-predicate count), `optional`.
//! Scopes over one ledger with 3000 subjects in each of four graphs:
//! - `sg`: the default graph g0, no dataset; `g1`: `FROM <urn:g1>`;
//! - `mg`: `FROM <urn:g1> FROM <urn:g2>`, where g2 repeats half of g1's
//!   subjects with identical triples (50% overlap);
//! - `mgd`: `FROM <urn:g1> FROM <urn:g3>`, where g3 shares no subject with g1;
//! - `mg3`: `FROM <ledger> FROM <urn:g1> FROM <urn:g2>`;
//! - `mgu`: the union default graph (`# PRAGMA union-default-graph`), all four.
//!
//! Instructions retired are read from the process (`proc_pid_rusage`) on
//! macOS; elsewhere the instruction columns report wall-clock nanoseconds per
//! query instead. Allocations are counted by a wrapping global allocator.
//! Fixture setup is not measured.

use fluree_db_api::{FlureeBuilder, GraphSnapshot, ReindexOptions};
use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

struct Counting;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

// SAFETY: every method forwards to `System` with the caller's arguments; the
// counters are relaxed atomics with no effect on allocation.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(layout.size() as u64, Relaxed);
        // SAFETY: forwarded unchanged.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(layout.size() as u64, Relaxed);
        // SAFETY: forwarded unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(new_size as u64, Relaxed);
        // SAFETY: forwarded unchanged.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// The process's instructions retired so far.
#[cfg(target_os = "macos")]
fn counter() -> u64 {
    // SAFETY: `rusage_info_v4` is plain data, zero is a valid value, and
    // `proc_pid_rusage` writes at most one `rusage_info_v4` through the pointer.
    let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V4,
            std::ptr::from_mut(&mut info).cast::<libc::rusage_info_t>(),
        )
    };
    assert_eq!(rc, 0, "proc_pid_rusage");
    info.ri_instructions
}

/// Nanoseconds since the first call: no portable instruction counter.
#[cfg(not(target_os = "macos"))]
fn counter() -> u64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    u64::try_from(
        START
            .get_or_init(std::time::Instant::now)
            .elapsed()
            .as_nanos(),
    )
    .unwrap_or(u64::MAX)
}

const COUNTER: &str = if cfg!(target_os = "macos") {
    "instr"
} else {
    "ns"
};

const N: usize = 3000;

/// One subject's triples: a group key, a kind, a name, every other one a
/// label and an email, one triangle edge, and one noise edge.
fn subject(ttl: &mut String, prefix: &str, i: usize) {
    let tri_next = 3 * (i / 3) + (i % 3 + 1) % 3;
    let noise = (i * 7 + 11) % N;
    let _ = writeln!(
        ttl,
        "ex:{prefix}{i} ex:n {} ; ex:kind ex:T{} ; ex:name \"n{i}\" ; \
         ex:knows ex:{prefix}{tri_next} , ex:{prefix}{noise} .",
        i % 20,
        i % 4
    );
    if i.is_multiple_of(2) {
        let _ = writeln!(
            ttl,
            "ex:{prefix}{i} ex:label \"l{i}\" ; ex:email \"e{i}\" ."
        );
    }
}

/// g0 (`d…`), g1 (`s0…s2999`), g2 (`s1500…s2999` again, then `t0…t1499`),
/// g3 (`u0…u2999`).
fn fixture_trig() -> String {
    let mut trig = String::with_capacity(N * 4 * 200);
    trig.push_str("@prefix ex: <http://example.org/> .\n");
    for i in 0..N {
        subject(&mut trig, "d", i);
    }
    trig.push_str("GRAPH <urn:g1> {\n");
    for i in 0..N {
        subject(&mut trig, "s", i);
    }
    trig.push_str("}\nGRAPH <urn:g2> {\n");
    for i in N / 2..N {
        subject(&mut trig, "s", i);
    }
    for i in 0..N / 2 {
        subject(&mut trig, "t", i);
    }
    trig.push_str("}\nGRAPH <urn:g3> {\n");
    for i in 0..N {
        subject(&mut trig, "u", i);
    }
    trig.push_str("}\n");
    trig
}

const LEDGER: &str = "ab/multigraph-lanes:main";
const PREFIXES: &str = "PREFIX ex: <http://example.org/>\n";

/// (name, query with `{ds}` for the dataset clause, iterations per repetition)
const SHAPES: &[(&str, &str, usize)] = &[
    (
        "group",
        "SELECT ?v (COUNT(*) AS ?k) {ds} WHERE { ?s ex:n ?v } GROUP BY ?v",
        40,
    ),
    (
        "cyclic",
        "SELECT ?x ?y ?z {ds} WHERE { ?x ex:knows ?y . ?y ex:knows ?z . ?z ex:knows ?x }",
        10,
    ),
    (
        "exists",
        "SELECT ?x {ds} WHERE { ?x ex:n ?v FILTER(EXISTS { ?x ex:label ?l } && ?v > 0) }",
        20,
    ),
    (
        "count_obj",
        "SELECT (COUNT(?s) AS ?c) {ds} WHERE { ?s ex:kind ex:T1 }",
        200,
    ),
    (
        "count_pred",
        "SELECT ?p (COUNT(?s) AS ?c) {ds} WHERE { ?s ?p ?o } GROUP BY ?p",
        20,
    ),
    (
        "optional",
        "SELECT ?s ?e {ds} WHERE { ?s ex:name ?n OPTIONAL { ?s ex:email ?e } }",
        20,
    ),
];

/// (scope, prologue, dataset clause)
const SCOPES: &[(&str, &str, &str)] = &[
    ("sg", "", ""),
    ("g1", "", "FROM <urn:g1>"),
    ("mg", "", "FROM <urn:g1> FROM <urn:g2>"),
    ("mgd", "", "FROM <urn:g1> FROM <urn:g3>"),
    (
        "mg3",
        "",
        "FROM <ab/multigraph-lanes:main> FROM <urn:g1> FROM <urn:g2>",
    ),
    ("mgu", "# PRAGMA union-default-graph: true\n", ""),
];

fn main() {
    let mut filters: Vec<String> = std::env::args().skip(1).collect();
    if let Ok(only) = std::env::var("MG_AB_ONLY") {
        filters.extend(
            only.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from),
        );
    }
    let wanted =
        |name: &str| filters.is_empty() || filters.iter().any(|f| name.contains(f.as_str()));
    let reps: usize = std::env::var("AB_REPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
        .max(1);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    // The connection spawns its workers on the runtime it is built in.
    let _runtime = rt.enter();

    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    let snapshot: GraphSnapshot<'_> = rt.block_on(async {
        fluree.create_ledger(LEDGER).await.expect("create ledger");
        let handle = fluree.ledger_cached(LEDGER).await.expect("ledger");
        fluree
            .stage(&handle)
            .upsert_turtle(&fixture_trig())
            .execute()
            .await
            .expect("seed");
        fluree
            .reindex(LEDGER, ReindexOptions::default())
            .await
            .expect("index");
        fluree.disconnect_ledger(LEDGER).await;
        fluree.graph(LEDGER).load().await.expect("load")
    });

    let scenarios: Vec<(String, String, usize)> = SCOPES
        .iter()
        .flat_map(|(scope, prologue, ds)| {
            SHAPES.iter().map(move |(shape, body, iters)| {
                let query = format!("{prologue}{PREFIXES}{}", body.replace("{ds}", ds));
                (format!("{scope}_{shape}"), query, *iters)
            })
        })
        .filter(|(name, _, _)| wanted(name))
        .collect();
    assert!(!scenarios.is_empty(), "no scenario matches {filters:?}");

    if std::env::var_os("AB_DIGEST").is_some() {
        use std::hash::{Hash, Hasher};
        println!("scenario\trows\tdigest");
        for (name, query, _) in &scenarios {
            let json = rt
                .block_on(async { snapshot.query().sparql(query).execute_formatted().await })
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            let mut rows: Vec<String> = json["results"]["bindings"]
                .as_array()
                .map(|a| a.iter().map(ToString::to_string).collect())
                .unwrap_or_default();
            rows.sort();
            let mut h = std::collections::hash_map::DefaultHasher::new();
            rows.hash(&mut h);
            println!("{name}\t{}\t{:016x}", rows.len(), h.finish());
        }
        return;
    }

    println!(
        "scenario\titers\t{COUNTER}_per_query_median\t{COUNTER}_min\t{COUNTER}_max\tallocs_per_query\tbytes_per_query\trows"
    );
    for (name, query, iters) in &scenarios {
        let run = || -> usize {
            rt.block_on(async {
                let result = snapshot
                    .query()
                    .sparql(query)
                    .execute()
                    .await
                    .unwrap_or_else(|e| panic!("{name}: {e}"));
                result.row_count()
            })
        };
        let rows = run();
        for _ in 0..5 {
            std::hint::black_box(run());
        }
        let mut per_query: Vec<f64> = Vec::with_capacity(reps);
        let (mut allocs, mut bytes) = (0u64, 0u64);
        for _ in 0..reps {
            let (a0, b0, c0) = (ALLOCS.load(Relaxed), BYTES.load(Relaxed), counter());
            for _ in 0..*iters {
                std::hint::black_box(run());
            }
            let c1 = counter();
            allocs += ALLOCS.load(Relaxed) - a0;
            bytes += BYTES.load(Relaxed) - b0;
            per_query.push((c1 - c0) as f64 / *iters as f64);
        }
        per_query.sort_by(f64::total_cmp);
        let n = (reps * iters) as f64;
        println!(
            "{name}\t{iters}\t{:.0}\t{:.0}\t{:.0}\t{:.1}\t{:.0}\t{rows}",
            per_query[reps / 2],
            per_query[0],
            per_query[reps - 1],
            allocs as f64 / n,
            bytes as f64 / n,
        );
    }
}
