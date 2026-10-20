//! Encrypted-storage read A/B harness.
//!
//! `import` generates a synthetic people graph (many leaves, many dictionary
//! packs) and bulk-imports it into a file store, plain or encrypted. `query`
//! opens that store in a fresh process and runs read workloads under three
//! cache states, writing one JSON line per query execution and one per state
//! snapshot (cache occupancy by entry type, decrypt work, RSS split into
//! anonymous and file-backed).
//!
//! ```bash
//! cargo run --release -p fluree-db-api --example encryption_ab -- \
//!     import --dir /data/v2 --subjects 1500000 --encrypted
//! FLUREE_IO_STATS=1 cargo run --release -p fluree-db-api --example encryption_ab -- \
//!     query --dir /data/v2 --encrypted --variant v2 --out v2.jsonl --cache-mb 4096
//! ```
//!
//! On an encrypted store `--dictionary-mb` sets how much of `--cache-mb`
//! dictionary packs may hold decrypted whole. Builds without the counter API
//! (the baseline branch) compile with `--cfg no_counters`.

#![allow(unexpected_cfgs)]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value};

const KEY: [u8; 32] = [0x5c; 32];
const ALIAS: &str = "people";
const EX: &str = "http://example.org/";

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

/// A deterministic word for subject `i`, so values are unique and vary.
fn word(i: u64) -> String {
    const SYL: [&str; 16] = [
        "ka", "lo", "mi", "ne", "ru", "sa", "te", "vo", "zu", "pi", "go", "da", "fe", "hu", "ji",
        "wy",
    ];
    let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (0..4)
        .map(|_| {
            let s = SYL[(x & 15) as usize];
            x >>= 4;
            s
        })
        .collect()
}

fn knows(i: u64, n: u64) -> [u64; 2] {
    [
        (i.wrapping_mul(7919) + 13) % n,
        (i.wrapping_mul(104_729) + 71) % n,
    ]
}

/// Subject `i`'s local name. In an ordered dataset it is zero-padded and
/// leads its name and bio, so dictionary order — lexicographic — follows
/// `i`, and a range of subjects is a contiguous slice of each dictionary.
fn pid(i: u64, ordered: bool) -> String {
    if ordered {
        format!("{i:08}")
    } else {
        i.to_string()
    }
}

fn write_turtle(path: &Path, n: u64, ordered: bool) -> std::io::Result<()> {
    let mut out = std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(path)?);
    writeln!(out, "@prefix ex: <{EX}> .")?;
    writeln!(out, "@prefix p: <{EX}person/> .")?;
    for i in 0..n {
        let [a, b] = knows(i, n).map(|k| pid(k, ordered));
        let id = pid(i, ordered);
        let w = word(i);
        let lead = if ordered {
            format!("{id}: ")
        } else {
            String::new()
        };
        writeln!(
            out,
            "p:{id} a ex:Person ; ex:name \"Person {id} {w}\" ; ex:age {} ; \
             ex:city \"City {}\" ; ex:knows p:{a} , p:{b} ; \
             ex:bio \"{lead}{w} lives in city {} and was born in year {}; \
             profile {i} of the synthetic people graph for {w}.\" .",
            i * 7 % 100,
            i % 1000,
            i % 1000,
            1900 + i % 120,
        )?;
    }
    out.flush()
}

async fn open(
    dir: &Path,
    encrypted: bool,
    cache_mb: Option<usize>,
    dictionary_mb: Option<usize>,
) -> fluree_db_api::Fluree {
    let mut builder = FlureeBuilder::file(dir.to_string_lossy().to_string());
    if encrypted {
        builder = builder.with_encryption_key(KEY);
    }
    if let Some(mb) = cache_mb {
        builder = builder.cache_max_mb(mb);
    }
    #[cfg(not(no_counters))]
    if let Some(mb) = dictionary_mb {
        builder = builder.cache_dictionary_max_mb(mb);
    }
    #[cfg(no_counters)]
    let _ = dictionary_mb;
    builder
        .build_async()
        .await
        .expect("build file-backed Fluree")
}

async fn import(args: &[String]) {
    let dir = PathBuf::from(arg(args, "--dir").expect("--dir"));
    let n: u64 = arg(args, "--subjects").map_or(1_500_000, |v| v.parse().unwrap());
    let encrypted = flag(args, "--encrypted");
    let ordered = flag(args, "--ordered");
    std::fs::create_dir_all(&dir).unwrap();
    let ttl = dir.with_extension("ttl");
    let started = Instant::now();
    write_turtle(&ttl, n, ordered).unwrap();
    let generate_ms = started.elapsed().as_millis();

    let fluree = open(&dir.join("db"), encrypted, None, None).await;
    let started = Instant::now();
    fluree
        .create(ALIAS)
        .import(&ttl)
        .execute()
        .await
        .expect("import");
    let import_ms = started.elapsed().as_millis();
    std::fs::remove_file(&ttl).ok();

    let bytes: u64 = walk(&dir.join("db")).iter().map(|(_, len)| len).sum();
    let meta = json!({
        "subjects": n,
        "ordered": ordered,
        "encrypted": encrypted,
        "generate_ms": generate_ms,
        "import_ms": import_ms,
        "bytes_on_disk": bytes,
    });
    std::fs::write(dir.join("meta.json"), meta.to_string()).unwrap();
    println!("{meta}");
}

fn walk(root: &Path) -> Vec<(PathBuf, u64)> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.metadata() {
                Ok(m) if m.is_dir() => stack.push(path),
                Ok(m) => out.push((path, m.len())),
                Err(_) => {}
            }
        }
    }
    out
}

/// One workload: a named list of SPARQL queries run in order.
struct Workload {
    name: &'static str,
    queries: Vec<String>,
}

fn workloads(n: u64, ordered: bool) -> Vec<Workload> {
    let prefix = format!("PREFIX ex: <{EX}> PREFIX p: <{EX}person/> ");
    // Spread over the whole id space, seeded so every variant runs the same.
    let pick = |count: u64, salt: u64| -> Vec<u64> {
        (0..count)
            .map(|k| (k.wrapping_mul(2_654_435_761).wrapping_add(salt * 97)) % n)
            .collect()
    };
    let q = |body: String| format!("{prefix}{body}");
    vec![
        Workload {
            name: "point",
            queries: pick(200, 1)
                .into_iter()
                .map(|i| {
                    let i = pid(i, ordered);
                    q(format!("SELECT ?p ?o WHERE {{ p:{i} ?p ?o }}"))
                })
                .collect(),
        },
        Workload {
            name: "hop",
            queries: pick(200, 2)
                .into_iter()
                .map(|i| {
                    let i = pid(i, ordered);
                    q(format!(
                        "SELECT ?f ?n WHERE {{ p:{i} ex:knows ?f . ?f ex:name ?n }}"
                    ))
                })
                .collect(),
        },
        Workload {
            name: "dict_city",
            queries: pick(10, 3)
                .into_iter()
                .map(|i| {
                    q(format!(
                        "SELECT ?s ?n WHERE {{ ?s ex:city \"City {}\" ; ex:name ?n }}",
                        i % 1000
                    ))
                })
                .collect(),
        },
        Workload {
            name: "output_bio",
            queries: (0..3)
                .map(|a| {
                    q(format!(
                        "SELECT ?s ?b WHERE {{ ?s ex:age {} ; ex:bio ?b }}",
                        a * 31 % 100
                    ))
                })
                .collect(),
        },
        Workload {
            name: "count_meta",
            queries: vec![
                q("SELECT (COUNT(?s) AS ?c) WHERE { ?s ex:age ?a }".into()),
                q("SELECT (COUNT(DISTINCT ?c) AS ?d) WHERE { ?s ex:city ?c }".into()),
            ],
        },
        Workload {
            name: "scan_narrow",
            queries: vec![q(
                "SELECT (COUNT(?s) AS ?c) WHERE { ?s ex:age ?a FILTER(?a > 95) }".into(),
            )],
        },
        Workload {
            name: "scan_broad",
            queries: vec![q("SELECT (SUM(?a) AS ?t) WHERE { ?s ex:age ?a }".into())],
        },
        Workload {
            name: "mixed",
            queries: vec![
                q("SELECT (COUNT(*) AS ?n) WHERE { ?s ex:city ?c }".into()),
                q("SELECT ?s ?c WHERE { ?s ex:city ?c } LIMIT 2000".into()),
            ],
        },
        // Dictionary working sets, for `--plan`: names and bios of subjects
        // drawn from one fifth of the id space (a, b), or from all of it.
        Workload {
            name: "sub_a",
            queries: values_queries(&prefix, n / 10..n * 3 / 10, 11, ordered),
        },
        Workload {
            name: "sub_b",
            queries: values_queries(&prefix, n * 11 / 20..n * 15 / 20, 12, ordered),
        },
        Workload {
            name: "scatter",
            queries: values_queries(&prefix, 0..n, 13, ordered),
        },
        // As scatter, but drawing new subjects every pass (see `query`).
        Workload {
            name: "scatter_drift",
            queries: Vec::new(),
        },
        Workload {
            name: "dense",
            queries: vec![q("SELECT ?s ?b WHERE { ?s ex:age 3 ; ex:bio ?b }".into())],
        },
    ]
}

/// Twenty queries each formatting the name and bio of 250 subjects drawn
/// from `range`.
fn values_queries(
    prefix: &str,
    range: std::ops::Range<u64>,
    salt: u64,
    ordered: bool,
) -> Vec<String> {
    let span = range.end - range.start;
    (0..20u64)
        .map(|k| {
            let ids: Vec<String> = (0..250u64)
                .map(|j| {
                    let x = (k * 250 + j + salt * 1_000_003).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                    format!("p:{}", pid(range.start + (x >> 11) % span, ordered))
                })
                .collect();
            format!(
                "{prefix}SELECT ?s ?n ?b WHERE {{ VALUES ?s {{ {} }} ?s ex:name ?n ; ex:bio ?b }}",
                ids.join(" ")
            )
        })
        .collect()
}

/// Order-insensitive digest and row count of a formatted SPARQL result.
fn digest(value: &Value) -> (u64, usize) {
    let rows = value
        .pointer("/results/bindings")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| value.as_array().cloned())
        .unwrap_or_default();
    let mut lines: Vec<String> = rows.iter().map(Value::to_string).collect();
    lines.sort();
    let mut h = DefaultHasher::new();
    lines.hash(&mut h);
    (h.finish(), lines.len())
}

#[cfg(not(no_counters))]
fn work() -> Value {
    let work: serde_json::Map<String, Value> = fluree_db_core::io_stats::work_snapshot()
        .into_iter()
        .map(|(k, n, b)| (k.to_string(), json!([n, b])))
        .collect();
    let reads: serde_json::Map<String, Value> = fluree_db_core::io_stats::reads_snapshot()
        .into_iter()
        .map(|(k, s, n, b)| (format!("{k}/{s}"), json!([n, b])))
        .collect();
    json!({"work": work, "reads": reads})
}

#[cfg(no_counters)]
fn work() -> Value {
    Value::Null
}

#[cfg(not(no_counters))]
fn occupancy(fluree: &fluree_db_api::Fluree) -> Value {
    fluree
        .leaflet_cache()
        .occupancy()
        .into_iter()
        .map(|(k, n, b)| json!({"kind": k, "entries": n, "bytes": b}))
        .collect()
}

#[cfg(no_counters)]
fn occupancy(_fluree: &fluree_db_api::Fluree) -> Value {
    Value::Null
}

/// The fenced-leaf pool: fence index bytes apart from the sources' mapping
/// charge.
#[cfg(not(no_counters))]
fn fences(fluree: &fluree_db_api::Fluree) -> Value {
    let m = fluree.leaflet_cache().fenced_leaf_memory();
    json!({"leaves": m.leaves, "charged": m.charged, "fence_bytes": m.fence_bytes,
           "source_charge": m.source_charge})
}

#[cfg(no_counters)]
fn fences(_fluree: &fluree_db_api::Fluree) -> Value {
    Value::Null
}

#[cfg(not(no_counters))]
fn allowance(fluree: &fluree_db_api::Fluree) -> Value {
    let s = fluree.leaflet_cache().dictionary_allowance().stats();
    json!({
        "capacity": (s.capacity != u64::MAX).then_some(s.capacity), "used": s.used,
        "reserved": fluree.leaflet_cache().reserved_bytes(),
        "peak": s.peak, "released": s.released,
        "resident_packs": s.resident_packs, "resident_bytes": s.resident_bytes,
        "promoted": s.promoted, "promoted_bytes": s.promoted_bytes,
        "assembly_decrypted": s.assembly_decrypted, "assembly_reused": s.assembly_reused,
        "rent_covered_bytes": s.rent_covered_bytes,
        "crossing_coverage": s.crossing_coverage, "promotion_coverage": s.promotion_coverage,
        "evicted": s.evicted, "evicted_bytes": s.evicted_bytes,
        "busy": s.busy, "no_room": s.no_room,
        "too_large": s.too_large, "rent_epoch": s.rent_epoch,
        "rent_counted": s.rent_counted,
        "repromoted": s.repromoted, "repromoted_bytes": s.repromoted_bytes,
        "outstanding": s.outstanding,
        // Per live pack: [kind, bytes, rent, covered, promotions, held, 0,
        // recent rent, why last not promoted]; the 0 keeps the place of the
        // retired recent-lookup count for the report scripts.
        "packs": fluree.leaflet_cache().dictionary_allowance().pack_report().iter()
            .map(|p| json!([p.kind, p.len, p.rent, p.covered, p.promotions, p.resident,
                            0, p.recent_rent, p.reason]))
            .collect::<Vec<_>>(),
    })
}

#[cfg(no_counters)]
fn allowance(_fluree: &fluree_db_api::Fluree) -> Value {
    Value::Null
}

/// Resident memory split by backing, from `/proc/self/smaps_rollup` and
/// `/proc/self/status` (Linux; null elsewhere).
fn memory() -> Value {
    let read_kb = |path: &str, key: &str| -> Option<u64> {
        std::fs::read_to_string(path).ok()?.lines().find_map(|l| {
            let rest = l.strip_prefix(key)?;
            rest.split_whitespace().next()?.parse().ok()
        })
    };
    let rss = read_kb("/proc/self/smaps_rollup", "Rss:");
    let anon = read_kb("/proc/self/smaps_rollup", "Anonymous:");
    json!({
        "rss_kb": rss,
        "anon_kb": anon,
        "file_kb": rss.zip(anon).map(|(r, a)| r.saturating_sub(a)),
        "hwm_kb": read_kb("/proc/self/status", "VmHWM:"),
        "cgroup": cgroup(),
    })
}

/// The process's cgroup v2 memory accounting, which charges page cache the
/// process faults in as well as its heap: current and peak usage, and the
/// anon/file split and fault counts from `memory.stat`.
fn cgroup() -> Value {
    let Some(path) = std::fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("0::").map(str::to_string))
        })
    else {
        return Value::Null;
    };
    let dir = format!("/sys/fs/cgroup{path}");
    let num = |file: &str| -> Option<u64> {
        std::fs::read_to_string(format!("{dir}/{file}"))
            .ok()?
            .trim()
            .parse()
            .ok()
    };
    let stat = std::fs::read_to_string(format!("{dir}/memory.stat")).unwrap_or_default();
    let stat_of = |key: &str| -> Option<u64> {
        stat.lines().find_map(|l| {
            let (k, v) = l.split_once(' ')?;
            (k == key).then(|| v.trim().parse().ok()).flatten()
        })
    };
    json!({
        "path": path,
        "current": num("memory.current"),
        "peak": num("memory.peak"),
        "max": std::fs::read_to_string(format!("{dir}/memory.max")).ok().map(|s| s.trim().to_string()),
        "anon": stat_of("anon"),
        "file": stat_of("file"),
        "file_mapped": stat_of("file_mapped"),
        "pgfault": stat_of("pgfault"),
        "pgmajfault": stat_of("pgmajfault"),
        "workingset_refault_file": stat_of("workingset_refault_file"),
        "oom_kill": std::fs::read_to_string(format!("{dir}/memory.events"))
            .ok()
            .and_then(|e| e.lines().find_map(|l| l.strip_prefix("oom_kill ")?.trim().parse::<u64>().ok())),
    })
}

async fn query(args: &[String]) {
    let dir = PathBuf::from(arg(args, "--dir").expect("--dir"));
    let encrypted = flag(args, "--encrypted");
    let variant = arg(args, "--variant").unwrap_or_else(|| "unnamed".into());
    let reps: usize = arg(args, "--reps").map_or(3, |v| v.parse().unwrap());
    let cache_mb: Option<usize> = arg(args, "--cache-mb").map(|v| v.parse().unwrap());
    let dictionary_mb: Option<usize> = arg(args, "--dictionary-mb").map(|v| v.parse().unwrap());
    let state = arg(args, "--state").unwrap_or_else(|| "default".into());
    // `pass` (default) snapshots memory and cache occupancy after each pass,
    // `workload` after each workload too, `none` never: snapshots walk the
    // page tables and the cache, which a profile should not see.
    let snapshots = arg(args, "--snapshots").unwrap_or_else(|| "pass".into());
    let only: Option<Vec<String>> =
        arg(args, "--workloads").map(|v| v.split(',').map(str::to_string).collect());
    // `--plan sub_a*5,sub_b*5` runs each workload for its passes in turn,
    // numbering passes across the plan; it replaces `--reps`/`--workloads`.
    let plan: Option<Vec<(String, usize)>> = arg(args, "--plan").map(|v| {
        v.split(',')
            .map(|step| {
                let (name, passes) = step.split_once('*').unwrap_or((step, "1"));
                (name.to_string(), passes.parse().expect("--plan passes"))
            })
            .collect()
    });
    let out_path = PathBuf::from(arg(args, "--out").expect("--out"));
    let meta: Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("meta.json")).unwrap()).unwrap();
    let n = meta["subjects"].as_u64().unwrap();
    let ordered = meta["ordered"].as_bool().unwrap_or(false);
    let mut out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&out_path)
        .unwrap();
    let mut emit = |v: Value| {
        writeln!(out, "{v}").unwrap();
    };

    let started = Instant::now();
    let fluree = open(&dir.join("db"), encrypted, cache_mb, dictionary_mb).await;
    let snapshot = fluree.graph(ALIAS).load().await.expect("load");
    emit(json!({
        "type": "open", "variant": variant, "state": state,
        "open_ms": started.elapsed().as_secs_f64() * 1e3,
        "memory": memory(), "work": work(),
    }));

    let all = workloads(n, ordered);
    // Pass 0 meets empty decoded caches; later passes find them warm.
    let passes: Vec<(usize, Vec<&Workload>)> = match &plan {
        Some(plan) => plan
            .iter()
            .flat_map(|(name, k)| {
                let w = all
                    .iter()
                    .find(|w| w.name == name)
                    .unwrap_or_else(|| panic!("no workload {name}"));
                std::iter::repeat_n(vec![w], *k)
            })
            .enumerate()
            .collect(),
        None => (0..=reps)
            .map(|pass| {
                let ws = all
                    .iter()
                    .filter(|w| only.as_ref().is_none_or(|o| o.iter().any(|x| x == w.name)))
                    .collect();
                (pass, ws)
            })
            .collect(),
    };
    let prefix = format!("PREFIX ex: <{EX}> PREFIX p: <{EX}person/> ");
    for (pass, workloads) in passes {
        let phase = if pass == 0 { "first" } else { "repeat" };
        for w in workloads {
            let drift;
            let queries = if w.name == "scatter_drift" {
                drift = values_queries(&prefix, 0..n, 1_000 + pass as u64, ordered);
                &drift
            } else {
                &w.queries
            };
            for (qi, sparql) in queries.iter().enumerate() {
                let before = work();
                let t = Instant::now();
                let formatted = snapshot
                    .query()
                    .sparql(sparql)
                    .execute_formatted()
                    .await
                    .unwrap_or_else(|e| panic!("{} q{qi}: {e}", w.name));
                let ns = t.elapsed().as_nanos() as u64;
                let (hash, rows) = digest(&formatted);
                emit(json!({
                    "type": "query", "variant": variant, "state": state, "pass": pass,
                    "phase": phase, "workload": w.name, "q": qi, "ns": ns,
                    "rows": rows, "hash": format!("{hash:016x}"),
                    "before": before, "after": work(),
                }));
            }
            if snapshots == "workload" {
                emit(json!({
                    "type": "workload_snapshot", "variant": variant, "state": state,
                    "pass": pass, "workload": w.name,
                    "memory": memory(), "occupancy": occupancy(&fluree), "fences": fences(&fluree), "work": work(),
                    "allowance": allowance(&fluree),
                }));
            }
        }
        if snapshots != "none" {
            emit(json!({
                "type": "snapshot", "variant": variant, "state": state, "pass": pass,
                "memory": memory(), "occupancy": occupancy(&fluree), "fences": fences(&fluree), "work": work(),
                "allowance": allowance(&fluree),
            }));
        }
    }
}

/// Encrypt/decrypt throughput of the storage envelope this process writes,
/// through `EncryptedStorage` over memory storage: whole writes and reads,
/// and one-segment range reads from the local source when segmented.
async fn crypto(args: &[String]) {
    use fluree_db_core::{MemoryStorage, StorageRead, StorageWrite};
    let out_path = PathBuf::from(arg(args, "--out").expect("--out"));
    let variant = arg(args, "--variant").unwrap_or_else(|| "unnamed".into());
    let mut out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&out_path)
        .unwrap();
    let storage = fluree_db_api::EncryptedStorage::new(
        MemoryStorage::new(),
        fluree_db_api::StaticKeyProvider::new(fluree_db_api::EncryptionKey::new(KEY, 1)),
    );
    for size in [4 << 10, 64 << 10, 721 << 10, 2 << 20, 16 << 20] {
        let plain: Vec<u8> = (0..size).map(|i: usize| (i * 131 % 251) as u8).collect();
        let iters = ((256usize << 20) / size).clamp(4, 4096);
        let t = Instant::now();
        for i in 0..iters {
            storage.write_bytes(&format!("o{i}"), &plain).await.unwrap();
        }
        let encrypt_ns = t.elapsed().as_nanos() as f64 / iters as f64;
        let t = Instant::now();
        for i in 0..iters {
            std::hint::black_box(storage.read_bytes(&format!("o{i}")).await.unwrap());
        }
        let decrypt_ns = t.elapsed().as_nanos() as f64 / iters as f64;
        #[cfg(not(no_counters))]
        let ranged_ns = storage.get_local_ranged("o0").unwrap().map(|source| {
            let t = Instant::now();
            for i in 0..iters {
                let start = ((i * 4093) % size) as u64;
                std::hint::black_box(source.read_range(start..start + 100).unwrap());
            }
            t.elapsed().as_nanos() as f64 / iters as f64
        });
        #[cfg(no_counters)]
        let ranged_ns: Option<f64> = None;
        for i in 0..iters {
            storage.delete(&format!("o{i}")).await.unwrap();
        }
        let mbs = |ns: f64| size as f64 / ns * 1e3;
        writeln!(
            out,
            "{}",
            json!({
                "type": "crypto", "variant": variant, "size": size, "iters": iters,
                "encrypt_ns": encrypt_ns, "decrypt_ns": decrypt_ns,
                "encrypt_mb_s": mbs(encrypt_ns), "decrypt_mb_s": mbs(decrypt_ns),
                "range_100b_ns": ranged_ns,
            })
        )
        .unwrap();
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    match args.get(1).map(String::as_str) {
        Some("import") => runtime.block_on(import(&args)),
        Some("query") => runtime.block_on(query(&args)),
        Some("crypto") => runtime.block_on(crypto(&args)),
        _ => eprintln!("usage: encryption_ab import|query|crypto ..."),
    }
}
