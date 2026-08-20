//! Reconstruct a ledger's real index-version graph from its root blobs.
//!
//! Diagnostic, not shipped behaviour. It exists because `chain_len` and root COUNT
//! disagreed wildly on a live deployment — GC reported a 21-version chain while the
//! ledger held 211 root files — and no amount of reasoning about the truncation
//! loop explained it. Every root carries `index_t` and `prev_index`, so the actual
//! structure is recoverable; this decodes it and says which of three shapes it is:
//!
//!   1. ONE BROKEN CHAIN — a middle root is missing, so everything older than the
//!      hole is unreachable in a single stroke. GC deletes oldest-first precisely to
//!      make this impossible, so finding it means something deletes out of order.
//!   2. MULTIPLE LINEAGES — roots fork: two builds from one parent, one publishes,
//!      the loser's root is written and abandoned. Orphaning at CREATION, fixable
//!      upstream of GC entirely.
//!   3. UNCHAINED ROOTS — roots with no `prev_index` that are not genesis, i.e.
//!      never linked in at all.
//!
//! Those have completely different fixes, which is why guessing was not good enough.
//!
//! Usage — copy a ledger's roots directory out of the pod, then:
//!     cargo run -p fluree-db-indexer --example root_graph -- <roots-dir> [head-digest]
//!
//! `head-digest` is the published index head (the nameservice's `index_head_id`) if
//! known; supplying it lets the tool report reachability from the real head rather
//! than guessing the newest by `index_t`.

use fluree_db_binary_index::IndexRoot;
use std::collections::{BTreeMap, HashMap, HashSet};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().unwrap_or_else(|| {
        eprintln!("usage: root_graph <roots-dir> [head-digest]");
        std::process::exit(2);
    });
    let head_arg = args.next();

    // digest -> (index_t, prev_digest)
    let mut nodes: HashMap<String, (i64, Option<String>)> = HashMap::new();
    let mut undecodable = 0usize;

    for entry in std::fs::read_dir(&dir).expect("read roots dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|s| s.to_str()) != Some("fir6") {
            continue;
        }
        let digest = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        let bytes = std::fs::read(&path).expect("read root");
        match IndexRoot::decode(&bytes) {
            Ok(root) => {
                let prev = root.prev_index.as_ref().map(|p| p.id.digest_hex());
                nodes.insert(digest, (root.index_t, prev));
            }
            Err(e) => {
                undecodable += 1;
                eprintln!("  ! undecodable {digest}: {e}");
            }
        }
    }

    println!("roots on disk    : {}", nodes.len());
    println!("undecodable      : {undecodable}");

    // Which roots are pointed AT by some other root?
    let mut referenced: HashSet<&String> = HashSet::new();
    let mut prev_targets: HashMap<&String, Vec<&String>> = HashMap::new();
    for (digest, (_, prev)) in &nodes {
        if let Some(p) = prev {
            referenced.insert(p);
            prev_targets.entry(p).or_default().push(digest);
        }
    }

    // Shape 3: roots with no prev_index. Exactly one (genesis) is expected.
    let genesis: Vec<_> = nodes
        .iter()
        .filter(|(_, (_, p))| p.is_none())
        .map(|(d, (t, _))| (*t, d.clone()))
        .collect();
    println!(
        "roots with no prev_index (expect 1 = genesis): {}",
        genesis.len()
    );
    for (t, d) in genesis.iter().take(5) {
        println!("    index_t={t} {}", &d[..16.min(d.len())]);
    }

    // Shape 2: a root pointed at by MORE THAN ONE successor is a fork point — two
    // builds based on the same parent. Only one can be the published head, so the
    // other lineage is abandoned at creation.
    let forks: Vec<_> = prev_targets
        .iter()
        .filter(|(_, kids)| kids.len() > 1)
        .collect();
    println!("fork points (parent with >1 child): {}", forks.len());
    for (parent, kids) in forks.iter().take(8) {
        let pt = nodes.get(**parent).map(|(t, _)| *t).unwrap_or(-1);
        let kid_ts: Vec<i64> = kids
            .iter()
            .map(|k| nodes.get(*k).map(|(t, _)| *t).unwrap_or(-1))
            .collect();
        println!(
            "    parent index_t={pt} {} -> {} children at index_t {:?}",
            &parent[..16.min(parent.len())],
            kids.len(),
            kid_ts
        );
    }

    // Shape 1: dangling prev pointers — a root references a parent that is GONE.
    // Each dangling pointer is a place the chain walk stops.
    let dangling: Vec<_> = nodes
        .iter()
        .filter_map(|(d, (t, p))| {
            p.as_ref()
                .filter(|p| !nodes.contains_key(*p))
                .map(|p| (*t, d.clone(), p.clone()))
        })
        .collect();
    println!(
        "dangling prev pointers (chain stops here): {}",
        dangling.len()
    );
    for (t, d, p) in dangling.iter().take(8) {
        println!(
            "    index_t={t} {} -> MISSING {}",
            &d[..16.min(d.len())],
            &p[..16.min(p.len())]
        );
    }

    // Reachability from the head: what GC can actually see.
    let head = head_arg.or_else(|| {
        nodes
            .iter()
            .max_by_key(|(_, (t, _))| *t)
            .map(|(d, _)| d.clone())
    });
    if let Some(head) = head {
        // Count only digests that are actually PRESENT on disk. Walking into a
        // missing parent (the tail of a GC truncation) and counting it would make
        // `seen` exceed `nodes` and the "unreachable" subtraction underflow.
        let mut seen = HashSet::new();
        let mut cur = Some(head.clone());
        while let Some(d) = cur {
            let Some((_, prev)) = nodes.get(&d) else {
                break; // dangling: parent not on disk, chain ends here
            };
            if !seen.insert(d.clone()) {
                println!("!! CYCLE at {}", &d[..16.min(d.len())]);
                break;
            }
            cur = prev.clone();
        }
        println!(
            "\nreachable from head {} : {} of {} roots  ({} UNREACHABLE)",
            &head[..16.min(head.len())],
            seen.len(),
            nodes.len(),
            nodes.len().saturating_sub(seen.len())
        );

        // How do the unreachable ones distribute over index_t? A contiguous block
        // means one truncation event; a scatter means repeated forking.
        let mut unreachable_ts: Vec<i64> = nodes
            .iter()
            .filter(|(d, _)| !seen.contains(*d))
            .map(|(_, (t, _))| *t)
            .collect();
        unreachable_ts.sort_unstable();
        if !unreachable_ts.is_empty() {
            println!(
                "unreachable index_t range: {} .. {}",
                unreachable_ts[0],
                unreachable_ts[unreachable_ts.len() - 1]
            );
            // Contiguity: how many distinct index_t values, and are there gaps?
            let distinct: BTreeMap<i64, usize> =
                unreachable_ts.iter().fold(BTreeMap::new(), |mut m, t| {
                    *m.entry(*t).or_insert(0) += 1;
                    m
                });
            let dupes: Vec<_> = distinct.iter().filter(|(_, n)| **n > 1).take(6).collect();
            println!(
                "distinct unreachable index_t: {} (so {} share a t with another root)",
                distinct.len(),
                unreachable_ts.len() - distinct.len()
            );
            if !dupes.is_empty() {
                // TWO roots at the same index_t is the signature of a fork: the same
                // logical version built twice.
                println!("  index_t values with MULTIPLE roots (fork signature):");
                for (t, n) in dupes {
                    println!("    index_t={t}: {n} roots");
                }
            }
        }
    }
}
