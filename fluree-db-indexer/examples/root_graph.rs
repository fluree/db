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
//! than guessing the newest by `index_t`. Either spelling works: the hex digest that
//! names the `.fir6` file, or the base32 CID that `index_head_id` stringifies to.
//! A head that is not a root in this directory is an error, not a walk of length
//! zero reported as total orphaning.

use fluree_db_binary_index::IndexRoot;
use fluree_db_core::ContentId;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().unwrap_or_else(|| {
        eprintln!("usage: root_graph <roots-dir> [head-digest]");
        std::process::exit(2);
    });
    let head_arg = args.next();

    // digest -> (index_t, prev_digest)
    let mut nodes: HashMap<String, (i64, Option<String>)> = HashMap::new();
    // Digests of roots that are on disk but did not decode. Kept as a set, not a
    // count: a root that fails to decode never enters `nodes`, so without this it is
    // indistinguishable from a deleted one everywhere downstream.
    let mut undecodable: HashSet<String> = HashSet::new();

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
                eprintln!("  ! undecodable {digest}: {e}");
                undecodable.insert(digest);
            }
        }
    }

    println!("roots on disk    : {}", nodes.len());
    println!("undecodable      : {}", undecodable.len());

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
    // Each dangling pointer is a place the chain walk stops. "Gone" has two causes
    // with different fixes: the parent was deleted, or it is still on disk and did
    // not decode. Truncating a root and deleting it leave the same hole, so the two
    // are counted and labelled apart rather than both reported as MISSING.
    let dangling: Vec<_> = nodes
        .iter()
        .filter_map(|(d, (t, p))| {
            p.as_ref()
                .filter(|p| !nodes.contains_key(*p))
                .map(|p| (*t, d.clone(), p.clone()))
        })
        .collect();
    let corrupt_parents = dangling
        .iter()
        .filter(|(_, _, p)| undecodable.contains(p))
        .count();
    print!(
        "dangling prev pointers (chain stops here): {}",
        dangling.len()
    );
    if dangling.is_empty() {
        println!();
    } else {
        println!(
            " ({} parent deleted, {} parent undecodable)",
            dangling.len() - corrupt_parents,
            corrupt_parents
        );
    }
    for (t, d, p) in dangling.iter().take(8) {
        let cause = if undecodable.contains(p) {
            "UNDECODABLE"
        } else {
            "MISSING"
        };
        println!(
            "    index_t={t} {} -> {cause} {}",
            &d[..16.min(d.len())],
            &p[..16.min(p.len())]
        );
    }
    if !dangling.is_empty() && !undecodable.is_empty() {
        // Shape 1 says something deletes out of order. A bad blob produces the same
        // hole without anything having deleted anything, so the verdict only stands
        // once nothing on disk is failing to decode.
        println!(
            "  ! shape 1 (something deletes out of order) is not the reading yet: \
             {} root(s) on disk did not decode, and a bad blob leaves the same hole",
            undecodable.len()
        );
    }

    // Reachability from the head: what GC can actually see.
    let head = match &head_arg {
        Some(arg) => Some(resolve_head(arg, &nodes, &undecodable)),
        None => nodes
            .iter()
            .max_by_key(|(_, (t, _))| *t)
            .map(|(d, _)| d.clone()),
    };
    if let Some(head) = head {
        // Count only digests that are actually PRESENT on disk. Walking into a
        // missing parent (the tail of a GC truncation) and counting it would make
        // `seen` exceed `nodes` and the "unreachable" subtraction underflow.
        let mut seen = HashSet::new();
        let mut cur = Some(head.clone());
        while let Some(d) = cur {
            let Some((_, prev)) = nodes.get(&d) else {
                // Chain ends here. If the blob is present but undecodable the walk
                // stops for a reason that has nothing to do with GC, so say which.
                if undecodable.contains(&d) {
                    println!(
                        "!! chain walk stopped at UNDECODABLE root {}",
                        &d[..16.min(d.len())]
                    );
                }
                break;
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

/// Turn the operator's `head-digest` argument into a key of `nodes`, or exit.
///
/// Roots are keyed by the hex digest that names the file, but `index_head_id`
/// stringifies as a base32 CIDv1, so the value the operator is most likely to have
/// on hand can never match a key directly. Accept both, and refuse to guess when
/// neither lands: an unmatched head walks zero roots, and a zero-length walk reads
/// as a totally orphaned ledger, which is the most alarming output the tool has.
fn resolve_head(
    arg: &str,
    nodes: &HashMap<String, (i64, Option<String>)>,
    undecodable: &HashSet<String>,
) -> String {
    if nodes.contains_key(arg) {
        return arg.to_string();
    }
    if let Ok(digest) = ContentId::from_str(arg).map(|cid| cid.digest_hex()) {
        if nodes.contains_key(&digest) {
            println!("head {arg} resolved to digest {digest}");
            return digest;
        }
        if undecodable.contains(&digest) {
            eprintln!("!! head {arg} is on disk as {digest} but did not decode");
            std::process::exit(2);
        }
    }
    if undecodable.contains(arg) {
        eprintln!("!! head {arg} is on disk but did not decode");
        std::process::exit(2);
    }
    eprintln!(
        "!! head {arg} is not a root in this directory.\n\
         !! Pass either the hex digest naming the .fir6 file, or the CID form of\n\
         !! index_head_id. Refusing to report a zero-length walk as total orphaning."
    );
    std::process::exit(2);
}
