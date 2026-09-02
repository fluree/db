//! Reconstruct a ledger's real index-version graph from its root blobs.
//!
//! Diagnostic, not shipped behaviour. It exists because `chain_len` and root COUNT
//! disagreed wildly on a live deployment — GC reported a 21-version chain while the
//! ledger held 211 root files — and no amount of reasoning about the truncation
//! loop explained it. Every root carries `index_t` and `prev_index`, so the actual
//! structure is recoverable; this decodes it and says which of four shapes it is:
//!
//!   1. ONE BROKEN CHAIN — a middle root is missing, so everything older than the
//!      hole is unreachable in a single stroke. GC deletes oldest-first precisely to
//!      make this impossible, so finding it means something deletes out of order.
//!   2. MULTIPLE LINEAGES — roots fork: two builds from one parent, one publishes,
//!      the loser's root is written and abandoned. Orphaning at CREATION, fixable
//!      upstream of GC entirely.
//!   3. UNCHAINED ROOTS — roots with no `prev_index` that are not genesis, i.e.
//!      never linked in at all.
//!   4. INCONSISTENT CHAIN METADATA — a root's `prev_index.t` disagrees with the
//!      `index_t` the parent itself carries. The pointer still resolves, so the walk
//!      succeeds and none of the three shapes above show it, but one of the two
//!      values was written wrong and anything reasoning from `prev_index.t` without
//!      fetching the parent is being lied to.
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

/// What we keep from each decoded root.
///
/// `prev_index` carries the parent's `t` alongside its CID, so keep both: the `t`
/// names where a missing parent sat in version space, which is the answer shape 1 is
/// asking for, and when the parent IS present the two `t` values are a free
/// consistency check on the chain metadata (shape 4).
struct Node {
    index_t: i64,
    /// `(parent digest, the parent `index_t` this root claims)`, or `None` for a
    /// root with no `prev_index`.
    prev: Option<(String, i64)>,
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().unwrap_or_else(|| {
        eprintln!("usage: root_graph <roots-dir> [head-digest]");
        std::process::exit(2);
    });
    let head_arg = args.next();

    // digest -> decoded root
    let mut nodes: HashMap<String, Node> = HashMap::new();
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
                let prev = root.prev_index.as_ref().map(|p| (p.id.digest_hex(), p.t));
                nodes.insert(
                    digest,
                    Node {
                        index_t: root.index_t,
                        prev,
                    },
                );
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
    for (digest, node) in &nodes {
        if let Some((p, _)) = &node.prev {
            referenced.insert(p);
            prev_targets.entry(p).or_default().push(digest);
        }
    }

    // Shape 3: roots with no prev_index. Exactly one (genesis) is expected.
    let mut genesis: Vec<_> = nodes
        .iter()
        .filter(|(_, node)| node.prev.is_none())
        .map(|(d, node)| (node.index_t, d.clone()))
        .collect();
    // Every listing below is sorted by (index_t, digest). These are collected out of
    // a HashMap, so without this the five roots a preview happens to show, and the
    // order it shows them in, change from run to run over an unchanged directory.
    // The tool's main use is diffing two measurements, which that makes impossible.
    genesis.sort();
    println!(
        "roots with no prev_index (expect 1 = genesis): {}",
        genesis.len()
    );
    for (t, d) in genesis.iter().take(5) {
        println!("    index_t={t} {}", &d[..16.min(d.len())]);
    }
    and_n_more(5, genesis.len());

    // Shape 2: a root pointed at by MORE THAN ONE successor is a fork point — two
    // builds based on the same parent. Only one can be the published head, so the
    // other lineage is abandoned at creation.
    let mut forks: Vec<_> = prev_targets
        .iter()
        .filter(|(_, kids)| kids.len() > 1)
        .map(|(parent, kids)| {
            let parent_t = nodes.get(*parent).map(|n| n.index_t).unwrap_or(-1);
            let mut kid_ts: Vec<i64> = kids
                .iter()
                .map(|k| nodes.get(*k).map(|n| n.index_t).unwrap_or(-1))
                .collect();
            kid_ts.sort_unstable();
            (parent_t, (*parent).clone(), kid_ts)
        })
        .collect();
    forks.sort();
    println!("fork points (parent with >1 child): {}", forks.len());
    for (parent_t, parent, kid_ts) in forks.iter().take(8) {
        println!(
            "    parent index_t={parent_t} {} -> {} children at index_t {:?}",
            &parent[..16.min(parent.len())],
            kid_ts.len(),
            kid_ts
        );
    }
    and_n_more(8, forks.len());

    // Shape 1: dangling prev pointers — a root references a parent that is GONE.
    // Each dangling pointer is a place the chain walk stops. "Gone" has two causes
    // with different fixes: the parent was deleted, or it is still on disk and did
    // not decode. Truncating a root and deleting it leave the same hole, so the two
    // are counted and labelled apart rather than both reported as MISSING.
    let mut dangling: Vec<_> = nodes
        .iter()
        .filter_map(|(d, node)| {
            node.prev
                .as_ref()
                .filter(|(p, _)| !nodes.contains_key(p))
                .map(|(p, prev_t)| (node.index_t, d.clone(), p.clone(), *prev_t))
        })
        .collect();
    dangling.sort();
    let corrupt_parents = dangling
        .iter()
        .filter(|(_, _, p, _)| undecodable.contains(p))
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
    for (t, d, p, prev_t) in dangling.iter().take(8) {
        let cause = if undecodable.contains(p) {
            "UNDECODABLE"
        } else {
            "MISSING"
        };
        // The parent's own `index_t`, from the child's pointer: it says where in
        // version space the hole is without needing the blob that is gone.
        println!(
            "    index_t={t} {} -> {cause} index_t={prev_t} {}",
            &d[..16.min(d.len())],
            &p[..16.min(p.len())]
        );
    }
    and_n_more(8, dangling.len());
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

    // Shape 4: the pointer resolves, but the child and the parent disagree about the
    // parent's index_t. Nothing above sees this — the walk follows the digest and
    // succeeds — yet one of the two values was written wrong.
    let mut inconsistent: Vec<_> = nodes
        .iter()
        .filter_map(|(d, node)| {
            let (p, claimed) = node.prev.as_ref()?;
            let actual = nodes.get(p)?.index_t;
            (actual != *claimed).then(|| (node.index_t, d.clone(), p.clone(), *claimed, actual))
        })
        .collect();
    inconsistent.sort();
    println!(
        "prev_index.t disagreeing with the parent's own index_t: {}",
        inconsistent.len()
    );
    for (t, d, p, claimed, actual) in inconsistent.iter().take(8) {
        println!(
            "    index_t={t} {} -> parent {} claimed index_t={claimed}, carries {actual}",
            &d[..16.min(d.len())],
            &p[..16.min(p.len())]
        );
    }
    and_n_more(8, inconsistent.len());

    // Reachability from the head: what GC can actually see.
    let head = match &head_arg {
        Some(arg) => Some(resolve_head(arg, &nodes, &undecodable)),
        None => guess_head(&nodes),
    };
    if let Some(head) = head {
        // Count only digests that are actually PRESENT on disk. Walking into a
        // missing parent (the tail of a GC truncation) and counting it would make
        // `seen` exceed `nodes` and the "unreachable" subtraction underflow.
        let mut seen = HashSet::new();
        let mut cur = Some(head.clone());
        while let Some(d) = cur {
            let Some(node) = nodes.get(&d) else {
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
            cur = node.prev.as_ref().map(|(p, _)| p.clone());
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
            .map(|(_, node)| node.index_t)
            .collect();
        unreachable_ts.sort_unstable();
        if !unreachable_ts.is_empty() {
            println!(
                "unreachable index_t range: {} .. {}",
                unreachable_ts[0],
                unreachable_ts[unreachable_ts.len() - 1]
            );
            // Contiguity: one root per t across the range is a truncated prefix;
            // fewer distinct values than roots means some t was built more than once.
            let mut distinct_ts = unreachable_ts.clone();
            distinct_ts.dedup();
            println!(
                "unreachable roots: {}, at {} distinct index_t",
                unreachable_ts.len(),
                distinct_ts.len()
            );
        }

        // Fork signature: an index_t carried by more than one root is one logical
        // version built twice. Counted over EVERY root, not just the unreachable
        // ones. In the ordinary fork the winner is published and therefore reachable
        // and only the loser is orphaned, so a multiplicity map over the unreachable
        // subset sees a count of 1 at that t, drops it, and prints nothing on
        // exactly the case the line is named for.
        let mut per_t: BTreeMap<i64, (usize, usize)> = BTreeMap::new();
        for (digest, node) in &nodes {
            let entry = per_t.entry(node.index_t).or_insert((0, 0));
            entry.0 += 1;
            if !seen.contains(digest) {
                entry.1 += 1;
            }
        }
        let shared: Vec<_> = per_t.iter().filter(|(_, (n, _))| *n > 1).collect();
        let roots_sharing_t: usize = shared.iter().map(|(_, (n, _))| *n).sum();
        println!(
            "index_t built more than once (fork signature): {} such index_t, {roots_sharing_t} roots",
            shared.len()
        );
        for (t, (n, unreachable)) in shared.iter().take(6) {
            println!("    index_t={t}: {n} roots ({unreachable} unreachable)");
        }
        and_n_more(6, shared.len());
    }
}

/// Turn the operator's `head-digest` argument into a key of `nodes`, or exit.
///
/// Roots are keyed by the hex digest that names the file, but `index_head_id`
/// stringifies as a base32 CIDv1, so the value the operator is most likely to have
/// on hand can never match a key directly. Accept both, and refuse to guess when
/// neither lands: an unmatched head walks zero roots, and a zero-length walk reads
/// as a totally orphaned ledger, which is the most alarming output the tool has.
fn resolve_head(arg: &str, nodes: &HashMap<String, Node>, undecodable: &HashSet<String>) -> String {
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

/// Close a truncated preview by saying what it left out.
///
/// Every listing here is a `take(n)`, so without this a complete list of five and a
/// cut-off list of two hundred look the same, and the count above the preview is the
/// only thing that says otherwise.
fn and_n_more(shown: usize, total: usize) {
    if total > shown {
        println!("    ... and {} more", total - shown);
    }
}

/// Pick the newest root by `index_t` when no head was supplied.
///
/// `max_by_key` over a HashMap picks arbitrarily among ties, so two runs over one
/// directory could report different reachability. That is worst precisely where it
/// matters: roots tied at the newest `index_t` are one version built twice, which is
/// the fork this tool exists to find. Break the tie on digest so runs agree, and say
/// the tie happened rather than silently guessing which side of the fork is the head.
fn guess_head(nodes: &HashMap<String, Node>) -> Option<String> {
    let (digest, newest) = nodes
        .iter()
        .max_by(|a, b| a.1.index_t.cmp(&b.1.index_t).then_with(|| a.0.cmp(b.0)))
        .map(|(d, n)| (d, n.index_t))?;
    let tied = nodes.values().filter(|n| n.index_t == newest).count();
    if tied > 1 {
        println!(
            "!! no head supplied and {tied} roots share the newest index_t ({newest}): \
             that tie is itself a fork signature. Guessing the largest digest; pass \
             the published head to get a real reachability answer."
        );
    }
    Some(digest.clone())
}
