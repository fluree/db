//! Subject reverse leaf costs, plain (`DLR1`) against front-coded (`DLR2`),
//! over one leaf of the same entries.
//!
//! - `lookup`: parse a cached leaf and probe it, as every commit's novelty
//!   population and every constant-IRI lookup does; half hits, half misses.
//! - `rewrite`: decode a leaf, merge new entries, encode it again, as an
//!   incremental index pass does for each leaf it touches.
//!
//! Two key shapes: local names under their own namespaces, and full IRIs
//! under the overflow namespace (most of a large ledger's subjects before
//! the `NamedLocal` split).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_db_binary_index::dict::builder::DEFAULT_TARGET_LEAF_BYTES;
use fluree_db_binary_index::dict::reverse_leaf::{
    subject_reverse_key, LeafFormat, ReverseEntry, ReverseLeaf,
};
use fluree_db_core::subject_id::SubjectId;

const OVERFLOW_NS: u16 = 0xFFFE;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// One `DLR1`-sized leaf of subject entries, as the builder cuts it.
fn leaf_entries(overflow: bool) -> Vec<ReverseEntry> {
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    let mut entries: Vec<ReverseEntry> = Vec::new();
    let mut bytes = 0usize;
    let mut n = 0u64;
    while bytes < DEFAULT_TARGET_LEAF_BYTES {
        let kind = ["Offer", "Review", "Product", "Person"][(n % 4) as usize];
        let local = rng.next() % 50_000_000;
        let (ns, suffix) = if overflow {
            (
                OVERFLOW_NS,
                format!(
                    "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor{}/{kind}{local}",
                    n % 300
                ),
            )
        } else {
            (40 + (n % 4) as u16, format!("{kind}{local}"))
        };
        let key = subject_reverse_key(ns, suffix.as_bytes());
        bytes += 12 + key.len();
        entries.push(ReverseEntry {
            key,
            id: SubjectId::new(ns, rng.next() % (1 << 26)).as_u64(),
        });
        n += 1;
    }
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    entries.dedup_by(|a, b| a.key == b.key);
    entries
}

fn probes(entries: &[ReverseEntry]) -> Vec<Vec<u8>> {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    (0..2048)
        .map(|i| {
            let mut key = entries[(rng.next() % entries.len() as u64) as usize]
                .key
                .clone();
            if i % 2 == 1 {
                key.push(b'0');
            }
            key
        })
        .collect()
}

const FORMATS: [(&str, LeafFormat); 2] = [
    ("plain", LeafFormat::Plain),
    (
        "front_coded",
        LeafFormat::FrontCoded {
            ns_relative_ids: true,
        },
    ),
];

fn bench_reverse_leaf(c: &mut Criterion) {
    for (shape, overflow) in [("named_local", false), ("overflow", true)] {
        let entries = leaf_entries(overflow);
        let probes = probes(&entries);
        let added: Vec<ReverseEntry> = entries
            .iter()
            .step_by(entries.len() / 100)
            .map(|e| {
                let mut key = e.key.clone();
                key.push(b'x');
                ReverseEntry { key, id: e.id + 1 }
            })
            .collect();

        let mut lookup = c.benchmark_group(format!("reverse_leaf_lookup/{shape}"));
        lookup.throughput(Throughput::Elements(probes.len() as u64));
        for (name, format) in FORMATS {
            let blob = format.encode(&entries);
            eprintln!(
                "{shape}/{name}: {} entries, {} bytes",
                entries.len(),
                blob.len()
            );
            lookup.bench_function(BenchmarkId::from_parameter(name), |b| {
                b.iter(|| {
                    let mut hits = 0usize;
                    for probe in &probes {
                        let leaf = ReverseLeaf::from_bytes(black_box(&blob)).unwrap();
                        hits += usize::from(leaf.lookup(probe).is_some());
                    }
                    black_box(hits)
                });
            });
        }
        lookup.finish();

        let mut rewrite = c.benchmark_group(format!("reverse_leaf_rewrite/{shape}"));
        rewrite.throughput(Throughput::Elements(entries.len() as u64));
        rewrite.sample_size(20);
        for (name, format) in FORMATS {
            let blob = format.encode(&entries);
            rewrite.bench_function(BenchmarkId::from_parameter(name), |b| {
                b.iter(|| {
                    let leaf = ReverseLeaf::from_bytes(black_box(&blob)).unwrap();
                    let mut merged: Vec<ReverseEntry> = leaf
                        .iter()
                        .map(|(key, id)| ReverseEntry {
                            key: key.into_owned(),
                            id,
                        })
                        .chain(added.iter().cloned())
                        .collect();
                    merged.sort_by(|a, b| a.key.cmp(&b.key));
                    black_box(format.encode(&merged))
                });
            });
        }
        rewrite.finish();
    }
}

criterion_group!(benches, bench_reverse_leaf);
criterion_main!(benches);
