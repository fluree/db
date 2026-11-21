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
//!
//! - `string_leaf_lookup`: a string probe with its key made from the value,
//!   raw keys in plain leaves against prefix-and-hash keys in front-coded
//!   ones, each leaf cut as its tree would cut it.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_db_binary_index::dict::builder::DEFAULT_TARGET_LEAF_BYTES;
use fluree_db_binary_index::dict::key_scheme::{
    KeyRules, KeyScheme, RawGroups, DEFAULT_GROUP_HASH_CAP, DEFAULT_STRING_KEY_PREFIX,
};
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

/// Review-like text: random words, lengths spread from short labels to
/// paragraphs.
fn string_values() -> Vec<Vec<u8>> {
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let words = [
        "hydrotherapeutic",
        "jolters",
        "daylighted",
        "biotical",
        "coaxers",
        "chilliest",
        "zebu",
        "zyzzyvas",
        "quantum",
        "lattice",
        "ember",
        "orchid",
    ];
    let mut values: Vec<Vec<u8>> = (0..60_000)
        .map(|i| {
            let target = [20usize, 90, 400, 1500][i % 4];
            let mut v = String::new();
            while v.len() < target {
                v.push_str(words[(rng.next() % words.len() as u64) as usize]);
                v.push(' ');
            }
            v.into_bytes()
        })
        .collect();
    values.sort();
    values.dedup();
    values
}

/// One leaf of `values` keyed by `rules`, cut where a `DLR1` leaf of those
/// keys reaches the target, as the tree writers cut it.
fn string_leaf(values: &[Vec<u8>], rules: KeyRules<'_>) -> Vec<ReverseEntry> {
    let mut entries: Vec<ReverseEntry> = values
        .iter()
        .enumerate()
        .map(|(id, v)| ReverseEntry {
            key: rules.key(v).into_owned(),
            id: id as u64,
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    let mut bytes = 0usize;
    let cut = entries
        .iter()
        .position(|e| {
            bytes += 12 + e.key.len();
            bytes >= DEFAULT_TARGET_LEAF_BYTES
        })
        .map_or(entries.len(), |i| i + 1);
    entries.truncate(cut);
    entries
}

/// A string probe as a lookup pays it: key the value, parse the cached
/// leaf, look the key up, and look up the hashed key too when a group that
/// filled late misses. Raw keys in plain leaves, hashed keys in front-coded
/// ones, and long values keyed by value in groups that also hold hashed
/// ones; half the probes miss.
fn bench_string_lookup(c: &mut Criterion) {
    let values = string_values();
    let hashed = KeyScheme::PrefixHash {
        prefix_len: DEFAULT_STRING_KEY_PREFIX,
        cap: DEFAULT_GROUP_HASH_CAP,
    };
    let none = RawGroups::default();
    let mut every_group = RawGroups::default();
    for v in &values {
        if let Some(group) = hashed.group(v) {
            every_group.insert(group, true);
        }
    }
    let front_coded = LeafFormat::FrontCoded {
        ns_relative_ids: false,
    };
    let mut group = c.benchmark_group("string_leaf_lookup");
    for (name, rules, format) in [
        (
            "raw_plain",
            KeyRules {
                scheme: KeyScheme::Raw,
                raw: &none,
            },
            LeafFormat::Plain,
        ),
        (
            "hashed_front_coded",
            KeyRules {
                scheme: hashed,
                raw: &none,
            },
            front_coded,
        ),
        (
            "filled_groups_front_coded",
            KeyRules {
                scheme: hashed,
                raw: &every_group,
            },
            front_coded,
        ),
    ] {
        let entries = string_leaf(&values, rules);
        let blob = format.encode(&entries);
        let ids: std::collections::HashSet<u64> = entries.iter().map(|e| e.id).collect();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let probes: Vec<Vec<u8>> = (0..2048)
            .map(|i| {
                let mut v = loop {
                    let id = rng.next() % values.len() as u64;
                    if ids.contains(&id) {
                        break values[id as usize].clone();
                    }
                };
                if i % 2 == 1 {
                    v.push(b'!');
                }
                v
            })
            .collect();
        eprintln!(
            "strings/{name}: {} entries, {} bytes",
            entries.len(),
            blob.len()
        );
        group.throughput(Throughput::Elements(probes.len() as u64));
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| {
                let mut hits = 0usize;
                for probe in &probes {
                    let keys = rules.lookup_keys(black_box(probe));
                    let leaf = ReverseLeaf::from_bytes(black_box(&blob)).unwrap();
                    let found = leaf
                        .lookup(&keys.first)
                        .or_else(|| keys.then().and_then(|key| leaf.lookup(&key)));
                    hits += usize::from(found.is_some());
                }
                black_box(hits)
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_reverse_leaf, bench_string_lookup);
criterion_main!(benches);
