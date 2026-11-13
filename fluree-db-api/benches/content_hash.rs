// Content-ID minting and verification.
//
// Times `ContentId::new` and `ContentId::verify` (the SHA2-256 path every
// commit, transaction and index artifact takes) over commit-sized inputs,
// and the same for SHA2-512.
//
// ## Running
//
//   cargo bench -p fluree-db-api --bench content_hash

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_db_core::{ContentId, ContentKind, HashAlgorithm};

const SIZES: [usize; 3] = [256, 4 * 1024, 256 * 1024];

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 % 251) as u8).collect()
}

fn sha2_256(c: &mut Criterion) {
    let mut group = c.benchmark_group("content_hash/sha2_256");
    for len in SIZES {
        let bytes = payload(len);
        group.throughput(Throughput::Bytes(len as u64));
        group.bench_with_input(BenchmarkId::new("new", len), &bytes, |b, bytes| {
            b.iter(|| ContentId::new(ContentKind::Commit, black_box(bytes)));
        });
        let id = ContentId::new(ContentKind::Commit, &bytes);
        group.bench_with_input(BenchmarkId::new("verify", len), &bytes, |b, bytes| {
            b.iter(|| id.verify(black_box(bytes)));
        });
    }
    group.finish();
}

fn sha2_512(c: &mut Criterion) {
    let mut group = c.benchmark_group("content_hash/sha2_512");
    for len in SIZES {
        let bytes = payload(len);
        group.throughput(Throughput::Bytes(len as u64));
        group.bench_with_input(BenchmarkId::new("new", len), &bytes, |b, bytes| {
            b.iter(|| {
                ContentId::new_with(
                    HashAlgorithm::Sha2_512,
                    ContentKind::Commit,
                    black_box(bytes),
                )
            });
        });
        let id = ContentId::new_with(HashAlgorithm::Sha2_512, ContentKind::Commit, &bytes);
        group.bench_with_input(BenchmarkId::new("verify", len), &bytes, |b, bytes| {
            b.iter(|| id.verify(black_box(bytes)));
        });
    }
    group.finish();
}

criterion_group!(benches, sha2_256, sha2_512);
criterion_main!(benches);
