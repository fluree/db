//! Correlated Explore-Q5 windows, separate from the legacy constant-price Q5.
//! A dedicated fixture keeps the existing Q3/Q5/Q9 baselines unchanged.
//! Indexed data follows the selected scale; fallback fixtures cap at 1,500
//! products so large-scale runs still exercise novelty/policy without timing
//! a pathological unindexed scan. Run with `query_range_semijoin` as filter.

use criterion::{black_box, BenchmarkId, Criterion};
use fluree_bench_support::{bench_runtime, current_profile, current_scale, next_ledger_alias};
use fluree_db_api::{
    CommitOpts, Fluree, FlureeBuilder, GovernanceOptions, GraphDb, IndexConfig, QueryInput, TxnOpts,
};
use std::fmt::Write;

// Count the actual execution route in an untimed preflight. Explain uses
// snapshot-only stats, which can differ from a novelty view's execution stats.
#[derive(Clone, Default)]
struct Routes(std::sync::Arc<std::sync::Mutex<(usize, usize)>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Routes {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        #[derive(Default)]
        struct Fields {
            semijoin: bool,
            walked: bool,
            runtime: bool,
        }
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                match field.name() {
                    "site" => self.semijoin = value == "range-semijoin",
                    "outcome" => {
                        self.walked = value == "proceed";
                        self.runtime = matches!(value, "proceed" | "fallback:gate_declined");
                    }
                    _ => {}
                }
            }
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.semijoin && fields.runtime {
            let mut counts = self.0.lock().unwrap();
            if fields.walked {
                counts.0 += 1;
            } else {
                counts.1 += 1;
            }
        }
    }
}

const QUERY: &str = r"
PREFIX ex: <http://example.org/range/>
SELECT DISTINCT ?product ?label WHERE {
  ?product ex:label ?label . FILTER (?product != ex:anchor)
  ex:anchor ex:feature ?f . ?product ex:feature ?f .
  ex:anchor ex:n1 ?o1 . ?product ex:n1 ?s1 .
  FILTER (?s1 > ?o1 - 120 && ?s1 < ?o1 + 120)
  ex:anchor ex:n2 ?o2 . ?product ex:n2 ?s2 .
  FILTER (?s2 > ?o2 - 170 && ?s2 < ?o2 + 170)
} ORDER BY ?label
";

fn fixture(n: usize) -> (String, usize) {
    let mut ttl = String::from("@prefix ex: <http://example.org/range/> .\nex:anchor ex:feature ex:f0, ex:f1 ; ex:n1 1000 ; ex:n2 1000 ; ex:label \"anchor\" .\n");
    let mut expected = 0;
    for i in 0..n {
        // Dense small fixtures clear the fold's driving-row gate in fallback
        // contexts; Medium/Large indexed fixtures retain ~12.5% candidates.
        let (f1, f2) = if n <= 1500 {
            (0, 1)
        } else {
            (i % 32, (i + 5) % 32)
        };
        let (n1, n2) = ((i * 811) % 2000, (i * 353 + 211) % 2000);
        writeln!(
            ttl,
            "ex:p{i} ex:feature ex:f{f1}, ex:f{f2} ; ex:n1 {n1} ; ex:n2 {n2} ; ex:label \"p{i}\" ."
        )
        .unwrap();
        if (f1 < 2 || f2 < 2) && n1 > 880 && n1 < 1120 && n2 > 830 && n2 < 1170 {
            expected += 1;
        }
    }
    (ttl, expected)
}

async fn setup(n: usize) -> (tempfile::TempDir, Fluree, GraphDb, GraphDb, GraphDb, usize) {
    let dir = tempfile::tempdir().unwrap();
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let alias = next_ledger_alias("range-semijoin-bench");
    let ledger = fluree.create_ledger(&alias).await.unwrap();
    let (ttl, expected) = fixture(n);
    let inserted = fluree
        .insert_turtle_with_opts(
            ledger,
            &ttl,
            TxnOpts::default(),
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 5_000_000_000,
                reindex_max_bytes: 5_000_000_000,
            },
            None,
        )
        .await
        .unwrap();
    let novelty = GraphDb::from_ledger_state(&inserted.ledger);
    fluree.reindex(&alias, Default::default()).await.unwrap();
    let indexed = fluree.db(&alias).await.unwrap();
    // A non-root view that permits the fixture's properties still declines
    // raw probe lanes. This measures fallback work without an empty result.
    let policy = fluree.db_with_policy(&alias, &GovernanceOptions {
        policy: Some(serde_json::json!([
            {"@id":"ex:denyHidden", "@type":"f:AccessPolicy", "f:action":"f:view",
             "f:onProperty":[{"@id":"http://example.org/range/hidden"}], "f:allow":false},
            {"@id":"ex:allow", "@type":"f:AccessPolicy", "f:action":"f:view", "f:allow":true}
        ])), default_allow: Some(true), ..Default::default()
    }).await.unwrap();
    assert!(!policy.is_root());
    (dir, fluree, indexed, novelty, policy, expected)
}

pub fn bench_range_semijoin(c: &mut Criterion) {
    fluree_bench_support::init_tracing_for_bench();
    let rt = bench_runtime();
    let scale = current_scale();
    let n = super::scale_n_products(scale);
    let (_dir, fluree, indexed, novelty, policy, expected) = rt.block_on(setup(n));
    let small = (n > 1500).then(|| rt.block_on(setup(1500)));
    let (fallback_fluree, novelty, policy, fallback_expected) = match &small {
        Some((_, f, _, novelty, policy, expected)) => (f, novelty, policy, *expected),
        None => (&fluree, &novelty, &policy, expected),
    };
    let mut group = c.benchmark_group("query_range_semijoin");
    group.sample_size(current_profile().sample_size());
    group.sampling_mode(criterion::SamplingMode::Flat);
    for (name, f, view, expected) in [
        ("indexed", &fluree, &indexed, expected),
        ("novelty", fallback_fluree, novelty, fallback_expected),
        ("policy", fallback_fluree, policy, fallback_expected),
    ] {
        use tracing_subscriber::prelude::*;
        let routes = Routes::default();
        let subscriber = tracing_subscriber::registry().with(routes.clone());
        let result = tracing::subscriber::with_default(subscriber, || {
            rt.block_on(f.query(view, QueryInput::Sparql(QUERY)))
                .unwrap()
        });
        let rows = result.to_jsonld(&view.snapshot).unwrap();
        assert_eq!(
            rows.as_array().unwrap().len(),
            expected,
            "{name} fixture results"
        );
        let (walk_batches, probe_batches) = *routes.0.lock().unwrap();
        let disabled = fluree_db_query::execute::fast_paths_disabled();
        if n >= 1000 && !disabled {
            assert!(
                walk_batches + probe_batches > 0,
                "{name} must benchmark the fold"
            );
        }
        eprintln!("[range-semijoin] {name}: products={} rows={expected} walk_batches={walk_batches} probe_batches={probe_batches} fast_paths_disabled={disabled}",
            if name == "indexed" { n } else { n.min(1500) });
        group.bench_function(BenchmarkId::new(name, scale.as_str()), |b| {
            b.iter(|| {
                black_box(
                    rt.block_on(f.query(view, QueryInput::Sparql(QUERY)))
                        .unwrap(),
                )
            });
        });
    }
    group.finish();
}
