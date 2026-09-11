//! Request-level policy authorization overhead with identical effective grants.
//!
//! Measures parsing already-verified claims, constructing the typed context,
//! and binding a cloned JSON request. Excludes signature verification, issuer
//! lookup, network, ledger access, policy compilation, and fact evaluation.
//! Ordinary claims use a server-selected class; delegated claims carry the
//! same class. The query-clone baseline isolates the cost of binding.
//!
//! Uses the shared tracking allocator: timings include its counter overhead.
//! Reports allocation churn/peak per operation to stderr; Criterion timings
//! go to CRITERION_HOME (or its usual target/criterion default).
//!
//! Run: CRITERION_HOME=/tmp/policy-authorization cargo bench -p fluree-db-api
//!      --features credential --bench policy_authorization

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use fluree_db_api::{GovernanceOptions, PolicyAuthorization};
use fluree_db_credential::jwt_claims::{EventsTokenPayload, PolicyClaim};
use serde_json::{json, Value};
use std::hint::black_box;

#[global_allocator]
static ALLOC: fluree_bench_alloc::TrackingAllocator = fluree_bench_alloc::TrackingAllocator::new();

const IDENTITY: &str = "https://app.example/users/123";
const CLASS: &str = "https://app.example/policies/Employee";

// This models context construction after a host has verified and authorized
// the issuer. It is not a replacement for the server's token verifier.
fn authorization(claims: &EventsTokenPayload) -> PolicyAuthorization {
    let options = match &claims.fluree_policy {
        Some(PolicyClaim::Fixed(policy)) => GovernanceOptions {
            identity: claims.resolve_identity(),
            policy_class: policy.policy_class.clone(),
            policy: policy.policy.clone(),
            policy_values: policy.policy_values.clone(),
            default_allow: policy.default_allow,
        },
        Some(PolicyClaim::Request(_)) => panic!("this benchmark measures fixed selections"),
        None => GovernanceOptions {
            identity: claims.resolve_identity(),
            policy_class: Some(vec![CLASS.into()]),
            ..Default::default()
        },
    };
    PolicyAuthorization::from_trusted_options(options)
}

fn bind_request(claims: &str, request: &Value) -> Value {
    let claims: EventsTokenPayload = serde_json::from_str(claims).unwrap();
    let authorization = authorization(&claims);
    let mut request = request.clone();
    authorization.apply_to_jsonld(&mut request).unwrap();
    request
}

fn memory<T>(scenario: &str, mut operation: impl FnMut() -> T) {
    // Warm one invocation before measuring. No concurrent workloads or
    // Criterion machinery run inside this allocation measurement.
    drop(black_box(operation()));
    let base = fluree_bench_alloc::reset_peak();
    let result = black_box(operation());
    let metrics = fluree_bench_alloc::snapshot();
    drop(result);
    eprintln!(
        "{scenario}: allocated={}B peak={}B per operation",
        metrics.total_allocated_bytes,
        metrics.peak_bytes.saturating_sub(base)
    );
}

fn bench_authorization(c: &mut Criterion) {
    let ordinary = json!({
        "iss": "https://issuer.example", "sub": IDENTITY,
        "aud": "fluree-production", "exp": u64::MAX,
        "fluree.ledger.read.ledgers": ["customer/data:main"]
    });
    let mut delegated = ordinary.clone();
    delegated["fluree.policy"] = json!({"policy-class": [CLASS]});
    let claims = [
        ("ordinary", ordinary.to_string()),
        ("delegated", delegated.to_string()),
    ];
    let ordinary_parsed: EventsTokenPayload = serde_json::from_str(&claims[0].1).unwrap();
    let delegated_parsed: EventsTokenPayload = serde_json::from_str(&claims[1].1).unwrap();
    let auth = authorization(&ordinary_parsed);

    let mut parse = c.benchmark_group("policy_claims_parse");
    for (mode, claims) in &claims {
        memory(&format!("policy_claims_parse/{mode}"), || {
            serde_json::from_str::<EventsTokenPayload>(claims).unwrap()
        });
        parse.bench_function(*mode, |b| {
            b.iter(|| serde_json::from_str::<EventsTokenPayload>(black_box(claims)).unwrap());
        });
    }
    parse.finish();

    let simple =
        json!({"select": ["?name"], "where": {"@id": "?s", "http://schema.org/name": "?name"}});
    let mut multi = simple.clone();
    multi["from"] = json!((0..8)
        .map(|i| json!({
            "@id": format!("customer/data{i}:main"), "policy": {"default-allow": true}
        }))
        .collect::<Vec<_>>());
    for (shape, request) in [("simple", simple), ("eight_sources", multi)] {
        // Correctness prerequisite: both modes enforce the exact same grants
        // over the exact same request, including clearing source overrides.
        let mut normal = request.clone();
        auth.apply_to_jsonld(&mut normal).unwrap();
        let mut delegated = request.clone();
        authorization(&delegated_parsed)
            .apply_to_jsonld(&mut delegated)
            .unwrap();
        assert_eq!(normal, delegated);

        let mut binding = c.benchmark_group("policy_request_binding");
        memory(&format!("policy_request_binding/clone/{shape}"), || {
            request.clone()
        });
        binding.bench_with_input(BenchmarkId::new("clone", shape), &request, |b, q| {
            b.iter(|| black_box(q).clone());
        });
        memory(
            &format!("policy_request_binding/clone_and_bind/{shape}"),
            || {
                let mut q = request.clone();
                auth.apply_to_jsonld(&mut q).unwrap();
                q
            },
        );
        binding.bench_with_input(
            BenchmarkId::new("clone_and_bind", shape),
            &request,
            |b, q| {
                b.iter(|| {
                    let mut q = black_box(q).clone();
                    auth.apply_to_jsonld(&mut q).unwrap();
                    q
                });
            },
        );
        binding.finish();

        let mut full = c.benchmark_group("policy_claims_and_binding");
        for (mode, claims) in &claims {
            memory(&format!("policy_claims_and_binding/{mode}/{shape}"), || {
                bind_request(claims, &request)
            });
            full.bench_with_input(BenchmarkId::new(*mode, shape), &request, |b, q| {
                b.iter(|| bind_request(black_box(claims), black_box(q)));
            });
        }
        full.finish();
    }
}

criterion_group!(benches, bench_authorization);
criterion_main!(benches);
