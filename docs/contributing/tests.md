# Tests

This guide covers testing practices, test organization, and how to run tests in the Fluree codebase.

## Test Organization

### Unit Tests

Tests in the same file as code:

```rust
// src/query.rs
pub fn execute_query(query: &Query) -> Result<Vec<Solution>> {
    // Implementation
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_query() {
        let query = Query::parse("SELECT ?s WHERE { ?s ?p ?o }").unwrap();
        let results = execute_query(&query).unwrap();
        assert!(!results.is_empty());
    }
}
```

### Integration Tests

Tests in `tests/` directory:

```rust
// tests/integration_test.rs
use fluree_db_api::{Dataset, query};

#[test]
fn test_query_workflow() {
    let dataset = Dataset::new_memory();
    
    // Insert data
    dataset.transact(test_data()).unwrap();
    
    // Query data
    let results = query(&dataset, test_query()).unwrap();
    
    // Verify
    assert_eq!(results.len(), 5);
}
```

### Example Tests

Tests in `examples/`:

```rust
// examples/basic_query.rs
fn main() -> Result<()> {
    let dataset = Dataset::new_memory();
    dataset.transact(sample_data())?;
    let results = dataset.query(sample_query())?;
    println!("Results: {:?}", results);
    Ok(())
}
```

Run with:
```bash
cargo run --example basic_query
```

## Running Tests

### All Tests

```bash
cargo test --all
```

### Opt-in LocalStack (S3/DynamoDB) tests

Some AWS/S3 tests are intentionally **opt-in** and will not run during typical `cargo test` runs.
They require Docker and start LocalStack automatically.

```bash
cargo test -p fluree-db-connection --features aws-testcontainers --test aws_testcontainers_test -- --nocapture
```

### Specific Crate

```bash
cargo test -p fluree-db-query
```

### Specific Test

```bash
cargo test test_query_execution
```

### With Output

```bash
cargo test -- --nocapture
```

### Integration Tests Only

```bash
cargo test --test '*'
```

### Doc Tests

```bash
cargo test --doc
```

### With Nextest (Faster)

```bash
cargo nextest run
```

## Writing Tests

### Unit Test Example

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_query() {
        let input = r#"{"select": ["?s"], "where": [{"@id": "?s"}]}"#;
        let query = parse_query(input).unwrap();
        
        assert_eq!(query.select_vars.len(), 1);
        assert_eq!(query.where_patterns.len(), 1);
    }

    #[test]
    fn test_parse_invalid_query() {
        let input = "invalid json";
        let result = parse_query(input);
        
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParseError::InvalidJson));
    }
}
```

### Integration Test Example

```rust
// tests/it_query.rs
use fluree_db_api::*;

#[tokio::test]
async fn test_basic_query() {
    // Setup
    let dataset = Dataset::new_memory().await.unwrap();
    
    // Insert test data
    let txn = r#"{
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    }"#;
    dataset.transact(txn).await.unwrap();
    
    // Execute query
    let query = r#"{
        "from": "test:main",
        "select": ["?name"],
        "where": [{"@id": "?s", "ex:name": "?name"}]
    }"#;
    let results = dataset.query(query).await.unwrap();
    
    // Verify
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["name"], "Alice");
}
```

### Async Tests

Use tokio test runtime:

```rust
#[tokio::test]
async fn test_async_operation() {
    let result = async_function().await.unwrap();
    assert_eq!(result, expected);
}
```

### Property-Based Tests

Use proptest for property-based testing:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_parse_roundtrip(s in "\\PC*") {
        let iri = Iri::parse(&s)?;
        let serialized = iri.to_string();
        let reparsed = Iri::parse(&serialized)?;
        assert_eq!(iri, reparsed);
    }
}
```

## Test Fixtures

### Test Data

Create reusable test data:

```rust
// tests/fixtures/mod.rs
pub fn sample_person_data() -> &'static str {
    r#"{
        "@context": {"schema": "http://schema.org/"},
        "@graph": [
            {"@id": "ex:alice", "@type": "schema:Person", "schema:name": "Alice"},
            {"@id": "ex:bob", "@type": "schema:Person", "schema:name": "Bob"}
        ]
    }"#
}

pub fn sample_query() -> &'static str {
    r#"{
        "select": ["?name"],
        "where": [{"@id": "?p", "schema:name": "?name"}]
    }"#
}
```

Use in tests:

```rust
#[test]
fn test_with_fixtures() {
    let dataset = Dataset::new_memory();
    dataset.transact(fixtures::sample_person_data()).unwrap();
    let results = dataset.query(fixtures::sample_query()).unwrap();
    assert_eq!(results.len(), 2);
}
```

### Test Helpers

```rust
// tests/helpers/mod.rs
pub async fn setup_test_dataset() -> Dataset {
    let dataset = Dataset::new_memory().await.unwrap();
    dataset.transact(sample_data()).await.unwrap();
    dataset
}

pub fn assert_query_results(results: &[Solution], expected: &[(&str, &str)]) {
    assert_eq!(results.len(), expected.len());
    for (result, (var, value)) in results.iter().zip(expected) {
        assert_eq!(result.get(var).unwrap().to_string(), *value);
    }
}
```

## Test Categories

### Fast Tests

Quick unit tests:

```rust
#[test]
fn test_fast_operation() {
    // < 1ms execution
}
```

### Slow Tests

Tests that take longer:

```rust
#[test]
#[ignore]  // Ignored by default
fn test_slow_operation() {
    // > 1s execution
}
```

Run slow tests:
```bash
cargo test -- --ignored
```

### Integration Tests

End-to-end workflows:

```rust
// tests/it_full_workflow.rs
#[tokio::test]
async fn test_complete_workflow() {
    let dataset = setup_test_dataset().await;
    
    // Multiple operations
    transact_initial_data(&dataset).await;
    query_and_verify(&dataset).await;
    update_data(&dataset).await;
    query_history(&dataset).await;
}
```

## Benchmarking

### Criterion Benchmarks

Create benchmarks:

```rust
// benches/query_bench.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fluree_db_query::*;

fn benchmark_query_execution(c: &mut Criterion) {
    let dataset = setup_benchmark_dataset();
    let query = parse_query(QUERY).unwrap();
    
    c.bench_function("query execution", |b| {
        b.iter(|| {
            execute_query(black_box(&dataset), black_box(&query))
        });
    });
}

criterion_group!(benches, benchmark_query_execution);
criterion_main!(benches);
```

Run benchmarks:

```bash
cargo bench
```

### Comparison Benchmarks

Compare different approaches:

```rust
fn benchmark_approaches(c: &mut Criterion) {
    let mut group = c.benchmark_group("approach_comparison");
    
    group.bench_function("approach_1", |b| {
        b.iter(|| approach_1(black_box(&data)))
    });
    
    group.bench_function("approach_2", |b| {
        b.iter(|| approach_2(black_box(&data)))
    });
    
    group.finish();
}
```

## Continuous Integration

### GitHub Actions

Tests run automatically on:
- Pull requests
- Commits to main
- Scheduled (nightly)

Workflow: `.github/workflows/test.yml`

```yaml
name: Tests

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - run: cargo test --all
      - run: cargo clippy -- -D warnings
      - run: cargo fmt -- --check
```

### Pre-commit Checks

Run before committing:

```bash
#!/bin/bash
# .git/hooks/pre-commit

cargo fmt --check || exit 1
cargo clippy -- -D warnings || exit 1
cargo test --all || exit 1
```

Make executable:
```bash
chmod +x .git/hooks/pre-commit
```

## Test Best Practices

### 1. Test One Thing

Each test should verify one behavior:

Good:
```rust
#[test]
fn test_query_returns_correct_count() {
    let results = query(&dataset, &query).unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_query_returns_correct_values() {
    let results = query(&dataset, &query).unwrap();
    assert_eq!(results[0]["name"], "Alice");
}
```

Bad:
```rust
#[test]
fn test_query() {
    let results = query(&dataset, &query).unwrap();
    assert_eq!(results.len(), 5);
    assert_eq!(results[0]["name"], "Alice");
    assert_eq!(results[1]["name"], "Bob");
    // Too many assertions
}
```

### 2. Use Descriptive Names

```rust
#[test]
fn test_query_with_filter_returns_only_matching_results() {
    // Clear what's being tested
}
```

### 3. Arrange-Act-Assert

Structure tests clearly:

```rust
#[test]
fn test_example() {
    // Arrange: Setup
    let dataset = setup_test_dataset();
    let query = parse_query(TEST_QUERY);
    
    // Act: Execute
    let results = execute_query(&dataset, &query).unwrap();
    
    // Assert: Verify
    assert_eq!(results.len(), 3);
}
```

### 4. Test Error Cases

```rust
#[test]
fn test_invalid_query_returns_error() {
    let result = parse_query("invalid");
    assert!(result.is_err());
}

#[tokio::test]
async fn test_missing_ledger_returns_ledger_not_found() {
    let result = fluree.ledger("nonexistent:main").await;
    assert!(matches!(result.unwrap_err(), Error::LedgerNotFound(_)));
}
```

### 5. Avoid Flaky Tests

Don't depend on:
- Timing
- Random values (use seeded RNG)
- External services
- File system state

### 6. Clean Up Resources

```rust
#[test]
fn test_with_temp_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let file_path = temp_dir.path().join("test.db");
    
    // Test with file_path
    
    // temp_dir automatically cleaned up
}
```

### 7. Use Test Utilities

```rust
// tests/common/mod.rs
pub fn assert_solution_contains(solutions: &[Solution], var: &str, value: &str) {
    let found = solutions.iter().any(|s| {
        s.get(var).map(|v| v.to_string() == value).unwrap_or(false)
    });
    assert!(found, "Expected to find {}={} in results", var, value);
}
```

## W3C SPARQL Compliance Tests

The `testsuite-sparql` crate runs official W3C SPARQL test cases against Fluree's parser and query engine. Tests are discovered automatically from W3C manifest files — zero hand-written test cases.

```bash
# Run all W3C SPARQL tests
cargo test -p testsuite-sparql

# Run with verbose output
cargo test -p testsuite-sparql -- --nocapture 2>&1
```

The suite covers SPARQL 1.0 and 1.1 syntax tests (293 tests) plus query evaluation tests across 12 categories (233 tests). Eval tests are `#[ignore]`'d by default — run with `--include-ignored` or via `make test-eval` in `testsuite-sparql/`.

For the full guide on interpreting results, debugging failures, and contributing fixes, see the [W3C SPARQL Compliance Suite](sparql-compliance.md) guide.

## Test Coverage

### Generate Coverage Report

Using tarpaulin:

```bash
cargo install cargo-tarpaulin

cargo tarpaulin --out Html --output-dir coverage/
```

View: `coverage/index.html`

### Coverage Goals

- Core functionality: 90%+ coverage
- Edge cases: Tested
- Error paths: Tested
- Public APIs: 100% covered

## Related Documentation

- [Dev Setup](dev-setup.md) - Development environment
- [Graph Identities and Naming](../reference/graph-identities.md) - Naming conventions
- [Contributing](README.md) - Contribution guidelines
