# Development Setup

This guide walks through setting up a development environment for contributing to Fluree.

## Prerequisites

### Required

**Rust:**
```bash
# Install rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Verify installation
rustc --version  # Should be 1.75.0 or later
cargo --version
```

**Git:**
```bash
git --version  # Should be 2.0 or later
```

### Recommended

**IDE/Editor:**
- Visual Studio Code with rust-analyzer
- IntelliJ IDEA with Rust plugin
- Vim/Neovim with rust-analyzer LSP

**Tools:**
- `cargo-watch` - Auto-rebuild on changes
- `cargo-nextest` - Faster test runner
- `cargo-flamegraph` - Performance profiling

```bash
cargo install cargo-watch cargo-nextest cargo-flamegraph
```

## Clone Repository

```bash
# Clone main repository
git clone https://github.com/fluree/db.git
cd db

# Or clone your fork
git clone https://github.com/YOUR-USERNAME/db.git
cd db
```

## Build from Source

### Development Build

```bash
# Build all crates
cargo build

# Build specific crate
cd fluree-db-query
cargo build
```

### Release Build

```bash
# Optimized build
cargo build --release

# Server binary at: target/release/fluree-db-server
```

### Build Server Only

```bash
cargo build --release --bin fluree-db-server
```

## Run Development Server

### Quick Start

```bash
# Run with default settings (memory storage)
cargo run --bin fluree-db-server
```

Server starts on http://localhost:8090

### With Custom Settings

```bash
cargo run --bin fluree-db-server -- \
  --storage file \
  --data-dir ./dev-data \
  --log-level debug
```

### Watch Mode

Auto-rebuild and restart on changes:

```bash
cargo watch -x 'run --bin fluree-db-server'
```

## Run Tests

### All Tests

```bash
cargo test --all
```

### Specific Crate Tests

```bash
cd fluree-db-query
cargo test
```

### Specific Test

```bash
cargo test test_query_execution
```

### With Output

```bash
cargo test -- --nocapture
```

### Integration Tests

```bash
cargo test --test integration_tests
```

### With Nextest (Faster)

```bash
cargo nextest run
```

## IDE Setup

### Visual Studio Code

**Install Extensions:**
- rust-analyzer
- CodeLLDB (debugging)
- Even Better TOML

**Settings (.vscode/settings.json):**
```json
{
  "rust-analyzer.cargo.features": "all",
  "rust-analyzer.checkOnSave.command": "clippy",
  "rust-analyzer.inlayHints.enable": true
}
```

**Launch Config (.vscode/launch.json):**
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "lldb",
      "request": "launch",
      "name": "Debug server",
      "cargo": {
        "args": ["build", "--bin=fluree-db-server"],
        "filter": {
          "name": "fluree-db-server",
          "kind": "bin"
        }
      },
      "args": ["--storage", "memory", "--log-level", "debug"],
      "cwd": "${workspaceFolder}"
    }
  ]
}
```

### IntelliJ IDEA

**Install Plugin:**
- Rust plugin (official)

**Configure:**
- File → Settings → Languages & Frameworks → Rust
- Set toolchain location
- Enable external linter (clippy)

### Vim/Neovim

**Install rust-analyzer:**

For Neovim with built-in LSP:
```lua
-- init.lua
require'lspconfig'.rust_analyzer.setup{}
```

For Vim with CoC:
```vim
" Install coc-rust-analyzer
:CocInstall coc-rust-analyzer
```

## Development Workflow

### Make Changes

```bash
# Create branch
git checkout -b feature/my-feature

# Edit code
vim fluree-db-query/src/execute.rs

# Format
cargo fmt

# Check
cargo clippy
```

### Test Changes

```bash
# Run affected tests
cargo test -p fluree-db-query

# Run all tests
cargo test --all
```

### Verify Build

```bash
# Development build
cargo build

# Release build
cargo build --release

# Check all features compile
cargo build --all-features
```

### Run Server Locally

```bash
cargo run --bin fluree-db-server -- \
  --storage memory \
  --log-level debug
```

Test your changes:
```bash
# In another terminal
curl http://localhost:8090/health

curl -X POST http://localhost:8090/v1/fluree/query -d '{...}'
```

## Debugging

### With rust-lldb

```bash
# Build with debug symbols
cargo build

# Run with lldb
rust-lldb target/debug/fluree-db-server

# Set breakpoint
(lldb) b fluree_db_query::execute::execute_query
(lldb) run --storage memory

# Debug commands
(lldb) continue
(lldb) step
(lldb) print variable_name
```

### With VS Code

Use launch.json configuration from above, then F5 to debug.

### Print Debugging

```rust
// Quick debugging
println!("Debug: value = {:?}", value);

// Better: use tracing
tracing::debug!(?value, "Processing query");
```

### Logging

Enable debug logs:

```bash
RUST_LOG=debug cargo run --bin fluree-db-server
```

Or trace specific module:

```bash
RUST_LOG=fluree_db_query=trace cargo run --bin fluree-db-server
```

## Performance Profiling

### Criterion Benchmarks

Run benchmarks:

```bash
cargo bench
```

View results: `target/criterion/report/index.html`

### Flamegraphs

Generate flamegraph:

```bash
# Install tools (Linux)
sudo apt install linux-tools-common linux-tools-generic

# Generate flamegraph
cargo flamegraph --bin fluree-db-server

# Open flamegraph.svg in browser
```

### perf (Linux)

```bash
# Record
cargo build --release
perf record -g target/release/fluree-db-server

# Report
perf report
```

## Common Development Tasks

### Add New Query Feature

1. Add to query parser (fluree-db-query/src/parse/)
2. Add to query executor (fluree-db-query/src/execute/)
3. Add tests (fluree-db-query/tests/)
4. Update documentation (docs/query/)

### Add New Transaction Feature

1. Add to transaction parser (fluree-db-transact/src/parse/)
2. Add to staging logic (fluree-db-transact/src/stage.rs)
3. Add tests (fluree-db-transact/tests/)
4. Update documentation (docs/transactions/)

### Add New Storage Backend

1. Implement Storage trait (fluree-db-storage/src/)
2. Add backend-specific logic
3. Add tests
4. Update configuration options
5. Document in docs/operations/storage.md

## Code Organization

### Module Structure

```text
fluree-db-query/
├── src/
│   ├── lib.rs           # Public API and re-exports
│   ├── triple.rs        # TriplePattern, Ref, Term, DatatypeConstraint
│   ├── parse/           # Query parsing
│   │   ├── mod.rs
│   │   ├── ast.rs       # Unresolved AST (before IRI resolution)
│   │   ├── lower.rs     # AST → IR lowering
│   │   └── node_map.rs  # JSON-LD node-map → AST
│   ├── execute/         # Query execution
│   │   ├── mod.rs
│   │   ├── runner.rs
│   │   ├── operator_tree.rs
│   │   └── where_plan.rs  # WHERE-clause planning (pattern types, reordering)
│   ├── bind.rs          # Variable binding
│   └── filter.rs        # Filter evaluation
├── tests/               # Integration tests
└── benches/             # Benchmarks
```

### Import Organization

```rust
// Standard library
use std::collections::HashMap;

// External crates
use serde::{Deserialize, Serialize};

// Internal crates
use fluree_db_common::{Iri, Literal};

// Current crate
use crate::parse::Query;
```

## Documentation

### Code Documentation

Use Rustdoc:

```rust
/// Executes a query against a dataset.
///
/// This function parses the query, generates an execution plan,
/// and runs the plan against the dataset's indexes.
///
/// # Arguments
///
/// * `dataset` - The dataset to query
/// * `query` - The query to execute
///
/// # Returns
///
/// A vector of solutions (variable bindings)
///
/// # Errors
///
/// Returns error if query is invalid or execution fails
///
/// # Examples
///
/// ```
/// use fluree_db_api::query;
///
/// let results = query(&dataset, &query)?;
/// assert_eq!(results.len(), 10);
/// ```
pub fn query(dataset: &Dataset, query: &Query) -> Result<Vec<Solution>> {
    // Implementation
}
```

Generate docs:

```bash
cargo doc --open
```

### User Documentation

Update relevant docs in `docs/` directory when adding user-facing features.

## Dependencies

### Adding Dependencies

Add to Cargo.toml:

```toml
[dependencies]
serde = { version = "1.0", features = ["derive"] }
tokio = { version = "1.35", features = ["full"] }
```

### Updating Dependencies

```bash
# Update all dependencies
cargo update

# Update specific dependency
cargo update -p serde
```

### Checking for Outdated

```bash
cargo install cargo-outdated
cargo outdated
```

## Troubleshooting Development Issues

### Build Fails

```bash
# Clean and rebuild
cargo clean
cargo build
```

### Tests Fail

```bash
# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_name -- --nocapture
```

### Clippy Warnings

```bash
# Fix automatically where possible
cargo clippy --fix
```

### rustfmt Issues

```bash
# Format all code
cargo fmt
```

## Development Tools

### Cargo Commands

```bash
cargo build          # Build
cargo test           # Test
cargo run            # Run
cargo bench          # Benchmark
cargo doc            # Documentation
cargo clean          # Clean
cargo check          # Quick check (no binary)
cargo clippy         # Lint
cargo fmt            # Format
```

### Useful Cargo Plugins

```bash
# Install useful plugins
cargo install cargo-watch      # Auto-rebuild
cargo install cargo-nextest    # Faster tests
cargo install cargo-outdated   # Check deps
cargo install cargo-audit      # Security audit
cargo install cargo-expand     # Expand macros
```

## Performance Tips

### Development Builds

Use development builds during development:
- Faster compilation
- Slower execution
- Debug symbols included

### Release Builds

Use release builds for testing performance:

```bash
cargo build --release
cargo test --release
```

### Link Time Optimization

For maximum performance:

```toml
[profile.release]
lto = true
codegen-units = 1
```

Warning: Significantly slower compile times.

## Related Documentation

- [Tests](tests.md) - Testing guide
- [Graph Identities and Naming](../reference/graph-identities.md) - Naming conventions
- [Crate Map](../reference/crate-map.md) - Code architecture
