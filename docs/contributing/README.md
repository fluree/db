# Contributing

Welcome to the Fluree contributor documentation! This section provides everything you need to contribute to Fluree.

## Getting Started

### [Dev Setup](dev-setup.md)

Set up your development environment:
- Install dependencies
- Clone repository
- Build from source
- Run development server
- IDE configuration

### [Tests](tests.md)

Testing guide:
- Running tests
- Writing tests
- Test organization
- Integration tests
- Benchmarking
- Continuous integration

### [Adding Tracing Spans](tracing-guide.md)

How to instrument new code paths with tracing spans:
- The two-tier span strategy (info / debug / trace)
- Code patterns for sync and async spans
- Deferred field recording
- Testing spans with SpanCaptureLayer
- Common gotchas (`!Send` guards, OTEL floods, etc.)

### [W3C SPARQL Compliance Suite](sparql-compliance.md)

Guide to the manifest-driven W3C compliance test suite:
- Running and interpreting results
- Debugging failures
- From failure to issue/PR workflow
- Using Claude Code for compliance work
- Architecture overview

### [SHACL Implementation](shacl-implementation.md)

How SHACL validation is wired into Fluree, for contributors adding
constraints or fixing bugs:
- Pipeline: compile → cache → validate
- Crate layout (`fluree-db-shacl` / `-transact` / `-api`)
- Shared post-stage helper and its call sites
- Per-graph config, `f:shapesSource`, target-type resolution
- Adding a new constraint (walkthrough)
- Testing patterns (unit + integration + temp-revert regression trick)
- Known gaps (`sh:uniqueLang`, `sh:qualifiedValueShape`, cross-txn cache)


## How to Contribute

### Ways to Contribute

1. **Report Bugs:** File issues with reproduction steps
2. **Suggest Features:** Propose enhancements with use cases
3. **Fix Bugs:** Submit pull requests for bug fixes
4. **Add Features:** Implement new capabilities
5. **Improve Documentation:** Fix typos, clarify explanations, add examples
6. **Review Pull Requests:** Help review others' contributions
7. **Answer Questions:** Help users in discussions

### Before Contributing

1. **Check existing issues:** Search for duplicate issues
2. **Read documentation:** Understand the feature area
3. **Discuss major changes:** Open issue before large PRs
4. **Follow style guide:** Match existing code style
5. **Add tests:** Include tests for new features
6. **Update docs:** Document new features

## Contribution Workflow

### 1. Fork Repository

```bash
# Fork on GitHub, then clone
git clone https://github.com/YOUR-USERNAME/db.git
cd db
```

### 2. Create Branch

```bash
git checkout -b feature/my-feature
```

Branch naming:
- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation
- `refactor/` - Code refactoring
- `test/` - Test additions

### 3. Make Changes

Edit code, following style guidelines.

### 4. Add Tests

```bash
# Run existing tests
cargo test

# Add new tests
# Edit tests/test_my_feature.rs
```

### 5. Run Checks

```bash
# Format code
cargo fmt

# Lint code
cargo clippy

# Run all tests
cargo test --all
```

### 6. Commit Changes

```bash
git add .
git commit -m "Add feature: description"
```

Commit message format:
```text
Short summary (50 chars or less)

More detailed explanation if needed. Wrap at 72 characters.

- Key point 1
- Key point 2

Fixes #123
```

### 7. Push and Create PR

```bash
git push origin feature/my-feature
```

Create pull request on GitHub.

### 8. Address Review Comments

Respond to reviewer feedback, make requested changes.

## Code Style

### Rust Style

Follow Rust standard style:

```bash
# Format all code
cargo fmt

# Check style
cargo clippy
```

### Naming Conventions

**Types:** PascalCase
```rust
struct Dataset { ... }
enum QueryResult { ... }
```

**Functions:** snake_case
```rust
fn execute_query() { ... }
fn parse_json_ld() { ... }
```

**Constants:** SCREAMING_SNAKE_CASE
```rust
const MAX_QUERY_SIZE: usize = 1_048_576;
```

**Modules:** snake_case
```rust
mod query_engine;
mod storage_backend;
```

### Documentation

Document public APIs:

```rust
/// Executes a query against the dataset.
///
/// # Arguments
///
/// * `query` - The query to execute
/// * `context` - Execution context
///
/// # Returns
///
/// Query results or error
///
/// # Examples
///
/// ```
/// let results = dataset.query(&query, &context)?;
/// ```
pub fn query(&self, query: &Query, context: &Context) -> Result<Vec<Solution>> {
    // Implementation
}
```

### Error Handling

Use Result types:

```rust
// Good
pub fn parse_query(input: &str) -> Result<Query, ParseError> {
    // ...
}

// Bad
pub fn parse_query(input: &str) -> Query {
    // No error handling
}
```

### Testing

Write tests for new code:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_execution() {
        let query = Query::parse("...").unwrap();
        let result = execute(&query).unwrap();
        assert_eq!(result.len(), 2);
    }
}
```

## Pull Request Guidelines

### PR Title

Format: `category: short description`

Examples:
- `feat: Add SPARQL property paths support`
- `fix: Correct transaction time ordering`
- `docs: Update query examples`
- `test: Add integration tests for time travel`
- `refactor: Simplify index scan logic`

### PR Description

Include:

1. **Summary:** What does this PR do?
2. **Motivation:** Why is this needed?
3. **Changes:** What changed?
4. **Testing:** How was it tested?
5. **Breaking Changes:** Any breaking changes?

Example:
```markdown
## Summary
Adds support for SPARQL property paths, enabling recursive graph traversal.

## Motivation
Many users need to query hierarchical data structures. Property paths are a standard SPARQL feature.

## Changes
- Added property path parser to fluree-db-sparql
- Implemented path evaluation in query engine
- Added tests for various path patterns

## Testing
- Unit tests for parser
- Integration tests for path queries
- Benchmarks show acceptable performance

## Breaking Changes
None
```

### PR Checklist

- [ ] Code follows style guidelines
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] All tests pass
- [ ] No clippy warnings
- [ ] Commit messages clear
- [ ] PR description complete

## Review Process

### What Reviewers Look For

1. **Correctness:** Does it work as intended?
2. **Tests:** Adequate test coverage?
3. **Style:** Follows conventions?
4. **Documentation:** Properly documented?
5. **Performance:** No obvious performance issues?
6. **Breaking Changes:** Backward compatible?

### Responding to Reviews

- Be receptive to feedback
- Ask questions if unclear
- Make requested changes promptly
- Explain your reasoning when appropriate
- Say thanks for helpful reviews

## Community Guidelines

### Code of Conduct

- Be respectful and inclusive
- Assume good intentions
- Give constructive feedback
- Welcome newcomers
- No harassment or discrimination

### Communication

- **GitHub Issues:** Bug reports, feature requests
- **Pull Requests:** Code contributions
- **Discussions:** Questions, ideas, help

### Getting Help

- Read documentation first
- Search existing issues
- Ask specific questions
- Provide reproduction steps
- Be patient and respectful

## License

Contributions licensed under Apache 2.0.

By contributing, you agree to license your contributions under the same license.

## Recognition

Contributors are recognized in:
- CONTRIBUTORS.md file
- Release notes
- GitHub contributors page

Thank you for contributing to Fluree!

## Related Documentation

- [Dev Setup](dev-setup.md) - Development environment
- [Tests](tests.md) - Testing guide
- [Graph Identities and Naming](../reference/graph-identities.md) - Naming conventions
