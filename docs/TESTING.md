# Testing Guide

This document covers testing strategies and practices for Fluree DB.

## Test Types and Organization

### Test Categories

#### Unit Tests
- **Location**: Throughout `test/` directory
- **Purpose**: Test individual functions and components in isolation
- **Dependencies**: No external services required
- **Meta tags**: None (run by default)
- **Example**: `s3_unit_test.clj` - tests S3 storage creation without external S3

#### Integration Tests  
- **Location**: Throughout `test/` directory
- **Purpose**: Test interactions between components and with external services
- **Dependencies**: May require external services or containers
- **Meta tags**: `^:integration`
- **Example**: Database operations, storage interactions

#### Docker-based Tests
- **Location**: `test/fluree/db/storage/s3_testcontainers_test.clj`
- **Purpose**: Integration tests using TestContainers for external service dependencies
- **Dependencies**: Docker must be running
- **Meta tags**: `^:integration ^:docker`
- **CI/CD**: Excluded from regular runs, can be run weekly or on-demand

#### Pending Tests
- **Purpose**: Tests that are temporarily disabled (e.g., due to infrastructure issues)
- **Meta tags**: `^:pending`
- **CI/CD**: Excluded from all runs until re-enabled

## Running Tests

### Basic Commands
```bash
# Run all tests (excluding docker and pending tests)
make test

# Run only CLJ tests
make cljtest

# Run CLJS tests
make cljstest

# Run specific test namespace
clojure -X:cljtest :kaocha.filter/focus [test-namespace]

# Run specific test function
clojure -X:cljtest :kaocha.filter/focus [namespace/test-function]
```

### Specialized Test Runs

#### Docker Tests
```bash
# Run all docker tests (requires Docker running)
clojure -M:docker-tests -m kaocha.runner

# Run specific docker test
clojure -M:cljtest -m kaocha.runner --focus fluree.db.storage.s3-testcontainers-test
```

#### Pending Tests
```bash
# Run only pending tests (for debugging)
clojure -M:pending-tests -m kaocha.runner
```

### Test Configuration (deps.edn)

The project uses different aliases for different test scenarios:

- `:cljtest` - Regular tests (skips `:pending` and `:docker`)
- `:docker-tests` - Focus only on `:docker` tagged tests  
- `:pending-tests` - Focus only on `:pending` tagged tests

## S3 TestContainers Integration

### Overview
S3 integration tests use TestContainers and LocalStack to provide:
1. Basic S3 operations (bucket creation, file storage/retrieval)
2. Fluree DB operations over S3 (create ledger, stage, commit, query, reload)
3. S3 indexing functionality

### Requirements
- Docker installed and running
- Tests tagged with `^:integration ^:docker`

### Implementation Details

#### Dependencies
```clojure
clj-test-containers/clj-test-containers {:mvn/version "0.7.4"}
org.testcontainers/testcontainers     {:mvn/version "1.19.3"}
org.testcontainers/localstack         {:mvn/version "1.19.3"}
```

#### Key Components
1. **LocalStack Container**: Uses LocalStack 3.0.2 with S3 service
2. **Fixture**: Sets up AWS credentials and manages container lifecycle
3. **Endpoint Configuration**: Properly parses and configures S3 endpoints for AWS client

#### AWS Credentials
- Set via system properties: `aws.accessKeyId`, `aws.secretAccessKey`, `aws.region`
- Uses "test" as access key/secret for LocalStack
- LocalStack doesn't validate credentials by default

### Benefits
1. **No manual setup**: Developers don't need to manually start LocalStack
2. **Isolated testing**: Each test run gets a fresh LocalStack instance
3. **CI/CD friendly**: Can be selectively run in CI/CD environments with Docker support
4. **Real S3 API**: Tests against actual S3 API implementation via LocalStack
5. **Deterministic**: No race conditions or timing issues with external services

## Best Practices

### Test Organization
- **Group related tests**: Keep tests for similar functionality in the same namespace
- **Use descriptive names**: Test names should clearly indicate what is being tested
- **Meta tags**: Use appropriate meta tags for test categorization

### Test Isolation
- **Clean state**: Each test should start with a clean state
- **No dependencies**: Tests should not depend on other tests
- **Resource cleanup**: Use fixtures to ensure proper cleanup

### CI/CD Strategy
- **Fast feedback**: Regular CI runs should be fast (exclude slow docker tests)
- **Comprehensive coverage**: Run full test suite (including docker tests) periodically
- **Selective execution**: Use meta tags to control which tests run in different contexts

### Writing New Tests

#### For Unit Tests
- No external dependencies
- Fast execution
- Test single functions or small components

#### For Integration Tests  
- Test component interactions
- May use in-memory implementations of external services
- Tag with `^:integration`

#### For Docker Tests
- Test against real external services via containers
- Tag with `^:integration ^:docker`
- Ensure Docker is documented as a requirement
- Use TestContainers for lifecycle management

### Debugging Tests

#### Common Issues
1. **Timing issues**: Use proper synchronization instead of `Thread/sleep`
2. **Resource leaks**: Ensure proper cleanup in fixtures
3. **Port conflicts**: Use dynamic ports in container tests
4. **State pollution**: Ensure tests are independent

#### Tools and Techniques
- Use `log/info` for debugging test execution
- Check container logs for Docker-based tests
- Use test-specific identifiers to avoid conflicts
- Run tests in isolation to identify dependencies