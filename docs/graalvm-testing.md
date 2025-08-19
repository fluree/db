# Testing GraalVM and SCI Functionality

## Overview

Fluree DB supports GraalVM native image compilation, which requires special handling for runtime code evaluation. This document explains how to test GraalVM-specific code paths without building a native image.

## Environment Variable

The `FLUREE_GRAALVM_BUILD` environment variable forces the use of GraalVM code paths during regular JVM execution:

```bash
FLUREE_GRAALVM_BUILD=true clojure -M:cljtest -m kaocha.runner --focus your-test
```

## How It Works

### Compile-Time Macros

The `if-graalvm` macro in `src/fluree/db/util/core.cljc` checks for GraalVM at compile time:

```clojure
(defmacro if-graalvm
  [graalvm-branch jvm-branch]
  (if (graalvm-build?)
    graalvm-branch
    jvm-branch))
```

The `graalvm-build?` function checks:
1. If running on GraalVM (via system property)
2. If `FLUREE_GRAALVM_BUILD` environment variable is set

### Code Path Differences

#### JVM Path
- Uses regular Clojure `eval`
- Direct macro expansion
- No special context handling needed

#### GraalVM/SCI Path
- Uses SCI for evaluation
- Transforms certain function calls (e.g., `iri`)
- Requires explicit context passing
- Functions evaluated through `eval-graalvm-with-context`

## Testing Strategies

### 1. Direct SCI Testing

Create tests that directly call SCI evaluation functions:

```clojure
(deftest test-sci-evaluation-directly
  (let [parsed-ctx (json-ld/parse-context {"ex" "http://example.org/"})
        form '(iri "ex:name")
        result (eval/eval-graalvm-with-context form parsed-ctx)]
    (is (= "http://example.org/name" (:value result)))))
```

### 2. Environment Variable Testing

Test the same functionality with the environment variable:

```clojure
;; Run with: FLUREE_GRAALVM_BUILD=true clojure -M:cljtest ...
(deftest test-with-graalvm-env
  ;; This test will use SCI paths when FLUREE_GRAALVM_BUILD=true
  (let [result (fluree/query db query)]
    ;; assertions
    ))
```

### 3. Testing Both Paths

Some tests should verify behavior is consistent across both paths:

```bash
# Test JVM path
clojure -M:cljtest -m kaocha.runner --focus your-test

# Test GraalVM/SCI path  
FLUREE_GRAALVM_BUILD=true clojure -M:cljtest -m kaocha.runner --focus your-test
```

## Common Testing Scenarios

### Testing IRI Function

The `iri` function is particularly important to test because:
- It's a macro in JVM mode
- It requires context transformation in SCI mode
- It's commonly used in queries

Example test pattern:
```clojure
(deftest test-iri-in-filter
  (let [query {"select" ["?s" "?p"]
               "where" [{"@id" "?s" "?p" "?o"}
                       ["filter" "(= ?p (iri \"ex:name\"))"]]
               "@context" {"ex" "http://example.org/"}}]
    ;; Test passes with both JVM and SCI evaluation
    ))
```

### Testing Datatype Functions

Functions like `datatype`, `str`, `lang` need testing in both modes:

```clojure
(deftest test-datatype-filter
  (let [query {"select" ["?s" "?o"]
               "where" [{"@id" "?s" "?p" "?o"}
                       ["bind" "?dt" "(datatype ?o)"]
                       ["filter" "(= ?dt \"http://www.w3.org/2001/XMLSchema#string\")"]]}]
    ;; Verify string values are filtered correctly
    ))
```

## Debugging Tips

### 1. Check Which Path Is Active

Add logging to verify which code path is executing:

```clojure
(if-graalvm
  (log/debug "Using GraalVM/SCI path")
  (log/debug "Using JVM eval path"))
```

### 2. SCI Context Issues

If functions aren't found in SCI:
1. Check if the function is in `qualified-symbols`
2. Verify it's added to the appropriate namespace
3. Ensure it's in `allowed-scalar-fns` if needed

### 3. Context Passing

For context-dependent functions:
1. Ensure context is properly parsed with `json-ld/parse-context`
2. Check that context is passed through the evaluation chain
3. Verify bindings are updated in `eval-graalvm-with-context`

## Running Full Test Suite

To ensure GraalVM compatibility:

```bash
# Run all tests with GraalVM paths
FLUREE_GRAALVM_BUILD=true make test

# Run specific namespace
FLUREE_GRAALVM_BUILD=true clojure -M:cljtest -m kaocha.runner --focus fluree.db.query

# Run with different log levels for debugging
LOG_LEVEL=debug FLUREE_GRAALVM_BUILD=true clojure -M:cljtest -m kaocha.runner --focus your-test
```

### Makefile shortcuts

```bash
# Run the full Clojure test suite with GraalVM/SCI paths enabled
make graaltest

# Run only tests marked ^:sci using the SCI path
make cljtest-sci
```

## CI/CD Considerations

Consider adding CI jobs that:
1. Run tests with `FLUREE_GRAALVM_BUILD=true`
2. Build actual native images and run integration tests
3. Compare results between JVM and native image execution

## Performance Testing

SCI evaluation is slower than regular eval. When testing performance:
1. Use realistic query complexity
2. Test with representative data sizes
3. Compare JVM vs SCI evaluation times
4. Consider caching compiled SCI expressions