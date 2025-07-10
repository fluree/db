# SCI Integration for GraalVM Compatibility

## Project Overview

This project aims to replace `eval` usage in Fluree DB with SCI (Small Clojure Interpreter) to achieve GraalVM native image compatibility. The main blocker for GraalVM compilation is the use of `eval` for dynamic query function compilation.

## Current Status

**Phase**: Implementation in progress - debugging symbol resolution issues
**Branch**: docs/update (continuing from previous work)
**Key File**: `/src/fluree/db/query/exec/eval.cljc`

## Problem Analysis

### GraalVM Compatibility Issues
1. **Primary Issue**: `eval` function calls prevent GraalVM native image compilation
2. **Location**: Query function compilation in `fluree.db.query.exec.eval` namespace
3. **Impact**: Dynamic query expressions cannot be compiled to native code
4. **Solution**: Replace `eval` with SCI (Small Clojure Interpreter)

### Technical Requirements
- Maintain existing query functionality
- Support all current query functions and operators
- Preserve dynamic code generation capabilities
- Ensure backward compatibility

## Implementation Progress

### Completed Tasks
1. **Dependency Addition**: Added SCI dependency to `deps.edn` (now using `org.babashka/sci {:mvn/version "0.10.47"}`)
2. **SCI Context Creation**: Implemented `create-sci-context` function
3. **Function Mapping**: Created comprehensive symbol mapping from `qualified-symbols`
4. **Code Integration**: Modified `compile` and `compile-filter` functions to use SCI

### Current Implementation

```clojure
;; SCI context for GraalVM-compatible code evaluation
(defn create-sci-context []
  (let [eval-fns (into {} (map (fn [[k v]]
                                [(symbol (name k)) (resolve v)])
                              qualified-symbols))
        all-fns (merge eval-fns
                       {'->typed-val where/->typed-val})]
    (sci/init {:namespaces {'fluree.db.query.exec.eval all-fns
                            'fluree.db.query.exec.where {'->typed-val where/->typed-val}
                            'clojure.core all-fns}})))
```

### Current Issue: Function Syntax Compatibility

**Problem**: Generated query code uses function syntax that's incompatible with SCI's expectations.

**Error**: "Parameter declaration solution should be a vector"

**Root Cause**: SCI generates functions with parameter lists like `[solution]` but our code generates `(fn [solution] ...)` with qualified symbols.

**Progress Made**: 
- Fixed qualified symbol resolution by mapping them in user namespace
- Basic SCI evaluation now works for simple expressions
- Core macros (fn, let) are properly mapped
- The `coerce` function correctly transforms expressions like `(+ 1 2)` to `(fluree.db.query.exec.eval/plus ...)`

**Remaining Issue**: Function parameter syntax compatibility between compile* output and SCI expectations.

## Technical Decisions Made

### 1. SCI Version Update (January 2025)
- **Current Version**: `org.babashka/sci {:mvn/version "0.10.47"}`
- **Previous Version**: `borkdude/sci 0.2.7`
- **Breaking Changes**: The API has changed significantly:
  - `eval-string` now takes options as second arg, not context as first
  - `eval-form` still takes context as first arg
  - Namespace resolution works differently
  - `:allow :all` syntax is no longer valid
- **Current Issue**: Qualified symbols (e.g., `fluree.db.query.exec.eval/plus`) are not resolving in SCI context

### 2. Symbol Mapping Strategy
- **Approach**: Map all functions from `qualified-symbols` to SCI context
- **Implementation**: Convert qualified symbols to unqualified ones in SCI namespaces
- **Benefit**: Preserves existing query function semantics

### 3. Context Structure
- **Namespaces**: 
  - `fluree.db.query.exec.eval` - main query functions
  - `fluree.db.query.exec.where` - helper functions
  - `clojure.core` - core functions
- **Function Resolution**: Use `resolve` to get actual function references

## Next Steps

### Immediate Priority
1. **Fix Symbol Resolution**: Debug why qualified symbols aren't resolving in SCI context
2. **Test Simple Cases**: Verify basic arithmetic operations work
3. **Incremental Testing**: Test each category of functions (math, string, logic, etc.)

### Investigation Areas
1. **Namespace Mapping**: Ensure qualified symbols map correctly to SCI namespaces
2. **Function References**: Verify all function references resolve properly
3. **Context Initialization**: Check if SCI context is properly initialized

### Testing Strategy
1. **Unit Tests**: Test individual query functions work with SCI
2. **Integration Tests**: Test complete query compilation and execution
3. **Regression Tests**: Ensure existing query functionality is preserved

## Files Modified

### Core Implementation
- `/src/fluree/db/query/exec/eval.cljc` - Main SCI integration
- `/deps.edn` - Added SCI dependency

### Test Files
- `/test/fluree/db/query/exec/eval_test.clj` - Contains existing function tests

## Key Functions Affected

### Modified Functions
- `compile` - Now uses SCI instead of eval
- `compile-filter` - Now uses SCI instead of eval
- `create-sci-context` - New function for SCI context creation

### Function Categories to Support
- **Math**: `+`, `-`, `*`, `/`, `abs`, `round`, etc.
- **Comparison**: `=`, `<`, `>`, `<=`, `>=`, `equal`, `not-equal`
- **Logic**: `and`, `or`, `not`, `if`
- **String**: `concat`, `contains`, `strStarts`, `strEnds`, etc.
- **Date/Time**: `now`, `year`, `month`, `day`, `hours`, etc.
- **Aggregation**: `sum`, `avg`, `count`, `max`, `min`, etc.
- **RDF**: `iri`, `lang`, `datatype`, `is-iri`, `is-literal`
- **Vector**: `dotProduct`, `cosineSimilarity`, `euclideanDistance`

## Design Principles

1. **Backward Compatibility**: All existing queries must continue to work
2. **Performance**: SCI should not significantly impact query performance
3. **Maintainability**: Code should remain readable and debuggable
4. **Completeness**: All query functions must be supported in SCI context

## Risk Assessment

### Low Risk
- SCI is a mature, well-tested library
- Function mapping is straightforward
- Limited scope of changes

### Medium Risk
- Symbol resolution complexity
- Potential performance impact
- Edge cases in complex queries

### Mitigation
- Comprehensive testing strategy
- Incremental implementation
- Fallback to eval for debugging (development only)

## Success Criteria

1. **All Tests Pass**: Existing query tests continue to pass
2. **GraalVM Compatibility**: Code compiles to native image
3. **Performance**: No significant query performance degradation
4. **Functionality**: All query features work identically

## Notes

- The `qualified-symbols` map contains 70+ function mappings
- SCI context is created once per compilation, not per query execution
- Function resolution uses `resolve` to get actual function references
- Context includes necessary namespaces for cross-function calls