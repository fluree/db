# SCI Integration in Fluree DB

## Overview

SCI (Small Clojure Interpreter) is used in Fluree DB to enable runtime code evaluation in GraalVM native images. Since GraalVM doesn't support regular Clojure `eval`, we use SCI to evaluate query expressions, filters, and other dynamic code at runtime.

## Architecture

### SCI Context Creation

The SCI context is created once and stored in a singleton (`sci-context-singleton`) for performance. The context initialization happens in `src/fluree/db/query/exec/eval.cljc` in the `create-sci-context` function.

### Namespace Structure

The SCI context includes several namespaces:

1. **`clojure.core`**: Core Clojure functions needed for query expressions
2. **`fluree.db.query.exec.eval`**: Query evaluation functions like `iri`, `datatype`, `str`, etc.
3. **`fluree.db.query.exec.where`**: Functions for handling typed values and query matching
4. **`fluree.json-ld`**: JSON-LD functions, particularly `expand-iri`
5. **`fluree.db.constants`**: Constants like datatype IRIs
6. **`user`**: The default evaluation namespace

### The User Namespace

The `user` namespace is special - it contains:
- All functions from `fluree.db.query.exec.eval` (unqualified)
- Additional qualified symbols that might be used in queries
- The `iri` function (both qualified and unqualified versions)

When SCI evaluates expressions without an explicit namespace (like filter expressions in queries), they execute in the `user` namespace by default. This is why most functions are duplicated there.

### Why Multiple Namespaces?

Even though many functions are available in `user`, we still need the other namespaces for:
- **Qualified symbol resolution**: Expressions like `where/->typed-val` need the `where` namespace
- **Namespace isolation**: Keeps the code organized as in regular Clojure
- **Compatibility**: Allows both qualified and unqualified symbol usage

## Key Functions and Patterns

### The `iri` Function

The `iri` function is particularly complex because it needs access to the query context at runtime. In regular Clojure, it's a macro that accesses a context variable. In SCI:

1. A default `iri-fn` is registered that throws an error if called
2. When evaluating with context, we create a context-aware version
3. This version is injected into the SCI namespaces via `eval-graalvm-with-context`

### Context Injection Pattern

```clojure
(defn eval-graalvm-with-context
  [form ctx]
  (let [iri-with-context (fn [input] 
                          ;; context-aware implementation
                          ...)
        updated-namespaces (-> current-namespaces
                              (assoc-in ['user 'iri] iri-with-context)
                              ;; ... other updates
                              )]
    (sci/eval-form (sci/merge-opts @sci-context-singleton
                                   {:namespaces updated-namespaces
                                    :bindings {'$-CONTEXT ctx}}))))
```

### Macro Replacements

Several Clojure macros are replaced with function equivalents for SCI:
- `if` → `-if-fn`
- `and` → `-and-fn`
- `or` → `-or-fn`
- `as` → `as-fn`

These replacements handle the special evaluation semantics of the original macros.

## Symbol Resolution

### Qualified Symbols

During macro expansion, symbols often get namespace-qualified. The `qualified-symbols` map in eval.cljc maps these qualified symbols to their implementations:

```clojure
'fluree.db.query.exec.eval/iri 'iri
'fluree.db.query.exec.eval/datatype 'datatype
;; etc.
```

### Transform Functions

For GraalVM builds, `transform-iri-calls` walks the code and transforms `(iri x)` calls to `(iri-fn-base x $-CONTEXT)` to make the context explicit.

## Adding New Functions

To add a new function to the SCI context:

1. Add it to `qualified-symbols` if it needs qualified symbol support
2. Add it to the appropriate namespace functions map (e.g., `eval-ns-fns`)
3. If it needs to be available unqualified, ensure it's in the `user` namespace
4. If it's a scalar function, add it to `allowed-scalar-fns`
5. For functions needing special context handling, follow the `iri` pattern

## Debugging SCI Issues

Common issues and solutions:

1. **"Could not resolve symbol"**: The function isn't registered in SCI context
2. **"X is not a function"**: The symbol maps to a non-function value
3. **Context-related errors**: The function needs runtime context injection
4. **Namespace resolution failures**: Check both qualified and unqualified symbol mappings

## Performance Considerations

- The SCI context is created once and reused (singleton pattern)
- Namespace updates create new contexts via `sci/merge-opts`
- Bindings are preferred over namespace updates when possible
- Context injection happens per-evaluation, so minimize overhead