# Fluree DB

Usage [documentation](https://docs.fluree.com) is located at https://docs.fluree.com.

## API Reference

Comprehensive API documentation is available:
- Generated docs: Run `make docs` to generate HTML documentation from docstrings
- Source: See `src/fluree/db/api.cljc` for detailed function documentation

### Main API Functions

**Connection Management:**
- `connect` - create connection from JSON-LD config
- `connect-memory` - create in-memory connection
- `connect-file` - create file-based connection
- `disconnect` - terminate connection and release resources

**Ledger Operations:**
- `create` - create new ledger with empty commit
- `load` - load existing ledger
- `drop` - delete ledger and data
- `exists?` - check if ledger exists

**Data Modification:**
- `insert` / `insert!` - stage/commit new entities
- `upsert` / `upsert!` - stage/commit insert-or-update entities
- `update` / `update!` - stage/commit updates
- `commit!` - persist staged changes

**Querying:**
- `query` - execute queries against database
- `query-connection` - query using connection's engine
- `history` - query entity history across commits
- `db` - get current database value

**Policy & Permissions:**
- `wrap-policy` - apply policy restrictions
- `wrap-identity-policy` - apply identity-based policies
- `credential-query` - query with verifiable credentials

**Reasoning:**
- `reason` - apply reasoning rules (datalog, OWL2RL)
- `reasoned-facts` - get inferred facts

Note: Functions ending with `!` perform operations and commit atomically.

## Overview

Fluree is an immutable, temporal, ledger-backed semantic graph database that has a cloud-native architecture.

This repository contains the core Fluree database library. It can be:
- Embedded in Clojure applications as a library
- Used in JavaScript environments (Node.js, browsers, web workers)
- Run as a standalone service
- Integrated with the [Fluree Server](https://github.com/fluree/server) for HTTP API access

Fluree supports multiple deployment patterns:
- Embedded: Direct integration in your application
- Microservice: Standalone database service
- Browser: Client-side database with web worker support
- Serverless: Distributed architecture with policy-based security

Key features include:
- **Immutable & Temporal**: Complete transaction history with time-travel queries
- **Semantic Graph**: RDF/JSON-LD native with SPARQL support
- **Policy-Based Security**: Data-level permissions and verifiable credentials
- **Reasoning**: Built-in datalog and OWL2RL reasoning engines
- **Multi-Format**: Supports JSON-LD, Turtle, and SPARQL

The best way to get started with Fluree is to visit the [documentation](https://docs.fluree.com).

## Quick Start

```clojure
(require '[fluree.db.api :as fluree])

;; Create an in-memory connection
(def conn @(fluree/connect-memory))

;; Create a ledger
(def ledger @(fluree/create conn "my-ledger"))

;; Insert some data
(def db @(fluree/insert! conn "my-ledger" 
                        [{"@id" "ex:alice"
                          "@type" "Person"
                          "name" "Alice"
                          "age" 30}]))

;; Query the data
(def results @(fluree/query db {"@context" {"ex" "http://example.com/"}
                               "select" "*"
                               "where" {"@type" "Person"}}))

;; Clean up
@(fluree/disconnect conn)
```

## Development

### Contributing

All contributors must complete a [Contributor License Agreement](https://cla-assistant.io/fluree/).

### Prerequisites

1. **Clojure**: Install Clojure CLI tools (version 1.11+ recommended)
   - macOS: `brew install clojure/tools/clojure`
   - Arch Linux: `pacman -S clojure`
   - Other Linux: See [official installation guide](https://clojure.org/guides/install_clojure)
   - Windows: See [official installation guide](https://clojure.org/guides/install_clojure)

2. **Node.js & npm**: Required for JavaScript builds and tests
   - macOS: `brew install node`
   - Arch Linux: `pacman -S nodejs npm`
   - Other Linux: `sudo apt install nodejs npm` (Ubuntu/Debian) or equivalent
   - Windows: Download from [nodejs.org](https://nodejs.org/)

3. **Java**: JDK 11+ required (see `.java-version` for current target)
   - macOS: `brew install openjdk@11`
   - Arch Linux: `pacman -S jdk-openjdk`
   - Other Linux: `sudo apt install openjdk-11-jdk` or equivalent
   - Windows: Download from [Oracle](https://www.oracle.com/java/technologies/downloads/) or [OpenJDK](https://openjdk.org/)
   
   **Note**: Builds should use the Java version specified in `.java-version` (currently 11.0). 
   If you use a Java version manager like [jenv](https://github.com/jenv/jenv) or [SDKMAN!](https://sdkman.io/), 
   it will automatically use the correct version.

4. **Additional tools** (optional but recommended):
   - `clj-kondo` for linting: `brew install borkdude/brew/clj-kondo` (macOS) or `pacman -S clj-kondo` (Arch Linux)
   - `cljfmt` for formatting: Available via Clojure deps

### Building

NOTE: use `make -j` to run tasks in parallel.

* `make` or `make help` - show all available commands with descriptions
* `make deps` - install all local dependencies
* `make all` - build all artifacts (JAR, JS packages, docs)
* `make jar` - build Clojure JAR file
* `make nodejs` - build JavaScript SDK for Node.js
* `make browser` - build JavaScript SDK for browsers
* `make webworker` - build JavaScript SDK for web workers
* `make js-packages` - build all JavaScript packages
* `make docs` - generate API documentation

* `make install` - install JAR to local Maven repository
* `make deploy` - deploy JAR to remote repository
* `make clean` - remove build artifacts and caches

### Tests

* `make test` - run all automated tests (Clojure + ClojureScript + integration)
* `make cljtest` - run Clojure tests
* `make cljstest` - run all ClojureScript tests
* `make cljs-browser-test` - run ClojureScript tests in headless Chrome
* `make cljs-node-test` - run ClojureScript tests in Node.js
* `make nodejs-test` - run Node.js SDK integration tests
* `make browser-test` - run browser SDK integration tests
* `make pending-tests` (or `make pt`) - run tests marked as pending

### Code Quality

* `make ci` - run all CI checks (tests, linting, formatting)
* `make clj-kondo-lint` - lint Clojure code with clj-kondo
* `make eastwood` - run Eastwood linter
* `make cljfmt-check` - check Clojure formatting
* `make cljfmt-fix` - fix Clojure formatting errors

#### Running specific tests

> This applies to CLJ tests only, not CLJS.

`clojure -X:cljtest :kaocha.filter/focus [focus-spec]`

...where `focus-spec` can be a test namespace or a fully-qualified `deftest`
var. Note that the square brackets around the `focus-spec` must be present in
the command, they are NOT there to indicate "optional" or "placeholder" in the
example.

This feature comes from the test runner kaocha which has
[additional features](https://cljdoc.org/d/lambdaisland/kaocha/1.77.1236/doc/6-focusing-and-skipping).

### ClojureScript REPL

For ClojureScript development, you can start a REPL for different targets:

**Node.js REPL:**
1. `npx shadow-cljs watch fluree-node-sdk`
2. In a separate terminal: `node out/nodejs/fluree-node-sdk.js`
3. Connect to nREPL (port in `.shadow-cljs/nrepl.port`)
4. Run `(shadow/repl :fluree-node-sdk)`

**Browser REPL:**
1. `npx shadow-cljs watch fluree-browser-sdk`
2. Open `http://localhost:9630/` for Shadow CLJS dashboard
3. Connect to nREPL and run `(shadow/repl :fluree-browser-sdk)`

**Webworker REPL:**
1. `npx shadow-cljs watch fluree-webworker`
2. Connect to nREPL and run `(shadow/repl :fluree-webworker)`

Test your REPL with ClojureScript-specific code like `(js/parseInt "42")`
