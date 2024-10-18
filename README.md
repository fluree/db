# Fluree DB

Usage [documentation](https://docs.flur.ee) is located at https://docs.flur.ee.

## Overview

Fluree is an immutable, temporal, ledger-backed semantic graph database that has a cloud-native architecture.

This repository is a stateless database as a library and designed to be utilized in conjunction with the
[Fluree Ledger](https://github.com/fluree/ledger) which maintains all state. This database
can be run in containers and dynamically scale to any desired load, can be embedded inside
of your applications (Clojure, NodeJS for now) or can run as a stand-alone JVM service.

This database can also be built as a web-worker, and be embedded inside the browser. Thus
far, a [React Wrapper](https://github.com/fluree/fluree-react) (Beta) has been developed that allows
you to create real-time apps by wrapping your React components with queries (GraphQL or FlureeQL).

It is also possible to run Fluree in a "serverless" manner, whereby utilizing Fluree SmartFunctions
to embed data security along side your data (Data Defending Itself), you can have a permissioned
application with just a single-page application (i.e. React) and Fluree Ledgers, but no application server.

Fluree includes time travel, allowing you to instantly query as of any historical moment in time,
and even allows the ability to stage proposed transactions to time travel into the future, to a hypothesized version
of your data.

The best way to get started with Fluree is to go to the [Getting Started](https://flur.ee/getstarted/) page
at https://flur.ee/getstarted/.

## Development

### Contributing

All contributors must complete a [Contributor License Agreement](https://cla-assistant.io/fluree/).

### Prerequisites

1. Install clojure tools-deps (version 1.10.3.933 or later).
   1. macOS: `brew install clojure/tools/clojure`
   2. Arch Linux: `pacman -S clojure`
   3. Windows: follow instructions here https://applab.unc.edu/posts/2019/09/11/how-to-install-clojure-on-windows/
2. Install Node & NPM
   1. macOS: `brew install node`
   2. Arch Linux: `pacman -S nodejs`
   3. Windows: Download installer here https://nodejs.org/en/download
3. Install babashka
   1. macOS: `brew install borkdude/brew/babashka`
   2. Windows: `scoop install babashka`

### Building

NOTE: use `make -j` to run tasks in parallel.

* `make deps` - install all local dependencies
* `make compile` - locally compile necessary classes (required for Clojure development)
* `make` - make everything below
* `make jar` - make Java JAR file
* `make nodejs` - make JavaScript Fluree DB for Node
* `make browser` - make JavaScript Fluree DB for browsers
* `make webworker` - make JavaScript Fluree DB for web worker

* `make install` - install jar file into local .m2/maven
* `make clean` - clean all build directories/files

### Tests

* `make test` - run all automated tests below
* `make cljtest` - run all CLJ tests
* `make cljstest` - run CLJS tests in headless Chrome & NodeJS
* `make cljs-browser-test` - run CLJS tests in headless Chrome
    * Needs karma-cli installed globally: `npm install -g karma-cli`
    * Needs Google Chrome browser installed
* `make cljs-node-test` - run CLJS tests in NodeJS
* `make nodejs-test` - run node package in node
* `make browser-test` - run browser package in headless Chrome

#### Running specific tests

> This applies to CLJ tests only, not CLJS.

`clojure -X:cljtest :kaocha.filter/focus [focus-spec]`

...where `focus-spec` can be a test namespace or a fully-qualified `deftest`
var. Note that the square brackets around the `focus-spec` must be present in
the command, they are NOT there to indicate "optional" or "placeholder" in the
example.

This feature comes from the test runner kaocha which has
[additional features](https://cljdoc.org/d/lambdaisland/kaocha/1.77.1236/doc/6-focusing-and-skipping).

### CLJS REPL

In order to get a Node or web browser CLJS REPL running, you need to do the
following:

1. Run `npx shadow-cljs watch flureenjs` (or `flureedb` for a browser REPL)
2. (Node only) Run `node out/nodejs/flureenjs.js` in a separate shell
3. (browser only) Connect to `http://localhost:9630/` in your browser
    1. This seems currently broken though. It first gets stuck at
       "shadow-cljs Loading..." with an "unknown route" error in the JS console.
       If you reload that seems to resolve. But even once the dashboard loads
       and you connect a REPL, it says "No available JS runtime."
4. Connect an nREPL to the port specified in `.shadow-cljs/nrepl.port`
5. Inside that REPL run `(shadow/repl :flureenjs)` (or `:flureedb` for a browser
   REPL)
6. Try running something CLJS-specific to ensure you've got a working CLJS REPL
    1. For example: `(js/parseInt "42")`
