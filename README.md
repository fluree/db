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

It is also possible to run Fluree in a "serverless" manner, where by utilizing Fluree SmartFunctions
to embed data security along side your data (Data Defending Itself), you can have a permissioned
application with just a single-page application (i.e. React) and Fluree Ledgers, but no application server.

Fluree includes time travel, allowing you to instantly query as of any historical moment in time,
and even allows the abilty to stage proposed transactions to time travel into the future, to a hypothesized version
of your data.

The best way to get started with Fluree is to go to the [Getting Started](https://flur.ee/getstarted/) page
at https://flur.ee/getstarted/.

## Development

### Contributing

All contributors must complete a [Contributor License Agreement](https://gist.github.com/bplatz/18bdfab7221fc1b03eb3293c8fe56077).

### Prerequisites

1. Install clojure tools-deps (version 1.10.1.697 or later).
    1. macOS: `brew install clojure/tools/clojure`
    1. Arch Linux: `pacman -S clojure`

### Building

1. `make deps` - install all local dependencies
1. `make` or `make jar` - make Java .jar file
1. `make nodejs` - make JavaScript Fluree DB
1. `make browser` - make JavaScript WebWorker Fluree DB
1. `make clean` - clean all build directories/files
1. `make install` - install jar file into local .m2/maven

### Deploying

1. Make sure you've setup everything in the [Prerequisites](#Prerequisites) section above.
1. Install maven
    1. macOS: `brew install maven`
1. Set the version number in `pom.xml`
1. Run `make deploy`
