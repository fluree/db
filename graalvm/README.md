# Fluree DB GraalVM Native Image Support

## Overview

Fluree DB can be compiled into a GraalVM native image, providing a standalone executable with no JVM dependency.

### Performance Metrics

| Metric | JVM | Native Image | Improvement |
|--------|-----|--------------|-------------|
| Startup Time | ~10s | <50ms* | ~30x |
| Memory (RSS) | ~200MB | ~30MB | ~7x |
| Executable Size | ~50MB (jar) + JVM | ~143MB | Single file |

*After OS caching. Initial execution may take 5 seconds.

## For Developers Embedding Fluree DB in GraalVM Applications

### Quick Start

1. **Add `:gen-class` to your main namespace**:
   ```clojure
   (ns your.main.namespace
     (:require [fluree.db.api :as fluree])
     (:gen-class))
   ```

2. **Build with required flags**:
   ```bash
   # Compile with direct linking and GraalVM build flag
   clojure -J-Dclojure.compiler.direct-linking=true \
           -J-Dclojure.spec.skip-macros=true \
           -J-Dfluree.graalvm.build=true \
           -M:graalvm \
           -e "(binding [*compile-path* \"classes\"] (compile 'your.main.namespace))"

   # Build native image
   native-image \
       --features=clj_easy.graal_build_time.InitClojureClasses \
       -H:+ReportExceptionStackTraces \
       -H:EnableURLProtocols=http,https \
       -H:IncludeResources='logback.xml|.*\.properties|contexts/.*\.jsonld|contexts/.*\.edn|.*\.edn' \
       --initialize-at-build-time \
       --initialize-at-run-time=jdk.internal.net.http,com.apicatalog.jsonld.loader,com.apicatalog.jsonld.http,com.apicatalog.rdf,io.setl.rdf \
       --no-fallback \
       -cp "$(clojure -Spath -M:graalvm):classes" \
       -H:Name=your-app \
       your.main.namespace
   ```

### Essential Requirements

1. **GraalVM Build Flag** (CRITICAL):
   ```bash
   # Set system property during compilation
   clojure -J-Dfluree.graalvm.build=true ...
   ```
   This enables conditional compilation to use SCI instead of eval for GraalVM compatibility.

2. **Resource Inclusion** (CRITICAL):
   ```bash
   -H:IncludeResources='logback.xml|.*\.properties|contexts/.*\.jsonld|contexts/.*\.edn|.*\.edn'
   ```
   Without JSON-LD context files, queries will return empty results.

3. **Runtime Initialization**:
   ```bash
   --initialize-at-run-time=jdk.internal.net.http,com.apicatalog.jsonld.loader,com.apicatalog.jsonld.http,com.apicatalog.rdf,io.setl.rdf
   ```

4. **Enable Protocols**:
   ```bash
   -H:EnableURLProtocols=http,https
   ```

5. **Dependencies**:
   ```clojure
   :graalvm {:extra-deps {com.github.clj-easy/graal-build-time {:mvn/version "1.0.5"}}}
   ```

## For Fluree DB Developers

### Testing GraalVM Compatibility

**Run the test suite**:
   ```bash
   ./graalvm/build-test.sh
   ./fluree-graalvm-test
   ```
  
   Note: compilation takes 5+ minutes.

   This tests:
   - All connection types (memory, file)
   - CRUD operations (insert, update, upsert, delete)
   - All query types (simple, complex, aggregation, SPARQL)
   - JSON-LD processing
   - Transaction commits

### Key Guidelines for Maintaining Compatibility

1. **Avoid Runtime Code Generation**:
   - Use SCI instead of `eval` (already implemented in `src/fluree/db/query/exec/eval.cljc`)
   - Avoid libraries that generate bytecode at runtime

2. **Resource Loading**:
   - Always use `io/resource` for loading resources
   - Ensure JSON-LD contexts are in `resources/contexts/`
   - Test resource loading in native images

3. **Lazy Initialization Pattern**:
   ```clojure
   ;; BAD - creates at namespace load
   (defonce http-client 
     (HttpClient/newBuilder)...)
   
   ;; GOOD - lazy initialization
   (def http-client
     (delay (HttpClient/newBuilder)...))
   
   ;; Usage
   @http-client
   ```

## Build Scripts and Files

### Build Scripts
- `build-test.sh` - Builds test suite
- `build-production.sh` - Production-optimized build with size optimizations

### Test Files
- `graalvm_test.clj` - Tests core Fluree operations

## Technical Details

### Prerequisites

1. **GraalVM 17+** with native-image:
   ```bash
   # Using SDKMAN
   sdk install java 17.0.8-graal
   sdk use java 17.0.8-graal
   
   # Install native-image
   gu install native-image
   ```

2. **Java Version Management**:
   This directory has its own `.java-version` file set to `17.0.12` (GraalVM).


### Troubleshooting

1. **Empty Query Results**: Ensure JSON-LD contexts are included with `-H:IncludeResources`
2. **Build Failures**: Enable `-H:+ReportExceptionStackTraces` for debugging
3. **Resource Verification**: Use `-H:Log=registerResource:` to verify resource inclusion
4. **HTTP Errors**: Check runtime initialization flags for HTTP clients

## Support

For issues or questions:
- Check example in the `graalvm/` directory
- Run the graalvm build-test.sh to verify functionality
- Review build scripts for working configurations