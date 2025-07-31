#!/bin/bash

# GraalVM Native Image production build script for Fluree DB test
# Optimized for size and performance using graalvm_test.clj

set -e

# Set Java version for this script
if command -v jenv &> /dev/null; then
    eval "$(jenv init -)"
    jenv shell oracle64-17.0.12
    echo "Using Java version: $(java -version 2>&1 | head -n 1)"
fi

echo "Building GraalVM native image for graalvm-test (production optimized)..."

# Ensure we're in the project root
cd "$(dirname "$0")/.."

# Create classes directory
mkdir -p classes

# Copy data readers (if they exist)
if [ -f "src/data_readers.cljc" ]; then
    cp src/data_readers.cljc classes/
fi

# Compile with direct linking using graalvm alias
echo "Compiling graalvm-test..."
clojure -J-Dclojure.compiler.direct-linking=true \
        -J-Dclojure.spec.skip-macros=true \
        -J-Dfluree.graalvm.build=true \
        -M:graalvm \
        -e "(binding [*compile-path* \"classes\"] (compile 'graalvm-test))"

# Create build directory if it doesn't exist
mkdir -p build

# Build native image with production optimizations
echo "Building native image with production optimizations..."
native-image \
    --features=clj_easy.graal_build_time.InitClojureClasses \
    -H:+ReportExceptionStackTraces \
    -H:EnableURLProtocols=http,https \
    -H:IncludeResources='logback.xml|.*\.properties|contexts/.*\.jsonld|contexts/.*\.edn|.*\.edn' \
    --initialize-at-build-time \
    --initialize-at-run-time=jdk.internal.net.http,com.apicatalog.jsonld.loader,com.apicatalog.jsonld.http,com.apicatalog.rdf,io.setl.rdf \
    --no-fallback \
    -O2 \
    -H:+RemoveUnusedSymbols \
    -cp "$(clojure -Spath -M:graalvm):classes" \
    -H:Name=fluree-db-production \
    -H:Path=build \
    graalvm_test

echo "Native image build complete: build/fluree-db-production"
echo "Binary size: $(ls -lh build/fluree-db-production | awk '{print $5}')"

# Optional: Strip debug symbols for even smaller size (macOS/Linux)
if command -v strip &> /dev/null; then
    echo "Stripping debug symbols..."
    cp build/fluree-db-production build/fluree-db-production.debug
    strip build/fluree-db-production
    echo "Stripped binary size: $(ls -lh build/fluree-db-production | awk '{print $5}')"
fi