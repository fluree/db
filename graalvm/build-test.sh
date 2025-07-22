#!/bin/bash

# GraalVM Native Image build script for comprehensive Fluree DB test
# This builds a native image that tests all Fluree DB functionality

set -e

# Set Java version for this script
if command -v jenv &> /dev/null; then
    eval "$(jenv init -)"
    jenv shell oracle64-17.0.12
    echo "Using Java version: $(java -version 2>&1 | head -n 1)"
fi

echo "Building GraalVM native image for comprehensive Fluree DB test..."

# Ensure we're in the project root
cd "$(dirname "$0")/.."

# Create classes directory
mkdir -p classes

# Copy data readers (if they exist)
if [ -f "src/data_readers.cljc" ]; then
    cp src/data_readers.cljc classes/
fi

# No need to copy - graalvm directory is now in classpath via :graalvm alias

# Compile with direct linking using graalvm alias
echo "Compiling graalvm-test..."
clojure -J-Dclojure.compiler.direct-linking=true \
        -J-Dclojure.spec.skip-macros=true \
        -J-Dfluree.graalvm.build=true \
        -M:graalvm \
        -e "(binding [*compile-path* \"classes\"] (compile 'graalvm-test))"

# Create build directory if it doesn't exist
mkdir -p build

# Build native image with same successful flags as full API
echo "Building native image..."
native-image \
    --features=clj_easy.graal_build_time.InitClojureClasses \
    -H:+ReportExceptionStackTraces \
    -H:EnableURLProtocols=http,https \
    -H:IncludeResources='logback.xml|.*\.properties|contexts/.*\.jsonld|contexts/.*\.edn|.*\.edn' \
    --initialize-at-build-time \
    --initialize-at-run-time=jdk.internal.net.http,com.apicatalog.jsonld.loader,com.apicatalog.jsonld.http,com.apicatalog.rdf,io.setl.rdf \
    --no-fallback \
    -cp "$(clojure -Spath -M:graalvm):classes" \
    -H:Name=fluree-graalvm-test \
    -H:Path=build \
    graalvm_test

# No cleanup needed - test file stays in graalvm directory

echo "Native image build complete: build/fluree-graalvm-test"
echo ""
echo "Run the test with: ./build/fluree-graalvm-test"