#!/bin/bash

# GraalVM Native Image build script for Fluree DB Iceberg integration test
# This builds a native image that tests Iceberg VG with REST catalog
# (minimal Hadoop for Parquet codec support)

set -e

# Set Java version for this script
if command -v jenv &> /dev/null; then
    eval "$(jenv init -)"
    jenv shell oracle64-17.0.12
    echo "Using Java version: $(java -version 2>&1 | head -n 1)"
fi

echo "Building GraalVM native image for Iceberg integration test..."

# Ensure we're in the project root
cd "$(dirname "$0")/.."

# Create classes directory
mkdir -p classes

# Copy data readers (if they exist)
if [ -f "src/data_readers.cljc" ]; then
    cp src/data_readers.cljc classes/
fi

# Compile with direct linking using graalvm-iceberg alias
echo "Compiling iceberg-graalvm-test..."
clojure -J-Dclojure.compiler.direct-linking=true \
        -J-Dclojure.spec.skip-macros=true \
        -J-Dfluree.graalvm.build=true \
        -J"--add-opens=java.base/java.nio=ALL-UNNAMED" \
        -J"--add-opens=java.base/java.lang=ALL-UNNAMED" \
        -J"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
        -M:graalvm-iceberg \
        -e "(binding [*compile-path* \"classes\"] (compile 'iceberg-graalvm-test))"

# Create build directory if it doesn't exist
mkdir -p build

# Build native image with Iceberg support
# Arrow requires these JVM opens for native-image analysis phase
# Configure Arrow to use Unsafe allocator (no Netty dependency)
echo "Building native image..."
# Resource patterns are also in resource-config.json but duplicated here for explicitness
native-image \
    -J"--add-opens=java.base/java.nio=ALL-UNNAMED" \
    -J"--add-opens=java.base/java.lang=ALL-UNNAMED" \
    -J"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
    -J"-Darrow.memory.allocator.default.type=Unsafe" \
    --features=clj_easy.graal_build_time.InitClojureClasses \
    -H:+ReportExceptionStackTraces \
    -H:EnableURLProtocols=http,https \
    -H:IncludeResources='logback.xml|.*\.properties|contexts/.*\.jsonld|contexts/.*\.edn|.*\.edn|darwin/.*/libzstd-jni.*|linux/.*/libzstd-jni.*|win/.*/zstd-jni.*' \
    --initialize-at-build-time \
    --initialize-at-run-time=com.apicatalog.jsonld.loader,com.apicatalog.jsonld.http,com.apicatalog.rdf,io.setl.rdf,org.apache.http.impl.auth.NTLMEngineImpl,com.github.luben.zstd \
    -H:-UseServiceLoaderFeature \
    --no-fallback \
    -cp "$(clojure -Spath -M:graalvm-iceberg):classes" \
    -H:Name=fluree-iceberg-test \
    -H:Path=build \
    -H:ConfigurationFileDirectories=resources/META-INF/native-image/com.fluree/db \
    iceberg_graalvm_test

echo ""
echo "Native image build complete: build/fluree-iceberg-test"
echo "Binary size: $(ls -lh build/fluree-iceberg-test | awk '{print $5}')"
echo ""
echo "To run the test, first start the REST catalog:"
echo "  docker-compose up -d  # (if you have docker-compose.yml for Iceberg REST)"
echo ""
echo "Then run:"
echo "  ./build/fluree-iceberg-test"
echo ""
echo "Or with custom config:"
echo "  ICEBERG_REST_URI=http://localhost:8181 \\"
echo "  ICEBERG_REST_S3_ENDPOINT=http://localhost:9000 \\"
echo "  ./build/fluree-iceberg-test"
