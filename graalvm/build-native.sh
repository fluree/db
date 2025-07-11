#!/bin/bash

echo "Building Fluree DB Native Image with GraalVM..."
echo "=============================================="

# Check if GraalVM is installed
if ! command -v native-image &> /dev/null; then
    echo "Error: native-image not found. Please install GraalVM and native-image."
    echo ""
    echo "On macOS:"
    echo "  brew install --cask graalvm-ce-java17"
    echo "  export JAVA_HOME=/Library/Java/JavaVirtualMachines/graalvm-ce-java17/Contents/Home"
    echo "  gu install native-image"
    exit 1
fi

# Create classes directory
mkdir -p classes

# Compile the test class
echo "Compiling test class..."
clojure -M:dev -e "(compile 'graalvm-simple-test)"

# Build classpath
echo "Building classpath..."
CLASSPATH=$(clojure -Spath):classes

# Run with tracing agent to detect additional reflection
echo "Running tracing agent..."
mkdir -p graalvm/agent-output
java -agentlib:native-image-agent=config-merge-dir=graalvm/agent-output \
     -cp "$CLASSPATH" \
     graalvm_simple_test 2>&1 | grep -v "WARNING: abs"

# Build native image
echo "Building native image..."
native-image \
    -cp "$CLASSPATH" \
    -H:ConfigurationFileDirectories=graalvm,graalvm/agent-output \
    -H:+ReportExceptionStackTraces \
    --no-fallback \
    --verbose \
    -H:Name=fluree-db-test \
    graalvm_simple_test

if [ $? -eq 0 ]; then
    echo ""
    echo "Success! Native image built: ./fluree-db-test"
    echo "Running native image test..."
    ./fluree-db-test
else
    echo ""
    echo "Native image build failed. Check the output above for errors."
    exit 1
fi