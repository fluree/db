#!/bin/bash

echo "GraalVM Native Image Diagnostic Script"
echo "======================================"
echo ""

# Check if running in Docker or locally
if [ -f /.dockerenv ]; then
    echo "Running in Docker container"
else
    echo "Running locally - checking for GraalVM installation..."
    
    if ! command -v native-image &> /dev/null; then
        echo "❌ native-image not found"
        echo ""
        echo "To test GraalVM locally, you need to:"
        echo "1. Install GraalVM CE 11 or 17"
        echo "2. Install native-image component"
        echo ""
        echo "Or use Docker:"
        echo "  docker build -f Dockerfile.graalvm-simple -t fluree-graalvm ."
        exit 1
    else
        echo "✓ native-image found: $(native-image --version)"
    fi
fi

echo ""
echo "Checking project structure..."

# Check required files
for file in deps.edn src/fluree/db/query/exec/eval.cljc test/graalvm_simple_test.clj; do
    if [ -f "$file" ]; then
        echo "✓ Found: $file"
    else
        echo "❌ Missing: $file"
    fi
done

echo ""
echo "Checking for eval usage..."
grep -r "eval" src/ | grep -v "eval.cljc" | grep -v "eval-" | grep -v "evaluation" | head -5

echo ""
echo "To build native image with Docker:"
echo "  docker build -f Dockerfile.graalvm-simple -t fluree-graalvm ."
echo ""
echo "To extract the native binary:"
echo "  docker create --name temp fluree-graalvm"
echo "  docker cp temp:/usr/local/bin/fluree-test ./fluree-test-native"
echo "  docker rm temp"