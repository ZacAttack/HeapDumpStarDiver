#!/bin/bash
set -e

SCALE=${1:-1.0}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Compiling test-heap-generator ==="
rm -rf build
mkdir -p build

find src -name "*.java" > sources.txt
javac -d build @sources.txt
echo "Compilation successful."

echo "=== Running with scale factor: $SCALE ==="
java -Xmx10g -Xms10g -XX:+UseG1GC \
  -cp build \
  com.heaptest.Main "$SCALE"

echo "=== Done ==="
if [ -f test-heap.hprof ]; then
  ls -lh test-heap.hprof
fi
