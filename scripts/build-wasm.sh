#!/bin/bash
# Build Chronicle WASM module
#
# Usage:
#   ./scripts/build-wasm.sh              # Standard Go build
#   ./scripts/build-wasm.sh --tinygo     # TinyGo build (smaller output)
#
# Output: wasm/chronicle.wasm

set -euo pipefail

cd "$(dirname "$0")/.."

OUTDIR="wasm"
OUTPUT="${OUTDIR}/chronicle.wasm"

if [ "${1:-}" = "--tinygo" ]; then
    echo "Building with TinyGo..."
    tinygo build -o "$OUTPUT" -target wasm ./wasm/main.go
else
    echo "Building with Go..."
    GOOS=js GOARCH=wasm go build -o "$OUTPUT" -ldflags="-s -w" ./wasm
fi

SIZE=$(wc -c < "$OUTPUT" | tr -d ' ')
SIZE_MB=$(echo "scale=1; $SIZE / 1048576" | bc)

echo "✓ Built: ${OUTPUT} (${SIZE_MB}MB)"

# Copy wasm_exec.js from Go installation if not present
WASM_EXEC="$(go env GOROOT)/lib/wasm/wasm_exec.js"
if [ -f "$WASM_EXEC" ] && [ ! -f "${OUTDIR}/wasm_exec.js" ]; then
    cp "$WASM_EXEC" "${OUTDIR}/wasm_exec.js"
    echo "✓ Copied wasm_exec.js"
fi

echo ""
echo "To test in browser, open wasm/index.html"
echo "To use in Node.js:"
echo '  const go = new Go();'
echo '  const { instance } = await WebAssembly.instantiate(fs.readFileSync("chronicle.wasm"), go.importObject);'
echo '  go.run(instance);'
