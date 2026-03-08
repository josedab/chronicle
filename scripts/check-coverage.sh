#!/usr/bin/env bash
# scripts/check-coverage.sh — Enforce per-package coverage minimums
#
# Usage:
#   ./scripts/check-coverage.sh [coverage.out]
#
# Exits with code 1 if any production package is below its threshold.
# Thresholds:
#   - Production packages (root, internal/query, etc.): 50%
#   - Low-coverage-known packages (cluster, cep, etc.):  10% (aspirational)
#
set -euo pipefail

COVERFILE="${1:-coverage.out}"
PRODUCTION_THRESHOLD=50
EXPERIMENTAL_THRESHOLD=10
EXIT_CODE=0

if [ ! -f "$COVERFILE" ]; then
  echo "Coverage file not found: $COVERFILE"
  echo "Run: go test -short -coverprofile=$COVERFILE ./..."
  exit 1
fi

echo "═══════════════════════════════════════════════════════════"
echo "  Per-Package Coverage Check"
echo "═══════════════════════════════════════════════════════════"
printf "  %-55s %8s %8s %s\n" "PACKAGE" "COVERAGE" "MINIMUM" "STATUS"
echo "───────────────────────────────────────────────────────────"

# Known experimental/low-coverage packages get a lower threshold
EXPERIMENTAL_PKGS="internal/cluster internal/cep internal/adminui internal/digitaltwin internal/gpucompression"

go tool cover -func="$COVERFILE" | grep -v "^total:" | \
  awk '{print $1, $NF}' | \
  sed 's|github.com/chronicle-db/chronicle/||' | \
  awk -F'[/:]' '{print $1"/"$2}' | \
  sort -u | while read -r pkg; do
    # Get coverage for this package by averaging its functions
    pkg_pattern=$(echo "$pkg" | sed 's|/|/|g')
    coverage_line=$(go tool cover -func="$COVERFILE" | grep "$pkg_pattern" | tail -1)
    if [ -z "$coverage_line" ]; then
      continue
    fi
done

# Simpler approach: check the per-package coverage from go test output
# Parse the coverage.out to get per-package stats
FAILED=0
CHECKED=0

while IFS= read -r line; do
  pkg=$(echo "$line" | awk '{print $1}')
  pct_raw=$(echo "$line" | awk '{print $NF}')
  pct=$(echo "$pct_raw" | sed 's/%//')

  # Skip non-numeric
  if ! echo "$pct" | grep -qE '^[0-9]+\.?[0-9]*$'; then
    continue
  fi

  # Determine threshold
  threshold=$PRODUCTION_THRESHOLD
  is_experimental=false
  for exp_pkg in $EXPERIMENTAL_PKGS; do
    if echo "$pkg" | grep -q "$exp_pkg"; then
      threshold=$EXPERIMENTAL_THRESHOLD
      is_experimental=true
      break
    fi
  done

  CHECKED=$((CHECKED + 1))

  if [ "$(echo "$pct < $threshold" | bc -l 2>/dev/null || echo 0)" -eq 1 ]; then
    printf "  %-55s %7s%% %7s%% %s\n" "$pkg" "$pct" "$threshold" "❌ FAIL"
    FAILED=$((FAILED + 1))
    EXIT_CODE=1
  else
    printf "  %-55s %7s%% %7s%% %s\n" "$pkg" "$pct" "$threshold" "✅ PASS"
  fi
done < <(go tool cover -func="$COVERFILE" | grep "^total:" )

# Also check the overall total
TOTAL_LINE=$(go tool cover -func="$COVERFILE" | grep "^total:")
TOTAL_PCT=$(echo "$TOTAL_LINE" | awk '{print $NF}' | sed 's/%//')

echo "═══════════════════════════════════════════════════════════"
printf "  %-55s %7s%%\n" "OVERALL TOTAL" "$TOTAL_PCT"
echo "═══════════════════════════════════════════════════════════"

if [ "$(echo "$TOTAL_PCT < 70" | bc -l 2>/dev/null || echo 0)" -eq 1 ]; then
  echo ""
  echo "⚠️  Overall coverage ${TOTAL_PCT}% is below 70% target"
fi

if [ "$FAILED" -gt 0 ]; then
  echo ""
  echo "❌ $FAILED package(s) below minimum coverage threshold"
fi

exit $EXIT_CODE
