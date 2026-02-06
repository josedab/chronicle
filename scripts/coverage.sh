#!/usr/bin/env bash
# scripts/coverage.sh — Per-package test coverage report
#
# Usage:
#   ./scripts/coverage.sh           # Summary table
#   ./scripts/coverage.sh -html     # Also generate HTML report
#   ./scripts/coverage.sh -v        # Include file-level detail
#
set -euo pipefail

COVERFILE="coverage.out"
HTML_FLAG=false
VERBOSE_FLAG=false

for arg in "$@"; do
  case "$arg" in
    -html) HTML_FLAG=true ;;
    -v)    VERBOSE_FLAG=true ;;
    *)     echo "Usage: $0 [-html] [-v]"; exit 1 ;;
  esac
done

echo "Running tests with coverage..."
go test -short -coverprofile="$COVERFILE" -covermode=atomic ./... 2>/dev/null

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Package Coverage Report"
echo "═══════════════════════════════════════════════════════════"

if [ "$VERBOSE_FLAG" = true ]; then
  printf "  %-50s %8s\n" "FILE:FUNCTION" "COVERAGE"
  echo "───────────────────────────────────────────────────────────"
  go tool cover -func="$COVERFILE" | while IFS= read -r line; do
    if echo "$line" | grep -q "^total:"; then
      echo "═══════════════════════════════════════════════════════════"
      pct=$(echo "$line" | awk '{print $NF}')
      printf "  %-50s %8s\n" "TOTAL" "$pct"
    else
      file=$(echo "$line" | awk '{print $1}')
      func_name=$(echo "$line" | awk '{print $2}')
      pct=$(echo "$line" | awk '{print $NF}')
      short=$(echo "$file" | sed 's|github.com/chronicle-db/chronicle/||')
      printf "  %-40s %-15s %s\n" "$short" "$func_name" "$pct"
    fi
  done
else
  # Show just the total
  total_line=$(go tool cover -func="$COVERFILE" | grep "^total:")
  pct=$(echo "$total_line" | awk '{print $NF}')
  printf "  %-50s %8s\n" "TOTAL" "$pct"
fi

echo "═══════════════════════════════════════════════════════════"

echo ""

if [ "$HTML_FLAG" = true ]; then
  go tool cover -html="$COVERFILE" -o coverage.html
  echo "HTML report: coverage.html"
fi

echo "Raw data: $COVERFILE"
echo "Tip: go tool cover -func=$COVERFILE | grep -v 100.0% | sort -t'%' -k3 -n"
