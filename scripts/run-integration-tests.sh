#!/usr/bin/env bash
# Fleet Router — Integration Test Runner
# Runs all automated tests: unit, E2E, mock stress, and optionally Docker
set -euo pipefail

echo "=== Building fleet-router ==="
cargo build

echo ""
echo "=== Running mock + E2E tests ==="
cargo test --workspace -- --nocapture

echo ""
echo "=== Docker stress tests (if services are up) ==="
if curl -sf http://localhost:3333/status >/dev/null 2>&1; then
    echo "Load generator detected, running Docker stress tests..."
    cargo test --test stress_test -- --ignored --nocapture
else
    echo "Skipping Docker tests (load generator not running on :3333)"
    echo "To run: docker compose -f docker/docker-compose.test.yml up --build -d"
fi

echo ""
echo "=== Done ==="
