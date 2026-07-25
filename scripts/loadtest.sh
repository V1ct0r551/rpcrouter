#!/usr/bin/env bash
set -euo pipefail

mkdir -p data
cargo run --release --bin loadtest -- \
  --qps "${QPS:-10000}" \
  --duration "${DURATION:-60}" \
  --concurrency "${CONCURRENCY:-64}" | tee data/loadtest-phase3.json
