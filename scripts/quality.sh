#!/usr/bin/env bash
set -euo pipefail

cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test

if command -v cargo-audit >/dev/null 2>&1; then
  cargo audit
else
  echo "cargo-audit is not installed; skipping dependency security audit" >&2
fi
