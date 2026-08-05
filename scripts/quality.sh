#!/usr/bin/env bash
set -euo pipefail

cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test

if command -v cargo-audit >/dev/null 2>&1; then
  # rust_decimal 1.42.1 declares rkyv 0.7.46 as an optional dependency, so it
  # is retained in Cargo.lock even though no rkyv feature is enabled and rkyv
  # is absent from the resolved feature graph (`cargo tree -e features -i
  # rkyv@0.7.46`). RUSTSEC-2026-0235 is therefore not compiled into the bot.
  cargo audit --ignore RUSTSEC-2026-0235
else
  echo "cargo-audit is not installed; skipping dependency security audit" >&2
fi
