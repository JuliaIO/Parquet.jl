#!/bin/sh
set -eu

oracle_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export CARGO_NET_OFFLINE=true
cd "$oracle_dir"
exec cargo test --manifest-path Cargo.toml --offline --locked
