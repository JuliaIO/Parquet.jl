#!/bin/sh
set -eu

if [ "$#" -lt 3 ]; then
    echo "usage: run.sh CORPUS_ROOT RAW_EVIDENCE OUTPUT [--draft]" >&2
    exit 2
fi
oracle_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repository=$(CDPATH= cd -- "$oracle_dir/../../../../.." && pwd -P)
if [ -z "${PARQUET_N6_INTEROP_PYTHON_ROOT:-}" ]; then
    echo "PARQUET_N6_INTEROP_PYTHON_ROOT is required" >&2
    exit 2
fi
if [ -z "${PARQUET_N6_JAVA_JDK_ROOT:-}" ]; then
    echo "PARQUET_N6_JAVA_JDK_ROOT is required" >&2
    exit 2
fi
if [ -z "${PARQUET_N6_JAVA_ROOT:-}" ]; then
    echo "PARQUET_N6_JAVA_ROOT is required" >&2
    exit 2
fi
python="$PARQUET_N6_INTEROP_PYTHON_ROOT/bin/python3.12"
if [ ! -x "$python" ]; then
    echo "the pinned Python interpreter is not executable: $python" >&2
    exit 2
fi
corpus_root=$1
raw_evidence=$2
output=$3
shift 3

exec "$python" -I -S -B "$oracle_dir/run.py" \
    --repository "$repository" \
    --manifest "$repository/test/conformance/n6/manifest.toml" \
    --capabilities "$repository/test/conformance/n6/capabilities.toml" \
    --fixtures "$repository/test/conformance/n6/fixtures.toml" \
    --descriptor "$oracle_dir/toolchain.toml" \
    --raw-evidence "$raw_evidence" \
    --corpus-root "$corpus_root" \
    --java-root "$PARQUET_N6_JAVA_ROOT" \
    --jdk-root "$PARQUET_N6_JAVA_JDK_ROOT" \
    --output "$output" \
    "$@"
