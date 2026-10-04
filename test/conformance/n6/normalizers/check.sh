#!/bin/sh
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 3 ]; then
    echo "usage: check.sh RAW_JSONL [RAW_NORMALIZED] [MODEL_NORMALIZED]" >&2
    exit 2
fi

normalizers=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
n6=$(CDPATH= cd -- "$normalizers/.." && pwd)
raw_input=$1
raw_output=${2:-$n6/evidence/raw-java-apache-corpus.normalized.jsonl}
model_output=${3:-$n6/evidence/independent-model.normalized.jsonl}
: "${N6_MODEL_JULIA_EXECUTABLE:?set N6_MODEL_JULIA_EXECUTABLE to a pinned absolute Julia executable}"

python3 -B -I "$normalizers/runtests.py"
python3 -B -I "$normalizers/normalize_raw.py" \
    --input "$raw_input" --output "$raw_output" --check
python3 -B -I "$normalizers/normalize_model.py" \
    --input "$raw_output" --raw-input "$raw_input" \
    --output "$model_output" --julia-executable "$N6_MODEL_JULIA_EXECUTABLE" --check
python3 -B -I "$n6/validate_evidence.py" \
    --schema "$n6/evidence.schema.json" \
    --manifest "$n6/manifest.toml" \
    --capabilities "$n6/capabilities.toml" \
    --fixtures "$n6/fixtures.toml" \
    "$raw_output" "$model_output"
