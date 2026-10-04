#!/bin/sh
set -eu

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "usage: check_generated.sh RAW_JSONL [NORMALIZED_JSONL]" >&2
    exit 2
fi

normalizers=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
n6=$(CDPATH= cd -- "$normalizers/.." && pwd)
raw_input=$1
output=${2:-$n6/evidence/raw-java-generated.normalized.jsonl}

python3 -B -I "$normalizers/runtests.py"
python3 -B -I "$normalizers/normalize_raw.py" \
    --fixture-set generated --input "$raw_input" --output "$output" --check
python3 -B -I "$n6/validate_evidence.py" \
    --schema "$n6/evidence.schema.json" \
    --manifest "$n6/manifest.toml" \
    --capabilities "$n6/capabilities.toml" \
    --fixtures "$n6/fixtures.toml" \
    "$output"
