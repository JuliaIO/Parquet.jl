#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=common.sh
source "$root/common.sh"

if [[ $# -gt 1 || ( $# -eq 1 && $1 != "--check" ) ]]; then
    echo "usage: generate.sh [--check]" >&2
    exit 64
fi

/bin/bash "$root/fetch-toolchain.sh" >/dev/null
mkdir -p "$RAW_JAVA_GENERATED_DIR"
find "$RAW_JAVA_GENERATED_DIR" -type f -name '*.java' -delete
"$RAW_JAVA_BUILD_DIR/compiler/thrift" \
    --gen java:generated_annotations=suppress \
    -out "$RAW_JAVA_GENERATED_DIR" \
    "$RAW_JAVA_IDL"

manifest="$RAW_JAVA_BUILD_DIR/generated.sha256"
: > "$manifest"
while IFS= read -r file; do
    relative=${file#"$RAW_JAVA_GENERATED_DIR/"}
    printf '%s  %s\n' "$(raw_java_sha256 "$file")" "$relative" >> "$manifest"
done < <(find "$RAW_JAVA_GENERATED_DIR" -type f -name '*.java' -print | LC_ALL=C sort)
raw_java_verify "$RAW_JAVA_GENERATED_MANIFEST_SHA256" "$manifest"
printf 'Generated %s pinned Java format sources.\n' \
    "$(find "$RAW_JAVA_GENERATED_DIR" -type f -name '*.java' | wc -l | tr -d ' ')"
