#!/bin/sh
set -eu

usage() {
    echo "usage: test-oracle-container.sh --image IMAGE --corpus CORPUS" >&2
    exit 64
}

host_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        n5_hash_output=$(sha256sum "$1") || return 1
    else
        n5_hash_output=$(shasum -a 256 "$1") || return 1
    fi
    printf '%s\n' "${n5_hash_output%% *}"
}

assert_no_host_stage() {
    n5_stage_entry=$(find "$n5_work" -maxdepth 1 \
        -name '.n5-*-stage.*' -print -quit) || return 1
    [ -z "$n5_stage_entry" ]
}

assert_canary_unchanged() {
    n5_canary_after=$(host_sha256 "$n5_canary") || return 1
    [ "$n5_canary_after" = "$n5_canary_before" ]
}

n5_image=
n5_corpus=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --image)
            [ "$#" -ge 2 ] || usage
            n5_image=$2
            shift 2
            ;;
        --corpus)
            [ "$#" -ge 2 ] || usage
            n5_corpus=$2
            shift 2
            ;;
        *) usage ;;
    esac
done
[ -n "$n5_image" ] && [ -d "$n5_corpus/.git" ] || usage
n5_repo=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
n5_work=$(mktemp -d)
cleanup() {
    rm -rf "$n5_work"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

n5_canary="$n5_repo/test/conformance/n5/manifest.toml"
n5_canary_before=$(host_sha256 "$n5_canary")
"$n5_repo/test/conformance/n5/run-oracle-container.sh" \
    --image "$n5_image" \
    --repo "$n5_repo" \
    --corpus "$n5_corpus" \
    --output "$n5_work/success"
[ -f "$n5_work/success/status.json" ]
[ -f "$n5_work/success/read-only-canary.json" ]
[ -f "$n5_work/success/evidence.sha256" ]
assert_canary_unchanged
assert_no_host_stage

cp -R "$n5_corpus" "$n5_work/corrupt-corpus"
printf '%s\n' n5-forced-corruption \
    >> "$n5_work/corrupt-corpus/data/datapage_v2.snappy.parquet"
if "$n5_repo/test/conformance/n5/run-oracle-container.sh" \
    --image "$n5_image" \
    --repo "$n5_repo" \
    --corpus "$n5_work/corrupt-corpus" \
    --output "$n5_work/failure"; then
    echo "test-oracle-container.sh: corrupt corpus unexpectedly passed" >&2
    exit 1
else
    n5_status=$?
fi
[ "$n5_status" -eq 65 ]
[ -f "$n5_work/failure/failure.txt" ]
[ -f "$n5_work/failure/container.log" ]
grep -F 'corpus file hash differs' "$n5_work/failure/failure.txt" >/dev/null
assert_canary_unchanged
assert_no_host_stage
echo "oracle container success and rollback checks passed"
