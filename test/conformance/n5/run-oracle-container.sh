#!/bin/sh
set -eu

usage() {
    echo "usage: run-oracle-container.sh --image IMAGE --repo REPO --corpus CORPUS --output DIR" >&2
    exit 64
}

fail() {
    echo "run-oracle-container.sh: $1" >&2
    exit 65
}

host_sha256() {
    n5_hash_file=$1
    [ -f "$n5_hash_file" ] || fail "required file $n5_hash_file is absent"
    if command -v sha256sum >/dev/null 2>&1; then
        n5_hash_output=$(sha256sum "$n5_hash_file") ||
            fail "cannot hash $n5_hash_file"
    else
        n5_hash_output=$(shasum -a 256 "$n5_hash_file") ||
            fail "cannot hash $n5_hash_file"
    fi
    n5_hash_value=${n5_hash_output%% *}
    case "$n5_hash_value" in
        *[!0-9a-f]*)
            fail "invalid SHA-256 output for $n5_hash_file"
            ;;
    esac
    [ "${#n5_hash_value}" -eq 64 ] ||
        fail "invalid SHA-256 output for $n5_hash_file"
    printf '%s\n' "$n5_hash_value"
}

verify_evidence() {
    n5_evidence_root=$1
    if command -v sha256sum >/dev/null 2>&1; then
        (cd "$n5_evidence_root" &&
            sha256sum --check --quiet --strict evidence.sha256)
    else
        (cd "$n5_evidence_root" && shasum -a 256 --check evidence.sha256)
    fi
}

n5_image=
n5_repo=
n5_corpus=
n5_output=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --image)
            [ "$#" -ge 2 ] || usage
            n5_image=$2
            shift 2
            ;;
        --repo)
            [ "$#" -ge 2 ] || usage
            n5_repo=$2
            shift 2
            ;;
        --corpus)
            [ "$#" -ge 2 ] || usage
            n5_corpus=$2
            shift 2
            ;;
        --output)
            [ "$#" -ge 2 ] || usage
            n5_output=$2
            shift 2
            ;;
        *) usage ;;
    esac
done
[ -n "$n5_image" ] && [ -n "$n5_repo" ] && [ -n "$n5_corpus" ] &&
    [ -n "$n5_output" ] || usage
command -v docker >/dev/null 2>&1 || fail "Docker is required"
[ -d "$n5_repo/test/conformance/n5" ] || fail "repository root is absent"
[ -d "$n5_corpus/.git" ] || fail "parquet-testing checkout is absent"
n5_repo=$(CDPATH= cd -- "$n5_repo" && pwd -P) || fail "cannot resolve repository root"
n5_corpus=$(CDPATH= cd -- "$n5_corpus" && pwd -P) || fail "cannot resolve corpus root"
n5_output_parent=$(dirname -- "$n5_output")
n5_output_name=$(basename -- "$n5_output")
case "$n5_output_name" in
    '' | . | .. | *[!A-Za-z0-9._-]*)
        fail "output must have a portable child-directory name"
        ;;
esac
mkdir -p "$n5_output_parent"
n5_output_parent=$(CDPATH= cd -- "$n5_output_parent" && pwd -P) ||
    fail "cannot resolve output parent"
case "$n5_repo$n5_corpus$n5_output_parent" in
    *','*) fail "bind-mount paths must not contain a comma" ;;
esac
n5_output="$n5_output_parent/$n5_output_name"
if [ -e "$n5_output" ]; then
    [ ! -L "$n5_output" ] || fail "output must not be a symbolic link"
    [ -d "$n5_output" ] || fail "output is not a directory"
    n5_output_entry=$(find "$n5_output" -mindepth 1 -print -quit) ||
        fail "cannot inspect output directory"
    [ -z "$n5_output_entry" ] ||
        fail "output directory must be empty"
fi
n5_host_stage=$(mktemp -d \
    "$n5_output_parent/.n5-$n5_output_name-stage.XXXXXX") ||
    fail "cannot create host oracle stage"
cleanup() {
    rm -rf "$n5_host_stage"
}
preserve_stage() {
    echo "run-oracle-container.sh: $1; preserved at $n5_host_stage" >&2
    trap - EXIT HUP INT TERM
    exit 73
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

n5_canary="$n5_repo/test/conformance/n5/manifest.toml"
n5_canary_before=$(host_sha256 "$n5_canary")
if docker run --rm \
    --platform linux/amd64 \
    --network none \
    --mount "type=bind,src=$n5_repo,dst=/work/repo,readonly" \
    --mount "type=bind,src=$n5_corpus,dst=/work/parquet-testing,readonly" \
    --mount "type=bind,src=$n5_host_stage,dst=/work/output-stage" \
    "$n5_image" \
    /bin/sh -c '
        if printf "%s\n" n5-write-must-fail >> "$1" 2>/dev/null; then
            echo "run-oracle-container.sh: repository canary was writable" >&2
            exit 70
        fi
        export N5_READONLY_CANARY_VERIFIED=1
        shift
        exec "$@"
    ' n5-readonly-canary \
        /work/repo/test/conformance/n5/manifest.toml \
        /work/repo/test/conformance/n5/oracle-gate.sh \
        --repo /work/repo \
        --corpus /work/parquet-testing \
        --output /work/output-stage/result \
        > "$n5_host_stage/container.log" 2>&1; then
    n5_container_status=0
else
    n5_container_status=$?
fi
n5_canary_after=$(host_sha256 "$n5_canary") ||
    preserve_stage "cannot verify the repository canary after the run"
if [ "$n5_canary_after" != "$n5_canary_before" ]; then
    n5_container_status=73
    n5_host_failure="read-only repository canary changed"
else
    n5_host_failure=
fi
n5_result="$n5_host_stage/result"
if [ ! -d "$n5_result" ]; then
    mkdir "$n5_result" ||
        preserve_stage "cannot preserve missing gate result"
    printf '%s\n' "oracle container did not produce a result" \
        > "$n5_result/failure.txt" ||
        preserve_stage "cannot record the missing gate result"
    [ "$n5_container_status" -ne 0 ] || n5_container_status=73
fi
if [ "$n5_container_status" -eq 0 ]; then
    if [ ! -f "$n5_result/status.json" ] ||
        [ ! -f "$n5_result/evidence.sha256" ] ||
        ! verify_evidence "$n5_result"; then
        n5_container_status=73
        n5_host_failure="successful oracle result is incomplete or corrupt"
    fi
fi
if [ -n "$n5_host_failure" ]; then
    rm -f "$n5_result/status.json" "$n5_result/evidence.sha256" ||
        preserve_stage "cannot remove invalid success markers"
    printf '%s\n' "$n5_host_failure" > "$n5_result/host-failure.txt" ||
        preserve_stage "cannot record the host failure"
fi
if [ "$n5_container_status" -ne 0 ]; then
    rm -f "$n5_result/status.json" "$n5_result/evidence.sha256" ||
        preserve_stage "cannot remove failed success markers"
    cp "$n5_host_stage/container.log" "$n5_result/container.log" ||
        preserve_stage "cannot preserve failed container log"
    printf '{"schema_version":1,"container_exit_status":%s}\n' \
        "$n5_container_status" > "$n5_result/container-status.json" ||
        preserve_stage "cannot record the container exit status"
fi
if [ -d "$n5_output" ]; then
    rmdir "$n5_output" || preserve_stage "cannot replace empty output"
fi
if ! mv "$n5_result" "$n5_output"; then
    echo "run-oracle-container.sh: cannot commit result; preserved at $n5_result" >&2
    trap - EXIT HUP INT TERM
    exit 73
fi
exit "$n5_container_status"
