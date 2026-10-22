#!/bin/sh
set -eu

N5_REPOSITORY=ghcr.io/juliaio/parquet-jl-n5-oracles

usage() {
    echo "usage: run-oracles.sh --lock LOCK --network none" >&2
    exit 64
}

fail() {
    echo "run-oracles.sh: $1" >&2
    exit 65
}

is_hex() {
    n5_hex_value=$1
    n5_hex_length=$2
    [ "${#n5_hex_value}" -eq "$n5_hex_length" ] || return 1
    case "$n5_hex_value" in
        *[!0-9a-f]*) return 1 ;;
    esac
    return 0
}

is_sha256() {
    n5_sha_value=$1
    case "$n5_sha_value" in
        sha256:*) n5_sha_hex=${n5_sha_value#sha256:} ;;
        *) return 1 ;;
    esac
    is_hex "$n5_sha_hex" 64
}

lock_value() {
    n5_lock_key=$1
    n5_lock_result=$(sed -n \
        "s/^${n5_lock_key}[[:space:]]*=[[:space:]]*\"\([^\"]*\)\"[[:space:]]*$/\1/p" \
        "$n5_lock") || fail "cannot read lock field $n5_lock_key"
    [ -n "$n5_lock_result" ] || fail "lock field $n5_lock_key is absent"
    printf '%s\n' "$n5_lock_result"
}

expect_lock_value() {
    n5_lock_key=$1
    n5_lock_expected=$2
    n5_lock_actual=$(lock_value "$n5_lock_key")
    [ "$n5_lock_actual" = "$n5_lock_expected" ] ||
        fail "lock field $n5_lock_key differs from the exact contract"
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
    is_hex "$n5_hash_value" 64 || fail "invalid SHA-256 output for $n5_hash_file"
    printf '%s\n' "$n5_hash_value"
}

image_sha256() {
    n5_hash_file=$1
    n5_hash_output=$(docker run --rm --platform linux/amd64 --network none \
        --entrypoint sha256sum "$n5_image" "$n5_hash_file") ||
        fail "cannot hash $n5_hash_file in $n5_image"
    n5_hash_value=${n5_hash_output%% *}
    is_hex "$n5_hash_value" 64 || fail "invalid image SHA-256 for $n5_hash_file"
    printf '%s\n' "$n5_hash_value"
}

n5_root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
n5_lock=
n5_network=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --lock)
            [ "$#" -ge 2 ] || usage
            n5_lock=$2
            shift 2
            ;;
        --network)
            [ "$#" -ge 2 ] || usage
            n5_network=$2
            shift 2
            ;;
        *)
            usage
            ;;
    esac
done
[ -f "$n5_lock" ] || usage
[ "$n5_network" = "none" ] || {
    echo "run-oracles.sh: the binding gate requires --network none" >&2
    exit 64
}
command -v docker >/dev/null 2>&1 || {
    echo "run-oracles.sh: Docker is required" >&2
    exit 69
}

n5_work=$(mktemp -d)
cleanup() {
    rm -rf "$n5_work"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

printf '%s\n' \
    schema_version \
    platform \
    image_repository \
    image_digest \
    image_reference \
    base_image_digest \
    maven_version \
    maven_archive_sha512 \
    maven_artifacts_manifest_sha256 \
    maven_dependency_tree_sha256 \
    rust_version \
    rust_channel_manifest_sha256 \
    rust_tarball_sha256 \
    cargo_vendor_manifest_sha256 \
    cargo_dependency_tree_sha256 \
    harness_source_manifest_sha256 \
    parquet_java_version \
    parquet_java_commit \
    arrow_rs_version \
    arrow_rs_commit \
    parquet_testing_commit \
    fixture_manifest_sha256 > "$n5_work/expected-keys"
sed -n 's/^\([a-z0-9_][a-z0-9_]*\)[[:space:]]*=.*$/\1/p' "$n5_lock" \
    > "$n5_work/actual-keys" || fail "cannot parse lock keys"
n5_lock_lines=$(wc -l < "$n5_lock")
[ "$n5_lock_lines" -eq 22 ] || fail "lock must contain exactly 22 fields"
cmp -s "$n5_work/expected-keys" "$n5_work/actual-keys" ||
    fail "lock keys or field order differ from the exact contract"
grep -F -x 'schema_version = 1' "$n5_lock" >/dev/null ||
    fail "lock schema_version differs from the exact contract"

expect_lock_value platform linux/amd64
expect_lock_value image_repository "$N5_REPOSITORY"
expect_lock_value base_image_digest \
    sha256:ab2527b3c9b7c15bc88f60dec19b2aa39939a6e0045fb8f538eeecbd7af59c69
expect_lock_value maven_version 3.9.8
expect_lock_value maven_archive_sha512 \
    7d171def9b85846bf757a2cec94b7529371068a0670df14682447224e57983528e97a6d1b850327e4ca02b139abaab7fcb93c4315119e6f0ffb3f0cbc0d0b9a2
expect_lock_value rust_version 1.96.1
expect_lock_value rust_channel_manifest_sha256 \
    87eb76c53073e72b766083bed5530820694253b832a762d8385bda5759f03975
expect_lock_value rust_tarball_sha256 \
    d29ccb1559a177c4e72291f6e5f629de7fe8885e7521ca47802627544b121e95
expect_lock_value parquet_java_version 1.17.1
expect_lock_value parquet_java_commit 78a8d3230eb4769db93de5f2f2e18363c04cae81
expect_lock_value arrow_rs_version 59.2.0
expect_lock_value arrow_rs_commit 782e5a685501a9db6cc8e9a3b7cbff894940c47a
expect_lock_value parquet_testing_commit 09f3cdbde45302f0f0c689c950e465e98a9df960

n5_digest=$(lock_value image_digest)
is_sha256 "$n5_digest" || fail "lock has no exact image digest"
n5_image=$(lock_value image_reference)
[ "$n5_image" = "$N5_REPOSITORY@$n5_digest" ] ||
    fail "image reference does not match the fixed repository and digest"
for n5_hash_key in \
    maven_artifacts_manifest_sha256 \
    maven_dependency_tree_sha256 \
    cargo_vendor_manifest_sha256 \
    cargo_dependency_tree_sha256 \
    harness_source_manifest_sha256 \
    fixture_manifest_sha256; do
    n5_hash_value=$(lock_value "$n5_hash_key")
    is_hex "$n5_hash_value" 64 || fail "lock field $n5_hash_key is not a SHA-256"
done

docker image inspect "$n5_image" >/dev/null 2>&1 ||
    docker pull --platform linux/amd64 "$n5_image" >/dev/null
n5_repo_digests=$(docker image inspect "$n5_image" \
    --format '{{join .RepoDigests "\n"}}') || fail "cannot inspect image RepoDigests"
n5_matched=0
for n5_candidate in $n5_repo_digests; do
    [ "$n5_candidate" = "$n5_image" ] && n5_matched=1
done
[ "$n5_matched" -eq 1 ] || fail "pulled RepoDigest does not match the lock"
n5_platform=$(docker image inspect "$n5_image" \
    --format '{{.Os}}/{{.Architecture}}') || fail "cannot inspect image platform"
[ "$n5_platform" = "linux/amd64" ] || fail "locked image is not linux/amd64"

[ "$(image_sha256 /opt/n5/manifests/maven-artifacts.sha256)" = \
    "$(lock_value maven_artifacts_manifest_sha256)" ] ||
    fail "Maven artifact manifest differs from the lock"
[ "$(image_sha256 /opt/n5/manifests/maven-dependency-tree.txt)" = \
    "$(lock_value maven_dependency_tree_sha256)" ] ||
    fail "Maven dependency tree differs from the lock"
[ "$(image_sha256 /opt/n5/manifests/cargo-vendor.sha256)" = \
    "$(lock_value cargo_vendor_manifest_sha256)" ] ||
    fail "Cargo content manifest differs from the lock"
[ "$(image_sha256 /opt/n5/manifests/cargo-dependency-tree.txt)" = \
    "$(lock_value cargo_dependency_tree_sha256)" ] ||
    fail "Cargo dependency tree differs from the lock"
[ "$(image_sha256 /opt/n5/manifests/harness-source.sha256)" = \
    "$(lock_value harness_source_manifest_sha256)" ] ||
    fail "harness source manifest differs from the lock"

n5_expected_manifest=$(lock_value fixture_manifest_sha256)
n5_actual_manifest=$(host_sha256 "$n5_root/test/conformance/n5/manifest.toml")
[ "$n5_actual_manifest" = "$n5_expected_manifest" ] ||
    fail "fixture manifest differs from the lock"

n5_corpus=${PARQUET_TESTING_DIR:-"$n5_root/test/parquet-testing"}
[ -d "$n5_corpus/.git" ] || {
    echo "run-oracles.sh: pinned parquet-testing checkout is absent" >&2
    exit 66
}
n5_corpus_commit=$(git -C "$n5_corpus" rev-parse HEAD) ||
    fail "cannot read parquet-testing commit"
[ "$n5_corpus_commit" = "$(lock_value parquet_testing_commit)" ] ||
    fail "parquet-testing commit differs from the lock"

n5_output=${N5_ORACLE_OUTPUT_DIR:-"$n5_root/test/conformance/n5/output"}
"$n5_root/test/conformance/n5/run-oracle-container.sh" \
    --image "$n5_image" \
    --repo "$n5_root" \
    --corpus "$n5_corpus" \
    --output "$n5_output"
