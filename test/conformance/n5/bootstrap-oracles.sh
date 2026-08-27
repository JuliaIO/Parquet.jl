#!/bin/sh
set -eu

N5_REPOSITORY=ghcr.io/juliaio/parquet-jl-n5-oracles

usage() {
    echo "usage: bootstrap-oracles.sh --output LOCK [--publish $N5_REPOSITORY]" >&2
    exit 64
}

fail() {
    echo "bootstrap-oracles.sh: $1" >&2
    exit 65
}

is_sha256() {
    n5_value=$1
    case "$n5_value" in
        sha256:*) n5_hex=${n5_value#sha256:} ;;
        *) return 1 ;;
    esac
    [ "${#n5_hex}" -eq 64 ] || return 1
    case "$n5_hex" in
        *[!0-9a-f]*) return 1 ;;
    esac
    return 0
}

host_sha256() {
    n5_file=$1
    [ -f "$n5_file" ] || fail "required file $n5_file is absent"
    if command -v sha256sum >/dev/null 2>&1; then
        n5_output=$(sha256sum "$n5_file") || fail "cannot hash $n5_file"
    else
        n5_output=$(shasum -a 256 "$n5_file") || fail "cannot hash $n5_file"
    fi
    n5_hash=${n5_output%% *}
    is_sha256 "sha256:$n5_hash" || fail "invalid SHA-256 output for $n5_file"
    printf '%s\n' "$n5_hash"
}

image_sha256() {
    n5_file=$1
    n5_output=$(docker run --rm --platform linux/amd64 --network none \
        --entrypoint sha256sum "$n5_image" "$n5_file") ||
        fail "cannot hash $n5_file in $n5_image"
    n5_hash=${n5_output%% *}
    is_sha256 "sha256:$n5_hash" || fail "invalid image SHA-256 for $n5_file"
    printf '%s\n' "$n5_hash"
}

verify_repo_digest() {
    n5_repo_digests=$(docker image inspect "$n5_image" \
        --format '{{join .RepoDigests "\n"}}') ||
        fail "cannot inspect $n5_image"
    n5_matched=0
    for n5_candidate in $n5_repo_digests; do
        [ "$n5_candidate" = "$n5_image" ] && n5_matched=1
    done
    [ "$n5_matched" -eq 1 ] || fail "pulled RepoDigest does not match $n5_image"
}

n5_root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
n5_oracle_dir="$n5_root/test/conformance/n5/oracles"
n5_output=
n5_repository=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output)
            [ "$#" -ge 2 ] || usage
            n5_output=$2
            shift 2
            ;;
        --publish)
            [ "$#" -ge 2 ] || usage
            n5_repository=$2
            shift 2
            ;;
        *)
            usage
            ;;
    esac
done
[ -n "$n5_output" ] || usage
if [ -n "$n5_repository" ] && [ "$n5_repository" != "$N5_REPOSITORY" ]; then
    fail "--publish accepts only $N5_REPOSITORY"
fi
command -v docker >/dev/null 2>&1 || {
    echo "bootstrap-oracles.sh: Docker is required" >&2
    exit 69
}
n5_output_dir=$(dirname -- "$n5_output")
[ -d "$n5_output_dir" ] || fail "lock output directory is absent"
n5_fixture_path="$n5_root/test/conformance/n5/manifest.toml"
n5_fixture_manifest_before=$(host_sha256 "$n5_fixture_path")
n5_verified_existing=0

n5_work=$(mktemp -d)
n5_staged_lock=
n5_local_tag=
n5_published_tag=
cleanup() {
    rm -rf "$n5_work"
    [ -z "$n5_staged_lock" ] || rm -f "$n5_staged_lock"
    [ -z "$n5_local_tag" ] ||
        docker image rm "$n5_local_tag" >/dev/null 2>&1 || true
    [ -z "$n5_published_tag" ] ||
        docker image rm "$n5_published_tag" >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

n5_local_tag="parquet-jl-n5-oracles:bootstrap-$$"
n5_image_archive="$n5_work/oracles.oci.tar"
docker buildx build \
    --no-cache \
    --file "$n5_oracle_dir/Dockerfile" \
    --build-arg SOURCE_DATE_EPOCH=1787356800 \
    --output "type=oci,dest=$n5_image_archive,rewrite-timestamp=true" \
    --platform linux/amd64 \
    --provenance=false \
    --sbom=false \
    --tag "$n5_local_tag" \
    "$n5_oracle_dir"
docker load --input "$n5_image_archive" >/dev/null
rm "$n5_image_archive"
docker run --rm --platform linux/amd64 --network none "$n5_local_tag"
n5_image_id=$(docker image inspect "$n5_local_tag" --format '{{.Id}}') ||
    fail "cannot inspect the validated local image"
is_sha256 "$n5_image_id" || fail "validated local image has no exact image ID"
if [ -f "$n5_output" ]; then
    n5_expected_image=$(sed -n \
        's/^image_reference[[:space:]]*=[[:space:]]*"\([^"]*\)"[[:space:]]*$/\1/p' \
        "$n5_output") || fail "cannot inspect existing oracle lock"
    case "$n5_expected_image" in
        "$N5_REPOSITORY"@sha256:*) ;;
        *) fail "existing oracle lock has no exact image reference" ;;
    esac
    docker image inspect "$n5_expected_image" >/dev/null 2>&1 ||
        docker pull --platform linux/amd64 "$n5_expected_image" >/dev/null
    n5_expected_id=$(docker image inspect "$n5_expected_image" --format '{{.Id}}') ||
        fail "cannot inspect existing locked oracle image"
    [ "$n5_image_id" = "$n5_expected_id" ] ||
        fail "clean build is not reproducible with the existing oracle lock"
    n5_verified_existing=1
fi
if [ -z "$n5_repository" ]; then
    if [ "$n5_verified_existing" -eq 1 ]; then
        echo "bootstrap-oracles.sh: clean image $n5_image_id matches the existing lock" >&2
        exit 0
    fi
    echo "bootstrap-oracles.sh: local image $n5_image_id passed the offline check" >&2
    echo "bootstrap-oracles.sh: no lock was written because no published repository digest exists" >&2
    echo "bootstrap-oracles.sh: rerun with explicit --publish $N5_REPOSITORY after publication is authorized" >&2
    exit 2
fi

n5_image_hex=${n5_image_id#sha256:}
n5_published_tag="$n5_repository:bootstrap-$n5_image_hex"
docker image tag "$n5_local_tag" "$n5_published_tag"
n5_push_output=$(docker image push --quiet --platform linux/amd64 \
    "$n5_published_tag" 2>&1) || fail "publishing the validated image failed"
n5_digest=
for n5_word in $n5_push_output; do
    if is_sha256 "$n5_word"; then
        n5_digest=$n5_word
    fi
done
if [ -z "$n5_digest" ]; then
    n5_repo_digests=$(docker image inspect "$n5_published_tag" \
        --format '{{join .RepoDigests "\n"}}') ||
        fail "cannot inspect the published image"
    for n5_candidate in $n5_repo_digests; do
        case "$n5_candidate" in
            "$n5_repository"@sha256:*) n5_digest=${n5_candidate#*@} ;;
        esac
    done
fi
is_sha256 "$n5_digest" || fail "published image digest is absent"
n5_image="$n5_repository@$n5_digest"
docker pull --platform linux/amd64 "$n5_image" >/dev/null
verify_repo_digest
n5_pulled_id=$(docker image inspect "$n5_image" --format '{{.Id}}') ||
    fail "cannot inspect the pulled image"
[ "$n5_pulled_id" = "$n5_image_id" ] ||
    fail "published image differs from the validated local image"
n5_platform=$(docker image inspect "$n5_image" \
    --format '{{.Os}}/{{.Architecture}}') || fail "cannot inspect image platform"
[ "$n5_platform" = "linux/amd64" ] || fail "published image is not linux/amd64"
docker run --rm --platform linux/amd64 --network none "$n5_image"

n5_maven_manifest=$(image_sha256 /opt/n5/manifests/maven-artifacts.sha256)
n5_cargo_manifest=$(image_sha256 /opt/n5/manifests/cargo-vendor.sha256)
n5_source_manifest=$(image_sha256 /opt/n5/manifests/harness-source.sha256)
n5_maven_tree=$(image_sha256 /opt/n5/manifests/maven-dependency-tree.txt)
n5_cargo_tree=$(image_sha256 /opt/n5/manifests/cargo-dependency-tree.txt)
n5_fixture_manifest=$(host_sha256 "$n5_fixture_path")
[ "$n5_fixture_manifest" = "$n5_fixture_manifest_before" ] ||
    fail "fixture manifest changed during bootstrap"

n5_lock="$n5_work/oracles.lock"
{
    printf 'schema_version = 1\n'
    printf 'platform = "linux/amd64"\n'
    printf 'image_repository = "%s"\n' "$N5_REPOSITORY"
    printf 'image_digest = "%s"\n' "$n5_digest"
    printf 'image_reference = "%s"\n' "$n5_image"
    printf 'base_image_digest = "sha256:ab2527b3c9b7c15bc88f60dec19b2aa39939a6e0045fb8f538eeecbd7af59c69"\n'
    printf 'maven_version = "3.9.8"\n'
    printf 'maven_archive_sha512 = "7d171def9b85846bf757a2cec94b7529371068a0670df14682447224e57983528e97a6d1b850327e4ca02b139abaab7fcb93c4315119e6f0ffb3f0cbc0d0b9a2"\n'
    printf 'maven_artifacts_manifest_sha256 = "%s"\n' "$n5_maven_manifest"
    printf 'maven_dependency_tree_sha256 = "%s"\n' "$n5_maven_tree"
    printf 'rust_version = "1.96.1"\n'
    printf 'rust_channel_manifest_sha256 = "87eb76c53073e72b766083bed5530820694253b832a762d8385bda5759f03975"\n'
    printf 'rust_tarball_sha256 = "d29ccb1559a177c4e72291f6e5f629de7fe8885e7521ca47802627544b121e95"\n'
    printf 'cargo_vendor_manifest_sha256 = "%s"\n' "$n5_cargo_manifest"
    printf 'cargo_dependency_tree_sha256 = "%s"\n' "$n5_cargo_tree"
    printf 'harness_source_manifest_sha256 = "%s"\n' "$n5_source_manifest"
    printf 'parquet_java_version = "1.17.1"\n'
    printf 'parquet_java_commit = "78a8d3230eb4769db93de5f2f2e18363c04cae81"\n'
    printf 'arrow_rs_version = "59.2.0"\n'
    printf 'arrow_rs_commit = "782e5a685501a9db6cc8e9a3b7cbff894940c47a"\n'
    printf 'parquet_testing_commit = "09f3cdbde45302f0f0c689c950e465e98a9df960"\n'
    printf 'fixture_manifest_sha256 = "%s"\n' "$n5_fixture_manifest"
} > "$n5_lock"
n5_staged_lock=$(mktemp "$n5_output_dir/.n5-oracles.lock.XXXXXX")
install -m 0444 "$n5_lock" "$n5_staged_lock"
mv -f "$n5_staged_lock" "$n5_output"
n5_staged_lock=
echo "wrote $n5_output for $n5_image"
