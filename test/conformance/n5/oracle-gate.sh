#!/bin/sh
set -eu

usage() {
    echo "usage: oracle-gate.sh --repo REPO --corpus CORPUS --output DIR" >&2
    exit 64
}

n5_stage=
fail() {
    echo "oracle-gate.sh: $1" >&2
    if [ -n "$n5_stage" ] && [ -d "$n5_stage" ]; then
        printf '%s\n' "$1" > "$n5_stage/failure.txt"
    fi
    exit 65
}

is_hex64() {
    n5_hex=$1
    [ "${#n5_hex}" -eq 64 ] || return 1
    case "$n5_hex" in
        *[!0-9a-f]*) return 1 ;;
    esac
    return 0
}

require_plain_tree() {
    n5_tree=$1
    n5_label=$2
    [ -d "$n5_tree" ] || fail "$n5_label directory is absent"
    n5_special=$(find "$n5_tree" ! -type d ! -type f -print -quit) ||
        fail "cannot inspect $n5_label file types"
    [ -z "$n5_special" ] || fail "$n5_label contains a non-regular file"
}

check_sha_manifest() {
    n5_manifest=$1
    n5_base=$2
    n5_expected_count=$3
    n5_label=$4
    [ -f "$n5_manifest" ] || fail "$n5_label hash manifest is absent"
    awk '
        NF != 2 || length($1) != 64 || $1 !~ /^[0-9a-f]+$/ ||
            $2 ~ /^\// || $2 ~ /(^|\/)\.\.?($|\/)/ || $2 ~ /\\/ { exit 1 }
        { print $2 }
    ' "$n5_manifest" > "$n5_stage/$n5_label.paths" ||
        fail "$n5_label hash manifest is malformed"
    n5_count=$(wc -l < "$n5_stage/$n5_label.paths")
    [ "$n5_count" -eq "$n5_expected_count" ] ||
        fail "$n5_label hash manifest count differs"
    LC_ALL=C sort "$n5_stage/$n5_label.paths" \
        > "$n5_stage/$n5_label.paths.sorted" ||
        fail "cannot sort $n5_label hash paths"
    cmp -s "$n5_stage/$n5_label.paths" "$n5_stage/$n5_label.paths.sorted" ||
        fail "$n5_label hash paths are not sorted"
    LC_ALL=C sort -u "$n5_stage/$n5_label.paths" \
        > "$n5_stage/$n5_label.paths.unique" ||
        fail "cannot deduplicate $n5_label hash paths"
    n5_unique=$(wc -l < "$n5_stage/$n5_label.paths.unique")
    [ "$n5_unique" -eq "$n5_expected_count" ] ||
        fail "$n5_label hash paths are not unique"
    (cd "$n5_base" && sha256sum --check --quiet --strict "$n5_manifest") ||
        fail "$n5_label file hash differs"
}

compare_fixture_directory() {
    n5_generated=$1
    n5_checked=$2
    n5_count=$3
    n5_label=$4
    require_plain_tree "$n5_generated" "$n5_label generated fixtures"
    require_plain_tree "$n5_checked" "$n5_label checked fixtures"
    n5_subdirectory=$(find "$n5_generated" -mindepth 1 -type d -print -quit) ||
        fail "cannot inspect generated $n5_label directories"
    [ -z "$n5_subdirectory" ] ||
        fail "$n5_label generated fixture tree contains a subdirectory"
    n5_subdirectory=$(find "$n5_checked" -mindepth 1 -type d -print -quit) ||
        fail "cannot inspect checked $n5_label directories"
    [ -z "$n5_subdirectory" ] ||
        fail "$n5_label checked fixture tree contains a subdirectory"
    find "$n5_generated" -mindepth 1 -maxdepth 1 -type f \
        -print > "$n5_stage/$n5_label.generated.raw" ||
        fail "cannot list generated $n5_label fixtures"
    sed 's#^.*/##' "$n5_stage/$n5_label.generated.raw" \
        > "$n5_stage/$n5_label.generated.unsorted" ||
        fail "cannot normalize generated $n5_label fixture names"
    LC_ALL=C sort "$n5_stage/$n5_label.generated.unsorted" \
        > "$n5_stage/$n5_label.generated" ||
        fail "cannot normalize generated $n5_label fixture names"
    find "$n5_checked" -mindepth 1 -maxdepth 1 -type f \
        -print > "$n5_stage/$n5_label.checked.raw" ||
        fail "cannot list checked $n5_label fixtures"
    sed 's#^.*/##' "$n5_stage/$n5_label.checked.raw" \
        > "$n5_stage/$n5_label.checked.unsorted" ||
        fail "cannot normalize checked $n5_label fixture names"
    LC_ALL=C sort "$n5_stage/$n5_label.checked.unsorted" \
        > "$n5_stage/$n5_label.checked" ||
        fail "cannot normalize checked $n5_label fixture names"
    n5_actual=$(wc -l < "$n5_stage/$n5_label.generated")
    [ "$n5_actual" -eq "$n5_count" ] ||
        fail "$n5_label generated fixture count differs"
    cmp -s "$n5_stage/$n5_label.generated" "$n5_stage/$n5_label.checked" ||
        fail "$n5_label generated fixture names differ"
    while IFS= read -r n5_name; do
        cmp -s "$n5_generated/$n5_name" "$n5_checked/$n5_name" ||
            fail "$n5_label generated fixture differs: $n5_name"
    done < "$n5_stage/$n5_label.generated"
}

n5_repo=
n5_corpus=
n5_output=
while [ "$#" -gt 0 ]; do
    case "$1" in
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
[ -n "$n5_repo" ] && [ -n "$n5_corpus" ] && [ -n "$n5_output" ] || usage
[ -d "$n5_repo/test/conformance/n5" ] || fail "N5 repository root is absent"
[ -d "$n5_corpus/.git" ] || fail "parquet-testing checkout is absent"
n5_parent=$(dirname "$n5_output")
[ -d "$n5_parent" ] || fail "output parent is absent"
if [ -e "$n5_output" ]; then
    [ -d "$n5_output" ] || fail "output path is not a directory"
    n5_output_entry=$(find "$n5_output" -mindepth 1 -print -quit) ||
        fail "cannot inspect output directory"
    [ -z "$n5_output_entry" ] ||
        fail "output directory must be empty"
fi
n5_stage=$(mktemp -d "$n5_output.stage.XXXXXX") || fail "cannot create output stage"
[ "${N5_READONLY_CANARY_VERIFIED:-}" = 1 ] ||
    fail "read-only repository canary was not verified"
printf '%s\n' '{"schema_version":1,"repository_mount":"read-only"}' \
    > "$n5_stage/read-only-canary.json" ||
    fail "cannot record read-only repository canary"

finish() {
    n5_status=$?
    trap - EXIT HUP INT TERM
    set +e
    if [ -d "$n5_stage" ]; then
        if [ "$n5_status" -eq 0 ]; then
            if ! printf '%s\n' '{"schema_version":1,"status":"ok"}' \
                > "$n5_stage/status.json"; then
                n5_status=73
            elif ! find "$n5_stage" -type f ! -name evidence.sha256 \
                ! -name evidence.raw ! -name evidence.unsorted \
                ! -name evidence.files -print > "$n5_stage/evidence.raw"; then
                n5_status=73
            elif ! sed "s#^$n5_stage/##" "$n5_stage/evidence.raw" \
                > "$n5_stage/evidence.unsorted"; then
                n5_status=73
            elif ! LC_ALL=C sort "$n5_stage/evidence.unsorted" \
                > "$n5_stage/evidence.files"; then
                n5_status=73
            elif ! (cd "$n5_stage" && while IFS= read -r n5_file; do
                sha256sum "$n5_file" || exit 1
            done < evidence.files > evidence.sha256); then
                n5_status=73
            elif ! rm "$n5_stage/evidence.raw" "$n5_stage/evidence.unsorted" \
                "$n5_stage/evidence.files"; then
                n5_status=73
            fi
            if [ "$n5_status" -ne 0 ]; then
                echo "oracle-gate.sh: cannot finalize evidence" >&2
                rm -f "$n5_stage/status.json" "$n5_stage/evidence.sha256"
                printf '%s\n' "cannot finalize evidence" \
                    > "$n5_stage/failure.txt"
            fi
        elif [ ! -f "$n5_stage/failure.txt" ]; then
            printf '%s\n' "oracle gate exited with status $n5_status" \
                > "$n5_stage/failure.txt"
        fi
        if [ -d "$n5_output" ]; then
            rmdir "$n5_output" || {
                echo "oracle-gate.sh: cannot commit evidence over output" >&2
                exit 73
            }
        fi
        mv "$n5_stage" "$n5_output" || {
            echo "oracle-gate.sh: cannot commit evidence" >&2
            exit 73
        }
    fi
    exit "$n5_status"
}
trap finish EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

n5_root="$n5_repo/test/conformance/n5"
n5_fixtures="$n5_root/julia-fixtures"
require_plain_tree "$n5_fixtures" "Julia fixture corpus"

check_sha_manifest "$n5_root/external-files.sha256" "$n5_repo" 42 external
{
    find "$n5_root/golden" "$n5_root/expected" -type f -print
    printf '%s\n' "$n5_root/manifest.toml"
} > "$n5_stage/external.actual.raw" || fail "cannot list external checked files"
sed "s#^$n5_repo/##" "$n5_stage/external.actual.raw" \
    > "$n5_stage/external.actual.unsorted" ||
    fail "cannot normalize external checked files"
LC_ALL=C sort "$n5_stage/external.actual.unsorted" \
    > "$n5_stage/external.actual" ||
    fail "cannot normalize external checked files"
cmp -s "$n5_stage/external.paths" "$n5_stage/external.actual" ||
    fail "external checked file set differs"

n5_corpus_commit=$(git -C "$n5_corpus" rev-parse HEAD) ||
    fail "cannot read parquet-testing revision"
[ "$n5_corpus_commit" = "09f3cdbde45302f0f0c689c950e465e98a9df960" ] ||
    fail "parquet-testing revision differs"
check_sha_manifest "$n5_root/corpus-files.sha256" "$n5_corpus" 14 corpus

check_sha_manifest "$n5_root/julia-fixtures.files.sha256" "$n5_root" 1 \
    julia-fixture-manifest
check_sha_manifest "$n5_fixtures/files.sha256" "$n5_fixtures" 353 julia-fixtures
find "$n5_fixtures" -type f -print > "$n5_stage/julia-fixtures.actual.raw" ||
    fail "cannot list Julia fixture corpus"
sed "s#^$n5_fixtures/##" "$n5_stage/julia-fixtures.actual.raw" \
    > "$n5_stage/julia-fixtures.relative" ||
    fail "cannot normalize Julia fixture corpus"
awk '$0 != "files.sha256" { print }' "$n5_stage/julia-fixtures.relative" \
    > "$n5_stage/julia-fixtures.unsorted" ||
    fail "cannot normalize Julia fixture corpus"
LC_ALL=C sort "$n5_stage/julia-fixtures.unsorted" \
    > "$n5_stage/julia-fixtures.actual" ||
    fail "cannot normalize Julia fixture corpus"
cmp -s "$n5_stage/julia-fixtures.paths" "$n5_stage/julia-fixtures.actual" ||
    fail "Julia fixture file set differs"

mkdir "$n5_stage/java-generated" "$n5_stage/rust-generated" \
    "$n5_stage/rust-owned-checked" "$n5_stage/rust-neighbor-generated" ||
    fail "cannot create generator stages"
if ! /opt/bootstrap/parquet-java/run.sh --offline generate \
    --output "$n5_stage/java-generated" \
    --evidence "$n5_stage/java-generated.jsonl" \
    > "$n5_stage/java-generate.log" 2>&1; then
    fail "Parquet Java fixture generation failed"
fi
compare_fixture_directory "$n5_stage/java-generated" \
    "$n5_root/golden/parquet-java" 30 parquet-java
cmp -s "$n5_stage/java-generated.jsonl" \
    "$n5_root/expected/parquet-java.jsonl" ||
    fail "Parquet Java generated evidence differs"

if ! parquet-jl-n5-arrow-rs-oracle generate \
    --output "$n5_stage/rust-generated" \
    --evidence "$n5_stage/rust-generated.json" \
    > "$n5_stage/rust-generate.log" 2>&1; then
    fail "Arrow Rust fixture generation failed"
fi
find "$n5_root/golden/arrow-rs" -mindepth 1 -maxdepth 1 -type f \
    -name '*.parquet' ! -name '*near-neighbor*' -print \
    > "$n5_stage/rust-owned.checked.raw" ||
    fail "cannot list checked Arrow Rust owned fixtures"
while IFS= read -r n5_file; do
    cp "$n5_file" "$n5_stage/rust-owned-checked/" ||
        fail "cannot stage checked Arrow Rust owned fixture"
done < "$n5_stage/rust-owned.checked.raw"
compare_fixture_directory "$n5_stage/rust-generated" \
    "$n5_stage/rust-owned-checked" 6 arrow-rs-owned
cmp -s "$n5_stage/rust-generated.json" "$n5_root/expected/arrow-rs.json" ||
    fail "Arrow Rust generated evidence differs"

if ! parquet-jl-n5-arrow-rs-oracle diagnose-rule3-near-neighbor \
    --output "$n5_stage/rust-neighbor-generated" \
    --evidence "$n5_stage/rust-neighbor-generated.json" \
    > "$n5_stage/rust-neighbor-generate.log" 2>&1; then
    fail "Arrow Rust near-neighbor generation failed"
fi
find "$n5_root/golden/arrow-rs" -mindepth 1 -maxdepth 1 -type f \
    -name '*near-neighbor*' -print > "$n5_stage/rust-neighbor.checked.raw" ||
    fail "cannot list checked Arrow Rust near-neighbor fixtures"
mkdir "$n5_stage/rust-neighbor-checked" ||
    fail "cannot create Arrow Rust near-neighbor comparison stage"
while IFS= read -r n5_file; do
    cp "$n5_file" "$n5_stage/rust-neighbor-checked/" ||
        fail "cannot stage checked Arrow Rust near-neighbor fixture"
done < "$n5_stage/rust-neighbor.checked.raw"
compare_fixture_directory "$n5_stage/rust-neighbor-generated" \
    "$n5_stage/rust-neighbor-checked" 2 arrow-rs-neighbor
cmp -s "$n5_stage/rust-neighbor-generated.json" \
    "$n5_root/expected/arrow-rs-rule3-near-neighbor.json" ||
    fail "Arrow Rust near-neighbor evidence differs"

if ! /opt/bootstrap/parquet-java/run.sh --offline audit \
    --input "$n5_fixtures" --evidence "$n5_stage/java-audit.jsonl" \
    > "$n5_stage/java-audit.log" 2>&1; then
    fail "Parquet Java Julia-fixture audit failed"
fi
if ! parquet-jl-n5-arrow-rs-oracle audit \
    --input "$n5_fixtures" --evidence "$n5_stage/rust-audit.json" \
    > "$n5_stage/rust-audit.log" 2>&1; then
    fail "Arrow Rust Julia-fixture audit failed"
fi
if ! perl "$n5_root/compare-evidence.pl" \
    --manifest "$n5_fixtures/fixture-manifest.tsv" \
    --unsupported "$n5_root/oracle-unsupported.tsv" \
    --java "$n5_stage/java-audit.jsonl" \
    --rust "$n5_stage/rust-audit.json" \
    --output "$n5_stage/summary.json" \
    > "$n5_stage/compare.log" 2>&1; then
    fail "Julia fixture evidence comparison failed"
fi

cp "$n5_root/external-files.sha256" "$n5_stage/" ||
    fail "cannot preserve external file manifest"
cp "$n5_root/corpus-files.sha256" "$n5_stage/" ||
    fail "cannot preserve corpus file manifest"
cp "$n5_root/julia-fixtures.files.sha256" "$n5_stage/" ||
    fail "cannot preserve Julia fixture manifest digest"
cp "$n5_fixtures/files.sha256" "$n5_stage/julia-files.sha256" ||
    fail "cannot preserve Julia fixture hashes"
cp "$n5_fixtures/fixture-manifest.tsv" "$n5_stage/" ||
    fail "cannot preserve Julia fixture mapping manifest"
cp "$n5_root/oracle-unsupported.tsv" "$n5_stage/" ||
    fail "cannot preserve oracle unsupported allowlist"

exit 0
