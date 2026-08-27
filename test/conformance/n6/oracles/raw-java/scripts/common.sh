#!/usr/bin/env bash
set -euo pipefail

RAW_JAVA_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RAW_JAVA_REPO=$(cd "$RAW_JAVA_ROOT/../../../../.." && pwd)
RAW_JAVA_BUILD_DIR=${PARQUET_N6_RAW_JAVA_BUILD_DIR:-$RAW_JAVA_ROOT/build}
RAW_JAVA_DOWNLOAD_DIR="$RAW_JAVA_BUILD_DIR/downloads"
RAW_JAVA_GENERATED_DIR="$RAW_JAVA_BUILD_DIR/generated"
RAW_JAVA_CLASSES_DIR="$RAW_JAVA_BUILD_DIR/classes"
RAW_JAVA_JDK_DIR="$RAW_JAVA_BUILD_DIR/jdk"
RAW_JAVA_JDK_MANIFEST="$RAW_JAVA_ROOT/jdk-darwin-arm64-sequoia.manifest"
RAW_JAVA_IDL="$RAW_JAVA_REPO/thrift/parquet.thrift"
RAW_JAVA_PLAN="$RAW_JAVA_REPO/docs/dev/n6-statistics-plan.md"

# shellcheck source=../toolchain.env
source "$RAW_JAVA_ROOT/toolchain.env"

raw_java_sha256() {
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        sha256sum "$1" | awk '{print $1}'
    fi
}

raw_java_verify() {
    local expected=$1
    local file=$2
    local actual
    actual=$(raw_java_sha256 "$file")
    [[ $actual == "$expected" ]] || {
        echo "raw-java: SHA-256 mismatch for $file" >&2
        echo "raw-java: expected $expected" >&2
        echo "raw-java: actual   $actual" >&2
        return 1
    }
}

raw_java_mode() {
    if stat -f '%Lp' "$1" >/dev/null 2>&1; then
        stat -f '%Lp' "$1"
    else
        stat -c '%a' "$1"
    fi
}

raw_java_tree_manifest() {
    local tree=$1
    local output=$2
    [[ -d $tree ]] || {
        echo "raw-java: JDK tree is absent: $tree" >&2
        return 1
    }
    : > "$output"
    while IFS= read -r path; do
        local relative=${path#"$tree"/}
        local mode
        mode=$(raw_java_mode "$path")
        if [[ -L $path ]]; then
            printf 'link\t%s\t%s\t%s\n' "$mode" "$relative" "$(readlink "$path")" >> "$output"
        elif [[ -d $path ]]; then
            printf 'directory\t%s\t%s\n' "$mode" "$relative" >> "$output"
        elif [[ -f $path ]]; then
            printf 'file\t%s\t%s\t%s\n' \
                "$mode" "$relative" "$(raw_java_sha256 "$path")" >> "$output"
        else
            echo "raw-java: unsupported entry in JDK tree: $relative" >&2
            return 1
        fi
    done < <(find "$tree" -mindepth 1 -print | LC_ALL=C sort)
}

raw_java_tree_sha256() {
    local tree=$1
    local manifest
    manifest=$(mktemp "${TMPDIR:-/tmp}/raw-java-jdk-tree.XXXXXX")
    if ! raw_java_tree_manifest "$tree" "$manifest"; then
        rm -f "$manifest"
        return 1
    fi
    local digest
    digest=$(raw_java_sha256 "$manifest")
    rm -f "$manifest"
    printf '%s\n' "$digest"
}

raw_java_verify_tree() {
    local expected=$1
    local tree=$2
    raw_java_verify "$expected" "$RAW_JAVA_JDK_MANIFEST"
    local actual_manifest
    actual_manifest=$(mktemp "${TMPDIR:-/tmp}/raw-java-jdk-tree.XXXXXX")
    if ! raw_java_tree_manifest "$tree" "$actual_manifest"; then
        rm -f "$actual_manifest"
        return 1
    fi
    if cmp -s "$RAW_JAVA_JDK_MANIFEST" "$actual_manifest"; then
        rm -f "$actual_manifest"
        return
    fi
    local actual
    actual=$(raw_java_sha256 "$actual_manifest")
    rm -f "$actual_manifest"
    {
        echo "raw-java: full JDK tree SHA-256 mismatch for $tree" >&2
        echo "raw-java: expected $expected" >&2
        echo "raw-java: actual   $actual" >&2
    }
    return 1
}

raw_java_download() {
    local url=$1
    local expected=$2
    local output=$3
    if [[ -f $output ]]; then
        raw_java_verify "$expected" "$output"
        return
    fi
    mkdir -p "$(dirname "$output")"
    local temporary="$output.part"
    curl --fail --location --silent --show-error --output "$temporary" "$url"
    raw_java_verify "$expected" "$temporary"
    mv "$temporary" "$output"
}

raw_java_verify_authority() {
    raw_java_verify "$RAW_JAVA_PLAN_SHA256" "$RAW_JAVA_PLAN"
    raw_java_verify "$RAW_JAVA_IDL_SHA256" "$RAW_JAVA_IDL"
}

raw_java_classpath() {
    printf '%s:%s:%s:%s\n' \
        "$RAW_JAVA_CLASSES_DIR" \
        "$RAW_JAVA_DOWNLOAD_DIR/libthrift-$RAW_JAVA_THRIFT_VERSION.jar" \
        "$RAW_JAVA_DOWNLOAD_DIR/slf4j-api-1.7.36.jar" \
        "$RAW_JAVA_DOWNLOAD_DIR/slf4j-nop-1.7.36.jar"
}

raw_java_java() {
    printf '%s\n' "$RAW_JAVA_JDK_DIR/bin/java"
}

raw_java_javac() {
    printf '%s\n' "$RAW_JAVA_JDK_DIR/bin/javac"
}
