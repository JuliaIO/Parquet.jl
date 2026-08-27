#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=common.sh
source "$root/common.sh"

raw_java_platform_pins() {
    local system
    local machine
    system=$(uname -s)
    machine=$(uname -m)
    case "$system:$machine" in
        Darwin:arm64)
            local major
            major=$(sw_vers -productVersion | cut -d. -f1)
            [[ $major == 15 ]] || {
                echo "raw-java: the exact toolchain is validated only on macOS 15 arm64" >&2
                return 1
            }
            printf '%s\n' \
                "$RAW_JAVA_BOTTLE_DARWIN_ARM64_SEQUOIA_SHA256" \
                "$RAW_JAVA_COMPILER_DARWIN_ARM64_SEQUOIA_SHA256" \
                "$RAW_JAVA_JDK_DARWIN_ARM64_SEQUOIA_URL" \
                "$RAW_JAVA_JDK_DARWIN_ARM64_SEQUOIA_SHA256" \
                "$RAW_JAVA_JDK_DARWIN_ARM64_SEQUOIA_JAVA_SHA256" \
                "$RAW_JAVA_JDK_DARWIN_ARM64_SEQUOIA_JAVAC_SHA256" \
                "$RAW_JAVA_JDK_DARWIN_ARM64_SEQUOIA_RELEASE_SHA256" \
                "$RAW_JAVA_JDK_DARWIN_ARM64_SEQUOIA_TREE_SHA256"
            ;;
        *)
            echo "raw-java: no pinned Thrift compiler for $system $machine" >&2
            return 1
            ;;
    esac
}

raw_java_fetch_bottle() {
    local digest=$1
    local output=$2
    if [[ -f $output ]]; then
        raw_java_verify "$digest" "$output"
        return
    fi
    mkdir -p "$(dirname "$output")"
    local token
    token=$(curl --fail --silent --show-error \
        'https://ghcr.io/token?scope=repository:homebrew/core/thrift:pull&service=ghcr.io' |
        sed -E 's/.*"token":"([^"]+)".*/\1/')
    [[ -n $token ]] || {
        echo "raw-java: cannot obtain the public GHCR token" >&2
        return 1
    }
    local temporary="$output.part"
    curl --fail --location --silent --show-error \
        -H "Authorization: Bearer $token" \
        --output "$temporary" \
        "https://ghcr.io/v2/homebrew/core/thrift/blobs/sha256:$digest"
    raw_java_verify "$digest" "$temporary"
    mv "$temporary" "$output"
}

raw_java_install_compiler() {
    local bottle_digest=$1
    local compiler_digest=$2
    local compiler="$RAW_JAVA_BUILD_DIR/compiler/thrift"
    if [[ -x $compiler ]]; then
        if [[ $(raw_java_sha256 "$compiler") == "$compiler_digest" ]] &&
                [[ $("$compiler" --version) == "Thrift version $RAW_JAVA_THRIFT_VERSION" ]]; then
            return
        fi
    fi
    local bottle="$RAW_JAVA_DOWNLOAD_DIR/thrift-$RAW_JAVA_THRIFT_VERSION-$bottle_digest.bottle.tar.gz"
    raw_java_fetch_bottle "$bottle_digest" "$bottle"
    local unpacked="$RAW_JAVA_BUILD_DIR/compiler-unpacked"
    mkdir -p "$unpacked" "$RAW_JAVA_BUILD_DIR/compiler"
    find "$unpacked" -mindepth 1 -delete
    tar -xzf "$bottle" -C "$unpacked"
    local source
    source=$(find "$unpacked" -type f -path '*/bin/thrift' -print -quit)
    [[ -n $source ]] || {
        echo "raw-java: compiler bottle does not contain bin/thrift" >&2
        return 1
    }
    cp "$source" "$compiler"
    chmod 0755 "$compiler"
    raw_java_verify "$compiler_digest" "$compiler"
    [[ $("$compiler" --version) == "Thrift version $RAW_JAVA_THRIFT_VERSION" ]] || {
        echo "raw-java: extracted compiler has the wrong version" >&2
        return 1
    }
}

raw_java_install_jdk() {
    local url=$1
    local archive_digest=$2
    local java_digest=$3
    local javac_digest=$4
    local release_digest=$5
    local tree_digest=$6
    local archive="$RAW_JAVA_DOWNLOAD_DIR/OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.8_9.tar.gz"
    raw_java_download "$url" "$archive_digest" "$archive"
    if [[ -x $RAW_JAVA_JDK_DIR/bin/java && -x $RAW_JAVA_JDK_DIR/bin/javac &&
            -f $RAW_JAVA_JDK_DIR/release ]] &&
            raw_java_verify "$java_digest" "$RAW_JAVA_JDK_DIR/bin/java" >/dev/null 2>&1 &&
            raw_java_verify "$javac_digest" "$RAW_JAVA_JDK_DIR/bin/javac" >/dev/null 2>&1 &&
            raw_java_verify "$release_digest" "$RAW_JAVA_JDK_DIR/release" >/dev/null 2>&1 &&
            raw_java_verify_tree "$tree_digest" "$RAW_JAVA_JDK_DIR" >/dev/null 2>&1; then
        return
    fi
    local unpacked="$RAW_JAVA_BUILD_DIR/jdk-unpacked"
    mkdir -p "$unpacked"
    find "$unpacked" -mindepth 1 -delete
    tar -xzf "$archive" -C "$unpacked"
    local home
    home=$(find "$unpacked" -type f -path '*/bin/java' -print -quit)
    [[ -n $home ]] || {
        echo "raw-java: pinned JDK archive does not contain bin/java" >&2
        return 1
    }
    home=${home%/bin/java}
    find "$RAW_JAVA_JDK_DIR" -mindepth 1 -delete 2>/dev/null || true
    mkdir -p "$RAW_JAVA_JDK_DIR"
    cp -R "$home/." "$RAW_JAVA_JDK_DIR/"
    raw_java_verify "$java_digest" "$RAW_JAVA_JDK_DIR/bin/java"
    raw_java_verify "$javac_digest" "$RAW_JAVA_JDK_DIR/bin/javac"
    raw_java_verify "$release_digest" "$RAW_JAVA_JDK_DIR/release"
    raw_java_verify_tree "$tree_digest" "$RAW_JAVA_JDK_DIR"
    [[ $("$RAW_JAVA_JDK_DIR/bin/javac" -version 2>&1) == "javac 21.0.8" ]] || {
        echo "raw-java: extracted javac has the wrong version" >&2
        return 1
    }
}

raw_java_verify_authority
mkdir -p "$RAW_JAVA_DOWNLOAD_DIR"
platform_pins=()
while IFS= read -r pin; do
    platform_pins+=("$pin")
done < <(raw_java_platform_pins)
[[ ${#platform_pins[@]} -eq 8 ]] || {
    echo "raw-java: incomplete platform toolchain pins" >&2
    exit 1
}
raw_java_download "$RAW_JAVA_THRIFT_SOURCE_URL" \
    "$RAW_JAVA_THRIFT_SOURCE_SHA256" \
    "$RAW_JAVA_DOWNLOAD_DIR/thrift-$RAW_JAVA_THRIFT_VERSION.tar.gz"
raw_java_download "$RAW_JAVA_HOMEBREW_FORMULA_URL" \
    "$RAW_JAVA_HOMEBREW_FORMULA_SHA256" \
    "$RAW_JAVA_DOWNLOAD_DIR/homebrew-thrift-$RAW_JAVA_HOMEBREW_FORMULA_COMMIT.rb"
raw_java_download "$RAW_JAVA_LIBTHRIFT_URL" \
    "$RAW_JAVA_LIBTHRIFT_SHA256" \
    "$RAW_JAVA_DOWNLOAD_DIR/libthrift-$RAW_JAVA_THRIFT_VERSION.jar"
raw_java_download "$RAW_JAVA_LIBTHRIFT_POM_URL" \
    "$RAW_JAVA_LIBTHRIFT_POM_SHA256" \
    "$RAW_JAVA_DOWNLOAD_DIR/libthrift-$RAW_JAVA_THRIFT_VERSION.pom"
raw_java_download "$RAW_JAVA_SLF4J_API_URL" \
    "$RAW_JAVA_SLF4J_API_SHA256" \
    "$RAW_JAVA_DOWNLOAD_DIR/slf4j-api-1.7.36.jar"
raw_java_download "$RAW_JAVA_SLF4J_NOP_URL" \
    "$RAW_JAVA_SLF4J_NOP_SHA256" \
    "$RAW_JAVA_DOWNLOAD_DIR/slf4j-nop-1.7.36.jar"
raw_java_install_compiler "${platform_pins[0]}" "${platform_pins[1]}"
raw_java_install_jdk "${platform_pins[2]}" "${platform_pins[3]}" \
    "${platform_pins[4]}" "${platform_pins[5]}" "${platform_pins[6]}" \
    "${platform_pins[7]}"
printf 'Thrift compiler: %s\n' "$RAW_JAVA_THRIFT_VERSION"
printf 'Thrift runtime: %s\n' "$RAW_JAVA_THRIFT_VERSION"
printf 'Java toolchain: %s %s\n' "$RAW_JAVA_JDK_VENDOR" "$RAW_JAVA_JDK_VERSION"
