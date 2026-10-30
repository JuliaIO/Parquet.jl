#!/bin/sh
set -eu

fail() {
    echo "validate-n5-oracle-image: $1" >&2
    exit 65
}

work=$(mktemp -d)
cleanup() {
    rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

test "$(uname -s)" = "Linux" || fail "operating system is not Linux"
test "$(uname -m)" = "x86_64" || fail "architecture is not x86_64"
java_output=$(java -version 2>&1) || fail "Java version query failed"
case "$java_output" in
    *'openjdk version "11.0.28"'*'Temurin-11.0.28+6 (build 11.0.28+6)'*) ;;
    *) fail "Java is not exact Temurin 11.0.28+6" ;;
esac
maven_output=$(mvn --version 2>&1) || fail "Maven version query failed"
case "$maven_output" in
    'Apache Maven 3.9.8 '* | 'Apache Maven 3.9.8') ;;
    *) fail "Maven is not version 3.9.8" ;;
esac
test "$(rustc --version)" = "rustc 1.96.1 (31fca3adb 2026-06-26)" ||
    fail "rustc is not the pinned build"
cargo_output=$(cargo --version 2>&1) || fail "Cargo version query failed"
test "$cargo_output" = "cargo 1.96.1 (356927216 2026-06-26)" ||
    fail "Cargo is not version 1.96.1"
test "$(dpkg-query -W -f='${Version}' perl)" = "5.38.2-3.2ubuntu0.3" ||
    fail "Perl package differs from the base-image pin"
test "$(perl -e 'print $^V')" = "v5.38.2" ||
    fail "Perl is not version 5.38.2"
test "$(perl -MJSON::PP -e 'print $JSON::PP::VERSION')" = "4.16" ||
    fail "JSON::PP is not version 4.16"
test "$(dpkg-query -W -f='${Version}' build-essential)" = "12.10ubuntu1" ||
    fail "build-essential differs from the snapshot pin"
test "$(dpkg-query -W -f='${Version}' ca-certificates)" = \
    "20260601~24.04.1" || fail "ca-certificates differs from the snapshot pin"
test "$(dpkg-query -W -f='${Version}' cmake)" = "3.28.3-1build7" ||
    fail "cmake differs from the snapshot pin"
test "$(dpkg-query -W -f='${Version}' git)" = "1:2.43.0-1ubuntu7.3" ||
    fail "git differs from the snapshot pin"
test "$(dpkg-query -W -f='${Version}' pkg-config)" = "1.8.1-2build1" ||
    fail "pkg-config differs from the snapshot pin"
test "$(dpkg-query -W -f='${Version}' xz-utils)" = \
    "5.6.1+really5.4.5-1ubuntu0.3" ||
    fail "xz-utils differs from the snapshot pin"

printf '%s\n' \
    'base_image=eclipse-temurin:11.0.28_6-jdk@sha256:ab2527b3c9b7c15bc88f60dec19b2aa39939a6e0045fb8f538eeecbd7af59c69' \
    'source_date_epoch=1787356800' \
    'ubuntu_snapshot=20260822T000000Z' \
    'ubuntu_build_essential=12.10ubuntu1' \
    'ubuntu_ca_certificates=20260601~24.04.1' \
    'ubuntu_cmake=3.28.3-1build7' \
    'ubuntu_git=1:2.43.0-1ubuntu7.3' \
    'ubuntu_pkg_config=1.8.1-2build1' \
    'ubuntu_xz_utils=5.6.1+really5.4.5-1ubuntu0.3' \
    'maven_version=3.9.8' \
    'maven_archive_sha512=7d171def9b85846bf757a2cec94b7529371068a0670df14682447224e57983528e97a6d1b850327e4ca02b139abaab7fcb93c4315119e6f0ffb3f0cbc0d0b9a2' \
    'rust_version=1.96.1' \
    'rust_channel_manifest_sha256=87eb76c53073e72b766083bed5530820694253b832a762d8385bda5759f03975' \
    'rust_tarball_sha256=d29ccb1559a177c4e72291f6e5f629de7fe8885e7521ca47802627544b121e95' \
    'perl_package_version=5.38.2-3.2ubuntu0.3' \
    'perl_version=v5.38.2' \
    'json_pp_version=4.16' \
    'parquet_java_version=1.17.1' \
    'parquet_java_commit=78a8d3230eb4769db93de5f2f2e18363c04cae81' \
    'arrow_rs_version=59.2.0' \
    'arrow_rs_commit=782e5a685501a9db6cc8e9a3b7cbff894940c47a' \
    > "$work/toolchains.txt"
cmp -s "$work/toolchains.txt" /opt/n5/manifests/toolchains.txt ||
    fail "toolchain manifest differs from the exact contract"
channel_output=$(sha256sum /opt/n5/pins/channel-rust-1.96.1.toml) ||
    fail "Rust channel pin is absent"
channel_hash=${channel_output%% *}
test "$channel_hash" = \
    "87eb76c53073e72b766083bed5530820694253b832a762d8385bda5759f03975" ||
    fail "Rust channel pin hash differs"

printf '%s\n' \
    cargo-dependency-tree.txt \
    cargo-vendor.sha256 \
    harness-source.sha256 \
    maven-artifacts.sha256 \
    maven-dependency-tree.txt \
    toolchains.txt > "$work/expected-manifests"
find /opt/n5/manifests -mindepth 1 -printf '%P\n' > "$work/actual-manifests" ||
    fail "cannot enumerate image manifests"
LC_ALL=C sort "$work/actual-manifests" -o "$work/actual-manifests"
cmp -s "$work/expected-manifests" "$work/actual-manifests" ||
    fail "image manifest directory is not closed"

special=$(find /opt/n5/maven/repository ! -type d ! -type f -print -quit) ||
    fail "cannot inspect the Maven repository"
test -z "$special" || fail "Maven repository contains a non-regular entry"
(cd /opt/n5/maven/repository &&
    find . -type f -print0 > "$work/maven-files") ||
    fail "cannot enumerate the Maven repository"
LC_ALL=C sort -z "$work/maven-files" -o "$work/maven-files"
(cd /opt/n5/maven/repository &&
    xargs -0 sha256sum < "$work/maven-files" > "$work/maven.sha256") ||
    fail "cannot hash the Maven repository"
cmp -s "$work/maven.sha256" /opt/n5/manifests/maven-artifacts.sha256 ||
    fail "Maven repository content is not exact"

special=$(find /opt/n5/vendor /opt/n5/cargo /opt/n5/pins \
    ! -type d ! -type f -print -quit) || fail "cannot inspect Cargo content"
test -z "$special" || fail "Cargo content contains a non-regular entry"
(cd / && find opt/n5/vendor opt/n5/cargo opt/n5/pins -type f -print0 \
    > "$work/cargo-files") || fail "cannot enumerate Cargo content"
printf '%s\000' usr/local/bin/parquet-jl-n5-arrow-rs-oracle \
    >> "$work/cargo-files"
LC_ALL=C sort -z "$work/cargo-files" -o "$work/cargo-files"
(cd / && xargs -0 sha256sum < "$work/cargo-files" \
    > "$work/cargo.sha256") || fail "cannot hash Cargo content"
cmp -s "$work/cargo.sha256" /opt/n5/manifests/cargo-vendor.sha256 ||
    fail "Cargo content is not exact"

special=$(find /opt/bootstrap ! -type d ! -type f -print -quit) ||
    fail "cannot inspect harness source"
test -z "$special" || fail "harness source contains a non-regular entry"
(cd / && find opt/bootstrap -type f -print0 > "$work/source-files") ||
    fail "cannot enumerate harness source"
printf '%s\000' opt/n5/manifests/toolchains.txt \
    usr/local/bin/validate-n5-oracle-image >> "$work/source-files"
LC_ALL=C sort -z "$work/source-files" -o "$work/source-files"
(cd / && xargs -0 sha256sum < "$work/source-files" \
    > "$work/source.sha256") || fail "cannot hash harness source"
cmp -s "$work/source.sha256" /opt/n5/manifests/harness-source.sha256 ||
    fail "harness source is not exact"

(cd /opt/bootstrap/parquet-java &&
    mvn --batch-mode --no-transfer-progress --offline test package &&
    mvn --batch-mode --no-transfer-progress --offline dependency:tree \
        -DoutputFile="$work/maven-tree.txt" -DappendOutput=false) ||
    fail "offline Maven validation failed"
cmp -s "$work/maven-tree.txt" /opt/n5/manifests/maven-dependency-tree.txt ||
    fail "Maven dependency tree is not exact"
(cd /opt/bootstrap/arrow-rs &&
    cargo metadata --format-version=1 --offline --locked >/dev/null &&
    cargo tree --offline --locked > "$work/cargo-tree.txt") ||
    fail "offline Cargo validation failed"
cmp -s "$work/cargo-tree.txt" /opt/n5/manifests/cargo-dependency-tree.txt ||
    fail "Cargo dependency tree is not exact"
parquet-jl-n5-arrow-rs-oracle generate \
    --output "$work/golden" \
    --evidence "$work/evidence.json" || fail "Rust oracle self-check failed"

cleanup
trap - EXIT HUP INT TERM
if [ "$#" -gt 0 ]; then
    exec "$@"
fi
