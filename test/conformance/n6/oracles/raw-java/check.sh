#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=scripts/common.sh
source "$root/scripts/common.sh"

/bin/bash "$root/scripts/build.sh"
classpath=$(raw_java_classpath)
java_bin=$(raw_java_java)
case "$classpath" in
    *parquet-avro*|*parquet-format-2.12*|*parquet-hadoop*)
        echo "raw-java: parquet-java leaked into the scanner classpath" >&2
        exit 1
        ;;
esac
fixture="$RAW_JAVA_BUILD_DIR/self-test.parquet"
actual="$RAW_JAVA_BUILD_DIR/self-test.actual.jsonl"
second="$RAW_JAVA_BUILD_DIR/self-test.second.jsonl"
compiler="$RAW_JAVA_BUILD_DIR/compiler/thrift"
java_backup="$RAW_JAVA_BUILD_DIR/java.backup"
compiler_backup="$RAW_JAVA_BUILD_DIR/thrift.backup"
modules="$RAW_JAVA_JDK_DIR/lib/modules"
modules_digest=$(raw_java_sha256 "$modules")
cp "$RAW_JAVA_JDK_DIR/bin/java" "$java_backup"
cp "$compiler" "$compiler_backup"
restore_toolchain() {
    [[ ! -f $java_backup ]] || mv "$java_backup" "$RAW_JAVA_JDK_DIR/bin/java"
    [[ ! -f $compiler_backup ]] || mv "$compiler_backup" "$compiler"
    if [[ ! -f $modules ]] || [[ $(raw_java_sha256 "$modules") != "$modules_digest" ]]; then
        /bin/bash "$root/scripts/fetch-toolchain.sh" >/dev/null || true
    fi
}
trap restore_toolchain EXIT
printf '#!/usr/bin/env bash\necho "Thrift version %s"\n' \
    "$RAW_JAVA_THRIFT_VERSION" > "$compiler"
chmod 0755 "$compiler"
printf '#!/usr/bin/env bash\necho "fake cached java"\n' > "$RAW_JAVA_JDK_DIR/bin/java"
chmod 0755 "$RAW_JAVA_JDK_DIR/bin/java"
/bin/bash "$root/scripts/fetch-toolchain.sh" >/dev/null
raw_java_verify "$RAW_JAVA_COMPILER_DARWIN_ARM64_SEQUOIA_SHA256" "$compiler"
raw_java_verify "$RAW_JAVA_JDK_DARWIN_ARM64_SEQUOIA_JAVA_SHA256" \
    "$RAW_JAVA_JDK_DIR/bin/java"
rm "$java_backup" "$compiler_backup"
printf 'corrupt cached module image\n' >> "$modules"
/bin/bash "$root/scripts/fetch-toolchain.sh" >/dev/null
raw_java_verify "$modules_digest" "$modules"
raw_java_verify_tree "$RAW_JAVA_JDK_DARWIN_ARM64_SEQUOIA_TREE_SHA256" \
    "$RAW_JAVA_JDK_DIR"
trap - EXIT
fake_cache="$RAW_JAVA_BUILD_DIR/fake-cache.jar"
printf 'fake cached artifact\n' > "$fake_cache"
if raw_java_download "$RAW_JAVA_LIBTHRIFT_URL" "$RAW_JAVA_LIBTHRIFT_SHA256" \
        "$fake_cache" >/dev/null 2>&1; then
    echo "raw-java: a fake cached artifact passed hash verification" >&2
    exit 1
fi
rm "$fake_cache"

"$java_bin" -Dfile.encoding=UTF-8 -cp "$classpath" \
    org.julialang.parquet.n6.raw.SelfTestFixture "$fixture"
"$java_bin" -Dfile.encoding=UTF-8 -cp "$classpath" \
    org.julialang.parquet.n6.raw.RawFooterScanner \
    scan --input "$fixture" --output "$actual"
"$java_bin" -Dfile.encoding=UTF-8 -cp "$classpath" \
    org.julialang.parquet.n6.raw.RawFooterScanner \
    scan --input "$fixture" --output "$second"
cmp "$actual" "$second"
cmp "$RAW_JAVA_ROOT/expected/self-test.jsonl" "$actual"
"$java_bin" -Dfile.encoding=UTF-8 -cp "$classpath" \
    org.julialang.parquet.n6.raw.SelfTestMain "$fixture"
printf 'raw-java self-test passed.\n'
