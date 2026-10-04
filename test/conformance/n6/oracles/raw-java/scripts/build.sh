#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=common.sh
source "$root/common.sh"

/bin/bash "$root/generate.sh" --check
JAVAC=$(raw_java_javac)
mkdir -p "$RAW_JAVA_CLASSES_DIR"
find "$RAW_JAVA_CLASSES_DIR" -type f -name '*.class' -delete
sources="$RAW_JAVA_BUILD_DIR/sources.list"
find "$RAW_JAVA_GENERATED_DIR" "$RAW_JAVA_ROOT/src" \
    -type f -name '*.java' -print | LC_ALL=C sort > "$sources"
"$JAVAC" \
    --release "$RAW_JAVA_JAVA_RELEASE" \
    -encoding UTF-8 \
    -cp "$RAW_JAVA_DOWNLOAD_DIR/libthrift-$RAW_JAVA_THRIFT_VERSION.jar:$RAW_JAVA_DOWNLOAD_DIR/slf4j-api-1.7.36.jar" \
    -d "$RAW_JAVA_CLASSES_DIR" \
    @"$sources"
raw_java_classpath > "$RAW_JAVA_BUILD_DIR/classpath.txt"
printf 'Compiled %s Java class files for release %s.\n' \
    "$(find "$RAW_JAVA_CLASSES_DIR" -type f -name '*.class' | wc -l | tr -d ' ')" \
    "$RAW_JAVA_JAVA_RELEASE"
