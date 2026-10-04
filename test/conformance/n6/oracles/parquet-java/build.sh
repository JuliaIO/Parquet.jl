#!/bin/sh
set -eu

oracle_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
build_root="$oracle_dir/build"
artifact_dir="$build_root/artifacts"

reject_directory_path() {
    path=$1
    label=$2
    if [ -L "$path" ] || { [ -e "$path" ] && [ ! -d "$path" ]; }; then
        echo "$label must be a real directory: $path" >&2
        exit 1
    fi
}

reject_output_path() {
    path=$1
    if [ -L "$path" ] || { [ -e "$path" ] && [ ! -f "$path" ]; }; then
        echo "artifact output must be a regular non-link file: $path" >&2
        exit 1
    fi
}

reject_directory_path "$build_root" "build root"
reject_directory_path "$artifact_dir" "artifact directory"
for output in \
        "$artifact_dir/parquet-cli-1.17.1-runtime.jar" \
        "$artifact_dir/hadoop-client-api-3.3.0.jar" \
        "$artifact_dir/hadoop-client-runtime-3.3.0.jar" \
        "$artifact_dir/parquet-java-n6-harness.jar"; do
    reject_output_path "$output"
done
if [ -z "${PARQUET_N6_JAVA_JDK_ROOT:-}" ]; then
    echo "PARQUET_N6_JAVA_JDK_ROOT is required" >&2
    exit 2
fi
if [ -z "${PARQUET_N6_PARQUET_CLI_JAR:-}" ]; then
    echo "PARQUET_N6_PARQUET_CLI_JAR is required" >&2
    exit 2
fi
if [ -z "${PARQUET_N6_HADOOP_CLIENT_API_JAR:-}" ]; then
    echo "PARQUET_N6_HADOOP_CLIENT_API_JAR is required" >&2
    exit 2
fi
if [ -z "${PARQUET_N6_HADOOP_CLIENT_RUNTIME_JAR:-}" ]; then
    echo "PARQUET_N6_HADOOP_CLIENT_RUNTIME_JAR is required" >&2
    exit 2
fi
jdk_root=$(CDPATH= cd -- "$PARQUET_N6_JAVA_JDK_ROOT" && pwd -P)
java_source="$oracle_dir/src/org/julialang/parquet/n6/java/AuditMain.java"
parquet_jar=$PARQUET_N6_PARQUET_CLI_JAR
hadoop_api_jar=$PARQUET_N6_HADOOP_CLIENT_API_JAR
hadoop_runtime_jar=$PARQUET_N6_HADOOP_CLIENT_RUNTIME_JAR
expected_javac=7be7937fc6bae0ca89f0866f9ce94fc40a935dfb87806d3c701eca3402cfb90a
expected_jar=b4b69691321fb426e95a21bae51724e9f98ab6b67370eba953ee40c4f7512cbf
expected_parquet=d0173051493c506a298c691e555a41a682a405895fd0c8cc429a7e1cb1fcc711
expected_parquet_size=50072122
expected_hadoop_api=d549ba6d131fd6c8e5d42a78dab5c790950edd6258523dedc556b537ca6654aa
expected_hadoop_api_size=19207034
expected_hadoop_runtime=2ba23f1e1dbb03e73600a41fcb187ad2626529684ed226085a91b0a0d6d67ee5
expected_hadoop_runtime_size=27255121
expected_harness=7d6de1067e4e01de65f5868c8643f4a68fe4ff6afc50759716d685bd7b9e764c

sha256() {
    shasum -a 256 "$1" | awk '{print $1}'
}

verify() {
    expected=$1
    file=$2
    actual=$(sha256 "$file")
    if [ "$actual" != "$expected" ]; then
        echo "SHA-256 mismatch: $file" >&2
        exit 1
    fi
}

for input in "$parquet_jar" "$hadoop_api_jar" "$hadoop_runtime_jar"; do
    [ ! -L "$input" ] && [ -f "$input" ] || {
        echo "every Java artifact must be a regular non-link file" >&2
        exit 1
    }
done
[ "$(stat -f '%z' "$parquet_jar")" = "$expected_parquet_size" ] || exit 1
[ "$(stat -f '%z' "$hadoop_api_jar")" = "$expected_hadoop_api_size" ] || exit 1
[ "$(stat -f '%z' "$hadoop_runtime_jar")" = "$expected_hadoop_runtime_size" ] || exit 1
verify "$expected_parquet" "$parquet_jar"
verify "$expected_hadoop_api" "$hadoop_api_jar"
verify "$expected_hadoop_runtime" "$hadoop_runtime_jar"
verify "$expected_javac" "$jdk_root/bin/javac"
verify "$expected_jar" "$jdk_root/bin/jar"

mkdir -p "$build_root"
reject_directory_path "$build_root" "build root"
mkdir -p "$artifact_dir"
reject_directory_path "$artifact_dir" "artifact directory"
temporary=$(mktemp -d "$build_root/.build.XXXXXX")
trap 'rm -rf "$temporary"' EXIT HUP INT TERM
mkdir -p "$temporary/classes"
classpath="$parquet_jar:$hadoop_api_jar:$hadoop_runtime_jar"
"$jdk_root/bin/javac" --release 11 -encoding UTF-8 \
    -cp "$classpath" -d "$temporary/classes" "$java_source"
"$jdk_root/bin/jar" --create \
    --date=2020-01-01T00:00:00Z \
    --file "$temporary/parquet-java-n6-harness.jar" \
    -C "$temporary/classes" .
verify "$expected_harness" "$temporary/parquet-java-n6-harness.jar"
cp "$parquet_jar" "$temporary/parquet-cli-1.17.1-runtime.jar"
cp "$hadoop_api_jar" "$temporary/hadoop-client-api-3.3.0.jar"
cp "$hadoop_runtime_jar" "$temporary/hadoop-client-runtime-3.3.0.jar"
verify "$expected_parquet" "$temporary/parquet-cli-1.17.1-runtime.jar"
verify "$expected_hadoop_api" "$temporary/hadoop-client-api-3.3.0.jar"
verify "$expected_hadoop_runtime" "$temporary/hadoop-client-runtime-3.3.0.jar"
mv -f "$temporary/parquet-cli-1.17.1-runtime.jar" \
    "$artifact_dir/parquet-cli-1.17.1-runtime.jar"
mv -f "$temporary/hadoop-client-api-3.3.0.jar" \
    "$artifact_dir/hadoop-client-api-3.3.0.jar"
mv -f "$temporary/hadoop-client-runtime-3.3.0.jar" \
    "$artifact_dir/hadoop-client-runtime-3.3.0.jar"
mv -f "$temporary/parquet-java-n6-harness.jar" \
    "$artifact_dir/parquet-java-n6-harness.jar"
printf 'Built the pinned Parquet Java N6 harness.\n'
