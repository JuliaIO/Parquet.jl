#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")" && pwd)
export LC_ALL=C
maven_args=(-q)
if [[ ${1:-} == "--offline" ]]; then
    maven_args+=(--offline)
    shift
fi
if [[ $# -eq 0 ]]; then
    echo "usage: run.sh [--offline] <generate|verify|inspect|audit|self-test> [options]" >&2
    exit 64
fi

(cd "$root" && mvn "${maven_args[@]}" -DskipTests package)
classpath="$root/target/classes"
dependency_count=0
for dependency in "$root"/target/dependency/*.jar; do
    [[ -f $dependency ]] || continue
    classpath="$classpath:$dependency"
    dependency_count=$((dependency_count + 1))
done
[[ $dependency_count -gt 0 ]] || {
    echo "run.sh: Maven runtime dependencies are absent" >&2
    exit 65
}
exec java \
    -Dfile.encoding=UTF-8 \
    -Duser.language=en \
    -Duser.country=US \
    -Duser.timezone=UTC \
    -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
    -Dparquet.avro.add-list-element-records=false \
    -cp "$classpath" \
    org.julialang.parquet.n5.OracleMain "$@"
