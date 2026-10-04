#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")" && pwd)
maven_args=(-q)
if [[ ${1:-} == "--offline" ]]; then
    maven_args+=(--offline)
    shift
fi
if [[ $# -ne 0 ]]; then
    echo "usage: check.sh [--offline]" >&2
    exit 64
fi

(cd "$root" && mvn "${maven_args[@]}" test)
