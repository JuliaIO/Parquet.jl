#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=scripts/common.sh
source "$root/scripts/common.sh"

[[ $# -gt 0 ]] || {
    echo "usage: run.sh scan --input <file-or-directory> [--input ...] [--output file]" >&2
    exit 64
}
/bin/bash "$root/scripts/build.sh" >/dev/null
exec "$(raw_java_java)" \
    -Dfile.encoding=UTF-8 \
    -Duser.language=en \
    -Duser.country=US \
    -Duser.timezone=UTC \
    -cp "$(raw_java_classpath)" \
    org.julialang.parquet.n6.raw.RawFooterScanner "$@"
