#!/bin/sh
set -eu

oracle_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
exec "$oracle_dir/run.sh" "$@" --check
