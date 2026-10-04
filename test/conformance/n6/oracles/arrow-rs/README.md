# N6 Arrow Rust interoperability harness

This harness runs the exact Arrow Rust 59.2.0 oracle that is already present in
the local canonical N5 image. A small N6 metadata oracle adds the positive
`TYPE_ORDER` observation that the N5 binary does not expose. `build.sh` compiles
that source inside the same immutable image from its frozen offline Cargo lock
and vendored sources. The ignored `build/` cache is not an authority input. The
descriptor pins its exact binary bytes. No build or run uses the network.

Each container has `--pull never`, `--network none`, a read-only root, a
read-only corpus mount, no capabilities, no new privileges, and fixed CPU,
memory, process, output, and time limits. The host wrapper rejects symbolic
links in all input and output paths. It writes evidence with a same-directory
temporary file, `fsync`, and atomic replacement. The only repository output it
accepts is the declared Arrow Rust evidence path. Output inside the corpus is
forbidden.

The base logical output has 30 records. It covers 14 files, 13 passing
`read.logical-values` results under `n6-logical-values-v1`, and two explicit
`UNSUPPORTED` results for `wire.column-order.ieee` and
`wire.statistics.nan-count`. The full reviewed scope adds ten passing
`wire.column-order.type` results and five new file records. The total is then
45 records. The metadata oracle compares every exposed order, path, physical
type, and value count with the frozen raw facts before it emits the shared raw
observation envelope. The mixed IEEE fixture remains outside this positive
scope because Arrow Rust exposes its IEEE member as unknown.

Use the explicit `--draft` option while the evidence entry is planned. Draft
mode does not relax any input, source, binary, or descriptor digest check. The
descriptor must have the exact hash authorized in `capabilities.toml`. A normal
run requires both the authorized descriptor and a verified evidence entry.

Build the metadata oracle once before enabling the positive TYPE_ORDER claims:

```sh
test/conformance/n6/oracles/arrow-rs/build.sh
```

Set `PARQUET_N6_INTEROP_PYTHON_ROOT` to the reviewed CPython 3.12.8 base tree.
The launcher derives only `$PARQUET_N6_INTEROP_PYTHON_ROOT/bin/python3.12` and
uses `-I -B -S`. The wrapper then verifies the executable, full base runtime
tree, version, platform, and isolation flags against `toolchain.toml`.

Run and check a provisional output outside the repository:

```sh
mkdir -p /private/tmp/parquet-n6-arrow-rs
export PARQUET_N6_INTEROP_PYTHON_ROOT=/path/to/reviewed-cpython-3.12.8
test/conformance/n6/oracles/arrow-rs/run.sh \
  /private/tmp/parquet-testing-09f3cdb \
  test/conformance/n6/evidence/raw-java-apache-corpus.normalized.jsonl \
  /private/tmp/parquet-n6-arrow-rs/arrow-rs.normalized.jsonl \
  --draft
test/conformance/n6/oracles/arrow-rs/check.sh \
  /private/tmp/parquet-testing-09f3cdb \
  test/conformance/n6/evidence/raw-java-apache-corpus.normalized.jsonl \
  /private/tmp/parquet-n6-arrow-rs/arrow-rs.normalized.jsonl \
  --draft
```

Run unit tests and the two-pass container integration test:

```sh
"$PARQUET_N6_INTEROP_PYTHON_ROOT/bin/python3.12" -I -B -S \
  test/conformance/n6/oracles/arrow-rs/runtests.py \
  --integration \
  --repository . \
  --corpus-root /private/tmp/parquet-testing-09f3cdb \
  --raw-evidence test/conformance/n6/evidence/raw-java-apache-corpus.normalized.jsonl \
  --draft
```

The integration command writes the exact manifest target at
`test/conformance/n6/evidence/arrow-rs.normalized.jsonl`, then checks it without
changing its bytes. It checks all shared logical digests, both unsupported
records, exact file coverage, deterministic write/check behavior, and the
record boundary selected by the exact authority scope. Checked-in normalized
evidence remains provisional until the capability and toolchain records receive
their final review and freeze.
