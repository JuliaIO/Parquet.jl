# N6 raw and semantic evidence normalization

These tools transform the exact 20-record raw Java scanner output into the N6
normalized schema. They then run the frozen independent Julia model over every
normalized column. They do not import or call Parquet.jl.

The raw normalizer also supports the separate exact 12-record generated-fixture
scan. Use `--fixture-set generated` for evidence ID
`normalized-raw-java-generated`. This mode accepts only verified generated
fixture identities and the source-pinned raw input digest. It emits file and
column facts for all 12 cases and PASS results only for reviewed raw-wire claims.

`normalize_raw.py` accepts only the raw JSONL digest and record count frozen in
`manifest.toml`. It validates raw records, leaf topology, effective logical
annotations, ColumnOrder union states, statistics field presence, and corpus
identity before it writes an atomic canonical JSONL result.

`normalize_model.py` first regenerates the normalized raw bytes and requires an
exact match. It runs the hash-checked frozen model in an isolated Julia 1.10 or
1.12 process. Every column must produce a model result without a format error.
Only planned or verified case claims from `capabilities.toml` become PASS
records. Its run record binds the exact normalized raw input as upstream
evidence. `model-producer.toml` binds the model, suite, cases, bridge, and both
normalizer implementations as one producer revision. An unsupported claim is
never emitted as a pass.

Both commands support `--check`. Check mode regenerates the complete payload in
memory and fails without changing the destination when the checked file is
absent or stale. Normal mode uses a same-directory temporary file, `fsync`, and
an atomic rename. Inputs and output aliases must be regular non-symlink files.

Run the full self-test and freshness check without invoking the raw scanner:

```sh
test/conformance/n6/normalizers/check.sh /absolute/path/to/raw.jsonl
```

Set `N6_MODEL_JULIA_EXECUTABLE` to the absolute `Sys.BINDIR/julia` path from
either authorized Julia 1.10.11 or Julia 1.12.6 toolchain.

Check the generated-fixture evidence without running the scanner:

```sh
test/conformance/n6/normalizers/check_generated.sh \
    /absolute/path/to/raw-generated.jsonl
```
