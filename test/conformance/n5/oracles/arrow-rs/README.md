# N5 Arrow Rust oracle

This test-only executable owns the Arrow Rust N5 producer cases and provides a
general low-level reader for Julia, Java, and Rust Parquet fixtures. It is not a
Parquet.jl dependency.

## Pins

- Arrow Rust tag: `59.2.0`
- Arrow Rust commit: `782e5a685501a9db6cc8e9a3b7cbff894940c47a`
- Rust toolchain: `1.96.1`
- Rust compiler identity: `rustc 1.96.1 (31fca3adb 2026-06-26)`
- Cargo identity: `cargo 1.96.1 (356927216 2026-06-26)`
- Rust channel manifest SHA-256:
  `87eb76c53073e72b766083bed5530820694253b832a762d8385bda5759f03975`

`Cargo.toml` pins all Arrow crates to the exact Git commit. `Cargo.lock` pins the
complete crate graph. `rust-toolchain.toml` selects the exact compiler. The
common N5 OCI bootstrap vendors this lock graph and records the vendor hashes in
`oracles.lock`.

The binding CI gate runs only inside the locked Linux/amd64 OCI image. Local
execution on another platform is useful validation, but is not a substitute for
that gate.

## Offline build and test

The checked scripts always use `--offline --locked` and set
`CARGO_NET_OFFLINE=true`:

```sh
scripts/build.sh
scripts/test.sh
```

A networked maintainer bootstrap must fetch the exact lock graph before these
commands run. The repository-level OCI bootstrap owns that operation. The gate
must not change the lock file or access the network.

## Generate owned fixtures

```sh
scripts/run.sh generate \
  --output /work/golden/arrow-rs \
  --evidence /work/evidence/arrow-rs-generate.json
```

Generation writes six files:

- `arrow-rs-duplicate-keys_v1.parquet`
- `arrow-rs-duplicate-keys_v2.parquet`
- `arrow-rs-optional-key-present_v1.parquet`
- `arrow-rs-optional-key-present_v2.parquet`
- `arrow-rs-list-rule3_v1.parquet`
- `arrow-rs-list-rule3_v2.parquet`

The writer uses explicit repetition and definition levels. It disables
dictionary encoding, compression, and statistics. It fixes `created_by` and all
writer properties that affect these files. Generation immediately reads every
file through the independent low-level column and page APIs. It also uses the
Arrow `RecordBatch` reader. A mismatch exits with a nonzero status.

The duplicate-key `RecordBatch` check reads the underlying `MapArray` entries.
It does not project the map through a Rust or JSON map. Thus duplicate keys and
their order remain observable. JSON lines are diagnostic only; they can contain
duplicate object member names.

The owned LIST rule-3 fixture follows the Parquet 2.13 compatibility example.
Its repeated `array` group carries a LIST annotation and contains a repeated
`INT32` field. The low-level check requires the binding repetition, definition,
and dense-value streams. The `RecordBatch` check separately requires the exact
type `List<List<Int32>>` and rows `null`, `[]`, `[[]]`, and
`[[1,2],[],[3]]`.

## Inspect another fixture

```sh
scripts/run.sh inspect \
  --input /work/julia/direct-list-of-map.parquet \
  --case-id julia-direct-list-of-map-v1 \
  --evidence /work/evidence/julia-direct-list-of-map-v1.arrow-rs.json
```

The machine-readable evidence contains:

- the exact file SHA-256 and row-group count;
- the printed physical Parquet schema;
- every leaf path, physical type, and maximum level;
- per-row-group repetition, definition, and dense-value streams;
- every data-page version, encoding, value count, and derived row count;
- canonical Arrow schema evidence and ordered high-level rows when representable; and
- an explicit `unsupported` diagnostic when Arrow cannot represent a layout.

Unsupported high-level Arrow semantics do not erase low-level evidence. The
command fails if the low-level reader cannot parse the physical file. Canonical
rows encode structs as ordered fields and maps as ordered key/value pairs. The
separate Arrow JSON lines are diagnostic. For example, Arrow can represent maps
with non-string keys even though its JSON writer cannot serialize them.

## Unannotated LIST rule-3 near-neighbor

This diagnostic removes the inner LIST annotation from the repeated one-child
group. It is not the binding Rule 3 case. Under pinned Arrow Rust 59.2.0, the
`RecordBatch` reader reports `List<List<Int32>>` and rows `null`, `[]`, `[[]]`,
and `[[1,2],[],[3]]`. The checked evidence records this actual outcome without
using it as the specification authority.

```sh
scripts/run.sh diagnose-rule3-near-neighbor \
  --output /work/diagnostics/rule3-near-neighbor \
  --evidence /work/evidence/arrow-rs-rule3-near-neighbor.json
```

## Compare with checked evidence

The `verify` command accepts one checked `FileEvidence` JSON object or a complete
oracle report containing the matching case ID and file name. It compares all
file fields, including the hash, schema, streams, pages, and high-level outcome.

```sh
scripts/run.sh verify \
  --input /work/julia/standard-map-v1.parquet \
  --expected /work/expected/standard-map-v1.arrow-rs.json \
  --case-id julia-standard-map-v1 \
  --evidence /work/evidence/standard-map-v1.verify.json
```

`generate` and `inspect` wrap file objects in a top-level oracle report. The
repository-level manifest task can retain that report or store each selected
`files[]` object as the input to `verify`.

## Version evidence

```sh
scripts/run.sh versions --evidence /work/evidence/arrow-rs-versions.json
```

Every report records the Arrow version, upstream commit, selected Rust
toolchain, and exact `rustc --version` and `cargo --version` results. Every
command fails when either tool is absent or differs from the pins above. Missing
inputs and unsupported commands also fail. The harness has no absence skip.
