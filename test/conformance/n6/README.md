# N6 statistics conformance gate

This directory owns the test-first evidence for trusted row-group statistics.
It is separate from the frozen N5 corpus and oracle evidence.

The authority is `docs/dev/n6-statistics-plan.md` at SHA-256
`15adf34af765a3300d8a49ced73532ed02b7e4edee5764453c58e53fcf12c304`.
Production N6 statistics files must not change until this preproduction gate
has an exact review with no open P0, P1, or P2 finding.

## Frozen roles

- `model/` computes order, count, and trust decisions without importing
  Parquet.jl or sharing production comparison code.
- `oracles/raw-java/` records Parquet 2.13 Compact-Thrift wire facts in a
  separate JVM. Its scanner-specific schema does not claim semantic support.
- `fixtures.toml` gives every selected Apache file a stable case ID, byte
  identity, expected topology, capability scope, and evidence status. It also
  freezes the exact generated Julia cases and output identities.
- `capabilities.toml` records each authority claim as `verified`, `planned`,
  `unsupported`, or `not_assessed`. It binds evidence producers to exact
  source revisions and allowed toolchain artifacts. Unsupported never means
  pass.
- `evidence.schema.json` is the normalized cross-tool JSONL boundary. It keeps
  the complete logical leaf description needed by the independent model. It
  also records explicit `PASS`, `FAIL`, and `UNSUPPORTED` case results.
- `validate_evidence.py` meta-validates the Draft 2020-12 schema and enforces
  cross-record facts that JSON Schema cannot express. A case result must be
  in that producer's reviewed capability scope.
- `manifest.toml` pins source revisions, policy files, runtimes, wheels,
  composite producer descriptors, and evidence identities without local
  absolute paths. It also pins the deterministic raw scanner digest for the
  exact 20-file Apache corpus. That digest is raw evidence only.
- `artifacts.sha256` pins every static preproduction N6 file except itself,
  `manifest.toml`, declared evidence outputs, declared generated Parquet files,
  and the raw build directory. The manifest pins both the artifact list and
  each frozen evidence file. The runner rejects every other unlisted file and
  every symbolic link before oracle orchestration.

The raw scanner output is normalized in a separate reviewed step. A normalizer
may not use Parquet.jl's production decoder or comparator. The raw toolchain,
raw corpus bytes, normalized capability claims, and external Java, Rust,
PyArrow, and DuckDB N6 harnesses are verified.
The raw evidence entry uses `storage = "gate-generated"`: its declared path is
a stable identity label, but the runner writes both comparison scans to fresh
temporary files and does not materialize that path in the repository.

## One-way evidence binding

A normalized run contains neither `manifest_sha256` nor
`artifact_manifest_sha256`. Either field could create a checksum cycle when
its containing file is pinned. A run binds these immutable inputs instead:

- the plan;
- the capability matrix;
- the fixture and corpus manifests;
- the normalized schema; and
- its exact producer toolchain.

The artifact manifest also excludes every declared evidence path. The
containing manifest separately binds the artifact list and each evidence path,
byte hash, authority, toolchain hash, case count, and record count. The
validator accepts a gate input only when its
canonical repository-relative path selects exactly one `[[frozen_evidence]]`
entry with status `verified` and format `normalized-jsonl`. A
`[[planned_evidence]]` entry can never satisfy `--gate`.

Every evidence exclusion must be a `.jsonl` file below the exact repository
prefix `test/conformance/n6/evidence/`. Every generated Parquet exclusion must
be a `.parquet` file below the N6-relative prefix `generated/`. The runner
checks both prefix sets and uniqueness before it derives any artifact
exclusion, so a manifest entry cannot hide an existing static N6 file.

All declared normalized evidence is frozen and verified. The exact gate still
has preproduction status. Publication and oracle locking remain unauthorized
until the final independent review and full exact gate complete.

## Normalized leaf meaning

`leaf_schema.logical_type` is the effective leaf annotation. It is not a raw
presence bit. A normalizer uses the raw `LogicalType` member when present. When
it is absent, the normalizer synthesizes the matching effective value from a
legacy `ConvertedType`; for example, `UTF8` becomes `STRING` and `UINT_16`
becomes `INTEGER`. `converted_type` separately preserves the raw legacy value,
or `null` when it was absent. Legacy integer names supply width and signedness.
Legacy decimal parameters come from `SchemaElement`. A legacy-only millisecond
or microsecond time annotation synthesizes `is_adjusted_to_utc = true`. When a
modern temporal annotation is present, its UTC flag wins. Both UTC and local
modern annotations carry the matching legacy enum for forward compatibility;
that enum cannot encode the UTC flag.

The pinned IDL requires compatible modern annotations to carry their exact
legacy counterpart when one exists. The semantic validator enforces that
pairing, its physical type, and every parameter that the legacy enum can
represent. This includes decimal precision and scale, integer width and
signedness, and temporal unit, but not the modern temporal UTC flag. It also
validates fixed widths and geospatial parameters. `TIME` and `TIMESTAMP` with
`NANOS` have no legacy counterpart. `UUID`, `FLOAT16`, `UNKNOWN`, `VARIANT`,
`GEOMETRY`, and `GEOGRAPHY` also have none. `INTERVAL` is synthesized from its
legacy annotation. `UNKNOWN` means the known Parquet null logical type. An
unrecognized future logical union member cannot be represented as `UNKNOWN`;
normalization must fail until the schema can preserve that member.

MAP, LIST, and VARIANT annotate groups and are rejected on normalized leaves.
A positive `type_length` is required for `FIXED_LEN_BYTE_ARRAY`. Any signed-i32
`type_length` on another physical type is preserved without assigning it N6
statistics meaning. The pinned IDL defines that raw field as an optional
maximum bit length but gives it no validity range.

## Result digest contracts

`n6-capability-result-sha256-v1` hashes a UTF-8 JSON observation envelope with
no final newline. Its object is exactly
`{"capability_id":string,"case_id":string,"observations":array}` and uses the
RFC 8785 JSON Canonicalization Scheme. Observations may contain integers but no
floating JSON numbers. The exact producer harness defines and freezes the
ordered contents of `observations` in the verified evidence.

`n6-no-pruning-trace-sha256-v1` is narrower. The reader harness parses metadata,
sets `footer_start = file_size - footer_length - 8`, clears its source trace,
and then materializes the table. Each later read is clipped to byte interval
`[4, footer_start)`; empty intersections are omitted. In call order, the trace
contains one UTF-8 line `offset=<decimal>;length=<decimal>\n` per clipped read.
`range_trace_sha256` hashes those exact lines. `body_sha256` hashes file bytes
`[0, footer_start)`. `logical_values_sha256` hashes the RFC 8785 logical value
array, again with no floating JSON numbers. The final result digest
hashes these exact UTF-8 lines, including the final line feed:

```text
body_sha256=<lowercase-hex-sha256>
logical_values_sha256=<lowercase-hex-sha256>
range_trace_sha256=<lowercase-hex-sha256>
read_count=<canonical-nonnegative-decimal>
```

The five no-pruning variants share one seed, comparison group, and digest
contract. They differ only by the declared statistics mutation. A verified
comparison group must have one equal passing digest across all five variants.
Every generated output has `output_identity_status = "verified"` and an exact
frozen byte hash and size.

## Bounded validation

The frozen manifest sets exact limits for input count, bytes per file, total
bytes, bytes per JSONL line, records per input, and total records. The record
limits are derived from all declared fixture topology and capability mappings.
The validator rejects duplicate JSON keys, non-standard numeric constants,
missing final line feeds, oversized inputs, duplicate producers, conflicting
file or leaf facts, column orders that change across row groups, and conflicting
passing digests.

PyArrow and DuckDB authority digests name composite descriptors under
`toolchains/`. Each descriptor binds the CPython source, exact standalone
distribution archive, clean extraction policy, executable, runtime tree, exact
wheel, platform, and harness sources. Only verified descriptors authorize
passing gate evidence.

The PyArrow and DuckDB source entries remain `planned`. The gate verifies their
official wheel bytes and runtime behavior, but it does not claim a
cryptographic tagged-source-tree-to-wheel build mapping. This provenance limit
does not weaken the exact wheel, harness, or observed interoperability results.

The gate requires a PASS for each declared case and capability that has at
least one positive reviewed claim. A pair with only `unsupported` or
`not_assessed` claims stays explicit but is not treated as a pass. Unsupported
never satisfies a positive claim.

## Local checks

Run the static gate and independent model from the repository root:

```sh
julia +1.10.11 --project=. --startup-file=no --history-file=no test/conformance/n6/runtests.jl
julia +1.12.6 --project=. --startup-file=no --history-file=no test/conformance/n6/runtests.jl
```

Default CI runs this static preflight with exact Julia 1.10.11 and 1.12.6 on
macOS 15 arm64. It sets `PARQUET_N6_GATE=0`. It does not run the expensive
external gate or any oracle.

The exact source gate uses environment-provided roots. Files never store
machine-local checkout paths.

```sh
PARQUET_N6_GATE=1 \
PARQUET_N6_FORMAT_ROOT=/path/to/parquet-format \
PARQUET_N6_TESTING_ROOT=/path/to/parquet-testing \
PARQUET_N6_JAVA_ROOT=/path/to/parquet-java \
PARQUET_N6_ARROW_RS_ROOT=/path/to/arrow-rs \
PARQUET_N6_ARROW_CPP_ROOT=/path/to/arrow-policy-source \
PARQUET_N6_PYARROW_ROOT=/path/to/arrow-25-source \
PARQUET_N6_DUCKDB_ROOT=/path/to/duckdb-1.5.5 \
PARQUET_N6_JULIA_SOURCE_DEPOT=/path/to/clean-exact-julia-source-depot \
PARQUET_N6_CPYTHON_312_ROOT=/path/to/cpython-3.12.8 \
PARQUET_N6_CPYTHON_314_ROOT=/path/to/cpython-3.14.2 \
PARQUET_N6_RAW_JAVA_DOWNLOAD_CACHE=/path/to/pinned-raw-java-downloads \
PARQUET_N6_VALIDATOR_PYTHON_ARCHIVE=/path/to/cpython-3.14.2+20260127-aarch64-apple-darwin-install_only_stripped.tar.gz \
PARQUET_N6_VALIDATOR_WHEEL_DIR=/path/to/pinned-validator-wheels \
PARQUET_N6_INTEROP_PYTHON_ARCHIVE=/path/to/cpython-3.12.8+20250115-aarch64-apple-darwin-install_only_stripped.tar.gz \
PARQUET_N6_INTEROP_WHEEL_DIR=/path/to/pinned-interop-wheels \
PARQUET_N6_JAVA_JDK_ROOT=/path/to/temurin-21.0.8+9-jdk \
PARQUET_N6_DOCKER=/absolute/path/to/docker \
julia +1.12.6 --project=. --startup-file=no --history-file=no test/conformance/n6/runtests.jl
```

`PARQUET_N6_JULIA_SOURCE_DEPOT` must point to a clean exact package-source
depot. When set, it is the exclusive package and artifact source. It must
contain each descriptor-selected
`packages/<name>/<depot-slug>` tree and each selected
`artifacts/<git-tree-sha1>` tree. Do not use a mutable working depot with
coverage files, preferences, compiled caches, or changed package sources. The
gate verifies each selected source tree before and after it copies the tree
into a fresh private depot. It locks that private depot read-only before Julia
loads Parquet.jl.

The gate also verifies the exact Julia 1.12.6 runtime. It copies the complete
runtime into a fresh private directory and locks the copy read-only. The
Parquet.jl producer bootstrap and the independent model both use the Julia
executable from this same private runtime. The gate checks the source and
private runtime identities again after execution.

The gate reads the exact bounded CPython 3.14.2 distribution archive once and
checks its pinned hash before extraction. It removes `site-packages` and all
bytecode caches, checks the clean tree, and locks the tree and exact wheel
snapshots read-only.
It starts the base interpreter with `-I -B -S` before it creates the fresh
validator environment. This prevents excluded system `site-packages`, `.pth`
files, and `sitecustomize` from running. It then starts the fresh environment
with `-I`. It does not trust an existing virtual environment or installed
package metadata.

The interoperability gate verifies the exact CPython 3.12.8 standalone archive
before extraction. It removes the complete bundled `site-packages` tree and all
bytecode caches. It then verifies the portable clean-tree and executable hashes
before it runs any oracle with `-I -B -S`.

The exact source gate always runs the raw scanner self-test and two scans of the
20-file corpus. It also regenerates and checks the normalized raw and
independent-model evidence. The raw scanner gate requires the exact eight-file
download cache named by `PARQUET_N6_RAW_JAVA_DOWNLOAD_CACHE`. It reads each
bounded file once, verifies the hash from `oracles/raw-java/toolchain.env`, and
copies the bytes into a fresh private build. The mandatory gate does not use the
network and does not write to the input cache.

No file here authorizes an oracle image publication, a repository
`oracles.lock`, a frozen N5 evidence change, pruning, or a release claim.

The Julia 1.12.6 runtime pin comes from the pristine official macOS arm64 archive
`julia-1.12.6-macaarch64.tar.gz` (SHA-256
`277d82fbd2eda99d0963b3e41f3dc979d7486f181399f8430fb637318ccd6a31`).
It contains 7,124 runtime entries totaling 818,749,783 regular-file bytes.
Local coverage output must not be included when establishing a runtime pin.
The gate still rejects any added or changed runtime file.

For the shared-value migration, Julia producer and independent-model evidence was
regenerated. The external oracle observations retain their original producer
identities and unchanged observation bytes. Their control and upstream digests
were updated after checking that generated fixture bytes and normalized raw
observations were unchanged. This metadata update does not assert a new execution
of the external oracle processes; the full external gate remains a separate check.
