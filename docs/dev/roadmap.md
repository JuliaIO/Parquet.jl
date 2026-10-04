# Parquet.jl 1.0 implementation roadmap

This roadmap is the primary implementation plan. The requested Claude Fable 5 Max
review did not run because the account reported insufficient credit. Independent
Codex reviews covered later implementation slices. This is a release plan, not a
statement of current support.

The review rounds and scope corrections are recorded in [`review-log.md`](review-log.md).

## Scope boundary

The stable target is Apache Parquet format 2.13.0. The source IDL is pinned by commit. The conformance corpus is also pinned. A later IDL can be inspected for forward compatibility, but post-tag features do not enter the stable 1.0 claim.

The implementation is pure Julia at the protocol layer. Audited JLL libraries can supply compression and cryptographic primitives. The package does not call C or C++ Parquet libraries. It does not need a native Thrift compiler.

LZO is a stable, nondeprecated codec in the 2.13.0 IDL, but it is out of scope. The registered LibLZO package and its binary are GPL-2 licensed, so they cannot be dependencies of this MIT core. Rather than block the release on a license-compatible implementation that nobody has asked for, this package reports an LZO file as an unsupported feature and does not claim complete codec coverage. No mainstream implementation offers full spec coverage either; the practical target is interoperability with the implementations people actually use.

## Target public API

The package has no exports. The intended public names are:

```julia
Parquet.File(source; limits, keyretriever, aadprefix)
Parquet.Table(source; scan, mmap, limits, keyretriever, aadprefix)
Parquet.Dataset(path; scan, limits, keyretriever, aadprefix)
Parquet.write(sink, table; codec, compressionlevel, dictionary, pagesize,
    rowgroupsize, pageversion, statistics, pageindex, bloom, encryption, metadata)
Parquet.close!(object)
```

Scalar schema and value helpers are also public through the package namespace:
`Parquet.LogicalColumn`, `Parquet.Timestamp`, `Parquet.Decimal`,
`Parquet.JSONValue`, `Parquet.BSONValue`, and `Parquet.Interval`. None of these names
is exported.

The implemented entry points are `File(input; limits)`, `Table(input; limits)`,
`write(sink, table; ...)`, and `close!`. The writer supports the documented codec,
encoding, dictionary, page, row-group, statistics, and offset-index options.
`Table` implements Tables.jl. `Dataset`, scan and encryption options, bloom
filters, compatibility shims, and cursor migration errors remain target work.
The current branch does not define `read_parquet` or `write_parquet`.

## Format coverage policy

### Physical types

Read and write BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, and FIXED_LEN_BYTE_ARRAY. INT96 is deprecated and is not supported in either direction; a file that uses it is reported as an unsupported feature rather than an invalid file.

### Encodings

Read PLAIN, PLAIN_DICTIONARY, RLE, BIT_PACKED, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, RLE_DICTIONARY, and BYTE_STREAM_SPLIT. Write every nondeprecated encoding. Dictionary output uses a PLAIN dictionary page, RLE_DICTIONARY indexes, and a deterministic fallback to PLAIN when the dictionary budget is exceeded.

### Compression

Read and write UNCOMPRESSED, SNAPPY, GZIP, BROTLI, ZSTD, and LZ4_RAW. Read deprecated Hadoop LZ4 and the raw-block fallback found in existing files. GZIP accepts concatenated members. LZO is not supported in either direction.

### Pages and checksums

Read and write Data Page V1, Data Page V2, and dictionary pages. Verify standard CRC32 when requested. Unknown and undefined page types are skipped without moving the cursor incorrectly. Page headers are authoritative. Footer encoding and page-offset lists are only hints.

### Logical types

Implement STRING, ENUM, UUID, JSON, BSON, DATE, TIME, TIMESTAMP, INTEGER, DECIMAL, FLOAT16, LIST, MAP, INTERVAL, UNKNOWN, VARIANT, GEOMETRY, and GEOGRAPHY. Read modern LogicalType fields first and legacy ConvertedType fields when needed. Write both representations where the compatibility rules require both.

INTERVAL has no LogicalType member. It uses ConvertedType only and has no min/max statistics. GEOMETRY and GEOGRAPHY also omit min/max. VARIANT, LIST, and MAP do not claim a defined sort order.

### Nested data

Implement the Dremel definition and repetition model for arbitrary nested lists, maps, and structs. Apply all standard list compatibility forms, legacy MAP_KEY_VALUE handling, and the known optional-map-key compatibility rule. Page splitting for nested columns starts each V2 page, and every page covered by an offset index, at repetition level zero.

### Statistics and indexes

Statistics are correctness data. Missing fields remain unknown. The reader applies signed, unsigned, and type-defined column order. It applies the Parquet float rules for NaN and signed zero. It does not trust legacy binary statistics from affected parquet-mr versions. It respects truncated-bound exactness fields and page-index boundary order.

The writer emits `column_orders` when it emits modern min/max. It emits `nan_count` for FLOAT, DOUBLE, and FLOAT16. It does not produce statistics for types without a defined order.

### Bloom filters

Implement split-block bloom filters with XXH64 seed zero and uncompressed bitsets. Hash the PLAIN value bytes without a byte-array length prefix. Legacy Murmur3 sidecar fixtures are not part of the format claim.

### Encryption

Implement Parquet modular encryption with AES-GCM and AES-CTR, 128/192/256-bit keys, correct module AAD, nonce construction, invocation limits, encrypted footer magic, plaintext-footer signatures, encrypted column metadata, and caller-supplied AAD prefixes. Key retrieval and envelope KMS behavior are interfaces or extensions. A specific parquet-mr KMS envelope is not part of the core format claim.

### Variant and geospatial

Variant is a separate bounded parser and writer. Version 1.0 reads and writes unshredded and shredded Variant values. The parser limits recursion and memory and rejects every invalid corpus fixture.

Geospatial support includes a core WKB walker. It handles ISO and EWKB dimensional codes, SRID, empty points, and geometry bounding boxes without placing NaN in metadata. Geography bounding boxes implement longitude wraparound, including `xmin > xmax`, and require independent oracle coverage.

## Target internal design

This section describes the intended architecture. Scan and dataset APIs remain
planned; the current implementation is summarized in `architecture.md` and the
stage notes below.

### Sources and ownership

A source implements `sourcelength`, `readrange`, and `concurrentreads`. Paths use mmap where safe. General IO is copied once. Byte arrays are borrowed. Owned and borrowed regions have explicit idempotent close behavior. Every slice validates offset arithmetic before access.

### Metadata

A small Compact Protocol runtime handles only the protocol needed by Parquet. A pure-Julia generator consumes the pinned IDL. Generated output is immutable, typed, deterministic, and checked in. CI regenerates it and requires no diff.

Every generated struct stores raw unknown field records. Encoding re-emits those records. Field 32767, unknown enum values, unknown union members, and post-tag page kinds survive a decode and encode pass.

### Vectors and scalar values

Primitive values decode into typed final buffers. Nested values use package-owned list, struct, and map vectors with a validity bitmap. Offsets use Int32 until data exceeds 2 GB, then Int64.

The current reader maps STRING to `DataStrings.DataString` and DATE to
`Dates.Date`. TIME remains nanosecond exact. UTC-adjusted millisecond timestamps
use `Parquet.Timestamp{:millis}`; local millisecond timestamps use
`Dates.DateTime`. Microsecond and nanosecond timestamps use unit-tagged
`Parquet.Timestamp` values. DECIMAL uses `DataDecimals.Decimal` for precision up
to 76 and `Parquet.Decimal` for larger precision. FLOAT16 maps to `Float16` and
UUID to `UUIDs.UUID`. JSON and BSON remain tagged bytes in core; parsed
integrations are planned extensions.

### Parallel reading

The reader uses a bounded worker pool. Work is assigned through an `@atomic` field. Every spawned task is wrapped in `errormonitor`. Workers decode directly into pre-sized final buffers. Result ordering and error choice are deterministic.

### Scan semantics

The decode set is the union of selected columns and filter-referenced columns. Row groups and pages are pruned only when metadata proves that no row can match. The exact filter remains residual work. Offset and limit are consumed only when no residual filter can change row positions. `select=()` means zero result columns. An identity scan selects all columns.

### Dataset safety

Datasets support Hive partitioning, schema unification, `_common_metadata`, and `_metadata`. Footer-only metadata can prune files. A metadata `file_path` never escapes the dataset root and is never followed as an arbitrary path.

## Dependencies

The intended core uses Tables, DataAPI, Dates, Mmap, UUIDs, CRC32, GeoFormatTypes,
ChunkCodecCore, and the JuliaIO chunk-codec bindings for Snappy, zlib, Zstandard,
LZ4, and Brotli. Their chunk API matches Parquet pages and permits an exact output
size to be charged before decompression. No LZO dependency is accepted, because every
available implementation is GPL-2. Parquet-specific wrappers handle Hadoop LZ4 framing
and exact-size validation. XXH64, compact varints, and the WKB walker stay in tree.

Extensions provide OpenSSL EVP encryption, cloud byte sources, JSON, BSON, GeoInterface, Arrow, alternate decimal values, and alternate nanosecond date values. The core does not depend on Thrift.jl, Arrow.jl, JSON3.jl, Decimals.jl, CategoricalArrays.jl, SentinelArrays.jl, or a native protocol compiler.

## Stages and gates

### Stage 0: repository and ledger

- Preserve the registered UUID, license, and Git history.
- Remove the old runtime on a rewrite branch.
- Pin the format IDL and corpus.
- Enumerate every stable enum, union, struct field, encoding, codec, page, and logical type in the ledger.
- Test Julia 1.10, stable Julia, and nightly on Linux, macOS, and Windows.
- Add a bounds-check lane, Aqua, docs, and deterministic generation.

Gate: the machine ledger covers the full pinned IDL. No high-level row can hide missing fields.

### Stage 1: Compact Protocol and metadata

- Decode and encode all metadata and page-header structs.
- Preserve unknown fields and enum values.
- Reject truncated, overlong, oversized, and deeply nested input under `Limits`.

Gate: decode-encode-decode identity for all corpus footers and headers, encrypted metadata included; seeded mutation tests fail safely; regeneration produces no diff.

### Stage 2: primitive vertical slice

- Complete bounded sources, footer framing, PLAIN primitives, hybrid RLE levels, flat schema interpretation, Data Page V1, Tables.jl facade, and a PLAIN V1 writer.

Gate: read the selected plain and malformed-offset corpus files; detect bad checksums when enabled; PyArrow and DuckDB both read Julia output.

### Stage 3: encodings and codecs

- Implement dictionary, all delta encodings, BYTE_STREAM_SPLIT, boolean RLE, deprecated BIT_PACKED input, every compression codec, Data Page V2, and CRC32.

Gate: exact corpus comparisons for every encoding and codec; PyArrow and DuckDB read each emitted form.

### Stage 4: logical and nested data

- Implement scalar logical mappings, legacy compatibility, Dremel reconstruction, nested vectors, and the nested writer.

Gate: all nested corpus forms and logical-type fixtures pass; generated schemas with null and empty values at every depth round-trip through PyArrow.

#### Scalar logical-type slice

The implemented scalar layer gives modern LogicalType annotations precedence over
legacy ConvertedType fields. It validates every supported annotation against its
physical type and preserves an unknown modern annotation as physical data. INTEGER
uses the matching signed or unsigned Julia integer type. DATE uses `Dates.Date`. TIME
uses `Dates.Time`. Millisecond TIMESTAMP uses `Dates.DateTime` for unadjusted local
values and `Parquet.Timestamp{:millis}` for UTC-adjusted values. Microsecond and
nanosecond values use `Parquet.Timestamp` to retain exact ticks and the UTC-adjustment
flag. STRING uses `DataStrings.DataString`. DECIMAL through 76 digits uses
`DataDecimals.Decimal` with fixed precision and scale; wider decimals use
`Parquet.Decimal`. UUID uses `UUIDs.UUID`, FLOAT16 uses `Float16`, and ENUM uses
`String`. JSON, BSON, and INTERVAL use tagged package values so their Parquet
identity is not lost.

High-level writes infer the unambiguous scalar schema. Plain `Dates.Time` writes
nanosecond local time, and plain `Dates.DateTime` writes millisecond local timestamp.
Tagged timestamp values carry exact millisecond, microsecond, or nanosecond ticks and one consistent
UTC-adjustment flag. Decimal precision and scale are checked exactly before a physical
INT32, INT64, or fixed-width byte representation is selected. A `Parquet.Table`
read-write cycle instead retains its leaf schema. This preserves time units, UTC flags,
ENUM identity, declared decimal precision, and unknown future annotations that Julia
runtime types cannot express by themselves.

`Parquet.LogicalColumn` supplies the missing explicit authoring path. It selects ENUM,
all TIME and TIMESTAMP units and UTC-adjustment values, or DECIMAL precision and scale.
It also carries the schema for empty and all-null parameterized columns without exposing
the internal Thrift metadata types.

The scalar slice passes the pinned logical-type corpus and exact bidirectional checks
with PyArrow 25.0.1. DuckDB 1.5.5 confirms the temporal, integer, decimal, and UUID
forms it supports. Recursive and legacy lists, structs, and maps are also implemented,
as described below. Stage 4 remains open under the feature ledger's complete evidence
contract. VARIANT and geospatial modules remain target work. A plain String column does not infer
ENUM; use `Parquet.LogicalColumn` when ENUM is intended. Core validates JSON against
[RFC 8259](https://www.rfc-editor.org/info/rfc8259/) and BSON against the
[BSON 1.1 document grammar](https://bsonspec.org/spec.html) without building object
trees. Parsed JSON and BSON object models stay in extensions. Embedded decimal byte
values have a separate `Limits.max_decimal_bytes` bound before BigInt conversion.

#### Recursive nested reader and writer

The recursive reader and writer distinguish null containers, empty containers,
null elements, and present elements in lists, structs, and maps. Declared Julia
element types provide schemas for empty and all-null containers; the reader also
accepts the legacy list and map layouts exercised by the pinned corpus.

For example, `optional LIST<optional DATE>` must
distinguish a null list, an empty list, a null element, and a present element. The
canonical leaf path is `list.element`, with maximum repetition level 1 and maximum
definition level 3. For the rows `missing`, `Date[]`, `[missing]`,
`[Date(1970, 1, 1), missing, Date(1969, 12, 31)]`, and `[Date(2000, 2, 29)]`, the
expected leaf stream is:

```text
repetition = [0, 0, 0, 0, 1, 1, 0]
definition = [0, 1, 2, 3, 2, 3, 3]
physical   = [0, -1, 11016]
```

Physical page decoding must first produce a leaf stream of repetition levels,
definition levels, and dense present values. Flat columns remain an adapter over this
stream. Nested assembly happens after page decoding. This boundary prevents page
framing, dictionary decoding, and physical encodings from depending on a specific
nested container representation.

The writer emits the canonical three-level LIST schema. V1 pages contain prefixed
repetition levels, prefixed definition levels, and values. V2 pages contain raw level
streams and compress only values. V2 pages must start at repetition level zero,
`num_rows` must count zero repetition levels, and `num_nulls` must count definitions
below the leaf maximum. `ColumnMetaData.num_values` is the number of leaf-stream
entries, not the table row count.

The acceptance gate uses `list_columns.parquet` plus generated V1 and V2 DATE
list files. PyArrow and DuckDB must read Julia output exactly. Julia must read their
null, empty, null-element, and repeated-element cases exactly. Recursive and legacy
forms have separate fixtures and tests; this single-list example does not establish
the complete nested-data gate.

The implementation has a physical `LeafStream` boundary, recursive nested plans,
and canonical LIST and MAP output. The pinned Apache
`list_columns.parquet` fixture passes for optional Int64 and STRING elements. Generated
PyArrow and DuckDB V1/V2 files pass in Julia, and both engines read Julia PLAIN,
DELTA_BINARY_PACKED, dictionary, Snappy, and Zstd output with exact null and empty-list
semantics. The broader Stage 4 gate remains open.

### Stage 5: pruning data and scans

- Implement statistics trust, column order, offset and column indexes, bloom filters, and residual-safe scan pushdown.

Gate: full-scan and pushed-scan results are identical across seeded filters; a counting source proves pruned chunks are not read; DuckDB consumes Julia indexes and bloom filters.

The current slice validates row-group statistics and column-order semantics,
emits bounded footer statistics and offset indexes by default, and checks offset
indexes against column-chunk ranges and page frames. Column-index production,
bloom filters, and residual-safe scan pushdown remain target work. A successful
full-table read does not establish the Stage 5 pruning gate.

### Stage 6: encryption

- Implement the cipher extension and every encrypted module.

Gate: all encrypted corpus files pass, including AES-256, CTR, plaintext footer, and bloom filters; tamper and module-swap tests fail authentication; PyArrow reads Julia encrypted files.

### Stage 7: Variant and geospatial

- Implement bounded Variant and WKB modules.

Gate: all valid Variant cases match reference data, all invalid cases fail, independent readers accept Julia unshredded and shredded output, geospatial statistics match the corpus reference YAML, and independent readers accept Julia GEOMETRY and GEOGRAPHY metadata.

### Stage 8: datasets, performance, and release

- Finish dataset semantics, compatibility shims, docs, fuzzing, and performance work.

Gate: hot decode loops allocate no memory after output buffers; representative single-thread decode is near the agreed PyArrow baseline; full corpus and oracle CI are green; licenses are complete; `Tables.Scan` is registered; reverse-dependency and PkgEval results are reviewed.

## Main risks

The highest risks are Dremel correctness for empty and null nested values, incorrect statistics pruning that silently changes results, unreleased `Tables.Scan` behavior, Variant size and robustness, encryption nonce or AAD mistakes, unsafe codec allocation, very wide footer performance, and keeping external oracles available in CI.

No stage becomes complete from a package-local round trip alone.
