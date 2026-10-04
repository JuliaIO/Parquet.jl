# Parquet.jl

Parquet.jl is being rebuilt as a pure-Julia implementation of the Apache Parquet format.

The `rewrite/1.0` branch is development work. It is not ready for data use. The
current foundation contains bounded byte sources, footer framing, a pure-Julia
Compact Protocol runtime, generated 2.13.0 metadata types, schema-tree and level
validation, and bounded encoding kernels. The current vertical slice reads flat Data
Page V1 and V2 files with PLAIN, dictionary, delta, Boolean RLE, and BYTE_STREAM_SPLIT
values. It writes every stable nondeprecated flat value encoding through
`Parquet.Table` and `Parquet.write`, including a name-based per-column policy. It
supports UNCOMPRESSED, SNAPPY, GZIP, BROTLI, ZSTD, and LZ4_RAW pages, plus deprecated
Hadoop and raw-block LZ4 input. The Stage 4 scalar layer reads STRING, ENUM, UUID, JSON,
BSON, DATE, TIME, TIMESTAMP, INTEGER, DECIMAL, FLOAT16, INTERVAL, and UNKNOWN values. It
writes these annotations from unambiguous Julia values or tagged package values, and a
`Parquet.Table` rewrite preserves the source scalar schema and file key-value
metadata. The nested implementation reads and writes recursive structs, lists, and
maps, including null containers, empty containers, and null elements. The reader also
handles the legacy list and map layouts covered by the pinned Apache corpus. The
writer emits row-group statistics and offset indexes by default; the reader checks
page-index declarations and offset-index contents. Scan pushdown, datasets, bloom
filters, encryption, Variant, and geospatial modules remain target work. LZO and INT96
are not supported and are not planned: every LZO implementation is GPL-2, and INT96 is
deprecated in the format.
`NTuple{N,UInt8}` writer columns map to FIXED_LEN_BYTE_ARRAY, and `Parquet.Table`
preserves their runtime width across later writes.

```julia
Parquet.write("output.parquet", table; codec=:zstd, dictionary=true,
    encoding=(id=:delta_binary_packed, measurement=:byte_stream_split),
    pageversion=:v2, statistics=true)
```

An `encoding` Symbol or string applies to every column. A `Pair`, `NamedTuple`, or
dictionary supplies exact column-name overrides. Unlisted columns remain PLAIN, or
use adaptive dictionary encoding when `dictionary=true`. Use `:dictionary` for an
adaptive dictionary override on one column.

The writer emits row-group statistics by default. Set `statistics=false` to omit
them. `Parquet.Limits(max_statistics_value_bytes=4096)` limits each raw minimum or
maximum value before the writer copies it into file metadata. Counts and column
order remain available when a bound is too large to emit.

Use `Parquet.LogicalColumn` when the Julia element type does not contain the complete
Parquet schema. It can select ENUM, a TIME or TIMESTAMP unit and UTC flag, or DECIMAL
precision and scale. It also supplies a schema for empty and all-null columns.

```julia
using Dates

values = Union{Missing,Dates.Time}[missing, Dates.Time(12)]
time = Parquet.LogicalColumn(values, :time; unit=:micros, adjusted=false)
Parquet.write("time.parquet", (; time))
```

`Parquet.JSONValue` validates [RFC 8259](https://www.rfc-editor.org/info/rfc8259/)
syntax. `Parquet.BSONValue` validates the
[BSON 1.1 document grammar](https://bsonspec.org/spec.html). Both validators are
bounded and do not build an object tree. Binary DECIMAL conversion has its own
`Limits.max_decimal_bytes` resource bound.

## Target

- Apache Parquet format 2.13.0 is the stable contract.
- Post-2.13 features such as ALP remain experimental until they are released.
- The reader will accept all stable encodings and codecs, including deprecated input.
- The writer will emit all nondeprecated stable encodings and codecs.
- The core will not depend on Arrow.jl or a native Thrift compiler.
- The public API will remain small and namespaced under `Parquet`.

Support is complete only after valid read coverage, valid write coverage when applicable, malformed-input coverage, and confirmation by an independent Parquet implementation.

See [the development architecture](docs/dev/architecture.md) and [the machine-readable feature ledger](test/conformance/features.toml).

## License

Parquet.jl is available under the MIT license.
