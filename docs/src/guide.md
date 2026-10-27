# Guide

## Read a table

`Parquet.Table(path)` reads a Parquet file into columns. A table is a
`Tables.AbstractColumns`: `table.price`, `table[:price]`, and
`Tables.getcolumn(table, :price)` return a column, and any Tables.jl sink accepts it.
The table owns its columns: the file is closed before `Parquet.Table` returns, so it
can be replaced or deleted while the table is in use.

```julia
using Parquet, DataFrames

table = Parquet.Table("input.parquet")
table.price                                   # a column
df = Parquet.read("input.parquet", DataFrame) # columns handed over without a copy
```

Column chunks decode on `Threads.nthreads()` tasks; pass `ntasks=1` to read on the
calling task. `DataAPI.metadata(table)` gives the file's key-value metadata and
`Parquet.metadata(table)` the raw footer metadata.

### Column types

| Parquet type | Julia element type |
|:--|:--|
| BOOLEAN, INT32, INT64, FLOAT, DOUBLE, INT(8-64), FLOAT16 | `Bool`, `Int32`, `Int64`, `Float32`, `Float64`, `Int8`-`UInt64`, `Float16` |
| STRING, ENUM | `DataStrings.DataString` (the column is a `DataStrings.StringVector`) |
| BYTE_ARRAY | `DataStrings.DataBytes` (the column is a `DataStrings.BytesVector`) |
| FIXED_LEN_BYTE_ARRAY(N) | `NTuple{N,UInt8}`; `DataStrings.DataBytes` when N > 1024 |
| UUID | `UUIDs.UUID` |
| DECIMAL(p, s) | `DataDecimals.Decimal{p,s}`; `Parquet.Decimal` when p > 76 |
| DATE, TIME | `Dates.Date`, `Dates.Time` |
| TIMESTAMP(unit) | `Durations.Timestamp{unit}` (`Millisecond`, `Microsecond`, `Nanosecond`) |
| TIMESTAMP(unit), adjusted to UTC | `Durations.ZonedTimestamp{unit,:UTC}` |
| INT96 | `Durations.Timestamp{Nanosecond}`, or the resolution given by `int96` |
| INTERVAL | `Durations.Duration` |
| JSON, BSON | `Parquet.JSONValue`, `Parquet.BSONValue` |

An optional column's elements are `Union{Missing,T}`. A local `Durations.Timestamp`
compares with `Dates.DateTime`, so a filter such as
`Parquet.colcmp(>=, Parquet.col(:pickup), DateTime(2025, 1, 1))` works on it; compare a
UTC timestamp with a `Durations.ZonedTimestamp`.

### Read some columns and rows

`select` and `drop` choose columns, `filter` keeps rows where a predicate is `true`,
and `limit` caps the row count. Only the columns a read selects or filters on are
decoded; the column chunks of every other column are never read, so columns this
package cannot decode can be skipped too. The filter and limit apply after reading.

```julia
table = Parquet.Table("input.parquet"; select=[:id, :price],
    filter=(Parquet.col(:qty) > 0) & !Parquet.isnull(Parquet.col(:price)), limit=1000)
```

`Parquet.col`, `Parquet.colcmp`, `Parquet.colin`, and `Parquet.isnull` are the
`Tables.col` family. For renames or an offset, pass a `Tables.Scan` as `scan` instead
of the keywords above:

```julia
table = Parquet.Table("input.parquet";
    scan=Tables.Scan(select=(:id, :price => :amount), offset=100, limit=10))
```

### Read a file one row group at a time

`Parquet.File(path; keywords...)` opens a file without reading column data. It takes
the same keywords as `Parquet.Table`. `Tables.schema(file)` gives the columns a read
returns; iterating the file yields one `Parquet.Table` per row group, so a file is its
own `Tables.partitions`, and `file[i]` reads row group `i`. Close the file when done.

```julia
file = Parquet.File("input.parquet"; select=[:id, :price])
total = sum(sum(part.price) for part in file)
close(file)
```

## Write a table

`Parquet.write` accepts a Tables.jl source and a path or `IO` sink, and returns the
sink. `Parquet.write(sink)` returns a function, so a table can be piped into it:
`CSV.File("in.csv") |> Parquet.write("out.parquet")`. Each partition of a
`Tables.partitions` source becomes one or more row groups; every partition must have
the first partition's column types and nullability.

```julia
Parquet.write("output.parquet", table;
    codec=:zstd,
    dictionary=true,
    encoding=(id=:delta_binary_packed, measurement=:byte_stream_split),
    pageversion=:v2,
)
```

An encoding symbol or string applies to all columns. A `Pair`, `NamedTuple`, or
dictionary supplies exact column-name overrides. Unlisted columns use PLAIN, or use
adaptive dictionary encoding when `dictionary=true`.

Each column's element type decides its Parquet type:

| Julia element type | Parquet column |
|:--|:--|
| `Bool`, `Int8`-`Int64`, `UInt8`-`UInt64`, `Float32`, `Float64`, `Float16` | BOOLEAN, INT32/INT64 with an INT annotation for the narrower and unsigned types, FLOAT, DOUBLE, FLOAT16 |
| `AbstractString`, `Symbol`, `Char` | STRING |
| `Vector{UInt8}`, `DataStrings.DataBytes` | BYTE_ARRAY |
| `NTuple{N,UInt8}`, `UUID` | FIXED_LEN_BYTE_ARRAY(N), UUID |
| `Date`, `Time`, `DateTime` | DATE, TIME(NANOS), TIMESTAMP(MILLIS) |
| `Durations.Timestamp{P}` | TIMESTAMP in unit `P`, not adjusted to UTC; `Second` values are stored as milliseconds |
| `Durations.ZonedTimestamp{P,Z}` | TIMESTAMP in unit `P`, adjusted to UTC |
| `Nanosecond`, `Microsecond`, `Millisecond`, `Second` | INT64 count of that unit |
| `Minute`, `Hour`, `Day`, `Week` | INT64 count of seconds |
| `Month`, `Year`, `Durations.Duration` | INTERVAL |
| `DataDecimals.Decimal{P,S}` | DECIMAL(P,S) on INT32 (P <= 9), INT64 (P <= 18), or FIXED_LEN_BYTE_ARRAY |
| `CategoricalValue` and other wrapped values | the type of the wrapped values |
| `Vector`, `NamedTuple`, `AbstractDict` | LIST, group, MAP |
| `Missing` only | a null column |

A column whose element type is abstract or a `Union` of several types is written as
the one type its values share. Numbers of different types are promoted when every
value converts exactly, and strings of different types are written as STRING. Other
mixes, `nothing` values, and `Int128` or `BigInt` columns are errors that name the
column.

The writer reads each value of a scalar column once: a column that is not a `Vector`
is first collected into one, so lazy or computed vectors are evaluated a single time.
Changes made to a source while it is being written are not detected.

## Nested columns

The development implementation reads and writes recursive structs, lists, and maps.
For ordinary Julia input, use `NamedTuple` values for structs, vectors for lists, and
dictionaries for maps. Declared element types supply the schema for empty and all-null
columns. Null containers, empty containers, and null elements remain distinct.

```@example nested
using Parquet, Tables

S = NamedTuple{(:name, :score),Tuple{String,Union{Missing,Int32}}}
profile = Union{Missing,S}[missing, S(("Ada", Int32(7))), S(("Ben", missing))]
samples = Union{Missing,Vector{Union{Missing,Int32}}}[
    missing, Union{Missing,Int32}[], Union{Missing,Int32}[1, missing, 2],
]
attrs = [Dict("a" => Int32(1)), Dict{String,Int32}(), Dict("b" => Int32(2))]

io = IOBuffer()
Parquet.write(io, (; profile, samples, attrs); codec=:zstd, pageversion=:v2)
table = Parquet.Table(take!(io))
columns = Tables.columntable(table)
(
    name=String(columns.profile[2]["name"]),
    null_list=ismissing(columns.samples[1]),
    empty_list=isempty(columns.samples[2]),
    values=collect(columns.samples[3]),
    map=Dict(columns.attrs[3]),
)
```

Nested read values expose indexing and iteration; their concrete container types may
differ from the input. The reader also accepts the legacy list and map layouts tested
in the pinned Apache corpus. The rewrite remains preproduction.

## What this package does not support

Complete coverage of the format is not a goal, so a few parts are deliberately left
out. Reading one of them reports an unsupported feature rather than an invalid file.

- Encrypted files (Parquet modular encryption). Reading one throws
  `Parquet.UnsupportedFeatureError`.
- LZO compression, in either direction. Every available implementation is GPL-2 and
  this package is MIT.
- Writing INT96. The type is deprecated in the format. INT96 columns, which Spark,
  Hive, and Impala still write by default, are read as `Durations.Timestamp{Nanosecond}`
  values that are not adjusted to UTC, as pyarrow reads them. A value outside that
  range (years 1677 to 2262), such as Spark's `9999-12-31` placeholder, throws
  `Parquet.UnsupportedFeatureError`; pass `int96=Dates.Microsecond` (Spark's own
  precision, which reads every value Spark writes) to read it. Rewriting a
  `Parquet.Table` that has an INT96 column reports an unsupported feature; write its
  columns instead to store them as INT64 timestamps.
- Writing the deprecated LZ4 codec or the deprecated BIT_PACKED encoding. Both are
  still read, because files in the wild use them. New files use `:lz4_raw` and RLE.

## Statistics

The writer emits row-group statistics and a complete column-order declaration by
default. Set `statistics=false` to omit both.

```julia
Parquet.write("without-statistics.parquet", table; statistics=false)
```

`Parquet.Limits.max_statistics_value_bytes` limits each raw minimum or maximum
before it is copied into footer metadata. The default is 4096 bytes. If one bound
is larger, the writer omits the minimum and maximum for that column chunk. It still
emits valid null and value counts and preserves column order.

```julia
limits = Parquet.Limits(max_statistics_value_bytes=1024)
Parquet.write("bounded.parquet", table; limits=limits)
```

Readers treat statistics as untrusted metadata. They validate physical widths,
logical ordering, and declared sort order before exposing a bound.

## Logical values

Use `Parquet.LogicalColumn` when a Julia element type does not contain the complete
Parquet schema. It can select ENUM, TIME, TIMESTAMP, or DECIMAL metadata. It also
supplies a schema for empty and all-null columns. Without it, such a column of
`Parquet.Decimal` values is written as DECIMAL(1, 0).

`Parquet.JSONValue` and `Parquet.BSONValue` preserve encoded documents. They validate
their input without building an object tree; `string(value)` is the document text.
`Parquet.Decimal` holds DECIMAL values wider than 76 digits.

### Preserve annotations when rewriting a table

Writing a `Parquet.Table` preserves its stored scalar annotations and file key-value
metadata. Extracting its columns into a new Tables.jl source loses that stored schema.
A Julia element type cannot always carry the complete annotation, so such columns
may be written with a different annotation even though their values remain exact.

- An `ENUM` column reads as strings and is written back as `STRING`.
- A millisecond or microsecond `TIME` column reads as `Dates.Time` and is written
  back as `TIME` with nanosecond units.
- A `Vector{UInt8}` column is written as `BYTE_ARRAY`. A list of `UInt8` elements is
  written as a `LIST` of 8-bit integers.

Use `Parquet.LogicalColumn` to select the exact annotation when it matters.

## Resource limits

Pass a `Parquet.Limits` value to `Parquet.File`, `Parquet.Table`, or `Parquet.write`.
Limits reject oversized metadata, pages, strings, decimals, and statistics bounds.
Row counts and total memory are unlimited by default. Set `max_container_elements` to
cap row and element counts, or `max_materialized_bytes` to cap the bytes a read
allocates, for example when reading untrusted files. `max_materialized_bytes` does not
cap writes.

Counts a file declares (values, rows, list and map lengths) are checked against the
bytes that back them before memory is reserved for them, and declared page sizes are
bounded by `max_page_bytes`, so corrupt metadata fails with a `FormatError` instead of
a large allocation. Highly repetitive data, such as long runs of nulls or of one
dictionary value, still decodes to its full size.

`max_schema_name_bytes` bounds the new top-level column names a single operation may
intern as Julia `Symbol`s. Interned names are process-permanent, so the registry also
keeps a cumulative byte count, but one file's names never consume a later
operation's budget.

### Nesting depth

Schema parsing is iterative and accepts very deep schemas under a raised
`max_metadata_depth`. Nested reading and writing recurse once per level. Reading
rejects a plan deeper than 1024 levels with a `LimitError` instead of exhausting the
stack; writing is bounded only by `max_metadata_depth`, so it can produce a synthetic
file that the reader declines. Ordinary Parquet nesting is far below these bounds.

## Sources and file lifetime

`Parquet.File` and `Parquet.Table` accept a path to one file, a byte vector, an `IO`, a
`Parquet.AbstractSource`, or (with CloudStore.jl loaded) a `CloudStore.Object`. A
directory path throws an `ArgumentError`. A path is memory mapped while it is read:
`Parquet.Table` unmaps and closes it before returning, and `Parquet.File` keeps it
mapped until `close(file)`. If another process truncates a mapped file while it is
open, reads of the removed region terminate the process, and no bounds check can
prevent that.

A `Parquet.AbstractSource` implements `Parquet.sourcelength` and `Parquet.readrange`,
and `Parquet.concurrentreads` when ranges may be read on several tasks at once. A read
takes the footer from one read of the source's last 64 KiB, then reads the column
chunks it decodes, merging chunks less than 64 KiB apart into one range. Parquet never
closes a source it was given.

```julia
using CloudStore, Parquet

object = CloudStore.Object(bucket, "data/part-0.parquet"; credentials)
table = Parquet.Table(object; select=[:id, :price])
```

When set, `max_materialized_bytes` covers the bytes this package allocates while
reading. A byte vector is borrowed and a mapped path is not copied, so neither is
charged. An `IO` source is copied and is charged. A source type supplied by another
package owns its own storage, so bytes it allocates are not charged here.

## Shared data values

Strings decode to `DataStrings.DataString`. A top-level STRING column is a
`DataStrings.StringVector`, whose values refer to the decoded pages rather than to
one copy per value. DECIMAL columns with precision up to
76 decode to fixed-scale `DataDecimals.Decimal` values. Wider decimals retain
`Parquet.Decimal`, whose coefficient is a `BigInt`. The writer accepts shared
fixed-scale and per-value-scale decimals. A fixed-scale type supplies the schema
for empty and all-null columns too.

`Durations.Duration` can be written as INTERVAL when its components are nonnegative,
its time component is an exact number of milliseconds, and each wire component
fits UInt32. INTERVAL columns read as `Durations.Duration`; a value with more than
2^31 - 1 months or days throws `Parquet.UnsupportedFeatureError`.
