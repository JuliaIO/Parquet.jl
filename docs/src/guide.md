# Guide

## Read a table

`Parquet.Table(path)` reads a Parquet file and exposes column access through
Tables.jl. Close the table when it is no longer needed.

```julia
using Parquet
using Tables

table = Parquet.Table("input.parquet")
columns = Tables.columntable(table)
close(table)
```

Use `Parquet.File(path)` when you need file metadata without materializing a table.

### Read some columns

Pass a `Tables.Scan` request to read only the columns it needs. The column chunks of
every other column are never read, so columns this package cannot decode can be
skipped too. The filter, row bounds, and renames apply after reading.

```julia
table = Parquet.Table("input.parquet";
    scan=Tables.Scan(select=(:id, :price => :amount), filter=Tables.col(:qty) > 0))
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
try
    columns = Tables.columntable(table)
    (
        name=String(columns.profile[2]["name"]),
        null_list=ismissing(columns.samples[1]),
        empty_list=isempty(columns.samples[2]),
        values=collect(columns.samples[3]),
        map=Dict(columns.attrs[3]),
    )
finally
    close(table)
end
```

Nested read values expose indexing and iteration; their concrete container types may
differ from the input. The reader also accepts the legacy list and map layouts tested
in the pinned Apache corpus. The rewrite remains preproduction.

## What this package does not support

Complete coverage of the format is not a goal, so a few parts are deliberately left
out. Reading one of them reports an unsupported feature rather than an invalid file.

- LZO compression, in either direction. Every available implementation is GPL-2 and
  this package is MIT.
- Writing INT96. The type is deprecated in the format. INT96 columns, which Spark,
  Hive, and Impala still write by default, are read as `Parquet.Timestamp{:nanos}`
  values that are not adjusted to UTC, as pyarrow reads them. A value outside that
  range (years 1677 to 2262) is a `FormatError`. Rewriting a `Parquet.Table` that has
  an INT96 column reports an unsupported feature; write its columns instead to store
  them as INT64 nanosecond timestamps.
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
`Parquet.Timestamp` values is written UTC-adjusted for milliseconds and local for
finer units, and a column of `Parquet.Decimal` values as DECIMAL(1, 0).

`Parquet.JSONValue` and `Parquet.BSONValue` preserve encoded documents. They validate
their input without building an object tree. `Parquet.Decimal`, `Parquet.Timestamp`,
and `Parquet.Interval` preserve exact Parquet values that have no single matching
Julia standard-library type.

### Preserve annotations when rewriting a table

Writing a `Parquet.Table` preserves its stored scalar annotations and file key-value
metadata. Extracting its columns into a new Tables.jl source loses that stored schema.
A Julia element type cannot always carry the complete annotation, so such columns
may be written with a different annotation even though their values remain exact.

- A millisecond `TIMESTAMP` reads as `Parquet.Timestamp{:millis}` when
  `isAdjustedToUTC` is true, and as `Dates.DateTime` when it is false. Microsecond
  and nanosecond timestamps always read as `Parquet.Timestamp`.
- An `ENUM` column reads as `String` and is written back as `STRING`.
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

`Parquet.File` and `Parquet.Table` accept a path, a byte vector, or an `IO`. A path is
memory mapped. `close` releases the file descriptor, but the mapping itself lives until
the garbage collector finalizes it. On Windows the file may therefore stay locked after
`close`. If another process truncates a mapped file while it is open, reads of the
removed region terminate the process, and no bounds check can prevent that.

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
fits UInt32. Read results retain `Parquet.Interval` because its unsigned range
exceeds Durations' signed month/day range. Convert with `Durations.Duration(x)`;
values outside the shared range throw instead of wrapping.
