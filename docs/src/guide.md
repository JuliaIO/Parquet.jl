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

## Write a table

`Parquet.write` accepts a Tables.jl source and a path or `IO` sink.

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
supplies a schema for empty and all-null columns.

`Parquet.JSONValue` and `Parquet.BSONValue` preserve encoded documents. They validate
their input without building an object tree. `Parquet.Decimal`, `Parquet.Timestamp`,
and `Parquet.Interval` preserve exact Parquet values that have no single matching
Julia standard-library type.

## Resource limits

Pass a `Parquet.Limits` value to `Parquet.File`, `Parquet.Table`, or `Parquet.write`.
Limits reject oversized metadata, pages, strings, decimals, statistics bounds, and
materialized values before large allocations occur.
