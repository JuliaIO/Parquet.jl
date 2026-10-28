# Parquet.jl

Parquet.jl is a pure-Julia reader and writer for the Apache Parquet columnar file
format. Files read as Tables.jl columns and any Tables.jl table writes. The public API
is small and namespaced: nothing but the deprecated 0.8 names is exported.

## Quick start

```@example quickstart
using Parquet
using Tables

path = joinpath(mktempdir(), "example.parquet")
source = (id=Int64[1, 2], label=Union{Missing,String}["alpha", missing])
Parquet.write(path, source)

table = Parquet.Table(path)
columns = Tables.columntable(table)
(id=collect(columns.id), label=collect(columns.label))
```

[Reading](@ref) covers columns, filters, row groups, threads, and sources;
[Datasets](@ref) reads directories of files; [Writing](@ref) covers the writer's options;
[Type mapping](@ref) lists the column types in both directions; and
[Limits and errors](@ref) covers resource limits, errors, and what is not supported. The
[API reference](@ref "API reference") lists the public types and functions.

## Format

Parquet.jl implements Apache Parquet format 2.13.0. It reads every encoding and codec of
that version, deprecated ones included, except LZO; it writes the forms that are not
deprecated. It needs no Arrow or Thrift library. [Limits and errors](@ref) lists what is
not supported.
