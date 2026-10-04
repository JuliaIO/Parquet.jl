# Parquet.jl

Parquet.jl is a pure-Julia reader and writer for the Apache Parquet columnar file
format. The package implements the Tables.jl interface and keeps its public API
small and namespaced.

!!! warning "Development branch"
    The `rewrite/1.0` branch is active development work. Do not use it for
    production data until its conformance gates are complete.

## Quick start

```@example quickstart
using Parquet
using Tables

path = joinpath(mktempdir(), "example.parquet")
source = (id=Int64[1, 2], label=Union{Missing,String}["alpha", missing])
Parquet.write(path, source)

table = Parquet.Table(path)
columns = Tables.columntable(table)
result = (id=collect(columns.id), label=collect(columns.label))
close(table)
result
```

See the [guide](@ref "Guide") for writer options, statistics, and resource limits.
See the [API reference](@ref "API reference") for the public types and functions.

## Format target

The stable contract is Apache Parquet format 2.13.0. The reader accepts stable
encodings and codecs, including deprecated input where practical. The writer emits
nondeprecated stable forms. The core does not depend on Arrow.jl or a native Thrift
compiler.
