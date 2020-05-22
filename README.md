# Parquet

[![Build Status](https://travis-ci.org/JuliaIO/Parquet.jl.svg?branch=master)](https://travis-ci.org/JuliaIO/Parquet.jl)
[![Build status](https://ci.appveyor.com/api/projects/status/gx8pvdiiery74r9l/branch/master?svg=true)](https://ci.appveyor.com/project/tanmaykm/parquet-jl-cufdj/branch/master)
[![Coverage Status](https://coveralls.io/repos/github/JuliaIO/Parquet.jl/badge.svg?branch=master)](https://coveralls.io/github/JuliaIO/Parquet.jl?branch=master)

## Reader

### High level reader

You can read a parquet file using `read_parquet` for example

```
df =  read_parquet(parquet_file_path);
```

### Lower level reader

Load a [parquet file](https://en.wikipedia.org/wiki/Apache_Parquet). Only metadata is read initially, data is loaded in chunks on demand. (Note: [ParquetFiles.jl](https://github.com/queryverse/ParquetFiles.jl) also provides load support for Parquet files under the FileIO.jl package.)

`ParFile` represents a Parquet file at `path` open for reading. Options to map logical types can be provided via `map_logical_types`.

```
ParFile(path; map_logical_types) => ParFile
```

`map_logical_types` can be one of:

- `false`: no mapping is done (default)
- `true`: default mappings are attempted on all columns (bytearray => String, int96 => DateTime)
- A user supplied dict mapping column names to a tuple of type and a converter function

`ParFile` also keeps a handle to the open file and the file metadata and also holds a LRU cache of raw bytes of the pages read. If the parquet file references other files in its metadata, they will be opened as and when required for reading and closed when they are not needed anymore.

The `close` method closes the reader, releases open files and makes cached internal data structures available for GC. A `ParFile` instance must not be used once closed.

```julia
julia> using Parquet

julia> parfile = "customer.impala.parquet";

julia> p = ParFile(parfile; map_logical_types=true)
Parquet file: customer.impala.parquet
    version: 1
    nrows: 150000
    created by: impala version 1.2-INTERNAL (build a462ec42e550c75fccbff98c720f37f3ee9d55a3)
    cached: 0 column chunks
```

Examine the schema.

```julia
julia> nrows(p)
150000

julia> ncols(p)
8

julia> colnames(p)
8-element Array{Array{String,1},1}:
 ["c_custkey"]
 ["c_name"]
 ["c_address"]
 ["c_nationkey"]
 ["c_phone"]
 ["c_acctbal"]
 ["c_mktsegment"]
 ["c_comment"]

julia> schema(p)
Schema:
    schema {
      optional INT64 c_custkey
      optional BYTE_ARRAY c_name
      optional BYTE_ARRAY c_address
      optional INT32 c_nationkey
      optional BYTE_ARRAY c_phone
      optional DOUBLE c_acctbal
      optional BYTE_ARRAY c_mktsegment
      optional BYTE_ARRAY c_comment
    }
```

Create cursor to iterate over records. In parallel mode, multiple remote cursors can be created and iterated on in parallel.

```julia
julia> rc = RecordCursor(p)
Record Cursor on customer.impala.parquet
    rows: 1:150000
    cols: c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment

julia> records = collect(rc);

julia> length(records)
150000

julia> first_record = first(records);

julia> isa(first_record, NamedTuple)
true

julia> propertynames(first_record)
(:c_custkey, :c_name, :c_address, :c_nationkey, :c_phone, :c_acctbal, :c_mktsegment, :c_comment)

julia> first_record.c_custkey
1

julia> first_record.c_name
"Customer#000000001"

julia> first_record.c_address
"IVhzIApeRb ot,c,E"
```

The reader will interpret logical types based on the `map_logical_types` provided. The following logical type mapping methods are available in the Parquet package and are applied by default if `map_logical_types` is set to `true`.

- `logical_timestamp(v; offset::Dates.Period=Dates.Second(0))`: Applicable for timestamps that are `INT96` values. Without this they are represented in a `Int128` type. With this they are converted to `DateTime` types.
- `logical_string(v): Applicable for strings that are `BYTE_ARRAY` values. Without this, they are represented in a `Vector{UInt8}` type. With this they are converted to `String` types.

Variants of these methods or custom methods can also be applied by caller.

## Writer

You can write any Tables.jl column-accessible table that contains columns of these types and their union with `Missing`: `Int32`, `Int64`, `String`, `Bool`, `Float32`, `Float64`.

However, `CategoricalArray`s are not yet supported. Furthermore, these types are not yet supported: `Int96`, `Int128`, `Date`, and `DateTime`.

### Writer Example

```julia
tbl = (
    int32 = Int32.(1:1000),
    int64 = Int64.(1:1000),
    float32 = Float32.(1:1000),
    float64 = Float64.(1:1000),
    bool = rand(Bool, 1000),
    string = [randstring(8) for i in 1:1000],
    int32m = rand([missing, 1:100...], 1000),
    int64m = rand([missing, 1:100...], 1000),
    float32m = rand([missing, Float32.(1:100)...], 1000),
    float64m = rand([missing, Float64.(1:100)...], 1000),
    boolm = rand([missing, true, false], 1000),
    stringm = rand([missing, "abc", "def", "ghi"], 1000)
)

file = tempname()*".parquet"
write_parquet(file, tbl)
```
