# Parquet

[![CI](https://github.com/JuliaIO/Parquet.jl/actions/workflows/ci.yaml/badge.svg)](https://github.com/JuliaIO/Parquet.jl/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/JuliaIO/Parquet.jl/graph/badge.svg?token=qchPEYSd5Q)](https://codecov.io/gh/JuliaIO/Parquet.jl)

## Reader

A [parquet file](https://en.wikipedia.org/wiki/Apache_Parquet) or dataset can be loaded using the `read_parquet` function. A parquet dataset is a directory with multiple parquet files, each of which is a partition belonging to the dataset.

`read_parquet(path; kwargs...)` returns a `Parquet.Table` or `Parquet.Dataset`, which is the table contained in the parquet file or dataset in an Tables.jl compatible format.

Options:
- `rows`: The row range to iterate through, all rows by default. Applicable only when reading a single file.
- `filter`: Filter function to apply while loading only a subset of partitions from a dataset. The path to the partition is provided as a parameter.
- `batchsize`: Maximum number of rows to read in each batch (default: row count of first row group). Applied only when reading a single file, and to each file when reading a dataset.
- `use_threads`: Whether to use threads while reading the file; applicable only for Julia v1.3 and later and switched on by default if julia processes is started with multiple threads.
- `column_generator`: Function to generate a partitioned column when not found in the partitioned table. Parameters provided to the function: table, column index, length of column to generate. Default implementation determines column values from the table path.

The returned object is a Tables.jl compatible Table and can be converted to other forms, e.g. a `DataFrames.DataFrame` via

```julia
using Parquet, DataFrames
df = DataFrame(read_parquet(path))
```

Partitions in a parquet file or dataset can also be iterated over using an iterator returned by the `Tables.partitions` method.

```julia
using Parquet, DataFrames
for partition in Tables.partitions(read_parquet(path))
    df = DataFrame(partition)
    ...
end
```

### Lower Level Reader

Load a [parquet file](https://en.wikipedia.org/wiki/Apache_Parquet). Only metadata is read initially, data is loaded in chunks on demand. (Note: [ParquetFiles.jl](https://github.com/queryverse/ParquetFiles.jl) also provides load support for Parquet files under the FileIO.jl package.)

`Parquet.File` represents a Parquet file at `path` open for reading.

```
Parquet.File(path) => Parquet.File
```

`Parquet.File` keeps a handle to the open file and the file metadata and also holds a weakly referenced cache of page data read. If the parquet file references other files in its metadata, they will be opened as and when required for reading and closed when they are not needed anymore.

The `close` method closes the reader, releases open files and makes cached internal data structures available for GC. A `Parquet.File` instance must not be used once closed.

```julia
julia> using Parquet

julia> filename = "customer.impala.parquet";

julia> parquetfile = Parquet.File(filename)
Parquet file: customer.impala.parquet
    version: 1
    nrows: 150000
    created by: impala version 1.2-INTERNAL (build a462ec42e550c75fccbff98c720f37f3ee9d55a3)
    cached: 0 column chunks
```

Examine the schema.

```julia
julia> nrows(parquetfile)
150000

julia> ncols(parquetfile)
8

julia> colnames(parquetfile)
8-element Array{Array{String,1},1}:
 ["c_custkey"]
 ["c_name"]
 ["c_address"]
 ["c_nationkey"]
 ["c_phone"]
 ["c_acctbal"]
 ["c_mktsegment"]
 ["c_comment"]

julia> schema(parquetfile)
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

The reader performs logical type conversions automatically for String (from byte arrays), decimals (from fixed length byte arrays) and DateTime (from Int96). It depends on the converted type being populated correctly in the file metadata to detect such conversions. To take care of files where such metadata is not populated, an optional `map_logical_types` argument can be provided while opening the parquet file. The `map_logical_types` value must map column names to a tuple of return type and converter functon. Return types of String and DateTime are supported as of now, and default implementations for them are included in the package.

```julia
julia> mapping = Dict(["column_name"] => (String, Parquet.logical_string));

julia> parquetfile = Parquet.File("filename"; map_logical_types=mapping);
```

The reader will interpret logical types based on the `map_logical_types` provided. The following logical type mapping methods are available in the Parquet package.

- `logical_timestamp(v; offset=Dates.Second(0))`: Applicable for timestamps that are `INT96` values. This converts the data read as `Int128` types to `DateTime` types.
- `logical_string(v)`: Applicable for strings that are `BYTE_ARRAY` values. Without this, they are represented in a `Vector{UInt8}` type. With this they are converted to `String` types.
- `logical_decimal(v, precision, scale; use_float=true)`: Applicable for reading decimals from `FIXED_LEN_BYTE_ARRAY`, `INT64`, or `INT32` values. This converts the data read as those types to `Integer`, `Float64` or `Decimal` of the given precision and scale, depending on the options provided.

Variants of these methods or custom methods can also be applied by caller.

### BatchedColumnsCursor

Create cursor to iterate over batches of column values. Each iteration returns a named tuple of column names with batch of column values. Files with nested schemas can not be read with this cursor.

```julia
BatchedColumnsCursor(parquetfile::Parquet.File; kwargs...)
```

Cursor options:
- `rows`: the row range to iterate through, all rows by default.
- `batchsize`: maximum number of rows to read in each batch (default: row count of first row group).
- `reusebuffer`: boolean to indicate whether to reuse the buffers with every iteration; if each iteration processes the batch and does not need to refer to the same data buffer again, then setting this to `true` reduces GC pressure and can help significantly while processing large files.
- `use_threads`: whether to use threads while reading the file; applicable only for Julia v1.3 and later and switched on by default if julia processes is started with multiple threads.

Example:

```julia
julia> typemap = Dict(["c_name"]=>(String,Parquet.logical_string), ["c_address"]=>(String,Parquet.logical_string));

julia> parquetfile = Parquet.File("customer.impala.parquet"; map_logical_types=typemap);

julia> cc = BatchedColumnsCursor(parquetfile)
Batched Columns Cursor on customer.impala.parquet
    rows: 1:150000
    batches: 1
    cols: c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment

julia> batchvals, state = iterate(cc);

julia> propertynames(batchvals)
(:c_custkey, :c_name, :c_address, :c_nationkey, :c_phone, :c_acctbal, :c_mktsegment, :c_comment)

julia> length(batchvals.c_name)
150000

julia> batchvals.c_name[1:5]
5-element Array{Union{Missing, String},1}:
 "Customer#000000001"
 "Customer#000000002"
 "Customer#000000003"
 "Customer#000000004"
 "Customer#000000005"
```

### RecordCursor

Create cursor to iterate over records. In parallel mode, multiple remote cursors can be created and iterated on in parallel.

```julia
RecordCursor(parquetfile::Parquet.File; kwargs...)
```

Cursor options:
- `rows`: the row range to iterate through, all rows by default.
- `colnames`: the column names to retrieve; all by default

Example:

```julia
julia> typemap = Dict(["c_name"]=>(String,Parquet.logical_string), ["c_address"]=>(String,Parquet.logical_string));

julia> parquetfile = Parquet.File("customer.impala.parquet"; map_logical_types=typemap);

julia> rc = RecordCursor(parquetfile)
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
