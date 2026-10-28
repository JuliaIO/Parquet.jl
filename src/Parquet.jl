module Parquet

import DataDecimals, DataStrings, Durations

using Mmap
import CRC32

include("errors.jl")
include("thrift.jl")
include("metadata/parquet.jl")
include("schema.jl")
include("nested_schema.jl")
include("logical.jl")
include("logical_temporal.jl")
include("logical_json.jl")
include("logical_bson.jl")
include("logical_binary.jl")
include("logical_decimal.jl")
include("arrow_schema.jl")
include("statistics.jl")
include("vectors.jl")
include("dremel.jl")
include("source.jl")
include("footer.jl")
include("plain.jl")
include("rle.jl")
include("delta.jl")
include("bss.jl")
include("checksum.jl")
include("page.jl")
include("codecs.jl")
include("dictionary.jl")
include("column.jl")
include("nested_reader.jl")
include("nested_table.jl")
include("xxhash.jl")
include("bloom.jl")
include("write_types.jl")
include("write_encode.jl")
include("write.jl")
include("write_statistics.jl")
include("write_logical.jl")
include("logical_column.jl")
include("table.jl")
include("pushdown.jl")
include("dataset.jl")
include("write_nested.jl")
include("writer.jl")
include("write_partitioned.jl")
include("precompile.jl")

# Parquet.jl 0.8 names, kept until 2.0. read_parquet takes 0.8's keywords too: `rows`
# (a range of rows), `use_threads`, and `batchsize` (which no longer applies).
function read_parquet(path; rows::Union{Nothing,AbstractUnitRange}=nothing,
        use_threads::Union{Nothing,Bool}=nothing, batchsize=nothing, kwargs...)
    Base.depwarn("`read_parquet(path; kwargs...)` is deprecated, use " *
        "`Parquet.Table(path; kwargs...)` instead.", :read_parquet)
    use_threads === nothing ||
        (kwargs = (; kwargs..., ntasks=use_threads ? Threads.nthreads() : 1))
    rows === nothing ||
        (kwargs = (; kwargs..., scan=Tables.Scan(offset=first(rows) - 1, limit=length(rows))))
    return Table(path; kwargs...)
end
export read_parquet
function write_parquet(sink, table; compression_codec="SNAPPY")
    Base.depwarn("`write_parquet(sink, table; compression_codec)` is deprecated, use " *
        "`Parquet.write(sink, table; compress)` instead.", :write_parquet)
    return write(sink, table; compress=Symbol(lowercase(String(compression_codec))))
end
export write_parquet

if VERSION >= v"1.11"
    Core.eval(@__MODULE__, Expr(:public, :AbstractSource, :BSONValue, :Dataset, :Decimal, :File,
        :FormatError, :JSONValue, :LimitError, :Limits, :ListValue, :ListVector,
        :LogicalColumn, :MapValue, :MapVector, :StructValue, :StructVector, :Table,
        :UnsupportedFeatureError, :Writer, :col, :colcmp, :colin, :concurrentreads,
        :isnull, :metadata, :read, :readrange, :sourcelength, :write))
end

end
