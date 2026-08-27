module Parquet

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
include("write.jl")
include("write_statistics.jl")
include("write_logical.jl")
include("logical_column.jl")
include("write_nested.jl")
include("table.jl")
include("write_provenance.jl")
include("write_splitting.jl")
include("page_index.jl")

if VERSION >= v"1.11"
    Core.eval(@__MODULE__, Expr(:public, :BSONValue, :Decimal, :File, :Interval,
        :JSONValue, :Limits, :LogicalColumn, :Table, :Timestamp, :close!, :write))
end

end
