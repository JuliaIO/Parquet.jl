module Parquet

using Thrift
using ProtoBuf
using Snappy
using CodecZlib
using MemPool
using Dates

import Base: show, open, close, values
import Thrift: isfilled

export is_par_file, ParFile, show, nrows, ncols, rowgroups, columns, pages, bytes, values, colname, colnames
export SchemaConverter, schema, JuliaConverter, ThriftConverter, ProtoConverter
export logical_timestamp, logical_string
export RowCursor, ColCursor, RecCursor
export AbstractBuilder, JuliaBuilder

# package code goes here
include("PAR2/PAR2.jl")
using .PAR2
include("codec.jl")
include("schema.jl")
include("reader.jl")
include("cursor.jl")
include("show.jl")

end # module
