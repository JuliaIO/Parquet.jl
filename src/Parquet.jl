module Parquet

using Thrift
using Snappy
using CodecZlib
using CodecZstd
using MemPool
using Dates

if VERSION < v"1.3"
    using Missings: nonmissingtype
end

const PARQUET_JL_VERSION = v"0.5.1"

import Base: show, open, close, values, eltype, length
import Thrift: isfilled

export is_par_file, ParFile, show, nrows, ncols, rowgroups, columns, pages, bytes, values, colname, colnames
export schema
export logical_timestamp, logical_string
export RecordCursor, BatchedColumnsCursor
export write_parquet

# package code goes here
include("PAR2/PAR2.jl")
using .PAR2
include("codec.jl")
include("schema.jl")
include("reader.jl")
include("cursor.jl")
include("show.jl")
include("writer.jl")

end # module
