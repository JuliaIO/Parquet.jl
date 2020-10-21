module Parquet

using Thrift
using Snappy
using CodecZlib
using CodecZstd
using Dates
using Decimals
using Mmap
using Base.Threads

if VERSION < v"1.3"
    using Missings: nonmissingtype
end

if VERSION < v"1.5"
    Base.signed(::Type{UInt16}) = Int16
    Base.signed(::Type{UInt32}) = Int32
    Base.signed(::Type{UInt64}) = Int64
    Base.signed(::Type{UInt128}) = Int128
end    

const PARQUET_JL_VERSION = v"0.7.0"

const _use_mmap = true

function use_mmap(b::Bool)
    global _use_mmap = b
end

import Base: show, open, close, values, eltype, length

export is_par_file, show, nrows, ncols, rowgroups, columns, pages, bytes, values, colname, colnames
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
