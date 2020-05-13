module Parquet

using Thrift
using ProtoBuf
using Snappy
using CodecZlib
using CodecZstd
using MemPool
using Dates

if VERSION < v"1.3"
    using Missings: nonmissingtype
end

using Pkg
const PARQUET_JL_VERSION = VersionNumber(Pkg.TOML.parsefile(joinpath(@__DIR__, "..", "Project.toml"))["version"])

import Base: show, open, close, values
import Thrift: isfilled

export is_par_file, ParFile, show, nrows, ncols, rowgroups, columns, pages, bytes, values, colname, colnames
export SchemaConverter, schema, JuliaConverter, ThriftConverter, ProtoConverter
export logical_timestamp, logical_string
export RowCursor, ColCursor, RecCursor
export AbstractBuilder, JuliaBuilder
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
