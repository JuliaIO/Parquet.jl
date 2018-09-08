module Parquet

using Thrift
using ProtoBuf
using Snappy
using Libz
using MemPool
using Compat

import Base: show, open, close, values, start, next, done
import Thrift: isfilled

export is_par_file, ParFile, show, nrows, ncols, rowgroups, columns, pages, bytes, values, colname, colnames
export SchemaConverter, schema, JuliaConverter, ThriftConverter, ProtoConverter
export RowCursor, ColCursor, RecCursor, start, next, done
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
