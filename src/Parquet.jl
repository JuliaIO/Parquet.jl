module Parquet

using Thrift
using ProtoBuf
using Snappy
using Zlib
using LRUCache

import Base: show, open, close, values, start, next, done
import Thrift: isfilled

export is_par_file, ParFile, show, nrows, ncols, rowgroups, columns, pages, bytes, values, colname, colnames
export SchemaConverter, schema, JuliaConverter, ThriftConverter, ProtoConverter
export RowCursor, ColCursor, start, next, done

# enable logging only during debugging
using Logging
const logger = Logging.configure(level=DEBUG)
#const logger = Logging.configure(filename="/tmp/hive$(getpid()).log", level=DEBUG)
macro logmsg(s)
    quote
        debug($(esc(s)))
    end
end
#macro logmsg(s)
#end

# package code goes here
include("PAR2/PAR2.jl")
using .PAR2
include("codec.jl")
include("schema.jl")
include("reader.jl")
include("cursor.jl")
include("show.jl")

end # module
