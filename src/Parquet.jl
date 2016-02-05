module Parquet

using Thrift
using ProtoBuf
using Snappy
using GZip

import Base: show
import Thrift: isfilled

export is_par_file, ParFile, show, rowgroups, columns, pages

# enable logging only during debugging
using Logging
const logger = Logging.configure(level=DEBUG)
##const logger = Logging.configure(filename="/tmp/hive$(getpid()).log", level=DEBUG)
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
include("reader.jl")
include("show.jl")

end # module
