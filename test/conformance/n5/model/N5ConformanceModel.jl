module N5ConformanceModel

using Parquet
using SHA

const MD = Parquet.Metadata
const TH = Parquet.Thrift

include("model.jl")
include("wire.jl")
include("goldens.jl")
include("properties.jl")
include("manifest.jl")
include("integration.jl")

end
