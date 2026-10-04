using Parquet
using Test

@testset "N5 nested conformance" begin
    include("corpus.jl")
    include("model/runtests.jl")
    include("hardening/source_mutation.jl")
    include("external.jl")
end
