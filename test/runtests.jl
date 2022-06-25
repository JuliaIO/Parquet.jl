using Parquet
using Test
using LazyArtifacts, Artifacts

# Note: readdir(...; join=true) requires Julia v1.4.
const parcompat = joinpath(artifact"parcompat", readdir(artifact"parcompat")[1])
const julia_parcompat = joinpath(artifact"julia_parcompat", readdir(artifact"julia_parcompat")[1])

@testset "parquet tests" begin
    include("test_load.jl")
    include("test_codec.jl")
    include("test_cursors.jl")
    include("test_writer.jl")
end
