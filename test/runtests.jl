include("get_parcompat.jl")
@testset "parquet tests" begin
    include("test_load.jl")
    include("test_codec.jl")
    include("test_cursors.jl")
    include("test_writer.jl")
end
