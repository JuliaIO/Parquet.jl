using Parquet

include(joinpath(@__DIR__, "N6ParquetJLHarness.jl"))

length(ARGS) == 1 || throw(ArgumentError(
    "usage: generate.jl --write|--check"))
mode = ARGS[1] == "--write" ? :write :
    ARGS[1] == "--check" ? :check : throw(ArgumentError(
        "usage: generate.jl --write|--check"))
output = N6ParquetJLHarness.runharness(mode)
verb = mode === :write ? "wrote" : "checked"
println(verb, " ", length(output.files),
    " generated N6 files and 96 evidence records")
