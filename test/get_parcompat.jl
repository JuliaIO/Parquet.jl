using Parquet
using Test

function get_parcompat(parcompat=joinpath(dirname(@__FILE__), "parquet-compatibility"))
    # look for parquet-compatibility in test folder, clone to tempdir if not found
    if !isdir(parcompat)
        run(`git clone https://github.com/Parquet/parquet-compatibility.git $parcompat`)
    end
end

function get_juliaparcompat(juliaparcompat=joinpath(dirname(@__FILE__), "julia-parquet-compatibility"))
    # look for julia-parquet-compatibility in test folder, clone to tempdir if not found
    if !isdir(juliaparcompat)
        run(`git clone https://github.com/JuliaIO/parquet-compatibility.git $juliaparcompat`)
    end
end

get_parcompat()
get_juliaparcompat()
