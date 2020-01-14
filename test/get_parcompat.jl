using Parquet
using Test

function get_parcompat(parcompat=joinpath(dirname(@__FILE__), "parquet-compatibility"))
    # look for parquet-compatibility in test folder, clone to tempdir if not found
    if !isdir(parcompat)
        parcompat = joinpath(dirname(@__FILE__), "parquet-compatibility")
        run(`git clone https://github.com/JuliaIO/parquet-compatibility.git $parcompat`)
    end
end

get_parcompat()
